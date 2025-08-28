use sqlparser::ast::{self as S, Insert, Join, JoinConstraint, JoinOperator};

use crate::optimizer::utils::{
    expr_refs_only_alias, join_conjuncts, projection_is_direct_columns,
    query_has_no_limit_or_fetch, select_is_simple_no_cardinality, split_conjuncts,
    strip_alias_in_expr,
};

/// Проброс предикатов (`WHERE`) внутрь простых подзапросов во `FROM` (predicate pushdown).
///
/// Консервативная эвристика (включается в «агрессивном» профиле):
/// - обрабатываем `FROM (SELECT ... FROM <base>) AS s`, где подзапрос — простой `SELECT`
///   без `DISTINCT/GROUP BY/HAVING`, без `LIMIT/FETCH` (и без объединений в `body`);
/// - у подзапроса один источник во `FROM` и без `JOIN`;
/// - проекция подзапроса состоит только из прямых ссылок на колонки (без `*`, алиасов и выражений).
///
/// Если внешний `SELECT` накладывает условие `WHERE`, чьи конъюнкты используют
/// только колонки подзапроса `s` (или не квалифицированы), то такие конъюнкты
/// переносятся во внутренний `WHERE`. Остальные конъюнкты остаются снаружи.
///
/// Функция **мутирует** переданный `Statement` и ничего не возвращает.
pub fn predicate_pushdown(stmt: &mut S::Statement) {
    // --- helpers ---

    #[inline]
    fn derived_accepts_pushdown(tf: &S::TableFactor) -> bool {
        match tf {
            S::TableFactor::Derived { subquery, .. } => {
                if !query_has_no_limit_or_fetch(subquery) {
                    return false;
                }
                match subquery.body.as_ref() {
                    S::SetExpr::Select(sel) => {
                        if !select_is_simple_no_cardinality(sel) {
                            return false;
                        }
                        if sel.from.len() != 1 || !sel.from[0].joins.is_empty() {
                            return false;
                        }
                        projection_is_direct_columns(sel)
                    }
                    _ => false,
                }
            }
            _ => false,
        }
    }

    // Визиторы (обход дерева)

    fn visit_expr(e: &mut S::Expr) {
        use S::Expr::*;
        match e {
            Subquery(q) => visit_query(q),
            Exists { subquery, .. } => visit_query(subquery),
            InSubquery { expr, subquery, .. } => {
                visit_expr(expr);
                visit_query(subquery);
            }
            UnaryOp { expr, .. } => visit_expr(expr),
            BinaryOp { left, right, .. } => {
                visit_expr(left);
                visit_expr(right);
            }
            Between {
                expr, low, high, ..
            } => {
                visit_expr(expr);
                visit_expr(low);
                visit_expr(high);
            }
            Cast { expr, .. } => visit_expr(expr),
            Extract { expr, .. } => visit_expr(expr),
            Nested(inner) => visit_expr(inner),
            Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(op) = operand.as_mut() {
                    visit_expr(op);
                }
                for w in conditions.iter_mut() {
                    visit_expr(&mut w.condition);
                    visit_expr(&mut w.result);
                }
                if let Some(er) = else_result.as_mut() {
                    visit_expr(er);
                }
            }
            Function(S::Function { args, .. }) => {
                use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
                match args {
                    FunctionArguments::List(list) => {
                        for a in &mut list.args {
                            match a {
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(x))
                                | FunctionArg::Named {
                                    arg: FunctionArgExpr::Expr(x),
                                    ..
                                } => visit_expr(x),
                                _ => {}
                            }
                        }
                    }
                    FunctionArguments::Subquery(q) => visit_query(q),
                    _ => {}
                }
            }
            _ => {}
        }
    }

    fn visit_join(j: &mut Join) {
        match &mut j.join_operator {
            JoinOperator::Join(c)
            | JoinOperator::Inner(c)
            | JoinOperator::Left(c)
            | JoinOperator::LeftOuter(c)
            | JoinOperator::Right(c)
            | JoinOperator::RightOuter(c)
            | JoinOperator::FullOuter(c)
            | JoinOperator::Semi(c)
            | JoinOperator::LeftSemi(c)
            | JoinOperator::RightSemi(c)
            | JoinOperator::Anti(c)
            | JoinOperator::LeftAnti(c)
            | JoinOperator::RightAnti(c)
            | JoinOperator::StraightJoin(c) => {
                if let JoinConstraint::On(e) = c {
                    visit_expr(e);
                }
            }
            JoinOperator::AsOf {
                match_condition,
                constraint,
            } => {
                visit_expr(match_condition);
                if let JoinConstraint::On(e) = constraint {
                    visit_expr(e);
                }
            }
            JoinOperator::CrossJoin | JoinOperator::CrossApply | JoinOperator::OuterApply => {}
        }
    }

    fn visit_table_with_joins(twj: &mut S::TableWithJoins) {
        visit_table_factor(&mut twj.relation);
        for j in &mut twj.joins {
            visit_table_factor(&mut j.relation);
            visit_join(j);
        }
    }
    fn visit_table_factor(tf: &mut S::TableFactor) {
        match tf {
            S::TableFactor::Derived { subquery, .. } => visit_query(subquery),
            S::TableFactor::NestedJoin {
                table_with_joins, ..
            } => visit_table_with_joins(table_with_joins),
            _ => {}
        }
    }

    // Собственно попытка pushdown на уровне одного SELECT
    fn try_pushdown_in_select(sel: &mut S::Select) {
        // Разрешаем только один источник и без JOIN'ов
        if sel.from.len() != 1 || !sel.from[0].joins.is_empty() {
            return;
        }
        // Нужен внешний WHERE; если его нет — делать нечего
        let outer_where = match sel.selection.take() {
            Some(e) => e,
            None => return,
        };

        // Проверить, что relation — Derived и пригоден для pushdown
        let (alias_name_opt, can_push) = {
            let rel = &sel.from[0].relation;
            if !derived_accepts_pushdown(rel) {
                (None, false)
            } else {
                if let S::TableFactor::Derived { alias, .. } = rel {
                    let a = alias.as_ref().map(|a| a.name.value.clone());
                    (a, true)
                } else {
                    (None, false)
                }
            }
        };

        if !can_push {
            // вернуть исходный where на место
            sel.selection = Some(outer_where);
            return;
        }

        let alias_name = match alias_name_opt {
            Some(s) => s,
            None => {
                sel.selection = Some(outer_where);
                return;
            }
        };

        // Разобрать конъюнкцию и отфильтровать переносимые части
        let mut parts = Vec::new();
        split_conjuncts(outer_where, &mut parts);

        let mut pushable = Vec::new();
        let mut remain = Vec::new();
        for mut e in parts {
            if expr_refs_only_alias(&e, &alias_name) {
                strip_alias_in_expr(&mut e, &alias_name); // «s.a» → «a»
                pushable.push(e);
            } else {
                remain.push(e);
            }
        }

        // Если нечего переносить — вернуть WHERE
        if pushable.is_empty() {
            sel.selection = join_conjuncts(remain);
            return;
        }

        // Достать внутренний SELECT и слить предикаты
        if let S::TableFactor::Derived { subquery, .. } = &mut sel.from[0].relation {
            if let S::SetExpr::Select(inner_sel) = subquery.body.as_mut() {
                let inner = inner_sel.as_mut();
                let moved = join_conjuncts(pushable);
                if let Some(m) = moved {
                    if let Some(cur) = inner.selection.take() {
                        inner.selection = Some(S::Expr::BinaryOp {
                            left: Box::new(cur),
                            op: S::BinaryOperator::And,
                            right: Box::new(m),
                        });
                    } else {
                        inner.selection = Some(m);
                    }
                }
            }
        }

        // Остаток оставить снаружи
        sel.selection = join_conjuncts(remain);
    }

    fn visit_set_expr(se: &mut S::SetExpr) {
        match se {
            S::SetExpr::Select(s) => {
                // сначала рекурсивно обойти вниз
                let s_mut = s.as_mut();
                for twj in &mut s_mut.from {
                    visit_table_with_joins(twj);
                }
                if let Some(e) = &mut s_mut.selection {
                    visit_expr(e);
                }
                if let Some(e) = &mut s_mut.having {
                    visit_expr(e);
                }
                if let S::GroupByExpr::Expressions(exprs, _) = &mut s_mut.group_by {
                    for e in exprs {
                        visit_expr(e);
                    }
                }
                for it in &mut s_mut.projection {
                    if let S::SelectItem::UnnamedExpr(e)
                    | S::SelectItem::ExprWithAlias { expr: e, .. } = it
                    {
                        visit_expr(e);
                    }
                }

                // затем попытаться сделать pushdown на текущем уровне
                try_pushdown_in_select(s_mut);
            }
            S::SetExpr::Query(q) => visit_query(q),
            S::SetExpr::SetOperation { left, right, .. } => {
                visit_set_expr(left);
                visit_set_expr(right);
            }
            _ => {}
        }
    }

    fn visit_query(q: &mut S::Query) {
        if let Some(w) = &mut q.with {
            for cte in &mut w.cte_tables {
                visit_query(&mut cte.query);
            }
        }
        visit_set_expr(q.body.as_mut());
    }

    // Точка входа
    match stmt {
        S::Statement::Query(q) => visit_query(q),
        S::Statement::Insert(Insert { source, .. }) => {
            if let Some(q) = source.as_mut() {
                visit_query(q);
            }
        }
        S::Statement::Update {
            selection,
            assignments,
            from,
            table: twj,
            ..
        } => {
            for a in assignments {
                visit_expr(&mut a.value);
            }
            if let Some(e) = selection {
                visit_expr(e);
            }
            visit_table_with_joins(twj);
            if let Some(kind) = from {
                match kind {
                    S::UpdateTableFromKind::BeforeSet(list)
                    | S::UpdateTableFromKind::AfterSet(list) => {
                        for t in list {
                            visit_table_with_joins(t);
                        }
                    }
                }
            }
        }
        S::Statement::Delete(S::Delete {
            selection,
            using,
            from,
            ..
        }) => {
            if let Some(e) = selection {
                visit_expr(e);
            }
            match from {
                S::FromTable::WithoutKeyword(list) | S::FromTable::WithFromKeyword(list) => {
                    for twj in list {
                        visit_table_with_joins(twj);
                    }
                }
            }
            if let Some(list) = using {
                for t in list {
                    visit_table_with_joins(t);
                }
            }
        }
        _ => {}
    }
}
