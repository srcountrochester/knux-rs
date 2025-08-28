use sqlparser::ast::{self as S, Insert, Join, JoinConstraint, JoinOperator};

use crate::optimizer::utils::{
    and_merge, projection_is_direct_columns, select_is_simple_no_cardinality,
};

/// Сплющивание тривиальных подзапросов `SELECT ... FROM (SELECT ...) AS s`.
///
/// Условия (консервативно, без изменения кардинальности):
/// - подзапрос во `FROM` — это `Query { body: Select(...) }` без `LIMIT/FETCH/ORDER BY`;
/// - внутри `Select` НЕТ `DISTINCT/GROUP BY/HAVING`, один источник во `FROM` и без `JOIN`;
/// - проекция подзапроса — только прямые ссылки на колонки (`col` или `u.col`), без `*`, алиасов и выражений.
///
/// Действия:
/// - заменить `FROM (SELECT col_list FROM T [WHERE ...]) AS s` на `FROM T AS s`;
/// - предикат из внутреннего `WHERE` склеить конъюнкцией с внешним `WHERE`.
///
/// Функция **мутирует** переданный `Statement` и ничего не возвращает.
#[inline]
pub fn flatten_simple_subqueries(stmt: &mut S::Statement) {
    // --- helpers (те же критерии, что и в pull-up) ---

    #[inline]
    fn query_has_no_limit_or_order(q: &S::Query) -> bool {
        q.order_by.is_none() && q.limit_clause.is_none() && q.fetch.is_none()
    }

    #[inline]
    fn derived_is_trivial(tf: &S::TableFactor) -> bool {
        match tf {
            S::TableFactor::Derived { subquery, .. } => {
                if !query_has_no_limit_or_order(subquery) {
                    return false;
                }
                match subquery.body.as_ref() {
                    S::SetExpr::Select(sel) => {
                        select_is_simple_no_cardinality(sel)
                            && sel.from.len() == 1
                            && sel.from[0].joins.is_empty()
                            && projection_is_direct_columns(sel)
                    }
                    _ => false,
                }
            }
            _ => false,
        }
    }

    // --- обход дерева ---

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
            Cast { expr, .. } | Extract { expr, .. } => visit_expr(expr),
            Nested(inner) => visit_expr(inner),
            S::Expr::Case {
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
            S::Expr::Function(S::Function { args, .. }) => {
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

    // Собственно «flatten»: заменить Derived → Table и перенести WHERE внутрь наружу.
    fn try_flatten_in_select(sel: &mut S::Select) {
        // вниз по дереву сначала
        for it in &mut sel.projection {
            if let S::SelectItem::UnnamedExpr(e) | S::SelectItem::ExprWithAlias { expr: e, .. } = it
            {
                visit_expr(e);
            }
        }
        if let Some(e) = &mut sel.selection {
            visit_expr(e);
        }
        if let Some(e) = &mut sel.having {
            visit_expr(e);
        }
        if let S::GroupByExpr::Expressions(exprs, _) = &mut sel.group_by {
            for e in exprs {
                visit_expr(e);
            }
        }
        for twj in &mut sel.from {
            visit_table_with_joins(twj);
        }

        // допустим только один источник без JOIN'ов
        if sel.from.len() != 1 || !sel.from[0].joins.is_empty() {
            return;
        }

        // проверка без удержания &mut заимствования
        let can_flatten = {
            let rel = &sel.from[0].relation;
            derived_is_trivial(rel)
        };
        if !can_flatten {
            return;
        }

        // забираем inner WHERE до замены relation
        let mut inner_where: Option<S::Expr> = None;
        if let S::TableFactor::Derived { subquery, .. } = &mut sel.from[0].relation {
            if let S::SetExpr::Select(inner_sel) = subquery.body.as_mut() {
                inner_where = inner_sel.as_mut().selection.take();
            }
        }

        // заменить relation: берём единственный источник из inner FROM и переносим alias
        if let S::TableFactor::Derived {
            subquery, alias, ..
        } = &mut sel.from[0].relation
        {
            let outer_alias = alias.clone();
            if let S::SetExpr::Select(inner_sel) = subquery.body.as_mut() {
                let inner = inner_sel.as_mut();
                let mut inner_twj = inner.from.remove(0);
                match &mut inner_twj.relation {
                    S::TableFactor::Table {
                        alias: base_alias, ..
                    } => {
                        *base_alias = outer_alias.clone();
                    }
                    _ => return, // по критериям выше сюда не дойдём, но оставим предохранитель
                }
                sel.from[0].relation = inner_twj.relation;
            }
        }

        // слить предикаты
        and_merge(&mut sel.selection, inner_where.take());
    }

    fn visit_set_expr(se: &mut S::SetExpr) {
        match se {
            S::SetExpr::Select(s) => {
                let s_mut = s.as_mut();
                try_flatten_in_select(s_mut);
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

    // точка входа
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
