use sqlparser::ast::{self as S, Insert, Join, JoinConstraint, JoinOperator};

use crate::optimizer::utils::is_literal_const;

/// Удаляет дубликаты **констант** внутри списков `IN ( ... )`.
///
/// Поведение:
/// - Обрабатываются только узлы `Expr::InList { list, .. }`.
/// - Дедуплицируются именно **константы** (числа, строки, `TRUE/FALSE`, `NULL`, дата/время и т.п.).
///   Не-константные элементы (`col`, выражения, подзапросы) остаются как есть, даже если повторяются.
/// - Сохраняется порядок первых вхождений констант (stable-удаление дубликатов).
///
/// Замечание: проход изменяет только AST выражений. Вектор параметров билдеров
/// (`params`) не трогается (как и в остальных оптимизациях). Это безопасно для генерации SQL,
/// т.к. в строке будет меньше плейсхолдеров, однако сами «лишние» значения
/// в `params` могут остаться неиспользованными. Текущие тесты и пайплайн это допускают.
///
/// Функция **мутирует** переданный `Statement` и ничего не возвращает.
#[inline]
pub fn dedup_in_list(stmt: &mut S::Statement) {
    use std::collections::HashSet;

    // ==== обход дерева: только чтобы добраться до InList и рекурсивно зайти внутрь ====

    fn visit_expr(e: &mut S::Expr) {
        use S::Expr::*;
        match e {
            InList { expr, list, .. } => {
                // сначала обойти дочерние узлы (сохраняем прежнюю логику обхода)
                visit_expr(expr);
                for it in list.iter_mut() {
                    visit_expr(it);
                }

                // затем дедуп только ЛИТЕРАЛОВ, сохраняя порядок
                let mut seen: HashSet<String> = HashSet::new();
                let mut out: Vec<S::Expr> = Vec::with_capacity(list.len());
                for item in list.drain(..) {
                    if let Some(k) = is_literal_const(&item) {
                        if seen.insert(k) {
                            out.push(item);
                        } // иначе — пропускаем дубль литерала
                    } else {
                        // плейсхолдеры и любые выражения — не дедупим
                        out.push(item);
                    }
                }
                *list = out;
            }

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

    fn visit_select(sel: &mut S::Select) {
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
        if let Some(S::Distinct::On(list)) = &mut sel.distinct {
            for e in list {
                visit_expr(e);
            }
        }
        for twj in &mut sel.from {
            visit_table_with_joins(twj);
        }
    }

    fn visit_set_expr(se: &mut S::SetExpr) {
        match se {
            S::SetExpr::Select(s) => visit_select(s.as_mut()),
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
