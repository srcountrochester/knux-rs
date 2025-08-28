use sqlparser::ast::{self as S, Insert, Join, JoinConstraint, JoinOperator};

/// Упростить подзапросы в `EXISTS (...)`: заменить проекцию на `SELECT 1`
/// и удалить внутренний `ORDER BY`.
///
/// Обоснование: список столбцов в `EXISTS` не используется БД для вычисления
/// истинности; сортировка внутри `EXISTS` также не влияет на результат,
/// поэтому её безопасно удалять. Это уменьшает объём работы оптимизатора.
///
/// Функция **мутирует** переданный `Statement` и ничего не возвращает.
#[inline]
pub fn simplify_exists(stmt: &mut S::Statement) {
    use crate::utils::num_expr;

    // Заменить проекцию на SELECT 1 у конкретного SELECT
    fn select_to_one(sel: &mut S::Select) {
        sel.projection.clear();
        sel.projection.push(S::SelectItem::UnnamedExpr(num_expr(1)));
    }

    // Пройти все SELECT'ы внутри SetExpr и заменить проекцию на 1
    fn rewrite_selects_to_one_in_setexpr(se: &mut S::SetExpr) {
        match se {
            S::SetExpr::Select(sel) => select_to_one(sel),
            S::SetExpr::Query(q) => rewrite_exists_query(q),
            S::SetExpr::SetOperation { left, right, .. } => {
                rewrite_selects_to_one_in_setexpr(left);
                rewrite_selects_to_one_in_setexpr(right);
            }
            _ => { /* VALUES/другие конструкции игнорируем */ }
        }
    }

    // Применить правило к подзапросу внутри EXISTS:
    //  - убрать ORDER BY именно у этого подзапроса
    //  - заменить проекцию на SELECT 1 во всех его SELECT-вetках
    //  - дополнительно обойти выражения внутри, чтобы найти вложенные EXISTS
    fn rewrite_exists_query(q: &mut S::Query) {
        // WITH: пройти CTE и упростить EXISTS внутри них
        if let Some(w) = &mut q.with {
            for cte in &mut w.cte_tables {
                visit_query(&mut cte.query);
            }
        }

        // Удалить ORDER BY у самого подзапроса EXISTS
        q.order_by = None;

        // Переписать SELECT -> SELECT 1
        rewrite_selects_to_one_in_setexpr(&mut q.body);

        // Обойти подвыражения внутри (WHERE/HAVING/JOIN/функции)
        visit_set_expr(&mut q.body);
    }

    // Обход выражений с интересом к EXISTS
    fn visit_expr(e: &mut S::Expr) {
        use S::Expr::*;
        match e {
            Exists { subquery, .. } => {
                rewrite_exists_query(subquery);
            }
            Subquery(q) => visit_query(q),
            InSubquery { expr, subquery, .. } => {
                visit_expr(expr);
                visit_query(subquery);
            }

            // Спуск по подвыражениям
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

            S::Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(op) = operand.as_mut() {
                    visit_expr(op);
                }
                for when in conditions.iter_mut() {
                    visit_expr(&mut when.condition);
                    visit_expr(&mut when.result);
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
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(x)) => visit_expr(x),
                                FunctionArg::Named {
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

    // Обход JOIN: вытащить ON-условие
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

    // Обход SetExpr, чтобы достичь выражений (WHERE/HAVING/проекция и т.д.)
    fn visit_set_expr(se: &mut S::SetExpr) {
        use S::SetExpr::*;
        match se {
            Select(sel) => {
                // FROM/JOIN
                for twj in &mut sel.from {
                    visit_table_with_joins(twj);
                }
                // Проекция
                for it in &mut sel.projection {
                    if let S::SelectItem::UnnamedExpr(e)
                    | S::SelectItem::ExprWithAlias { expr: e, .. } = it
                    {
                        visit_expr(e);
                    }
                }
                // WHERE
                if let Some(e) = &mut sel.selection {
                    visit_expr(e);
                }
                // HAVING
                if let Some(e) = &mut sel.having {
                    visit_expr(e);
                }
                // GROUP BY
                if let S::GroupByExpr::Expressions(exprs, _) = &mut sel.group_by {
                    for e in exprs {
                        visit_expr(e);
                    }
                }
                // DISTINCT ON (...)
                if let Some(S::Distinct::On(list)) = &mut sel.distinct {
                    for e in list {
                        visit_expr(e);
                    }
                }
            }
            Query(q) => visit_query(q),
            SetOperation { left, right, .. } => {
                visit_set_expr(left);
                visit_set_expr(right);
            }
            _ => {}
        }
    }

    // Обход Query — общий случай (не трогаем order_by)
    fn visit_query(q: &mut S::Query) {
        if let Some(w) = &mut q.with {
            for cte in &mut w.cte_tables {
                visit_query(&mut cte.query);
            }
        }
        visit_set_expr(&mut q.body);
    }

    // Точка входа: Statement
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
