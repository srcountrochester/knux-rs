use sqlparser::ast::{self as S};

use crate::optimizer::utils::{first_projection_expr, rewrite_select_to_one};

/// Переписать `IN (подзапрос)` в `EXISTS (подзапрос)` (ручная опция).
///
/// Правило (консервативно):
/// - Обрабатываем выражения вида `lhs IN (subquery)` с `negated == false`;
/// - `subquery.body` должен быть `Select(...)` (не объединения);
/// - Внутри `EXISTS` переписываем проекцию на `SELECT 1`, удаляем `ORDER BY`,
///   а выражение сравнения `lhs = <first_select_item_expr>` конъюнктим во внутренний `WHERE`.
///
/// Замечание по эквивалентности: в общем случае `IN` и `EXISTS` отличаются
/// при наличии `NULL` в подзапросе. Здесь преобразование выполняется **только**
/// при явном включении пользователем и применяется к обычному `IN` (не `NOT IN`).
/// Для `NOT IN` преобразование не выполняется.
#[inline]
pub fn in_to_exists(stmt: &mut S::Statement) {
    // Сформировать равенство `inner_expr = lhs`
    #[inline]
    fn make_eq(inner: S::Expr, lhs: &S::Expr) -> S::Expr {
        S::Expr::BinaryOp {
            left: Box::new(inner),
            op: S::BinaryOperator::Eq,
            right: Box::new(lhs.clone()),
        }
    }

    // Переписать один узел `InSubquery` → `Exists(...)`
    fn rewrite_in_to_exists_node(lhs: &S::Expr, subq: &mut S::Query) -> bool {
        // Удаляем ORDER BY (в EXISTS сортировка не влияет)
        subq.order_by = None;

        // Должен быть простой Select
        let inner_sel = match subq.body.as_mut() {
            S::SetExpr::Select(s) => s.as_mut(),
            _ => return false,
        };

        // Берём первый элемент проекции как выражение сравнения
        let eq_expr_rhs = match first_projection_expr(inner_sel) {
            Some(e) => e,
            None => return false,
        };

        // Сравнение: <proj_expr> = lhs
        let eq = make_eq(eq_expr_rhs, lhs);

        // Слить сравнение с текущим WHERE подзапроса
        if let Some(cur) = inner_sel.selection.take() {
            inner_sel.selection = Some(S::Expr::BinaryOp {
                left: Box::new(cur),
                op: S::BinaryOperator::And,
                right: Box::new(eq),
            });
        } else {
            inner_sel.selection = Some(eq);
        }

        // Переписать SELECT ... → SELECT 1
        rewrite_select_to_one(inner_sel);

        true
    }

    // ===== обход AST (по аналогии с другими проходами) =====

    fn visit_expr(e: &mut S::Expr) {
        use S::Expr::*;
        match e {
            // Цель: lhs IN (subquery) → EXISTS(subquery')
            InSubquery {
                expr: lhs,
                subquery,
                negated,
            } => {
                // сначала обойти дочерние узлы
                visit_expr(lhs);
                visit_query(subquery);

                if !*negated {
                    if rewrite_in_to_exists_node(lhs, subquery) {
                        // заменить текущий узел на EXISTS
                        *e = S::Expr::Exists {
                            subquery: subquery.clone(), // подставляем модифицированный subquery
                            negated: false,
                        };
                    }
                }
            }

            Subquery(q) => visit_query(q),
            Exists { subquery, .. } => visit_query(subquery),

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

    fn visit_join(j: &mut S::Join) {
        use S::{JoinConstraint, JoinOperator};
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
        S::Statement::Insert(S::Insert { source, .. }) => {
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
