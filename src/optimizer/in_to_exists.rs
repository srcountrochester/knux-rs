use sqlparser::ast::{self as S};

use crate::optimizer::utils::{and_merge, walk_statement_mut};

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
    // Переписываем только:  expr IN (SELECT <single-expr> FROM ...)
    // НЕ переписываем: NOT IN, многоколоночные IN (кортежи), подзапросы без единственного выражения в проекции.
    walk_statement_mut(stmt, &mut |_, _| {}, &mut |e: &mut S::Expr| {
        if let S::Expr::InSubquery {
            expr,
            subquery,
            negated,
        } = e
        {
            if *negated {
                // Важно: NOT IN и NOT EXISTS не эквивалентны при наличии NULL в подзапросе.
                // Оставляем без изменений. :contentReference[oaicite:1]{index=1}
                return;
            }

            // Поддерживаем только простой SELECT с единственным элементом проекции
            if let S::SetExpr::Select(sel_box) = subquery.body.as_mut() {
                subquery.order_by = None;
                let sel = sel_box.as_mut();

                if sel.projection.len() != 1 {
                    return;
                }

                // Достаём выражение из проекции
                let projected: Option<S::Expr> = match &mut sel.projection[0] {
                    S::SelectItem::UnnamedExpr(pe) => Some(pe.clone()),
                    S::SelectItem::ExprWithAlias { expr: pe, .. } => Some(pe.clone()),
                    _ => None, // Wildcard/QualifiedWildcard и т.п. — не трогаем
                };
                let Some(rhs_expr) = projected else {
                    return;
                };

                // Коррелирующее условие: <proj> = <outer expr>
                let corr_pred = S::Expr::BinaryOp {
                    left: Box::new(rhs_expr),
                    op: S::BinaryOperator::Eq,
                    right: Box::new((**expr).clone()),
                };

                // Вталкиваем в WHERE подзапроса
                and_merge(&mut sel.selection, Some(corr_pred));

                // Проекцию упрощаем до SELECT 1 — для EXISTS содержимое не важно. :contentReference[oaicite:2]{index=2}
                sel.projection.clear();
                sel.projection
                    .push(S::SelectItem::UnnamedExpr(S::Expr::Value(
                        S::ValueWithSpan::from(S::Value::Number("1".into(), false)),
                    )));

                // Заменяем исходное выражение на EXISTS(подзапрос)
                let new_subq = subquery.as_ref().clone(); // недорого: AST реализует Clone
                *e = S::Expr::Exists {
                    subquery: Box::new(new_subq),
                    negated: false,
                };
            }
        }
    });
}
