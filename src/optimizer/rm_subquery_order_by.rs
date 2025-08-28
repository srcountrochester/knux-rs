use crate::optimizer::utils::walk_statement_mut;
use sqlparser::ast as S;

/// Удаляет `ORDER BY` из **подзапросов** (не верхнего уровня) там, где он
/// не влияет на результат: при отсутствии `LIMIT`/`OFFSET`/`FETCH`.
///
/// Обоснование:
/// - В PostgreSQL порядок строк без верхнего `ORDER BY` не определён; вложенная
///   сортировка результата не гарантирует порядок снаружи. Это даёт право
///   удалять внутренний `ORDER BY`, если он не ограничивает выборку
///   (`LIMIT`/`FETCH`). См. док.: «A particular output ordering can only be
///   guaranteed if the sort step is explicitly chosen.» :contentReference[oaicite:3]{index=3}
/// - В MySQL для подзапросов в `IN/EXISTS` (семиджоины) `ORDER BY` допускается,
///   но игнорируется оптимизатором, т.к. для логики предиката порядок не важен. :contentReference[oaicite:4]{index=4}
/// - В SQLite реализована оптимизация исключения `ORDER BY` у подзапросов
///   (в т.ч. во `FROM`), если выполняется ряд условий, ключевое — отсутствие
///   `LIMIT`. :contentReference[oaicite:5]{index=5}
///
/// Функция **мутирует** переданный `Statement` и ничего не возвращает.
/// Условия удаления:
/// - узел — именно *подзапрос* (не верхний);
/// - у подзапроса нет `limit_clause` и `fetch`;
/// - у подзапроса есть `order_by`.
#[inline]
pub fn rm_subquery_order_by(stmt: &mut sqlparser::ast::Statement) {
    #[inline]
    fn has_limit_or_fetch(q: &S::Query) -> bool {
        q.limit_clause.is_some() || q.fetch.is_some()
    }

    // Удаляем ORDER BY только в нетоповых Query и только если нет LIMIT/FETCH
    walk_statement_mut(
        stmt,
        &mut |q: &mut S::Query, top_level: bool| {
            if !top_level && !has_limit_or_fetch(q) && q.order_by.is_some() {
                q.order_by = None;
            }
        },
        &mut |_e: &mut S::Expr| {},
    );
}
