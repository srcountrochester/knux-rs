use sqlparser::ast as S;

use crate::optimizer::utils::walk_statement_mut;
use std::cell::Cell;

/// Быстрый «пробник»: если в дереве нет подзапросов/IN-списков/ORDER BY,
/// большая часть проходов ничего не сделает — можно выйти раньше.
#[inline]
pub fn probably_no_optimizations(stmt: &S::Statement) -> bool {
    let has_triggers = Cell::new(false);
    // Только дешёвая проверка признаков; останавливаемся при первом же попадании.
    let mut s = stmt.clone();
    walk_statement_mut(
        &mut s,
        &mut |q: &mut S::Query, _| {
            if has_triggers.get() {
                return;
            }
            if q.order_by.is_some() || q.limit_clause.is_some() || q.fetch.is_some() {
                has_triggers.set(true);
            }
        },
        &mut |e: &mut S::Expr| {
            if has_triggers.get() {
                return;
            }
            if matches!(
                e,
                S::Expr::InList { .. } | S::Expr::InSubquery { .. } | S::Expr::Exists { .. }
            ) {
                has_triggers.set(true);
            }
        },
    );
    !has_triggers.get()
}
