use sqlparser::ast::{self as S};

use crate::optimizer::utils::{is_literal_const, walk_statement_mut};

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

    walk_statement_mut(
        stmt,
        &mut |_, _| {}, // Query-хук не нужен
        &mut |e: &mut S::Expr| {
            if let S::Expr::InList { list, .. } = e {
                let mut seen: HashSet<String> = HashSet::with_capacity(list.len());
                let mut out: Vec<S::Expr> = Vec::with_capacity(list.len());

                for it in list.drain(..) {
                    if let Some(key) = is_literal_const(&it) {
                        if seen.insert(key) {
                            out.push(it);
                        }
                    } else {
                        // не константы оставляем без изменений и в исходном порядке
                        out.push(it);
                    }
                }

                *list = out;
            }
        },
    );
}
