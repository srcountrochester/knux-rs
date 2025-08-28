use crate::optimizer::utils::{
    WalkOrder, and_merge, projection_is_direct_columns, select_is_simple_no_cardinality,
    walk_expr_mut, walk_statement_mut,
};
use sqlparser::ast as S;

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
#[inline]
pub fn predicate_pushdown(stmt: &mut sqlparser::ast::Statement) {
    #[inline]
    fn subquery_ok(q: &S::Query) -> bool {
        // Без WITH/ORDER BY/LIMIT/FETCH — порядок незначим и кардинальность не меняется.
        q.with.is_none() && q.order_by.is_none() && q.limit_clause.is_none() && q.fetch.is_none()
    }

    #[inline]
    fn derived_is_trivial(tf: &S::TableFactor) -> bool {
        match tf {
            S::TableFactor::Derived { subquery, .. } => {
                if !subquery_ok(subquery) {
                    return false;
                }
                match subquery.body.as_ref() {
                    S::SetExpr::Select(sel_box) => {
                        let sel = sel_box.as_ref();
                        // один источник, без JOIN, проекция — прямые колонки, без DISTINCT/GROUP/HAVING
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

    // Проверка: выражение ссылается только на один алиас (alias.col), без одиночных идентификаторов.
    fn expr_uses_only_alias(e: &S::Expr, alias: &str) -> bool {
        let mut ok = true;
        let mut tmp = e.clone();
        walk_expr_mut(&mut tmp, WalkOrder::Pre, &mut |x| {
            match x {
                S::Expr::Identifier(_) => {
                    ok = false;
                } // не квалифицировано — оставляем снаружи
                S::Expr::CompoundIdentifier(parts) => {
                    if parts.len() != 2 || parts[0].value.as_str() != alias {
                        ok = false;
                    }
                }
                S::Expr::Wildcard(_) | S::Expr::QualifiedWildcard(_, _) => {
                    ok = false;
                }
                _ => {}
            }
        });
        ok
    }

    // Удалить префикс <alias>. в квалифицированных колонках alias.col → col
    fn strip_alias_in_expr(e: &mut S::Expr, alias: &str) {
        walk_expr_mut(e, WalkOrder::Pre, &mut |x| {
            if let S::Expr::CompoundIdentifier(parts) = x {
                if parts.len() == 2 && parts[0].value.as_str() == alias {
                    let col_ident = parts[1].clone();
                    *x = S::Expr::Identifier(col_ident);
                }
            }
        });
    }

    // Основной проход: продвигаем WHERE из внешнего SELECT внутрь простого подзапроса во FROM.
    walk_statement_mut(
        stmt,
        &mut |q: &mut S::Query, _top_level: bool| {
            if let S::SetExpr::Select(sel_box) = q.body.as_mut() {
                let sel = sel_box.as_mut();
                if sel.selection.is_none() {
                    return;
                }

                for twj in &mut sel.from {
                    // 1) Быстрая проверка по ИММУТАБЕЛЬНОЙ ссылке (нет &mut ещё)
                    let alias_name = match &twj.relation {
                        S::TableFactor::Derived { alias: Some(a), .. } => a.name.value.clone(),
                        _ => continue,
                    };
                    if !derived_is_trivial(&twj.relation) {
                        continue;
                    }

                    // 2) Теперь берём &mut, чтобы получить inner SELECT из subquery
                    let inner_sel = match &mut twj.relation {
                        S::TableFactor::Derived { subquery, .. } => match subquery.body.as_mut() {
                            S::SetExpr::Select(inner_sel_box) => inner_sel_box.as_mut(),
                            _ => continue,
                        },
                        _ => unreachable!(),
                    };

                    // 3) Если внешний WHERE относится только к этому алиасу — переносим внутрь
                    if let Some(outer_where) = sel.selection.as_ref() {
                        if expr_uses_only_alias(outer_where, &alias_name) {
                            let mut pushed = outer_where.clone();
                            strip_alias_in_expr(&mut pushed, &alias_name);
                            and_merge(&mut inner_sel.selection, Some(pushed));
                            sel.selection = None;
                        }
                    }
                }
            }
        },
        &mut |_e: &mut S::Expr| {},
    );
}
