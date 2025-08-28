use crate::optimizer::utils::walk_statement_mut;
use sqlparser::ast as S;
use sqlparser::ast::LimitClause;

/// Упростить подзапросы в `EXISTS (...)`: заменить проекцию на `SELECT 1`
/// и удалить внутренний `ORDER BY`.
///
/// Обоснование: список столбцов в `EXISTS` не используется БД для вычисления
/// истинности; сортировка внутри `EXISTS` также не влияет на результат,
/// поэтому её безопасно удалять. Это уменьшает объём работы оптимизатора.
///
/// Функция **мутирует** переданный `Statement` и ничего не возвращает.
#[inline]
pub fn simplify_exists(stmt: &mut sqlparser::ast::Statement) {
    #[inline]
    fn is_zero_literal(e: &S::Expr) -> bool {
        if let S::Expr::Value(v) = e {
            if let S::Value::Number(s, _) = &v.value {
                return s == "0" || s == "0.0";
            }
        }
        false
    }

    walk_statement_mut(stmt, &mut |_, _| {}, &mut |e: &mut S::Expr| {
        if let S::Expr::Exists { subquery, .. } = e {
            // 1) Если LIMIT/FETCH гарантирует 0 строк — EXISTS -> FALSE
            let mut zero_rows = false;

            if let Some(lim) = &subquery.limit_clause {
                match lim {
                    LimitClause::LimitOffset {
                        limit: Some(le), ..
                    } => {
                        if is_zero_literal(le) {
                            zero_rows = true;
                        }
                    }
                    LimitClause::OffsetCommaLimit { limit, .. } => {
                        if is_zero_literal(limit) {
                            zero_rows = true;
                        }
                    }
                    _ => {}
                }
            }
            if !zero_rows {
                if let Some(fetch) = &subquery.fetch {
                    if let Some(q) = &fetch.quantity {
                        if is_zero_literal(q) {
                            zero_rows = true;
                        }
                    }
                }
            }
            if zero_rows {
                *e = S::Expr::Value(S::Value::Boolean(false).into());
                return;
            }

            // 2) Для EXISTS порядок и список колонок неважны — упростим подзапрос
            subquery.order_by = None; // ORDER BY бессмысленен для EXISTS

            if let S::SetExpr::Select(sel_box) = subquery.body.as_mut() {
                let sel = sel_box.as_mut();
                sel.distinct = None; // DISTINCT не влияет на EXISTS
                sel.projection.clear();
                sel.projection
                    .push(S::SelectItem::UnnamedExpr(S::Expr::Value(
                        S::Value::Number("1".into(), false).into(),
                    )));
            }
        }
    });
}
