//! Общие вспомогательные утилиты для оптимизаторных проходов.
//!
//! Здесь собраны часто повторяющиеся проверки и мелкие трансформации AST,
//! чтобы избегать дублирования кода внутри `src/optimizer/passes.rs`.

use crate::utils::num_expr;
use sqlparser::ast as S;

/// Слить предикаты по И (`AND`): добавить `add` к `dst` (если оба есть — обернуть в `AND`).
///
/// Пример:
/// - `dst=None, add=Some(p)` → `dst=Some(p)`
/// - `dst=Some(a), add=Some(b)` → `dst=Some(a AND b)`
pub fn and_merge(dst: &mut Option<S::Expr>, add: Option<S::Expr>) {
    match (dst.take(), add) {
        (None, None) => *dst = None,
        (Some(a), None) => *dst = Some(a),
        (None, Some(b)) => *dst = Some(b),
        (Some(a), Some(b)) => {
            *dst = Some(S::Expr::BinaryOp {
                left: Box::new(a),
                op: S::BinaryOperator::And,
                right: Box::new(b),
            });
        }
    }
}

/// Разбить выражение по конъюнкции `AND` в плоский список, сохраняя порядок.
pub fn split_conjuncts(expr: S::Expr, out: &mut Vec<S::Expr>) {
    match expr {
        S::Expr::BinaryOp {
            left,
            op: S::BinaryOperator::And,
            right,
        } => {
            split_conjuncts(*left, out);
            split_conjuncts(*right, out);
        }
        S::Expr::Nested(inner) => {
            split_conjuncts(*inner, out);
        }
        other => out.push(other),
    }
}

/// Сконструировать выражение как конъюнкцию из списка (или `None`, если список пуст).
pub fn join_conjuncts(mut parts: Vec<S::Expr>) -> Option<S::Expr> {
    if parts.is_empty() {
        return None;
    }
    let mut it = parts.drain(..);
    let mut acc = it.next().unwrap();
    for e in it {
        acc = S::Expr::BinaryOp {
            left: Box::new(acc),
            op: S::BinaryOperator::And,
            right: Box::new(e),
        };
    }
    Some(acc)
}

/// Проверка: в `Query` нет ограничений выборки (`LIMIT`/`FETCH`).
pub fn query_has_no_limit_or_fetch(q: &S::Query) -> bool {
    q.limit_clause.is_none() && q.fetch.is_none()
}

/// Проверка: `Select` не меняет кардинальность —
/// нет `DISTINCT`, `GROUP BY`, `HAVING`.
pub fn select_is_simple_no_cardinality(sel: &S::Select) -> bool {
    group_by_is_empty(&sel.group_by) && sel.distinct.is_none() && sel.having.is_none()
}

/// Переписать проекцию `SELECT ...` на `SELECT 1`.
pub fn rewrite_select_to_one(sel: &mut S::Select) {
    sel.projection.clear();
    sel.projection.push(S::SelectItem::UnnamedExpr(num_expr(1)));
}

/// Вернуть выражение из первого элемента проекции `SELECT`,
/// если это `UnnamedExpr(..)` или `ExprWithAlias{expr,..}`. Иначе — `None` (например, `*`).
pub fn first_projection_expr(sel: &S::Select) -> Option<S::Expr> {
    match sel.projection.first() {
        Some(S::SelectItem::UnnamedExpr(e)) => Some(e.clone()),
        Some(S::SelectItem::ExprWithAlias { expr, .. }) => Some((*expr).clone()),
        _ => None,
    }
}

/// Проверка: выражение использует только колонки указанного псевдонима `alias`
/// либо неквалифицированные имена (`a`).
pub fn expr_refs_only_alias(e: &S::Expr, alias: &str) -> bool {
    use S::Expr::*;
    match e {
        Identifier(_) => true,
        CompoundIdentifier(path) => {
            if let Some(first) = path.first() {
                first.value == alias
            } else {
                true
            }
        }
        BinaryOp { left, right, .. } => {
            expr_refs_only_alias(left, alias) && expr_refs_only_alias(right, alias)
        }
        UnaryOp { expr, .. } => expr_refs_only_alias(expr, alias),
        Between {
            expr, low, high, ..
        } => {
            expr_refs_only_alias(expr, alias)
                && expr_refs_only_alias(low, alias)
                && expr_refs_only_alias(high, alias)
        }
        Cast { expr, .. } => expr_refs_only_alias(expr, alias),
        Extract { expr, .. } => expr_refs_only_alias(expr, alias),
        Nested(inner) => expr_refs_only_alias(inner, alias),
        Function(S::Function { args, .. }) => {
            use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
            match args {
                FunctionArguments::List(list) => list.args.iter().all(|a| match a {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(x)) => {
                        expr_refs_only_alias(x, alias)
                    }
                    FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(x),
                        ..
                    } => expr_refs_only_alias(x, alias),
                    _ => true,
                }),
                FunctionArguments::Subquery(_) => false,
                _ => true,
            }
        }
        Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let ok_op = operand
                .as_ref()
                .map(|b| expr_refs_only_alias(b, alias))
                .unwrap_or(true);
            let ok_when = conditions.iter().all(|w| {
                expr_refs_only_alias(&w.condition, alias) && expr_refs_only_alias(&w.result, alias)
            });
            let ok_else = else_result
                .as_ref()
                .map(|b| expr_refs_only_alias(b, alias))
                .unwrap_or(true);
            ok_op && ok_when && ok_else
        }
        InSubquery { .. } | Exists { .. } | Subquery(_) => false,
        _ => true, // константы и пр.
    }
}

/// Убрать ведущий префикс `alias.` у составных идентификаторов (`alias.col`) внутри выражения.
pub fn strip_alias_in_expr(e: &mut S::Expr, alias: &str) {
    use S::Expr::*;
    match e {
        CompoundIdentifier(path) => {
            // забираем в локальную переменную, чтобы безопасно переassign-ить e при необходимости
            let mut p = std::mem::take(path);
            if !p.is_empty() && p[0].value == alias {
                p.remove(0);
            }
            if p.len() == 1 {
                *e = Identifier(p.remove(0));
            } else {
                *path = p;
            }
        }
        BinaryOp { left, right, .. } => {
            strip_alias_in_expr(left, alias);
            strip_alias_in_expr(right, alias);
        }
        UnaryOp { expr, .. } => strip_alias_in_expr(expr, alias),
        Between {
            expr, low, high, ..
        } => {
            strip_alias_in_expr(expr, alias);
            strip_alias_in_expr(low, alias);
            strip_alias_in_expr(high, alias);
        }
        Cast { expr, .. } => strip_alias_in_expr(expr, alias),
        Extract { expr, .. } => strip_alias_in_expr(expr, alias),
        Nested(inner) => strip_alias_in_expr(inner, alias),
        Function(S::Function { args, .. }) => {
            use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
            match args {
                FunctionArguments::List(list) => {
                    for a in &mut list.args {
                        match a {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(x))
                            | FunctionArg::Named {
                                arg: FunctionArgExpr::Expr(x),
                                ..
                            } => strip_alias_in_expr(x, alias),
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
        Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(b) = operand.as_mut() {
                strip_alias_in_expr(b, alias);
            }
            for w in conditions.iter_mut() {
                strip_alias_in_expr(&mut w.condition, alias);
                strip_alias_in_expr(&mut w.result, alias);
            }
            if let Some(b) = else_result.as_mut() {
                strip_alias_in_expr(b, alias);
            }
        }
        _ => {}
    }
}

/* ————— Вспомогательные приватные утилиты ————— */

#[inline]
fn group_by_is_empty(g: &S::GroupByExpr) -> bool {
    match g {
        S::GroupByExpr::Expressions(v, _) => v.is_empty(),
        _ => false,
    }
}

/// Локальная проверка «прямой идентификатор колонки».
#[inline]
pub fn is_plain_column(e: &S::Expr) -> bool {
    matches!(e, S::Expr::Identifier(_) | S::Expr::CompoundIdentifier(_))
}

/// Локальная проверка «проекция = набор прямых колонок».
#[inline]
pub fn projection_is_direct_columns(sel: &S::Select) -> bool {
    sel.projection
        .iter()
        .all(|it| matches!(it, S::SelectItem::UnnamedExpr(e) if is_plain_column(e)))
}

pub fn is_literal_const(e: &S::Expr) -> Option<String> {
    if let S::Expr::Value(vws) = e {
        // ВАЖНО: плейсхолдеры не считаем константами
        match &vws.value {
            S::Value::Placeholder(_) => None,
            // всё остальное считаем литералом; ключ — нормализованный Debug
            v => Some(format!("{v:?}")),
        }
    } else {
        None
    }
}
