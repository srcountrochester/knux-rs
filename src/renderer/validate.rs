use super::Error;
use crate::renderer::{
    Dialect,
    ast::{self as R, Stmt},
    config::{FeaturePolicy, SqlRenderCfg},
};

pub fn validate_query_features(q: &R::Query, cfg: &SqlRenderCfg) -> Option<Error> {
    if !matches!(cfg.policy, FeaturePolicy::Strict) {
        return None;
    }

    // 1) DISTINCT ON — только PG
    if let R::QueryBody::Select(s) = &q.body {
        if !s.distinct_on.is_empty() && !matches!(cfg.dialect, Dialect::Postgres) {
            return Some(Error::UnsupportedFeature {
                feature: "DISTINCT ON".into(),
                dialect: cfg.dialect,
            });
        }
        // 2) ILIKE — только PG
        if contains_ilike(&s.r#where) && !matches!(cfg.dialect, Dialect::Postgres) {
            return Some(Error::UnsupportedFeature {
                feature: "ILIKE".into(),
                dialect: cfg.dialect,
            });
        }
        // 3) NULLS LAST — только PG
        if !q.order_by.is_empty() {
            if q.order_by.iter().any(|oi| oi.nulls_last)
                && !matches!(cfg.dialect, Dialect::Postgres)
            {
                return Some(Error::UnsupportedFeature {
                    feature: "ORDER BY ... NULLS LAST".into(),
                    dialect: cfg.dialect,
                });
            }
        }
        // 4) GROUP BY модификаторы
        if !s.group_by_modifiers.is_empty() {
            let has_cube = s
                .group_by_modifiers
                .iter()
                .any(|m| matches!(m, R::GroupByModifier::Cube));
            let has_gs = s
                .group_by_modifiers
                .iter()
                .any(|m| matches!(m, R::GroupByModifier::GroupingSets(_)));
            let has_rollup = s
                .group_by_modifiers
                .iter()
                .any(|m| matches!(m, R::GroupByModifier::Rollup));

            match cfg.dialect {
                Dialect::Postgres => { /* ok: rollup/cube/grouping sets */ }
                Dialect::MySQL => {
                    // поддержка только WITH ROLLUP (не как функция), мы рендерим корректно;
                    if has_cube || has_gs {
                        return Some(Error::UnsupportedFeature {
                            feature: "GROUP BY CUBE/GROUPING SETS".into(),
                            dialect: cfg.dialect,
                        });
                    }
                }
                Dialect::SQLite => {
                    if has_rollup || has_cube || has_gs {
                        return Some(Error::UnsupportedFeature {
                            feature: "GROUP BY modifiers".into(),
                            dialect: cfg.dialect,
                        });
                    }
                }
            }
        }
    }

    if let Some(with) = &q.with {
        for cte in &with.ctes {
            if cte.materialized.is_some() && !matches!(cfg.dialect, Dialect::Postgres) {
                return Some(Error::UnsupportedFeature {
                    feature: "WITH [NOT] MATERIALIZED".into(),
                    dialect: cfg.dialect,
                });
            }
        }
    }

    // 5) UNION ... BY NAME — не поддерживается в PG/MySQL/SQLite
    if contains_by_name(&q.body) {
        return Some(Error::UnsupportedFeature {
            feature: "UNION/INTERSECT/EXCEPT ... BY NAME".into(),
            dialect: cfg.dialect,
        });
    }

    None
}

pub fn validate_stmt_features(_s: &Stmt, _cfg: &SqlRenderCfg) -> Option<Error> {
    None
}

fn contains_ilike(w: &Option<R::Expr>) -> bool {
    fn walk(e: &R::Expr) -> bool {
        match e {
            R::Expr::Like { ilike: true, .. } => true,
            R::Expr::Unary { expr, .. } | R::Expr::Paren(expr) => walk(expr),
            R::Expr::Binary { left, right, .. } => walk(left) || walk(right),
            R::Expr::Tuple(xs) => xs.iter().any(walk),
            R::Expr::FuncCall { args, .. } => args.iter().any(walk),
            R::Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand.as_deref().map(walk).unwrap_or(false)
                    || when_then.iter().any(|(a, b)| walk(a) || walk(b))
                    || else_expr.as_deref().map(walk).unwrap_or(false)
            }
            _ => false,
        }
    }
    w.as_ref().map(walk).unwrap_or(false)
}

fn contains_by_name(body: &R::QueryBody) -> bool {
    match body {
        R::QueryBody::Select(_) => false,
        R::QueryBody::Set {
            left,
            right,
            by_name,
            ..
        } => *by_name || contains_by_name(left) || contains_by_name(right),
    }
}
