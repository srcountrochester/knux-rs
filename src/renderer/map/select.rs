use super::utils::{map_expr, map_order_by, map_select_item};
use crate::renderer::{ast as R, map::utils::map_table_factor_any};
use sqlparser::ast::{
    self as S, Cte as SCte, CteAsMaterialized as SCteMat, Distinct, Expr as SExpr, Function,
    FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, GroupByWithModifier,
    Join as SJoin, LimitClause, Query as SQuery, Select as SSelect, SetExpr, With as SWith,
};

// Query -> R::Query
pub fn map_to_render_query(q: &SQuery) -> R::Query {
    let body = map_query_body(&q.body);
    let order_by = q.order_by.as_ref().map(map_order_by).unwrap_or_default();

    let (limit, offset) = q
        .limit_clause
        .as_ref()
        .map(read_limit_offset)
        .unwrap_or((None, None));

    let with = q.with.as_ref().map(map_with_clause);

    R::Query {
        with,
        body,
        order_by,
        limit,
        offset,
    }
}

// SetExpr -> R::QueryBody
pub fn map_query_body(se: &SetExpr) -> R::QueryBody {
    match se {
        SetExpr::Select(boxed) => R::QueryBody::Select(map_select_to_render_ast(boxed)),
        SetExpr::Query(q) => map_query_body(&q.body),

        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            let base = match op {
                S::SetOperator::Union => R::SetOp::Union,
                S::SetOperator::Intersect => R::SetOp::Intersect,
                S::SetOperator::Except | S::SetOperator::Minus => R::SetOp::Except,
            };
            let (op, by_name) = map_set_quantifier(base, set_quantifier);

            R::QueryBody::Set {
                left: Box::new(map_query_body(left)),
                op,
                right: Box::new(map_query_body(right)),
                by_name,
            }
        }

        _ => R::QueryBody::Select(R::Select {
            distinct: false,
            distinct_on: vec![],
            items: vec![R::SelectItem::Star { opts: None }],
            from: None,
            joins: vec![],
            r#where: None,
            group_by: vec![],
            group_by_modifiers: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        }),
    }
}

fn map_with_clause(sw: &SWith) -> R::With {
    R::With {
        recursive: sw.recursive,
        ctes: sw.cte_tables.iter().map(map_cte).collect(),
    }
}

fn map_cte(cte: &SCte) -> R::Cte {
    R::Cte {
        name: cte.alias.name.value.clone(),
        columns: cte
            .alias
            .columns
            .iter()
            .map(|c| c.name.value.clone())
            .collect(),
        from: cte.from.as_ref().map(|i| i.value.clone()),
        materialized: cte.materialized.as_ref().map(|m| match m {
            SCteMat::Materialized => R::CteMaterialized::Materialized,
            SCteMat::NotMaterialized => R::CteMaterialized::NotMaterialized,
        }),
        query: Box::new(map_query_body(&cte.query.as_ref().body)),
    }
}

// Главная Select-точка (без ORDER/LIMIT/OFFSET)
pub fn map_to_render_ast(q: &SQuery) -> R::Select {
    let mut rsel = match q.body.as_ref() {
        SetExpr::Select(boxed) => map_select_to_render_ast(boxed),
        _ => R::Select {
            distinct: false,
            distinct_on: vec![],
            items: vec![R::SelectItem::Star { opts: None }],
            from: None,
            joins: vec![],
            r#where: None,
            group_by: vec![],
            group_by_modifiers: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        },
    };

    if let Some(ob) = q.order_by.as_ref() {
        rsel.order_by = map_order_by(ob);
    }
    if let Some(lc) = q.limit_clause.as_ref() {
        let (lim, off) = read_limit_offset(lc);
        rsel.limit = lim;
        rsel.offset = off;
    }

    rsel
}

fn map_select_to_render_ast(sel: &SSelect) -> R::Select {
    let mut from_named: Option<R::TableRef> = None;

    // capacity для joins: (число дополнительных FROM) + сумма явных joins
    let crosses = sel.from.iter().skip(1).count();
    let explicit: usize = sel.from.iter().map(|twj| twj.joins.len()).sum();
    let mut joins: Vec<R::Join> = Vec::with_capacity(crosses + explicit);

    for twj in &sel.from {
        if from_named.is_none() {
            from_named = Some(map_table_factor_any(&twj.relation));
        } else {
            joins.push(R::Join {
                kind: R::JoinKind::Cross,
                table: map_table_factor_any(&twj.relation),
                on: None,
                using_cols: None,
            });
        }
        for j in &twj.joins {
            joins.push(map_join(j));
        }
    }

    let (distinct_flag, distinct_on): (bool, Vec<R::Expr>) = match &sel.distinct {
        None => (false, vec![]),
        Some(Distinct::Distinct) => (true, vec![]),
        Some(Distinct::On(exprs)) => {
            let mut v = Vec::with_capacity(exprs.len());
            for e in exprs {
                v.push(map_expr(e));
            }
            (false, v)
        }
    };

    let items = {
        let mut v = Vec::with_capacity(sel.projection.len());
        for it in &sel.projection {
            v.push(map_select_item(it));
        }
        v
    };

    let (group_by_vec, group_by_mods) = map_group_by(sel);

    R::Select {
        distinct: distinct_flag,
        distinct_on,
        items,
        from: from_named,
        joins,
        r#where: sel.selection.as_ref().map(map_expr),
        group_by: group_by_vec,
        group_by_modifiers: group_by_mods,
        having: sel.having.as_ref().map(map_expr),
        order_by: Vec::new(),
        limit: None,
        offset: None,
    }
}

fn map_group_by(sel: &SSelect) -> (Vec<R::Expr>, Vec<R::GroupByModifier>) {
    match &sel.group_by {
        GroupByExpr::All(_) => (Vec::new(), Vec::new()),
        GroupByExpr::Expressions(exprs, items) => {
            let mut out_exprs = Vec::<R::Expr>::with_capacity(exprs.len());
            let mut out_mods = Vec::<R::GroupByModifier>::with_capacity(items.len());

            for e in exprs {
                // те же кейсы, что и было
                #[allow(unreachable_patterns)]
                match e {
                    SExpr::Rollup(groups) => {
                        out_mods.push(R::GroupByModifier::Rollup);
                        for grp in groups {
                            for ee in grp {
                                out_exprs.push(map_expr(ee));
                            }
                        }
                    }
                    SExpr::Cube(groups) => {
                        out_mods.push(R::GroupByModifier::Cube);
                        for grp in groups {
                            for ee in grp {
                                out_exprs.push(map_expr(ee));
                            }
                        }
                    }
                    SExpr::GroupingSets(groups) => {
                        let gs_expr = R::Expr::FuncCall {
                            name: "GROUPING SETS".into(),
                            args: groups
                                .iter()
                                .map(|grp| R::Expr::FuncCall {
                                    name: "tuple".into(),
                                    args: grp.iter().map(map_expr).collect(),
                                })
                                .collect(),
                        };
                        out_mods.push(R::GroupByModifier::GroupingSets(gs_expr));
                        for grp in groups {
                            for ee in grp {
                                out_exprs.push(map_expr(ee));
                            }
                        }
                    }
                    SExpr::Function(Function { name, args, .. })
                        if name.to_string().eq_ignore_ascii_case("ROLLUP") =>
                    {
                        out_mods.push(R::GroupByModifier::Rollup);
                        if let FunctionArguments::List(list) = args {
                            for a in &list.args {
                                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) = a {
                                    out_exprs.push(map_expr(inner));
                                }
                            }
                        }
                    }
                    SExpr::Function(Function { name, args, .. })
                        if name.to_string().eq_ignore_ascii_case("CUBE") =>
                    {
                        out_mods.push(R::GroupByModifier::Cube);
                        if let FunctionArguments::List(list) = args {
                            for a in &list.args {
                                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) = a {
                                    out_exprs.push(map_expr(inner));
                                }
                            }
                        }
                    }
                    _ => out_exprs.push(map_expr(e)),
                }
            }

            for m in items {
                match m {
                    GroupByWithModifier::Rollup => out_mods.push(R::GroupByModifier::Rollup),
                    GroupByWithModifier::Cube => out_mods.push(R::GroupByModifier::Cube),
                    GroupByWithModifier::Totals => out_mods.push(R::GroupByModifier::Totals),
                    GroupByWithModifier::GroupingSets(e) => {
                        out_mods.push(R::GroupByModifier::GroupingSets(map_expr(e)))
                    }
                }
            }

            (out_exprs, out_mods)
        }
    }
}

fn map_join(j: &SJoin) -> R::Join {
    use R::JoinKind as JK;
    use sqlparser::ast::{JoinConstraint as JC, JoinOperator as JO};

    #[inline]
    fn is_natural(c: &JC) -> bool {
        matches!(c, JC::Natural)
    }
    #[inline]
    fn non_natural<'a>(c: &'a JC) -> Option<&'a JC> {
        if is_natural(c) { None } else { Some(c) }
    }

    // Вид JOIN + NATURAL + (возможное) ограничение (без NATURAL)
    let (mut kind, nat, constraint_opt): (JK, bool, Option<&JC>) = match &j.join_operator {
        JO::Join(c) | JO::Inner(c) => (JK::Inner, is_natural(c), non_natural(c)),

        JO::Left(c) | JO::LeftOuter(c) => (JK::Left, is_natural(c), non_natural(c)),
        JO::Right(c) | JO::RightOuter(c) => (JK::Right, is_natural(c), non_natural(c)),
        JO::FullOuter(c) => (JK::Full, is_natural(c), non_natural(c)),

        // Полу-/анти-джоины в нашей модели отсутствуют — понижаем до INNER.
        JO::Semi(c)
        | JO::LeftSemi(c)
        | JO::RightSemi(c)
        | JO::Anti(c)
        | JO::LeftAnti(c)
        | JO::RightAnti(c) => (JK::Inner, is_natural(c), non_natural(c)),

        JO::CrossJoin | JO::CrossApply => (JK::Cross, false, None),
        JO::OuterApply => (JK::Left, false, None),

        // ASOF: нет специального JoinKind — понижаем до INNER.
        // match_condition пойдёт в ON (внизу объединим с constraint через AND).
        JO::AsOf {
            match_condition: _,
            constraint,
        } => (JK::Inner, is_natural(constraint), non_natural(constraint)),

        // STRAIGHT_JOIN: ведём как INNER, пробрасываем constraint (ON/USING), NATURAL игнорируем.
        JO::StraightJoin(c) => (JK::Inner, is_natural(c), non_natural(c)),
    };

    if nat {
        kind = match kind {
            JK::Inner => JK::NaturalInner,
            JK::Left => JK::NaturalLeft,
            JK::Right => JK::NaturalRight,
            JK::Full => JK::NaturalFull,
            other => other,
        };
    }

    // Преобразуем constraint → (on, using)
    let (on, using_cols) = match constraint_opt {
        Some(JC::On(expr)) => (Some(map_expr(expr)), None),
        Some(JC::Using(cols)) => (None, Some(cols.iter().map(|i| i.to_string()).collect())),
        _ => (None, None),
    };

    R::Join {
        kind,
        table: crate::renderer::map::utils::map_table_factor_any(&j.relation),
        on,
        using_cols,
    }
}

#[inline]
fn read_limit_offset(lc: &LimitClause) -> (Option<u64>, Option<u64>) {
    match lc {
        LimitClause::LimitOffset { limit, offset, .. } => {
            let off = offset
                .as_ref()
                .and_then(|o| super::utils::literal_u64(&o.value));
            let lim = limit.as_ref().and_then(|e| super::utils::literal_u64(e));
            (lim, off)
        }
        LimitClause::OffsetCommaLimit { offset, limit } => (
            super::utils::literal_u64(limit),
            super::utils::literal_u64(offset),
        ),
    }
}

#[inline]
fn map_set_quantifier(base: R::SetOp, q: &S::SetQuantifier) -> (R::SetOp, bool) {
    let by_name = matches!(
        q,
        S::SetQuantifier::ByName | S::SetQuantifier::AllByName | S::SetQuantifier::DistinctByName
    );
    let op = match base {
        R::SetOp::Union => match q {
            S::SetQuantifier::All | S::SetQuantifier::AllByName => R::SetOp::UnionAll,
            _ => R::SetOp::Union,
        },
        R::SetOp::Intersect => match q {
            S::SetQuantifier::All | S::SetQuantifier::AllByName => R::SetOp::IntersectAll,
            _ => R::SetOp::Intersect,
        },
        R::SetOp::Except => match q {
            S::SetQuantifier::All | S::SetQuantifier::AllByName => R::SetOp::ExceptAll,
            _ => R::SetOp::Except,
        },
        // сюда не попадём (base вычисляется только как Union/Intersect/Except)
        o => o,
    };
    (op, by_name)
}
