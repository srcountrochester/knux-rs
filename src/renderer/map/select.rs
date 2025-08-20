use super::utils::{literal_u64, map_expr, map_order_by, map_select_item, split_object_name};
use crate::renderer::ast as R;
use sqlparser::ast::{
    self as S, Cte as SCte, CteAsMaterialized as SCteMat, Distinct, Expr as SExpr, Function,
    FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, GroupByWithModifier,
    Join as SJoin, JoinOperator as SJoinKind, LimitClause, Query as SQuery, Select as SSelect,
    SetExpr, TableFactor, With as SWith,
};

// Query -> R::Query
pub fn map_to_render_query(q: &SQuery) -> R::Query {
    let body = map_query_body(&q.body);
    let order_by = q.order_by.as_ref().map(map_order_by).unwrap_or_default();

    let mut limit = None;
    let mut offset = None;
    if let Some(lim_expr) = q.limit_clause.as_ref() {
        match lim_expr {
            LimitClause::LimitOffset {
                limit: lim,
                offset: off,
                ..
            } => {
                if let Some(v) = off {
                    if let Some(v) = literal_u64(&v.value) {
                        offset = Some(v);
                    }
                }
                if let Some(v) = lim {
                    if let Some(v) = literal_u64(&v) {
                        limit = Some(v);
                    }
                }
            }
            LimitClause::OffsetCommaLimit {
                offset: off,
                limit: lim,
            } => {
                if let Some(v) = literal_u64(off) {
                    offset = Some(v);
                }
                if let Some(v) = literal_u64(lim) {
                    limit = Some(v);
                }
            }
        }
    }

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
            let base_op = match op {
                S::SetOperator::Union => R::SetOp::Union,
                S::SetOperator::Intersect => R::SetOp::Intersect,
                S::SetOperator::Except | S::SetOperator::Minus => R::SetOp::Except,
            };
            let (op, by_name) = match (base_op, set_quantifier) {
                (R::SetOp::Union, S::SetQuantifier::All) => (R::SetOp::UnionAll, false),
                (R::SetOp::Union, S::SetQuantifier::Distinct) => (R::SetOp::Union, false),
                (R::SetOp::Union, S::SetQuantifier::ByName) => (R::SetOp::Union, true),
                (R::SetOp::Union, S::SetQuantifier::AllByName) => (R::SetOp::UnionAll, true),
                (R::SetOp::Union, S::SetQuantifier::DistinctByName) => (R::SetOp::Union, true),
                (R::SetOp::Union, S::SetQuantifier::None) => (R::SetOp::Union, false),

                (R::SetOp::Intersect, S::SetQuantifier::All) => (R::SetOp::IntersectAll, false),
                (R::SetOp::Intersect, S::SetQuantifier::Distinct) => (R::SetOp::Intersect, false),
                (R::SetOp::Intersect, S::SetQuantifier::AllByName) => {
                    (R::SetOp::IntersectAll, true)
                }
                (R::SetOp::Intersect, S::SetQuantifier::DistinctByName) => {
                    (R::SetOp::Intersect, true)
                }

                (R::SetOp::Except, S::SetQuantifier::All) => (R::SetOp::ExceptAll, false),
                (R::SetOp::Except, S::SetQuantifier::Distinct) => (R::SetOp::Except, false),
                (R::SetOp::Except, S::SetQuantifier::AllByName) => (R::SetOp::ExceptAll, true),
                (R::SetOp::Except, S::SetQuantifier::DistinctByName) => (R::SetOp::Except, true),

                (o @ R::SetOp::Intersect, S::SetQuantifier::ByName) => (o, true),
                (o @ R::SetOp::Intersect, S::SetQuantifier::None) => (o, false),
                (o @ R::SetOp::IntersectAll, S::SetQuantifier::All) => (o, false),
                (o @ R::SetOp::IntersectAll, S::SetQuantifier::Distinct) => (o, false),
                (o @ R::SetOp::IntersectAll, S::SetQuantifier::ByName) => (o, true),
                (o @ R::SetOp::IntersectAll, S::SetQuantifier::AllByName) => (o, true),
                (o @ R::SetOp::IntersectAll, S::SetQuantifier::DistinctByName) => (o, true),
                (o @ R::SetOp::IntersectAll, S::SetQuantifier::None) => (o, false),

                (o @ R::SetOp::Except, S::SetQuantifier::ByName) => (o, true),
                (o @ R::SetOp::Except, S::SetQuantifier::None) => (o, false),
                (o @ R::SetOp::ExceptAll, S::SetQuantifier::All) => (o, false),
                (o @ R::SetOp::ExceptAll, S::SetQuantifier::Distinct) => (o, false),
                (o @ R::SetOp::ExceptAll, S::SetQuantifier::ByName) => (o, true),
                (o @ R::SetOp::ExceptAll, S::SetQuantifier::AllByName) => (o, true),
                (o @ R::SetOp::ExceptAll, S::SetQuantifier::DistinctByName) => (o, true),
                (o @ R::SetOp::ExceptAll, S::SetQuantifier::None) => (o, false),

                (o @ R::SetOp::UnionAll, _) => {
                    (o, matches!(set_quantifier, S::SetQuantifier::AllByName))
                }
            };

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

    if let Some(lim_expr) = q.limit_clause.as_ref() {
        match lim_expr {
            LimitClause::LimitOffset { limit, offset, .. } => {
                if let Some(v) = offset {
                    if let Some(v) = literal_u64(&v.value) {
                        rsel.offset = Some(v);
                    }
                }
                if let Some(v) = limit {
                    if let Some(v) = literal_u64(&v) {
                        rsel.limit = Some(v);
                    }
                }
            }
            LimitClause::OffsetCommaLimit { offset, limit } => {
                if let Some(v) = literal_u64(offset) {
                    rsel.offset = Some(v);
                }
                if let Some(v) = literal_u64(limit) {
                    rsel.limit = Some(v);
                }
            }
        }
    }

    rsel
}

fn map_select_to_render_ast(sel: &SSelect) -> R::Select {
    let mut from_named: Option<R::TableRef> = None;
    let mut joins: Vec<R::Join> = Vec::new();

    for twj in &sel.from {
        if from_named.is_none() {
            from_named = Some(map_table_factor(&twj.relation));
        } else {
            joins.push(R::Join {
                kind: R::JoinKind::Cross,
                table: map_table_factor(&twj.relation),
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
        Some(Distinct::On(exprs)) => (false, exprs.iter().map(map_expr).collect()),
    };

    let (group_by_vec, group_by_mods) = map_group_by(sel);

    R::Select {
        distinct: distinct_flag,
        distinct_on,
        items: sel.projection.iter().map(map_select_item).collect(),
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
            let mut out_exprs = Vec::<R::Expr>::new();
            let mut out_mods = Vec::<R::GroupByModifier>::new();

            for e in exprs {
                match e {
                    // ROLLUP/CUBE/GROUPING SETS
                    #[allow(unreachable_patterns)]
                    SExpr::Rollup(groups) => {
                        out_mods.push(R::GroupByModifier::Rollup);
                        for grp in groups {
                            for ee in grp {
                                out_exprs.push(map_expr(ee));
                            }
                        }
                    }
                    #[allow(unreachable_patterns)]
                    SExpr::Cube(groups) => {
                        out_mods.push(R::GroupByModifier::Cube);
                        for grp in groups {
                            for ee in grp {
                                out_exprs.push(map_expr(ee));
                            }
                        }
                    }
                    #[allow(unreachable_patterns)]
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

                    // ROLLUP/CUBE как функция
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

fn map_table_factor(tf: &TableFactor) -> R::TableRef {
    match tf {
        TableFactor::Table { name, alias, .. } => {
            let (schema, table) = split_object_name(name);
            R::TableRef::Named {
                schema,
                name: table,
                alias: alias.as_ref().map(|a| a.name.value.clone()),
            }
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            let inner = map_to_render_ast(subquery);
            R::TableRef::Subquery {
                query: Box::new(inner),
                alias: alias.as_ref().map(|a| a.name.value.clone()),
            }
        }
        other => R::TableRef::Named {
            schema: None,
            name: other.to_string(),
            alias: None,
        },
    }
}

fn map_join(j: &SJoin) -> R::Join {
    use R::JoinKind as JK;
    let (nat, constraint_opt) = match &j.join_operator {
        SJoinKind::Inner(c) => (false, Some(c)),
        SJoinKind::LeftOuter(c) => (false, Some(c)),
        SJoinKind::RightOuter(c) => (false, Some(c)),
        SJoinKind::FullOuter(c) => (false, Some(c)),
        SJoinKind::CrossJoin => (false, None),
        SJoinKind::CrossApply => (false, None),
        SJoinKind::OuterApply => (false, None),
        SJoinKind::Join(S::JoinConstraint::Natural) => (true, None),
        SJoinKind::Join(c) => (false, Some(c)),
        _ => (false, None),
    };

    let mut kind = match j.join_operator {
        SJoinKind::Inner(_) | SJoinKind::Join(_) => JK::Inner,
        SJoinKind::LeftOuter(_) => JK::Left,
        SJoinKind::RightOuter(_) => JK::Right,
        SJoinKind::FullOuter(_) => JK::Full,
        SJoinKind::CrossJoin | SJoinKind::CrossApply => JK::Cross,
        SJoinKind::OuterApply => JK::Left,
        _ => JK::Inner,
    };

    if nat {
        kind = match kind {
            JK::Inner => JK::NaturalInner,
            JK::Left => JK::NaturalLeft,
            JK::Right => JK::NaturalRight,
            JK::Full => JK::NaturalFull,
            _ => kind,
        };
    }

    let mut on = None;
    let mut using_cols = None;
    if let Some(c) = constraint_opt {
        match c {
            S::JoinConstraint::On(expr) => on = Some(map_expr(expr)),
            S::JoinConstraint::Using(cols) => {
                using_cols = Some(cols.iter().map(|i| i.to_string()).collect())
            }
            _ => {}
        }
    }

    R::Join {
        kind,
        table: map_table_factor(&j.relation),
        on,
        using_cols,
    }
}
