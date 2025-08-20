use crate::renderer::ident::push_quoted_path;

use super::super::ast as R;
use super::super::ast::*;
use super::super::config::SqlRenderCfg;
use super::super::ident::quote_ident;
use super::super::writer::SqlWriter;
use super::core_fn::*;

pub(crate) fn render_query_body(w: &mut SqlWriter, body: &R::QueryBody, cfg: &SqlRenderCfg) {
    match body {
        R::QueryBody::Select(s) => {
            let inner = render_select(s, cfg, 128);
            w.push(inner);
        }
        R::QueryBody::Set {
            left,
            op,
            right,
            by_name: _,
        } => {
            w.push("(");
            render_query_body(w, left, cfg);
            w.push(") ");
            match op {
                R::SetOp::Union => w.push("UNION "),
                R::SetOp::UnionAll => w.push("UNION ALL "),
                R::SetOp::Intersect => w.push("INTERSECT "),
                R::SetOp::IntersectAll => w.push("INTERSECT ALL "),
                R::SetOp::Except => w.push("EXCEPT "),
                R::SetOp::ExceptAll => w.push("EXCEPT ALL "),
            }
            w.push("(");
            render_query_body(w, right, cfg);
            w.push(")");
        }
    }
}

pub(crate) fn render_select_item(w: &mut SqlWriter, it: &SelectItem, cfg: &SqlRenderCfg) {
    match it {
        // SELECT *
        SelectItem::Star { .. } => {
            w.push("*");
        }

        // SELECT table.*  (или kind.*)
        SelectItem::QualifiedStar { table, .. } => {
            w.push(&quote_ident(table, cfg));
            w.push(".*");
        }

        // SELECT expr [AS alias]
        SelectItem::Expr { expr, alias } => {
            render_expr(w, expr, cfg);
            if let Some(a) = alias {
                if cfg.emit_as_for_column_alias {
                    w.push(" AS ");
                } else {
                    w.push(" ");
                }
                w.push(&quote_ident(a, cfg));
            }
        }
    }
}

pub(crate) fn render_table_ref(w: &mut SqlWriter, t: &TableRef, cfg: &SqlRenderCfg) {
    match t {
        TableRef::Named {
            schema,
            name,
            alias,
        } => {
            if let Some(s) = schema {
                push_quoted_path(w, [s.as_str(), name.as_str()], cfg);
            } else {
                w.push(&quote_ident(name, cfg));
            }
            if let Some(a) = alias {
                push_alias(w, a, cfg, cfg.emit_as_for_table_alias);
            }
        }
        TableRef::Subquery { query, alias } => {
            w.push("(");
            let inner = render_select(query, cfg, 128);
            w.push(inner);
            w.push(")");
            if let Some(a) = alias {
                push_alias(w, a, cfg, cfg.emit_as_for_table_alias);
            }
        }
    }
}

pub(crate) fn render_join(w: &mut SqlWriter, j: &Join, cfg: &SqlRenderCfg) {
    match j.kind {
        JoinKind::Inner => w.push("INNER JOIN "),
        JoinKind::Left => w.push("LEFT JOIN "),
        JoinKind::Right => w.push("RIGHT JOIN "),
        JoinKind::Full => w.push("FULL JOIN "),
        JoinKind::Cross => w.push("CROSS JOIN "),
        JoinKind::NaturalInner => w.push("NATURAL INNER JOIN "),
        JoinKind::NaturalLeft => w.push("NATURAL LEFT JOIN "),
        JoinKind::NaturalRight => w.push("NATURAL RIGHT JOIN "),
        JoinKind::NaturalFull => w.push("NATURAL FULL JOIN "),
    }
    render_table_ref(w, &j.table, cfg);
    if !matches!(
        j.kind,
        JoinKind::Cross
            | JoinKind::NaturalInner
            | JoinKind::NaturalLeft
            | JoinKind::NaturalRight
            | JoinKind::NaturalFull
    ) {
        if let Some(on) = &j.on {
            w.push(" ON ");
            render_expr(w, on, cfg);
        } else if let Some(cols) = &j.using_cols {
            w.push(" USING (");
            for (i, c) in cols.iter().enumerate() {
                w.push_sep(i, ", ");
                w.push(&quote_ident(c, cfg));
            }
            w.push(")");
        }
    }
}

pub(crate) fn render_paren_if_needed(w: &mut SqlWriter, e: &Expr, cfg: &SqlRenderCfg) {
    match e {
        Expr::Binary { .. } | Expr::Unary { .. } => {
            w.push("(");
            render_expr(w, e, cfg);
            w.push(")");
        }
        _ => render_expr(w, e, cfg),
    }
}

#[inline]
pub(crate) fn push_alias(w: &mut SqlWriter, alias: &str, cfg: &SqlRenderCfg, emit_as: bool) {
    if emit_as {
        w.push(" AS ");
    } else {
        w.push(" ");
    }
    w.push(&quote_ident(alias, cfg));
}
