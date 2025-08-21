use crate::renderer::ast as R;
use crate::renderer::ident::quote_ident;
use crate::renderer::select::render_expr;
use crate::renderer::{Dialect, SqlRenderCfg, SqlWriter, render_select};

#[inline]
fn push_joined<T>(w: &mut SqlWriter, items: &[T], mut f: impl FnMut(&mut SqlWriter, &T)) {
    for (i, it) in items.iter().enumerate() {
        if i > 0 {
            w.push(", ");
        }
        f(w, it);
    }
}

#[inline]
fn render_table_ref(w: &mut SqlWriter, t: &R::TableRef, cfg: &SqlRenderCfg) {
    match t {
        R::TableRef::Named {
            schema,
            name,
            alias,
        } => {
            if let Some(s) = schema {
                w.push(&quote_ident(s, cfg));
                w.push(".");
            }
            w.push(&quote_ident(name, cfg));
            push_table_alias(w, alias, cfg);
        }
        R::TableRef::Subquery { query, alias } => {
            w.push("(");
            let sub = render_select(query, cfg, 128); // оставляем как есть (без инвазивных правок select-рендера)
            w.push(&sub);
            w.push(")");
            push_table_alias(w, alias, cfg);
        }
    }
}

#[inline]
fn render_returning(w: &mut SqlWriter, items: &[R::SelectItem], cfg: &SqlRenderCfg) {
    if items.is_empty() {
        return;
    }
    w.push(" RETURNING ");
    push_joined(w, items, |w, it| match it {
        R::SelectItem::Star { .. } => w.push("*"),
        R::SelectItem::QualifiedStar { table, .. } => {
            w.push(&quote_ident(table, cfg));
            w.push(".*");
        }
        R::SelectItem::Expr { expr, alias } => {
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
    });
}

/// Рендер `DELETE FROM ... [USING ...] [WHERE ...] [RETURNING ...]`
pub fn render_delete(d: &R::Delete, cfg: &SqlRenderCfg, cap: usize) -> String {
    let mut w = SqlWriter::new(cap, cfg.placeholders);

    w.push("DELETE FROM ");
    render_table_ref(&mut w, &d.table, cfg);

    if !d.using.is_empty() {
        w.push(" USING ");
        push_joined(&mut w, &d.using, |w, t| render_table_ref(w, t, cfg));
    }

    if let Some(pred) = &d.r#where {
        w.push(" WHERE ");
        render_expr(&mut w, pred, cfg);
    }

    match cfg.dialect {
        Dialect::Postgres | Dialect::SQLite => render_returning(&mut w, &d.returning, cfg),
        Dialect::MySQL => { /* ignore */ }
        _ => {}
    }

    w.finish()
}

#[inline]
fn push_table_alias(w: &mut SqlWriter, alias: &Option<String>, cfg: &SqlRenderCfg) {
    if let Some(a) = alias {
        if cfg.emit_as_for_table_alias {
            w.push(" AS ");
        } else {
            w.push(" ");
        }
        w.push(&quote_ident(a, cfg));
    }
}
