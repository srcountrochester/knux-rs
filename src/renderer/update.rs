use crate::renderer::ast as R;
use crate::renderer::ident::quote_ident;
use crate::renderer::select::render_expr;
use crate::renderer::{Dialect, SqlRenderCfg, SqlWriter};

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
            if let Some(a) = alias {
                if cfg.emit_as_for_table_alias {
                    w.push(" AS ");
                } else {
                    w.push(" ");
                }
                w.push(&quote_ident(a, cfg));
            }
        }
        _ => unreachable!("UPDATE target must be a named table"),
    }
}

#[inline]
fn render_returning(w: &mut SqlWriter, items: &[R::SelectItem], cfg: &SqlRenderCfg) {
    if items.is_empty() {
        return;
    }
    w.push(" RETURNING ");
    for (i, it) in items.iter().enumerate() {
        if i > 0 {
            w.push(", ");
        }
        match it {
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
        }
    }
}

/// Рендер `UPDATE ... SET ... [WHERE ...] [RETURNING ...]`
pub fn render_update(u: &R::Update, cfg: &SqlRenderCfg, cap: usize) -> String {
    let mut w = SqlWriter::new(cap, cfg.placeholders);

    // Префикс
    match cfg.dialect {
        Dialect::SQLite => {
            w.push("UPDATE");
            if let Some(or_) = &u.sqlite_or {
                w.push(" OR ");
                match or_ {
                    R::SqliteOr::Replace => w.push("REPLACE"),
                    R::SqliteOr::Ignore => w.push("IGNORE"),
                }
            }
            w.push(" ");
        }
        _ => w.push("UPDATE "),
    }

    render_table_ref(&mut w, &u.table, cfg);

    w.push(" SET ");
    push_joined(&mut w, &u.set, |w, a| {
        w.push(&quote_ident(&a.col, cfg));
        w.push(" = ");
        render_expr(w, &a.value, cfg);
    });

    // FROM (PG/SQLite)
    match cfg.dialect {
        Dialect::Postgres | Dialect::SQLite => {
            if !u.from.is_empty() {
                w.push(" FROM ");
                push_joined(&mut w, &u.from, |w, t| render_table_ref(w, t, cfg));
            }
        }
        _ => {}
    }

    if let Some(pred) = &u.r#where {
        w.push(" WHERE ");
        render_expr(&mut w, pred, cfg);
    }

    match cfg.dialect {
        Dialect::Postgres | Dialect::SQLite => {
            render_returning(&mut w, &u.returning, cfg);
        }
        Dialect::MySQL => { /* MySQL: RETURNING не печатаем */ }
        _ => {}
    }

    w.finish()
}
