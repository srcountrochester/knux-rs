use crate::renderer::ast as R;
use crate::renderer::ident::quote_ident;
use crate::renderer::select::render_expr;
use crate::renderer::{Dialect, SqlRenderCfg, SqlWriter};

/// Рендер `INSERT` с учётом диалектов.
///
/// Параметры возвращаем в порядке:
///   1) все params значений `VALUES` (построчно слева направо),
///   2) затем params из `merge()` (RHS выражения).
pub fn render_insert(i: &R::Insert, cfg: &SqlRenderCfg, cap: usize) -> String {
    let mut w = SqlWriter::new(cap, cfg.placeholders);

    // 1) Префикс
    match cfg.dialect {
        Dialect::SQLite => {
            if i.ignore
                && i.on_conflict
                    .as_ref()
                    .and_then(|c| c.action.as_ref())
                    .is_none()
            {
                w.push("INSERT OR IGNORE INTO ");
            } else {
                w.push("INSERT INTO ");
            }
        }
        Dialect::MySQL => {
            if i.ignore {
                w.push("INSERT IGNORE INTO ");
            } else {
                w.push("INSERT INTO ");
            }
        }
        _ => w.push("INSERT INTO "),
    }

    // 2) Таблица и колонки
    render_table_ref(&mut w, &i.table, cfg);
    let need_alias_new = matches!(cfg.dialect, Dialect::MySQL)
        && i.on_conflict
            .as_ref()
            .map(|c| matches!(c.action, Some(R::OnConflictAction::DoUpdate { .. })))
            .unwrap_or(false);

    print!("need_alias_new: {need_alias_new} ");
    print!("cfg.dialect: {:?} ", cfg.dialect);
    print!("i.on_conflict: {:?} ", i.on_conflict);

    if need_alias_new {
        w.push(" AS ");
        w.push(&quote_ident("new", cfg)); // в MySQL даст `new`
    }
    render_columns(&mut w, &i.columns, cfg);

    // 3) VALUES
    render_values(&mut w, &i.rows, cfg);

    // 4) UPSERT/IGNORE
    match cfg.dialect {
        Dialect::Postgres => {
            if let Some(spec) = &i.on_conflict {
                w.push(" ON CONFLICT");
                if let Some(name) = &spec.on_constraint {
                    w.push(" ON CONSTRAINT ");
                    w.push(&quote_ident(name, cfg));
                } else if !spec.target_columns.is_empty() {
                    w.push(" (");
                    for (k, c) in spec.target_columns.iter().enumerate() {
                        if k > 0 {
                            w.push(", ");
                        }
                        w.push(&quote_ident(c, cfg));
                    }
                    w.push(")");
                }
                match &spec.action {
                    None => {
                        if i.ignore {
                            w.push(" DO NOTHING");
                        }
                    }
                    Some(R::OnConflictAction::DoNothing) => w.push(" DO NOTHING"),
                    Some(R::OnConflictAction::DoUpdate {
                        set,
                        where_predicate,
                    }) => {
                        w.push(" DO UPDATE SET ");
                        for (s, a) in set.iter().enumerate() {
                            if s > 0 {
                                w.push(", ");
                            }
                            w.push(&quote_ident(&a.col, cfg));
                            w.push(" = ");
                            if a.from_inserted {
                                w.push("EXCLUDED.");
                                w.push(&quote_ident(&a.col, cfg));
                            } else {
                                render_expr(&mut w, &a.value, cfg);
                            }
                        }
                        if let Some(pred) = where_predicate {
                            w.push(" WHERE ");
                            render_expr(&mut w, pred, cfg);
                        }
                    }
                }
            } else if i.ignore {
                w.push(" ON CONFLICT DO NOTHING");
            }
            render_returning(&mut w, &i.returning, cfg);
        }

        Dialect::SQLite => {
            if let Some(spec) = &i.on_conflict {
                w.push(" ON CONFLICT");
                if !spec.target_columns.is_empty() {
                    w.push(" (");
                    for (k, c) in spec.target_columns.iter().enumerate() {
                        if k > 0 {
                            w.push(", ");
                        }
                        w.push(&quote_ident(c, cfg));
                    }
                    w.push(")");
                }
                match &spec.action {
                    None => {
                        if i.ignore {
                            w.push(" DO NOTHING");
                        }
                    }
                    Some(R::OnConflictAction::DoNothing) => w.push(" DO NOTHING"),
                    Some(R::OnConflictAction::DoUpdate {
                        set,
                        where_predicate,
                    }) => {
                        w.push(" DO UPDATE SET ");
                        for (s, a) in set.iter().enumerate() {
                            if s > 0 {
                                w.push(", ");
                            }
                            w.push(&quote_ident(&a.col, cfg));
                            w.push(" = ");
                            if a.from_inserted {
                                w.push("EXCLUDED.");
                                w.push(&quote_ident(&a.col, cfg));
                            } else {
                                render_expr(&mut w, &a.value, cfg);
                            }
                        }
                        if let Some(pred) = where_predicate {
                            w.push(" WHERE ");
                            render_expr(&mut w, pred, cfg);
                        }
                    }
                }
            }
            render_returning(&mut w, &i.returning, cfg);
        }

        Dialect::MySQL => {
            if let Some(spec) = &i.on_conflict {
                if let Some(R::OnConflictAction::DoUpdate {
                    set,
                    where_predicate,
                }) = &spec.action
                {
                    w.push(" ON DUPLICATE KEY UPDATE ");
                    for (idx, a) in set.iter().enumerate() {
                        if idx > 0 {
                            w.push(", ");
                        }
                        w.push(&quote_ident(&a.col, cfg));
                        w.push(" = ");
                        if a.from_inserted {
                            w.push("new.");
                            w.push(&quote_ident(&a.col, cfg));
                        } else {
                            render_expr(&mut w, &a.value, cfg);
                        }
                    }
                    if let Some(pred) = where_predicate {
                        w.push(" /* WHERE */ ");
                        render_expr(&mut w, pred, cfg);
                    }
                }
            }
            // RETURNING в MySQL не печатаем
        }

        _ => { /* no-op */ }
    }

    w.finish()
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
                w.push(" AS ");
                w.push(&quote_ident(a, cfg));
            }
        }
        _ => unreachable!("INSERT target must be a named table"),
    }
}

#[inline]
fn render_columns(w: &mut SqlWriter, cols: &[String], cfg: &SqlRenderCfg) {
    if cols.is_empty() {
        return;
    }
    w.push(" (");
    for (i, c) in cols.iter().enumerate() {
        if i > 0 {
            w.push(", ");
        }
        w.push(&quote_ident(c, cfg));
    }
    w.push(")");
}

#[inline]
fn render_values(w: &mut SqlWriter, rows: &[Vec<R::Expr>], cfg: &SqlRenderCfg) {
    w.push(" VALUES ");
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            w.push(", ");
        }
        w.push("(");
        for (j, e) in row.iter().enumerate() {
            if j > 0 {
                w.push(", ");
            }
            render_expr(w, e, cfg);
        }
        w.push(")");
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
                    w.push(" AS ");
                    w.push(&quote_ident(a, cfg));
                }
            }
        }
    }
}
