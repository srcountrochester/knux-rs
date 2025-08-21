use crate::renderer::ast as R;
use crate::renderer::ident::quote_ident;
use crate::renderer::select::render_expr;
use crate::renderer::{Dialect, SqlRenderCfg, SqlWriter};

/// Рендер `INSERT` с учётом диалектов.
pub fn render_insert(i: &R::Insert, cfg: &SqlRenderCfg, cap: usize) -> String {
    let mut w = SqlWriter::new(cap, cfg.placeholders);

    // 1) Префикс
    let sqlite_ignore_without_action = matches!(cfg.dialect, Dialect::SQLite)
        && i.ignore
        && i.on_conflict
            .as_ref()
            .and_then(|c| c.action.as_ref())
            .is_none();

    match cfg.dialect {
        Dialect::SQLite if sqlite_ignore_without_action => w.push("INSERT OR IGNORE INTO "),
        Dialect::MySQL if i.ignore => w.push("INSERT IGNORE INTO "),
        _ => w.push("INSERT INTO "),
    }

    // 2) Таблица и колонки
    render_table_ref(&mut w, &i.table, cfg);

    // В MySQL для ON DUPLICATE KEY UPDATE понадобятся ссылки на "new.col"
    let need_alias_new = matches!(cfg.dialect, Dialect::MySQL)
        && i.on_conflict.as_ref().map_or(false, |c| {
            matches!(c.action, Some(R::OnConflictAction::DoUpdate { .. }))
        });
    if need_alias_new {
        w.push(" AS ");
        w.push(&quote_ident("new", cfg)); // → `new`
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
                } else {
                    render_conflict_target_columns(&mut w, &spec.target_columns, cfg);
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
                        render_set_assignments_common(
                            &mut w,
                            set,
                            where_predicate.as_ref(),
                            cfg,
                            "EXCLUDED.",
                            " WHERE ",
                        );
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
                render_conflict_target_columns(&mut w, &spec.target_columns, cfg);
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
                        render_set_assignments_common(
                            &mut w,
                            set,
                            where_predicate.as_ref(),
                            cfg,
                            "EXCLUDED.",
                            " WHERE ",
                        );
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
                    render_set_assignments_common(
                        &mut w,
                        set,
                        where_predicate.as_ref(),
                        cfg,
                        "new.",
                        " /* WHERE */ ",
                    );
                }
            }
            // RETURNING в MySQL не печатаем
        }

        _ => {}
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
    push_joined(w, cols, |w, c| w.push(&quote_ident(c, cfg)));
    w.push(")");
}

#[inline]
fn render_values(w: &mut SqlWriter, rows: &[Vec<R::Expr>], cfg: &SqlRenderCfg) {
    w.push(" VALUES ");
    push_joined(w, rows, |w, row| {
        w.push("(");
        push_joined(w, row, |w, e| render_expr(w, e, cfg));
        w.push(")");
    });
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
                w.push(" AS ");
                w.push(&quote_ident(a, cfg));
            }
        }
    });
}

/// Универсальный вывод списка через запятую
#[inline]
fn push_joined<T>(w: &mut SqlWriter, items: &[T], mut f: impl FnMut(&mut SqlWriter, &T)) {
    for (i, it) in items.iter().enumerate() {
        if i > 0 {
            w.push(", ");
        }
        f(w, it);
    }
}

/// (a = b, c = d, ...) + опциональный WHERE/комментарий.
/// `inserted_prefix`: "EXCLUDED." (PG/SQLite) или "new." (MySQL)
/// `where_kw`: " WHERE " (PG/SQLite) или " /* WHERE */ " (MySQL)
#[inline]
fn render_set_assignments_common(
    w: &mut SqlWriter,
    set: &[R::Assign],
    where_predicate: Option<&R::Expr>,
    cfg: &SqlRenderCfg,
    inserted_prefix: &str,
    where_kw: &str,
) {
    push_joined(w, set, |w, a| {
        w.push(&quote_ident(&a.col, cfg));
        w.push(" = ");
        if a.from_inserted {
            w.push(inserted_prefix);
            w.push(&quote_ident(&a.col, cfg));
        } else {
            render_expr(w, &a.value, cfg);
        }
    });

    if let Some(pred) = where_predicate {
        w.push(where_kw);
        render_expr(w, pred, cfg);
    }
}

/// (col1, col2, ...)
#[inline]
fn render_conflict_target_columns<T: AsRef<str>>(
    w: &mut SqlWriter,
    cols: &[T],
    cfg: &SqlRenderCfg,
) {
    if cols.is_empty() {
        return;
    }
    w.push(" (");
    push_joined(w, cols, |w, c| w.push(&quote_ident(c.as_ref(), cfg)));
    w.push(")");
}
