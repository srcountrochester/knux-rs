use super::utils::{map_expr, map_select_item};
use crate::renderer::{
    ast as R,
    map::utils::{object_name_join, split_object_name_cow},
};
use sqlparser::ast as S;

pub(crate) fn map_insert(i: &S::Insert) -> R::Insert {
    // TARGET
    let table = map_table_object(&i.table);

    // COLUMNS (с предвыделением)
    let columns = {
        let mut v = Vec::with_capacity(i.columns.len());
        for id in &i.columns {
            v.push(id.value.clone());
        }
        v
    };

    // VALUES (...) (без промежуточных collect/extend)
    let rows = match i.source.as_deref().map(|q| q.body.as_ref()) {
        Some(S::SetExpr::Values(S::Values { rows, .. })) => {
            let mut out = Vec::with_capacity(rows.len());
            for r in rows {
                let mut row = Vec::with_capacity(r.len());
                for e in r {
                    row.push(map_expr(e));
                }
                out.push(row);
            }
            out
        }
        _ => Vec::new(),
    };

    // RETURNING (с предвыделением)
    let returning = i.returning.as_ref().map_or_else(Vec::new, |v| {
        let mut out = Vec::with_capacity(v.len());
        for it in v {
            out.push(map_select_item(it));
        }
        out
    });

    let ignore = i.ignore;

    // ON CONFLICT / ON DUPLICATE (без промежуточного mutable on_conflict)
    let on_conflict = i.on.as_ref().and_then(|on| match on {
        // MySQL: ON DUPLICATE KEY UPDATE
        S::OnInsert::DuplicateKeyUpdate(assignments) => {
            let mut set = Vec::with_capacity(assignments.len());
            for a in assignments {
                set.push(map_assignment(a));
            }
            Some(R::OnConflict {
                target_columns: Vec::new(),
                on_constraint: None,
                action: Some(R::OnConflictAction::DoUpdate {
                    set,
                    where_predicate: None,
                }),
            })
        }

        // PG/SQLite: ON CONFLICT (...)
        S::OnInsert::OnConflict(conf) => {
            let (target_columns, on_constraint) = match &conf.conflict_target {
                Some(S::ConflictTarget::Columns(cols)) => {
                    let mut v = Vec::with_capacity(cols.len());
                    for c in cols {
                        v.push(c.value.clone());
                    }
                    (v, None)
                }
                Some(S::ConflictTarget::OnConstraint(obj)) => {
                    (Vec::new(), Some(object_name_join(obj, ".")))
                }
                None => (Vec::new(), None),
            };

            match &conf.action {
                S::OnConflictAction::DoNothing => Some(R::OnConflict {
                    target_columns,
                    on_constraint,
                    action: Some(R::OnConflictAction::DoNothing),
                }),
                S::OnConflictAction::DoUpdate(du) => {
                    let mut set = Vec::with_capacity(du.assignments.len());
                    for a in &du.assignments {
                        set.push(map_assignment(a));
                    }
                    let where_predicate = du.selection.as_ref().map(map_expr);
                    Some(R::OnConflict {
                        target_columns,
                        on_constraint,
                        action: Some(R::OnConflictAction::DoUpdate {
                            set,
                            where_predicate,
                        }),
                    })
                }
            }
        }

        _ => None,
    });

    R::Insert {
        table,
        columns,
        rows,
        ignore,
        on_conflict,
        returning,
    }
}

fn map_table_object(to: &S::TableObject) -> R::TableRef {
    match to {
        S::TableObject::TableName(obj) => {
            let (schema_cow, name_cow) = split_object_name_cow(obj);
            let schema: Option<String> = schema_cow.map(|c| c.into_owned());
            let name: String = name_cow.into_owned();
            R::TableRef::Named {
                schema,
                name,
                alias: None,
            }
        }
        _ => panic!("unsupported INSERT target table"),
    }
}

fn map_assignment(a: &S::Assignment) -> R::Assign {
    let col = match &a.target {
        S::AssignmentTarget::ColumnName(obj) => {
            obj.0.last().map(|id| id.to_string()).unwrap_or_default()
        }
        S::AssignmentTarget::Tuple(cols) => cols
            .last()
            .and_then(|o| o.0.last())
            .map(|id| id.to_string())
            .unwrap_or_default(),
    };

    let mut from_inserted = false;
    if let S::Expr::CompoundIdentifier(parts) = &a.value {
        if let Some(first) = parts.first() {
            let s = first.value.as_str();
            from_inserted = s.eq_ignore_ascii_case("EXCLUDED") || s.eq_ignore_ascii_case("NEW");
        }
    }

    R::Assign {
        col,
        value: map_expr(&a.value),
        from_inserted,
    }
}
