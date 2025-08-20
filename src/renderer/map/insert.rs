use super::utils::{map_expr, map_select_item};
use crate::renderer::{
    ast as R,
    map::utils::{object_name_join, split_object_name_cow},
};
use sqlparser::ast as S;

pub(crate) fn map_insert(i: &S::Insert) -> R::Insert {
    let table = map_table_object(&i.table);
    let columns: Vec<String> = i.columns.iter().map(|id| id.value.clone()).collect();

    let mut rows: Vec<Vec<R::Expr>> = Vec::new();
    if let Some(q) = &i.source {
        if let S::SetExpr::Values(S::Values { rows: vs, .. }) = q.body.as_ref() {
            rows = Vec::with_capacity(vs.len());
            for r in vs {
                let mut row = Vec::with_capacity(r.len());
                row.extend(r.iter().map(map_expr));
                rows.push(row);
            }
        }
    }

    let returning: Vec<R::SelectItem> = i
        .returning
        .as_ref()
        .map(|v| v.iter().map(map_select_item).collect())
        .unwrap_or_default();

    let ignore = i.ignore;

    let mut on_conflict: Option<R::OnConflict> = None;
    if let Some(on) = &i.on {
        match on {
            // MySQL: ON DUPLICATE KEY UPDATE
            S::OnInsert::DuplicateKeyUpdate(assignments) => {
                let set = assignments.iter().map(map_assignment).collect::<Vec<_>>();
                on_conflict = Some(R::OnConflict {
                    target_columns: Vec::new(),
                    on_constraint: None,
                    action: Some(R::OnConflictAction::DoUpdate {
                        set,
                        where_predicate: None,
                    }),
                });
            }

            // PG/SQLite: ON CONFLICT (...)
            S::OnInsert::OnConflict(conf) => {
                let (target_columns, on_constraint) = match &conf.conflict_target {
                    Some(S::ConflictTarget::Columns(cols)) => {
                        (cols.iter().map(|c| c.value.clone()).collect(), None)
                    }
                    Some(S::ConflictTarget::OnConstraint(obj)) => {
                        (Vec::new(), Some(object_name_join(obj, ".")))
                    }
                    None => (Vec::new(), None),
                };

                match &conf.action {
                    S::OnConflictAction::DoNothing => {
                        on_conflict = Some(R::OnConflict {
                            target_columns,
                            on_constraint,
                            action: Some(R::OnConflictAction::DoNothing),
                        });
                    }
                    S::OnConflictAction::DoUpdate(du) => {
                        let set = du
                            .assignments
                            .iter()
                            .map(map_assignment)
                            .collect::<Vec<_>>();
                        let where_predicate = du.selection.as_ref().map(map_expr);
                        on_conflict = Some(R::OnConflict {
                            target_columns,
                            on_constraint,
                            action: Some(R::OnConflictAction::DoUpdate {
                                set,
                                where_predicate,
                            }),
                        });
                    }
                }
            }
            _ => {}
        }
    }

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
