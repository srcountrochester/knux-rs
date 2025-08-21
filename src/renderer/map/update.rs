use super::utils::{map_expr, map_select_item, map_table_factor_named};
use crate::renderer::ast as R;
use sqlparser::ast as S;

pub(crate) fn map_update(stmt: &S::Statement) -> R::Update {
    let (table_wj, assignments, selection, returning_opt, from_opt, or_opt) = match stmt {
        S::Statement::Update {
            table,
            assignments,
            selection,
            returning,
            from,
            or,
        } => (table, assignments, selection, returning, from, or),
        _ => unreachable!("map_update called with non-UPDATE statement"),
    };

    // target table
    if !table_wj.joins.is_empty() {
        panic!("unsupported UPDATE with joins");
    }
    let table = map_table_factor_named(&table_wj.relation);

    // SET
    let set = assignments
        .iter()
        .map(|a| {
            let col = match &a.target {
                S::AssignmentTarget::ColumnName(obj) => {
                    obj.0.last().map(|p| p.to_string()).unwrap_or_default()
                }
                S::AssignmentTarget::Tuple(cols) => cols
                    .last()
                    .and_then(|o| o.0.last())
                    .map(|p| p.to_string())
                    .unwrap_or_default(),
            };
            R::Assign {
                col,
                value: super::utils::map_expr(&a.value),
                from_inserted: false,
            }
        })
        .collect::<Vec<_>>();

    // WHERE
    let r#where = selection.as_ref().map(map_expr);

    // RETURNING
    let returning = returning_opt
        .as_ref()
        .map(|v| v.iter().map(map_select_item).collect())
        .unwrap_or_default();

    // FROM
    let from = match from_opt {
        Some(S::UpdateTableFromKind::BeforeSet(list))
        | Some(S::UpdateTableFromKind::AfterSet(list)) => list
            .iter()
            .map(|twj| map_table_factor_named(&twj.relation))
            .collect(),
        None => Vec::new(),
    };

    // SQLite OR
    let sqlite_or = match or_opt {
        Some(S::SqliteOnConflict::Replace) => Some(R::SqliteOr::Replace),
        Some(S::SqliteOnConflict::Ignore) => Some(R::SqliteOr::Ignore),
        _ => None,
    };

    R::Update {
        table,
        set,
        r#where,
        returning,
        from,
        sqlite_or,
    }
}
