use super::utils::{map_expr, map_select_item, map_table_factor_named};
use crate::renderer::ast as R;
use sqlparser::ast as S;

pub(crate) fn map_update(stmt: &S::Statement) -> R::Update {
    let S::Statement::Update {
        table: table_wj,
        assignments,
        selection,
        returning: returning_opt,
        from: from_opt,
        or: or_opt,
    } = stmt
    else {
        unreachable!("map_update called with non-UPDATE statement");
    };

    // target table
    if !table_wj.joins.is_empty() {
        panic!("unsupported UPDATE with joins");
    }
    let table = map_table_factor_named(&table_wj.relation);

    // SET
    let mut set = Vec::with_capacity(assignments.len());
    for a in assignments {
        let col = match &a.target {
            S::AssignmentTarget::ColumnName(obj) => last_part_to_string(&obj.0),
            S::AssignmentTarget::Tuple(cols) => cols
                .last()
                .map(|o| last_part_to_string(&o.0))
                .unwrap_or_default(),
        };
        set.push(R::Assign {
            col,
            value: map_expr(&a.value),
            from_inserted: false,
        });
    }

    // WHERE
    let r#where = selection.as_ref().map(map_expr);

    // RETURNING
    let returning = if let Some(v) = returning_opt.as_ref() {
        let mut out = Vec::with_capacity(v.len());
        out.extend(v.iter().map(map_select_item));
        out
    } else {
        Vec::new()
    };

    // FROM (BeforeSet / AfterSet — одинаково маппим список)
    let from = if let Some(kind) = from_opt {
        let list = match kind {
            S::UpdateTableFromKind::BeforeSet(l) | S::UpdateTableFromKind::AfterSet(l) => l,
        };
        let mut out = Vec::with_capacity(list.len());
        for twj in list {
            out.push(map_table_factor_named(&twj.relation));
        }
        out
    } else {
        Vec::new()
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

#[inline]
fn last_part_to_string(parts: &[S::ObjectNamePart]) -> String {
    match parts.last() {
        Some(S::ObjectNamePart::Identifier(id)) => id.value.clone(),
        Some(other) => other.to_string(),
        None => String::new(),
    }
}
