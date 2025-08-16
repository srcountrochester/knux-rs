#[cfg(test)]
mod core_fn;

#[cfg(test)]
mod utils;

#[cfg(test)]
mod where_clause;

#[cfg(test)]
mod where_json;

#[cfg(test)]
mod where_between;

#[cfg(test)]
mod where_exists;

#[cfg(test)]
mod where_in;

#[cfg(test)]
mod where_like;

#[cfg(test)]
mod where_null;

#[cfg(test)]
mod where_raw;

use sqlparser::ast::{Expr as SqlExpr, Query, SetExpr};

fn extract_where(q: &Query) -> Option<&SqlExpr> {
    let select = match q.body.as_ref() {
        SetExpr::Select(select_box) => select_box.as_ref(),
        _ => panic!("expected SELECT body"),
    };
    select.selection.as_ref()
}
