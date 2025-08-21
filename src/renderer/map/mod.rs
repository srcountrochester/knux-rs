use crate::renderer::ast as R;
use sqlparser::ast as S;

mod __tests__;
mod insert;
mod select;
mod update;
mod utils;

pub use select::{map_query_body, map_to_render_ast, map_to_render_query};

// Универсальный роутер Statement -> renderer::ast::Stmt
pub fn map_to_render_stmt(stmt: &S::Statement) -> R::Stmt {
    match stmt {
        S::Statement::Query(q) => R::Stmt::Query(select::map_to_render_query(q)),
        S::Statement::Insert(i) => R::Stmt::Insert(insert::map_insert(i)),
        u if matches!(u, S::Statement::Update { .. }) => R::Stmt::Update(update::map_update(u)),
        _ => unimplemented!("unsupported statement for renderer"),
    }
}
