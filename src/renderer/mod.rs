mod __tests__;
pub mod ast;
mod config;
mod delete;
mod error;
mod ident;
mod insert;
pub mod map;
mod select;
mod update;
mod validate;
mod writer;

pub use config::Dialect;
pub use config::{FeaturePolicy, PlaceholderStyle, QuoteMode, SqlRenderCfg};
pub use map::{map_to_render_query, map_to_render_stmt};
pub use select::{render_select, render_sql_query};

use crate::renderer::insert::render_insert;
use crate::renderer::validate::validate_stmt_features;
use crate::renderer::{config::MysqlLimitStyle, validate::validate_query_features};
pub use ast::Expr;
use ast::Stmt;
pub use error::{Error, Result};
pub use writer::SqlWriter;

/// High-level API: рендер SELECT AST в строку SQL.
pub fn render_sql_select(sel: &ast::Select, cfg: &SqlRenderCfg) -> String {
    // capacity эвристика; можно параметризовать
    render_select(sel, cfg, 256)
}

pub fn try_render_sql_query(q: &ast::Query, cfg: &SqlRenderCfg) -> Result<String> {
    if let Some(err) = validate_query_features(q, cfg) {
        return Err(err);
    }
    Ok(render_sql_query(q, cfg))
}

pub fn render_sql_stmt(s: &Stmt, cfg: &SqlRenderCfg) -> String {
    match s {
        Stmt::Query(q) => render_sql_query(q, cfg),
        Stmt::Insert(i) => render_insert(i, cfg, 256),
        Stmt::Update(u) => update::render_update(u, cfg, 256),
        Stmt::Delete(d) => delete::render_delete(d, cfg, 256),
    }
}

pub fn try_render_sql_stmt(s: &Stmt, cfg: &SqlRenderCfg) -> Result<String> {
    if let Some(err) = validate_stmt_features(s, cfg) {
        return Err(err);
    }
    Ok(render_sql_stmt(s, cfg))
}

/// Удобные пресеты под диалекты
pub fn cfg_postgres_knex() -> SqlRenderCfg {
    SqlRenderCfg {
        dialect: Dialect::Postgres,
        quote: QuoteMode::Always,
        placeholders: PlaceholderStyle::Numbered,
        emulate_nulls_ordering: false,
        mysql_limit_style: MysqlLimitStyle::LimitOffset,
        policy: FeaturePolicy::Lenient,
        emit_as_for_table_alias: true,
        emit_as_for_column_alias: true,
        fold_idents: None,
    }
}
pub fn cfg_mysql_knex() -> SqlRenderCfg {
    SqlRenderCfg {
        dialect: Dialect::MySQL,
        quote: QuoteMode::Always,
        placeholders: PlaceholderStyle::Question,
        emulate_nulls_ordering: false,
        mysql_limit_style: MysqlLimitStyle::LimitOffset,
        policy: FeaturePolicy::Lenient,
        emit_as_for_table_alias: true,
        emit_as_for_column_alias: true,
        fold_idents: None,
    }
}
pub fn cfg_sqlite_knex() -> SqlRenderCfg {
    SqlRenderCfg {
        dialect: Dialect::SQLite,
        quote: QuoteMode::Always,
        placeholders: PlaceholderStyle::Question,
        emulate_nulls_ordering: false,
        mysql_limit_style: MysqlLimitStyle::LimitOffset,
        policy: FeaturePolicy::Lenient,
        emit_as_for_table_alias: true,
        emit_as_for_column_alias: true,
        fold_idents: None,
    }
}
