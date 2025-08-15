mod __tests__;
pub mod ast;
mod config;
mod error;
mod ident;
pub mod map;
mod select;
mod validate;
mod writer;

pub use config::Dialect;
pub use config::{FeaturePolicy, PlaceholderStyle, QuoteMode, SqlRenderCfg};
pub use map::map_to_render_query;
pub use select::{render_select, render_sql_query};

use crate::renderer::{config::MysqlLimitStyle, validate::validate_query_features};
pub use error::{Error, Result};

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
