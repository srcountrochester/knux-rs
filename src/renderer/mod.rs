mod __tests__;
pub mod ast;
mod config;
mod ident;
pub mod map;
mod select;
mod writer;

pub use config::Dialect;
use config::{PlaceholderStyle, QuoteMode, SqlRenderCfg};
use select::render_select;

/// High-level API: рендер SELECT AST в строку SQL.
pub fn render_sql_select(sel: &ast::Select, cfg: &SqlRenderCfg) -> String {
    // capacity эвристика; можно параметризовать
    render_select(sel, cfg, 256)
}

/// Удобные пресеты под диалекты
pub fn cfg_postgres_knex() -> SqlRenderCfg {
    SqlRenderCfg {
        dialect: Dialect::Postgres,
        quote: QuoteMode::Always,
        placeholders: PlaceholderStyle::Numbered,
    }
}
pub fn cfg_mysql_knex() -> SqlRenderCfg {
    SqlRenderCfg {
        dialect: Dialect::MySQL,
        quote: QuoteMode::Always,
        placeholders: PlaceholderStyle::Question,
    }
}
pub fn cfg_sqlite_knex() -> SqlRenderCfg {
    SqlRenderCfg {
        dialect: Dialect::SQLite,
        quote: QuoteMode::Always,
        placeholders: PlaceholderStyle::Question,
    }
}
