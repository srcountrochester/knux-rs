#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dialect {
    Postgres,
    SQLite,
    MySQL,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuoteMode {
    /// Всегда квотить идентификаторы (как knex): "Users", `Users`
    Always,
    /// Квотить только при необходимости; если preserve_case=true — тоже квотим
    Smart { preserve_case: bool },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlaceholderStyle {
    /// $1, $2, $3... (Postgres)
    Numbered,
    /// ? (SQLite/MySQL)
    Question,
}

#[derive(Clone, Copy, Debug)]
pub struct SqlRenderCfg {
    pub dialect: Dialect,
    pub quote: QuoteMode,
    pub placeholders: PlaceholderStyle,
}

impl Default for SqlRenderCfg {
    fn default() -> Self {
        Self {
            dialect: Dialect::Postgres,
            quote: QuoteMode::Always,
            placeholders: PlaceholderStyle::Numbered,
        }
    }
}
