use std::fmt::Display;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dialect {
    Postgres,
    SQLite,
    MySQL,
}

impl Display for Dialect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Dialect::Postgres => write!(f, "postgres"),
            Dialect::SQLite => write!(f, "sqlite"),
            Dialect::MySQL => write!(f, "mysql"),
        }
    }
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
pub enum MysqlLimitStyle {
    /// LIMIT 10 OFFSET 20  (текущий единый стиль)
    LimitOffset,
    /// LIMIT 20, 10
    OffsetCommaLimit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeaturePolicy {
    /// Мягкая деградация: ILIKE→LIKE, DISTINCT ON→DISTINCT и т.п.
    Lenient,
    /// Строгая политика: неподдержанные фичи → ошибка рендера
    Strict,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FoldCase {
    Lower,
    Upper,
}

#[derive(Clone, Copy, Debug)]
pub struct SqlRenderCfg {
    pub dialect: Dialect,
    pub quote: QuoteMode,
    pub placeholders: PlaceholderStyle,

    /// Эмулировать NULLS LAST/FIRST на MySQL/SQLite
    pub emulate_nulls_ordering: bool,

    /// Способ печати LIMIT в MySQL
    pub mysql_limit_style: MysqlLimitStyle,

    /// Политика по фичам (строго/мягко)
    pub policy: FeaturePolicy,

    /// Добавлять ли "AS" для алиаса таблицы/колонки
    pub emit_as_for_table_alias: bool,
    pub emit_as_for_column_alias: bool,

    /// Принудительная нормализация идентификаторов (после квотирования логика не меняется)
    pub fold_idents: Option<FoldCase>,
}

impl Default for SqlRenderCfg {
    fn default() -> Self {
        Self {
            dialect: Dialect::Postgres,
            quote: QuoteMode::Smart {
                preserve_case: false,
            },
            placeholders: PlaceholderStyle::Question,
            emulate_nulls_ordering: false,
            mysql_limit_style: MysqlLimitStyle::LimitOffset,
            policy: FeaturePolicy::Lenient,
            emit_as_for_table_alias: true,
            emit_as_for_column_alias: true,
            fold_idents: None,
        }
    }
}
