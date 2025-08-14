use super::config::{Dialect, QuoteMode, SqlRenderCfg};
use std::{borrow::Cow, cell::OnceCell, sync::OnceLock};

fn is_simple_ident(s: &str) -> bool {
    let mut it = s.chars();
    match it.next() {
        Some(c) if c == '_' || c.is_ascii_alphabetic() => {}
        _ => return false,
    }
    it.all(|c| c == '_' || c.is_ascii_alphanumeric())
}

const COMMON_KEYWORDS: &[&str] = &[
    "select",
    "from",
    "where",
    "group",
    "order",
    "by",
    "limit",
    "offset",
    "join",
    "left",
    "right",
    "inner",
    "outer",
    "on",
    "as",
    "and",
    "or",
    "not",
    "user",
    "table",
    "index",
    "primary",
    "key",
    "unique",
    "constraint",
];

static SORTED_KEYWORDS: OnceLock<Vec<&'static str>> = OnceLock::new();

fn sorted_keywords() -> &'static [&'static str] {
    SORTED_KEYWORDS.get_or_init(|| {
        let mut v = COMMON_KEYWORDS.to_vec();
        v.sort_unstable();
        v
    })
}

fn is_common_keyword(s: &str) -> bool {
    let s = s.to_ascii_lowercase();
    sorted_keywords().binary_search(&&*s).is_ok()
}

fn escape_body<'a>(s: &'a str, dialect: Dialect) -> Cow<'a, str> {
    match dialect {
        Dialect::Postgres | Dialect::SQLite => {
            if s.contains('"') {
                Cow::Owned(s.replace('"', "\"\""))
            } else {
                Cow::Borrowed(s)
            }
        }
        Dialect::MySQL => {
            if s.contains('`') {
                Cow::Owned(s.replace('`', "``"))
            } else {
                Cow::Borrowed(s)
            }
        }
    }
}

pub fn quote_ident_always(name: &str, dialect: Dialect) -> String {
    match dialect {
        Dialect::Postgres | Dialect::SQLite => {
            let body = escape_body(name, dialect);
            format!("\"{}\"", body)
        }
        Dialect::MySQL => {
            let body = escape_body(name, dialect);
            format!("`{}`", body)
        }
    }
}

pub fn quote_ident(name: &str, cfg: &SqlRenderCfg) -> String {
    match cfg.quote {
        QuoteMode::Always => quote_ident_always(name, cfg.dialect),
        QuoteMode::Smart { preserve_case } => {
            if preserve_case || !is_simple_ident(name) || is_common_keyword(name) {
                quote_ident_always(name, cfg.dialect)
            } else {
                name.to_string()
            }
        }
    }
}

/// schema.table / table.column / schema.table.column
pub fn quote_path<'a, I>(parts: I, cfg: &SqlRenderCfg) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    parts
        .into_iter()
        .map(|p| quote_ident(p, cfg))
        .collect::<Vec<_>>()
        .join(".")
}
