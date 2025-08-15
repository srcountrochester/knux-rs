use crate::renderer::config::FoldCase;

use super::config::{Dialect, QuoteMode, SqlRenderCfg};
use std::{borrow::Cow, cell::OnceCell, collections::HashSet, sync::OnceLock};

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

static KW_SET: OnceLock<HashSet<&'static str>> = OnceLock::new();

fn kw_set() -> &'static HashSet<&'static str> {
    KW_SET.get_or_init(|| COMMON_KEYWORDS.iter().copied().collect())
}

fn is_common_keyword(s: &str) -> bool {
    kw_set().contains(&*s.to_ascii_lowercase())
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
    let name = if let Some(fold) = cfg.fold_idents {
        match fold {
            FoldCase::Lower => name.to_ascii_lowercase(),
            FoldCase::Upper => name.to_ascii_uppercase(),
        }
    } else {
        name.to_string()
    };

    match cfg.quote {
        QuoteMode::Always => quote_ident_always(&name, cfg.dialect),
        QuoteMode::Smart { preserve_case } => {
            if preserve_case || !is_simple_ident(&name) || is_common_keyword(&name) {
                quote_ident_always(&name, cfg.dialect)
            } else {
                name
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
        .map(|p| {
            if p == "*" {
                "*".to_string()
            } else {
                quote_ident(p, cfg)
            }
        })
        .collect::<Vec<_>>()
        .join(".")
}
