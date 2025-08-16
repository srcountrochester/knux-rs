use sqlparser::ast::{Expr as SqlExpr, SetExpr, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use super::super::{Error, Result};

pub fn parse_where_expr(s: &str) -> Result<SqlExpr> {
    let dialect = GenericDialect {};
    let sql = format!("SELECT 1 WHERE {}", s);

    let stmts = Parser::parse_sql(&dialect, &sql).map_err(|e| Error::InvalidExpression {
        reason: e.to_string().into(),
    })?;
    let stmt = stmts
        .into_iter()
        .next()
        .ok_or_else(|| Error::InvalidExpression {
            reason: "empty parse".into(),
        })?;

    match stmt {
        Statement::Query(q) => match *q.body {
            SetExpr::Select(sel) => sel.selection.ok_or_else(|| Error::InvalidExpression {
                reason: "no where".into(),
            }),
            _ => Err(Error::InvalidExpression {
                reason: "unexpected setexpr".into(),
            }),
        },
        _ => Err(Error::InvalidExpression {
            reason: "unexpected statement".into(),
        }),
    }
}

/// Простейшее экранирование одинарных кавычек и оборачивание в '...'
pub fn quote_sql_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        if ch == '\'' {
            out.push('\''); // escape → ''
        }
        out.push(ch);
    }
    out.push('\'');
    out
}

/// SQLite: проверка, что `left` содержит все ключи и значения `right_json`.
/// Реализовано через отрицание существования «несовпадающих» пар.
pub fn sqlite_json_superset_sql(left: &str, right_json: &str) -> String {
    // NOT EXISTS (
    //   SELECT 1 FROM json_each(<right_json>) r
    //   WHERE json_type(json_extract(<left>, '$.'||r.key)) IS NULL
    //      OR json_extract(<left>, '$.'||r.key) <> r.value
    // )
    format!(
        "NOT EXISTS (\
           SELECT 1 FROM json_each({right}) AS r \
           WHERE json_type(json_extract({left}, '$.'||r.key)) IS NULL \
             OR json_extract({left}, '$.'||r.key) <> r.value\
         )",
        left = left,
        right = quote_sql_str(right_json),
    )
}

/// SQLite: проверка, что `left_json` является подмножеством `right`.
pub fn sqlite_json_subset_sql(left_json: &str, right: &str) -> String {
    // subset(left, right) == every (k,v) из left есть в right
    format!(
        "NOT EXISTS (\
           SELECT 1 FROM json_each({left}) AS l \
           WHERE json_type(json_extract({right}, '$.'||l.key)) IS NULL \
             OR json_extract({right}, '$.'||l.key) <> l.value\
         )",
        left = quote_sql_str(left_json),
        right = right,
    )
}
