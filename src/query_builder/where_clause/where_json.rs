use smallvec::smallvec;

use crate::{
    query_builder::{
        QueryBuilder,
        where_clause::utils::{
            parse_where_expr, quote_sql_str, sqlite_json_subset_sql, sqlite_json_superset_sql,
        },
    },
    renderer::Dialect,
};

impl<'a, T> QueryBuilder<'a, T> {
    /// WHERE <col> contains <json-obj>
    pub fn where_json_object(mut self, col: &str, json: &str) -> Self {
        let sql = match self.dialect {
            // PG: left @> right::jsonb
            Dialect::Postgres => format!("({}::jsonb) @> ({}::jsonb)", col, quote_sql_str(json)),
            // MySQL: JSON_CONTAINS(left, right_json)
            Dialect::MySQL => format!("JSON_CONTAINS({}, {})", col, quote_sql_str(json)),
            // SQLite: left ⊇ right  через проверку отсутствия «пробелов» в правом объекте
            Dialect::SQLite => sqlite_json_superset_sql(col, json),
            // fallback: MySQL-стиль
            _ => format!("JSON_CONTAINS({}, {})", col, quote_sql_str(json)),
        };
        match parse_where_expr(&sql) {
            Ok(expr) => self.attach_where_with_and(expr, smallvec![]),
            Err(e) => self.push_builder_error(format!("where_json_object(): {}", e)),
        }
        self
    }

    /// WHERE json-path exists
    pub fn where_json_path(mut self, json_expr: &str, path: &str) -> Self {
        let sql = match self.dialect {
            // PG: jsonb_path_exists(target::jsonb, path::jsonpath)
            Dialect::Postgres => format!(
                "jsonb_path_exists(({}::jsonb), ({}::jsonpath))",
                json_expr,
                quote_sql_str(path)
            ),
            // MySQL: JSON_CONTAINS_PATH(target, 'one', '$.a.b')
            Dialect::MySQL => format!(
                "JSON_CONTAINS_PATH({}, 'one', {})",
                json_expr,
                quote_sql_str(path)
            ),
            // SQLite: json_extract(target, '$.a.b') IS NOT NULL
            Dialect::SQLite => format!(
                "json_extract({}, {}) IS NOT NULL",
                json_expr,
                quote_sql_str(path)
            ),
            // fallback → MySQL-стиль
            _ => format!(
                "JSON_CONTAINS_PATH({}, 'one', {})",
                json_expr,
                quote_sql_str(path)
            ),
        };
        match parse_where_expr(&sql) {
            Ok(expr) => self.attach_where_with_and(expr, smallvec![]),
            Err(e) => self.push_builder_error(format!("where_json_path(): {}", e)),
        }
        self
    }

    /// WHERE left ⊇ right
    pub fn where_json_superset_of(mut self, left: &str, right_json: &str) -> Self {
        let sql = match self.dialect {
            Dialect::Postgres => format!(
                "({}::jsonb) @> ({}::jsonb)",
                left,
                quote_sql_str(right_json)
            ),
            Dialect::MySQL => format!("JSON_CONTAINS({}, {})", left, quote_sql_str(right_json)),
            Dialect::SQLite => sqlite_json_superset_sql(left, right_json),
            _ => format!("JSON_CONTAINS({}, {})", left, quote_sql_str(right_json)),
        };
        match parse_where_expr(&sql) {
            Ok(expr) => self.attach_where_with_and(expr, smallvec![]),
            Err(e) => self.push_builder_error(format!("where_json_superset_of(): {}", e)),
        }
        self
    }

    /// WHERE left ⊆ right
    pub fn where_json_subset_of(mut self, left_json: &str, right: &str) -> Self {
        let sql = match self.dialect {
            Dialect::Postgres => format!("({}::jsonb) <@ ({}::jsonb)", left_json, right),
            // MySQL: subset(a,b) эквивалент JSON_CONTAINS(b, a)
            Dialect::MySQL => format!("JSON_CONTAINS({}, {})", right, quote_sql_str(left_json)),
            Dialect::SQLite => sqlite_json_subset_sql(left_json, right),
            _ => format!("JSON_CONTAINS({}, {})", right, quote_sql_str(left_json)),
        };
        match parse_where_expr(&sql) {
            Ok(expr) => self.attach_where_with_and(expr, smallvec![]),
            Err(e) => self.push_builder_error(format!("where_json_subset_of(): {}", e)),
        }
        self
    }
}
