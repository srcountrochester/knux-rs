use crate::expression::helpers::val;
use crate::query_builder::QueryBuilder;
use crate::tests::dialect_test_helpers::{col_list, qi, qn};

/// Грубая нормализация пробелов: схлопываем последовательности в один пробел,
/// убираем ведущие/замыкающие пробелы. Помогает сделать проверки более стабильными.
fn norm(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_space = false;
    for ch in s.chars() {
        if ch.is_whitespace() {
            if !prev_space {
                out.push(' ');
                prev_space = true;
            }
        } else {
            out.push(ch);
            prev_space = false;
        }
    }
    out.trim().to_string()
}

#[test]
fn simple_select_from_table() {
    let (sql, params) = QueryBuilder::new_empty()
        .select(&["id", "name"])
        .from("users")
        .to_sql()
        .unwrap();

    assert!(
        sql.contains(&format!(
            "SELECT {} FROM {}",
            col_list(&["id", "name"]),
            qi("users")
        )),
        "got: {sql}"
    );
    assert!(params.is_empty());
}

#[test]
fn select_from_table_with_default_schema() {
    let qb = QueryBuilder::new_empty()
        .with_default_schema(Some("public".into()))
        .select(("id", "name"))
        .from("users");

    let (sql, params) = qb.to_sql().expect("to_sql");
    assert!(
        sql.contains(&format!(
            "SELECT {} FROM {}",
            col_list(&["id", "name"]),
            qn(&["public", "users"])
        )) || sql.contains(&format!("FROM {}", qn(&["public", "users"]))),
        "got: {sql}"
    );
    assert!(params.is_empty());
}

#[test]
fn select_from_qualified_table() {
    let (sql, _) = QueryBuilder::new_empty()
        .select(("id",))
        .from("app.users")
        .to_sql()
        .unwrap();
    assert!(
        sql.contains(&format!("FROM {}", qn(&["app", "users"]))),
        "got: {sql}"
    );
}

#[test]
fn select_from_subquery_and_collect_params() {
    // subquery: SELECT ?
    let sub = QueryBuilder::new_empty().select((val(10i32),));
    // closure-subquery: SELECT ?
    let qb = QueryBuilder::new_empty()
        .select(("x",))
        .from((sub, |q: QueryBuilder| q.select((val(20i32),))));

    let (sql, params) = qb.to_sql().unwrap();
    assert!(sql.contains("FROM (SELECT"));
    assert_eq!(params.len(), 2);
}

#[test]
fn to_sql_from_multiple_plain_tables_with_default_schema() {
    let (sql, params) = QueryBuilder::new_empty()
        .with_default_schema(Some("app".into()))
        .select(("id",))
        .from(("users", "auth.roles", "logs"))
        .to_sql()
        .expect("to_sql");

    assert!(params.is_empty(), "no params expected");

    let sql = norm(&sql);

    // Проверяем, что все источники присутствуют и в правильном порядке:
    // app.users, auth.roles, app.logs
    let from_users = format!("FROM {}", qn(&["app", "users"]));
    let i_users = sql
        .find(&from_users)
        .expect(&format!(r#"{} not found in: {}"#, from_users, sql));

    let roles_pat = qn(&["auth", "roles"]);
    let i_roles = sql
        .find(&roles_pat)
        .expect(&format!(r#""{}" not found in: {}"#, roles_pat, sql));

    let logs_pat = qn(&["app", "logs"]);
    let i_logs = sql
        .find(&logs_pat)
        .expect(&format!(r#""{}" not found in: {}"#, logs_pat, sql));

    assert!(
        i_users < i_roles && i_roles < i_logs,
        "sources must keep order: users, roles, logs; got: {sql}"
    );

    assert!(
        sql.contains(&format!("FROM {}", qn(&["app", "users"]))),
        r#"FROM {} not found in: {}"#,
        qn(&["app", "users"]),
        sql
    );
}

#[test]
fn to_sql_from_mixed_table_subquery_and_closure() {
    // subquery: SELECT ?
    let sub = QueryBuilder::new_empty().select((val(10i32),));
    // closure-subquery: SELECT ?
    let (sql, params) = QueryBuilder::new_empty()
        .select(("x",))
        .from(("users", sub, |q: QueryBuilder| q.select((val(20i32),))))
        .to_sql()
        .expect("to_sql");

    // Два параметра: 10 и 20 — в таком порядке.
    assert_eq!(
        params.len(),
        2,
        "params from both subqueries must be collected"
    );
    assert!(matches!(params[0], crate::param::Param::I32(10)));
    assert!(matches!(params[1], crate::param::Param::I32(20)));

    let sql = norm(&sql);

    // Должно быть: сначала users, затем два подзапроса "(SELECT ..."
    let from_users = format!("FROM {}", qi("users"));
    let i_users = sql
        .find(&from_users)
        .expect(&format!(r#"{} not found in: {}"#, from_users, sql));

    // Найдём два вхождения "(SELECT"
    let mut idx = 0usize;
    let mut hits = Vec::new();
    while let Some(pos) = sql[idx..].find("(SELECT") {
        let p = idx + pos;
        hits.push(p);
        idx = p + 7; // длина "(SELECT"
    }
    assert_eq!(
        hits.len(),
        2,
        "expected two subqueries in FROM, got {} in: {sql}",
        hits.len()
    );

    assert!(
        i_users < hits[0] && hits[0] < hits[1],
        "expected order: users, subquery1, subquery2; got: {sql}"
    );

    // Убедимся, что SELECT-часть тоже адекватна
    assert!(
        sql.contains(&format!("FROM {}", qi("users"))),
        r#"FROM {} not found in: {}"#,
        qi("users"),
        sql
    );
}
