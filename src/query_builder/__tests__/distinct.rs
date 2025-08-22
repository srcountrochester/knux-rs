use crate::expression::helpers::{col, val};
use crate::query_builder::QueryBuilder;
use crate::tests::dialect_test_helpers::qi;

/// Нормализатор для сравнения строк SQL (если у вас уже есть — используйте его)
fn norm(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[test]
fn distinct_without_columns_falls_back_to_star() {
    let (sql, params) = QueryBuilder::new_empty()
        .from("users")
        .distinct([] as [&str; 0]) // пустой список
        .to_sql()
        .expect("to_sql");

    assert!(params.is_empty());
    let sql = norm(&sql);
    assert!(
        sql.starts_with("SELECT DISTINCT"),
        "SQL must start with SELECT DISTINCT, got: {sql}"
    );
    assert!(sql.contains("FROM"), "expected FROM in SQL");
}

#[test]
fn distinct_with_columns_adds_projection_and_flag() {
    let (sql, params) = QueryBuilder::new_empty()
        .from("users")
        .distinct(("first_name", "last_name"))
        .to_sql()
        .expect("to_sql");

    assert!(params.is_empty());
    let sql = norm(&sql);
    assert!(
        sql.starts_with("SELECT DISTINCT"),
        "expected SELECT DISTINCT..., got: {sql}"
    );
    assert!(sql.contains(&qi("first_name")));
    assert!(sql.contains(&qi("last_name")));
    assert!(sql.contains(&format!("FROM {}", qi("users"))));
}

#[test]
fn distinct_collects_params_from_expressions() {
    // DISTINCT val(10), col("x")
    let (sql, params) = QueryBuilder::new_empty()
        .from("t")
        .distinct((val(10i32), col("x")))
        .to_sql()
        .expect("to_sql");

    assert_eq!(params.len(), 1, "param from val(10) must be collected");
    let sql = norm(&sql);
    assert!(
        sql.starts_with("SELECT DISTINCT"),
        "expected SELECT DISTINCT..., got: {sql}"
    );
}

#[test]
fn distinct_on_builds_distinct_on_clause() {
    let (sql, _params) = QueryBuilder::new_empty()
        .from("users")
        .select(("*",))
        .distinct_on((col("age"),))
        .to_sql()
        .expect("to_sql");

    let sql = norm(&sql).to_uppercase();

    if sql.contains("DISTINCT ON") {
        // строгая проверка для Postgres-ветки
        assert!(
            sql.starts_with("SELECT DISTINCT ON") && sql.contains("\"AGE\""),
            "expected DISTINCT ON (\"age\"), got: {sql}"
        );
    } else {
        // fallback для диалектов без DISTINCT ON
        assert!(
            sql.starts_with("SELECT DISTINCT"),
            "expected fallback SELECT DISTINCT..., got: {sql}"
        );
    }
}

#[test]
fn distinct_on_accepts_multiple_expressions_and_params() {
    // DISTINCT ON (users.age, users.country, (SELECT ?))
    let sub = QueryBuilder::new_empty().select((val(99i32),));
    let (sql, params) = QueryBuilder::new_empty()
        .from("users")
        .select(("*",))
        .distinct_on((col("users.age"), col("users.country"), sub))
        .to_sql()
        .expect("to_sql");

    // параметр из подзапроса должен собраться всегда
    assert_eq!(params.len(), 1, "subquery param must bubble up");
    let sql_u = norm(&sql).to_uppercase();

    if sql_u.contains("DISTINCT ON") {
        assert!(sql_u.contains("\"USERS\".\"AGE\""));
        assert!(sql_u.contains("\"USERS\".\"COUNTRY\""));
    } else {
        // В fallback-ветке `DISTINCT ON` не печатается — просто проверим, что есть DISTINCT
        assert!(
            sql_u.starts_with("SELECT DISTINCT"),
            "expected fallback SELECT DISTINCT..., got: {sql_u}"
        );
    }
}
