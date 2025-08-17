use crate::expression::helpers::{col, val};
use crate::query_builder::QueryBuilder;
use crate::renderer::Dialect;

fn norm(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[test]
fn render_with_basic_cte_pg() {
    let sql = QueryBuilder::new_empty()
        .with("t", |q: QueryBuilder| q.from("users").select((col("id"),)))
        .from("t")
        .select(("*",))
        .dialect(Dialect::Postgres)
        .to_sql()
        .expect("to_sql")
        .0;

    let s = norm(&sql).to_uppercase();
    assert!(s.starts_with("WITH "), "expected WITH..., got: {s}");
    assert!(s.contains("WITH \"T\" AS ("), "cte header missing: {s}");
    assert!(s.contains("FROM \"T\""), "usage of CTE missing: {s}");
}

#[test]
fn render_with_recursive_pg() {
    let sql = QueryBuilder::new_empty()
        .with_recursive("r", |q: QueryBuilder| q.select((val(1i32),)))
        .from("r")
        .select((col("x"),))
        .dialect(Dialect::Postgres)
        .to_sql()
        .expect("to_sql")
        .0;

    let s = norm(&sql).to_uppercase();
    assert!(
        s.starts_with("WITH RECURSIVE "),
        "expected WITH RECURSIVE..., got: {s}"
    );
}

#[test]
fn render_with_as_materialized_pg() {
    let sql = QueryBuilder::new_empty()
        .with_materialized("m", |q: QueryBuilder| q.select((val(1i32),)))
        .from("m")
        .select(("*",))
        .dialect(Dialect::Postgres)
        .to_sql()
        .expect("to_sql")
        .0;

    let s = norm(&sql).to_uppercase();
    assert!(
        s.contains("WITH \"M\" AS MATERIALIZED ("),
        "expected AS MATERIALIZED, got: {s}"
    );
}

#[test]
fn render_with_as_not_materialized_pg() {
    let sql = QueryBuilder::new_empty()
        .with_not_materialized("n", |q: QueryBuilder| q.select((val(1i32),)))
        .from("n")
        .select(("*",))
        .dialect(Dialect::Postgres)
        .to_sql()
        .expect("to_sql")
        .0;

    let s = norm(&sql).to_uppercase();
    assert!(
        s.contains("WITH \"N\" AS NOT MATERIALIZED ("),
        "expected AS NOT MATERIALIZED, got: {s}"
    );
}

#[test]
fn render_with_from_in_header_pg() {
    let sql = QueryBuilder::new_empty()
        .with_from("t", "base", |q: QueryBuilder| q.select((col("id"),)))
        .from("t")
        .select((col("id"),))
        .dialect(Dialect::Postgres)
        .to_sql()
        .expect("to_sql")
        .0;

    let s = norm(&sql).to_uppercase();
    assert!(
        s.contains("WITH \"T\" FROM \"BASE\" AS ("),
        "expected FROM <ident> in CTE header: {s}"
    );
}

#[test]
fn render_multiple_ctes_pg() {
    let sql = QueryBuilder::new_empty()
        .with("a", |q: QueryBuilder| q.select((val(1i32),)))
        .with_not_materialized("b", |q: QueryBuilder| q.select((val(2i32),)))
        .from("a")
        .select((col("x"),))
        .dialect(Dialect::Postgres)
        .to_sql()
        .expect("to_sql")
        .0;

    let s = norm(&sql).to_uppercase();
    assert!(s.starts_with("WITH "), "expected WITH..., got: {s}");
    // запятая между CTE
    assert!(
        s.contains("\"A\" AS (") && s.contains(", \"B\" AS NOT MATERIALIZED ("),
        "expected two CTEs separated by comma: {s}"
    );
}

#[test]
fn materialized_keywords_are_omitted_for_sqlite_and_mysql() {
    // SQLite
    let sql_sqlite = QueryBuilder::new_empty()
        .with_materialized("m", |q: QueryBuilder| q.select((val(1i32),)))
        .from("m")
        .select(("*",))
        .dialect(Dialect::SQLite)
        .to_sql()
        .expect("to_sql")
        .0;

    // MySQL
    let sql_mysql = QueryBuilder::new_empty()
        .with_not_materialized("n", |q: QueryBuilder| q.select((val(1i32),)))
        .from("n")
        .select(("*",))
        .dialect(Dialect::MySQL)
        .to_sql()
        .expect("to_sql")
        .0;

    let s1 = norm(&sql_sqlite).to_uppercase();
    let s2 = norm(&sql_mysql).to_uppercase();

    assert!(
        s1.starts_with("WITH "),
        "sqlite: expected WITH..., got: {s1}"
    );
    assert!(
        !s1.contains("MATERIALIZED"),
        "sqlite: MATERIALIZED must be omitted: {s1}"
    );

    assert!(
        s2.starts_with("WITH "),
        "mysql: expected WITH..., got: {s2}"
    );
    assert!(
        !s2.contains("MATERIALIZED"),
        "mysql: MATERIALIZED must be omitted: {s2}"
    );
}
