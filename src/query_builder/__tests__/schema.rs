use super::dialect_test_helpers::{col_list, qi, qn};
use crate::query_builder::QueryBuilder;

#[test]
fn from_with_explicit_schema_overrides_default() {
    let (sql, _params) = QueryBuilder::new_empty()
        .with_default_schema(Some("def".into()))
        .schema("forced")
        .select(("id",))
        .from(("users",))
        .to_sql()
        .expect("to_sql");

    assert!(
        sql.contains(&format!(
            "SELECT {} FROM {}",
            qi("id"),
            qn(&["forced", "users"])
        )),
        "got: {sql}"
    );
}

#[test]
fn from_uses_default_schema_if_no_explicit_set() {
    let (sql, _params) = QueryBuilder::new_empty()
        .with_default_schema(Some("myschema".into()))
        .select(("id",))
        .from(("users",))
        .to_sql()
        .expect("to_sql");

    assert!(
        sql.contains(&format!(
            "SELECT {} FROM {}",
            qi("id"),
            qn(&["myschema", "users"])
        )),
        "got: {sql}"
    );
}

#[test]
fn from_without_schema_or_default_uses_plain_table() {
    let (sql, _params) = QueryBuilder::new_empty()
        .select(("id",))
        .from(("users",))
        .to_sql()
        .expect("to_sql");

    assert!(
        sql.contains(&format!("SELECT {} FROM {}", qi("id"), qi("users"))),
        "got: {sql}"
    )
}
