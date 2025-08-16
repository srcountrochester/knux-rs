use crate::expression::helpers::{schema, table};
use sqlparser::ast::Expr as SqlExpr;

fn parts(expr: &SqlExpr) -> Vec<String> {
    match expr {
        SqlExpr::Identifier(id) => vec![id.value.clone()],
        SqlExpr::CompoundIdentifier(v) => v.iter().map(|i| i.value.clone()).collect(),
        other => panic!("unexpected expr in path tests: {:?}", other),
    }
}

#[test]
fn schema_then_table_equals_table_with_schema() {
    let a = schema("public").table("users");
    let b = table("users").schema("public");
    assert_eq!(parts(&a.expr), vec!["public", "users"]);
    assert_eq!(parts(&b.expr), vec!["public", "users"]);
}

#[test]
fn table_then_col_appends_column() {
    let e = table("public.users").col("id");
    assert_eq!(parts(&e.expr), vec!["public", "users", "id"]);
}

#[test]
fn schema_table_col_full_chain() {
    let e = schema("auth").table("profiles").col("email");
    assert_eq!(parts(&e.expr), vec!["auth", "profiles", "email"]);
}

#[test]
fn col_splits_dot_path() {
    // проверяем, что .col("profile.email") корректно разбивает на два сегмента
    let e = table("auth.users").col("profile.email");
    assert_eq!(parts(&e.expr), vec!["auth", "users", "profile", "email"]);
}
