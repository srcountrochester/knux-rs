use super::extract_where;
use crate::expression::helpers::{col, val};
use crate::query_builder::QueryBuilder;
use sqlparser::ast::{BinaryOperator as BO, Expr as SqlExpr};

#[test]
fn where_raw_parses_sql() {
    let qb = QueryBuilder::new_empty()
        .from("users")
        .select("*")
        .where_raw("users.id = 1 AND users.is_active");
    let (q, _) = qb.build_query_ast().expect("ok");
    // просто убеждаемся, что выражение есть
    assert!(extract_where(&q).is_some());
}

#[test]
fn or_where_raw_appends_with_or() {
    // (a = 1) OR (b > 2)
    let qb = QueryBuilder::new_empty()
        .from("users")
        .select("*")
        .where_(col("a").eq(val(1)))
        .or_where_raw("b > 2");

    let (q, _params) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).expect("where present");

    match w {
        SqlExpr::BinaryOp { op, left, right } => {
            assert!(matches!(op, BO::Or), "top-level must be OR, got {:?}", op);
            assert!(matches!(
                left.as_ref(),
                SqlExpr::BinaryOp { op: BO::Eq, .. }
            ));
            assert!(matches!(
                right.as_ref(),
                SqlExpr::BinaryOp { op: BO::Gt, .. }
            ));
        }
        other => panic!("expected BinaryOp OR, got {:?}", other),
    }
}

#[test]
fn where_raw_invalid_sql_records_builder_error() {
    // Заведомо невалидное where-выражение
    let qb = QueryBuilder::new_empty()
        .from("users")
        .select("*")
        .where_raw("a = 1 AND"); // парсер упадёт

    let err = qb.build_query_ast().unwrap_err();
    let s = err.to_string();
    assert!(
        s.contains("where_raw()"),
        "expected error to reference where_raw(), got: {s}"
    );
}

#[test]
fn or_where_raw_invalid_sql_records_builder_error() {
    let qb = QueryBuilder::new_empty()
        .from("users")
        .select("*")
        .where_(col("a").eq(val(1)))
        .or_where_raw("b ="); // парсер упадёт

    let err = qb.build_query_ast().unwrap_err();
    let s = err.to_string();
    assert!(
        s.contains("or_where_raw()"),
        "expected error to reference or_where_raw(), got: {s}"
    );
}

#[test]
fn multiple_where_raw_calls_chain_with_and() {
    // where_raw + where_raw → склейка AND
    let qb = QueryBuilder::new_empty()
        .from("users")
        .select("*")
        .where_raw("a = 1")
        .where_raw("b = 2");

    let (q, _params) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).expect("where present");
    match w {
        SqlExpr::BinaryOp { op, left, right } => {
            assert!(matches!(op, BO::And), "top-level must be AND");
            assert!(matches!(
                left.as_ref(),
                SqlExpr::BinaryOp { op: BO::Eq, .. }
            ));
            assert!(matches!(
                right.as_ref(),
                SqlExpr::BinaryOp { op: BO::Eq, .. }
            ));
        }
        other => panic!("expected BinaryOp AND, got {:?}", other),
    }
}
