use super::extract_where;
use crate::expression::helpers::{col, val};
use crate::query_builder::QueryBuilder;
use sqlparser::ast::{BinaryOperator as BO, Expr as SqlExpr};

#[test]
fn where_null_basic() {
    let qb = QueryBuilder::new_empty()
        .from("users")
        .select("*")
        .where_null(col("deleted_at"));

    let (q, _params) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).expect("where present");
    assert!(matches!(w, SqlExpr::IsNull(_)));
}

#[test]
fn where_not_null_basic() {
    let qb = QueryBuilder::new_empty()
        .from("users")
        .select("*")
        .where_not_null(col("deleted_at"));

    let (q, _params) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).expect("where present");
    assert!(matches!(w, SqlExpr::IsNotNull(_)));
}

#[test]
fn or_where_null_appends_with_or() {
    // (age > 18) OR deleted_at IS NULL
    let qb = QueryBuilder::new_empty()
        .from("users")
        .select("*")
        .where_(col("age").gt(val(18)))
        .or_where_null(col("deleted_at"));

    let (q, _params) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).expect("where present");

    match w {
        SqlExpr::BinaryOp { op, left, right } => {
            assert!(matches!(op, BO::Or), "top must be OR, got {:?}", op);
            assert!(matches!(
                left.as_ref(),
                SqlExpr::BinaryOp { op: BO::Gt, .. }
            ));
            assert!(matches!(right.as_ref(), SqlExpr::IsNull(_)));
        }
        other => panic!("expected BinaryOp OR, got {:?}", other),
    }
}

#[test]
fn or_where_not_null_appends_with_or() {
    // (age > 18) OR deleted_at IS NOT NULL
    let qb = QueryBuilder::new_empty()
        .from("users")
        .select("*")
        .where_(col("age").gt(val(18)))
        .or_where_not_null(col("deleted_at"));

    let (q, _params) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).expect("where present");

    match w {
        SqlExpr::BinaryOp { op, left, right } => {
            assert!(matches!(op, BO::Or));
            assert!(matches!(
                left.as_ref(),
                SqlExpr::BinaryOp { op: BO::Gt, .. }
            ));
            assert!(matches!(right.as_ref(), SqlExpr::IsNotNull(_)));
        }
        other => panic!("expected BinaryOp OR, got {:?}", other),
    }
}

#[test]
fn where_null_accepts_str_column_via_into_qbarg() {
    let qb = QueryBuilder::new_empty()
        .from("users")
        .select("*")
        .where_null("deleted_at"); // &str → IntoQBArg → col("deleted_at")

    let (q, _params) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).expect("where present");
    assert!(matches!(w, SqlExpr::IsNull(_)));
}
