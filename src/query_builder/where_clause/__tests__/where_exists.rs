use super::extract_where;
use crate::expression::helpers::{col, val};
use crate::query_builder::QueryBuilder;
use sqlparser::ast::{BinaryOperator as BO, Expr as SqlExpr};

#[test]
fn where_exists_and_not_exists() {
    let sub = |qb: QueryBuilder| {
        qb.from("orders")
            .select("*")
            .where_(col("amount").gt(val(100)))
    };

    // EXISTS
    let qb1 = QueryBuilder::new_empty()
        .from("users")
        .select("*")
        .where_exists(sub);
    let (q1, _) = qb1.build_query_ast().expect("ok");
    assert!(
        matches!(
            extract_where(&q1).unwrap(),
            SqlExpr::Exists { negated: false, .. }
        ),
        "expected EXISTS (negated=false)"
    );

    // NOT EXISTS
    let qb2 = QueryBuilder::new_empty()
        .from("users")
        .select("*")
        .where_not_exists(sub);
    let (q2, _) = qb2.build_query_ast().expect("ok");
    assert!(
        matches!(
            extract_where(&q2).unwrap(),
            SqlExpr::Exists { negated: true, .. }
        ),
        "expected NOT EXISTS (negated=true)"
    );
}

#[test]
fn or_where_exists_appends_with_or() {
    // (a = 1) OR EXISTS (sub)
    let qb = QueryBuilder::new_empty()
        .from("users")
        .select("*")
        .where_(col("a").eq(val(1)))
        .or_where_exists(|qb: QueryBuilder| qb.from("orders").select("*"));

    let (q, _params) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).unwrap();

    match w {
        SqlExpr::BinaryOp { op, left, right } => {
            assert!(matches!(op, BO::Or), "top must be OR, got {:?}", op);
            assert!(matches!(
                left.as_ref(),
                SqlExpr::BinaryOp { op: BO::Eq, .. }
            ));
            assert!(matches!(
                right.as_ref(),
                SqlExpr::Exists { negated: false, .. }
            ));
        }
        other => panic!("expected BinaryOp OR, got {:?}", other),
    }
}

#[test]
fn or_where_not_exists_appends_with_or_and_negated() {
    // (a = 1) OR NOT EXISTS (sub)
    let qb = QueryBuilder::new_empty()
        .from("users")
        .select("*")
        .where_(col("a").eq(val(1)))
        .or_where_not_exists(|qb: QueryBuilder| qb.from("orders").select("*"));

    let (q, _params) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).unwrap();

    match w {
        SqlExpr::BinaryOp { op, left, right } => {
            assert!(matches!(op, BO::Or));
            assert!(matches!(
                left.as_ref(),
                SqlExpr::BinaryOp { op: BO::Eq, .. }
            ));
            assert!(matches!(
                right.as_ref(),
                SqlExpr::Exists { negated: true, .. }
            ));
        }
        other => panic!("expected BinaryOp OR, got {:?}", other),
    }
}

#[test]
fn where_exists_with_non_subquery_adds_builder_error() {
    // Передаём Expression вместо подзапроса → регистрируется ошибка билдера
    let qb = QueryBuilder::new_empty()
        .from("users")
        .select("*")
        .where_exists(col("x"));

    let err = qb.build_query_ast().unwrap_err();
    let s = err.to_string();
    assert!(
        s.contains("where_exists()") && (s.contains("подзапрос") || s.contains("subquery")),
        "unexpected error text: {s}"
    );
}

#[test]
fn where_exists_collects_params_from_subquery() {
    // В подзапросе есть параметр (amount > 100) — он должен попасть в общий список params
    let qb = QueryBuilder::new_empty()
        .from("users")
        .select("*")
        .where_exists(|qb: QueryBuilder| {
            qb.from("orders")
                .select("*")
                .where_(col("amount").gt(val(100)))
        });

    let (_q, params) = qb.build_query_ast().expect("ok");
    assert!(
        params.len() >= 1,
        "expected params from subquery to be collected, got {}",
        params.len()
    );
}
