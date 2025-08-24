use super::super::*;
use super::extract_where;
use crate::expression::helpers::{col, val};
use sqlparser::ast::{BinaryOperator as BO, Expr as SqlExpr};

type QB = QueryBuilder<'static, ()>;

#[test]
fn where_between_and_not_between() {
    let qb1 = QB::new_empty()
        .from("t")
        .select("*")
        .where_between(col("age"), val(18), val(30));
    let (q1, _) = qb1.build_query_ast().expect("ok");
    let w1 = extract_where(&q1).unwrap();
    if let SqlExpr::Between { negated, .. } = w1 {
        assert!(!negated);
    } else {
        panic!("expected Between");
    }

    let qb2 = QB::new_empty()
        .from("t")
        .select("*")
        .where_not_between(col("age"), val(18), val(30));
    let (q2, _) = qb2.build_query_ast().expect("ok");
    let w2 = extract_where(&q2).unwrap();
    if let SqlExpr::Between { negated, .. } = w2 {
        assert!(*negated);
    } else {
        panic!("expected Between");
    }
}

#[test]
fn or_where_between_appends_with_or() {
    // (a = 1) OR (age BETWEEN 18 AND 30)
    let qb = QB::new_empty()
        .from("t")
        .select("*")
        .where_(col("a").eq(val(1)))
        .or_where_between(col("age"), val(18), val(30));

    let (q, _params) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).unwrap();

    match w {
        SqlExpr::BinaryOp { op, left, right } => {
            assert!(matches!(op, BO::Or), "top-level must be OR, got {:?}", op);
            assert!(matches!(
                left.as_ref(),
                SqlExpr::BinaryOp { op: BO::Eq, .. }
            ));
            match right.as_ref() {
                SqlExpr::Between { negated, .. } => assert!(!negated),
                other => panic!("right side must be BETWEEN, got {:?}", other),
            }
        }
        other => panic!("expected BinaryOp OR, got {:?}", other),
    }
}

#[test]
fn or_where_not_between_appends_with_or_and_negated() {
    // (a = 1) OR (age NOT BETWEEN 18 AND 30)
    let qb = QB::new_empty()
        .from("t")
        .select("*")
        .where_(col("a").eq(val(1)))
        .or_where_not_between(col("age"), val(18), val(30));

    let (q, _params) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).unwrap();

    match w {
        SqlExpr::BinaryOp { op, left, right } => {
            assert!(matches!(op, BO::Or));
            assert!(matches!(
                left.as_ref(),
                SqlExpr::BinaryOp { op: BO::Eq, .. }
            ));
            match right.as_ref() {
                SqlExpr::Between { negated, .. } => assert!(*negated),
                other => panic!("right side must be NOT BETWEEN, got {:?}", other),
            }
        }
        other => panic!("expected BinaryOp OR, got {:?}", other),
    }
}

#[test]
fn where_between_collects_params_for_bounds() {
    // Проверяем, что параметры от low/high попадают в билдер
    let qb = QB::new_empty()
        .from("t")
        .select("*")
        .where_between(col("age"), val(18), val(30));

    let (_q, params) = qb.build_query_ast().expect("ok");
    // как минимум 2 placeholder'а от 18 и 30
    assert!(
        params.len() >= 2,
        "expected at least two params gathered from bounds, got {}",
        params.len()
    );
}
