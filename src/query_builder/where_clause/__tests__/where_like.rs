use super::extract_where;
use crate::expression::helpers::{col, val};
use crate::query_builder::QueryBuilder;
use sqlparser::ast::{BinaryOperator as BO, Expr as SqlExpr};

type QB = QueryBuilder<'static, ()>;
#[test]
fn where_like_basic() {
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        // шаблон как параметр
        .where_like(col("name"), val("%A%"));

    let (q, params) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).expect("where present");
    assert!(matches!(
        w,
        SqlExpr::Like {
            negated: false,
            any: false,
            ..
        }
    ));
    // хотя бы 1 параметр от шаблона
    assert!(
        params.len() >= 1,
        "expected at least one param from pattern"
    );
}

#[test]
fn where_ilike_basic() {
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .where_ilike(col("name"), val("a_%"));

    let (q, params) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).expect("where present");
    assert!(matches!(
        w,
        SqlExpr::ILike {
            negated: false,
            any: false,
            ..
        }
    ));
    assert!(
        params.len() >= 1,
        "expected at least one param from pattern"
    );
}

#[test]
fn or_where_like_appends_with_or() {
    // (age > 18) OR name LIKE '%A%'
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .where_(col("age").gt(val(18)))
        .or_where_like(col("name"), val("%A%"));

    let (q, _params) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).expect("where present");

    match w {
        SqlExpr::BinaryOp { op, left, right } => {
            assert!(matches!(op, BO::Or), "top must be OR, got {:?}", op);
            assert!(matches!(
                left.as_ref(),
                SqlExpr::BinaryOp { op: BO::Gt, .. }
            ));
            assert!(matches!(
                right.as_ref(),
                SqlExpr::Like {
                    negated: false,
                    any: false,
                    ..
                }
            ));
        }
        other => panic!("expected BinaryOp OR, got {:?}", other),
    }
}

#[test]
fn or_where_ilike_appends_with_or() {
    // (age > 18) OR name ILIKE 'a_%'
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .where_(col("age").gt(val(18)))
        .or_where_ilike(col("name"), val("a_%"));

    let (q, _params) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).expect("where present");

    match w {
        SqlExpr::BinaryOp { op, left, right } => {
            assert!(matches!(op, BO::Or));
            assert!(matches!(
                left.as_ref(),
                SqlExpr::BinaryOp { op: BO::Gt, .. }
            ));
            assert!(matches!(
                right.as_ref(),
                SqlExpr::ILike {
                    negated: false,
                    any: false,
                    ..
                }
            ));
        }
        other => panic!("expected BinaryOp OR, got {:?}", other),
    }
}

#[test]
fn where_like_accepts_str_column() {
    // Левая часть как &str → через IntoQBArg станет col("name")
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .where_like("name", val("%B%"));

    let (q, _params) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).expect("where present");
    assert!(
        matches!(w, SqlExpr::Like { .. }),
        "expected Like for &str column"
    );
}
