use super::extract_where;
use crate::expression::Expression;
use crate::expression::helpers::{col, val};
use crate::query_builder::QueryBuilder;
use crate::type_helpers::QBClosureHelper;
use sqlparser::ast::{BinaryOperator as BO, Expr as SqlExpr};

type QB = QueryBuilder<'static, ()>;
#[test]
fn where_in_with_list() {
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .where_in(col("id"), (val(1), val(2), val(3)));
    let (q, _) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).unwrap();
    assert!(matches!(w, SqlExpr::InList { .. }));
}

#[test]
fn where_in_with_subquery() {
    let sub = QB::new_empty().from("orders").select("user_id");
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .where_in(col("id"), sub);
    let (q, _) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).unwrap();
    assert!(matches!(w, SqlExpr::InSubquery { .. }));
}

#[test]
fn or_where_in_appends_with_or() {
    // (a = 1) OR id IN (1,2)
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .where_(col("a").eq(val(1)))
        .or_where_in(col("id"), (val(1), val(2)));

    let (q, _) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).unwrap();

    match w {
        SqlExpr::BinaryOp { op, left, right } => {
            assert!(matches!(op, BO::Or), "top-level must be OR, got {:?}", op);
            assert!(matches!(
                left.as_ref(),
                SqlExpr::BinaryOp { op: BO::Eq, .. }
            ));
            assert!(matches!(
                right.as_ref(),
                SqlExpr::InList { negated: false, .. }
            ));
        }
        other => panic!("expected BinaryOp OR, got {:?}", other),
    }
}

#[test]
fn where_not_in_sets_negated() {
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .where_not_in(col("id"), (val(1), val(2), val(3)));

    let (q, _) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).unwrap();
    match w {
        SqlExpr::InList { negated, .. } => assert!(*negated, "expected negated=true"),
        other => panic!("expected InList, got {:?}", other),
    }
}

#[test]
fn or_where_not_in_sets_negated_and_or() {
    // (a = 1) OR id NOT IN (1,2,3)
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .where_(col("a").eq(val(1)))
        .or_where_not_in(col("id"), (val(1), val(2), val(3)));

    let (q, _) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).unwrap();
    match w {
        SqlExpr::BinaryOp { op, left, right } => {
            assert!(matches!(op, BO::Or));
            assert!(matches!(
                left.as_ref(),
                SqlExpr::BinaryOp { op: BO::Eq, .. }
            ));
            match right.as_ref() {
                SqlExpr::InList { negated, .. } => assert!(*negated),
                other => panic!("right must be InList(negated), got {:?}", other),
            }
        }
        other => panic!("expected BinaryOp OR, got {:?}", other),
    }
}

#[test]
fn where_in_accepts_closure_subquery() {
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .where_in::<Expression, QBClosureHelper<()>>(col("id"), |qb| {
            qb.from("orders").select("user_id")
        });

    let (q, _) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).unwrap();
    assert!(matches!(w, SqlExpr::InSubquery { negated: false, .. }));
}

#[test]
fn where_in_accepts_str_column_via_into_qbarg() {
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .where_in("id", (val(10), val(20)));

    let (q, _) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).unwrap();
    assert!(
        matches!(w, SqlExpr::InList { .. }),
        "expected InList for &str column"
    );
}
