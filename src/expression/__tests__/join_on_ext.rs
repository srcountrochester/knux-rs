use crate::expression::JoinOnExt; // важный use — это extension-trait
use crate::expression::helpers::{col, val};
use crate::param::Param;
use crate::query_builder::QueryBuilder;
use sqlparser::ast::{
    BinaryOperator as BO, Expr as SqlExpr, JoinConstraint, JoinOperator, Query, SetExpr,
};

type QB = QueryBuilder<'static, ()>;

// --- маленький хелпер: достать ON-выражение первого JOIN (возвращаем клонированный Expr) ---
fn first_join_on_expr(q: &Query) -> SqlExpr {
    let body = &q.body;
    let SetExpr::Select(sel) = &**body else {
        panic!("expected SELECT");
    };
    assert!(!sel.from.is_empty(), "FROM must not be empty");
    let twj = &sel.from[0];
    assert!(!twj.joins.is_empty(), "expected at least one JOIN");
    let j = &twj.joins[0];
    let c = match &j.join_operator {
        JoinOperator::Inner(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c) => c,
        JoinOperator::CrossJoin => panic!("CROSS JOIN has no ON"),
        _ => panic!("unexpected join operator"),
    };
    match c {
        JoinConstraint::On(e) => e.clone(),
        _ => panic!("expected ON constraint"),
    }
}

#[test]
fn on_in_builds_and_inlist_and_collects_params() {
    let qb = QB::new_empty().from("users").select("*").join(
        "accounts",
        col("users.id")
            .eq(col("accounts.user_id"))
            .on_in(col("users.role_id"), [val(1), val(2), val(3)]),
    );
    let (q, params) = qb.build_query_ast().expect("build ok");
    // форма: (eq AND (role_id IN (...)))
    let on = first_join_on_expr(&q);
    match on {
        SqlExpr::BinaryOp { op, right, .. } => {
            assert!(matches!(op, BO::And), "top must be AND");
            match *right {
                SqlExpr::InList { negated, .. } => assert!(!negated, "expected IN (not negated)"),
                other => panic!("expected InList, got {:?}", other),
            }
        }
        other => panic!("expected BinaryOp AND, got {:?}", other),
    }
    // параметры: 1,2,3
    assert_eq!(params.len(), 3);
    assert!(matches!(params[0], Param::I32(1)));
    assert!(matches!(params[1], Param::I32(2)));
    assert!(matches!(params[2], Param::I32(3)));
}

#[test]
fn or_on_in_builds_or() {
    let qb = QB::new_empty().from("u").select("*").join(
        "a",
        col("u.id")
            .eq(col("a.uid"))
            .or_on_in(col("u.role_id"), [val(10), val(20)]),
    );
    let (q, _p) = qb.build_query_ast().expect("build ok");
    let on = first_join_on_expr(&q);
    match on {
        SqlExpr::BinaryOp { op, right, .. } => {
            assert!(matches!(op, BO::Or), "top must be OR");
            match *right {
                SqlExpr::InList { .. } => {}
                other => panic!("expected InList, got {:?}", other),
            }
        }
        other => panic!("expected BinaryOp OR, got {:?}", other),
    }
}

#[test]
fn on_not_in_builds_negated_inlist() {
    let qb = QB::new_empty().from("u").select("*").join(
        "a",
        col("u.id")
            .eq(col("a.uid"))
            .on_not_in(col("u.role_id"), [val(1), val(2)]),
    );
    let (q, _p) = qb.build_query_ast().expect("build ok");
    let on = first_join_on_expr(&q);
    match on {
        SqlExpr::BinaryOp { op, right, .. } => {
            assert!(matches!(op, BO::And));
            match *right {
                SqlExpr::InList { negated, .. } => assert!(negated, "expected NOT IN"),
                other => panic!("expected InList, got {:?}", other),
            }
        }
        other => panic!("expected BinaryOp, got {:?}", other),
    }
}

#[test]
fn on_null_and_or_on_null() {
    // AND IsNull
    let qb1 = QB::new_empty().from("u").select("*").join(
        "a",
        col("u.id").eq(col("a.uid")).on_null(col("a.deleted_at")),
    );
    let (q1, _p1) = qb1.build_query_ast().expect("ok");
    let on1 = first_join_on_expr(&q1);
    match on1 {
        SqlExpr::BinaryOp { op, right, .. } => {
            assert!(matches!(op, BO::And));
            assert!(matches!(*right, SqlExpr::IsNull(_)));
        }
        other => panic!("expected AND IsNull, got {:?}", other),
    }

    // OR IsNull
    let qb2 = QB::new_empty().from("u").select("*").join(
        "a",
        col("u.id").eq(col("a.uid")).or_on_null(col("a.deleted_at")),
    );
    let (q2, _p2) = qb2.build_query_ast().expect("ok");
    let on2 = first_join_on_expr(&q2);
    match on2 {
        SqlExpr::BinaryOp { op, right, .. } => {
            assert!(matches!(op, BO::Or));
            assert!(matches!(*right, SqlExpr::IsNull(_)));
        }
        other => panic!("expected OR IsNull, got {:?}", other),
    }
}

#[test]
fn on_not_null_and_or_on_not_null() {
    // AND IsNotNull
    let qb1 = QB::new_empty().from("u").select("*").join(
        "a",
        col("u.id")
            .eq(col("a.uid"))
            .on_not_null(col("a.deleted_at")),
    );
    let (q1, _p1) = qb1.build_query_ast().expect("ok");
    let on1 = first_join_on_expr(&q1);
    match on1 {
        SqlExpr::BinaryOp { op, right, .. } => {
            assert!(matches!(op, BO::And));
            assert!(matches!(*right, SqlExpr::IsNotNull(_)));
        }
        other => panic!("expected AND IsNotNull, got {:?}", other),
    }

    // OR IsNotNull
    let qb2 = QB::new_empty().from("u").select("*").join(
        "a",
        col("u.id")
            .eq(col("a.uid"))
            .or_on_not_null(col("a.deleted_at")),
    );
    let (q2, _p2) = qb2.build_query_ast().expect("ok");
    let on2 = first_join_on_expr(&q2);
    match on2 {
        SqlExpr::BinaryOp { op, right, .. } => {
            assert!(matches!(op, BO::Or));
            assert!(matches!(*right, SqlExpr::IsNotNull(_)));
        }
        other => panic!("expected OR IsNotNull, got {:?}", other),
    }
}

#[test]
fn on_between_and_or_on_between() {
    // AND BETWEEN 18..30
    let qb1 = QB::new_empty().from("u").select("*").join(
        "a",
        col("u.id")
            .eq(col("a.uid"))
            .on_between(col("u.age"), val(18), val(30)),
    );
    let (q1, params1) = qb1.build_query_ast().expect("ok");
    let on1 = first_join_on_expr(&q1);
    match on1 {
        SqlExpr::BinaryOp { op, right, .. } => {
            assert!(matches!(op, BO::And));
            match *right {
                SqlExpr::Between { negated, .. } => assert!(!negated),
                other => panic!("expected Between, got {:?}", other),
            }
        }
        other => panic!("expected AND Between, got {:?}", other),
    }
    assert_eq!(params1.len(), 2);
    assert!(matches!(params1[0], Param::I32(18)));
    assert!(matches!(params1[1], Param::I32(30)));

    // OR BETWEEN 18..30
    let qb2 = QB::new_empty().from("u").select("*").join(
        "a",
        col("u.id")
            .eq(col("a.uid"))
            .or_on_between(col("u.age"), val(18), val(30)),
    );
    let (q2, _p2) = qb2.build_query_ast().expect("ok");
    let on2 = first_join_on_expr(&q2);
    match on2 {
        SqlExpr::BinaryOp { op, right, .. } => {
            assert!(matches!(op, BO::Or));
            assert!(matches!(*right, SqlExpr::Between { .. }));
        }
        other => panic!("expected OR Between, got {:?}", other),
    }
}

#[test]
fn on_not_between_and_or_on_not_between() {
    // AND NOT BETWEEN
    let qb1 = QB::new_empty().from("u").select("*").join(
        "a",
        col("u.id")
            .eq(col("a.uid"))
            .on_not_between(col("u.age"), val(1), val(10)),
    );
    let (q1, _p1) = qb1.build_query_ast().expect("ok");
    let on1 = first_join_on_expr(&q1);
    match on1 {
        SqlExpr::BinaryOp { op, right, .. } => {
            assert!(matches!(op, BO::And));
            match *right {
                SqlExpr::Between { negated, .. } => assert!(negated),
                other => panic!("expected Between(negated), got {:?}", other),
            }
        }
        other => panic!("expected AND NotBetween, got {:?}", other),
    }

    // OR NOT BETWEEN
    let qb2 = QB::new_empty().from("u").select("*").join(
        "a",
        col("u.id")
            .eq(col("a.uid"))
            .or_on_not_between(col("u.age"), val(1), val(10)),
    );
    let (q2, _p2) = qb2.build_query_ast().expect("ok");
    let on2 = first_join_on_expr(&q2);
    match on2 {
        SqlExpr::BinaryOp { op, right, .. } => {
            assert!(matches!(op, BO::Or));
            match *right {
                SqlExpr::Between { negated, .. } => assert!(negated),
                other => panic!("expected Between(negated), got {:?}", other),
            }
        }
        other => panic!("expected OR NotBetween, got {:?}", other),
    }
}

#[test]
fn on_exists_and_on_not_exists_collect_params() {
    // Подзапрос с параметром: SELECT * FROM orders WHERE amount > 100
    let sub = {
        let inner = QB::new_empty()
            .from("orders")
            .select("*")
            .where_(col("amount").gt(val(100)));
        let (q, p) = inner.build_query_ast().expect("sub ok");
        // оборачиваем в Expression::Subquery вручную (для on_exists)
        crate::expression::Expression {
            expr: SqlExpr::Subquery(Box::new(q)),
            alias: None,
            params: p.into(),
            mark_distinct_for_next: false,
        }
    };

    // AND EXISTS(sub)
    let qb1 = QB::new_empty()
        .from("u")
        .select("*")
        .join("a", col("u.id").eq(col("a.uid")).on_exists(sub.clone()));
    let (q1, p1) = qb1.build_query_ast().expect("ok");
    assert_eq!(p1.len(), 1, "subquery param must be collected");
    assert!(matches!(p1[0], Param::I32(100)));
    let on1 = first_join_on_expr(&q1);
    match on1 {
        SqlExpr::BinaryOp { op, right, .. } => {
            assert!(matches!(op, BO::And));
            match *right {
                SqlExpr::Exists { negated, .. } => assert!(!negated),
                other => panic!("expected EXISTS, got {:?}", other),
            }
        }
        other => panic!("expected AND EXISTS, got {:?}", other),
    }

    // AND NOT EXISTS(sub)
    let qb2 = QB::new_empty()
        .from("u")
        .select("*")
        .join("a", col("u.id").eq(col("a.uid")).on_not_exists(sub));
    let (q2, p2) = qb2.build_query_ast().expect("ok");
    assert_eq!(p2.len(), 1);
    assert!(matches!(p2[0], Param::I32(100)));
    let on2 = first_join_on_expr(&q2);
    match on2 {
        SqlExpr::BinaryOp { op, right, .. } => {
            assert!(matches!(op, BO::And));
            match *right {
                SqlExpr::Exists { negated, .. } => assert!(negated),
                other => panic!("expected NOT EXISTS, got {:?}", other),
            }
        }
        other => panic!("expected AND NOT EXISTS, got {:?}", other),
    }
}
