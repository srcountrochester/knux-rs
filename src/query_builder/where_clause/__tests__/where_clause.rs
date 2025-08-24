use super::super::*;
use super::extract_where;
use crate::expression::helpers::{col, val};
use crate::type_helpers::QBClosureHelper;
use sqlparser::ast::{BinaryOperator as BO, Expr as SqlExpr, UnaryOperator};

type QB = QueryBuilder<'static, ()>;

#[test]
fn where_single_expression() {
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .where_(col("id").eq(val(1)));

    let (query, params) = qb.build_query_ast().expect("build ok");

    let w = extract_where(&query).expect("where present");
    match w {
        SqlExpr::BinaryOp { op, .. } => assert!(matches!(op, BO::Eq)),
        other => panic!("expected Eq, got {:?}", other),
    }
    // Должен быть хотя бы 1 параметр от val(1)
    assert!(params.len() >= 1);
}

#[test]
fn where_multiple_args_tuple_and_chaining() {
    // .where((a=1, b=2)) + повторный .where(c=3) → ((a=1 AND b=2) AND c=3)
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .where_((col("a").eq(val(1)), col("b").eq(val(2))))
        .where_(col("c").eq(val(3)));

    let (query, _params) = qb.build_query_ast().expect("build ok");
    let w = extract_where(&query).expect("where present");

    // верхний уровень — AND
    match w {
        SqlExpr::BinaryOp { op, left, right } => {
            assert!(matches!(op, BO::And));

            // левая часть — (a=1 AND b=2)
            if let SqlExpr::BinaryOp {
                op: l_op,
                left: l_l,
                right: l_r,
            } = left.as_ref()
            {
                assert!(matches!(l_op, BO::And));
                // проверим, что обе части — Eq(...)
                assert!(matches!(l_l.as_ref(), SqlExpr::BinaryOp { op: BO::Eq, .. }));
                assert!(matches!(l_r.as_ref(), SqlExpr::BinaryOp { op: BO::Eq, .. }));
            } else {
                panic!("left side must be AND");
            }

            // правая часть — c=3
            assert!(matches!(
                right.as_ref(),
                SqlExpr::BinaryOp { op: BO::Eq, .. }
            ));
        }
        other => panic!("expected AND at top, got {:?}", other),
    }
}

#[test]
fn where_accepts_subquery_and_closure() {
    // подзапрос как цель WHERE: получим Expr::Subquery
    let sub = QB::new_empty()
        .from("accounts")
        .select("*")
        .where_(col("user_id").eq(val(42)));

    let qb = QB::new_empty().from("users").select("*").where_(sub);

    let (query, _params) = qb.build_query_ast().expect("build ok");
    let w = extract_where(&query).expect("where present");
    assert!(matches!(w, SqlExpr::Subquery(_)));

    // closure-вариант
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .where_::<QBClosureHelper<()>>(|qb| {
            qb.from("accounts")
                .select("*")
                .where_(col("user_id").eq(val(7)))
        });

    let (query, _params) = qb.build_query_ast().expect("build ok");
    let w = extract_where(&query).expect("where present");
    assert!(matches!(w, SqlExpr::Subquery(_)));
}

#[test]
fn where_accepts_string_as_boolean_identifier_via_col() {
    // &str мапится в Expression через col("..."), это ок для boolean-колонок
    // Здесь просто проверим, что WHERE присутствует и это идентификатор.
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .where_("is_active"); // через IntoQBArg -> QBArg::Expr(col("is_active"))

    let (query, _params) = qb.build_query_ast().expect("build ok");
    let w = extract_where(&query).expect("where present");
    assert!(matches!(w, SqlExpr::Identifier(_)) || matches!(w, SqlExpr::CompoundIdentifier(_)));
}

#[test]
fn and_where_appends_with_and() {
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .where_(col("a").eq(val(1)))
        .and_where(col("b").eq(val(2)));

    let (query, _params) = qb.build_query_ast().expect("build ok");
    let w = extract_where(&query).expect("where present");
    match w {
        SqlExpr::BinaryOp { op, left, right } => {
            assert!(matches!(op, BO::And));
            assert!(matches!(
                left.as_ref(),
                SqlExpr::BinaryOp { op: BO::Eq, .. }
            ));
            assert!(matches!(
                right.as_ref(),
                SqlExpr::BinaryOp { op: BO::Eq, .. }
            ));
        }
        other => panic!("expected AND, got {:?}", other),
    }
}

#[test]
fn or_where_appends_with_or() {
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .where_(col("a").eq(val(1)))
        .or_where(col("b").eq(val(2)));

    let (query, _params) = qb.build_query_ast().expect("build ok");
    let w = extract_where(&query).expect("where present");
    match w {
        SqlExpr::BinaryOp { op, left, right } => {
            assert!(matches!(op, BO::Or));
            assert!(matches!(
                left.as_ref(),
                SqlExpr::BinaryOp { op: BO::Eq, .. }
            ));
            assert!(matches!(
                right.as_ref(),
                SqlExpr::BinaryOp { op: BO::Eq, .. }
            ));
        }
        other => panic!("expected OR, got {:?}", other),
    }
}

#[test]
fn or_then_and_preserves_grouping() {
    // (a=1 OR b=2) AND c=3
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .where_(col("a").eq(val(1)))
        .or_where(col("b").eq(val(2)))
        .and_where(col("c").eq(val(3)));

    let (query, _params) = qb.build_query_ast().expect("build ok");
    let w = extract_where(&query).expect("where present");

    // top-level must be AND
    if let SqlExpr::BinaryOp { op, left, right } = w {
        assert!(matches!(op, BO::And));
        // left must be OR subtree
        assert!(matches!(
            left.as_ref(),
            SqlExpr::BinaryOp { op: BO::Or, .. }
        ));
        // right must be Eq
        assert!(matches!(
            right.as_ref(),
            SqlExpr::BinaryOp { op: BO::Eq, .. }
        ));
    } else {
        panic!("expected AND at top");
    }
}

#[test]
fn group_inside_or_where_is_and_of_items() {
    // where a=1 OR (b=2 AND c=3)
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .where_(col("a").eq(val(1)))
        .or_where((col("b").eq(val(2)), col("c").eq(val(3))));

    let (query, _params) = qb.build_query_ast().expect("build ok");
    let w = extract_where(&query).expect("where present");

    if let SqlExpr::BinaryOp { op, left, right } = w {
        assert!(matches!(op, BO::Or));
        assert!(matches!(
            left.as_ref(),
            SqlExpr::BinaryOp { op: BO::Eq, .. }
        ));
        if let SqlExpr::BinaryOp { op: r_op, .. } = right.as_ref() {
            assert!(matches!(r_op, BO::And));
        } else {
            panic!("right side must be AND group");
        }
    } else {
        panic!("expected OR at top");
    }
}

#[test]
fn where_not_wraps_group_with_not() {
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .where_not((col("a").eq(val(1)), col("b").eq(val(2))));
    let (q, _) = qb.build_query_ast().expect("ok");
    let w = extract_where(&q).unwrap();
    if let SqlExpr::UnaryOp { op, expr } = w {
        assert!(matches!(op, UnaryOperator::Not));
        // внутри должен быть AND-узел
        assert!(matches!(
            expr.as_ref(),
            SqlExpr::BinaryOp { op: BO::And, .. }
        ));
    } else {
        panic!("expected NOT(AND(...))");
    }
}
