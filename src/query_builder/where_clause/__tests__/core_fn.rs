use super::super::*;
use super::extract_where;
use crate::expression::helpers::{col, val};
use crate::query_builder::args::QBClosure;
use sqlparser::ast::{BinaryOperator as BO, Expr as SqlExpr};

#[test]
fn attach_where_with_and_then_or() {
    let mut qb = QueryBuilder::new_empty().from("users").select("*");

    // a = 1
    let (e1, p1) = qb
        .resolve_qbarg_into_expr(QBArg::Expr(col("a").eq(val(1))))
        .expect("expr ok");
    qb.attach_where_with_and(e1, p1);

    // b = 2
    let (e2, p2) = qb
        .resolve_qbarg_into_expr(QBArg::Expr(col("b").eq(val(2))))
        .expect("expr ok");
    qb.attach_where_with_or(e2, p2);

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
        other => panic!("expected OR at top, got {:?}", other),
    }
}

#[test]
fn resolve_where_group_empty_returns_none() {
    let mut qb = QueryBuilder::new_empty().from("t").select("*");
    let group = qb.resolve_where_group(Vec::<crate::expression::Expression>::new());
    assert!(group.is_none());
}

#[test]
fn resolve_qbarg_into_expr_passes_expression_and_subquery_and_closure() {
    let qb = QueryBuilder::new_empty().from("t").select("*");

    // Expression passthrough
    let (e, _p) = qb
        .resolve_qbarg_into_expr(QBArg::Expr(col("x").eq(val(10))))
        .expect("expr ok");
    assert!(matches!(e, SqlExpr::BinaryOp { op: BO::Eq, .. }));

    // Subquery
    let sub = QueryBuilder::new_empty().from("u").select("id");
    let (e2, _p2) = qb
        .resolve_qbarg_into_expr(QBArg::Subquery(sub))
        .expect("subquery ok");
    assert!(matches!(e2, SqlExpr::Subquery(_)));

    // Closure
    let (e3, _p3) = qb
        .resolve_qbarg_into_expr(QBArg::Closure(QBClosure::new(|q| q.from("u").select("id"))))
        .expect("closure ok");
    assert!(matches!(e3, SqlExpr::Subquery(_)));
}

#[test]
fn build_in_predicate_empty_values_records_error() {
    // Пустой список значений → None + ошибка билдера
    let mut qb = QueryBuilder::new_empty().from("users").select("*");
    let pred = qb.build_in_predicate(
        col("id"),
        Vec::<crate::expression::Expression>::new(),
        false,
    );
    assert!(pred.is_none());

    let err = qb.build_query_ast().unwrap_err();
    let s = err.to_string();
    assert!(
        s.contains("пустой список значений"),
        "unexpected error text: {s}"
    );
}

#[test]
fn build_in_predicate_single_value_list_produces_inlist_and_collects_params() {
    use crate::expression::helpers::{col, val};
    use sqlparser::ast::Expr as SqlExpr;

    // Один элемент (не подзапрос) → IN (expr_list) с 1 элементом
    let mut qb = QueryBuilder::new_empty().from("users").select("*");
    let (pred, params) = qb
        .build_in_predicate(col("id"), val(1), false)
        .expect("predicate");

    // параметры от val(1) должны вернуться из build_in_predicate
    assert!(
        !params.is_empty(),
        "expected at least one param collected from val(1)"
    );

    match pred {
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            assert!(!negated, "negated must be false");
            assert!(
                matches!(
                    expr.as_ref(),
                    SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_)
                ),
                "left side must be identifier/compound identifier"
            );
            assert_eq!(list.len(), 1, "IN list must contain exactly one item");
        }
        other => panic!("expected InList, got {:?}", other),
    }
}
