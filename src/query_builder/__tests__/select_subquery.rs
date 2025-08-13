use sqlparser::ast;
use sqlparser::ast::SelectItem;

use crate::expression::helpers::{col, val};
use crate::param::Param;
use crate::query_builder::QueryBuilder;

#[test]
fn select_includes_subquery_and_closure_as_expr_subquery() {
    // подзапрос 1: SELECT x
    let sub1 = QueryBuilder::new_empty().select((col("x"),));

    // подзапрос 2 из замыкания: SELECT y
    let qb = QueryBuilder::new_empty().select((sub1, |q: QueryBuilder| q.select((col("y"),))));

    assert_eq!(qb.select_items.len(), 2);

    // оба элемента — UnnamedExpr(Expr::Subquery(_))
    for item in &qb.select_items {
        match item {
            SelectItem::UnnamedExpr(ast::Expr::Subquery(_)) => {}
            other => panic!("expected UnnamedExpr(Subquery), got {:?}", other),
        }
    }
    // параметров нет
    assert!(qb.params.is_empty());
}

#[test]
fn subquery_params_are_merged_into_outer_builder() {
    // subquery: SELECT ? (10)
    let sub = QueryBuilder::new_empty().select((val(10i32),));

    // closure-subquery: SELECT ? (20)
    let qb = QueryBuilder::new_empty().select((sub, |q: QueryBuilder| q.select((val(20i32),))));

    assert_eq!(qb.select_items.len(), 2);

    // Оба — Subquery
    match &qb.select_items[0] {
        SelectItem::UnnamedExpr(ast::Expr::Subquery(_)) => {}
        other => panic!("expected Subquery, got {:?}", other),
    }
    match &qb.select_items[1] {
        SelectItem::UnnamedExpr(ast::Expr::Subquery(_)) => {}
        other => panic!("expected Subquery, got {:?}", other),
    }

    // Параметры из обоих подзапросов должны оказаться в корневом билдере
    assert_eq!(qb.params.len(), 2);
    match (&qb.params[0], &qb.params[1]) {
        (Param::I32(a), Param::I32(b)) => {
            assert_eq!((*a, *b), (10, 20));
        }
        other => panic!("expected [I32(10), I32(20)], got {:?}", other),
    }
}

#[test]
fn select_mixed_items_expr_and_subquery_preserve_alias_on_expr() {
    let sub = QueryBuilder::new_empty().select((col("inner"),));
    let qb = QueryBuilder::new_empty().select((col("name").alias("n"), sub));

    assert_eq!(qb.select_items.len(), 2);

    // 0: ExprWithAlias(name as n)
    match &qb.select_items[0] {
        SelectItem::ExprWithAlias { expr, alias } => {
            assert!(matches!(expr, ast::Expr::Identifier(_)));
            assert_eq!(alias.value, "n");
        }
        other => panic!("expected ExprWithAlias, got {:?}", other),
    }

    // 1: Subquery
    match &qb.select_items[1] {
        SelectItem::UnnamedExpr(ast::Expr::Subquery(_)) => {}
        other => panic!("expected Subquery item, got {:?}", other),
    }
}
