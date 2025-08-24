use sqlparser::ast;
use sqlparser::ast::SelectItem;

use crate::expression::helpers::{col, val};
use crate::param::Param;
use crate::query_builder::QueryBuilder;
use crate::type_helpers::QBClosureHelper;

type QB = QueryBuilder<'static, ()>;

#[test]
fn select_includes_subquery_and_closure_as_expr_subquery() {
    // подзапрос 1: SELECT x
    let sub1 = QB::new_empty().select((col("x"),));
    let scalar_subq: QBClosureHelper<()> = |q| q.select((col("y"),));

    // подзапрос 2 из замыкания: SELECT y
    let qb = QB::new_empty().select((sub1, scalar_subq));

    assert_eq!(qb.select_items.len(), 2);

    // оба элемента — UnnamedExpr(Expr::Subquery(_))
    for node in &qb.select_items {
        match &node.item {
            SelectItem::UnnamedExpr(ast::Expr::Subquery(_)) => {
                // для этих подзапросов параметров нет
                assert!(node.params.is_empty());
            }
            other => panic!("expected UnnamedExpr(Subquery), got {:?}", other),
        }
    }

    // пока сборки не было — общая копилка параметров пуста
    // (параметры из SELECT добавятся в конце build_query_ast)
    assert!(qb.params.is_empty());
}

#[test]
fn subquery_params_are_merged_into_outer_builder_on_build() {
    // subquery: SELECT ? (10)
    let sub = QB::new_empty().select((val(10i32),));
    let scalar_subq: QBClosureHelper<()> = |q| q.select((val(20i32),));

    // closure-subquery: SELECT ? (20)
    let qb = QB::new_empty().select((sub, scalar_subq));

    assert_eq!(qb.select_items.len(), 2);

    // Оба — Subquery
    match &qb.select_items[0].item {
        SelectItem::UnnamedExpr(ast::Expr::Subquery(_)) => {}
        other => panic!("expected Subquery, got {:?}", other),
    }
    match &qb.select_items[1].item {
        SelectItem::UnnamedExpr(ast::Expr::Subquery(_)) => {}
        other => panic!("expected Subquery, got {:?}", other),
    }

    // До сборки параметры лежат внутри нод:
    assert_eq!(qb.select_items[0].params.len(), 1);
    assert_eq!(qb.select_items[1].params.len(), 1);

    // При сборке они сливаются в общий список (в порядке добавления)
    let (_q, params) = qb.build_query_ast().expect("build ok");
    assert_eq!(
        params.len(),
        2,
        "expected two params collected from subqueries"
    );
    match (&params[0], &params[1]) {
        (Param::I32(a), Param::I32(b)) => assert_eq!((*a, *b), (10, 20)),
        other => panic!("expected [I32(10), I32(20)], got {:?}", other),
    }
}

#[test]
fn select_mixed_items_expr_and_subquery_preserve_alias_on_expr() {
    let sub = QB::new_empty().select((col("inner"),));
    let qb = QB::new_empty().select((col("name").alias("n"), sub));

    assert_eq!(qb.select_items.len(), 2);

    // 0: ExprWithAlias(name as n)
    match &qb.select_items[0].item {
        SelectItem::ExprWithAlias { expr, alias } => {
            assert!(matches!(expr, ast::Expr::Identifier(_)));
            assert_eq!(alias.value, "n");
        }
        other => panic!("expected ExprWithAlias, got {:?}", other),
    }

    // 1: Subquery
    match &qb.select_items[1].item {
        SelectItem::UnnamedExpr(ast::Expr::Subquery(_)) => {}
        other => panic!("expected Subquery item, got {:?}", other),
    }

    // Параметров у этих двух пунктов нет на этом этапе
    assert!(qb.select_items[0].params.is_empty());
    assert!(qb.select_items[1].params.is_empty());
}
