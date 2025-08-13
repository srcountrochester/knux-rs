use sqlparser::ast;
use sqlparser::ast::SelectItem;

use crate::expression::helpers::{col, val};
use crate::param::Param;
use crate::query_builder::QueryBuilder;

#[test]
fn select_with_string_items_builds_unnamed_exprs() {
    let qb = QueryBuilder::new_empty().select(&["id", "users.name"]);

    assert_eq!(qb.select_items.len(), 2, "should have two select items");
    assert!(qb.params.is_empty(), "string items must not add params");

    // id -> Identifier
    match &qb.select_items[0] {
        SelectItem::UnnamedExpr(ast::Expr::Identifier(ident)) => {
            assert_eq!(ident.value, "id");
        }
        other => panic!("expected UnnamedExpr(Identifier), got {:?}", other),
    }

    // users.name -> CompoundIdentifier
    match &qb.select_items[1] {
        SelectItem::UnnamedExpr(ast::Expr::CompoundIdentifier(parts)) => {
            assert_eq!(parts.len(), 2);
            assert_eq!(parts[0].value, "users");
            assert_eq!(parts[1].value, "name");
        }
        other => panic!("expected UnnamedExpr(CompoundIdentifier), got {:?}", other),
    }
}

#[test]
fn select_with_expression_and_alias_preserves_alias_and_params() {
    // val(100) даёт плейсхолдер в AST и один параметр; ставим алиас
    let qb = QueryBuilder::new_empty().select((val(100i32).alias("p"),));

    assert_eq!(qb.select_items.len(), 1);
    assert_eq!(qb.params.len(), 1);

    // проверяем alias в SelectItem
    match &qb.select_items[0] {
        SelectItem::ExprWithAlias { expr, alias } => {
            // Expr должен быть Value (placeholder)
            assert!(matches!(expr, ast::Expr::Value(_)));
            assert_eq!(alias.value, "p");
        }
        other => panic!("expected ExprWithAlias, got {:?}", other),
    }

    // параметр — именно наш 100 i32
    match &qb.params[0] {
        Param::I32(v) => assert_eq!(*v, 100),
        other => panic!("expected Param::I32(100), got {:?}", other),
    }
}

#[test]
fn select_tuple_mixed_types_keeps_order_and_alias() {
    // кортеж → ArgList (без .into())
    let qb = QueryBuilder::new_empty().select(("id", col("name").alias("n")));

    assert_eq!(qb.select_items.len(), 2);

    // 0: "id" → UnnamedExpr(Identifier)
    match &qb.select_items[0] {
        SelectItem::UnnamedExpr(ast::Expr::Identifier(ident)) => {
            assert_eq!(ident.value, "id");
        }
        other => panic!("expected UnnamedExpr(Identifier), got {:?}", other),
    }

    // 1: Expression с alias → ExprWithAlias(...)
    match &qb.select_items[1] {
        SelectItem::ExprWithAlias { expr, alias } => {
            assert!(matches!(expr, ast::Expr::Identifier(_)));
            assert_eq!(alias.value, "n");
        }
        other => panic!("expected ExprWithAlias, got {:?}", other),
    }

    // params отсутствуют
    assert!(qb.params.is_empty());
}

#[test]
fn select_vec_of_strs_and_slice_work() {
    // Vec<&str>
    let qb1 = QueryBuilder::new_empty().select(vec!["a", "b"]);
    assert_eq!(qb1.select_items.len(), 2);
    assert!(qb1.params.is_empty());

    // &[] с IntoQBArg + Clone
    let items: &[&str] = &["x", "y", "z"];
    let qb2 = QueryBuilder::new_empty().select(items);
    assert_eq!(qb2.select_items.len(), 3);
    assert!(qb2.params.is_empty());
}

#[test]
fn select_subquery_and_closure_expand_into_subqueries() {
    // subquery: SELECT x
    let sub = QueryBuilder::new_empty().select(("x",));

    // closure-subquery: SELECT y
    let qb = QueryBuilder::new_empty().select((sub, |q: QueryBuilder| q.select(("y",))));

    // теперь должно быть ДВА элемента: оба — подзапросы
    assert_eq!(qb.select_items.len(), 2);

    for item in &qb.select_items {
        match item {
            SelectItem::UnnamedExpr(ast::Expr::Subquery(_)) => {}
            other => panic!("expected UnnamedExpr(Subquery), got {:?}", other),
        }
    }

    // так как ни sub, ни closure не использовали val(...), параметров быть не должно
    assert!(qb.params.is_empty());
}
