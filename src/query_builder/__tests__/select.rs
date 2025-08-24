use sqlparser::ast;
use sqlparser::ast::SelectItem;

use crate::expression::helpers::{col, val};
use crate::expression::raw;
use crate::param::Param;
use crate::query_builder::QueryBuilder;
use crate::type_helpers::QBClosureHelper;

type QB = QueryBuilder<'static, ()>;

#[test]
fn select_with_string_items_builds_unnamed_exprs() {
    let qb = QB::new_empty().select(&["id", "users.name"]);

    assert_eq!(qb.select_items.len(), 2, "should have two select items");

    // id -> Identifier
    match &qb.select_items[0].item {
        SelectItem::UnnamedExpr(ast::Expr::Identifier(ident)) => {
            assert_eq!(ident.value, "id");
        }
        other => panic!("expected UnnamedExpr(Identifier), got {:?}", other),
    }
    assert!(qb.select_items[0].params.is_empty());

    // users.name -> CompoundIdentifier
    match &qb.select_items[1].item {
        SelectItem::UnnamedExpr(ast::Expr::CompoundIdentifier(parts)) => {
            assert_eq!(parts.len(), 2);
            assert_eq!(parts[0].value, "users");
            assert_eq!(parts[1].value, "name");
        }
        other => panic!("expected UnnamedExpr(CompoundIdentifier), got {:?}", other),
    }
    assert!(qb.select_items[1].params.is_empty());

    // строковые элементы не добавляют параметров ни в ноды, ни в общий буфер
    assert!(qb.params.is_empty());
}

#[test]
fn select_with_expression_and_alias_preserves_alias_and_params() {
    // val(100) даёт placeholder и один параметр; ставим алиас
    let qb = QB::new_empty().select((val(100i32).alias("p"),));

    assert_eq!(qb.select_items.len(), 1);

    // проверяем alias в SelectItem
    match &qb.select_items[0].item {
        SelectItem::ExprWithAlias { expr, alias } => {
            assert!(matches!(expr, ast::Expr::Value(_)));
            assert_eq!(alias.value, "p");
        }
        other => panic!("expected ExprWithAlias, got {:?}", other),
    }

    // параметр пока хранится в ноде
    assert_eq!(qb.select_items[0].params.len(), 1);
    match &qb.select_items[0].params[0] {
        Param::I32(v) => assert_eq!(*v, 100),
        other => panic!("expected Param::I32(100), got {:?}", other),
    }

    // при сборке он попадает в общий список
    let (_q, params) = qb.build_query_ast().expect("build ok");
    assert_eq!(params.len(), 1);
    match &params[0] {
        Param::I32(v) => assert_eq!(*v, 100),
        other => panic!("expected Param::I32(100), got {:?}", other),
    }
}

#[test]
fn select_tuple_mixed_types_keeps_order_and_alias() {
    // кортеж → ArgList (без .into())
    let qb = QB::new_empty().select(("id", col("name").alias("n")));

    assert_eq!(qb.select_items.len(), 2);

    // 0: "id" → UnnamedExpr(Identifier)
    match &qb.select_items[0].item {
        SelectItem::UnnamedExpr(ast::Expr::Identifier(ident)) => {
            assert_eq!(ident.value, "id");
        }
        other => panic!("expected UnnamedExpr(Identifier), got {:?}", other),
    }
    assert!(qb.select_items[0].params.is_empty());

    // 1: Expression с alias → ExprWithAlias(...)
    match &qb.select_items[1].item {
        SelectItem::ExprWithAlias { expr, alias } => {
            assert!(matches!(expr, ast::Expr::Identifier(_)));
            assert_eq!(alias.value, "n");
        }
        other => panic!("expected ExprWithAlias, got {:?}", other),
    }
    assert!(qb.select_items[1].params.is_empty());

    // общий буфер пока пуст
    assert!(qb.params.is_empty());
}

#[test]
fn select_vec_of_strs_and_slice_work() {
    // Vec<&str>
    let qb1 = QB::new_empty().select(vec!["a", "b"]);
    assert_eq!(qb1.select_items.len(), 2);
    assert!(qb1.select_items.iter().all(|n| n.params.is_empty()));
    assert!(qb1.params.is_empty());

    // &[] с IntoQBArg + Clone
    let items: &[&str] = &["x", "y", "z"];
    let qb2 = QB::new_empty().select(items);
    assert_eq!(qb2.select_items.len(), 3);
    assert!(qb2.select_items.iter().all(|n| n.params.is_empty()));
    assert!(qb2.params.is_empty());
}

#[test]
fn select_subquery_and_closure_expand_into_subqueries() {
    // subquery: SELECT x
    let sub = QB::new_empty().select(("x",));
    let scalar_subq: QBClosureHelper<()> = |q| q.select(("y",));

    // closure-subquery: SELECT y
    let qb = QB::new_empty().select((sub, scalar_subq));

    // теперь должно быть ДВА элемента: оба — подзапросы
    assert_eq!(qb.select_items.len(), 2);

    for node in &qb.select_items {
        match &node.item {
            SelectItem::UnnamedExpr(ast::Expr::Subquery(_)) => {}
            other => panic!("expected UnnamedExpr(Subquery), got {:?}", other),
        }
        // ни один из этих вариантов не добавляет параметров
        assert!(node.params.is_empty());
    }

    // общий буфер параметров до сборки пуст
    assert!(qb.params.is_empty());
}

#[test]
fn select_raw_count_star_renders_plain_sql_without_quotes() {
    let qb = QB::new_empty().select(raw("COUNT(*)")).from("users");
    let (sql, params) = qb.to_sql().unwrap();

    assert!(sql.contains("SELECT COUNT(*) FROM"));
    assert!(!sql.contains("\"COUNT(*)\""));
    assert!(params.is_empty());
}

#[test]
fn select_col_star_count_renders_count_star() {
    let qb = QB::new_empty().select(col("*").count()).from("users");
    let (sql, _params) = qb.to_sql().unwrap();
    assert!(sql.contains("SELECT COUNT(*) FROM"));
}
