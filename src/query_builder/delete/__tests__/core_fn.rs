use crate::expression::{col, lit, table, val};
use crate::query_builder::{FromItem, QueryBuilder};
use crate::renderer::Dialect;
use sqlparser::ast::{
    BinaryOperator, Expr as SExpr, Ident, ObjectName, ObjectNamePart, SelectItem,
};

fn qb_pg() -> QueryBuilder<'static> {
    QueryBuilder::new_empty()
}
fn qb_sqlite() -> QueryBuilder<'static> {
    QueryBuilder::new_empty().dialect(Dialect::SQLite)
}

#[test]
fn delete_sets_table_from_expr_and_where_and_returning_qstar() {
    // DELETE FROM public.users WHERE id = $1 RETURNING users.*
    let b = qb_pg()
        .delete(table("users").schema("public"))
        .r#where(col("id").eq(val(10)))
        .returning_all_from("users");

    // table == public.users
    let got = b.table.as_ref().expect("table is set");
    let expected = ObjectName(vec![
        ObjectNamePart::Identifier(Ident::new("public")),
        ObjectNamePart::Identifier(Ident::new("users")),
    ]);
    assert_eq!(*got, expected);

    // WHERE: id = $1
    let where_expr = b.where_predicate.as_ref().expect("WHERE present");
    match where_expr {
        SExpr::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::Eq),
        other => panic!("expected BinaryOp Eq, got {:?}", other),
    }

    // RETURNING users.*
    assert_eq!(b.returning.len(), 1);
    match &b.returning[0] {
        SelectItem::QualifiedWildcard(_, _) => {}
        other => panic!("expected QualifiedWildcard (users.*), got {:?}", other),
    }

    // Параметры накопились (от val(10))
    assert!(
        b.params.len() >= 1,
        "expected at least 1 param, got {}",
        b.params.len()
    );
}

#[test]
fn delete_using_collects_multiple_tables() {
    // DELETE FROM t USING a, b
    let b = qb_pg().delete(table("t")).using((table("a"), table("b")));

    assert!(
        b.builder_errors.is_empty(),
        "errors: {:?}",
        b.builder_errors
    );
    assert_eq!(b.using_items.len(), 2);

    match &b.using_items[0] {
        FromItem::TableName(n) => assert_eq!(n.to_string(), "a"),
        other => panic!("expected TableName(a), got {:?}", other),
    }
    match &b.using_items[1] {
        FromItem::TableName(n) => assert_eq!(n.to_string(), "b"),
        other => panic!("expected TableName(b), got {:?}", other),
    }
}

#[test]
fn delete_using_invalid_expr_yields_error() {
    // lit("not_ident") не является идентификатором таблицы
    let b = qb_pg().delete(table("t")).using((lit("not_ident"),));

    assert!(
        b.builder_errors
            .iter()
            .any(|e| e.contains("using(): invalid table reference")),
        "expected 'invalid table reference' error, got {:?}",
        b.builder_errors
    );
}

#[test]
fn delete_where_chaining_merges_with_and() {
    // WHERE (x > 1) AND (y IS NULL AND z = 2)
    let b = qb_pg()
        .delete(table("t"))
        .r#where(col("x").gt(val(1)))
        .r#where((col("y").is_null(), col("z").eq(val(2))));

    let w = b.where_predicate.as_ref().expect("WHERE present");
    match w {
        SExpr::BinaryOp { op, left, right } => {
            assert_eq!(*op, BinaryOperator::And, "top-level AND expected");
            match &**left {
                SExpr::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::Gt),
                other => panic!("expected left as BinaryOp(Gt), got {:?}", other),
            }
            match &**right {
                SExpr::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::And),
                other => panic!("expected right as BinaryOp(And), got {:?}", other),
            }
        }
        other => panic!("expected BinaryOp AND, got {:?}", other),
    }

    // минимум два параметра от val(1) и val(2)
    assert!(
        b.params.len() >= 2,
        "expected >=2 params, got {}",
        b.params.len()
    );
}

#[test]
fn delete_returning_variants_work() {
    // returning(...)
    let b1 = qb_pg()
        .delete(table("t"))
        .r#where(col("a").eq(val(1)))
        .returning((col("a"), col("b")));
    assert_eq!(b1.returning.len(), 2);

    // returning_all() → *
    let b2 = qb_pg().delete(table("t")).returning_all();
    assert_eq!(b2.returning.len(), 1);
    assert!(matches!(b2.returning[0], SelectItem::Wildcard(_)));

    // returning_one((x,y)) → только x
    let b3 = qb_pg()
        .delete(table("t"))
        .returning_all()
        .returning_one((col("x"), col("y")));
    assert_eq!(b3.returning.len(), 1);
    match &b3.returning[0] {
        SelectItem::UnnamedExpr(SExpr::Identifier(Ident { value, .. })) => {
            assert_eq!(value, "x")
        }
        other => panic!("expected UnnamedExpr(Identifier x), got {:?}", other),
    }

    // returning_all_from("s.t")
    let b4 = qb_pg().delete(table("t")).returning_all_from("s.t");
    assert_eq!(b4.returning.len(), 1);
    match &b4.returning[0] {
        SelectItem::QualifiedWildcard(_, _) => {}
        other => panic!("expected QualifiedWildcard(s.t.*), got {:?}", other),
    }
}

#[test]
fn delete_where_empty_tuple_is_noop() {
    let b = qb_pg().delete(table("t")).r#where(()); // пусто
    assert!(b.where_predicate.is_none());
    assert!(b.builder_errors.is_empty());
}

#[test]
fn delete_empty_table_arg_reports_error() {
    let b = qb_pg().delete(());
    assert!(
        b.builder_errors
            .iter()
            .any(|e| e.contains("table is not set")),
        "expected 'table is not set' error, got {:?}",
        b.builder_errors
    );
    assert!(b.table.is_none());
}

#[test]
fn delete_supports_sqlite_dialect_context() {
    // диалект хранится в билдере; функционально не влияет на сборку, но должен сохраняться
    let b = qb_sqlite().delete(table("t"));
    assert!(matches!(b.dialect, Dialect::SQLite));
}
