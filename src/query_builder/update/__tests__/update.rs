use crate::expression::{col, lit, table, val};
use crate::query_builder::QueryBuilder;
use sqlparser::ast::{BinaryOperator, Expr as E, Ident, ObjectName, ObjectNamePart, SelectItem};

fn qb_pg() -> QueryBuilder<'static> {
    QueryBuilder::new_empty()
}

#[test]
fn update_complex_chain_set_where_and_returning_qstar() {
    // UPDATE s.u SET a = $1, b = b + $2, c = 'ok'
    // WHERE org_id = $3 AND (active = $4 AND deleted_at IS NULL)
    // RETURNING u.*
    let b = qb_pg()
        .update(table("u").schema("s"))
        .set((col("a"), val(1), col("b"), col("b").add(val(2))))
        .set((col("c"), lit("ok")))
        .r#where(col("org_id").eq(val(10)))
        .r#where((col("active").eq(val(true)), col("deleted_at").is_null()))
        .returning_all_from("u");

    // table == s.u
    let got = b.table.expect("table is set");
    let expected = ObjectName(vec![
        ObjectNamePart::Identifier(Ident::new("s")),
        ObjectNamePart::Identifier(Ident::new("u")),
    ]);
    assert_eq!(got, expected);

    // SET три присваивания: a, b, c
    assert_eq!(b.set.len(), 3);
    assert_eq!(b.set[0].col, "a");
    assert_eq!(b.set[1].col, "b");
    assert_eq!(b.set[2].col, "c");

    // RHS второго присваивания — b + $2
    match &b.set[1].value {
        E::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::Plus),
        other => panic!("expected BinaryOp Plus, got {:?}", other),
    }

    // WHERE: prev AND (A AND B)
    let where_expr = b.where_predicate.as_ref().expect("WHERE present");
    match where_expr {
        E::BinaryOp { op, left, right } => {
            assert_eq!(*op, BinaryOperator::And, "top-level AND expected");
            // right = (A AND B)
            match &**right {
                E::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::And),
                other => panic!("expected nested AND, got {:?}", other),
            }
            // left присутствует (org_id = $3)
            match &**left {
                E::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::Eq),
                other => panic!("expected EQ on left, got {:?}", other),
            }
        }
        other => panic!("expected BinaryOp AND, got {:?}", other),
    }

    // RETURNING u.*
    assert_eq!(b.returning.len(), 1);
    match &b.returning[0] {
        SelectItem::QualifiedWildcard(_, _) => {}
        other => panic!("expected QualifiedWildcard (u.*), got {:?}", other),
    }

    // Параметры должны накопиться (>= 3 от val(1), val(2), val(10); val(true) может быть bind)
    assert!(
        b.params.len() >= 3,
        "expected >=3 params, got {}",
        b.params.len()
    );
}

#[test]
fn update_error_on_empty_set_and_table_from_expr() {
    // Пустой SET → ошибка
    let b = qb_pg().update(table("users").schema("public")).set(());

    assert!(
        b.builder_errors
            .iter()
            .any(|e| e.contains("empty assignment list")),
        "expected empty assignment list error, got {:?}",
        b.builder_errors
    );

    // Таблица корректно разобрана
    let got = b.table.as_ref().expect("table set");
    assert_eq!(got.to_string(), "public.users");
}

#[test]
fn update_returning_one_overwrites_previous_list_takes_first() {
    // Сначала RETURNING *, потом .returning_one(col("x"), col("y")) → только x
    let b = qb_pg()
        .update(table("t"))
        .set((col("a"), val(1)))
        .returning_all()
        .returning_one((col("x"), col("y")));

    assert_eq!(b.returning.len(), 1);
    match &b.returning[0] {
        SelectItem::UnnamedExpr(E::Identifier(id)) => assert_eq!(id.value, "x"),
        other => panic!("expected UnnamedExpr(Identifier x), got {:?}", other),
    }
}

#[test]
fn update_compound_lhs_identifier_keeps_last_segment() {
    // SET t.a = 5 → col == "a"
    let b = qb_pg().update(table("t")).set((col("t.a"), val(5)));

    assert_eq!(b.set.len(), 1);
    assert_eq!(b.set[0].col, "a");
}

#[test]
fn update_where_noop_on_empty_tuple() {
    let b = qb_pg()
        .update(table("t"))
        .set((col("a"), val(1)))
        .r#where(()); // пусто

    assert!(b.where_predicate.is_none());
    assert!(b.builder_errors.is_empty());
}
