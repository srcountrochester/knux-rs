use crate::{
    expression::{col, lit, table, val},
    query_builder::QueryBuilder,
    renderer::Dialect,
};
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, Ident, ObjectName, ObjectNamePart, SelectItem,
};

fn qb_pg() -> QueryBuilder {
    QueryBuilder::new_empty()
}

#[test]
fn update_accepts_table_expr_with_schema() {
    // qb.update(table("users").schema("non_public"))
    let b = qb_pg().update(table("users").schema("non_public"));

    // ошибок не должно быть
    assert!(
        b.builder_errors.is_empty(),
        "errors: {:?}",
        b.builder_errors
    );

    // table должен быть non_public.users
    let got = b.table.expect("table is set");
    let expected = ObjectName(vec![
        ObjectNamePart::Identifier(Ident::new("non_public")),
        ObjectNamePart::Identifier(Ident::new("users")),
    ]);
    assert_eq!(got, expected);
}

#[test]
fn update_set_and_where_chain_and_returning() {
    // UPDATE counters SET hits = hits + 1 WHERE id = 10 AND deleted_at IS NULL RETURNING id
    let b = qb_pg()
        .update(table("counters"))
        .set((col("hits"), col("hits").add(val(1))))
        .r#where(col("id").eq(val(10)))
        .r#where(col("deleted_at").is_null())
        .returning_one(col("id"));

    // SET: один элемент
    assert_eq!(b.set.len(), 1);
    assert_eq!(b.set[0].col, "hits");

    // WHERE: должно быть склеено через AND
    let Some(where_expr) = b.where_predicate.as_ref() else {
        panic!("WHERE is None")
    };
    match where_expr {
        SqlExpr::BinaryOp { op, .. } => {
            assert_eq!(*op, BinaryOperator::And);
        }
        other => panic!("expected BinaryOp AND, got {:?}", other),
    }

    // RETURNING: ровно один элемент UnnamedExpr
    assert_eq!(b.returning.len(), 1);
    match &b.returning[0] {
        SelectItem::UnnamedExpr(expr) => {
            // Простая sanity-проверка: expr — это идентификатор id
            match expr {
                SqlExpr::Identifier(id) => assert_eq!(id.value.as_str(), "id"),
                other => panic!("expected Identifier(id), got {:?}", other),
            }
        }
        other => panic!("expected UnnamedExpr, got {:?}", other),
    }

    // Параметры должны накопиться хотя бы из val(10) и val(1) (если val -> bind)
    // Если в твоей реализации val/lit не создаёт bind-параметр — этот ассерт можно ослабить.
    assert!(
        b.params.len() >= 1,
        "expected some params from val(...), got {}",
        b.params.len()
    );
}

#[test]
fn update_returning_all() {
    let b = qb_pg()
        .update(table("public.users"))
        .set((col("status"), lit("active")))
        .returning_all();

    assert_eq!(b.returning.len(), 1);
    match &b.returning[0] {
        SelectItem::Wildcard(_) => {} // ок
        other => panic!("expected RETURNING *, got {:?}", other),
    }
}

#[test]
fn update_invalid_table_argument_reports_error_but_keeps_first() {
    // Передаём два аргумента — ожидаем запись ошибки "expected a single table argument"
    let b = qb_pg().update((table("a"), table("b"))); // второй аргумент лишний

    assert!(!b.builder_errors.is_empty(), "expected builder error");
    // При этом таблица всё равно берётся из первого аргумента
    let got = b.table.expect("table is set");
    let expected = ObjectName(vec![ObjectNamePart::Identifier(Ident::new("a"))]);
    // "a" без схемы
    assert_eq!(got, expected);
}

#[test]
fn update_where_empty_is_noop() {
    let b = qb_pg()
        .update(table("t"))
        .set((col("x"), val(1)))
        .r#where(()); // пустой список условий

    assert!(b.where_predicate.is_none());
    assert!(b.builder_errors.is_empty());
}

#[test]
fn update_set_pairs_collects_params() {
    // Два присваивания, оба через val(...) — должны попасть параметры (если val -> bind)
    let b = qb_pg()
        .update(table("t"))
        .set((col("a"), val(1), col("b"), val(2)));

    assert_eq!(b.set.len(), 2);
    assert_eq!(b.set[0].col, "a");
    assert_eq!(b.set[1].col, "b");

    // Параметров как минимум столько же, сколько val(...); если у тебя lit(...) вместо val(...),
    // ослабь ассерт до >= 0.
    assert!(
        b.params.len() >= 2,
        "expected >=2 params from val(...), got {}",
        b.params.len()
    );
}

#[test]
fn update_from_collects_multiple_tables() {
    use crate::expression::table;
    use crate::query_builder::FromItem;

    let b = qb_pg()
        .update(table("t"))
        .from((table("a"), table("b")))
        .set((col("x"), val(1)));

    assert!(
        b.builder_errors.is_empty(),
        "errors: {:?}",
        b.builder_errors
    );
    assert_eq!(b.from_items.len(), 2);

    match &b.from_items[0] {
        FromItem::TableName(n) => assert_eq!(n.to_string(), "a"),
        other => panic!("expected TableName(a), got {:?}", other),
    }
    match &b.from_items[1] {
        FromItem::TableName(n) => assert_eq!(n.to_string(), "b"),
        other => panic!("expected TableName(b), got {:?}", other),
    }
}

#[test]
fn update_sqlite_or_replace_and_or_ignore_flags() {
    use sqlparser::ast::SqliteOnConflict;

    let brep = qb_pg()
        .dialect(Dialect::SQLite)
        .update(table("t"))
        .or_replace()
        .set((col("x"), val(1)));
    assert_eq!(brep.sqlite_or, Some(SqliteOnConflict::Replace));

    let bir = qb_pg()
        .dialect(Dialect::SQLite)
        .update(table("t"))
        .or_ignore()
        .set((col("x"), val(1)));
    assert_eq!(bir.sqlite_or, Some(SqliteOnConflict::Ignore));
}
