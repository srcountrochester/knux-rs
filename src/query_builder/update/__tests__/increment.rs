use crate::expression::{col, table, val};
use crate::query_builder::QueryBuilder;
use crate::{UpdateBuilder, lit};
use sqlparser::ast::{BinaryOperator, Expr as E};

fn qb_pg() -> QueryBuilder<'static> {
    QueryBuilder::new_empty()
}

/// Тест: `.increment` со строкой как колонкой.
/// Ожидаем `SET balance = balance + $1` (или `?`) и накопление параметров.
#[test]
fn increment_with_str_column_and_value_expr() {
    let b = qb_pg()
        .update(table("users"))
        .where_(col("id").eq(val(1)))
        .increment("balance", val(100));

    assert_eq!(b.set.len(), 1, "one assignment expected");
    assert_eq!(b.set[0].col, "balance");
    match &b.set[0].value {
        E::BinaryOp { op, left, .. } => {
            assert_eq!(*op, BinaryOperator::Plus);
            match &**left {
                E::Identifier(id) => assert_eq!(id.value, "balance"),
                other => panic!("expected left Identifier(balance), got {:?}", other),
            }
        }
        other => panic!("expected BinaryOp Plus, got {:?}", other),
    }
    // >=2: WHERE + RHS increment
    assert!(b.params.len() >= 2, "expected >=2 params");
}

/// Тест: `.increment` с `Expression`-колонкой.
/// Ожидаем `SET balance = balance + $1`.
#[test]
fn increment_with_expr_column() {
    let b = qb_pg()
        .update(table("users"))
        .increment(col("balance"), val(5));

    assert_eq!(b.set.len(), 1);
    assert_eq!(b.set[0].col, "balance");
    match &b.set[0].value {
        E::BinaryOp { op, left, .. } => {
            assert_eq!(*op, BinaryOperator::Plus);
            match &**left {
                E::Identifier(id) => assert_eq!(id.value, "balance"),
                other => panic!("expected left Identifier(balance), got {:?}", other),
            }
        }
        other => panic!("expected BinaryOp Plus, got {:?}", other),
    }
    assert!(b.params.len() >= 1, "increment value collected as param");
}

/// Тест: `.decrement` со строкой как колонкой.
/// Ожидаем `SET balance = balance - $1` (или `?)`.
#[test]
fn decrement_with_str_column_and_value_expr() {
    let b = qb_pg()
        .update(table("users"))
        .where_(col("id").eq(val(1)))
        .decrement("balance", val(100));

    assert_eq!(b.set.len(), 1);
    assert_eq!(b.set[0].col, "balance");
    match &b.set[0].value {
        E::BinaryOp { op, left, .. } => {
            assert_eq!(*op, BinaryOperator::Minus);
            match &**left {
                E::Identifier(id) => assert_eq!(id.value, "balance"),
                other => panic!("expected left Identifier(balance), got {:?}", other),
            }
        }
        other => panic!("expected BinaryOp Minus, got {:?}", other),
    }
    assert!(b.params.len() >= 2, "expected >=2 params");
}

/// Тест: `.decrement` с `Expression`-колонкой.
/// Ожидаем `SET balance = balance - $1`.
#[test]
fn decrement_with_expr_column() {
    let b = qb_pg()
        .update(table("users"))
        .decrement(col("balance"), val(5));

    assert_eq!(b.set.len(), 1);
    assert_eq!(b.set[0].col, "balance");
    match &b.set[0].value {
        E::BinaryOp { op, left, .. } => {
            assert_eq!(*op, BinaryOperator::Minus);
            match &**left {
                E::Identifier(id) => assert_eq!(id.value, "balance"),
                other => panic!("expected left Identifier(balance), got {:?}", other),
            }
        }
        other => panic!("expected BinaryOp Minus, got {:?}", other),
    }
    assert!(b.params.len() >= 1, "decrement value collected as param");
}

/// Негативный тест для `.increment`: левая часть — не идентификатор колонки.
/// Ожидаем запись ошибки билдера и пустой SET.
#[test]
fn increment_left_not_identifier_error() {
    let b = qb_pg()
        .update(table("users"))
        .increment(lit("not_ident"), val(1));

    assert!(
        !b.builder_errors.is_empty(),
        "ожидали ошибку билдера при неидентификаторе слева"
    );
    let msg = b.builder_errors[0].to_string();
    assert!(
        msg.contains("left item must be a column identifier")
            || msg.contains("invalid compound identifier"),
        "неожиданное сообщение ошибки: {msg}"
    );
    assert!(b.set.is_empty(), "SET не должен пополниться при ошибке");
}

/// Составной идентификатор в `.increment`: `u.balance`.
/// Ожидаем, что колонка нормализуется к последнему сегменту `balance`.
#[test]
fn increment_with_compound_identifier_uses_last_segment() {
    let b = qb_pg()
        .update(table("users"))
        .increment("u.balance", val(1));

    assert_eq!(b.set.len(), 1);
    assert_eq!(b.set[0].col, "balance");

    match &b.set[0].value {
        E::BinaryOp { left, .. } => match &**left {
            E::Identifier(id) => assert_eq!(id.value, "balance"),
            other => panic!("ожидали Identifier(balance), получили {:?}", other),
        },
        other => panic!("ожидали BinaryOp, получили {:?}", other),
    }
}

/// Негативный тест для `.decrement`: левая часть — не идентификатор.
/// Ожидаем запись ошибки билдера и отсутствие новых присваиваний.
#[test]
fn decrement_left_not_identifier_error() {
    let b = qb_pg()
        .update(table("users"))
        .decrement(lit("not_ident"), val(1));

    assert!(
        !b.builder_errors.is_empty(),
        "ожидали ошибку билдера при неидентификаторе слева"
    );
    let msg = b.builder_errors[0].to_string();
    assert!(
        msg.contains("left item must be a column identifier")
            || msg.contains("invalid compound identifier"),
        "неожиданное сообщение ошибки: {msg}"
    );
    assert!(b.set.is_empty(), "SET не должен пополниться при ошибке");
}

/// Составной идентификатор в `.decrement`: `u.balance`.
/// Ожидаем нормализацию к `balance` и правильную левую часть бинарной операции.
#[test]
fn decrement_with_compound_identifier_uses_last_segment() {
    let b = qb_pg()
        .update(table("users"))
        .decrement("u.balance", val(1));

    assert_eq!(b.set.len(), 1);
    assert_eq!(b.set[0].col, "balance");

    match &b.set[0].value {
        E::BinaryOp { left, .. } => match &**left {
            E::Identifier(id) => assert_eq!(id.value, "balance"),
            other => panic!("ожидали Identifier(balance), получили {:?}", other),
        },
        other => panic!("ожидали BinaryOp, получили {:?}", other),
    }
}

/// Проверка: `clear_counters()` удаляет инкремент/декремент и оставляет обычные SET.
/// Ожидаем, что после вызова останется только обычное присваивание.
#[test]
fn clear_counters_removes_only_counters() {
    let b: UpdateBuilder<'_, ()> = QueryBuilder::new_empty()
        .update(table("users"))
        .set((col("a"), val(0))) // обычный SET
        .increment("a", val(1)) // счётчик
        .decrement(col("b"), val(2)) // счётчик
        .clear_counters();

    // Должен остаться только один SET: a = 0
    assert_eq!(b.set.len(), 1);
    assert_eq!(b.set[0].col, "a");

    // Значение колонки 'a' больше не бинарная операция.
    use sqlparser::ast::Expr as E;
    match &b.set[0].value {
        E::BinaryOp { .. } => panic!("ожидали обычное присваивание, получили BinaryOp"),
        _ => {}
    }
}

/// Проверка: составной идентификатор (`u.balance`) корректно обрабатывается при сбросе.
/// Ожидаем, что счётчик по `u.balance` будет удалён (берём последний сегмент — `balance`).
#[test]
fn clear_counters_handles_compound_identifier() {
    let b: UpdateBuilder<'_, ()> = QueryBuilder::new_empty()
        .update(table("users"))
        .increment("u.balance", val(100))
        .clear_counters();

    assert!(b.set.is_empty(), "все счётчики должны быть удалены");
}
