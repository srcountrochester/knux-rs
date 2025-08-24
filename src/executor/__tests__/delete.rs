#![cfg(feature = "sqlite")]

use sqlx::{Executor, FromRow};

use crate::executor::{QueryExecutor, config::ExecutorConfig};
use crate::expression::helpers::{col, table, val};

#[derive(Debug, FromRow, PartialEq)]
struct User {
    id: i64,
    name: String,
    age: i32,
    is_active: bool,
}

async fn setup_db() -> QueryExecutor {
    let cfg = ExecutorConfig::builder()
        .database_url("sqlite::memory:")
        .max_connections(1)
        .build();
    let exec = QueryExecutor::connect(cfg).await.unwrap();

    exec.as_sqlite_pool()
        .unwrap()
        .execute(
            r#"
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                age INTEGER NOT NULL,
                is_active BOOLEAN NOT NULL
            );
            "#,
        )
        .await
        .unwrap();

    exec.as_sqlite_pool()
        .unwrap()
        .execute(
            r#"
            INSERT INTO users (name, age, is_active) VALUES
                ('Alice', 30, TRUE),
                ('Bob',   18, FALSE),
                ('Cara',  25, TRUE);
            "#,
        )
        .await
        .unwrap();

    exec
}

#[tokio::test]
async fn delete_returning_single_row_vec() {
    let exec = setup_db().await;

    // Удаляем Bob и возвращаем удалённую строку (DELETE ... RETURNING *)
    let rows: Vec<User> = exec
        .query()
        .delete(table("users"))
        .r#where(col("name").eq(val("Bob")))
        .returning("*")
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "Bob");

    // Контроль: Bob отсутствует
    let left: Vec<User> = exec.query().select("*").from("users").await.unwrap();
    assert_eq!(left.len(), 2);
    assert!(left.iter().all(|u| u.name != "Bob"));
}

#[tokio::test]
async fn delete_returning_multiple_rows_vec() {
    let exec = setup_db().await;

    // Удаляем всех активных, возвращаем удалённые строки
    let rows: Vec<User> = exec
        .query()
        .delete(table("users"))
        .r#where(col("is_active").eq(val(true)))
        .returning("*")
        .await
        .unwrap();

    // Должны вернуться Alice и Cara
    assert_eq!(rows.len(), 2);
    let names: Vec<_> = rows.iter().map(|u| u.name.as_str()).collect();
    assert!(names.contains(&"Alice") && names.contains(&"Cara"));

    // Контроль: в таблице остался только Bob
    let left: Vec<User> = exec.query().select("*").from("users").await.unwrap();
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].name, "Bob");
}

#[tokio::test]
async fn delete_returning_no_match_empty_vec() {
    let exec = setup_db().await;

    // WHERE не совпадёт — ожидаем пустой вектор
    let rows: Vec<User> = exec
        .query()
        .delete(table("users"))
        .r#where(col("id").eq(val(-1)))
        .returning("*")
        .await
        .unwrap();

    assert!(rows.is_empty());
}

#[tokio::test]
async fn delete_exec_rows_affected_without_returning() {
    let exec = setup_db().await;

    // Без RETURNING — используем .exec() и проверяем rows_affected
    let affected = exec
        .query::<()>() // тип результата не нужен при exec()
        .delete(table("users"))
        .r#where(col("name").eq(val("Alice")))
        .exec()
        .await
        .unwrap();

    assert_eq!(affected, 1);

    // Контроль: Alice удалена
    let left: Vec<User> = exec.query().select("*").from("users").await.unwrap();
    assert_eq!(left.len(), 2);
    assert!(left.iter().all(|u| u.name != "Alice"));
}

#[tokio::test]
async fn delete_await_without_returning_is_error() {
    let exec = setup_db().await;

    // Ожидаем осмысленную ошибку: await без RETURNING на DeleteBuilder
    let res: Result<Vec<User>, _> = exec
        .query()
        .delete(table("users"))
        .r#where(col("name").eq(val("Cara")))
        .await; // НЕТ returning()

    assert!(res.is_err(), "await без RETURNING должен вернуть ошибку");
}
