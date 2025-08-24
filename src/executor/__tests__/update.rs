#![cfg(feature = "sqlite")]

use sqlx::{Executor, FromRow};

use crate::executor::{QueryExecutor, config::ExecutorConfig};
use crate::expression::helpers::{col, val};

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

    // схема
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

    // данные
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
async fn update_returning_all_rows_vec() {
    let exec = setup_db().await;

    // повышаем возраст всем активным и возвращаем изменённые строки
    let rows: Vec<User> = exec
        .query()
        .update("users")
        .set((col("age"), val(26)))
        .r#where(col("is_active").eq(val(true)))
        .returning("*")
        .await
        .unwrap();

    // должны вернуться Alice и Cara с age=26
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|u| u.is_active && u.age == 26));

    // повторная проверка чтением
    let after: Vec<User> = exec
        .query()
        .select("*")
        .from("users")
        .r#where(col("is_active").eq(val(true)))
        .await
        .unwrap();
    assert!(after.iter().all(|u| u.age == 26));
}

#[tokio::test]
async fn update_returning_single_row_vec() {
    let exec = setup_db().await;

    // обновляем Bob и возвращаем именно эту строку
    let rows: Vec<User> = exec
        .query()
        .update("users")
        .set((col("is_active"), val(true)))
        .r#where(col("name").eq(val("Bob")))
        .returning("*")
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "Bob");
    assert!(rows[0].is_active);

    // сверка одиночным селектом
    let only_bob: Vec<User> = exec
        .query()
        .select("*")
        .from("users")
        .r#where(col("name").eq(val("Bob")))
        .await
        .unwrap();
    assert_eq!(only_bob.len(), 1);
    assert!(only_bob[0].is_active);
}

#[tokio::test]
async fn update_exec_rows_affected_without_returning() {
    let exec = setup_db().await;

    // без RETURNING — используем exec() и проверяем rows_affected
    let affected = exec
        .query::<()>() // тип результата не нужен при exec()
        .update("users")
        .set((col("age"), val(99)))
        .r#where(col("name").eq(val("Alice")))
        .exec()
        .await
        .unwrap();

    assert_eq!(affected, 1);

    // проверка чтением
    let rows: Vec<User> = exec
        .query()
        .select("*")
        .from("users")
        .r#where(col("name").eq(val("Alice")))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].age, 99);
}

#[tokio::test]
async fn update_returning_no_match_empty_vec() {
    let exec = setup_db().await;

    // WHERE не совпадёт — ожидаем пустой вектор
    let rows: Vec<User> = exec
        .query()
        .update("users")
        .set((col("age"), val(100)))
        .r#where(col("id").eq(val(-1)))
        .returning("*")
        .await
        .unwrap();

    assert!(rows.is_empty());
}
