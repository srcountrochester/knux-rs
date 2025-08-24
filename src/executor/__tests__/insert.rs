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

    exec
}

#[tokio::test]
async fn insert_returning_all_single_row_vec() {
    let exec = setup_db().await;

    // INSERT ... RETURNING * → Vec<User> из 1 строки
    let rows: Vec<User> = exec
        .query()
        .into("users")
        .insert((
            col("name"),
            val("Alice"),
            col("age"),
            val(30),
            col("is_active"),
            val(true),
        ))
        .returning_all()
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    let u = &rows[0];
    assert_eq!(u.name, "Alice");
    assert_eq!(u.age, 30);
    assert!(u.is_active);
    assert!(u.id > 0); // AUTOINCREMENT/rowid
}

#[tokio::test]
async fn insert_returning_multiple_rows_vec() {
    let exec = setup_db().await;

    // Через columns(...) + flat values: две строки сразу
    let rows: Vec<User> = exec
        .query()
        .into("users")
        .columns((col("name"), col("age"), col("is_active")))
        .insert((
            val("Bob"),
            val(18),
            val(false),
            val("Cara"),
            val(25),
            val(true),
        ))
        .returning("*")
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    let names: Vec<_> = rows.iter().map(|u| u.name.as_str()).collect();
    assert!(names.contains(&"Bob") && names.contains(&"Cara"));
}

#[tokio::test]
async fn insert_exec_rows_affected_without_returning() {
    let exec = setup_db().await;

    // Без RETURNING — используем exec() и проверяем rows_affected
    let affected = exec
        .query::<()>() // тип результата не нужен при exec()
        .into("users")
        .insert((
            col("name"),
            val("Dan"),
            col("age"),
            val(41),
            col("is_active"),
            val(true),
        ))
        .exec()
        .await
        .unwrap();

    assert_eq!(affected, 1);

    // Контроль чтением: одна строка с именем Dan
    let got: Vec<User> = exec
        .query()
        .select("*")
        .from("users")
        .r#where(col("name").eq(val("Dan")))
        .await
        .unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].age, 41);
}

#[tokio::test]
async fn insert_await_without_returning_is_error() {
    let exec = setup_db().await;

    // Ожидаем осмысленную ошибку от IntoFuture без RETURNING
    let res: Result<Vec<User>, _> = exec
        .query()
        .into("users")
        .insert((
            col("name"),
            val("Eve"),
            col("age"),
            val(22),
            col("is_active"),
            val(false),
        ))
        .await; // НЕТ returning()

    assert!(res.is_err(), "await без RETURNING должен вернуть ошибку");
}

#[tokio::test]
async fn insert_returning_all_single_row_with_explicit_id() {
    let exec = setup_db().await;

    // Явно задаём PK и возвращаем вставленную строку
    let rows: Vec<User> = exec
        .query()
        .into("users")
        .insert((
            col("id"),
            val(10_i64),
            col("name"),
            val("Zoe"),
            col("age"),
            val(27),
            col("is_active"),
            val(true),
        ))
        .returning_all()
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    let u = &rows[0];
    assert_eq!(u.id, 10);
    assert_eq!(u.name, "Zoe");
    assert_eq!(u.age, 27);
    assert!(u.is_active);
}

#[tokio::test]
async fn insert_returning_all_multi_rows_with_explicit_ids() {
    let exec = setup_db().await;

    // Вставляем две строки с явными PK за один INSERT ... VALUES ... RETURNING *
    let rows: Vec<User> = exec
        .query()
        .into("users")
        .columns((col("id"), col("name"), col("age"), col("is_active")))
        .insert((
            val(20_i64),
            val("Ann"),
            val(31),
            val(true),
            val(21_i64),
            val("Ben"),
            val(19),
            val(false),
        ))
        .returning("*")
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    // порядок строк в RETURNING соответствует порядку VALUES (для SQLite это ожидаемо)
    assert_eq!(rows[0].id, 20);
    assert_eq!(rows[0].name, "Ann");
    assert_eq!(rows[1].id, 21);
    assert_eq!(rows[1].name, "Ben");
}

#[tokio::test]
async fn insert_exec_bulk_rows_affected_without_returning() {
    let exec = setup_db().await;

    // Без RETURNING — используем exec() и проверяем rows_affected
    let affected = exec
        .query::<()>()
        .into("users")
        .columns((col("name"), col("age"), col("is_active")))
        .insert((
            val("U1"),
            val(40),
            val(false),
            val("U2"),
            val(41),
            val(true),
            val("U3"),
            val(42),
            val(false),
        ))
        .exec()
        .await
        .unwrap();

    assert_eq!(affected, 3);

    // Контроль: три строки появились
    let got: Vec<User> = exec
        .query()
        .select("*")
        .from("users")
        .where_(col("name").isin(vec![val("U1"), val("U2"), val("U3")]))
        .await
        .unwrap();
    assert_eq!(got.len(), 3);
}
