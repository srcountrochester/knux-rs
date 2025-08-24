use crate::{
    executor::{QueryExecutor, config::ExecutorConfig},
    expression::{col, val},
    param::Param,
};

use serde::{Deserialize, Serialize};
use sqlx::{Executor, FromRow};

#[cfg(feature = "sqlite")]
#[derive(Debug, FromRow)]
struct UserQB {
    id: i64,
    name: String,
    age: i32,
    is_active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromRow)]
struct User {
    id: i64,
    name: String,
    age: i32,
    is_active: bool,
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn connect_sqlite_and_init_sql() {
    // инициализация с init SQL (SQLite-подобные PRAGMA, без schema и без postgres)
    let cfg = ExecutorConfig::builder()
        .database_url("sqlite::memory:?cache=shared")
        .after_connect_sql("PRAGMA foreign_keys = ON;")
        .build();

    let exec = QueryExecutor::connect(cfg).await.expect("connect");

    // проверим, что PRAGMA применился
    let val: i64 = sqlx::query_scalar("PRAGMA foreign_keys;")
        .fetch_one(exec.as_sqlite_pool().unwrap())
        .await
        .expect("read pragma");
    assert_eq!(val, 1);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn fetch_typed_binds_work() {
    let cfg = ExecutorConfig::builder()
        .database_url("sqlite::memory:")
        .max_connections(1)
        .build();
    let exec = QueryExecutor::connect(cfg).await.unwrap();

    // создаём таблицу и пару записей
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
            r#"INSERT INTO users (name, age, is_active) VALUES
           ('Alice', 30, 1),
           ('Bob',   18, 0),
           ('Cara',  25, 1)
        ;"#,
        )
        .await
        .unwrap();

    // подготовим SQL и параметры
    let sql = "SELECT id, name, age, is_active FROM users WHERE age >= ? AND is_active = ?";
    let params = vec![Param::I32(21), Param::Bool(true)];

    let rows: Vec<User> = exec.fetch_typed(sql, params).await.unwrap();
    assert_eq!(rows.len(), 2);

    let names: Vec<_> = rows.iter().map(|u| u.name.as_str()).collect();
    assert!(names.contains(&"Alice") && names.contains(&"Cara"));
}

#[cfg(feature = "sqlite")]
async fn setup_users(exec: &QueryExecutor) {
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
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn qb_into_future_many_select_sqlite() {
    let cfg = ExecutorConfig::builder()
        .database_url("sqlite::memory:")
        .max_connections(1)
        .build();
    let exec = QueryExecutor::connect(cfg).await.unwrap();

    setup_users(&exec).await;

    // many-rows: .await на QueryBuilder<T> → Result<Vec<T>>
    let rows: Vec<UserQB> = exec
        .query()
        .select(vec![col("id"), col("name"), col("age"), col("is_active")])
        .from("users")
        .where_(col("is_active").eq(val(true)))
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    let names: Vec<_> = rows.iter().map(|u| u.name.as_str()).collect();
    assert!(names.contains(&"Alice") && names.contains(&"Cara"));
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn qb_one_and_optional_sqlite() {
    use crate::expression::raw;

    let cfg = ExecutorConfig::builder()
        .database_url("sqlite::memory:")
        .max_connections(1)
        .build();
    let exec = QueryExecutor::connect(cfg).await.unwrap();

    setup_users(&exec).await;

    // COUNT(*) через raw
    let (c1,): (i64,) = exec
        .query()
        .select(raw("COUNT(*)"))
        .from("users")
        .one()
        .await
        .unwrap();
    assert_eq!(c1, 3);

    // COUNT(*) через col("*").count()
    let (c2,): (i64,) = exec
        .query()
        .select(col("*").count())
        .from("users")
        .one()
        .await
        .unwrap();
    assert_eq!(c2, 3);

    // optional: ноль строк -> Ok(None)
    let maybe: Option<UserQB> = exec
        .query()
        .select("*")
        .from("users")
        .where_(col("id").eq(val(-1)))
        .optional()
        .await
        .unwrap();
    assert!(maybe.is_none());
}
