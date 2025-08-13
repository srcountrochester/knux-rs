use crate::{
    executor::{QueryExecutor, config::ExecutorConfig},
    param::Param,
};
use serde::{Deserialize, Serialize};
use sqlx::{Executor, FromRow};

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromRow)]
struct User {
    id: i64,
    name: String,
    age: i32,
    is_active: bool,
}

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
