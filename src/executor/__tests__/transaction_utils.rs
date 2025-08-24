// cfg только для sqlite
#![cfg(feature = "sqlite")]

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Connection, Executor as _, Pool, Sqlite};
use std::{
    path::PathBuf,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::executor::transaction_utils::{execute_sqlite_exec, fetch_typed_sqlite_exec};
use crate::param::Param;

#[derive(Debug, sqlx::FromRow, PartialEq)]
struct User {
    id: i64,
    name: String,
    age: i64,
    is_active: bool,
}

// --- helpers ----------------------------------------------------------------

async fn make_sqlite_pool() -> Pool<Sqlite> {
    // уникальный файл БД (без пересечений между тестами)
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let db_path: PathBuf = std::env::temp_dir().join(format!("txn_utils_{ts}.db"));
    let dsn = format!("sqlite://{}", db_path.to_string_lossy());

    let opts = SqliteConnectOptions::from_str(&dsn)
        .unwrap()
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(5));

    // создаём сам файл и включаем WAL на уровне файла
    {
        let mut conn = sqlx::SqliteConnection::connect_with(&opts).await.unwrap();
        conn.execute("PRAGMA foreign_keys=ON;").await.unwrap();
    }

    SqlitePoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(opts)
        .await
        .unwrap()
}

// --- tests ------------------------------------------------------------------

// 1) Пул: CREATE/INSERT/SELECT через exec-функции
#[tokio::test]
async fn sqlite_exec_and_fetch_on_pool() {
    let pool = make_sqlite_pool().await;

    execute_sqlite_exec(&pool,
        "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER NOT NULL, is_active INTEGER NOT NULL)",
        vec![]
    ).await.unwrap();

    // два INSERT с параметрами разных типов
    let n1 = execute_sqlite_exec(
        &pool,
        "INSERT INTO users(name, age, is_active) VALUES(?, ?, ?)",
        vec![
            Param::Str("Alice".into()),
            Param::I32(30),
            Param::Bool(true),
        ],
    )
    .await
    .unwrap();
    assert_eq!(n1, 1);

    let n2 = execute_sqlite_exec(
        &pool,
        "INSERT INTO users(name, age, is_active) VALUES(?, ?, ?)",
        vec![Param::Str("Bob".into()), Param::I64(25), Param::Bool(false)],
    )
    .await
    .unwrap();
    assert_eq!(n2, 1);

    // выборка типизированно
    let rows: Vec<User> = fetch_typed_sqlite_exec::<_, User>(
        &pool,
        "SELECT id, name, age, is_active FROM users ORDER BY id",
        vec![],
    )
    .await
    .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].name, "Alice");
    assert_eq!(rows[0].age, 30);
    assert!(rows[0].is_active);

    assert_eq!(rows[1].name, "Bob");
    assert_eq!(rows[1].age, 25);
    assert!(!rows[1].is_active);
}

// 2) Внутри транзакции через tx.as_mut(): видно внутри, не видно после rollback
#[tokio::test]
async fn sqlite_exec_and_fetch_inside_tx_then_rollback() {
    let pool = make_sqlite_pool().await;

    execute_sqlite_exec(
        &pool,
        "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT)",
        vec![],
    )
    .await
    .unwrap();

    let mut tx = pool.begin().await.unwrap();

    // вставка только внутри транзакции
    let affected = execute_sqlite_exec(
        tx.as_mut(),
        "INSERT INTO items(name) VALUES (?), (?), (?)",
        vec![
            Param::Str("a".into()),
            Param::Str("b".into()),
            Param::Str("c".into()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(affected, 3);

    // внутри видим 3
    let inside: Vec<(i64, String)> = fetch_typed_sqlite_exec::<_, (i64, String)>(
        tx.as_mut(),
        "SELECT id, name FROM items ORDER BY id",
        vec![],
    )
    .await
    .unwrap();
    assert_eq!(inside.len(), 3);

    // откат
    tx.rollback().await.unwrap();

    // снаружи после отката пусто
    let outside: Vec<(i64, String)> =
        fetch_typed_sqlite_exec::<_, (i64, String)>(&pool, "SELECT id, name FROM items", vec![])
            .await
            .unwrap();
    assert!(outside.is_empty());
}

// 3) NULL-биндинги и F32→F64 для SQLite
#[derive(Debug, sqlx::FromRow, PartialEq)]
struct Rec {
    f: f64,
    s: Option<String>,
    n: Option<i64>,
}

#[tokio::test]
async fn sqlite_nulls_and_f32_mapping() {
    let pool = make_sqlite_pool().await;

    execute_sqlite_exec(&pool, "CREATE TABLE t(f REAL, s TEXT, n INTEGER)", vec![])
        .await
        .unwrap();

    // f32 должен биндиться как f64; текст и число — как NULL
    let aff = execute_sqlite_exec(
        &pool,
        "INSERT INTO t(f, s, n) VALUES(?, ?, ?)",
        vec![Param::F32(1.5), Param::NullText, Param::NullI64],
    )
    .await
    .unwrap();
    assert_eq!(aff, 1);

    let rows: Vec<Rec> = fetch_typed_sqlite_exec::<_, Rec>(&pool, "SELECT f, s, n FROM t", vec![])
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].f, 1.5_f64);
    assert!(rows[0].s.is_none());
    assert!(rows[0].n.is_none());
}

// 4) Пустой результат → пустой Vec
#[tokio::test]
async fn sqlite_fetch_empty_is_ok() {
    let pool = make_sqlite_pool().await;

    execute_sqlite_exec(
        &pool,
        "CREATE TABLE u(id INTEGER PRIMARY KEY, name TEXT)",
        vec![],
    )
    .await
    .unwrap();

    let out: Vec<(i64, String)> = fetch_typed_sqlite_exec::<_, (i64, String)>(
        &pool,
        "SELECT id, name FROM u WHERE 1=0",
        vec![],
    )
    .await
    .unwrap();

    assert!(out.is_empty());
}
