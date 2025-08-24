#![cfg(feature = "sqlite")]

use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous};
use sqlx::{Connection, Executor, FromRow, SqliteConnection};

use crate::SpawnExt;
use crate::executor::{QueryExecutor, config::ExecutorConfig};

#[derive(Debug, FromRow, PartialEq)]
struct User {
    id: i64,
    name: String,
    age: i32,
    is_active: bool,
}

async fn make_exec(db_name: &str, max_conn: u32) -> QueryExecutor {
    // уникальное имя файла на каждый запуск
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pid = std::process::id();
    let db_file = format!("{db_name}_{pid}_{ts}.db");

    let db_path: PathBuf = std::env::temp_dir().join(db_file);
    let _ = std::fs::remove_file(&db_path); // если остался от прошлого запуска
    let dsn = format!("sqlite://{}", db_path.to_string_lossy());

    // включаем WAL (свойство файла) и базовые опции
    {
        let opts = SqliteConnectOptions::from_str(&dsn)
            .expect("bad DSN")
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal) // WAL: читатели не блокируются
            .synchronous(SqliteSynchronous::Normal) // рекомендуемо для WAL
            .busy_timeout(Duration::from_secs(5));

        let mut conn = SqliteConnection::connect_with(&opts).await.unwrap();
        conn.execute("PRAGMA foreign_keys=ON;").await.unwrap();
        // conn закроется здесь
    }

    let cfg = ExecutorConfig::builder()
        .database_url(&dsn)
        .max_connections(max_conn)
        .build();

    QueryExecutor::connect(cfg).await.unwrap()
}

async fn setup_schema_and_seed(exec: &QueryExecutor) {
    exec.as_sqlite_pool()
        .unwrap()
        .execute(
            r#"
            CREATE TABLE IF NOT EXISTS users (
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

#[tokio::test]
async fn poolquery_parallel_selects_sqlite() {
    let exec = make_exec("parallel_db", 5).await;
    setup_schema_and_seed(&exec).await;

    use crate::expression::{col, val};

    let q1 = exec
        .query()
        .select("*")
        .from("users")
        .where_(col("is_active").eq(val(true)))
        .into_send()
        .unwrap()
        .spawn();

    let q2 = exec
        .query()
        .select("*")
        .from("users")
        .where_(col("age").eq(val(25)))
        .into_send()
        .unwrap()
        .spawn();

    let (r1, r2): (Vec<User>, Vec<User>) = {
        let a = q1.await.unwrap().unwrap();
        let b = q2.await.unwrap().unwrap();
        (a, b)
    };

    assert!(r1.iter().all(|u| u.is_active));
    assert!(r2.iter().all(|u| u.age == 25));
}

#[tokio::test]
async fn poolquery_parallel_update_and_select_sqlite() {
    let exec = make_exec("parallel_dml", 5).await;
    setup_schema_and_seed(&exec).await;

    use crate::expression::{col, table, val};

    // UPDATE в отдельной задаче
    let upd = exec
        .query::<u64>()
        .update(table("users"))
        .set((col("age"), val(30)))
        .r#where(col("name").eq(val("Alice")))
        .exec_send()
        .unwrap()
        .spawn();

    // Одновременно SELECT
    let sel = exec
        .query()
        .select("*")
        .from("users")
        .into_send()
        .unwrap()
        .spawn();

    let _rows_affected = upd.await.unwrap().unwrap();
    let all: Vec<User> = sel.await.unwrap().unwrap();
    assert!(all.iter().any(|u| u.name == "Alice" && u.age == 30));
}
