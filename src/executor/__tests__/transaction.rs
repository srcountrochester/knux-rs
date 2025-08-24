#![cfg(feature = "sqlite")]

use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous};
use sqlx::{Connection, Executor, FromRow, SqliteConnection};

use crate::executor::{QueryExecutor, config::ExecutorConfig};
use crate::expression::helpers::{col, raw, table, val};
use crate::param::Param;

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
async fn tx_commit_makes_changes_visible_outside() {
    let db = make_exec("tx_commit_db", 2).await;
    setup_schema_and_seed(&db).await;

    // базовая проверка
    let (c0,): (i64,) = db
        .query()
        .select(raw("COUNT(*)"))
        .from("users")
        .one()
        .await
        .unwrap();
    assert_eq!(c0, 3);

    // начинаем транзакцию
    let mut tx = db.begin().await.unwrap();

    // вставка ВНУТРИ транзакции (должна быть видна только внутри)
    let _inserted: Vec<User> = tx
        .query()
        .into("users")
        .insert((
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

    // // внутри: видим 4
    let (inside_cnt,): (i64,) = tx
        .query()
        .select(raw("COUNT(*)"))
        .from("users")
        .one()
        .await
        .unwrap();
    assert_eq!(inside_cnt, 4);

    // // СНАРУЖИ (другой коннект из пула): до коммита всё ещё 3
    let (outside_before,): (i64,) = db
        .query()
        .select(raw("COUNT(*)"))
        .from("users")
        .one()
        .await
        .unwrap();
    assert_eq!(outside_before, 3);

    // коммитим
    tx.commit().await.unwrap();

    // снаружи стало видно 4
    let (outside_after,): (i64,) = db
        .query()
        .select(raw("COUNT(*)"))
        .from("users")
        .one()
        .await
        .unwrap();
    assert_eq!(outside_after, 4);
}

#[tokio::test]
async fn tx_rollback_discards_changes() {
    let db = make_exec("tx_rollback_db", 2).await;
    setup_schema_and_seed(&db).await;

    let (c0,): (i64,) = db
        .query()
        .select(raw("COUNT(*)"))
        .from("users")
        .one()
        .await
        .unwrap();
    assert_eq!(c0, 3);

    let mut tx = db.begin().await.unwrap();

    // обновление внутри транзакции
    let _upd: Vec<User> = tx
        .query()
        .update("users")
        .set((col("age"), val(99)))
        .r#where(col("name").eq(val("Alice")))
        .returning("*")
        .await
        .unwrap();

    // внутри видим изменение
    let alice_in: Vec<User> = tx
        .query()
        .select("*")
        .from("users")
        .r#where(col("name").eq(val("Alice")))
        .await
        .unwrap();
    assert_eq!(alice_in.len(), 1);
    assert_eq!(alice_in[0].age, 99);

    // снаружи пока без изменений
    let alice_out: Vec<User> = db
        .query()
        .select("*")
        .from("users")
        .r#where(col("name").eq(val("Alice")))
        .await
        .unwrap();
    assert_eq!(alice_out.len(), 1);
    assert_eq!(alice_out[0].age, 30);

    // откат
    tx.rollback().await.unwrap();

    // снаружи всё как было
    let alice_after: Vec<User> = db
        .query()
        .select("*")
        .from("users")
        .r#where(col("name").eq(val("Alice")))
        .await
        .unwrap();
    assert_eq!(alice_after.len(), 1);
    assert_eq!(alice_after[0].age, 30);
}

#[tokio::test]
async fn tx_executes_inside_not_on_pool_connection() {
    // Этот тест «ловит» ошибку маршрутизации, если tx.query() фактически шлёт запросы в пул мимо транзакции.
    // Ожидаемое поведение: пока нет COMMIT — внешний коннект не видит изменений.
    let db = make_exec("tx_isolation_db", 2).await;
    setup_schema_and_seed(&db).await;

    let mut tx = db.begin().await.unwrap();

    // удаляем запись внутри транзакции и возвращаем удалённую строку
    let deleted: Vec<User> = tx
        .query()
        .delete(table("users"))
        .r#where(col("name").eq(val("Bob")))
        .returning("*")
        .await
        .unwrap();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].name, "Bob");

    // внутри "Bob" уже нет
    let inside: Vec<User> = tx
        .query()
        .select("*")
        .from("users")
        .r#where(col("name").eq(val("Bob")))
        .await
        .unwrap();
    assert!(inside.is_empty());

    // СНАРУЖИ "Bob" всё ещё есть — если этот ассерт падает,
    // значит tx.query() выполняется НЕ в транзакции (ошибка маршрутизации через пул).
    let outside: Vec<User> = db
        .query()
        .select("*")
        .from("users")
        .r#where(col("name").eq(val("Bob")))
        .await
        .unwrap();
    assert_eq!(outside.len(), 1);

    // после коммита "Bob" пропадает и снаружи
    tx.commit().await.unwrap();

    let outside_after: Vec<User> = db
        .query()
        .select("*")
        .from("users")
        .r#where(col("name").eq(val("Bob")))
        .await
        .unwrap();
    assert!(outside_after.is_empty());
}

// 1) После commit() билдеры из tx должны возвращать MissingConnection
#[tokio::test]
async fn tx_query_after_commit_returns_missing_connection() {
    let db = make_exec("tx_after_commit_err", 2).await;
    setup_schema_and_seed(&db).await;

    let mut tx = db.begin().await.unwrap();

    // внутри всё работает
    let (inside_before,): (i64,) = tx
        .query()
        .select(raw("COUNT(*)"))
        .from("users")
        .one()
        .await
        .unwrap();
    assert_eq!(inside_before, 3);

    tx.commit().await.unwrap();

    // после коммита контекст пустой — ждём MissingConnection
    let err = tx
        .query::<User>()
        .select(raw("COUNT(*)"))
        .from("users")
        .one()
        .await
        .unwrap_err();

    // точный тип ошибки вашего executor::Error; проверяем вариант MissingConnection
    match err {
        crate::executor::Error::MissingConnection => {}
        other => panic!("expected MissingConnection, got {other:?}"),
    }
}

// 2) execute()/fetch_typed() внутри транзакции
#[tokio::test]
async fn tx_execute_and_fetch_typed_inside_tx() {
    let db = make_exec("tx_exec_fetch", 2).await;
    setup_schema_and_seed(&db).await;

    let mut tx = db.begin().await.unwrap();

    // вставка через TxExecutor::execute (raw SQL)
    let affected = tx
        .execute(
            "INSERT INTO users(name, age, is_active) VALUES (?, ?, ?)",
            vec![Param::Str("Zoe".into()), Param::I32(27), Param::Bool(true)],
        )
        .await
        .unwrap();
    assert_eq!(affected, 1);

    // чтение через TxExecutor::fetch_typed
    let users: Vec<User> = tx
        .fetch_typed(
            "SELECT id, name, age, is_active FROM users WHERE name = ?",
            vec![Param::Str("Zoe".into())],
        )
        .await
        .unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].name, "Zoe");

    // внутри видим +1
    let (inside_cnt,): (i64,) = tx
        .query()
        .select(raw("COUNT(*)"))
        .from("users")
        .one()
        .await
        .unwrap();
    assert_eq!(inside_cnt, 4);

    // снаружи до коммита: 3 (WAL снапшот)
    let (outside_before,): (i64,) = db
        .query()
        .select(raw("COUNT(*)"))
        .from("users")
        .one()
        .await
        .unwrap();
    assert_eq!(outside_before, 3);

    tx.commit().await.unwrap();

    let (outside_after,): (i64,) = db
        .query()
        .select(raw("COUNT(*)"))
        .from("users")
        .one()
        .await
        .unwrap();
    assert_eq!(outside_after, 4);
}

// 3) nested (savepoint): rollback вложенной не откатывает внешнюю
#[tokio::test]
async fn tx_nested_savepoint_rollback_only_inner() {
    let db = make_exec("tx_nested_rb", 2).await;
    setup_schema_and_seed(&db).await;

    let mut tx = db.begin().await.unwrap();

    // во внешней добавим одну запись
    let _: Vec<User> = tx
        .query()
        .into("users")
        .insert((
            col("name"),
            val("A"),
            col("age"),
            val(10),
            col("is_active"),
            val(true),
        ))
        .returning_all()
        .await
        .unwrap();

    {
        // вложенная (SAVEPOINT)
        let mut inner = tx.begin_nested().await.unwrap();

        // добавим ещё одну внутри savepoint
        let _: Vec<User> = inner
            .query()
            .into("users")
            .insert((
                col("name"),
                val("B"),
                col("age"),
                val(20),
                col("is_active"),
                val(false),
            ))
            .returning_all()
            .await
            .unwrap();

        // внутри inner видим +2 (3+1+1)
        let (cnt_inner,): (i64,) = inner
            .query()
            .select(raw("COUNT(*)"))
            .from("users")
            .one()
            .await
            .unwrap();
        assert_eq!(cnt_inner, 5);

        // откатываем только вложенную
        inner.rollback().await.unwrap();
    }

    // во внешней осталось только +1
    let (cnt_outer,): (i64,) = tx
        .query()
        .select(raw("COUNT(*)"))
        .from("users")
        .one()
        .await
        .unwrap();
    assert_eq!(cnt_outer, 4);

    tx.commit().await.unwrap();

    // снаружи после коммита: 4
    let (after,): (i64,) = db
        .query()
        .select(raw("COUNT(*)"))
        .from("users")
        .one()
        .await
        .unwrap();
    assert_eq!(after, 4);
}

// 4) nested commit: обе фиксации видны снаружи
#[tokio::test]
async fn tx_nested_savepoint_commit() {
    let db = make_exec("tx_nested_commit", 2).await;
    setup_schema_and_seed(&db).await;

    let mut tx = db.begin().await.unwrap();

    let _: Vec<User> = tx
        .query()
        .into("users")
        .insert((
            col("name"),
            val("X"),
            col("age"),
            val(11),
            col("is_active"),
            val(true),
        ))
        .returning_all()
        .await
        .unwrap();

    {
        let mut inner = tx.begin_nested().await.unwrap();

        let _: Vec<User> = inner
            .query()
            .into("users")
            .insert((
                col("name"),
                val("Y"),
                col("age"),
                val(22),
                col("is_active"),
                val(true),
            ))
            .returning_all()
            .await
            .unwrap();

        inner.commit().await.unwrap();
    }
    tx.commit().await.unwrap();

    let (after,): (i64,) = db
        .query()
        .select(raw("COUNT(*)"))
        .from("users")
        .one()
        .await
        .unwrap();
    assert_eq!(after, 5); // было 3, +X, +Y
}

// 5) commit/rollback идемпотентны с точки зрения API (после take())
#[tokio::test]
async fn tx_commit_is_idempotent_ok() {
    let db = make_exec("tx_commit_idem", 1).await;
    setup_schema_and_seed(&db).await;

    let mut tx = db.begin().await.unwrap();
    tx.commit().await.unwrap();
    // повторный commit() теперь no-op (tx уже None)
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn tx_rollback_is_idempotent_ok() {
    let db = make_exec("tx_rb_idem", 1).await;
    setup_schema_and_seed(&db).await;

    let mut tx = db.begin().await.unwrap();
    tx.rollback().await.unwrap();
    tx.rollback().await.unwrap(); // повторный no-op
}
