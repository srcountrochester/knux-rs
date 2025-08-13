use crate::executor::config::ExecutorConfig;

#[test]
fn parse_dsn_ok() {
    let dsn = concat!(
        "sqlite::memory:",
        "?schema=main",
        "&pool.max=20",
        "&pool.min=3",
        "&pool.acquire_timeout=5s",
        "&pool.idle_timeout=30s",
        "&pool.max_lifetime=1h",
        "&pool.connect_timeout=10s",
        "&pool.test_before_acquire=1",
        "&init=PRAGMA%20foreign_keys%3DON",
        "&init=PRAGMA%20journal_mode%3DWAL",
    );

    let cfg = ExecutorConfig::from_dsn(dsn).expect("parse dsn");

    assert_eq!(
        cfg.database_url.as_deref(),
        Some(
            "sqlite::memory:?schema=main&pool.max=20&pool.min=3&pool.acquire_timeout=5s&pool.idle_timeout=30s&pool.max_lifetime=1h&pool.connect_timeout=10s&pool.test_before_acquire=1&init=PRAGMA%20foreign_keys%3DON&init=PRAGMA%20journal_mode%3DWAL"
        )
    );
    assert_eq!(cfg.schema.as_deref(), Some("main"));
    assert_eq!(cfg.max_connections, Some(20));
    assert_eq!(cfg.min_connections, Some(3));
    assert!(cfg.acquire_timeout.is_some());
    assert!(cfg.idle_timeout.is_some());
    assert!(cfg.max_lifetime.is_some());
    assert!(cfg.connect_timeout.is_some());
    assert_eq!(cfg.test_before_acquire, Some(true));

    // init-скрипты склеены через "; "
    let sql = cfg.after_connect_sql.as_deref().unwrap();
    assert!(sql.contains("PRAGMA foreign_keys=ON"));
    assert!(sql.contains("PRAGMA journal_mode=WAL"));
}

#[test]
fn scheme_detection_flag() {
    // проверим выставление is_postgres по схеме
    let pg = ExecutorConfig::from_dsn("postgres://u:p@localhost/db").unwrap();
    assert!(pg.is_postgres);

    let sqlite = ExecutorConfig::from_dsn("sqlite::memory:").unwrap();
    assert!(!sqlite.is_postgres);
}

#[test]
fn builder_overrides_dsn() {
    let dsn = "sqlite::memory:?schema=a&pool.max=5&pool.min=1";
    let from_dsn = ExecutorConfig::from_dsn(dsn).unwrap();

    // builder выставляет другие значения — они должны перекрыть
    let cfg = ExecutorConfig::builder()
        .database_url(dsn)
        .schema("b")
        .max_connections(50)
        .min_connections(10)
        .build()
        .merge_override(from_dsn);

    assert_eq!(cfg.schema.as_deref(), Some("b")); // builder > dsn
    assert_eq!(cfg.max_connections, Some(50)); // builder > dsn
    assert_eq!(cfg.min_connections, Some(10)); // builder > dsn
}
