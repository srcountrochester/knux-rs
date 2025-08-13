use std::time::Duration;
use url::Url;

use super::utils::{parse_bool, parse_duration, parse_u32};
use super::{Error, Result};
use crate::executor::DbPool;

/// Конфиг для инициализации QueryExecutor.
/// Можно либо передать `database_url` (тогда мы соберём пул),
/// либо `pool` (тогда просто обёрнём его).
#[derive(Clone, Debug)]
pub struct ExecutorConfig {
    pub database_url: Option<String>,
    pub pool: Option<DbPool>,

    /// Необязательная схема по умолчанию (для Postgres -> search_path).
    pub schema: Option<String>,

    /// Тайминги и размер пула
    pub max_connections: Option<u32>,
    pub min_connections: Option<u32>,
    pub acquire_timeout: Option<Duration>,
    pub idle_timeout: Option<Duration>,
    pub max_lifetime: Option<Duration>,
    pub connect_timeout: Option<Duration>,
    pub test_before_acquire: Option<bool>,
    pub is_postgres: bool,

    /// Необязательный SQL, который выполняется на каждом подключении
    /// (полезно для нестандартных установок окружения).
    pub after_connect_sql: Option<String>,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            database_url: None,
            pool: None,
            schema: None,
            max_connections: None,
            min_connections: None,
            acquire_timeout: None,
            idle_timeout: Some(Duration::from_secs(30)),
            max_lifetime: Some(Duration::from_secs(60 * 60)),
            connect_timeout: Some(Duration::from_secs(30)),
            test_before_acquire: None,
            after_connect_sql: None,
            is_postgres: false,
        }
    }
}

pub struct ExecutorConfigBuilder {
    pub(crate) cfg: ExecutorConfig,
}

impl ExecutorConfig {
    pub fn builder() -> ExecutorConfigBuilder {
        ExecutorConfigBuilder {
            cfg: ExecutorConfig::default(),
        }
    }

    /// Создать конфиг из DSN, разбирая query-параметры по нашим правилам.
    /// Все распознанные параметры выставляются в конфиг.
    /// Сама строка кладётся в `database_url`.
    pub fn from_dsn(dsn: &str) -> Result<Self> {
        let url = Url::parse(dsn).map_err(Error::InvalidUrl)?;
        let mut cfg = ExecutorConfig::default();
        cfg.database_url = Some(dsn.to_string());

        let scheme = url.scheme();
        cfg.is_postgres = matches!(scheme, "postgres" | "postgresql");

        // собираем несколько init
        let mut inits: Vec<String> = Vec::new();

        for (k, v) in url.query_pairs() {
            let key = k.as_ref();
            let val = v.as_ref();

            match key {
                // schema
                "schema" | "search_path" => {
                    if !val.is_empty() {
                        cfg.schema = Some(val.to_string());
                    }
                }

                // pool.* (ints)
                "pool.max" => cfg.max_connections = Some(parse_u32(val, "pool.max")?),
                "pool.min" => cfg.min_connections = Some(parse_u32(val, "pool.min")?),

                // timeouts (durations)
                "pool.acquire_timeout" => cfg.acquire_timeout = Some(parse_duration(val, key)?),
                "pool.idle_timeout" => cfg.idle_timeout = Some(parse_duration(val, key)?),
                "pool.max_lifetime" => cfg.max_lifetime = Some(parse_duration(val, key)?),
                "pool.connect_timeout" => cfg.connect_timeout = Some(parse_duration(val, key)?),

                // bool
                "pool.test_before_acquire" => {
                    cfg.test_before_acquire = Some(parse_bool(val, key)?);
                }

                // init SQL (многоразовый)
                "init" => {
                    if !val.is_empty() {
                        inits.push(val.to_string());
                    }
                }

                // нераспознанное — игнорируем
                _ => {}
            }
        }

        if !inits.is_empty() {
            // соединим с '; ' чтобы было читаемо
            cfg.after_connect_sql = Some(inits.join("; "));
        }

        Ok(cfg)
    }

    /// Мердж текущего конфига с «перекрытием» полями из другого конфига.
    /// Используем, чтобы Builder-параметры имели приоритет над DSN.
    pub fn merge_override(mut self, other: ExecutorConfig) -> Self {
        // URL и пул: если в билдере нет — подставим из DSN
        if self.database_url.is_none() {
            self.database_url = other.database_url;
        }
        if self.pool.is_none() {
            self.pool = other.pool;
        }

        // schema: только если не задана в билдере
        if self.schema.is_none() {
            self.schema = other.schema;
        }

        // числовые/булевые/таймауты: заполняем только пустые
        if self.max_connections.is_none() {
            self.max_connections = other.max_connections;
        }
        if self.min_connections.is_none() {
            self.min_connections = other.min_connections;
        }
        if self.acquire_timeout.is_none() {
            self.acquire_timeout = other.acquire_timeout;
        }
        if self.idle_timeout.is_none() {
            self.idle_timeout = other.idle_timeout;
        }
        if self.max_lifetime.is_none() {
            self.max_lifetime = other.max_lifetime;
        }
        if self.connect_timeout.is_none() {
            self.connect_timeout = other.connect_timeout;
        }
        if self.test_before_acquire.is_none() {
            self.test_before_acquire = other.test_before_acquire;
        }

        // init sql — если в билдере не задан
        if self.after_connect_sql.is_none() {
            self.after_connect_sql = other.after_connect_sql;
        }

        // is_postgres: если уже true — оставляем; иначе берём из other
        if !self.is_postgres && other.is_postgres {
            self.is_postgres = true;
        }

        self
    }
}

impl ExecutorConfigBuilder {
    pub fn database_url(mut self, url: impl Into<String>) -> Self {
        self.cfg.database_url = Some(url.into());
        self
    }
    pub fn pool(mut self, pool: DbPool) -> Self {
        self.cfg.pool = Some(pool);
        self
    }
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.cfg.schema = Some(schema.into());
        self
    }
    pub fn max_connections(mut self, v: u32) -> Self {
        self.cfg.max_connections = Some(v);
        self
    }
    pub fn min_connections(mut self, v: u32) -> Self {
        self.cfg.min_connections = Some(v);
        self
    }
    pub fn acquire_timeout(mut self, v: Duration) -> Self {
        self.cfg.acquire_timeout = Some(v);
        self
    }
    pub fn idle_timeout(mut self, v: Duration) -> Self {
        self.cfg.idle_timeout = Some(v);
        self
    }
    pub fn max_lifetime(mut self, v: Duration) -> Self {
        self.cfg.max_lifetime = Some(v);
        self
    }
    pub fn connect_timeout(mut self, v: Duration) -> Self {
        self.cfg.connect_timeout = Some(v);
        self
    }
    pub fn test_before_acquire(mut self, v: bool) -> Self {
        self.cfg.test_before_acquire = Some(v);
        self
    }
    pub fn after_connect_sql(mut self, sql: impl Into<String>) -> Self {
        self.cfg.after_connect_sql = Some(sql.into());
        self
    }
    pub fn build(self) -> ExecutorConfig {
        self.cfg
    }
}
