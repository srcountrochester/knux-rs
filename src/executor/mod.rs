mod __tests__;
mod config;
mod error;
pub mod transaction;
pub mod transaction_utils;
pub mod utils;

use sqlx::Executor;
#[cfg(feature = "mysql")]
use sqlx::mysql::{MySqlPool, MySqlPoolOptions, MySqlRow};
#[cfg(feature = "postgres")]
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
#[cfg(feature = "sqlite")]
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions, SqliteRow};

#[cfg(feature = "mysql")]
use crate::executor::utils::fetch_typed_mysql;
#[cfg(feature = "postgres")]
use crate::executor::utils::fetch_typed_pg;
#[cfg(feature = "sqlite")]
use crate::executor::utils::fetch_typed_sqlite;

use crate::{
    optimizer::{OptimizeConfig, OptimizeConfigBuilder},
    param::Param,
    query_builder::{PoolQuery, QueryBuilder},
};
pub use config::ExecutorConfig;
pub use error::{Error, Result};

// ВЕРХ ФАЙЛА: алиасы под активную БД
#[cfg(feature = "postgres")]
pub type DbRow = PgRow;
#[cfg(feature = "mysql")]
pub type DbRow = MySqlRow;
#[cfg(feature = "sqlite")]
pub type DbRow = SqliteRow;

#[derive(Clone, Debug)]
pub enum DbPool {
    #[cfg(feature = "postgres")]
    Postgres(PgPool),
    #[cfg(feature = "mysql")]
    MySql(MySqlPool),
    #[cfg(feature = "sqlite")]
    Sqlite(SqlitePool),
}

#[derive(Clone)]
pub struct QueryExecutor {
    pub pool: DbPool,
    pub schema: Option<String>,
    pub(crate) optimize_cfg: OptimizeConfig,
}

impl QueryExecutor {
    /// Подключиться по конфигу: либо используем готовый pool, либо создаём через database_url.
    pub async fn connect(cfg: ExecutorConfig) -> Result<Self> {
        // если cfg.database_url есть, дополнительно парсим DSN и мержим:
        let cfg = if let Some(ref dsn) = cfg.database_url {
            let from_dsn = ExecutorConfig::from_dsn(dsn)
                .map_err(|e| Error::Sqlx(sqlx::Error::Protocol(format!("{e}").into())))?;
            // ВАЖНО: builder-поля перекрывают DSN
            cfg.merge_override(from_dsn)
        } else {
            cfg
        };

        // если передан готовый пул — используем его
        if let Some(pool) = cfg.pool.clone() {
            // возможно, выполним after_connect_sql один раз для совместимости?
            // (обычно это делается на каждый коннект; см. ниже в ветке построения пула)
            return Ok(Self {
                pool,
                schema: cfg.schema.clone(),
                optimize_cfg: OptimizeConfig::default(),
            });
        }

        let url = cfg.database_url.clone().ok_or(Error::MissingConnection)?;
        let scheme = url::Url::parse(&url)
            .map_err(Error::InvalidUrl)?
            .scheme()
            .to_string();

        // дефолты / опции пула
        let max_conn = cfg.max_connections.unwrap_or(10);
        let min_conn = cfg.min_connections.unwrap_or(0);
        let acquire = cfg.acquire_timeout;
        let idle = cfg.idle_timeout;
        let life = cfg.max_lifetime;
        let test_before = cfg.test_before_acquire.unwrap_or(false);
        let init_sql_all = cfg.after_connect_sql.clone(); // init SQL для всех СУБД
        #[cfg(feature = "postgres")]
        let schema = cfg.schema.clone();

        // выбираем драйвер по схеме URL
        let pool = match scheme.as_str() {
            #[cfg(feature = "postgres")]
            "postgres" | "postgresql" => {
                let mut opts = PgPoolOptions::new()
                    .max_connections(max_conn)
                    .min_connections(min_conn)
                    .test_before_acquire(test_before);
                if let Some(d) = acquire {
                    opts = opts.acquire_timeout(d);
                }
                if let Some(d) = idle {
                    opts = opts.idle_timeout(d);
                }
                if let Some(d) = life {
                    opts = opts.max_lifetime(d);
                }

                let init_sql_outer = init_sql_all.clone();
                let schema_outer = schema.clone();

                let pool = opts
                    .after_connect(move |conn, _| {
                        let init_sql = init_sql_outer.clone();
                        let schema = schema_outer.clone();
                        Box::pin(async move {
                            if let Some(sql) = init_sql.as_deref() {
                                conn.execute(sql).await?;
                            }
                            if let Some(s) = schema {
                                let set_path = format!("SET search_path TO {}", s);
                                let _ = conn.execute(set_path.as_str()).await;
                            }
                            Ok(())
                        })
                    })
                    .connect(&url)
                    .await?;
                DbPool::Postgres(pool)
            }

            #[cfg(feature = "mysql")]
            "mysql" | "mariadb" => {
                let mut opts = MySqlPoolOptions::new()
                    .max_connections(max_conn)
                    .min_connections(min_conn)
                    .test_before_acquire(test_before);
                if let Some(d) = acquire {
                    opts = opts.acquire_timeout(d);
                }
                if let Some(d) = idle {
                    opts = opts.idle_timeout(d);
                }
                if let Some(d) = life {
                    opts = opts.max_lifetime(d);
                }

                let pool = opts
                    .after_connect(move |conn, _| {
                        let init_sql = init_sql_all.clone();
                        Box::pin(async move {
                            if let Some(sql) = init_sql {
                                conn.execute(sql.as_str()).await?;
                            }
                            Ok::<_, sqlx::Error>(())
                        })
                    })
                    .connect(&url)
                    .await?;
                DbPool::MySql(pool)
            }

            #[cfg(feature = "sqlite")]
            "sqlite" => {
                let mut opts = SqlitePoolOptions::new()
                    .max_connections(max_conn)
                    .min_connections(min_conn)
                    .test_before_acquire(test_before);
                if let Some(d) = acquire {
                    opts = opts.acquire_timeout(d);
                }
                if let Some(d) = idle {
                    opts = opts.idle_timeout(d);
                }
                if let Some(d) = life {
                    opts = opts.max_lifetime(d);
                }

                let pool = opts
                    .after_connect(move |conn, _| {
                        let init_sql = init_sql_all.clone();
                        Box::pin(async move {
                            if let Some(sql) = init_sql {
                                conn.execute(sql.as_str()).await?;
                            }
                            Ok::<_, sqlx::Error>(())
                        })
                    })
                    .connect(&url)
                    .await?;
                DbPool::Sqlite(pool)
            }

            // если сборка без нужной фичи — вернём осмысленную ошибку
            _ => return Err(Error::UnsupportedScheme(scheme)),
        };

        Ok(Self {
            pool,
            schema: cfg.schema,
            optimize_cfg: OptimizeConfig::default(),
        })
    }

    /// Альтернатива: обернуть уже созданный пул (например, специфичный под БД).
    pub fn from_pool(pool: DbPool, schema: Option<String>) -> Self {
        Self {
            pool,
            schema,
            optimize_cfg: OptimizeConfig::default(),
        }
    }

    /// Начать строить запрос (интерфейс дальше останется как у knex-подобного билдера).
    pub fn query<T>(&self) -> PoolQuery<'_, T> {
        let qb = QueryBuilder::new_pool(self.pool.clone(), self.schema.clone())
            .with_optimize(self.optimize_cfg.clone());
        PoolQuery::new(qb)
    }

    pub async fn fetch_typed<T>(&self, sql: &str, params: Vec<Param>) -> Result<Vec<T>>
    where
        for<'r> T: sqlx::FromRow<'r, DbRow> + Send + Unpin,
    {
        match &self.pool {
            #[cfg(feature = "postgres")]
            DbPool::Postgres(pool) => fetch_typed_pg::<T>(pool, sql, params)
                .await
                .map_err(Into::into),

            #[cfg(feature = "mysql")]
            DbPool::MySql(pool) => fetch_typed_mysql::<T>(pool, sql, params)
                .await
                .map_err(Into::into),

            #[cfg(feature = "sqlite")]
            DbPool::Sqlite(pool) => fetch_typed_sqlite::<T>(pool, sql, params)
                .await
                .map_err(Into::into),
        }
    }

    #[cfg(feature = "sqlite")]
    pub fn as_sqlite_pool(&self) -> Option<&SqlitePool> {
        #[allow(irrefutable_let_patterns)]
        if let DbPool::Sqlite(pool) = &self.pool {
            Some(pool)
        } else {
            None
        }
    }

    #[cfg(feature = "postgres")]
    pub fn as_pg_pool(&self) -> Option<&PgPool> {
        if let DbPool::Postgres(pool) = &self.pool {
            Some(pool)
        } else {
            None
        }
    }

    #[cfg(feature = "mysql")]
    pub fn as_mysql_pool(&self) -> Option<&MySqlPool> {
        if let DbPool::MySql(pool) = &self.pool {
            Some(pool)
        } else {
            None
        }
    }
}
