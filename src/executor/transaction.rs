use super::{Error, Result};
use crate::executor::{DbPool, QueryExecutor};
use sqlx::Acquire;

#[allow(dead_code)]
pub enum DbTx<'a> {
    #[cfg(feature = "postgres")]
    Postgres(sqlx::Transaction<'a, sqlx::Postgres>),
    #[cfg(feature = "mysql")]
    MySql(sqlx::Transaction<'a, sqlx::MySql>),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::Transaction<'a, sqlx::Sqlite>),
}

// NEW: исполнитель в контексте транзакции
#[derive()]
pub struct TxExecutor<'a> {
    tx: Option<DbTx<'a>>,
    // Нужен для построения SQL через наш QueryBuilder (диалект/квотинг и т.п.)
    pool: DbPool,
    schema: Option<String>,
}

impl QueryExecutor {
    pub async fn begin(&self) -> Result<TxExecutor<'_>> {
        match &self.pool {
            #[cfg(feature = "postgres")]
            DbPool::Postgres(pool) => {
                let tx = pool.begin().await?;
                Ok(TxExecutor {
                    tx: Some(DbTx::Postgres(tx)),
                    pool: self.pool.clone(),
                    schema: self.schema.clone(),
                })
            }
            #[cfg(feature = "mysql")]
            DbPool::MySql(pool) => {
                let tx = pool.begin().await?;
                Ok(TxExecutor {
                    tx: Some(DbTx::MySql(tx)),
                    pool: self.pool.clone(),
                    schema: self.schema.clone(),
                })
            }
            #[cfg(feature = "sqlite")]
            DbPool::Sqlite(pool) => {
                let tx = pool.begin().await?;
                Ok(TxExecutor {
                    tx: Some(DbTx::Sqlite(tx)),
                    pool: self.pool.clone(),
                    schema: self.schema.clone(),
                })
            }
        }
    }
}

impl<'tx> TxExecutor<'tx> {
    #[inline]
    pub fn query<'s, T>(&'s mut self) -> crate::query_builder::TxQuery<'s, T> {
        use crate::query_builder::{ExecCtx, QueryBuilder, TxQuery};

        let exec_ctx: ExecCtx<'s> = match self.tx.as_mut() {
            #[cfg(feature = "postgres")]
            Some(DbTx::Postgres(tx)) => ExecCtx::PgConn(tx.as_mut()),
            #[cfg(feature = "mysql")]
            Some(DbTx::MySql(tx)) => ExecCtx::MySqlConn(tx.as_mut()),
            #[cfg(feature = "sqlite")]
            Some(DbTx::Sqlite(tx)) => ExecCtx::SqliteConn(tx.as_mut()),
            None => ExecCtx::None,
        };
        let qb = QueryBuilder::new_tx(self.schema.clone(), exec_ctx);
        TxQuery::new(qb)
    }

    pub async fn fetch_typed<T>(
        &mut self,
        sql: &str,
        params: Vec<crate::param::Param>,
    ) -> Result<Vec<T>>
    where
        T: for<'r> sqlx::FromRow<'r, DbRow> + Send + Unpin,
    {
        match self.tx.as_mut() {
            #[cfg(feature = "postgres")]
            Some(DbTx::Postgres(tx)) => {
                crate::executor::transaction_utils::fetch_typed_pg_exec::<_, T>(
                    tx.as_mut(),
                    sql,
                    params,
                )
                .await
            }
            #[cfg(feature = "mysql")]
            Some(DbTx::MySql(tx)) => {
                crate::executor::transaction_utils::fetch_typed_mysql_exec::<_, T>(
                    tx.as_mut(),
                    sql,
                    params,
                )
                .await
            }
            #[cfg(feature = "sqlite")]
            Some(DbTx::Sqlite(tx)) => {
                crate::executor::transaction_utils::fetch_typed_sqlite_exec::<_, T>(
                    tx.as_mut(),
                    sql,
                    params,
                )
                .await
            }
            None => Err(Error::MissingConnection),
        }
    }

    pub async fn execute(&mut self, sql: &str, params: Vec<crate::param::Param>) -> Result<u64> {
        match self.tx.as_mut() {
            #[cfg(feature = "postgres")]
            Some(DbTx::Postgres(tx)) => {
                crate::executor::transaction_utils::execute_pg_exec(tx.as_mut(), sql, params).await
            }
            #[cfg(feature = "mysql")]
            Some(DbTx::MySql(tx)) => {
                crate::executor::transaction_utils::execute_mysql_exec(tx.as_mut(), sql, params)
                    .await
            }
            #[cfg(feature = "sqlite")]
            Some(DbTx::Sqlite(tx)) => {
                crate::executor::transaction_utils::execute_sqlite_exec(tx.as_mut(), sql, params)
                    .await
            }
            None => Err(Error::MissingConnection),
        }
    }

    // NEW: вложенная транзакция (SAVEPOINT)
    pub async fn begin_nested<'b>(&'b mut self) -> Result<TxExecutor<'b>> {
        match self.tx.as_mut() {
            #[cfg(feature = "postgres")]
            Some(DbTx::Postgres(tx)) => {
                let nested = tx.begin().await?; // SAVEPOINT
                Ok(TxExecutor {
                    tx: Some(DbTx::Postgres(nested)),
                    pool: self.pool.clone(),
                    schema: self.schema.clone(),
                })
            }
            #[cfg(feature = "mysql")]
            Some(DbTx::MySql(tx)) => {
                let nested = tx.begin().await?;
                Ok(TxExecutor {
                    tx: Some(DbTx::MySql(nested)),
                    pool: self.pool.clone(),
                    schema: self.schema.clone(),
                })
            }
            #[cfg(feature = "sqlite")]
            Some(DbTx::Sqlite(tx)) => {
                let nested = tx.begin().await?;
                Ok(TxExecutor {
                    tx: Some(DbTx::Sqlite(nested)),
                    pool: self.pool.clone(),
                    schema: self.schema.clone(),
                })
            }
            None => Err(Error::MissingConnection),
        }
    }

    // NEW: завершение
    pub async fn commit(&mut self) -> Result<()> {
        if let Some(dbtx) = self.tx.take() {
            match dbtx {
                #[cfg(feature = "postgres")]
                DbTx::Postgres(tx) => tx.commit().await?,
                #[cfg(feature = "mysql")]
                DbTx::MySql(tx) => tx.commit().await?,
                #[cfg(feature = "sqlite")]
                DbTx::Sqlite(tx) => tx.commit().await?,
            }
        }
        Ok(())
    }

    pub async fn rollback(&mut self) -> Result<()> {
        if let Some(dbtx) = self.tx.take() {
            match dbtx {
                #[cfg(feature = "postgres")]
                DbTx::Postgres(tx) => tx.rollback().await?,
                #[cfg(feature = "mysql")]
                DbTx::MySql(tx) => tx.rollback().await?,
                #[cfg(feature = "sqlite")]
                DbTx::Sqlite(tx) => tx.rollback().await?,
            }
        }
        Ok(())
    }
}

#[cfg(feature = "postgres")]
type DbRow = sqlx::postgres::PgRow;
#[cfg(all(not(feature = "postgres"), feature = "mysql"))]
type DbRow = sqlx::mysql::MySqlRow;
#[cfg(all(not(any(feature = "postgres", feature = "mysql")), feature = "sqlite"))]
type DbRow = sqlx::sqlite::SqliteRow;
