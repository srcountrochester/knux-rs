use crate::{
    executor::{
        DbPool, Error as ExecError, Result as ExecResult, transaction_utils as tx_exec,
        utils as pool_exec,
    },
    param::Param,
};

#[allow(dead_code)]
#[derive(Debug)]
pub enum ExecCtx<'e> {
    None,
    Pool(DbPool),

    #[cfg(feature = "postgres")]
    PgConn(&'e mut sqlx::PgConnection),

    #[cfg(feature = "mysql")]
    MySqlConn(&'e mut sqlx::MySqlConnection),

    #[cfg(feature = "sqlite")]
    SqliteConn(&'e mut sqlx::SqliteConnection),
}

impl<'e> ExecCtx<'e> {
    pub async fn execute(&mut self, sql: &str, params: Vec<Param>) -> ExecResult<u64> {
        match self {
            ExecCtx::None => Err(ExecError::MissingConnection),

            // выполнение через пул
            ExecCtx::Pool(pool) => match pool {
                #[cfg(feature = "postgres")]
                DbPool::Postgres(p) => pool_exec::execute_pg(&*p, sql, params).await,
                #[cfg(feature = "mysql")]
                DbPool::MySql(p) => pool_exec::execute_mysql(&*p, sql, params).await,
                #[cfg(feature = "sqlite")]
                DbPool::Sqlite(p) => pool_exec::execute_sqlite(&*p, sql, params).await,
            },

            // выполнение ВНУТРИ транзакции (через коннект из tx.as_mut())
            #[cfg(feature = "postgres")]
            ExecCtx::PgConn(conn) => tx_exec::execute_pg_exec(&mut **conn, sql, params).await,
            #[cfg(feature = "mysql")]
            ExecCtx::MySqlConn(conn) => tx_exec::execute_mysql_exec(&mut **conn, sql, params).await,
            #[cfg(feature = "sqlite")]
            ExecCtx::SqliteConn(conn) => {
                tx_exec::execute_sqlite_exec(&mut **conn, sql, params).await
            }
        }
    }

    pub fn select_send<T>(
        self,
        sql: String,
        params: Vec<crate::param::Param>,
    ) -> ExecResult<impl core::future::Future<Output = ExecResult<Vec<T>>> + Send + 'static>
    where
        T: for<'r> sqlx::FromRow<'r, super::DbRow> + Send + Unpin + 'static,
    {
        let pool = match self {
            ExecCtx::Pool(p) => p, // перемещаем пул
            _ => return Err(ExecError::MissingConnection),
        };
        Ok(async move {
            match pool {
                #[cfg(feature = "postgres")]
                DbPool::Postgres(p) => pool_exec::fetch_typed_pg::<T>(&p, &sql, params).await,
                #[cfg(feature = "mysql")]
                DbPool::MySql(p) => pool_exec::fetch_typed_mysql::<T>(&p, &sql, params).await,
                #[cfg(feature = "sqlite")]
                DbPool::Sqlite(p) => pool_exec::fetch_typed_sqlite::<T>(&p, &sql, params).await,
            }
        })
    }

    pub fn select_one_send<T>(
        self,
        sql: String,
        params: Vec<crate::param::Param>,
    ) -> ExecResult<impl core::future::Future<Output = ExecResult<T>> + Send + 'static>
    where
        T: for<'r> sqlx::FromRow<'r, super::DbRow> + Send + Unpin + 'static,
    {
        let pool = match self {
            ExecCtx::Pool(p) => p,
            _ => return Err(ExecError::MissingConnection),
        };
        Ok(async move {
            match pool {
                #[cfg(feature = "postgres")]
                DbPool::Postgres(p) => pool_exec::fetch_one_typed_pg::<T>(&p, &sql, params).await,
                #[cfg(feature = "mysql")]
                DbPool::MySql(p) => pool_exec::fetch_one_typed_mysql::<T>(&p, &sql, params).await,
                #[cfg(feature = "sqlite")]
                DbPool::Sqlite(p) => pool_exec::fetch_one_typed_sqlite::<T>(&p, &sql, params).await,
            }
        })
    }

    pub fn select_optional_send<T>(
        self,
        sql: String,
        params: Vec<crate::param::Param>,
    ) -> ExecResult<impl core::future::Future<Output = ExecResult<Option<T>>> + Send + 'static>
    where
        T: for<'r> sqlx::FromRow<'r, super::DbRow> + Send + Unpin + 'static,
    {
        let pool = match self {
            ExecCtx::Pool(p) => p,
            _ => return Err(ExecError::MissingConnection),
        };
        Ok(async move {
            match pool {
                #[cfg(feature = "postgres")]
                DbPool::Postgres(p) => {
                    pool_exec::fetch_optional_typed_pg::<T>(&p, &sql, params).await
                }
                #[cfg(feature = "mysql")]
                DbPool::MySql(p) => {
                    pool_exec::fetch_optional_typed_mysql::<T>(&p, &sql, params).await
                }
                #[cfg(feature = "sqlite")]
                DbPool::Sqlite(p) => {
                    pool_exec::fetch_optional_typed_sqlite::<T>(&p, &sql, params).await
                }
            }
        })
    }

    pub fn execute_send(
        self,
        sql: String,
        params: Vec<crate::param::Param>,
    ) -> ExecResult<impl core::future::Future<Output = ExecResult<u64>> + Send + 'static> {
        let pool = match self {
            ExecCtx::Pool(p) => p,
            _ => return Err(ExecError::MissingConnection),
        };
        Ok(async move {
            match pool {
                #[cfg(feature = "postgres")]
                DbPool::Postgres(p) => pool_exec::execute_pg(&p, &sql, params).await,
                #[cfg(feature = "mysql")]
                DbPool::MySql(p) => pool_exec::execute_mysql(&p, &sql, params).await,
                #[cfg(feature = "sqlite")]
                DbPool::Sqlite(p) => pool_exec::execute_sqlite(&p, &sql, params).await,
            }
        })
    }
}

impl<'e> Clone for ExecCtx<'e> {
    fn clone(&self) -> Self {
        match self {
            ExecCtx::None => ExecCtx::None,
            ExecCtx::Pool(p) => ExecCtx::Pool(p.clone()),
            // ссылки на подключение/транзакцию преднамеренно НЕ клонируем
            #[cfg(feature = "postgres")]
            ExecCtx::PgConn(_) => ExecCtx::None,
            #[cfg(feature = "mysql")]
            ExecCtx::MySqlConn(_) => ExecCtx::None,
            #[cfg(feature = "sqlite")]
            ExecCtx::SqliteConn(_) => ExecCtx::None,
        }
    }
}
