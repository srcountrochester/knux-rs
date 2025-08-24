use super::Result;
use crate::param::Param;

// NEW: Postgres
#[cfg(feature = "postgres")]
pub async fn fetch_typed_pg_exec<'e, E, T>(exec: E, sql: &str, params: Vec<Param>) -> Result<Vec<T>>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    for<'r> T: sqlx::FromRow<'r, sqlx::postgres::PgRow> + Send + Unpin,
{
    let mut q = sqlx::query_as::<_, T>(sql);
    for p in params {
        q = match p {
            Param::I8(v) => q.bind(v),
            Param::I16(v) => q.bind(v),
            Param::I32(v) => q.bind(v),
            Param::I64(v) => q.bind(v),
            Param::F32(v) => q.bind(v as f64),
            Param::F64(v) => q.bind(v),
            Param::Str(v) => q.bind(v),
            Param::Bool(v) => q.bind(v),
            Param::Bytes(v) => q.bind(v),

            #[cfg(feature = "serde_json")]
            Param::Json(v) => q.bind(v),
            #[cfg(feature = "uuid")]
            Param::Uuid(v) => q.bind(v),
            #[cfg(feature = "rust_decimal")]
            Param::Decimal(v) => q.bind(v),

            // NULL-хинты
            Param::NullText => q.bind(None::<&str>),
            Param::NullBytes => q.bind(None::<Vec<u8>>),
            Param::NullBool => q.bind(None::<bool>),
            Param::NullI64 => q.bind(None::<i64>),
            Param::NullI32 => q.bind(None::<i32>),
            Param::NullF64 => q.bind(None::<f64>),

            #[cfg(feature = "time")]
            Param::Date(v) => q.bind(v),
            #[cfg(feature = "time")]
            Param::Time(v) => q.bind(v),
            #[cfg(feature = "time")]
            Param::DateTime(v) => q.bind(v),
            #[cfg(feature = "time")]
            Param::NaiveDateTime(v) => q.bind(v),

            #[cfg(feature = "time")]
            Param::NullDate => q.bind(None::<time::Date>),
            #[cfg(feature = "time")]
            Param::NullTime => q.bind(None::<time::Time>),
            #[cfg(feature = "time")]
            Param::NullDateTime => q.bind(None::<time::OffsetDateTime>),
            #[cfg(feature = "time")]
            Param::NullNaiveDateTime => q.bind(None::<time::PrimitiveDateTime>),

            #[cfg(feature = "chrono")]
            Param::ChronoNaiveDate(v) => q.bind(v),
            #[cfg(feature = "chrono")]
            Param::ChronoNaiveTime(v) => q.bind(v),
            #[cfg(feature = "chrono")]
            Param::ChronoNaiveDateTime(v) => q.bind(v),

            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveDate => q.bind(None::<chrono::NaiveDate>),
            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveTime => q.bind(None::<chrono::NaiveTime>),
            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveDateTime => q.bind(None::<chrono::NaiveDateTime>),
        };
    }
    Ok(q.fetch_all(exec).await?)
}

#[cfg(feature = "postgres")]
pub async fn execute_pg_exec<'e, E>(exec: E, sql: &str, params: Vec<Param>) -> Result<u64>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let mut q = sqlx::query(sql);
    for p in params {
        q = match p {
            Param::I8(v) => q.bind(v),
            Param::I16(v) => q.bind(v),
            Param::I32(v) => q.bind(v),
            Param::I64(v) => q.bind(v),
            Param::F32(v) => q.bind(v as f64),
            Param::F64(v) => q.bind(v),
            Param::Str(v) => q.bind(v),
            Param::Bool(v) => q.bind(v),
            Param::Bytes(v) => q.bind(v),

            #[cfg(feature = "serde_json")]
            Param::Json(v) => q.bind(v),
            #[cfg(feature = "uuid")]
            Param::Uuid(v) => q.bind(v),
            #[cfg(feature = "rust_decimal")]
            Param::Decimal(v) => q.bind(v),

            Param::NullText => q.bind(None::<&str>),
            Param::NullBytes => q.bind(None::<Vec<u8>>),
            Param::NullBool => q.bind(None::<bool>),
            Param::NullI64 => q.bind(None::<i64>),
            Param::NullI32 => q.bind(None::<i32>),
            Param::NullF64 => q.bind(None::<f64>),

            #[cfg(feature = "time")]
            Param::Date(v) => q.bind(v),
            #[cfg(feature = "time")]
            Param::Time(v) => q.bind(v),
            #[cfg(feature = "time")]
            Param::DateTime(v) => q.bind(v),
            #[cfg(feature = "time")]
            Param::NaiveDateTime(v) => q.bind(v),

            #[cfg(feature = "time")]
            Param::NullDate => q.bind(None::<time::Date>),
            #[cfg(feature = "time")]
            Param::NullTime => q.bind(None::<time::Time>),
            #[cfg(feature = "time")]
            Param::NullDateTime => q.bind(None::<time::OffsetDateTime>),
            #[cfg(feature = "time")]
            Param::NullNaiveDateTime => q.bind(None::<time::PrimitiveDateTime>),

            #[cfg(feature = "chrono")]
            Param::ChronoNaiveDate(v) => q.bind(v),
            #[cfg(feature = "chrono")]
            Param::ChronoNaiveTime(v) => q.bind(v),
            #[cfg(feature = "chrono")]
            Param::ChronoNaiveDateTime(v) => q.bind(v),

            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveDate => q.bind(None::<chrono::NaiveDate>),
            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveTime => q.bind(None::<chrono::NaiveTime>),
            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveDateTime => q.bind(None::<chrono::NaiveDateTime>),
        };
    }
    Ok(q.execute(exec).await?.rows_affected())
}

// NEW: MySQL
#[cfg(feature = "mysql")]
pub async fn fetch_typed_mysql_exec<'e, E, T>(
    exec: E,
    sql: &str,
    params: Vec<Param>,
) -> Result<Vec<T>>
where
    E: sqlx::Executor<'e, Database = sqlx::MySql>,
    for<'r> T: sqlx::FromRow<'r, sqlx::mysql::MySqlRow> + Send + Unpin,
{
    let mut q = sqlx::query_as::<_, T>(sql);
    for p in params {
        q = match p {
            Param::I8(v) => q.bind(v),
            Param::I16(v) => q.bind(v),
            Param::I32(v) => q.bind(v),
            Param::I64(v) => q.bind(v),
            Param::F32(v) => q.bind(v as f64),
            Param::F64(v) => q.bind(v),
            Param::Str(v) => q.bind(v),
            Param::Bool(v) => q.bind(v),
            Param::Bytes(v) => q.bind(v),

            #[cfg(feature = "serde_json")]
            Param::Json(v) => q.bind(v),
            #[cfg(feature = "uuid")]
            Param::Uuid(v) => q.bind(v),
            #[cfg(feature = "rust_decimal")]
            Param::Decimal(v) => q.bind(v),

            // NULL-хинты
            Param::NullText => q.bind(None::<&str>),
            Param::NullBytes => q.bind(None::<Vec<u8>>),
            Param::NullBool => q.bind(None::<bool>),
            Param::NullI64 => q.bind(None::<i64>),
            Param::NullI32 => q.bind(None::<i32>),
            Param::NullF64 => q.bind(None::<f64>),
        };
    }
    Ok(q.fetch_all(exec).await?)
}

#[cfg(feature = "mysql")]
pub async fn execute_mysql_exec<'e, E>(exec: E, sql: &str, params: Vec<Param>) -> Result<u64>
where
    E: sqlx::Executor<'e, Database = sqlx::MySql>,
{
    let mut q = sqlx::query(sql);
    for p in params {
        q = match p {
            Param::I8(v) => q.bind(v),
            Param::I16(v) => q.bind(v),
            Param::I32(v) => q.bind(v),
            Param::I64(v) => q.bind(v),
            Param::F32(v) => q.bind(v as f64),
            Param::F64(v) => q.bind(v),
            Param::Str(v) => q.bind(v),
            Param::Bool(v) => q.bind(v),
            Param::Bytes(v) => q.bind(v),

            #[cfg(feature = "serde_json")]
            Param::Json(v) => q.bind(v),
            #[cfg(feature = "uuid")]
            Param::Uuid(v) => q.bind(v),
            #[cfg(feature = "rust_decimal")]
            Param::Decimal(v) => q.bind(v),

            Param::NullText => q.bind(None::<&str>),
            Param::NullBytes => q.bind(None::<Vec<u8>>),
            Param::NullBool => q.bind(None::<bool>),
            Param::NullI64 => q.bind(None::<i64>),
            Param::NullI32 => q.bind(None::<i32>),
            Param::NullF64 => q.bind(None::<f64>),
        };
    }
    Ok(q.execute(exec).await?.rows_affected())
}

// NEW: SQLite
#[cfg(feature = "sqlite")]
pub async fn fetch_typed_sqlite_exec<'e, E, T>(
    exec: E,
    sql: &str,
    params: Vec<Param>,
) -> Result<Vec<T>>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    for<'r> T: sqlx::FromRow<'r, sqlx::sqlite::SqliteRow> + Send + Unpin,
{
    let mut q = sqlx::query_as::<_, T>(sql);
    for p in params {
        q = match p {
            Param::I8(v) => q.bind(v),
            Param::I16(v) => q.bind(v),
            Param::I32(v) => q.bind(v),
            Param::I64(v) => q.bind(v),
            Param::F32(v) => q.bind(v as f64),
            Param::F64(v) => q.bind(v),
            Param::Str(v) => q.bind(v),
            Param::Bool(v) => q.bind(v),
            Param::Bytes(v) => q.bind(v),

            #[cfg(feature = "serde_json")]
            Param::Json(v) => q.bind(v),
            #[cfg(feature = "uuid")]
            Param::Uuid(v) => q.bind(v),
            #[cfg(feature = "rust_decimal")]
            Param::Decimal(v) => q.bind(v),

            Param::NullText => q.bind(None::<&str>),
            Param::NullBytes => q.bind(None::<Vec<u8>>),
            Param::NullBool => q.bind(None::<bool>),
            Param::NullI64 => q.bind(None::<i64>),
            Param::NullI32 => q.bind(None::<i32>),
            Param::NullF64 => q.bind(None::<f64>),
            Param::NullF32 => q.bind(None::<f32>),
            Param::NullI16 => q.bind(None::<i16>),
            Param::NullI8 => q.bind(None::<i8>),
        };
    }
    Ok(q.fetch_all(exec).await?)
}

#[cfg(feature = "sqlite")]
pub async fn execute_sqlite_exec<'e, E>(exec: E, sql: &str, params: Vec<Param>) -> Result<u64>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let mut q = sqlx::query(sql);
    for p in params {
        q = match p {
            Param::I8(v) => q.bind(v),
            Param::I16(v) => q.bind(v),
            Param::I32(v) => q.bind(v),
            Param::I64(v) => q.bind(v),
            Param::F32(v) => q.bind(v as f64),
            Param::F64(v) => q.bind(v),
            Param::Str(v) => q.bind(v),
            Param::Bool(v) => q.bind(v),
            Param::Bytes(v) => q.bind(v),

            #[cfg(feature = "serde_json")]
            Param::Json(v) => q.bind(v),
            #[cfg(feature = "uuid")]
            Param::Uuid(v) => q.bind(v),
            #[cfg(feature = "rust_decimal")]
            Param::Decimal(v) => q.bind(v),

            Param::NullText => q.bind(None::<&str>),
            Param::NullBytes => q.bind(None::<Vec<u8>>),
            Param::NullBool => q.bind(None::<bool>),
            Param::NullI64 => q.bind(None::<i64>),
            Param::NullI32 => q.bind(None::<i32>),
            Param::NullF64 => q.bind(None::<f64>),
            Param::NullF32 => q.bind(None::<f32>),
            Param::NullI16 => q.bind(None::<i16>),
            Param::NullI8 => q.bind(None::<i8>),
        };
    }
    Ok(q.execute(exec).await?.rows_affected())
}
