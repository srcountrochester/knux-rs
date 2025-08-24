#[cfg(feature = "postgres")]
use sqlx::{Arguments, PgPool, postgres::PgRow};

#[cfg(feature = "mysql")]
use sqlx::{Arguments, MySqlPool, mysql::MySqlRow};

#[cfg(feature = "sqlite")]
use sqlx::{Arguments, SqlitePool, sqlite::SqliteRow};

use std::{borrow::Cow, time::Duration};

use crate::param::Param;

use super::{Error, Result};

pub fn parse_u32(v: &str, key: &str) -> Result<u32> {
    v.parse::<u32>().map_err(|_| Error::InvalidInt {
        key: Cow::Owned(key.to_string()),
        value: v.to_string(),
    })
}
pub fn parse_bool(v: &str, key: &str) -> Result<bool> {
    match v {
        "1" | "true" | "TRUE" => Ok(true),
        "0" | "false" | "FALSE" => Ok(false),
        _ => Err(Error::InvalidBool {
            key: Cow::Owned(key.to_string()),
            value: v.to_string(),
        }),
    }
}
pub fn parse_duration(v: &str, key: &str) -> Result<Duration> {
    humantime::parse_duration(v).map_err(|_| Error::InvalidDuration {
        key: Cow::Owned(key.to_string()),
        value: v.to_string(),
    })
}

#[cfg(feature = "postgres")]
pub async fn fetch_typed_pg<T>(pool: &PgPool, sql: &str, params: Vec<Param>) -> Result<Vec<T>>
where
    for<'r> T: sqlx::FromRow<'r, PgRow> + Send + Unpin,
{
    let q = build_query_as_pg::<T>(sql, params);
    Ok(q.fetch_all(pool).await?)
}

#[cfg(feature = "postgres")]
pub async fn fetch_one_typed_pg<T>(pool: &PgPool, sql: &str, params: Vec<Param>) -> Result<T>
where
    for<'r> T: sqlx::FromRow<'r, PgRow> + Send + Unpin,
{
    let q = build_query_as_pg::<T>(sql, params);
    Ok(q.fetch_one(pool).await?)
}

#[cfg(feature = "postgres")]
pub async fn fetch_optional_typed_pg<T>(
    pool: &PgPool,
    sql: &str,
    params: Vec<Param>,
) -> Result<Option<T>>
where
    for<'r> T: sqlx::FromRow<'r, PgRow> + Send + Unpin,
{
    let q = build_query_as_pg::<T>(sql, params);
    Ok(q.fetch_optional(pool).await?)
}

#[cfg(feature = "mysql")]
pub async fn fetch_typed_mysql<T>(pool: &MySqlPool, sql: &str, params: Vec<Param>) -> Result<Vec<T>>
where
    for<'r> T: sqlx::FromRow<'r, MySqlRow> + Send + Unpin,
{
    let q = build_query_as_mysql::<T>(sql, params);
    Ok(q.fetch_all(pool).await?)
}

#[cfg(feature = "mysql")]
pub async fn fetch_one_typed_mysql<T>(pool: &MySqlPool, sql: &str, params: Vec<Param>) -> Result<T>
where
    for<'r> T: sqlx::FromRow<'r, MySqlRow> + Send + Unpin,
{
    let q = build_query_as_mysql::<T>(sql, params);
    Ok(q.fetch_one(pool).await?)
}

#[cfg(feature = "mysql")]
pub async fn fetch_optional_typed_mysql<T>(
    pool: &MySqlPool,
    sql: &str,
    params: Vec<Param>,
) -> Result<Option<T>>
where
    for<'r> T: sqlx::FromRow<'r, MySqlRow> + Send + Unpin,
{
    let q = build_query_as_mysql::<T>(sql, params);
    Ok(q.fetch_optional(pool).await?)
}

#[cfg(feature = "sqlite")]
pub async fn fetch_typed_sqlite<T>(
    pool: &SqlitePool,
    sql: &str,
    params: Vec<Param>,
) -> Result<Vec<T>>
where
    for<'r> T: sqlx::FromRow<'r, SqliteRow> + Send + Unpin,
{
    let q = build_query_as_sqlite::<T>(sql, params);
    Ok(q.fetch_all(pool).await?)
}

#[cfg(feature = "sqlite")]
pub async fn fetch_one_typed_sqlite<T>(
    pool: &SqlitePool,
    sql: &str,
    params: Vec<Param>,
) -> Result<T>
where
    for<'r> T: sqlx::FromRow<'r, SqliteRow> + Send + Unpin,
{
    let q = build_query_as_sqlite::<T>(sql, params);
    Ok(q.fetch_one(pool).await?)
}

#[cfg(feature = "sqlite")]
pub async fn fetch_optional_typed_sqlite<T>(
    pool: &SqlitePool,
    sql: &str,
    params: Vec<Param>,
) -> Result<Option<T>>
where
    for<'r> T: sqlx::FromRow<'r, SqliteRow> + Send + Unpin,
{
    let q = build_query_as_sqlite::<T>(sql, params);
    Ok(q.fetch_optional(pool).await?)
}

#[cfg(feature = "sqlite")]
pub async fn execute_sqlite(pool: &SqlitePool, sql: &str, params: Vec<Param>) -> Result<u64> {
    let args = build_sqlite_args(params);
    let res = sqlx::query_with(sql, args).execute(pool).await?;
    Ok(res.rows_affected())
}

#[cfg(feature = "postgres")]
pub async fn execute_pg(pool: &PgPool, sql: &str, params: Vec<Param>) -> Result<u64> {
    let args = build_pg_args(params);
    let res = sqlx::query_with(sql, args).execute(pool).await?;
    Ok(res.rows_affected())
}

#[cfg(feature = "mysql")]
pub async fn execute_mysql(pool: &MySqlPool, sql: &str, params: Vec<Param>) -> Result<u64> {
    let args = build_mysql_args(params);
    let res = sqlx::query_with(sql, args).execute(pool).await?;
    Ok(res.rows_affected())
}

#[cfg(feature = "sqlite")]
pub(crate) fn build_query_as_sqlite<'q, T>(
    sql: &'q str,
    params: Vec<Param>,
) -> sqlx::query::QueryAs<'q, sqlx::Sqlite, T, sqlx::sqlite::SqliteArguments<'q>>
where
    for<'r> T: sqlx::FromRow<'r, SqliteRow> + Send + Unpin,
{
    let args = build_sqlite_args(params);
    sqlx::query_as_with::<_, T, _>(sql, args)
}

#[cfg(feature = "postgres")]
pub(crate) fn build_query_as_pg<'q, T>(
    sql: &'q str,
    params: Vec<Param>,
) -> sqlx::query::QueryAs<'q, sqlx::Postgres, T, sqlx::postgres::PgArguments>
where
    for<'r> T: sqlx::FromRow<'r, PgRow> + Send + Unpin,
{
    let args = build_pg_args(params);
    sqlx::query_as_with::<_, T, _>(sql, args)
}

#[cfg(feature = "mysql")]
pub(crate) fn build_query_as_mysql<'q, T>(
    sql: &'q str,
    params: Vec<Param>,
) -> sqlx::query::QueryAs<'q, sqlx::MySql, T, sqlx::mysql::MySqlArguments>
where
    for<'r> T: sqlx::FromRow<'r, MySqlRow> + Send + Unpin,
{
    let args = build_mysql_args(params);
    sqlx::query_as_with::<_, T, _>(sql, args)
}

#[cfg(feature = "sqlite")]
pub(crate) fn build_sqlite_args<'q>(params: Vec<Param>) -> sqlx::sqlite::SqliteArguments<'q> {
    use sqlx::sqlite::SqliteArguments;
    let mut args = SqliteArguments::default();
    for p in params {
        match p {
            Param::I8(v) => args.add(v).unwrap(),
            Param::I16(v) => args.add(v).unwrap(),
            Param::I32(v) => args.add(v).unwrap(),
            Param::I64(v) => args.add(v).unwrap(),
            Param::F32(v) => args.add(v as f64).unwrap(),
            Param::F64(v) => args.add(v).unwrap(),

            Param::Str(v) => args.add(v).unwrap(),
            Param::Bool(v) => args.add(v).unwrap(),
            Param::Bytes(v) => args.add(v).unwrap(),

            #[cfg(feature = "time")]
            Param::Date(v) => args.add(v).unwrap(),
            #[cfg(feature = "time")]
            Param::Time(v) => args.add(v).unwrap(),
            #[cfg(feature = "time")]
            Param::DateTime(v) => args.add(v).unwrap(),
            #[cfg(feature = "time")]
            Param::NaiveDateTime(v) => args.add(v).unwrap(),

            #[cfg(feature = "chrono")]
            Param::ChronoNaiveDate(v) => args.add(v).unwrap(),
            #[cfg(feature = "chrono")]
            Param::ChronoNaiveTime(v) => args.add(v).unwrap(),
            #[cfg(feature = "chrono")]
            Param::ChronoNaiveDateTime(v) => args.add(v).unwrap(),
            #[cfg(feature = "chrono")]
            Param::ChronoDateTimeUtc(v) => args.add(v).unwrap(),
            #[cfg(feature = "chrono")]
            Param::ChronoDateTimeFixed(v) => args.add(v).unwrap(),

            #[cfg(feature = "serde_json")]
            Param::Json(v) => args.add(v).unwrap(),
            #[cfg(feature = "uuid")]
            Param::Uuid(v) => args.add(v).unwrap(),
            #[cfg(feature = "rust_decimal")]
            Param::Decimal(v) => args.add(v).unwrap(),

            Param::NullText => args.add(Option::<&str>::None).unwrap(),
            Param::NullBytes => args.add(Option::<Vec<u8>>::None).unwrap(),
            Param::NullBool => args.add(Option::<bool>::None).unwrap(),
            Param::NullI64 => args.add(Option::<i64>::None).unwrap(),
            Param::NullI32 => args.add(Option::<i32>::None).unwrap(),
            Param::NullI16 => args.add(Option::<i16>::None).unwrap(),
            Param::NullI8 => args.add(Option::<i8>::None).unwrap(),
            Param::NullF64 => args.add(Option::<f64>::None).unwrap(),
            Param::NullF32 => args.add(Option::<f64>::None).unwrap(),

            #[cfg(feature = "time")]
            Param::NullDate => args.add(Option::<time::Date>::None).unwrap(),
            #[cfg(feature = "time")]
            Param::NullTime => args.add(Option::<time::Time>::None).unwrap(),
            #[cfg(feature = "time")]
            Param::NullDateTime => args.add(Option::<time::OffsetDateTime>::None).unwrap(),
            #[cfg(feature = "time")]
            Param::NullNaiveDateTime => args.add(Option::<time::PrimitiveDateTime>::None).unwrap(),

            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveDate => args.add(Option::<chrono::NaiveDate>::None).unwrap(),
            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveTime => args.add(Option::<chrono::NaiveTime>::None).unwrap(),
            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveDateTime => {
                args.add(Option::<chrono::NaiveDateTime>::None).unwrap()
            }
            #[cfg(feature = "chrono")]
            Param::NullChronoDateTimeUtc => args
                .add(Option::<chrono::DateTime<chrono::Utc>>::None)
                .unwrap(),
            #[cfg(feature = "chrono")]
            Param::NullChronoDateTimeFixed => args
                .add(Option::<chrono::DateTime<chrono::FixedOffset>>::None)
                .unwrap(),

            #[cfg(feature = "serde_json")]
            Param::NullJson => args.add(Option::<serde_json::Value>::None).unwrap(),
            #[cfg(feature = "uuid")]
            Param::NullUuid => args.add(Option::<uuid::Uuid>::None).unwrap(),
            #[cfg(feature = "rust_decimal")]
            Param::NullDecimal => args.add(Option::<rust_decimal::Decimal>::None).unwrap(),
        }
    }
    args
}

#[cfg(feature = "postgres")]
pub(crate) fn build_pg_args<'q>(params: Vec<Param>) -> sqlx::postgres::PgArguments {
    use sqlx::postgres::PgArguments;
    let mut args = PgArguments::default();
    for p in params {
        match p {
            Param::I8(v) => args.add(v).unwrap(),
            Param::I16(v) => args.add(v).unwrap(),
            Param::I32(v) => args.add(v).unwrap(),
            Param::I64(v) => args.add(v).unwrap(),
            Param::F32(v) => args.add(v as f64).unwrap(),
            Param::F64(v) => args.add(v).unwrap(),
            Param::Str(v) => args.add(v).unwrap(),
            Param::Bool(v) => args.add(v).unwrap(),
            Param::Bytes(v) => args.add(v).unwrap(),
            #[cfg(feature = "time")]
            Param::Date(v) => args.add(v).unwrap(),
            #[cfg(feature = "time")]
            Param::Time(v) => args.add(v).unwrap(),
            #[cfg(feature = "time")]
            Param::DateTime(v) => args.add(v).unwrap(),
            #[cfg(feature = "time")]
            Param::NaiveDateTime(v) => args.add(v).unwrap(),
            #[cfg(feature = "chrono")]
            Param::ChronoNaiveDate(v) => args.add(v).unwrap(),
            #[cfg(feature = "chrono")]
            Param::ChronoNaiveTime(v) => args.add(v).unwrap(),
            #[cfg(feature = "chrono")]
            Param::ChronoNaiveDateTime(v) => args.add(v).unwrap(),
            #[cfg(feature = "chrono")]
            Param::ChronoDateTimeUtc(v) => args.add(v).unwrap(),
            #[cfg(feature = "chrono")]
            Param::ChronoDateTimeFixed(v) => args.add(v).unwrap(),
            #[cfg(feature = "serde_json")]
            Param::Json(v) => args.add(v).unwrap(),
            #[cfg(feature = "uuid")]
            Param::Uuid(v) => args.add(v).unwrap(),
            #[cfg(feature = "rust_decimal")]
            Param::Decimal(v) => args.add(v).unwrap(),
            Param::NullText => args.add(Option::<&str>::None).unwrap(),
            Param::NullBytes => args.add(Option::<Vec<u8>>::None).unwrap(),
            Param::NullBool => args.add(Option::<bool>::None).unwrap(),
            Param::NullI64 => args.add(Option::<i64>::None).unwrap(),
            Param::NullI32 => args.add(Option::<i32>::None).unwrap(),
            Param::NullI16 => args.add(Option::<i16>::None).unwrap(),
            Param::NullI8 => args.add(Option::<i8>::None).unwrap(),
            Param::NullF64 => args.add(Option::<f64>::None).unwrap(),
            Param::NullF32 => args.add(Option::<f64>::None).unwrap(),
            #[cfg(feature = "time")]
            Param::NullDate => args.add(Option::<time::Date>::None).unwrap(),
            #[cfg(feature = "time")]
            Param::NullTime => args.add(Option::<time::Time>::None).unwrap(),
            #[cfg(feature = "time")]
            Param::NullDateTime => args.add(Option::<time::OffsetDateTime>::None).unwrap(),
            #[cfg(feature = "time")]
            Param::NullNaiveDateTime => args.add(Option::<time::PrimitiveDateTime>::None).unwrap(),
            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveDate => args.add(Option::<chrono::NaiveDate>::None).unwrap(),
            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveTime => args.add(Option::<chrono::NaiveTime>::None).unwrap(),
            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveDateTime => {
                args.add(Option::<chrono::NaiveDateTime>::None).unwrap()
            }
            #[cfg(feature = "chrono")]
            Param::NullChronoDateTimeUtc => args
                .add(Option::<chrono::DateTime<chrono::Utc>>::None)
                .unwrap(),
            #[cfg(feature = "chrono")]
            Param::NullChronoDateTimeFixed => args
                .add(Option::<chrono::DateTime<chrono::FixedOffset>>::None)
                .unwrap(),
            #[cfg(feature = "serde_json")]
            Param::NullJson => args.add(Option::<serde_json::Value>::None).unwrap(),
            #[cfg(feature = "uuid")]
            Param::NullUuid => args.add(Option::<uuid::Uuid>::None).unwrap(),
            #[cfg(feature = "rust_decimal")]
            Param::NullDecimal => args.add(Option::<rust_decimal::Decimal>::None).unwrap(),
        }
    }
    args
}

#[cfg(feature = "mysql")]
pub(crate) fn build_mysql_args<'q>(params: Vec<Param>) -> sqlx::mysql::MySqlArguments {
    use sqlx::mysql::MySqlArguments;
    let mut args = MySqlArguments::default();
    for p in params {
        match p {
            Param::I8(v) => args.add(v).unwrap(),
            Param::I16(v) => args.add(v).unwrap(),
            Param::I32(v) => args.add(v).unwrap(),
            Param::I64(v) => args.add(v).unwrap(),
            Param::F32(v) => args.add(v as f64).unwrap(),
            Param::F64(v) => args.add(v).unwrap(),

            Param::Str(v) => args.add(v).unwrap(),
            Param::Bool(v) => args.add(v).unwrap(),
            Param::Bytes(v) => args.add(v).unwrap(),

            #[cfg(feature = "time")]
            Param::Date(v) => args.add(v).unwrap(),
            #[cfg(feature = "time")]
            Param::Time(v) => args.add(v).unwrap(),
            #[cfg(feature = "time")]
            Param::DateTime(v) => args.add(v).unwrap(),
            #[cfg(feature = "time")]
            Param::NaiveDateTime(v) => args.add(v).unwrap(),

            #[cfg(feature = "chrono")]
            Param::ChronoNaiveDate(v) => args.add(v).unwrap(),
            #[cfg(feature = "chrono")]
            Param::ChronoNaiveTime(v) => args.add(v).unwrap(),
            #[cfg(feature = "chrono")]
            Param::ChronoNaiveDateTime(v) => args.add(v).unwrap(),
            #[cfg(feature = "chrono")]
            Param::ChronoDateTimeUtc(v) => args.add(v).unwrap(),
            #[cfg(feature = "chrono")]
            Param::ChronoDateTimeFixed(v) => args.add(v).unwrap(),

            #[cfg(feature = "serde_json")]
            Param::Json(v) => args.add(v).unwrap(),
            #[cfg(feature = "uuid")]
            Param::Uuid(v) => args.add(v).unwrap(),
            #[cfg(feature = "rust_decimal")]
            Param::Decimal(v) => args.add(v).unwrap(),

            Param::NullText => args.add(Option::<&str>::None).unwrap(),
            Param::NullBytes => args.add(Option::<Vec<u8>>::None).unwrap(),
            Param::NullBool => args.add(Option::<bool>::None).unwrap(),
            Param::NullI64 => args.add(Option::<i64>::None).unwrap(),
            Param::NullI32 => args.add(Option::<i32>::None).unwrap(),
            Param::NullI16 => args.add(Option::<i16>::None).unwrap(),
            Param::NullI8 => args.add(Option::<i8>::None).unwrap(),
            Param::NullF64 => args.add(Option::<f64>::None).unwrap(),
            Param::NullF32 => args.add(Option::<f64>::None).unwrap(),

            #[cfg(feature = "time")]
            Param::NullDate => args.add(Option::<time::Date>::None).unwrap(),
            #[cfg(feature = "time")]
            Param::NullTime => args.add(Option::<time::Time>::None).unwrap(),
            #[cfg(feature = "time")]
            Param::NullDateTime => args.add(Option::<time::OffsetDateTime>::None).unwrap(),
            #[cfg(feature = "time")]
            Param::NullNaiveDateTime => args.add(Option::<time::PrimitiveDateTime>::None).unwrap(),

            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveDate => args.add(Option::<chrono::NaiveDate>::None).unwrap(),
            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveTime => args.add(Option::<chrono::NaiveTime>::None).unwrap(),
            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveDateTime => {
                args.add(Option::<chrono::NaiveDateTime>::None).unwrap()
            }
            #[cfg(feature = "chrono")]
            Param::NullChronoDateTimeUtc => args
                .add(Option::<chrono::DateTime<chrono::Utc>>::None)
                .unwrap(),
            #[cfg(feature = "chrono")]
            Param::NullChronoDateTimeFixed => args
                .add(Option::<chrono::DateTime<chrono::FixedOffset>>::None)
                .unwrap(),

            #[cfg(feature = "serde_json")]
            Param::NullJson => args.add(Option::<serde_json::Value>::None).unwrap(),
            #[cfg(feature = "uuid")]
            Param::NullUuid => args.add(Option::<uuid::Uuid>::None).unwrap(),
            #[cfg(feature = "rust_decimal")]
            Param::NullDecimal => args.add(Option::<rust_decimal::Decimal>::None).unwrap(),
        }
    }
    args
}
