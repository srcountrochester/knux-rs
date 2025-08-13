#[cfg(feature = "postgres")]
use sqlx::{PgPool, postgres::PgRow};

#[cfg(feature = "mysql")]
use sqlx::{MySqlPool, mysql::MySqlRow};

#[cfg(feature = "sqlite")]
use sqlx::{SqlitePool, sqlite::SqliteRow};

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
    let mut q = sqlx::query_as::<_, T>(sql);
    for p in params {
        q = match p {
            // ints / floats (f32 -> f64)
            Param::I8(v) => q.bind(v),
            Param::I16(v) => q.bind(v),
            Param::I32(v) => q.bind(v),
            Param::I64(v) => q.bind(v),
            Param::F32(v) => q.bind(v as f64),
            Param::F64(v) => q.bind(v),

            // primitives
            Param::Str(v) => q.bind(v),
            Param::Bool(v) => q.bind(v),
            Param::Bytes(v) => q.bind(v),

            // time / chrono
            #[cfg(feature = "time")]
            Param::Date(v) => q.bind(v),
            #[cfg(feature = "time")]
            Param::Time(v) => q.bind(v),
            #[cfg(feature = "time")]
            Param::DateTime(v) => q.bind(v),
            #[cfg(feature = "time")]
            Param::NaiveDateTime(v) => q.bind(v),

            #[cfg(feature = "chrono")]
            Param::ChronoNaiveDate(v) => q.bind(v),
            #[cfg(feature = "chrono")]
            Param::ChronoNaiveTime(v) => q.bind(v),
            #[cfg(feature = "chrono")]
            Param::ChronoNaiveDateTime(v) => q.bind(v),
            #[cfg(feature = "chrono")]
            Param::ChronoDateTimeUtc(v) => q.bind(v),
            #[cfg(feature = "chrono")]
            Param::ChronoDateTimeFixed(v) => q.bind(v),

            // json / uuid / decimal
            #[cfg(feature = "serde_json")]
            Param::Json(v) => q.bind(v),
            #[cfg(feature = "uuid")]
            Param::Uuid(v) => q.bind(v),
            #[cfg(feature = "rust_decimal")]
            Param::Decimal(v) => q.bind(v),

            // NULL hints (конкретный тип!)
            Param::NullText => q.bind(None::<&str>),
            Param::NullBytes => q.bind(None::<Vec<u8>>),
            Param::NullBool => q.bind(None::<bool>),
            Param::NullI64 => q.bind(None::<i64>),
            Param::NullI32 => q.bind(None::<i32>),
            Param::NullI16 => q.bind(None::<i16>),
            Param::NullI8 => q.bind(None::<i8>),
            Param::NullF64 => q.bind(None::<f64>),
            Param::NullF32 => q.bind(None::<f64>),

            #[cfg(feature = "time")]
            Param::NullDate => q.bind(None::<time::Date>),
            #[cfg(feature = "time")]
            Param::NullTime => q.bind(None::<time::Time>),
            #[cfg(feature = "time")]
            Param::NullDateTime => q.bind(None::<time::OffsetDateTime>),
            #[cfg(feature = "time")]
            Param::NullNaiveDateTime => q.bind(None::<time::PrimitiveDateTime>),

            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveDate => q.bind(None::<chrono::NaiveDate>),
            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveTime => q.bind(None::<chrono::NaiveTime>),
            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveDateTime => q.bind(None::<chrono::NaiveDateTime>),
            #[cfg(feature = "chrono")]
            Param::NullChronoDateTimeUtc => q.bind(None::<chrono::DateTime<chrono::Utc>>),
            #[cfg(feature = "chrono")]
            Param::NullChronoDateTimeFixed => q.bind(None::<chrono::DateTime<chrono::FixedOffset>>),

            #[cfg(feature = "serde_json")]
            Param::NullJson => q.bind(None::<serde_json::Value>),
            #[cfg(feature = "uuid")]
            Param::NullUuid => q.bind(None::<uuid::Uuid>),
            #[cfg(feature = "rust_decimal")]
            Param::NullDecimal => q.bind(None::<rust_decimal::Decimal>),
        };
    }
    Ok(q.fetch_all(pool).await?)
}

#[cfg(feature = "mysql")]
pub async fn fetch_typed_mysql<T>(pool: &MySqlPool, sql: &str, params: Vec<Param>) -> Result<Vec<T>>
where
    for<'r> T: sqlx::FromRow<'r, MySqlRow> + Send + Unpin,
{
    // точь-в-точь как в pg; оставляю одинаковую реализацию
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
            #[cfg(feature = "time")]
            Param::Date(v) => q.bind(v),
            #[cfg(feature = "time")]
            Param::Time(v) => q.bind(v),
            #[cfg(feature = "time")]
            Param::DateTime(v) => q.bind(v),
            #[cfg(feature = "time")]
            Param::NaiveDateTime(v) => q.bind(v),
            #[cfg(feature = "chrono")]
            Param::ChronoNaiveDate(v) => q.bind(v),
            #[cfg(feature = "chrono")]
            Param::ChronoNaiveTime(v) => q.bind(v),
            #[cfg(feature = "chrono")]
            Param::ChronoNaiveDateTime(v) => q.bind(v),
            #[cfg(feature = "chrono")]
            Param::ChronoDateTimeUtc(v) => q.bind(v),
            #[cfg(feature = "chrono")]
            Param::ChronoDateTimeFixed(v) => q.bind(v),
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
            Param::NullI16 => q.bind(None::<i16>),
            Param::NullI8 => q.bind(None::<i8>),
            Param::NullF64 => q.bind(None::<f64>),
            Param::NullF32 => q.bind(None::<f64>),
            #[cfg(feature = "time")]
            Param::NullDate => q.bind(None::<time::Date>),
            #[cfg(feature = "time")]
            Param::NullTime => q.bind(None::<time::Time>),
            #[cfg(feature = "time")]
            Param::NullDateTime => q.bind(None::<time::OffsetDateTime>),
            #[cfg(feature = "time")]
            Param::NullNaiveDateTime => q.bind(None::<time::PrimitiveDateTime>),
            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveDate => q.bind(None::<chrono::NaiveDate>),
            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveTime => q.bind(None::<chrono::NaiveTime>),
            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveDateTime => q.bind(None::<chrono::NaiveDateTime>),
            #[cfg(feature = "chrono")]
            Param::NullChronoDateTimeUtc => q.bind(None::<chrono::DateTime<chrono::Utc>>),
            #[cfg(feature = "chrono")]
            Param::NullChronoDateTimeFixed => q.bind(None::<chrono::DateTime<chrono::FixedOffset>>),
            #[cfg(feature = "serde_json")]
            Param::NullJson => q.bind(None::<serde_json::Value>),
            #[cfg(feature = "uuid")]
            Param::NullUuid => q.bind(None::<uuid::Uuid>),
            #[cfg(feature = "rust_decimal")]
            Param::NullDecimal => q.bind(None::<rust_decimal::Decimal>),
        };
    }
    Ok(q.fetch_all(pool).await?)
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
    let mut q = sqlx::query_as::<_, T>(sql);
    for p in params {
        q = match p {
            Param::I8(v) => q.bind(v),
            Param::I16(v) => q.bind(v),
            Param::I32(v) => q.bind(v),
            Param::I64(v) => q.bind(v),
            Param::F32(v) => q.bind(v as f64), // обязательно!
            Param::F64(v) => q.bind(v),

            Param::Str(v) => q.bind(v),
            Param::Bool(v) => q.bind(v),
            Param::Bytes(v) => q.bind(v),

            #[cfg(feature = "time")]
            Param::Date(v) => q.bind(v),
            #[cfg(feature = "time")]
            Param::Time(v) => q.bind(v),
            #[cfg(feature = "time")]
            Param::DateTime(v) => q.bind(v),
            #[cfg(feature = "time")]
            Param::NaiveDateTime(v) => q.bind(v),

            #[cfg(feature = "chrono")]
            Param::ChronoNaiveDate(v) => q.bind(v),
            #[cfg(feature = "chrono")]
            Param::ChronoNaiveTime(v) => q.bind(v),
            #[cfg(feature = "chrono")]
            Param::ChronoNaiveDateTime(v) => q.bind(v),
            #[cfg(feature = "chrono")]
            Param::ChronoDateTimeUtc(v) => q.bind(v),
            #[cfg(feature = "chrono")]
            Param::ChronoDateTimeFixed(v) => q.bind(v),

            #[cfg(feature = "serde_json")]
            Param::Json(v) => q.bind(v),
            #[cfg(feature = "uuid")]
            Param::Uuid(v) => q.bind(v),
            #[cfg(feature = "rust_decimal")]
            Param::Decimal(v) => q.bind(v),

            // NULLы (SQLite чаще всего «съедает» &str)
            Param::NullText => q.bind(None::<&str>),
            Param::NullBytes => q.bind(None::<Vec<u8>>),
            Param::NullBool => q.bind(None::<bool>),
            Param::NullI64 => q.bind(None::<i64>),
            Param::NullI32 => q.bind(None::<i32>),
            Param::NullI16 => q.bind(None::<i16>),
            Param::NullI8 => q.bind(None::<i8>),
            Param::NullF64 => q.bind(None::<f64>),
            Param::NullF32 => q.bind(None::<f64>),

            #[cfg(feature = "time")]
            Param::NullDate => q.bind(None::<time::Date>),
            #[cfg(feature = "time")]
            Param::NullTime => q.bind(None::<time::Time>),
            #[cfg(feature = "time")]
            Param::NullDateTime => q.bind(None::<time::OffsetDateTime>),
            #[cfg(feature = "time")]
            Param::NullNaiveDateTime => q.bind(None::<time::PrimitiveDateTime>),

            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveDate => q.bind(None::<chrono::NaiveDate>),
            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveTime => q.bind(None::<chrono::NaiveTime>),
            #[cfg(feature = "chrono")]
            Param::NullChronoNaiveDateTime => q.bind(None::<chrono::NaiveDateTime>),
            #[cfg(feature = "chrono")]
            Param::NullChronoDateTimeUtc => q.bind(None::<chrono::DateTime<chrono::Utc>>),
            #[cfg(feature = "chrono")]
            Param::NullChronoDateTimeFixed => q.bind(None::<chrono::DateTime<chrono::FixedOffset>>),

            #[cfg(feature = "serde_json")]
            Param::NullJson => q.bind(None::<serde_json::Value>),
            #[cfg(feature = "uuid")]
            Param::NullUuid => q.bind(None::<uuid::Uuid>),
            #[cfg(feature = "rust_decimal")]
            Param::NullDecimal => q.bind(None::<rust_decimal::Decimal>),
        };
    }
    Ok(q.fetch_all(pool).await?)
}
