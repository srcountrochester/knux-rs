#[derive(Debug, Clone)]
pub enum Param {
    // целые
    I64(i64),
    I32(i32),
    I16(i16),
    I8(i8),

    // числа с плавающей
    F64(f64),
    F32(f32),

    Bool(bool),

    // строки/байты
    Str(String),
    Bytes(Vec<u8>),

    // ---- даты/время (features) ----
    // time
    #[cfg(feature = "time")]
    Date(time::Date),
    #[cfg(feature = "time")]
    Time(time::Time),
    #[cfg(feature = "time")]
    DateTime(time::OffsetDateTime), // с TZ
    #[cfg(feature = "time")]
    NaiveDateTime(time::PrimitiveDateTime), // без TZ

    // chrono
    #[cfg(feature = "chrono")]
    ChronoNaiveDate(chrono::NaiveDate),
    #[cfg(feature = "chrono")]
    ChronoNaiveTime(chrono::NaiveTime),
    #[cfg(feature = "chrono")]
    ChronoNaiveDateTime(chrono::NaiveDateTime),
    #[cfg(feature = "chrono")]
    ChronoDateTimeUtc(chrono::DateTime<chrono::Utc>),
    #[cfg(feature = "chrono")]
    ChronoDateTimeFixed(chrono::DateTime<chrono::FixedOffset>),

    // JSON / UUID / DECIMAL — опционально
    #[cfg(feature = "serde_json")]
    Json(serde_json::Value),
    #[cfg(feature = "uuid")]
    Uuid(uuid::Uuid),
    #[cfg(feature = "rust_decimal")]
    Decimal(rust_decimal::Decimal),

    // ---- NULL c типовым намёком ----
    NullText,
    NullBytes,
    NullBool,
    NullI64,
    NullI32,
    NullI16,
    NullI8,
    NullF64,
    NullF32,

    #[cfg(feature = "time")]
    NullDate,
    #[cfg(feature = "time")]
    NullTime,
    #[cfg(feature = "time")]
    NullDateTime,
    #[cfg(feature = "time")]
    NullNaiveDateTime,

    #[cfg(feature = "chrono")]
    NullChronoNaiveDate,
    #[cfg(feature = "chrono")]
    NullChronoNaiveTime,
    #[cfg(feature = "chrono")]
    NullChronoNaiveDateTime,
    #[cfg(feature = "chrono")]
    NullChronoDateTimeUtc,
    #[cfg(feature = "chrono")]
    NullChronoDateTimeFixed,

    #[cfg(feature = "serde_json")]
    NullJson,
    #[cfg(feature = "uuid")]
    NullUuid,
    #[cfg(feature = "rust_decimal")]
    NullDecimal,
}

// ---- From impls ----
impl From<i8> for Param {
    fn from(v: i8) -> Self {
        Param::I8(v)
    }
}
impl From<i16> for Param {
    fn from(v: i16) -> Self {
        Param::I16(v)
    }
}
impl From<i32> for Param {
    fn from(v: i32) -> Self {
        Param::I32(v)
    }
}
impl From<i64> for Param {
    fn from(v: i64) -> Self {
        Param::I64(v)
    }
}

impl From<f32> for Param {
    fn from(v: f32) -> Self {
        Param::F32(v)
    }
}
impl From<f64> for Param {
    fn from(v: f64) -> Self {
        Param::F64(v)
    }
}

impl From<bool> for Param {
    fn from(v: bool) -> Self {
        Param::Bool(v)
    }
}

impl From<&str> for Param {
    fn from(v: &str) -> Self {
        Param::Str(v.to_string())
    }
}
impl From<String> for Param {
    fn from(v: String) -> Self {
        Param::Str(v)
    }
}
impl From<&String> for Param {
    fn from(v: &String) -> Self {
        Param::Str(v.clone())
    }
}
impl<'a> From<std::borrow::Cow<'a, str>> for Param {
    fn from(v: std::borrow::Cow<'a, str>) -> Self {
        Param::Str(v.into_owned())
    }
}

impl From<Vec<u8>> for Param {
    fn from(v: Vec<u8>) -> Self {
        Param::Bytes(v)
    }
}
impl From<&[u8]> for Param {
    fn from(v: &[u8]) -> Self {
        Param::Bytes(v.to_vec())
    }
}

#[cfg(feature = "time")]
impl From<time::Date> for Param {
    fn from(v: time::Date) -> Self {
        Param::Date(v)
    }
}
#[cfg(feature = "time")]
impl From<time::Time> for Param {
    fn from(v: time::Time) -> Self {
        Param::Time(v)
    }
}
#[cfg(feature = "time")]
impl From<time::OffsetDateTime> for Param {
    fn from(v: time::OffsetDateTime) -> Self {
        Param::DateTime(v)
    }
}
#[cfg(feature = "time")]
impl From<time::PrimitiveDateTime> for Param {
    fn from(v: time::PrimitiveDateTime) -> Self {
        Param::NaiveDateTime(v)
    }
}

#[cfg(feature = "chrono")]
impl From<chrono::NaiveDate> for Param {
    fn from(v: chrono::NaiveDate) -> Self {
        Param::ChronoNaiveDate(v)
    }
}
#[cfg(feature = "chrono")]
impl From<chrono::NaiveTime> for Param {
    fn from(v: chrono::NaiveTime) -> Self {
        Param::ChronoNaiveTime(v)
    }
}
#[cfg(feature = "chrono")]
impl From<chrono::NaiveDateTime> for Param {
    fn from(v: chrono::NaiveDateTime) -> Self {
        Param::ChronoNaiveDateTime(v)
    }
}
#[cfg(feature = "chrono")]
impl From<chrono::DateTime<chrono::Utc>> for Param {
    fn from(v: chrono::DateTime<chrono::Utc>) -> Self {
        Param::ChronoDateTimeUtc(v)
    }
}
#[cfg(feature = "chrono")]
impl From<chrono::DateTime<chrono::FixedOffset>> for Param {
    fn from(v: chrono::DateTime<chrono::FixedOffset>) -> Self {
        Param::ChronoDateTimeFixed(v)
    }
}

#[cfg(feature = "serde_json")]
impl From<serde_json::Value> for Param {
    fn from(v: serde_json::Value) -> Self {
        Param::Json(v)
    }
}
#[cfg(feature = "uuid")]
impl From<uuid::Uuid> for Param {
    fn from(v: uuid::Uuid) -> Self {
        Param::Uuid(v)
    }
}
#[cfg(feature = "rust_decimal")]
impl From<rust_decimal::Decimal> for Param {
    fn from(v: rust_decimal::Decimal) -> Self {
        Param::Decimal(v)
    }
}
