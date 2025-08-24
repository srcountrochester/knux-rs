use std::borrow::Cow;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("No database_url or pool was provided")]
    MissingConnection,

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error(transparent)]
    QueryBuild(#[from] crate::query_builder::Error),

    #[error("Invalid DSN URL: {0}")]
    InvalidUrl(url::ParseError),

    #[error("Invalid integer for {key}: {value}")]
    InvalidInt {
        key: Cow<'static, str>,
        value: String,
    },

    #[error("Invalid bool for {key}: {value} (use true/false/1/0)")]
    InvalidBool {
        key: Cow<'static, str>,
        value: String,
    },

    #[error("Invalid duration for {key}: {value} (e.g. 250ms, 5s, 2m, 1h)")]
    InvalidDuration {
        key: Cow<'static, str>,
        value: String,
    },

    #[error("Invalid database scheme: {0}")]
    UnsupportedScheme(String),

    #[error("Invalid database mode")]
    InvalidDBMode,

    #[error("Record not found in DB")]
    NotFound,

    #[error("Unsupported: {0}")]
    Unsupported(Cow<'static, str>),

    #[error("Unable to execute query in transaction")]
    NotSendInTx,
}
