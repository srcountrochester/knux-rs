use std::borrow::Cow;

use crate::renderer::Dialect;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Feature: {feature} is not supported by dialect: {dialect}")]
    UnsupportedFeature {
        feature: Cow<'static, str>,
        dialect: Dialect,
    },
}
