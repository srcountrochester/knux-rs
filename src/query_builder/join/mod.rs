mod __tests__;
mod core_fn;
mod utils;

use crate::expression::{Expression, JoinOnBuilder};
use crate::query_builder::QueryBuilder;
use crate::query_builder::join::core_fn::JoinKind;

/// Варианты аргумента для ON
pub enum JoinOnArg {
    Expr(Expression),
    /// Строка вида "users.id = accounts.user_id" (будет разобрана/преобразована позже)
    Raw(String),
    /// Замыкание билдера: |on| on.on(...).andOn(...).orOn(...)
    Builder(Box<dyn FnOnce(JoinOnBuilder) -> JoinOnBuilder + Send + 'static>),
    None,
}

impl From<Expression> for JoinOnArg {
    fn from(e: Expression) -> Self {
        JoinOnArg::Expr(e)
    }
}
impl From<&str> for JoinOnArg {
    fn from(s: &str) -> Self {
        JoinOnArg::Raw(s.to_string())
    }
}
impl From<String> for JoinOnArg {
    fn from(s: String) -> Self {
        JoinOnArg::Raw(s)
    }
}
impl<F> From<F> for JoinOnArg
where
    F: FnOnce(JoinOnBuilder) -> JoinOnBuilder + Send + 'static,
{
    fn from(f: F) -> Self {
        JoinOnArg::Builder(Box::new(f))
    }
}

impl QueryBuilder {
    /// INNER JOIN <target> [ON <expr>]
    ///
    /// Примеры:
    ///   .join("accounts", "users.id = accounts.user_id")
    ///   .join(table("accounts"), col("u.id").eq(col("a.user_id")))
    ///   .join(sub_q, |on| on.on(...).and_on(...))
    pub fn join<T, O>(self, target: T, on: O) -> Self
    where
        T: crate::query_builder::args::IntoQBArg,
        O: Into<JoinOnArg>,
    {
        self.push_join_internal(JoinKind::Inner, target, on)
    }

    /// LEFT JOIN <target> ON <expr>
    pub fn left_join<T, O>(self, target: T, on: O) -> Self
    where
        T: crate::query_builder::args::IntoQBArg,
        O: Into<JoinOnArg>,
    {
        self.push_join_internal(JoinKind::Left, target, on)
    }

    /// RIGHT JOIN <target> ON <expr>
    pub fn right_join<T, O>(self, target: T, on: O) -> Self
    where
        T: crate::query_builder::args::IntoQBArg,
        O: Into<JoinOnArg>,
    {
        self.push_join_internal(JoinKind::Right, target, on)
    }

    /// FULL [OUTER] JOIN <target> ON <expr>
    pub fn full_join<T, O>(self, target: T, on: O) -> Self
    where
        T: crate::query_builder::args::IntoQBArg,
        O: Into<JoinOnArg>,
    {
        self.push_join_internal(JoinKind::Full, target, on)
    }

    /// CROSS JOIN <target>
    pub fn cross_join<T>(self, target: T) -> Self
    where
        T: crate::query_builder::args::IntoQBArg,
    {
        self.push_join_internal(JoinKind::Cross, target, JoinOnArg::None)
    }

    /// NATURAL [INNER] JOIN <target>
    pub fn natural_join<T>(self, target: T) -> Self
    where
        T: crate::query_builder::args::IntoQBArg,
    {
        self.push_join_internal(JoinKind::NaturalInner, target, JoinOnArg::None)
    }

    /// NATURAL LEFT JOIN <target>
    pub fn natural_left_join<T>(self, target: T) -> Self
    where
        T: crate::query_builder::args::IntoQBArg,
    {
        self.push_join_internal(JoinKind::NaturalLeft, target, JoinOnArg::None)
    }

    /// NATURAL RIGHT JOIN <target>
    pub fn natural_right_join<T>(self, target: T) -> Self
    where
        T: crate::query_builder::args::IntoQBArg,
    {
        self.push_join_internal(JoinKind::NaturalRight, target, JoinOnArg::None)
    }

    /// NATURAL FULL JOIN <target>
    pub fn natural_full_join<T>(self, target: T) -> Self
    where
        T: crate::query_builder::args::IntoQBArg,
    {
        self.push_join_internal(JoinKind::NaturalFull, target, JoinOnArg::None)
    }
}
