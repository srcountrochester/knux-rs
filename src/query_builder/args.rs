use std::fmt;

use crate::expression::{self, Expression};
use crate::param::Param;
use crate::query_builder::QueryBuilder;
use smallvec::SmallVec;
use sqlparser::ast;

use super::{Error, Result};

pub struct QBClosure(Box<dyn FnOnce(QueryBuilder) -> QueryBuilder + Send + 'static>);

impl QBClosure {
    pub fn new<F>(f: F) -> Self
    where
        F: FnOnce(QueryBuilder) -> QueryBuilder + Send + 'static,
    {
        Self(Box::new(f))
    }

    #[inline]
    /// Применить замыкание, потребив обёртку.
    pub fn apply(self, qb: QueryBuilder) -> QueryBuilder {
        (self.0)(qb)
    }
}

impl fmt::Debug for QBClosure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Само тело замыкания неотображаемо — выводим метку.
        f.write_str("<closure>")
    }
}

/// Унифицированный аргумент для методов билдера:
/// - готовое выражение (`Expr`)
/// - подзапрос (`Subquery`) — целый QueryBuilder
/// - замыкание (`Closure`), в которое мы передаём «пустой» QueryBuilder, а результат — это подзапрос
#[derive(Debug)]
pub enum QBArg<'a> {
    Expr(Expression),
    Subquery(QueryBuilder<'a>),
    Closure(QBClosure),
}

impl<'a> QBArg<'a> {
    /// Превратить аргумент в `ast::Expr`, если это **готовое выражение**.
    /// Для `Subquery/Closure` вернёт ошибку с пояснением.
    pub fn try_into_expr(self) -> Result<(ast::Expr, SmallVec<[Param; 8]>)> {
        match self {
            QBArg::Expr(e) => Ok((e.expr, e.params)),
            QBArg::Subquery(_) => Err(Error::InvalidExpression {
                reason: "argument is a subquery; use resolve_into_expr_with(...) instead".into(),
            }),
            QBArg::Closure(_) => Err(Error::InvalidExpression {
                reason: "argument is a closure; use resolve_into_expr_with(...) instead".into(),
            }),
        }
    }

    /// Универсальный резолв в `ast::Expr`, включая подзапросы.
    ///
    /// `build_subquery` — функция, которая принимает `QueryBuilder`
    /// и возвращает `(ast::Query, params)`. Её предоставит сам `QueryBuilder`
    /// там, где уже есть контекст (`SELECT`, `WHERE IN (subquery)`, и т.п.).
    pub fn resolve_into_expr_with<F>(self, build_subquery: F) -> Result<(ast::Expr, Vec<Param>)>
    where
        F: FnOnce(QueryBuilder) -> Result<(ast::Query, Vec<Param>)>,
    {
        match self {
            QBArg::Expr(e) => Ok((e.expr, e.params.into_vec())),
            QBArg::Subquery(qb) => {
                let (q, params) = build_subquery(qb)?;
                Ok((ast::Expr::Subquery(Box::new(q)), params))
            }
            QBArg::Closure(c) => {
                let built = c.apply(QueryBuilder::new_empty());
                let (q, params) = build_subquery(built)?;
                Ok((ast::Expr::Subquery(Box::new(q)), params))
            }
        }
    }
}

/// Трейт, который позволяет передавать «что угодно» в методы билдера.
/// Поддерживаем:
/// - &str / String → колонка (col("..."))
/// - Expression → как есть
/// - QueryBuilder → подзапрос
/// - FnOnce(QueryBuilder) -> QueryBuilder → подзапрос, сконструированный на лету
pub trait IntoQBArg<'a> {
    fn into_qb_arg(self) -> QBArg<'a>;
}

// &str → колонка
impl<'a> IntoQBArg<'a> for &str {
    #[inline]
    fn into_qb_arg(self) -> QBArg<'a> {
        QBArg::Expr(expression::helpers::col(self))
    }
}

// String → колонка
impl<'a> IntoQBArg<'a> for String {
    #[inline]
    fn into_qb_arg(self) -> QBArg<'a> {
        QBArg::Expr(expression::helpers::col(&self))
    }
}

// Expression → как есть
impl<'a> IntoQBArg<'a> for Expression {
    #[inline]
    fn into_qb_arg(self) -> QBArg<'a> {
        QBArg::Expr(self)
    }
}

// QueryBuilder → подзапрос
impl<'a> IntoQBArg<'a> for QueryBuilder<'a> {
    #[inline]
    fn into_qb_arg(self) -> QBArg<'a> {
        QBArg::Subquery(self)
    }
}

// Замыкание → подзапрос (построим позже, когда вызывающий метод решит его выполнять)
impl<'a, F> IntoQBArg<'a> for F
where
    F: FnOnce(QueryBuilder) -> QueryBuilder + Send + 'static,
{
    #[inline]
    fn into_qb_arg(self) -> QBArg<'a> {
        QBArg::Closure(QBClosure::new(self))
    }
}

impl<'a> IntoQBArg<'a> for QBArg<'a> {
    #[inline]
    fn into_qb_arg(self) -> QBArg<'a> {
        self
    }
}

// [T; N] по значению
impl<'a, T, const N: usize> ArgList<'a> for [T; N]
where
    T: IntoQBArg<'a>,
{
    #[inline]
    fn into_vec(self) -> Vec<QBArg<'a>> {
        // into_iter() по массиву доступен в стабильном Rust
        self.into_iter().map(IntoQBArg::into_qb_arg).collect()
    }
}

// &[T; N] по ссылке
impl<'a, T, const N: usize> ArgList<'a> for &'a [T; N]
where
    T: IntoQBArg<'a> + Clone,
{
    #[inline]
    fn into_vec(self) -> Vec<QBArg<'a>> {
        self.iter().cloned().map(IntoQBArg::into_qb_arg).collect()
    }
}

// ОДИНОЧНЫЙ аргумент: позволяет .from("users") / .select("id") и т.п.
impl<'a, T> ArgList<'a> for T
where
    T: IntoQBArg<'a>,
{
    #[inline]
    fn into_vec(self) -> Vec<QBArg<'a>> {
        vec![IntoQBArg::into_qb_arg(self)]
    }
}

#[inline]
/// Удобная утилита: собрать вектор аргументов из любого итератора.
pub fn collect_args<'a, I, T>(items: I) -> Vec<QBArg<'a>>
where
    I: IntoIterator<Item = T>,
    T: IntoQBArg<'a>,
{
    items.into_iter().map(|it| it.into_qb_arg()).collect()
}

pub trait ArgList<'a> {
    fn into_vec(self) -> Vec<QBArg<'a>>;
}

// Vec<T>
impl<'a, T> ArgList<'a> for Vec<T>
where
    T: IntoQBArg<'a>,
{
    #[inline]
    fn into_vec(self) -> Vec<QBArg<'a>> {
        self.into_iter().map(IntoQBArg::into_qb_arg).collect()
    }
}

// &[T]
impl<'a, T> ArgList<'a> for &'a [T]
where
    T: IntoQBArg<'a> + Clone,
{
    #[inline]
    fn into_vec(self) -> Vec<QBArg<'a>> {
        self.iter().cloned().map(IntoQBArg::into_qb_arg).collect()
    }
}

impl<'a> ArgList<'a> for () {
    #[inline]
    fn into_vec(self) -> Vec<QBArg<'a>> {
        Vec::new()
    }
}

macro_rules! impl_arglist_for_tuple {
    ( $($T:ident),+ ) => {
        impl<'a,  $($T),+ > ArgList<'a> for ( $($T,)+ )
        where
            $( $T: IntoQBArg<'a> ),+
        {
            #[allow(non_snake_case)]
            fn into_vec(self) -> Vec<QBArg<'a>> {
                let ( $($T,)+ ) = self;
                let mut v = Vec::new();
                $( v.push($T.into_qb_arg()); )+
                v
            }
        }
    };
}

impl_arglist_for_tuple!(A);
impl_arglist_for_tuple!(A, B);
impl_arglist_for_tuple!(A, B, C);
impl_arglist_for_tuple!(A, B, C, D);
impl_arglist_for_tuple!(A, B, C, D, E);
impl_arglist_for_tuple!(A, B, C, D, E, F);
impl_arglist_for_tuple!(A, B, C, D, E, F, G);
impl_arglist_for_tuple!(A, B, C, D, E, F, G, H);
impl_arglist_for_tuple!(A, B, C, D, E, F, G, H, I);
impl_arglist_for_tuple!(A, B, C, D, E, F, G, H, I, J);
impl_arglist_for_tuple!(A, B, C, D, E, F, G, H, I, J, K);
impl_arglist_for_tuple!(A, B, C, D, E, F, G, H, I, J, K, L);
