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
pub enum QBArg {
    Expr(Expression),
    Subquery(QueryBuilder),
    Closure(QBClosure),
}

impl QBArg {
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
    pub fn resolve_into_expr_with<F>(
        self,
        build_subquery: F,
    ) -> Result<(ast::Expr, SmallVec<[Param; 8]>)>
    where
        F: FnOnce(QueryBuilder) -> Result<(ast::Query, SmallVec<[Param; 8]>)>,
    {
        match self {
            QBArg::Expr(e) => Ok((e.expr, e.params)),
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
pub trait IntoQBArg {
    fn into_qb_arg(self) -> QBArg;
}

// &str → колонка
impl IntoQBArg for &str {
    fn into_qb_arg(self) -> QBArg {
        QBArg::Expr(expression::helpers::col(self))
    }
}

// String → колонка
impl IntoQBArg for String {
    fn into_qb_arg(self) -> QBArg {
        QBArg::Expr(expression::helpers::col(&self))
    }
}

// Expression → как есть
impl IntoQBArg for Expression {
    fn into_qb_arg(self) -> QBArg {
        QBArg::Expr(self)
    }
}

// QueryBuilder → подзапрос
impl IntoQBArg for QueryBuilder {
    fn into_qb_arg(self) -> QBArg {
        QBArg::Subquery(self)
    }
}

// Замыкание → подзапрос (построим позже, когда вызывающий метод решит его выполнять)
impl<F> IntoQBArg for F
where
    F: FnOnce(QueryBuilder) -> QueryBuilder + Send + 'static,
{
    fn into_qb_arg(self) -> QBArg {
        QBArg::Closure(QBClosure::new(self))
    }
}

impl IntoQBArg for QBArg {
    fn into_qb_arg(self) -> QBArg {
        self
    }
}

// [T; N] по значению
impl<T, const N: usize> ArgList for [T; N]
where
    T: IntoQBArg,
{
    fn into_vec(self) -> Vec<QBArg> {
        // into_iter() по массиву доступен в стабильном Rust
        self.into_iter().map(IntoQBArg::into_qb_arg).collect()
    }
}

// &[T; N] по ссылке
impl<'a, T, const N: usize> ArgList for &'a [T; N]
where
    T: IntoQBArg + Clone,
{
    fn into_vec(self) -> Vec<QBArg> {
        self.iter().cloned().map(IntoQBArg::into_qb_arg).collect()
    }
}

// ОДИНОЧНЫЙ аргумент: позволяет .from("users") / .select("id") и т.п.
impl<T> ArgList for T
where
    T: IntoQBArg,
{
    fn into_vec(self) -> Vec<QBArg> {
        vec![IntoQBArg::into_qb_arg(self)]
    }
}

#[inline]
/// Удобная утилита: собрать вектор аргументов из любого итератора.
pub fn collect_args<I, T>(items: I) -> Vec<QBArg>
where
    I: IntoIterator<Item = T>,
    T: IntoQBArg,
{
    items.into_iter().map(|it| it.into_qb_arg()).collect()
}

pub trait ArgList {
    fn into_vec(self) -> Vec<QBArg>;
}

// Vec<T>
impl<T> ArgList for Vec<T>
where
    T: IntoQBArg,
{
    fn into_vec(self) -> Vec<QBArg> {
        self.into_iter().map(IntoQBArg::into_qb_arg).collect()
    }
}

// &[T]
impl<'a, T> ArgList for &'a [T]
where
    T: IntoQBArg + Clone,
{
    fn into_vec(self) -> Vec<QBArg> {
        self.iter().cloned().map(IntoQBArg::into_qb_arg).collect()
    }
}

macro_rules! impl_arglist_for_tuple {
    ( $($T:ident),+ ) => {
        impl< $($T),+ > ArgList for ( $($T,)+ )
        where
            $( $T: IntoQBArg ),+
        {
            #[allow(non_snake_case)]
            fn into_vec(self) -> Vec<QBArg> {
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
