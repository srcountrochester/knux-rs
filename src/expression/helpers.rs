use super::Expression;
use crate::param::Param;
use smallvec::smallvec;
use sqlparser::{
    ast::{
        self, Expr as SqlExpr, SelectItem, SelectItemQualifiedWildcardKind, SetExpr, Statement,
        WildcardAdditionalOptions, helpers::attached_token::AttachedToken,
    },
    dialect::GenericDialect,
    parser::Parser,
    tokenizer::{Token, TokenWithSpan},
};

pub trait RawArg {
    fn into_expr(self) -> ast::Expr;
}

/// Колонка: col("users.id")
pub fn col(name: &str) -> Expression {
    let ident = if name.contains('.') {
        let parts = name
            .split('.')
            .map(|s| ast::Ident::new(s))
            .collect::<Vec<_>>();
        Expression {
            expr: ast::Expr::CompoundIdentifier(parts),
            alias: None,
            params: smallvec![],
            mark_distinct_for_next: false,
        }
    } else {
        Expression {
            expr: ast::Expr::Identifier(ast::Ident::new(name)),
            alias: None,
            params: smallvec![],
            mark_distinct_for_next: false,
        }
    };
    ident
}

/// Параметр с bind'ом, эквивалент `?`, значение кладётся в params
pub fn val<T: Into<Param>>(v: T) -> Expression {
    Expression {
        expr: ast::Expr::Value(ast::Value::Number("?".into(), false).into()), // плейсхолдер
        alias: None,
        params: smallvec![v.into()],
        mark_distinct_for_next: false,
    }
}

/// Явный литерал (используй экономно; для безопасности предпочитай `val`)
pub fn lit<S: Into<String>>(s: S) -> Expression {
    Expression {
        expr: ast::Expr::Value(ast::Value::SingleQuotedString(s.into()).into()),
        alias: None,
        params: smallvec![],
        mark_distinct_for_next: false,
    }
}

/// Сырой фрагмент AST (на свой риск). Полезно для функций/операторов, которых ещё нет в DSL.
pub fn raw<A: RawArg>(arg: A) -> Expression {
    Expression {
        expr: arg.into_expr(),
        alias: None,
        params: smallvec![],
        mark_distinct_for_next: false,
    }
}

/// Сырой идентификатор (таблица/схема), без автоквотинга.
/// Используется, например, как цель JOIN: join(raw_ident("accounts"), ...)
pub fn raw_ident<S: Into<String>>(_s: S) -> Expression {
    // TODO: вернуть Expression с Expr::Identifier/CompoundIdentifier без квотирования
    todo!()
}

/// Семантическая ссылка на таблицу/схему. Поддерживает формы:
/// - "users"
/// - "public.users"
/// - "users u"
/// - "users AS u"
///
pub fn table<S: Into<String>>(name: S) -> Expression {
    col(&name.into())
}

pub fn schema(name: &str) -> Expression {
    Expression {
        expr: ast::Expr::Identifier(ast::Ident::new(name)),
        alias: None,
        params: smallvec![],
        mark_distinct_for_next: false,
    }
}

pub fn expr() -> Expression {
    Expression::empty()
}

impl<'a> RawArg for &'a str {
    fn into_expr(self) -> SqlExpr {
        let dialect = GenericDialect {};
        let sql = format!("SELECT {}", self);

        let first_item = Parser::parse_sql(&dialect, &sql)
            .ok()
            .and_then(|stmts| stmts.into_iter().next())
            .and_then(|stmt| match stmt {
                Statement::Query(q) => match q.body.as_ref() {
                    SetExpr::Select(sel) => sel.projection.first().cloned(),
                    _ => None,
                },
                _ => None,
            });

        first_item
            .and_then(select_item_to_expr)
            .unwrap_or_else(|| SqlExpr::Value(ast::Value::Null.into()))
    }
}

fn select_item_to_expr(item: SelectItem) -> Option<SqlExpr> {
    use SelectItem::*;
    Some(match item {
        UnnamedExpr(e) => e,
        ExprWithAlias { expr, .. } => expr,
        Wildcard(opts) => SqlExpr::Wildcard(opts.wildcard_token),
        QualifiedWildcard(kind, opts) => match kind {
            SelectItemQualifiedWildcardKind::ObjectName(obj) => {
                SqlExpr::QualifiedWildcard(obj, opts.wildcard_token)
            }
            SelectItemQualifiedWildcardKind::Expr(e) => e,
        },
    })
}

impl<F> RawArg for F
where
    F: FnOnce() -> SqlExpr,
{
    fn into_expr(self) -> SqlExpr {
        self()
    }
}
