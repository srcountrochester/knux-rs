use super::Expression;
use crate::param::Param;
use smallvec::smallvec;
use sqlparser::ast;

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
pub fn raw<F>(build: F) -> Expression
where
    F: FnOnce() -> ast::Expr,
{
    Expression {
        expr: build(),
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
