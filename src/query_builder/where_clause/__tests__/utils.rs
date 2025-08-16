use sqlparser::ast::{BinaryOperator as BO, Expr as SqlExpr, UnaryOperator as UO};

use super::super::utils::*;
use crate::query_builder::Error;

#[inline]
fn unwrap_parens<'a>(e: &'a SqlExpr) -> &'a SqlExpr {
    match e {
        SqlExpr::Nested(inner) => inner.as_ref(),
        _ => e,
    }
}

#[track_caller]
fn must_parse(s: &str) -> SqlExpr {
    parse_where_expr(s).expect(&format!("parse_where_expr failed for: {s}"))
}

#[test]
fn parses_simple_binary_eq() {
    let e = must_parse("users.id = accounts.user_id");
    match e {
        SqlExpr::BinaryOp { op, .. } => assert!(matches!(op, BO::Eq)),
        other => panic!("expected BinaryOp Eq, got {:?}", other),
    }
}

#[test]
fn parses_and_or_chain() {
    let e = must_parse("a = 1 AND (b > 2 OR c < 3)");
    // верхний уровень должен быть AND
    match e {
        SqlExpr::BinaryOp { op, left, right } => {
            assert!(matches!(op, BO::And));
            // левая — Eq
            assert!(matches!(
                left.as_ref(),
                SqlExpr::BinaryOp { op: BO::Eq, .. }
            ));

            // правая — OR (возможна обёртка Nested(...))
            let right_inner = unwrap_parens(right.as_ref());
            if let SqlExpr::BinaryOp { op: r_op, .. } = right_inner {
                assert!(matches!(r_op, BO::Or));
            } else {
                panic!(
                    "right side must be OR (optionally nested), got {:?}",
                    right_inner
                );
            }
        }
        other => panic!("expected top-level AND, got {:?}", other),
    }
}

#[test]
fn parses_unary_not() {
    let e = must_parse("NOT is_active");
    match e {
        SqlExpr::UnaryOp { op, expr } => {
            assert!(matches!(op, UO::Not));
            // внутри — идентификатор
            matches!(
                expr.as_ref(),
                SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_)
            );
        }
        other => panic!("expected NOT <expr>, got {:?}", other),
    }
}

#[test]
fn parses_between_and_not_between() {
    let e = must_parse("age BETWEEN 18 AND 30");
    match e {
        SqlExpr::Between { negated, .. } => assert!(!negated),
        other => panic!("expected Between, got {:?}", other),
    }

    let e2 = must_parse("age NOT BETWEEN 18 AND 30");
    match e2 {
        SqlExpr::Between { negated, .. } => assert!(negated),
        other => panic!("expected NOT BETWEEN, got {:?}", other),
    }
}

#[test]
fn parses_like_and_ilike() {
    let e = must_parse("name LIKE 'A%'");
    assert!(matches!(
        e,
        SqlExpr::Like {
            negated: false,
            any: false,
            ..
        }
    ));

    let e2 = must_parse("name ILIKE 'a_%'");
    assert!(matches!(
        e2,
        SqlExpr::ILike {
            negated: false,
            any: false,
            ..
        }
    ));
}

#[test]
fn parses_in_list_and_in_subquery() {
    let e = must_parse("id IN (1, 2, 3)");
    assert!(matches!(e, SqlExpr::InList { negated: false, .. }));

    // минимально валидный подзапрос
    let e2 = must_parse("id IN (SELECT 1)");
    assert!(matches!(e2, SqlExpr::InSubquery { negated: false, .. }));
}

#[test]
fn parses_is_null_and_is_not_null() {
    let e = must_parse("deleted_at IS NULL");
    assert!(matches!(e, SqlExpr::IsNull(_)));

    let e2 = must_parse("deleted_at IS NOT NULL");
    assert!(matches!(e2, SqlExpr::IsNotNull(_)));
}

#[test]
fn returns_invalid_expression_on_parser_error() {
    // Было "a == 1" — GenericDialect это съедает как '='.
    // Делаем явно невалидное выражение:
    let err = parse_where_expr("a = 1 AND").unwrap_err();
    match err {
        Error::InvalidExpression { reason } => {
            assert!(!reason.is_empty(), "reason must not be empty");
        }
        other => panic!("expected InvalidExpression, got {:?}", other),
    }
}

#[test]
fn boolean_literal_ok() {
    let e = must_parse("TRUE OR FALSE");
    assert!(matches!(e, SqlExpr::BinaryOp { op: BO::Or, .. }));
}

#[test]
fn quote_sql_str_basic_and_escaping() {
    // базовый случай
    let s = quote_sql_str("abc");
    assert_eq!(s, "'abc'");

    // пустая строка
    let s = quote_sql_str("");
    assert_eq!(s, "''");

    // экранирование одинарной кавычки → удвоение
    let s = quote_sql_str("O'Reilly");
    assert_eq!(s, "'O''Reilly'");

    // несколько кавычек
    let s = quote_sql_str("a'b'c");
    assert_eq!(s, "'a''b''c'");
}

#[test]
fn sqlite_json_superset_sql_shape_and_parse() {
    // left ⊇ right_json
    let sql = sqlite_json_superset_sql("payload", r#"{"a":1,"b":"x"}"#);

    // форма: NOT EXISTS ... json_each('<json>') AS r ... json_extract(payload, '$.'||r.key) ...
    assert!(
        sql.starts_with("NOT EXISTS ("),
        "must wrap in NOT EXISTS: {sql}"
    );
    assert!(
        sql.contains(r#"json_each('{"a":1,"b":"x"}') AS r"#),
        "must iterate right_json via json_each: {sql}"
    );
    assert!(
        sql.contains(r#"json_extract(payload, '$.'||r.key)"#),
        "must extract from LEFT using keys of right: {sql}"
    );

    // фрагмент должен парситься как валидное WHERE-выражение
    parse_where_expr(&sql).expect("superset SQL must be parseable");
}

#[test]
fn sqlite_json_subset_sql_shape_and_parse() {
    // left_json ⊆ right
    let sql = sqlite_json_subset_sql(r#"{"k":2}"#, "payload");

    // форма: NOT EXISTS ... json_each('<left_json>') AS l ... json_extract(payload, '$.'||l.key) ...
    assert!(
        sql.contains(r#"json_each('{"k":2}') AS l"#),
        "must iterate left_json via json_each: {sql}"
    );
    assert!(
        sql.contains(r#"json_extract(payload, '$.'||l.key)"#),
        "must extract from RIGHT using keys of left: {sql}"
    );

    // парсинг как WHERE-выражения
    parse_where_expr(&sql).expect("subset SQL must be parseable");
}
