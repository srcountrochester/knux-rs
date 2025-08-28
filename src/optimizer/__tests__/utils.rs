//! Тесты для утилит оптимизатора (`src/optimizer/utils.rs`).

use sqlparser::ast as S;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::optimizer::utils::{
    and_merge, expr_refs_only_alias, first_projection_expr, join_conjuncts,
    query_has_no_limit_or_fetch, rewrite_select_to_one, select_is_simple_no_cardinality,
    split_conjuncts, strip_alias_in_expr,
};

fn sel_from(sql: &str) -> S::Select {
    let dialect = GenericDialect {};
    let stmt = Parser::parse_sql(&dialect, sql).unwrap().remove(0);
    match stmt {
        S::Statement::Query(q) => match *q.body {
            S::SetExpr::Select(s) => *s,
            _ => panic!("ожидали SELECT"),
        },
        _ => panic!("ожидали SELECT"),
    }
}

fn query_from(sql: &str) -> S::Query {
    let dialect = GenericDialect {};
    let stmt = Parser::parse_sql(&dialect, sql).unwrap().remove(0);
    match stmt {
        S::Statement::Query(q) => *q,
        _ => panic!("ожидали Query"),
    }
}

fn expr_from(sql: &str) -> S::Expr {
    let dialect = GenericDialect {};
    let mut v = Parser::parse_sql(&dialect, format!("SELECT {}", sql).as_str()).unwrap();
    match v.remove(0) {
        S::Statement::Query(q) => match *q.body {
            S::SetExpr::Select(s) => match s.projection.into_iter().next().unwrap() {
                S::SelectItem::UnnamedExpr(e) => e,
                _ => panic!("ожидали выражение"),
            },
            _ => panic!("ожидали SELECT"),
        },
        _ => panic!("ожидали Query"),
    }
}

/// Проверка `and_merge`: добавление к пустому и объединение в AND.
#[test]
fn utils_and_merge_basic() {
    let mut dst: Option<S::Expr> = None;
    and_merge(&mut dst, Some(expr_from("a > 0")));

    assert!(dst.is_some(), "должен появиться первый предикат");

    and_merge(&mut dst, Some(expr_from("b < 10")));
    // dst = (a > 0) AND (b < 10)
    if let Some(S::Expr::BinaryOp { op, .. }) = &dst {
        assert!(matches!(op, S::BinaryOperator::And), "ожидали AND");
    } else {
        panic!("ожидали BinaryOp");
    }
}

/// Проверка `split_conjuncts/join_conjuncts`: разбиение и обратная сборка.
#[test]
fn utils_split_join_conjuncts_roundtrip() {
    let e = expr_from("a > 0 AND (b < 10 AND c IS NOT NULL)");
    let mut parts = Vec::new();
    split_conjuncts(e, &mut parts);
    assert_eq!(parts.len(), 3, "ожидали 3 конъюнкта");

    let j = join_conjuncts(parts).expect("join back");
    // грубая проверка по типу корня — это снова конъюнкция
    if let S::Expr::BinaryOp { op, .. } = j {
        assert!(matches!(op, S::BinaryOperator::And));
    } else {
        panic!("ожидали BinaryOp");
    }
}

/// Проверка `query_has_no_limit_or_fetch`: true/false.
#[test]
fn utils_query_has_no_limit_or_fetch() {
    let q1 = query_from("SELECT a FROM t");
    assert!(query_has_no_limit_or_fetch(&q1));

    let q2 = query_from("SELECT a FROM t LIMIT 5");
    assert!(!query_has_no_limit_or_fetch(&q2));
}

/// Проверка `select_is_simple_no_cardinality`: без DISTINCT/GROUP BY/HAVING → true.
#[test]
fn utils_select_is_simple_no_cardinality() {
    let s1 = sel_from("SELECT a FROM t");
    assert!(select_is_simple_no_cardinality(&s1));

    let s2 = sel_from("SELECT DISTINCT a FROM t");
    assert!(!select_is_simple_no_cardinality(&s2));
}

/// Проверка `rewrite_select_to_one`: проекция становится `SELECT 1`.
#[test]
fn utils_rewrite_select_to_one() {
    let mut s = sel_from("SELECT a, b FROM t WHERE a > 0");
    rewrite_select_to_one(&mut s);
    assert_eq!(s.projection.len(), 1);

    match &s.projection[0] {
        S::SelectItem::UnnamedExpr(S::Expr::Value(v)) => {
            assert!(matches!(v.value, S::Value::Number(ref n, false) if n == "1"));
        }
        _ => panic!("ожидали SELECT 1"),
    }
    assert!(s.selection.is_some(), "WHERE должен сохраниться");
}

/// Проверка `first_projection_expr`: возвращает expr для `UnnamedExpr`/`ExprWithAlias`, None для `*`.
#[test]
fn utils_first_projection_expr() {
    let s1 = sel_from("SELECT a FROM t");
    assert!(first_projection_expr(&s1).is_some());

    let s2 = sel_from("SELECT a AS x FROM t");
    assert!(first_projection_expr(&s2).is_some());

    let s3 = sel_from("SELECT * FROM t");
    assert!(first_projection_expr(&s3).is_none());
}

/// Проверка `expr_refs_only_alias`: true, если все идентификаторы принадлежат `s.` или без префикса.
#[test]
fn utils_expr_refs_only_alias() {
    let e1 = expr_from("s.a > 0 AND s.b < 10");
    assert!(expr_refs_only_alias(&e1, "s"));

    let e2 = expr_from("s.a > 0 AND t.b < 10");
    assert!(!expr_refs_only_alias(&e2, "s"));
}

/// Проверка `strip_alias_in_expr`: `s.a + s.b` → `a + b`.
#[test]
fn utils_strip_alias_in_expr() {
    let mut e = expr_from("s.a + s.b");
    strip_alias_in_expr(&mut e, "s");

    // ожидаем оба операнда как Identifier без префикса
    if let S::Expr::BinaryOp { left, right, .. } = e {
        match (*left, *right) {
            (S::Expr::Identifier(id1), S::Expr::Identifier(id2)) => {
                assert_eq!(id1.value, "a");
                assert_eq!(id2.value, "b");
            }
            other => panic!("ожидали два Identifier, получили {:?}", other),
        }
    } else {
        panic!("ожидали BinaryOp");
    }
}
