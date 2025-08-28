//! Тесты для утилит оптимизатора (`src/optimizer/utils.rs`).

use sqlparser::ast as S;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Span;

use crate::optimizer::utils::*;

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

fn parse_stmt(sql: &str) -> S::Statement {
    Parser::parse_sql(&GenericDialect {}, sql)
        .unwrap()
        .remove(0)
}

fn parse_query(sql: &str) -> S::Query {
    if let S::Statement::Query(q) = parse_stmt(sql) {
        *q
    } else {
        panic!("not a query")
    }
}

fn parse_select(sql: &str) -> S::Select {
    if let S::SetExpr::Select(bx) = parse_query(sql).body.as_ref() {
        // Клонируем сам Select (не потребляя Box)
        bx.as_ref().clone()
    } else {
        panic!("not a SELECT body")
    }
}

/// Вспомогательно: извлечь выражение из `SELECT <expr>`.
fn parse_expr(expr_sql: &str) -> S::Expr {
    let sql = format!("SELECT {expr_sql}");
    if let S::Statement::Query(q) = parse_stmt(&sql) {
        if let S::SetExpr::Select(bx) = q.body.as_ref() {
            if let S::SelectItem::UnnamedExpr(e) = &bx.projection[0] {
                return e.clone();
            }
        }
    }
    panic!("cannot extract expr");
}

fn strip_parens(s: &str) -> String {
    s.replace('(', "").replace(')', "")
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

/// Проверка `select_is_simple_no_cardinality`: без DISTINCT/GROUP BY/HAVING → true.
#[test]
fn utils_select_is_simple_no_cardinality() {
    let s1 = sel_from("SELECT a FROM t");
    assert!(select_is_simple_no_cardinality(&s1));

    let s2 = sel_from("SELECT DISTINCT a FROM t");
    assert!(!select_is_simple_no_cardinality(&s2));
}

/* ===========================
and_merge
=========================== */

/// Проверяет, что `and_merge(None, Some(p))` присваивает предикат.
#[test]
fn utils_and_merge_assigns() {
    let mut dst: Option<S::Expr> = None;
    let add = Some(parse_expr("a = 1"));
    and_merge(&mut dst, add);
    assert_eq!(dst.unwrap().to_string(), "a = 1");
}

/// Проверяет, что `and_merge(Some(a), Some(b))` оборачивает в `a AND b`.
#[test]
fn utils_and_merge_conjunction() {
    let mut dst = Some(parse_expr("x > 0"));
    let add = Some(parse_expr("y < 10"));
    and_merge(&mut dst, add);
    let got = dst.unwrap().to_string();
    assert_eq!(strip_parens(&got), "x > 0 AND y < 10");
}

/* ===========================
select_is_simple_no_cardinality
=========================== */

/// TRUE для простого SELECT без DISTINCT/GROUP BY/HAVING.
#[test]
fn utils_simple_no_cardinality_true() {
    let sel = parse_select("SELECT a FROM t");
    assert!(select_is_simple_no_cardinality(&sel));
}

/// FALSE, если есть DISTINCT/GROUP BY/HAVING.
#[test]
fn utils_simple_no_cardinality_false() {
    let sel1 = parse_select("SELECT DISTINCT a FROM t");
    assert!(!select_is_simple_no_cardinality(&sel1));

    let sel2 = parse_select("SELECT a FROM t GROUP BY a");
    assert!(!select_is_simple_no_cardinality(&sel2));

    let sel3 = parse_select("SELECT a FROM t GROUP BY a HAVING COUNT(*) > 1");
    assert!(!select_is_simple_no_cardinality(&sel3));
}

/* ===========================
is_plain_column
=========================== */

/// TRUE для `a` и `t.a` (прямые идентификаторы колонки).
#[test]
fn utils_is_plain_column_true() {
    assert!(is_plain_column(&parse_expr("a")));
    assert!(is_plain_column(&parse_expr("t.a")));
}

/// FALSE для неколонок (выражение/функция).
#[test]
fn utils_is_plain_column_false() {
    assert!(!is_plain_column(&parse_expr("a + 1")));
    assert!(!is_plain_column(&parse_expr("coalesce(a, 0)")));
}

/* ===========================
projection_is_direct_columns
=========================== */

/// TRUE: проекция только из прямых колонок.
#[test]
fn utils_projection_direct_columns_true() {
    let sel = parse_select("SELECT a, b, t.c FROM t");
    assert!(projection_is_direct_columns(&sel));
}

/// FALSE: выражения/звёздочка/алиасы ломают условие.
#[test]
fn utils_projection_direct_columns_false() {
    let sel1 = parse_select("SELECT a + 1 FROM t");
    assert!(!projection_is_direct_columns(&sel1));

    let sel2 = parse_select("SELECT * FROM t");
    assert!(!projection_is_direct_columns(&sel2));
}

/* ===========================
is_literal_const
=========================== */

/// Литералы (`1`, `'x'`) распознаются как константы.
#[test]
fn utils_is_literal_const_true() {
    let k1 = is_literal_const(&parse_expr("1"));
    let k2 = is_literal_const(&parse_expr("'x'"));
    assert!(k1.is_some() && k2.is_some());
}

/// Placeholders не считаются константами.
#[test]
fn utils_is_literal_const_placeholder_false() {
    // Конструируем Expr::Value(ValueWithSpan{ value: Placeholder("?"), .. })
    let expr = S::Expr::Value(S::ValueWithSpan {
        value: S::Value::Placeholder("?".into()),
        span: Span::empty(),
    });
    assert_eq!(is_literal_const(&expr), None);
}

/* ===========================
walk_expr_mut
=========================== */

/// Пост-обход: последний вызов колбэка — на корневом узле.
#[test]
fn utils_walk_expr_post_root_last() {
    let mut e = parse_expr("a + 1 * 2");
    let mut last = String::new();
    walk_expr_mut(&mut e, WalkOrder::Post, &mut |x| {
        last = x.to_string();
    });
    assert_eq!(last, "a + 1 * 2");
}

/// Обходит подзапросы: колбэк получает предикат из `WHERE` внутри.
#[test]
fn utils_walk_expr_visits_subquery_where() {
    let mut e = parse_expr("a IN (SELECT b FROM t WHERE b = 1)");
    let mut seen_eq = false;
    walk_expr_mut(&mut e, WalkOrder::Post, &mut |x| {
        if let S::Expr::BinaryOp {
            op: S::BinaryOperator::Eq,
            ..
        } = x
        {
            seen_eq = true;
        }
    });
    assert!(seen_eq);
}

/* ===========================
walk_join_mut
=========================== */

/// Для `JOIN ... ON` колбэк вызывается по выражению `ON`.
#[test]
fn utils_walk_join_mut_on_called() {
    let sel = parse_select("SELECT * FROM a JOIN b ON a.id = b.id");
    let mut called = false;
    let mut join = sel.from[0].joins[0].clone();
    walk_join_mut(&mut join, &mut |_e| {
        called = true;
    });
    assert!(called);
}

/// Для `CROSS JOIN` выражений нет — колбэк не вызывается.
#[test]
fn utils_walk_join_mut_cross_noop() {
    let sel = parse_select("SELECT * FROM a CROSS JOIN b");
    let mut called = false;
    if let Some(j) = sel.from[0].joins.get(0).cloned() {
        let mut j = j.clone();
        walk_join_mut(&mut j, &mut |_e| {
            called = true;
        });
    }
    assert!(called == false);
}

/* ===========================
walk_statement_mut
=========================== */

/// Для `Statement::Query` гарантируется вызов `on_query(top_level=true)`.
#[test]
fn utils_walk_statement_query_top_true() {
    let mut s = parse_stmt("SELECT * FROM t WHERE 1=1");
    let mut tops = Vec::new();
    let mut expr_cnt = 0usize;
    walk_statement_mut(&mut s, &mut |_q, top| tops.push(top), &mut |_e| {
        expr_cnt += 1;
    });
    assert_eq!(tops, vec![true]);
    assert!(expr_cnt >= 1);
}

/// Обходит выражения UPDATE и JOIN’ы через таблицы/ON.
#[test]
fn utils_walk_statement_update_with_join() {
    let mut s = parse_stmt(
        "UPDATE t SET x = (SELECT 1) FROM u JOIN v ON u.id = v.id WHERE t.id IN (SELECT id FROM u)",
    );
    let mut q_cnt = 0usize;
    let mut e_cnt = 0usize;
    walk_statement_mut(
        &mut s,
        &mut |_q, _| {
            q_cnt += 1;
        },
        &mut |_e| {
            e_cnt += 1;
        },
    );
    // on_query не дергается для подзапросов в выражениях
    assert_eq!(
        q_cnt, 0,
        "on_query не должен срабатывать на подзапросы в выражениях"
    );
    // выражения (SET/ON/WHERE) посещены
    assert!(e_cnt >= 3, "ожидали вызовы по выражениям, e_cnt={e_cnt}");
}
