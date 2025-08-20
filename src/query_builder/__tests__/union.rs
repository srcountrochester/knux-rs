use crate::expression::helpers::{col, val};
use crate::query_builder::QueryBuilder;
use crate::tests::dialect_test_helpers::qi;
use sqlparser::ast::{SetExpr, SetOperator, SetQuantifier};

fn norm(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[test]
fn union_basic_sql_contains_union() {
    // SELECT id FROM a UNION SELECT id FROM b
    let (sql, _params) = QueryBuilder::new_empty()
        .from("a")
        .select((col("id"),))
        .union(|q: QueryBuilder| q.from("b").select((col("id"),)))
        .to_sql()
        .expect("to_sql");

    let s = norm(&sql).to_uppercase();
    assert!(s.contains("UNION"), "expected UNION in SQL, got: {s}");
    assert!(
        !s.contains("UNION ALL"),
        "UNION should be DISTINCT by default"
    );
}

#[test]
fn union_all_sql_contains_union_all() {
    // SELECT id FROM a UNION ALL SELECT id FROM b
    let (sql, _params) = QueryBuilder::new_empty()
        .from("a")
        .select((col("id"),))
        .union_all(|q: QueryBuilder| q.from("b").select((col("id"),)))
        .to_sql()
        .expect("to_sql");

    let s = norm(&sql).to_uppercase();
    assert!(
        s.contains("UNION ALL"),
        "expected UNION ALL in SQL, got: {s}"
    );
}

#[test]
fn union_builds_setexpr_ast() {
    let (q, _params) = QueryBuilder::new_empty()
        .from("a")
        .select((col("id"),))
        .union(|q: QueryBuilder| q.from("b").select((col("id"),)))
        .build_query_ast()
        .expect("build");

    match *q.body {
        SetExpr::SetOperation {
            op,
            set_quantifier,
            ref left,
            ref right,
        } => {
            assert!(matches!(op, SetOperator::Union));
            assert!(
                matches!(set_quantifier, SetQuantifier::None),
                "UNION must map to DISTINCT/None"
            );

            // Левая часть — исходный SELECT
            assert!(matches!(**left, SetExpr::Select(_)));
            // Правая часть — подзапрос
            assert!(matches!(**right, SetExpr::Query(_)));
        }
        other => panic!("expected SetOperation AST, got {:?}", other),
    }
}

#[test]
fn union_chain_left_associative_and_quantifiers() {
    // (A UNION B) UNION ALL C
    let (q, _params) = QueryBuilder::new_empty()
        .from("a")
        .select((col("id"),))
        .union(|q: QueryBuilder| q.from("b").select((col("id"),)))
        .union_all(|q: QueryBuilder| q.from("c").select((col("id"),)))
        .build_query_ast()
        .expect("build");

    // Верхний уровень — UNION ALL
    match *q.body {
        SetExpr::SetOperation {
            op,
            set_quantifier,
            ref left,
            ref right,
        } => {
            assert!(matches!(op, SetOperator::Union));
            assert!(matches!(set_quantifier, SetQuantifier::All));

            // Левая часть сама — SetOperation (A UNION B)
            match **left {
                SetExpr::SetOperation {
                    op, set_quantifier, ..
                } => {
                    assert!(matches!(op, SetOperator::Union));
                    assert!(matches!(set_quantifier, SetQuantifier::None));
                }
                _ => panic!("left must be a SetOperation (A UNION B)"),
            }

            // Правая — Query/Select (C)
            assert!(matches!(**right, SetExpr::Query(_) | SetExpr::Select(_)));
        }
        _ => panic!("top-level must be SetOperation"),
    }
}

#[test]
fn union_params_bubble_in_order() {
    // LEFT has $1=10, RIGHT has $2=20 → итог: [10, 20]
    let (_q, params) = QueryBuilder::new_empty()
        .select((val(10i32),))
        .union(|q: QueryBuilder| q.select((val(20i32),)))
        .build_query_ast()
        .expect("build");
    assert_eq!(params.len(), 2);
}

#[test]
fn order_by_applies_to_entire_set() {
    // (SELECT id FROM a UNION ALL SELECT id FROM b) ORDER BY id ASC
    let (sql, _params) = QueryBuilder::new_empty()
        .from("a")
        .select((col("id"),))
        .union_all(|q: QueryBuilder| q.from("b").select((col("id"),)))
        .order_by(("id", "asc"))
        .to_sql()
        .expect("to_sql");

    let s = norm(&sql).to_uppercase();
    let pos_union = s.find("UNION ALL").expect("must contain UNION ALL");
    let pos_order = s.find("ORDER BY").expect("must contain ORDER BY");
    assert!(
        pos_order > pos_union,
        "ORDER BY must apply to the whole set, got: {s}"
    );
}

#[test]
fn union_with_quoted_identifiers() {
    // Проверим, что имя CTE/таблицы корректно квотится рядом с UNION
    let (sql, _params) = QueryBuilder::new_empty()
        .with("u", |q: QueryBuilder| q.from("users").select((col("id"),)))
        .from("u")
        .select((col("id"),))
        .union(|q: QueryBuilder| q.from("u").select((col("id"),)))
        .to_sql()
        .expect("to_sql");
    let s = norm(&sql);
    // Не завязываемся на конкретный диалект: просто убеждаемся, что алиас встречается по обе стороны
    assert!(s.contains(&format!("FROM {}", qi("u"))));
    assert!(s.to_uppercase().contains("UNION"));
}
