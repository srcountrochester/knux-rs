use crate::expression::helpers::{col, val};
use crate::query_builder::QueryBuilder;
use crate::tests::dialect_test_helpers::qi;
use crate::type_helpers::QBClosureHelper;
use sqlparser::ast::{SetExpr, SetOperator, SetQuantifier};

type QB = QueryBuilder<'static, ()>;

fn norm(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

//
// INTERSECT
//

#[test]
fn intersect_basic_sql_contains_intersect() {
    let (sql, _params) = QB::new_empty()
        .from("a")
        .select((col("id"),))
        .intersect::<QBClosureHelper<()>>(|q| q.from("b").select((col("id"),)))
        .to_sql()
        .expect("to_sql");

    let s = norm(&sql).to_uppercase();
    assert!(
        s.contains(" INTERSECT "),
        "expected INTERSECT in SQL, got: {s}"
    );
    assert!(
        !s.contains("INTERSECT ALL"),
        "plain INTERSECT must be DISTINCT by default"
    );
}

#[test]
fn intersect_all_sql_contains_intersect_all() {
    let (sql, _params) = QB::new_empty()
        .from("a")
        .select((col("id"),))
        .intersect_all::<QBClosureHelper<()>>(|q| q.from("b").select((col("id"),)))
        .to_sql()
        .expect("to_sql");

    let s = norm(&sql).to_uppercase();
    assert!(
        s.contains(" INTERSECT ALL "),
        "expected INTERSECT ALL in SQL, got: {s}"
    );
}

#[test]
fn intersect_builds_setexpr_ast() {
    let (q, _params) = QB::new_empty()
        .from("a")
        .select((col("id"),))
        .intersect::<QBClosureHelper<()>>(|q| q.from("b").select((col("id"),)))
        .build_query_ast()
        .expect("build");

    match *q.body {
        SetExpr::SetOperation {
            op,
            set_quantifier,
            ref left,
            ref right,
        } => {
            assert!(matches!(op, SetOperator::Intersect));
            assert!(
                matches!(set_quantifier, SetQuantifier::None),
                "DISTINCT/None for INTERSECT"
            );
            assert!(matches!(**left, SetExpr::Select(_)));
            assert!(matches!(**right, SetExpr::Query(_) | SetExpr::Select(_)));
        }
        other => panic!("expected SetOperation AST, got {other:?}"),
    }
}

//
// EXCEPT
//

#[test]
fn except_basic_sql_contains_except() {
    let (sql, _params) = QB::new_empty()
        .from("a")
        .select((col("id"),))
        .except::<QBClosureHelper<()>>(|q| q.from("b").select((col("id"),)))
        .to_sql()
        .expect("to_sql");

    let s = norm(&sql).to_uppercase();
    assert!(s.contains(" EXCEPT "), "expected EXCEPT in SQL, got: {s}");
    assert!(
        !s.contains("EXCEPT ALL"),
        "plain EXCEPT must be DISTINCT by default"
    );
}

#[test]
fn except_all_sql_contains_except_all() {
    let (sql, _params) = QB::new_empty()
        .from("a")
        .select((col("id"),))
        .except_all::<QBClosureHelper<()>>(|q| q.from("b").select((col("id"),)))
        .to_sql()
        .expect("to_sql");

    let s = norm(&sql).to_uppercase();
    assert!(
        s.contains(" EXCEPT ALL "),
        "expected EXCEPT ALL in SQL, got: {s}"
    );
}

#[test]
fn except_builds_setexpr_ast() {
    let (q, _params) = QB::new_empty()
        .from("a")
        .select((col("id"),))
        .except::<QBClosureHelper<()>>(|q| q.from("b").select((col("id"),)))
        .build_query_ast()
        .expect("build");

    match *q.body {
        SetExpr::SetOperation {
            op,
            set_quantifier,
            ref left,
            ref right,
        } => {
            assert!(matches!(op, SetOperator::Except));
            assert!(matches!(set_quantifier, SetQuantifier::None));
            assert!(matches!(**left, SetExpr::Select(_)));
            assert!(matches!(**right, SetExpr::Query(_) | SetExpr::Select(_)));
        }
        _ => panic!("expected SetOperation AST"),
    }
}

//
// Связки, порядок и ORDER BY
//

#[test]
fn set_chain_left_associative_and_quantifiers() {
    // (A INTERSECT B) EXCEPT ALL C
    let (q, _params) = QB::new_empty()
        .from("a")
        .select((col("id"),))
        .intersect::<QBClosureHelper<()>>(|q| q.from("b").select((col("id"),)))
        .except_all::<QBClosureHelper<()>>(|q| q.from("c").select((col("id"),)))
        .build_query_ast()
        .expect("build");

    match *q.body {
        SetExpr::SetOperation {
            op,
            set_quantifier,
            ref left,
            ref right,
        } => {
            assert!(matches!(op, SetOperator::Except));
            assert!(matches!(set_quantifier, SetQuantifier::All));

            match **left {
                SetExpr::SetOperation {
                    op, set_quantifier, ..
                } => {
                    assert!(matches!(op, SetOperator::Intersect));
                    assert!(matches!(set_quantifier, SetQuantifier::None));
                }
                _ => panic!("left must be (A INTERSECT B)"),
            }

            assert!(matches!(**right, SetExpr::Query(_) | SetExpr::Select(_)));
        }
        _ => panic!("top-level must be SetOperation"),
    }
}

#[test]
fn set_params_bubble_in_order_intersect_except() {
    // LEFT: $1=10; RHS1: $2=20; RHS2: $3=30  → итог: [10,20,30]
    let (_q, params) = QB::new_empty()
        .select((val(10i32),))
        .intersect::<QBClosureHelper<()>>(|q| q.select((val(20i32),)))
        .except_all::<QBClosureHelper<()>>(|q| q.select((val(30i32),)))
        .build_query_ast()
        .expect("build");
    assert_eq!(params.len(), 3);
}

#[test]
fn order_by_applies_to_entire_set_for_intersect_except() {
    let (sql, _params) = QB::new_empty()
        .from("a")
        .select((col("id"),))
        .intersect_all::<QBClosureHelper<()>>(|q| q.from("b").select((col("id"),)))
        .order_by(("id", "asc"))
        .to_sql()
        .expect("to_sql");

    let s = norm(&sql).to_uppercase();
    let pos_set = s.find("INTERSECT ALL").expect("must contain INTERSECT ALL");
    let pos_order = s.find("ORDER BY").expect("must contain ORDER BY");
    assert!(
        pos_order > pos_set,
        "ORDER BY must apply to the whole set, got: {s}"
    );
}

#[test]
fn set_with_quoted_identifiers() {
    // Проверим корректный квотинг вокруг set-операторов
    let (sql, _params) = QB::new_empty()
        .with::<QBClosureHelper<()>>("t", |q| q.from("users").select((col("id"),)))
        .from("t")
        .select((col("id"),))
        .except::<QBClosureHelper<()>>(|q| q.from("t").select((col("id"),)))
        .to_sql()
        .expect("to_sql");
    let s = norm(&sql);
    assert!(s.contains(&format!("FROM {}", qi("t"))));
    assert!(s.to_uppercase().contains(" EXCEPT "));
}
