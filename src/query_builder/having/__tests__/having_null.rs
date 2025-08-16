use super::super::super::*;
use crate::expression::helpers::col;
use sqlparser::ast::{BinaryOperator as BO, Expr as SqlExpr, Query, SetExpr};

/// Достаём ссылку на HAVING из AST без клонов.
fn extract_having(q: &Query) -> Option<&SqlExpr> {
    match q.body.as_ref() {
        SetExpr::Select(sel) => sel.having.as_ref(),
        _ => None,
    }
}

#[test]
fn having_null_basic() {
    let qb = QueryBuilder::new_empty()
        .from("t")
        .select(("x",))
        .group_by(("x",))
        .having_null(col("deleted_at"));

    let (q, params) = qb.build_query_ast().expect("ok");
    assert!(params.is_empty(), "NULL-предикаты не добавляют параметров");

    let h = extract_having(&q).expect("HAVING must exist");
    assert!(
        matches!(h, SqlExpr::IsNull(_)),
        "ожидался IsNull(..), got: {h:?}"
    );
}

#[test]
fn having_not_null_basic() {
    let qb = QueryBuilder::new_empty()
        .from("t")
        .select(("x",))
        .group_by(("x",))
        .having_not_null(col("updated_at"));

    let (q, params) = qb.build_query_ast().expect("ok");
    assert!(params.is_empty());

    let h = extract_having(&q).expect("HAVING must exist");
    assert!(
        matches!(h, SqlExpr::IsNotNull(_)),
        "ожидался IsNotNull(..), got: {h:?}"
    );
}

#[test]
fn or_having_null_combines_with_or() {
    let qb = QueryBuilder::new_empty()
        .from("t")
        .select(("x",))
        .group_by(("x",))
        .having_null(col("a"))
        .or_having_null(col("b"));

    let (q, _params) = qb.build_query_ast().expect("ok");
    let h = extract_having(&q).unwrap();

    match h {
        SqlExpr::BinaryOp { op, left, right } => {
            assert_eq!(*op, BO::Or, "верхний оператор должен быть OR");
            assert!(
                matches!(**left, SqlExpr::IsNull(_)),
                "левая часть: IsNull(..)"
            );
            assert!(
                matches!(**right, SqlExpr::IsNull(_)),
                "правая часть: IsNull(..)"
            );
        }
        other => panic!("ожидался BinaryOp(OR), получено: {:?}", other),
    }
}

#[test]
fn having_null_then_not_null_combines_with_and() {
    let qb = QueryBuilder::new_empty()
        .from("t")
        .select(("x",))
        .group_by(("x",))
        .having_null(col("a"))
        .having_not_null(col("b"));

    let (q, _params) = qb.build_query_ast().expect("ok");
    let h = extract_having(&q).unwrap();

    match h {
        SqlExpr::BinaryOp { op, left, right } => {
            assert_eq!(*op, BO::And, "две having_* должны соединяться AND");
            assert!(matches!(**left, SqlExpr::IsNull(_)), "левая: IsNull(..)");
            assert!(
                matches!(**right, SqlExpr::IsNotNull(_)),
                "правая: IsNotNull(..)"
            );
        }
        other => panic!("ожидался BinaryOp(AND), получено: {:?}", other),
    }
}

#[test]
fn or_having_not_null_combines_with_or() {
    let qb = QueryBuilder::new_empty()
        .from("t")
        .select(("x",))
        .group_by(("x",))
        .having_not_null(col("a"))
        .or_having_not_null(col("b"));

    let (q, _params) = qb.build_query_ast().expect("ok");
    let h = extract_having(&q).unwrap();

    match h {
        SqlExpr::BinaryOp { op, left, right } => {
            assert_eq!(*op, BO::Or);
            assert!(matches!(**left, SqlExpr::IsNotNull(_)));
            assert!(matches!(**right, SqlExpr::IsNotNull(_)));
        }
        other => panic!("ожидался BinaryOp(OR), получено: {:?}", other),
    }
}
