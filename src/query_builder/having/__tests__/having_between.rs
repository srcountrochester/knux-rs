use super::super::super::*;
use crate::expression::helpers::{col, val};
use sqlparser::ast::{BinaryOperator as BO, Expr as SqlExpr, Query, SetExpr};

/// Аккуратно достаём HAVING из AST
fn extract_having(q: &Query) -> Option<&SqlExpr> {
    match q.body.as_ref() {
        SetExpr::Select(sel) => sel.having.as_ref(),
        _ => None,
    }
}

#[test]
fn having_between_and_not_between() {
    // HAVING x BETWEEN 10 AND 20
    let qb1 = QueryBuilder::new_empty()
        .from("t")
        .select(("x",))
        .group_by(("x",))
        .having_between(col("x"), val(10), val(20));

    let (q1, p1) = qb1.build_query_ast().expect("ok");
    assert_eq!(p1.len(), 2, "ожидалось 2 параметра (10 и 20)");
    let h1 = extract_having(&q1).expect("HAVING must exist");
    match h1 {
        SqlExpr::Between { negated, .. } => assert!(!negated, "должно быть negated=false"),
        other => panic!("ожидался Between, получено: {other:?}"),
    }

    // HAVING x NOT BETWEEN 10 AND 20
    let qb2 = QueryBuilder::new_empty()
        .from("t")
        .select(("x",))
        .group_by(("x",))
        .having_not_between(col("x"), val(10), val(20));

    let (q2, p2) = qb2.build_query_ast().expect("ok");
    assert_eq!(p2.len(), 2, "ожидалось 2 параметра (10 и 20)");
    let h2 = extract_having(&q2).expect("HAVING must exist");
    match h2 {
        SqlExpr::Between { negated, .. } => assert!(*negated, "должно быть negated=true"),
        other => panic!("ожидался Between, получено: {other:?}"),
    }
}

#[test]
fn or_having_between_combines_with_or() {
    let qb = QueryBuilder::new_empty()
        .from("t")
        .select(("x",))
        .group_by(("x",))
        .having_between(col("x"), val(1), val(10))
        .or_having_between(col("x"), val(100), val(200));

    let (q, params) = qb.build_query_ast().expect("ok");
    assert_eq!(params.len(), 4, "ожидалось 4 параметра (1,10,100,200)");

    let h = extract_having(&q).unwrap();
    match h {
        SqlExpr::BinaryOp { op, left, right } => {
            assert_eq!(*op, BO::Or, "верхний оператор должен быть OR");
            assert!(
                matches!(**left, SqlExpr::Between { negated: false, .. }),
                "левая часть: BETWEEN"
            );
            assert!(
                matches!(**right, SqlExpr::Between { negated: false, .. }),
                "правая часть: BETWEEN"
            );
        }
        other => panic!("ожидался BinaryOp(OR) над двумя BETWEEN, получено: {other:?}"),
    }
}

#[test]
fn or_having_not_between_combines_with_or() {
    let qb = QueryBuilder::new_empty()
        .from("t")
        .select(("x",))
        .group_by(("x",))
        .having_not_between(col("x"), val(1), val(10))
        .or_having_not_between(col("x"), val(100), val(200));

    let (q, params) = qb.build_query_ast().expect("ok");
    assert_eq!(params.len(), 4);

    let h = extract_having(&q).unwrap();
    match h {
        SqlExpr::BinaryOp { op, left, right } => {
            assert_eq!(*op, BO::Or);
            assert!(matches!(**left, SqlExpr::Between { negated: true, .. }));
            assert!(matches!(**right, SqlExpr::Between { negated: true, .. }));
        }
        other => panic!("ожидался BinaryOp(OR) над двумя NOT BETWEEN, получено: {other:?}"),
    }
}

#[test]
fn having_between_collects_params_only_from_bounds() {
    // target без параметров, границы через val(..) — должно быть 2 параметра
    let qb = QueryBuilder::new_empty()
        .from("t")
        .select(("x",))
        .group_by(("x",))
        .having_between(col("x"), val(5), val(15));

    let (_q, params) = qb.build_query_ast().expect("ok");
    assert_eq!(params.len(), 2);
    assert!(matches!(params[0], crate::param::Param::I32(5)));
    assert!(matches!(params[1], crate::param::Param::I32(15)));
}
