use super::super::super::*;
use crate::expression::helpers::{col, val};
use sqlparser::ast::{BinaryOperator as BO, Expr as SqlExpr, Query, SetExpr};

type QB = QueryBuilder<'static, ()>;

/// Аккуратный helper: достаём ссылку на HAVING из AST без лишних клонов.
fn extract_having(q: &Query) -> Option<&SqlExpr> {
    match q.body.as_ref() {
        SetExpr::Select(sel) => sel.having.as_ref(),
        _ => None,
    }
}

#[test]
fn attach_having_with_and_accumulates() {
    let mut qb = QB::new_empty().from("t").select(("x",));

    // expr1: sum > 10
    let e1 = col("sum").gt(val(10));
    let p1 = e1.params; // вручную переносим параметры
    qb.attach_having_with_and(e1.expr, p1);

    // expr2: sum < 100
    let e2 = col("sum").lt(val(100));
    let p2 = e2.params;
    qb.attach_having_with_and(e2.expr, p2);

    let (q, params) = qb.build_query_ast().expect("ok");
    assert_eq!(
        params.len(),
        2,
        "должно было собраться два параметра (10 и 100)"
    );

    let h = extract_having(&q).expect("HAVING must exist");
    match h {
        SqlExpr::BinaryOp { op, left, right } => {
            assert_eq!(*op, BO::And, "верхний оператор HAVING должен быть AND");
            assert!(
                matches!(**left, SqlExpr::BinaryOp { .. }),
                "левая часть должна быть бинарной"
            );
            assert!(
                matches!(**right, SqlExpr::BinaryOp { .. }),
                "правая часть должна быть бинарной"
            );
        }
        other => panic!("ожидался BinaryOp(AND), получено: {:?}", other),
    }
}

#[test]
fn attach_having_with_or_builds_or_tree() {
    let mut qb = QB::new_empty().from("t").select(("x",));

    // cnt > 1
    let e1 = col("cnt").gt(val(1));
    let p1 = e1.params;
    qb.attach_having_with_and(e1.expr, p1);

    // cnt < 10 (через OR)
    let e2 = col("cnt").lt(val(10));
    let p2 = e2.params;
    qb.attach_having_with_or(e2.expr, p2);

    let (q, params) = qb.build_query_ast().expect("ok");
    assert_eq!(params.len(), 2, "ожидается 2 параметра (1 и 10)");

    let h = extract_having(&q).expect("HAVING must exist");
    match h {
        SqlExpr::BinaryOp { op, left, right } => {
            assert_eq!(*op, BO::Or, "верхний оператор должен быть OR");
            assert!(matches!(**left, SqlExpr::BinaryOp { .. }));
            assert!(matches!(**right, SqlExpr::BinaryOp { .. }));
        }
        other => panic!("ожидался BinaryOp(OR), получено {:?}", other),
    }
}
#[test]
fn resolve_having_group_and_attach_collects_params_and_chains_with_and() {
    let mut qb = QB::new_empty().from("t").select(("x",));

    // группа из двух выражений -> внутри AND
    let (pred, params) = qb
        .resolve_having_group((col("sum").gt(val(10)), col("sum").lt(val(100))))
        .expect("group must produce predicate");

    qb.attach_having_with_and(pred, params);

    let (q, params) = qb.build_query_ast().expect("ok");
    assert_eq!(params.len(), 2, "оба val(..) должны попасть в параметры");

    let h = extract_having(&q).unwrap();
    match h {
        SqlExpr::BinaryOp { op, left, right } => {
            assert_eq!(*op, BO::And, "внутри группы ожидался AND");
            assert!(matches!(**left, SqlExpr::BinaryOp { .. }));
            assert!(matches!(**right, SqlExpr::BinaryOp { .. }));
        }
        other => panic!("ожидался AND над двумя сравнениями, получено {:?}", other),
    }
}

#[test]
fn resolve_having_group_empty_returns_none_and_changes_nothing() {
    let mut qb = QB::new_empty().from("t").select(("x",));

    // пустой список аргументов — None
    let empty: Vec<&str> = Vec::new();
    let res = qb.resolve_having_group(empty);
    assert!(res.is_none(), "пустая группа должна возвращать None");

    // HAVING не должен появиться
    let (q, _params) = qb.build_query_ast().expect("ok");
    assert!(
        extract_having(&q).is_none(),
        "HAVING не должен присутствовать"
    );
}
