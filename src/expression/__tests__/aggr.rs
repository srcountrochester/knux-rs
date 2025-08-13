use crate::expression::{Expression, col};
use sqlparser::ast::{
    DuplicateTreatment, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectNamePart,
};

fn assert_func_name(expr: &Expr, expected: &str) {
    if let Expr::Function(func) = expr {
        let name = &func.name.0[0];
        match name {
            ObjectNamePart::Identifier(ident) => {
                assert_eq!(ident.value, expected, "function name mismatch");
            }
            _ => panic!("unexpected function name part: {:?}", name),
        }
    } else {
        panic!("expected Expr::Function, got {:?}", expr);
    }
}

fn assert_first_arg_is_identifier(expr: &Expr, expected: &str) {
    if let Expr::Function(func) = expr {
        if let FunctionArguments::List(list) = &func.args {
            match &list.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => match inner {
                    Expr::Identifier(ident) => assert_eq!(ident.value, expected),
                    _ => panic!("expected identifier arg, got {:?}", inner),
                },
                _ => panic!("unexpected arg type"),
            }
        } else {
            panic!("expected FunctionArguments::List");
        }
    }
}

fn assert_first_arg_is_wildcard(expr: &Expr) {
    if let Expr::Function(func) = expr {
        if let FunctionArguments::List(list) = &func.args {
            match &list.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {}
                _ => panic!("expected wildcard arg"),
            }
        } else {
            panic!("expected FunctionArguments::List");
        }
    }
}

#[test]
fn test_count() {
    let expr = col("id").count().expr;
    assert_func_name(&expr, "COUNT");
    assert_first_arg_is_identifier(&expr, "id");
}

#[test]
fn test_count_all() {
    let expr = col("id").count_all().expr;
    assert_func_name(&expr, "COUNT");
    assert_first_arg_is_wildcard(&expr);
}

#[test]
fn test_max() {
    let expr = col("price").max().expr;
    assert_func_name(&expr, "MAX");
    assert_first_arg_is_identifier(&expr, "price");
}

#[test]
fn test_min() {
    let expr = col("price").min().expr;
    assert_func_name(&expr, "MIN");
    assert_first_arg_is_identifier(&expr, "price");
}

#[test]
fn test_sum() {
    let expr = col("price").sum().expr;
    assert_func_name(&expr, "SUM");
    assert_first_arg_is_identifier(&expr, "price");
}

#[test]
fn test_avg() {
    let expr = col("price").avg().expr;
    assert_func_name(&expr, "AVG");
    assert_first_arg_is_identifier(&expr, "price");
}

#[test]
fn test_distinct_for_next() {
    // distinct перед вызовом
    let expr = col("id").distinct().count().expr;
    if let Expr::Function(func) = &expr {
        if let FunctionArguments::List(list) = &func.args {
            assert_eq!(list.duplicate_treatment, Some(DuplicateTreatment::Distinct));
        } else {
            panic!("expected FunctionArguments::List");
        }
    }
}

#[test]
fn test_distinct_on_existing_function() {
    // distinct после вызова
    let expr = col("id").count().distinct().expr;
    if let Expr::Function(func) = &expr {
        if let FunctionArguments::List(list) = &func.args {
            assert_eq!(list.duplicate_treatment, Some(DuplicateTreatment::Distinct));
        } else {
            panic!("expected FunctionArguments::List");
        }
    }
}

#[test]
fn distinct_marker_is_consumed_once() {
    use crate::expression::col;
    use sqlparser::ast::{DuplicateTreatment, Expr, FunctionArguments};

    // distinct навешиваем перед max(); он должен сработать только ОДИН раз
    let (e1, _, _) = col("x").distinct().max().__into_parts();
    if let Expr::Function(fun) = e1 {
        if let FunctionArguments::List(list) = fun.args {
            assert_eq!(list.duplicate_treatment, Some(DuplicateTreatment::Distinct));
        } else {
            panic!("expected args list");
        }
    } else {
        panic!("expected function");
    }

    // следующий вызов sum() уже без distinct — маркер должен быть сброшен
    let (e2, _, _) = col("x").distinct().max().sum().__into_parts();
    if let Expr::Function(fun) = e2 {
        if let FunctionArguments::List(list) = fun.args {
            assert_eq!(list.duplicate_treatment, Some(DuplicateTreatment::All));
        } else {
            panic!("expected args list");
        }
    } else {
        panic!("expected function");
    }
}

#[test]
fn fn_call_with_clauses_sets_clauses() {
    use crate::expression::aggr::fn_call_with_clauses;
    use sqlparser::ast;

    // Например, AVG(x) RESPECT NULLS (или IGNORE NULLS)
    let expr = fn_call_with_clauses(
        "AVG",
        vec![ast::Expr::Identifier(ast::Ident::new("x"))],
        false,
        vec![ast::FunctionArgumentClause::IgnoreOrRespectNulls(
            ast::NullTreatment::RespectNulls,
        )],
    );

    match expr {
        ast::Expr::Function(f) => match f.args {
            ast::FunctionArguments::List(list) => {
                assert_eq!(list.clauses.len(), 1);
                assert!(matches!(
                    list.clauses[0],
                    ast::FunctionArgumentClause::IgnoreOrRespectNulls(
                        ast::NullTreatment::RespectNulls,
                    )
                ));
            }
            _ => panic!("expected args list"),
        },
        _ => panic!("expected function"),
    }
}
