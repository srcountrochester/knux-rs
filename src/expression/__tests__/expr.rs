use crate::expression::Expression;
use crate::expression::{col, lit, val};
use sqlparser::ast;

#[test]
fn col_builds_identifier_and_compound() {
    // simple
    let (e, alias, params) = col("id").clone().__into_parts();
    assert!(matches!(e, ast::Expr::Identifier(_)));
    assert!(alias.is_none());
    assert!(params.is_empty());

    // compound
    let (e2, _, _) = col("users.id").clone().__into_parts();
    match e2 {
        ast::Expr::CompoundIdentifier(idents) => {
            assert_eq!(idents.len(), 2);
            assert_eq!(idents[0].value, "users");
            assert_eq!(idents[1].value, "id");
        }
        _ => panic!("expected CompoundIdentifier"),
    }
}

#[test]
fn val_binds_one_param_placeholder() {
    let (e, alias, params) = val(123i32).__into_parts();
    match e {
        ast::Expr::Value(v) => {
            // проверяем, что внутри плейсхолдер '?'
            let s = format!("{:?}", v);
            assert!(s.contains("?"));
        }
        _ => panic!("expected Value placeholder"),
    }
    assert!(alias.is_none());
    assert_eq!(params.len(), 1);
}

#[test]
fn max_with_alias() {
    let (e, alias, params) = col("id").max().r#as("max_id").__into_parts();
    match e {
        ast::Expr::Function(fun) => {
            let name = fun.name.0;
            assert_eq!(name.len(), 1);
            match &name[0] {
                ast::ObjectNamePart::Identifier(ident) => {
                    assert_eq!(ident.value.to_uppercase(), "MAX")
                }
                _ => panic!("expected identifier"),
            }
            match &fun.args {
                ast::FunctionArguments::List(list) => {
                    assert_eq!(list.args.len(), 1);
                    match &list.args[0] {
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(inner)) => {
                            assert!(matches!(inner, ast::Expr::Identifier(_)));
                        }
                        other => panic!("unexpected arg: {:?}", other),
                    }
                }
                _ => panic!("expected FunctionArguments::List"),
            }
        }
        _ => panic!("expected Function"),
    }
    assert_eq!(alias.as_deref(), Some("max_id"));
    assert!(params.is_empty());
}

#[test]
fn count_then_distinct_marks_duplicate_treatment() {
    let (e, _, _) = col("id").count().distinct().__into_parts();
    match e {
        ast::Expr::Function(fun) => match &fun.args {
            ast::FunctionArguments::List(list) => {
                assert!(matches!(
                    list.duplicate_treatment,
                    Some(ast::DuplicateTreatment::Distinct)
                ));
            }
            _ => panic!("expected args List"),
        },
        _ => panic!("expected Function"),
    }
}

#[test]
fn count_all_stars() {
    let (e, _, _) = col("id").count_all().__into_parts();
    match e {
        ast::Expr::Function(fun) => match &fun.args {
            ast::FunctionArguments::List(list) => {
                assert_eq!(list.args.len(), 1);
                match &list.args[0] {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {}
                    _ => panic!("expected Wildcard in COUNT(*)"),
                }
            }
            _ => panic!("expected args List"),
        },
        _ => panic!("expected Function"),
    }
}

#[test]
fn logic_and_math_combo() {
    // (a + 1) > 2 AND b IS NOT NULL
    let expr = col("a").add(val(1)).gt(val(2)).and(col("b").is_not_null());
    let (e, _, params) = expr.__into_parts();
    assert_eq!(params.len(), 2); // 1 и 2
    match e {
        ast::Expr::BinaryOp { op, .. } => {
            assert!(matches!(op, ast::BinaryOperator::And));
        }
        _ => panic!("expected top-level BinaryOp AND"),
    }
}
