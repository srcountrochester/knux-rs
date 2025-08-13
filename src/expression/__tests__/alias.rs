use crate::expression::{Expression, col, val};
use sqlparser::ast;

#[test]
fn as_sets_alias_on_plain_col() {
    let (expr, alias, params) = col("id").r#as("user_id").__into_parts();
    // AST не меняется — это всё ещё идентификатор
    assert!(matches!(expr, ast::Expr::Identifier(_)));
    // алиас установлен
    assert_eq!(alias.as_deref(), Some("user_id"));
    // параметров не добавляется
    assert!(params.is_empty());
}

#[test]
fn alias_synonym_behaves_like_as() {
    let (expr_as, alias_as, _) = col("name").r#as("n").__into_parts();
    let (expr_alias, alias_alias, _) = col("name").alias("n").__into_parts();

    // Оба варианта дают идентичный результат по смыслу:
    assert!(matches!(expr_as, ast::Expr::Identifier(_)));
    assert!(matches!(expr_alias, ast::Expr::Identifier(_)));
    assert_eq!(alias_as.as_deref(), Some("n"));
    assert_eq!(alias_alias.as_deref(), Some("n"));
}

#[test]
fn alias_overwrites_previous_alias() {
    let (expr, alias, _) = col("id").r#as("first").alias("second").__into_parts();
    assert!(matches!(expr, ast::Expr::Identifier(_)));
    // последний вызов побеждает
    assert_eq!(alias.as_deref(), Some("second"));
}

#[test]
fn alias_after_aggregate() {
    // важно: аггрегаты в вашем коде сбрасывают alias перед установкой новой функции,
    // поэтому алиас надо ставить ПОСЛЕ max()/count()/...
    let (expr, alias, params) = col("id").max().r#as("max_id").__into_parts();

    // Проверяем, что это Function MAX(...)
    match expr {
        ast::Expr::Function(func) => {
            // имя функции
            if let Some(ast::ObjectNamePart::Identifier(ident)) = func.name.0.first() {
                assert_eq!(ident.value.to_uppercase(), "MAX");
            } else {
                panic!("expected function name MAX");
            }
            // аргумент — идентификатор "id"
            match &func.args {
                ast::FunctionArguments::List(list) => {
                    assert_eq!(list.args.len(), 1);
                    match &list.args[0] {
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(inner)) => {
                            assert!(matches!(inner, ast::Expr::Identifier(_)));
                        }
                        other => panic!("unexpected arg: {:?}", other),
                    }
                }
                _ => panic!("expected args list"),
            }
        }
        _ => panic!("expected Expr::Function"),
    }

    // алиас проставлен и параметры не изменились
    assert_eq!(alias.as_deref(), Some("max_id"));
    assert!(params.is_empty());
}

#[test]
fn alias_keeps_params_intact() {
    // алиас не должен влиять на набор параметров
    let (expr, alias, params) = val(42).r#as("answer").__into_parts();

    // сам expr — плейсхолдер/литерал-значение
    assert!(matches!(expr, ast::Expr::Value(_)));
    // алиас применён
    assert_eq!(alias.as_deref(), Some("answer"));
    // параметр остался 1 (значение 42)
    assert_eq!(params.len(), 1);
}
