use crate::expression::{col, lit, raw, val};
use crate::param::Param;
use sqlparser::ast;

#[test]
fn col_builds_identifier_and_compound() {
    // простой идентификатор
    let (e1, a1, p1) = col("id").__into_parts();
    assert!(matches!(e1, ast::Expr::Identifier(_)));
    assert!(a1.is_none());
    assert!(p1.is_empty());

    // составной идентификатор
    let (e2, a2, p2) = col("users.id").__into_parts();
    match e2 {
        ast::Expr::CompoundIdentifier(idents) => {
            assert_eq!(idents.len(), 2);
            assert_eq!(idents[0].value, "users");
            assert_eq!(idents[1].value, "id");
        }
        other => panic!("expected CompoundIdentifier, got {:?}", other),
    }
    assert!(a2.is_none());
    assert!(p2.is_empty());
}

#[test]
fn val_inserts_placeholder_and_collects_param_i32() {
    let (e, alias, params) = val(42i32).__into_parts();

    // в AST должен быть Value (мы кладём плейсхолдер)
    assert!(matches!(e, ast::Expr::Value(_)));
    assert!(alias.is_none());

    // в params один элемент и это именно наш 42 (через Into<Param>)
    assert_eq!(params.len(), 1);
    match &params[0] {
        Param::I32(v) => assert_eq!(*v, 42),
        other => panic!("expected Param::I32(42), got {:?}", other),
    }
}

#[test]
fn val_works_with_string_sources() {
    let (e, alias, params) = val("hello").__into_parts();
    assert!(matches!(e, ast::Expr::Value(_)));
    assert!(alias.is_none());
    assert_eq!(params.len(), 1);
    match &params[0] {
        Param::Str(s) => assert_eq!(s, "hello"),
        other => panic!("expected Param::Str(\"hello\"), got {:?}", other),
    }

    // и для String
    let (e2, _, params2) = val(String::from("world")).__into_parts();
    assert!(matches!(e2, ast::Expr::Value(_)));
    assert_eq!(params2.len(), 1);
    match &params2[0] {
        Param::Str(s) => assert_eq!(s, "world"),
        other => panic!("expected Param::Str(\"world\"), got {:?}", other),
    }
}

#[test]
fn lit_builds_single_quoted_literal_without_params() {
    let (e, alias, params) = lit("abc").__into_parts();
    match e {
        ast::Expr::Value(vws) => match vws.value {
            ast::Value::SingleQuotedString(s) => assert_eq!(s, "abc"),
            other => panic!("expected SingleQuotedString, got {:?}", other),
        },
        other => panic!("expected Expr::Value, got {:?}", other),
    }
    assert!(alias.is_none());
    assert!(params.is_empty());
}

#[test]
fn raw_wraps_custom_expr_without_params() {
    // сделаем RAW: CURRENT_TIMESTAMP (как идентификатор-функция без скобок для простоты)
    let (e, alias, params) =
        raw(|| ast::Expr::Identifier(ast::Ident::new("CURRENT_TIMESTAMP"))).__into_parts();

    assert!(matches!(e, ast::Expr::Identifier(_)));
    assert!(alias.is_none());
    assert!(params.is_empty());
}

#[test]
fn val_f32_and_f64_params() {
    use crate::param::Param;
    let (_, _, p1) = crate::expression::val(1.5f32).__into_parts();
    assert!(matches!(p1.as_slice(), [Param::F32(v)] if (*v - 1.5).abs() < 1e-6));

    let (_, _, p2) = crate::expression::val(2.5f64).__into_parts();
    assert!(matches!(p2.as_slice(), [Param::F64(v)] if (*v - 2.5).abs() < 1e-12));
}

#[test]
fn col_three_part_compound() {
    use sqlparser::ast;
    let (e, _, _) = crate::expression::col("a.b.c").__into_parts();
    match e {
        ast::Expr::CompoundIdentifier(parts) => {
            assert_eq!(parts.len(), 3);
            assert_eq!(parts[0].value, "a");
            assert_eq!(parts[1].value, "b");
            assert_eq!(parts[2].value, "c");
        }
        _ => panic!("expected CompoundIdentifier"),
    }
}
