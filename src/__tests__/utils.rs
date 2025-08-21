use super::super::utils::*;
use sqlparser::ast as S;

fn obj_parts(obj: &S::ObjectName) -> Vec<String> {
    obj.0.iter().map(|p| p.to_string()).collect()
}

// ---------- num_expr / num_value / strip_span / attach_empty_span ----------

#[test]
fn num_expr_builds_number_expr() {
    let e = num_expr(123);
    assert_eq!(expr_to_string(&e), "123");
}

#[test]
fn num_value_strip_and_attach_roundtrip() {
    let vw = num_value(42);
    let v = strip_span(&vw);
    match &v {
        S::Value::Number(s, neg) => {
            assert_eq!(s, "42");
            assert!(!neg);
        }
        other => panic!("expected Number(42), got {:?}", other),
    }
    let vw2 = attach_empty_span(v.clone());
    assert_eq!(strip_span(&vw2), v);
}

// ---------- value_to_string / expr_to_string ----------

#[test]
fn value_to_string_formats_basic_values() {
    assert_eq!(value_to_string(&S::Value::Number("7".into(), false)), "7");
    assert_eq!(
        value_to_string(&S::Value::SingleQuotedString("hi".into())),
        "'hi'"
    );
    assert_eq!(value_to_string(&S::Value::Boolean(true)), "true");
    assert_eq!(value_to_string(&S::Value::Null), "NULL");
}

#[test]
fn expr_to_string_formats_basic_exprs() {
    let id = S::Expr::Identifier(S::Ident::new("x"));
    assert_eq!(expr_to_string(&id), "x");

    let cid = S::Expr::CompoundIdentifier(vec![S::Ident::new("s"), S::Ident::new("t")]);
    assert_eq!(expr_to_string(&cid), "s.t");
}

// ---------- parse_object_name / object_name_from_default ----------

#[test]
fn parse_object_name_splits_by_dot() {
    let o = parse_object_name("a.b.c");
    assert_eq!(obj_parts(&o), vec!["a", "b", "c"]);
}

#[test]
fn object_name_from_default_prefixed_when_no_schema() {
    // default_schema = Some("s"), table without schema
    let o = object_name_from_default(Some("s"), "t");
    assert_eq!(obj_parts(&o), vec!["s", "t"]);

    // default_schema = None
    let o = object_name_from_default(None, "t");
    assert_eq!(obj_parts(&o), vec!["t"]);

    // table already has schema → default_schema ignored
    let o = object_name_from_default(Some("s"), "x.y");
    assert_eq!(obj_parts(&o), vec!["x", "y"]);
}

// ---------- expr_to_object_name ----------

#[test]
fn expr_to_object_name_identifier_and_compound() {
    // Identifier + no default schema
    let e = S::Expr::Identifier(S::Ident::new("t"));
    let o = expr_to_object_name(e, None).expect("should map");
    assert_eq!(obj_parts(&o), vec!["t"]);

    // Identifier + default schema
    let e = S::Expr::Identifier(S::Ident::new("t"));
    let o = expr_to_object_name(e, Some("s")).expect("should map");
    assert_eq!(obj_parts(&o), vec!["s", "t"]);

    // CompoundIdentifier
    let e = S::Expr::CompoundIdentifier(vec![S::Ident::new("a"), S::Ident::new("b")]);
    let o = expr_to_object_name(e, Some("ignored")).expect("should map");
    assert_eq!(obj_parts(&o), vec!["a", "b"]);
}

#[test]
fn expr_to_object_name_non_identifier_returns_none() {
    let e = S::Expr::Value(S::Value::Number("1".into(), false).into());
    assert!(expr_to_object_name(e, None).is_none());
}
