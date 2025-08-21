use crate::renderer::{
    ast as R,
    map::utils::{
        literal_u64, map_bin_op, map_expr, map_select_item, map_un_op, object_name_join,
        object_name_to_strings, split_object_name, split_object_name_cow,
    },
};
use sqlparser::{ast as S, tokenizer::Span};

// ---------- ObjectName helpers ----------

fn on(parts: &[&str]) -> S::ObjectName {
    S::ObjectName(
        parts
            .iter()
            .map(|s| S::ObjectNamePart::Identifier(S::Ident::new(*s)))
            .collect(),
    )
}

#[test]
fn object_name_to_strings_basic() {
    let obj = on(&["schema", "table"]);
    let v = object_name_to_strings(&obj);
    assert_eq!(v, vec!["schema".to_string(), "table".to_string()]);
}

#[test]
fn split_object_name_one_and_two_parts() {
    let (sch, name) = split_object_name(&on(&["t"]));
    assert_eq!(sch, None);
    assert_eq!(name, "t");

    let (sch, name) = split_object_name(&on(&["s", "t"]));
    assert_eq!(sch.as_deref(), Some("s"));
    assert_eq!(name, "t");
}

#[test]
fn object_name_join_with_dot() {
    let obj = on(&["a", "b", "c"]);
    let s = object_name_join(&obj, ".");
    assert_eq!(s, "a.b.c");

    let empty = S::ObjectName(vec![]);
    assert_eq!(object_name_join(&empty, "."), "");
}

#[test]
fn split_object_name_cow_identifiers() {
    let obj = on(&["s", "t"]);
    let (sch, name) = split_object_name_cow(&obj);
    assert_eq!(sch.as_deref(), Some("s"));
    assert_eq!(name.as_ref(), "t");

    let obj1 = on(&["t"]);
    let (sch1, name1) = split_object_name_cow(&obj1);
    assert!(sch1.is_none());
    assert_eq!(name1.as_ref(), "t");
}

// ---------- literal_u64 ----------

fn num(n: &str) -> S::Expr {
    S::Expr::Value(S::ValueWithSpan {
        value: S::Value::Number(n.into(), false),
        span: Span::empty(),
    })
}

#[test]
fn literal_u64_number_and_unary() {
    assert_eq!(literal_u64(&num("10")), Some(10));

    let plus = S::Expr::UnaryOp {
        op: S::UnaryOperator::Plus,
        expr: Box::new(num("7")),
    };
    assert_eq!(literal_u64(&plus), Some(7));

    let minus = S::Expr::UnaryOp {
        op: S::UnaryOperator::Minus,
        expr: Box::new(num("5")),
    };
    assert_eq!(literal_u64(&minus), None);
}

// ---------- wildcard/select item mapping ----------

#[test]
fn map_select_item_wildcards_and_expr() {
    // *
    let it = S::SelectItem::Wildcard(S::WildcardAdditionalOptions::default());
    match map_select_item(&it) {
        R::SelectItem::Star { opts } => assert!(opts.is_none()),
        other => panic!("expected Star, got {:?}", other),
    }

    // u.*
    let kind = S::SelectItemQualifiedWildcardKind::ObjectName(on(&["u"]));
    let it = S::SelectItem::QualifiedWildcard(kind, S::WildcardAdditionalOptions::default());
    match map_select_item(&it) {
        R::SelectItem::QualifiedStar { table, opts } => {
            assert_eq!(table, "u");
            assert!(opts.is_none());
        }
        other => panic!("expected QualifiedStar, got {:?}", other),
    }

    // expr with alias
    let it = S::SelectItem::ExprWithAlias {
        expr: S::Expr::Identifier(S::Ident::new("x")),
        alias: S::Ident::new("k"),
    };
    match map_select_item(&it) {
        R::SelectItem::Expr { expr, alias } => {
            assert_eq!(alias.as_deref(), Some("k"));
            // expr => Ident(["x"])
            match expr {
                R::Expr::Ident { path } => assert_eq!(path, vec!["x"]),
                other => panic!("expected Ident, got {:?}", other),
            }
        }
        other => panic!("expected ExprWithAlias -> R::Expr, got {:?}", other),
    }
}

// ---------- map_expr: базовые случаи ----------

fn val_str(s: &str) -> S::Expr {
    S::Expr::Value(S::ValueWithSpan {
        value: S::Value::SingleQuotedString(s.into()),
        span: Span::empty(),
    })
}

#[test]
fn map_expr_ident_and_compound() {
    // id
    match map_expr(&S::Expr::Identifier(S::Ident::new("id"))) {
        R::Expr::Ident { path } => assert_eq!(path, vec!["id"]),
        other => panic!("expected Ident, got {:?}", other),
    }
    // s.t
    match map_expr(&S::Expr::CompoundIdentifier(vec![
        S::Ident::new("s"),
        S::Ident::new("t"),
    ])) {
        R::Expr::Ident { path } => assert_eq!(path, vec!["s", "t"]),
        other => panic!("expected Ident, got {:?}", other),
    }
}

#[test]
fn map_expr_values() {
    // 'ok'
    match map_expr(&val_str("ok")) {
        R::Expr::String(s) => assert_eq!(s, "ok"),
        other => panic!("expected String, got {:?}", other),
    }
    // 42
    match map_expr(&num("42")) {
        R::Expr::Number(s) => assert_eq!(s, "42"),
        other => panic!("expected Number, got {:?}", other),
    }
    // true
    let b = S::Expr::Value(S::ValueWithSpan {
        value: S::Value::Boolean(true),
        span: Span::empty(),
    });
    match map_expr(&b) {
        R::Expr::Bool(v) => assert!(v),
        other => panic!("expected Bool(true), got {:?}", other),
    }
    // null
    let n = S::Expr::Value(S::ValueWithSpan {
        value: S::Value::Null,
        span: Span::empty(),
    });
    match map_expr(&n) {
        R::Expr::Null => {}
        other => panic!("expected Null, got {:?}", other),
    }
    // placeholder -> Bind
    let ph = S::Expr::Value(S::ValueWithSpan {
        value: S::Value::Placeholder("$1".into()),
        span: Span::empty(),
    });
    match map_expr(&ph) {
        R::Expr::Bind => {}
        other => panic!("expected Bind, got {:?}", other),
    }
}

#[test]
fn map_expr_unary_and_binary_and_between() {
    // -id
    let un = S::Expr::UnaryOp {
        op: S::UnaryOperator::Minus,
        expr: Box::new(S::Expr::Identifier(S::Ident::new("id"))),
    };
    match map_expr(&un) {
        R::Expr::Unary {
            op: R::UnOp::Neg, ..
        } => {}
        other => panic!("expected Unary(Neg), got {:?}", other),
    }

    // a = 1
    let bin = S::Expr::BinaryOp {
        left: Box::new(S::Expr::Identifier(S::Ident::new("a"))),
        op: S::BinaryOperator::Eq,
        right: Box::new(num("1")),
    };
    match map_expr(&bin) {
        R::Expr::Binary {
            op: R::BinOp::Eq, ..
        } => {}
        other => panic!("expected Binary(Eq), got {:?}", other),
    }

    // x BETWEEN 1 AND 2 (не negated)
    let btw = S::Expr::Between {
        expr: Box::new(S::Expr::Identifier(S::Ident::new("x"))),
        low: Box::new(num("1")),
        high: Box::new(num("2")),
        negated: false,
    };
    match map_expr(&btw) {
        R::Expr::Binary {
            op: R::BinOp::And, ..
        } => {} // разворачивается в (x>=1 AND x<=2)
        other => panic!("expected Binary(And), got {:?}", other),
    }

    // NOT (x BETWEEN 1 AND 2) -> Unary(Not, ...)
    let nbtw = S::Expr::Between {
        expr: Box::new(S::Expr::Identifier(S::Ident::new("x"))),
        low: Box::new(num("1")),
        high: Box::new(num("2")),
        negated: true,
    };
    match map_expr(&nbtw) {
        R::Expr::Unary {
            op: R::UnOp::Not, ..
        } => {}
        other => panic!("expected Unary(Not), got {:?}", other),
    }
}

#[test]
fn map_expr_isnull_inlist_like_nested_cast_collate() {
    // a IS NULL
    let isnull = S::Expr::IsNull(Box::new(S::Expr::Identifier(S::Ident::new("a"))));
    match map_expr(&isnull) {
        R::Expr::Binary {
            op: R::BinOp::Is, ..
        } => {}
        other => panic!("expected IS, got {:?}", other),
    }

    // a IN (1,2)
    let inlist = S::Expr::InList {
        expr: Box::new(S::Expr::Identifier(S::Ident::new("a"))),
        list: vec![num("1"), num("2")],
        negated: false,
    };
    match map_expr(&inlist) {
        R::Expr::Binary {
            op: R::BinOp::In,
            right,
            ..
        } => match &*right {
            R::Expr::Tuple(v) => assert_eq!(v.len(), 2),
            other => panic!("expected Tuple RHS, got {:?}", other),
        },
        other => panic!("expected Binary(In), got {:?}", other),
    }

    // LIKE без ESCAPE
    let like = S::Expr::Like {
        negated: false,
        expr: Box::new(S::Expr::Identifier(S::Ident::new("a"))),
        pattern: Box::new(val_str("%x%")),
        escape_char: None,
        any: false,
    };
    match map_expr(&like) {
        R::Expr::Like {
            not: false,
            ilike: false,
            escape,
            ..
        } => assert!(escape.is_none()),
        other => panic!("expected Like, got {:?}", other),
    }

    // Nested + Cast + Collate
    let nested = S::Expr::Nested(Box::new(S::Expr::Identifier(S::Ident::new("x"))));
    match map_expr(&nested) {
        R::Expr::Paren(inner) => match *inner {
            R::Expr::Ident { .. } => {}
            other => panic!("expected Paren(Ident), got {:?}", other),
        },
        other => panic!("expected Paren, got {:?}", other),
    }

    let cast = S::Expr::Cast {
        expr: Box::new(S::Expr::Identifier(S::Ident::new("n"))),
        data_type: S::DataType::Int(None),
        format: None,
        kind: S::CastKind::Cast,
    };
    match map_expr(&cast) {
        R::Expr::Cast { ty, .. } => assert!(ty.to_lowercase().contains("int")),
        other => panic!("expected Cast, got {:?}", other),
    }

    let coll = S::Expr::Collate {
        expr: Box::new(S::Expr::Identifier(S::Ident::new("s"))),
        collation: on(&["C"]),
    };
    match map_expr(&coll) {
        R::Expr::Collate { collation, .. } => assert_eq!(collation, "C"),
        other => panic!("expected Collate, got {:?}", other),
    }
}

// ---------- binop/unop ----------

#[test]
fn map_bin_op_and_un_op() {
    use R::BinOp as B;
    assert_eq!(map_bin_op(&S::BinaryOperator::Eq), B::Eq);
    assert_eq!(map_bin_op(&S::BinaryOperator::Plus), B::Add);
    assert_eq!(map_un_op(&S::UnaryOperator::Not), R::UnOp::Not);
    assert_eq!(map_un_op(&S::UnaryOperator::Minus), R::UnOp::Neg);
}
