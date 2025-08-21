use super::super::delete::*;
use crate::renderer::ast as R;
use sqlparser::{ast as S, tokenizer::Span};

// ------- helpers -------
fn obj(parts: &[&str]) -> S::ObjectName {
    S::ObjectName(
        parts
            .iter()
            .map(|s| S::ObjectNamePart::Identifier(S::Ident::new(*s)))
            .collect(),
    )
}

fn twj(name: &str) -> S::TableWithJoins {
    S::TableWithJoins {
        relation: S::TableFactor::Table {
            name: obj(&[name]),
            alias: None,
            args: None,
            with_hints: vec![],
            version: None,
            partitions: vec![],
            with_ordinality: false,
            index_hints: vec![],
            json_path: None,
            sample: None,
        },
        joins: vec![],
    }
}

fn twj_sa(schema: Option<&str>, name: &str, alias: Option<&str>) -> S::TableWithJoins {
    S::TableWithJoins {
        relation: S::TableFactor::Table {
            name: match schema {
                Some(s) => obj(&[s, name]),
                None => obj(&[name]),
            },
            alias: alias.map(|a| S::TableAlias {
                name: S::Ident::new(a),
                columns: vec![],
            }),
            args: None,
            with_hints: vec![],
            version: None,
            partitions: vec![],
            with_ordinality: false,
            index_hints: vec![],
            json_path: None,
            sample: None,
        },
        joins: vec![],
    }
}

fn num(n: &str) -> S::Expr {
    S::Expr::Value(S::ValueWithSpan {
        value: S::Value::Number(n.into(), false),
        span: Span::empty(),
    })
}

// ------- tests -------

#[test]
fn map_delete_with_from_using_where_returning() {
    // DELETE FROM t USING a, b WHERE id = 1 RETURNING *, x AS k
    let del = S::Delete {
        tables: vec![],
        from: S::FromTable::WithFromKeyword(vec![twj("t")]),
        using: Some(vec![twj("a"), twj("b")]),
        selection: Some(S::Expr::BinaryOp {
            left: Box::new(S::Expr::Identifier(S::Ident::new("id"))),
            op: S::BinaryOperator::Eq,
            right: Box::new(num("1")),
        }),
        returning: Some(vec![
            S::SelectItem::Wildcard(S::WildcardAdditionalOptions::default()),
            S::SelectItem::ExprWithAlias {
                expr: S::Expr::Identifier(S::Ident::new("x")),
                alias: S::Ident::new("k"),
            },
        ]),
        order_by: vec![],
        limit: None,
    };

    let u = map_delete(&del);

    // table
    match &u.table {
        R::TableRef::Named {
            schema,
            name,
            alias,
        } => {
            assert!(schema.is_none());
            assert_eq!(name, "t");
            assert!(alias.is_none());
        }
        other => panic!("unexpected table ref: {:?}", other),
    }

    // using
    assert_eq!(u.using.len(), 2);
    match &u.using[0] {
        R::TableRef::Named {
            schema,
            name,
            alias,
        } => {
            assert!(schema.is_none());
            assert_eq!(name, "a");
            assert!(alias.is_none());
        }
        other => panic!("unexpected using[0]: {:?}", other),
    }
    match &u.using[1] {
        R::TableRef::Named {
            schema,
            name,
            alias,
        } => {
            assert!(schema.is_none());
            assert_eq!(name, "b");
            assert!(alias.is_none());
        }
        other => panic!("unexpected using[1]: {:?}", other),
    }

    // where
    match &u.r#where {
        Some(R::Expr::Binary { op, .. }) => assert!(matches!(op, R::BinOp::Eq)),
        other => panic!("expected WHERE Binary(Eq), got {:?}", other),
    }

    // returning
    assert_eq!(u.returning.len(), 2);
    assert!(matches!(u.returning[0], R::SelectItem::Star { .. }));
    match &u.returning[1] {
        R::SelectItem::Expr { alias, .. } => assert_eq!(alias.as_deref(), Some("k")),
        other => panic!("expected Expr with alias k, got {:?}", other),
    }
}

#[test]
fn map_delete_without_from_keyword() {
    // DELETE t WHERE id = 1
    let del = S::Delete {
        tables: vec![],
        from: S::FromTable::WithoutKeyword(vec![twj("t")]),
        using: None,
        selection: Some(S::Expr::BinaryOp {
            left: Box::new(S::Expr::Identifier(S::Ident::new("id"))),
            op: S::BinaryOperator::Eq,
            right: Box::new(num("1")),
        }),
        returning: None,
        order_by: vec![],
        limit: None,
    };

    let u = map_delete(&del);

    match &u.table {
        R::TableRef::Named {
            schema,
            name,
            alias,
        } => {
            assert!(schema.is_none());
            assert_eq!(name, "t");
            assert!(alias.is_none());
        }
        other => panic!("unexpected table ref: {:?}", other),
    }
    assert!(u.using.is_empty());
    assert!(u.returning.is_empty());
}

#[test]
fn map_delete_target_with_schema_and_alias() {
    // DELETE FROM s.users AS u
    let del = S::Delete {
        tables: vec![],
        from: S::FromTable::WithFromKeyword(vec![twj_sa(Some("s"), "users", Some("u"))]),
        using: None,
        selection: None,
        returning: None,
        order_by: vec![],
        limit: None,
    };

    let u = map_delete(&del);

    match &u.table {
        R::TableRef::Named {
            schema,
            name,
            alias,
        } => {
            assert_eq!(schema.as_deref(), Some("s"));
            assert_eq!(name, "users");
            assert_eq!(alias.as_deref(), Some("u"));
        }
        other => panic!("unexpected table ref: {:?}", other),
    }
}

#[test]
fn map_delete_using_absent_is_empty_vec() {
    let del = S::Delete {
        tables: vec![],
        from: S::FromTable::WithFromKeyword(vec![twj("t")]),
        using: None,
        selection: None,
        returning: None,
        order_by: vec![],
        limit: None,
    };

    let u = map_delete(&del);
    assert!(u.using.is_empty());
}
