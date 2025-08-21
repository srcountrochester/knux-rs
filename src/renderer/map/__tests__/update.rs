use crate::renderer::{
    ast::{SqliteOr, Stmt, TableRef},
    map::map_to_render_stmt as map_stmt,
};
use sqlparser::{ast as S, tokenizer::Span};

// Вспомогатели для ObjectName и TableWithJoins с схемой/алиасом
fn obj(parts: &[&str]) -> S::ObjectName {
    S::ObjectName(
        parts
            .iter()
            .map(|s| S::ObjectNamePart::Identifier(S::Ident::new(*s)))
            .collect(),
    )
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

fn twj(name: &str) -> S::TableWithJoins {
    S::TableWithJoins {
        relation: S::TableFactor::Table {
            name: S::ObjectName(vec![S::ObjectNamePart::Identifier(S::Ident::new(name))]),
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

#[test]
fn map_update_from_after_set_and_sqlite_or() {
    // UPDATE t SET x = 1 FROM a, b  (OR IGNORE — SQLite)
    let stmt = S::Statement::Update {
        table: twj("t"),
        assignments: vec![S::Assignment {
            target: S::AssignmentTarget::ColumnName(S::ObjectName(vec![
                S::ObjectNamePart::Identifier(S::Ident::new("x")),
            ])),
            value: S::Expr::Value(S::ValueWithSpan {
                value: S::Value::Number("1".into(), false),
                span: Span::empty(),
            }),
        }],
        from: Some(S::UpdateTableFromKind::AfterSet(vec![twj("a"), twj("b")])),
        selection: None,
        returning: None,
        or: Some(S::SqliteOnConflict::Ignore),
    };

    let r = map_stmt(&stmt);
    let Stmt::Update(u) = r else {
        panic!("expected Update")
    };

    assert_eq!(u.set.len(), 1);
    assert_eq!(u.from.len(), 2);
    match &u.table {
        TableRef::Named {
            schema,
            name,
            alias,
        } => {
            assert_eq!(schema.as_deref(), None);
            assert_eq!(name, "t");
            assert!(alias.is_none());
        }
        other => panic!("unexpected table ref: {:?}", other),
    }

    assert_eq!(u.from.len(), 2);

    match &u.from[0] {
        TableRef::Named {
            schema,
            name,
            alias,
        } => {
            assert_eq!(schema.as_deref(), None);
            assert_eq!(name, "a");
            assert!(alias.is_none());
        }
        other => panic!("unexpected from[0]: {:?}", other),
    }

    match &u.from[1] {
        TableRef::Named {
            schema,
            name,
            alias,
        } => {
            assert_eq!(schema.as_deref(), None);
            assert_eq!(name, "b");
            assert!(alias.is_none());
        }
        other => panic!("unexpected from[1]: {:?}", other),
    }
    assert!(matches!(u.sqlite_or, Some(SqliteOr::Ignore)));
}

#[test]
fn map_update_from_before_set_is_also_supported() {
    let stmt = S::Statement::Update {
        table: twj("t"),
        assignments: vec![S::Assignment {
            target: S::AssignmentTarget::ColumnName(S::ObjectName(vec![
                S::ObjectNamePart::Identifier(S::Ident::new("x")),
            ])),
            value: S::Expr::Value(S::ValueWithSpan {
                value: S::Value::Number("1".into(), false),
                span: Span::empty(),
            }),
        }],
        from: Some(S::UpdateTableFromKind::BeforeSet(vec![twj("a")])),
        selection: None,
        returning: None,
        or: None,
    };

    let r = map_stmt(&stmt);
    let Stmt::Update(u) = r else {
        panic!("expected Update")
    };
    assert_eq!(u.from.len(), 1);
    match &u.from[0] {
        TableRef::Named {
            schema,
            name,
            alias,
        } => {
            assert_eq!(schema.as_deref(), None);
            assert_eq!(name, "a");
            assert!(alias.is_none());
        }
        other => panic!("unexpected from[0]: {:?}", other),
    }
}

#[test]
fn map_update_maps_where_and_returning() {
    // UPDATE t SET a = 1 WHERE a > 0 RETURNING *, x AS k
    let stmt = S::Statement::Update {
        table: twj("t"),
        assignments: vec![S::Assignment {
            target: S::AssignmentTarget::ColumnName(obj(&["a"])),
            value: S::Expr::Value(S::ValueWithSpan {
                value: S::Value::Number("1".into(), false),
                span: Span::empty(),
            }),
        }],
        from: None,
        selection: Some(S::Expr::BinaryOp {
            left: Box::new(S::Expr::Identifier(S::Ident::new("a"))),
            op: S::BinaryOperator::Gt,
            right: Box::new(S::Expr::Value(S::ValueWithSpan {
                value: S::Value::Number("0".into(), false),
                span: Span::empty(),
            })),
        }),
        returning: Some(vec![
            S::SelectItem::Wildcard(S::WildcardAdditionalOptions::default()),
            S::SelectItem::ExprWithAlias {
                expr: S::Expr::Identifier(S::Ident::new("x")),
                alias: S::Ident::new("k"),
            },
        ]),
        or: None,
    };

    let r = map_stmt(&stmt);
    let Stmt::Update(u) = r else {
        panic!("expected Update")
    };

    // WHERE — бинарное сравнение >
    match u.r#where {
        Some(crate::renderer::ast::Expr::Binary { ref op, .. }) => {
            assert!(matches!(op, crate::renderer::ast::BinOp::Gt));
        }
        other => panic!("expected Binary(Gt), got {:?}", other),
    }

    // RETURNING: *, x AS k
    assert_eq!(u.returning.len(), 2);
    assert!(matches!(
        u.returning[0],
        crate::renderer::ast::SelectItem::Star { .. }
    ));
    match &u.returning[1] {
        crate::renderer::ast::SelectItem::Expr { alias, .. } => {
            assert_eq!(alias.as_deref(), Some("k"));
        }
        other => panic!("expected Expr with alias, got {:?}", other),
    }
}

#[test]
fn map_update_target_with_schema_and_alias() {
    // UPDATE s.users AS u SET a = 1
    let stmt = S::Statement::Update {
        table: twj_sa(Some("s"), "users", Some("u")),
        assignments: vec![S::Assignment {
            target: S::AssignmentTarget::ColumnName(obj(&["a"])),
            value: S::Expr::Value(S::ValueWithSpan {
                value: S::Value::Number("1".into(), false),
                span: Span::empty(),
            }),
        }],
        from: None,
        selection: None,
        returning: None,
        or: None,
    };

    let r = map_stmt(&stmt);
    let Stmt::Update(u) = r else {
        panic!("expected Update")
    };

    match &u.table {
        TableRef::Named {
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
fn map_update_set_target_tuple_uses_last_segment() {
    // UPDATE t SET (t.a, t.b) = (1, 2)  → возьмём последний сегмент "b" (как реализовано)
    let stmt = S::Statement::Update {
        table: twj("t"),
        assignments: vec![S::Assignment {
            target: S::AssignmentTarget::Tuple(vec![obj(&["t", "a"]), obj(&["t", "b"])]),
            value: S::Expr::Value(S::ValueWithSpan {
                value: S::Value::Number("1".into(), false),
                span: Span::empty(),
            }),
        }],
        from: None,
        selection: None,
        returning: None,
        or: None,
    };

    let r = map_stmt(&stmt);
    let Stmt::Update(u) = r else {
        panic!("expected Update")
    };

    assert_eq!(u.set.len(), 1);
    assert_eq!(u.set[0].col, "b"); // важно: берётся последний элемент последнего имени
}

#[test]
fn map_update_sqlite_or_replace() {
    // UPDATE OR REPLACE t SET a = 1
    let stmt = S::Statement::Update {
        table: twj("t"),
        assignments: vec![S::Assignment {
            target: S::AssignmentTarget::ColumnName(obj(&["a"])),
            value: S::Expr::Value(S::ValueWithSpan {
                value: S::Value::Number("1".into(), false),
                span: Span::empty(),
            }),
        }],
        from: None,
        selection: None,
        returning: None,
        or: Some(S::SqliteOnConflict::Replace),
    };

    let r = map_stmt(&stmt);
    let Stmt::Update(u) = r else {
        panic!("expected Update")
    };
    assert!(matches!(u.sqlite_or, Some(SqliteOr::Replace)));
}

#[test]
fn map_update_from_none_is_empty() {
    // UPDATE t SET a = 1  (без FROM)
    let stmt = S::Statement::Update {
        table: twj("t"),
        assignments: vec![S::Assignment {
            target: S::AssignmentTarget::ColumnName(obj(&["a"])),
            value: S::Expr::Value(S::ValueWithSpan {
                value: S::Value::Number("1".into(), false),
                span: Span::empty(),
            }),
        }],
        from: None,
        selection: None,
        returning: None,
        or: None,
    };

    let r = map_stmt(&stmt);
    let Stmt::Update(u) = r else {
        panic!("expected Update")
    };
    assert!(u.from.is_empty());
}

#[test]
#[should_panic(expected = "unsupported UPDATE with joins")]
fn map_update_panics_on_target_joins() {
    // UPDATE t JOIN a ON 1=1 SET x = 1  → не поддержано (ожидаем панику)
    let mut t = twj("t");
    t.joins.push(S::Join {
        relation: S::TableFactor::Table {
            name: obj(&["a"]),
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
        join_operator: S::JoinOperator::Inner(S::JoinConstraint::None),
        global: false,
    });

    let stmt = S::Statement::Update {
        table: t,
        assignments: vec![S::Assignment {
            target: S::AssignmentTarget::ColumnName(obj(&["x"])),
            value: S::Expr::Value(S::ValueWithSpan {
                value: S::Value::Number("1".into(), false),
                span: Span::empty(),
            }),
        }],
        from: None,
        selection: None,
        returning: None,
        or: None,
    };

    let _ = map_stmt(&stmt); // должен запаниковать
}
