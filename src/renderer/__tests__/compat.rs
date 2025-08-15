use crate::renderer::{
    ast as R,
    config::{Dialect, FeaturePolicy, MysqlLimitStyle, PlaceholderStyle, QuoteMode, SqlRenderCfg},
    render_sql_query, try_render_sql_query,
};

fn cfg(dialect: Dialect) -> SqlRenderCfg {
    SqlRenderCfg {
        dialect,
        quote: QuoteMode::Always, // чтобы ожидания были стабильными
        placeholders: PlaceholderStyle::Numbered,
        emulate_nulls_ordering: false,
        mysql_limit_style: MysqlLimitStyle::LimitOffset,
        policy: FeaturePolicy::Lenient,
        emit_as_for_table_alias: true,
        emit_as_for_column_alias: true,
        fold_idents: None,
    }
}

fn cfg_strict(dialect: Dialect) -> SqlRenderCfg {
    SqlRenderCfg {
        policy: FeaturePolicy::Strict,
        ..cfg(dialect)
    }
}

fn base_select(items: Vec<R::SelectItem>) -> R::Select {
    R::Select {
        distinct: false,
        distinct_on: vec![],
        items,
        from: None,
        joins: vec![],
        r#where: None,
        group_by: vec![],
        group_by_modifiers: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
    }
}

fn wrap_query(sel: R::Select) -> R::Query {
    R::Query {
        with: None,
        body: R::QueryBody::Select(sel),
        order_by: vec![],
        limit: None,
        offset: None,
    }
}

#[test]
fn raw_expr_is_rendered_as_is() {
    // SELECT json_build_object('a', 1) AS j
    let sel = base_select(vec![R::SelectItem::Expr {
        expr: R::Expr::Raw("json_build_object('a', 1)".to_string()),
        alias: Some("j".into()),
    }]);
    let q = wrap_query(sel);
    let out = render_sql_query(&q, &cfg(Dialect::Postgres));
    assert_eq!(out, r#"SELECT json_build_object('a', 1) AS "j""#);
}

#[test]
fn strict_policy_blocks_ilike_on_mysql_and_sqlite() {
    // WHERE name ILIKE '%x%'
    let mut sel = base_select(vec![R::SelectItem::Expr {
        expr: R::Expr::Ident {
            path: vec!["name".into()],
        },
        alias: None,
    }]);
    sel.r#where = Some(R::Expr::Like {
        not: false,
        ilike: true,
        expr: Box::new(R::Expr::Ident {
            path: vec!["name".into()],
        }),
        pattern: Box::new(R::Expr::String("%x%".into())),
        escape: None,
    });
    let q = wrap_query(sel);

    assert!(try_render_sql_query(&q, &cfg_strict(Dialect::MySQL)).is_err());
    assert!(try_render_sql_query(&q, &cfg_strict(Dialect::SQLite)).is_err());

    // В PG это валидно
    assert!(try_render_sql_query(&q, &cfg_strict(Dialect::Postgres)).is_ok());
}

#[test]
fn strict_policy_blocks_distinct_on_outside_postgres() {
    let mut sel = base_select(vec![R::SelectItem::Expr {
        expr: R::Expr::Ident {
            path: vec!["email".into()],
        },
        alias: None,
    }]);
    sel.distinct_on = vec![R::Expr::Ident {
        path: vec!["email".into()],
    }];
    let q = wrap_query(sel);

    assert!(try_render_sql_query(&q, &cfg_strict(Dialect::MySQL)).is_err());
    assert!(try_render_sql_query(&q, &cfg_strict(Dialect::SQLite)).is_err());

    // В PG — ок
    assert!(try_render_sql_query(&q, &cfg_strict(Dialect::Postgres)).is_ok());
}

#[test]
fn strict_policy_blocks_by_name_in_set_ops() {
    // (SELECT 1) UNION BY NAME (SELECT 2)
    let left = R::QueryBody::Select(base_select(vec![R::SelectItem::Expr {
        expr: R::Expr::Number("1".into()),
        alias: None,
    }]));
    let right = R::QueryBody::Select(base_select(vec![R::SelectItem::Expr {
        expr: R::Expr::Number("2".into()),
        alias: None,
    }]));
    let q = R::Query {
        with: None,
        body: R::QueryBody::Set {
            left: Box::new(left),
            op: R::SetOp::Union,
            right: Box::new(right),
            by_name: true, // критично: BY NAME
        },
        order_by: vec![],
        limit: None,
        offset: None,
    };

    // Во всех трёх диалектах BY NAME не поддержан в строгом режиме
    for d in [Dialect::Postgres, Dialect::MySQL, Dialect::SQLite] {
        assert!(
            try_render_sql_query(&q, &cfg_strict(d)).is_err(),
            "BY NAME must be rejected in strict policy for {:?}",
            d
        );
    }

    // В lenient режиме рендерим как обычный UNION (без BY NAME)
    let out = render_sql_query(&q, &cfg(Dialect::Postgres));
    assert_eq!(out, "(SELECT 1) UNION (SELECT 2)");
}

#[test]
fn table_alias_without_as_when_flag_disabled() {
    // cfg: без AS для алиасов таблиц
    let mut c = cfg(Dialect::Postgres);
    c.emit_as_for_table_alias = false;

    let mut sel = base_select(vec![R::SelectItem::Expr {
        expr: R::Expr::Ident {
            path: vec!["u".into(), "id".into()],
        },
        alias: None,
    }]);
    sel.from = Some(R::TableRef::Named {
        schema: None,
        name: "Users".into(),
        alias: Some("u".into()),
    });

    let q = wrap_query(sel);
    let out = render_sql_query(&q, &c);
    assert_eq!(out, r#"SELECT "u"."id" FROM "Users" "u""#); // без "AS"
}

#[test]
fn column_alias_without_as_when_flag_disabled() {
    // cfg: без AS для алиасов колонок
    let mut c = cfg(Dialect::Postgres);
    c.emit_as_for_column_alias = false;

    let sel = base_select(vec![R::SelectItem::Expr {
        expr: R::Expr::Ident {
            path: vec!["id".into()],
        },
        alias: Some("x".into()),
    }]);
    let q = wrap_query(sel);
    let out = render_sql_query(&q, &c);
    assert_eq!(out, r#"SELECT "id" "x""#); // без "AS"
}
