use crate::renderer::validate::validate_query_features;
use crate::renderer::{
    ast as R,
    config::{Dialect, FeaturePolicy, MysqlLimitStyle, PlaceholderStyle, QuoteMode, SqlRenderCfg},
};

fn cfg_strict(dialect: Dialect) -> SqlRenderCfg {
    SqlRenderCfg {
        dialect,
        quote: QuoteMode::Always,
        placeholders: PlaceholderStyle::Numbered,
        emulate_nulls_ordering: false,
        mysql_limit_style: MysqlLimitStyle::LimitOffset,
        policy: FeaturePolicy::Strict,
        emit_as_for_table_alias: true,
        emit_as_for_column_alias: true,
        fold_idents: None,
    }
}

fn cfg_lenient(dialect: Dialect) -> SqlRenderCfg {
    SqlRenderCfg {
        policy: FeaturePolicy::Lenient,
        ..cfg_strict(dialect)
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
fn distinct_on_is_forbidden_outside_pg_in_strict() {
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

    assert!(validate_query_features(&q, &cfg_strict(Dialect::MySQL)).is_some());
    assert!(validate_query_features(&q, &cfg_strict(Dialect::SQLite)).is_some());
    // В PG — допустимо
    assert!(validate_query_features(&q, &cfg_strict(Dialect::Postgres)).is_none());
}

#[test]
fn ilike_is_forbidden_in_mysql_and_sqlite_in_strict() {
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

    assert!(validate_query_features(&q, &cfg_strict(Dialect::MySQL)).is_some());
    assert!(validate_query_features(&q, &cfg_strict(Dialect::SQLite)).is_some());
    // В PG — допустимо
    assert!(validate_query_features(&q, &cfg_strict(Dialect::Postgres)).is_none());
}

#[test]
fn nulls_last_is_forbidden_outside_pg_in_strict() {
    let sel = base_select(vec![R::SelectItem::Expr {
        expr: R::Expr::Ident {
            path: vec!["email".into()],
        },
        alias: None,
    }]);
    let mut q = wrap_query(sel);
    q.order_by = vec![R::OrderItem {
        expr: R::Expr::Ident {
            path: vec!["email".into()],
        },
        dir: R::OrderDirection::Asc,
        nulls_last: true,
    }];

    assert!(validate_query_features(&q, &cfg_strict(Dialect::MySQL)).is_some());
    assert!(validate_query_features(&q, &cfg_strict(Dialect::SQLite)).is_some());
    // В PG — допустимо
    assert!(validate_query_features(&q, &cfg_strict(Dialect::Postgres)).is_none());
}

#[test]
fn group_by_modifiers_rules() {
    // подготовим селект с group_by exprs
    let mut sel = base_select(vec![R::SelectItem::Expr {
        expr: R::Expr::Ident {
            path: vec!["city".into()],
        },
        alias: None,
    }]);
    sel.group_by = vec![R::Expr::Ident {
        path: vec!["city".into()],
    }];

    // вариант с ROLLUP
    let mut sel_rollup = sel.clone();
    sel_rollup.group_by_modifiers = vec![R::GroupByModifier::Rollup];
    let q_rollup = wrap_query(sel_rollup);

    // вариант с CUBE
    let mut sel_cube = sel.clone();
    sel_cube.group_by_modifiers = vec![R::GroupByModifier::Cube];
    let q_cube = wrap_query(sel_cube);

    // вариант с GROUPING SETS (внутрь кладём что-то простое)
    let mut sel_gs = sel.clone();
    sel_gs.group_by_modifiers = vec![R::GroupByModifier::GroupingSets(R::Expr::Tuple(vec![
        R::Expr::Ident {
            path: vec!["city".into()],
        },
        R::Expr::Ident {
            path: vec!["country".into()],
        },
    ]))];
    let q_gs = wrap_query(sel_gs);

    // SQLite: любые модификаторы запрещены
    for q in [&q_rollup, &q_cube, &q_gs] {
        assert!(validate_query_features(q, &cfg_strict(Dialect::SQLite)).is_some());
    }

    // MySQL: только ROLLUP допустим (как WITH ROLLUP), остальные запрещены
    assert!(validate_query_features(&q_rollup, &cfg_strict(Dialect::MySQL)).is_none());
    assert!(validate_query_features(&q_cube, &cfg_strict(Dialect::MySQL)).is_some());
    assert!(validate_query_features(&q_gs, &cfg_strict(Dialect::MySQL)).is_some());

    // PG: все три допустимы
    for q in [&q_rollup, &q_cube, &q_gs] {
        assert!(validate_query_features(q, &cfg_strict(Dialect::Postgres)).is_none());
    }
}

#[test]
fn by_name_in_set_ops_is_forbidden_in_strict_everywhere() {
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
            by_name: true, // ключевой флаг
        },
        order_by: vec![],
        limit: None,
        offset: None,
    };

    for d in [Dialect::Postgres, Dialect::MySQL, Dialect::SQLite] {
        assert!(
            validate_query_features(&q, &cfg_strict(d)).is_some(),
            "BY NAME must be rejected in strict policy for {:?}",
            d
        );
    }
}

#[test]
fn lenient_policy_allows_features() {
    // соберём запрос с несколькими «спорными» фичами одновременно:
    let mut sel = base_select(vec![R::SelectItem::Expr {
        expr: R::Expr::Ident {
            path: vec!["email".into()],
        },
        alias: None,
    }]);
    // DISTINCT ON
    sel.distinct_on = vec![R::Expr::Ident {
        path: vec!["email".into()],
    }];
    // WHERE ILIKE
    sel.r#where = Some(R::Expr::Like {
        not: false,
        ilike: true,
        expr: Box::new(R::Expr::Ident {
            path: vec!["name".into()],
        }),
        pattern: Box::new(R::Expr::String("%x%".into())),
        escape: None,
    });
    let mut q = wrap_query(sel);
    // ORDER BY ... NULLS LAST
    q.order_by = vec![R::OrderItem {
        expr: R::Expr::Ident {
            path: vec!["email".into()],
        },
        dir: R::OrderDirection::Asc,
        nulls_last: true,
    }];

    // В Lenient — везде разрешаем
    for d in [Dialect::Postgres, Dialect::MySQL, Dialect::SQLite] {
        assert!(validate_query_features(&q, &cfg_lenient(d)).is_none());
    }
}
