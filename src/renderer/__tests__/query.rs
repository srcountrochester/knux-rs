use crate::renderer::{
    ast as R,
    config::{Dialect, FeaturePolicy, MysqlLimitStyle, PlaceholderStyle, QuoteMode, SqlRenderCfg},
    render_sql_query,
};

fn cfg(d: Dialect) -> SqlRenderCfg {
    SqlRenderCfg {
        dialect: d,
        quote: QuoteMode::Always,
        placeholders: PlaceholderStyle::Numbered,
        emulate_nulls_ordering: false,
        mysql_limit_style: MysqlLimitStyle::LimitOffset,
        policy: FeaturePolicy::Lenient,
        emit_as_for_table_alias: true,
        emit_as_for_column_alias: true,
        fold_idents: None,
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
fn with_cte_simple_pg() {
    // WITH c AS (SELECT 1) SELECT * FROM c
    let cte_query = R::QueryBody::Select(base_select(vec![R::SelectItem::Expr {
        expr: R::Expr::Number("1".into()),
        alias: None,
    }]));
    let q = R::Query {
        with: Some(R::With {
            recursive: false,
            ctes: vec![R::Cte {
                name: "c".into(),
                columns: vec![],
                query: Box::new(cte_query),
            }],
        }),
        body: R::QueryBody::Select(R::Select {
            from: Some(R::TableRef::Named {
                schema: None,
                name: "c".into(),
                alias: None,
            }),
            items: vec![R::SelectItem::Star { opts: None }],
            ..base_select(vec![])
        }),
        order_by: vec![],
        limit: None,
        offset: None,
    };

    let out = render_sql_query(&q, &cfg(Dialect::Postgres));
    assert_eq!(out, r#"WITH "c" AS (SELECT 1) SELECT * FROM "c""#);
}

#[test]
fn with_recursive_union_all_pg() {
    // WITH RECURSIVE t(n) AS (
    //   SELECT 1 UNION ALL SELECT n+1
    // )
    // SELECT * FROM t
    let left = R::QueryBody::Select(base_select(vec![R::SelectItem::Expr {
        expr: R::Expr::Number("1".into()),
        alias: None,
    }]));
    let right = R::QueryBody::Select(base_select(vec![R::SelectItem::Expr {
        expr: R::Expr::Binary {
            left: Box::new(R::Expr::Ident {
                path: vec!["n".into()],
            }),
            op: R::BinOp::Add,
            right: Box::new(R::Expr::Number("1".into())),
        },
        alias: None,
    }]));
    let set = R::QueryBody::Set {
        left: Box::new(left),
        op: R::SetOp::UnionAll,
        right: Box::new(right),
        by_name: false,
    };
    let q = R::Query {
        with: Some(R::With {
            recursive: true,
            ctes: vec![R::Cte {
                name: "t".into(),
                columns: vec!["n".into()],
                query: Box::new(set),
            }],
        }),
        body: R::QueryBody::Select(R::Select {
            from: Some(R::TableRef::Named {
                schema: None,
                name: "t".into(),
                alias: None,
            }),
            items: vec![R::SelectItem::Star { opts: None }],
            ..base_select(vec![])
        }),
        order_by: vec![],
        limit: None,
        offset: None,
    };

    let out = render_sql_query(&q, &cfg(Dialect::Postgres));
    assert_eq!(
        out,
        r#"WITH RECURSIVE "t" ("n") AS ((SELECT 1) UNION ALL (SELECT "n" + 1)) SELECT * FROM "t""#
    );
}

#[test]
fn set_operations_parentheses_across_dialects() {
    // (SELECT 1) UNION (SELECT 2)
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
            by_name: false,
        },
        order_by: vec![],
        limit: None,
        offset: None,
    };

    for d in [Dialect::Postgres, Dialect::MySQL, Dialect::SQLite] {
        let out = render_sql_query(&q, &cfg(d));
        assert_eq!(out, "(SELECT 1) UNION (SELECT 2)");
    }
}

#[test]
fn mysql_limit_offset_comma_style() {
    // MySQL: LIMIT offset, count
    let sel = base_select(vec![R::SelectItem::Expr {
        expr: R::Expr::Number("1".into()),
        alias: None,
    }]);
    let mut q = wrap_query(sel);
    q.limit = Some(10);
    q.offset = Some(20);

    let mut c = cfg(Dialect::MySQL);
    c.mysql_limit_style = MysqlLimitStyle::OffsetCommaLimit;

    let out = render_sql_query(&q, &c);
    assert_eq!(out, "SELECT 1 LIMIT 20, 10");
}
