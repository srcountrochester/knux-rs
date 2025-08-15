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
fn join_using_and_natural() {
    // FROM Users u INNER JOIN Accounts a USING (user_id)
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
    sel.joins = vec![
        R::Join {
            kind: R::JoinKind::Inner,
            table: R::TableRef::Named {
                schema: None,
                name: "Accounts".into(),
                alias: Some("a".into()),
            },
            on: None,
            using_cols: Some(vec!["user_id".into()]),
        },
        R::Join {
            kind: R::JoinKind::NaturalInner,
            table: R::TableRef::Named {
                schema: None,
                name: "Logs".into(),
                alias: Some("l".into()),
            },
            on: None,
            using_cols: None,
        },
    ];
    let out = render_sql_query(&wrap_query(sel), &cfg(Dialect::Postgres));
    assert_eq!(
        out,
        r#"SELECT "u"."id" FROM "Users" AS "u" INNER JOIN "Accounts" AS "a" USING ("user_id") NATURAL INNER JOIN "Logs" AS "l""#
    );
}

#[test]
fn count_star_and_in_tuple() {
    // SELECT COUNT(*) FROM Users WHERE city IN ('NY','LA')
    let mut sel = base_select(vec![R::SelectItem::Expr {
        expr: R::Expr::FuncCall {
            name: "COUNT".into(),
            args: vec![R::Expr::Star],
        },
        alias: None,
    }]);
    sel.from = Some(R::TableRef::Named {
        schema: None,
        name: "Users".into(),
        alias: None,
    });
    sel.r#where = Some(R::Expr::Binary {
        left: Box::new(R::Expr::Ident {
            path: vec!["city".into()],
        }),
        op: R::BinOp::In,
        right: Box::new(R::Expr::Tuple(vec![
            R::Expr::String("NY".into()),
            R::Expr::String("LA".into()),
        ])),
    });

    for d in [Dialect::Postgres, Dialect::MySQL, Dialect::SQLite] {
        let out = render_sql_query(&wrap_query(sel.clone()), &cfg(d));
        match d {
            Dialect::Postgres | Dialect::SQLite => assert_eq!(
                out,
                r#"SELECT COUNT(*) FROM "Users" WHERE "city" IN ('NY', 'LA')"#
            ),
            Dialect::MySQL => assert_eq!(
                out,
                "SELECT COUNT(*) FROM `Users` WHERE `city` IN ('NY', 'LA')"
            ),
        }
    }
}

#[test]
fn like_with_escape() {
    // WHERE name LIKE '%\_%' ESCAPE '\'
    let mut sel = base_select(vec![R::SelectItem::Expr {
        expr: R::Expr::Ident {
            path: vec!["name".into()],
        },
        alias: None,
    }]);
    sel.r#where = Some(R::Expr::Like {
        not: false,
        ilike: false,
        expr: Box::new(R::Expr::Ident {
            path: vec!["name".into()],
        }),
        pattern: Box::new(R::Expr::String("%\\_%".into())),
        escape: Some('\\'),
    });

    let out_pg = render_sql_query(&wrap_query(sel.clone()), &cfg(Dialect::Postgres));
    assert_eq!(
        out_pg,
        r#"SELECT "name" WHERE "name" LIKE '%\_%' ESCAPE '\'"#
    );

    let out_my = render_sql_query(&wrap_query(sel.clone()), &cfg(Dialect::MySQL));
    assert_eq!(
        out_my,
        "SELECT `name` WHERE `name` LIKE '%\\_%' ESCAPE '\\'"
    );

    let out_sq = render_sql_query(&wrap_query(sel), &cfg(Dialect::SQLite));
    assert_eq!(
        out_sq,
        r#"SELECT "name" WHERE "name" LIKE '%\_%' ESCAPE '\'"#
    );
}

#[test]
fn emulate_nulls_last_when_enabled() {
    // ORDER BY email ASC NULLS LAST, id DESC — включаем эмуляцию вне PG
    let sel = base_select(vec![R::SelectItem::Expr {
        expr: R::Expr::Ident {
            path: vec!["u".into(), "email".into()],
        },
        alias: None,
    }]);
    let mut q = wrap_query(sel);
    q.order_by = vec![
        R::OrderItem {
            expr: R::Expr::Ident {
                path: vec!["u".into(), "email".into()],
            },
            dir: R::OrderDirection::Asc,
            nulls_last: true,
        },
        R::OrderItem {
            expr: R::Expr::Ident {
                path: vec!["u".into(), "id".into()],
            },
            dir: R::OrderDirection::Desc,
            nulls_last: false,
        },
    ];

    // MySQL с эмуляцией
    let mut c_my = cfg(Dialect::MySQL);
    c_my.emulate_nulls_ordering = true;
    let out_my = render_sql_query(&q, &c_my);
    assert_eq!(
        out_my,
        "SELECT `u`.`email` ORDER BY (`u`.`email` IS NULL) ASC, `u`.`email` ASC, `u`.`id` DESC"
    );

    // SQLite с эмуляцией
    let mut c_sq = cfg(Dialect::SQLite);
    c_sq.emulate_nulls_ordering = true;
    let out_sq = render_sql_query(&q, &c_sq);
    assert_eq!(
        out_sq,
        r#"SELECT "u"."email" ORDER BY ("u"."email" IS NULL) ASC, "u"."email" ASC, "u"."id" DESC"#
    );
}

#[test]
fn cast_and_collate_render() {
    // SELECT CAST(age AS INTEGER) AS a, name COLLATE "C"
    let sel = base_select(vec![
        R::SelectItem::Expr {
            expr: R::Expr::Cast {
                expr: Box::new(R::Expr::Ident {
                    path: vec!["age".into()],
                }),
                ty: "INTEGER".into(),
            },
            alias: Some("a".into()),
        },
        R::SelectItem::Expr {
            expr: R::Expr::Collate {
                expr: Box::new(R::Expr::Ident {
                    path: vec!["name".into()],
                }),
                collation: "C".into(),
            },
            alias: None,
        },
    ]);
    let out = render_sql_query(&wrap_query(sel), &cfg(Dialect::Postgres));
    assert_eq!(
        out,
        r#"SELECT CAST("age" AS INTEGER) AS "a", "name" COLLATE "C""#
    );
}

#[test]
fn window_function_partition_and_order() {
    // SELECT ROW_NUMBER() OVER (PARTITION BY u.city ORDER BY u.id)
    let sel = base_select(vec![R::SelectItem::Expr {
        expr: R::Expr::WindowFunc {
            name: "ROW_NUMBER".into(),
            args: vec![],
            window: R::WindowSpec {
                partition_by: vec![R::Expr::Ident {
                    path: vec!["u".into(), "city".into()],
                }],
                order_by: vec![R::OrderItem {
                    expr: R::Expr::Ident {
                        path: vec!["u".into(), "id".into()],
                    },
                    dir: R::OrderDirection::Asc,
                    nulls_last: false,
                }],
            },
        },
        alias: None,
    }]);
    let out = render_sql_query(&wrap_query(sel), &cfg(Dialect::Postgres));
    assert_eq!(
        out,
        r#"SELECT ROW_NUMBER() OVER (PARTITION BY "u"."city" ORDER BY "u"."id" ASC)"#
    );
}
