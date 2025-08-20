use crate::renderer::{
    ast as R, cfg_mysql_knex, cfg_postgres_knex, cfg_sqlite_knex, render_sql_select,
};

fn ident(path: &[&str]) -> R::Expr {
    R::Expr::Ident {
        path: path.iter().map(|s| s.to_string()).collect(),
    }
}
fn bind() -> R::Expr {
    R::Expr::Bind
}
fn num(s: &str) -> R::Expr {
    R::Expr::Number(s.to_string())
}
fn str_(s: &str) -> R::Expr {
    R::Expr::String(s.to_string())
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
fn with_from(
    mut s: R::Select,
    schema: Option<&str>,
    table: &str,
    alias: Option<&str>,
) -> R::Select {
    s.from = Some(R::TableRef::Named {
        schema: schema.map(|x| x.to_string()),
        name: table.to_string(),
        alias: alias.map(|x| x.to_string()),
    });
    s
}

fn sql_pg(sel: &R::Select) -> String {
    render_sql_select(sel, &cfg_postgres_knex())
}
fn sql_my(sel: &R::Select) -> String {
    render_sql_select(sel, &cfg_mysql_knex())
}
fn sql_sq(sel: &R::Select) -> String {
    render_sql_select(sel, &cfg_sqlite_knex())
}

#[test]
fn basic_select_items_and_from_where() {
    let mut sel = with_from(
        base_select(vec![
            R::SelectItem::Star { opts: None },
            R::SelectItem::Expr {
                expr: ident(&["u", "id"]),
                alias: None,
            },
        ]),
        Some("public"),
        "Users",
        Some("u"),
    );
    sel.r#where = Some(R::Expr::Binary {
        left: Box::new(ident(&["u", "is_active"])),
        op: R::BinOp::Eq,
        right: Box::new(bind()),
    });

    assert_eq!(
        sql_pg(&sel),
        r#"SELECT *, "u"."id" FROM "public"."Users" AS "u" WHERE "u"."is_active" = $1"#
    );
    assert_eq!(
        sql_my(&sel),
        r#"SELECT *, `u`.`id` FROM `public`.`Users` AS `u` WHERE `u`.`is_active` = ?"#
    );
    assert_eq!(
        sql_sq(&sel),
        r#"SELECT *, "u"."id" FROM "public"."Users" AS "u" WHERE "u"."is_active" = ?"#
    );
}

#[test]
fn distinct_and_distinct_on_rendering() {
    // DISTINCT
    let sel = with_from(
        base_select(vec![R::SelectItem::Expr {
            expr: ident(&["u", "email"]),
            alias: None,
        }]),
        None,
        "Users",
        Some("u"),
    );
    let mut sel_distinct = sel.clone();
    sel_distinct.distinct = true;

    assert_eq!(
        sql_pg(&sel_distinct),
        r#"SELECT DISTINCT "u"."email" FROM "Users" AS "u""#
    );
    assert_eq!(
        sql_my(&sel_distinct),
        r#"SELECT DISTINCT `u`.`email` FROM `Users` AS `u`"#
    );

    // DISTINCT ON(...) → PG печатает ON, остальные — мягкая деградация до DISTINCT
    let mut sel_on = sel.clone();
    sel_on.distinct_on = vec![ident(&["u", "email"]), ident(&["u", "id"])];
    assert_eq!(
        sql_pg(&sel_on),
        r#"SELECT DISTINCT ON ("u"."email", "u"."id") "u"."email" FROM "Users" AS "u""#
    );
    assert_eq!(
        sql_my(&sel_on),
        r#"SELECT DISTINCT `u`.`email` FROM `Users` AS `u`"#
    );
    assert_eq!(
        sql_sq(&sel_on),
        r#"SELECT DISTINCT "u"."email" FROM "Users" AS "u""#
    );
}

#[test]
fn group_by_and_rollup_across_dialects() {
    let mut sel = with_from(
        base_select(vec![R::SelectItem::Expr {
            expr: ident(&["u", "city"]),
            alias: None,
        }]),
        None,
        "Users",
        Some("u"),
    );
    sel.group_by = vec![ident(&["u", "city"]), ident(&["u", "country"])];
    sel.group_by_modifiers = vec![R::GroupByModifier::Rollup];

    assert_eq!(
        sql_pg(&sel),
        r#"SELECT "u"."city" FROM "Users" AS "u" GROUP BY ROLLUP ("u"."city", "u"."country")"#
    );
    assert_eq!(
        sql_my(&sel),
        r#"SELECT `u`.`city` FROM `Users` AS `u` GROUP BY `u`.`city`, `u`.`country` WITH ROLLUP"#
    );
    assert_eq!(
        sql_sq(&sel),
        r#"SELECT "u"."city" FROM "Users" AS "u" GROUP BY "u"."city", "u"."country""#
    );
}

#[test]
fn order_by_with_nulls_last_only_in_pg() {
    let mut sel = with_from(
        base_select(vec![R::SelectItem::Expr {
            expr: ident(&["u", "email"]),
            alias: None,
        }]),
        None,
        "Users",
        Some("u"),
    );
    sel.order_by = vec![
        R::OrderItem {
            expr: ident(&["u", "email"]),
            dir: R::OrderDirection::Asc,
            nulls_last: true,
        },
        R::OrderItem {
            expr: ident(&["u", "id"]),
            dir: R::OrderDirection::Desc,
            nulls_last: false,
        },
    ];

    assert_eq!(
        sql_pg(&sel),
        r#"SELECT "u"."email" FROM "Users" AS "u" ORDER BY "u"."email" ASC NULLS LAST, "u"."id" DESC"#
    );
    assert_eq!(
        sql_my(&sel),
        r#"SELECT `u`.`email` FROM `Users` AS `u` ORDER BY `u`.`email` ASC, `u`.`id` DESC"#
    );
    assert_eq!(
        sql_sq(&sel),
        r#"SELECT "u"."email" FROM "Users" AS "u" ORDER BY "u"."email" ASC, "u"."id" DESC"#
    );
}

#[test]
fn limit_and_offset_rendering() {
    let mut sel = with_from(
        base_select(vec![R::SelectItem::Expr {
            expr: ident(&["u", "id"]),
            alias: None,
        }]),
        None,
        "Users",
        Some("u"),
    );
    sel.limit = Some(10);
    sel.offset = Some(20);

    assert_eq!(
        sql_pg(&sel),
        r#"SELECT "u"."id" FROM "Users" AS "u" LIMIT 10 OFFSET 20"#
    );
    assert_eq!(
        sql_my(&sel),
        r#"SELECT `u`.`id` FROM `Users` AS `u` LIMIT 10 OFFSET 20"#
    );
    assert_eq!(
        sql_sq(&sel),
        r#"SELECT "u"."id" FROM "Users" AS "u" LIMIT 10 OFFSET 20"#
    );
}

#[test]
fn join_and_subquery_in_from_render() {
    // (SELECT *) AS "u" INNER JOIN "Accounts" AS "a" ON ...
    let sub = base_select(vec![R::SelectItem::Star { opts: None }]);

    let sel = R::Select {
        distinct: false,
        distinct_on: vec![],
        items: vec![R::SelectItem::Expr {
            expr: ident(&["u", "id"]),
            alias: None,
        }],
        from: Some(R::TableRef::Subquery {
            query: Box::new(sub),
            alias: Some("u".into()),
        }),
        joins: vec![R::Join {
            kind: R::JoinKind::Inner,
            table: R::TableRef::Named {
                schema: None,
                name: "Accounts".into(),
                alias: Some("a".into()),
            },
            on: Some(R::Expr::Binary {
                left: Box::new(ident(&["u", "id"])),
                op: R::BinOp::Eq,
                right: Box::new(ident(&["a", "user_id"])),
            }),
            using_cols: None,
        }],
        r#where: None,
        group_by: vec![],
        group_by_modifiers: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
    };

    assert_eq!(
        sql_pg(&sel),
        r#"SELECT "u"."id" FROM (SELECT *) AS "u" INNER JOIN "Accounts" AS "a" ON "u"."id" = "a"."user_id""#
    );
    assert_eq!(
        sql_my(&sel),
        r#"SELECT `u`.`id` FROM (SELECT *) AS `u` INNER JOIN `Accounts` AS `a` ON `u`.`id` = `a`.`user_id`"#
    );
    assert_eq!(
        sql_sq(&sel),
        r#"SELECT "u"."id" FROM (SELECT *) AS "u" INNER JOIN "Accounts" AS "a" ON "u"."id" = "a"."user_id""#
    );
}

#[test]
fn ilike_downgrade_on_non_pg() {
    let mut sel = with_from(
        base_select(vec![R::SelectItem::Expr {
            expr: ident(&["u", "name"]),
            alias: None,
        }]),
        None,
        "Users",
        Some("u"),
    );
    sel.r#where = Some(R::Expr::Binary {
        left: Box::new(ident(&["u", "name"])),
        op: R::BinOp::Ilike,
        right: Box::new(str_("%ann%")),
    });

    assert_eq!(
        sql_pg(&sel),
        r#"SELECT "u"."name" FROM "Users" AS "u" WHERE "u"."name" ILIKE '%ann%'"#
    );
    assert_eq!(
        sql_my(&sel),
        r#"SELECT `u`.`name` FROM `Users` AS `u` WHERE `u`.`name` LIKE '%ann%'"#
    );
    assert_eq!(
        sql_sq(&sel),
        r#"SELECT "u"."name" FROM "Users" AS "u" WHERE "u"."name" LIKE '%ann%'"#
    );
}

#[test]
fn case_when_else_render() {
    let sel = with_from(
        base_select(vec![R::SelectItem::Expr {
            expr: R::Expr::Case {
                operand: None,
                when_then: vec![(
                    R::Expr::Binary {
                        left: Box::new(ident(&["u", "age"])),
                        op: R::BinOp::Gt,
                        right: Box::new(num("18")),
                    },
                    str_("adult"),
                )],
                else_expr: Some(Box::new(str_("minor"))),
            },
            alias: Some("tag".into()),
        }]),
        None,
        "Users",
        Some("u"),
    );

    assert_eq!(
        sql_pg(&sel),
        r#"SELECT CASE WHEN "u"."age" > 18 THEN 'adult' ELSE 'minor' END AS "tag" FROM "Users" AS "u""#
    );
    assert_eq!(
        sql_my(&sel),
        r#"SELECT CASE WHEN `u`.`age` > 18 THEN 'adult' ELSE 'minor' END AS `tag` FROM `Users` AS `u`"#
    );
    assert_eq!(
        sql_sq(&sel),
        r#"SELECT CASE WHEN "u"."age" > 18 THEN 'adult' ELSE 'minor' END AS "tag" FROM "Users" AS "u""#
    );
}
