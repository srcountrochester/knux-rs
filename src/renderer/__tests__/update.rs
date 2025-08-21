use crate::renderer::{self, ast as R, update::render_update};

fn upd_named(schema: Option<&str>, name: &str) -> R::TableRef {
    R::TableRef::Named {
        schema: schema.map(|s| s.to_string()),
        name: name.to_string(),
        alias: None,
    }
}

fn assign(col: &str, value_sql: &str) -> R::Assign {
    R::Assign {
        col: col.to_string(),
        value: R::Expr::Raw(value_sql.to_string()),
        from_inserted: false,
    }
}

#[test]
fn pg_update_with_where_and_returning_star() {
    let u = R::Update {
        table: upd_named(Some("s"), "users"),
        set: vec![assign("a", "1"), assign("b", "a")],
        r#where: Some(R::Expr::Raw("id = 10".into())),
        returning: vec![R::SelectItem::Star { opts: None }],
        from: vec![],
        sqlite_or: None,
    };

    let cfg = renderer::cfg_postgres_knex();

    let sql = render_update(&u, &cfg, 128);
    assert_eq!(
        sql,
        r#"UPDATE "s"."users" SET "a" = 1, "b" = a WHERE id = 10 RETURNING *"#
    );
}

#[test]
fn sqlite_update_with_qualified_returning() {
    let u = R::Update {
        table: upd_named(None, "t"),
        set: vec![assign("x", "x + 1"), assign("y", "'ok'")],
        r#where: None,
        returning: vec![R::SelectItem::QualifiedStar {
            table: "u".to_string(),
            opts: None,
        }],
        from: vec![],
        sqlite_or: None,
    };

    let cfg = renderer::cfg_sqlite_knex();

    let sql = render_update(&u, &cfg, 128);
    assert_eq!(
        sql,
        r#"UPDATE "t" SET "x" = x + 1, "y" = 'ok' RETURNING "u".*"#
    );
}

#[test]
fn mysql_update_ignores_returning() {
    let u = R::Update {
        table: upd_named(None, "accounts"),
        set: vec![assign("balance", "balance + 100")],
        r#where: Some(R::Expr::Raw("id = 1".into())),
        returning: vec![R::SelectItem::Star { opts: None }], // будет проигнорирован
        from: vec![],
        sqlite_or: None,
    };

    let cfg = renderer::cfg_mysql_knex();

    let sql = render_update(&u, &cfg, 128);
    assert_eq!(
        sql,
        r#"UPDATE `accounts` SET `balance` = balance + 100 WHERE id = 1"#
    );
}

#[test]
fn pg_update_with_from_multiple_tables() {
    let u = R::Update {
        table: upd_named(None, "t"),
        set: vec![assign("x", "1")],
        r#where: None,
        returning: vec![],
        from: vec![upd_named(None, "a"), upd_named(None, "b")],
        sqlite_or: None,
    };
    let cfg = renderer::cfg_postgres_knex();
    let sql = render_update(&u, &cfg, 128);
    assert_eq!(sql, r#"UPDATE "t" SET "x" = 1 FROM "a", "b""#);
}

#[test]
fn sqlite_update_or_ignore_with_from() {
    let u = R::Update {
        table: upd_named(None, "t"),
        set: vec![assign("x", "1")],
        r#where: None,
        returning: vec![],
        from: vec![upd_named(None, "a")],
        sqlite_or: Some(R::SqliteOr::Ignore),
    };
    let cfg = renderer::cfg_sqlite_knex();
    let sql = render_update(&u, &cfg, 128);
    assert_eq!(sql, r#"UPDATE OR IGNORE "t" SET "x" = 1 FROM "a""#);
}

#[test]
fn mysql_update_ignores_from_and_or() {
    let u = R::Update {
        table: upd_named(None, "t"),
        set: vec![assign("x", "1")],
        r#where: None,
        returning: vec![],
        from: vec![upd_named(None, "a")],
        sqlite_or: Some(R::SqliteOr::Replace),
    };
    let cfg = renderer::cfg_mysql_knex();
    let sql = render_update(&u, &cfg, 128);
    // ни FROM, ни OR
    assert_eq!(sql, r#"UPDATE `t` SET `x` = 1"#);
    assert!(!sql.contains(" FROM "));
    assert!(!sql.contains(" OR "));
}
