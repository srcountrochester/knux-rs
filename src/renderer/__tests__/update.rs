use crate::{
    UpdateBuilder, col,
    renderer::{self, Dialect, ast as R, map_to_render_stmt, update::render_update},
    table, val,
};

/// Утилита: `R::TableRef::Named` для таблицы UPDATE.
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

/// Утилита: идентификатор колонки (`"balance"`) как выражение.
fn ident_col(col: &str) -> R::Expr {
    R::Expr::Ident {
        path: vec![col.to_string()],
    }
}

/// Утилита: числовой литерал (например, `"100"`).
fn num(n: &str) -> R::Expr {
    R::Expr::Number(n.to_string())
}

/// Утилита: плейсхолдер параметра (`$1`/`?`).
fn bind() -> R::Expr {
    R::Expr::Bind
}

/// Вспомогалка: получаем `R::Update` из `UpdateBuilder` через sqlparser AST и map-проекцию.
fn render_from_builder(b: UpdateBuilder<'static, ()>, dialect: Dialect) -> String {
    // build_update_ast: sqlparser::ast::Statement + params
    let (stmt, _params) = b.build_update_ast().expect("build_update_ast");
    // map_to_render_stmt: sqlparser::ast::Statement -> renderer::ast::Stmt
    let rstmt = map_to_render_stmt(&stmt);
    let cfg = match dialect {
        Dialect::Postgres => renderer::cfg_postgres_knex(),
        Dialect::MySQL => renderer::cfg_mysql_knex(),
        Dialect::SQLite => renderer::cfg_sqlite_knex(),
    };
    match rstmt {
        R::Stmt::Update(u) => render_update(&u, &cfg, 128),
        other => panic!("ожидали Update, получили {:?}", other),
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

/// Тест рендера `.increment` с числовым литералом справа.
/// Должно получиться: `UPDATE "users" SET "balance" = "balance" + 100 WHERE id = 1`.
#[test]
fn render_increment_with_number_rhs() {
    let u = R::Update {
        table: upd_named(None, "users"),
        set: vec![R::Assign {
            col: "balance".into(),
            value: R::Expr::Binary {
                left: Box::new(ident_col("balance")),
                op: R::BinOp::Add,
                right: Box::new(num("100")),
            },
            from_inserted: false,
        }],
        r#where: Some(R::Expr::Raw("id = 1".into())),
        returning: vec![],
        from: vec![],
        sqlite_or: None,
    };

    let cfg = renderer::cfg_postgres_knex();
    let sql = render_update(&u, &cfg, 128);

    assert_eq!(
        sql,
        r#"UPDATE "users" SET "balance" = "balance" + 100 WHERE id = 1"#
    );
}

/// Тест рендера `.increment` с параметром справа.
/// Должно получиться: `UPDATE "users" SET "balance" = "balance" + $1`
/// (для Postgres; в MySQL/SQLite будет `?`).
#[test]
fn render_increment_with_bind_rhs() {
    let u = R::Update {
        table: upd_named(None, "users"),
        set: vec![R::Assign {
            col: "balance".into(),
            value: R::Expr::Binary {
                left: Box::new(ident_col("balance")),
                op: R::BinOp::Add,
                right: Box::new(bind()),
            },
            from_inserted: false,
        }],
        r#where: None,
        returning: vec![],
        from: vec![],
        sqlite_or: None,
    };

    let cfg = renderer::cfg_postgres_knex();
    let sql = render_update(&u, &cfg, 128);

    assert_eq!(sql, r#"UPDATE "users" SET "balance" = "balance" + $1"#);
}

/// Тест рендера `.decrement` с числовым литералом справа.
/// Должно получиться: `UPDATE "users" SET "balance" = "balance" - 5`.
#[test]
fn render_decrement_with_number_rhs() {
    let u = R::Update {
        table: upd_named(None, "users"),
        set: vec![R::Assign {
            col: "balance".into(),
            value: R::Expr::Binary {
                left: Box::new(ident_col("balance")),
                op: R::BinOp::Sub,
                right: Box::new(num("5")),
            },
            from_inserted: false,
        }],
        r#where: None,
        returning: vec![],
        from: vec![],
        sqlite_or: None,
    };

    let cfg = renderer::cfg_postgres_knex();
    let sql = render_update(&u, &cfg, 128);

    assert_eq!(sql, r#"UPDATE "users" SET "balance" = "balance" - 5"#);
}

/// Тест рендера `.decrement` с параметром справа.
/// Должно получиться: `UPDATE "users" SET "balance" = "balance" - $1`.
#[test]
fn render_decrement_with_bind_rhs() {
    let u = R::Update {
        table: upd_named(None, "users"),
        set: vec![R::Assign {
            col: "balance".into(),
            value: R::Expr::Binary {
                left: Box::new(ident_col("balance")),
                op: R::BinOp::Sub,
                right: Box::new(bind()),
            },
            from_inserted: false,
        }],
        r#where: None,
        returning: vec![],
        from: vec![],
        sqlite_or: None,
    };

    let cfg = renderer::cfg_postgres_knex();
    let sql = render_update(&u, &cfg, 128);

    assert_eq!(sql, r#"UPDATE "users" SET "balance" = "balance" - $1"#);
}

/// Тест рендера `.increment` со строковой колонкой, AST сгенерирован QueryBuilder.
/// Ожидаем: `UPDATE "users" SET "balance" = "balance" + $1 WHERE "id" = $2` (Postgres preset).
#[test]
fn render_increment_with_str_column_via_qb_ast() {
    let b = crate::QueryBuilder::new_empty()
        .dialect(Dialect::Postgres)
        .update(table("users"))
        .where_(col("id").eq(val(1)))
        .increment("balance", val(100));

    let sql = render_from_builder(b, Dialect::Postgres);
    assert_eq!(
        sql,
        r#"UPDATE "users" SET "balance" = "balance" + $1 WHERE "id" = $2"#
    );
}

/// Тест рендера `.decrement` с `Expression` в качестве колонки, AST генерирует QueryBuilder.
/// Ожидаем: `UPDATE "users" SET "balance" = "balance" - $1`.
#[test]
fn render_decrement_with_expr_column_via_qb_ast() {
    let b = crate::QueryBuilder::new_empty()
        .dialect(Dialect::Postgres)
        .update(table("users"))
        .decrement(col("balance"), val(5));

    let sql = render_from_builder(b, Dialect::Postgres);
    assert_eq!(sql, r#"UPDATE "users" SET "balance" = "balance" - $1"#);
}
