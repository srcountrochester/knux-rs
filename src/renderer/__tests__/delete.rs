use super::super::delete::*;
use crate::renderer::{SqlRenderCfg, ast as R, cfg_mysql_knex, cfg_postgres_knex, cfg_sqlite_knex};

fn bin_eq_ident_num(name: &str, n: &str) -> R::Expr {
    R::Expr::Binary {
        left: Box::new(R::Expr::Ident {
            path: vec![name.to_string()],
        }),
        op: R::BinOp::Eq,
        right: Box::new(R::Expr::Number(n.to_string())),
    }
}

#[test]
fn pg_delete_with_using_where_returning_star() {
    // DELETE FROM "s"."users" USING "a", "b" WHERE id = 10 RETURNING *
    let d = R::Delete {
        table: R::TableRef::Named {
            schema: Some("s".into()),
            name: "users".into(),
            alias: None,
        },
        using: vec![
            R::TableRef::Named {
                schema: None,
                name: "a".into(),
                alias: None,
            },
            R::TableRef::Named {
                schema: None,
                name: "b".into(),
                alias: None,
            },
        ],
        r#where: Some(bin_eq_ident_num("id", "10")),
        returning: vec![R::SelectItem::Star { opts: None }],
    };

    let cfg: SqlRenderCfg = cfg_postgres_knex();
    let sql = render_delete(&d, &cfg, 128);

    assert_eq!(
        sql, r#"DELETE FROM "s"."users" USING "a", "b" WHERE "id" = 10 RETURNING *"#,
        "got: {sql}"
    );
}

#[test]
fn sqlite_delete_with_using_and_qualified_returning() {
    // DELETE FROM "t" USING "a" WHERE x = 'ok' RETURNING "u".*
    let d = R::Delete {
        table: R::TableRef::Named {
            schema: None,
            name: "t".into(),
            alias: None,
        },
        using: vec![R::TableRef::Named {
            schema: None,
            name: "a".into(),
            alias: None,
        }],
        r#where: Some(R::Expr::Binary {
            left: Box::new(R::Expr::Ident {
                path: vec!["x".into()],
            }),
            op: R::BinOp::Eq,
            right: Box::new(R::Expr::String("ok".into())),
        }),
        returning: vec![R::SelectItem::QualifiedStar {
            table: "u".into(),
            opts: None,
        }],
    };

    let cfg = cfg_sqlite_knex();
    let sql = render_delete(&d, &cfg, 128);

    assert_eq!(
        sql, r#"DELETE FROM "t" USING "a" WHERE "x" = 'ok' RETURNING "u".*"#,
        "got: {sql}"
    );
}

#[test]
fn mysql_delete_ignores_returning_but_renders_using() {
    // DELETE FROM `t` USING `a`, `b` WHERE id = 1
    // (RETURNING должен быть проигнорирован)
    let d = R::Delete {
        table: R::TableRef::Named {
            schema: None,
            name: "t".into(),
            alias: None,
        },
        using: vec![
            R::TableRef::Named {
                schema: None,
                name: "a".into(),
                alias: None,
            },
            R::TableRef::Named {
                schema: None,
                name: "b".into(),
                alias: None,
            },
        ],
        r#where: Some(bin_eq_ident_num("id", "1")),
        returning: vec![
            R::SelectItem::Star { opts: None }, // должен быть проигнорирован
        ],
    };

    let cfg = cfg_mysql_knex();
    let sql = render_delete(&d, &cfg, 128);

    assert_eq!(
        sql, "DELETE FROM `t` USING `a`, `b` WHERE `id` = 1",
        "got: {sql}"
    );
}
