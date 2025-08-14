#![allow(clippy::needless_borrow)]
use crate::renderer::config::{Dialect, QuoteMode, SqlRenderCfg};
use crate::renderer::ident::{quote_ident, quote_ident_always, quote_path};

fn cfg(dialect: Dialect, mode: QuoteMode) -> SqlRenderCfg {
    SqlRenderCfg {
        dialect,
        quote: mode,
        // placeholder стиль тут не важен, но если поле есть в конфиге — заполни своё значение
        ..SqlRenderCfg::default()
    }
}

#[test]
fn always_quotes_postgres_and_sqlite() {
    // простые имена
    assert_eq!(quote_ident_always("users", Dialect::Postgres), r#""users""#);
    assert_eq!(quote_ident_always("users", Dialect::SQLite), r#""users""#);
    // экзотика + регистр
    assert_eq!(quote_ident_always("Users", Dialect::Postgres), r#""Users""#);
    assert_eq!(quote_ident_always(r#"a"b"#, Dialect::Postgres), r#""a""b""#);
    assert_eq!(quote_ident_always(r#"a"b"#, Dialect::SQLite), r#""a""b""#);
}

#[test]
fn always_quotes_mysql_with_backticks_and_escapes() {
    assert_eq!(quote_ident_always("users", Dialect::MySQL), "`users`");
    assert_eq!(quote_ident_always("Users", Dialect::MySQL), "`Users`");
    assert_eq!(quote_ident_always("a`b", Dialect::MySQL), "`a``b`");
}

#[test]
fn smart_mode_keeps_simple_safe_idents_unquoted() {
    let pg_smart = cfg(
        Dialect::Postgres,
        QuoteMode::Smart {
            preserve_case: false,
        },
    );
    let my_smart = cfg(
        Dialect::MySQL,
        QuoteMode::Smart {
            preserve_case: false,
        },
    );
    let sq_smart = cfg(
        Dialect::SQLite,
        QuoteMode::Smart {
            preserve_case: false,
        },
    );

    // простые идентификаторы — без кавычек
    assert_eq!(quote_ident("users", &pg_smart), "users");
    assert_eq!(quote_ident("users", &my_smart), "users");
    assert_eq!(quote_ident("users", &sq_smart), "users");

    // со спецсимволом — нужно квотить
    assert_eq!(quote_ident("user-id", &pg_smart), r#""user-id""#);
    assert_eq!(quote_ident("user-id", &my_smart), "`user-id`");
    assert_eq!(quote_ident("user-id", &sq_smart), r#""user-id""#);
}

#[test]
fn smart_mode_quotes_keywords_and_preserve_case_quotes() {
    // В Smart(false) ключевые слова квотируются
    let pg = cfg(
        Dialect::Postgres,
        QuoteMode::Smart {
            preserve_case: false,
        },
    );
    let my = cfg(
        Dialect::MySQL,
        QuoteMode::Smart {
            preserve_case: false,
        },
    );
    let sq = cfg(
        Dialect::SQLite,
        QuoteMode::Smart {
            preserve_case: false,
        },
    );

    assert_eq!(quote_ident("select", &pg), r#""select""#);
    assert_eq!(quote_ident("order", &my), "`order`");
    assert_eq!(quote_ident("from", &sq), r#""from""#);
    assert_eq!(quote_ident("FrOm", &sq), r#""FrOm""#);

    // preserve_case=true — квотируем даже «безопасные» идентификаторы
    let pg_preserve = cfg(
        Dialect::Postgres,
        QuoteMode::Smart {
            preserve_case: true,
        },
    );
    assert_eq!(quote_ident("Users", &pg_preserve), r#""Users""#);
    assert_eq!(quote_ident("users", &pg_preserve), r#""users""#);
}

#[test]
fn quote_path_joins_and_quotes_each_segment() {
    let pg = cfg(Dialect::Postgres, QuoteMode::Always);
    let my = cfg(Dialect::MySQL, QuoteMode::Always);
    let sq = cfg(Dialect::SQLite, QuoteMode::Always);

    // schema.table
    assert_eq!(quote_path(["public", "Users"], &pg), r#""public"."Users""#);
    assert_eq!(quote_path(["public", "Users"], &my), "`public`.`Users`");
    assert_eq!(quote_path(["public", "Users"], &sq), r#""public"."Users""#);

    // table.column
    assert_eq!(quote_path(["Users", "id"], &pg), r#""Users"."id""#);

    // ключевые слова внутри пути — тоже квотим
    assert_eq!(quote_path(["public", "order"], &pg), r#""public"."order""#);
    assert_eq!(quote_path(["public", "order"], &my), "`public`.`order`");
}

#[test]
fn escaping_inside_path_segments() {
    let pg = cfg(Dialect::Postgres, QuoteMode::Always);
    let my = cfg(Dialect::MySQL, QuoteMode::Always);

    assert_eq!(quote_path([r#"a"b"#, "tbl"], &pg), r#""a""b"."tbl""#);
    assert_eq!(quote_path(["a`b", "tbl"], &my), "`a``b`.`tbl`");
}
