use crate::renderer::{
    config::{Dialect, QuoteMode, SqlRenderCfg},
    ident::{quote_ident, quote_ident_always, quote_path},
};

fn cfg(dialect: Dialect, mode: QuoteMode) -> SqlRenderCfg {
    SqlRenderCfg {
        dialect,
        quote: mode,
        ..SqlRenderCfg::default()
    }
}

#[test]
fn always_mode_quotes_everything_pg_mysql_sqlite() {
    // Postgres / SQLite — двойные кавычки, экранирование " → ""
    assert_eq!(quote_ident_always("users", Dialect::Postgres), r#""users""#);
    assert_eq!(quote_ident_always("Users", Dialect::Postgres), r#""Users""#);
    assert_eq!(quote_ident_always(r#"a"b"#, Dialect::Postgres), r#""a""b""#);
    assert_eq!(quote_ident_always(r#"a"b"#, Dialect::SQLite), r#""a""b""#);

    // MySQL — бэктики, экранирование ` → ``
    assert_eq!(quote_ident_always("users", Dialect::MySQL), "`users`");
    assert_eq!(quote_ident_always("Users", Dialect::MySQL), "`Users`");
    assert_eq!(quote_ident_always("a`b", Dialect::MySQL), "`a``b`");
}

#[test]
fn smart_mode_preserve_case_false_basic_rules_pg() {
    let cfg_pg = cfg(
        Dialect::Postgres,
        QuoteMode::Smart {
            preserve_case: false,
        },
    );

    // простые безопасные — без кавычек
    assert_eq!(quote_ident("users", &cfg_pg), "users");
    assert_eq!(quote_ident("Users", &cfg_pg), "Users"); // регистр сохраняется, кавычек нет

    // не-простые — с кавычками
    assert_eq!(quote_ident("user-id", &cfg_pg), r#""user-id""#);
    assert_eq!(quote_ident("user id", &cfg_pg), r#""user id""#);

    // ключевые слова — с кавычками (case-insensitive)
    assert_eq!(quote_ident("select", &cfg_pg), r#""select""#);
    assert_eq!(quote_ident("SeLeCt", &cfg_pg), r#""SeLeCt""#);

    // path: смешанное квотирование
    assert_eq!(quote_path(["public", "users"], &cfg_pg), r#"public.users"#);
    assert_eq!(
        quote_path(["public", "order"], &cfg_pg),
        r#"public."order""#
    );
}

#[test]
fn smart_mode_preserve_case_false_mysql_and_sqlite() {
    let cfg_my = cfg(
        Dialect::MySQL,
        QuoteMode::Smart {
            preserve_case: false,
        },
    );
    let cfg_sq = cfg(
        Dialect::SQLite,
        QuoteMode::Smart {
            preserve_case: false,
        },
    );

    // простые — без кавычек
    assert_eq!(quote_ident("users", &cfg_my), "users");
    assert_eq!(quote_ident("users", &cfg_sq), "users");

    // ключевое слово — с кавычками по диалекту
    assert_eq!(quote_ident("order", &cfg_my), "`order`");
    assert_eq!(quote_ident("from", &cfg_sq), r#""from""#);

    // экранирование в path
    assert_eq!(quote_path(["a`b", "tbl"], &cfg_my), "`a``b`.tbl");
    assert_eq!(quote_path([r#"a"b"#, "tbl"], &cfg_sq), r#""a""b".tbl"#);
}

#[test]
fn smart_mode_preserve_case_true_forces_quotes() {
    let cfg_pg = cfg(
        Dialect::Postgres,
        QuoteMode::Smart {
            preserve_case: true,
        },
    );
    let cfg_my = cfg(
        Dialect::MySQL,
        QuoteMode::Smart {
            preserve_case: true,
        },
    );
    let cfg_sq = cfg(
        Dialect::SQLite,
        QuoteMode::Smart {
            preserve_case: true,
        },
    );

    // даже простые безопасные — квотируются
    assert_eq!(quote_ident("users", &cfg_pg), r#""users""#);
    assert_eq!(quote_ident("users", &cfg_my), "`users`");
    assert_eq!(quote_ident("users", &cfg_sq), r#""users""#);

    // path — каждый сегмент в кавычках
    assert_eq!(
        quote_path(["public", "users"], &cfg_pg),
        r#""public"."users""#
    );
    assert_eq!(quote_path(["public", "users"], &cfg_my), "`public`.`users`");
    assert_eq!(
        quote_path(["public", "users"], &cfg_sq),
        r#""public"."users""#
    );
}

#[test]
fn smart_mode_with_keyword_and_mixed_path() {
    let cfg_pg = cfg(
        Dialect::Postgres,
        QuoteMode::Smart {
            preserve_case: false,
        },
    );
    let cfg_my = cfg(
        Dialect::MySQL,
        QuoteMode::Smart {
            preserve_case: false,
        },
    );

    // keyword в одиночку
    assert_eq!(quote_ident("group", &cfg_pg), r#""group""#);
    assert_eq!(quote_ident("group", &cfg_my), "`group`");

    // смешанный путь: schema безопасен → без кавычек, таблица keyword → квотится
    assert_eq!(quote_path(["app", "order"], &cfg_pg), r#"app."order""#);
    assert_eq!(quote_path(["app", "order"], &cfg_my), "app.`order`");
}
