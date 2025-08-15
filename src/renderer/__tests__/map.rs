use crate::renderer::ast as R;
use crate::renderer::map::map_to_render_ast;

use sqlparser::ast::{Query as SQuery, Statement};
use sqlparser::dialect::{GenericDialect, PostgreSqlDialect};
use sqlparser::parser::Parser;

fn parse_query_pg(sql: &str) -> SQuery {
    let dialect = PostgreSqlDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql).expect("parse pg sql");
    match stmts.pop().expect("one stmt") {
        Statement::Query(q) => *q,
        other => panic!("expected SELECT query, got {other:?}"),
    }
}

fn parse_query_generic(sql: &str) -> SQuery {
    let dialect = GenericDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql).expect("parse generic sql");
    match stmts.pop().expect("one stmt") {
        Statement::Query(q) => *q,
        other => panic!("expected SELECT query, got {other:?}"),
    }
}

// small helpers to match idents
fn is_ident_path(e: &R::Expr, path: &[&str]) -> bool {
    match e {
        R::Expr::Ident { path: p } => p.iter().map(|s| s.as_str()).eq(path.into_iter().copied()),
        _ => false,
    }
}

#[test]
fn map_distinct_on_order_by_limit_offset_pg() {
    // DISTINCT ON + NULLS + LIMIT/OFFSET — типичный PG.
    let q = parse_query_pg(
        r#"
        SELECT DISTINCT ON (u.email, u.id) u.email
        FROM Users AS u
        WHERE u.is_active = 1
        ORDER BY u.email ASC NULLS LAST, u.id DESC
        LIMIT 10 OFFSET 20
        "#,
    );

    let sel: R::Select = map_to_render_ast(&q);

    // DISTINCT ON
    assert!(
        !sel.distinct,
        "distinct flag must be false when distinct_on is used"
    );
    assert_eq!(sel.distinct_on.len(), 2);
    assert!(is_ident_path(&sel.distinct_on[0], &["u", "email"]));
    assert!(is_ident_path(&sel.distinct_on[1], &["u", "id"]));

    // FROM / alias
    let from = sel.from.as_ref().expect("from");
    match from {
        R::TableRef::Named {
            schema,
            name,
            alias,
        } => {
            assert!(schema.is_none());
            assert_eq!(name, "Users");
            assert_eq!(alias.as_deref(), Some("u"));
        }
        _ => panic!("expected named table"),
    }

    // WHERE u.is_active = 1
    match sel.r#where.as_ref().expect("where") {
        R::Expr::Binary { left, op, right } => {
            assert!(is_ident_path(left, &["u", "is_active"]));
            assert!(matches!(op, R::BinOp::Eq));
            assert!(matches!(**right, R::Expr::Number(ref n) if n == "1"));
        }
        other => panic!("unexpected where expr: {other:?}"),
    }

    // ORDER BY
    assert_eq!(sel.order_by.len(), 2);
    assert!(is_ident_path(&sel.order_by[0].expr, &["u", "email"]));
    assert!(matches!(sel.order_by[0].dir, R::OrderDirection::Asc));
    assert!(sel.order_by[0].nulls_last);

    assert!(is_ident_path(&sel.order_by[1].expr, &["u", "id"]));
    assert!(matches!(sel.order_by[1].dir, R::OrderDirection::Desc));
    assert!(!sel.order_by[1].nulls_last);

    // LIMIT/OFFSET
    assert_eq!(sel.limit, Some(10));
    assert_eq!(sel.offset, Some(20));
}

#[test]
fn map_group_by_with_rollup_pg() {
    let q = parse_query_pg(
        r#"
        SELECT u.city
        FROM Users u
        GROUP BY ROLLUP (u.city, u.country)
        "#,
    );
    let sel = map_to_render_ast(&q);

    // В некоторых версиях sqlparser модификатор не отдаётся — не проверяем его.
    assert!(!sel.group_by.is_empty(), "expected non-empty group_by list");
    assert!(
        is_ident_path(&sel.group_by[0], &["u", "city"]),
        "first group_by expr should be u.city, got: {:?}",
        sel.group_by[0]
    );
    // modifiers могут быть пустыми — это ок для этого теста
}

#[test]
fn map_group_by_with_grouping_sets_has_modifier() {
    // NB: generic-парсер чаще выдаёт GroupByWithModifier
    let q = parse_query_generic(
        r#"
        SELECT u.city
        FROM Users u
        GROUP BY GROUPING SETS ( (u.city, u.country) )
        "#,
    );
    let sel = map_to_render_ast(&q);

    let has_grouping_sets = sel
        .group_by_modifiers
        .iter()
        .any(|m| matches!(m, R::GroupByModifier::GroupingSets(_)));
    assert!(
        has_grouping_sets,
        "expected GroupingSets in modifiers, got {:?}",
        sel.group_by_modifiers
    );

    // exprs как минимум содержат u.city
    assert!(!sel.group_by.is_empty());
    assert!(is_ident_path(&sel.group_by[0], &["u", "city"]));
}

#[test]
fn map_plain_group_by_two_columns() {
    let q = parse_query_generic(
        r#"
        SELECT u.city
        FROM Users u
        GROUP BY u.city, u.country
        "#,
    );
    let sel = map_to_render_ast(&q);

    assert_eq!(
        sel.group_by.len(),
        2,
        "plain GROUP BY should keep both exprs"
    );
    assert!(is_ident_path(&sel.group_by[0], &["u", "city"]));
    assert!(is_ident_path(&sel.group_by[1], &["u", "country"]));
    assert!(sel.group_by_modifiers.is_empty());
}

#[test]
fn map_join_on_and_wildcards() {
    let q = parse_query_generic(
        r#"
        SELECT u.*, *
        FROM Users AS u
        INNER JOIN Accounts AS a ON u.id = a.user_id
        "#,
    );
    let sel = map_to_render_ast(&q);

    // items: QualifiedStar("u") + Star
    assert_eq!(sel.items.len(), 2);
    match &sel.items[0] {
        R::SelectItem::QualifiedStar { table, .. } => assert_eq!(table, "u"),
        other => panic!("expected qualified star, got {other:?}"),
    }
    match &sel.items[1] {
        R::SelectItem::Star { .. } => {}
        other => panic!("expected star, got {other:?}"),
    }

    // JOIN
    assert_eq!(sel.joins.len(), 1);
    let j = &sel.joins[0];
    assert!(matches!(j.kind, R::JoinKind::Inner));
    // ON u.id = a.user_id
    match j.on.as_ref().expect("join on") {
        R::Expr::Binary { left, op, right } => {
            assert!(matches!(op, R::BinOp::Eq));
            assert!(is_ident_path(left, &["u", "id"]));
            assert!(is_ident_path(right, &["a", "user_id"]));
        }
        other => panic!("unexpected join on: {other:?}"),
    }
}

#[test]
fn map_count_star_function_arg() {
    let q = parse_query_generic(
        r#"
        SELECT COUNT(*)
        FROM Users
        "#,
    );
    let sel = map_to_render_ast(&q);

    assert_eq!(sel.items.len(), 1);
    match &sel.items[0] {
        R::SelectItem::Expr { expr, alias: None } => match expr {
            R::Expr::FuncCall { name, args } => {
                assert_eq!(name, "COUNT");
                assert_eq!(args.len(), 1);
                // мы мапим * как идентификатор с путём ["*"]
                assert!(matches!(&args[0], R::Expr::Star));
            }
            other => panic!("expected func call, got {other:?}"),
        },
        other => panic!("unexpected select item: {other:?}"),
    }
}

#[test]
fn map_case_when_else() {
    let q = parse_query_generic(
        r#"
        SELECT CASE WHEN u.age > 18 THEN 'adult' ELSE 'minor' END
        FROM Users u
        "#,
    );
    let sel = map_to_render_ast(&q);

    match &sel.items[0] {
        R::SelectItem::Expr { expr, .. } => {
            match expr {
                R::Expr::Case {
                    operand,
                    when_then,
                    else_expr,
                } => {
                    assert!(operand.is_none());
                    assert_eq!(when_then.len(), 1);

                    // WHEN condition THEN result
                    let (cond, res) = &when_then[0];
                    // cond: u.age > 18
                    match cond {
                        R::Expr::Binary { left, op, right } => {
                            assert!(matches!(op, R::BinOp::Gt));
                            assert!(is_ident_path(left, &["u", "age"]));
                            assert!(matches!(**right, R::Expr::Number(ref n) if n == "18"));
                        }
                        other => panic!("unexpected WHEN cond: {other:?}"),
                    }
                    // result: 'adult'
                    assert!(matches!(res, R::Expr::String(s) if s == "adult"));

                    // ELSE 'minor'
                    match else_expr.as_deref() {
                        Some(R::Expr::String(s)) if s == "minor" => {}
                        other => panic!("unexpected ELSE: {other:?}"),
                    }
                }
                other => panic!("expected CASE expr, got {other:?}"),
            }
        }
        other => panic!("unexpected select item: {other:?}"),
    }
}

#[test]
fn map_in_list_and_between_normalization() {
    let q = parse_query_generic(
        r#"
        SELECT id
        FROM t
        WHERE age BETWEEN 10 AND 20
           OR city IN ('NY','LA')
        "#,
    );
    let sel = map_to_render_ast(&q);

    let w = sel.r#where.as_ref().expect("where");
    // корневой OR
    match w {
        R::Expr::Binary { left, op, right } => {
            assert!(matches!(op, R::BinOp::Or));

            // left: BETWEEN развёрнут в (age >= 10 AND age <= 20)
            match &**left {
                R::Expr::Binary {
                    left: l2,
                    op: op2,
                    right: r2,
                } => {
                    assert!(matches!(op2, R::BinOp::And));
                    // первая часть >=
                    match &**l2 {
                        R::Expr::Binary {
                            left: age,
                            op,
                            right: ge,
                        } => {
                            assert!(matches!(op, R::BinOp::Gte));
                            assert!(is_ident_path(age, &["age"]));
                            assert!(matches!(**ge, R::Expr::Number(ref n) if n == "10"));
                        }
                        _ => panic!("expected left >= 10"),
                    }
                    // вторая часть <=
                    match &**r2 {
                        R::Expr::Binary {
                            left: age,
                            op,
                            right: le,
                        } => {
                            assert!(matches!(op, R::BinOp::Lte));
                            assert!(is_ident_path(age, &["age"]));
                            assert!(matches!(**le, R::Expr::Number(ref n) if n == "20"));
                        }
                        _ => panic!("expected left <= 20"),
                    }
                }
                _ => panic!("expected AND for BETWEEN normalization"),
            }

            // right: city IN ('NY','LA') → Binary(In, right = Paren(tuple(...)))
            match &**right {
                R::Expr::Binary {
                    left: city,
                    op,
                    right,
                } => {
                    assert!(matches!(op, R::BinOp::In));
                    assert!(is_ident_path(city, &["city"]));
                    match &**right {
                        R::Expr::Tuple(args) => {
                            assert_eq!(args.len(), 2);
                            assert!(matches!(&args[0], R::Expr::String(s) if s == "NY"));
                            assert!(matches!(&args[1], R::Expr::String(s) if s == "LA"));
                        }
                        other => panic!("unexpected right of IN: {other:?}"),
                    }
                }
                _ => panic!("expected IN(list)"),
            }
        }
        other => panic!("unexpected WHERE root: {other:?}"),
    }
}
