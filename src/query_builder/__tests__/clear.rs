use crate::expression::helpers::{col, val};
use crate::query_builder::QueryBuilder;
use sqlparser::ast::{Expr as SqlExpr, LimitClause, Query, SetExpr};

/// Достаём SELECT из Query
fn select_of(q: &Query) -> &sqlparser::ast::Select {
    match q.body.as_ref() {
        SetExpr::Select(sel) => sel,
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn clear_select_and_where_via_router() {
    let qb = QueryBuilder::new_empty()
        .from("users")
        .select(("email", "name"))
        .r#where(col("id").lt(val(10)))
        .clear("select")
        .clear("where");

    let (q, params) = qb.build_query_ast().expect("ok");
    assert!(params.is_empty(), "params must be empty");

    let sel = select_of(&q);
    // после clear_select проекция должна стать пустой у билдера,
    // а в AST по умолчанию рендерится SELECT *
    assert!(sel.projection.len() == 1, "projection must be * by default");
    assert!(sel.selection.is_none(), "WHERE must be cleared");
}

#[test]
fn clear_group() {
    let qb = QueryBuilder::new_empty()
        .from("t")
        .select(("*",))
        .group_by(("a", "b"))
        .clear_group();

    let (q, _) = qb.build_query_ast().expect("ok");
    let sel = select_of(&q);
    match &sel.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, _) => {
            assert!(exprs.is_empty(), "GROUP BY must be cleared");
        }
        _ => panic!("unexpected group_by variant"),
    }
}

#[test]
fn clear_order() {
    let qb = QueryBuilder::new_empty()
        .from("t")
        .select(("x",))
        .order_by(("a", "b"))
        .clear_order();

    let (q, _) = qb.build_query_ast().expect("ok");
    assert!(q.order_by.is_none(), "ORDER BY must be cleared");
}

#[test]
fn clear_having() {
    let qb = QueryBuilder::new_empty()
        .from("t")
        .select(("x",))
        .group_by(("x",))
        .having_raw("sum(x) > 10")
        .clear_having();

    let (q, _) = qb.build_query_ast().expect("ok");
    let sel = select_of(&q);
    assert!(sel.having.is_none(), "HAVING must be cleared");
}

#[test]
fn clear_join() {
    let qb = QueryBuilder::new_empty()
        .from("users")
        .select(("*",))
        .join("accounts", "users.id = accounts.user_id")
        .clear_join();

    let (q, _) = qb.build_query_ast().expect("ok");
    let sel = select_of(&q);
    assert!(!sel.from.is_empty(), "FROM must not be empty");
    assert!(
        sel.from[0].joins.is_empty(),
        "all JOINs for the first FROM item must be cleared"
    );
}

#[test]
fn clear_limit_and_clear_offset_individually() {
    // лимит очищаем, оффсет оставляем
    let qb1 = QueryBuilder::new_empty()
        .from("t")
        .select(("*",))
        .limit(10)
        .offset(5)
        .clear_limit();

    let (q1, _) = qb1.build_query_ast().expect("ok");
    match q1.limit_clause {
        Some(LimitClause::LimitOffset { limit, offset, .. }) => {
            assert!(limit.is_none(), "limit must be None after clear_limit()");
            assert!(offset.is_some(), "offset must stay present");
        }
        Some(LimitClause::OffsetCommaLimit { .. }) => {
            // для MySQL при обеих — OffsetCommaLimit; после clear_limit должен остаться только offset,
            // но мы рендерим LimitOffset при одиночном offset → сюда идеологически не попадём
            panic!("unexpected MySQL-style clause after clear_limit()");
        }
        None => panic!("offset should remain"),
    }

    // оффсет очищаем, лимит оставляем
    let qb2 = QueryBuilder::new_empty()
        .from("t")
        .select(("*",))
        .limit(10)
        .offset(5)
        .clear_offset();

    let (q2, _) = qb2.build_query_ast().expect("ok");
    match q2.limit_clause {
        Some(LimitClause::LimitOffset { limit, offset, .. }) => {
            assert!(limit.is_some(), "limit must remain present");
            assert!(offset.is_none(), "offset must be None after clear_offset()");
        }
        Some(LimitClause::OffsetCommaLimit { .. }) => {
            panic!("unexpected MySQL-style clause after clear_offset()");
        }
        None => panic!("limit should remain"),
    }
}

#[test]
fn clear_limit_offset_both() {
    let mut qb = QueryBuilder::new_empty()
        .from("t")
        .select(("*",))
        .limit(10)
        .offset(20);

    qb.clear_limit_offset(); // &mut self

    let (q, _) = qb.build_query_ast().expect("ok");
    assert!(
        q.limit_clause.is_none(),
        "both LIMIT and OFFSET must be cleared"
    );
}

#[test]
fn clear_counters_pushes_todo_error() {
    let err = QueryBuilder::new_empty()
        .from("t")
        .select(("*",))
        .clear_counters()
        .to_sql()
        .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("clear_counters(): TODO"),
        "expected TODO message, got: {msg}"
    );
}

#[test]
fn clear_router_unknown_and_unsupported() {
    // неизвестный оператор
    let err1 = QueryBuilder::new_empty()
        .from("t")
        .select(("*",))
        .clear("foobar")
        .to_sql()
        .unwrap_err();
    let m1 = err1.to_string();
    assert!(
        m1.contains("clear(): неизвестный оператор"),
        "must report unknown operator, got: {m1}"
    );

    // пока не поддерживаемые with/union
    let err2 = QueryBuilder::new_empty()
        .from("t")
        .select(("*",))
        .clear("with")
        .to_sql()
        .unwrap_err();
    let m2 = err2.to_string();
    assert!(
        m2.contains("оператор пока не поддерживается"),
        "must report unsupported operator, got: {m2}"
    );
}

#[test]
fn clear_distinct_resets_flag_and_items() {
    let qb = QueryBuilder::new_empty()
        .from("users")
        .select(("*",))
        .distinct_on((col("age"),))
        .distinct(())
        .clear_distinct();

    assert!(!qb.select_distinct);
    assert!(qb.distinct_on_items.is_empty());

    let (sql, _p) = qb.to_sql().expect("to_sql");
    // убедимся, что DISTINCT не попал
    assert!(!sql.to_uppercase().contains("DISTINCT"));
}
