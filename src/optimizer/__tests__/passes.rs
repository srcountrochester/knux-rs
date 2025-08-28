//! Тесты прохода оптимизатора по AST (без генерации SQL).

use sqlparser::ast as S;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::optimizer::dedup_in_list::*;
use crate::optimizer::flatten_simple_subqueries::*;
use crate::optimizer::predicate_pullup::*;
use crate::optimizer::predicate_pushdown::*;
use crate::optimizer::rm_subquery_order_by::*;
use crate::optimizer::simplify_exists::*;
use crate::optimizer::in_to_exists::*;

/// Проверяем: `ORDER BY` удаляется у подзапроса в FROM, если нет LIMIT/OFFSET/FETCH.
#[test]
fn rm_ob_in_derived_subquery_without_limit() {
    let sql = r#"
        SELECT *
        FROM (SELECT a FROM t ORDER BY a) AS s
    "#;

    let dialect = GenericDialect {};
    let mut stmt = Parser::parse_sql(&dialect, sql).unwrap().remove(0);

    rm_subquery_order_by(&mut stmt);

    // Достаём подзапрос и проверяем, что order_by снят.
    let inner_ob_is_none = match &stmt {
        S::Statement::Query(q) => match &*q.body {
            S::SetExpr::Select(sel) => match sel.from.first() {
                Some(S::TableWithJoins {
                    relation: S::TableFactor::Derived { subquery, .. },
                    ..
                }) => subquery.order_by.is_none(),
                _ => false,
            },
            _ => false,
        },
        _ => false,
    };

    assert!(inner_ob_is_none, "ORDER BY должен быть удалён у подзапроса");
}

/// Проверяем: `ORDER BY` сохраняется у подзапроса, если есть LIMIT.
#[test]
fn keep_ob_in_subquery_with_limit() {
    let sql = r#"
        SELECT id
        FROM users
        WHERE id IN (SELECT id FROM t ORDER BY id LIMIT 10)
    "#;

    let dialect = GenericDialect {};
    let mut stmt = Parser::parse_sql(&dialect, sql).unwrap().remove(0);

    rm_subquery_order_by(&mut stmt);

    // Достаём IN (subquery) и убеждаемся, что order_by остался.
    let ob_present = match &stmt {
        S::Statement::Query(q) => match &*q.body {
            S::SetExpr::Select(sel) => match &sel.selection {
                Some(S::Expr::InSubquery { subquery, .. }) => subquery.order_by.is_some(),
                _ => false,
            },
            _ => false,
        },
        _ => false,
    };

    assert!(ob_present, "ORDER BY в подзапросе с LIMIT трогать нельзя");
}

/// Тест: упрощение `EXISTS` в WHERE — ORDER BY внутри подзапроса удаляется,
/// проекция переписывается на `SELECT 1`.
#[test]
fn simplify_exists_in_where_sets_select_one_and_removes_order() {
    let sql = r#"
        SELECT *
        FROM t
        WHERE EXISTS (SELECT a FROM u ORDER BY a)
    "#;

    let dialect = GenericDialect {};
    let mut stmt = Parser::parse_sql(&dialect, sql).unwrap().remove(0);

    simplify_exists(&mut stmt);

    // Достаём EXISTS(subquery)
    let check = match &stmt {
        S::Statement::Query(q) => match &*q.body {
            S::SetExpr::Select(sel) => match &sel.selection {
                Some(S::Expr::Exists { subquery, .. }) => {
                    // ORDER BY должен быть удалён
                    if subquery.order_by.is_some() {
                        return panic!("ORDER BY должен быть удалён");
                    }
                    // Проекция: ровно один элемент — число 1
                    match &*subquery.body {
                        S::SetExpr::Select(inner_sel) => {
                            if inner_sel.projection.len() != 1 {
                                return panic!("проекция должна быть из одного элемента");
                            }
                            match &inner_sel.projection[0] {
                                S::SelectItem::UnnamedExpr(S::Expr::Value(vws)) => {
                                    matches!(vws.value, S::Value::Number(ref s, false) if s == "1")
                                }
                                _ => false,
                            }
                        }
                        _ => false,
                    }
                }
                _ => false,
            },
            _ => false,
        },
        _ => false,
    };

    assert!(check, "ожидалась проекция SELECT 1 и отсутствие ORDER BY");
}

/// Тест: `EXISTS` в проекции (SELECT ...) — даже если внутри стоит LIMIT,
/// ORDER BY всё равно удаляется, проекция заменяется на `1`.
#[test]
fn simplify_exists_in_projection_ignores_order_even_with_limit() {
    let sql = r#"
        SELECT EXISTS(SELECT x FROM u ORDER BY x LIMIT 10) FROM t
    "#;

    let dialect = GenericDialect {};
    let mut stmt = Parser::parse_sql(&dialect, sql).unwrap().remove(0);

    simplify_exists(&mut stmt);

    // Найти EXISTS в списке SELECT
    let check = match &stmt {
        S::Statement::Query(q) => match &*q.body {
            S::SetExpr::Select(sel) => match &sel.projection[0] {
                S::SelectItem::UnnamedExpr(S::Expr::Exists { subquery, .. }) => {
                    // ORDER BY должен быть снят
                    if subquery.order_by.is_some() {
                        return panic!("ORDER BY должен быть удалён");
                    }
                    // Проекция подзапроса должна стать SELECT 1
                    match &*subquery.body {
                        S::SetExpr::Select(inner_sel) => {
                            matches!(
                                &inner_sel.projection[0],
                                S::SelectItem::UnnamedExpr(S::Expr::Value(vws))
                                    if matches!(vws.value, S::Value::Number(ref s, false) if s == "1")
                            )
                        }
                        _ => false,
                    }
                }
                _ => false,
            },
            _ => false,
        },
        _ => false,
    };

    assert!(check, "ожидалась проекция SELECT 1 и отсутствие ORDER BY");
}

/// Тест: поднимаем простой подзапрос во FROM без JOIN/DISTINCT/AGG.
/// Ожидаем: `TableFactor::Derived` заменён на базовую таблицу `TableFactor::Table`,
/// а предикат из внутреннего WHERE перенесён наверх (конъюнкция с внешним WHERE).
#[test]
fn pp_pull_up_simple_derived_and_merge_where() {
    let sql = r#"
        SELECT s.a
        FROM (SELECT a FROM t WHERE a > 0) AS s
        WHERE s.a > 10
    "#;

    let dialect = GenericDialect {};
    let mut stmt = Parser::parse_sql(&dialect, sql).unwrap().remove(0);

    predicate_pullup(&mut stmt);

    // FROM должен стать именованной таблицей
    let (is_table, outer_has_where) = match &stmt {
        S::Statement::Query(q) => match &*q.body {
            S::SetExpr::Select(sel) => {
                let is_table = matches!(sel.from[0].relation, S::TableFactor::Table { .. });
                let has_where = sel.selection.is_some();
                (is_table, has_where)
            }
            _ => (false, false),
        },
        _ => (false, false),
    };

    assert!(is_table, "ожидали замену Derived на Table");
    assert!(
        outer_has_where,
        "внешний WHERE должен содержать конъюнкцию предикатов"
    );
}

/// Тест: не поднимать подзапрос, если в нём DISTINCT.
/// Ожидаем: `TableFactor::Derived` сохраняется.
#[test]
fn pp_dont_pull_up_when_distinct_present() {
    let sql = r#"
        SELECT *
        FROM (SELECT DISTINCT a FROM t) AS s
    "#;

    let dialect = GenericDialect {};
    let mut stmt = Parser::parse_sql(&dialect, sql).unwrap().remove(0);

    predicate_pullup(&mut stmt);

    let remains_derived = match &stmt {
        S::Statement::Query(q) => match &*q.body {
            S::SetExpr::Select(sel) => {
                matches!(sel.from[0].relation, S::TableFactor::Derived { .. })
            }
            _ => false,
        },
        _ => false,
    };

    assert!(
        remains_derived,
        "при DISTINCT подзапрос не должен подниматься"
    );
}

/// Тест: predicate pushdown — переносим `WHERE s.a > 10` во внутренний подзапрос.
/// Ожидаем: `outer.selection == None`, а у `inner.selection` появился конъюнкт `a > 10`.
#[test]
fn ppd_push_where_into_simple_derived() {
    let sql = r#"
        SELECT *
        FROM (SELECT a FROM t) AS s
        WHERE s.a > 10
    "#;

    let dialect = GenericDialect {};
    let mut stmt = Parser::parse_sql(&dialect, sql).unwrap().remove(0);

    predicate_pushdown(&mut stmt);

    let (outer_where_is_none, inner_where_exists) = match &stmt {
        S::Statement::Query(q) => match &*q.body {
            S::SetExpr::Select(sel) => {
                let ow = sel.selection.is_none();
                let iw = match &sel.from[0].relation {
                    S::TableFactor::Derived { subquery, .. } => match subquery.body.as_ref() {
                        S::SetExpr::Select(isel) => isel.selection.is_some(),
                        _ => false,
                    },
                    _ => false,
                };
                (ow, iw)
            }
            _ => (false, false),
        },
        _ => (false, false),
    };

    assert!(
        outer_where_is_none,
        "внешний WHERE должен быть перенесён внутрь"
    );
    assert!(inner_where_exists, "у подзапроса должен появиться WHERE");
}

/// Тест: predicate pushdown НЕ выполняется, если подзапрос содержит DISTINCT.
/// Ожидаем: внешний `WHERE` остаётся на месте, внутренний — отсутствует.
#[test]
fn ppd_do_not_push_when_distinct() {
    let sql = r#"
        SELECT *
        FROM (SELECT DISTINCT a FROM t) AS s
        WHERE s.a > 10
    "#;

    let dialect = GenericDialect {};
    let mut stmt = Parser::parse_sql(&dialect, sql).unwrap().remove(0);

    predicate_pushdown(&mut stmt);

    let (outer_where_exists, inner_where_is_none) = match &stmt {
        S::Statement::Query(q) => match &*q.body {
            S::SetExpr::Select(sel) => {
                let ow = sel.selection.is_some();
                let iw = match &sel.from[0].relation {
                    S::TableFactor::Derived { subquery, .. } => match subquery.body.as_ref() {
                        S::SetExpr::Select(isel) => isel.selection.is_none(),
                        _ => false,
                    },
                    _ => false,
                };
                (ow, iw)
            }
            _ => (false, false),
        },
        _ => (false, false),
    };

    assert!(outer_where_exists, "внешний WHERE должен остаться");
    assert!(inner_where_is_none, "внутреннего WHERE быть не должно");
}

/// Тест: сплющивание простого derived-подзапроса, перенос внутреннего WHERE наружу.
/// Ожидаем: `TableFactor::Derived` заменён на `TableFactor::Table`, внешний `WHERE` появился.
#[test]
fn flatten_simple_subquery_basic() {
    let sql = r#"
        SELECT s.a
        FROM (SELECT a FROM t WHERE a > 0) AS s
    "#;

    let dialect = GenericDialect {};
    let mut stmt = Parser::parse_sql(&dialect, sql).unwrap().remove(0);

    flatten_simple_subqueries(&mut stmt);

    let (is_table, outer_where_some) = match &stmt {
        S::Statement::Query(q) => match &*q.body {
            S::SetExpr::Select(sel) => {
                let is_table = matches!(sel.from[0].relation, S::TableFactor::Table { .. });
                (is_table, sel.selection.is_some())
            }
            _ => (false, false),
        },
        _ => (false, false),
    };

    assert!(is_table, "ожидали замену Derived на Table");
    assert!(outer_where_some, "внешний WHERE должен появиться (a > 0)");
}

/// Тест: сплющивание с тривиальной проекцией внутреннего SELECT (прямые колонки, без WHERE).
/// Ожидаем: `Derived` → `Table`, внешний `WHERE` отсутствует.
#[test]
fn flatten_simple_subquery_projection_only() {
    let sql = r#"
        SELECT *
        FROM (SELECT a FROM t) AS s
    "#;

    let dialect = GenericDialect {};
    let mut stmt = Parser::parse_sql(&dialect, sql).unwrap().remove(0);

    flatten_simple_subqueries(&mut stmt);

    let ok = match &stmt {
        S::Statement::Query(q) => match &*q.body {
            S::SetExpr::Select(sel) => {
                matches!(sel.from[0].relation, S::TableFactor::Table { .. })
                    && sel.selection.is_none()
            }
            _ => false,
        },
        _ => false,
    };

    assert!(
        ok,
        "должны получить базовую таблицу и отсутствие внешнего WHERE"
    );
}

/// Тест: удаляем дубликаты констант в `IN` — числа 1 и 2 повторяются.
#[test]
fn dedup_in_list_numbers_basic() {
    let sql = r#"
        SELECT * FROM t
        WHERE id IN (1, 2, 2, 3, 1)
    "#;

    let dialect = GenericDialect {};
    let mut stmt = Parser::parse_sql(&dialect, sql).unwrap().remove(0);

    dedup_in_list(&mut stmt);

    // Достаём список IN и проверяем, что остались 1,2,3 (в порядке первых вхождений).
    let ok = match &stmt {
        S::Statement::Query(q) => match &*q.body {
            S::SetExpr::Select(sel) => match &sel.selection {
                Some(S::Expr::InList { list, .. }) => {
                    if list.len() != 3 {
                        false
                    } else {
                        matches!(&list[0], S::Expr::Value(v0) if matches!(v0.value, S::Value::Number(ref s, _) if s == "1"))
                            && matches!(&list[1], S::Expr::Value(v1) if matches!(v1.value, S::Value::Number(ref s, _) if s == "2"))
                            && matches!(&list[2], S::Expr::Value(v2) if matches!(v2.value, S::Value::Number(ref s, _) if s == "3"))
                    }
                }
                _ => false,
            },
            _ => false,
        },
        _ => false,
    };

    assert!(ok, "ожидались элементы [1, 2, 3] после дедупликации");
}

/// Тест: смешанный список — дубли удаляются только у констант, идентификаторы остаются.
/// Пример: `a IN (1, b, 1, b, 2)` → `[1, b, b, 2]` (дубликат `1` удалён, два `b` остаются).
#[test]
fn dedup_in_list_mixed_constants_and_identifiers() {
    let sql = r#"
        SELECT * FROM t
        WHERE a IN (1, b, 1, b, 2)
    "#;

    let dialect = GenericDialect {};
    let mut stmt = Parser::parse_sql(&dialect, sql).unwrap().remove(0);

    dedup_in_list(&mut stmt);

    let ok = match &stmt {
        S::Statement::Query(q) => match &*q.body {
            S::SetExpr::Select(sel) => match &sel.selection {
                Some(S::Expr::InList { list, .. }) => {
                    if list.len() != 4 {
                        false
                    } else {
                        // 1-й: число 1
                        let c1 = matches!(&list[0], S::Expr::Value(v) if matches!(v.value, S::Value::Number(ref s, _) if s == "1"));
                        // 2-й: идентификатор b
                        let c2 = matches!(&list[1], S::Expr::Identifier(id) if id.value == "b");
                        // 3-й: идентификатор b
                        let c3 = matches!(&list[2], S::Expr::Identifier(id) if id.value == "b");
                        // 4-й: число 2
                        let c4 = matches!(&list[3], S::Expr::Value(v) if matches!(v.value, S::Value::Number(ref s, _) if s == "2"));
                        c1 && c2 && c3 && c4
                    }
                }
                _ => false,
            },
            _ => false,
        },
        _ => false,
    };

    assert!(
        ok,
        "должны удалить только дубликаты констант, идентификаторы остаются"
    );
}

/// Тест: простой `IN (subquery)` переписывается в `EXISTS (...)`,
/// при этом проекция подзапроса становится `SELECT 1`, а внутри WHERE добавляется равенство.
#[test]
fn in_to_exists_basic_ast() {
    let sql = r#"
        SELECT *
        FROM u
        WHERE u.id IN (SELECT o.user_id FROM orders o WHERE o.status = 'open' ORDER BY o.id)
    "#;

    let dialect = GenericDialect {};
    let mut stmt = Parser::parse_sql(&dialect, sql).unwrap().remove(0);

    in_to_exists(&mut stmt);

    // WHERE должен стать EXISTS(...)
    let ok = match &stmt {
        S::Statement::Query(q) => match &*q.body {
            S::SetExpr::Select(sel) => match &sel.selection {
                Some(S::Expr::Exists { subquery, .. }) => {
                    // Внутренний SELECT: SELECT 1, без ORDER BY
                    if subquery.order_by.is_some() {
                        return panic!("ORDER BY должен быть удалён");
                    }
                    match subquery.body.as_ref() {
                        S::SetExpr::Select(isel) => {
                            // проекция ровно из одного элемента — число 1
                            if isel.projection.len() != 1 {
                                false
                            } else {
                                matches!(
                                    &isel.projection[0],
                                    S::SelectItem::UnnamedExpr(S::Expr::Value(v))
                                        if matches!(v.value, S::Value::Number(ref s, false) if s == "1")
                                ) && isel.selection.is_some()
                            }
                        }
                        _ => false,
                    }
                }
                _ => false,
            },
            _ => false,
        },
        _ => false,
    };

    assert!(ok, "ожидался EXISTS с SELECT 1 и объединённым WHERE");
}

/// Тест: `NOT IN (subquery)` не переписывается (мы избегаем этой замены).
#[test]
fn in_to_exists_not_in_is_untouched_ast() {
    let sql = r#"
        SELECT *
        FROM u
        WHERE u.id NOT IN (SELECT user_id FROM orders)
    "#;

    let dialect = GenericDialect {};
    let mut stmt = Parser::parse_sql(&dialect, sql).unwrap().remove(0);

    in_to_exists(&mut stmt);

    // WHERE остаётся NOT IN (subquery)
    let ok = match &stmt {
        S::Statement::Query(q) => match &*q.body {
            S::SetExpr::Select(sel) => matches!(
                sel.selection,
                Some(S::Expr::InSubquery { negated: true, .. })
            ),
            _ => false,
        },
        _ => false,
    };

    assert!(ok, "NOT IN должен остаться без изменений");
}
