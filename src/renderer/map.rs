use crate::renderer::ast as R;

use sqlparser::ast::{
    BinaryOperator as SBinOp, CaseWhen, Distinct, Expr as SExpr, Function, FunctionArg,
    FunctionArgExpr, FunctionArguments, GroupByExpr, GroupByWithModifier, Join as SJoin,
    JoinConstraint, JoinOperator as SJoinKind, LimitClause, ObjectName, OrderBy, OrderByExpr,
    OrderByKind, Query as SQuery, Select as SSelect, SelectItem as SSelectItem, SetExpr,
    TableFactor, UnaryOperator as SUnOp, Value, ValueWithSpan, WildcardAdditionalOptions,
};

/// Главная функция: Query -> renderer::ast::Select
///
/// Поддерживает `SELECT ... FROM ...` (без UNION/EXCEPT/INTERSECT).
/// Если `body` не `Select`, возвращаем минимальный SELECT-заглушку (`SELECT *`),
/// чтобы рендер не падал (при необходимости расширишь под set-выражения).
pub fn map_to_render_ast(q: &SQuery) -> R::Select {
    // 1) Разворачиваем тело запроса до Select
    let mut rsel = match q.body.as_ref() {
        SetExpr::Select(boxed) => map_select_to_render_ast(boxed),
        other => {
            // Заглушка на случай UNION/...: рендерим простой SELECT *
            let _ = other; // silence warning
            R::Select {
                distinct: false,
                distinct_on: vec![],
                items: vec![R::SelectItem::Star { opts: None }],
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
    };

    // 2) ORDER BY (в sqlparser он лежит на уровне Query)
    // if !q.order_by.is_none() {
    //     rsel.order_by = q.order_by.iter().map(map_order_by_item).collect();
    // }
    if let Some(ob) = q.order_by.as_ref() {
        rsel.order_by = map_order_by(ob);
    }

    // 3) LIMIT / OFFSET
    if let Some(lim_expr) = q.limit_clause.as_ref() {
        match lim_expr {
            LimitClause::LimitOffset {
                limit,
                offset,
                limit_by: _,
            } => {
                if let Some(v) = offset {
                    if let Some(v) = literal_u64(&v.value) {
                        rsel.offset = Some(v);
                    }
                }

                if let Some(v) = limit {
                    if let Some(v) = literal_u64(&v) {
                        rsel.limit = Some(v);
                    }
                }
            }
            LimitClause::OffsetCommaLimit { offset, limit } => {
                if let Some(v) = literal_u64(offset) {
                    rsel.offset = Some(v);
                }

                if let Some(v) = literal_u64(limit) {
                    rsel.limit = Some(v);
                }
            }
        }
    }

    rsel
}

/// Вспомогательная: Select -> renderer::ast::Select (без order/limit/offset)
fn map_select_to_render_ast(sel: &SSelect) -> R::Select {
    // Собираем базовый FROM и JOIN'ы (в sqlparser это Vec<TableWithJoins>)
    let mut from_named: Option<R::TableRef> = None;
    let mut joins: Vec<R::Join> = Vec::new();

    for twj in &sel.from {
        if from_named.is_none() {
            from_named = Some(map_table_factor(&twj.relation));
        } else {
            // Дополнительные элементы FROM сведём в CROSS JOIN
            joins.push(R::Join {
                kind: R::JoinKind::Cross,
                table: map_table_factor(&twj.relation),
                on: None,
            });
        }
        for j in &twj.joins {
            joins.push(map_join(j));
        }
    }

    // DISTINCT / DISTINCT ON(...)
    let (distinct_flag, distinct_on_vec): (bool, Vec<R::Expr>) = match &sel.distinct {
        None => (false, Vec::new()),
        Some(Distinct::Distinct) => (true, Vec::new()),
        Some(Distinct::On(exprs)) => (false, exprs.iter().map(map_expr).collect()),
    };

    // GROUP BY: выражения + модификаторы
    let (group_by_vec, group_by_mods) = map_group_by(sel);

    R::Select {
        distinct: distinct_flag,
        distinct_on: distinct_on_vec,
        items: sel.projection.iter().map(map_select_item).collect(),
        from: from_named,
        joins,
        r#where: sel.selection.as_ref().map(map_expr),
        group_by: group_by_vec,
        group_by_modifiers: group_by_mods,
        having: sel.having.as_ref().map(map_expr),
        order_by: Vec::new(),
        limit: None,
        offset: None,
    }
}

fn map_group_by(sel: &SSelect) -> (Vec<R::Expr>, Vec<R::GroupByModifier>) {
    match &sel.group_by {
        GroupByExpr::All(_) => (Vec::new(), Vec::new()),
        GroupByExpr::Expressions(exprs, items) => {
            let mut out_exprs: Vec<R::Expr> = Vec::new();
            let mut out_mods: Vec<R::GroupByModifier> = Vec::new();

            // 1) exprs могут содержать спец-узлы ROLLUP/CUBE/GROUPING SETS
            for e in exprs {
                match e {
                    // ROLLUP([ [e1, e2], [e1], [] ... ])
                    #[allow(unreachable_patterns)]
                    SExpr::Rollup(groups) => {
                        out_mods.push(R::GroupByModifier::Rollup);
                        for grp in groups {
                            for ee in grp {
                                out_exprs.push(map_expr(&ee));
                            }
                        }
                    }
                    // CUBE([...])
                    #[allow(unreachable_patterns)]
                    SExpr::Cube(groups) => {
                        out_mods.push(R::GroupByModifier::Cube);
                        for grp in groups {
                            for ee in grp {
                                out_exprs.push(map_expr(&ee));
                            }
                        }
                    }
                    // GROUPING SETS([...]) — представим полезную нагрузку как Expr::FuncCall
                    #[allow(unreachable_patterns)]
                    SExpr::GroupingSets(groups) => {
                        // модификатор — как и раньше
                        let gs_expr = R::Expr::FuncCall {
                            name: "GROUPING SETS".into(),
                            args: groups
                                .iter()
                                .map(|grp| R::Expr::FuncCall {
                                    name: "tuple".into(),
                                    args: grp.iter().map(map_expr).collect(),
                                })
                                .collect(),
                        };
                        out_mods.push(R::GroupByModifier::GroupingSets(gs_expr));

                        // ВАЖНО: положим и сами выражения в group_by,
                        // чтобы тесты (и рендер) видели столбцы.
                        for grp in groups {
                            for ee in grp {
                                out_exprs.push(map_expr(&ee));
                            }
                        }
                    }

                    // Некоторые сборки парсят ROLLUP/CUBE как функцию
                    SExpr::Function(Function { name, args, .. })
                        if name.to_string().eq_ignore_ascii_case("ROLLUP") =>
                    {
                        out_mods.push(R::GroupByModifier::Rollup);
                        if let FunctionArguments::List(list) = args {
                            for a in &list.args {
                                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) = a {
                                    out_exprs.push(map_expr(inner));
                                }
                            }
                        }
                    }
                    SExpr::Function(Function { name, args, .. })
                        if name.to_string().eq_ignore_ascii_case("CUBE") =>
                    {
                        out_mods.push(R::GroupByModifier::Cube);
                        if let FunctionArguments::List(list) = args {
                            for a in &list.args {
                                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) = a {
                                    out_exprs.push(map_expr(inner));
                                }
                            }
                        }
                    }

                    // Обычный GROUP BY expr
                    _ => out_exprs.push(map_expr(&e)),
                }
            }

            // 2) плюс модификаторы, если пришли "правильным" путём (items)
            for m in items {
                match m {
                    GroupByWithModifier::Rollup => out_mods.push(R::GroupByModifier::Rollup),
                    GroupByWithModifier::Cube => out_mods.push(R::GroupByModifier::Cube),
                    GroupByWithModifier::Totals => out_mods.push(R::GroupByModifier::Totals),
                    GroupByWithModifier::GroupingSets(e) => {
                        out_mods.push(R::GroupByModifier::GroupingSets(map_expr(&e)))
                    }
                }
            }

            (out_exprs, out_mods)
        }
    }
}

fn map_order_by(ob: &OrderBy) -> Vec<R::OrderItem> {
    match &ob.kind {
        OrderByKind::Expressions(list) => list.iter().map(map_order_by_expr).collect(),
        // Вариант ALL(...) не несёт конкретных выражений.
        // В наших диалектах (pg/mysql/sqlite) корректнее вернуть пусто
        OrderByKind::All(_) => Vec::new(),
    }
}

fn map_order_by_expr(obe: &OrderByExpr) -> R::OrderItem {
    R::OrderItem {
        expr: map_expr(&obe.expr),
        dir: match obe.options.asc {
            Some(true) => R::OrderDirection::Asc,
            Some(false) => R::OrderDirection::Desc,
            None => R::OrderDirection::Asc,
        },
        nulls_last: match obe.options.nulls_first {
            Some(true) => false,
            Some(false) => true,
            None => false,
        },
    }
}

fn map_wildcard_opts(o: &WildcardAdditionalOptions) -> Option<R::WildcardOpts> {
    if o.opt_ilike.is_none()
        && o.opt_exclude.is_none()
        && o.opt_except.is_none()
        && o.opt_replace.is_none()
        && o.opt_rename.is_none()
    {
        return None;
    }
    Some(R::WildcardOpts {
        ilike: o.opt_ilike.as_ref().map(|x| x.to_string()),
        exclude_raw: o.opt_exclude.as_ref().map(|x| x.to_string()),
        except_raw: o.opt_except.as_ref().map(|x| x.to_string()),
        replace_raw: o.opt_replace.as_ref().map(|x| x.to_string()),
        rename_raw: o.opt_rename.as_ref().map(|x| x.to_string()),
    })
}

fn map_select_item(it: &SSelectItem) -> R::SelectItem {
    match it {
        SSelectItem::Wildcard(opts) => R::SelectItem::Star {
            opts: map_wildcard_opts(opts),
        },
        SSelectItem::QualifiedWildcard(kind, opts) => {
            let mut s = kind.to_string();
            if let Some(prefix) = s.strip_suffix(".*") {
                s = prefix.to_string();
            }
            R::SelectItem::QualifiedStar {
                table: s,
                opts: map_wildcard_opts(opts),
            }
        }
        SSelectItem::ExprWithAlias { expr, alias } => R::SelectItem::Expr {
            expr: map_expr(expr),
            alias: Some(alias.value.clone()),
        },
        SSelectItem::UnnamedExpr(expr) => R::SelectItem::Expr {
            expr: map_expr(expr),
            alias: None,
        },
    }
}

fn map_table_factor(tf: &TableFactor) -> R::TableRef {
    match tf {
        TableFactor::Table { name, alias, .. } => {
            let (schema, table) = split_object_name(name);
            R::TableRef::Named {
                schema,
                name: table,
                alias: alias.as_ref().map(|a| a.name.value.clone()),
            }
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            // Подзапрос: Query -> Select
            let inner = map_to_render_ast(subquery);
            R::TableRef::Subquery {
                query: Box::new(inner),
                alias: alias.as_ref().map(|a| a.name.value.clone()),
            }
        }
        // Остальные факторы сведём к "именованной" таблице строкой — это универсально
        other => R::TableRef::Named {
            schema: None,
            name: other.to_string(),
            alias: None,
        },
    }
}

fn map_join(j: &SJoin) -> R::Join {
    let kind = match j.join_operator {
        SJoinKind::Inner(..) => R::JoinKind::Inner,
        SJoinKind::LeftOuter(..) => R::JoinKind::Left,
        SJoinKind::RightOuter(..) => R::JoinKind::Right,
        SJoinKind::FullOuter(..) => R::JoinKind::Full,
        SJoinKind::CrossJoin => R::JoinKind::Cross,
        SJoinKind::CrossApply => R::JoinKind::Cross,
        SJoinKind::OuterApply => R::JoinKind::Left, // приближённо
        SJoinKind::Join { .. } => R::JoinKind::Inner,
        _ => R::JoinKind::Inner,
    };

    // constraint (ON / USING)
    let on = match &j.join_operator {
        SJoinKind::Inner(constraint)
        | SJoinKind::LeftOuter(constraint)
        | SJoinKind::RightOuter(constraint)
        | SJoinKind::FullOuter(constraint) => match constraint {
            JoinConstraint::On(expr) => Some(map_expr(expr)),
            // USING(col1, col2, ...) — можно развернуть в цепочку равенств; пропустим для краткости
            _ => None,
        },
        _ => None,
    };

    R::Join {
        kind,
        table: map_table_factor(&j.relation),
        on,
    }
}

fn map_expr(e: &SExpr) -> R::Expr {
    use R::Expr as E;

    match e {
        // ----- Идентификаторы -----
        SExpr::Identifier(id) => E::Ident {
            path: vec![id.value.clone()],
        },
        SExpr::CompoundIdentifier(ids) => E::Ident {
            path: ids.iter().map(|i| i.value.clone()).collect(),
        },

        // ----- Литералы (новый обёртчик ValueWithSpan) -----
        SExpr::Value(v) => map_value_with_span(v),

        // ----- Унарные / бинарные -----
        SExpr::UnaryOp { op, expr } => E::Unary {
            op: map_un_op(op),
            expr: Box::new(map_expr(expr)),
        },
        SExpr::BinaryOp { left, op, right } => E::Binary {
            left: Box::new(map_expr(left)),
            op: map_bin_op(op),
            right: Box::new(map_expr(right)),
        },

        // ----- IS NULL / IS NOT NULL -----
        SExpr::IsNull(expr) => E::Binary {
            left: Box::new(map_expr(expr)),
            op: R::BinOp::Is,
            right: Box::new(E::Null),
        },
        SExpr::IsNotNull(expr) => E::Binary {
            left: Box::new(map_expr(expr)),
            op: R::BinOp::IsNot,
            right: Box::new(E::Null),
        },

        // ----- IN / NOT IN (expr IN (list)) -----
        SExpr::InList {
            expr,
            list,
            negated,
        } => E::Binary {
            left: Box::new(map_expr(expr)),
            op: if *negated {
                R::BinOp::NotIn
            } else {
                R::BinOp::In
            },
            // временно кодируем список через tuple(...) + скобки
            right: Box::new(E::Paren(Box::new(list_to_tuple(list)))),
        },

        // ----- LIKE / ILIKE (ESCAPE пока опускаем) -----
        SExpr::Like {
            negated,
            expr,
            pattern,
            ..
        } => E::Binary {
            left: Box::new(map_expr(expr)),
            op: if *negated {
                R::BinOp::NotLike
            } else {
                R::BinOp::Like
            },
            right: Box::new(map_expr(pattern)),
        },
        SExpr::ILike {
            negated,
            expr,
            pattern,
            ..
        } => E::Binary {
            left: Box::new(map_expr(expr)),
            op: if *negated {
                R::BinOp::NotIlike
            } else {
                R::BinOp::Ilike
            },
            right: Box::new(map_expr(pattern)),
        },

        // ----- BETWEEN -> (>= AND <=) / NOT (...) -----
        SExpr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let ge = E::Binary {
                left: Box::new(map_expr(expr)),
                op: R::BinOp::Gte,
                right: Box::new(map_expr(low)),
            };
            let le = E::Binary {
                left: Box::new(map_expr(expr)),
                op: R::BinOp::Lte,
                right: Box::new(map_expr(high)),
            };
            let and = E::Binary {
                left: Box::new(ge),
                op: R::BinOp::And,
                right: Box::new(le),
            };
            if *negated {
                E::Unary {
                    op: R::UnOp::Not,
                    expr: Box::new(and),
                }
            } else {
                and
            }
        }

        // ----- Скобки -----
        SExpr::Nested(inner) => E::Paren(Box::new(map_expr(inner))),

        // ----- Функции (новый тип FunctionArguments) -----
        SExpr::Function(Function { name, args, .. }) => E::FuncCall {
            name: name.to_string(),
            args: map_function_arguments(args),
        },

        // ----- CASE (в новых версиях нужно игнорировать служебные токены) -----
        SExpr::Case {
            operand,
            conditions,
            else_result,
            case_token: _,
            end_token: _,
        } => {
            let when_then = conditions
                .iter()
                .map(
                    |CaseWhen {
                         condition, result, ..
                     }| (map_expr(condition), map_expr(result)),
                )
                .collect::<Vec<_>>();
            E::Case {
                operand: operand.as_ref().map(|o| Box::new(map_expr(o))),
                when_then,
                else_expr: else_result.as_ref().map(|e| Box::new(map_expr(e))),
            }
        }

        // ----- fallback -----
        other => E::Ident {
            path: vec![other.to_string()],
        },
    }
}

fn map_value_with_span(v: &ValueWithSpan) -> R::Expr {
    match &v.value {
        Value::SingleQuotedString(s) | Value::NationalStringLiteral(s) => {
            R::Expr::String(s.clone())
        }
        Value::Number(n, _) => R::Expr::Number(n.clone()),
        Value::Boolean(b) => R::Expr::Bool(*b),
        Value::Null => R::Expr::Null,
        Value::Placeholder(_) => R::Expr::Bind, // ? / $1
        // любые другие — в строку (безопасный fallback)
        other => R::Expr::Ident {
            path: vec![other.to_string()],
        },
    }
}

fn map_function_arguments(args: &FunctionArguments) -> Vec<R::Expr> {
    match args {
        FunctionArguments::None => Vec::new(),
        // список обычных аргументов
        FunctionArguments::List(list) => list.args.iter().map(map_func_arg).collect(),
        // подзапрос как аргумент функции — сведём к строке
        FunctionArguments::Subquery(q) => vec![R::Expr::Ident {
            path: vec![q.to_string()],
        }],
        // на всякий случай: fallback
        _ => Vec::new(),
    }
}

fn map_func_arg(a: &FunctionArg) -> R::Expr {
    match a {
        // Именованный аргумент: name: Ident
        FunctionArg::Named { arg, .. } => match arg {
            FunctionArgExpr::Expr(e) => map_expr(e),
            FunctionArgExpr::Wildcard => R::Expr::Ident {
                path: vec!["*".into()],
            },
            FunctionArgExpr::QualifiedWildcard(obj) => R::Expr::Ident {
                path: vec![obj.to_string(), "*".into()],
            },
        },

        // Именованный аргумент: name: Expr
        FunctionArg::ExprNamed { arg, .. } => match arg {
            FunctionArgExpr::Expr(e) => map_expr(e),
            FunctionArgExpr::Wildcard => R::Expr::Ident {
                path: vec!["*".into()],
            },
            FunctionArgExpr::QualifiedWildcard(obj) => R::Expr::Ident {
                path: vec![obj.to_string(), "*".into()],
            },
        },

        // Неименованный
        FunctionArg::Unnamed(inner) => match inner {
            FunctionArgExpr::Expr(e) => map_expr(e),
            FunctionArgExpr::Wildcard => R::Expr::Ident {
                path: vec!["*".into()],
            },
            FunctionArgExpr::QualifiedWildcard(obj) => R::Expr::Ident {
                path: vec![obj.to_string(), "*".into()],
            },
        },
    }
}

// ——— helpers ———

fn split_object_name(obj: &ObjectName) -> (Option<String>, String) {
    // Берём строковое представление каждой части (schema / table / ...)
    let mut parts: Vec<String> = obj.0.iter().map(|p| p.to_string()).collect();

    if parts.len() >= 2 {
        let name = parts.pop().unwrap();
        let schema = parts.pop();
        (schema, name)
    } else {
        (None, parts.pop().unwrap_or_default())
    }
}

fn literal_u64(e: &SExpr) -> Option<u64> {
    match e {
        // VALUE -> ValueWithSpan -> Value::Number(...)
        SExpr::Value(v) => match &v.value {
            Value::Number(s, _) => s.parse::<u64>().ok(),
            _ => None,
        },

        // Иногда лимиты пишут как унарный плюс: +10 (редко)
        SExpr::UnaryOp { op, expr } if matches!(op, SUnOp::Plus) => literal_u64(expr),

        // Негативные значения игнорируем (для LIMIT/OFFSET невалидны)
        SExpr::UnaryOp { op, expr } if matches!(op, SUnOp::Minus) => {
            // можно явно вернуть None, даже если внутри число
            match &**expr {
                SExpr::Value(v) => match &v.value {
                    Value::Number(_, _) => None,
                    _ => None,
                },
                _ => None,
            }
        }

        _ => None,
    }
}

// пока у нашего AST нет Tuple — временно кодируем список как tuple(a,b,c)
fn list_to_tuple(list: &[SExpr]) -> R::Expr {
    R::Expr::FuncCall {
        name: "tuple".into(),
        args: list.iter().map(map_expr).collect(),
    }
}

fn map_bin_op(op: &SBinOp) -> R::BinOp {
    use R::BinOp as B;
    match op {
        SBinOp::Eq => B::Eq,
        SBinOp::NotEq => B::Neq,
        SBinOp::Lt => B::Lt,
        SBinOp::LtEq => B::Lte,
        SBinOp::Gt => B::Gt,
        SBinOp::GtEq => B::Gte,
        SBinOp::Plus => B::Add,
        SBinOp::Minus => B::Sub,
        SBinOp::Multiply => B::Mul,
        SBinOp::Divide => B::Div,
        SBinOp::Modulo => B::Mod,
        SBinOp::And => B::And,
        SBinOp::Or => B::Or,
        SBinOp::PGLikeMatch => B::Like,
        SBinOp::PGNotLikeMatch => B::NotLike,
        SBinOp::PGILikeMatch => B::Ilike,
        SBinOp::PGNotILikeMatch => B::NotIlike,
        _ => B::Eq, // безопасный fallback
    }
}

fn map_un_op(op: &SUnOp) -> R::UnOp {
    match op {
        SUnOp::Not => R::UnOp::Not,
        SUnOp::Minus => R::UnOp::Neg,
        _ => R::UnOp::Neg,
    }
}
