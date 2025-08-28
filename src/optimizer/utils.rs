use sqlparser::ast::{self as S, Insert, LimitClause};

/// Слить предикаты по И (`AND`): добавить `add` к `dst` (если оба есть — обернуть в `AND`).
///
/// Пример:
/// - `dst=None, add=Some(p)` → `dst=Some(p)`
/// - `dst=Some(a), add=Some(b)` → `dst=Some(a AND b)`
#[inline]
pub fn and_merge(dst: &mut Option<S::Expr>, add: Option<S::Expr>) {
    match (dst.take(), add) {
        (None, None) => *dst = None,
        (Some(a), None) => *dst = Some(a),
        (None, Some(b)) => *dst = Some(b),
        (Some(a), Some(b)) => {
            *dst = Some(S::Expr::BinaryOp {
                left: Box::new(a),
                op: S::BinaryOperator::And,
                right: Box::new(b),
            });
        }
    }
}

/// Проверка: `Select` не меняет кардинальность —
/// нет `DISTINCT`, `GROUP BY`, `HAVING`.
#[inline]
pub fn select_is_simple_no_cardinality(sel: &S::Select) -> bool {
    group_by_is_empty(&sel.group_by) && sel.distinct.is_none() && sel.having.is_none()
}

/* ————— Вспомогательные приватные утилиты ————— */

#[inline]
fn group_by_is_empty(g: &S::GroupByExpr) -> bool {
    match g {
        // Отсутствие GROUP BY в текущем AST кодируется через All(..)
        S::GroupByExpr::All(_mods) => true,
        S::GroupByExpr::Expressions(v, _mods) => v.is_empty(),
    }
}

/// Локальная проверка «прямой идентификатор колонки».
#[inline]
pub fn is_plain_column(e: &S::Expr) -> bool {
    matches!(e, S::Expr::Identifier(_) | S::Expr::CompoundIdentifier(_))
}

/// Локальная проверка «проекция = набор прямых колонок».
#[inline]
pub fn projection_is_direct_columns(sel: &S::Select) -> bool {
    sel.projection
        .iter()
        .all(|it| matches!(it, S::SelectItem::UnnamedExpr(e) if is_plain_column(e)))
}

/// Детерминированный "ключ" для сравнения литералов без `format!("{:?}")`.
/// Поведение не меняется: **Placeholder** исключаем, остальные варианты считаем константами.
#[inline]
pub fn is_literal_const(e: &S::Expr) -> Option<String> {
    use S::Value as V;
    let S::Expr::Value(vws) = e else {
        return None;
    };
    match &vws.value {
        // Не считаем константой
        V::Placeholder(_) => None,
        // Числа
        #[cfg(not(feature = "bigdecimal"))]
        V::Number(s, exact) => {
            let mut out = String::with_capacity(3 + s.len());
            out.push('N');
            out.push(':');
            out.push(if *exact { '1' } else { '0' });
            out.push(':');
            out.push_str(s);
            Some(out)
        }
        #[cfg(feature = "bigdecimal")]
        V::Number(n, exact) => {
            let s = n.to_string();
            let mut out = String::with_capacity(3 + s.len());
            out.push('N');
            out.push(':');
            out.push(if *exact { '1' } else { '0' });
            out.push(':');
            out.push_str(&s);
            Some(out)
        }
        // Булевы и NULL
        V::Boolean(b) => Some(if *b { "B:1" } else { "B:0" }.to_string()),
        V::Null => Some("Z".to_string()),
        // Строковые варианты (частые)
        V::SingleQuotedString(s) => {
            let mut out = String::with_capacity(2 + s.len());
            out.push_str("S:");
            out.push_str(s);
            Some(out)
        }
        V::DoubleQuotedString(s) => {
            let mut out = String::with_capacity(2 + s.len());
            out.push_str("D:");
            out.push_str(s);
            Some(out)
        }
        V::EscapedStringLiteral(s) => {
            let mut out = String::with_capacity(3 + s.len());
            out.push_str("ES:");
            out.push_str(s);
            Some(out)
        }
        V::UnicodeStringLiteral(s) => {
            let mut out = String::with_capacity(3 + s.len());
            out.push_str("US:");
            out.push_str(s);
            Some(out)
        }
        V::NationalStringLiteral(s) => {
            let mut out = String::with_capacity(3 + s.len());
            out.push_str("NS:");
            out.push_str(s);
            Some(out)
        }
        V::HexStringLiteral(s) => {
            let mut out = String::with_capacity(3 + s.len());
            out.push_str("HX:");
            out.push_str(s);
            Some(out)
        }
        V::SingleQuotedByteStringLiteral(s) => {
            let mut out = String::with_capacity(3 + s.len());
            out.push_str("SB:");
            out.push_str(s);
            Some(out)
        }
        V::DoubleQuotedByteStringLiteral(s) => {
            let mut out = String::with_capacity(3 + s.len());
            out.push_str("DB:");
            out.push_str(s);
            Some(out)
        }
        V::TripleSingleQuotedByteStringLiteral(s) => {
            let mut out = String::with_capacity(4 + s.len());
            out.push_str("TSB:");
            out.push_str(s);
            Some(out)
        }
        V::TripleDoubleQuotedByteStringLiteral(s) => {
            let mut out = String::with_capacity(4 + s.len());
            out.push_str("TDB:");
            out.push_str(s);
            Some(out)
        }
        V::SingleQuotedRawStringLiteral(s) => {
            let mut out = String::with_capacity(3 + s.len());
            out.push_str("RS:");
            out.push_str(s);
            Some(out)
        }
        V::DoubleQuotedRawStringLiteral(s) => {
            let mut out = String::with_capacity(3 + s.len());
            out.push_str("RD:");
            out.push_str(s);
            Some(out)
        }
        V::TripleSingleQuotedRawStringLiteral(s) => {
            let mut out = String::with_capacity(4 + s.len());
            out.push_str("TRS:");
            out.push_str(s);
            Some(out)
        }
        V::TripleDoubleQuotedRawStringLiteral(s) => {
            let mut out = String::with_capacity(4 + s.len());
            out.push_str("TRD:");
            out.push_str(s);
            Some(out)
        }
        V::TripleSingleQuotedString(s) => {
            let mut out = String::with_capacity(3 + s.len());
            out.push_str("TS:");
            out.push_str(s);
            Some(out)
        }
        V::TripleDoubleQuotedString(s) => {
            let mut out = String::with_capacity(3 + s.len());
            out.push_str("TD:");
            out.push_str(s);
            Some(out)
        }
        // Редкие/сложные — холодная ветка: оставляем стабильный Debug
        v => Some(format!("{v:?}")),
    }
}

// ===== Общие обходчики AST (mut) =====

/// Порядок вызова колбэка при обходе выражения.
#[derive(Clone, Copy)]
pub enum WalkOrder {
    /// Сначала узел, потом дети.
    Pre,
    /// Сначала дети, потом узел.
    Post,
}

/// Обойти `Expr` (мутирующий обход) с заданным порядком вызова.
#[inline]
pub fn walk_expr_mut<F>(e: &mut S::Expr, order: WalkOrder, f: &mut F)
where
    F: FnMut(&mut S::Expr),
{
    walk_expr_mut_dyn(e, order, f as &mut dyn FnMut(&mut S::Expr));
}

fn walk_expr_mut_dyn(e: &mut S::Expr, order: WalkOrder, f: &mut dyn FnMut(&mut S::Expr)) {
    use S::Expr::*;

    // Сделали f объектным типом, чтобы избежать рекурсивной мономорфизации.
    fn go(e: &mut S::Expr, order: WalkOrder, f: &mut dyn FnMut(&mut S::Expr)) {
        if matches!(order, WalkOrder::Pre) {
            f(e);
        }

        match e {
            Subquery(q) => {
                // Устраняем выделение временного замыкания для on_expr:
                // передаём исходный f напрямую через внутренний обходчик.
                _walk_query_mut_with_order(q, false, order, &mut |_, _| {}, f);
            }
            Exists { subquery, .. } => {
                _walk_query_mut_with_order(subquery, false, order, &mut |_, _| {}, f);
            }
            InSubquery { expr, subquery, .. } => {
                go(expr, order, f);
                _walk_query_mut_with_order(subquery, false, order, &mut |_, _| {}, f);
            }

            UnaryOp { expr, .. } => go(expr, order, f),

            BinaryOp { left, right, .. } => {
                go(left, order, f);
                go(right, order, f);
            }

            Between {
                expr, low, high, ..
            } => {
                go(expr, order, f);
                go(low, order, f);
                go(high, order, f);
            }

            Cast { expr, .. } | Extract { expr, .. } => go(expr, order, f),
            Nested(inner) => go(inner, order, f),

            Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(op) = operand.as_mut() {
                    go(op, order, f);
                }
                for w in conditions {
                    go(&mut w.condition, order, f);
                    go(&mut w.result, order, f);
                }
                if let Some(er) = else_result.as_mut() {
                    go(er, order, f);
                }
            }

            Function(S::Function { args, .. }) => {
                use sqlparser::ast::{
                    FunctionArg, FunctionArgExpr, FunctionArgumentClause, FunctionArguments,
                };

                match args {
                    FunctionArguments::List(list) => {
                        for a in &mut list.args {
                            match a {
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(x))
                                | FunctionArg::Named {
                                    arg: FunctionArgExpr::Expr(x),
                                    ..
                                } => go(x, order, f),
                                _ => {}
                            }
                        }
                        for clause in &mut list.clauses {
                            match clause {
                                FunctionArgumentClause::OrderBy(items) => {
                                    for ob in items {
                                        go(&mut ob.expr, order, f);
                                    }
                                }
                                FunctionArgumentClause::Limit(e) => go(e, order, f),
                                FunctionArgumentClause::Having(h) => go(&mut h.1, order, f),
                                FunctionArgumentClause::OnOverflow(_)
                                | FunctionArgumentClause::Separator(_)
                                | FunctionArgumentClause::JsonNullClause(_)
                                | FunctionArgumentClause::IgnoreOrRespectNulls(_) => {}
                            }
                        }
                    }
                    FunctionArguments::Subquery(x) => {
                        _walk_query_mut_with_order(x, false, order, &mut |_, _| {}, f);
                    }
                    _ => {}
                }
            }

            _ => {}
        }

        if matches!(order, WalkOrder::Post) {
            f(e);
        }
    }

    go(e, order, f);
}
/// Обойти `Join`: посетить только выражения внутри `ON`/`AS OF`.
#[inline]
pub fn walk_join_mut(j: &mut S::Join, f_expr: &mut dyn FnMut(&mut S::Expr)) {
    use S::JoinOperator;
    match &mut j.join_operator {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::Semi(c)
        | JoinOperator::LeftSemi(c)
        | JoinOperator::RightSemi(c)
        | JoinOperator::Anti(c)
        | JoinOperator::LeftAnti(c)
        | JoinOperator::RightAnti(c)
        | JoinOperator::StraightJoin(c) => {
            if let S::JoinConstraint::On(e) = c {
                walk_expr_mut_dyn(e, WalkOrder::Post, f_expr);
            }
        }
        JoinOperator::AsOf {
            match_condition,
            constraint,
        } => {
            walk_expr_mut_dyn(match_condition, WalkOrder::Post, f_expr);
            if let S::JoinConstraint::On(e) = constraint {
                walk_expr_mut_dyn(e, WalkOrder::Post, f_expr);
            }
        }
        JoinOperator::CrossJoin | JoinOperator::CrossApply | JoinOperator::OuterApply => {}
    }
}

/// Обойти `TableFactor` (интересуют только Derived/NestedJoin).
#[inline]
pub(crate) fn walk_table_factor_mut<FQ>(
    tf: &mut S::TableFactor,
    on_query: &mut FQ,
    on_expr: &mut dyn FnMut(&mut S::Expr),
) where
    FQ: FnMut(&mut S::Query, bool),
{
    match tf {
        S::TableFactor::Derived { subquery, .. } => {
            _walk_query_mut_with_order(subquery, false, WalkOrder::Post, on_query, on_expr)
        }
        S::TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            walk_table_with_joins_mut(table_with_joins, on_query, on_expr);
        }
        _ => {}
    }
}

/// Обойти `TableWithJoins`: relation + joins.
#[inline]
pub(crate) fn walk_table_with_joins_mut<FQ>(
    twj: &mut S::TableWithJoins,
    on_query: &mut FQ,
    on_expr: &mut dyn FnMut(&mut S::Expr),
) where
    FQ: FnMut(&mut S::Query, bool),
{
    walk_table_factor_mut(&mut twj.relation, on_query, on_expr);
    for j in &mut twj.joins {
        walk_table_factor_mut(&mut j.relation, on_query, on_expr);
        walk_join_mut(j, on_expr);
    }
}

/// Внутренний вариант `walk_query_mut` с явным порядком вызова `on_expr`.
#[inline]
fn _walk_query_mut_with_order<FQ>(
    q: &mut S::Query,
    top_level: bool,
    order: WalkOrder,
    on_query: &mut FQ,
    on_expr: &mut dyn FnMut(&mut S::Expr),
) where
    FQ: FnMut(&mut S::Query, bool),
{
    if let Some(w) = &mut q.with {
        for cte in &mut w.cte_tables {
            _walk_query_mut_with_order(&mut cte.query, false, order, on_query, on_expr);
        }
    }
    walk_setexpr_mut(q.body.as_mut(), on_query, on_expr);
    if let Some(ob) = &mut q.order_by {
        if let S::OrderByKind::Expressions(items) = &mut ob.kind {
            for o in items {
                match order {
                    WalkOrder::Pre => on_expr(&mut o.expr),
                    WalkOrder::Post => walk_expr_mut_dyn(&mut o.expr, WalkOrder::Post, on_expr),
                }
            }
        }
    }
    if let Some(limit) = &mut q.limit_clause {
        match limit {
            LimitClause::LimitOffset { limit, offset, .. } => {
                if let Some(e) = limit {
                    walk_expr_mut_dyn(e, order, on_expr);
                }
                if let Some(e) = offset {
                    walk_expr_mut_dyn(&mut e.value, order, on_expr);
                }
            }
            LimitClause::OffsetCommaLimit { limit, offset } => {
                walk_expr_mut_dyn(limit, order, on_expr);
                walk_expr_mut_dyn(offset, order, on_expr);
            }
        }
    }
    if let Some(fetch) = &mut q.fetch {
        if let Some(ref mut e) = fetch.quantity {
            walk_expr_mut_dyn(e, order, on_expr);
        }
    }
    on_query(q, top_level);
}

/// Унифицированный обход `Statement`. Гарантирует `on_query(top_level=true)`
/// для `Statement::Query`.
#[inline]
pub fn walk_statement_mut<FQ>(
    stmt: &mut S::Statement,
    on_query: &mut FQ,
    on_expr: &mut dyn FnMut(&mut S::Expr),
) where
    FQ: FnMut(&mut S::Query, bool),
{
    match stmt {
        S::Statement::Query(q) => {
            _walk_query_mut_with_order(q, true, WalkOrder::Post, on_query, on_expr)
        }
        S::Statement::Insert(Insert { source, .. }) => {
            if let Some(q) = source.as_mut() {
                _walk_query_mut_with_order(q, true, WalkOrder::Post, on_query, on_expr);
            }
        }
        S::Statement::Update {
            selection,
            assignments,
            from,
            table,
            ..
        } => {
            for a in assignments {
                walk_expr_mut_dyn(&mut a.value, WalkOrder::Post, on_expr)
            }
            if let Some(e) = selection {
                walk_expr_mut_dyn(e, WalkOrder::Post, on_expr);
            }
            crate::optimizer::utils::walk_table_with_joins_mut(table, on_query, on_expr);
            if let Some(kind) = from {
                match kind {
                    S::UpdateTableFromKind::BeforeSet(list)
                    | S::UpdateTableFromKind::AfterSet(list) => {
                        for t in list {
                            crate::optimizer::utils::walk_table_with_joins_mut(
                                t, on_query, on_expr,
                            );
                        }
                    }
                }
            }
        }
        S::Statement::Delete(S::Delete {
            selection,
            from,
            using,
            ..
        }) => {
            if let Some(e) = selection {
                walk_expr_mut_dyn(e, WalkOrder::Post, on_expr);
            }
            match from {
                S::FromTable::WithFromKeyword(list) | S::FromTable::WithoutKeyword(list) => {
                    for t in list {
                        crate::optimizer::utils::walk_table_with_joins_mut(t, on_query, on_expr);
                    }
                }
            }
            if let Some(list) = using {
                for t in list {
                    crate::optimizer::utils::walk_table_with_joins_mut(t, on_query, on_expr);
                }
            }
        }
        _ => {}
    }
}

/// Вспомогательно: рекурсивный обход SetExpr.
#[inline]
fn walk_setexpr_mut<FQ>(
    se: &mut S::SetExpr,
    on_query: &mut FQ,
    on_expr: &mut dyn FnMut(&mut S::Expr),
) where
    FQ: FnMut(&mut S::Query, bool),
{
    match se {
        S::SetExpr::Select(sel_box) => {
            // Обработка SELECT (как было в walk_query_mut для Select)
            let sel = sel_box.as_mut();
            for it in &mut sel.projection {
                match it {
                    S::SelectItem::UnnamedExpr(e) => walk_expr_mut_dyn(e, WalkOrder::Post, on_expr),
                    S::SelectItem::ExprWithAlias { expr, .. } => {
                        walk_expr_mut_dyn(expr, WalkOrder::Post, on_expr)
                    }
                    _ => {}
                }
            }
            if let Some(e) = &mut sel.selection {
                walk_expr_mut_dyn(e, WalkOrder::Post, on_expr);
            }
            // group_by в 0.58 — GroupByExpr
            match &mut sel.group_by {
                S::GroupByExpr::Expressions(exprs, _mods) => {
                    for e in exprs {
                        walk_expr_mut_dyn(e, WalkOrder::Post, on_expr);
                    }
                }
                S::GroupByExpr::All(_mods) => {}
            }
            if let Some(e) = &mut sel.having {
                walk_expr_mut_dyn(e, WalkOrder::Post, on_expr);
            }
            for t in &mut sel.from {
                walk_table_with_joins_mut(t, on_query, on_expr);
            }
        }
        S::SetExpr::Query(nested_q) => {
            // Вложенный полноценный Query
            _walk_query_mut_with_order(
                nested_q.as_mut(),
                false,
                WalkOrder::Post,
                on_query,
                on_expr,
            );
        }
        S::SetExpr::SetOperation { left, right, .. } => {
            walk_setexpr_mut(left.as_mut(), on_query, on_expr);
            walk_setexpr_mut(right.as_mut(), on_query, on_expr);
        }
        _ => {}
    }
}
