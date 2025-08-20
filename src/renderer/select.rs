use crate::renderer::config::MysqlLimitStyle;
use crate::renderer::ident::push_quoted_path;

use super::ast as R;
use super::ast::*;
use super::config::{Dialect, SqlRenderCfg};
use super::ident::quote_ident;
use super::writer::SqlWriter;

pub fn render_sql_query(q: &R::Query, cfg: &SqlRenderCfg) -> String {
    let mut w = SqlWriter::new(256, cfg.placeholders);

    // WITH
    if let Some(with) = &q.with {
        w.push("WITH");
        if with.recursive {
            w.push(" RECURSIVE");
        }
        w.push(" ");
        for (i, cte) in with.ctes.iter().enumerate() {
            w.push_sep(i, ", ");

            // name
            w.push(&quote_ident(&cte.name, cfg));

            // (col1, col2, ...)
            if !cte.columns.is_empty() {
                w.push(" (");
                for (j, c) in cte.columns.iter().enumerate() {
                    if j > 0 {
                        w.push(", ");
                    }
                    w.push(&quote_ident(c, cfg));
                }
                w.push(")");
            }

            // FROM <ident>  (если задано)
            if let Some(from) = &cte.from {
                w.push(" FROM ");
                w.push(&quote_ident(from, cfg));
            }

            // AS [MATERIALIZED|NOT MATERIALIZED] ( ... )
            w.push(" AS");
            if let Some(mat) = &cte.materialized {
                // печатаем только в Postgres; в остальных диалектах опускаем ключевые слова
                if matches!(cfg.dialect, Dialect::Postgres) {
                    match mat {
                        CteMaterialized::Materialized => w.push(" MATERIALIZED"),
                        CteMaterialized::NotMaterialized => w.push(" NOT MATERIALIZED"),
                    }
                }
            }
            w.push(" (");
            render_query_body(&mut w, &cte.query, cfg);
            w.push(")");
        }
        w.push(" ");
    }

    // тело (Select/Set)
    render_query_body(&mut w, &q.body, cfg);

    // общий ORDER BY / LIMIT / OFFSET
    if !q.order_by.is_empty() {
        w.push(" ORDER BY ");
        for (i, oi) in q.order_by.iter().enumerate() {
            w.push_sep(i, ", ");

            // ── NEW: эмуляция NULLS LAST вне Postgres
            if !matches!(cfg.dialect, Dialect::Postgres)
                && cfg.emulate_nulls_ordering
                && oi.nulls_last
            {
                // 1) (expr IS NULL) ASC
                w.push("(");
                render_expr(&mut w, &oi.expr, cfg);
                w.push(" IS NULL) ASC, ");

                // 2) expr ASC|DESC
                render_expr(&mut w, &oi.expr, cfg);
                match oi.dir {
                    OrderDirection::Asc => w.push(" ASC"),
                    OrderDirection::Desc => w.push(" DESC"),
                }
                continue;
            }

            // обычный путь
            render_expr(&mut w, &oi.expr, cfg);
            match oi.dir {
                OrderDirection::Asc => w.push(" ASC"),
                OrderDirection::Desc => w.push(" DESC"),
            }

            // NULLS LAST печатаем только в PG
            if matches!(cfg.dialect, Dialect::Postgres) && oi.nulls_last {
                w.push(" NULLS LAST");
            }
        }
    }
    match (cfg.dialect, cfg.mysql_limit_style, q.limit, q.offset) {
        (Dialect::MySQL, MysqlLimitStyle::OffsetCommaLimit, Some(l), Some(o)) => {
            w.push(" LIMIT ");
            w.push_u64(o);
            w.push(", ");
            w.push_u64(l);
        }
        _ => {
            if let Some(l) = q.limit {
                w.push(" LIMIT ");
                w.push_u64(l);
            }
            if let Some(o) = q.offset {
                w.push(" OFFSET ");
                w.push_u64(o);
            }
        }
    }

    w.finish()
}

fn render_query_body(w: &mut SqlWriter, body: &R::QueryBody, cfg: &SqlRenderCfg) {
    match body {
        R::QueryBody::Select(s) => {
            let inner = render_select(s, cfg, 128);
            w.push(inner);
        }
        R::QueryBody::Set {
            left,
            op,
            right,
            by_name: _,
        } => {
            w.push("(");
            render_query_body(w, left, cfg);
            w.push(") ");
            match op {
                R::SetOp::Union => w.push("UNION "),
                R::SetOp::UnionAll => w.push("UNION ALL "),
                R::SetOp::Intersect => w.push("INTERSECT "),
                R::SetOp::IntersectAll => w.push("INTERSECT ALL "),
                R::SetOp::Except => w.push("EXCEPT "),
                R::SetOp::ExceptAll => w.push("EXCEPT ALL "),
            }
            w.push("(");
            render_query_body(w, right, cfg);
            w.push(")");
        }
    }
}
pub fn render_select(sel: &Select, cfg: &SqlRenderCfg, cap: usize) -> String {
    let mut w = SqlWriter::new(cap, cfg.placeholders);

    w.push("SELECT ");
    if !sel.distinct_on.is_empty() {
        // DISTINCT ON поддерживается только в Postgres
        match cfg.dialect {
            Dialect::Postgres => {
                w.push("DISTINCT ON (");
                for (i, e) in sel.distinct_on.iter().enumerate() {
                    w.push_sep(i, ", ");
                    render_expr(&mut w, e, cfg);
                }
                w.push(") ");
            }
            _ => {
                // Мягкая деградация: печатаем обычный DISTINCT
                w.push("DISTINCT ");
            }
        }
    } else if sel.distinct {
        w.push("DISTINCT ");
    }

    if sel.items.is_empty() {
        w.push("*");
    } else {
        for (i, it) in sel.items.iter().enumerate() {
            w.push_sep(i, ", ");
            render_select_item(&mut w, it, cfg);
        }
    }

    if let Some(from) = &sel.from {
        w.push(" FROM ");
        render_table_ref(&mut w, from, cfg);
    }

    for j in &sel.joins {
        w.push(" ");
        render_join(&mut w, j, cfg);
    }

    if let Some(pred) = &sel.r#where {
        w.push(" WHERE ");
        render_expr(&mut w, pred, cfg);
    }

    if !sel.group_by.is_empty() {
        match cfg.dialect {
            Dialect::Postgres => {
                // Если есть GroupingSets — печатаем конструкцию целиком
                if let Some(gs) = sel.group_by_modifiers.iter().find_map(|m| {
                    if let GroupByModifier::GroupingSets(e) = m {
                        Some(e)
                    } else {
                        None
                    }
                }) {
                    w.push(" GROUP BY GROUPING SETS (");
                    render_expr(&mut w, gs, cfg);
                    w.push(")");
                } else if sel
                    .group_by_modifiers
                    .iter()
                    .any(|m| matches!(m, GroupByModifier::Rollup))
                {
                    w.push(" GROUP BY ROLLUP (");
                    for (i, e) in sel.group_by.iter().enumerate() {
                        w.push_sep(i, ", ");
                        render_expr(&mut w, e, cfg);
                    }
                    w.push(")");
                } else if sel
                    .group_by_modifiers
                    .iter()
                    .any(|m| matches!(m, GroupByModifier::Cube))
                {
                    w.push(" GROUP BY CUBE (");
                    for (i, e) in sel.group_by.iter().enumerate() {
                        w.push_sep(i, ", ");
                        render_expr(&mut w, e, cfg);
                    }
                    w.push(")");
                } else {
                    w.push(" GROUP BY ");
                    for (i, e) in sel.group_by.iter().enumerate() {
                        w.push_sep(i, ", ");
                        render_expr(&mut w, e, cfg);
                    }
                }
                // Totals — не стандарт PG; игнорируем
            }
            Dialect::MySQL => {
                // MySQL поддерживает только WITH ROLLUP
                w.push(" GROUP BY ");
                for (i, e) in sel.group_by.iter().enumerate() {
                    w.push_sep(i, ", ");
                    render_expr(&mut w, e, cfg);
                }
                if sel
                    .group_by_modifiers
                    .iter()
                    .any(|m| matches!(m, GroupByModifier::Rollup))
                {
                    w.push(" WITH ROLLUP");
                }
                // Cube/GroupingSets/Totals — игнорируем
            }
            Dialect::SQLite => {
                // Нет расширений — печатаем обычный GROUP BY
                w.push(" GROUP BY ");
                for (i, e) in sel.group_by.iter().enumerate() {
                    w.push_sep(i, ", ");
                    render_expr(&mut w, e, cfg);
                }
            }
        }
    }

    if let Some(h) = &sel.having {
        w.push(" HAVING ");
        render_expr(&mut w, h, cfg);
    }

    if !sel.order_by.is_empty() {
        w.push(" ORDER BY ");
        for (i, oi) in sel.order_by.iter().enumerate() {
            w.push_sep(i, ", ");
            let emulate_nulls =
                !matches!(cfg.dialect, Dialect::Postgres) && cfg.emulate_nulls_ordering;

            if emulate_nulls && oi.nulls_last {
                // (expr IS NULL) ASC, expr <ASC|DESC>
                w.push("(");
                render_expr(&mut w, &oi.expr, cfg);
                w.push(" IS NULL) ASC, ");
                render_expr(&mut w, &oi.expr, cfg);
                match oi.dir {
                    OrderDirection::Asc => w.push(" ASC"),
                    OrderDirection::Desc => w.push(" DESC"),
                }
            } else {
                // обычный путь
                render_expr(&mut w, &oi.expr, cfg);
                match oi.dir {
                    OrderDirection::Asc => w.push(" ASC"),
                    OrderDirection::Desc => w.push(" DESC"),
                }
                if matches!(cfg.dialect, Dialect::Postgres) && oi.nulls_last {
                    w.push(" NULLS LAST");
                }
            }
        }
    }

    match (cfg.dialect, cfg.mysql_limit_style, sel.limit, sel.offset) {
        (Dialect::MySQL, MysqlLimitStyle::OffsetCommaLimit, Some(l), Some(o)) => {
            w.push(" LIMIT ");
            w.push_u64(o); // offset
            w.push(", ");
            w.push_u64(l); // count
        }
        _ => {
            if let Some(l) = sel.limit {
                w.push(" LIMIT ");
                w.push_u64(l);
            }
            if let Some(o) = sel.offset {
                w.push(" OFFSET ");
                w.push_u64(o);
            }
        }
    }

    w.finish()
}

fn render_select_item(w: &mut SqlWriter, it: &SelectItem, cfg: &SqlRenderCfg) {
    match it {
        // SELECT *
        SelectItem::Star { .. } => {
            w.push("*");
        }

        // SELECT table.*  (или kind.*)
        SelectItem::QualifiedStar { table, .. } => {
            w.push(&quote_ident(table, cfg));
            w.push(".*");
        }

        // SELECT expr [AS alias]
        SelectItem::Expr { expr, alias } => {
            render_expr(w, expr, cfg);
            if let Some(a) = alias {
                if cfg.emit_as_for_column_alias {
                    w.push(" AS ");
                } else {
                    w.push(" ");
                }
                w.push(&quote_ident(a, cfg));
            }
        }
    }
}

fn render_table_ref(w: &mut SqlWriter, t: &TableRef, cfg: &SqlRenderCfg) {
    match t {
        TableRef::Named {
            schema,
            name,
            alias,
        } => {
            if let Some(s) = schema {
                push_quoted_path(w, [s.as_str(), name.as_str()], cfg);
            } else {
                w.push(&quote_ident(name, cfg));
            }
            if let Some(a) = alias {
                push_alias(w, a, cfg, cfg.emit_as_for_table_alias);
            }
        }
        TableRef::Subquery { query, alias } => {
            w.push("(");
            let inner = render_select(query, cfg, 128);
            w.push(inner);
            w.push(")");
            if let Some(a) = alias {
                push_alias(w, a, cfg, cfg.emit_as_for_table_alias);
            }
        }
    }
}

fn render_join(w: &mut SqlWriter, j: &Join, cfg: &SqlRenderCfg) {
    match j.kind {
        JoinKind::Inner => w.push("INNER JOIN "),
        JoinKind::Left => w.push("LEFT JOIN "),
        JoinKind::Right => w.push("RIGHT JOIN "),
        JoinKind::Full => w.push("FULL JOIN "),
        JoinKind::Cross => w.push("CROSS JOIN "),
        JoinKind::NaturalInner => w.push("NATURAL INNER JOIN "),
        JoinKind::NaturalLeft => w.push("NATURAL LEFT JOIN "),
        JoinKind::NaturalRight => w.push("NATURAL RIGHT JOIN "),
        JoinKind::NaturalFull => w.push("NATURAL FULL JOIN "),
    }
    render_table_ref(w, &j.table, cfg);
    if !matches!(
        j.kind,
        JoinKind::Cross
            | JoinKind::NaturalInner
            | JoinKind::NaturalLeft
            | JoinKind::NaturalRight
            | JoinKind::NaturalFull
    ) {
        if let Some(on) = &j.on {
            w.push(" ON ");
            render_expr(w, on, cfg);
        } else if let Some(cols) = &j.using_cols {
            w.push(" USING (");
            for (i, c) in cols.iter().enumerate() {
                w.push_sep(i, ", ");
                w.push(&quote_ident(c, cfg));
            }
            w.push(")");
        }
    }
}

pub fn render_expr(w: &mut SqlWriter, e: &Expr, cfg: &SqlRenderCfg) {
    match e {
        Expr::Raw(s) => w.push(s),
        Expr::Star => w.push("*"),
        Expr::Tuple(xs) => {
            w.push("(");
            for (i, x) in xs.iter().enumerate() {
                w.push_sep(i, ", ");
                render_expr(w, x, cfg);
            }
            w.push(")");
        }
        Expr::Like {
            not,
            ilike,
            expr,
            pattern,
            escape,
        } => {
            render_paren_if_needed(w, expr, cfg);
            match (*not, *ilike, cfg.dialect) {
                (false, false, _) => w.push(" LIKE "),
                (true, false, _) => w.push(" NOT LIKE "),
                (false, true, Dialect::Postgres) => w.push(" ILIKE "),
                (true, true, Dialect::Postgres) => w.push(" NOT ILIKE "),
                // ILIKE вне PG — деградация до LIKE/NOT LIKE
                (false, true, _) => w.push(" LIKE "),
                (true, true, _) => w.push(" NOT LIKE "),
            }
            render_paren_if_needed(w, pattern, cfg);
            if let Some(ch) = escape {
                w.push(" ESCAPE ");
                // одинарные кавычки вокруг символа экранирования
                let mut esc = String::with_capacity(3);
                esc.push('\'');
                if *ch == '\'' {
                    esc.push_str("''");
                } else {
                    esc.push(*ch);
                }
                esc.push('\'');
                w.push(esc);
            }
        }
        Expr::Ident { path } => {
            push_quoted_path(w, path.iter().map(|s| s.as_str()), cfg);
        }
        Expr::Bind => w.push_placeholder(),
        Expr::String(s) => {
            // простое экранирование одинарной кавычки
            let mut esc = String::with_capacity(s.len() + 2);
            esc.push('\'');
            for ch in s.chars() {
                if ch == '\'' {
                    esc.push_str("''");
                } else {
                    esc.push(ch);
                }
            }
            esc.push('\'');
            w.push(esc);
        }
        Expr::Number(n) => w.push(n),
        Expr::Bool(b) => w.push(if *b { "TRUE" } else { "FALSE" }),
        Expr::Null => w.push("NULL"),
        Expr::Unary { op, expr } => {
            match op {
                UnOp::Not => w.push("NOT "),
                UnOp::Neg => w.push("-"),
            }
            render_paren_if_needed(w, expr, cfg);
        }
        Expr::Binary { left, op, right } => {
            render_paren_if_needed(w, left, cfg);
            w.push(match op {
                BinOp::Eq => " = ",
                BinOp::Neq => " <> ",
                BinOp::Lt => " < ",
                BinOp::Lte => " <= ",
                BinOp::Gt => " > ",
                BinOp::Gte => " >= ",
                BinOp::And => " AND ",
                BinOp::Or => " OR ",
                BinOp::Add => " + ",
                BinOp::Sub => " - ",
                BinOp::Mul => " * ",
                BinOp::Div => " / ",
                BinOp::Mod => " % ",
                BinOp::Like => " LIKE ",
                BinOp::NotLike => " NOT LIKE ",
                BinOp::Ilike => match cfg.dialect {
                    Dialect::Postgres => " ILIKE ",
                    _ => " LIKE ", // в MySQL/SQLite нет ILIKE; оставляем LIKE
                },
                BinOp::NotIlike => match cfg.dialect {
                    Dialect::Postgres => " NOT ILIKE ",
                    _ => " NOT LIKE ",
                },
                BinOp::In => " IN ",
                BinOp::NotIn => " NOT IN ",
                BinOp::Is => " IS ",
                BinOp::IsNot => " IS NOT ",
            });
            render_paren_if_needed(w, right, cfg);
        }
        Expr::Paren(inner) => {
            w.push("(");
            render_expr(w, inner, cfg);
            w.push(")");
        }
        Expr::FuncCall { name, args } => {
            w.push(&name);
            w.push("(");
            for (i, a) in args.iter().enumerate() {
                w.push_sep(i, ", ");
                render_expr(w, a, cfg);
            }
            w.push(")");
        }
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            w.push("CASE");
            if let Some(op) = operand {
                w.push(" ");
                render_expr(w, op, cfg);
            }
            for (wcond, wval) in when_then {
                w.push(" WHEN ");
                render_expr(w, wcond, cfg);
                w.push(" THEN ");
                render_expr(w, wval, cfg);
            }
            if let Some(e) = else_expr.as_deref() {
                w.push(" ELSE ");
                render_expr(w, e, cfg);
            }
            w.push(" END");
        }
        Expr::Cast { expr, ty } => {
            w.push("CAST(");
            render_expr(w, expr, cfg);
            w.push(" AS ");
            w.push(ty);
            w.push(")");
        }

        Expr::Collate { expr, collation } => {
            render_expr(w, expr, cfg);
            w.push(" COLLATE ");
            w.push(&quote_ident(collation, cfg)); // аккуратно: можно и без кавычек если хочешь
        }

        Expr::WindowFunc { name, args, window } => {
            w.push(&name);
            w.push("(");
            for (i, a) in args.iter().enumerate() {
                w.push_sep(i, ", ");
                render_expr(w, a, cfg);
            }
            w.push(") OVER (");
            if !window.partition_by.is_empty() {
                w.push("PARTITION BY ");
                for (i, e) in window.partition_by.iter().enumerate() {
                    w.push_sep(i, ", ");
                    render_expr(w, e, cfg);
                }
                if !window.order_by.is_empty() {
                    w.push(" ");
                }
            }
            if !window.order_by.is_empty() {
                w.push("ORDER BY ");
                for (i, oi) in window.order_by.iter().enumerate() {
                    w.push_sep(i, ", ");
                    render_expr(w, &oi.expr, cfg);
                    match oi.dir {
                        OrderDirection::Asc => w.push(" ASC"),
                        OrderDirection::Desc => w.push(" DESC"),
                    }
                }
            }
            w.push(")");
        }
    }
}

fn render_paren_if_needed(w: &mut SqlWriter, e: &Expr, cfg: &SqlRenderCfg) {
    match e {
        Expr::Binary { .. } | Expr::Unary { .. } => {
            w.push("(");
            render_expr(w, e, cfg);
            w.push(")");
        }
        _ => render_expr(w, e, cfg),
    }
}

#[inline]
fn push_alias(w: &mut SqlWriter, alias: &str, cfg: &SqlRenderCfg, emit_as: bool) {
    if emit_as {
        w.push(" AS ");
    } else {
        w.push(" ");
    }
    w.push(&quote_ident(alias, cfg));
}
