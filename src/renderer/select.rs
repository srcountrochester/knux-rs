use super::ast::*;
use super::config::{Dialect, SqlRenderCfg};
use super::ident::{quote_ident, quote_path};
use super::writer::SqlWriter;

pub fn render_select(sel: &Select, cfg: &SqlRenderCfg, cap: usize) -> String {
    let mut w = SqlWriter::new(cap, cfg.placeholders);

    w.push("SELECT ");
    if !sel.distinct_on.is_empty() {
        // DISTINCT ON поддерживается только в Postgres
        match cfg.dialect {
            Dialect::Postgres => {
                w.push("DISTINCT ON (");
                for (i, e) in sel.distinct_on.iter().enumerate() {
                    if i > 0 {
                        w.push(", ");
                    }
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
            if i > 0 {
                w.push(", ");
            }
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
                        if i > 0 {
                            w.push(", ");
                        }
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
                        if i > 0 {
                            w.push(", ");
                        }
                        render_expr(&mut w, e, cfg);
                    }
                    w.push(")");
                } else {
                    w.push(" GROUP BY ");
                    for (i, e) in sel.group_by.iter().enumerate() {
                        if i > 0 {
                            w.push(", ");
                        }
                        render_expr(&mut w, e, cfg);
                    }
                }
                // Totals — не стандарт PG; игнорируем
            }
            Dialect::MySQL => {
                // MySQL поддерживает только WITH ROLLUP
                w.push(" GROUP BY ");
                for (i, e) in sel.group_by.iter().enumerate() {
                    if i > 0 {
                        w.push(", ");
                    }
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
                    if i > 0 {
                        w.push(", ");
                    }
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
            if i > 0 {
                w.push(", ");
            }
            render_expr(&mut w, &oi.expr, cfg);
            match oi.dir {
                OrderDirection::Asc => w.push(" ASC"),
                OrderDirection::Desc => w.push(" DESC"),
            }
            // NULLS LAST только в PG; в MySQL/SQLite рендерить не будем
            if matches!(cfg.dialect, Dialect::Postgres) && oi.nulls_last {
                w.push(" NULLS LAST");
            }
        }
    }

    if let Some(l) = sel.limit {
        w.push(" LIMIT ");
        w.push(l.to_string());
    }
    if let Some(o) = sel.offset {
        w.push(" OFFSET ");
        w.push(o.to_string());
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
                w.push(" AS ");
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
                w.push(&quote_path([s.as_str(), name.as_str()], cfg));
            } else {
                w.push(&quote_ident(name, cfg));
            }
            if let Some(a) = alias {
                w.push(" AS ");
                w.push(&quote_ident(a, cfg));
            }
        }
        TableRef::Subquery { query, alias } => {
            w.push("(");
            let inner = render_select(query, cfg, 128);
            w.push(inner);
            w.push(")");
            if let Some(a) = alias {
                w.push(" AS ");
                w.push(&quote_ident(a, cfg));
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
    }
    render_table_ref(w, &j.table, cfg);
    if !matches!(j.kind, JoinKind::Cross) {
        if let Some(on) = &j.on {
            w.push(" ON ");
            render_expr(w, on, cfg);
        }
    }
}

fn render_expr(w: &mut SqlWriter, e: &Expr, cfg: &SqlRenderCfg) {
    match e {
        Expr::Ident { path } => {
            let parts: Vec<&str> = path.iter().map(|s| s.as_str()).collect();
            w.push(&quote_path(parts, cfg));
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
                if i > 0 {
                    w.push(", ");
                }
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
