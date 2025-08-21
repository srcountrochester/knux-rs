use std::borrow::Cow;

use crate::renderer::{ast as R, map::map_to_render_ast};
use sqlparser::ast::{
    self as S, Expr as SExpr, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
    ObjectName, OrderBy, OrderByExpr, OrderByKind, UnaryOperator as SUnOp, Value, ValueWithSpan,
    WildcardAdditionalOptions,
};

// Общие помощники для имён
pub(crate) fn object_name_to_strings(obj: &S::ObjectName) -> Vec<String> {
    obj.0.iter().map(|p| p.to_string()).collect()
}

pub(crate) fn split_object_name(obj: &ObjectName) -> (Option<String>, String) {
    let mut parts: Vec<String> = obj.0.iter().map(|p| p.to_string()).collect();
    if parts.len() >= 2 {
        let name = parts.pop().unwrap();
        let schema = parts.pop();
        (schema, name)
    } else {
        (None, parts.pop().unwrap_or_default())
    }
}

// Литерал-число для LIMIT/OFFSET
pub(crate) fn literal_u64(e: &SExpr) -> Option<u64> {
    match e {
        SExpr::Value(v) => match &v.value {
            Value::Number(s, _) => s.parse::<u64>().ok(),
            _ => None,
        },
        SExpr::UnaryOp {
            op: SUnOp::Plus,
            expr,
        } => literal_u64(expr),
        SExpr::UnaryOp {
            op: SUnOp::Minus, ..
        } => None,
        _ => None,
    }
}

// SELECT item helpers
pub(crate) fn map_wildcard_opts(o: &WildcardAdditionalOptions) -> Option<R::WildcardOpts> {
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

pub(crate) fn map_select_item(it: &S::SelectItem) -> R::SelectItem {
    match it {
        S::SelectItem::Wildcard(opts) => R::SelectItem::Star {
            opts: map_wildcard_opts(opts),
        },
        S::SelectItem::QualifiedWildcard(kind, opts) => {
            let mut s = kind.to_string();
            if let Some(prefix) = s.strip_suffix(".*") {
                s = prefix.to_string();
            }
            R::SelectItem::QualifiedStar {
                table: s,
                opts: map_wildcard_opts(opts),
            }
        }
        S::SelectItem::ExprWithAlias { expr, alias } => R::SelectItem::Expr {
            expr: map_expr(expr),
            alias: Some(alias.value.clone()),
        },
        S::SelectItem::UnnamedExpr(expr) => R::SelectItem::Expr {
            expr: map_expr(expr),
            alias: None,
        },
    }
}

// ORDER BY helpers
pub(crate) fn map_order_by(ob: &OrderBy) -> Vec<R::OrderItem> {
    match &ob.kind {
        OrderByKind::Expressions(list) => list.iter().map(map_order_by_expr).collect(),
        OrderByKind::All(_) => Vec::new(),
    }
}
pub(crate) fn map_order_by_expr(obe: &OrderByExpr) -> R::OrderItem {
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

// Expr → R::Expr (общая, т.к. нужна и select, и insert)
pub(crate) fn map_expr(e: &SExpr) -> R::Expr {
    use R::Expr as E;
    match e {
        SExpr::Identifier(id) => E::Ident {
            path: vec![id.value.clone()],
        },
        SExpr::CompoundIdentifier(ids) => E::Ident {
            path: ids.iter().map(|i| i.value.clone()).collect(),
        },
        SExpr::Value(v) => map_value_with_span(v),

        SExpr::UnaryOp { op, expr } => E::Unary {
            op: map_un_op(op),
            expr: Box::new(map_expr(expr)),
        },
        SExpr::BinaryOp { left, op, right } => E::Binary {
            left: Box::new(map_expr(left)),
            op: map_bin_op(op),
            right: Box::new(map_expr(right)),
        },

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
            right: Box::new(R::Expr::Tuple(list.iter().map(map_expr).collect())),
        },

        SExpr::Like {
            negated,
            expr,
            pattern,
            escape_char,
            ..
        } => R::Expr::Like {
            not: *negated,
            ilike: false,
            expr: Box::new(map_expr(expr)),
            pattern: Box::new(map_expr(pattern)),
            escape: escape_char
                .as_ref()
                .map_or(None, |v| v.to_string().chars().next()),
        },
        SExpr::ILike {
            negated,
            expr,
            pattern,
            escape_char,
            ..
        } => R::Expr::Like {
            not: *negated,
            ilike: true,
            expr: Box::new(map_expr(expr)),
            pattern: Box::new(map_expr(pattern)),
            escape: escape_char
                .as_ref()
                .map_or(None, |v| v.to_string().chars().next()),
        },

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

        SExpr::Nested(inner) => E::Paren(Box::new(map_expr(inner))),

        SExpr::Function(Function {
            name, args, over, ..
        }) if over.is_none() => E::FuncCall {
            name: name.to_string(),
            args: map_function_arguments(args),
        },

        SExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let when_then = conditions
                .iter()
                .map(
                    |S::CaseWhen {
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

        SExpr::Cast {
            expr, data_type, ..
        } => R::Expr::Cast {
            expr: Box::new(map_expr(expr)),
            ty: data_type.to_string(),
        },
        SExpr::Collate { expr, collation } => R::Expr::Collate {
            expr: Box::new(map_expr(expr)),
            collation: collation.to_string(),
        },

        SExpr::Function(Function {
            name,
            args,
            over: Some(ow),
            ..
        }) => match ow {
            S::WindowType::WindowSpec(S::WindowSpec {
                partition_by,
                order_by,
                ..
            }) => {
                let part = partition_by.iter().map(map_expr).collect::<Vec<_>>();
                let ob = order_by.iter().map(map_order_by_expr).collect();
                R::Expr::WindowFunc {
                    name: name.to_string(),
                    args: map_function_arguments(args),
                    window: R::WindowSpec {
                        partition_by: part,
                        order_by: ob,
                    },
                }
            }
            S::WindowType::NamedWindow(_name) => R::Expr::WindowFunc {
                name: name.to_string(),
                args: map_function_arguments(args),
                window: R::WindowSpec {
                    partition_by: vec![],
                    order_by: vec![],
                },
            },
        },

        other => E::Raw(other.to_string()),
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
        Value::Placeholder(_) => R::Expr::Bind,
        other => R::Expr::Ident {
            path: vec![other.to_string()],
        },
    }
}

fn map_function_arguments(args: &FunctionArguments) -> Vec<R::Expr> {
    match args {
        FunctionArguments::None => Vec::new(),
        FunctionArguments::List(list) => list.args.iter().map(map_func_arg).collect(),
        FunctionArguments::Subquery(q) => vec![R::Expr::Ident {
            path: vec![q.to_string()],
        }],
        _ => Vec::new(),
    }
}

fn map_func_arg(a: &FunctionArg) -> R::Expr {
    match a {
        FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => match arg {
            FunctionArgExpr::Expr(e) => map_expr(e),
            FunctionArgExpr::Wildcard => R::Expr::Star,
            FunctionArgExpr::QualifiedWildcard(obj) => R::Expr::Ident {
                path: vec![obj.to_string(), "*".into()],
            },
        },
        FunctionArg::Unnamed(inner) => match inner {
            FunctionArgExpr::Expr(e) => map_expr(e),
            FunctionArgExpr::Wildcard => R::Expr::Star,
            FunctionArgExpr::QualifiedWildcard(obj) => R::Expr::Ident {
                path: vec![obj.to_string(), "*".into()],
            },
        },
    }
}

// binop/unop
#[inline]
pub(crate) fn map_bin_op(op: &S::BinaryOperator) -> R::BinOp {
    use R::BinOp as B;
    match op {
        S::BinaryOperator::Eq => B::Eq,
        S::BinaryOperator::NotEq => B::Neq,
        S::BinaryOperator::Lt => B::Lt,
        S::BinaryOperator::LtEq => B::Lte,
        S::BinaryOperator::Gt => B::Gt,
        S::BinaryOperator::GtEq => B::Gte,
        S::BinaryOperator::Plus => B::Add,
        S::BinaryOperator::Minus => B::Sub,
        S::BinaryOperator::Multiply => B::Mul,
        S::BinaryOperator::Divide => B::Div,
        S::BinaryOperator::Modulo => B::Mod,
        S::BinaryOperator::And => B::And,
        S::BinaryOperator::Or => B::Or,
        S::BinaryOperator::PGLikeMatch => B::Like,
        S::BinaryOperator::PGNotLikeMatch => B::NotLike,
        S::BinaryOperator::PGILikeMatch => B::Ilike,
        S::BinaryOperator::PGNotILikeMatch => B::NotIlike,
        _ => B::Eq,
    }
}
#[inline]
pub(crate) fn map_un_op(op: &S::UnaryOperator) -> R::UnOp {
    match op {
        S::UnaryOperator::Not => R::UnOp::Not,
        S::UnaryOperator::Minus => R::UnOp::Neg,
        _ => R::UnOp::Neg,
    }
}

/// Соединяет части ObjectName через `sep`, без промежуточных аллокаций.
#[inline]
pub(crate) fn object_name_join(obj: &S::ObjectName, sep: &str) -> String {
    let parts = &obj.0;
    if parts.is_empty() {
        return String::new();
    }
    let mut out = String::new();
    for (i, p) in parts.iter().enumerate() {
        if i > 0 {
            out.push_str(sep);
        }
        out.push_str(&p.to_string());
    }
    out
}

#[inline]
fn part_as_cow<'a>(p: &'a S::ObjectNamePart) -> Cow<'a, str> {
    use S::ObjectNamePart::*;
    match p {
        Identifier(id) => Cow::Borrowed(id.value.as_str()),
        _ => Cow::Owned(p.to_string()),
    }
}

/// Возвращает (schema, name) через Cow:
///  - для Identifier — без аллокаций,
///  - для Function — String.
#[inline]
pub(crate) fn split_object_name_cow<'a>(
    obj: &'a S::ObjectName,
) -> (Option<Cow<'a, str>>, Cow<'a, str>) {
    match obj.0.as_slice() {
        [] => (None, Cow::Borrowed("")),
        [only] => (None, part_as_cow(only)),
        [schema, name] => (Some(part_as_cow(schema)), part_as_cow(name)),
        parts => {
            let last = parts.len() - 1;
            (
                Some(part_as_cow(&parts[last - 1])),
                part_as_cow(&parts[last]),
            )
        }
    }
}

/// TableFactor -> TableRef (разрешает и базовые таблицы, и подзапросы)
pub(crate) fn map_table_factor_any(tf: &S::TableFactor) -> R::TableRef {
    match tf {
        S::TableFactor::Table { name, alias, .. } => {
            let (schema_cow, name_cow) = split_object_name_cow(name);
            R::TableRef::Named {
                schema: schema_cow.map(|c| c.into_owned()),
                name: name_cow.into_owned(),
                alias: alias.as_ref().map(|a| a.name.value.clone()),
            }
        }
        S::TableFactor::Derived {
            subquery, alias, ..
        } => {
            let inner = map_to_render_ast(subquery);
            R::TableRef::Subquery {
                query: Box::new(inner),
                alias: alias.as_ref().map(|a| a.name.value.clone()),
            }
        }
        other => R::TableRef::Named {
            schema: None,
            name: other.to_string(),
            alias: None,
        },
    }
}

/// Только базовая таблица; прочие варианты — panic (удобно для UPDATE FROM)
pub(crate) fn map_table_factor_named(tf: &S::TableFactor) -> R::TableRef {
    match tf {
        S::TableFactor::Table { name, alias, .. } => {
            let (schema_cow, name_cow) = split_object_name_cow(name);
            R::TableRef::Named {
                schema: schema_cow.map(|c| c.into_owned()),
                name: name_cow.into_owned(),
                alias: alias.as_ref().map(|a| a.name.value.clone()),
            }
        }
        _ => panic!("unsupported table factor (expected base table)"),
    }
}
