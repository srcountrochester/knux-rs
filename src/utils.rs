use sqlparser::ast::{Expr as SqlExpr, Ident, ObjectName, ObjectNamePart, Value, ValueWithSpan};

#[inline]
pub(crate) fn num_expr(n: u64) -> SqlExpr {
    SqlExpr::Value(Value::Number(n.to_string(), false).into())
}

#[inline]
pub(crate) fn num_value(n: u64) -> ValueWithSpan {
    Value::Number(n.to_string(), false).into()
}

/// Convert a `ValueWithSpan` into a `Value` (dropping the span)
#[inline]
pub(crate) fn strip_span(val: &ValueWithSpan) -> Value {
    val.value.clone()
}

/// Wrap a `Value` into a `ValueWithSpan` with an empty span
#[inline]
pub(crate) fn attach_empty_span(val: Value) -> ValueWithSpan {
    val.with_empty_span()
}

/// Convert a `Value` (AST literal) to its string representation
#[inline]
pub(crate) fn value_to_string(val: &Value) -> String {
    val.to_string()
}

/// Convert an `Expr` (AST expression) to its string representation
#[inline]
pub(crate) fn expr_to_string(expr: &SqlExpr) -> String {
    expr.to_string()
}

/// Parse a table name (possibly with schema prefix) into an `ObjectName`
#[inline]
pub(crate) fn parse_object_name(name: &str) -> ObjectName {
    ObjectName(
        name.split('.')
            .map(|v| ObjectNamePart::Identifier(v.into()))
            .collect::<Vec<_>>(),
    )
}

/// Если table без схемы — префиксует default_schema (если задана)
#[inline]
pub(crate) fn object_name_from_default(default_schema: Option<&str>, table: &str) -> ObjectName {
    if table.contains('.') {
        parse_object_name(table)
    } else if let Some(schema) = default_schema {
        ObjectName::from(vec![Ident::new(schema), Ident::new(table)])
    } else {
        ObjectName::from(vec![Ident::new(table)])
    }
}

/// Преобразует выражение идентификатора в ObjectName c учётом default_schema:
/// - Identifier("t")         -> [default_schema?,"t"]
/// - CompoundIdentifier(a.b) -> ["a","b"]
#[inline]
pub(crate) fn expr_to_object_name(
    expr: SqlExpr,
    default_schema: Option<&str>,
) -> Option<ObjectName> {
    match expr {
        SqlExpr::Identifier(ident) => {
            let mut parts = Vec::with_capacity(2);
            if let Some(s) = default_schema {
                parts.push(ObjectNamePart::Identifier(Ident::new(s)));
            }
            parts.push(ObjectNamePart::Identifier(ident));
            Some(ObjectName(parts))
        }
        SqlExpr::CompoundIdentifier(idents) => Some(ObjectName(
            idents.into_iter().map(ObjectNamePart::Identifier).collect(),
        )),
        _ => None,
    }
}
