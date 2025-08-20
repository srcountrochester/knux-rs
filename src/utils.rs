use sqlparser::ast::{Expr as SqlExpr, ObjectName, ObjectNamePart, Value, ValueWithSpan};

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
