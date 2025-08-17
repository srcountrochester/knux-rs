use sqlparser::ast::{Expr as SqlExpr, Value, ValueWithSpan};

#[inline]
pub(crate) fn num_expr(n: u64) -> SqlExpr {
    SqlExpr::Value(Value::Number(n.to_string(), false).into())
}

#[inline]
pub(crate) fn num_value(n: u64) -> ValueWithSpan {
    Value::Number(n.to_string(), false).into()
}
