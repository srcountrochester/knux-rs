use smallvec::SmallVec;
use sqlparser::ast::JoinConstraint;

use crate::expression::Expression;
use crate::param::Param;
use crate::query_builder::QueryBuilder;

pub fn clone_params(e: &Expression) -> SmallVec<[crate::param::Param; 4]> {
    let mut out = SmallVec::new();
    for p in &e.params {
        out.push(p.clone());
    }
    out
}

/// Требует наличие JoinConstraint; если None — регистрирует ошибку и возвращает None.
pub fn must_have_constraint(
    ctx: &str,
    c: Option<JoinConstraint>,
    qb: &mut QueryBuilder,
) -> Option<JoinConstraint> {
    if c.is_none() {
        qb.push_builder_error(format!("{ctx}: требуется ON-условие"));
    }
    c
}

#[inline]
pub fn clone_params_from_expr(e: &crate::expression::Expression) -> SmallVec<[Param; 4]> {
    let mut out = SmallVec::new();
    for p in &e.params {
        out.push(p.clone());
    }
    out
}
