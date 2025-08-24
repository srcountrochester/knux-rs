mod delete;
mod insert;
mod query;
mod update;

use crate::query_builder::{
    args::QBClosure,
    insert::{Assignment, MergeValue},
};
use sqlparser::ast::{self as S, ObjectName};

use super::QueryBuilder;

#[derive(Debug, Clone)]
pub enum FromItem<'a, T = ()> {
    TableName(ObjectName),
    Subquery(Box<QueryBuilder<'a>>),
    SubqueryClosure(QBClosure<T>),
}

#[inline]
pub(crate) fn make_update_assignments_pg_sqlite(set: &[Assignment]) -> Vec<S::Assignment> {
    set.iter()
        .map(|a| {
            let target = S::AssignmentTarget::ColumnName(S::ObjectName::from(vec![a.col.clone()]));
            let value = match &a.value {
                MergeValue::Expr(e) => e.clone(),
                MergeValue::FromInserted(id) => {
                    S::Expr::CompoundIdentifier(vec![S::Ident::new("EXCLUDED"), id.clone()])
                }
            };
            S::Assignment { target, value }
        })
        .collect()
}

#[inline]
pub(crate) fn make_update_assignments_mysql(set: &[Assignment]) -> Vec<S::Assignment> {
    set.iter()
        .map(|a| {
            let target = S::AssignmentTarget::ColumnName(S::ObjectName::from(vec![a.col.clone()]));
            let value = match &a.value {
                MergeValue::Expr(e) => e.clone(),
                MergeValue::FromInserted(id) => {
                    // В рендере INSERT мы добавим "AS new", здесь ссылаемся на new.col
                    S::Expr::CompoundIdentifier(vec![S::Ident::new("new"), id.clone()])
                }
            };
            S::Assignment { target, value }
        })
        .collect()
}
