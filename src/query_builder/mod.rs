use std::borrow::Cow;

use crate::{executor::DbPool, param::Param, renderer::Dialect};
use smallvec::{SmallVec, smallvec};

mod __tests__;
mod alias;
mod args;
mod ast;
mod clear;
mod distinct;
mod error;
mod from;
mod group_by;
mod having;
pub mod insert;
mod join;
mod limit;
mod order_by;
mod schema;
mod select;
mod sql;
mod union;
mod where_clause;
mod with;

use ast::FromItem;
use distinct::DistinctOnNode;
pub use error::{BuilderErrorList, Error, Result};
use group_by::GroupByNode;
use having::HavingNode;
pub use insert::InsertBuilder;
use join::JoinNode;
use order_by::OrderByNode;
use select::SelectItemNode;
use union::SetOpNode;
use where_clause::WhereNode;
use with::WithItemNode;

pub use join::on;

#[cfg(feature = "postgres")]
const DEFAULT_DIALECT: Dialect = Dialect::Postgres;
#[cfg(feature = "mysql")]
const DEFAULT_DIALECT: Dialect = Dialect::MySQL;
#[cfg(feature = "sqlite")]
const DEFAULT_DIALECT: Dialect = Dialect::SQLite;

#[derive(Debug)]
pub struct QueryBuilder {
    pub pool: Option<DbPool>,
    pub(self) select_items: SmallVec<[SelectItemNode; 4]>,
    pub(self) from_items: SmallVec<[FromItem; 1]>,
    pub(self) where_clause: Option<WhereNode>,
    pub params: SmallVec<[Param; 8]>,
    pub default_schema: Option<String>,
    pub(crate) pending_schema: Option<String>,
    pub alias: Option<String>,
    pub(crate) dialect: Dialect,
    builder_errors: SmallVec<[Cow<'static, str>; 2]>,
    pub(self) from_joins: SmallVec<[SmallVec<[JoinNode; 2]>; 1]>,
    pub(self) group_by_items: SmallVec<[GroupByNode; 4]>,
    pub(self) order_by_items: SmallVec<[OrderByNode; 4]>,
    pub(self) limit_num: Option<u64>,
    pub(self) offset_num: Option<u64>,
    pub(self) having_clause: Option<HavingNode>,
    pub(self) select_distinct: bool,
    pub(self) distinct_on_items: SmallVec<[DistinctOnNode; 2]>,
    pub(self) with_items: SmallVec<[WithItemNode; 1]>,
    pub(self) with_recursive: bool,
    pub(self) set_ops: SmallVec<[SetOpNode; 1]>,
}

impl QueryBuilder {
    pub fn new(pool: DbPool, schema: Option<String>) -> Self {
        Self {
            pool: Some(pool),
            select_items: smallvec![],
            from_items: smallvec![],
            where_clause: None,
            params: smallvec![],
            builder_errors: smallvec![],
            default_schema: schema,
            pending_schema: None,
            from_joins: smallvec![],
            group_by_items: smallvec![],
            order_by_items: smallvec![],
            having_clause: None,
            alias: None,
            dialect: DEFAULT_DIALECT,
            limit_num: None,
            offset_num: None,
            select_distinct: false,
            distinct_on_items: smallvec![],
            with_items: smallvec![],
            with_recursive: false,
            set_ops: smallvec![],
        }
    }

    /// Пустой QueryBuilder без пула — удобно для замыканий |qb| qb.select(...)
    pub fn new_empty() -> Self {
        Self {
            pool: None,
            select_items: smallvec![],
            from_items: smallvec![],
            where_clause: None,
            params: smallvec![],
            builder_errors: smallvec![],
            from_joins: smallvec![],
            default_schema: None,
            pending_schema: None,
            group_by_items: smallvec![],
            order_by_items: smallvec![],
            having_clause: None,
            alias: None,
            dialect: DEFAULT_DIALECT,
            limit_num: None,
            offset_num: None,
            select_distinct: false,
            distinct_on_items: smallvec![],
            with_items: smallvec![],
            with_recursive: false,
            set_ops: smallvec![],
        }
    }

    #[inline]
    pub fn with_default_schema(mut self, schema: Option<String>) -> Self {
        self.default_schema = schema;
        self
    }

    #[inline]
    pub fn with_estimated_select_capacity(mut self, cap: usize) -> Self {
        self.select_items.reserve(cap);
        self
    }

    #[inline]
    pub fn with_estimated_from_capacity(mut self, cap: usize) -> Self {
        self.from_items.reserve(cap);
        self
    }

    #[inline]
    pub fn with_estimated_param_capacity(mut self, cap: usize) -> Self {
        self.params.reserve(cap);
        self
    }

    #[inline]
    /// Очищает накопленные параметры
    pub fn clear_params(&mut self) -> &mut Self {
        self.params.clear();
        self
    }

    #[inline]
    pub(crate) fn push_builder_error<S: Into<Cow<'static, str>>>(&mut self, msg: S) {
        self.builder_errors.push(msg.into());
    }

    /// Быстрая проверка наличия ошибок билдера.
    #[inline]
    pub fn has_builder_errors(&self) -> bool {
        !self.builder_errors.is_empty()
    }

    #[inline]
    pub fn dialect(mut self, dialect: Dialect) -> Self {
        self.dialect = dialect;
        self
    }

    #[inline]
    fn take_builder_error_list(&mut self) -> Option<BuilderErrorList> {
        if self.builder_errors.is_empty() {
            None
        } else {
            // Передаём SmallVec в From — он сконвертит в BuilderErrorList
            Some(BuilderErrorList::from(std::mem::take(
                &mut self.builder_errors,
            )))
        }
    }

    #[inline]
    fn extend_params<I>(&mut self, it: I)
    where
        I: IntoIterator<Item = crate::param::Param>,
    {
        self.params.extend(it);
    }

    #[inline]
    fn is_mysql(&self) -> bool {
        self.dialect == crate::renderer::Dialect::MySQL
    }
}

impl Default for QueryBuilder {
    fn default() -> Self {
        Self::new_empty()
    }
}
