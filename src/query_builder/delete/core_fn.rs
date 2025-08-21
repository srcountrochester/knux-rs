use crate::param::Param;
use crate::query_builder::QueryBuilder;
use crate::query_builder::args::{ArgList, QBArg};
use crate::query_builder::ast::FromItem;
use crate::renderer::Dialect;
use crate::utils::expr_to_object_name;
use smallvec::{SmallVec, smallvec};
use sqlparser::ast::{Expr as SqlExpr, ObjectName, SelectItem};

/// Билдер DELETE FROM ... [USING ...] [WHERE ...] [RETURNING ...]
#[derive(Debug)]
pub struct DeleteBuilder {
    pub(crate) table: Option<ObjectName>,
    pub(crate) using_items: SmallVec<[FromItem; 2]>,
    pub(crate) where_predicate: Option<SqlExpr>,
    pub(crate) returning: SmallVec<[SelectItem; 4]>,
    pub(crate) params: SmallVec<[Param; 8]>,

    // ошибки сбора
    pub(crate) builder_errors: SmallVec<[std::borrow::Cow<'static, str>; 2]>,

    // контекст
    pub(crate) default_schema: Option<String>,
    pub(crate) dialect: Dialect,
}

impl DeleteBuilder {
    #[inline]
    pub(crate) fn from_qb(qb: QueryBuilder) -> Self {
        Self {
            table: None,
            using_items: smallvec![],
            where_predicate: None,
            returning: smallvec![],
            params: qb.params,
            builder_errors: smallvec![],
            default_schema: qb.default_schema,
            dialect: qb.dialect,
        }
    }

    /// USING <tables...> — дополнительные таблицы (PG/MySQL)
    pub fn using<L>(mut self, items: L) -> Self
    where
        L: ArgList,
    {
        let args = items.into_vec();
        self.using_items.reserve(args.len());

        for arg in args {
            match arg {
                // Имя таблицы из Expr (col/ident/строка)
                QBArg::Expr(e) => {
                    let mut p = e.params;
                    if !p.is_empty() {
                        self.params.append(&mut p);
                    }
                    if let Some(name) = expr_to_object_name(e.expr, self.default_schema.as_deref())
                    {
                        self.using_items.push(FromItem::TableName(name));
                    } else {
                        self.push_builder_error("using(): invalid table reference");
                    }
                }
                // Подзапрос: поддержка в будущем (пока запретим — как и в update.from())
                QBArg::Subquery(_) | QBArg::Closure(_) => {
                    self.push_builder_error("using(): subqueries are not supported yet");
                }
            }
        }
        self
    }

    /// WHERE <expr>[, <expr2>, ...] — элементы связываются AND
    pub fn r#where<A>(mut self, args: A) -> Self
    where
        A: ArgList,
    {
        match self.resolve_where_group(args) {
            Ok(Some((expr, params))) => self.attach_where_with_and(expr, params),
            Ok(None) => {}
            Err(msg) => self.push_builder_error(msg),
        }
        self
    }

    /// RETURNING <expr, ...> (PG/SQLite; в MySQL будет проигнорировано на рендере)
    pub fn returning<L>(mut self, items: L) -> Self
    where
        L: ArgList,
    {
        if let Err(msg) = super::returning::push_returning_list(&mut self.returning, items) {
            self.push_builder_error(msg);
        }
        self
    }

    /// RETURNING один элемент, перезаписывает предыдущий список
    pub fn returning_one<L>(mut self, item: L) -> Self
    where
        L: ArgList,
    {
        if let Err(msg) = super::returning::set_returning_one(&mut self.returning, item) {
            self.push_builder_error(msg);
        }
        self
    }

    /// RETURNING *
    pub fn returning_all(mut self) -> Self {
        super::returning::set_returning_all(&mut self.returning);
        self
    }

    /// RETURNING <qualifier>.*
    pub fn returning_all_from(mut self, qualifier: &str) -> Self {
        super::returning::set_returning_all_from(&mut self.returning, qualifier);
        self
    }

    // ===== helpers =====

    #[inline]
    fn attach_where_with_and(&mut self, expr: SqlExpr, mut params: SmallVec<[Param; 8]>) {
        if let Some(prev) = self.where_predicate.take() {
            self.where_predicate = Some(SqlExpr::BinaryOp {
                left: Box::new(prev),
                op: sqlparser::ast::BinaryOperator::And,
                right: Box::new(expr),
            });
        } else {
            self.where_predicate = Some(expr);
        }
        if !params.is_empty() {
            self.params.extend(params.drain(..));
        }
    }

    /// Собрать WHERE-группу из ArgList (разрешая подзапросы)
    fn resolve_where_group<A>(
        &mut self,
        args: A,
    ) -> Result<Option<(SqlExpr, SmallVec<[Param; 8]>)>, std::borrow::Cow<'static, str>>
    where
        A: ArgList,
    {
        let items = args.into_vec();
        if items.is_empty() {
            return Ok(None);
        }

        let mut exprs: SmallVec<[SqlExpr; 4]> = SmallVec::with_capacity(items.len());
        let mut params: SmallVec<[Param; 8]> = SmallVec::new();

        for it in items {
            match it.resolve_into_expr_with(|qb| qb.build_query_ast()) {
                Ok((e, p)) => {
                    exprs.push(e);
                    if !p.is_empty() {
                        params.extend(p);
                    }
                }
                Err(e) => return Err(format!("where(): {e}").into()),
            }
        }

        // Склеиваем через AND
        let mut it = exprs.into_iter();
        let Some(mut acc) = it.next() else {
            return Ok(None);
        };
        for e in it {
            acc = SqlExpr::BinaryOp {
                left: Box::new(acc),
                op: sqlparser::ast::BinaryOperator::And,
                right: Box::new(e),
            };
        }
        Ok(Some((acc, params)))
    }

    #[inline]
    pub(crate) fn push_builder_error<S: Into<std::borrow::Cow<'static, str>>>(&mut self, msg: S) {
        self.builder_errors.push(msg.into());
    }
}

impl QueryBuilder {
    /// Начать DELETE с указанием таблицы (поддерживает выражения: table("t").schema("s"))
    pub fn delete<L>(self, table_arg: L) -> DeleteBuilder
    where
        L: ArgList,
    {
        let mut b = DeleteBuilder::from_qb(self);

        let mut args = table_arg.into_vec();
        if args.is_empty() {
            b.push_builder_error("delete(): table is not set");
            return b;
        }
        if args.len() > 1 {
            b.push_builder_error("delete(): expected a single table argument");
        }

        // Берём первый аргумент и пробуем интерпретировать как имя таблицы
        match args.swap_remove(0).try_into_expr() {
            Ok((expr, _params)) => {
                if let Some(obj) = expr_to_object_name(expr, b.default_schema.as_deref()) {
                    b.table = Some(obj);
                } else {
                    b.push_builder_error(
                        "delete(): invalid table reference; expected identifier or schema.table",
                    );
                }
            }
            Err(e) => b.push_builder_error(format!("delete(): {e}")),
        }

        b
    }
}
