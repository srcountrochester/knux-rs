use smallvec::SmallVec;

use crate::param::Param;
use crate::query_builder::QueryBuilder;
use crate::query_builder::args::{ArgList, QBArg};

impl QueryBuilder {
    /// Добавляет выражения в GROUP BY.
    ///
    /// Поддерживаются:
    /// - строковые литералы (интерпретируются как `col("...")`)
    /// - выражения из модуля `expression`
    /// - ❌ подзапросы и замыкания для GROUP BY не поддерживаются (регистрируется ошибка билдера)
    pub fn group_by<A>(mut self, args: A) -> Self
    where
        A: ArgList,
    {
        let items: Vec<QBArg> = args.into_vec();
        if items.is_empty() {
            return self;
        }

        for it in items {
            match it {
                QBArg::Expr(e) => {
                    let sql_expr = e.expr;
                    let mut params: SmallVec<[Param; 8]> = e.params;
                    self.group_by_items.push(sql_expr);
                    self.params.append(&mut params);
                }
                QBArg::Subquery(_) | QBArg::Closure(_) => {
                    self.push_builder_error(
                        "group_by(): подзапросы/замыкания в GROUP BY не поддерживаются",
                    );
                }
            }
        }

        self
    }
}
