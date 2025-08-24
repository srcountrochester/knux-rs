use smallvec::SmallVec;
use sqlparser::ast::{
    Expr as SqlExpr, Ident, Join, JoinConstraint, JoinOperator, ObjectName, ObjectNamePart,
    TableAlias, TableFactor,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::{
    ast::{SetExpr, Statement, Value},
    parser::Parser,
};

use super::super::{Error, Result};
use crate::expression::JoinOnBuilder;
use crate::param::Param;
use crate::query_builder::join::JoinOnArg;
use crate::query_builder::join::utils::{
    clone_params, clone_params_from_expr, must_have_constraint,
};
use crate::query_builder::{
    QueryBuilder,
    args::{IntoQBArg, QBArg},
};
use crate::renderer::Dialect;

#[derive(Clone, Copy, Debug)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    NaturalInner,
    NaturalLeft,
    NaturalRight,
    NaturalFull,
}

#[derive(Debug, Clone)]
pub(crate) struct JoinNode {
    pub join: Join,
    pub params: SmallVec<[Param; 8]>,
}

impl JoinNode {
    #[inline]
    pub fn new(join: Join, params: SmallVec<[Param; 8]>) -> Self {
        Self { join, params }
    }
}

impl<'a, K> QueryBuilder<'a, K> {
    pub(super) fn push_join_internal<T, O>(mut self, kind: JoinKind, target: T, on: O) -> Self
    where
        T: IntoQBArg<'a>,
        O: Into<JoinOnArg>,
    {
        // 0) Запреты по диалектам (SQLite не умеет RIGHT/FULL)
        if self.is_sqlite() {
            match kind {
                JoinKind::Right | JoinKind::Full => {
                    self.push_builder_error("SQLite не поддерживает RIGHT/FULL JOIN");
                    return self;
                }
                _ => {}
            }
        }

        // 1) Цель JOIN -> TableFactor
        let qbarg: QBArg = target.into_qb_arg();
        let (relation, mut collect_params) = match self.resolve_join_target(qbarg) {
            Ok(v) => v,
            Err(e) => {
                self.push_builder_error(e.to_string());
                return self;
            }
        };

        // Нужен ли резолв ON?
        let needs_on = matches!(
            kind,
            JoinKind::Inner | JoinKind::Left | JoinKind::Right | JoinKind::Full
        );

        // 2) ON-условие
        let (constraint_opt, mut on_params) = if needs_on {
            match self.resolve_join_on(on.into()) {
                Ok(v) => v,
                Err(e) => {
                    self.push_builder_error(e.to_string());
                    return self;
                }
            }
        } else {
            (None, SmallVec::new())
        };

        // 3) Оператор
        let join_operator = match kind {
            JoinKind::Inner => {
                // Требуем ON/USING/NATURAL. Если нет — зафиксируем ошибку и подставим ON TRUE
                let c = must_have_constraint("INNER JOIN", constraint_opt, &mut self)
                    .unwrap_or_else(|| {
                        JoinConstraint::On(SqlExpr::Value(Value::Boolean(true).into()))
                    });
                JoinOperator::Inner(c)
            }
            JoinKind::Left => {
                let c = must_have_constraint("LEFT JOIN", constraint_opt, &mut self)
                    .unwrap_or_else(|| {
                        JoinConstraint::On(SqlExpr::Value(Value::Boolean(true).into()))
                    });
                JoinOperator::LeftOuter(c)
            }
            JoinKind::Right => {
                let c = must_have_constraint("RIGHT JOIN", constraint_opt, &mut self)
                    .unwrap_or_else(|| {
                        JoinConstraint::On(SqlExpr::Value(Value::Boolean(true).into()))
                    });
                JoinOperator::RightOuter(c)
            }
            JoinKind::Full => {
                let c = must_have_constraint("FULL JOIN", constraint_opt, &mut self)
                    .unwrap_or_else(|| {
                        JoinConstraint::On(SqlExpr::Value(Value::Boolean(true).into()))
                    });
                JoinOperator::FullOuter(c)
            }
            JoinKind::Cross => JoinOperator::CrossJoin,

            // NATURAL-варианты: всегда JoinConstraint::Natural
            JoinKind::NaturalInner => JoinOperator::Inner(JoinConstraint::Natural),
            JoinKind::NaturalLeft => JoinOperator::LeftOuter(JoinConstraint::Natural),
            JoinKind::NaturalRight => JoinOperator::RightOuter(JoinConstraint::Natural),
            JoinKind::NaturalFull => JoinOperator::FullOuter(JoinConstraint::Natural),
        };

        // 4) Кладём JOIN к последнему FROM
        if self.from_items.is_empty() {
            self.push_builder_error(
                "join(): отсутствует источник FROM — вызови .from(...) перед .join(...)",
            );
            return self;
        }
        self.ensure_joins_slots();
        let last_idx = self.from_items.len() - 1;

        let join = Join {
            relation,
            global: false,
            join_operator,
        };

        let mut node_params: SmallVec<[Param; 8]> = SmallVec::new();
        node_params.append(&mut collect_params);
        node_params.append(&mut on_params);

        self.from_joins[last_idx].push(JoinNode::new(join, node_params));

        self
    }

    /// Превращает цель JOIN (QBArg) в TableFactor + собирает параметры (если были).
    pub(super) fn resolve_join_target(
        &self,
        arg: QBArg,
    ) -> Result<(TableFactor, SmallVec<[Param; 4]>)> {
        match arg {
            QBArg::Expr(e) => {
                // Разрешаем только идентификаторы: Identifier/CompoundIdentifier
                match &e.expr {
                    SqlExpr::Identifier(id) => {
                        let name = ObjectName(vec![ObjectNamePart::Identifier(id.clone())]);
                        Ok((
                            TableFactor::Table {
                                name,
                                alias: None,
                                args: None,
                                with_hints: vec![],
                                partitions: vec![],
                                version: None,
                                index_hints: vec![],
                                json_path: None,
                                sample: None,
                                with_ordinality: false,
                            },
                            clone_params_from_expr(&e),
                        ))
                    }
                    SqlExpr::CompoundIdentifier(parts) => {
                        let name = ObjectName(
                            parts
                                .iter()
                                .cloned()
                                .map(ObjectNamePart::Identifier)
                                .collect(),
                        );
                        Ok((
                            TableFactor::Table {
                                name,
                                alias: None,
                                args: None,
                                with_hints: vec![],
                                partitions: vec![],
                                version: None,
                                index_hints: vec![],
                                json_path: None,
                                sample: None,
                                with_ordinality: false,
                            },
                            clone_params_from_expr(&e),
                        ))
                    }
                    other => Err(Error::InvalidExpression {
                        reason: format!(
                            "join(): target must be a table/schema identifier, got {:?}",
                            other
                        )
                        .into(),
                    }),
                }
            }

            QBArg::Subquery(qb) => {
                // Собираем подзапрос и переносим алиас из qb.alias (если задан)
                let alias = qb.alias.clone();
                let (q, params) = qb.build_query_ast()?;
                Ok((
                    TableFactor::Derived {
                        lateral: false,
                        subquery: Box::new(q),
                        alias: alias.map(|a| TableAlias {
                            name: Ident::new(a),
                            columns: vec![],
                        }),
                    },
                    params.into(),
                ))
            }

            QBArg::Closure(c) => {
                // Выполняем замыкание на пустом билдере, берём его alias
                let built = c.apply(QueryBuilder::new_empty());
                let alias = built.alias.clone();
                let (q, params) = built.build_query_ast()?;
                Ok((
                    TableFactor::Derived {
                        lateral: false,
                        subquery: Box::new(q),
                        alias: alias.map(|a| TableAlias {
                            name: Ident::new(a),
                            columns: vec![],
                        }),
                    },
                    params.into(),
                ))
            }
        }
    }

    /// Строит JoinConstraint из JoinOnArg; возвращает также собранные параметры.
    fn resolve_join_on(
        &self,
        arg: JoinOnArg,
    ) -> Result<(Option<JoinConstraint>, SmallVec<[crate::param::Param; 4]>)> {
        match arg {
            JoinOnArg::None => Ok((None, SmallVec::new())),
            JoinOnArg::Expr(e) => {
                let params = clone_params(&e);
                Ok((Some(JoinConstraint::On(e.expr)), params))
            }
            // Строка -> парсим через SELECT 1 WHERE <s>
            JoinOnArg::Raw(s) => {
                // Парсим ON-строку как выражение. Берём GenericDialect — он покрывает простые сравнения.
                let dialect = GenericDialect {};
                let sql = format!("SELECT 1 WHERE {}", s);
                let stmts =
                    Parser::parse_sql(&dialect, &sql).map_err(|e| Error::InvalidExpression {
                        reason: format!("join(): не удалось распарсить ON-условие: {e}").into(),
                    })?;
                let stmt = stmts
                    .into_iter()
                    .next()
                    .ok_or_else(|| Error::InvalidExpression {
                        reason: "join(): пустой результат парсинга".into(),
                    })?;

                let select = match stmt {
                    Statement::Query(q) => q,
                    other => {
                        return Err(Error::InvalidExpression {
                            reason: format!("join(): ожидался SELECT, получено {:?}", other).into(),
                        });
                    }
                };
                let body = *select.body;
                let where_expr = match body {
                    SetExpr::Select(boxed) => {
                        boxed.selection.ok_or_else(|| Error::InvalidExpression {
                            reason: "join(): не найдено WHERE-условие при парсинге".into(),
                        })?
                    }
                    _ => {
                        return Err(Error::InvalidExpression {
                            reason: "join(): неожиданный SetExpr при парсинге ON".into(),
                        });
                    }
                };
                Ok((Some(JoinConstraint::On(where_expr)), SmallVec::new()))
            }
            JoinOnArg::Builder(f) => {
                let chain = f(JoinOnBuilder::default());
                match chain.build() {
                    Some(expr) => {
                        let params = clone_params(&expr);
                        let ast = expr.expr;
                        Ok((Some(JoinConstraint::On(ast)), params))
                    }
                    None => Ok((None, SmallVec::new())),
                }
            }
        }
    }

    #[inline]
    fn is_sqlite(&self) -> bool {
        self.dialect == Dialect::SQLite
    }

    #[inline]
    fn ensure_joins_slots(&mut self) {
        if self.from_joins.len() < self.from_items.len() {
            let need = self.from_items.len() - self.from_joins.len();
            self.from_joins.reserve(need);
            for _ in 0..need {
                self.from_joins.push(SmallVec::new());
            }
        }
    }
}
