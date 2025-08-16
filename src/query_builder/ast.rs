use crate::{param::Param, query_builder::args::QBClosure};
use smallvec::SmallVec;
use sqlparser::ast::{
    GroupByExpr, Ident, Join, ObjectName, Query, Select, SelectFlavor, SelectItem, SetExpr,
    TableAlias, TableFactor, TableWithJoins, helpers::attached_token::AttachedToken,
};

use super::{BuilderErrorList, Error, QueryBuilder, Result};

#[derive(Debug)]
pub enum FromItem {
    TableName(ObjectName),
    Subquery(Box<QueryBuilder>),
    SubqueryClosure(QBClosure),
}

impl QueryBuilder {
    pub(crate) fn build_query_ast(mut self) -> Result<(Query, Vec<Param>)> {
        if let Some(list) = self.take_builder_error_list() {
            return Err(Error::BuilderErrors(list));
        }

        // проекция по умолчанию: SELECT *
        let projection = if self.select_items.is_empty() {
            let mut sv = SmallVec::new();
            sv.push(SelectItem::Wildcard(Default::default()));
            sv
        } else {
            self.select_items
        };

        let mut params = self.params.into_vec();
        // FROM: либо один TableWithJoins, либо пусто
        let mut from: Vec<TableWithJoins> = Vec::with_capacity(self.from_items.len());

        for (i, item) in self.from_items.into_iter().enumerate() {
            // достаём joins для этого FROM (или пустой вектор)
            let joins_vec: Vec<Join> = if i < self.from_joins.len() {
                // забираем владение: превращаем SmallVec в Vec
                let sv = std::mem::take(&mut self.from_joins[i]);
                sv.into_vec()
            } else {
                Vec::new()
            };

            match item {
                FromItem::TableName(name) => from.push(TableWithJoins {
                    joins: joins_vec,
                    relation: TableFactor::Table {
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
                }),
                FromItem::Subquery(qb) => {
                    let alias = qb.alias.clone();
                    let (q, mut p) = qb.build_query_ast()?;
                    if !p.is_empty() {
                        params.append(&mut p);
                    }
                    from.push(TableWithJoins {
                        joins: joins_vec,
                        relation: TableFactor::Derived {
                            lateral: false,
                            subquery: Box::new(q),
                            alias: alias.map(|a| TableAlias {
                                name: Ident::new(a),
                                columns: vec![],
                            }),
                        },
                    })
                }
                FromItem::SubqueryClosure(closure) => {
                    let built = closure.apply(QueryBuilder::new_empty());
                    let alias = built.alias.clone();
                    let (q, mut p) = built.build_query_ast()?;
                    if !p.is_empty() {
                        params.append(&mut p);
                    }
                    from.push(TableWithJoins {
                        joins: joins_vec,
                        relation: TableFactor::Derived {
                            lateral: false,
                            subquery: Box::new(q),
                            alias: alias.map(|a| TableAlias {
                                name: Ident::new(a),
                                columns: vec![],
                            }),
                        },
                    })
                }
            }
        }

        let selection = self.where_clause;

        let select = Select {
            distinct: None,
            top: None,
            projection: projection.into_vec(),
            into: None,
            from,
            lateral_views: vec![],
            selection,
            group_by: GroupByExpr::Expressions(vec![], vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            named_window: vec![],
            qualify: None,
            connect_by: None,
            exclude: None,
            prewhere: None,
            value_table_mode: None,
            top_before_distinct: false,
            window_before_qualify: false,
            flavor: SelectFlavor::Standard,
            select_token: AttachedToken::empty(),
        };

        let query = Query {
            with: None,
            body: Box::new(SetExpr::Select(Box::new(select))),
            order_by: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
            format_clause: None,
            limit_clause: None,
            pipe_operators: vec![],
            settings: None,
        };

        if !self.builder_errors.is_empty() {
            return Err(Error::BuilderErrors(BuilderErrorList::from(
                std::mem::take(&mut self.builder_errors),
            )));
        }

        Ok((query, params))
    }
}
