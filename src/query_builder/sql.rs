use crate::{
    param::Param,
    query_builder::{QueryBuilder, Result},
};

impl QueryBuilder {
    pub fn to_sql(self) -> Result<(String, Vec<Param>)> {
        let (query, params) = self.build_query_ast()?;
        Ok((query.to_string(), params.to_vec()))
    }
}
