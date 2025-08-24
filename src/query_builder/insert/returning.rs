use sqlparser::ast::{SelectItem, SelectItemQualifiedWildcardKind, WildcardAdditionalOptions};

use super::core_fn::InsertBuilder;
use crate::{query_builder::args::ArgList, utils::parse_object_name};

impl<'a, T> InsertBuilder<'a, T> {
    /// RETURNING <expr, ...>
    pub fn returning<L>(mut self, items: L) -> Self
    where
        L: ArgList<'a>,
    {
        let list = items.into_vec();
        if list.is_empty() {
            self.push_builder_error("returning(): empty list");
            return self;
        }
        for a in list {
            match a.try_into_expr() {
                Ok((expr, _p)) => self.returning.push(SelectItem::UnnamedExpr(expr)),
                Err(e) => self.push_builder_error(format!("returning(): {e}")),
            }
        }
        self
    }

    /// RETURNING ровно одного выражения. Перезаписывает ранее заданный returning.
    pub fn returning_one<L>(mut self, item: L) -> Self
    where
        L: ArgList<'a>,
    {
        let mut args = item.into_vec();
        if args.is_empty() {
            self.push_builder_error("returning_one(): expected a single expression");
            return self;
        }
        if args.len() != 1 {
            self.push_builder_error(format!(
                "returning_one(): expected 1 item, got {}",
                args.len()
            ));
            // берём первый корректный, чтобы не срывать пайплайн
        }
        // Берём первый и пытаемся сконвертировать в Expr
        match args.swap_remove(0).try_into_expr() {
            Ok((expr, _)) => {
                self.returning.clear();
                self.returning.push(SelectItem::UnnamedExpr(expr));
            }
            Err(e) => self.push_builder_error(format!("returning_one(): {e}")),
        }
        self
    }

    /// RETURNING * — вернуть все колонки вставленных строк.
    /// (Поддерживается PG и SQLite; для MySQL рендер позже аккуратно отключит/свалидирует)
    pub fn returning_all(mut self) -> Self {
        self.returning.clear();
        self.returning
            .push(SelectItem::Wildcard(WildcardAdditionalOptions::default()));
        self
    }

    /// RETURNING <qualifier>.*
    pub fn returning_all_from(mut self, qualifier: &str) -> Self {
        self.returning.clear();

        // поддерживаем alias.* и schema.table.* (разбиваем по '.')
        let obj = parse_object_name(qualifier);

        let kind = SelectItemQualifiedWildcardKind::ObjectName(obj);
        self.returning.push(SelectItem::QualifiedWildcard(
            kind,
            WildcardAdditionalOptions::default(),
        ));

        self
    }
}
