use super::Expression;
use sqlparser::ast;

impl Expression {
    /// Префиксует текущий идентификатор схемой: `users` -> `public.users`
    pub fn schema(mut self, name: &str) -> Self {
        let ident = ast::Ident::new(name);
        self.expr = match self.expr {
            ast::Expr::Identifier(first) => ast::Expr::CompoundIdentifier(vec![ident, first]),
            ast::Expr::CompoundIdentifier(mut parts) => {
                parts.insert(0, ident);
                ast::Expr::CompoundIdentifier(parts)
            }
            _ => {
                // Нестандартный случай: начинаем путь со схемы
                ast::Expr::CompoundIdentifier(vec![ast::Ident::new(name)])
            }
        };
        self
    }

    /// Добавляет сегмент таблицы к пути: `public` -> `public.users`
    pub fn table(mut self, name: &str) -> Self {
        let ident = ast::Ident::new(name);
        self.expr = match self.expr {
            ast::Expr::Identifier(first) => ast::Expr::CompoundIdentifier(vec![first, ident]),
            ast::Expr::CompoundIdentifier(mut parts) => {
                parts.push(ident);
                ast::Expr::CompoundIdentifier(parts)
            }
            _ => ast::Expr::Identifier(ident),
        };
        self
    }

    /// Добавляет сегмент колонки: `public.users` -> `public.users.id`
    pub fn col(mut self, name: &str) -> Self {
        // разбиваем "a.b.c" на идентификаторы, игнорируем пустые сегменты
        let mut segs: Vec<ast::Ident> = name
            .split('.')
            .filter(|s| !s.is_empty())
            .map(|s| ast::Ident::new(s.trim()))
            .collect();

        if segs.is_empty() {
            return self; // нечего добавлять
        }

        self.expr = match self.expr {
            // уже есть одиночный идентификатор (например, "users")
            ast::Expr::Identifier(first) => {
                let mut parts = Vec::with_capacity(1 + segs.len());
                parts.push(first);
                parts.append(&mut segs);
                ast::Expr::CompoundIdentifier(parts)
            }

            // уже есть составной путь (например, "auth"."users")
            ast::Expr::CompoundIdentifier(mut parts) => {
                parts.append(&mut segs);
                ast::Expr::CompoundIdentifier(parts)
            }

            // если база не идентификатор — начинаем путь с переданных сегментов
            _ => {
                if segs.len() == 1 {
                    ast::Expr::Identifier(segs.pop().unwrap())
                } else {
                    ast::Expr::CompoundIdentifier(segs)
                }
            }
        };

        self
    }
}
