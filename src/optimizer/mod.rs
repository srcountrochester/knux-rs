mod __tests__;
pub mod config;
mod passes;

pub use config::{OptimizeConfig, OptimizeConfigBuilder};
use sqlparser::ast as S;

use crate::QueryBuilder;
use crate::QueryExecutor;

impl<'a, T> QueryBuilder<'a, T> {
    /// Жёстко задать конфигурацию оптимизаций для конкретного запроса.
    #[inline]
    pub fn with_optimize(mut self, cfg: crate::optimizer::OptimizeConfig) -> Self {
        self.optimize_cfg = cfg;
        self
    }

    /// Локально скорректировать конфиг оптимизаций через билдер.
    #[inline]
    pub fn optimize<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut crate::optimizer::OptimizeConfigBuilder),
    {
        let mut b = crate::optimizer::OptimizeConfigBuilder::from(self.optimize_cfg.clone());
        f(&mut b);
        self.optimize_cfg = b.build();
        self
    }

    /// Внутреннее чтение — пригодится на шаге 3.
    #[inline]
    pub(crate) fn optimize_cfg(&self) -> &crate::optimizer::OptimizeConfig {
        &self.optimize_cfg
    }

    pub(crate) fn optimize_ast(&self, stmt: &mut S::Statement) {
        if self.optimize_cfg.rm_subquery_order_by {
            passes::rm_subquery_order_by(stmt);
        }
        if self.optimize_cfg.simplify_exists {
            passes::simplify_exists(stmt);
        }
        if self.optimize_cfg.flatten_simple_subqueries {
            passes::flatten_simple_subqueries(stmt);
        }
        if self.optimize_cfg.predicate_pushdown {
            passes::predicate_pushdown(stmt);
        }
        if self.optimize_cfg.dedup_in_list {
            passes::dedup_in_list(stmt);
        }
        if self.optimize_cfg.in_to_exists {
            passes::in_to_exists(stmt);
        }
    }
}

impl QueryExecutor {
    /// Установить конфиг оптимизаций целиком.
    #[inline]
    pub fn with_optimize(mut self, cfg: OptimizeConfig) -> Self {
        self.optimize_cfg = cfg;
        self
    }

    /// Точечная настройка через билдер (мутирующая).
    #[inline]
    pub fn optimize<F>(&mut self, f: F) -> &mut Self
    where
        F: FnOnce(&mut OptimizeConfigBuilder),
    {
        let mut b = OptimizeConfigBuilder::from(self.optimize_cfg.clone());
        f(&mut b);
        self.optimize_cfg = b.build();
        self
    }

    /// Внутренний доступ — для QueryBuilder.
    #[inline]
    pub(crate) fn base_optimize_cfg(&self) -> &OptimizeConfig {
        &self.optimize_cfg
    }
}

pub fn apply(stmt: &mut S::Statement, cfg: &OptimizeConfig) {
    if is_noop(cfg) {
        return;
    }

    // --- Консервативные ---
    if cfg.rm_subquery_order_by {
        passes::rm_subquery_order_by(stmt);
    }
    if cfg.simplify_exists {
        passes::simplify_exists(stmt);
    }

    // --- Агрессивные ---
    if cfg.predicate_pushdown {
        passes::predicate_pushdown(stmt);
    }
    if cfg.flatten_simple_subqueries {
        passes::flatten_simple_subqueries(stmt);
    }
    if cfg.dedup_in_list {
        passes::dedup_in_list(stmt);
    }

    // --- Вручную ---
    if cfg.in_to_exists {
        passes::in_to_exists(stmt);
    }
}

/// Удобный вход для SELECT: прямой вызов на &mut Query без визуальной обёртки.
pub fn apply_query(q: &mut S::Query, cfg: &OptimizeConfig) {
    if is_noop(cfg) {
        return;
    }

    // На старте делегируем в apply(), чтобы переиспользовать общие проходы.
    // В будущем можно переписать на прямые проходы по Query.
    let mut stmt = S::Statement::Query(Box::new(q.clone()));
    apply(&mut stmt, cfg);
    if let S::Statement::Query(new_q) = stmt {
        *q = *new_q;
    }
}

#[inline]
fn is_noop(cfg: &OptimizeConfig) -> bool {
    !(cfg.rm_subquery_order_by
        || cfg.simplify_exists
        || cfg.predicate_pushdown
        || cfg.flatten_simple_subqueries
        || cfg.dedup_in_list
        || cfg.in_to_exists)
}
