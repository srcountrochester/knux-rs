#[derive(Clone, Debug, Default)]
pub struct OptimizeConfig {
    // Консервативные
    pub rm_subquery_order_by: bool,
    pub simplify_exists: bool,

    // Агрессивные
    pub predicate_pushdown: bool,
    pub flatten_simple_subqueries: bool,
    pub dedup_in_list: bool,

    // Включается только вручную
    pub in_to_exists: bool,
}

#[derive(Clone, Debug)]
pub struct OptimizeConfigBuilder {
    cfg: OptimizeConfig,
}

impl Default for OptimizeConfigBuilder {
    fn default() -> Self {
        Self {
            cfg: OptimizeConfig::default(),
        }
    }
}

impl From<OptimizeConfig> for OptimizeConfigBuilder {
    fn from(cfg: OptimizeConfig) -> Self {
        Self { cfg }
    }
}

impl OptimizeConfigBuilder {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }
    #[inline]
    pub fn build(self) -> OptimizeConfig {
        self.cfg
    }

    // Варианты преднастроек
    pub fn none(mut self) -> Self {
        self.cfg = OptimizeConfig::default();
        self
    }
    pub fn conservative(mut self) -> Self {
        self.cfg.rm_subquery_order_by = true;
        self.cfg.simplify_exists = true;

        self.cfg.predicate_pushdown = false;
        self.cfg.flatten_simple_subqueries = false;
        self.cfg.dedup_in_list = false;
        self.cfg.in_to_exists = false;
        self
    }
    pub fn aggressive(mut self) -> Self {
        // conservative + aggressive; in_to_exists — отдельно
        self.cfg.rm_subquery_order_by = true;
        self.cfg.simplify_exists = true;

        self.cfg.predicate_pushdown = true;
        self.cfg.flatten_simple_subqueries = true;
        self.cfg.dedup_in_list = true;

        self.cfg.in_to_exists = false;
        self
    }
    pub fn all(mut self) -> Self {
        self.cfg.rm_subquery_order_by = true;
        self.cfg.simplify_exists = true;
        self.cfg.predicate_pushdown = true;
        self.cfg.flatten_simple_subqueries = true;
        self.cfg.dedup_in_list = true;
        self.cfg.in_to_exists = true;
        self
    }

    // with_*/without_* флаги
    pub fn with_rm_subquery_order_by(mut self) -> Self {
        self.cfg.rm_subquery_order_by = true;
        self
    }
    pub fn without_rm_subquery_order_by(mut self) -> Self {
        self.cfg.rm_subquery_order_by = false;
        self
    }

    pub fn with_simplify_exists(mut self) -> Self {
        self.cfg.simplify_exists = true;
        self
    }
    pub fn without_simplify_exists(mut self) -> Self {
        self.cfg.simplify_exists = false;
        self
    }

    pub fn with_predicate_pushdown(mut self) -> Self {
        self.cfg.predicate_pushdown = true;
        self
    }
    pub fn without_predicate_pushdown(mut self) -> Self {
        self.cfg.predicate_pushdown = false;
        self
    }

    pub fn with_flatten_simple_subqueries(mut self) -> Self {
        self.cfg.flatten_simple_subqueries = true;
        self
    }
    pub fn without_flatten_simple_subqueries(mut self) -> Self {
        self.cfg.flatten_simple_subqueries = false;
        self
    }

    pub fn with_dedup_in_list(mut self) -> Self {
        self.cfg.dedup_in_list = true;
        self
    }
    pub fn without_dedup_in_list(mut self) -> Self {
        self.cfg.dedup_in_list = false;
        self
    }

    pub fn with_in_to_exists(mut self) -> Self {
        self.cfg.in_to_exists = true;
        self
    }
    pub fn without_in_to_exists(mut self) -> Self {
        self.cfg.in_to_exists = false;
        self
    }
}

// Удобные алиасы
impl OptimizeConfig {
    #[inline]
    pub fn builder() -> OptimizeConfigBuilder {
        OptimizeConfigBuilder::default()
    }
}
