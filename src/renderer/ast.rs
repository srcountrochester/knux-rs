#[derive(Clone, Debug, PartialEq)]
pub enum GroupByModifier {
    Rollup,
    Cube,
    Totals,
    GroupingSets(Expr),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CteMaterialized {
    Materialized,
    NotMaterialized,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Select {
    pub distinct: bool,
    pub distinct_on: Vec<Expr>,
    pub items: Vec<SelectItem>,
    pub from: Option<TableRef>,
    pub joins: Vec<Join>,
    pub r#where: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub group_by_modifiers: Vec<GroupByModifier>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct WildcardOpts {
    pub ilike: Option<String>,
    pub exclude_raw: Option<String>,
    pub except_raw: Option<String>,
    pub replace_raw: Option<String>,
    pub rename_raw: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SelectItem {
    Star {
        opts: Option<WildcardOpts>,
    },
    QualifiedStar {
        table: String,
        opts: Option<WildcardOpts>,
    },
    Expr {
        expr: Expr,
        alias: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum TableRef {
    Named {
        schema: Option<String>,
        name: String,
        alias: Option<String>,
    },
    Subquery {
        query: Box<Select>,
        alias: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq)]
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

#[derive(Clone, Debug, PartialEq)]
pub struct Join {
    pub kind: JoinKind,
    pub table: TableRef,
    pub on: Option<Expr>, // для CROSS on=None
    pub using_cols: Option<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OrderItem {
    pub expr: Expr,
    pub dir: OrderDirection,
    pub nulls_last: bool, // только для диалектов где применимо
}

/// Упрощённые выражения — достаточно для 90% CRUD
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Raw(String),
    Ident {
        path: Vec<String>,
    }, // ["schema","table","col"] или ["table","col"] или ["col"]
    Bind, // плейсхолдер параметра
    String(String),
    Number(String),
    Bool(bool),
    Null,
    Star,
    Tuple(Vec<Expr>),
    Unary {
        op: UnOp,
        expr: Box<Expr>,
    },
    Binary {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
    },
    Paren(Box<Expr>),
    FuncCall {
        name: String,
        args: Vec<Expr>,
    },
    Like {
        not: bool,
        ilike: bool,          // true → ILIKE (только PG), иначе LIKE
        expr: Box<Expr>,      // слева
        pattern: Box<Expr>,   // справа
        escape: Option<char>, // ESCAPE '\'
    },
    Cast {
        expr: Box<Expr>,
        ty: String,
    },
    Collate {
        expr: Box<Expr>,
        collation: String,
    },
    WindowFunc {
        name: String,
        args: Vec<Expr>,
        window: WindowSpec,
    },
    Case {
        operand: Option<Box<Expr>>,
        when_then: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum BinOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Like,
    NotLike,
    Ilike,
    NotIlike,
    In,
    NotIn,
    Is,
    IsNot,
}

#[derive(Clone, Debug, PartialEq)]
pub enum UnOp {
    Not,
    Neg,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SetOp {
    Union,
    UnionAll,
    Intersect,
    IntersectAll,
    Except,
    ExceptAll,
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryBody {
    Select(Select),
    Set {
        left: Box<QueryBody>,
        op: SetOp,
        right: Box<QueryBody>,
        by_name: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct With {
    pub recursive: bool,
    pub ctes: Vec<Cte>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Cte {
    pub name: String,
    pub columns: Vec<String>,
    pub from: Option<String>,
    pub materialized: Option<CteMaterialized>,
    pub query: Box<QueryBody>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub with: Option<With>,
    pub body: QueryBody,
    pub order_by: Vec<OrderItem>, // ORDER BY ... на уровне всего запроса
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderItem>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Stmt {
    Query(Query),
    Insert(Insert),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Insert {
    pub table: TableRef, // ожидаем Named { .. }, alias не обязателен (MySQL alias "new" проставим в рендере при необходимости)
    pub columns: Vec<String>, // пусто ⇒ вставка "по всем"
    pub rows: Vec<Vec<Expr>>, // VALUES(...) [, (...)]
    pub ignore: bool, // MySQL: INSERT IGNORE; SQLite: INSERT OR IGNORE; PG: через ON CONFLICT DO NOTHING
    pub on_conflict: Option<OnConflict>,
    pub returning: Vec<SelectItem>, // PG/SQLite
}

#[derive(Clone, Debug, PartialEq)]
pub struct OnConflict {
    pub target_columns: Vec<String>, // может быть пустым (см. SQLite last clause)
    pub on_constraint: Option<String>, // для PG/SQLite: ON CONSTRAINT <name>
    pub action: Option<OnConflictAction>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum OnConflictAction {
    DoNothing,
    DoUpdate {
        set: Vec<Assign>, // SET col = expr / EXCLUDED.col / new.col
        where_predicate: Option<Expr>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct Assign {
    pub col: String,
    pub value: Expr,
    pub from_inserted: bool, // true → PG/SQLite: EXCLUDED.col, MySQL: new.col
}
