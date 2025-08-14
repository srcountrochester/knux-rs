#[derive(Clone, Debug)]
pub enum GroupByModifier {
    Rollup,
    Cube,
    Totals,
    GroupingSets(Expr),
}

#[derive(Clone, Debug)]
pub struct Select {
    pub distinct: bool,
    pub distinct_on: Vec<Expr>,
    pub items: Vec<SelectItem>,
    pub from: Option<TableRef>,
    pub joins: Vec<Join>, // упрощённый вид
    pub r#where: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub group_by_modifiers: Vec<GroupByModifier>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct WildcardOpts {
    pub ilike: Option<String>,
    pub exclude_raw: Option<String>,
    pub except_raw: Option<String>,
    pub replace_raw: Option<String>,
    pub rename_raw: Option<String>,
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Clone, Debug)]
pub struct Join {
    pub kind: JoinKind,
    pub table: TableRef,
    pub on: Option<Expr>, // для CROSS on=None
}

#[derive(Clone, Debug)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Clone, Debug)]
pub struct OrderItem {
    pub expr: Expr,
    pub dir: OrderDirection,
    pub nulls_last: bool, // только для диалектов где применимо
}

/// Упрощённые выражения — достаточно для 90% CRUD
#[derive(Clone, Debug)]
pub enum Expr {
    Ident {
        path: Vec<String>,
    }, // ["schema","table","col"] или ["table","col"] или ["col"]
    Bind, // плейсхолдер параметра
    String(String),
    Number(String),
    Bool(bool),
    Null,
    Binary {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
    },
    Unary {
        op: UnOp,
        expr: Box<Expr>,
    },
    Paren(Box<Expr>),
    FuncCall {
        name: String,
        args: Vec<Expr>,
    },
    Case {
        operand: Option<Box<Expr>>,
        when_then: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub enum UnOp {
    Not,
    Neg,
}
