use std::collections::HashMap;

use crate::db::sql::parser::{DataType, Expression};

/// Represents different types of SQL tokens
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Select,
    Insert,
    Update,
    Delete,
    From,
    Where,
    Join,
    Inner,
    Left,
    Right,
    Full,
    Outer,
    On,
    Group,
    By,
    Order,
    Having,
    Limit,
    Offset,
    Into,
    Values,
    Set,
    Create,
    Table,
    Database,
    Index,
    Drop,
    Alter,
    Add,
    Column,
    Primary,
    Key,
    Foreign,
    References,
    Unique,
    Not,
    Null,
    Auto,
    Increment,
    Default,
    Check,
    Union,
    All,
    Distinct,
    As,
    In,
    Exists,
    Between,
    Like,
    Is,
    And,
    Or,
    Case,
    When,
    Then,
    Else,
    End,
    If,
    Begin,
    Commit,
    Rollback,
    Transaction,

    // Data types
    Integer,
    Varchar,
    Text,
    Boolean,
    Float,
    Double,
    Date,
    DateTime,
    Timestamp,

    // Operators
    Equals,
    NotEquals,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,

    // Literals and identifiers
    Identifier(String),
    StringLiteral(String),
    NumberLiteral(String),
    BooleanLiteral(bool),

    // Punctuation
    LeftParen,
    RightParen,
    Comma,
    Semicolon,
    Dot,
    Star,

    // Special
    Eof,
    Whitespace,
    Comment(String),
}

/// Represents parsing errors with detailed information
#[derive(Debug, Clone)]
pub struct ParseError {
    pub message: String,
    pub position: usize,
    pub line: usize,
    pub column: usize,
}

/// Literal values
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    String(String),
    Number(String),
    Boolean(bool),
    Null,
}

/// Binary operators
#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOperator {
    Equals,
    NotEquals,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    And,
    Or,
    Like,
    In,
    Between,
}

/// Unary operators
#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    Not,
    Minus,
    Plus,
}

/// Join types
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// Order direction
#[derive(Debug, Clone, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

/// Column definition for CREATE TABLE
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<ColumnConstraint>,
}

/// Column constraints
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraint {
    NotNull,
    PrimaryKey,
    Unique,
    ForeignKey {
        references_table: String,
        references_column: String,
    },
    Default(Expression),
    Check(Expression),
    AutoIncrement,
}

/// SQL statements AST
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select {
        projection: Vec<Expression>,
        from: Option<TableReference>,
        joins: Vec<Join>,
        where_clause: Option<Expression>,
        group_by: Vec<Expression>,
        having: Option<Expression>,
        order_by: Vec<OrderBy>,
        limit: Option<u64>,
        offset: Option<u64>,
        distinct: bool,
    },
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expression>>,
    },
    Update {
        table: String,
        assignments: Vec<Assignment>,
        where_clause: Option<Expression>,
    },
    Delete {
        table: String,
        where_clause: Option<Expression>,
    },
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
        if_not_exists: bool,
    },
    CreateDatabase {
        name: String,
        if_not_exists: bool,
    },
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<String>,
        unique: bool,
        if_not_exists: bool,
    },
    DropTable {
        name: String,
        if_exists: bool,
    },
    DropDatabase {
        name: String,
        if_exists: bool,
    },
    AlterTable {
        name: String,
        action: AlterAction,
    },
    Union {
        left: Box<Statement>,
        right: Box<Statement>,
        all: bool,
    },
    Transaction(TransactionStatement),
}

/// Table references (can be table name or subquery)
#[derive(Debug, Clone, PartialEq)]
pub enum TableReference {
    Table {
        name: String,
        alias: Option<String>,
    },
    Subquery {
        query: Box<Statement>,
        alias: String,
    },
}

/// Join clause
#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub join_type: JoinType,
    pub table: TableReference,
    pub condition: Option<Expression>,
}

/// Order by clause
#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    pub expression: Expression,
    pub direction: OrderDirection,
}

/// Assignment for UPDATE statements
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Expression,
}

/// Table constraints
#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    PrimaryKey(Vec<String>),
    ForeignKey {
        columns: Vec<String>,
        references_table: String,
        references_columns: Vec<String>,
    },
    Unique(Vec<String>),
    Check(Expression),
}

/// ALTER TABLE actions
#[derive(Debug, Clone, PartialEq)]
pub enum AlterAction {
    AddColumn(ColumnDef),
    DropColumn(String),
    AddConstraint(TableConstraint),
    DropConstraint(String),
}

/// Transaction statements
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionStatement {
    Begin,
    Commit,
    Rollback,
}

/// Tokenizer for SQL input
pub struct Tokenizer {
    pub input: Vec<char>,
    pub position: usize,
    pub line: usize,
    pub column: usize,
    pub keywords: HashMap<String, Token>,
}
