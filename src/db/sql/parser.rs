use super::constants::*;
use std::collections::HashMap;
use std::fmt;

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Parse error at line {}, column {}: {}",
            self.line, self.column, self.message
        )
    }
}

impl std::error::Error for ParseError {}

/// SQL data types
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Integer,
    Varchar(Option<u32>),
    Text,
    Boolean,
    Float,
    Double,
    Date,
    DateTime,
    Timestamp,
}

/// SQL expressions (values, operations, functions)
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Literal(Literal),
    Identifier(String),
    BinaryOp {
        left: Box<Expression>,
        operator: BinaryOperator,
        right: Box<Expression>,
    },
    UnaryOp {
        operator: UnaryOperator,
        operand: Box<Expression>,
    },
    Function {
        name: String,
        args: Vec<Expression>,
    },
    Case {
        when_clauses: Vec<(Expression, Expression)>,
        else_clause: Option<Box<Expression>>,
    },
    Subquery(Box<Statement>),
    QualifiedColumn {
        table: String,
        column: String,
    },
    /// Expression with an alias (e.g., COUNT(x) AS count)
    Alias {
        expr: Box<Expression>,
        alias: String,
    },
}
impl Tokenizer {
    /// Create a new tokenizer with the given input
    pub fn new(input: &str) -> Self {
        let mut keywords = HashMap::new();

        // Populate keywords map for O(1) lookup
        let keyword_pairs = [
            ("SELECT", Token::Select),
            ("INSERT", Token::Insert),
            ("UPDATE", Token::Update),
            ("DELETE", Token::Delete),
            ("FROM", Token::From),
            ("WHERE", Token::Where),
            ("JOIN", Token::Join),
            ("INNER", Token::Inner),
            ("LEFT", Token::Left),
            ("RIGHT", Token::Right),
            ("FULL", Token::Full),
            ("OUTER", Token::Outer),
            ("ON", Token::On),
            ("GROUP", Token::Group),
            ("BY", Token::By),
            ("ORDER", Token::Order),
            ("HAVING", Token::Having),
            ("LIMIT", Token::Limit),
            ("OFFSET", Token::Offset),
            ("INTO", Token::Into),
            ("VALUES", Token::Values),
            ("SET", Token::Set),
            ("CREATE", Token::Create),
            ("TABLE", Token::Table),
            ("DATABASE", Token::Database),
            ("INDEX", Token::Index),
            ("DROP", Token::Drop),
            ("ALTER", Token::Alter),
            ("ADD", Token::Add),
            ("COLUMN", Token::Column),
            ("PRIMARY", Token::Primary),
            ("KEY", Token::Key),
            ("FOREIGN", Token::Foreign),
            ("REFERENCES", Token::References),
            ("UNIQUE", Token::Unique),
            ("NOT", Token::Not),
            ("NULL", Token::Null),
            ("AUTO", Token::Auto),
            ("INCREMENT", Token::Increment),
            ("DEFAULT", Token::Default),
            ("CHECK", Token::Check),
            ("UNION", Token::Union),
            ("ALL", Token::All),
            ("DISTINCT", Token::Distinct),
            ("AS", Token::As),
            ("IN", Token::In),
            ("EXISTS", Token::Exists),
            ("BETWEEN", Token::Between),
            ("LIKE", Token::Like),
            ("IS", Token::Is),
            ("AND", Token::And),
            ("OR", Token::Or),
            ("CASE", Token::Case),
            ("WHEN", Token::When),
            ("THEN", Token::Then),
            ("ELSE", Token::Else),
            ("END", Token::End),
            ("IF", Token::If),
            ("BEGIN", Token::Begin),
            ("COMMIT", Token::Commit),
            ("ROLLBACK", Token::Rollback),
            ("TRANSACTION", Token::Transaction),
            ("INTEGER", Token::Integer),
            ("INT", Token::Integer),
            ("VARCHAR", Token::Varchar),
            ("TEXT", Token::Text),
            ("BOOLEAN", Token::Boolean),
            ("BOOL", Token::Boolean),
            ("FLOAT", Token::Float),
            ("DOUBLE", Token::Double),
            ("DATE", Token::Date),
            ("DATETIME", Token::DateTime),
            ("TIMESTAMP", Token::Timestamp),
            ("TRUE", Token::BooleanLiteral(true)),
            ("FALSE", Token::BooleanLiteral(false)),
        ];

        for (keyword, token) in keyword_pairs {
            keywords.insert(keyword.to_string(), token);
        }

        Self {
            input: input.chars().collect(),
            position: 0,
            line: 1,
            column: 1,
            keywords,
        }
    }

    /// Peek at the current character without consuming it
    fn peek(&self) -> Option<char> {
        self.input.get(self.position).copied()
    }

    /// Peek at the character at a specific offset from current position
    fn peek_offset(&self, offset: usize) -> Option<char> {
        self.input.get(self.position + offset).copied()
    }

    /// Consume and return the current character
    fn consume(&mut self) -> Option<char> {
        if let Some(ch) = self.input.get(self.position) {
            let ch = *ch;
            self.position += 1;
            if ch == '\n' {
                self.line += 1;
                self.column = 1;
            } else {
                self.column += 1;
            }
            Some(ch)
        } else {
            None
        }
    }

    /// Skip whitespace characters
    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.peek() {
            if ch.is_whitespace() {
                self.consume();
            } else {
                break;
            }
        }
    }

    /// Read an identifier or keyword
    fn read_identifier(&mut self) -> String {
        let mut identifier = String::new();

        while let Some(ch) = self.peek() {
            if ch.is_alphanumeric() || ch == '_' {
                identifier.push(ch);
                self.consume();
            } else {
                break;
            }
        }

        identifier
    }

    /// Read a string literal (quoted)
    fn read_string_literal(&mut self) -> Result<String, ParseError> {
        let quote_char = self.consume().unwrap(); // consume opening quote
        let mut value = String::new();

        while let Some(ch) = self.peek() {
            if ch == quote_char {
                self.consume(); // consume closing quote
                return Ok(value);
            } else if ch == '\\' {
                self.consume(); // consume backslash
                if let Some(escaped) = self.consume() {
                    match escaped {
                        'n' => value.push('\n'),
                        't' => value.push('\t'),
                        'r' => value.push('\r'),
                        '\\' => value.push('\\'),
                        '\'' => value.push('\''),
                        '"' => value.push('"'),
                        _ => {
                            value.push('\\');
                            value.push(escaped);
                        }
                    }
                }
            } else {
                value.push(ch);
                self.consume();
            }
        }

        Err(ParseError {
            message: "Unterminated string literal".to_string(),
            position: self.position,
            line: self.line,
            column: self.column,
        })
    }

    /// Read a number literal
    fn read_number_literal(&mut self) -> String {
        let mut number = String::new();
        let mut has_dot = false;

        while let Some(ch) = self.peek() {
            if ch.is_ascii_digit() {
                number.push(ch);
                self.consume();
            } else if ch == '.' && !has_dot {
                has_dot = true;
                number.push(ch);
                self.consume();
            } else {
                break;
            }
        }

        number
    }

    /// Read a comment (single-line or multi-line)
    fn read_comment(&mut self) -> String {
        let mut comment = String::new();

        if self.peek() == Some('-') && self.peek_offset(1) == Some('-') {
            // Single-line comment
            self.consume(); // consume first -
            self.consume(); // consume second -

            while let Some(ch) = self.peek() {
                if ch == '\n' {
                    break;
                } else {
                    comment.push(ch);
                    self.consume();
                }
            }
        } else if self.peek() == Some('/') && self.peek_offset(1) == Some('*') {
            // Multi-line comment
            self.consume(); // consume /
            self.consume(); // consume *

            while let Some(ch) = self.peek() {
                if ch == '*' && self.peek_offset(1) == Some('/') {
                    self.consume(); // consume *
                    self.consume(); // consume /
                    break;
                } else {
                    comment.push(ch);
                    self.consume();
                }
            }
        }

        comment
    }

    /// Get the next token from input
    pub fn next_token(&mut self) -> Result<Token, ParseError> {
        self.skip_whitespace();

        match self.peek() {
            None => Ok(Token::Eof),
            Some('(') => {
                self.consume();
                Ok(Token::LeftParen)
            }
            Some(')') => {
                self.consume();
                Ok(Token::RightParen)
            }
            Some(',') => {
                self.consume();
                Ok(Token::Comma)
            }
            Some(';') => {
                self.consume();
                Ok(Token::Semicolon)
            }
            Some('.') => {
                self.consume();
                Ok(Token::Dot)
            }
            Some('*') => {
                self.consume();
                Ok(Token::Star)
            }
            Some('=') => {
                self.consume();
                Ok(Token::Equals)
            }
            Some('<') => {
                self.consume();
                if self.peek() == Some('=') {
                    self.consume();
                    Ok(Token::LessThanOrEqual)
                } else if self.peek() == Some('>') {
                    self.consume();
                    Ok(Token::NotEquals)
                } else {
                    Ok(Token::LessThan)
                }
            }
            Some('>') => {
                self.consume();
                if self.peek() == Some('=') {
                    self.consume();
                    Ok(Token::GreaterThanOrEqual)
                } else {
                    Ok(Token::GreaterThan)
                }
            }
            Some('!') => {
                self.consume();
                if self.peek() == Some('=') {
                    self.consume();
                    Ok(Token::NotEquals)
                } else {
                    Err(ParseError {
                        message: "Unexpected character '!'".to_string(),
                        position: self.position,
                        line: self.line,
                        column: self.column,
                    })
                }
            }
            Some('+') => {
                self.consume();
                Ok(Token::Plus)
            }
            Some('-') => {
                if self.peek_offset(1) == Some('-') {
                    let comment = self.read_comment();
                    Ok(Token::Comment(comment))
                } else {
                    self.consume();
                    Ok(Token::Minus)
                }
            }
            Some('/') => {
                if self.peek_offset(1) == Some('*') {
                    let comment = self.read_comment();
                    Ok(Token::Comment(comment))
                } else {
                    self.consume();
                    Ok(Token::Divide)
                }
            }
            Some('%') => {
                self.consume();
                Ok(Token::Modulo)
            }
            Some('\'') | Some('"') => {
                let value = self.read_string_literal()?;
                Ok(Token::StringLiteral(value))
            }
            Some(ch) if ch.is_ascii_digit() => {
                let number = self.read_number_literal();
                Ok(Token::NumberLiteral(number))
            }
            Some(ch) if ch.is_alphabetic() || ch == '_' => {
                let identifier = self.read_identifier();
                let upper_identifier = identifier.to_uppercase();

                // Check if it's a keyword
                if let Some(keyword_token) = self.keywords.get(&upper_identifier) {
                    Ok(keyword_token.clone())
                } else {
                    Ok(Token::Identifier(identifier))
                }
            }
            Some(ch) => Err(ParseError {
                message: format!("Unexpected character '{}'", ch),
                position: self.position,
                line: self.line,
                column: self.column,
            }),
        }
    }

    /// Tokenize the entire input and return a vector of tokens
    pub fn tokenize(&mut self) -> Result<Vec<Token>, ParseError> {
        let mut tokens = Vec::new();

        loop {
            let token = self.next_token()?;
            let is_eof = matches!(token, Token::Eof);

            // Skip comments and whitespace tokens
            if !matches!(token, Token::Comment(_) | Token::Whitespace) {
                tokens.push(token);
            }

            if is_eof {
                break;
            }
        }

        Ok(tokens)
    }
}

/// SQL Parser that builds an AST from tokens
pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    /// Create a new parser with the given tokens
    pub fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            position: 0,
        }
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.position).unwrap_or(&Token::Eof)
    }

    fn consume(&mut self) -> Token {
        if self.position < self.tokens.len() {
            let token = self.tokens[self.position].clone();
            self.position += 1;
            token
        } else {
            Token::Eof
        }
    }

    /// Expect a specific token and consume it, or return an error
    fn expect(&mut self, expected: Token) -> Result<(), ParseError> {
        let current = self.peek().clone();
        if std::mem::discriminant(&current) == std::mem::discriminant(&expected) {
            self.consume();
            Ok(())
        } else {
            Err(ParseError {
                message: format!("Expected {:?}, found {:?}", expected, current),
                position: self.position,
                line: 0,
                column: 0,
            })
        }
    }

    /// Parse a complete SQL statement
    pub fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        match self.peek() {
            Token::Select => self.parse_select(),
            Token::Insert => self.parse_insert(),
            Token::Update => self.parse_update(),
            Token::Delete => self.parse_delete(),
            Token::Create => self.parse_create(),
            Token::Drop => self.parse_drop(),
            Token::Alter => self.parse_alter(),
            Token::Begin => self.parse_transaction(),
            Token::Commit => self.parse_transaction(),
            Token::Rollback => self.parse_transaction(),
            _ => Err(ParseError {
                message: format!("Unexpected token at start of statement: {:?}", self.peek()),
                position: self.position,
                line: 0,
                column: 0,
            }),
        }
    }

    /// Parse SELECT statement
    fn parse_select(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Select)?;

        let distinct = if matches!(self.peek(), Token::Distinct) {
            self.consume();
            true
        } else {
            false
        };

        // Parse projection (columns)
        let projection = self.parse_projection()?;

        // Parse FROM clause
        let from = if matches!(self.peek(), Token::From) {
            self.consume();
            Some(self.parse_table_reference()?)
        } else {
            None
        };

        // Parse JOIN clauses
        let mut joins = Vec::new();
        while self.is_join_keyword() {
            joins.push(self.parse_join()?);
        }

        // Parse WHERE clause
        let where_clause = if matches!(self.peek(), Token::Where) {
            self.consume();
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse GROUP BY clause
        let group_by = if matches!(self.peek(), Token::Group) {
            self.consume();
            self.expect(Token::By)?;
            self.parse_expression_list()?
        } else {
            Vec::new()
        };

        // Parse HAVING clause
        let having = if matches!(self.peek(), Token::Having) {
            self.consume();
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse ORDER BY clause
        let order_by = if matches!(self.peek(), Token::Order) {
            self.consume();
            self.expect(Token::By)?;
            self.parse_order_by_list()?
        } else {
            Vec::new()
        };

        // Parse LIMIT clause
        let limit = if matches!(self.peek(), Token::Limit) {
            self.consume();
            if let Token::NumberLiteral(n) = self.consume() {
                Some(n.parse::<u64>().map_err(|_| ParseError {
                    message: "Invalid number in LIMIT clause".to_string(),
                    position: self.position,
                    line: 0,
                    column: 0,
                })?)
            } else {
                return Err(ParseError {
                    message: "Expected number after LIMIT".to_string(),
                    position: self.position,
                    line: 0,
                    column: 0,
                });
            }
        } else {
            None
        };

        // Parse OFFSET clause
        let offset = if matches!(self.peek(), Token::Offset) {
            self.consume();
            if let Token::NumberLiteral(n) = self.consume() {
                Some(n.parse::<u64>().map_err(|_| ParseError {
                    message: "Invalid number in OFFSET clause".to_string(),
                    position: self.position,
                    line: 0,
                    column: 0,
                })?)
            } else {
                return Err(ParseError {
                    message: "Expected number after OFFSET".to_string(),
                    position: self.position,
                    line: 0,
                    column: 0,
                });
            }
        } else {
            None
        };

        Ok(Statement::Select {
            projection,
            from,
            joins,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            distinct,
        })
    }

    /// Parse projection (SELECT column list)
    fn parse_projection(&mut self) -> Result<Vec<Expression>, ParseError> {
        let mut projection = Vec::new();

        loop {
            let mut expr = if matches!(self.peek(), Token::Star) {
                self.consume();
                Expression::Identifier("*".to_string())
            } else {
                self.parse_expression()?
            };

            // Check for AS alias (optional)
            if matches!(self.peek(), Token::As) {
                self.consume();
                if let Token::Identifier(alias) = self.consume() {
                    expr = Expression::Alias {
                        expr: Box::new(expr),
                        alias,
                    };
                } else {
                    return Err(ParseError {
                        message: "Expected identifier after AS".to_string(),
                        position: self.position,
                        line: 0,
                        column: 0,
                    });
                }
            }
            // Also handle implicit alias (identifier directly after expression)
            else if matches!(self.peek(), Token::Identifier(_)) && !self.is_keyword() {
                if let Token::Identifier(alias) = self.consume() {
                    expr = Expression::Alias {
                        expr: Box::new(expr),
                        alias,
                    };
                }
            }

            projection.push(expr);

            if matches!(self.peek(), Token::Comma) {
                self.consume();
            } else {
                break;
            }
        }

        Ok(projection)
    }

    /// Parse table reference (table name or subquery)
    fn parse_table_reference(&mut self) -> Result<TableReference, ParseError> {
        if matches!(self.peek(), Token::LeftParen) {
            // Subquery
            self.consume(); // consume '('
            let query = Box::new(self.parse_statement()?);
            self.expect(Token::RightParen)?;

            // Alias for subqueries (AS is optional)
            let alias = if matches!(self.peek(), Token::As) {
                self.consume();
                if let Token::Identifier(alias) = self.consume() {
                    alias
                } else {
                    return Err(ParseError {
                        message: "Expected alias after AS".to_string(),
                        position: self.position,
                        line: 0,
                        column: 0,
                    });
                }
            } else if let Token::Identifier(alias) = self.peek() {
                if !self.is_keyword() {
                    self.consume();
                    if let Token::Identifier(a) =
                        self.tokens.get(self.position - 1).unwrap_or(&Token::Eof)
                    {
                        a.clone()
                    } else {
                        return Err(ParseError {
                            message: "Expected alias for subquery".to_string(),
                            position: self.position,
                            line: 0,
                            column: 0,
                        });
                    }
                } else {
                    return Err(ParseError {
                        message: "Subquery requires an alias".to_string(),
                        position: self.position,
                        line: 0,
                        column: 0,
                    });
                }
            } else {
                return Err(ParseError {
                    message: "Subquery requires an alias".to_string(),
                    position: self.position,
                    line: 0,
                    column: 0,
                });
            };

            Ok(TableReference::Subquery { query, alias })
        } else if let Token::Identifier(name) = self.consume() {
            // Table name with optional alias
            let alias = if matches!(self.peek(), Token::As) {
                // Explicit AS alias
                self.consume();
                if let Token::Identifier(alias) = self.consume() {
                    Some(alias)
                } else {
                    return Err(ParseError {
                        message: "Expected alias after AS".to_string(),
                        position: self.position,
                        line: 0,
                        column: 0,
                    });
                }
            } else if matches!(self.peek(), Token::Identifier(_)) && !self.is_keyword() {
                // Implicit alias (identifier right after table name)
                if let Token::Identifier(alias) = self.consume() {
                    Some(alias)
                } else {
                    None
                }
            } else {
                None
            };

            Ok(TableReference::Table { name, alias })
        } else {
            Err(ParseError {
                message: "Expected table name or subquery".to_string(),
                position: self.position,
                line: 0,
                column: 0,
            })
        }
    }

    /// Check if current token is a JOIN keyword
    /// Check if current token is a SQL keyword (not an identifier we can use as alias)
    fn is_keyword(&self) -> bool {
        matches!(
            self.peek(),
            Token::Select
                | Token::Insert
                | Token::Update
                | Token::Delete
                | Token::From
                | Token::Where
                | Token::Join
                | Token::Inner
                | Token::Left
                | Token::Right
                | Token::Full
                | Token::Outer
                | Token::On
                | Token::Group
                | Token::By
                | Token::Having
                | Token::Order
                | Token::Limit
                | Token::Offset
                | Token::And
                | Token::Or
                | Token::Not
                | Token::In
                | Token::Is
                | Token::Like
                | Token::Between
                | Token::Null
                | Token::As
                | Token::Create
                | Token::Drop
                | Token::Alter
                | Token::Table
                | Token::Database
                | Token::Index
                | Token::Primary
                | Token::Key
                | Token::Foreign
                | Token::References
                | Token::Unique
                | Token::Default
                | Token::Values
                | Token::Set
                | Token::Into
                | Token::Begin
                | Token::Commit
                | Token::Rollback
                | Token::Distinct
                | Token::All
                | Token::Union
                | Token::Case
                | Token::When
                | Token::Then
                | Token::Else
                | Token::End
                | Token::If
                | Token::Exists
        )
    }

    fn is_join_keyword(&self) -> bool {
        matches!(
            self.peek(),
            Token::Join | Token::Inner | Token::Left | Token::Right | Token::Full
        )
    }

    /// Parse JOIN clause
    fn parse_join(&mut self) -> Result<Join, ParseError> {
        let join_type = match self.peek() {
            Token::Inner => {
                self.consume();
                self.expect(Token::Join)?;
                JoinType::Inner
            }
            Token::Left => {
                self.consume();
                if matches!(self.peek(), Token::Outer) {
                    self.consume();
                }
                self.expect(Token::Join)?;
                JoinType::Left
            }
            Token::Right => {
                self.consume();
                if matches!(self.peek(), Token::Outer) {
                    self.consume();
                }
                self.expect(Token::Join)?;
                JoinType::Right
            }
            Token::Full => {
                self.consume();
                if matches!(self.peek(), Token::Outer) {
                    self.consume();
                }
                self.expect(Token::Join)?;
                JoinType::Full
            }
            Token::Join => {
                self.consume();
                JoinType::Inner
            }
            _ => {
                return Err(ParseError {
                    message: "Expected JOIN keyword".to_string(),
                    position: self.position,
                    line: 0,
                    column: 0,
                });
            }
        };

        let table = self.parse_table_reference()?;

        let condition = if matches!(self.peek(), Token::On) {
            self.consume();
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(Join {
            join_type,
            table,
            condition,
        })
    }

    /// Parse expression list (comma-separated expressions)
    fn parse_expression_list(&mut self) -> Result<Vec<Expression>, ParseError> {
        let mut expressions = Vec::new();

        loop {
            expressions.push(self.parse_expression()?);

            if matches!(self.peek(), Token::Comma) {
                self.consume();
            } else {
                break;
            }
        }

        Ok(expressions)
    }

    /// Parse ORDER BY list
    fn parse_order_by_list(&mut self) -> Result<Vec<OrderBy>, ParseError> {
        let mut order_by = Vec::new();

        loop {
            let expression = self.parse_expression()?;
            let direction = match self.peek() {
                Token::Identifier(dir) if dir.to_uppercase() == "ASC" => {
                    self.consume();
                    OrderDirection::Asc
                }
                Token::Identifier(dir) if dir.to_uppercase() == "DESC" => {
                    self.consume();
                    OrderDirection::Desc
                }
                _ => OrderDirection::Asc, // Default to ASC
            };

            order_by.push(OrderBy {
                expression,
                direction,
            });

            if matches!(self.peek(), Token::Comma) {
                self.consume();
            } else {
                break;
            }
        }

        Ok(order_by)
    }

    /// Parse expression with precedence handling
    fn parse_expression(&mut self) -> Result<Expression, ParseError> {
        self.parse_or_expression()
    }

    /// Parse OR expressions (lowest precedence)
    fn parse_or_expression(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_and_expression()?;

        while matches!(self.peek(), Token::Or) {
            self.consume();
            let right = self.parse_and_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                operator: BinaryOperator::Or,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse AND expressions
    fn parse_and_expression(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_equality_expression()?;

        while matches!(self.peek(), Token::And) {
            self.consume();
            let right = self.parse_equality_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                operator: BinaryOperator::And,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse equality expressions (=, <>, !=)
    fn parse_equality_expression(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_comparison_expression()?;

        while let Some(op) = self.match_equality_operator() {
            self.consume();
            let right = self.parse_comparison_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                operator: op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse comparison expressions (<, >, <=, >=, LIKE, IN, BETWEEN)
    fn parse_comparison_expression(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_additive_expression()?;

        while let Some(op) = self.match_comparison_operator() {
            if matches!(op, BinaryOperator::Between) {
                self.consume(); // consume BETWEEN
                let low = self.parse_additive_expression()?;
                self.expect(Token::And)?;
                let high = self.parse_additive_expression()?;

                // Transform BETWEEN into: left >= low AND left <= high
                left = Expression::BinaryOp {
                    left: Box::new(Expression::BinaryOp {
                        left: Box::new(left.clone()),
                        operator: BinaryOperator::GreaterThanOrEqual,
                        right: Box::new(low),
                    }),
                    operator: BinaryOperator::And,
                    right: Box::new(Expression::BinaryOp {
                        left: Box::new(left),
                        operator: BinaryOperator::LessThanOrEqual,
                        right: Box::new(high),
                    }),
                };
            } else if matches!(op, BinaryOperator::In) {
                self.consume(); // consume IN
                self.expect(Token::LeftParen)?;

                if matches!(self.peek(), Token::Select) {
                    // Subquery
                    let subquery = Box::new(self.parse_statement()?);
                    left = Expression::BinaryOp {
                        left: Box::new(left),
                        operator: BinaryOperator::In,
                        right: Box::new(Expression::Subquery(subquery)),
                    };
                } else {
                    // Value list
                    let values = self.parse_expression_list()?;
                    // For simplicity, we'll represent IN with a list as a function call
                    left = Expression::BinaryOp {
                        left: Box::new(left),
                        operator: BinaryOperator::In,
                        right: Box::new(Expression::Function {
                            name: "IN_LIST".to_string(),
                            args: values,
                        }),
                    };
                }

                self.expect(Token::RightParen)?;
            } else {
                self.consume();
                let right = self.parse_additive_expression()?;
                left = Expression::BinaryOp {
                    left: Box::new(left),
                    operator: op,
                    right: Box::new(right),
                };
            }
        }

        Ok(left)
    }

    /// Parse additive expressions (+, -)
    fn parse_additive_expression(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_multiplicative_expression()?;

        while let Some(op) = self.match_additive_operator() {
            self.consume();
            let right = self.parse_multiplicative_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                operator: op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse multiplicative expressions (*, /, %)
    fn parse_multiplicative_expression(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_unary_expression()?;

        while let Some(op) = self.match_multiplicative_operator() {
            self.consume();
            let right = self.parse_unary_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                operator: op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse unary expressions (NOT, -, +)
    fn parse_unary_expression(&mut self) -> Result<Expression, ParseError> {
        match self.peek() {
            Token::Not => {
                self.consume();
                let operand = Box::new(self.parse_unary_expression()?);
                Ok(Expression::UnaryOp {
                    operator: UnaryOperator::Not,
                    operand,
                })
            }
            Token::Minus => {
                self.consume();
                let operand = Box::new(self.parse_unary_expression()?);
                Ok(Expression::UnaryOp {
                    operator: UnaryOperator::Minus,
                    operand,
                })
            }
            Token::Plus => {
                self.consume();
                let operand = Box::new(self.parse_unary_expression()?);
                Ok(Expression::UnaryOp {
                    operator: UnaryOperator::Plus,
                    operand,
                })
            }
            _ => self.parse_primary_expression(),
        }
    }

    /// Parse primary expressions (literals, identifiers, function calls, parenthesized expressions)
    fn parse_primary_expression(&mut self) -> Result<Expression, ParseError> {
        match self.peek().clone() {
            Token::StringLiteral(s) => {
                self.consume();
                Ok(Expression::Literal(Literal::String(s)))
            }
            Token::NumberLiteral(n) => {
                self.consume();
                Ok(Expression::Literal(Literal::Number(n)))
            }
            Token::BooleanLiteral(b) => {
                self.consume();
                Ok(Expression::Literal(Literal::Boolean(b)))
            }
            Token::Null => {
                self.consume();
                Ok(Expression::Literal(Literal::Null))
            }
            Token::Identifier(name) => {
                self.consume();

                if matches!(self.peek(), Token::LeftParen) {
                    // Function call
                    self.consume(); // consume '('

                    let mut args = Vec::new();
                    if !matches!(self.peek(), Token::RightParen) {
                        args = self.parse_expression_list()?;
                    }

                    self.expect(Token::RightParen)?;

                    Ok(Expression::Function { name, args })
                } else if matches!(self.peek(), Token::Dot) {
                    // Qualified column (table.column)
                    self.consume(); // consume '.'
                    if let Token::Identifier(column) = self.consume() {
                        Ok(Expression::QualifiedColumn {
                            table: name,
                            column,
                        })
                    } else {
                        Err(ParseError {
                            message: "Expected column name after '.'".to_string(),
                            position: self.position,
                            line: 0,
                            column: 0,
                        })
                    }
                } else {
                    // Simple identifier/column
                    Ok(Expression::Identifier(name))
                }
            }
            Token::LeftParen => {
                self.consume(); // consume '('

                // Check if this is a subquery
                if matches!(self.peek(), Token::Select) {
                    let subquery = Box::new(self.parse_statement()?);
                    self.expect(Token::RightParen)?;
                    Ok(Expression::Subquery(subquery))
                } else {
                    // Parenthesized expression
                    let expr = self.parse_expression()?;
                    self.expect(Token::RightParen)?;
                    Ok(expr)
                }
            }
            Token::Case => self.parse_case_expression(),
            _ => Err(ParseError {
                message: format!("Unexpected token in expression: {:?}", self.peek()),
                position: self.position,
                line: 0,
                column: 0,
            }),
        }
    }

    /// Parse CASE expression
    fn parse_case_expression(&mut self) -> Result<Expression, ParseError> {
        self.expect(Token::Case)?;

        let mut when_clauses = Vec::new();

        while matches!(self.peek(), Token::When) {
            self.consume(); // consume WHEN
            let condition = self.parse_expression()?;
            self.expect(Token::Then)?;
            let result = self.parse_expression()?;
            when_clauses.push((condition, result));
        }

        let else_clause = if matches!(self.peek(), Token::Else) {
            self.consume();
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        self.expect(Token::End)?;

        Ok(Expression::Case {
            when_clauses,
            else_clause,
        })
    }

    /// Match equality operators
    fn match_equality_operator(&self) -> Option<BinaryOperator> {
        match self.peek() {
            Token::Equals => Some(BinaryOperator::Equals),
            Token::NotEquals => Some(BinaryOperator::NotEquals),
            _ => None,
        }
    }

    /// Match comparison operators
    fn match_comparison_operator(&self) -> Option<BinaryOperator> {
        match self.peek() {
            Token::LessThan => Some(BinaryOperator::LessThan),
            Token::LessThanOrEqual => Some(BinaryOperator::LessThanOrEqual),
            Token::GreaterThan => Some(BinaryOperator::GreaterThan),
            Token::GreaterThanOrEqual => Some(BinaryOperator::GreaterThanOrEqual),
            Token::Like => Some(BinaryOperator::Like),
            Token::In => Some(BinaryOperator::In),
            Token::Between => Some(BinaryOperator::Between),
            _ => None,
        }
    }

    /// Match additive operators
    fn match_additive_operator(&self) -> Option<BinaryOperator> {
        match self.peek() {
            Token::Plus => Some(BinaryOperator::Plus),
            Token::Minus => Some(BinaryOperator::Minus),
            _ => None,
        }
    }

    /// Match multiplicative operators
    fn match_multiplicative_operator(&self) -> Option<BinaryOperator> {
        match self.peek() {
            // Tokenizer emits `Star` for '*'
            Token::Star => Some(BinaryOperator::Multiply),
            Token::Divide => Some(BinaryOperator::Divide),
            Token::Modulo => Some(BinaryOperator::Modulo),
            _ => None,
        }
    }

    /// Parse INSERT statement
    fn parse_insert(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Insert)?;
        self.expect(Token::Into)?;

        let table = if let Token::Identifier(name) = self.consume() {
            name
        } else {
            return Err(ParseError {
                message: "Expected table name after INSERT INTO".to_string(),
                position: self.position,
                line: 0,
                column: 0,
            });
        };

        // Parse optional column list
        let columns = if matches!(self.peek(), Token::LeftParen) {
            self.consume(); // consume '('
            let mut cols = Vec::new();

            loop {
                if let Token::Identifier(col) = self.consume() {
                    cols.push(col);
                } else {
                    return Err(ParseError {
                        message: "Expected column name".to_string(),
                        position: self.position,
                        line: 0,
                        column: 0,
                    });
                }

                if matches!(self.peek(), Token::Comma) {
                    self.consume();
                } else {
                    break;
                }
            }

            self.expect(Token::RightParen)?;
            Some(cols)
        } else {
            None
        };

        self.expect(Token::Values)?;

        // Parse values
        let mut values = Vec::new();

        loop {
            self.expect(Token::LeftParen)?;
            let row_values = self.parse_expression_list()?;
            self.expect(Token::RightParen)?;
            values.push(row_values);

            if matches!(self.peek(), Token::Comma) {
                self.consume();
            } else {
                break;
            }
        }

        Ok(Statement::Insert {
            table,
            columns,
            values,
        })
    }

    /// Parse UPDATE statement
    fn parse_update(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Update)?;

        let table = if let Token::Identifier(name) = self.consume() {
            name
        } else {
            return Err(ParseError {
                message: "Expected table name after UPDATE".to_string(),
                position: self.position,
                line: 0,
                column: 0,
            });
        };

        self.expect(Token::Set)?;

        // Parse assignments
        let mut assignments = Vec::new();

        loop {
            let column = if let Token::Identifier(col) = self.consume() {
                col
            } else {
                return Err(ParseError {
                    message: "Expected column name in SET clause".to_string(),
                    position: self.position,
                    line: 0,
                    column: 0,
                });
            };

            self.expect(Token::Equals)?;
            let value = self.parse_expression()?;

            assignments.push(Assignment { column, value });

            if matches!(self.peek(), Token::Comma) {
                self.consume();
            } else {
                break;
            }
        }

        // Parse WHERE clause
        let where_clause = if matches!(self.peek(), Token::Where) {
            self.consume();
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(Statement::Update {
            table,
            assignments,
            where_clause,
        })
    }

    /// Parse DELETE statement
    fn parse_delete(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Delete)?;
        self.expect(Token::From)?;

        let table = if let Token::Identifier(name) = self.consume() {
            name
        } else {
            return Err(ParseError {
                message: "Expected table name after DELETE FROM".to_string(),
                position: self.position,
                line: 0,
                column: 0,
            });
        };

        // Parse WHERE clause
        let where_clause = if matches!(self.peek(), Token::Where) {
            self.consume();
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(Statement::Delete {
            table,
            where_clause,
        })
    }

    /// Parse CREATE statement
    fn parse_create(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Create)?;

        match self.peek() {
            Token::Table => self.parse_create_table(),
            Token::Database => self.parse_create_database(),
            Token::Index => self.parse_create_index(),
            _ => Err(ParseError {
                message: "Expected TABLE, DATABASE, or INDEX after CREATE".to_string(),
                position: self.position,
                line: 0,
                column: 0,
            }),
        }
    }

    /// Parse CREATE TABLE statement
    fn parse_create_table(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Table)?;

        let if_not_exists = if matches!(self.peek(), Token::If) {
            self.consume();
            self.expect(Token::Not)?;
            self.expect(Token::Exists)?;
            true
        } else {
            false
        };

        let name = if let Token::Identifier(n) = self.consume() {
            n
        } else {
            return Err(ParseError {
                message: "Expected table name".to_string(),
                position: self.position,
                line: 0,
                column: 0,
            });
        };

        self.expect(Token::LeftParen)?;

        let mut columns: Vec<ColumnDef> = Vec::new();
        let mut constraints: Vec<TableConstraint> = Vec::new();

        loop {
            if self.is_table_constraint() {
                constraints.push(self.parse_table_constraint()?);
            } else {
                columns.push(self.parse_column_definition()?);
            }

            if matches!(self.peek(), Token::Comma) {
                self.consume();
            } else {
                break;
            }
        }

        self.expect(Token::RightParen)?;

        Ok(Statement::CreateTable {
            name,
            columns,
            constraints,
            if_not_exists,
        })
    }

    /// Parse CREATE DATABASE statement
    fn parse_create_database(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Database)?;

        let if_not_exists = if matches!(self.peek(), Token::If) {
            self.consume();
            self.expect(Token::Not)?;
            self.expect(Token::Exists)?;
            true
        } else {
            false
        };

        let name = if let Token::Identifier(n) = self.consume() {
            n
        } else {
            return Err(ParseError {
                message: "Expected database name".to_string(),
                position: self.position,
                line: 0,
                column: 0,
            });
        };

        Ok(Statement::CreateDatabase {
            name,
            if_not_exists,
        })
    }

    /// Parse CREATE INDEX statement
    fn parse_create_index(&mut self) -> Result<Statement, ParseError> {
        let unique = if matches!(self.peek(), Token::Unique) {
            self.consume();
            true
        } else {
            false
        };

        self.expect(Token::Index)?;

        let if_not_exists = if matches!(self.peek(), Token::If) {
            self.consume(); // consume IF
            self.expect(Token::Not)?;
            self.expect(Token::Exists)?;
            true
        } else {
            false
        };

        let name = if let Token::Identifier(n) = self.consume() {
            n
        } else {
            return Err(ParseError {
                message: "Expected index name".to_string(),
                position: self.position,
                line: 0,
                column: 0,
            });
        };

        self.expect(Token::On)?;

        let table = if let Token::Identifier(t) = self.consume() {
            t
        } else {
            return Err(ParseError {
                message: "Expected table name after ON".to_string(),
                position: self.position,
                line: 0,
                column: 0,
            });
        };

        self.expect(Token::LeftParen)?;

        let mut columns = Vec::new();
        loop {
            if let Token::Identifier(col) = self.consume() {
                columns.push(col);
            } else {
                return Err(ParseError {
                    message: "Expected column name in index".to_string(),
                    position: self.position,
                    line: 0,
                    column: 0,
                });
            }

            if matches!(self.peek(), Token::Comma) {
                self.consume();
            } else {
                break;
            }
        }

        self.expect(Token::RightParen)?;

        Ok(Statement::CreateIndex {
            name,
            table,
            columns,
            unique,
            if_not_exists,
        })
    }

    /// Parse DROP statement
    fn parse_drop(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Drop)?;

        match self.peek() {
            Token::Table => {
                self.consume();

                let if_exists = if matches!(self.peek(), Token::If) {
                    self.consume(); // consume IF
                    self.expect(Token::Exists)?;
                    true
                } else {
                    false
                };

                let name = if let Token::Identifier(n) = self.consume() {
                    n
                } else {
                    return Err(ParseError {
                        message: "Expected table name after DROP TABLE".to_string(),
                        position: self.position,
                        line: 0,
                        column: 0,
                    });
                };

                Ok(Statement::DropTable { name, if_exists })
            }
            Token::Database => {
                self.consume();

                let if_exists = if matches!(self.peek(), Token::If) {
                    self.consume(); // consume IF
                    self.expect(Token::Exists)?;
                    true
                } else {
                    false
                };

                let name = if let Token::Identifier(n) = self.consume() {
                    n
                } else {
                    return Err(ParseError {
                        message: "Expected database name after DROP DATABASE".to_string(),
                        position: self.position,
                        line: 0,
                        column: 0,
                    });
                };

                Ok(Statement::DropDatabase { name, if_exists })
            }
            _ => Err(ParseError {
                message: "Expected TABLE or DATABASE after DROP".to_string(),
                position: self.position,
                line: 0,
                column: 0,
            }),
        }
    }

    /// Parse ALTER statement
    fn parse_alter(&mut self) -> Result<Statement, ParseError> {
        self.expect(Token::Alter)?;
        self.expect(Token::Table)?;

        let name = if let Token::Identifier(n) = self.consume() {
            n
        } else {
            return Err(ParseError {
                message: "Expected table name after ALTER TABLE".to_string(),
                position: self.position,
                line: 0,
                column: 0,
            });
        };

        let action = match self.peek() {
            Token::Add => {
                self.consume();

                if matches!(self.peek(), Token::Column) {
                    self.consume();

                    // TODO : Create this function
                    let column_def = self.parse_column_definition()?;
                    AlterAction::AddColumn(column_def)
                } else {
                    // Add constraint
                    // TODO : Create this function
                    let constraint = self.parse_table_constraint()?;
                    AlterAction::AddConstraint(constraint)
                }
            }
            Token::Drop => {
                self.consume();

                if matches!(self.peek(), Token::Column) {
                    self.consume();
                    let column_name = if let Token::Identifier(name) = self.consume() {
                        name
                    } else {
                        return Err(ParseError {
                            message: "Expected column name after DROP COLUMN".to_string(),
                            position: self.position,
                            line: 0,
                            column: 0,
                        });
                    };
                    AlterAction::DropColumn(column_name)
                } else {
                    // Drop constraint
                    let constraint_name = if let Token::Identifier(name) = self.consume() {
                        name
                    } else {
                        return Err(ParseError {
                            message: "Expected constraint name after DROP".to_string(),
                            position: self.position,
                            line: 0,
                            column: 0,
                        });
                    };
                    AlterAction::DropConstraint(constraint_name)
                }
            }
            _ => {
                return Err(ParseError {
                    message: "Expected ADD or DROP after ALTER TABLE".to_string(),
                    position: self.position,
                    line: 0,
                    column: 0,
                });
            }
        };

        Ok(Statement::AlterTable { name, action })
    }

    /// Parse transaction statements
    fn parse_transaction(&mut self) -> Result<Statement, ParseError> {
        let transaction = match self.consume() {
            Token::Begin => TransactionStatement::Begin,
            Token::Commit => TransactionStatement::Commit,
            Token::Rollback => TransactionStatement::Rollback,
            token => {
                return Err(ParseError {
                    message: format!("Unexpected transaction token: {:?}", token),
                    position: self.position,
                    line: 0,
                    column: 0,
                });
            }
        };

        // Optional TRANSACTION keyword
        if matches!(self.peek(), Token::Transaction) {
            self.consume();
        }

        Ok(Statement::Transaction(transaction))
    }

    /// Parse multiple statements separated by semicolons
    pub fn parse_statements(&mut self) -> Result<Vec<Statement>, ParseError> {
        let mut statements = Vec::new();

        while !matches!(self.peek(), Token::Eof) {
            let statement = self.parse_statement()?;
            statements.push(statement);

            // Consume optional semicolon
            if matches!(self.peek(), Token::Semicolon) {
                self.consume();
            }

            // Skip any whitespace or comments
            while matches!(self.peek(), Token::Whitespace | Token::Comment(_)) {
                self.consume();
            }
        }

        Ok(statements)
    }
}

/// Main SQL parser interface
pub struct SqlParser;

impl SqlParser {
    /// Parse SQL string into AST
    pub fn parse(input: &str) -> Result<Vec<Statement>, ParseError> {
        let mut tokenizer = Tokenizer::new(input);
        let tokens = tokenizer.tokenize()?;
        let mut parser = Parser::new(tokens);
        parser.parse_statements()
    }

    /// Parse a single SQL statement
    pub fn parse_statement(input: &str) -> Result<Statement, ParseError> {
        let statements = Self::parse(input)?;

        if statements.is_empty() {
            Err(ParseError {
                message: "No statements found".to_string(),
                position: 0,
                line: 1,
                column: 1,
            })
        } else if statements.len() > 1 {
            Err(ParseError {
                message: "Multiple statements found, expected single statement".to_string(),
                position: 0,
                line: 1,
                column: 1,
            })
        } else {
            Ok(statements.into_iter().next().unwrap())
        }
    }
}

/// Visitor trait for traversing the AST
pub trait AstVisitor {
    type Output;

    fn visit_statement(&mut self, stmt: &Statement) -> Self::Output;
    fn visit_expression(&mut self, expr: &Expression) -> Self::Output;
}

/// Pretty printer for SQL AST
pub struct SqlPrettyPrinter {
    indent_level: usize,
    indent_size: usize,
}

impl SqlPrettyPrinter {
    pub fn new() -> Self {
        Self {
            indent_level: 0,
            indent_size: 2,
        }
    }

    fn indent(&self) -> String {
        " ".repeat(self.indent_level * self.indent_size)
    }

    fn increase_indent(&mut self) {
        self.indent_level += 1;
    }

    fn decrease_indent(&mut self) {
        if self.indent_level > 0 {
            self.indent_level -= 1;
        }
    }

    pub fn print_statement(&mut self, stmt: &Statement) -> String {
        match stmt {
            Statement::Select {
                projection,
                from,
                joins,
                where_clause,
                group_by,
                having,
                order_by,
                limit,
                offset,
                distinct,
            } => {
                let mut result = String::new();

                result.push_str("SELECT");
                if *distinct {
                    result.push_str(" DISTINCT");
                }

                result.push('\n');
                self.increase_indent();

                for (i, expr) in projection.iter().enumerate() {
                    if i > 0 {
                        result.push_str(",\n");
                    }
                    result.push_str(&self.indent());
                    result.push_str(&self.print_expression(expr));
                }

                self.decrease_indent();

                if let Some(from_table) = from {
                    result.push_str("\nFROM ");
                    result.push_str(&self.print_table_reference(from_table));
                }

                for join in joins {
                    result.push('\n');
                    result.push_str(&self.print_join(join));
                }

                if let Some(where_expr) = where_clause {
                    result.push_str("\nWHERE ");
                    result.push_str(&self.print_expression(where_expr));
                }

                if !group_by.is_empty() {
                    result.push_str("\nGROUP BY ");
                    for (i, expr) in group_by.iter().enumerate() {
                        if i > 0 {
                            result.push_str(", ");
                        }
                        result.push_str(&self.print_expression(expr));
                    }
                }

                if let Some(having_expr) = having {
                    result.push_str("\nHAVING ");
                    result.push_str(&self.print_expression(having_expr));
                }

                if !order_by.is_empty() {
                    result.push_str("\nORDER BY ");
                    for (i, order) in order_by.iter().enumerate() {
                        if i > 0 {
                            result.push_str(", ");
                        }
                        result.push_str(&self.print_expression(&order.expression));
                        match order.direction {
                            OrderDirection::Desc => result.push_str(" DESC"),
                            OrderDirection::Asc => {} // ASC is default, don't print
                        }
                    }
                }

                if let Some(limit_val) = limit {
                    result.push_str(&format!("\nLIMIT {}", limit_val));
                }

                if let Some(offset_val) = offset {
                    result.push_str(&format!("\nOFFSET {}", offset_val));
                }

                result
            }
            Statement::Insert {
                table,
                columns,
                values,
            } => {
                let mut result = format!("INSERT INTO {}", table);

                if let Some(cols) = columns {
                    result.push_str(" (");
                    for (i, col) in cols.iter().enumerate() {
                        if i > 0 {
                            result.push_str(", ");
                        }
                        result.push_str(col);
                    }
                    result.push(')');
                }

                result.push_str("\nVALUES ");

                for (i, row) in values.iter().enumerate() {
                    if i > 0 {
                        result.push_str(",\n       ");
                    }
                    result.push('(');
                    for (j, val) in row.iter().enumerate() {
                        if j > 0 {
                            result.push_str(", ");
                        }
                        result.push_str(&self.print_expression(val));
                    }
                    result.push(')');
                }

                result
            }
            Statement::Update {
                table,
                assignments,
                where_clause,
            } => {
                let mut result = format!("UPDATE {}\nSET ", table);

                for (i, assignment) in assignments.iter().enumerate() {
                    if i > 0 {
                        result.push_str(",\n    ");
                    }
                    result.push_str(&format!(
                        "{} = {}",
                        assignment.column,
                        self.print_expression(&assignment.value)
                    ));
                }

                if let Some(where_expr) = where_clause {
                    result.push_str("\nWHERE ");
                    result.push_str(&self.print_expression(where_expr));
                }

                result
            }
            Statement::Delete {
                table,
                where_clause,
            } => {
                let mut result = format!("DELETE FROM {}", table);

                if let Some(where_expr) = where_clause {
                    result.push_str("\nWHERE ");
                    result.push_str(&self.print_expression(where_expr));
                }

                result
            }
            _ => format!("{:?}", stmt), // Fallback for other statement types
        }
    }

    fn print_expression(&self, expr: &Expression) -> String {
        match expr {
            Expression::Literal(lit) => match lit {
                Literal::String(s) => format!("'{}'", s),
                Literal::Number(n) => n.clone(),
                Literal::Boolean(b) => {
                    if *b {
                        "TRUE".to_string()
                    } else {
                        "FALSE".to_string()
                    }
                }
                Literal::Null => "NULL".to_string(),
            },
            Expression::Identifier(name) => name.clone(),
            Expression::QualifiedColumn { table, column } => format!("{}.{}", table, column),
            Expression::BinaryOp {
                left,
                operator,
                right,
            } => {
                let op_str = match operator {
                    BinaryOperator::Equals => "=",
                    BinaryOperator::NotEquals => "<>",
                    BinaryOperator::LessThan => "<",
                    BinaryOperator::LessThanOrEqual => "<=",
                    BinaryOperator::GreaterThan => ">",
                    BinaryOperator::GreaterThanOrEqual => ">=",
                    BinaryOperator::Plus => "+",
                    BinaryOperator::Minus => "-",
                    BinaryOperator::Multiply => "*",
                    BinaryOperator::Divide => "/",
                    BinaryOperator::Modulo => "%",
                    BinaryOperator::And => "AND",
                    BinaryOperator::Or => "OR",
                    BinaryOperator::Like => "LIKE",
                    BinaryOperator::In => "IN",
                    BinaryOperator::Between => "BETWEEN",
                };
                format!(
                    "({} {} {})",
                    self.print_expression(left),
                    op_str,
                    self.print_expression(right)
                )
            }
            Expression::UnaryOp { operator, operand } => {
                let op_str = match operator {
                    UnaryOperator::Not => "NOT",
                    UnaryOperator::Minus => "-",
                    UnaryOperator::Plus => "+",
                };
                format!("{} {}", op_str, self.print_expression(operand))
            }
            Expression::Function { name, args } => {
                let mut result = format!("{}(", name);
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&self.print_expression(arg));
                }
                result.push(')');
                result
            }
            _ => format!("{:?}", expr), // Fallback
        }
    }

    fn print_table_reference(&mut self, table_ref: &TableReference) -> String {
        match table_ref {
            TableReference::Table { name, alias } => {
                if let Some(alias_name) = alias {
                    format!("{} AS {}", name, alias_name)
                } else {
                    name.clone()
                }
            }
            TableReference::Subquery { query, alias } => {
                format!("({}) AS {}", self.print_statement(query), alias)
            }
        }
    }

    fn print_join(&mut self, join: &Join) -> String {
        let join_type_str = match join.join_type {
            JoinType::Inner => "INNER JOIN",
            JoinType::Left => "LEFT JOIN",
            JoinType::Right => "RIGHT JOIN",
            JoinType::Full => "FULL JOIN",
            JoinType::Cross => "CROSS JOIN",
        };

        let mut result = format!(
            "{} {}",
            join_type_str,
            self.print_table_reference(&join.table)
        );

        if let Some(condition) = &join.condition {
            result.push_str(" ON ");
            result.push_str(&self.print_expression(condition));
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenizer() {
        let input = "SELECT * FROM users WHERE id = 1";
        let mut tokenizer = Tokenizer::new(input);
        let tokens = tokenizer.tokenize().unwrap();

        assert_eq!(tokens[0], Token::Select);
        assert_eq!(tokens[1], Token::Star);
        assert_eq!(tokens[2], Token::From);
        assert_eq!(tokens[3], Token::Identifier("users".to_string()));
        assert_eq!(tokens[4], Token::Where);
        assert_eq!(tokens[5], Token::Identifier("id".to_string()));
        assert_eq!(tokens[6], Token::Equals);
        assert_eq!(tokens[7], Token::NumberLiteral("1".to_string()));
        assert_eq!(tokens[8], Token::Eof);
    }

    #[test]
    fn test_simple_select() {
        let input = "SELECT id, name FROM users";
        let result = SqlParser::parse_statement(input).unwrap();

        match result {
            Statement::Select {
                projection, from, ..
            } => {
                assert_eq!(projection.len(), 2);
                assert!(from.is_some());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_select_with_where() {
        let input = "SELECT * FROM users WHERE age > 18 AND active = TRUE";
        let result = SqlParser::parse_statement(input).unwrap();

        match result {
            Statement::Select { where_clause, .. } => {
                assert!(where_clause.is_some());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_insert_statement() {
        let input = "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')";
        let result = SqlParser::parse_statement(input).unwrap();

        match result {
            Statement::Insert {
                table,
                columns,
                values,
            } => {
                assert_eq!(table, "users");
                assert!(columns.is_some());
                assert_eq!(values.len(), 1);
            }
            _ => panic!("Expected INSERT statement"),
        }
    }

    #[test]
    fn test_create_table() {
        let input = r#"
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) UNIQUE,
                age INTEGER CHECK (age >= 0)
            )
        "#;

        let result = SqlParser::parse_statement(input).unwrap();

        match result {
            Statement::CreateTable { name, columns, .. } => {
                assert_eq!(name, "users");
                assert_eq!(columns.len(), 4);
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn test_complex_query() {
        let input = r#"
            SELECT u.name, p.title, COUNT(c.id) as comment_count
            FROM users u
            INNER JOIN posts p ON u.id = p.user_id
            LEFT JOIN comments c ON p.id = c.post_id
            WHERE u.active = TRUE
              AND p.published_at > '2023-01-01'
            GROUP BY u.id, p.id
            HAVING COUNT(c.id) > 0
            ORDER BY comment_count DESC
            LIMIT 10
        "#;

        let result = SqlParser::parse_statement(input).unwrap();

        match result {
            Statement::Select {
                projection,
                from,
                joins,
                where_clause,
                group_by,
                having,
                order_by,
                limit,
                ..
            } => {
                assert_eq!(projection.len(), 3);
                assert!(from.is_some());
                assert_eq!(joins.len(), 2);
                assert!(where_clause.is_some());
                assert_eq!(group_by.len(), 2);
                assert!(having.is_some());
                assert_eq!(order_by.len(), 1);
                assert_eq!(limit, Some(10));
            }
            _ => panic!("Expected SELECT statement"),
        }
    }
}

impl Parser {
    /// Parse column definition
    fn parse_column_definition(&mut self) -> Result<ColumnDef, ParseError> {
        let name = if let Token::Identifier(n) = self.consume() {
            n
        } else {
            return Err(ParseError {
                message: "Expected column name".to_string(),
                position: self.position,
                line: 0,
                column: 0,
            });
        };

        let data_type = self.parse_data_type()?;

        let mut constraints = Vec::new();
        while self.is_column_constraint() {
            constraints.push(self.parse_column_constraint()?);
        }

        Ok(ColumnDef {
            name,
            data_type,
            constraints,
        })
    }

    /// Parse data type
    fn parse_data_type(&mut self) -> Result<DataType, ParseError> {
        match self.consume() {
            Token::Integer => Ok(DataType::Integer),
            Token::Varchar => {
                if matches!(self.peek(), Token::LeftParen) {
                    self.consume();
                    let size = if let Token::NumberLiteral(n) = self.consume() {
                        n.parse::<u32>().map_err(|_| ParseError {
                            message: "Invalid varchar size".to_string(),
                            position: self.position,
                            line: 0,
                            column: 0,
                        })?
                    } else {
                        return Err(ParseError {
                            message: "Expected number for varchar size".to_string(),
                            position: self.position,
                            line: 0,
                            column: 0,
                        });
                    };
                    self.expect(Token::RightParen)?;
                    Ok(DataType::Varchar(Some(size)))
                } else {
                    Ok(DataType::Varchar(None))
                }
            }
            Token::Text => Ok(DataType::Text),
            Token::Boolean => Ok(DataType::Boolean),
            Token::Float => Ok(DataType::Float),
            Token::Double => Ok(DataType::Double),
            Token::Date => Ok(DataType::Date),
            Token::DateTime => Ok(DataType::DateTime),
            Token::Timestamp => Ok(DataType::Timestamp),
            token => Err(ParseError {
                message: format!("Unexpected data type: {:?}", token),
                position: self.position,
                line: 0,
                column: 0,
            }),
        }
    }

    /// Check if current tokens form a column constraint
    fn is_column_constraint(&self) -> bool {
        matches!(
            self.peek(),
            Token::Not
                | Token::Primary
                | Token::Unique
                | Token::Foreign
                | Token::Default
                | Token::Check
                | Token::Auto
        )
    }

    /// Parse column constraint
    fn parse_column_constraint(&mut self) -> Result<ColumnConstraint, ParseError> {
        match self.peek() {
            Token::Not => {
                self.consume();
                self.expect(Token::Null)?;
                Ok(ColumnConstraint::NotNull)
            }
            Token::Primary => {
                self.consume();
                self.expect(Token::Key)?;
                Ok(ColumnConstraint::PrimaryKey)
            }
            Token::Unique => {
                self.consume();
                Ok(ColumnConstraint::Unique)
            }
            Token::Foreign => {
                self.consume();
                self.expect(Token::Key)?;
                self.expect(Token::References)?;
                let references_table = if let Token::Identifier(table) = self.consume() {
                    table
                } else {
                    return Err(ParseError {
                        message: "Expected table name in REFERENCES".to_string(),
                        position: self.position,
                        line: 0,
                        column: 0,
                    });
                };
                self.expect(Token::LeftParen)?;
                let references_column = if let Token::Identifier(col) = self.consume() {
                    col
                } else {
                    return Err(ParseError {
                        message: "Expected column name in REFERENCES".to_string(),
                        position: self.position,
                        line: 0,
                        column: 0,
                    });
                };
                self.expect(Token::RightParen)?;
                Ok(ColumnConstraint::ForeignKey {
                    references_table,
                    references_column,
                })
            }
            Token::Default => {
                self.consume();
                let value = self.parse_expression()?;
                Ok(ColumnConstraint::Default(value))
            }
            Token::Check => {
                self.consume();
                self.expect(Token::LeftParen)?;
                let condition = self.parse_expression()?;
                self.expect(Token::RightParen)?;
                Ok(ColumnConstraint::Check(condition))
            }
            Token::Auto => {
                self.consume();
                self.expect(Token::Increment)?;
                Ok(ColumnConstraint::AutoIncrement)
            }
            _ => Err(ParseError {
                message: "Expected column constraint".to_string(),
                position: self.position,
                line: 0,
                column: 0,
            }),
        }
    }

    /// Check if current tokens form a table constraint
    fn is_table_constraint(&self) -> bool {
        matches!(
            self.peek(),
            Token::Primary | Token::Foreign | Token::Unique | Token::Check
        )
    }

    /// Parse table constraint
    fn parse_table_constraint(&mut self) -> Result<TableConstraint, ParseError> {
        match self.peek() {
            Token::Primary => {
                self.consume();
                self.expect(Token::Key)?;
                self.expect(Token::LeftParen)?;
                let mut columns = Vec::new();
                loop {
                    if let Token::Identifier(col) = self.consume() {
                        columns.push(col);
                    } else {
                        return Err(ParseError {
                            message: "Expected column name in PRIMARY KEY".to_string(),
                            position: self.position,
                            line: 0,
                            column: 0,
                        });
                    }
                    if matches!(self.peek(), Token::Comma) {
                        self.consume();
                    } else {
                        break;
                    }
                }
                self.expect(Token::RightParen)?;
                Ok(TableConstraint::PrimaryKey(columns))
            }
            Token::Foreign => {
                self.consume();
                self.expect(Token::Key)?;
                self.expect(Token::LeftParen)?;
                let mut columns = Vec::new();
                loop {
                    if let Token::Identifier(col) = self.consume() {
                        columns.push(col);
                    } else {
                        return Err(ParseError {
                            message: "Expected column name in FOREIGN KEY".to_string(),
                            position: self.position,
                            line: 0,
                            column: 0,
                        });
                    }
                    if matches!(self.peek(), Token::Comma) {
                        self.consume();
                    } else {
                        break;
                    }
                }
                self.expect(Token::RightParen)?;
                self.expect(Token::References)?;
                let references_table = if let Token::Identifier(table) = self.consume() {
                    table
                } else {
                    return Err(ParseError {
                        message: "Expected table name in REFERENCES".to_string(),
                        position: self.position,
                        line: 0,
                        column: 0,
                    });
                };
                self.expect(Token::LeftParen)?;
                let mut references_columns = Vec::new();
                loop {
                    if let Token::Identifier(col) = self.consume() {
                        references_columns.push(col);
                    } else {
                        return Err(ParseError {
                            message: "Expected column name in REFERENCES".to_string(),
                            position: self.position,
                            line: 0,
                            column: 0,
                        });
                    }
                    if matches!(self.peek(), Token::Comma) {
                        self.consume();
                    } else {
                        break;
                    }
                }
                self.expect(Token::RightParen)?;
                Ok(TableConstraint::ForeignKey {
                    columns,
                    references_table,
                    references_columns,
                })
            }
            Token::Unique => {
                self.consume();
                self.expect(Token::LeftParen)?;
                let mut columns = Vec::new();
                loop {
                    if let Token::Identifier(col) = self.consume() {
                        columns.push(col);
                    } else {
                        return Err(ParseError {
                            message: "Expected column name in UNIQUE".to_string(),
                            position: self.position,
                            line: 0,
                            column: 0,
                        });
                    }
                    if matches!(self.peek(), Token::Comma) {
                        self.consume();
                    } else {
                        break;
                    }
                }
                self.expect(Token::RightParen)?;
                Ok(TableConstraint::Unique(columns))
            }
            Token::Check => {
                self.consume();
                self.expect(Token::LeftParen)?;
                let condition = self.parse_expression()?;
                self.expect(Token::RightParen)?;
                Ok(TableConstraint::Check(condition))
            }
            _ => Err(ParseError {
                message: "Expected table constraint".to_string(),
                position: self.position,
                line: 0,
                column: 0,
            }),
        }
    }
}
