// SQL Module - Parser and query interface
pub mod constants;
pub mod parser;

// Re-export key types for external use
pub use constants::{
    Statement, Token, ParseError, Literal, BinaryOperator, UnaryOperator,
    JoinType, OrderDirection, ColumnDef, ColumnConstraint, TableReference,
    Join, OrderBy, Assignment, TableConstraint, AlterAction, TransactionStatement,
    Tokenizer,
};
pub use parser::{SqlParser, Expression, DataType};

use crate::db::executor::{Executor, ExecutionResult};

/// Execute a SQL query string and return the result
pub fn execute_sql(query: &str) -> ExecutionResult {
    // Parse the SQL
    match SqlParser::parse(query) {
        Ok(statements) => {
            if statements.is_empty() {
                return ExecutionResult::Error {
                    message: "No SQL statements found".to_string(),
                };
            }
            
            // Execute each statement (for now, just the first one)
            // In the future, we could support multi-statement execution
            let stmt = &statements[0];
            Executor::execute(stmt)
        }
        Err(e) => ExecutionResult::Error {
            message: format!("SQL parse error: {}", e),
        },
    }
}

/// Parse SQL without executing (for validation)
pub fn parse_sql(query: &str) -> Result<Vec<Statement>, String> {
    SqlParser::parse(query).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==========================================
    // parse_sql Tests
    // ==========================================

    #[test]
    fn test_parse_sql_simple_select() {
        let result = parse_sql("SELECT * FROM users");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_parse_sql_select_with_columns() {
        let result = parse_sql("SELECT id, name FROM users");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_sql_select_with_where() {
        let result = parse_sql("SELECT * FROM users WHERE id = 1");
        assert!(result.is_ok());
    }

    #[test]  
    fn test_parse_sql_insert() {
        let result = parse_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_sql_create_table() {
        let result = parse_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(255))");
        assert!(result.is_ok());
        
        let stmts = result.unwrap();
        assert!(matches!(stmts[0], Statement::CreateTable { .. }));
    }

    #[test]
    fn test_parse_sql_create_database() {
        let result = parse_sql("CREATE DATABASE mydb");
        assert!(result.is_ok());
        
        let stmts = result.unwrap();
        assert!(matches!(stmts[0], Statement::CreateDatabase { .. }));
    }

    #[test]
    fn test_parse_sql_drop_table() {
        let result = parse_sql("DROP TABLE users");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_sql_update() {
        let result = parse_sql("UPDATE users SET name = 'Bob' WHERE id = 1");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_sql_delete() {
        let result = parse_sql("DELETE FROM users WHERE id = 1");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_sql_invalid() {
        let result = parse_sql("THIS IS NOT SQL");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_sql_empty() {
        let result = parse_sql("");
        // Empty input should return Ok with empty vec
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    // ==========================================
    // execute_sql Tests
    // ==========================================

    #[test]
    fn test_execute_sql_empty() {
        let result = execute_sql("");
        match result {
            ExecutionResult::Error { message } => {
                assert!(message.contains("No SQL statements"));
            }
            _ => panic!("Expected error for empty SQL"),
        }
    }

    #[test]
    fn test_execute_sql_invalid() {
        let result = execute_sql("NOT VALID SQL");
        match result {
            ExecutionResult::Error { message } => {
                assert!(message.contains("parse error"));
            }
            _ => panic!("Expected parse error"),
        }
    }

    #[test]
    fn test_execute_sql_select_literal() {
        let result = execute_sql("SELECT 1");
        match result {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("Expected rows result"),
        }
    }
}
