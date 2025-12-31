// SQL Query Executor - Executes parsed SQL statements
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

use crate::db::catalog::{ColumnSchema, CATALOG, data_type_to_string};
use crate::db::storage::{Row, Value, STORAGE};
use crate::db::sql::constants::{
    Statement, Assignment, ColumnDef, ColumnConstraint, 
    TableReference, Literal, BinaryOperator,
};
use crate::db::sql::parser::Expression;

/// Result of executing a SQL statement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutionResult {
    /// For DDL statements (CREATE, DROP, ALTER)
    Success { message: String },
    /// For INSERT, UPDATE, DELETE
    RowsAffected { count: usize },
    /// For SELECT
    Rows { columns: Vec<String>, rows: Vec<HashMap<String, serde_json::Value>> },
    /// For errors
    Error { message: String },
}

impl ExecutionResult {
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| r#"{"error":"serialization failed"}"#.to_string())
    }
}

/// The SQL Executor
pub struct Executor;

impl Executor {
    /// Execute a single SQL statement
    pub fn execute(stmt: &Statement) -> ExecutionResult {
        match stmt {
            Statement::CreateDatabase { name, if_not_exists } => {
                Self::execute_create_database(name, *if_not_exists)
            }
            Statement::CreateTable { name, columns, if_not_exists, .. } => {
                Self::execute_create_table(name, columns, *if_not_exists)
            }
            Statement::DropDatabase { name, if_exists } => {
                Self::execute_drop_database(name, *if_exists)
            }
            Statement::DropTable { name, if_exists } => {
                Self::execute_drop_table(name, *if_exists)
            }
            Statement::Insert { table, columns, values } => {
                Self::execute_insert(table, columns.as_ref(), values)
            }
            Statement::Select { projection, from, where_clause, limit, .. } => {
                Self::execute_select(projection, from.as_ref(), where_clause.as_ref(), *limit)
            }
            Statement::Update { table, assignments, where_clause } => {
                Self::execute_update(table, assignments, where_clause.as_ref())
            }
            Statement::Delete { table, where_clause } => {
                Self::execute_delete(table, where_clause.as_ref())
            }
            _ => ExecutionResult::Error {
                message: format!("Statement type not yet supported: {:?}", std::mem::discriminant(stmt)),
            },
        }
    }

    fn execute_create_database(name: &str, if_not_exists: bool) -> ExecutionResult {
        match CATALOG.create_database(name, if_not_exists) {
            Ok(()) => ExecutionResult::Success {
                message: format!("Database '{}' created", name),
            },
            Err(e) => ExecutionResult::Error { message: e },
        }
    }

    fn execute_drop_database(name: &str, if_exists: bool) -> ExecutionResult {
        match CATALOG.drop_database(name, if_exists) {
            Ok(()) => ExecutionResult::Success {
                message: format!("Database '{}' dropped", name),
            },
            Err(e) => ExecutionResult::Error { message: e },
        }
    }

    fn execute_create_table(name: &str, columns: &[ColumnDef], if_not_exists: bool) -> ExecutionResult {
        let column_schemas: Vec<ColumnSchema> = columns.iter().map(|col| {
            let is_primary = col.constraints.iter().any(|c| matches!(c, ColumnConstraint::PrimaryKey));
            let is_nullable = !col.constraints.iter().any(|c| matches!(c, ColumnConstraint::NotNull));
            
            ColumnSchema {
                name: col.name.clone(),
                data_type: data_type_to_string(&col.data_type),
                nullable: is_nullable,
                is_primary_key: is_primary,
            }
        }).collect();

        match CATALOG.create_table(name, column_schemas, if_not_exists) {
            Ok(()) => {
                // Also initialize storage for the table
                let _ = STORAGE.get_or_create_table(name);
                ExecutionResult::Success {
                    message: format!("Table '{}' created", name),
                }
            }
            Err(e) => ExecutionResult::Error { message: e },
        }
    }

    fn execute_drop_table(name: &str, if_exists: bool) -> ExecutionResult {
        match CATALOG.drop_table(name, if_exists) {
            Ok(()) => {
                let _ = STORAGE.drop_table(name);
                ExecutionResult::Success {
                    message: format!("Table '{}' dropped", name),
                }
            }
            Err(e) => ExecutionResult::Error { message: e },
        }
    }

    fn execute_insert(
        table: &str,
        columns: Option<&Vec<String>>,
        values: &[Vec<Expression>],
    ) -> ExecutionResult {
        // Verify table exists
        let schema = match CATALOG.get_table(table) {
            Ok(s) => s,
            Err(e) => return ExecutionResult::Error { message: e },
        };

        let col_names: Vec<String> = if let Some(cols) = columns {
            cols.clone()
        } else {
            schema.columns.iter().map(|c| c.name.clone()).collect()
        };

        let mut total_inserted = 0;
        for row_values in values {
            if row_values.len() != col_names.len() {
                return ExecutionResult::Error {
                    message: format!(
                        "Column count mismatch: expected {}, got {}",
                        col_names.len(),
                        row_values.len()
                    ),
                };
            }

            let mut row: Row = HashMap::new();
            for (i, expr) in row_values.iter().enumerate() {
                let value = Self::eval_expression(expr, &HashMap::new());
                row.insert(col_names[i].clone(), value);
            }

            match STORAGE.insert(table, row) {
                Ok(n) => total_inserted += n,
                Err(e) => return ExecutionResult::Error { message: e },
            }
        }

        ExecutionResult::RowsAffected { count: total_inserted }
    }

    fn execute_select(
        projection: &[Expression],
        from: Option<&TableReference>,
        where_clause: Option<&Expression>,
        limit: Option<u64>,
    ) -> ExecutionResult {
        let table_name = match from {
            Some(TableReference::Table { name, .. }) => name.as_str(),
            Some(TableReference::Subquery { .. }) => {
                return ExecutionResult::Error {
                    message: "Subqueries not yet supported".to_string(),
                };
            }
            None => {
                // SELECT without FROM (e.g., SELECT 1+1)
                let values: Vec<serde_json::Value> = projection
                    .iter()
                    .map(|expr| {
                        let v = Self::eval_expression(expr, &HashMap::new());
                        Self::value_to_json(&v)
                    })
                    .collect();
                
                return ExecutionResult::Rows {
                    columns: (0..projection.len()).map(|i| format!("column{}", i)).collect(),
                    rows: vec![values.into_iter().enumerate().map(|(i, v)| (format!("column{}", i), v)).collect()],
                };
            }
        };

        // Get columns to select
        let col_names: Vec<String> = projection
            .iter()
            .filter_map(|expr| match expr {
                Expression::Identifier(name) => Some(name.clone()),
                Expression::QualifiedColumn { column, .. } => Some(column.clone()),
                _ => None,
            })
            .collect();

        // Create predicate from WHERE clause
        let predicate = |row: &Row| -> bool {
            match where_clause {
                Some(expr) => Self::eval_condition(expr, row),
                None => true,
            }
        };

        match STORAGE.select(table_name, &col_names, predicate) {
            Ok(mut rows) => {
                // Apply limit
                if let Some(lim) = limit {
                    rows.truncate(lim as usize);
                }

                // Convert to JSON-friendly format
                let json_rows: Vec<HashMap<String, serde_json::Value>> = rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|(k, v)| (k, Self::value_to_json(&v)))
                            .collect()
                    })
                    .collect();

                let columns = if col_names.is_empty() || col_names.contains(&"*".to_string()) {
                    json_rows.first().map(|r| r.keys().cloned().collect()).unwrap_or_default()
                } else {
                    col_names
                };

                ExecutionResult::Rows { columns, rows: json_rows }
            }
            Err(e) => ExecutionResult::Error { message: e },
        }
    }

    fn execute_update(
        table: &str,
        assignments: &[Assignment],
        where_clause: Option<&Expression>,
    ) -> ExecutionResult {
        let mut updates: HashMap<String, Value> = HashMap::new();
        for assignment in assignments {
            let value = Self::eval_expression(&assignment.value, &HashMap::new());
            updates.insert(assignment.column.clone(), value);
        }

        let predicate = |row: &Row| -> bool {
            match where_clause {
                Some(expr) => Self::eval_condition(expr, row),
                None => true,
            }
        };

        match STORAGE.update(table, &updates, predicate) {
            Ok(count) => ExecutionResult::RowsAffected { count },
            Err(e) => ExecutionResult::Error { message: e },
        }
    }

    fn execute_delete(
        table: &str,
        where_clause: Option<&Expression>,
    ) -> ExecutionResult {
        let predicate = |row: &Row| -> bool {
            match where_clause {
                Some(expr) => Self::eval_condition(expr, row),
                None => true,
            }
        };

        match STORAGE.delete(table, predicate) {
            Ok(count) => ExecutionResult::RowsAffected { count },
            Err(e) => ExecutionResult::Error { message: e },
        }
    }

    /// Evaluate an expression to a Value
    fn eval_expression(expr: &Expression, row: &Row) -> Value {
        match expr {
            Expression::Literal(lit) => Value::from_literal(lit),
            Expression::Identifier(name) => {
                row.get(name).cloned().unwrap_or(Value::Null)
            }
            Expression::QualifiedColumn { column, .. } => {
                row.get(column).cloned().unwrap_or(Value::Null)
            }
            Expression::BinaryOp { left, operator, right } => {
                let l = Self::eval_expression(left, row);
                let r = Self::eval_expression(right, row);
                Self::eval_binary_op(&l, operator, &r)
            }
            _ => Value::Null,
        }
    }

    /// Evaluate a binary operation
    fn eval_binary_op(left: &Value, op: &BinaryOperator, right: &Value) -> Value {
        match op {
            BinaryOperator::Plus => {
                match (left, right) {
                    (Value::Integer(a), Value::Integer(b)) => Value::Integer(a + b),
                    (Value::Float(a), Value::Float(b)) => Value::Float(a + b),
                    (Value::Integer(a), Value::Float(b)) => Value::Float(*a as f64 + b),
                    (Value::Float(a), Value::Integer(b)) => Value::Float(a + *b as f64),
                    _ => Value::Null,
                }
            }
            BinaryOperator::Minus => {
                match (left, right) {
                    (Value::Integer(a), Value::Integer(b)) => Value::Integer(a - b),
                    (Value::Float(a), Value::Float(b)) => Value::Float(a - b),
                    _ => Value::Null,
                }
            }
            BinaryOperator::Multiply => {
                match (left, right) {
                    (Value::Integer(a), Value::Integer(b)) => Value::Integer(a * b),
                    (Value::Float(a), Value::Float(b)) => Value::Float(a * b),
                    _ => Value::Null,
                }
            }
            BinaryOperator::Divide => {
                match (left, right) {
                    (Value::Integer(a), Value::Integer(b)) if *b != 0 => Value::Integer(a / b),
                    (Value::Float(a), Value::Float(b)) if *b != 0.0 => Value::Float(a / b),
                    _ => Value::Null,
                }
            }
            _ => Value::Null,
        }
    }

    /// Evaluate a condition expression to a boolean
    fn eval_condition(expr: &Expression, row: &Row) -> bool {
        match expr {
            Expression::Literal(Literal::Boolean(b)) => *b,
            Expression::BinaryOp { left, operator, right } => {
                let l = Self::eval_expression(left, row);
                let r = Self::eval_expression(right, row);
                
                match operator {
                    BinaryOperator::Equals => l == r,
                    BinaryOperator::NotEquals => l != r,
                    BinaryOperator::LessThan => Self::compare(&l, &r) < 0,
                    BinaryOperator::LessThanOrEqual => Self::compare(&l, &r) <= 0,
                    BinaryOperator::GreaterThan => Self::compare(&l, &r) > 0,
                    BinaryOperator::GreaterThanOrEqual => Self::compare(&l, &r) >= 0,
                    BinaryOperator::And => {
                        Self::eval_condition(left, row) && Self::eval_condition(right, row)
                    }
                    BinaryOperator::Or => {
                        Self::eval_condition(left, row) || Self::eval_condition(right, row)
                    }
                    BinaryOperator::Like => {
                        if let (Value::Text(text), Value::Text(pattern)) = (&l, &r) {
                            Self::match_like(text, pattern)
                        } else {
                            false
                        }
                    }
                    _ => false,
                }
            }
            _ => true,
        }
    }

    /// Compare two values (-1, 0, 1)
    fn compare(left: &Value, right: &Value) -> i8 {
        match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => {
                if a < b { -1 } else if a > b { 1 } else { 0 }
            }
            (Value::Float(a), Value::Float(b)) => {
                if a < b { -1.0 as i8 } else if a > b { 1 } else { 0 }
            }
            (Value::Text(a), Value::Text(b)) => {
                if a < b { -1 } else if a > b { 1 } else { 0 }
            }
            _ => 0,
        }
    }

    /// Simple LIKE pattern matching (% and _ wildcards)
    fn match_like(text: &str, pattern: &str) -> bool {
        let regex_pattern = pattern
            .replace('%', ".*")
            .replace('_', ".");
        regex::Regex::new(&format!("^{}$", regex_pattern))
            .map(|re| re.is_match(text))
            .unwrap_or(false)
    }

    /// Convert Value to JSON
    fn value_to_json(value: &Value) -> serde_json::Value {
        match value {
            Value::Null => serde_json::Value::Null,
            Value::Integer(i) => serde_json::json!(i),
            Value::Float(f) => serde_json::json!(f),
            Value::Text(s) => serde_json::json!(s),
            Value::Boolean(b) => serde_json::json!(b),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::sql::constants::Literal;

    // ==========================================
    // ExecutionResult Tests
    // ==========================================

    #[test]
    fn test_execution_result_success_to_json() {
        let result = ExecutionResult::Success {
            message: "Table created".to_string(),
        };
        let json = result.to_json();
        assert!(json.contains("Table created"));
    }

    #[test]
    fn test_execution_result_rows_affected_to_json() {
        let result = ExecutionResult::RowsAffected { count: 5 };
        let json = result.to_json();
        assert!(json.contains("5"));
    }

    #[test]
    fn test_execution_result_error_to_json() {
        let result = ExecutionResult::Error {
            message: "Something failed".to_string(),
        };
        let json = result.to_json();
        assert!(json.contains("Something failed"));
    }

    // ==========================================
    // eval_expression Tests
    // ==========================================

    #[test]
    fn test_eval_expression_literal_integer() {
        let expr = Expression::Literal(Literal::Number("42".to_string()));
        let result = Executor::eval_expression(&expr, &HashMap::new());
        assert_eq!(result, Value::Integer(42));
    }

    #[test]
    fn test_eval_expression_literal_float() {
        let expr = Expression::Literal(Literal::Number("3.14".to_string()));
        let result = Executor::eval_expression(&expr, &HashMap::new());
        assert_eq!(result, Value::Float(3.14));
    }

    #[test]
    fn test_eval_expression_literal_string() {
        let expr = Expression::Literal(Literal::String("hello".to_string()));
        let result = Executor::eval_expression(&expr, &HashMap::new());
        assert_eq!(result, Value::Text("hello".to_string()));
    }

    #[test]
    fn test_eval_expression_literal_boolean() {
        let expr = Expression::Literal(Literal::Boolean(true));
        let result = Executor::eval_expression(&expr, &HashMap::new());
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_eval_expression_literal_null() {
        let expr = Expression::Literal(Literal::Null);
        let result = Executor::eval_expression(&expr, &HashMap::new());
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_eval_expression_identifier_found() {
        let expr = Expression::Identifier("name".to_string());
        let mut row = HashMap::new();
        row.insert("name".to_string(), Value::Text("Alice".to_string()));
        
        let result = Executor::eval_expression(&expr, &row);
        assert_eq!(result, Value::Text("Alice".to_string()));
    }

    #[test]
    fn test_eval_expression_identifier_not_found() {
        let expr = Expression::Identifier("missing".to_string());
        let result = Executor::eval_expression(&expr, &HashMap::new());
        assert_eq!(result, Value::Null);
    }

    // ==========================================
    // eval_binary_op Tests
    // ==========================================

    #[test]
    fn test_eval_binary_op_plus_integers() {
        let result = Executor::eval_binary_op(
            &Value::Integer(10),
            &BinaryOperator::Plus,
            &Value::Integer(5),
        );
        assert_eq!(result, Value::Integer(15));
    }

    #[test]
    fn test_eval_binary_op_plus_floats() {
        let result = Executor::eval_binary_op(
            &Value::Float(10.5),
            &BinaryOperator::Plus,
            &Value::Float(4.5),
        );
        assert_eq!(result, Value::Float(15.0));
    }

    #[test]
    fn test_eval_binary_op_plus_mixed() {
        let result = Executor::eval_binary_op(
            &Value::Integer(10),
            &BinaryOperator::Plus,
            &Value::Float(5.5),
        );
        assert_eq!(result, Value::Float(15.5));
    }

    #[test]
    fn test_eval_binary_op_minus_integers() {
        let result = Executor::eval_binary_op(
            &Value::Integer(10),
            &BinaryOperator::Minus,
            &Value::Integer(3),
        );
        assert_eq!(result, Value::Integer(7));
    }

    #[test]
    fn test_eval_binary_op_multiply_integers() {
        let result = Executor::eval_binary_op(
            &Value::Integer(6),
            &BinaryOperator::Multiply,
            &Value::Integer(7),
        );
        assert_eq!(result, Value::Integer(42));
    }

    #[test]
    fn test_eval_binary_op_divide_integers() {
        let result = Executor::eval_binary_op(
            &Value::Integer(20),
            &BinaryOperator::Divide,
            &Value::Integer(4),
        );
        assert_eq!(result, Value::Integer(5));
    }

    #[test]
    fn test_eval_binary_op_divide_by_zero() {
        let result = Executor::eval_binary_op(
            &Value::Integer(10),
            &BinaryOperator::Divide,
            &Value::Integer(0),
        );
        assert_eq!(result, Value::Null);
    }

    // ==========================================
    // eval_condition Tests
    // ==========================================

    #[test]
    fn test_eval_condition_boolean_literal_true() {
        let expr = Expression::Literal(Literal::Boolean(true));
        let result = Executor::eval_condition(&expr, &HashMap::new());
        assert!(result);
    }

    #[test]
    fn test_eval_condition_boolean_literal_false() {
        let expr = Expression::Literal(Literal::Boolean(false));
        let result = Executor::eval_condition(&expr, &HashMap::new());
        assert!(!result);
    }

    #[test]
    fn test_eval_condition_equals_true() {
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(Literal::Number("5".to_string()))),
            operator: BinaryOperator::Equals,
            right: Box::new(Expression::Literal(Literal::Number("5".to_string()))),
        };
        let result = Executor::eval_condition(&expr, &HashMap::new());
        assert!(result);
    }

    #[test]
    fn test_eval_condition_equals_false() {
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(Literal::Number("5".to_string()))),
            operator: BinaryOperator::Equals,
            right: Box::new(Expression::Literal(Literal::Number("10".to_string()))),
        };
        let result = Executor::eval_condition(&expr, &HashMap::new());
        assert!(!result);
    }

    #[test]
    fn test_eval_condition_not_equals() {
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(Literal::Number("5".to_string()))),
            operator: BinaryOperator::NotEquals,
            right: Box::new(Expression::Literal(Literal::Number("10".to_string()))),
        };
        let result = Executor::eval_condition(&expr, &HashMap::new());
        assert!(result);
    }

    #[test]
    fn test_eval_condition_less_than() {
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(Literal::Number("5".to_string()))),
            operator: BinaryOperator::LessThan,
            right: Box::new(Expression::Literal(Literal::Number("10".to_string()))),
        };
        let result = Executor::eval_condition(&expr, &HashMap::new());
        assert!(result);
    }

    #[test]
    fn test_eval_condition_greater_than() {
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(Literal::Number("10".to_string()))),
            operator: BinaryOperator::GreaterThan,
            right: Box::new(Expression::Literal(Literal::Number("5".to_string()))),
        };
        let result = Executor::eval_condition(&expr, &HashMap::new());
        assert!(result);
    }

    #[test]
    fn test_eval_condition_and_both_true() {
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(Literal::Boolean(true))),
            operator: BinaryOperator::And,
            right: Box::new(Expression::Literal(Literal::Boolean(true))),
        };
        let result = Executor::eval_condition(&expr, &HashMap::new());
        assert!(result);
    }

    #[test]
    fn test_eval_condition_and_one_false() {
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(Literal::Boolean(true))),
            operator: BinaryOperator::And,
            right: Box::new(Expression::Literal(Literal::Boolean(false))),
        };
        let result = Executor::eval_condition(&expr, &HashMap::new());
        assert!(!result);
    }

    #[test]
    fn test_eval_condition_or_one_true() {
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(Literal::Boolean(false))),
            operator: BinaryOperator::Or,
            right: Box::new(Expression::Literal(Literal::Boolean(true))),
        };
        let result = Executor::eval_condition(&expr, &HashMap::new());
        assert!(result);
    }

    // ==========================================
    // compare Tests
    // ==========================================

    #[test]
    fn test_compare_integers_less() {
        let result = Executor::compare(&Value::Integer(5), &Value::Integer(10));
        assert_eq!(result, -1);
    }

    #[test]
    fn test_compare_integers_greater() {
        let result = Executor::compare(&Value::Integer(10), &Value::Integer(5));
        assert_eq!(result, 1);
    }

    #[test]
    fn test_compare_integers_equal() {
        let result = Executor::compare(&Value::Integer(5), &Value::Integer(5));
        assert_eq!(result, 0);
    }

    #[test]
    fn test_compare_strings() {
        let result = Executor::compare(
            &Value::Text("apple".to_string()),
            &Value::Text("banana".to_string()),
        );
        assert_eq!(result, -1);
    }

    // ==========================================
    // match_like Tests
    // ==========================================

    #[test]
    fn test_match_like_exact() {
        assert!(Executor::match_like("hello", "hello"));
    }

    #[test]
    fn test_match_like_percent_end() {
        assert!(Executor::match_like("hello world", "hello%"));
    }

    #[test]
    fn test_match_like_percent_start() {
        assert!(Executor::match_like("hello world", "%world"));
    }

    #[test]
    fn test_match_like_percent_both() {
        assert!(Executor::match_like("hello world", "%lo wo%"));
    }

    #[test]
    fn test_match_like_underscore() {
        assert!(Executor::match_like("cat", "c_t"));
    }

    #[test]
    fn test_match_like_no_match() {
        assert!(!Executor::match_like("hello", "world"));
    }

    // ==========================================
    // value_to_json Tests
    // ==========================================

    #[test]
    fn test_value_to_json_null() {
        let result = Executor::value_to_json(&Value::Null);
        assert!(result.is_null());
    }

    #[test]
    fn test_value_to_json_integer() {
        let result = Executor::value_to_json(&Value::Integer(42));
        assert_eq!(result, serde_json::json!(42));
    }

    #[test]
    fn test_value_to_json_float() {
        let result = Executor::value_to_json(&Value::Float(3.14));
        assert_eq!(result, serde_json::json!(3.14));
    }

    #[test]
    fn test_value_to_json_text() {
        let result = Executor::value_to_json(&Value::Text("hello".to_string()));
        assert_eq!(result, serde_json::json!("hello"));
    }

    #[test]
    fn test_value_to_json_boolean() {
        let result = Executor::value_to_json(&Value::Boolean(true));
        assert_eq!(result, serde_json::json!(true));
    }
}
