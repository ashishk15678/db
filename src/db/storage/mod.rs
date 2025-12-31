// In-memory Storage Engine for table data
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::db::catalog::{TableSchema, CATALOG};
use crate::db::sql::parser::Expression;
use crate::db::sql::constants::Literal;

/// Represents a value in a row
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Boolean(bool),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Text(s) => write!(f, "{}", s),
            Value::Boolean(b) => write!(f, "{}", b),
        }
    }
}

impl Value {
    /// Convert from parser Literal to storage Value
    pub fn from_literal(lit: &Literal) -> Self {
        match lit {
            Literal::Null => Value::Null,
            Literal::Boolean(b) => Value::Boolean(*b),
            Literal::String(s) => Value::Text(s.clone()),
            Literal::Number(n) => {
                if let Ok(i) = n.parse::<i64>() {
                    Value::Integer(i)
                } else if let Ok(f) = n.parse::<f64>() {
                    Value::Float(f)
                } else {
                    Value::Text(n.clone())
                }
            }
        }
    }
}

/// A row of data as a map from column name to value
pub type Row = HashMap<String, Value>;

/// In-memory table data storage
#[derive(Debug, Default)]
pub struct TableData {
    pub rows: Vec<Row>,
}

impl TableData {
    pub fn new() -> Self {
        Self { rows: Vec::new() }
    }

    /// Insert a row
    pub fn insert(&mut self, row: Row) -> usize {
        self.rows.push(row);
        1
    }

    /// Select rows matching a predicate
    pub fn select<F>(&self, predicate: F) -> Vec<&Row>
    where
        F: Fn(&Row) -> bool,
    {
        self.rows.iter().filter(|row| predicate(*row)).collect()
    }

    /// Select specific columns from rows matching a predicate  
    pub fn select_columns<F>(&self, columns: &[String], predicate: F) -> Vec<Row>
    where
        F: Fn(&Row) -> bool,
    {
        self.rows
            .iter()
            .filter(|row| predicate(*row))
            .map(|row| {
                if columns.is_empty() || columns.iter().any(|c| c == "*") {
                    row.clone()
                } else {
                    columns
                        .iter()
                        .filter_map(|col| row.get(col).map(|v| (col.clone(), v.clone())))
                        .collect()
                }
            })
            .collect()
    }

    /// Delete rows matching a predicate, return count deleted
    pub fn delete<F>(&mut self, predicate: F) -> usize
    where
        F: Fn(&Row) -> bool,
    {
        let original_len = self.rows.len();
        self.rows.retain(|row| !predicate(row));
        original_len - self.rows.len()
    }

    /// Update rows matching a predicate
    pub fn update<F>(&mut self, updates: &HashMap<String, Value>, predicate: F) -> usize
    where
        F: Fn(&Row) -> bool,
    {
        let mut count = 0;
        for row in &mut self.rows {
            if predicate(row) {
                for (col, val) in updates {
                    row.insert(col.clone(), val.clone());
                }
                count += 1;
            }
        }
        count
    }

    /// Get row count
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Serializable storage data for persistence
#[derive(Debug, Default, Serialize, Deserialize)]
struct StorageData {
    tables: HashMap<String, Vec<Row>>,
}

/// Global storage manager for all tables with persistence
pub struct Storage {
    tables: Arc<RwLock<HashMap<String, TableData>>>,
    storage_path: std::path::PathBuf,
}

impl Storage {
    pub fn new() -> Self {
        let storage_path = Self::get_default_path();
        let mut storage = Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            storage_path,
        };
        // Load existing data from disk
        let _ = storage.load();
        storage
    }

    fn get_default_path() -> std::path::PathBuf {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        let path = std::path::PathBuf::from(home).join(".butterfly_db");
        std::fs::create_dir_all(&path).ok();
        path.join("data.json")
    }

    /// Save all table data to disk
    pub fn save(&self) -> Result<(), String> {
        let tables = self.tables.read().map_err(|e| e.to_string())?;
        
        // Convert TableData to serializable format
        let mut data = StorageData::default();
        for (name, table_data) in tables.iter() {
            data.tables.insert(name.clone(), table_data.rows.clone());
        }
        
        let json = serde_json::to_string_pretty(&data)
            .map_err(|e| e.to_string())?;
        std::fs::write(&self.storage_path, json)
            .map_err(|e| e.to_string())?;
        
        Ok(())
    }

    /// Load table data from disk
    pub fn load(&mut self) -> Result<(), String> {
        if !self.storage_path.exists() {
            return Ok(());
        }

        let content = std::fs::read_to_string(&self.storage_path)
            .map_err(|e| e.to_string())?;
        
        let data: StorageData = serde_json::from_str(&content)
            .map_err(|e| e.to_string())?;
        
        let mut tables = self.tables.write().map_err(|e| e.to_string())?;
        for (name, rows) in data.tables {
            let mut table_data = TableData::new();
            table_data.rows = rows;
            tables.insert(name, table_data);
        }
        
        Ok(())
    }

    /// Get or create table data storage
    pub fn get_or_create_table(&self, table_name: &str) -> Result<(), String> {
        let mut tables = self.tables.write().map_err(|e| e.to_string())?;
        if !tables.contains_key(table_name) {
            tables.insert(table_name.to_string(), TableData::new());
        }
        Ok(())
    }

    /// Insert a row into a table (auto-persists)
    pub fn insert(&self, table_name: &str, row: Row) -> Result<usize, String> {
        self.get_or_create_table(table_name)?;
        
        let mut tables = self.tables.write().map_err(|e| e.to_string())?;
        let table = tables
            .get_mut(table_name)
            .ok_or(format!("Table '{}' not found", table_name))?;
        
        let count = table.insert(row);
        drop(tables);
        
        // Auto-persist after insert
        let _ = self.save();
        
        Ok(count)
    }

    /// Select from a table
    pub fn select<F>(&self, table_name: &str, columns: &[String], predicate: F) -> Result<Vec<Row>, String>
    where
        F: Fn(&Row) -> bool,
    {
        let tables = self.tables.read().map_err(|e| e.to_string())?;
        let table = tables
            .get(table_name)
            .ok_or(format!("Table '{}' not found", table_name))?;
        
        Ok(table.select_columns(columns, predicate))
    }

    /// Delete from a table (auto-persists)
    pub fn delete<F>(&self, table_name: &str, predicate: F) -> Result<usize, String>
    where
        F: Fn(&Row) -> bool,
    {
        let mut tables = self.tables.write().map_err(|e| e.to_string())?;
        let table = tables
            .get_mut(table_name)
            .ok_or(format!("Table '{}' not found", table_name))?;
        
        let count = table.delete(predicate);
        drop(tables);
        
        // Auto-persist after delete
        let _ = self.save();
        
        Ok(count)
    }

    /// Update a table (auto-persists)
    pub fn update<F>(&self, table_name: &str, updates: &HashMap<String, Value>, predicate: F) -> Result<usize, String>
    where
        F: Fn(&Row) -> bool,
    {
        let mut tables = self.tables.write().map_err(|e| e.to_string())?;
        let table = tables
            .get_mut(table_name)
            .ok_or(format!("Table '{}' not found", table_name))?;
        
        let count = table.update(updates, predicate);
        drop(tables);
        
        // Auto-persist after update
        let _ = self.save();
        
        Ok(count)
    }

    /// Drop a table's data (auto-persists)
    pub fn drop_table(&self, table_name: &str) -> Result<(), String> {
        let mut tables = self.tables.write().map_err(|e| e.to_string())?;
        tables.remove(table_name);
        drop(tables);
        
        // Auto-persist after drop
        let _ = self.save();
        
        Ok(())
    }
}

// Global storage instance (loads data from disk on creation)
lazy_static::lazy_static! {
    pub static ref STORAGE: Storage = Storage::new();
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==========================================
    // Value Tests
    // ==========================================

    #[test]
    fn test_value_null() {
        let value = Value::Null;
        assert_eq!(format!("{}", value), "NULL");
    }

    #[test]
    fn test_value_integer() {
        let value = Value::Integer(42);
        assert_eq!(format!("{}", value), "42");
    }

    #[test]
    fn test_value_float() {
        let value = Value::Float(3.14);
        assert_eq!(format!("{}", value), "3.14");
    }

    #[test]
    fn test_value_text() {
        let value = Value::Text("hello".to_string());
        assert_eq!(format!("{}", value), "hello");
    }

    #[test]
    fn test_value_boolean() {
        assert_eq!(format!("{}", Value::Boolean(true)), "true");
        assert_eq!(format!("{}", Value::Boolean(false)), "false");
    }

    #[test]
    fn test_value_from_literal_null() {
        let value = Value::from_literal(&Literal::Null);
        assert_eq!(value, Value::Null);
    }

    #[test]
    fn test_value_from_literal_boolean() {
        assert_eq!(Value::from_literal(&Literal::Boolean(true)), Value::Boolean(true));
        assert_eq!(Value::from_literal(&Literal::Boolean(false)), Value::Boolean(false));
    }

    #[test]
    fn test_value_from_literal_string() {
        let value = Value::from_literal(&Literal::String("test".to_string()));
        assert_eq!(value, Value::Text("test".to_string()));
    }

    #[test]
    fn test_value_from_literal_integer() {
        let value = Value::from_literal(&Literal::Number("123".to_string()));
        assert_eq!(value, Value::Integer(123));
    }

    #[test]
    fn test_value_from_literal_float() {
        let value = Value::from_literal(&Literal::Number("3.14".to_string()));
        assert_eq!(value, Value::Float(3.14));
    }

    // ==========================================
    // TableData Tests
    // ==========================================

    #[test]
    fn test_table_data_new() {
        let table = TableData::new();
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn test_table_data_insert() {
        let mut table = TableData::new();
        
        let mut row = Row::new();
        row.insert("id".to_string(), Value::Integer(1));
        row.insert("name".to_string(), Value::Text("Alice".to_string()));
        
        let count = table.insert(row);
        assert_eq!(count, 1);
        assert_eq!(table.len(), 1);
        assert!(!table.is_empty());
    }

    #[test]
    fn test_table_data_insert_multiple() {
        let mut table = TableData::new();
        
        for i in 1..=5 {
            let mut row = Row::new();
            row.insert("id".to_string(), Value::Integer(i));
            table.insert(row);
        }
        
        assert_eq!(table.len(), 5);
    }

    #[test]
    fn test_table_data_select_all() {
        let mut table = TableData::new();
        
        let mut row = Row::new();
        row.insert("id".to_string(), Value::Integer(1));
        table.insert(row);
        
        let results = table.select(|_| true);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_table_data_select_with_predicate() {
        let mut table = TableData::new();
        
        for i in 1..=10 {
            let mut row = Row::new();
            row.insert("id".to_string(), Value::Integer(i));
            table.insert(row);
        }
        
        // Select only even IDs
        let results = table.select(|row| {
            if let Some(Value::Integer(id)) = row.get("id") {
                *id % 2 == 0
            } else {
                false
            }
        });
        
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_table_data_select_columns() {
        let mut table = TableData::new();
        
        let mut row = Row::new();
        row.insert("id".to_string(), Value::Integer(1));
        row.insert("name".to_string(), Value::Text("Alice".to_string()));
        row.insert("email".to_string(), Value::Text("alice@example.com".to_string()));
        table.insert(row);
        
        let columns = vec!["id".to_string(), "name".to_string()];
        let results = table.select_columns(&columns, |_| true);
        
        assert_eq!(results.len(), 1);
        assert!(results[0].contains_key("id"));
        assert!(results[0].contains_key("name"));
        assert!(!results[0].contains_key("email"));
    }

    #[test]
    fn test_table_data_select_star() {
        let mut table = TableData::new();
        
        let mut row = Row::new();
        row.insert("id".to_string(), Value::Integer(1));
        row.insert("name".to_string(), Value::Text("Alice".to_string()));
        table.insert(row);
        
        let columns = vec!["*".to_string()];
        let results = table.select_columns(&columns, |_| true);
        
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].len(), 2); // All columns
    }

    #[test]
    fn test_table_data_delete() {
        let mut table = TableData::new();
        
        for i in 1..=5 {
            let mut row = Row::new();
            row.insert("id".to_string(), Value::Integer(i));
            table.insert(row);
        }
        
        // Delete id = 3
        let deleted = table.delete(|row| {
            row.get("id") == Some(&Value::Integer(3))
        });
        
        assert_eq!(deleted, 1);
        assert_eq!(table.len(), 4);
    }

    #[test]
    fn test_table_data_delete_multiple() {
        let mut table = TableData::new();
        
        for i in 1..=10 {
            let mut row = Row::new();
            row.insert("id".to_string(), Value::Integer(i));
            table.insert(row);
        }
        
        // Delete all even IDs
        let deleted = table.delete(|row| {
            if let Some(Value::Integer(id)) = row.get("id") {
                *id % 2 == 0
            } else {
                false
            }
        });
        
        assert_eq!(deleted, 5);
        assert_eq!(table.len(), 5);
    }

    #[test]
    fn test_table_data_update() {
        let mut table = TableData::new();
        
        let mut row = Row::new();
        row.insert("id".to_string(), Value::Integer(1));
        row.insert("name".to_string(), Value::Text("Alice".to_string()));
        table.insert(row);
        
        let mut updates = HashMap::new();
        updates.insert("name".to_string(), Value::Text("Bob".to_string()));
        
        let count = table.update(&updates, |row| {
            row.get("id") == Some(&Value::Integer(1))
        });
        
        assert_eq!(count, 1);
        
        // Verify the update
        let results = table.select(|_| true);
        assert_eq!(results[0].get("name"), Some(&Value::Text("Bob".to_string())));
    }

    // ==========================================
    // Storage Tests
    // ==========================================

    #[test]
    fn test_storage_new() {
        let storage = Storage::new();
        // Should not panic
        assert!(storage.get_or_create_table("test").is_ok());
    }

    #[test]
    fn test_storage_insert_and_select() {
        let storage = Storage::new();
        
        let mut row = Row::new();
        row.insert("id".to_string(), Value::Integer(1));
        row.insert("name".to_string(), Value::Text("Test".to_string()));
        
        storage.insert("test_table", row).unwrap();
        
        let results = storage.select("test_table", &[], |_| true).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("id"), Some(&Value::Integer(1)));
    }

    #[test]
    fn test_storage_delete() {
        let storage = Storage::new();
        
        let mut row = Row::new();
        row.insert("id".to_string(), Value::Integer(1));
        storage.insert("del_table", row).unwrap();
        
        let deleted = storage.delete("del_table", |_| true).unwrap();
        assert_eq!(deleted, 1);
        
        let results = storage.select("del_table", &[], |_| true).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_storage_update() {
        let storage = Storage::new();
        
        let mut row = Row::new();
        row.insert("id".to_string(), Value::Integer(1));
        row.insert("value".to_string(), Value::Integer(100));
        storage.insert("upd_table", row).unwrap();
        
        let mut updates = HashMap::new();
        updates.insert("value".to_string(), Value::Integer(200));
        
        let count = storage.update("upd_table", &updates, |_| true).unwrap();
        assert_eq!(count, 1);
        
        let results = storage.select("upd_table", &[], |_| true).unwrap();
        assert_eq!(results[0].get("value"), Some(&Value::Integer(200)));
    }

    #[test]
    fn test_storage_drop_table() {
        let storage = Storage::new();
        
        let mut row = Row::new();
        row.insert("id".to_string(), Value::Integer(1));
        storage.insert("drop_test", row).unwrap();
        
        storage.drop_table("drop_test").unwrap();
        
        // Table no longer exists, select should fail
        let result = storage.select("drop_test", &[], |_| true);
        assert!(result.is_err());
    }

    #[test]
    fn test_storage_select_nonexistent_table() {
        let storage = Storage::new();
        
        let result = storage.select("nonexistent", &[], |_| true);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }
}
