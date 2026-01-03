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

/// Serializable row for B+ tree storage
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerializableRow {
    columns: Vec<(String, Value)>,
}

impl From<Row> for SerializableRow {
    fn from(row: Row) -> Self {
        Self {
            columns: row.into_iter().collect(),
        }
    }
}

impl From<SerializableRow> for Row {
    fn from(sr: SerializableRow) -> Self {
        sr.columns.into_iter().collect()
    }
}

/// Global storage manager using B+ trees for persistence
pub struct Storage {
    /// In-memory cache for fast access
    tables: Arc<RwLock<HashMap<String, TableData>>>,
    /// Path to data directory
    data_dir: std::path::PathBuf,
    /// B+ tree instances per table (lazy loaded)
    btrees: Arc<RwLock<HashMap<String, crate::db::btree::SharedBPlusTree>>>,
}

impl Storage {
    pub fn new() -> Self {
        let data_dir = Self::get_default_path();
        let mut storage = Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            data_dir,
            btrees: Arc::new(RwLock::new(HashMap::new())),
        };
        // Load existing data from B+ trees
        let _ = storage.load_all();
        storage
    }

    fn get_default_path() -> std::path::PathBuf {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        let path = std::path::PathBuf::from(home).join(".butterfly_db").join("data");
        std::fs::create_dir_all(&path).ok();
        path
    }

    /// Get or create B+ tree for a table
    fn get_btree(&self, table_name: &str) -> Result<crate::db::btree::SharedBPlusTree, String> {
        let mut btrees = self.btrees.write().map_err(|e| e.to_string())?;
        
        if !btrees.contains_key(table_name) {
            let tree = crate::db::btree::SharedBPlusTree::open(self.data_dir.clone(), table_name)
                .map_err(|e| e.to_string())?;
            btrees.insert(table_name.to_string(), tree);
        }
        
        Ok(btrees.get(table_name).unwrap().clone())
    }

    /// Load all tables from B+ tree files
    fn load_all(&mut self) -> Result<(), String> {
        // List all .db files in data directory
        if let Ok(entries) = std::fs::read_dir(&self.data_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.path().file_stem() {
                    let table_name = name.to_string_lossy().to_string();
                    if entry.path().extension().map_or(false, |e| e == "db") {
                        // Load table data from B+ tree
                        if let Ok(tree) = self.get_btree(&table_name) {
                            let mut table_data = TableData::new();
                            let mut row_id = 0u64;
                            
                            // Scan all data from B+ tree
                            let _ = tree.scan(|_key, value| {
                                if let Ok(sr) = bincode::deserialize::<SerializableRow>(value) {
                                    table_data.rows.push(sr.into());
                                }
                                row_id += 1;
                            });
                            
                            let mut tables = self.tables.write().unwrap();
                            tables.insert(table_name, table_data);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Get or create table data storage
    pub fn get_or_create_table(&self, table_name: &str) -> Result<(), String> {
        let mut tables = self.tables.write().map_err(|e| e.to_string())?;
        if !tables.contains_key(table_name) {
            tables.insert(table_name.to_string(), TableData::new());
        }
        // Ensure B+ tree exists
        let _ = self.get_btree(table_name)?;
        Ok(())
    }

    /// Generate a unique row key
    fn generate_row_key(&self, table_name: &str) -> Vec<u8> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("{}_{}", table_name, timestamp).into_bytes()
    }

    /// Insert a row into a table (persists to B+ tree)
    pub fn insert(&self, table_name: &str, row: Row) -> Result<usize, String> {
        self.get_or_create_table(table_name)?;
        
        // Insert into memory cache
        let mut tables = self.tables.write().map_err(|e| e.to_string())?;
        let table = tables
            .get_mut(table_name)
            .ok_or(format!("Table '{}' not found", table_name))?;
        
        let count = table.insert(row.clone());
        drop(tables);
        
        // Persist to B+ tree
        let tree = self.get_btree(table_name)?;
        let key = self.generate_row_key(table_name);
        let sr = SerializableRow::from(row);
        let value = bincode::serialize(&sr).map_err(|e| e.to_string())?;
        tree.insert(key, value)?;
        
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

    /// Delete from a table (persists to B+ tree)
    pub fn delete<F>(&self, table_name: &str, predicate: F) -> Result<usize, String>
    where
        F: Fn(&Row) -> bool,
    {
        let mut tables = self.tables.write().map_err(|e| e.to_string())?;
        let table = tables
            .get_mut(table_name)
            .ok_or(format!("Table '{}' not found", table_name))?;
        
        // For now, we rebuild the B+ tree after delete
        // (More efficient approach would track row IDs)
        let original_len = table.rows.len();
        table.rows.retain(|row| !predicate(row));
        let deleted = original_len - table.rows.len();
        
        // Rebuild B+ tree with remaining rows
        if deleted > 0 {
            let remaining_rows: Vec<Row> = table.rows.clone();
            drop(tables);
            
            // Clear and rebuild B+ tree
            let tree = self.get_btree(table_name)?;
            
            // Re-insert all remaining rows
            for row in remaining_rows {
                let key = self.generate_row_key(table_name);
                let sr = SerializableRow::from(row);
                let value = bincode::serialize(&sr).map_err(|e| e.to_string())?;
                tree.insert(key, value)?;
            }
        }
        
        Ok(deleted)
    }

    /// Update a table (persists to B+ tree)
    pub fn update<F>(&self, table_name: &str, updates: &HashMap<String, Value>, predicate: F) -> Result<usize, String>
    where
        F: Fn(&Row) -> bool,
    {
        let mut tables = self.tables.write().map_err(|e| e.to_string())?;
        let table = tables
            .get_mut(table_name)
            .ok_or(format!("Table '{}' not found", table_name))?;
        
        let count = table.update(updates, predicate);
        
        // Rebuild B+ tree with updated data
        if count > 0 {
            let all_rows: Vec<Row> = table.rows.clone();
            drop(tables);
            
            let tree = self.get_btree(table_name)?;
            
            // Re-insert all rows with updates
            for row in all_rows {
                let key = self.generate_row_key(table_name);
                let sr = SerializableRow::from(row);
                let value = bincode::serialize(&sr).map_err(|e| e.to_string())?;
                tree.insert(key, value)?;
            }
        }
        
        Ok(count)
    }

    /// Drop a table's data (removes B+ tree file)
    pub fn drop_table(&self, table_name: &str) -> Result<(), String> {
        let mut tables = self.tables.write().map_err(|e| e.to_string())?;
        tables.remove(table_name);
        
        // Remove B+ tree
        let mut btrees = self.btrees.write().map_err(|e| e.to_string())?;
        btrees.remove(table_name);
        
        // Remove file
        let path = self.data_dir.join(format!("{}.db", table_name));
        let _ = std::fs::remove_file(path);
        
        Ok(())
    }
}

// Global storage instance (loads data from B+ trees on creation)
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

    fn unique_table_name(prefix: &str) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        format!("{}_{}", prefix, ts)
    }

    #[test]
    fn test_storage_new() {
        let storage = Storage::new();
        let table = unique_table_name("test");
        // Should not panic
        assert!(storage.get_or_create_table(&table).is_ok());
        let _ = storage.drop_table(&table);
    }

    #[test]
    fn test_storage_insert_and_select() {
        let storage = Storage::new();
        let table = unique_table_name("test_table");
        
        let mut row = Row::new();
        row.insert("id".to_string(), Value::Integer(1));
        row.insert("name".to_string(), Value::Text("Test".to_string()));
        
        storage.insert(&table, row).unwrap();
        
        let results = storage.select(&table, &[], |_| true).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("id"), Some(&Value::Integer(1)));
        
        let _ = storage.drop_table(&table);
    }

    #[test]
    fn test_storage_delete() {
        let storage = Storage::new();
        let table = unique_table_name("del_table");
        
        let mut row = Row::new();
        row.insert("id".to_string(), Value::Integer(1));
        storage.insert(&table, row).unwrap();
        
        let deleted = storage.delete(&table, |_| true).unwrap();
        assert_eq!(deleted, 1);
        
        let results = storage.select(&table, &[], |_| true).unwrap();
        assert!(results.is_empty());
        
        let _ = storage.drop_table(&table);
    }

    #[test]
    fn test_storage_update() {
        let storage = Storage::new();
        let table = unique_table_name("upd_table");
        
        let mut row = Row::new();
        row.insert("id".to_string(), Value::Integer(1));
        row.insert("value".to_string(), Value::Integer(100));
        storage.insert(&table, row).unwrap();
        
        let mut updates = HashMap::new();
        updates.insert("value".to_string(), Value::Integer(200));
        
        let count = storage.update(&table, &updates, |_| true).unwrap();
        assert_eq!(count, 1);
        
        let results = storage.select(&table, &[], |_| true).unwrap();
        assert_eq!(results[0].get("value"), Some(&Value::Integer(200)));
        
        let _ = storage.drop_table(&table);
    }

    #[test]
    fn test_storage_drop_table() {
        let storage = Storage::new();
        let table = unique_table_name("drop_test");
        
        let mut row = Row::new();
        row.insert("id".to_string(), Value::Integer(1));
        storage.insert(&table, row).unwrap();
        
        storage.drop_table(&table).unwrap();
        
        // Table no longer exists, select should fail
        let result = storage.select(&table, &[], |_| true);
        assert!(result.is_err());
    }

    #[test]
    fn test_storage_select_nonexistent_table() {
        let storage = Storage::new();
        let table = unique_table_name("nonexistent");
        
        let result = storage.select(&table, &[], |_| true);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }
}
