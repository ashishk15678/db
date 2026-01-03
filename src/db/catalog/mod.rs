// Database Catalog - Stores table schemas and metadata
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::db::sql::parser::DataType;

/// Column definition stored in the catalog
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: String, // Simplified type storage
    pub nullable: bool,
    pub is_primary_key: bool,
}

/// Table schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
    pub created_at: String,
}

impl TableSchema {
    pub fn new(name: String, columns: Vec<ColumnSchema>) -> Self {
        Self {
            name,
            columns,
            created_at: chrono::Local::now().to_rfc3339(),
        }
    }

    pub fn get_column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

/// Database metadata
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DatabaseSchema {
    pub name: String,
    pub tables: HashMap<String, TableSchema>,
}

/// Catalog stores all database and table metadata
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct CatalogData {
    pub databases: HashMap<String, DatabaseSchema>,
    pub current_database: Option<String>,
}

/// Thread-safe catalog wrapper
pub struct Catalog {
    data: Arc<RwLock<CatalogData>>,
    storage_path: PathBuf,
}

impl Default for Catalog {
    /// Create a new catalog, loading from disk if available
    fn default() -> Self {
        let storage_path = Self::get_default_path();
        let data = Self::load_or_create(&storage_path);

        Self {
            data: Arc::new(RwLock::new(data)),
            storage_path,
        }
    }
}

impl Catalog {
    fn get_default_path() -> PathBuf {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        let path = PathBuf::from(home).join(".butterfly_db");
        fs::create_dir_all(&path).ok();
        path.join("catalog.json")
    }

    fn load_or_create(path: &PathBuf) -> CatalogData {
        if path.exists() {
            match fs::read_to_string(path) {
                Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
                Err(_) => CatalogData::default(),
            }
        } else {
            // Create default database
            let mut data = CatalogData::default();
            data.databases.insert(
                "default".to_string(),
                DatabaseSchema {
                    name: "default".to_string(),
                    tables: HashMap::new(),
                },
            );
            data.current_database = Some("default".to_string());
            data
        }
    }

    /// Persist catalog to disk
    pub fn save(&self) -> Result<(), String> {
        let data = self.data.read().map_err(|e| e.to_string())?;
        let json = serde_json::to_string_pretty(&*data).map_err(|e| e.to_string())?;
        fs::write(&self.storage_path, json).map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Create a new database
    pub fn create_database(&self, name: &str, if_not_exists: bool) -> Result<(), String> {
        let mut data = self.data.write().map_err(|e| e.to_string())?;

        if data.databases.contains_key(name) {
            if if_not_exists {
                return Ok(());
            }
            return Err(format!("Database '{}' already exists", name));
        }

        data.databases.insert(
            name.to_string(),
            DatabaseSchema {
                name: name.to_string(),
                tables: HashMap::new(),
            },
        );

        // Set as current if none selected
        if data.current_database.is_none() {
            data.current_database = Some(name.to_string());
        }

        drop(data);
        self.save()
    }

    /// Drop a database
    pub fn drop_database(&self, name: &str, if_exists: bool) -> Result<(), String> {
        let mut data = self.data.write().map_err(|e| e.to_string())?;

        if !data.databases.contains_key(name) {
            if if_exists {
                return Ok(());
            }
            return Err(format!("Database '{}' does not exist", name));
        }

        data.databases.remove(name);

        if data.current_database.as_deref() == Some(name) {
            data.current_database = data.databases.keys().next().cloned();
        }

        drop(data);
        self.save()
    }

    /// Create a new table in the current database
    pub fn create_table(
        &self,
        name: &str,
        columns: Vec<ColumnSchema>,
        if_not_exists: bool,
    ) -> Result<(), String> {
        let mut data = self.data.write().map_err(|e| e.to_string())?;

        let db_name = data
            .current_database
            .clone()
            .ok_or("No database selected")?;

        let db = data
            .databases
            .get_mut(&db_name)
            .ok_or(format!("Database '{}' not found", db_name))?;

        if db.tables.contains_key(name) {
            if if_not_exists {
                return Ok(());
            }
            return Err(format!("Table '{}' already exists", name));
        }

        db.tables.insert(
            name.to_string(),
            TableSchema::new(name.to_string(), columns),
        );

        drop(data);
        self.save()
    }

    /// Drop a table from the current database
    pub fn drop_table(&self, name: &str, if_exists: bool) -> Result<(), String> {
        let mut data = self.data.write().map_err(|e| e.to_string())?;

        let db_name = data
            .current_database
            .clone()
            .ok_or("No database selected")?;

        let db = data
            .databases
            .get_mut(&db_name)
            .ok_or(format!("Database '{}' not found", db_name))?;

        if !db.tables.contains_key(name) {
            if if_exists {
                return Ok(());
            }
            return Err(format!("Table '{}' does not exist", name));
        }

        db.tables.remove(name);

        drop(data);
        self.save()
    }

    /// Get a table schema
    pub fn get_table(&self, name: &str) -> Result<TableSchema, String> {
        let data = self.data.read().map_err(|e| e.to_string())?;

        let db_name = data
            .current_database
            .as_ref()
            .ok_or("No database selected")?;

        let db = data
            .databases
            .get(db_name)
            .ok_or(format!("Database '{}' not found", db_name))?;

        db.tables
            .get(name)
            .cloned()
            .ok_or(format!("Table '{}' does not exist", name))
    }

    /// List all tables in current database
    pub fn list_tables(&self) -> Result<Vec<String>, String> {
        let data = self.data.read().map_err(|e| e.to_string())?;

        let db_name = data
            .current_database
            .as_ref()
            .ok_or("No database selected")?;

        let db = data
            .databases
            .get(db_name)
            .ok_or(format!("Database '{}' not found", db_name))?;

        Ok(db.tables.keys().cloned().collect())
    }

    /// Get current database name
    pub fn current_database(&self) -> Option<String> {
        self.data.read().ok()?.current_database.clone()
    }
}

/// Convert parser DataType to string for storage
pub fn data_type_to_string(dt: &DataType) -> String {
    match dt {
        DataType::Integer => "INTEGER".to_string(),
        DataType::Varchar(Some(n)) => format!("VARCHAR({})", n),
        DataType::Varchar(None) => "VARCHAR".to_string(),
        DataType::Text => "TEXT".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Float => "FLOAT".to_string(),
        DataType::Double => "DOUBLE".to_string(),
        DataType::Date => "DATE".to_string(),
        DataType::DateTime => "DATETIME".to_string(),
        DataType::Timestamp => "TIMESTAMP".to_string(),
    }
}

// Global catalog instance
lazy_static::lazy_static! {
    pub static ref CATALOG: Catalog = Catalog::new();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Counter for unique test catalog names
    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn create_test_catalog() -> Catalog {
        let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let temp_dir = std::env::temp_dir();
        let storage_path = temp_dir.join(format!("test_catalog_{}.json", count));

        // Clean up any existing file
        let _ = fs::remove_file(&storage_path);

        let mut data = CatalogData::default();
        data.databases.insert(
            "test_db".to_string(),
            DatabaseSchema {
                name: "test_db".to_string(),
                tables: HashMap::new(),
            },
        );
        data.current_database = Some("test_db".to_string());

        Catalog {
            data: Arc::new(RwLock::new(data)),
            storage_path,
        }
    }

    #[test]
    fn test_column_schema_creation() {
        let col = ColumnSchema {
            name: "id".to_string(),
            data_type: "INTEGER".to_string(),
            nullable: false,
            is_primary_key: true,
        };

        assert_eq!(col.name, "id");
        assert_eq!(col.data_type, "INTEGER");
        assert!(!col.nullable);
        assert!(col.is_primary_key);
    }

    #[test]
    fn test_table_schema_creation() {
        let columns = vec![
            ColumnSchema {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
                is_primary_key: true,
            },
            ColumnSchema {
                name: "name".to_string(),
                data_type: "VARCHAR(255)".to_string(),
                nullable: true,
                is_primary_key: false,
            },
        ];

        let table = TableSchema::new("users".to_string(), columns);

        assert_eq!(table.name, "users");
        assert_eq!(table.columns.len(), 2);
        assert!(!table.created_at.is_empty());
    }

    #[test]
    fn test_table_schema_get_column() {
        let columns = vec![ColumnSchema {
            name: "id".to_string(),
            data_type: "INTEGER".to_string(),
            nullable: false,
            is_primary_key: true,
        }];

        let table = TableSchema::new("users".to_string(), columns);

        assert!(table.get_column("id").is_some());
        assert!(table.get_column("nonexistent").is_none());
    }

    #[test]
    fn test_table_schema_column_names() {
        let columns = vec![
            ColumnSchema {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
                is_primary_key: true,
            },
            ColumnSchema {
                name: "email".to_string(),
                data_type: "VARCHAR(255)".to_string(),
                nullable: true,
                is_primary_key: false,
            },
        ];

        let table = TableSchema::new("users".to_string(), columns);
        let names = table.column_names();

        assert_eq!(names.len(), 2);
        assert!(names.contains(&"id"));
        assert!(names.contains(&"email"));
    }

    #[test]
    fn test_catalog_create_table() {
        let catalog = create_test_catalog();

        let columns = vec![ColumnSchema {
            name: "id".to_string(),
            data_type: "INTEGER".to_string(),
            nullable: false,
            is_primary_key: true,
        }];

        let result = catalog.create_table("test_table", columns, false);
        assert!(result.is_ok(), "Failed to create table: {:?}", result);

        // Verify table was created
        let table = catalog.get_table("test_table");
        assert!(table.is_ok());
        assert_eq!(table.unwrap().name, "test_table");
    }

    #[test]
    fn test_catalog_create_duplicate_table_error() {
        let catalog = create_test_catalog();

        let columns = vec![ColumnSchema {
            name: "id".to_string(),
            data_type: "INTEGER".to_string(),
            nullable: false,
            is_primary_key: true,
        }];

        catalog
            .create_table("dup_table", columns.clone(), false)
            .unwrap();

        // Second create should fail
        let result = catalog.create_table("dup_table", columns, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("already exists"));
    }

    #[test]
    fn test_catalog_create_table_if_not_exists() {
        let catalog = create_test_catalog();

        let columns = vec![ColumnSchema {
            name: "id".to_string(),
            data_type: "INTEGER".to_string(),
            nullable: false,
            is_primary_key: true,
        }];

        catalog
            .create_table("exists_table", columns.clone(), false)
            .unwrap();

        // With if_not_exists = true, should succeed
        let result = catalog.create_table("exists_table", columns, true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_catalog_drop_table() {
        let catalog = create_test_catalog();

        let columns = vec![ColumnSchema {
            name: "id".to_string(),
            data_type: "INTEGER".to_string(),
            nullable: false,
            is_primary_key: true,
        }];

        catalog.create_table("drop_me", columns, false).unwrap();

        let result = catalog.drop_table("drop_me", false);
        assert!(result.is_ok());

        // Table should no longer exist
        let table = catalog.get_table("drop_me");
        assert!(table.is_err());
    }

    #[test]
    fn test_catalog_drop_nonexistent_table_error() {
        let catalog = create_test_catalog();

        let result = catalog.drop_table("nonexistent", false);
        assert!(result.is_err());
    }

    #[test]
    fn test_catalog_drop_table_if_exists() {
        let catalog = create_test_catalog();

        // Should not error with if_exists = true
        let result = catalog.drop_table("nonexistent", true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_catalog_list_tables() {
        let catalog = create_test_catalog();

        let columns = vec![ColumnSchema {
            name: "id".to_string(),
            data_type: "INTEGER".to_string(),
            nullable: false,
            is_primary_key: true,
        }];

        catalog
            .create_table("table1", columns.clone(), false)
            .unwrap();
        catalog.create_table("table2", columns, false).unwrap();

        let tables = catalog.list_tables().unwrap();
        assert!(tables.len() >= 2);
        assert!(tables.contains(&"table1".to_string()));
        assert!(tables.contains(&"table2".to_string()));
    }

    #[test]
    fn test_catalog_create_database() {
        let catalog = create_test_catalog();

        let result = catalog.create_database("new_db", false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_catalog_create_duplicate_database_error() {
        let catalog = create_test_catalog();

        catalog.create_database("dup_db", false).unwrap();
        let result = catalog.create_database("dup_db", false);
        assert!(result.is_err());
    }

    #[test]
    fn test_data_type_to_string() {
        assert_eq!(data_type_to_string(&DataType::Integer), "INTEGER");
        assert_eq!(
            data_type_to_string(&DataType::Varchar(Some(255))),
            "VARCHAR(255)"
        );
        assert_eq!(data_type_to_string(&DataType::Varchar(None)), "VARCHAR");
        assert_eq!(data_type_to_string(&DataType::Text), "TEXT");
        assert_eq!(data_type_to_string(&DataType::Boolean), "BOOLEAN");
        assert_eq!(data_type_to_string(&DataType::Float), "FLOAT");
        assert_eq!(data_type_to_string(&DataType::Double), "DOUBLE");
        assert_eq!(data_type_to_string(&DataType::Date), "DATE");
        assert_eq!(data_type_to_string(&DataType::DateTime), "DATETIME");
        assert_eq!(data_type_to_string(&DataType::Timestamp), "TIMESTAMP");
    }
}
