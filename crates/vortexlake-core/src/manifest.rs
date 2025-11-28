//! Versioned manifest for VortexLake database metadata
//!
//! The manifest tracks table schemas, fragments, and version history
//! to provide ACID operations and schema evolution.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{anyhow, Result};
use arrow::ipc::convert::{fb_to_schema, IpcSchemaEncoder};
use arrow::ipc::root_as_schema;
use arrow::ipc::writer::DictionaryTracker;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::fs;

use crate::fragment::Fragment;
use crate::schema::Schema;

/// Metadata for a table fragment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FragmentMetadata {
    /// Unique fragment ID
    pub id: String,
    /// Table this fragment belongs to
    pub table_name: String,
    /// Path to the fragment file (relative to database root)
    pub path: String,
    /// Row count in this fragment
    pub row_count: usize,
    /// Size in bytes
    pub size_bytes: u64,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Column statistics
    pub column_stats: HashMap<String, ColumnStats>,
}

impl FragmentMetadata {
    /// Check if this fragment can be pruned based on a simple predicate.
    /// 
    /// Returns `true` if the fragment can be safely skipped (i.e., no rows match).
    /// 
    /// # Arguments
    /// * `column` - The column name to check
    /// * `op` - The comparison operator (">", ">=", "<", "<=", "=", "!=")
    /// * `value` - The value to compare against
    pub fn can_prune(&self, column: &str, op: &str, value: &serde_json::Value) -> bool {
        let Some(stats) = self.column_stats.get(column) else {
            return false; // No statistics available, cannot prune
        };

        match op {
            ">" => {
                // column > value: if max <= value, can prune
                if let Some(max) = &stats.max_value {
                    return Self::compare_json(max, value) <= 0;
                }
            }
            ">=" => {
                // column >= value: if max < value, can prune
                if let Some(max) = &stats.max_value {
                    return Self::compare_json(max, value) < 0;
                }
            }
            "<" => {
                // column < value: if min >= value, can prune
                if let Some(min) = &stats.min_value {
                    return Self::compare_json(min, value) >= 0;
                }
            }
            "<=" => {
                // column <= value: if min > value, can prune
                if let Some(min) = &stats.min_value {
                    return Self::compare_json(min, value) > 0;
                }
            }
            "=" => {
                // column = value: if value < min or value > max, can prune
                if let (Some(min), Some(max)) = (&stats.min_value, &stats.max_value) {
                    let cmp_min = Self::compare_json(value, min);
                    let cmp_max = Self::compare_json(value, max);
                    return cmp_min < 0 || cmp_max > 0;
                }
            }
            "!=" => {
                // column != value: if is_constant and min == value, can prune
                if stats.is_constant {
                    if let Some(min) = &stats.min_value {
                        return Self::compare_json(min, value) == 0;
                    }
                }
            }
            _ => {}
        }
        false
    }

    /// Check if this fragment might contain rows matching the given range.
    /// 
    /// Returns `true` if the fragment might have matching rows (i.e., ranges overlap).
    pub fn overlaps_range(
        &self,
        column: &str,
        min_value: Option<&serde_json::Value>,
        max_value: Option<&serde_json::Value>,
    ) -> bool {
        let Some(stats) = self.column_stats.get(column) else {
            return true; // No statistics, assume overlap
        };

        // Check if ranges overlap: !(frag_max < query_min || frag_min > query_max)
        if let (Some(frag_max), Some(query_min)) = (&stats.max_value, min_value) {
            if Self::compare_json(frag_max, query_min) < 0 {
                return false; // Fragment is entirely below the query range
            }
        }

        if let (Some(frag_min), Some(query_max)) = (&stats.min_value, max_value) {
            if Self::compare_json(frag_min, query_max) > 0 {
                return false; // Fragment is entirely above the query range
            }
        }

        true
    }

    /// Compare two JSON values.
    /// Returns: -1 if a < b, 0 if a == b, 1 if a > b
    fn compare_json(a: &serde_json::Value, b: &serde_json::Value) -> i32 {
        match (a, b) {
            (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
                let a_f64 = a.as_f64().unwrap_or(0.0);
                let b_f64 = b.as_f64().unwrap_or(0.0);
                if a_f64 < b_f64 {
                    -1
                } else if a_f64 > b_f64 {
                    1
                } else {
                    0
                }
            }
            (serde_json::Value::String(a), serde_json::Value::String(b)) => {
                a.cmp(b) as i32
            }
            (serde_json::Value::Bool(a), serde_json::Value::Bool(b)) => {
                (*a as i32) - (*b as i32)
            }
            _ => 0 // Incomparable types
        }
    }
}

/// Column statistics for optimization and predicate pruning
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ColumnStats {
    /// Minimum value (for numeric columns)
    pub min_value: Option<serde_json::Value>,
    /// Maximum value (for numeric columns)
    pub max_value: Option<serde_json::Value>,
    /// Null count
    pub null_count: usize,
    /// Distinct value count (approximate)
    pub distinct_count: Option<usize>,
    /// Whether the column is sorted in ascending order
    #[serde(default)]
    pub is_sorted: bool,
    /// Whether all values in the column are the same
    #[serde(default)]
    pub is_constant: bool,
}

/// Serializable schema representation using Arrow IPC format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableSchema {
    /// Arrow schema encoded as base64 IPC bytes
    pub ipc_bytes_base64: String,
    /// Schema metadata (for quick access without deserializing)
    pub metadata: HashMap<String, String>,
}

impl SerializableSchema {
    /// Convert from Schema
    pub fn from_schema(schema: &Schema) -> Self {
        let arrow_schema = schema.to_arrow();
        
        // Encode schema using Arrow IPC format
        let mut dictionary_tracker = DictionaryTracker::new(false);
        let fb = IpcSchemaEncoder::new()
            .with_dictionary_tracker(&mut dictionary_tracker)
            .schema_to_fb(&arrow_schema);
        
        let ipc_bytes = fb.finished_data();
        let ipc_bytes_base64 = BASE64.encode(ipc_bytes);

        Self {
            ipc_bytes_base64,
            metadata: schema.metadata.clone(),
        }
    }

    /// Convert to Schema
    pub fn to_schema(&self) -> Result<Schema> {
        let ipc_bytes = BASE64.decode(&self.ipc_bytes_base64)?;
        let ipc_schema = root_as_schema(&ipc_bytes)
            .map_err(|e| anyhow!("Failed to decode IPC schema: {}", e))?;
        let arrow_schema = fb_to_schema(ipc_schema);
        
        let mut schema = Schema::from_arrow(&arrow_schema);
        schema.metadata = self.metadata.clone();
        Ok(schema)
    }
}

/// Serializable table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableTableMetadata {
    /// Table name
    pub name: String,
    /// Table schema (serializable)
    pub schema: SerializableSchema,
    /// All fragments for this table
    pub fragments: Vec<FragmentMetadata>,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Last modification timestamp
    pub updated_at: DateTime<Utc>,
}

/// Table metadata (runtime representation)
#[derive(Debug, Clone)]
pub struct TableMetadata {
    /// Table name
    pub name: String,
    /// Table schema
    pub schema: Schema,
    /// All fragments for this table
    pub fragments: Vec<FragmentMetadata>,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Last modification timestamp
    pub updated_at: DateTime<Utc>,
}

impl TableMetadata {
    fn to_serializable(&self) -> SerializableTableMetadata {
        SerializableTableMetadata {
            name: self.name.clone(),
            schema: SerializableSchema::from_schema(&self.schema),
            fragments: self.fragments.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }

    fn from_serializable(s: SerializableTableMetadata) -> Result<Self> {
        Ok(Self {
            name: s.name,
            schema: s.schema.to_schema()?,
            fragments: s.fragments,
            created_at: s.created_at,
            updated_at: s.updated_at,
        })
    }
}

/// Serializable manifest for persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableManifest {
    /// Manifest version
    pub version: String,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Last update timestamp
    pub updated_at: DateTime<Utc>,
    /// All tables in the database
    pub tables: HashMap<String, SerializableTableMetadata>,
}

/// Versioned manifest for the entire database
#[derive(Debug, Clone)]
pub struct Manifest {
    /// Manifest version
    pub version: String,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Last update timestamp
    pub updated_at: DateTime<Utc>,
    /// All tables in the database
    pub tables: HashMap<String, TableMetadata>,
}

impl Manifest {
    /// Create a new empty manifest
    pub fn new() -> Self {
        Self {
            version: "1.0".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            tables: HashMap::new(),
        }
    }

    /// Load manifest from file
    pub async fn load(path: &Path) -> Result<Self> {
        let data = fs::read(path).await?;
        let serializable: SerializableManifest = serde_json::from_slice(&data)?;
        Self::from_serializable(serializable)
    }

    /// Save manifest to file
    pub async fn save(&self, path: &Path) -> Result<()> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let serializable = self.to_serializable();
        let data = serde_json::to_vec_pretty(&serializable)?;
        
        // Write atomically using temp file
        let temp_path = path.with_extension("tmp");
        fs::write(&temp_path, data).await?;
        fs::rename(&temp_path, path).await?;
        
        Ok(())
    }

    /// Convert to serializable form
    fn to_serializable(&self) -> SerializableManifest {
        SerializableManifest {
            version: self.version.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
            tables: self
                .tables
                .iter()
                .map(|(k, v)| (k.clone(), v.to_serializable()))
                .collect(),
        }
    }

    /// Convert from serializable form
    fn from_serializable(s: SerializableManifest) -> Result<Self> {
        let tables: Result<HashMap<_, _>> = s
            .tables
            .into_iter()
            .map(|(k, v)| Ok((k, TableMetadata::from_serializable(v)?)))
            .collect();

        Ok(Self {
            version: s.version,
            created_at: s.created_at,
            updated_at: s.updated_at,
            tables: tables?,
        })
    }

    /// Create a new table
    pub fn create_table(&mut self, name: &str, schema: Schema) -> Result<()> {
        if self.tables.contains_key(name) {
            return Err(anyhow!("Table '{}' already exists", name));
        }

        let table = TableMetadata {
            name: name.to_string(),
            schema,
            fragments: Vec::new(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        self.tables.insert(name.to_string(), table);
        self.updated_at = Utc::now();

        Ok(())
    }

    /// Drop a table
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        if self.tables.remove(name).is_none() {
            return Err(anyhow!("Table '{}' not found", name));
        }
        self.updated_at = Utc::now();
        Ok(())
    }

    /// Add a fragment to a table
    pub fn add_fragment(&mut self, table_name: &str, fragment: Fragment) -> Result<()> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| anyhow!("Table '{}' not found", table_name))?;

        let id = fragment.id.clone();
        let metadata = FragmentMetadata {
            id: id.clone(),
            table_name: table_name.to_string(),
            path: format!("data/{}.vortex", id),
            row_count: fragment.row_count,
            size_bytes: fragment.size_bytes,
            created_at: Utc::now(),
            column_stats: fragment.column_stats,
        };

        table.fragments.push(metadata);
        table.updated_at = Utc::now();
        self.updated_at = Utc::now();

        Ok(())
    }

    /// Remove a fragment from a table
    pub fn remove_fragment(&mut self, table_name: &str, fragment_id: &str) -> Result<()> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| anyhow!("Table '{}' not found", table_name))?;

        let idx = table
            .fragments
            .iter()
            .position(|f| f.id == fragment_id)
            .ok_or_else(|| anyhow!("Fragment '{}' not found", fragment_id))?;

        table.fragments.remove(idx);
        table.updated_at = Utc::now();
        self.updated_at = Utc::now();

        Ok(())
    }

    /// Get table schema
    pub fn get_schema(&self, table_name: &str) -> Result<&Schema> {
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| anyhow!("Table '{}' not found", table_name))?;
        Ok(&table.schema)
    }

    /// Get all fragments for a table
    pub fn get_fragments(&self, table_name: &str) -> Result<&[FragmentMetadata]> {
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| anyhow!("Table '{}' not found", table_name))?;
        Ok(&table.fragments)
    }

    /// Get table metadata
    pub fn get_table(&self, table_name: &str) -> Result<&TableMetadata> {
        self.tables
            .get(table_name)
            .ok_or_else(|| anyhow!("Table '{}' not found", table_name))
    }

    /// List all table names
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Check if table exists
    pub fn table_exists(&self, table_name: &str) -> bool {
        self.tables.contains_key(table_name)
    }

    /// Get total row count across all tables
    pub fn total_row_count(&self) -> usize {
        self.tables
            .values()
            .map(|table| table.fragments.iter().map(|f| f.row_count).sum::<usize>())
            .sum()
    }

    /// Get total size in bytes across all tables
    pub fn total_size_bytes(&self) -> u64 {
        self.tables
            .values()
            .map(|table| table.fragments.iter().map(|f| f.size_bytes).sum::<u64>())
            .sum()
    }
}

impl Default for Manifest {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Field, Schema};
    use arrow::datatypes::DataType;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn test_manifest_create_table() {
        let mut manifest = Manifest::new();

        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Float32, false),
        ])
        .unwrap();

        manifest.create_table("test", schema).unwrap();
        assert!(manifest.table_exists("test"));
        assert!(!manifest.table_exists("nonexistent"));
    }

    #[test]
    fn test_manifest_duplicate_table() {
        let mut manifest = Manifest::new();
        let schema = Schema::new(vec![Field::new("id", DataType::Utf8, false)]).unwrap();

        manifest.create_table("test", schema.clone()).unwrap();
        assert!(manifest.create_table("test", schema).is_err());
    }

    #[test]
    fn test_manifest_get_schema() {
        let mut manifest = Manifest::new();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("data", DataType::Binary, true),
        ])
        .unwrap();

        manifest.create_table("test", schema.clone()).unwrap();
        let retrieved = manifest.get_schema("test").unwrap();
        assert_eq!(retrieved.fields.len(), 2);
    }

    #[tokio::test]
    async fn test_manifest_save_load() {
        let temp_dir = tempdir().unwrap();
        let manifest_path = temp_dir.path().join("manifest.json");

        // Create manifest with table
        let mut manifest = Manifest::new();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Float32, false),
            Field::new("tags", DataType::List(
                Arc::new(arrow::datatypes::Field::new("item", DataType::Utf8, true))
            ), true),
        ])
        .unwrap();

        manifest.create_table("test_table", schema).unwrap();

        // Save manifest
        manifest.save(&manifest_path).await.unwrap();
        assert!(manifest_path.exists());

        // Load manifest
        let loaded = Manifest::load(&manifest_path).await.unwrap();
        assert!(loaded.table_exists("test_table"));
        
        let loaded_schema = loaded.get_schema("test_table").unwrap();
        assert_eq!(loaded_schema.fields.len(), 3);
        assert_eq!(loaded_schema.fields[0].name, "id");
        assert_eq!(loaded_schema.fields[1].name, "value");
        assert_eq!(loaded_schema.fields[2].name, "tags");
    }

    #[test]
    fn test_manifest_drop_table() {
        let mut manifest = Manifest::new();
        let schema = Schema::new(vec![Field::new("id", DataType::Utf8, false)]).unwrap();

        manifest.create_table("test", schema).unwrap();
        assert!(manifest.table_exists("test"));

        manifest.drop_table("test").unwrap();
        assert!(!manifest.table_exists("test"));
    }
    
    #[test]
    fn test_schema_serialization_roundtrip() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("vector", DataType::FixedSizeList(
                Arc::new(arrow::datatypes::Field::new("item", DataType::Float32, false)),
                128
            ), false),
        ])
        .unwrap();
        
        // Serialize
        let serializable = SerializableSchema::from_schema(&schema);
        
        // Deserialize
        let restored = serializable.to_schema().unwrap();
        
        assert_eq!(schema.fields.len(), restored.fields.len());
        assert_eq!(schema.fields[0].name, restored.fields[0].name);
        assert_eq!(schema.fields[0].data_type, restored.fields[0].data_type);
        assert_eq!(schema.fields[1].name, restored.fields[1].name);
        assert_eq!(schema.fields[2].name, restored.fields[2].name);
    }

    // ============================================================
    // Fragment Pruning Tests - Test can_prune method
    // ============================================================

    fn create_fragment_with_stats(
        id: &str,
        column: &str,
        min: Option<serde_json::Value>,
        max: Option<serde_json::Value>,
        null_count: usize,
        is_constant: bool,
    ) -> FragmentMetadata {
        use chrono::Utc;
        let mut column_stats = std::collections::HashMap::new();
        column_stats.insert(
            column.to_string(),
            ColumnStats {
                min_value: min,
                max_value: max,
                null_count,
                distinct_count: None,
                is_sorted: false,
                is_constant,
            },
        );
        FragmentMetadata {
            id: id.to_string(),
            table_name: "test".to_string(),
            path: format!("data/{}.vortex", id),
            row_count: 100,
            size_bytes: 1000,
            created_at: Utc::now(),
            column_stats,
        }
    }

    #[test]
    fn test_can_prune_greater_than() {
        // Fragment with age: [10, 50]
        let fragment = create_fragment_with_stats(
            "f1",
            "age",
            Some(serde_json::json!(10)),
            Some(serde_json::json!(50)),
            0,
            false,
        );

        // Query: age > 60 -> can prune (max=50 <= 60)
        assert!(fragment.can_prune("age", ">", &serde_json::json!(60)));
        
        // Query: age > 50 -> can prune (max=50 <= 50)
        assert!(fragment.can_prune("age", ">", &serde_json::json!(50)));
        
        // Query: age > 40 -> cannot prune (max=50 > 40)
        assert!(!fragment.can_prune("age", ">", &serde_json::json!(40)));
        
        // Query: age > 5 -> cannot prune (max=50 > 5)
        assert!(!fragment.can_prune("age", ">", &serde_json::json!(5)));
    }

    #[test]
    fn test_can_prune_greater_than_or_equal() {
        // Fragment with age: [10, 50]
        let fragment = create_fragment_with_stats(
            "f1",
            "age",
            Some(serde_json::json!(10)),
            Some(serde_json::json!(50)),
            0,
            false,
        );

        // Query: age >= 60 -> can prune (max=50 < 60)
        assert!(fragment.can_prune("age", ">=", &serde_json::json!(60)));
        
        // Query: age >= 51 -> can prune (max=50 < 51)
        assert!(fragment.can_prune("age", ">=", &serde_json::json!(51)));
        
        // Query: age >= 50 -> cannot prune (max=50 >= 50)
        assert!(!fragment.can_prune("age", ">=", &serde_json::json!(50)));
        
        // Query: age >= 10 -> cannot prune
        assert!(!fragment.can_prune("age", ">=", &serde_json::json!(10)));
    }

    #[test]
    fn test_can_prune_less_than() {
        // Fragment with age: [10, 50]
        let fragment = create_fragment_with_stats(
            "f1",
            "age",
            Some(serde_json::json!(10)),
            Some(serde_json::json!(50)),
            0,
            false,
        );

        // Query: age < 5 -> can prune (min=10 >= 5)
        assert!(fragment.can_prune("age", "<", &serde_json::json!(5)));
        
        // Query: age < 10 -> can prune (min=10 >= 10)
        assert!(fragment.can_prune("age", "<", &serde_json::json!(10)));
        
        // Query: age < 15 -> cannot prune (min=10 < 15)
        assert!(!fragment.can_prune("age", "<", &serde_json::json!(15)));
        
        // Query: age < 60 -> cannot prune
        assert!(!fragment.can_prune("age", "<", &serde_json::json!(60)));
    }

    #[test]
    fn test_can_prune_less_than_or_equal() {
        // Fragment with age: [10, 50]
        let fragment = create_fragment_with_stats(
            "f1",
            "age",
            Some(serde_json::json!(10)),
            Some(serde_json::json!(50)),
            0,
            false,
        );

        // Query: age <= 5 -> can prune (min=10 > 5)
        assert!(fragment.can_prune("age", "<=", &serde_json::json!(5)));
        
        // Query: age <= 9 -> can prune (min=10 > 9)
        assert!(fragment.can_prune("age", "<=", &serde_json::json!(9)));
        
        // Query: age <= 10 -> cannot prune (min=10 <= 10)
        assert!(!fragment.can_prune("age", "<=", &serde_json::json!(10)));
        
        // Query: age <= 60 -> cannot prune
        assert!(!fragment.can_prune("age", "<=", &serde_json::json!(60)));
    }

    #[test]
    fn test_can_prune_equals() {
        // Fragment with age: [10, 50]
        let fragment = create_fragment_with_stats(
            "f1",
            "age",
            Some(serde_json::json!(10)),
            Some(serde_json::json!(50)),
            0,
            false,
        );

        // Query: age = 5 -> can prune (5 < min)
        assert!(fragment.can_prune("age", "=", &serde_json::json!(5)));
        
        // Query: age = 60 -> can prune (60 > max)
        assert!(fragment.can_prune("age", "=", &serde_json::json!(60)));
        
        // Query: age = 30 -> cannot prune (30 is in range)
        assert!(!fragment.can_prune("age", "=", &serde_json::json!(30)));
        
        // Query: age = 10 -> cannot prune (equals min)
        assert!(!fragment.can_prune("age", "=", &serde_json::json!(10)));
        
        // Query: age = 50 -> cannot prune (equals max)
        assert!(!fragment.can_prune("age", "=", &serde_json::json!(50)));
    }

    #[test]
    fn test_can_prune_not_equals_constant() {
        // Fragment with constant column (all values are 42)
        let fragment = create_fragment_with_stats(
            "f1",
            "status",
            Some(serde_json::json!(42)),
            Some(serde_json::json!(42)),
            0,
            true, // is_constant
        );

        // Query: status != 42 -> can prune (constant column, value matches)
        assert!(fragment.can_prune("status", "!=", &serde_json::json!(42)));
        
        // Query: status != 0 -> cannot prune (value doesn't match)
        assert!(!fragment.can_prune("status", "!=", &serde_json::json!(0)));
    }

    #[test]
    fn test_can_prune_missing_stats() {
        let fragment = create_fragment_with_stats(
            "f1",
            "age",
            Some(serde_json::json!(10)),
            Some(serde_json::json!(50)),
            0,
            false,
        );

        // Query on non-existent column -> cannot prune
        assert!(!fragment.can_prune("unknown_column", ">", &serde_json::json!(5)));
    }

    #[test]
    fn test_can_prune_float_values() {
        // Fragment with price: [10.5, 99.9]
        let fragment = create_fragment_with_stats(
            "f1",
            "price",
            Some(serde_json::json!(10.5)),
            Some(serde_json::json!(99.9)),
            0,
            false,
        );

        // Query: price > 100.0 -> can prune
        assert!(fragment.can_prune("price", ">", &serde_json::json!(100.0)));
        
        // Query: price < 10.0 -> can prune
        assert!(fragment.can_prune("price", "<", &serde_json::json!(10.0)));
        
        // Query: price = 50.0 -> cannot prune (in range)
        assert!(!fragment.can_prune("price", "=", &serde_json::json!(50.0)));
    }

    #[test]
    fn test_can_prune_string_values() {
        // Fragment with name: ["alice", "charlie"]
        let fragment = create_fragment_with_stats(
            "f1",
            "name",
            Some(serde_json::json!("alice")),
            Some(serde_json::json!("charlie")),
            0,
            false,
        );

        // Query: name < "aaa" -> can prune ("alice" >= "aaa")
        assert!(fragment.can_prune("name", "<", &serde_json::json!("aaa")));
        
        // Query: name > "zzz" -> can prune ("charlie" <= "zzz")
        assert!(fragment.can_prune("name", ">", &serde_json::json!("zzz")));
        
        // Query: name = "bob" -> cannot prune (in range)
        assert!(!fragment.can_prune("name", "=", &serde_json::json!("bob")));
    }

    // ============================================================
    // Range Overlap Tests - Test overlaps_range method
    // ============================================================

    #[test]
    fn test_overlaps_range_basic() {
        // Fragment with age: [10, 50]
        let fragment = create_fragment_with_stats(
            "f1",
            "age",
            Some(serde_json::json!(10)),
            Some(serde_json::json!(50)),
            0,
            false,
        );

        // Query range [20, 30] -> overlaps
        assert!(fragment.overlaps_range(
            "age",
            Some(&serde_json::json!(20)),
            Some(&serde_json::json!(30)),
        ));

        // Query range [60, 80] -> no overlap (fragment is below)
        assert!(!fragment.overlaps_range(
            "age",
            Some(&serde_json::json!(60)),
            Some(&serde_json::json!(80)),
        ));

        // Query range [0, 5] -> no overlap (fragment is above)
        assert!(!fragment.overlaps_range(
            "age",
            Some(&serde_json::json!(0)),
            Some(&serde_json::json!(5)),
        ));
    }

    #[test]
    fn test_overlaps_range_edge_cases() {
        // Fragment with age: [10, 50]
        let fragment = create_fragment_with_stats(
            "f1",
            "age",
            Some(serde_json::json!(10)),
            Some(serde_json::json!(50)),
            0,
            false,
        );

        // Query range [50, 60] -> overlaps (touching boundary)
        assert!(fragment.overlaps_range(
            "age",
            Some(&serde_json::json!(50)),
            Some(&serde_json::json!(60)),
        ));

        // Query range [0, 10] -> overlaps (touching boundary)
        assert!(fragment.overlaps_range(
            "age",
            Some(&serde_json::json!(0)),
            Some(&serde_json::json!(10)),
        ));
    }

    #[test]
    fn test_overlaps_range_open_ended() {
        // Fragment with age: [10, 50]
        let fragment = create_fragment_with_stats(
            "f1",
            "age",
            Some(serde_json::json!(10)),
            Some(serde_json::json!(50)),
            0,
            false,
        );

        // Query range [None, 30] (unbounded min) -> overlaps
        assert!(fragment.overlaps_range(
            "age",
            None,
            Some(&serde_json::json!(30)),
        ));

        // Query range [30, None] (unbounded max) -> overlaps
        assert!(fragment.overlaps_range(
            "age",
            Some(&serde_json::json!(30)),
            None,
        ));

        // Query range [60, None] -> no overlap
        assert!(!fragment.overlaps_range(
            "age",
            Some(&serde_json::json!(60)),
            None,
        ));
    }

    #[test]
    fn test_overlaps_range_missing_column() {
        // Fragment with only 'age' stats
        let fragment = create_fragment_with_stats(
            "f1",
            "age",
            Some(serde_json::json!(10)),
            Some(serde_json::json!(50)),
            0,
            false,
        );

        // Query on non-existent column -> assume overlap (conservative)
        assert!(fragment.overlaps_range(
            "unknown",
            Some(&serde_json::json!(0)),
            Some(&serde_json::json!(100)),
        ));
    }

    // ============================================================
    // Column Stats Tests
    // ============================================================

    #[test]
    fn test_column_stats_default() {
        let stats = ColumnStats::default();
        assert!(stats.min_value.is_none());
        assert!(stats.max_value.is_none());
        assert_eq!(stats.null_count, 0);
        assert!(stats.distinct_count.is_none());
        assert!(!stats.is_sorted);
        assert!(!stats.is_constant);
    }

    #[test]
    fn test_column_stats_serde() {
        let stats = ColumnStats {
            min_value: Some(serde_json::json!(10)),
            max_value: Some(serde_json::json!(100)),
            null_count: 5,
            distinct_count: Some(50),
            is_sorted: true,
            is_constant: false,
        };

        // Serialize and deserialize
        let json = serde_json::to_string(&stats).unwrap();
        let restored: ColumnStats = serde_json::from_str(&json).unwrap();

        assert_eq!(stats.min_value, restored.min_value);
        assert_eq!(stats.max_value, restored.max_value);
        assert_eq!(stats.null_count, restored.null_count);
        assert_eq!(stats.distinct_count, restored.distinct_count);
        assert_eq!(stats.is_sorted, restored.is_sorted);
        assert_eq!(stats.is_constant, restored.is_constant);
    }
}
