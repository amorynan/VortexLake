//! Physical fragment management for VortexLake
//!
//! Fragments are the basic unit of physical storage, containing
//! compressed columnar data with metadata for efficient access.
//! Uses Vortex format for high-performance columnar storage.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use tokio::fs;
use uuid::Uuid;
use vortex::VortexSessionDefault;
use vortex::compute::min_max;
use vortex_array::arrow::FromArrowArray;
use vortex_array::stream::ArrayStreamExt;
use vortex_array::ArrayRef;
use vortex_file::VortexFile;
use vortex_file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
use vortex_session::VortexSession;

use crate::manifest::ColumnStats;

/// A physical fragment containing columnar data in Vortex format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fragment {
    /// Unique fragment ID
    pub id: String,
    /// Table this fragment belongs to
    pub table_name: String,
    /// Row count in this fragment
    pub row_count: usize,
    /// Size in bytes (estimated in memory)
    pub size_bytes: u64,
    /// Column statistics for query optimization
    pub column_stats: HashMap<String, ColumnStats>,
    /// Compression algorithm used
    pub compression: CompressionType,
    /// Additional metadata
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum CompressionType {
    /// No compression
    None,
    /// Zstandard compression
    Zstd,
    /// LZ4 compression
    Lz4,
    /// Vortex native compression (default)
    #[default]
    Vortex,
}

impl Fragment {
    /// Create a new fragment from a record batch
    pub fn new(table_name: String, batch: &RecordBatch) -> Result<Self> {
        let id = Uuid::new_v4().to_string();
        let row_count = batch.num_rows();
        let size_bytes = batch.get_array_memory_size() as u64;

        // Compute column statistics
        let column_stats = Self::compute_column_stats(batch)?;

        Ok(Self {
            id,
            table_name,
            row_count,
            size_bytes,
            column_stats,
            compression: CompressionType::Vortex,
            metadata: serde_json::Value::Null,
        })
    }

    /// Write fragment data to a file path using Vortex format
    pub async fn write_data_to_path(&self, path: &Path, batch: &RecordBatch) -> Result<()> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Create Vortex session with default configuration
        let session = VortexSession::default();

        // Convert Arrow RecordBatch to Vortex Array
        // RecordBatch implements From<&RecordBatch> for ArrayRef
        let nullable = batch.schema().fields().iter().any(|f| f.is_nullable());
        let vortex_array: ArrayRef = ArrayRef::from_arrow(batch, nullable);

        // Write using Vortex file format
        let mut file = tokio::fs::File::create(path).await?;
        session
            .write_options()
            .write(&mut file, vortex_array.to_array_stream())
            .await?;

        // Write metadata to a separate JSON file
        let metadata_path = path.with_extension("meta.json");
        let metadata = serde_json::to_vec_pretty(self)?;
        fs::write(metadata_path, metadata).await?;

        Ok(())
    }

    /// Read fragment data from a file path using Vortex format
    pub async fn read_from_path(path: &Path) -> Result<RecordBatch> {
        // Create Vortex session
        let session = VortexSession::default();

        // Open and read Vortex file
        let vortex_file: VortexFile = session.open_options().open(path).await?;

        // Scan and read all data
        let array: ArrayRef = vortex_file
            .scan()?
            .into_array_stream()?
            .read_all()
            .await?;

        // Convert Vortex array back to Arrow RecordBatch
        // Use TryFrom<&dyn Array> for RecordBatch
        let record_batch = RecordBatch::try_from(array.as_ref())?;

        Ok(record_batch)
    }

    /// Read fragment data with projection (only selected columns)
    pub async fn read_with_projection(path: &Path, column_names: &[&str]) -> Result<RecordBatch> {
        use vortex_array::expr::{root, select};

        let session = VortexSession::default();

        let vortex_file: VortexFile = session.open_options().open(path).await?;

        // Build projection expression using select
        // select takes field names and a child expression (root for top-level)
        let field_names: Vec<Arc<str>> = column_names.iter().map(|s| Arc::from(*s)).collect();
        let projection_expr = select(field_names, root());

        // Create projection and scan
        let array: ArrayRef = vortex_file
            .scan()?
            .with_projection(projection_expr)
            .into_array_stream()?
            .read_all()
            .await?;

        // Convert to RecordBatch
        let record_batch = RecordBatch::try_from(array.as_ref())?;

        Ok(record_batch)
    }

    /// Read fragment metadata from a file path
    pub async fn read_metadata_from_path(path: &Path) -> Result<Fragment> {
        let metadata_path = path.with_extension("meta.json");
        let data = fs::read(metadata_path).await?;
        let fragment: Fragment = serde_json::from_slice(&data)?;
        Ok(fragment)
    }

    /// Write metadata only (for backward compatibility)
    pub async fn write_to_path(&self, path: &Path) -> Result<()> {
        let metadata_path = path.with_extension("meta.json");
        let data = serde_json::to_vec_pretty(self)?;
        fs::write(metadata_path, data).await?;
        Ok(())
    }

    /// Compute column statistics for query optimization
    fn compute_column_stats(batch: &RecordBatch) -> Result<HashMap<String, ColumnStats>> {
        use arrow::array::{
            Array, Decimal128Array, Decimal256Array, Decimal32Array, Decimal64Array, Float32Array,
            Float64Array, Int32Array, Int64Array, LargeStringArray, StringArray,
        };
        use arrow::datatypes::DataType;

        let mut stats = HashMap::new();

        for (i, field) in batch.schema().fields().iter().enumerate() {
            let array = batch.column(i);
            let null_count = array.null_count();

            // Compute min/max and additional stats for numeric types
            let (min_value, max_value, is_sorted, is_constant) = match field.data_type() {
                DataType::Int32 => {
                    let arr = array.as_any().downcast_ref::<Int32Array>();
                    if let Some(arr) = arr {
                        let min = arrow::compute::min(arr);
                        let max = arrow::compute::max(arr);
                        let is_constant = min.is_some() && min == max && arr.len() > 0;
                        let is_sorted = Self::check_sorted(arr);
                        (
                            min.map(|v| serde_json::json!(v)),
                            max.map(|v| serde_json::json!(v)),
                            is_sorted,
                            is_constant,
                        )
                    } else {
                        (None, None, false, false)
                    }
                }
                DataType::Int64 => {
                    let arr = array.as_any().downcast_ref::<Int64Array>();
                    if let Some(arr) = arr {
                        let min = arrow::compute::min(arr);
                        let max = arrow::compute::max(arr);
                        let is_constant = min.is_some() && min == max && arr.len() > 0;
                        let is_sorted = Self::check_sorted(arr);
                        (
                            min.map(|v| serde_json::json!(v)),
                            max.map(|v| serde_json::json!(v)),
                            is_sorted,
                            is_constant,
                        )
                    } else {
                        (None, None, false, false)
                    }
                }
                DataType::Float32 => {
                    let arr = array.as_any().downcast_ref::<Float32Array>();
                    if let Some(arr) = arr {
                        let min = arrow::compute::min(arr);
                        let max = arrow::compute::max(arr);
                        let is_constant = min.is_some() && min == max && arr.len() > 0;
                        let is_sorted = Self::check_sorted(arr);
                        (
                            min.map(|v| serde_json::json!(v)),
                            max.map(|v| serde_json::json!(v)),
                            is_sorted,
                            is_constant,
                        )
                    } else {
                        (None, None, false, false)
                    }
                }
                DataType::Float64 => {
                    let arr = array.as_any().downcast_ref::<Float64Array>();
                    if let Some(arr) = arr {
                        let min = arrow::compute::min(arr);
                        let max = arrow::compute::max(arr);
                        let is_constant = min.is_some() && min == max && arr.len() > 0;
                        let is_sorted = Self::check_sorted(arr);
                        (
                            min.map(|v| serde_json::json!(v)),
                            max.map(|v| serde_json::json!(v)),
                            is_sorted,
                            is_constant,
                        )
                    } else {
                        (None, None, false, false)
                    }
                }
                DataType::Utf8 => {
                    let arr = array.as_any().downcast_ref::<StringArray>();
                    if let Some(arr) = arr {
                        let min = arr.iter().flatten().min();
                        let max = arr.iter().flatten().max();
                        let is_constant = min.is_some() && min == max && arr.len() > 0;
                        let is_sorted = Self::check_sorted_utf8_generic(arr);
                        (
                            min.map(|v| serde_json::json!(v)),
                            max.map(|v| serde_json::json!(v)),
                            is_sorted,
                            is_constant,
                        )
                    } else {
                        (None, None, false, false)
                    }
                }
                DataType::LargeUtf8 => {
                    let arr = array.as_any().downcast_ref::<LargeStringArray>();
                    if let Some(arr) = arr {
                        let min = arr.iter().flatten().min();
                        let max = arr.iter().flatten().max();
                        let is_constant = min.is_some() && min == max && arr.len() > 0;
                        let is_sorted = Self::check_sorted_utf8_generic(arr);
                        (
                            min.map(|v| serde_json::json!(v)),
                            max.map(|v| serde_json::json!(v)),
                            is_sorted,
                            is_constant,
                        )
                    } else {
                        (None, None, false, false)
                    }
                }
                DataType::Decimal32(_precision, _scale) => {
                    let arr = array.as_any().downcast_ref::<Decimal32Array>();
                    if let Some(arr) = arr {
                        let min = arrow::compute::min(arr);
                        let max = arrow::compute::max(arr);
                        let is_constant = min.is_some() && min == max && arr.len() > 0;
                        let is_sorted = Self::check_sorted(arr);
                        (
                            min.map(|v| serde_json::json!(v)),
                            max.map(|v| serde_json::json!(v)),
                            is_sorted,
                            is_constant,
                        )
                    } else {
                        (None, None, false, false)
                    }
                }
                DataType::Decimal64(_precision, _scale) => {
                    let arr = array.as_any().downcast_ref::<Decimal64Array>();
                    if let Some(arr) = arr {
                        let min = arrow::compute::min(arr);
                        let max = arrow::compute::max(arr);
                        let is_constant = min.is_some() && min == max && arr.len() > 0;
                        let is_sorted = Self::check_sorted(arr);
                        (
                            min.map(|v| serde_json::json!(v)),
                            max.map(|v| serde_json::json!(v)),
                            is_sorted,
                            is_constant,
                        )
                    } else {
                        (None, None, false, false)
                    }
                }
                DataType::Decimal128(_precision, _scale) => {
                    let arr = array.as_any().downcast_ref::<Decimal128Array>();
                    if let Some(arr) = arr {
                            let min = arrow::compute::min(arr);
                        let max = arrow::compute::max(arr);
                        let is_constant = min.is_some() && min == max && arr.len() > 0;
                        let is_sorted = Self::check_sorted(arr);
                        (
                            min.map(|v| serde_json::json!(v)),
                            max.map(|v| serde_json::json!(v)),
                            is_sorted,
                            is_constant,
                        )
                    } else {
                        (None, None, false, false)
                    }
                }
                DataType::Decimal256(_precision, _scale) => {
                    let arr = array.as_any().downcast_ref::<Decimal256Array>();
                    if let Some(arr) = arr {
                        let min = arr.iter().flatten().min();
                        let max = arr.iter().flatten().max();
                        let is_constant = min.is_some() && min == max && arr.len() > 0;
                        let is_sorted = Self::check_sorted(arr);
                        (
                            min.map(|v| serde_json::json!(v.to_string())),
                            max.map(|v| serde_json::json!(v.to_string())),
                            is_sorted,
                            is_constant,
                        )
                    } else {
                        (None, None, false, false)
                    }
                }
                _ => (None, None, false, false),
            };

            let column_stats = ColumnStats {
                min_value,
                max_value,
                null_count,
                distinct_count: None, // TODO: Estimate distinct count using HyperLogLog
                is_sorted,
                is_constant,
            };

            stats.insert(field.name().clone(), column_stats);
        }

        Ok(stats)
    }

    /// Check if a PrimitiveArray is sorted in ascending order
    /// 
    /// This generic function replaces the type-specific check_sorted_* functions
    /// for Int32, Int64, Float32, and Float64 arrays.
    fn check_sorted<T>(arr: &arrow::array::PrimitiveArray<T>) -> bool
    where
        T: arrow::array::ArrowPrimitiveType,
        T::Native: PartialOrd,
    {
        use arrow::array::Array;
        if arr.len() <= 1 {
            return true;
        }
        let mut prev: Option<T::Native> = None;
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let val = arr.value(i);
            if let Some(p) = prev {
                if val < p {
                    return false;
                }
            }
            prev = Some(val);
        }
        true
    }

    /// Check if a UTF-8 array (utf8 or largeutf8) is sorted in ascending order
    fn check_sorted_utf8_generic<O: arrow::array::OffsetSizeTrait>(
        arr: &arrow::array::GenericStringArray<O>,
    ) -> bool {
        use arrow::array::Array;
        if arr.len() <= 1 {
            return true;
        }
        let mut prev: Option<&str> = None;
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let val = arr.value(i);
            if let Some(p) = prev {
                if val < p {
                    return false;
                }
            }
            prev = Some(val);
        }
        true
    }

    /// Get fragment file size on disk
    pub async fn file_size(&self, base_path: &Path) -> Result<u64> {
        let path = base_path
            .join("data")
            .join(format!("{}.vortex", self.id));
        let metadata = fs::metadata(path).await?;
        Ok(metadata.len())
    }

    /// Check if fragment exists on disk
    pub async fn exists(&self, base_path: &Path) -> bool {
        let path = base_path
            .join("data")
            .join(format!("{}.vortex", self.id));
        fs::try_exists(&path).await.unwrap_or(false)
    }
}

/// Fragment manager for coordinating multiple fragments
pub struct FragmentManager {
    base_path: std::path::PathBuf,
}

impl FragmentManager {
    /// Create a new fragment manager
    pub fn new(base_path: std::path::PathBuf) -> Self {
        Self { base_path }
    }

    /// Compact multiple fragments into fewer, larger ones
    pub async fn compact(&self, fragments: Vec<Fragment>) -> Result<Vec<Fragment>> {
        if fragments.is_empty() {
            return Ok(vec![]);
        }

        // Read all fragments
        let mut all_batches = Vec::new();
        let table_name = fragments[0].table_name.clone();

        for fragment in &fragments {
            let path = self
                .base_path
                .join("data")
                .join(format!("{}.vortex", fragment.id));
            let batch = Fragment::read_from_path(&path).await?;
            all_batches.push(batch);
        }

        // Combine all batches
        if all_batches.is_empty() {
            return Ok(vec![]);
        }

        let schema = all_batches[0].schema();
        let combined = arrow::compute::concat_batches(&schema, &all_batches)?;

        // Create new fragment
        let new_fragment = Fragment::new(table_name, &combined)?;
        let new_path = self
            .base_path
            .join("data")
            .join(format!("{}.vortex", new_fragment.id));
        new_fragment.write_data_to_path(&new_path, &combined).await?;

        Ok(vec![new_fragment])
    }

    /// Delete old fragments that are no longer referenced
    pub async fn cleanup(&self, active_fragment_ids: &[String]) -> Result<()> {
        let data_path = self.base_path.join("data");
        if !data_path.exists() {
            return Ok(());
        }

        let mut entries = fs::read_dir(&data_path).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if let Some(stem) = path.file_stem() {
                let stem_str = stem.to_string_lossy();
                // Check if this fragment is still active
                if !active_fragment_ids.iter().any(|id| stem_str.starts_with(id)) {
                    // Delete the file
                    let _ = fs::remove_file(&path).await;
                }
            }
        }

        Ok(())
    }

    /// Get fragment metadata by ID
    pub async fn get_fragment(&self, fragment_id: &str) -> Result<Fragment> {
        let metadata_path = self
            .base_path
            .join("data")
            .join(format!("{}.meta.json", fragment_id));

        let data = fs::read(metadata_path).await?;
        let fragment: Fragment = serde_json::from_slice(&data)?;
        Ok(fragment)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use tempfile::tempdir;

    fn create_test_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Arc::new(Field::new("id", DataType::Utf8, false)),
                Arc::new(Field::new("value", DataType::Float32, false)),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_fragment_creation() {
        let batch = create_test_batch();
        let fragment = Fragment::new("test_table".to_string(), &batch).unwrap();

        assert_eq!(fragment.table_name, "test_table");
        assert_eq!(fragment.row_count, 3);
        assert!(fragment.size_bytes > 0);
        assert_eq!(fragment.column_stats.len(), 2);
    }

    #[tokio::test]
    async fn test_fragment_write_read_vortex() {
        let temp_dir = tempdir().unwrap();
        let batch = create_test_batch();
        let fragment = Fragment::new("test_table".to_string(), &batch).unwrap();

        // Write fragment using Vortex format
        let path = temp_dir.path().join("data").join("test.vortex");
        fragment.write_data_to_path(&path, &batch).await.unwrap();

        // Verify files were created
        assert!(path.exists());
        assert!(path.with_extension("meta.json").exists());

        // Read fragment back
        let read_batch = Fragment::read_from_path(&path).await.unwrap();

        assert_eq!(read_batch.num_rows(), 3);
        assert_eq!(read_batch.num_columns(), 2);
    }

    #[test]
    fn test_compression_type_default() {
        assert!(matches!(
            CompressionType::default(),
            CompressionType::Vortex
        ));
    }
}
