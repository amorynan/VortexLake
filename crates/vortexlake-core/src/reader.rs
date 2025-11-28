//! Reader for VortexLake data
//!
//! Provides efficient data reading capabilities from Vortex format files.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use tokio::sync::RwLock;

use crate::fragment::Fragment;
use crate::manifest::Manifest;
use crate::schema::Schema;

/// Reader for querying VortexLake tables
#[derive(Debug)]
pub struct Reader {
    table_name: String,
    base_path: PathBuf,
    manifest: Arc<RwLock<Manifest>>,
    schema: Schema,
}

impl Reader {
    /// Create a new reader for a table
    pub fn new(
        base_path: PathBuf,
        table_name: &str,
        manifest: Arc<RwLock<Manifest>>,
    ) -> Result<Self> {
        let manifest_read = manifest
            .try_read()
            .map_err(|_| anyhow::anyhow!("Manifest is locked"))?;
        let schema = manifest_read.get_schema(table_name)?.clone();

        Ok(Self {
            table_name: table_name.to_string(),
            base_path,
            manifest: manifest.clone(),
            schema,
        })
    }

    /// Read all fragments for this table
    pub async fn read_all(&self) -> Result<Vec<RecordBatch>> {
        let manifest = self.manifest.read().await;
        let fragments = manifest.get_fragments(&self.table_name)?;

        let mut batches = Vec::new();
        for fragment in fragments {
            let fragment_path = self
                .base_path
                .join("data")
                .join(format!("{}.vortex", fragment.id));

            // Use Vortex format reader
            let batch = Fragment::read_from_path(&fragment_path).await?;
            batches.push(batch);
        }

        Ok(batches)
    }

    /// Read all data combined into a single RecordBatch
    pub async fn read_all_combined(&self) -> Result<RecordBatch> {
        let batches = self.read_all().await?;

        if batches.is_empty() {
            return Err(anyhow::anyhow!("No data found for table {}", self.table_name));
        }

        let schema = batches[0].schema();
        let combined = arrow::compute::concat_batches(&schema, &batches)?;
        Ok(combined)
    }

    /// Read with column projection
    pub async fn read_with_projection(&self, column_names: &[&str]) -> Result<Vec<RecordBatch>> {
        let manifest = self.manifest.read().await;
        let fragments = manifest.get_fragments(&self.table_name)?;

        let mut batches = Vec::new();
        for fragment in fragments {
            let fragment_path = self
                .base_path
                .join("data")
                .join(format!("{}.vortex", fragment.id));

            let batch = Fragment::read_with_projection(&fragment_path, column_names).await?;
            batches.push(batch);
        }

        Ok(batches)
    }

    /// Get the table schema
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Get table name
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Get the base path
    pub fn base_path(&self) -> &PathBuf {
        &self.base_path
    }

    /// Get row count estimate (sum of all fragment row counts)
    pub async fn row_count(&self) -> Result<usize> {
        let manifest = self.manifest.read().await;
        let fragments = manifest.get_fragments(&self.table_name)?;
        Ok(fragments.iter().map(|f| f.row_count).sum())
    }

    /// Get total data size estimate (sum of all fragment sizes)
    pub async fn total_size_bytes(&self) -> Result<u64> {
        let manifest = self.manifest.read().await;
        let fragments = manifest.get_fragments(&self.table_name)?;
        Ok(fragments.iter().map(|f| f.size_bytes).sum())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_reader_basic() {
        let temp_dir = tempdir().unwrap();
        let mut manifest = Manifest::new();
        let schema = Schema::new(vec![
            crate::schema::Field::new("id", DataType::Utf8, false),
            crate::schema::Field::new("value", DataType::Float32, false),
        ])
        .unwrap();

        manifest.create_table("test", schema.clone()).unwrap();
        let manifest = Arc::new(RwLock::new(manifest));

        let reader = Reader::new(temp_dir.path().to_path_buf(), "test", manifest).unwrap();

        assert_eq!(reader.schema().fields.len(), 2);
        assert_eq!(reader.table_name(), "test");
    }
}
