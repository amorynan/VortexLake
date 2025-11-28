//! Batch writer for VortexLake data ingestion
//!
//! Provides high-performance batch writing capabilities with Vortex compression
//! and automatic fragment management.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::record_batch::RecordBatch;
use tokio::sync::RwLock;

use crate::fragment::Fragment;
use crate::manifest::Manifest;
use crate::schema::Schema;

/// Batch writer for data ingestion
pub struct Writer {
    table_name: String,
    base_path: PathBuf,
    manifest: Arc<RwLock<Manifest>>,
    buffer: Vec<RecordBatch>,
    max_buffer_size: usize,
    schema: Schema,
    /// Path for manifest persistence
    manifest_path: PathBuf,
}

impl Writer {
    /// Create a new writer for a table
    pub fn new(
        base_path: PathBuf,
        table_name: &str,
        manifest: Arc<RwLock<Manifest>>,
    ) -> Result<Self> {
        let manifest_read = manifest.try_read().map_err(|_| anyhow!("Manifest is locked"))?;
        let schema = manifest_read
            .get_schema(table_name)?
            .clone();
        let manifest_path = base_path.join("manifest.json");

        Ok(Self {
            table_name: table_name.to_string(),
            base_path,
            manifest: manifest.clone(),
            buffer: Vec::new(),
            max_buffer_size: 100_000, // Default buffer size
            schema,
            manifest_path,
        })
    }

    /// Set the maximum buffer size before automatic flush
    pub fn with_max_buffer_size(mut self, size: usize) -> Self {
        self.max_buffer_size = size;
        self
    }

    /// Write a record batch to the buffer
    pub async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // Validate batch against schema
        self.schema.validate_batch(&batch)?;

        self.buffer.push(batch);

        // Auto-flush if buffer is full
        if self.total_buffered_rows() >= self.max_buffer_size {
            self.flush().await?;
        }

        Ok(())
    }

    /// Write multiple batches
    pub async fn write_batches(&mut self, batches: Vec<RecordBatch>) -> Result<()> {
        for batch in batches {
            self.write_batch(batch).await?;
        }
        Ok(())
    }

    /// Flush buffered data to disk as a new fragment
    pub async fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Combine all buffered batches
        let combined_batch = arrow::compute::concat_batches(
            &Arc::new(self.schema.to_arrow()),
            &self.buffer,
        )?;

        // Create new fragment (generates its own UUID)
        let fragment = Fragment::new(self.table_name.clone(), &combined_batch)?;
        
        // Use the fragment's ID for the file path
        let fragment_path = self.base_path
            .join("data")
            .join(format!("{}.vortex", fragment.id));

        // Ensure data directory exists
        tokio::fs::create_dir_all(fragment_path.parent().unwrap()).await?;

        // Write fragment using Vortex format
        fragment.write_data_to_path(&fragment_path, &combined_batch).await?;

        // Update manifest and persist to disk
        {
            let mut manifest = self.manifest.write().await;
            manifest.add_fragment(&self.table_name, fragment)?;
            // Persist manifest to ensure durability
            manifest.save(&self.manifest_path).await?;
        }

        // Clear buffer
        self.buffer.clear();

        Ok(())
    }

    /// Commit all pending writes and finalize the writer
    pub async fn commit(mut self) -> Result<()> {
        self.flush().await?;
        Ok(())
    }

    /// Get the total number of buffered rows
    pub fn total_buffered_rows(&self) -> usize {
        self.buffer.iter().map(|batch| batch.num_rows()).sum()
    }

    /// Get the number of buffered batches
    pub fn buffered_batches(&self) -> usize {
        self.buffer.len()
    }

    /// Check if the writer has any buffered data
    pub fn has_buffered_data(&self) -> bool {
        !self.buffer.is_empty()
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        // Note: In a real implementation, we'd want to ensure
        // buffered data is flushed before dropping. However,
        // async drop is not stable yet, so this is a compromise.
        if !self.buffer.is_empty() {
            tracing::warn!("Writer dropped with uncommitted data");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Array, StringArray};
    use arrow::datatypes::DataType;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_writer_basic() {
        let temp_dir = tempdir().unwrap();
        let mut manifest = Manifest::new();
        let schema = Schema::new(vec![
            crate::schema::Field::new("id", DataType::Utf8, false),
            crate::schema::Field::new("value", DataType::Float32, false),
        ]).unwrap();

        manifest.create_table("test", schema.clone()).unwrap();
        let manifest = Arc::new(RwLock::new(manifest));

        let mut writer = Writer::new(
            temp_dir.path().to_path_buf(),
            "test",
            manifest,
        ).unwrap();

        // Create test batch
        let batch = RecordBatch::try_new(
            Arc::new(schema.to_arrow()),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0])),
            ],
        ).unwrap();

        writer.write_batch(batch).await.unwrap();
        assert_eq!(writer.total_buffered_rows(), 3);

        writer.flush().await.unwrap();
        assert_eq!(writer.total_buffered_rows(), 0);
    }
}
