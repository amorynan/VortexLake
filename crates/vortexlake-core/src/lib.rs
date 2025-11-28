//! # VortexLake Core
//!
//! OLAP-optimized storage engine built on Apache Vortex, extended with vector column support.
//! Provides the foundation for unified analytical and similarity search workloads.
//!
//! ## Features
//!
//! - **OLAP-Optimized Storage**: Vortex columnar engine for analytical performance
//! - **Vector Column Support**: Native fixed-size vector columns alongside traditional data
//! - **Fragment Management**: Efficient physical layout for both scans and vector access
//! - **ACID Operations**: Versioned manifests for transactional consistency
//! - **Hybrid Ingestion**: Batch/streaming writes for mixed analytical and vector data
//! - **DataFusion Integration**: SQL analytics with vector extensions
//!
//! ## Example
//!
//! ```rust
//! use vortexlake_core::{VortexLake, Schema, Writer};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create database
//! let db = VortexLake::new("/tmp/vortexlake")?;
//!
//! // Define schema
//! let schema = Schema::new(vec![
//!     Field::new("id", DataType::Utf8, false),
//!     Field::new("vector", DataType::FixedSizeList(
//!         Arc::new(Field::new("item", DataType::Float32, false)),
//!         384
//!     ), false),
//! ])?;
//!
//! // Create table
//! db.create_table("docs", schema)?;
//!
//! // Ingest data
//! let mut writer = db.writer("docs")?;
//! writer.write_batch(batch)?;
//! writer.commit()?;
//! # Ok(())
//! # }
//! ```

pub mod compression;
pub mod config;
pub mod fragment;
pub mod manifest;
pub mod reader;
pub mod schema;
pub mod writer;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::RwLock;

pub use config::{VortexLakeWriteConfig, timeseries_config, vector_config};
pub use schema::{Field, Schema};
pub use writer::Writer;
pub use reader::Reader;
pub use manifest::{Manifest, FragmentMetadata, ColumnStats};

/// Main VortexLake database instance
#[derive(Clone, Debug)]
pub struct VortexLake {
    path: PathBuf,
    manifest: Arc<RwLock<Manifest>>,
}

impl VortexLake {
    /// Create a new VortexLake database at the given path
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Create directory if it doesn't exist
        tokio::fs::create_dir_all(&path).await?;

        // Load existing manifest or create new one
        let manifest_path = path.join("manifest.json");
        let manifest = if manifest_path.exists() {
            Manifest::load(&manifest_path).await?
        } else {
            Manifest::new()
        };

        Ok(Self {
            path,
            manifest: Arc::new(RwLock::new(manifest)),
        })
    }

    /// Create a new table with the given schema
    pub async fn create_table(&self, name: &str, schema: Schema) -> Result<()> {
        let mut manifest: tokio::sync::RwLockWriteGuard<'_, Manifest> = self.manifest.write().await;
        manifest.create_table(name, schema)?;
        self.save_manifest(&manifest).await
    }

    /// Get a writer for batch ingestion
    pub fn writer(&self, table_name: &str) -> Result<Writer> {
        Writer::new(self.path.clone(), table_name, self.manifest.clone())
    }

    /// Get a reader for querying data
    pub fn reader(&self, table_name: &str) -> Result<Reader> {
        Reader::new(self.path.clone(), table_name, self.manifest.clone())
    }

    /// List all tables
    pub async fn list_tables(&self) -> Vec<String> {
        let manifest: tokio::sync::RwLockReadGuard<'_, Manifest> = self.manifest.read().await;
        manifest.tables.keys().cloned().collect()
    }

    /// Get table schema
    pub async fn get_schema(&self, table_name: &str) -> Result<Schema> {
        let manifest: tokio::sync::RwLockReadGuard<'_, Manifest> = self.manifest.read().await;
        manifest.get_schema(table_name).cloned()
    }

    /// Get all fragments for a table
    pub async fn get_fragments(&self, table_name: &str) -> Result<Vec<FragmentMetadata>> {
        let manifest = self.manifest.read().await;
        manifest.get_fragments(table_name).map(|f| f.to_vec())
    }

    /// Save the manifest to disk
    async fn save_manifest(&self, manifest: &Manifest) -> Result<()> {
        let manifest_path = self.path.join("manifest.json");
        manifest.save(&manifest_path).await
    }

    /// Get the database path
    pub fn path(&self) -> &Path {
        &self.path
    }
}