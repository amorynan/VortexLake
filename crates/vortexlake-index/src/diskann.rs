//! DiskANN implementation (planned)
//!
//! DiskANN is a graph-based indexing algorithm designed for disk-resident vectors.
//! This is a placeholder implementation until the Vortex DiskANN plugin is available.

use std::path::Path;

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use super::{DistanceMetric, IndexConfig, IndexStats, SearchResult, VectorIndex};

/// DiskANN index implementation (placeholder)
pub struct DiskAnnIndex {
    config: IndexConfig,
    stats: IndexStats,
    is_built: bool,
}

impl DiskAnnIndex {
    /// Create a new DiskANN index
    pub fn new(config: IndexConfig) -> Result<Self> {
        let (degree, complexity, dimension, metric) = match config {
            IndexConfig::DiskAnn {
                degree,
                complexity,
                dimension,
                metric,
            } => (degree, complexity, dimension, metric),
            _ => return Err(anyhow!("Invalid config for DiskANN index")),
        };

        Ok(Self {
            config,
            stats: IndexStats {
                num_vectors: 0,
                dimension,
                index_size_bytes: 0,
                build_time_seconds: 0.0,
                avg_search_time_ms: 0.0,
                metric,
            },
            is_built: false,
        })
    }
}

#[async_trait]
impl VectorIndex for DiskAnnIndex {
    async fn build(&mut self, _vectors: &[Vec<f32>], _ids: &[u64]) -> Result<()> {
        Err(anyhow!("DiskANN implementation is not yet available. Use IVF-PQ for now."))
    }

    async fn search(&self, _query: &[f32], _k: usize) -> Result<Vec<SearchResult>> {
        Err(anyhow!("DiskANN implementation is not yet available. Use IVF-PQ for now."))
    }

    async fn add(&mut self, _vectors: &[Vec<f32>], _ids: &[u64]) -> Result<()> {
        Err(anyhow!("DiskANN implementation is not yet available. Use IVF-PQ for now."))
    }

    async fn remove(&mut self, _ids: &[u64]) -> Result<()> {
        Err(anyhow!("DiskANN implementation is not yet available. Use IVF-PQ for now."))
    }

    async fn save(&self, _path: &Path) -> Result<()> {
        Err(anyhow!("DiskANN implementation is not yet available. Use IVF-PQ for now."))
    }

    async fn load(&mut self, _path: &Path) -> Result<()> {
        Err(anyhow!("DiskANN implementation is not yet available. Use IVF-PQ for now."))
    }

    fn stats(&self) -> IndexStats {
        self.stats.clone()
    }
}
