//! # VortexLake Index
//!
//! Vector indexing layer that enhances columnar OLAP storage with similarity search capabilities.
//!
//! ## Features
//!
//! - **IVF-PQ**: Inverted File with Product Quantization for approximate nearest neighbors
//! - **DiskANN**: Graph-based indexing for disk-resident vectors (planned Vortex integration)
//! - **Metadata Indexes**: Inverted, bitmap, and range indexes for hybrid filtering
//! - **OLAP Compatible**: Indexes built on columnar storage, preserving analytical performance
//! - **Pluggable Architecture**: Extensible for custom indexing and distance metrics
//!
//! ## Example
//!
//! ```rust
//! use vortexlake_index::{VectorIndex, IvfPqIndex, IndexConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create IVF-PQ index
//! let config = IndexConfig::IvfPq {
//!     num_clusters: 1000,
//!     num_subvectors: 32,
//!     dimension: 384,
//! };
//!
//! let mut index = IvfPqIndex::new(config)?;
//!
//! // Build index
//! let vectors = vec![/* your vectors */];
//! let ids = vec![/* vector ids */];
//! index.build(&vectors, &ids).await?;
//!
//! // Search
//! let query = vec![0.1f32; 384];
//! let results = index.search(&query, 10).await?;
//! # Ok(())
//! # }
//! ```

pub mod ivf_pq;
pub mod diskann;
pub mod metadata_index;

use std::path::Path;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use ivf_pq::IvfPqIndex;
pub use diskann::DiskAnnIndex;
pub use metadata_index::{MetadataIndex, InvertedIndex, BitmapIndex};

/// Search result containing ID and distance
#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    /// Vector ID
    pub id: u64,
    /// Distance to query vector
    pub distance: f32,
}

impl PartialOrd for SearchResult {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.distance.partial_cmp(&other.distance)
    }
}

/// Distance metrics for vector comparison
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DistanceMetric {
    /// Euclidean distance (L2)
    Euclidean,
    /// Cosine similarity (converted to distance)
    Cosine,
    /// Dot product
    DotProduct,
    /// Manhattan distance (L1)
    Manhattan,
}

impl Default for DistanceMetric {
    fn default() -> Self {
        DistanceMetric::Euclidean
    }
}

impl DistanceMetric {
    /// Compute distance between two vectors
    pub fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            DistanceMetric::Euclidean => euclidean_distance(a, b),
            DistanceMetric::Cosine => cosine_distance(a, b),
            DistanceMetric::DotProduct => -dot_product(a, b), // Negative for distance
            DistanceMetric::Manhattan => manhattan_distance(a, b),
        }
    }
}

/// Configuration for different index types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexConfig {
    /// IVF-PQ configuration
    IvfPq {
        /// Number of clusters (coarse quantizer)
        num_clusters: usize,
        /// Number of subvectors (fine quantizer)
        num_subvectors: usize,
        /// Vector dimension
        dimension: usize,
        /// Distance metric
        metric: DistanceMetric,
    },
    /// DiskANN configuration (planned)
    DiskAnn {
        /// Graph degree
        degree: usize,
        /// Build complexity
        complexity: usize,
        /// Vector dimension
        dimension: usize,
        /// Distance metric
        metric: DistanceMetric,
    },
}

/// Common trait for all vector indexes
#[async_trait]
pub trait VectorIndex: Send + Sync {
    /// Build the index from vectors and their IDs
    async fn build(&mut self, vectors: &[Vec<f32>], ids: &[u64]) -> anyhow::Result<()>;

    /// Search for nearest neighbors
    async fn search(&self, query: &[f32], k: usize) -> anyhow::Result<Vec<SearchResult>>;

    /// Add vectors to an existing index
    async fn add(&mut self, vectors: &[Vec<f32>], ids: &[u64]) -> anyhow::Result<()>;

    /// Remove vectors from the index
    async fn remove(&mut self, ids: &[u64]) -> anyhow::Result<()>;

    /// Save index to disk
    async fn save(&self, path: &Path) -> anyhow::Result<()>;

    /// Load index from disk
    async fn load(&mut self, path: &Path) -> anyhow::Result<()>;

    /// Get index statistics
    fn stats(&self) -> IndexStats;
}

/// Index statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStats {
    /// Number of vectors in the index
    pub num_vectors: usize,
    /// Vector dimension
    pub dimension: usize,
    /// Index size in bytes
    pub index_size_bytes: u64,
    /// Build time in seconds
    pub build_time_seconds: f64,
    /// Average search time in milliseconds
    pub avg_search_time_ms: f64,
    /// Distance metric used
    pub metric: DistanceMetric,
}

/// Create a vector index from configuration
pub async fn create_index(config: IndexConfig) -> anyhow::Result<Box<dyn VectorIndex>> {
    match config {
        IndexConfig::IvfPq { .. } => {
            let index = IvfPqIndex::new(config)?;
            Ok(Box::new(index))
        }
        IndexConfig::DiskAnn { .. } => {
            let index = DiskAnnIndex::new(config)?;
            Ok(Box::new(index))
        }
    }
}

// Distance computation functions

fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt()
}

fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let dot = dot_product(a, b);
    let norm_a = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        1.0
    } else {
        1.0 - (dot / (norm_a * norm_b))
    }
}

fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

fn manhattan_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y).abs())
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distance_metrics() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];

        let euclidean = DistanceMetric::Euclidean.distance(&a, &b);
        assert!(euclidean > 0.0);

        let cosine = DistanceMetric::Cosine.distance(&a, &b);
        assert!(cosine >= 0.0 && cosine <= 2.0);

        let dot = DistanceMetric::DotProduct.distance(&a, &b);
        assert_eq!(dot, -dot_product(&a, &b));
    }

    #[test]
    fn test_search_result_ordering() {
        let mut results = vec![
            SearchResult { id: 1, distance: 0.5 },
            SearchResult { id: 2, distance: 0.3 },
            SearchResult { id: 3, distance: 0.7 },
        ];

        results.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(results[0].id, 2); // Smallest distance first
        assert_eq!(results[1].id, 1);
        assert_eq!(results[2].id, 3);
    }
}
