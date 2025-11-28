//! IVF-PQ (Inverted File with Product Quantization) implementation
//!
//! IVF-PQ is a popular approximate nearest neighbor search algorithm that
//! combines clustering (IVF) with product quantization for efficient search.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use ndarray::Array2;
use rand::prelude::*;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::{DistanceMetric, IndexConfig, IndexStats, SearchResult, VectorIndex};

/// IVF-PQ index implementation
pub struct IvfPqIndex {
    /// Index configuration
    config: IndexConfig,
    /// Coarse quantizer centroids (num_clusters x dimension)
    coarse_centroids: Option<Array2<f32>>,
    /// PQ codebooks (num_subvectors x 256 x subvector_dim)
    pq_codebooks: Option<Vec<Array2<f32>>>,
    /// Inverted file: cluster_id -> (vector_ids, pq_codes)
    inverted_file: HashMap<usize, (Vec<u64>, Vec<Vec<u8>>)>,
    /// Vector ID to cluster mapping
    id_to_cluster: HashMap<u64, usize>,
    /// Statistics
    stats: Arc<RwLock<IndexStats>>,
    /// Whether the index is built
    is_built: bool,
}

impl IvfPqIndex {
    /// Create a new IVF-PQ index
    pub fn new(config: IndexConfig) -> Result<Self> {
        let (num_clusters, num_subvectors, dimension, metric) = match config {
            IndexConfig::IvfPq {
                num_clusters,
                num_subvectors,
                dimension,
                metric,
            } => (num_clusters, num_subvectors, dimension, metric),
            _ => return Err(anyhow!("Invalid config for IVF-PQ index")),
        };

        // Validate parameters
        if dimension % num_subvectors != 0 {
            return Err(anyhow!(
                "Vector dimension ({}) must be divisible by num_subvectors ({})",
                dimension,
                num_subvectors
            ));
        }

        let subvector_dim = dimension / num_subvectors;

        Ok(Self {
            config,
            coarse_centroids: None,
            pq_codebooks: Some(vec![Array2::zeros((256, subvector_dim)); num_subvectors]),
            inverted_file: HashMap::new(),
            id_to_cluster: HashMap::new(),
            stats: Arc::new(RwLock::new(IndexStats {
                num_vectors: 0,
                dimension,
                index_size_bytes: 0,
                build_time_seconds: 0.0,
                avg_search_time_ms: 0.0,
                metric,
            })),
            is_built: false,
        })
    }

    /// Train the coarse quantizer using k-means
    fn train_coarse_quantizer(&mut self, vectors: &[Vec<f32>], num_clusters: usize) -> Result<()> {
        let dimension = self.stats.blocking_read().dimension;
        let mut centroids = Array2::zeros((num_clusters, dimension));

        // Initialize centroids randomly
        let mut rng = rand::thread_rng();
        for i in 0..num_clusters {
            let random_vector = vectors.choose(&mut rng).unwrap();
            for j in 0..dimension {
                centroids[[i, j]] = random_vector[j];
            }
        }

        // K-means training (simplified version)
        let max_iterations = 50;
        for _ in 0..max_iterations {
            let mut cluster_sums = vec![vec![0.0f32; dimension]; num_clusters];
            let mut cluster_counts = vec![0usize; num_clusters];

            // Assign vectors to nearest centroids
            for vector in vectors {
                let nearest_cluster = self.find_nearest_centroid(vector, &centroids);
                for j in 0..dimension {
                    cluster_sums[nearest_cluster][j] += vector[j];
                }
                cluster_counts[nearest_cluster] += 1;
            }

            // Update centroids
            let mut converged = true;
            for i in 0..num_clusters {
                if cluster_counts[i] > 0 {
                    for j in 0..dimension {
                        let new_centroid = cluster_sums[i][j] / cluster_counts[i] as f32;
                        if (new_centroid - centroids[[i, j]]).abs() > 1e-6 {
                            converged = false;
                        }
                        centroids[[i, j]] = new_centroid;
                    }
                }
            }

            if converged {
                break;
            }
        }

        self.coarse_centroids = Some(centroids);
        Ok(())
    }

    /// Train PQ codebooks
    fn train_pq_codebooks(&mut self, vectors: &[Vec<f32>], num_subvectors: usize) -> Result<()> {
        let dimension = self.stats.blocking_read().dimension;
        let subvector_dim = dimension / num_subvectors;

        // Sample vectors for training (use all for now)
        let training_vectors = vectors;

        // Train each codebook
        for subvec_idx in 0..num_subvectors {
            let mut subvectors = Vec::new();

            // Extract subvectors
            for vector in training_vectors {
                let start = subvec_idx * subvector_dim;
                let end = start + subvector_dim;
                subvectors.push(vector[start..end].to_vec());
            }

            // K-means for PQ codebook (256 centroids)
            let codebook = self.train_pq_subvector(&subvectors, subvector_dim)?;
            self.pq_codebooks.as_mut().unwrap()[subvec_idx] = codebook;
        }

        Ok(())
    }

    /// Train PQ codebook for a single subvector
    fn train_pq_subvector(&self, subvectors: &[Vec<f32>], dim: usize) -> Result<Array2<f32>> {
        let num_centroids = 256;
        let mut centroids = Array2::zeros((num_centroids, dim));

        // Initialize centroids
        let mut rng = rand::thread_rng();
        for i in 0..num_centroids {
            let random_subvec = subvectors.choose(&mut rng).unwrap();
            for j in 0..dim {
                centroids[[i, j]] = random_subvec[j];
            }
        }

        // K-means (simplified)
        for _ in 0..20 {
            let mut cluster_sums = vec![vec![0.0f32; dim]; num_centroids];
            let mut cluster_counts = vec![0usize; num_centroids];

            for subvec in subvectors {
                let nearest = self.find_nearest_centroid(subvec, &centroids);
                for j in 0..dim {
                    cluster_sums[nearest][j] += subvec[j];
                }
                cluster_counts[nearest] += 1;
            }

            for i in 0..num_centroids {
                if cluster_counts[i] > 0 {
                    for j in 0..dim {
                        centroids[[i, j]] = cluster_sums[i][j] / cluster_counts[i] as f32;
                    }
                }
            }
        }

        Ok(centroids)
    }

    /// Find nearest centroid to a vector
    fn find_nearest_centroid(&self, vector: &[f32], centroids: &Array2<f32>) -> usize {
        let mut min_distance = f32::INFINITY;
        let mut nearest = 0;

        for (i, centroid) in centroids.outer_iter().enumerate() {
            let distance = self.stats.blocking_read().metric.distance(
                vector,
                centroid.as_slice().unwrap(),
            );
            if distance < min_distance {
                min_distance = distance;
                nearest = i;
            }
        }

        nearest
    }

    /// Encode vector using PQ
    fn encode_pq(&self, vector: &[f32]) -> Result<Vec<u8>> {
        let num_subvectors = self.pq_codebooks.as_ref().unwrap().len();
        let subvector_dim = self.stats.blocking_read().dimension / num_subvectors;
        let mut codes = Vec::with_capacity(num_subvectors);

        for subvec_idx in 0..num_subvectors {
            let start = subvec_idx * subvector_dim;
            let end = start + subvector_dim;
            let subvector = &vector[start..end];

            let codebook = &self.pq_codebooks.as_ref().unwrap()[subvec_idx];
            let code = self.find_nearest_centroid(subvector, codebook) as u8;
            codes.push(code);
        }

        Ok(codes)
    }

    /// Search within a cluster
    fn search_cluster(&self, query: &[f32], cluster_id: usize, k: usize) -> Vec<SearchResult> {
        let (ids, codes) = match self.inverted_file.get(&cluster_id) {
            Some(data) => data,
            None => return Vec::new(),
        };

        // Compute distances using PQ approximation
        let mut results: Vec<(u64, f32)> = ids
            .iter()
            .zip(codes.iter())
            .map(|(&id, code)| {
                let distance = self.approximate_distance(query, code);
                (id, distance)
            })
            .collect();

        // Sort by distance and take top k
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        results
            .into_iter()
            .take(k)
            .map(|(id, distance)| SearchResult { id, distance })
            .collect()
    }

    /// Approximate distance using PQ codes
    fn approximate_distance(&self, query: &[f32], pq_code: &[u8]) -> f32 {
        let num_subvectors = self.pq_codebooks.as_ref().unwrap().len();
        let subvector_dim = self.stats.blocking_read().dimension / num_subvectors;
        let mut total_distance: f32 = 0.0;

        for subvec_idx in 0..num_subvectors {
            let start = subvec_idx * subvector_dim;
            let end = start + subvector_dim;
            let query_subvec = &query[start..end];

            let code = pq_code[subvec_idx] as usize;
            let codebook = &self.pq_codebooks.as_ref().unwrap()[subvec_idx];
            let centroid = codebook.row(code);

            let distance = self.stats.blocking_read().metric.distance(
                query_subvec,
                centroid.as_slice().unwrap(),
            );
            total_distance += distance * distance; // Sum of squared distances
        }

        total_distance.sqrt()
    }
}

#[async_trait]
impl VectorIndex for IvfPqIndex {
    async fn build(&mut self, vectors: &[Vec<f32>], ids: &[u64]) -> Result<()> {
        if vectors.len() != ids.len() {
            return Err(anyhow!("Vectors and IDs must have the same length"));
        }

        let start_time = std::time::Instant::now();

        let (num_clusters, num_subvectors, _, _) = match self.config {
            IndexConfig::IvfPq {
                num_clusters,
                num_subvectors,
                ..
            } => (num_clusters, num_subvectors, 0, DistanceMetric::default()),
            _ => unreachable!(),
        };

        // Train coarse quantizer
        self.train_coarse_quantizer(vectors, num_clusters)?;

        // Train PQ codebooks
        self.train_pq_codebooks(vectors, num_subvectors)?;

        // Assign vectors to clusters and encode
        for (vector, &id) in vectors.iter().zip(ids.iter()) {
            let cluster_id = self.find_nearest_centroid(
                vector,
                self.coarse_centroids.as_ref().unwrap(),
            );

            let pq_code = self.encode_pq(vector)?;
            self.id_to_cluster.insert(id, cluster_id);

            self.inverted_file
                .entry(cluster_id)
                .or_insert_with(|| (Vec::new(), Vec::new()))
                .0
                .push(id);
            self.inverted_file
                .get_mut(&cluster_id)
                .unwrap()
                .1
                .push(pq_code);
        }

        // tokio::task::spawn_blocking(move || {
        //     let mut stats = self.stats.blocking_write();
        //     stats.num_vectors = vectors.len();
        //     stats.build_time_seconds = start_time.elapsed().as_secs_f64();
        //     stats.index_size_bytes = self.calculate_index_size();
        // });
        // self.is_built = true;

        Ok(())
    }

    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        if !self.is_built {
            return Err(anyhow!("Index must be built before searching"));
        }

        let stats = self.stats.read().await;
        if query.len() != stats.dimension {
            return Err(anyhow!(
                "Query dimension {} does not match index dimension {}",
                query.len(),
                stats.dimension
            ));
        }

        let start_time = std::time::Instant::now();

        // Find nearest clusters (probe some clusters)
        let coarse_centroids = self.coarse_centroids.as_ref().unwrap();
        let mut cluster_distances: Vec<(usize, f32)> = (0..coarse_centroids.nrows())
            .map(|i| {
                let centroid = coarse_centroids.row(i);
                let distance = self.stats.blocking_read().metric.distance(
                    query,
                    centroid.as_slice().unwrap(),
                );
                (i, distance)
            })
            .collect();

        cluster_distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        // Search in the nearest clusters
        let num_probes = 10.min(cluster_distances.len());
        let mut all_results = Vec::new();

        for (cluster_id, _) in cluster_distances.into_iter().take(num_probes) {
            let results = self.search_cluster(query, cluster_id, k);
            all_results.extend(results);
        }

        // Sort all results and take top k
        all_results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        let final_results = all_results.into_iter().take(k).collect();

        // Update search time stats
        let search_time = start_time.elapsed().as_millis() as f64;
        drop(stats); // Release read lock
        self.stats.write().await.avg_search_time_ms = search_time;

        Ok(final_results)
    }

    async fn add(&mut self, _vectors: &[Vec<f32>], _ids: &[u64]) -> Result<()> {
        // TODO: Implement incremental addition
        Err(anyhow!("Incremental addition not yet implemented"))
    }

    async fn remove(&mut self, _ids: &[u64]) -> Result<()> {
        // TODO: Implement removal
        Err(anyhow!("Removal not yet implemented"))
    }

    async fn save(&self, _path: &Path) -> Result<()> {
        // TODO: Implement serialization
        Err(anyhow!("Save not yet implemented"))
    }

    async fn load(&mut self, _path: &Path) -> Result<()> {
        // TODO: Implement deserialization
        Err(anyhow!("Load not yet implemented"))
    }

    fn stats(&self) -> IndexStats {
        (*self.stats.blocking_read()).clone()
    }
}

impl IvfPqIndex {
    /// Calculate approximate index size in bytes
    fn calculate_index_size(&self) -> u64 {
        let mut size = 0u64;

        // Coarse centroids
        if let Some(centroids) = &self.coarse_centroids {
            size += (centroids.nrows() * centroids.ncols() * std::mem::size_of::<f32>()) as u64;
        }

        // PQ codebooks
        if let Some(codebooks) = &self.pq_codebooks {
            for codebook in codebooks {
                size += (codebook.nrows() * codebook.ncols() * std::mem::size_of::<f32>()) as u64;
            }
        }

        // Inverted file
        for (_, (ids, codes)) in &self.inverted_file {
            size += (ids.len() * std::mem::size_of::<u64>()) as u64;
            size += (codes.len() * codes[0].len()) as u64;
        }

        size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ivf_pq_basic() {
        let config = IndexConfig::IvfPq {
            num_clusters: 10,
            num_subvectors: 4,
            dimension: 8,
            metric: DistanceMetric::Euclidean,
        };

        let mut index = IvfPqIndex::new(config).unwrap();

        // Create test vectors
        let vectors: Vec<Vec<f32>> = (0..100)
            .map(|i| (0..8).map(|j| (i * 8 + j) as f32).collect())
            .collect();
        let ids: Vec<u64> = (0..100).collect();

        // Build index
        index.build(&vectors, &ids).await.unwrap();
        assert!(index.is_built);

        // Search
        let query = vec![50.0; 8];
        let results = index.search(&query, 5).await.unwrap();
        assert_eq!(results.len(), 5);
        assert!(results[0].distance <= results[1].distance); // Should be sorted
    }
}
