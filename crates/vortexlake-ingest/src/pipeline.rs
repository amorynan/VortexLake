//! PipelineBuilder for chain-style document ingestion
//!
//! This module provides a fluent builder pattern for configuring
//! and executing document ingestion pipelines.

use std::sync::Arc;
use anyhow::Result;
use async_trait::async_trait;

use crate::models::*;
use crate::splitter::{DocumentSplitter, SplitterConfig};
use crate::metadata::MetadataExtractor;

/// Fluent pipeline builder for document ingestion
pub struct PipelineBuilder {
    splitter: Option<Box<dyn DocumentSplitter>>,
    embedding_fn: Option<Arc<dyn EmbeddingFunction>>,
    metadata_extractor: MetadataExtractor,
    batch_size: usize,
    enable_hierarchy: bool,
}

impl PipelineBuilder {
    /// Create a new pipeline builder
    pub fn new() -> Self {
        Self {
            splitter: None,
            embedding_fn: None,
            metadata_extractor: MetadataExtractor::new(),
            batch_size: 10,
            enable_hierarchy: false,
        }
    }

    /// Set the document splitter
    pub fn with_splitter(mut self, splitter: Box<dyn DocumentSplitter>) -> Self {
        self.splitter = Some(splitter);
        self
    }

    /// Set the embedding function
    pub fn with_embedding(mut self, embedding_fn: Arc<dyn EmbeddingFunction>) -> Self {
        self.embedding_fn = Some(embedding_fn);
        self
    }

    /// Set batch size for processing
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Enable hierarchical chunk relationships
    pub fn with_hierarchy(mut self, enable: bool) -> Self {
        self.enable_hierarchy = enable;
        self
    }

    /// Build the ingestion pipeline
    pub fn build(self) -> Result<IngestionPipeline> {
        let splitter = self.splitter.ok_or_else(|| anyhow::anyhow!("Splitter is required"))?;

        Ok(IngestionPipeline {
            splitter,
            embedding_fn: self.embedding_fn,
            metadata_extractor: self.metadata_extractor,
            batch_size: self.batch_size,
            enable_hierarchy: self.enable_hierarchy,
        })
    }
}

/// Embedding function trait
#[async_trait]
pub trait EmbeddingFunction: Send + Sync {
    /// Generate embedding for text
    async fn embed(&self, text: &str) -> Result<Vec<f32>>;
}

/// Mock embedding function for testing
pub struct MockEmbeddingFunction;

#[async_trait]
impl EmbeddingFunction for MockEmbeddingFunction {
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        // Return a simple mock embedding based on text length
        let dim = 384; // Common embedding dimension
        let mut embedding = Vec::with_capacity(dim);

        for i in 0..dim {
            let value = (text.len() as f32 * (i as f32).sin() * 0.01).tanh();
            embedding.push(value);
        }

        Ok(embedding)
    }
}

/// Ingestion pipeline for processing documents
pub struct IngestionPipeline {
    splitter: Box<dyn DocumentSplitter>,
    embedding_fn: Option<Arc<dyn EmbeddingFunction>>,
    metadata_extractor: MetadataExtractor,
    batch_size: usize,
    enable_hierarchy: bool,
}

impl IngestionPipeline {
    /// Process documents through the pipeline
    pub async fn process(&self, documents: Vec<Document>) -> Result<Vec<ProcessedChunk>> {
        let mut all_chunks = Vec::new();

        // Split documents into chunks
        for doc in documents {
            let chunks = self.splitter.split(doc).await?;
            all_chunks.extend(chunks);
        }

        // Apply hierarchy if enabled
        let chunks = if self.enable_hierarchy {
            use crate::metadata::hierarchy::{HierarchyManager, utils};
            let (hierarchical_chunks, _manager) = HierarchyManager::create_hierarchy_from_chunks(all_chunks);
            hierarchical_chunks
        } else {
            all_chunks
        };

        // Process in batches
        let mut processed = Vec::new();

        for batch in chunks.chunks(self.batch_size) {
            let batch_processed = self.process_batch(batch).await?;
            processed.extend(batch_processed);
        }

        Ok(processed)
    }

    async fn process_batch(&self, batch: &[Chunk]) -> Result<Vec<ProcessedChunk>> {
        let mut processed = Vec::new();

        for chunk in batch {
            let embedding = if let Some(ref embedding_fn) = self.embedding_fn {
                Some(embedding_fn.embed(&chunk.content).await?)
            } else {
                chunk.embedding.clone()
            };

            processed.push(ProcessedChunk::from_chunk(chunk.clone(), embedding));
        }

        Ok(processed)
    }

    /// Quick builder method for simple pipelines
    pub fn builder() -> PipelineBuilder {
        PipelineBuilder::new()
    }
}

/// Evaluation metrics (only exist in tests/ and examples/, never in lib)
pub mod eval {
    use crate::models::*;

    /// Chunk quality report for analyzing splitting effectiveness
    #[derive(Debug, Clone)]
    pub struct ChunkQualityReport {
        /// Semantic coherence (0-1)
        pub semantic_coherence: f32,
        /// Average overlap ratio
        pub avg_overlap_ratio: f32,
        /// Overlap quality score
        pub overlap_quality_score: f32,
        /// Average gap ratio (missing content between chunks)
        pub avg_gap_ratio: f32,
        /// Content coverage (what % of original doc is in chunks)
        pub content_coverage: f32,
        /// Chunk size consistency (lower std_dev = more consistent)
        pub chunk_size_consistency: f32,
    }

    /// End-to-end RAG metrics
    #[derive(Debug, Clone)]
    pub struct EndToEndRagMetrics {
        /// Recall@5 (fraction of relevant chunks retrieved)
        pub recall_at_5: f32,
        /// Mean Reciprocal Rank
        pub mrr: f32,
        /// Answer correctness (0-1)
        pub answer_correctness: f32,
        /// Retrieval latency (ms)
        pub retrieval_latency_ms: f32,
        /// Generation quality score
        pub generation_quality: f32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::splitter::proposition::PropositionSplitter;

    #[tokio::test]
    async fn test_pipeline_builder() {
        let config = SplitterConfig::default();
        let splitter = PropositionSplitter::new(config);

        let pipeline = PipelineBuilder::new()
            .with_splitter(Box::new(splitter))
            .with_batch_size(5)
            .build()
            .unwrap();

        assert_eq!(pipeline.batch_size, 5);
    }

    #[tokio::test]
    async fn test_pipeline_process() {
        let config = SplitterConfig {
            enable_llm_propositions: Some(false),
            max_chunk_size: 512,
            overlap: 50,
            preserve_metadata: true,
            language: None,
            custom_params: std::collections::HashMap::new(),
        };
        let splitter = PropositionSplitter::new(config);

        let pipeline = PipelineBuilder::new()
            .with_splitter(Box::new(splitter))
            .build()
            .unwrap();

        let documents = vec![
            Document {
                content: "This is a test document with multiple sentences. It should be split appropriately.".to_string(),
                metadata: Default::default(),
                source_id: "test_doc".to_string(),
            }
        ];

        let chunks = pipeline.process(documents).await.unwrap();
        assert!(!chunks.is_empty());
    }
}
