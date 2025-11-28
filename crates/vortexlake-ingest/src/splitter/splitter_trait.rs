//! Document splitter trait and core interfaces

use crate::models::{Document, Chunk};
use anyhow::Result;
use async_trait::async_trait;

/// Document splitter trait (returns Chunk, not Document!)
#[async_trait]
pub trait DocumentSplitter: Send + Sync {
    /// Split a document into chunks (returns Chunk, not Document!)
    async fn split(&self, document: Document) -> Result<Vec<Chunk>>;

    /// Get splitter name for identification
    fn name(&self) -> &str;

    /// Get configuration description
    fn config(&self) -> SplitterConfig;
}

/// Configuration for all splitters
#[derive(Debug, Clone)]
pub struct SplitterConfig {
    pub enable_llm_propositions: Option<bool>,  // 开启 LLM 模式
    /// Maximum chunk size (characters)
    pub max_chunk_size: usize,
    /// Overlap between chunks (characters)
    pub overlap: usize,
    /// Preserve original document metadata
    pub preserve_metadata: bool,
    /// Language for processing
    pub language: Option<String>,
    /// Custom parameters
    pub custom_params: std::collections::HashMap<String, serde_json::Value>,
}

impl Default for SplitterConfig {
    fn default() -> Self {
        Self {
            max_chunk_size: 512,
            overlap: 50,
            preserve_metadata: true,
            language: None,
            custom_params: std::collections::HashMap::new(),
            enable_llm_propositions: Some(false),
        }
    }
}

/// Factory function to create splitters by type
pub fn create_splitter(splitter_type: &str, config: SplitterConfig) -> Result<Box<dyn DocumentSplitter>> {
    match splitter_type {
        "proposition" => Ok(Box::new(crate::splitter::proposition::PropositionSplitter::new(config))),
        "hierarchical" => Ok(Box::new(crate::splitter::hierarchical::HierarchicalSplitter::new(config))),
        "llm_rerank" => Ok(Box::new(crate::splitter::llm_rerank::LLMRerankSplitter::new(config))),
        _ => Err(anyhow::anyhow!("Unknown splitter type: {}", splitter_type)),
    }
}
