//! LLM-Rerank chunking
//!
//! Best for: High-precision QA
//! Recall Improvement: +8~15%
//! Implementation: Split -> LLM Review -> Merge/Split -> Rerank

use crate::models::{Document, Chunk, ChunkMetadata, Granularity};
use crate::splitter::DocumentSplitter;
use crate::splitter::SplitterConfig;
use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use uuid::Uuid;

pub struct LLMRerankSplitter {
    config: SplitterConfig,
}

impl LLMRerankSplitter {
    pub fn new(config: SplitterConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl DocumentSplitter for LLMRerankSplitter {
    async fn split(&self, document: Document) -> Result<Vec<Chunk>> {
        // Step 1: Initial split (e.g., recursive)
        let initial_chunks = vec![document.content.clone()]; // Placeholder

        // Step 2: LLM Critique & Refine (Mock)
        // "Are these chunks coherent? Merge 1 and 2 if related."
        let refined_chunks = initial_chunks;
        let total_chunks = refined_chunks.len();

        let mut chunks = Vec::new();
        for (i, chunk_content) in refined_chunks.into_iter().enumerate() {
            let metadata = ChunkMetadata {
                title: None,
                section_hierarchy: Vec::new(),
                page_number: None,
                keywords: Vec::new(),
                summary: None,
                is_proposition: false,
                parent_chunk_id: None,
                language: "en".to_string(),
                created_at: Utc::now(),
                custom_fields: document.metadata.clone(),
            };

            chunks.push(Chunk {
                id: Uuid::new_v4(),
                doc_id: document.source_id.clone(),
                content: chunk_content,
                embedding: None,
                metadata,
                chunk_index: i,
                total_chunks,
                granularity: Granularity::Medium,
            });
        }

        Ok(chunks)
    }

    fn name(&self) -> &str {
        "llm_rerank"    
    }

    fn config(&self) -> SplitterConfig {
        self.config.clone()
    }
}
