//! Core data models for VortexLake ingestion
//!
//! This module defines the fundamental data structures used throughout
//! the ingestion pipeline, including Document, Chunk, and rich metadata.

use std::collections::HashMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Granularity levels for hierarchical chunking
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Granularity {
    Small,
    Medium,
    Large,
}

/// A document to be ingested (core input from any source)
#[derive(Clone, Debug)]
pub struct Document {
    /// Document content
    pub content: String,
    /// Document metadata (extremely important! All original metadata here)
    pub metadata: HashMap<String, serde_json::Value>,
    /// Source identifier
    pub source_id: String,
}

/// Super rich metadata for chunks (this is where you crush the competition)
#[derive(Clone, Debug, Serialize)]
pub struct ChunkMetadata {
    /// Document title
    pub title: Option<String>,
    /// Section hierarchy (e.g., ["Chapter 1", "1.1 Background"])
    pub section_hierarchy: Vec<String>,
    /// Page number
    pub page_number: Option<i32>,
    /// Extracted keywords
    pub keywords: Vec<String>,
    /// Chunk summary
    pub summary: Option<String>,
    /// Whether this chunk represents a proposition
    pub is_proposition: bool,
    /// Parent chunk ID for hierarchical retrieval
    pub parent_chunk_id: Option<Uuid>,
    /// Language code
    pub language: String,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Custom metadata extensions
    pub custom_fields: HashMap<String, serde_json::Value>,
}

/// A chunk ready for indexing in Vortex (core output unit)
#[derive(Clone, Debug, Serialize)]
pub struct Chunk {
    /// Unique chunk ID
    pub id: Uuid,
    /// Original document ID this chunk belongs to
    pub doc_id: String,
    /// Chunk content
    pub content: String,
    /// Embedding (can be computed lazily)
    pub embedding: Option<Vec<f32>>,
    /// Super rich metadata
    pub metadata: ChunkMetadata,
    /// Index within document (0-based)
    pub chunk_index: usize,
    /// Total number of chunks from this document
    pub total_chunks: usize,
    /// Granularity level
    pub granularity: Granularity,
}

/// Processed chunk ready for indexing in Vortex
#[derive(Debug, Clone)]
pub struct ProcessedChunk {
    /// Chunk ID
    pub id: Uuid,
    /// Original document ID
    pub doc_id: String,
    /// Chunk content
    pub content: String,
    /// Chunk embedding (if computed)
    pub embedding: Option<Vec<f32>>,
    /// Rich chunk metadata
    pub metadata: ChunkMetadata,
    /// Chunk index within document
    pub chunk_index: usize,
    /// Total chunks from document
    pub total_chunks: usize,
    /// Granularity level
    pub granularity: Granularity,
}

impl Default for ChunkMetadata {
    fn default() -> Self {
        Self {
            title: None,
            section_hierarchy: Vec::new(),
            page_number: None,
            keywords: Vec::new(),
            summary: None,
            is_proposition: false,
            parent_chunk_id: None,
            language: "en".to_string(),
            created_at: Utc::now(),
            custom_fields: HashMap::new(),
        }
    }
}

impl Chunk {
    /// Create a new chunk with minimal metadata
    pub fn new(doc_id: String, content: String, chunk_index: usize, total_chunks: usize) -> Self {
        Self {
            id: Uuid::new_v4(),
            doc_id,
            content,
            embedding: None,
            metadata: ChunkMetadata::default(),
            chunk_index,
            total_chunks,
            granularity: Granularity::Medium,
        }
    }

    /// Create a chunk with full metadata
    pub fn with_metadata(
        doc_id: String,
        content: String,
        chunk_index: usize,
        total_chunks: usize,
        metadata: ChunkMetadata,
        granularity: Granularity,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            doc_id,
            content,
            embedding: None,
            metadata,
            chunk_index,
            total_chunks,
            granularity,
        }
    }

    /// Add embedding to the chunk
    pub fn with_embedding(mut self, embedding: Vec<f32>) -> Self {
        self.embedding = Some(embedding);
        self
    }

    /// Get chunk size in characters
    pub fn size(&self) -> usize {
        self.content.len()
    }

    /// Check if chunk has embedding
    pub fn has_embedding(&self) -> bool {
        self.embedding.is_some()
    }
}

impl ProcessedChunk {
    /// Create from Chunk
    pub fn from_chunk(chunk: Chunk, embedding: Option<Vec<f32>>) -> Self {
        Self {
            id: chunk.id,
            doc_id: chunk.doc_id,
            content: chunk.content,
            embedding: embedding.or(chunk.embedding),
            metadata: chunk.metadata,
            chunk_index: chunk.chunk_index,
            total_chunks: chunk.total_chunks,
            granularity: chunk.granularity,
        }
    }
}
