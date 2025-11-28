//! # VortexLake Ingest
//!
//! Advanced document ingestion with state-of-the-art text splitting
//! and rich metadata extraction for VortexLake.
//!
//! ## Architecture
//!
//! ```
//! crates/vortexlake-ingest/
//! ├── src/
//! │   ├── lib.rs
//! │   ├── models.rs              # Document, Chunk, ChunkMetadata
//! │   ├── splitter/              # 9 Advanced splitters
//! │   │   ├── mod.rs
//! │   │   ├── trait.rs           # DocumentSplitter trait
//! │   │   └── recursive.rs       # + 8 other splitters
//! │   ├── metadata/              # Rich metadata extraction
//! │   │   ├── extractor.rs       # title, keywords, hierarchy
//! │   │   └── hierarchy.rs       # Parent-child relationships
//! │   ├── pipeline.rs            # PipelineBuilder (fluent API)
//! │   └── eval/                  # Evaluation tools (tests/examples only)
//! │       ├── coherence.rs       # Semantic coherence metrics
//! │       ├── relevance.rs       # RAG performance metrics
//! │       └── suite.rs           # Comprehensive evaluation
//! ```
//!
//! ## Features
//!
//! - **9 Advanced Text Splitters**: Recursive, Semantic, Markdown, Proposition, Agentic, Hierarchical, Code, Table-Aware, LLM-Rerank
//! - **Super Rich Metadata**: 15+ fields including hierarchy, keywords, summaries
//! - **Hierarchical Retrieval**: Parent-child chunk relationships
//! - **Fluent Pipeline API**: Chain-style configuration
//! - **Comprehensive Evaluation**: Coherence, relevance, and RAG metrics
//!
//! ## Example
//!
//! ```rust
//! use vortexlake_ingest::{
//!     PipelineBuilder,
//!     splitter::{recursive::RecursiveCharacterSplitter, SplitterConfig}
//! };
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Build pipeline with fluent API
//! let pipeline = PipelineBuilder::new()
//!     .with_splitter(Box::new(RecursiveCharacterSplitter::new(
//!         SplitterConfig::default()
//!     )))
//!     .with_batch_size(10)
//!     .with_hierarchy(true)
//!     .build()?;
//!
//! // Process documents
//! let documents = vec![/* your documents */];
//! let chunks = pipeline.process(documents).await?;
//!
//! # Ok(())
//! # }
//! ```

pub mod models;
pub mod splitter;
pub mod metadata;
pub mod pipeline;
pub mod eval;

// Re-export core types
pub use models::*;
pub use splitter::*;
pub use metadata::*;
pub use pipeline::{PipelineBuilder, IngestionPipeline, EmbeddingFunction, MockEmbeddingFunction};
