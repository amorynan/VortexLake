//! Advanced text splitters for VortexLake
//!
//! This module provides state-of-the-art text splitting algorithms
//! optimized for different document types and retrieval scenarios.

pub mod splitter_trait;
pub mod agentic;
pub mod proposition;
pub mod hierarchical;
pub mod llm_rerank;

// Re-export for convenience
pub use splitter_trait::{DocumentSplitter, SplitterConfig};
pub use agentic::AgenticSplitter;
pub use proposition::PropositionSplitter;
pub use hierarchical::HierarchicalSplitter;
pub use llm_rerank::LLMRerankSplitter;
