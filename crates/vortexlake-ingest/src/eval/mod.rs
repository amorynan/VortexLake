//! Comprehensive evaluation framework for chunking quality
//!
//! This module provides tools for evaluating chunking performance
//! across multiple dimensions: coherence, relevance, and RAG effectiveness.
//!
//! Only used in tests/ and examples/, never in core library.

pub mod coherence;
pub mod relevance;
pub mod suite;

// Re-export for convenience
pub use coherence::{CoherenceEvaluator, CoherenceScore};
pub use relevance::{RelevanceEvaluator, RetrievalMetrics, TestQuery};
pub use suite::{EvaluationSuite, EvaluationReport, RagEvaluationReport, BenchmarkRunner};
