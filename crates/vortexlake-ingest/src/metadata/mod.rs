//! Metadata extraction and hierarchical management
//!
//! This module provides tools for extracting rich metadata from documents
//! and managing hierarchical relationships between chunks.

pub mod extractor;
pub mod hierarchy;

// Re-export for convenience
pub use extractor::MetadataExtractor;
pub use hierarchy::{HierarchyManager, utils};
