//! Hierarchical metadata management
//!
//! This module handles hierarchical relationships between chunks,
//! enabling parent-child retrieval patterns for complex documents.

use crate::models::{Chunk, ChunkMetadata};
use uuid::Uuid;
use std::collections::HashMap;

/// Hierarchical relationship manager
pub struct HierarchyManager {
    /// Map of chunk ID to its children
    children_map: HashMap<Uuid, Vec<Uuid>>,
    /// Map of chunk ID to its parent
    parent_map: HashMap<Uuid, Option<Uuid>>,
}

impl HierarchyManager {
    /// Create a new hierarchy manager
    pub fn new() -> Self {
        Self {
            children_map: HashMap::new(),
            parent_map: HashMap::new(),
        }
    }

    /// Add a hierarchical relationship
    pub fn add_relationship(&mut self, parent_id: Uuid, child_id: Uuid) {
        self.children_map.entry(parent_id).or_insert_with(Vec::new).push(child_id);
        self.parent_map.insert(child_id, Some(parent_id));
    }

    /// Get children of a chunk
    pub fn get_children(&self, chunk_id: &Uuid) -> Vec<Uuid> {
        self.children_map.get(chunk_id).cloned().unwrap_or_default()
    }

    /// Get parent of a chunk
    pub fn get_parent(&self, chunk_id: &Uuid) -> Option<Uuid> {
        self.parent_map.get(chunk_id).copied().flatten()
    }

    /// Get all descendants of a chunk (recursive)
    pub fn get_descendants(&self, chunk_id: &Uuid) -> Vec<Uuid> {
        let mut descendants = Vec::new();
        let mut to_visit = vec![*chunk_id];

        while let Some(current_id) = to_visit.pop() {
            if let Some(children) = self.children_map.get(&current_id) {
                for child in children {
                    descendants.push(*child);
                    to_visit.push(*child);
                }
            }
        }

        descendants
    }

    /// Get all ancestors of a chunk (recursive)
    pub fn get_ancestors(&self, chunk_id: &Uuid) -> Vec<Uuid> {
        let mut ancestors = Vec::new();
        let mut current_id = *chunk_id;

        while let Some(parent_id) = self.get_parent(&current_id) {
            ancestors.push(parent_id);
            current_id = parent_id;
        }

        ancestors
    }

    /// Create hierarchical chunks from flat chunks
    ///
    /// This method organizes flat chunks into a hierarchical structure
    /// where larger chunks are parents of smaller chunks.
    pub fn create_hierarchy_from_chunks(chunks: Vec<Chunk>) -> (Vec<Chunk>, HierarchyManager) {
        let mut manager = HierarchyManager::new();
        let mut hierarchical_chunks = Vec::new();

        // Group chunks by document and granularity
        let mut chunks_by_doc: HashMap<String, Vec<Chunk>> = HashMap::new();

        for chunk in chunks {
            chunks_by_doc.entry(chunk.doc_id.clone()).or_insert_with(Vec::new).push(chunk);
        }

        // For each document, create hierarchy
        for (doc_id, doc_chunks) in chunks_by_doc {
            let (hier_chunks, doc_manager) = Self::create_document_hierarchy(doc_chunks);
            hierarchical_chunks.extend(hier_chunks);

            // Merge document hierarchy into global manager
            for (child_id, parent_id) in doc_manager.parent_map.iter() {
                if let Some(pid) = parent_id {
                    manager.add_relationship(*pid, *child_id);
                }
            }
        }

        (hierarchical_chunks, manager)
    }

    /// Create hierarchy for chunks from a single document
    fn create_document_hierarchy(mut chunks: Vec<Chunk>) -> (Vec<Chunk>, HierarchyManager) {
        let mut manager = HierarchyManager::new();

        // Sort chunks by size (smallest first)
        chunks.sort_by_key(|c| c.content.len());

        // For each small chunk, find the best parent (closest larger chunk that contains it)
        for i in 0..chunks.len() {
            let child = &chunks[i];

            // Find the smallest chunk that completely contains this one
            let mut best_parent_idx = None;
            let mut best_overlap_score = 0.0;

            for j in (i + 1)..chunks.len() {
                let potential_parent = &chunks[j];

                if potential_parent.content.contains(&child.content) {
                    // Calculate overlap score (how much of child content is covered)
                    let overlap_score = child.content.len() as f32 / potential_parent.content.len() as f32;

                    if overlap_score > best_overlap_score {
                        best_overlap_score = overlap_score;
                        best_parent_idx = Some(j);
                    }
                }
            }

            // Set parent relationship
            if let Some(parent_idx) = best_parent_idx {
                let parent_id = chunks[parent_idx].id;
                let child_id = chunks[i].id;

                // Update chunk metadata
                chunks[i].metadata.parent_chunk_id = Some(parent_id);

                manager.add_relationship(parent_id, child_id);
            }
        }

        (chunks, manager)
    }
}

/// Utility functions for hierarchy operations
pub mod utils {
    use super::*;
    use crate::models::Chunk;

    /// Find chunks that are good parents for a given child chunk
    pub fn find_potential_parents(child: &Chunk, candidates: &[Chunk]) -> Vec<(Uuid, f32)> {
        candidates.iter()
            .filter(|parent| parent.content.contains(&child.content))
            .map(|parent| {
                let overlap_score = child.content.len() as f32 / parent.content.len() as f32;
                (parent.id, overlap_score)
            })
            .collect()
    }

    /// Calculate hierarchy depth for a chunk
    pub fn calculate_hierarchy_depth(chunk: &Chunk, manager: &HierarchyManager) -> usize {
        let ancestors = manager.get_ancestors(&chunk.id);
        ancestors.len()
    }

    /// Get hierarchical context for a chunk (parent + children summaries)
    pub fn get_hierarchical_context(chunk: &Chunk, all_chunks: &[Chunk], manager: &HierarchyManager) -> String {
        let mut context_parts = Vec::new();

        // Add parent context
        if let Some(parent_id) = manager.get_parent(&chunk.id) {
            if let Some(parent) = all_chunks.iter().find(|c| c.id == parent_id) {
                context_parts.push(format!("Parent context: {}", &parent.content[..200.min(parent.content.len())]));
            }
        }

        // Add children summaries
        let children_ids = manager.get_children(&chunk.id);
        if !children_ids.is_empty() {
            let children_count = children_ids.len();
            context_parts.push(format!("Has {} child chunks for detailed information", children_count));
        }

        if context_parts.is_empty() {
            chunk.content.clone()
        } else {
            format!("{}\n\n{}", context_parts.join("\n"), chunk.content)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Chunk, ChunkMetadata, Granularity};

    #[test]
    fn test_hierarchy_creation() {
        let mut manager = HierarchyManager::new();

        let parent_id = Uuid::new_v4();
        let child1_id = Uuid::new_v4();
        let child2_id = Uuid::new_v4();

        manager.add_relationship(parent_id, child1_id);
        manager.add_relationship(parent_id, child2_id);

        assert_eq!(manager.get_parent(&child1_id), Some(parent_id));
        assert_eq!(manager.get_children(&parent_id), vec![child1_id, child2_id]);
    }

    #[test]
    fn test_descendants() {
        let mut manager = HierarchyManager::new();

        let grandparent = Uuid::new_v4();
        let parent = Uuid::new_v4();
        let child = Uuid::new_v4();

        manager.add_relationship(grandparent, parent);
        manager.add_relationship(parent, child);

        let descendants = manager.get_descendants(&grandparent);
        assert!(descendants.contains(&parent));
        assert!(descendants.contains(&child));
    }
}
