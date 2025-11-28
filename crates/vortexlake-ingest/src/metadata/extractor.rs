//! Metadata extraction utilities
//!
//! This module provides tools for extracting rich metadata from documents,
//! including titles, keywords, summaries, and hierarchical structure.

use crate::models::{Document, ChunkMetadata};
use anyhow::Result;
use std::collections::HashMap;

/// Extract rich metadata from a document
pub struct MetadataExtractor;

impl MetadataExtractor {
    /// Create a new metadata extractor
    pub fn new() -> Self {
        Self
    }

    /// Extract title from document content
    pub fn extract_title(&self, document: &Document) -> Option<String> {
        // Check metadata first
        if let Some(title) = document.metadata.get("title") {
            if let Some(title_str) = title.as_str() {
                return Some(title_str.to_string());
            }
        }

        // Try to extract from content
        self.extract_title_from_content(&document.content)
    }

    /// Extract keywords from document
    pub fn extract_keywords(&self, document: &Document, max_keywords: usize) -> Vec<String> {
        // Check metadata first
        if let Some(keywords) = document.metadata.get("keywords") {
            if let Some(arr) = keywords.as_array() {
                return arr.iter()
                    .filter_map(|v| v.as_str())
                    .map(|s| s.to_string())
                    .take(max_keywords)
                    .collect();
            }
        }

        // Extract from content (simplified)
        self.extract_keywords_from_content(&document.content, max_keywords)
    }

    /// Extract section hierarchy from document
    pub fn extract_section_hierarchy(&self, document: &Document) -> Vec<String> {
        // Check metadata first
        if let Some(sections) = document.metadata.get("sections") {
            if let Some(arr) = sections.as_array() {
                return arr.iter()
                    .filter_map(|v| v.as_str())
                    .map(|s| s.to_string())
                    .collect();
            }
        }

        // Extract from content (markdown-style)
        self.extract_sections_from_content(&document.content)
    }

    /// Generate summary for a chunk (placeholder - would use LLM)
    pub async fn generate_chunk_summary(&self, _chunk_content: &str) -> Result<Option<String>> {
        // TODO: Implement LLM-based summarization
        // For now, return None
        Ok(None)
    }

    /// Extract language from document
    pub fn detect_language(&self, document: &Document) -> String {
        // Check metadata first
        if let Some(lang) = document.metadata.get("language") {
            if let Some(lang_str) = lang.as_str() {
                return lang_str.to_string();
            }
        }

        // Default to English
        "en".to_string()
    }

    /// Create ChunkMetadata from document
    pub async fn create_chunk_metadata(&self, document: &Document) -> Result<ChunkMetadata> {
        let summary = if let Some(content) = document.metadata.get("summary") {
            content.as_str().map(|s| s.to_string())
        } else {
            None
        };

        Ok(ChunkMetadata {
            title: self.extract_title(document),
            section_hierarchy: self.extract_section_hierarchy(document),
            page_number: document.metadata.get("page")
                .and_then(|v| v.as_i64())
                .map(|n| n as i32),
            keywords: self.extract_keywords(document, 10),
            summary: if summary.is_some() {
                summary
            } else {
                self.generate_chunk_summary(&document.content).await.ok().flatten()
            },
            is_proposition: false, // Set by specific splitters
            parent_chunk_id: None, // Set by hierarchical splitters
            language: self.detect_language(document),
            created_at: chrono::Utc::now(),
            custom_fields: document.metadata.clone(),
        })
    }

    // Private helper methods

    fn extract_title_from_content(&self, content: &str) -> Option<String> {
        // Try to find title in first few lines
        for line in content.lines().take(5) {
            let line = line.trim();
            if !line.is_empty() && !line.starts_with('#') && line.len() < 100 {
                return Some(line.to_string());
            }
        }

        // Try to find markdown title
        if let Some(line) = content.lines().find(|l| l.starts_with("# ")) {
            return Some(line.trim_start_matches("# ").to_string());
        }

        None
    }

    fn extract_keywords_from_content(&self, content: &str, max_keywords: usize) -> Vec<String> {
        // Simple keyword extraction (in practice, use NLP libraries)
        let words: Vec<String> = content
            .split_whitespace()
            .filter(|word| word.len() > 4) // Only longer words
            .map(|word| word.to_lowercase().trim_matches(|c: char| !c.is_alphanumeric()).to_string())
            .filter(|word| !word.is_empty())
            .collect();

        // Count frequency
        let mut word_counts = HashMap::new();
        for word in words {
            *word_counts.entry(word).or_insert(0) += 1;
        }

        // Sort by frequency and take top keywords
        let mut keywords: Vec<(String, usize)> = word_counts.into_iter().collect();
        keywords.sort_by(|a, b| b.1.cmp(&a.1));

        keywords.into_iter()
            .take(max_keywords)
            .map(|(word, _)| word)
            .collect()
    }

    fn extract_sections_from_content(&self, content: &str) -> Vec<String> {
        let mut sections = Vec::new();
        let lines: Vec<&str> = content.lines().collect();

        for i in 0..lines.len() {
            let line = lines[i].trim();

            // Markdown headers
            if line.starts_with("# ") {
                sections.push(line.trim_start_matches("# ").to_string());
            } else if line.starts_with("## ") {
                sections.push(line.trim_start_matches("## ").to_string());
            } else if line.starts_with("### ") {
                sections.push(line.trim_start_matches("### ").to_string());
            }

            // Try to detect section headers (lines followed by underlines)
            if i + 1 < lines.len() {
                let next_line = lines[i + 1].trim();
                if next_line.chars().all(|c| c == '=' || c == '-') && next_line.len() >= 3 {
                    sections.push(line.to_string());
                }
            }
        }

        sections
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_extract_title_from_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("title".to_string(), json!("Test Title"));

        let doc = Document {
            content: "Some content".to_string(),
            metadata,
            source_id: "test".to_string(),
        };

        let extractor = MetadataExtractor::new();
        assert_eq!(extractor.extract_title(&doc), Some("Test Title".to_string()));
    }

    #[test]
    fn test_extract_title_from_content() {
        let doc = Document {
            content: "# My Document Title\n\nSome content here.".to_string(),
            metadata: HashMap::new(),
            source_id: "test".to_string(),
        };

        let extractor = MetadataExtractor::new();
        assert_eq!(extractor.extract_title(&doc), Some("My Document Title".to_string()));
    }

    #[test]
    fn test_extract_keywords() {
        let doc = Document {
            content: "machine learning is transforming industries machine learning artificial intelligence".to_string(),
            metadata: HashMap::new(),
            source_id: "test".to_string(),
        };

        let extractor = MetadataExtractor::new();
        let keywords = extractor.extract_keywords(&doc, 3);
        assert!(!keywords.is_empty());
        assert!(keywords.contains(&"machine".to_string()) || keywords.contains(&"learning".to_string()));
    }
}
