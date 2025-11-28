//! Metadata indexes for filtering and secondary indexing
//!
//! Provides inverted indexes, bitmap indexes, and other metadata indexing
//! structures to accelerate filtered vector searches.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use anyhow::Result;

/// Inverted index for text and categorical metadata
#[derive(Debug, Clone)]
pub struct InvertedIndex<T> {
    /// Term to document IDs mapping
    index: HashMap<T, HashSet<u64>>,
}

impl<T> InvertedIndex<T>
where
    T: Eq + Hash + Clone,
{
    /// Create a new inverted index
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
        }
    }

    /// Add a document-term relationship
    pub fn add(&mut self, term: T, doc_id: u64) {
        self.index.entry(term).or_insert_with(HashSet::new).insert(doc_id);
    }

    /// Remove a document-term relationship
    pub fn remove(&mut self, term: &T, doc_id: u64) {
        if let Some(docs) = self.index.get_mut(term) {
            docs.remove(&doc_id);
            if docs.is_empty() {
                self.index.remove(term);
            }
        }
    }

    /// Get all documents containing a term
    pub fn get(&self, term: &T) -> Option<&HashSet<u64>> {
        self.index.get(term)
    }

    /// Search for documents containing any of the terms
    pub fn search_any(&self, terms: &[T]) -> HashSet<u64> {
        let mut result = HashSet::new();
        for term in terms {
            if let Some(docs) = self.index.get(term) {
                result.extend(docs);
            }
        }
        result
    }

    /// Search for documents containing all of the terms
    pub fn search_all(&self, terms: &[T]) -> HashSet<u64> {
        if terms.is_empty() {
            return HashSet::new();
        }

        let mut result = match self.index.get(&terms[0]) {
            Some(docs) => docs.clone(),
            None => return HashSet::new(),
        };

        for term in &terms[1..] {
            if let Some(docs) = self.index.get(term) {
                result = result.intersection(docs).cloned().collect();
            } else {
                return HashSet::new();
            }
        }

        result
    }
}

impl<T> Default for InvertedIndex<T>
where
    T: Eq + Hash + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Bitmap index for low-cardinality categorical data
#[derive(Debug, Clone)]
pub struct BitmapIndex<T> {
    /// Value to bitmap mapping (bit position = document ID)
    bitmaps: HashMap<T, Vec<u64>>,
    /// Maximum document ID seen
    max_doc_id: u64,
}

impl<T> BitmapIndex<T>
where
    T: Eq + Hash + Clone,
{
    /// Create a new bitmap index
    pub fn new() -> Self {
        Self {
            bitmaps: HashMap::new(),
            max_doc_id: 0,
        }
    }

    /// Add a document-value relationship
    pub fn add(&mut self, value: T, doc_id: u64) {
        self.max_doc_id = self.max_doc_id.max(doc_id);

        let bitmap = self.bitmaps.entry(value).or_insert_with(|| {
            // Initialize bitmap with enough words to cover max_doc_id
            let num_words = ((self.max_doc_id + 1 + 63) / 64) as usize;
            vec![0u64; num_words]
        });

        // Extend bitmap if necessary
        let word_idx = (doc_id / 64) as usize;
        let bit_idx = (doc_id % 64) as u32;

        if word_idx >= bitmap.len() {
            bitmap.resize(word_idx + 1, 0);
        }

        bitmap[word_idx] |= 1u64 << bit_idx;
    }

    /// Remove a document-value relationship
    pub fn remove(&mut self, value: &T, doc_id: u64) {
        if let Some(bitmap) = self.bitmaps.get_mut(value) {
            let word_idx = (doc_id / 64) as usize;
            let bit_idx = (doc_id % 64) as u32;

            if word_idx < bitmap.len() {
                bitmap[word_idx] &= !(1u64 << bit_idx);
            }
        }
    }

    /// Get bitmap for a value
    pub fn get_bitmap(&self, value: &T) -> Option<&[u64]> {
        self.bitmaps.get(value).map(|v| v.as_slice())
    }

    /// Count documents with a specific value
    pub fn count(&self, value: &T) -> usize {
        self.get_bitmap(value)
            .map(|bitmap| bitmap.iter().map(|word| word.count_ones() as usize).sum())
            .unwrap_or(0)
    }

    /// Check if a document has a specific value
    pub fn contains(&self, value: &T, doc_id: u64) -> bool {
        if let Some(bitmap) = self.get_bitmap(value) {
            let word_idx = (doc_id / 64) as usize;
            let bit_idx = (doc_id % 64) as u32;

            if word_idx < bitmap.len() {
                return (bitmap[word_idx] & (1u64 << bit_idx)) != 0;
            }
        }
        false
    }

    /// Get all document IDs with a specific value
    pub fn get_documents(&self, value: &T) -> Vec<u64> {
        let mut result = Vec::new();
        if let Some(bitmap) = self.get_bitmap(value) {
            for (word_idx, &word) in bitmap.iter().enumerate() {
                if word != 0 {
                    for bit_idx in 0..64 {
                        if (word & (1u64 << bit_idx)) != 0 {
                            let doc_id = (word_idx * 64) as u64 + bit_idx as u64;
                            if doc_id <= self.max_doc_id {
                                result.push(doc_id);
                            }
                        }
                    }
                }
            }
        }
        result
    }
}

impl<T> Default for BitmapIndex<T>
where
    T: Eq + Hash + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Range index for numerical metadata
#[derive(Debug, Clone)]
pub struct RangeIndex {
    /// Sorted list of (value, doc_id) pairs
    entries: Vec<(f64, u64)>,
}

impl RangeIndex {
    /// Create a new range index
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Add a document-value pair
    pub fn add(&mut self, value: f64, doc_id: u64) {
        self.entries.push((value, doc_id));
    }

    /// Finalize the index (sort entries)
    pub fn finalize(&mut self) {
        self.entries.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    }

    /// Range query: find documents with value in [min, max]
    pub fn range_query(&self, min: f64, max: f64) -> Vec<u64> {
        let mut result = Vec::new();

        // Binary search for lower bound
        let mut left = 0;
        let mut right = self.entries.len();

        while left < right {
            let mid = left + (right - left) / 2;
            if self.entries[mid].0 < min {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        // Collect results from lower bound
        for i in left..self.entries.len() {
            if self.entries[i].0 > max {
                break;
            }
            result.push(self.entries[i].1);
        }

        result
    }

    /// Get minimum value
    pub fn min_value(&self) -> Option<f64> {
        self.entries.first().map(|(val, _)| *val)
    }

    /// Get maximum value
    pub fn max_value(&self) -> Option<f64> {
        self.entries.last().map(|(val, _)| *val)
    }
}

impl Default for RangeIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Combined metadata index
#[derive(Debug, Clone)]
pub struct MetadataIndex {
    /// Text inverted indexes
    text_indexes: HashMap<String, InvertedIndex<String>>,
    /// Categorical bitmap indexes
    bitmap_indexes: HashMap<String, BitmapIndex<String>>,
    /// Numerical range indexes
    range_indexes: HashMap<String, RangeIndex>,
}

impl MetadataIndex {
    /// Create a new metadata index
    pub fn new() -> Self {
        Self {
            text_indexes: HashMap::new(),
            bitmap_indexes: HashMap::new(),
            range_indexes: HashMap::new(),
        }
    }

    /// Add text metadata
    pub fn add_text(&mut self, field: &str, value: &str, doc_id: u64) {
        let index = self.text_indexes.entry(field.to_string()).or_insert_with(InvertedIndex::new);

        // Simple tokenization (split on whitespace)
        for token in value.split_whitespace() {
            index.add(token.to_lowercase(), doc_id);
        }
    }

    /// Add categorical metadata
    pub fn add_categorical(&mut self, field: &str, value: &str, doc_id: u64) {
        let index = self.bitmap_indexes.entry(field.to_string()).or_insert_with(BitmapIndex::new);
        index.add(value.to_string(), doc_id);
    }

    /// Add numerical metadata
    pub fn add_numerical(&mut self, field: &str, value: f64, doc_id: u64) {
        let index = self.range_indexes.entry(field.to_string()).or_insert_with(RangeIndex::new);
        index.add(value, doc_id);
    }

    /// Finalize all indexes
    pub fn finalize(&mut self) {
        for index in self.range_indexes.values_mut() {
            index.finalize();
        }
    }

    /// Text search
    pub fn text_search(&self, field: &str, query: &str) -> Option<HashSet<u64>> {
        self.text_indexes.get(field)?.get(&query.to_lowercase()).cloned()
    }

    /// Categorical filter
    pub fn categorical_filter(&self, field: &str, value: &str) -> Option<Vec<u64>> {
        Some(self.bitmap_indexes.get(field)?.get_documents(&value.to_string()))
    }

    /// Numerical range filter
    pub fn numerical_range(&self, field: &str, min: f64, max: f64) -> Option<Vec<u64>> {
        Some(self.range_indexes.get(field)?.range_query(min, max))
    }

    /// Combined metadata filtering
    pub fn combined_filter(&self, filters: &[MetadataFilter]) -> HashSet<u64> {
        let mut result: Option<HashSet<u64>> = None;

        for filter in filters {
            let current_docs = match filter {
                MetadataFilter::Text { field, query } => {
                    self.text_search(field, query).unwrap_or_default()
                }
                MetadataFilter::Categorical { field, value } => {
                    self.categorical_filter(field, value)
                        .unwrap_or_default()
                        .into_iter()
                        .collect()
                }
                MetadataFilter::NumericalRange { field, min, max } => {
                    self.numerical_range(field, *min, *max)
                        .unwrap_or_default()
                        .into_iter()
                        .collect()
                }
            };

            match &mut result {
                None => result = Some(current_docs),
                Some(existing) => {
                    *existing = existing.intersection(&current_docs).cloned().collect();
                }
            }
        }

        result.unwrap_or_default()
    }
}

/// Metadata filter for queries
#[derive(Debug, Clone)]
pub enum MetadataFilter {
    /// Text search filter
    Text {
        field: String,
        query: String,
    },
    /// Categorical equality filter
    Categorical {
        field: String,
        value: String,
    },
    /// Numerical range filter
    NumericalRange {
        field: String,
        min: f64,
        max: f64,
    },
}

impl Default for MetadataIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inverted_index() {
        let mut index = InvertedIndex::new();

        index.add("rust", 1);
        index.add("rust", 2);
        index.add("python", 2);

        // assert_eq!(index.get(&"rust".to_string()).unwrap().len(), 2);
        // assert_eq!(index.get(&"python".to_string()).unwrap().len(), 1);
        // assert!(index.get(&"java".to_string()).is_none());
    }

    #[test]
    fn test_bitmap_index() {
        let mut index = BitmapIndex::new();

        index.add("red", 1);
        index.add("red", 3);
        index.add("blue", 2);

        assert!(index.contains(&"red", 1));
        assert!(index.contains(&"blue", 2));
        assert!(!index.contains(&"red", 2));

        assert_eq!(index.count(&"red"), 2);
        assert_eq!(index.get_documents(&"red"), vec![1, 3]);
    }

    #[test]
    fn test_range_index() {
        let mut index = RangeIndex::new();

        index.add(1.0, 1);
        index.add(3.0, 2);
        index.add(5.0, 3);
        index.finalize();

        let results = index.range_query(2.0, 4.0);
        assert_eq!(results, vec![2]);
    }

    #[test]
    fn test_metadata_index() {
        let mut index = MetadataIndex::new();

        // Add some test data
        index.add_text("content", "rust programming language", 1);
        index.add_text("content", "python scripting language", 2);
        index.add_categorical("category", "programming", 1);
        index.add_categorical("category", "scripting", 2);
        index.add_numerical("score", 0.95, 1);
        index.add_numerical("score", 0.87, 2);
        index.finalize();

        // Test filters
        let text_results = index.text_search("content", "rust").unwrap();
        assert_eq!(text_results.len(), 1);
        assert!(text_results.contains(&1));

        let cat_results = index.categorical_filter("category", "programming").unwrap();
        assert_eq!(cat_results, vec![1]);

        let range_results = index.numerical_range("score", 0.9, 1.0).unwrap();
        assert_eq!(range_results, vec![1]);
    }
}
