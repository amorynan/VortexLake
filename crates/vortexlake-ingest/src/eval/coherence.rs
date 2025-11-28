//! Chunk coherence evaluation
//!
//! This module evaluates how semantically coherent chunks are,
//! measuring both internal consistency and contextual continuity.

use crate::models::Chunk;

/// Coherence evaluation results
#[derive(Debug, Clone)]
pub struct CoherenceScore {
    /// Internal coherence (0-1): how well sentences within chunk relate
    pub internal_coherence: f32,
    /// Contextual coherence (0-1): how well chunk fits with neighbors
    pub contextual_coherence: f32,
    /// Overall coherence score (0-1)
    pub overall_score: f32,
    /// Detailed breakdown
    pub details: CoherenceDetails,
}

/// Detailed coherence metrics
#[derive(Debug, Clone)]
pub struct CoherenceDetails {
    /// Average sentence similarity within chunk
    pub avg_sentence_similarity: f32,
    /// Topic consistency score
    pub topic_consistency: f32,
    /// Lexical overlap with neighbors
    pub lexical_overlap: f32,
    /// Semantic flow score
    pub semantic_flow: f32,
}

/// Chunk coherence evaluator
pub struct CoherenceEvaluator {
    /// Similarity threshold for coherence
    similarity_threshold: f32,
}

impl CoherenceEvaluator {
    /// Create a new coherence evaluator
    pub fn new() -> Self {
        Self {
            similarity_threshold: 0.7,
        }
    }

    /// Evaluate coherence of a single chunk
    pub fn evaluate_chunk(&self, chunk: &Chunk) -> CoherenceScore {
        let internal_coherence = self.calculate_internal_coherence(chunk);
        let contextual_coherence = 0.0; // Would need neighbor chunks
        let overall_score = (internal_coherence + contextual_coherence) / 2.0;

        CoherenceScore {
            internal_coherence,
            contextual_coherence,
            overall_score,
            details: CoherenceDetails {
                avg_sentence_similarity: internal_coherence,
                topic_consistency: internal_coherence * 0.9,
                lexical_overlap: 0.0, // Would need neighbors
                semantic_flow: internal_coherence * 0.8,
            },
        }
    }

    /// Evaluate coherence across multiple chunks
    pub fn evaluate_chunks(&self, chunks: &[Chunk]) -> Vec<CoherenceScore> {
        chunks.iter()
            .map(|chunk| self.evaluate_chunk(chunk))
            .collect()
    }

    /// Calculate coherence between two adjacent chunks
    pub fn evaluate_chunk_pair(&self, chunk1: &Chunk, chunk2: &Chunk) -> f32 {
        // Simple lexical overlap calculation
        self.calculate_lexical_overlap(&chunk1.content, &chunk2.content)
    }

    // Private methods

    fn calculate_internal_coherence(&self, chunk: &Chunk) -> f32 {
        let sentences = self.split_into_sentences(&chunk.content);

        if sentences.len() <= 1 {
            return 1.0; // Single sentence is perfectly coherent
        }

        // Calculate pairwise sentence similarities
        let mut total_similarity = 0.0;
        let mut pair_count = 0;

        for i in 0..sentences.len() - 1 {
            for j in i + 1..sentences.len() {
                let similarity = self.calculate_sentence_similarity(&sentences[i], &sentences[j]);
                total_similarity += similarity;
                pair_count += 1;
            }
        }

        if pair_count == 0 {
            1.0
        } else {
            total_similarity / pair_count as f32
        }
    }

    fn split_into_sentences(&self, text: &str) -> Vec<String> {
        text.split(|c| c == '.' || c == '!' || c == '?')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }

    fn calculate_sentence_similarity(&self, sent1: &str, sent2: &str) -> f32 {
        // Simple Jaccard similarity based on words
        let words1: std::collections::HashSet<&str> = sent1.split_whitespace().collect();
        let words2: std::collections::HashSet<&str> = sent2.split_whitespace().collect();

        let intersection = words1.intersection(&words2).count();
        let union = words1.len() + words2.len() - intersection;

        if union == 0 {
            0.0
        } else {
            intersection as f32 / union as f32
        }
    }

    fn calculate_lexical_overlap(&self, text1: &str, text2: &str) -> f32 {
        // Calculate overlap at word boundaries
        let words1: std::collections::HashSet<&str> = text1.split_whitespace().collect();
        let words2: std::collections::HashSet<&str> = text2.split_whitespace().collect();

        let intersection = words1.intersection(&words2).count();
        let union = words1.len() + words2.len() - intersection;

        if union == 0 {
            0.0
        } else {
            intersection as f32 / union as f32
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Chunk};

    fn create_test_chunk(content: &str) -> Chunk {
        Chunk::new(
            "test_doc".to_string(),
            content.to_string(),
            0,
            1,
        )
    }

    #[test]
    fn test_single_sentence_coherence() {
        let evaluator = CoherenceEvaluator::new();
        let chunk = create_test_chunk("This is a single sentence.");
        let score = evaluator.evaluate_chunk(&chunk);

        assert_eq!(score.internal_coherence, 1.0);
        assert_eq!(score.overall_score, 0.5); // Half because contextual is 0
    }

    #[test]
    fn test_multiple_sentences_coherence() {
        let evaluator = CoherenceEvaluator::new();
        let chunk = create_test_chunk("Machine learning is powerful. It can solve complex problems. Neural networks are amazing.");
        let score = evaluator.evaluate_chunk(&chunk);

        assert!(score.internal_coherence > 0.0);
        assert!(score.internal_coherence <= 1.0);
    }

    #[test]
    fn test_lexical_overlap() {
        let evaluator = CoherenceEvaluator::new();

        let overlap = evaluator.calculate_lexical_overlap(
            "machine learning artificial intelligence",
            "artificial intelligence deep learning"
        );

        assert!(overlap > 0.0); // Should have some overlap with "artificial intelligence"
        assert!(overlap < 1.0); // Should not be perfect overlap
    }
}
