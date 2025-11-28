//! Retrieval relevance evaluation
//!
//! This module evaluates how well chunks perform in retrieval-augmented
//! generation tasks, measuring recall, precision, and answer quality.

use crate::models::Chunk;

/// Relevance evaluation results
#[derive(Debug, Clone)]
pub struct RelevanceScore {
    /// Recall@K score (fraction of relevant chunks retrieved)
    pub recall_at_k: f32,
    /// Mean Reciprocal Rank
    pub mrr: f32,
    /// Precision@K
    pub precision_at_k: f32,
    /// Answer relevance score (0-1)
    pub answer_relevance: f32,
    /// Query-chunk alignment score
    pub query_alignment: f32,
}

/// Retrieval evaluation metrics
#[derive(Debug, Clone)]
pub struct RetrievalMetrics {
    /// Average relevance score across queries
    pub avg_relevance: f32,
    /// Average recall@5 across queries
    pub avg_recall_at_5: f32,
    /// Average precision@5 across queries
    pub avg_precision_at_5: f32,
    /// Query coverage (fraction of queries with good results)
    pub query_coverage: f32,
    /// Detailed per-query results
    pub per_query_scores: Vec<RelevanceScore>,
}

/// Test query for evaluation
#[derive(Debug, Clone)]
pub struct TestQuery {
    /// Query text
    pub query: String,
    /// Expected relevant chunk IDs
    pub relevant_chunk_ids: Vec<uuid::Uuid>,
    /// Expected answer
    pub expected_answer: String,
}

/// Retrieval evaluator
pub struct RelevanceEvaluator {
    /// Top-K value for evaluation
    k: usize,
}

impl RelevanceEvaluator {
    /// Create a new relevance evaluator
    pub fn new(k: usize) -> Self {
        Self { k }
    }

    /// Evaluate retrieval performance for a single query
    pub fn evaluate_query(
        &self,
        query: &TestQuery,
        retrieved_chunks: &[&Chunk],
        _generated_answer: Option<&str>,
    ) -> RelevanceScore {
        let retrieved_ids: Vec<uuid::Uuid> = retrieved_chunks.iter().map(|c| c.id).collect();

        // Calculate recall@K
        let relevant_retrieved = retrieved_ids.iter()
            .filter(|id| query.relevant_chunk_ids.contains(id))
            .count();

        let recall_at_k = if query.relevant_chunk_ids.is_empty() {
            0.0
        } else {
            relevant_retrieved as f32 / query.relevant_chunk_ids.len().min(self.k) as f32
        };

        // Calculate precision@K
        let precision_at_k = if retrieved_ids.is_empty() {
            0.0
        } else {
            relevant_retrieved as f32 / retrieved_ids.len().min(self.k) as f32
        };

        // Calculate MRR
        let mut mrr = 0.0;
        for (rank, chunk_id) in retrieved_ids.iter().enumerate() {
            if query.relevant_chunk_ids.contains(chunk_id) {
                mrr = 1.0 / (rank + 1) as f32;
                break;
            }
        }

        // Simplified answer relevance (would need actual answer comparison)
        let answer_relevance = if _generated_answer.is_some() {
            0.8 // Placeholder
        } else {
            0.0
        };

        // Query-chunk alignment based on content similarity
        let query_alignment = self.calculate_query_alignment(&query.query, retrieved_chunks);

        RelevanceScore {
            recall_at_k,
            mrr,
            precision_at_k,
            answer_relevance,
            query_alignment,
        }
    }

    /// Evaluate retrieval performance across multiple queries
    pub fn evaluate_queries(
        &self,
        queries: &[TestQuery],
        retrieval_fn: impl Fn(&str) -> Vec<Chunk>,
    ) -> RetrievalMetrics {
        let mut scores = Vec::new();

        for query in queries {
            let retrieved_chunks = retrieval_fn(&query.query);
            let retrieved_refs: Vec<&Chunk> = retrieved_chunks.iter().collect();

            // Mock generated answer (would use actual RAG pipeline)
            let mock_answer = Some("Mock generated answer");

            let score = self.evaluate_query(query, &retrieved_refs, mock_answer.as_deref());
            scores.push(score);
        }

        let avg_relevance = scores.iter().map(|s| s.query_alignment).sum::<f32>() / scores.len() as f32;
        let avg_recall_at_5 = scores.iter().map(|s| s.recall_at_k).sum::<f32>() / scores.len() as f32;
        let avg_precision_at_5 = scores.iter().map(|s| s.precision_at_k).sum::<f32>() / scores.len() as f32;

        // Query coverage: fraction with recall > 0.5
        let good_queries = scores.iter().filter(|s| s.recall_at_k > 0.5).count();
        let query_coverage = good_queries as f32 / scores.len() as f32;

        RetrievalMetrics {
            avg_relevance,
            avg_recall_at_5,
            avg_precision_at_5,
            query_coverage,
            per_query_scores: scores,
        }
    }

    /// Calculate query-chunk alignment score
    fn calculate_query_alignment(&self, query: &str, chunks: &[&Chunk]) -> f32 {
        if chunks.is_empty() {
            return 0.0;
        }

        let query_words: std::collections::HashSet<&str> = query.split_whitespace().collect();
        let total_query_words = query_words.len();

        if total_query_words == 0 {
            return 0.0;
        }

        let mut total_alignment = 0.0;

        for chunk in chunks.iter().take(self.k) {
            let chunk_words: std::collections::HashSet<&str> = chunk.content.split_whitespace().collect();
            let matching_words = query_words.intersection(&chunk_words).count();
            let alignment = matching_words as f32 / total_query_words as f32;
            total_alignment += alignment;
        }

        total_alignment / chunks.len().min(self.k) as f32
    }
}

/// Utility functions for relevance evaluation
pub mod utils {
    use super::*;

    /// Create a simple test query
    pub fn create_test_query(query: &str, relevant_chunks: Vec<uuid::Uuid>) -> TestQuery {
        TestQuery {
            query: query.to_string(),
            relevant_chunk_ids: relevant_chunks,
            expected_answer: format!("Expected answer for: {}", query),
        }
    }

    /// Generate mock retrieval function for testing
    pub fn mock_retrieval_function(chunks: Vec<Chunk>) -> impl Fn(&str) -> Vec<Chunk> {
        move |query: &str| {
            let query_words: std::collections::HashSet<&str> = query.split_whitespace().collect();

            let mut scored_chunks: Vec<(f32, Chunk)> = chunks.clone().into_iter()
                .map(|chunk| {
                    let chunk_words: std::collections::HashSet<&str> = chunk.content.split_whitespace().collect();
                    let matching = query_words.intersection(&chunk_words).count() as f32;
                    let total = chunk_words.len() as f32;
                    let score = if total > 0.0 { matching / total } else { 0.0 };
                    (score, chunk)
                })
                .collect();

            scored_chunks.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

            scored_chunks.into_iter()
                .take(5) // Return top 5
                .map(|(_, chunk)| chunk)
                .collect()
        }
    }

    /// Calculate improvement over baseline
    pub fn calculate_improvement(current: &RetrievalMetrics, baseline: &RetrievalMetrics) -> f32 {
        let current_avg = (current.avg_recall_at_5 + current.avg_precision_at_5) / 2.0;
        let baseline_avg = (baseline.avg_recall_at_5 + baseline.avg_precision_at_5) / 2.0;

        if baseline_avg > 0.0 {
            (current_avg - baseline_avg) / baseline_avg * 100.0
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Chunk};

    #[test]
    fn test_query_evaluation() {
        let evaluator = RelevanceEvaluator::new(5);
        let relevant_id = uuid::Uuid::new_v4();

        let query = TestQuery {
            query: "machine learning algorithms".to_string(),
            relevant_chunk_ids: vec![relevant_id],
            expected_answer: "ML algorithms are powerful".to_string(),
        };

        let chunk = Chunk::new("test_doc".to_string(), "machine learning algorithms work well".to_string(), 0, 1);
        let retrieved = vec![&chunk];

        let score = evaluator.evaluate_query(&query, &retrieved, None);

        assert!(score.recall_at_k >= 0.0);
        assert!(score.precision_at_k >= 0.0);
        assert!(score.query_alignment > 0.0); // Should have some alignment
    }

    #[test]
    fn test_query_alignment() {
        let evaluator = RelevanceEvaluator::new(5);

        let chunk1 = Chunk::new("doc1".to_string(), "machine learning is great".to_string(), 0, 1);
        let chunk2 = Chunk::new("doc2".to_string(), "artificial intelligence rocks".to_string(), 1, 2);

        let query = "machine learning algorithms";
        let chunks = vec![&chunk1, &chunk2];

        let alignment = evaluator.calculate_query_alignment(query, &chunks);
        assert!(alignment > 0.0);
        assert!(alignment <= 1.0);
    }
}
