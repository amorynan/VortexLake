//! Evaluation suite for comprehensive chunking assessment
//!
//! This module provides a complete evaluation framework that combines
//! coherence, relevance, and other metrics for thorough chunking assessment.

use crate::eval::{coherence::CoherenceEvaluator, relevance::{RelevanceEvaluator, TestQuery}};
use crate::models::Chunk;

/// Comprehensive evaluation suite
pub struct EvaluationSuite {
    coherence_evaluator: CoherenceEvaluator,
    relevance_evaluator: RelevanceEvaluator,
}

impl EvaluationSuite {
    /// Create a new evaluation suite
    pub fn new() -> Self {
        Self {
            coherence_evaluator: CoherenceEvaluator::new(),
            relevance_evaluator: RelevanceEvaluator::new(5), // Top-5 evaluation
        }
    }

    /// Run comprehensive evaluation on chunks
    pub fn evaluate_chunks(&self, chunks: &[Chunk]) -> EvaluationReport {
        let coherence_scores = self.coherence_evaluator.evaluate_chunks(chunks);

        // Calculate aggregate coherence
        let avg_coherence = coherence_scores.iter()
            .map(|s| s.overall_score)
            .sum::<f32>() / coherence_scores.len() as f32;

        // Calculate chunk size statistics
        let chunk_sizes: Vec<f32> = chunks.iter().map(|c| c.content.len() as f32).collect();
        let avg_chunk_size = chunk_sizes.iter().sum::<f32>() / chunk_sizes.len() as f32;

        let variance = chunk_sizes.iter()
            .map(|s| (s - avg_chunk_size).powi(2))
            .sum::<f32>() / chunk_sizes.len() as f32;
        let size_std_dev = variance.sqrt();

        // Size consistency score (inverse of coefficient of variation)
        let size_consistency = if avg_chunk_size > 0.0 {
            1.0 / (1.0 + (size_std_dev / avg_chunk_size))
        } else {
            0.0
        };

        EvaluationReport {
            coherence_score: avg_coherence,
            size_consistency,
            avg_chunk_size,
            chunk_count: chunks.len(),
            recommendations: self.generate_recommendations(avg_coherence, size_consistency),
        }
    }

    /// Evaluate end-to-end RAG performance
    pub async fn evaluate_rag_performance(
        &self,
        test_queries: &[TestQuery],
        retrieval_fn: impl Fn(&str) -> Vec<Chunk>,
    ) -> RagEvaluationReport {
        let retrieval_metrics = self.relevance_evaluator.evaluate_queries(test_queries, retrieval_fn);
        let overall_score = (retrieval_metrics.avg_recall_at_5 + retrieval_metrics.avg_precision_at_5) / 2.0;

        RagEvaluationReport {
            retrieval_metrics,
            overall_score,
        }
    }

    /// Generate recommendations based on evaluation scores
    fn generate_recommendations(&self, coherence: f32, size_consistency: f32) -> Vec<String> {
        let mut recommendations = Vec::new();

        if coherence < 0.7 {
            recommendations.push("Consider using semantic splitters for better coherence".to_string());
        }

        if size_consistency < 0.6 {
            recommendations.push("Chunk sizes vary significantly - consider adjusting split parameters".to_string());
        }

        if coherence > 0.8 && size_consistency > 0.7 {
            recommendations.push("Excellent chunking quality!".to_string());
        }

        recommendations
    }
}

/// Evaluation report for chunking quality
#[derive(Debug, Clone)]
pub struct EvaluationReport {
    /// Average coherence score (0-1)
    pub coherence_score: f32,
    /// Size consistency score (0-1)
    pub size_consistency: f32,
    /// Average chunk size in characters
    pub avg_chunk_size: f32,
    /// Total number of chunks
    pub chunk_count: usize,
    /// Recommendations for improvement
    pub recommendations: Vec<String>,
}

/// End-to-end RAG evaluation report
#[derive(Debug, Clone)]
pub struct RagEvaluationReport {
    /// Detailed retrieval metrics
    pub retrieval_metrics: crate::eval::relevance::RetrievalMetrics,
    /// Overall RAG performance score (0-1)
    pub overall_score: f32,
}

/// Benchmark runner for comparing different splitters
pub struct BenchmarkRunner {
    suite: EvaluationSuite,
}

impl BenchmarkRunner {
    /// Create a new benchmark runner
    pub fn new() -> Self {
        Self {
            suite: EvaluationSuite::new(),
        }
    }

    /// Run benchmark comparing multiple splitter configurations
    pub async fn run_benchmark(
        &self,
        name: &str,
        chunks: &[Chunk],
        test_queries: &[TestQuery],
        retrieval_fn: impl Fn(&str) -> Vec<Chunk>,
    ) -> BenchmarkResult {
        let quality_report = self.suite.evaluate_chunks(chunks);
        let rag_report = self.suite.evaluate_rag_performance(test_queries, retrieval_fn).await;

        BenchmarkResult {
            name: name.to_string(),
            timestamp: chrono::Utc::now(),
            quality_report,
            rag_report,
        }
    }

    /// Compare two benchmark results
    pub fn compare_results(&self, baseline: &BenchmarkResult, current: &BenchmarkResult) -> ComparisonReport {
        let quality_improvement = self.calculate_improvement(
            baseline.quality_report.coherence_score,
            current.quality_report.coherence_score,
        );

        let rag_improvement = self.calculate_improvement(
            baseline.rag_report.overall_score,
            current.rag_report.overall_score,
        );

        ComparisonReport {
            quality_improvement,
            rag_improvement,
            baseline_name: baseline.name.clone(),
            current_name: current.name.clone(),
        }
    }

    fn calculate_improvement(&self, baseline: f32, current: f32) -> f32 {
        if baseline > 0.0 {
            (current - baseline) / baseline * 100.0
        } else {
            0.0
        }
    }
}

/// Benchmark result for a single configuration
#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    /// Configuration name
    pub name: String,
    /// When the benchmark was run
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Quality evaluation report
    pub quality_report: EvaluationReport,
    /// RAG evaluation report
    pub rag_report: RagEvaluationReport,
}

/// Comparison between two benchmark results
#[derive(Debug, Clone)]
pub struct ComparisonReport {
    /// Quality score improvement (percentage)
    pub quality_improvement: f32,
    /// RAG performance improvement (percentage)
    pub rag_improvement: f32,
    /// Baseline configuration name
    pub baseline_name: String,
    /// Current configuration name
    pub current_name: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evaluation_suite() {
        let suite = EvaluationSuite::new();

        let chunks = vec![
            Chunk::new("doc1".to_string(), "This is a test chunk about machine learning.".to_string(), 0, 1),
            Chunk::new("doc1".to_string(), "Another chunk discussing artificial intelligence.".to_string(), 1, 2),
        ];

        let report = suite.evaluate_chunks(&chunks);

        assert!(report.coherence_score >= 0.0);
        assert!(report.coherence_score <= 1.0);
        assert_eq!(report.chunk_count, 2);
        assert!(!report.recommendations.is_empty());
    }

    #[test]
    fn test_benchmark_runner() {
        let runner = BenchmarkRunner::new();

        let chunks = vec![
            Chunk::new("doc1".to_string(), "Machine learning algorithms are powerful.".to_string(), 0, 1),
        ];

        let test_queries = vec![
            crate::eval::relevance::utils::create_test_query(
                "machine learning",
                vec![chunks[0].id],
            ),
        ];

        let mock_retrieval = crate::eval::relevance::utils::mock_retrieval_function(chunks.clone());

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(runner.run_benchmark("test_config", &chunks, &test_queries, mock_retrieval));

        assert_eq!(result.name, "test_config");
        assert!(result.quality_report.coherence_score >= 0.0);
    }
}
