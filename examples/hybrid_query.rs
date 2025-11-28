//! Hybrid search example combining vector similarity and metadata filtering
//!
//! This example demonstrates how to perform hybrid queries that combine
//! vector similarity search with traditional metadata filtering.

use anyhow::Result;
use vortexlake_index::{MetadataIndex, MetadataFilter};
use vortexlake_sql::{Session, execute_query};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::init();

    let db_path = "/tmp/vortexlake_hybrid_example";

    // Initialize metadata index for filtering
    let mut metadata_index = MetadataIndex::new();

    // Add some sample metadata
    metadata_index.add_text("content", "Rust programming language tutorial", 1);
    metadata_index.add_categorical("category", "programming", 1);
    metadata_index.add_numerical("score", 0.95, 1);

    metadata_index.add_text("content", "Python data science guide", 2);
    metadata_index.add_categorical("category", "data-science", 2);
    metadata_index.add_numerical("score", 0.87, 2);

    metadata_index.add_text("content", "Machine learning with Rust", 3);
    metadata_index.add_categorical("category", "ml", 3);
    metadata_index.add_numerical("score", 0.92, 3);

    metadata_index.finalize();

    println!("Metadata index created with 3 documents");

    // Example hybrid search: find documents about "Rust" with high scores
    let filters = vec![
        MetadataFilter::Text {
            field: "content".to_string(),
            query: "rust".to_string(),
        },
        MetadataFilter::NumericalRange {
            field: "score".to_string(),
            min: 0.9,
            max: 1.0,
        },
    ];

    let candidate_ids = metadata_index.combined_filter(&filters);
    println!("Found {} documents matching filters: {:?}", candidate_ids.len(), candidate_ids);

    // In a real implementation, you would:
    // 1. Get vectors for candidate documents
    // 2. Perform vector search on the filtered set
    // 3. Combine scores and rerank

    // Example SQL query (would work with real data)
    println!("\nExample SQL queries:");

    let sql_queries = vec![
        "SELECT * FROM documents WHERE category = 'programming'",
        "SELECT * FROM documents WHERE score > 0.9",
        "SELECT * FROM documents WHERE content LIKE '%rust%'",
        "SELECT * FROM vector_search(documents, [0.1, 0.2, ...], 10) WHERE score > 0.8",
    ];

    for query in sql_queries {
        println!("  {}", query);
    }

    // Demonstrate session usage
    let mut session = Session::new(db_path)?;
    println!("\nSession created for database: {}", db_path);

    // In a real scenario, you would register tables and execute queries
    // session.register_table("documents").await?;
    // let result = session.execute("SELECT COUNT(*) FROM documents").await?;

    Ok(())
}
