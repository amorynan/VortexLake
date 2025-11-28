//! Complete RAG ingestion example for VortexLake
//!
//! This example demonstrates how to ingest documents for a RAG application
//! using VortexLake's ingestion pipeline and vector indexing.

use anyhow::Result;
use std::collections::HashMap;
use vortexlake_core::{VortexLake, Schema, Field};
use vortexlake_index::{VectorIndex, IvfPqIndex, IndexConfig, DistanceMetric};
use vortexlake_ingest::{Document, IngestionPipeline, RecursiveSplitter, MockEmbeddingFunction};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::init();

    // Create database
    let db_path = "/tmp/vortexlake_rag_example";
    let db = VortexLake::new(db_path).await?;

    // Define schema for documents
    let schema = Schema::new(vec![
        Field::new("id", arrow::datatypes::DataType::Utf8, false),
        Field::new("content", arrow::datatypes::DataType::Utf8, false),
        Field::new("title", arrow::datatypes::DataType::Utf8, true),
        Field::new("url", arrow::datatypes::DataType::Utf8, true),
        Field::new(
            "vector",
            arrow::datatypes::DataType::FixedSizeList(
                std::sync::Arc::new(arrow::datatypes::Field::new(
                    "item",
                    arrow::datatypes::DataType::Float32,
                    false,
                )),
                384, // Embedding dimension
            ),
            false,
        ),
    ])?;

    // Create table
    db.create_table("documents", schema).await?;
    println!("Created documents table");

    // Sample documents
    let documents = vec![
        Document {
            id: "doc1".to_string(),
            content: "Rust is a systems programming language that runs blazingly fast, prevents segfaults, and guarantees thread safety.".to_string(),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("title".to_string(), serde_json::json!("Introduction to Rust"));
                meta.insert("url".to_string(), serde_json::json!("https://rust-lang.org"));
                meta
            },
        },
        Document {
            id: "doc2".to_string(),
            content: "VortexLake is a high-performance vector database built on Apache Arrow and Vortex columnar storage.".to_string(),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("title".to_string(), serde_json::json!("VortexLake Overview"));
                meta.insert("url".to_string(), serde_json::json!("https://github.com/vortexlake"));
                meta
            },
        },
        // Add more documents...
    ];

    // Create ingestion pipeline
    let splitter = std::sync::Arc::new(RecursiveSplitter::new(512, 50));
    let embedding_fn = std::sync::Arc::new(MockEmbeddingFunction::new(384));

    let pipeline = IngestionPipeline::new(splitter)
        .with_embedding_fn(embedding_fn)
        .with_batch_size(10);

    println!("Processing {} documents...", documents.len());

    // Process documents
    let processed_docs = pipeline.process(documents).await?;
    println!("Generated {} chunks", processed_docs.len());

    // Prepare data for insertion
    let mut ids = Vec::new();
    let mut contents = Vec::new();
    let mut titles = Vec::new();
    let mut urls = Vec::new();
    let mut vectors = Vec::new();

    for doc in &processed_docs {
        ids.push(doc.id.as_str());
        contents.push(doc.content.as_str());

        // Extract metadata
        titles.push(doc.metadata.get("title")
            .and_then(|v| v.as_str())
            .unwrap_or(""));
        urls.push(doc.metadata.get("url")
            .and_then(|v| v.as_str())
            .unwrap_or(""));

        // Get embedding
        if let Some(embedding) = &doc.embedding {
            vectors.push(embedding.clone());
        }
    }

    // Create record batch
    use arrow::array::{StringArray, FixedSizeListArray, Float32Array};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let batch = RecordBatch::try_new(
        Arc::new(schema.to_arrow()),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(StringArray::from(contents)),
            Arc::new(StringArray::from(titles)),
            Arc::new(StringArray::from(urls)),
            Arc::new(create_vector_array(vectors, 384)),
        ],
    )?;

    // Insert data
    let mut writer = db.writer("documents")?;
    writer.write_batch(batch).await?;
    writer.commit().await?;
    println!("Inserted {} document chunks", processed_docs.len());

    // Create vector index
    println!("Building vector index...");
    let index_config = IndexConfig::IvfPq {
        num_clusters: 10,
        num_subvectors: 8,
        dimension: 384,
        metric: DistanceMetric::Cosine,
    };

    let mut index = IvfPqIndex::new(index_config)?;
    let vector_ids: Vec<u64> = (0..vectors.len()).map(|i| i as u64).collect();

    index.build(&vectors, &vector_ids).await?;
    println!("Index built with {} vectors", vectors.len());

    // Example search
    let query_vector = vec![0.1f32; 384]; // Mock query
    let results = index.search(&query_vector, 3).await?;
    println!("Search results:");
    for result in results {
        println!("  ID: {}, Distance: {:.4}", result.id, result.distance);
    }

    Ok(())
}

fn create_vector_array(vectors: Vec<Vec<f32>>, dimension: usize) -> FixedSizeListArray {
    let mut flat_data = Vec::new();
    for vector in vectors {
        flat_data.extend(vector);
    }

    let values = Float32Array::from(flat_data);
    let field = Arc::new(arrow::datatypes::Field::new(
        "item",
        arrow::datatypes::DataType::Float32,
        false,
    ));

    FixedSizeListArray::new(field, dimension as i32, Arc::new(values), None)
}
