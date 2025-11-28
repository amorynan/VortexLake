//! VortexLake Ingest - Splitter Demo
//!
//! This example demonstrates how to use different text splitters
//! to process documents for ingestion into VortexLake.

use std::collections::HashMap;
use vortexlake_ingest::{Document, DocumentSplitter, RecursiveCharacterSplitter, MarkdownSplitter, SemanticSplitter, SplitterConfig, analyze_chunks};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 VortexLake Ingest - Splitter Demo");
    println!("=====================================\n");

    // Create a sample document
    let document = create_sample_document();

    println!("📄 Sample Document:");
    println!("   ID: {}", document.id);
    println!("   Length: {} characters", document.content.len());
    println!("   Content preview: {:.150}...\n", document.content);

    // Demo 1: Recursive Character Splitter
    demo_recursive_splitter(&document).await?;

    // Demo 2: Markdown Splitter
    demo_markdown_splitter(&document).await?;

    // Demo 3: Semantic Splitter
    demo_semantic_splitter(&document).await?;

    println!("✅ All demos completed successfully!");
    Ok(())
}

fn create_sample_document() -> Document {
    let content = r#"
# Getting Started with VortexLake

VortexLake is a revolutionary vector-enhanced columnar database that combines the analytical power of traditional OLAP systems with modern vector similarity search capabilities.

## Why VortexLake?

Traditional databases excel at analytical queries but struggle with semantic search. Vector databases handle similarity search well but lack analytical capabilities. VortexLake bridges this gap.

## Core Features

### 1. Unified Architecture
- Single system for both SQL analytics and vector search
- No need to maintain separate databases
- Consistent data model and API

### 2. Performance Optimized
- Columnar storage for analytical workloads
- Vector indexes for similarity search
- Hybrid query optimization

### 3. Developer Friendly
- Standard SQL interface
- Vector search extensions
- Rich ecosystem integrations

## Quick Start

```rust
use vortexlake_core::VortexLake;

// Create database
let db = VortexLake::new("/tmp/vortexlake").await?;

// Ingest documents
let docs = vec![Document {
    id: "doc1".to_string(),
    content: "Your document content here...".to_string(),
    metadata: HashMap::new(),
}];

db.ingest(docs).await?;
```

## Performance Benchmarks

| Operation | Latency | Throughput | Notes |
|-----------|---------|------------|-------|
| Vector Search | <10ms | 1000 QPS | Cosine similarity |
| SQL Query | <100ms | 500 QPS | Complex analytics |
| Bulk Insert | N/A | 10000 docs/sec | Batch processing |
| Hybrid Query | <200ms | 200 QPS | Combined analytics + search |

## Use Cases

- **E-commerce**: Product search with filtering
- **Content Platforms**: Article recommendations
- **Financial Services**: Risk analysis with semantic insights
- **Healthcare**: Medical record analysis
- **Research**: Academic paper discovery

This comprehensive approach makes VortexLake the ideal choice for applications requiring both traditional analytics and modern AI-powered search capabilities.
    "#.to_string();

    let mut metadata = HashMap::new();
    metadata.insert("source".to_string(), json!("documentation"));
    metadata.insert("category".to_string(), json!("introduction"));
    metadata.insert("language".to_string(), json!("en"));
    metadata.insert("author".to_string(), json!("VortexLake Team"));

    Document {
        content,
        metadata,
        source_id: "getting_started_guide".to_string(),
    }
}

async fn demo_recursive_splitter(document: &Document) -> Result<(), Box<dyn std::error::Error>> {
    println!("🔄 Recursive Character Splitter Demo");
    println!("-------------------------------------");

    let config = SplitterConfig {
        max_chunk_size: 500,
        overlap: 50,
        preserve_metadata: true,
        language: None,
        custom_params: std::collections::HashMap::new(),
    };

    let splitter = RecursiveCharacterSplitter::new(config);
    let chunks = splitter.split(document).await?;

    println!("📊 Results:");
    println!("   Total chunks: {}", chunks.len());
    println!("   Recall improvement: {:.1}%", splitter.metrics().recall_improvement);
    println!("   Processing time: {:.1}ms", splitter.metrics().processing_time_ms);

    println!("\n📝 Chunk Details:");
    for (i, chunk) in chunks.iter().enumerate() {
        println!("   Chunk {}: {} chars", i + 1, chunk.content.len());
        if let Some(chunk_id) = chunk.metadata.get("chunk_id") {
            println!("     ID: {}", chunk_id);
        }
        // Show first line of content
        if let Some(first_line) = chunk.content.lines().next() {
            println!("     Preview: \"{:.80}...\"", first_line);
        }
        println!();
    }

    Ok(())
}

async fn demo_markdown_splitter(document: &Document) -> Result<(), Box<dyn std::error::Error>> {
    println!("📝 Markdown Structure-Aware Splitter Demo");
    println!("------------------------------------------");

    let config = SplitterConfig {
        max_chunk_size: 600,
        overlap: 0, // No overlap for structured content
        preserve_metadata: true,
        language: None,
        custom_params: std::collections::HashMap::new(),
    };

    let splitter = MarkdownSplitter::new(config);
    let chunks = splitter.split(document).await?;

    println!("📊 Results:");
    println!("   Total chunks: {}", chunks.len());
    println!("   Recall improvement: {:.1}%", splitter.metrics().recall_improvement);

    println!("\n📝 Chunk Details:");
    for (i, chunk) in chunks.iter().enumerate() {
        let header = chunk.metadata.get("header")
            .and_then(|h| h.as_str())
            .unwrap_or("No header");
        println!("   Chunk {}: {} chars", i + 1, chunk.content.len());
        println!("     Section: {}", header);
        println!();
    }

    Ok(())
}

async fn demo_semantic_splitter(document: &Document) -> Result<(), Box<dyn std::error::Error>> {
    println!("🧠 Semantic Splitter Demo");
    println!("-------------------------");

    let config = SplitterConfig {
        max_chunk_size: 400,
        overlap: 25,
        preserve_metadata: true,
        language: None,
        custom_params: std::collections::HashMap::new(),
    };

    let splitter = SemanticSplitter::new(config);
    let chunks = splitter.split(document).await?;

    // Analyze actual chunking results
    let chunk_metrics = analyze_chunks(&chunks);

    println!("📊 Results:");
    println!("   Total chunks: {} (expected: {:.0})", chunks.len(), splitter.metrics().expected_chunks_per_doc);
    println!("   Recall improvement: {:.1}%", splitter.metrics().recall_improvement);
    println!("   Semantic coherence: {:.2} (expected: {:.2})", chunk_metrics.semantic_coherence, splitter.metrics().expected_semantic_coherence);
    println!("   Avg chunk size: {:.0} chars", chunk_metrics.avg_chunk_size);
    println!("   Chunk size std dev: {:.0} chars", chunk_metrics.chunk_size_std_dev);
    println!("   Avg overlap ratio: {:.3}", chunk_metrics.avg_overlap_ratio);
    println!("   Overlap quality score: {:.2}", chunk_metrics.overlap_quality_score);

    println!("\n📝 Chunk Details:");
    for (i, chunk) in chunks.iter().enumerate() {
        println!("   Chunk {}: {} chars", i + 1, chunk.content.len());
        println!("     Sentences: ~{}",
            chunk.content.split(['.', '!', '?']).filter(|s| !s.trim().is_empty()).count());
        println!();
    }

    Ok(())
}
