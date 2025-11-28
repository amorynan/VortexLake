# VortexLake

A high-performance columnar database built on Apache Vortex, enhanced with vector indexing capabilities for efficient RAG (Retrieval-Augmented Generation) workloads. Combines OLAP analytical processing with vector similarity search in a unified system.

## Features

- **OLAP-Compatible Storage**: Built on Vortex columnar storage engine, optimized for analytical workloads
- **Vector Enhancement**: Advanced vector indexing (IVF-PQ, DiskANN) for similarity search
- **Unified Data Model**: Single system for both structured analytics and vector operations
- **SQL Interface**: Full SQL support via Apache DataFusion integration
- **Python Bindings**: Easy Python integration with PyO3
- **Document Ingestion**: Flexible document processing pipelines for RAG applications
- **Hybrid Queries**: Combine traditional SQL analytics with vector similarity search

## Architecture

VortexLake is designed as a **vector-enhanced columnar database** that unifies OLAP analytics and vector similarity search:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Python API    │    │   SQL Analytics │    │   REST/gRPC     │
│                 │    │  (DataFusion)   │    │    Server       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │  Vector Index   │
                    │   (IVF-PQ,      │
                    │    DiskANN)     │
                    └─────────────────┘
                             │
                    ┌─────────────────┐
                    │   Columnar      │
                    │   Storage       │
                    │   (Vortex OLAP) │
                    └─────────────────┘
```

**Key Design Principles:**
- **OLAP Foundation**: Vortex provides the columnar storage optimized for analytical queries
- **Vector Extension**: Adds similarity search capabilities on top of OLAP storage
- **Unified Schema**: Single table can contain both analytical columns and vector columns
- **Hybrid Queries**: SQL queries can combine aggregations with vector similarity search

## Quick Start

### Installation

```bash
# Install from source
cargo build --release

# Or install Python package (when available)
pip install vortexlake
```

### Basic Usage

```rust
use vortexlake_core::{VortexLake, Schema, Writer};
use vortexlake_index::VectorIndex;

// Create a new database
let db = VortexLake::new("/path/to/database")?;
let schema = Schema::new(vec![
    Field::new("id", DataType::Utf8, false),
    Field::new("vector", DataType::FixedSizeList(
        Arc::new(Field::new("item", DataType::Float32, false)),
        384
    ), false),
    Field::new("text", DataType::Utf8, false),
])?;

// Create tables and indexes
db.create_table("documents", schema)?;
db.create_vector_index("documents", "vector", IndexType::IVFPQ {
    num_clusters: 1000,
    num_subvectors: 32,
})?;

// Ingest data
let mut writer = db.writer("documents")?;
writer.write_batch(batch)?;
writer.commit()?;

// Query data
let results = db.vector_search("documents", query_vector, 10)?;
```

### Python API

```python
import vortexlake

# Connect to database
db = vortexlake.connect("/path/to/database")

# Create table
db.create_table("documents", {
    "id": "string",
    "vector": f"fixed_size_list[float32; 384]",
    "text": "string"
})

# Ingest documents
documents = [
    {"id": "doc1", "vector": embedding1, "text": "Document content..."},
    {"id": "doc2", "vector": embedding2, "text": "More content..."}
]
db.insert("documents", documents)

# Vector search
results = db.vector_search("documents", query_embedding, top_k=10)
```

## Project Structure

- `crates/vortexlake-core/`: Core storage engine with Vortex integration
- `crates/vortexlake-index/`: Vector indexing implementations (IVF-PQ, DiskANN)
- `crates/vortexlake-sql/`: SQL interface via DataFusion
- `crates/vortexlake-ingest/`: Document ingestion pipelines
- `crates/vortexlake-server/`: Optional gRPC/REST server
- `crates/vortexlake-python/`: Python bindings

## Development

### Prerequisites

- Rust nightly-2025-11-21 or later
- Python 3.8+ (for Python bindings)

### Building

```bash
# Build all crates
cargo build

# Build with optimizations
cargo build --release

# Run tests
cargo test

# Build Python extension
cd crates/vortexlake-python
maturin develop
```

### Examples

See the `examples/` directory for complete usage examples:

- `rag_ingest.rs`: Complete RAG ingestion pipeline
- `hybrid_query.rs`: Hybrid vector + keyword search
- `benchmark_compress.rs`: Compression benchmark

## Performance

VortexLake inherits Vortex's OLAP-optimized performance while adding vector capabilities:

- **OLAP Performance**: Vortex columnar storage provides analytical query performance comparable to modern OLAP systems
- **Vector Search**: Sub-millisecond similarity search with IVF-PQ indexing
- **Compression**: Vortex's advanced compression (up to 10x better than traditional formats)
- **Memory Efficiency**: Columnar storage with lazy loading for both analytics and vectors
- **Scalability**: Handles billions of rows with mixed analytical and vector data
- **Hybrid Workloads**: Efficiently processes both traditional analytics and RAG applications

## Contributing

Contributions are welcome! Please see our contributing guidelines and code of conduct.

## License

Licensed under either of:
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
