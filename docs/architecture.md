# VortexLake Architecture

## Overview

VortexLake is a **vector-enhanced columnar database** that combines Apache Vortex's OLAP-optimized columnar storage with advanced vector indexing capabilities. Unlike specialized vector databases, VortexLake maintains full OLAP compatibility while adding efficient similarity search, making it ideal for applications requiring both analytical processing and vector operations.

**Key Differentiation:**
- **OLAP Foundation**: Built on Vortex, inherits OLAP-optimized columnar storage
- **Vector Enhancement**: Adds similarity search without sacrificing analytical performance
- **Unified System**: Single database for both structured analytics and unstructured vector search
- **RAG-Optimized**: Designed for retrieval-augmented generation workloads that need both analytics and similarity search

## Core Principles

1. **OLAP Foundation**: Built on Vortex columnar storage, maintains full OLAP compatibility and performance
2. **Vector Enhancement**: Adds similarity search capabilities without compromising analytical workloads
3. **Unified Data Model**: Single schema supports both structured columns and vector columns
4. **SQL + Vectors**: Combines DataFusion SQL analytics with vector similarity search
5. **Extensible Architecture**: Plugin system for custom indexing, compression, and storage strategies
6. **Python Integration**: Native PyO3 bindings for seamless Python ecosystem integration

## Architecture Components

### 1. Storage Layer (vortexlake-core)

The foundation is Apache Vortex, a columnar storage engine optimized for OLAP workloads, extended with vector capabilities:

- **OLAP-Optimized Storage**: Vortex provides analytical query performance comparable to modern OLAP systems
- **Vector Column Support**: Native support for fixed-size vector columns alongside traditional data types
- **Schema Management**: Type-safe schema definitions with Arrow compatibility for unified data model
- **Fragment Management**: Physical file fragmentation optimized for both analytical scans and vector access
- **Advanced Compression**: Vortex's compression algorithms (often 10x better than traditional formats)
- **ACID Operations**: Versioned manifest system ensures transactional consistency
- **Hybrid Ingestion**: Batch and streaming writes supporting mixed analytical and vector data

**Key Files:**
- `schema.rs`: Table and column definitions
- `writer.rs`: Bulk data ingestion with Vortex writer
- `reader.rs`: DataFusion-compatible reader implementation
- `manifest.rs`: Versioned metadata management
- `fragment.rs`: Physical file fragmentation
- `compression/`: Pluggable compression strategies

### 2. Vector Enhancement Layer (vortexlake-index)

Adds similarity search capabilities to the OLAP foundation without compromising analytical performance:

- **IVF-PQ**: Inverted File with Product Quantization for efficient approximate nearest neighbor search
- **DiskANN**: Graph-based indexing for disk-resident vectors (planned Vortex plugin integration)
- **Metadata Indexes**: Inverted, bitmap, and range indexes for filtering vector search results
- **Hybrid Filtering**: Combines vector similarity with traditional metadata filtering
- **Indexing Agnostic**: Vector indexes built on top of columnar storage, preserving OLAP query capabilities

**Key Files:**
- `ivf_pq.rs`: IVF-PQ implementation (initially using usearch)
- `diskann.rs`: DiskANN integration (planned for Vortex plugin)
- `metadata_index.rs`: Secondary indexes for metadata filtering

### 3. SQL Analytics Layer (vortexlake-sql)

DataFusion integration provides full SQL analytics while extending support for vector operations:

- **Session Management**: Query execution contexts
- **Table Provider**: DataFusion TableProvider implementation
- **Execution Engine**: Optimized query execution plans

**Key Files:**
- `session.rs`: Session and context management
- `execution.rs`: DataFusion integration and UDFs
- `table_provider.rs`: TableProvider trait implementation

### 4. Ingestion Pipeline (vortexlake-ingest)

Document processing and ingestion workflows:

- **Document Splitters**: Text chunking strategies
- **Embedding Integration**: Vector generation pipelines
- **Batch Processing**: High-throughput data ingestion

**Key Files:**
- `pipeline.rs`: Document processing pipelines

### 5. Python Bindings (vortexlake-python)

PyO3-based Python interface for easy integration:

- **High-Level API**: Pythonic interface to Rust functionality
- **DataFrame Integration**: Pandas/Polars compatibility
- **Async Support**: Native async/await support

## Data Flow

### Ingestion Flow

```
Documents → Splitter → Embedder → Batch Writer → Fragments
     ↓         ↓         ↓         ↓         ↓
   Raw Text → Chunks → Vectors → Vortex → Storage
```

### Query Flow

**Traditional OLAP Queries:**
```
SQL Query → Parser → Planner → Executor → Reader → Results
     ↓         ↓         ↓         ↓         ↓         ↓
   "SELECT" → AST → Plan → DataFusion → Vortex → Arrow
```

**Vector-Enhanced Queries:**
```
Hybrid Query → Parser → Planner → Executor → Vector Index → Reader → Results
     ↓         ↓         ↓         ↓         ↓         ↓         ↓
   "SELECT" → AST → Plan → DataFusion → IVF-PQ → Vortex → Arrow
```

**Pure Vector Search:**
```
Vector Query → Index → Distance → Ranking → Results
     ↓         ↓         ↓         ↓         ↓
   Query Vec → IVF-PQ → Compute → Top-K → Matches
```

## Storage Format

### Fragment Structure

Each fragment contains:

- **Data Files**: Columnar data in Vortex format (.vortex)
- **Index Files**: Vector indexes and metadata indexes
- **Manifest**: Fragment metadata and statistics

### Manifest Format

```json
{
  "version": "1.0",
  "fragments": [
    {
      "id": "frag_001",
      "path": "data/frag_001.vortex",
      "schema": {...},
      "statistics": {
        "row_count": 1000000,
        "column_stats": {...}
      },
      "indexes": [
        {
          "type": "ivf_pq",
          "path": "index/frag_001_ivf.idx",
          "parameters": {...}
        }
      ]
    }
  ],
  "global_stats": {...}
}
```

## Indexing Strategy

### IVF-PQ Implementation

1. **Training Phase**:
   - Sample vectors for PQ codebook training
   - K-means clustering for IVF centroids
   - Build inverted index structure

2. **Query Phase**:
   - Find nearest IVF clusters
   - Search within clusters using PQ distance
   - Merge and rank results

### Metadata Filtering

- **Bitmap Indexes**: For categorical metadata
- **Inverted Indexes**: For text metadata
- **Range Indexes**: For numerical metadata

## Performance Optimizations

### Memory Management

- **Columnar Lazy Loading**: Load only required columns, preserving OLAP memory efficiency
- **Vector Memory Mapping**: Direct file access for large vector datasets
- **Hybrid Object Pooling**: Reuse allocations for both analytical and vector operations

### Query Optimization

- **OLAP Predicate Pushdown**: Traditional filters pushed down to Vortex storage layer
- **Vector Index Selection**: Automatic selection between IVF-PQ, DiskANN, or brute force
- **Hybrid Parallel Execution**: Multi-threaded execution for mixed analytical and vector workloads
- **Query Planning**: Intelligent planning for queries combining SQL analytics with vector search

### Compression

- **Dictionary Encoding**: For categorical data
- **Run-Length Encoding**: For sorted data
- **Delta Encoding**: For time-series data
- **Vortex Compression**: Adaptive compression based on data patterns

## Extensibility

### Custom Index Plugins

```rust
pub trait VectorIndex {
    fn build(&mut self, vectors: &[f32], ids: &[u64]) -> Result<()>;
    fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>>;
    fn save(&self, path: &Path) -> Result<()>;
    fn load(&self, path: &Path) -> Result<()>;
}
```

### Custom Compression

```rust
pub trait Compressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>>;
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>>;
    fn name(&self) -> &str;
}
```

## Future Enhancements

1. **Distributed Storage**: Multi-node scaling with consistent hashing
2. **Real-time Updates**: Streaming ingestion with incremental indexing
3. **GPU Acceleration**: CUDA/ROCm support for vector operations
4. **Advanced Indexing**: HNSW, SQ, and other ANN algorithms
5. **Multi-modal**: Support for images, audio, and other modalities
