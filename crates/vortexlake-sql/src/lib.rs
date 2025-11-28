//! # VortexLake SQL
//!
//! SQL interface for VortexLake built on Apache DataFusion.
//!
//! ## Features
//!
//! - Full SQL support via DataFusion
//! - Vector search functions (planned)
//! - Hybrid search queries
//! - Session management
//! - Custom UDFs for vector operations
//!
//! ## Example
//!
//! ```rust,no_run
//! use vortexlake_sql::{Session, QueryResult};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create session (async)
//! let mut session = Session::new("/tmp/vortexlake").await?;
//!
//! // Execute simple SQL query
//! let results = session.execute("SELECT 1 as test").await?;
//!
//! // Query with registered tables
//! // session.register_table("docs").await?;
//! // let results = session.execute("SELECT * FROM docs").await?;
//! # Ok(())
//! # }
//! ```

pub mod execution;
pub mod file_source;
pub mod session;
pub mod table_provider;

pub use file_source::{PruningStats, VortexLakeFileList};
pub use session::{QueryResult, Session};
pub use table_provider::VortexLakeTableProvider;

/// Initialize the VortexLake SQL environment
pub async fn init() -> anyhow::Result<()> {
    // Initialize DataFusion context with VortexLake extensions
    Ok(())
}

/// Execute a SQL query against a VortexLake database
pub async fn execute_query(
    db_path: &str,
    query: &str,
) -> anyhow::Result<QueryResult> {
    let mut session = Session::new(db_path).await?;
    session.execute(query).await
}

/// Register vector search functions
pub fn register_vector_functions() -> anyhow::Result<()> {
    // TODO: Register UDFs for vector operations
    // - vector_search(table, query_vector, k)
    // - cosine_similarity(a, b)
    // - euclidean_distance(a, b)
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_basic_query() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().to_string_lossy();

        // This would require a full database setup
        // For now, just test session creation
        let result = Session::new(&db_path).await;
        assert!(result.is_ok());
    }
}
