//! Session management for VortexLake SQL queries

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
// Use DataFusion's re-exported Arrow types to avoid version conflicts
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use vortexlake_core::VortexLake;

use crate::table_provider::VortexLakeTableProvider;

/// Query execution result
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Result batches
    pub batches: Vec<RecordBatch>,
    /// Number of rows affected (for DDL/DML)
    pub affected_rows: Option<usize>,
    /// Execution time in milliseconds
    pub execution_time_ms: f64,
}

/// SQL session for VortexLake
pub struct Session {
    /// VortexLake database instance
    db: VortexLake,
    /// DataFusion session context
    df_ctx: datafusion::execution::context::SessionContext,
    /// Registered table providers
    table_providers: HashMap<String, Arc<VortexLakeTableProvider>>,
}

impl Session {
    /// Create a new session
    pub async fn new(db_path: &str) -> Result<Self> {
        // Create VortexLake database
        let db = VortexLake::new(db_path).await?;

        // Create DataFusion context
        let df_ctx = datafusion::execution::context::SessionContext::new();

        Ok(Self {
            db,
            df_ctx,
            table_providers: HashMap::new(),
        })
    }

    /// Execute a SQL query
    pub async fn execute(&mut self, query: &str) -> Result<QueryResult> {
        let start_time = std::time::Instant::now();

        // Execute with DataFusion
        let df = self.df_ctx.sql(query).await?;
        let batches = df.collect().await?;

        let execution_time = start_time.elapsed().as_millis() as f64;

        Ok(QueryResult {
            batches,
            affected_rows: None, // TODO: Calculate for DML operations
            execution_time_ms: execution_time,
        })
    }

    /// Register a table in the session
    pub async fn register_table(&mut self, table_name: &str) -> Result<()> {
        let provider =
            VortexLakeTableProvider::new(self.db.path().to_string_lossy().as_ref(), table_name)
                .await?;

        // Register with DataFusion
        self.df_ctx
            .register_table(table_name, Arc::new(provider.clone()))?;

        self.table_providers
            .insert(table_name.to_string(), Arc::new(provider));
        Ok(())
    }

    /// Unregister a table from the session
    pub fn unregister_table(&mut self, table_name: &str) -> Result<()> {
        // Unregister from DataFusion
        self.df_ctx.deregister_table(table_name)?;

        self.table_providers.remove(table_name);
        Ok(())
    }

    /// List registered tables
    pub fn list_tables(&self) -> Vec<String> {
        self.table_providers.keys().cloned().collect()
    }

    /// Get table schema
    pub fn get_schema(&self, table_name: &str) -> Result<SchemaRef> {
        if let Some(provider) = self.table_providers.get(table_name) {
            Ok(provider.schema())
        } else {
            Err(anyhow::anyhow!("Table '{}' not registered", table_name))
        }
    }

    /// Execute DDL statement (CREATE, DROP, ALTER)
    pub async fn execute_ddl(&mut self, ddl: &str) -> Result<QueryResult> {
        // Parse DDL and execute
        match parse_ddl(ddl)? {
            DDLStatement::CreateTable { name, schema: _ } => {
                // TODO: Create table in VortexLake
                self.register_table(&name).await?;
                Ok(QueryResult {
                    batches: vec![],
                    affected_rows: Some(0),
                    execution_time_ms: 0.0,
                })
            }
            DDLStatement::DropTable { name } => {
                self.unregister_table(&name)?;
                // TODO: Drop table from VortexLake
                Ok(QueryResult {
                    batches: vec![],
                    affected_rows: Some(0),
                    execution_time_ms: 0.0,
                })
            }
        }
    }

    /// Execute DML statement (INSERT, UPDATE, DELETE)
    pub async fn execute_dml(&mut self, dml: &str) -> Result<QueryResult> {
        // For now, just execute as regular SQL
        self.execute(dml).await
    }

    /// Execute SELECT query
    pub async fn execute_select(&mut self, select: &str) -> Result<QueryResult> {
        self.execute(select).await
    }

    /// Get the underlying VortexLake database
    pub fn database(&self) -> &VortexLake {
        &self.db
    }

    /// Get the DataFusion session context
    pub fn df_context(&self) -> &datafusion::execution::context::SessionContext {
        &self.df_ctx
    }
}

/// DDL statement types
enum DDLStatement {
    CreateTable { name: String, schema: String },
    DropTable { name: String },
}

/// Parse DDL statement (simplified)
fn parse_ddl(ddl: &str) -> Result<DDLStatement> {
    let ddl_upper = ddl.trim().to_uppercase();

    if ddl_upper.starts_with("CREATE TABLE") {
        // Very simplified parsing
        let parts: Vec<&str> = ddl_upper.split_whitespace().collect();
        if parts.len() >= 3 {
            let table_name = parts[2].to_string();
            // TODO: Parse schema properly
            let schema = ddl.to_string();
            return Ok(DDLStatement::CreateTable {
                name: table_name,
                schema,
            });
        }
    } else if ddl_upper.starts_with("DROP TABLE") {
        let parts: Vec<&str> = ddl_upper.split_whitespace().collect();
        if parts.len() >= 3 {
            let table_name = parts[2].to_string();
            return Ok(DDLStatement::DropTable { name: table_name });
        }
    }

    Err(anyhow::anyhow!("Unsupported DDL statement: {}", ddl))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_session_creation() {
        let temp_dir = tempdir().unwrap();
        let session = Session::new(temp_dir.path().to_string_lossy().as_ref())
            .await
            .unwrap();
        assert!(session.list_tables().is_empty());
    }

    #[test]
    fn test_ddl_parsing() {
        let create = parse_ddl("CREATE TABLE test (id INT, name VARCHAR)").unwrap();
        match create {
            DDLStatement::CreateTable { name, .. } => assert_eq!(name, "TEST"),
            _ => panic!("Expected CreateTable"),
        }

        let drop = parse_ddl("DROP TABLE test").unwrap();
        match drop {
            DDLStatement::DropTable { name } => assert_eq!(name, "TEST"),
            _ => panic!("Expected DropTable"),
        }
    }
}
