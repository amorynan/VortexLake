//! # VortexLake Server
//!
//! gRPC and REST API server for VortexLake.
//!
//! ## Features
//!
//! - REST API endpoints
//! - gRPC services
//! - Health checks
//! - Metrics collection (planned)

pub mod rest;
pub mod grpc;

/// Start the VortexLake server
pub async fn start_server(addr: &str, db_path: &str) -> anyhow::Result<()> {
    // TODO: Implement server startup
    tracing::info!("Starting VortexLake server on {} with database at {}", addr, db_path);
    Ok(())
}
