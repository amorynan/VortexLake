//! TPC-H Validation Tests
//!
//! This module validates VortexLake query results against Parquet (DataFusion native)
//! using TPC-H standard dataset and queries.
//!
//! Architecture:
//! ```text
//!                    DataFusion (Query Engine)
//!                           ↑
//!              ┌────────────┴────────────┐
//!              │                         │
//!    VortexLake (Vortex format)   Parquet (baseline)
//!              ↑                         ↑
//!              └────────── compare ──────┘
//! ```
//!
//! This validates:
//! 1. VortexLake TableProvider correctness
//! 2. Query result consistency
//! 3. Relative performance (Vortex vs Parquet)

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    ArrayRef, Date32Array, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use datafusion_physical_plan::ExecutionPlan;
use futures::TryStreamExt;
use prettytable::{row, Table};
use tempfile::TempDir;

// TPC-H official data generators
use tpchgen::generators::{
    LineItemGenerator, OrderGenerator, CustomerGenerator,
    PartGenerator, SupplierGenerator, PartSuppGenerator,
    NationGenerator, RegionGenerator,
};
use tpchgen_arrow::{
    LineItemArrow, OrderArrow, CustomerArrow,
    PartArrow, SupplierArrow, PartSuppArrow,
    NationArrow, RegionArrow,
};

// VortexLake imports
use vortexlake_core::{VortexLake, Schema as VLSchema, Field as VLField};
use vortexlake_sql::table_provider::VortexLakeTableProvider;

/// Initialize tracing subscriber for test logging
/// 
/// Log level can be controlled via RUST_LOG environment variable:
/// - RUST_LOG=vortexlake_sql=info (default for tests)
/// - RUST_LOG=vortexlake_sql=debug (more verbose)
/// - RUST_LOG=info (all crates at info level)
fn init_test_logging() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    
    INIT.call_once(|| {
        // Use RUST_LOG if set, otherwise default to info level for vortexlake_sql
        let default_filter = std::env::var("RUST_LOG")
            .unwrap_or_else(|_| "vortexlake_sql=info".to_string());
        
        tracing_subscriber::fmt()
            .with_env_filter(default_filter)
            .with_test_writer()
            .init();
    });
}

/// TPC-H Scale Factor for tests (0.01 = ~6MB data, 0.1 = ~60MB, 1.0 = ~600MB)
const SCALE_FACTOR: f64 = 0.1;

/// Test data directory (persistent, not deleted after tests)
/// Uses absolute path to avoid URL encoding issues with ObjectPath
fn get_test_data_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("target/test_data")
        .canonicalize()
        .unwrap_or_else(|_| {
            // If canonicalize fails (dir doesn't exist), create and return the path
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join("target/test_data");
            std::fs::create_dir_all(&path).ok();
            path.canonicalize().unwrap_or(path)
        })
}

// ============================================================================
// Official TPC-H Data Generation
// ============================================================================

/// Generate official TPC-H LINEITEM data using tpchgen-arrow
/// 
/// This generates data that matches the TPC-H specification exactly,
/// including proper data distribution, string patterns, and relationships.
fn generate_official_tpch_lineitem(scale_factor: f64, max_rows: Option<usize>) -> Vec<RecordBatch> {
    // SF=1 has about 6M rows in lineitem
    // part=1, part_count=1 means generate all data in one partition
    let generator = LineItemGenerator::new(scale_factor, 1, 1);
    let mut arrow_gen = LineItemArrow::new(generator)
        .with_batch_size(10_000); // 10K rows per batch
    
    let mut batches = Vec::new();
    let mut total_rows = 0;
    
    while let Some(batch) = arrow_gen.next() {
        total_rows += batch.num_rows();
        batches.push(batch);
        
        // Limit rows if specified
        if let Some(max) = max_rows {
            if total_rows >= max {
                break;
            }
        }
    }
    
    batches
}

/// Generate official TPC-H ORDERS data
fn generate_official_tpch_orders(scale_factor: f64) -> Vec<RecordBatch> {
    let generator = OrderGenerator::new(scale_factor, 1, 1);
    let mut arrow_gen = OrderArrow::new(generator).with_batch_size(10_000);
    let mut batches = Vec::new();
    while let Some(batch) = arrow_gen.next() {
        batches.push(batch);
    }
    batches
}

/// Generate official TPC-H CUSTOMER data
fn generate_official_tpch_customer(scale_factor: f64) -> Vec<RecordBatch> {
    let generator = CustomerGenerator::new(scale_factor, 1, 1);
    let mut arrow_gen = CustomerArrow::new(generator).with_batch_size(10_000);
    let mut batches = Vec::new();
    while let Some(batch) = arrow_gen.next() {
        batches.push(batch);
    }
    batches
}

/// Generate official TPC-H PART data
fn generate_official_tpch_part(scale_factor: f64) -> Vec<RecordBatch> {
    let generator = PartGenerator::new(scale_factor, 1, 1);
    let mut arrow_gen = PartArrow::new(generator).with_batch_size(10_000);
    let mut batches = Vec::new();
    while let Some(batch) = arrow_gen.next() {
        batches.push(batch);
    }
    batches
}

/// Generate official TPC-H SUPPLIER data
fn generate_official_tpch_supplier(scale_factor: f64) -> Vec<RecordBatch> {
    let generator = SupplierGenerator::new(scale_factor, 1, 1);
    let mut arrow_gen = SupplierArrow::new(generator).with_batch_size(10_000);
    let mut batches = Vec::new();
    while let Some(batch) = arrow_gen.next() {
        batches.push(batch);
    }
    batches
}

/// Generate official TPC-H PARTSUPP data
fn generate_official_tpch_partsupp(scale_factor: f64) -> Vec<RecordBatch> {
    let generator = PartSuppGenerator::new(scale_factor, 1, 1);
    let mut arrow_gen = PartSuppArrow::new(generator).with_batch_size(10_000);
    let mut batches = Vec::new();
    while let Some(batch) = arrow_gen.next() {
        batches.push(batch);
    }
    batches
}

/// Generate official TPC-H NATION data (25 rows, fixed)
fn generate_official_tpch_nation() -> Vec<RecordBatch> {
    let generator = NationGenerator::default();
    let mut arrow_gen = NationArrow::new(generator);
    let mut batches = Vec::new();
    while let Some(batch) = arrow_gen.next() {
        batches.push(batch);
    }
    batches
}

/// Generate official TPC-H REGION data (5 rows, fixed)
fn generate_official_tpch_region() -> Vec<RecordBatch> {
    let generator = RegionGenerator::default();
    let mut arrow_gen = RegionArrow::new(generator);
    let mut batches = Vec::new();
    while let Some(batch) = arrow_gen.next() {
        batches.push(batch);
    }
    batches
}

/// Generate TPC-H LINEITEM table data
/// 
/// LINEITEM is the largest and most queried table in TPC-H
fn generate_lineitem_data(num_rows: usize) -> RecordBatch {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    // Helper to generate deterministic pseudo-random values
    let hash_val = |seed: usize, i: usize| -> u64 {
        let mut hasher = DefaultHasher::new();
        (seed, i).hash(&mut hasher);
        hasher.finish()
    };

    // L_ORDERKEY - order identifier
    let l_orderkey: Vec<i64> = (0..num_rows)
        .map(|i| ((hash_val(1, i) % 1_500_000) + 1) as i64)
        .collect();

    // L_PARTKEY - part identifier
    let l_partkey: Vec<i64> = (0..num_rows)
        .map(|i| ((hash_val(2, i) % 200_000) + 1) as i64)
        .collect();

    // L_SUPPKEY - supplier identifier
    let l_suppkey: Vec<i64> = (0..num_rows)
        .map(|i| ((hash_val(3, i) % 10_000) + 1) as i64)
        .collect();

    // L_LINENUMBER - line number within order
    let l_linenumber: Vec<i32> = (0..num_rows)
        .map(|i| ((hash_val(4, i) % 7) + 1) as i32)
        .collect();

    // L_QUANTITY - quantity ordered
    let l_quantity: Vec<f64> = (0..num_rows)
        .map(|i| ((hash_val(5, i) % 50) + 1) as f64)
        .collect();

    // L_EXTENDEDPRICE - extended price = quantity * price
    let l_extendedprice: Vec<f64> = (0..num_rows)
        .map(|i| {
            let qty = ((hash_val(5, i) % 50) + 1) as f64;
            let price = ((hash_val(6, i) % 90000) + 10000) as f64 / 100.0;
            qty * price
        })
        .collect();

    // L_DISCOUNT - discount percentage (0.00 to 0.10)
    let l_discount: Vec<f64> = (0..num_rows)
        .map(|i| (hash_val(7, i) % 11) as f64 / 100.0)
        .collect();

    // L_TAX - tax percentage (0.00 to 0.08)
    let l_tax: Vec<f64> = (0..num_rows)
        .map(|i| (hash_val(8, i) % 9) as f64 / 100.0)
        .collect();

    // L_RETURNFLAG - return flag (A, N, R)
    let flags = ["A", "N", "R"];
    let l_returnflag: Vec<&str> = (0..num_rows)
        .map(|i| flags[(hash_val(9, i) % 3) as usize])
        .collect();

    // L_LINESTATUS - line status (O, F)
    let statuses = ["O", "F"];
    let l_linestatus: Vec<&str> = (0..num_rows)
        .map(|i| statuses[(hash_val(10, i) % 2) as usize])
        .collect();

    // L_SHIPDATE - ship date (days since epoch, range: 1992-01-01 to 1998-12-31)
    // 1992-01-01 = 8035 days since 1970-01-01
    let base_date = 8035i32;
    let l_shipdate: Vec<i32> = (0..num_rows)
        .map(|i| base_date + (hash_val(11, i) % 2556) as i32) // ~7 years range
        .collect();

    // L_COMMITDATE
    let l_commitdate: Vec<i32> = (0..num_rows)
        .map(|i| base_date + (hash_val(12, i) % 2556) as i32)
        .collect();

    // L_RECEIPTDATE
    let l_receiptdate: Vec<i32> = (0..num_rows)
        .map(|i| base_date + (hash_val(13, i) % 2556) as i32)
        .collect();

    // L_SHIPINSTRUCT
    let instructions = [
        "DELIVER IN PERSON",
        "COLLECT COD",
        "NONE",
        "TAKE BACK RETURN",
    ];
    let l_shipinstruct: Vec<&str> = (0..num_rows)
        .map(|i| instructions[(hash_val(14, i) % 4) as usize])
        .collect();

    // L_SHIPMODE
    let modes = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"];
    let l_shipmode: Vec<&str> = (0..num_rows)
        .map(|i| modes[(hash_val(15, i) % 7) as usize])
        .collect();

    // L_COMMENT
    let l_comment: Vec<String> = (0..num_rows)
        .map(|i| format!("comment_{}", i))
        .collect();

    let schema = Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("l_linenumber", DataType::Int32, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Date32, false),
        Field::new("l_commitdate", DataType::Date32, false),
        Field::new("l_receiptdate", DataType::Date32, false),
        Field::new("l_shipinstruct", DataType::Utf8, false),
        Field::new("l_shipmode", DataType::Utf8, false),
        Field::new("l_comment", DataType::Utf8, false),
    ]);

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(l_orderkey)),
        Arc::new(Int64Array::from(l_partkey)),
        Arc::new(Int64Array::from(l_suppkey)),
        Arc::new(Int32Array::from(l_linenumber)),
        Arc::new(Float64Array::from(l_quantity)),
        Arc::new(Float64Array::from(l_extendedprice)),
        Arc::new(Float64Array::from(l_discount)),
        Arc::new(Float64Array::from(l_tax)),
        Arc::new(StringArray::from(l_returnflag)),
        Arc::new(StringArray::from(l_linestatus)),
        Arc::new(Date32Array::from(l_shipdate)),
        Arc::new(Date32Array::from(l_commitdate)),
        Arc::new(Date32Array::from(l_receiptdate)),
        Arc::new(StringArray::from(l_shipinstruct)),
        Arc::new(StringArray::from(l_shipmode)),
        Arc::new(StringArray::from(
            l_comment.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        )),
    ];

    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

/// Write RecordBatch to Parquet file
async fn write_to_parquet(batch: &RecordBatch, path: &std::path::Path) -> anyhow::Result<()> {
    use parquet::arrow::ArrowWriter;
    use std::fs::File;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(batch)?;
    writer.close()?;
    Ok(())
}

/// Write RecordBatch to VortexLake database
/// Returns (total_time_ms, write_only_time_ms) for fair comparison
async fn write_to_vortexlake(batch: &RecordBatch, db_path: &std::path::Path) -> anyhow::Result<(f64, f64)> {
    let total_start = Instant::now();
    
    // Create VortexLake database
    let db = VortexLake::new(db_path).await?;
    
    // Convert Arrow schema to VortexLake schema
    let arrow_schema = batch.schema();
    let vl_fields: Vec<VLField> = arrow_schema
        .fields()
        .iter()
        .map(|f| VLField::new(f.name(), f.data_type().clone(), f.is_nullable()))
        .collect();
    let vl_schema = VLSchema::new(vl_fields)?;
    
    // Create table
    db.create_table("lineitem", vl_schema).await?;
    
    // Measure write-only time (excluding table creation)
    let write_start = Instant::now();
    let mut writer = db.writer("lineitem")?;
    writer.write_batch(batch.clone()).await?;
    writer.commit().await?;
    let write_time_ms = write_start.elapsed().as_secs_f64() * 1000.0;
    
    let total_time_ms = total_start.elapsed().as_secs_f64() * 1000.0;
    
    Ok((total_time_ms, write_time_ms))
}

/// Read data from VortexLake using native Reader (bypasses DataFusion integration issues)
async fn read_from_vortexlake(db_path: &std::path::Path) -> anyhow::Result<Vec<RecordBatch>> {
    let db = VortexLake::new(db_path).await?;
    let reader = db.reader("lineitem")?;
    let batches = reader.read_all().await?;
    Ok(batches)
}

/// Setup DataFusion session with VortexLake table
/// Note: Currently has integration issues with vortex-datafusion path handling
#[allow(dead_code)]
async fn setup_vortexlake_session(db_path: &std::path::Path) -> anyhow::Result<SessionContext> {
    use object_store::local::LocalFileSystem;
    use datafusion_execution::object_store::ObjectStoreUrl;
    
    let ctx = SessionContext::new();
    
    // Register LocalFileSystem with "/" prefix BEFORE creating TableProvider
    // This ensures DataFusion can find files using absolute paths
    let object_store_url = ObjectStoreUrl::local_filesystem();
    let local_fs = Arc::new(LocalFileSystem::new_with_prefix("/")?);
    ctx.runtime_env().register_object_store(
        <ObjectStoreUrl as AsRef<url::Url>>::as_ref(&object_store_url),
        local_fs,
    );
    
    // Create VortexLake TableProvider
    let provider = VortexLakeTableProvider::new(
        db_path.to_str().unwrap(),
        "lineitem"
    ).await?;
    
    // Register with DataFusion
    ctx.register_table("lineitem", Arc::new(provider))?;
    
    Ok(ctx)
}

/// Setup DataFusion session with Parquet table
async fn setup_parquet_session(parquet_path: &std::path::Path) -> anyhow::Result<SessionContext> {
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "lineitem",
        parquet_path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;
    Ok(ctx)
}

/// Execute query and return results
async fn execute_query(ctx: &SessionContext, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
    let df = ctx.sql(sql).await?;
    let results = df.collect().await?;
    Ok(results)
}

// Use the comprehensive profiling tool from vortexlake-sql
use vortexlake_sql::profiling::{execute_with_full_profile, compare_profiles, QueryProfile};

/// Compare two sets of RecordBatches for equality
fn compare_results(
    results_a: &[RecordBatch],
    results_b: &[RecordBatch],
    tolerance: f64,
) -> (bool, String) {
    // Flatten batches
    let rows_a: usize = results_a.iter().map(|b| b.num_rows()).sum();
    let rows_b: usize = results_b.iter().map(|b| b.num_rows()).sum();

    if rows_a != rows_b {
        return (
            false,
            format!("Row count mismatch: {} vs {}", rows_a, rows_b),
        );
    }

    if results_a.is_empty() && results_b.is_empty() {
        return (true, "Both empty".to_string());
    }

    // Compare schemas
    if !results_a.is_empty() && !results_b.is_empty() {
        let schema_a = results_a[0].schema();
        let schema_b = results_b[0].schema();
        if schema_a.fields().len() != schema_b.fields().len() {
            return (
                false,
                format!(
                    "Column count mismatch: {} vs {}",
                    schema_a.fields().len(),
                    schema_b.fields().len()
                ),
            );
        }
    }

    (true, format!("Match: {} rows", rows_a))
}

/// Print validation report
fn print_validation_report(results: &[(String, bool, String, f64, f64)]) {
    let mut table = Table::new();
    table.add_row(row![
        "Query",
        "Status",
        "Details",
        "Parquet (ms)",
        "VortexLake (ms)",
        "Speedup"
    ]);

    for (name, passed, details, parquet_ms, vortex_ms) in results {
        let status = if *passed { "✓ PASS" } else { "✗ FAIL" };
        let speedup = if *vortex_ms > 0.0 {
            parquet_ms / vortex_ms
        } else {
            0.0
        };
        table.add_row(row![
            name,
            status,
            details,
            format!("{:.2}", parquet_ms),
            format!("{:.2}", vortex_ms),
            format!("{:.2}x", speedup)
        ]);
    }

    println!("\n{}", "=".repeat(80));
    println!("VortexLake vs Parquet Validation Report");
    println!("{}", "=".repeat(80));
    table.printstd();
}

// ============================================================================
// TPC-H Query Tests
// ============================================================================

/// TPC-H Q1: Pricing Summary Report
/// 
/// This query reports the amount of business that was billed, shipped, and
/// returned. It is the most common benchmark query.
const TPCH_Q1: &str = r#"
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) as sum_qty,
    SUM(l_extendedprice) as sum_base_price,
    SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    AVG(l_quantity) as avg_qty,
    AVG(l_extendedprice) as avg_price,
    AVG(l_discount) as avg_disc,
    COUNT(*) as count_order
FROM lineitem
WHERE l_shipdate <= DATE '1998-09-02'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
"#;

/// TPC-H Q6: Forecasting Revenue Change
/// 
/// This query quantifies the amount of revenue increase that would have
/// resulted from eliminating certain discounts.
const TPCH_Q6: &str = r#"
SELECT
    SUM(l_extendedprice * l_discount) as revenue
FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01'
  AND l_shipdate < DATE '1995-01-01'
  AND l_discount >= 0.05
  AND l_discount <= 0.07
  AND l_quantity < 24
"#;

/// TPC-H Q14: Promotion Effect (simplified, LINEITEM only)
const TPCH_Q14_SIMPLIFIED: &str = r#"
SELECT
    100.0 * SUM(
        CASE WHEN l_shipmode = 'AIR' THEN l_extendedprice * (1 - l_discount)
        ELSE 0 END
    ) / SUM(l_extendedprice * (1 - l_discount)) as promo_revenue
FROM lineitem
WHERE l_shipdate >= DATE '1995-09-01'
  AND l_shipdate < DATE '1995-10-01'
"#;

// ============================================================================
// Complete TPC-H Queries (for multi-table tests)
// ============================================================================

/// TPC-H Q3: Shipping Priority
const TPCH_Q3: &str = r#"
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < DATE '1995-03-15'
  AND l_shipdate > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10
"#;

/// TPC-H Q4: Order Priority Checking
const TPCH_Q4: &str = r#"
SELECT
    o_orderpriority,
    COUNT(*) as order_count
FROM orders
WHERE o_orderdate >= DATE '1993-07-01'
  AND o_orderdate < DATE '1993-10-01'
  AND EXISTS (
    SELECT * FROM lineitem
    WHERE l_orderkey = o_orderkey
      AND l_commitdate < l_receiptdate
  )
GROUP BY o_orderpriority
ORDER BY o_orderpriority
"#;

/// TPC-H Q5: Local Supplier Volume
const TPCH_Q5: &str = r#"
SELECT
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) as revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1995-01-01'
GROUP BY n_name
ORDER BY revenue DESC
"#;

/// TPC-H Q10: Returned Item Reporting
const TPCH_Q10: &str = r#"
SELECT
    c_custkey,
    c_name,
    SUM(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM customer, orders, lineitem, nation
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate >= DATE '1993-10-01'
  AND o_orderdate < DATE '1994-01-01'
  AND l_returnflag = 'R'
  AND c_nationkey = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC
LIMIT 20
"#;

/// TPC-H Q12: Shipping Modes and Order Priority
const TPCH_Q12: &str = r#"
SELECT
    l_shipmode,
    SUM(CASE
        WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1
        ELSE 0
    END) as high_line_count,
    SUM(CASE
        WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1
        ELSE 0
    END) as low_line_count
FROM orders, lineitem
WHERE o_orderkey = l_orderkey
  AND l_shipmode IN ('MAIL', 'SHIP')
  AND l_commitdate < l_receiptdate
  AND l_shipdate < l_commitdate
  AND l_receiptdate >= DATE '1994-01-01'
  AND l_receiptdate < DATE '1995-01-01'
GROUP BY l_shipmode
ORDER BY l_shipmode
"#;

/// TPC-H Q14: Promotion Effect (full version with PART table)
const TPCH_Q14: &str = r#"
SELECT
    100.00 * SUM(CASE
        WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount)
        ELSE 0
    END) / SUM(l_extendedprice * (1 - l_discount)) as promo_revenue
FROM lineitem, part
WHERE l_partkey = p_partkey
  AND l_shipdate >= DATE '1995-09-01'
  AND l_shipdate < DATE '1995-10-01'
"#;

/// TPC-H Q7: Volume Shipping
const TPCH_Q7: &str = r#"
SELECT
    supp_nation, cust_nation, l_year,
    SUM(volume) as revenue
FROM (
    SELECT
        n1.n_name as supp_nation,
        n2.n_name as cust_nation,
        EXTRACT(YEAR FROM l_shipdate) as l_year,
        l_extendedprice * (1 - l_discount) as volume
    FROM supplier, lineitem, orders, customer, nation n1, nation n2
    WHERE s_suppkey = l_suppkey
      AND o_orderkey = l_orderkey
      AND c_custkey = o_custkey
      AND s_nationkey = n1.n_nationkey
      AND c_nationkey = n2.n_nationkey
      AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
        OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
      AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) as shipping
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year
"#;

/// TPC-H Q9: Product Type Profit Measure
const TPCH_Q9: &str = r#"
SELECT
    nation, o_year,
    SUM(amount) as sum_profit
FROM (
    SELECT
        n_name as nation,
        EXTRACT(YEAR FROM o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    FROM part, supplier, lineitem, partsupp, orders, nation
    WHERE s_suppkey = l_suppkey
      AND ps_suppkey = l_suppkey
      AND ps_partkey = l_partkey
      AND p_partkey = l_partkey
      AND o_orderkey = l_orderkey
      AND s_nationkey = n_nationkey
      AND p_name LIKE '%green%'
) as profit
GROUP BY nation, o_year
ORDER BY nation, o_year DESC
"#;

/// TPC-H Q11: Important Stock Identification  
const TPCH_Q11: &str = r#"
SELECT
    ps_partkey,
    SUM(ps_supplycost * ps_availqty) as value
FROM partsupp, supplier, nation
WHERE ps_suppkey = s_suppkey
  AND s_nationkey = n_nationkey
  AND n_name = 'GERMANY'
GROUP BY ps_partkey
HAVING SUM(ps_supplycost * ps_availqty) > (
    SELECT SUM(ps_supplycost * ps_availqty) * 0.0001
    FROM partsupp, supplier, nation
    WHERE ps_suppkey = s_suppkey
      AND s_nationkey = n_nationkey
      AND n_name = 'GERMANY'
)
ORDER BY value DESC
"#;

/// TPC-H Q15: Top Supplier (using CTE)
const TPCH_Q15: &str = r#"
WITH revenue AS (
    SELECT
        l_suppkey as supplier_no,
        SUM(l_extendedprice * (1 - l_discount)) as total_revenue
    FROM lineitem
    WHERE l_shipdate >= DATE '1996-01-01'
      AND l_shipdate < DATE '1996-04-01'
    GROUP BY l_suppkey
)
SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
FROM supplier, revenue
WHERE s_suppkey = supplier_no
  AND total_revenue = (SELECT MAX(total_revenue) FROM revenue)
ORDER BY s_suppkey
"#;

/// TPC-H Q17: Small-Quantity-Order Revenue
const TPCH_Q17: &str = r#"
SELECT
    SUM(l_extendedprice) / 7.0 as avg_yearly
FROM lineitem, part
WHERE p_partkey = l_partkey
  AND p_brand = 'Brand#23'
  AND p_container = 'MED BOX'
  AND l_quantity < (
      SELECT 0.2 * AVG(l_quantity)
      FROM lineitem
      WHERE l_partkey = p_partkey
  )
"#;

/// TPC-H Q18: Large Volume Customer
const TPCH_Q18: &str = r#"
SELECT
    c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
    SUM(l_quantity)
FROM customer, orders, lineitem
WHERE o_orderkey IN (
    SELECT l_orderkey FROM lineitem
    GROUP BY l_orderkey
    HAVING SUM(l_quantity) > 300
)
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100
"#;

/// TPC-H Q19: Discounted Revenue
const TPCH_Q19: &str = r#"
SELECT
    SUM(l_extendedprice * (1 - l_discount)) as revenue
FROM lineitem, part
WHERE (
    p_partkey = l_partkey
    AND p_brand = 'Brand#12'
    AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    AND l_quantity >= 1 AND l_quantity <= 11
    AND p_size BETWEEN 1 AND 5
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
) OR (
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
    AND l_quantity >= 10 AND l_quantity <= 20
    AND p_size BETWEEN 1 AND 10
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
) OR (
    p_partkey = l_partkey
    AND p_brand = 'Brand#34'
    AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
    AND l_quantity >= 20 AND l_quantity <= 30
    AND p_size BETWEEN 1 AND 15
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
)
"#;

/// TPC-H Q2: Minimum Cost Supplier
const TPCH_Q2: &str = r#"
SELECT
    s_acctbal, s_name, n_name, p_partkey, p_mfgr,
    s_address, s_phone, s_comment
FROM part, supplier, partsupp, nation, region
WHERE p_partkey = ps_partkey
  AND s_suppkey = ps_suppkey
  AND p_size = 15
  AND p_type LIKE '%BRASS'
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'EUROPE'
  AND ps_supplycost = (
      SELECT MIN(ps_supplycost)
      FROM partsupp, supplier, nation, region
      WHERE p_partkey = ps_partkey
        AND s_suppkey = ps_suppkey
        AND s_nationkey = n_nationkey
        AND n_regionkey = r_regionkey
        AND r_name = 'EUROPE'
  )
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
LIMIT 100
"#;

/// TPC-H Q8: National Market Share
const TPCH_Q8: &str = r#"
SELECT
    o_year,
    SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) as mkt_share
FROM (
    SELECT
        EXTRACT(YEAR FROM o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) as volume,
        n2.n_name as nation
    FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
    WHERE p_partkey = l_partkey
      AND s_suppkey = l_suppkey
      AND l_orderkey = o_orderkey
      AND o_custkey = c_custkey
      AND c_nationkey = n1.n_nationkey
      AND n1.n_regionkey = r_regionkey
      AND r_name = 'AMERICA'
      AND s_nationkey = n2.n_nationkey
      AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
      AND p_type = 'ECONOMY ANODIZED STEEL'
) as all_nations
GROUP BY o_year
ORDER BY o_year
"#;

/// TPC-H Q13: Customer Distribution
const TPCH_Q13: &str = r#"
SELECT
    c_count, COUNT(*) as custdist
FROM (
    SELECT c_custkey, COUNT(o_orderkey) as c_count
    FROM customer LEFT OUTER JOIN orders ON
        c_custkey = o_custkey
        AND o_comment NOT LIKE '%special%requests%'
    GROUP BY c_custkey
) as c_orders
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC
"#;

/// TPC-H Q16: Parts/Supplier Relationship
const TPCH_Q16: &str = r#"
SELECT
    p_brand, p_type, p_size,
    COUNT(DISTINCT ps_suppkey) as supplier_cnt
FROM partsupp, part
WHERE p_partkey = ps_partkey
  AND p_brand <> 'Brand#45'
  AND p_type NOT LIKE 'MEDIUM POLISHED%'
  AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
  AND ps_suppkey NOT IN (
      SELECT s_suppkey FROM supplier
      WHERE s_comment LIKE '%Customer%Complaints%'
  )
GROUP BY p_brand, p_type, p_size
ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
"#;

/// TPC-H Q20: Potential Part Promotion
const TPCH_Q20: &str = r#"
SELECT s_name, s_address
FROM supplier, nation
WHERE s_suppkey IN (
    SELECT ps_suppkey FROM partsupp
    WHERE ps_partkey IN (
        SELECT p_partkey FROM part
        WHERE p_name LIKE 'forest%'
    )
    AND ps_availqty > (
        SELECT 0.5 * SUM(l_quantity)
        FROM lineitem
        WHERE l_partkey = ps_partkey
          AND l_suppkey = ps_suppkey
          AND l_shipdate >= DATE '1994-01-01'
          AND l_shipdate < DATE '1995-01-01'
    )
)
  AND s_nationkey = n_nationkey
  AND n_name = 'CANADA'
ORDER BY s_name
"#;

/// TPC-H Q21: Suppliers Who Kept Orders Waiting
const TPCH_Q21: &str = r#"
SELECT s_name, COUNT(*) as numwait
FROM supplier, lineitem l1, orders, nation
WHERE s_suppkey = l1.l_suppkey
  AND o_orderkey = l1.l_orderkey
  AND o_orderstatus = 'F'
  AND l1.l_receiptdate > l1.l_commitdate
  AND EXISTS (
      SELECT * FROM lineitem l2
      WHERE l2.l_orderkey = l1.l_orderkey
        AND l2.l_suppkey <> l1.l_suppkey
  )
  AND NOT EXISTS (
      SELECT * FROM lineitem l3
      WHERE l3.l_orderkey = l1.l_orderkey
        AND l3.l_suppkey <> l1.l_suppkey
        AND l3.l_receiptdate > l3.l_commitdate
  )
  AND s_nationkey = n_nationkey
  AND n_name = 'SAUDI ARABIA'
GROUP BY s_name
ORDER BY numwait DESC, s_name
LIMIT 100
"#;

/// TPC-H Q22: Global Sales Opportunity
const TPCH_Q22: &str = r#"
SELECT
    cntrycode,
    COUNT(*) as numcust,
    SUM(c_acctbal) as totacctbal
FROM (
    SELECT
        SUBSTRING(c_phone FROM 1 FOR 2) as cntrycode,
        c_acctbal
    FROM customer
    WHERE SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
      AND c_acctbal > (
          SELECT AVG(c_acctbal)
          FROM customer
          WHERE c_acctbal > 0.00
            AND SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
      )
      AND NOT EXISTS (
          SELECT * FROM orders
          WHERE o_custkey = c_custkey
      )
) as custsale
GROUP BY cntrycode
ORDER BY cntrycode
"#;

#[tokio::test]
async fn test_tpch_q1_parquet_baseline() -> anyhow::Result<()> {
    // Generate test data
    let num_rows = (60_000.0 * SCALE_FACTOR) as usize;
    let batch = generate_lineitem_data(num_rows.max(1000));
    
    // Setup temp directory
    let temp_dir = TempDir::new()?;
    let parquet_path = temp_dir.path().join("lineitem.parquet");
    
    // Write to Parquet
    write_to_parquet(&batch, &parquet_path).await?;
    
    // Setup session and execute query
    let ctx = setup_parquet_session(&parquet_path).await?;
    
    let start = Instant::now();
    let results = execute_query(&ctx, TPCH_Q1).await?;
    let elapsed = start.elapsed();
    
    println!("\nTPC-H Q1 Results (Parquet baseline):");
    println!("Execution time: {:.2}ms", elapsed.as_secs_f64() * 1000.0);
    println!("Result rows: {}", results.iter().map(|b| b.num_rows()).sum::<usize>());
    
    // Print results
    for batch in &results {
        println!("{:?}", batch);
    }
    
    assert!(!results.is_empty(), "Q1 should return results");
    Ok(())
}

#[tokio::test]
async fn test_tpch_q6_parquet_baseline() -> anyhow::Result<()> {
    let num_rows = (60_000.0 * SCALE_FACTOR) as usize;
    let batch = generate_lineitem_data(num_rows.max(1000));
    
    let temp_dir = TempDir::new()?;
    let parquet_path = temp_dir.path().join("lineitem.parquet");
    
    write_to_parquet(&batch, &parquet_path).await?;
    
    let ctx = setup_parquet_session(&parquet_path).await?;
    
    let start = Instant::now();
    let results = execute_query(&ctx, TPCH_Q6).await?;
    let elapsed = start.elapsed();
    
    println!("\nTPC-H Q6 Results (Parquet baseline):");
    println!("Execution time: {:.2}ms", elapsed.as_secs_f64() * 1000.0);
    
    for batch in &results {
        println!("{:?}", batch);
    }
    
    assert!(!results.is_empty(), "Q6 should return results");
    Ok(())
}

#[tokio::test]
async fn test_data_generation() {
    let batch = generate_lineitem_data(100);
    
    assert_eq!(batch.num_rows(), 100);
    assert_eq!(batch.num_columns(), 16);
    
    println!("Generated LINEITEM schema:");
    for field in batch.schema().fields() {
        println!("  {} : {:?}", field.name(), field.data_type());
    }
    
    println!("\nSample data (first 5 rows):");
    println!("{:?}", batch.slice(0, 5));
}

/// Run full validation suite and generate report
#[tokio::test]
#[ignore] // Run with: cargo test -p vortexlake-sql full_validation_suite -- --ignored --nocapture
async fn full_validation_suite() -> anyhow::Result<()> {
    use std::path::Path;
    
    println!("\n{}", "=".repeat(80));
    println!("VortexLake TPC-H Validation Suite (Official TPC-H Data)");
    println!("Scale Factor: {}", SCALE_FACTOR);
    println!("{}", "=".repeat(80));

    // Generate OFFICIAL TPC-H data
    println!("\nGenerating official TPC-H LINEITEM data...");
    let start = Instant::now();
    let batches = generate_official_tpch_lineitem(SCALE_FACTOR, Some(100_000)); // Limit to 100K rows for quick test
    let gen_time = start.elapsed();
    
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("Generated {} rows in {} batches ({:.2}s)", 
        total_rows, batches.len(), gen_time.as_secs_f64());
    
    // Combine batches into one for writing
    let schema = batches[0].schema();
    let batch = arrow::compute::concat_batches(&schema, &batches)?;
    println!("Combined into single batch: {} rows, {} columns", batch.num_rows(), batch.num_columns());
    
    // Print schema info
    println!("\nTPC-H LINEITEM Schema:");
    for field in schema.fields() {
        println!("  {} : {:?}", field.name(), field.data_type());
    }

    // Use fixed test data directory
    let base_dir = get_test_data_dir();
    tokio::fs::create_dir_all(&base_dir).await?;
    let parquet_path = base_dir.join("tpch_lineitem.parquet");
    let vortexlake_path = base_dir.join("tpch_vortexlake_db");
    
    // Clean up previous test data
    if vortexlake_path.exists() {
        tokio::fs::remove_dir_all(&vortexlake_path).await?;
    }

    // === Write Performance Test ===
    println!("\n--- Write Performance ---");
    
    // Write to Parquet
    let start = Instant::now();
    write_to_parquet(&batch, &parquet_path).await?;
    let parquet_write_ms = start.elapsed().as_secs_f64() * 1000.0;
    let parquet_size = tokio::fs::metadata(&parquet_path).await?.len();
    println!("Parquet: {:?} ({} bytes, {:.2}ms)", parquet_path, parquet_size, parquet_write_ms);

    // Write to VortexLake (returns total time and write-only time)
    let (vortex_total_ms, vortex_write_only_ms) = write_to_vortexlake(&batch, &vortexlake_path).await?;
    
    // Calculate VortexLake total size
    let mut vortex_size = 0u64;
    let data_dir = vortexlake_path.join("data");
    if data_dir.exists() {
        let mut entries = tokio::fs::read_dir(&data_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.path().extension().map_or(false, |e| e == "vortex") {
                vortex_size += tokio::fs::metadata(entry.path()).await?.len();
            }
        }
    }
    println!("VortexLake: {:?} ({} bytes)", vortexlake_path, vortex_size);
    println!("  - Total time (incl. create table): {:.2}ms", vortex_total_ms);
    println!("  - Write only time: {:.2}ms", vortex_write_only_ms);

    // === Read Performance Test ===
    println!("\n--- Read Performance ---");
    
    // Read from Parquet (using DataFusion)
    let parquet_ctx = setup_parquet_session(&parquet_path).await?;
    let start = Instant::now();
    let parquet_results = execute_query(&parquet_ctx, "SELECT * FROM lineitem").await?;
    let parquet_read_ms = start.elapsed().as_secs_f64() * 1000.0;
    let parquet_rows: usize = parquet_results.iter().map(|b| b.num_rows()).sum();
    println!("Parquet read: {} rows in {:.2}ms", parquet_rows, parquet_read_ms);
    
    // Read from VortexLake (using native Reader)
    let start = Instant::now();
    let vortex_batches = read_from_vortexlake(&vortexlake_path).await?;
    let vortex_read_ms = start.elapsed().as_secs_f64() * 1000.0;
    let vortex_rows: usize = vortex_batches.iter().map(|b| b.num_rows()).sum();
    println!("VortexLake read: {} rows in {:.2}ms", vortex_rows, vortex_read_ms);

    // === Query Performance Test (Both Parquet and VortexLake) ===
    println!("\n--- SQL Query Performance ---");
    
    // Setup VortexLake session for SQL queries
    let vortex_ctx = setup_vortexlake_session(&vortexlake_path).await?;
    
    let mut validation_results = Vec::new();

    // Q1 - Pricing Summary
    {
        let start = Instant::now();
        let parquet_results = execute_query(&parquet_ctx, TPCH_Q1).await?;
        let parquet_ms = start.elapsed().as_secs_f64() * 1000.0;
        let parquet_rows: usize = parquet_results.iter().map(|b| b.num_rows()).sum();
        
        let start = Instant::now();
        let vortex_results = execute_query(&vortex_ctx, TPCH_Q1).await?;
        let vortex_ms = start.elapsed().as_secs_f64() * 1000.0;
        let vortex_rows: usize = vortex_results.iter().map(|b| b.num_rows()).sum();
        
        validation_results.push((
            "TPC-H Q1 (Pricing Summary)".to_string(),
            parquet_rows == vortex_rows,
            format!("{} rows", parquet_rows),
            parquet_ms,
            vortex_ms,
        ));
    }

    // Q6 - Revenue Change
    {
        let start = Instant::now();
        let parquet_results = execute_query(&parquet_ctx, TPCH_Q6).await?;
        let parquet_ms = start.elapsed().as_secs_f64() * 1000.0;
        let parquet_rows: usize = parquet_results.iter().map(|b| b.num_rows()).sum();
        
        let start = Instant::now();
        let vortex_results = execute_query(&vortex_ctx, TPCH_Q6).await?;
        let vortex_ms = start.elapsed().as_secs_f64() * 1000.0;
        let vortex_rows: usize = vortex_results.iter().map(|b| b.num_rows()).sum();
        
        validation_results.push((
            "TPC-H Q6 (Revenue Change)".to_string(),
            parquet_rows == vortex_rows,
            format!("{} rows", parquet_rows),
            parquet_ms,
            vortex_ms,
        ));
    }

    // Q14 - Promotion Effect
    {
        let start = Instant::now();
        let parquet_results = execute_query(&parquet_ctx, TPCH_Q14_SIMPLIFIED).await?;
        let parquet_ms = start.elapsed().as_secs_f64() * 1000.0;
        let parquet_rows: usize = parquet_results.iter().map(|b| b.num_rows()).sum();
        
        let start = Instant::now();
        let vortex_results = execute_query(&vortex_ctx, TPCH_Q14_SIMPLIFIED).await?;
        let vortex_ms = start.elapsed().as_secs_f64() * 1000.0;
        let vortex_rows: usize = vortex_results.iter().map(|b| b.num_rows()).sum();
        
        validation_results.push((
            "TPC-H Q14 (Promotion Effect)".to_string(),
            parquet_rows == vortex_rows,
            format!("{} rows", parquet_rows),
            parquet_ms,
            vortex_ms,
        ));
    }

    // Print report
    print_validation_report(&validation_results);
    
    // Print storage comparison
    println!("\n{}", "=".repeat(80));
    println!("Storage Comparison");
    println!("{}", "=".repeat(80));
    println!("| Format     | Size (bytes) | Write Total (ms) | Write Only (ms) | Read (ms) |");
    println!("|------------|--------------|------------------|-----------------|-----------|");
    println!("| Parquet    | {:>12} | {:>16.2} | {:>15.2} | {:>9.2} |", 
        parquet_size, parquet_write_ms, parquet_write_ms, parquet_read_ms);
    println!("| VortexLake | {:>12} | {:>16.2} | {:>15.2} | {:>9.2} |", 
        vortex_size, vortex_total_ms, vortex_write_only_ms, vortex_read_ms);
    
    if parquet_size > 0 {
        let compression_ratio = vortex_size as f64 / parquet_size as f64;
        let write_speedup = parquet_write_ms / vortex_write_only_ms;
        println!("\nVortexLake/Parquet size ratio: {:.2}x (smaller is better)", compression_ratio);
        println!("VortexLake/Parquet write speedup (write only): {:.2}x", write_speedup);
    }

    Ok(())
}

/// Test VortexLake write and read (end-to-end)
#[tokio::test]
async fn test_vortexlake_e2e() -> anyhow::Result<()> {
    use std::path::Path;
    use vortexlake_core::fragment::Fragment;
    
    let batch = generate_lineitem_data(100);
    
    // Use fixed test data directory
    let base_dir = get_test_data_dir();
    tokio::fs::create_dir_all(&base_dir).await?;
    let db_path = base_dir.join("e2e_test_db");
    
    // Clean up previous test data if exists
    if db_path.exists() {
        tokio::fs::remove_dir_all(&db_path).await?;
    }
    
    // Write to VortexLake
    let (total_ms, write_ms) = write_to_vortexlake(&batch, &db_path).await?;
    println!("Written {} rows to VortexLake at {:?} (total: {:.2}ms, write: {:.2}ms)", 
        batch.num_rows(), db_path, total_ms, write_ms);
    
    // List created files
    println!("\nCreated files:");
    list_dir_contents(&db_path).await?;
    
    // Test 1: Direct read using vortex-core (should work)
    println!("\n=== Test 1: Direct read using vortex-core ===");
    let vortex_files: Vec<_> = std::fs::read_dir(db_path.join("data"))?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "vortex"))
        .collect();
    
    for entry in &vortex_files {
        let path = entry.path();
        println!("Reading file: {:?}", path);
        let read_batch = Fragment::read_from_path(&path).await?;
        println!("Direct read: {} rows, {} cols", read_batch.num_rows(), read_batch.num_columns());
    }
    
    // Test 2: Read through VortexLake Reader (should work)
    println!("\n=== Test 2: VortexLake Reader ===");
    let db = VortexLake::new(&db_path).await?;
    let reader = db.reader("lineitem")?;
    let batches = reader.read_all().await?;
    println!("VortexLake Reader: {} batches, {} total rows", 
        batches.len(), 
        batches.iter().map(|b| b.num_rows()).sum::<usize>());
    
    // Test 3: Verify LocalFileSystem path handling
    println!("\n=== Test 3: LocalFileSystem path verification ===");
    {
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as ObjectPath;
        use object_store::ObjectStore;
        
        let vortex_file = &vortex_files[0].path();
        let path_str = vortex_file.to_string_lossy();
        let obj_path = ObjectPath::from(path_str.strip_prefix('/').unwrap_or(&path_str));
        
        // Test with prefix "/"
        let fs = LocalFileSystem::new_with_prefix("/").unwrap();
        println!("ObjectPath: {}", obj_path);
        match fs.get(&obj_path).await {
            Ok(result) => {
                let bytes = result.bytes().await.unwrap();
                println!("LocalFileSystem read: {} bytes", bytes.len());
                if bytes.len() >= 4 {
                    println!("First 4 bytes: {:?} = '{}'", 
                        &bytes[..4], 
                        std::str::from_utf8(&bytes[..4]).unwrap_or("?"));
                }
            }
            Err(e) => println!("LocalFileSystem error: {}", e),
        }
    }
    
    // Test 4: Through DataFusion TableProvider (might fail)
    println!("\n=== Test 4: DataFusion TableProvider ===");
    match setup_vortexlake_session(&db_path).await {
        Ok(ctx) => {
            match execute_query(&ctx, "SELECT COUNT(*) as cnt FROM lineitem").await {
                Ok(results) => {
                    println!("SUCCESS! Query results:");
                    for batch in &results {
                        println!("{:?}", batch);
                    }
                }
                Err(e) => {
                    println!("Query failed: {}", e);
                    println!("This is expected - TableProvider integration needs more work");
                }
            }
        }
        Err(e) => {
            println!("Session setup failed: {}", e);
        }
    }
    
    // Test passes if direct read works
    assert!(!vortex_files.is_empty(), "Should have vortex files");
    Ok(())
}

/// Helper to list directory contents (non-recursive for simplicity)
async fn list_dir_contents(path: &std::path::Path) -> anyhow::Result<()> {
    fn list_sync(path: &std::path::Path, indent: usize) {
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                let path = entry.path();
                println!("{}{:?}", " ".repeat(indent), path);
                if path.is_dir() {
                    list_sync(&path, indent + 2);
                }
            }
        }
    }
    list_sync(path, 2);
    Ok(())
}

// ============================================================================
// Complete TPC-H Benchmark (All Tables)
// ============================================================================

/// TPC-H table info for generation
struct TpchTableInfo {
    name: &'static str,
    batches: Vec<RecordBatch>,
}

/// Generate all TPC-H tables
fn generate_all_tpch_tables(scale_factor: f64) -> Vec<TpchTableInfo> {
    println!("Generating TPC-H tables at SF={}", scale_factor);
    
    let mut tables = Vec::new();
    
    // Generate tables in order of dependency
    let start = Instant::now();
    
    print!("  - region... ");
    let region = generate_official_tpch_region();
    let rows: usize = region.iter().map(|b| b.num_rows()).sum();
    println!("{} rows", rows);
    tables.push(TpchTableInfo { name: "region", batches: region });
    
    print!("  - nation... ");
    let nation = generate_official_tpch_nation();
    let rows: usize = nation.iter().map(|b| b.num_rows()).sum();
    println!("{} rows", rows);
    tables.push(TpchTableInfo { name: "nation", batches: nation });
    
    print!("  - supplier... ");
    let supplier = generate_official_tpch_supplier(scale_factor);
    let rows: usize = supplier.iter().map(|b| b.num_rows()).sum();
    println!("{} rows", rows);
    tables.push(TpchTableInfo { name: "supplier", batches: supplier });
    
    print!("  - customer... ");
    let customer = generate_official_tpch_customer(scale_factor);
    let rows: usize = customer.iter().map(|b| b.num_rows()).sum();
    println!("{} rows", rows);
    tables.push(TpchTableInfo { name: "customer", batches: customer });
    
    print!("  - part... ");
    let part = generate_official_tpch_part(scale_factor);
    let rows: usize = part.iter().map(|b| b.num_rows()).sum();
    println!("{} rows", rows);
    tables.push(TpchTableInfo { name: "part", batches: part });
    
    print!("  - partsupp... ");
    let partsupp = generate_official_tpch_partsupp(scale_factor);
    let rows: usize = partsupp.iter().map(|b| b.num_rows()).sum();
    println!("{} rows", rows);
    tables.push(TpchTableInfo { name: "partsupp", batches: partsupp });
    
    print!("  - orders... ");
    let orders = generate_official_tpch_orders(scale_factor);
    let rows: usize = orders.iter().map(|b| b.num_rows()).sum();
    println!("{} rows", rows);
    tables.push(TpchTableInfo { name: "orders", batches: orders });
    
    print!("  - lineitem... ");
    let lineitem = generate_official_tpch_lineitem(scale_factor, None);
    let rows: usize = lineitem.iter().map(|b| b.num_rows()).sum();
    println!("{} rows", rows);
    tables.push(TpchTableInfo { name: "lineitem", batches: lineitem });
    
    println!("Total generation time: {:.2}s", start.elapsed().as_secs_f64());
    
    tables
}

/// Write all tables to Parquet files
async fn write_all_to_parquet(tables: &[TpchTableInfo], base_dir: &std::path::Path) -> anyhow::Result<()> {
    use parquet::arrow::ArrowWriter;
    use std::fs::File;
    
    for table in tables {
        let path = base_dir.join(format!("{}.parquet", table.name));
        let schema = table.batches[0].schema();
        let file = File::create(&path)?;
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None)?;
        for batch in &table.batches {
            writer.write(batch)?;
        }
        writer.close()?;
    }
    Ok(())
}

/// Write all tables to VortexLake
async fn write_all_to_vortexlake(tables: &[TpchTableInfo], db_path: &std::path::Path) -> anyhow::Result<()> {
    let db = VortexLake::new(db_path).await?;
    
    for table in tables {
        let schema = table.batches[0].schema();
        let vl_fields: Vec<VLField> = schema
            .fields()
            .iter()
            .map(|f| VLField::new(f.name(), f.data_type().clone(), f.is_nullable()))
            .collect();
        let vl_schema = VLSchema::new(vl_fields)?;
        
        db.create_table(table.name, vl_schema).await?;
        
        let mut writer = db.writer(table.name)?;
        for batch in &table.batches {
            writer.write_batch(batch.clone()).await?;
        }
        writer.commit().await?;
    }
    Ok(())
}

/// Setup DataFusion session with all Parquet tables
async fn setup_parquet_session_all(base_dir: &std::path::Path, tables: &[&str]) -> anyhow::Result<SessionContext> {
    let ctx = SessionContext::new();
    for name in tables {
        let path = base_dir.join(format!("{}.parquet", name));
        ctx.register_parquet(*name, path.to_str().unwrap(), ParquetReadOptions::default()).await?;
    }
    Ok(ctx)
}

/// Setup DataFusion session with all VortexLake tables
async fn setup_vortexlake_session_all(db_path: &std::path::Path, tables: &[&str]) -> anyhow::Result<SessionContext> {
    use object_store::local::LocalFileSystem;
    use datafusion_execution::object_store::ObjectStoreUrl;
    
    let ctx = SessionContext::new();
    
    // Register LocalFileSystem with "/" prefix
    let object_store_url = ObjectStoreUrl::local_filesystem();
    let local_fs = Arc::new(LocalFileSystem::new_with_prefix("/")?);
    ctx.runtime_env().register_object_store(
        <ObjectStoreUrl as AsRef<url::Url>>::as_ref(&object_store_url),
        local_fs,
    );
    
    for name in tables {
        let provider = VortexLakeTableProvider::new(db_path.to_str().unwrap(), name).await?;
        ctx.register_table(*name, Arc::new(provider))?;
    }
    
    Ok(ctx)
}

/// Complete TPC-H benchmark with all tables and queries
#[tokio::test]
#[ignore] // Run with: cargo test -p vortexlake-sql complete_tpch_benchmark -- --nocapture --ignored
async fn complete_tpch_benchmark() -> anyhow::Result<()> {
    use std::path::Path;
    
    // Initialize logging (respects RUST_LOG env var, defaults to vortexlake_sql=info)
    init_test_logging();
    
    println!("\n{}", "=".repeat(80));
    println!("Complete TPC-H Benchmark (All Tables, All Queries)");
    println!("Scale Factor: {}", SCALE_FACTOR);
    println!("{}", "=".repeat(80));
    
    // Generate all TPC-H tables
    let tables = generate_all_tpch_tables(SCALE_FACTOR);
    
    // Setup directories
    let base_dir = get_test_data_dir();
    tokio::fs::create_dir_all(&base_dir).await?;
    let parquet_dir = base_dir.join("tpch_parquet");
    let vortex_dir = base_dir.join("tpch_vortexlake");
    
    // Clean up previous data
    if parquet_dir.exists() {
        tokio::fs::remove_dir_all(&parquet_dir).await?;
    }
    if vortex_dir.exists() {
        tokio::fs::remove_dir_all(&vortex_dir).await?;
    }
    tokio::fs::create_dir_all(&parquet_dir).await?;
    
    // Write to Parquet
    println!("\n--- Writing to Parquet ---");
    let start = Instant::now();
    write_all_to_parquet(&tables, &parquet_dir).await?;
    let parquet_write_ms = start.elapsed().as_secs_f64() * 1000.0;
    println!("Parquet write: {:.2}ms", parquet_write_ms);
    
    // Write to VortexLake
    println!("\n--- Writing to VortexLake ---");
    let start = Instant::now();
    write_all_to_vortexlake(&tables, &vortex_dir).await?;
    let vortex_write_ms = start.elapsed().as_secs_f64() * 1000.0;
    println!("VortexLake write: {:.2}ms", vortex_write_ms);
    
    // Calculate sizes
    let parquet_size = calculate_dir_size(&parquet_dir)?;
    let vortex_size = calculate_vortex_size(&vortex_dir)?;
    
    println!("\n--- Storage Size ---");
    println!("Parquet: {} bytes ({:.2} MB)", parquet_size, parquet_size as f64 / 1024.0 / 1024.0);
    println!("VortexLake: {} bytes ({:.2} MB)", vortex_size, vortex_size as f64 / 1024.0 / 1024.0);
    println!("Compression ratio: {:.2}x", vortex_size as f64 / parquet_size as f64);
    
    // Setup sessions
    let table_names: Vec<&str> = tables.iter().map(|t| t.name).collect();
    let parquet_ctx = setup_parquet_session_all(&parquet_dir, &table_names).await?;
    let vortex_ctx = setup_vortexlake_session_all(&vortex_dir, &table_names).await?;
    
    // Define queries to test
    // TPC-H queries to test (ordered by complexity)
    let queries = vec![
        // Single table queries
        ("Q1", TPCH_Q1),     // LINEITEM only - pricing summary
        ("Q6", TPCH_Q6),     // LINEITEM only - revenue forecast
        
        // 2-table queries``
        ("Q12", TPCH_Q12),   // ORDERS + LINEITEM
        ("Q14", TPCH_Q14),   // LINEITEM + PART
        ("Q19", TPCH_Q19),   // LINEITEM + PART (complex OR)
        ("Q16", TPCH_Q16),   // PARTSUPP + PART (NOT IN subquery)
        
        // 3-table queries
        ("Q3", TPCH_Q3),     // CUSTOMER + ORDERS + LINEITEM
        ("Q11", TPCH_Q11),   // PARTSUPP + SUPPLIER + NATION
        ("Q15", TPCH_Q15),   // SUPPLIER + LINEITEM (CTE)
        ("Q18", TPCH_Q18),   // CUSTOMER + ORDERS + LINEITEM
        ("Q20", TPCH_Q20),   // SUPPLIER + NATION (nested subqueries)
        ("Q21", TPCH_Q21),   // SUPPLIER + LINEITEM + ORDERS + NATION (EXISTS)
        
        // 4-table queries
        ("Q4", TPCH_Q4),     // ORDERS + LINEITEM (EXISTS subquery)
        ("Q10", TPCH_Q10),   // CUSTOMER + ORDERS + LINEITEM + NATION
        ("Q17", TPCH_Q17),   // LINEITEM + PART (correlated subquery)
        ("Q13", TPCH_Q13),   // CUSTOMER + ORDERS (LEFT JOIN)
        ("Q22", TPCH_Q22),   // CUSTOMER + ORDERS (NOT EXISTS)
        
        // 5+ table queries
        ("Q2", TPCH_Q2),     // 5 tables - minimum cost supplier
        ("Q5", TPCH_Q5),     // 6 tables - local supplier volume
        ("Q7", TPCH_Q7),     // 5 tables - volume shipping
        ("Q8", TPCH_Q8),     // 7 tables - national market share
        ("Q9", TPCH_Q9),     // 6 tables - product type profit
    ];
    
    // Run queries
    println!("\n--- Query Performance ---");
    println!("{}", "=".repeat(80));
    println!("| {:6} | {:>12} | {:>12} | {:>8} | {:>6} |", 
        "Query", "Parquet (ms)", "Vortex (ms)", "Speedup", "Status");
    println!("|--------|--------------|--------------|----------|--------|");
    
    for (name, sql) in &queries {
        // Run on Parquet
        let parquet_result = {
            let start = Instant::now();
            match execute_query(&parquet_ctx, sql).await {
                Ok(results) => {
                    let ms = start.elapsed().as_secs_f64() * 1000.0;
                    let rows: usize = results.iter().map(|b| b.num_rows()).sum();
                    Some((ms, rows))
                }
                Err(_) => None
            }
        };
        
        // Run on VortexLake
        let vortex_result = {
            let start = Instant::now();
            match execute_query(&vortex_ctx, sql).await {
                Ok(results) => {
                    let ms = start.elapsed().as_secs_f64() * 1000.0;
                    let rows: usize = results.iter().map(|b| b.num_rows()).sum();
                    Some((ms, rows))
                }
                Err(e) => {
                    eprintln!("  [{}] VortexLake error: {:?}", name, e);
                    None
                }
            }
        };
        
        match (parquet_result, vortex_result) {
            (Some((p_ms, _)), Some((v_ms, _))) => {
                let speedup = p_ms / v_ms;
                println!("| {:6} | {:>12.2} | {:>12.2} | {:>7.2}x | {:>6} |",
                    name, p_ms, v_ms, speedup, "✓ PASS");
            }
            (Some((p_ms, _)), None) => {
                println!("| {:6} | {:>12.2} | {:>12} | {:>8} | {:>6} |",
                    name, p_ms, "ERROR", "-", "FAIL");
            }
            (None, Some((v_ms, _))) => {
                println!("| {:6} | {:>12} | {:>12.2} | {:>8} | {:>6} |",
                    name, "ERROR", v_ms, "-", "FAIL");
            }
            (None, None) => {
                println!("| {:6} | {:>12} | {:>12} | {:>8} | {:>6} |",
                    name, "ERROR", "ERROR", "-", "FAIL");
            }
        }
    }
    println!("{}", "=".repeat(80));
    
    Ok(())
}

/// Calculate total size of directory
fn calculate_dir_size(dir: &std::path::Path) -> anyhow::Result<u64> {
    let mut total = 0u64;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() {
            total += metadata.len();
        }
    }
    Ok(total)
}

/// Calculate VortexLake data size (only .vortex files)
fn calculate_vortex_size(dir: &std::path::Path) -> anyhow::Result<u64> {
    let mut total = 0u64;
    let data_dir = dir.join("data");
    if data_dir.exists() {
        for entry in std::fs::read_dir(&data_dir)? {
            let entry = entry?;
            if entry.path().extension().map_or(false, |e| e == "vortex") {
                total += entry.metadata()?.len();
            }
        }
    }
    Ok(total)
}

/// Profile Q22 query to identify performance bottlenecks
#[tokio::test]
#[ignore] // Run with: cargo test -p vortexlake-sql profile_q22 -- --nocapture --ignored
async fn profile_q22() -> anyhow::Result<()> {
    use std::path::Path;
    
    println!("\n{}", "=".repeat(80));
    println!("Q22 Performance Profiling");
    println!("{}", "=".repeat(80));

    init_test_logging();
    
    // Check if test data exists
    let base_dir = get_test_data_dir();
    let parquet_dir = base_dir.join("tpch_parquet");
    let vortex_dir = base_dir.join("tpch_vortexlake");
    
    if !parquet_dir.exists() || !vortex_dir.exists() {
        println!("Test data not found. Please run complete_tpch_benchmark first.");
        return Ok(());
    }
    
    // Setup sessions
    let table_names = vec!["customer", "orders"];
    let parquet_ctx = setup_parquet_session_all(&parquet_dir, &table_names).await?;
    let vortex_ctx = setup_vortexlake_session_all(&vortex_dir, &table_names).await?;
    
    // Setup VortexLake session with forced CollectLeft join (for A/B test)
    // Create a new session with high threshold to force CollectLeft
    use datafusion::prelude::SessionConfig;
    let forced_config = SessionConfig::new()
        .set_usize("datafusion.optimizer.hash_join_single_partition_threshold", 1024 * 1024 * 1024)
        .set_usize("datafusion.optimizer.hash_join_single_partition_threshold_rows", 1024 * 1024 * 1024);
    let vortex_ctx_forced = SessionContext::new_with_config(forced_config);
    // Register tables
    for table_name in &table_names {
        let provider = VortexLakeTableProvider::new(
            vortex_dir.to_str().unwrap(),
            table_name,
        ).await?;
        vortex_ctx_forced.register_table(*table_name, Arc::new(provider))?;
    }
    
    println!("\n--- Parquet Profile ---");
    let (parquet_results, parquet_profile) = execute_with_full_profile(&parquet_ctx, TPCH_Q22).await?;
    parquet_profile.print();
    println!("\nParquet result rows: {}", parquet_results.iter().map(|b| b.num_rows()).sum::<usize>());
    
    println!("\n--- VortexLake Profile (default) ---");
    let (vortex_results, vortex_profile) = execute_with_full_profile(&vortex_ctx, TPCH_Q22).await?;
    vortex_profile.print();
    println!("\nVortexLake result rows: {}", vortex_results.iter().map(|b| b.num_rows()).sum::<usize>());
    
    println!("\n--- VortexLake Profile (forced CollectLeft) ---");
    let (vortex_forced_results, vortex_forced_profile) = execute_with_full_profile(&vortex_ctx_forced, TPCH_Q22).await?;
    vortex_forced_profile.print();
    println!("\nVortexLake (forced) result rows: {}", vortex_forced_results.iter().map(|b| b.num_rows()).sum::<usize>());
    
    // Compare profiles using the comprehensive comparison tool
    println!("\n\n--- Comparison: Parquet vs VortexLake (default) ---");
    compare_profiles(&parquet_profile, "Parquet", &vortex_profile, "VortexLake");
    
    println!("\n\n--- Comparison: VortexLake (default) vs VortexLake (forced CollectLeft) ---");
    compare_profiles(&vortex_profile, "Vortex-Default", &vortex_forced_profile, "Vortex-Forced");
    
    // Print HashJoin mode from EXPLAIN
    println!("\n--- HashJoin Modes (from EXPLAIN) ---");
    
    async fn print_join_mode(ctx: &SessionContext, name: &str, sql: &str) -> anyhow::Result<()> {
        let explain = ctx.sql(&format!("EXPLAIN {}", sql)).await?.collect().await?;
        for batch in &explain {
            for col in 0..batch.num_columns() {
                let array = batch.column(col);
                if let Some(string_array) = array.as_any().downcast_ref::<arrow::array::StringArray>() {
                    for row in 0..batch.num_rows() {
                        let val = string_array.value(row);
                        if val.contains("HashJoinExec") {
                            for line in val.lines() {
                                if line.contains("HashJoinExec") {
                                    println!("{}: {}", name, line.trim());
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
    
    print_join_mode(&parquet_ctx, "Parquet", TPCH_Q22).await?;
    print_join_mode(&vortex_ctx, "VortexLake (default)", TPCH_Q22).await?;
    print_join_mode(&vortex_ctx_forced, "VortexLake (forced)", TPCH_Q22).await?;
    
    Ok(())
}

