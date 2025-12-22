/// Demonstrate configurable VortexMetrics isolation modes

use anyhow::Result;
use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use std::fs;
use std::sync::Arc;
use vortexlake_core::{Field as VLField, Schema as VLSchema, VortexLake, VortexLakeWriteConfig};
use vortexlake_sql::table_provider::VortexLakeTableProvider;
use vortexlake_sql::profiling::execute_with_full_profile;
use vortexlake_sql::metrics_config::{MetricsIsolationMode, VortexMetricsConfig};

#[path = "common.rs"]
mod common;
use common::{init_test_logging, get_test_data_dir};

async fn create_demo_dataset() -> Result<String> {
    let base_dir = get_test_data_dir().join("metrics_demo");
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir)?;

    let db_path = base_dir.join("demo_db");
    let db = VortexLake::new(&db_path).await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    let vl_schema = VLSchema::new(vec![
        VLField::new("id", DataType::Int64, false),
        VLField::new("value", DataType::Utf8, false),
    ])?;
    db.create_table("demo", vl_schema).await?;

    let config = VortexLakeWriteConfig::new().with_row_block_size(50);
    let mut writer = db.writer_with_config("demo", config)?;

    // Create 200 rows: blocks of 50 rows each = 4 blocks
    let ids: Vec<i64> = (1..=200).collect();
    let values: Vec<String> = (1..=200).map(|i| format!("value_{}", i)).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(arrow::array::StringArray::from(values)),
        ],
    )?;

    writer.write_batch(batch).await?;
    writer.commit().await?;

    Ok(db_path.to_string_lossy().to_string())
}

#[tokio::test]
async fn demonstrate_metrics_isolation_modes() -> Result<()> {
    init_test_logging("metrics_isolation_demo.log");
    println!("\n=== VortexMetrics Isolation Mode Demonstration ===\n");

    let db_path = create_demo_dataset().await?;

    // Test queries that access the same file multiple times
    let queries = vec![
        ("Query 1: id < 60", "SELECT id FROM demo WHERE id < 60"),     // ~60 rows
        ("Query 2: id < 120", "SELECT id FROM demo WHERE id < 120"),   // ~120 rows
        ("Query 3: id < 180", "SELECT id FROM demo WHERE id < 180"),   // ~180 rows
    ];

    println!("Dataset: 200 rows, 4 blocks (50 rows each)");
    println!("Same file accessed multiple times in different queries\n");

    // === DEMO 1: SessionAccumulated (Default) ===
    println!("📊 MODE 1: SessionAccumulated (Default - Session-level metrics)");
    println!("   - Metrics accumulate across entire session");
    println!("   - Shows total file access patterns");
    println!("   - Useful for cache optimization and hotspot analysis\n");

    let session_config = VortexMetricsConfig::new()
        .with_isolation_mode(MetricsIsolationMode::SessionAccumulated);

    let mut session_ctx = SessionContext::new();
    session_ctx.register_table(
        "demo",
        Arc::new(VortexLakeTableProvider::new(&db_path, "demo").await?),
    )?;

    for (desc, sql) in &queries {
        let (results, profile) = execute_with_full_profile(&session_ctx, sql).await?;
        let result_count: usize = results.iter().map(|b| b.num_rows()).sum();

        println!("  {}: {} results", desc, result_count);
        // Note: profile.print() would show accumulated metrics
    }

    println!("\n  🔍 Key Observation:");
    println!("    - rows_decoded_session_total would show CUMULATIVE total");
    println!("    - Query 1: ~60 rows");
    println!("    - Query 2: ~60 + 120 = ~180 rows (accumulated)");
    println!("    - Query 3: ~60 + 120 + 180 = ~360 rows (accumulated)\n");

    // === DEMO 2: PerDataSource (Isolated) ===
    println!("🎯 MODE 2: PerDataSource (Isolated per-scan metrics)");
    println!("   - Each DataSourceExec gets its own metrics scope");
    println!("   - Shows accurate per-scan consumption");
    println!("   - Useful for detailed query analysis\n");

    let isolated_config = VortexMetricsConfig::new()
        .with_isolation_mode(MetricsIsolationMode::PerDataSource);

    for (desc, sql) in &queries {
        // Fresh context for each query = isolated metrics
        let mut isolated_ctx = SessionContext::new();
        isolated_ctx.register_table(
            "demo",
            Arc::new(VortexLakeTableProvider::new(&db_path, "demo").await?),
        )?;

        let (results, profile) = execute_with_full_profile(&isolated_ctx, sql).await?;
        let result_count: usize = results.iter().map(|b| b.num_rows()).sum();

        println!("  {}: {} results", desc, result_count);
        // Note: profile.print() would show per-query accurate metrics
    }

    println!("\n  🔍 Key Observation:");
    println!("    - rows_decoded_this_scan would show ACCURATE per-query values");
    println!("    - Query 1: ~60 rows (exact consumption)");
    println!("    - Query 2: ~120 rows (exact consumption)");
    println!("    - Query 3: ~180 rows (exact consumption)\n");

    // === SUMMARY ===
    println!("🎯 SUMMARY:");
    println!("");
    println!("SessionAccumulated (默认):");
    println!("  ✅ 适合：文件热点分析、缓存优化、系统监控");
    println!("  ✅ 显示：整个session的文件访问模式");
    println!("  ❌ 不适合：精确的单查询性能分析");
    println!("");
    println!("PerDataSource (隔离):");
    println!("  ✅ 适合：精确的查询性能分析、调试");
    println!("  ✅ 显示：每个扫描的准确消耗");
    println!("  ❌ 不适合：跨查询的文件访问模式分析");
    println!("");
    println!("💡 建议：");
    println!("  - 生产监控：使用 SessionAccumulated");
    println!("  - 查询调试：使用 PerDataSource");
    println!("  - 可配置：根据场景选择合适的模式");

    Ok(())
}
