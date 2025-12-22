/// Test to demonstrate metrics isolation issue and solutions

use anyhow::Result;
use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use std::fs;
use std::sync::Arc;
use vortexlake_core::{Field as VLField, Schema as VLSchema, VortexLake, VortexLakeWriteConfig};
use vortexlake_sql::table_provider::VortexLakeTableProvider;
use vortexlake_sql::profiling::execute_with_full_profile;

#[path = "common.rs"]
mod common;
use common::{init_test_logging, get_test_data_dir};

fn find_rows_decoded(node: &vortexlake_sql::profiling::PlanNode) -> Option<usize> {
    if node.operator_name.contains("DataSourceExec") {
        return node.metrics.rows_decoded;
    }
    for child in &node.children {
        if let Some(rows) = find_rows_decoded(child) {
            return Some(rows);
        }
    }
    None
}

async fn create_test_dataset() -> Result<String> {
    let base_dir = get_test_data_dir().join("metrics_isolation");
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir)?;

    let db_path = base_dir.join("test_db");
    let db = VortexLake::new(&db_path).await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
    ]));

    let vl_schema = VLSchema::new(vec![
        VLField::new("id", DataType::Int64, false),
    ])?;
    db.create_table("test", vl_schema).await?;

    let config = VortexLakeWriteConfig::new().with_row_block_size(32);
    let mut writer = db.writer_with_config("test", config)?;

    let ids: Vec<i64> = (1..=100).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(ids))],
    )?;

    writer.write_batch(batch).await?;
    writer.commit().await?;

    Ok(db_path.to_string_lossy().to_string())
}

/// ✅ SOLUTION 1: Fresh SessionContext per query (Recommended)
#[tokio::test]
async fn solution_fresh_context() -> Result<()> {
    init_test_logging("solution_fresh_context.log");
    println!("\n=== SOLUTION 1: Fresh SessionContext per query (RECOMMENDED) ===");

    let db_path = create_test_dataset().await?;
    let queries = vec![
        ("Query 1: id <= 10", "SELECT id FROM test WHERE id <= 10"),
        ("Query 2: id <= 20", "SELECT id FROM test WHERE id <= 20"),
        ("Query 3: id <= 30", "SELECT id FROM test WHERE id <= 30"),
    ];

    for (desc, sql) in queries {
        // ✅ Fresh context = fresh metrics
        let ctx = SessionContext::new();
        ctx.register_table(
            "test",
            Arc::new(VortexLakeTableProvider::new(&db_path, "test").await?),
        )?;

        let (results, profile) = execute_with_full_profile(&ctx, sql).await?;
        let result_count: usize = results.iter().map(|b| b.num_rows()).sum();
        let rows_decoded = find_rows_decoded(&profile.plan_tree).unwrap_or(0);

        println!("{}: {} results, {} decoded ✅", desc, result_count, rows_decoded);
        assert_eq!(result_count, rows_decoded, "Should be equal with fresh context");
    }

    Ok(())
}

/// ❌ PROBLEM: Shared SessionContext causes accumulation
#[tokio::test]
async fn problem_shared_context() -> Result<()> {
    init_test_logging("problem_shared_context.log");
    println!("\n=== PROBLEM: Shared SessionContext causes accumulation ===");

    let db_path = create_test_dataset().await?;

    // ❌ Shared context = accumulated metrics
    let ctx = SessionContext::new();
    ctx.register_table(
        "test",
        Arc::new(VortexLakeTableProvider::new(&db_path, "test").await?),
    )?;

    let queries = vec![
        ("Query 1: id <= 10", "SELECT id FROM test WHERE id <= 10", 10),
        ("Query 2: id <= 20", "SELECT id FROM test WHERE id <= 20", 20),
        ("Query 3: id <= 30", "SELECT id FROM test WHERE id <= 30", 30),
    ];

    let mut total_decoded = 0;
    for (desc, sql, expected_results) in queries {
        let (results, profile) = execute_with_full_profile(&ctx, sql).await?;
        let result_count: usize = results.iter().map(|b| b.num_rows()).sum();
        let rows_decoded = find_rows_decoded(&profile.plan_tree).unwrap_or(0);

        println!("{}: {} results, {} decoded (cumulative: {}) ❌",
                desc, result_count, rows_decoded, total_decoded);

        // Show the accumulation
        let this_query_decoded = rows_decoded - total_decoded;
        println!("  → This query decoded: {} (expected: {})",
                this_query_decoded, expected_results);

        assert_eq!(result_count, expected_results, "Result count should match expected");
        total_decoded = rows_decoded;
    }

    Ok(())
}

/// 💡 SOLUTION 2: Metrics diff calculation (if you must reuse context)
#[tokio::test]
async fn solution_metrics_diff() -> Result<()> {
    init_test_logging("solution_metrics_diff.log");
    println!("\n=== SOLUTION 2: Metrics diff calculation (if context reuse needed) ===");

    let db_path = create_test_dataset().await?;

    let ctx = SessionContext::new();
    ctx.register_table(
        "test",
        Arc::new(VortexLakeTableProvider::new(&db_path, "test").await?),
    )?;

    // This would require modifying VortexLakeTableProvider to track metrics per query
    // For now, just show the concept

    println!("💡 Concept: Track metrics before/after each query");
    println!("   Before query: capture initial_rows_decoded");
    println!("   After query:  capture final_rows_decoded");
    println!("   This query:   final - initial");
    println!("");
    println!("⚠️  This requires VortexLakeTableProvider modifications");
    println!("⚠️  Fresh SessionContext is simpler and recommended");

    Ok(())
}

/// 🎯 CONCLUSION: SessionContext should be per-query for accurate metrics
#[tokio::test]
async fn conclusion_session_context_design() -> Result<()> {
    println!("\n=== CONCLUSION: SessionContext Metrics Design ===");
    println!("");
    println!("🔍 ROOT CAUSE:");
    println!("   - VortexMetrics are bound to VortexSession");
    println!("   - SessionContext can reuse VortexSession across queries");
    println!("   - Metrics accumulate across queries in same session");
    println!("");
    println!("✅ CORRECT APPROACH:");
    println!("   - Use fresh SessionContext per query for metrics isolation");
    println!("   - Each query gets clean metrics baseline");
    println!("   - No accumulation artifacts");
    println!("");
    println!("💡 ALTERNATIVE (if reuse needed):");
    println!("   - Implement metrics diff tracking");
    println!("   - Record before/after values per query");
    println!("   - Calculate per-query deltas");
    println!("   - More complex, less reliable");
    println!("");
    println!("🚀 RECOMMENDATION:");
    println!("   Use fresh SessionContext for metrics-accurate testing!");
    println!("   Fresh context = reliable metrics = correct analysis");

    Ok(())
}
