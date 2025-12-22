/// Test to trace the exact cause of overreading (rows_decoded > result_count)
/// This test creates a minimal dataset with controlled conditions

use anyhow::Result;
use arrow::array::{Int64Array, StringArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use std::fs;
use std::sync::Arc;
use vortexlake_core::{Field as VLField, Schema as VLSchema, VortexLake, VortexLakeWriteConfig};
use vortexlake_sql::table_provider::VortexLakeTableProvider;
use vortexlake_sql::profiling::execute_with_full_profile;

// Import common test utilities  
#[path = "common.rs"]
mod common;
use common::init_test_logging;

/// Create a minimal dataset with exactly 100 rows
async fn create_minimal_dataset(block_size: usize) -> Result<String> {
    let base_dir = common::get_test_data_dir().join("trace_overread");
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir)?;

    let db_path = base_dir.join("test_db");
    let db = VortexLake::new(&db_path).await?;

    // Create simple schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    let vl_fields = vec![
        VLField::new("id", DataType::Int64, false),
        VLField::new("value", DataType::Utf8, false),
    ];
    let vl_schema = VLSchema::new(vl_fields)?;
    db.create_table("test", vl_schema).await?;

    // Create exactly 100 rows
    let config = VortexLakeWriteConfig::new().with_row_block_size(block_size);
    let mut writer = db.writer_with_config("test", config)?;

    let mut ids = Vec::new();
    let mut values = Vec::new();
    for i in 1..=100 {
        ids.push(i as i64);
        values.push(format!("value_{}", i));
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(values)),
        ],
    )?;

    writer.write_batch(batch).await?;
    writer.commit().await?;

    println!("✓ Created dataset: 100 rows, block_size={}", block_size);
    Ok(db_path.to_string_lossy().to_string())
}

/// Extract rows_decoded from profiling tree
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

#[tokio::test]
async fn trace_minimal_overread() -> Result<()> {
    init_test_logging("trace_overread.log");
    println!("\n=== TRACE: Minimal Overread Analysis ===\n");

    // Test with block_size = 32 (will have 4 blocks for 100 rows)
    let block_size = 32;
    println!("Configuration:");
    println!("  Total rows: 100");
    println!("  Block size: {}", block_size);
    println!("  Expected blocks: {} blocks", (100 + block_size - 1) / block_size);
    println!();

    let db_path = create_minimal_dataset(block_size).await?;

    // Test queries with precise expectations
    let test_cases = vec![
        ("Exact first block", "SELECT id FROM test WHERE id <= 32", 32, 32),
        ("Partial first block", "SELECT id FROM test WHERE id <= 10", 10, 32),  // Expect full block decode
        ("Cross block boundary", "SELECT id FROM test WHERE id <= 40", 40, 64), // Blocks 0+1
        ("Middle range", "SELECT id FROM test WHERE id BETWEEN 30 AND 35", 6, 64), // Touches 2 blocks
    ];

    // ✅ 正确方法：每个查询使用新的SessionContext
    println!("✅ Testing with FRESH SessionContext per query (correct approach):");
    for (desc, sql, expected_results, expected_decode_max) in test_cases {
        println!("Test: {}", desc);
        println!("  Query: {}", sql);

        // Create a NEW session for each query to avoid metric accumulation
        let ctx = SessionContext::new();
        ctx.register_table(
            "test",
            Arc::new(VortexLakeTableProvider::new(&db_path, "test").await?),
        )?;

        let (results, profile) = execute_with_full_profile(&ctx, sql).await?;
        let result_count: usize = results.iter().map(|b| b.num_rows()).sum();
        let rows_decoded = find_rows_decoded(&profile.plan_tree).unwrap_or(0);
        
        println!("  Results: {} rows", result_count);
        println!("  Decoded: {} rows", rows_decoded);
        
        assert_eq!(result_count, expected_results, "Result count mismatch");
        
        // Now that each query has its own SessionContext, metrics shouldn't accumulate
        if rows_decoded > result_count {
            let overread_pct = ((rows_decoded as f64 / result_count as f64) - 1.0) * 100.0;
            println!("  📊 OVERREAD: {:.1}% ({} extra rows)", overread_pct, rows_decoded - result_count);
            
            // Analyze why
            if rows_decoded == expected_decode_max {
                println!("  ✅ CONFIRMED: Decoding full blocks as expected");
            } else if rows_decoded % block_size == 0 {
                println!("  ✅ CONFIRMED: Decoding aligned to block boundaries");
            } else {
                println!("  ⚠️ UNEXPECTED: Decoding {} rows doesn't align with block_size {}", 
                        rows_decoded, block_size);
            }
        } else if rows_decoded == result_count {
            println!("  ✅ NO OVERREAD: Exact decode");
        } else {
            println!("  ⚠️ STRANGE: Decoded {} is less than results {}", rows_decoded, result_count);
        }
        println!();
    }

    Ok(())
}

#[tokio::test]
async fn trace_split_generation() -> Result<()> {
    init_test_logging("trace_split_generation.log");
    println!("\n=== TRACE: Split Generation Analysis ===\n");

    // Create dataset with larger block size
    let block_size = 1024;
    let db_path = create_minimal_dataset(block_size).await?;

    // Directly inspect VortexLake fragments
    let db = VortexLake::new(&db_path).await?;
    let fragments = db.get_fragments("test").await?;

    println!("Fragment Analysis:");
    for (i, fragment) in fragments.iter().enumerate() {
        println!("  Fragment {}: {} rows", i, fragment.row_count);
        
        // Calculate expected splits
        // Vortex may create splits based on internal logic
        let estimated_splits = if fragment.row_count > 8192 {
            (fragment.row_count + 8191) / 8192
        } else {
            1
        };
        
        println!("    Expected splits: {}", estimated_splits);
        
        // Check fragment statistics
        if !fragment.column_stats.is_empty() {
            if let Some(id_stats) = fragment.column_stats.get("id") {
                println!("    ID range: {:?} - {:?}", id_stats.min_value, id_stats.max_value);
            }
        }
    }
    println!();

    // Execute a query to see actual split behavior
    let ctx = SessionContext::new();
    ctx.register_table(
        "test",
        Arc::new(VortexLakeTableProvider::new(&db_path, "test").await?),
    )?;

    println!("Query Execution:");
    let sql = "SELECT id FROM test WHERE id <= 50";
    println!("  SQL: {}", sql);
    
    let (results, profile) = execute_with_full_profile(&ctx, sql).await?;
    let result_count: usize = results.iter().map(|b| b.num_rows()).sum();
    let rows_decoded = find_rows_decoded(&profile.plan_tree).unwrap_or(0);
    
    println!("  Results: {} rows", result_count);
    println!("  Decoded: {} rows", rows_decoded);
    
    // Since we have 100 rows with block_size=1024, all in one block
    // Querying id <= 50 should ideally decode 50 rows
    // But if the entire block is decoded, it would be 100 rows
    if rows_decoded == 100 && result_count == 50 {
        println!("  📊 CONFIRMED: Entire block decoded (100 rows) for partial query (50 rows)");
        println!("  REASON: Block size ({}) > total rows (100), so single block contains all data", block_size);
    } else if rows_decoded == result_count {
        println!("  ✅ PERFECT: Vortex can slice within blocks, no overread!");
    } else {
        println!("  ⚠️ UNEXPECTED: rows_decoded={} doesn't match expected patterns", rows_decoded);
    }

    Ok(())
}
