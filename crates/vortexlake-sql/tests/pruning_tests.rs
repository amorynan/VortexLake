//! Integration tests for Manifest-based pruning
//!
//! These tests verify that the two-layer pruning architecture works correctly:
//! - Layer 1: Manifest-based fragment pruning
//! - Layer 2: Vortex Zone Map pruning (delegated to vortex-datafusion)

use std::sync::Arc;

use arrow::array::{Float32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use tempfile::tempdir;
use vortexlake_core::{Field as VLField, Schema, VortexLake};
use vortexlake_sql::VortexLakeTableProvider;
// Import TableProvider trait to access schema() method
use datafusion::datasource::TableProvider;

/// Helper function to create a batch with sequential IDs
fn create_batch_with_range(start_id: i64, count: usize) -> RecordBatch {
    let ids: Vec<i64> = (start_id..start_id + count as i64).collect();
    let names: Vec<&str> = ids.iter().map(|_| "name").collect();
    let scores: Vec<f32> = ids.iter().map(|id| (*id as f32) * 1.5).collect();

    let schema = Arc::new(ArrowSchema::new(vec![
        Arc::new(Field::new("id", DataType::Int64, false)),
        Arc::new(Field::new("name", DataType::Utf8, false)),
        Arc::new(Field::new("score", DataType::Float32, false)),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float32Array::from(scores)),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_manifest_pruning_with_multiple_fragments() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("testdb");

    // Create database and table
    let db = VortexLake::new(&db_path).await.unwrap();
    let schema = Schema::new(vec![
        VLField::new("id", DataType::Int64, false),
        VLField::new("name", DataType::Utf8, false),
        VLField::new("score", DataType::Float32, false),
    ])
    .unwrap();
    db.create_table("test", schema).await.unwrap();

    // Write multiple fragments with non-overlapping ID ranges
    // Fragment 1: id in [0, 99]
    let mut writer = db.writer("test").unwrap();
    writer.write_batch(create_batch_with_range(0, 100)).await.unwrap();
    writer.commit().await.unwrap();

    // Fragment 2: id in [100, 199]
    let mut writer = db.writer("test").unwrap();
    writer.write_batch(create_batch_with_range(100, 100)).await.unwrap();
    writer.commit().await.unwrap();

    // Fragment 3: id in [200, 299]
    let mut writer = db.writer("test").unwrap();
    writer.write_batch(create_batch_with_range(200, 100)).await.unwrap();
    writer.commit().await.unwrap();

    // Verify fragments were created
    let fragments = db.get_fragments("test").await.unwrap();
    assert_eq!(fragments.len(), 3);
    eprintln!("Created {} fragments", fragments.len());

    // Check fragment statistics
    for (i, frag) in fragments.iter().enumerate() {
        eprintln!("Fragment {}: id={}, rows={}", i, frag.id, frag.row_count);
        if let Some(id_stats) = frag.column_stats.get("id") {
            eprintln!(
                "  id stats: min={:?}, max={:?}",
                id_stats.min_value, id_stats.max_value
            );
        }
    }

    // Verify column statistics for pruning
    // Fragment 1 should have id in [0, 99]
    let frag1_stats = fragments[0].column_stats.get("id").unwrap();
    assert_eq!(frag1_stats.min_value, Some(serde_json::json!(0)));
    assert_eq!(frag1_stats.max_value, Some(serde_json::json!(99)));

    // Fragment 2 should have id in [100, 199]
    let frag2_stats = fragments[1].column_stats.get("id").unwrap();
    assert_eq!(frag2_stats.min_value, Some(serde_json::json!(100)));
    assert_eq!(frag2_stats.max_value, Some(serde_json::json!(199)));

    // Fragment 3 should have id in [200, 299]
    let frag3_stats = fragments[2].column_stats.get("id").unwrap();
    assert_eq!(frag3_stats.min_value, Some(serde_json::json!(200)));
    assert_eq!(frag3_stats.max_value, Some(serde_json::json!(299)));
}

#[tokio::test]
async fn test_fragment_can_prune_semantics() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("testdb");

    // Create database and table
    let db = VortexLake::new(&db_path).await.unwrap();
    let schema = Schema::new(vec![
        VLField::new("value", DataType::Int64, false),
    ])
    .unwrap();
    db.create_table("test", schema).await.unwrap();

    // Create a fragment with values [10, 50]
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![Arc::new(Field::new(
            "value",
            DataType::Int64,
            false,
        ))])),
        vec![Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50]))],
    )
    .unwrap();

    let mut writer = db.writer("test").unwrap();
    writer.write_batch(batch).await.unwrap();
    writer.commit().await.unwrap();

    let fragments = db.get_fragments("test").await.unwrap();
    assert_eq!(fragments.len(), 1);
    let fragment = &fragments[0];

    // Verify can_prune logic
    // value > 60: should prune (max=50 <= 60)
    assert!(fragment.can_prune("value", ">", &serde_json::json!(60)));
    
    // value > 40: should NOT prune (max=50 > 40)
    assert!(!fragment.can_prune("value", ">", &serde_json::json!(40)));

    // value < 5: should prune (min=10 >= 5)
    assert!(fragment.can_prune("value", "<", &serde_json::json!(5)));
    
    // value < 15: should NOT prune (min=10 < 15)
    assert!(!fragment.can_prune("value", "<", &serde_json::json!(15)));

    // value = 100: should prune (100 > max)
    assert!(fragment.can_prune("value", "=", &serde_json::json!(100)));
    
    // value = 30: should NOT prune (30 is in range)
    assert!(!fragment.can_prune("value", "=", &serde_json::json!(30)));
}

#[tokio::test]
async fn test_overlaps_range_semantics() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("testdb");

    // Create database and table
    let db = VortexLake::new(&db_path).await.unwrap();
    let schema = Schema::new(vec![
        VLField::new("value", DataType::Int64, false),
    ])
    .unwrap();
    db.create_table("test", schema).await.unwrap();

    // Create a fragment with values [10, 50]
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![Arc::new(Field::new(
            "value",
            DataType::Int64,
            false,
        ))])),
        vec![Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50]))],
    )
    .unwrap();

    let mut writer = db.writer("test").unwrap();
    writer.write_batch(batch).await.unwrap();
    writer.commit().await.unwrap();

    let fragments = db.get_fragments("test").await.unwrap();
    let fragment = &fragments[0];

    // Range [20, 30] overlaps [10, 50]
    assert!(fragment.overlaps_range(
        "value",
        Some(&serde_json::json!(20)),
        Some(&serde_json::json!(30))
    ));

    // Range [60, 80] does not overlap [10, 50]
    assert!(!fragment.overlaps_range(
        "value",
        Some(&serde_json::json!(60)),
        Some(&serde_json::json!(80))
    ));

    // Range [0, 5] does not overlap [10, 50]
    assert!(!fragment.overlaps_range(
        "value",
        Some(&serde_json::json!(0)),
        Some(&serde_json::json!(5))
    ));

    // Range [50, 100] overlaps (boundary)
    assert!(fragment.overlaps_range(
        "value",
        Some(&serde_json::json!(50)),
        Some(&serde_json::json!(100))
    ));
}

#[tokio::test]
async fn test_table_provider_with_pruning() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("testdb");

    // Create database and table
    let db = VortexLake::new(&db_path).await.unwrap();
    let schema = Schema::new(vec![
        VLField::new("id", DataType::Int64, false),
        VLField::new("value", DataType::Float32, false),
    ])
    .unwrap();
    db.create_table("data", schema).await.unwrap();

    // Write multiple fragments
    for batch_num in 0..3 {
        let mut writer = db.writer("data").unwrap();
        let start = batch_num * 100;
        let ids: Vec<i64> = (start..start + 100).collect();
        let values: Vec<f32> = ids.iter().map(|id| (*id as f32) * 0.5).collect();
        
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Arc::new(Field::new("id", DataType::Int64, false)),
                Arc::new(Field::new("value", DataType::Float32, false)),
            ])),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Float32Array::from(values)),
            ],
        )
        .unwrap();
        
        writer.write_batch(batch).await.unwrap();
        writer.commit().await.unwrap();
    }

    // Create TableProvider
    let provider = VortexLakeTableProvider::new(
        db_path.to_string_lossy().as_ref(),
        "data",
    )
    .await
    .unwrap();

    // Verify provider properties
    assert_eq!(provider.table_name(), "data");
    assert_eq!(provider.schema().fields().len(), 2);
    
    eprintln!("TableProvider created successfully with schema: {:?}", provider.schema());
}

#[tokio::test]
async fn test_is_sorted_detection() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("testdb");

    let db = VortexLake::new(&db_path).await.unwrap();
    let schema = Schema::new(vec![
        VLField::new("sorted_col", DataType::Int64, false),
        VLField::new("unsorted_col", DataType::Int64, false),
    ])
    .unwrap();
    db.create_table("test", schema).await.unwrap();

    // Create a batch with one sorted and one unsorted column
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Arc::new(Field::new("sorted_col", DataType::Int64, false)),
            Arc::new(Field::new("unsorted_col", DataType::Int64, false)),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])), // Sorted
            Arc::new(Int64Array::from(vec![5, 2, 8, 1, 3])), // Not sorted
        ],
    )
    .unwrap();

    let mut writer = db.writer("test").unwrap();
    writer.write_batch(batch).await.unwrap();
    writer.commit().await.unwrap();

    let fragments = db.get_fragments("test").await.unwrap();
    let fragment = &fragments[0];

    // Check is_sorted detection
    let sorted_stats = fragment.column_stats.get("sorted_col").unwrap();
    let unsorted_stats = fragment.column_stats.get("unsorted_col").unwrap();

    eprintln!("sorted_col.is_sorted: {}", sorted_stats.is_sorted);
    eprintln!("unsorted_col.is_sorted: {}", unsorted_stats.is_sorted);

    assert!(sorted_stats.is_sorted);
    assert!(!unsorted_stats.is_sorted);
}

#[tokio::test]
async fn test_is_constant_detection() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("testdb");

    let db = VortexLake::new(&db_path).await.unwrap();
    let schema = Schema::new(vec![
        VLField::new("constant_col", DataType::Int64, false),
        VLField::new("varying_col", DataType::Int64, false),
    ])
    .unwrap();
    db.create_table("test", schema).await.unwrap();

    // Create a batch with one constant and one varying column
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Arc::new(Field::new("constant_col", DataType::Int64, false)),
            Arc::new(Field::new("varying_col", DataType::Int64, false)),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![42, 42, 42, 42, 42])), // Constant
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),      // Varying
        ],
    )
    .unwrap();

    let mut writer = db.writer("test").unwrap();
    writer.write_batch(batch).await.unwrap();
    writer.commit().await.unwrap();

    let fragments = db.get_fragments("test").await.unwrap();
    let fragment = &fragments[0];

    // Check is_constant detection
    let constant_stats = fragment.column_stats.get("constant_col").unwrap();
    let varying_stats = fragment.column_stats.get("varying_col").unwrap();

    eprintln!("constant_col.is_constant: {}", constant_stats.is_constant);
    eprintln!("varying_col.is_constant: {}", varying_stats.is_constant);

    assert!(constant_stats.is_constant);
    assert!(!varying_stats.is_constant);
}

