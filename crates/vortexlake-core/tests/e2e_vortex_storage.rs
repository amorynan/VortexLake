//! End-to-end tests for VortexLake Vortex storage
//!
//! Tests the complete flow from creating a database, writing data,
//! and reading it back using Vortex format.

use std::sync::Arc;

use arrow::array::{Float32Array, Int64Array, StringArray, StringViewArray, FixedSizeListArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use tempfile::tempdir;
use vortexlake_core::{VortexLake, Schema};

/// Helper function to create a test batch with simple data
fn create_simple_batch() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Arc::new(Field::new("id", DataType::Int64, false)),
        Arc::new(Field::new("name", DataType::Utf8, false)),
        Arc::new(Field::new("score", DataType::Float32, false)),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Diana", "Eve"])),
            Arc::new(Float32Array::from(vec![95.5, 87.3, 92.1, 88.9, 91.0])),
        ],
    )
    .unwrap()
}

/// Helper function to create a batch with vector columns (for embeddings)
fn create_vector_batch() -> RecordBatch {
    let dim = 4;  // Small dimension for testing
    
    let schema = Arc::new(ArrowSchema::new(vec![
        Arc::new(Field::new("doc_id", DataType::Utf8, false)),
        Arc::new(Field::new(
            "embedding",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, false)),
                dim,
            ),
            false,
        )),
        Arc::new(Field::new("content", DataType::Utf8, false)),
    ]));

    // Create embedding vectors
    let values = Float32Array::from(vec![
        // Doc 1: [0.1, 0.2, 0.3, 0.4]
        0.1, 0.2, 0.3, 0.4,
        // Doc 2: [0.5, 0.6, 0.7, 0.8]
        0.5, 0.6, 0.7, 0.8,
        // Doc 3: [0.9, 1.0, 1.1, 1.2]
        0.9, 1.0, 1.1, 1.2,
    ]);
    
    let field = Arc::new(Field::new("item", DataType::Float32, false));
    let embeddings = FixedSizeListArray::try_new(field, dim, Arc::new(values), None).unwrap();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["doc1", "doc2", "doc3"])),
            Arc::new(embeddings),
            Arc::new(StringArray::from(vec![
                "Hello world",
                "Machine learning",
                "Vector search",
            ])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_e2e_simple_table() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("testdb");

    eprintln!("=== Test: Creating database at {:?}", db_path);

    // Create database
    let db = VortexLake::new(&db_path).await.unwrap();

    // Define schema
    let schema = Schema::new(vec![
        vortexlake_core::Field::new("id", DataType::Int64, false),
        vortexlake_core::Field::new("name", DataType::Utf8, false),
        vortexlake_core::Field::new("score", DataType::Float32, false),
    ])
    .unwrap();

    // Create table
    eprintln!("=== Creating table...");
    db.create_table("students", schema).await.unwrap();

    // Verify table was created
    let tables = db.list_tables().await;
    eprintln!("=== Tables: {:?}", tables);
    assert!(tables.contains(&"students".to_string()));

    // Write data
    eprintln!("=== Writing data...");
    let mut writer = db.writer("students").unwrap();
    let batch = create_simple_batch();
    writer.write_batch(batch).await.unwrap();
    eprintln!("=== Committing...");
    writer.commit().await.unwrap();

    // List files
    eprintln!("=== Listing files in {:?}", db_path);
    if let Ok(entries) = std::fs::read_dir(&db_path) {
        for entry in entries {
            if let Ok(entry) = entry {
                eprintln!("  {:?}", entry.path());
                if entry.path().is_dir() {
                    if let Ok(subentries) = std::fs::read_dir(entry.path()) {
                        for subentry in subentries {
                            if let Ok(subentry) = subentry {
                                eprintln!("    {:?}", subentry.path());
                            }
                        }
                    }
                }
            }
        }
    }

    // Check manifest content
    eprintln!("=== Reading manifest...");
    let manifest_content = std::fs::read_to_string(db_path.join("manifest.json")).unwrap();
    eprintln!("Manifest content:\n{}", manifest_content);

    // Read data back
    eprintln!("=== Reading data...");
    let reader = db.reader("students").unwrap();
    eprintln!("=== Reader base_path: {:?}", reader.base_path());
    let batches = reader.read_all().await.unwrap();

    eprintln!("=== Got {} batches", batches.len());
    assert_eq!(batches.len(), 1);
    
    // Print schema info
    eprintln!("=== Schema: {:?}", batches[0].schema());
    eprintln!("=== Num rows: {}, Num columns: {}", batches[0].num_rows(), batches[0].num_columns());
    for (i, col) in batches[0].columns().iter().enumerate() {
        eprintln!("  Column {}: type={:?}, len={}", i, col.data_type(), col.len());
    }
    
    assert_eq!(batches[0].num_rows(), 5);
    assert_eq!(batches[0].num_columns(), 3);

    // Verify data integrity - get columns by name for robustness
    let schema = batches[0].schema();
    let id_idx = schema.index_of("id").expect("id column not found");
    let name_idx = schema.index_of("name").expect("name column not found");
    
    let id_col = batches[0]
        .column(id_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Failed to downcast id column to Int64Array");
    assert_eq!(id_col.value(0), 1);
    assert_eq!(id_col.value(4), 5);

    // Vortex converts Utf8 to Utf8View for efficiency
    let name_col = batches[0]
        .column(name_idx)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("Failed to downcast name column to StringViewArray");
    assert_eq!(name_col.value(0), "Alice");
    assert_eq!(name_col.value(2), "Charlie");
    
    eprintln!("=== Test passed!");
}

#[tokio::test]
async fn test_e2e_vector_table() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("vectordb");

    // Create database
    let db = VortexLake::new(&db_path).await.unwrap();

    // Define schema with vector column
    let schema = Schema::new(vec![
        vortexlake_core::Field::new("doc_id", DataType::Utf8, false),
        vortexlake_core::Field::new(
            "embedding",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, false)),
                4,
            ),
            false,
        ),
        vortexlake_core::Field::new("content", DataType::Utf8, false),
    ])
    .unwrap();

    // Create table
    db.create_table("documents", schema).await.unwrap();

    // Write vector data
    let mut writer = db.writer("documents").unwrap();
    let batch = create_vector_batch();
    writer.write_batch(batch).await.unwrap();
    writer.commit().await.unwrap();

    // Read data back
    let reader = db.reader("documents").unwrap();
    let batches = reader.read_all().await.unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);
    assert_eq!(batches[0].num_columns(), 3);

    // Verify vector data (Vortex converts Utf8 to Utf8View)
    let doc_id_col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();
    assert_eq!(doc_id_col.value(0), "doc1");
    assert_eq!(doc_id_col.value(2), "doc3");
}

#[tokio::test]
async fn test_e2e_persistence() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("persistdb");

    // Create database and write data
    {
        let db = VortexLake::new(&db_path).await.unwrap();

        let schema = Schema::new(vec![
            vortexlake_core::Field::new("id", DataType::Int64, false),
            vortexlake_core::Field::new("value", DataType::Utf8, false),
        ])
        .unwrap();

        db.create_table("test", schema).await.unwrap();

        let mut writer = db.writer("test").unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Arc::new(Field::new("id", DataType::Int64, false)),
                Arc::new(Field::new("value", DataType::Utf8, false)),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![100, 200, 300])),
                Arc::new(StringArray::from(vec!["one", "two", "three"])),
            ],
        )
        .unwrap();
        writer.write_batch(batch).await.unwrap();
        writer.commit().await.unwrap();
    }

    // Reopen database and verify data persisted
    {
        let db = VortexLake::new(&db_path).await.unwrap();

        // Verify table exists
        let tables = db.list_tables().await;
        assert!(tables.contains(&"test".to_string()));

        // Verify schema
        let schema = db.get_schema("test").await.unwrap();
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.fields[0].name, "id");
        assert_eq!(schema.fields[1].name, "value");

        // Read data
        let reader = db.reader("test").unwrap();
        let batches = reader.read_all().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);

        let id_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 100);
        assert_eq!(id_col.value(1), 200);
        assert_eq!(id_col.value(2), 300);
    }
}

#[tokio::test]
async fn test_e2e_multiple_writes() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("multiwrite");

    let db = VortexLake::new(&db_path).await.unwrap();

    let schema = Schema::new(vec![
        vortexlake_core::Field::new("id", DataType::Int64, false),
    ])
    .unwrap();

    db.create_table("numbers", schema).await.unwrap();

    // Write multiple batches
    for i in 0..3 {
        let mut writer = db.writer("numbers").unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Arc::new(Field::new(
                "id",
                DataType::Int64,
                false,
            ))])),
            vec![Arc::new(Int64Array::from(vec![i * 10, i * 10 + 1, i * 10 + 2]))],
        )
        .unwrap();
        writer.write_batch(batch).await.unwrap();
        writer.commit().await.unwrap();
    }

    // Read all data (should be 3 fragments, 9 total rows)
    let reader = db.reader("numbers").unwrap();
    let batches = reader.read_all().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 9);
    assert_eq!(batches.len(), 3);  // 3 fragments
}

