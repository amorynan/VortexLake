//! TPC-H Performance Benchmark
//!
//! Benchmarks VortexLake storage vs Parquet using Criterion.
//!
//! Run with: cargo bench -p vortexlake-sql
//! Report: target/criterion/report/index.html

use std::sync::Arc;

use arrow::array::{ArrayRef, Date32Array, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::prelude::*;
use tempfile::TempDir;
use tokio::runtime::Runtime;

// ============================================================================
// Data Generation (same as validation tests)
// ============================================================================

fn generate_lineitem_data(num_rows: usize) -> RecordBatch {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let hash_val = |seed: usize, i: usize| -> u64 {
        let mut hasher = DefaultHasher::new();
        (seed, i).hash(&mut hasher);
        hasher.finish()
    };

    let l_orderkey: Vec<i64> = (0..num_rows)
        .map(|i| ((hash_val(1, i) % 1_500_000) + 1) as i64)
        .collect();
    let l_partkey: Vec<i64> = (0..num_rows)
        .map(|i| ((hash_val(2, i) % 200_000) + 1) as i64)
        .collect();
    let l_suppkey: Vec<i64> = (0..num_rows)
        .map(|i| ((hash_val(3, i) % 10_000) + 1) as i64)
        .collect();
    let l_linenumber: Vec<i32> = (0..num_rows)
        .map(|i| ((hash_val(4, i) % 7) + 1) as i32)
        .collect();
    let l_quantity: Vec<f64> = (0..num_rows)
        .map(|i| ((hash_val(5, i) % 50) + 1) as f64)
        .collect();
    let l_extendedprice: Vec<f64> = (0..num_rows)
        .map(|i| {
            let qty = ((hash_val(5, i) % 50) + 1) as f64;
            let price = ((hash_val(6, i) % 90000) + 10000) as f64 / 100.0;
            qty * price
        })
        .collect();
    let l_discount: Vec<f64> = (0..num_rows)
        .map(|i| (hash_val(7, i) % 11) as f64 / 100.0)
        .collect();
    let l_tax: Vec<f64> = (0..num_rows)
        .map(|i| (hash_val(8, i) % 9) as f64 / 100.0)
        .collect();

    let flags = ["A", "N", "R"];
    let l_returnflag: Vec<&str> = (0..num_rows)
        .map(|i| flags[(hash_val(9, i) % 3) as usize])
        .collect();
    let statuses = ["O", "F"];
    let l_linestatus: Vec<&str> = (0..num_rows)
        .map(|i| statuses[(hash_val(10, i) % 2) as usize])
        .collect();

    let base_date = 8035i32;
    let l_shipdate: Vec<i32> = (0..num_rows)
        .map(|i| base_date + (hash_val(11, i) % 2556) as i32)
        .collect();
    let l_commitdate: Vec<i32> = (0..num_rows)
        .map(|i| base_date + (hash_val(12, i) % 2556) as i32)
        .collect();
    let l_receiptdate: Vec<i32> = (0..num_rows)
        .map(|i| base_date + (hash_val(13, i) % 2556) as i32)
        .collect();

    let instructions = [
        "DELIVER IN PERSON",
        "COLLECT COD",
        "NONE",
        "TAKE BACK RETURN",
    ];
    let l_shipinstruct: Vec<&str> = (0..num_rows)
        .map(|i| instructions[(hash_val(14, i) % 4) as usize])
        .collect();
    let modes = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"];
    let l_shipmode: Vec<&str> = (0..num_rows)
        .map(|i| modes[(hash_val(15, i) % 7) as usize])
        .collect();
    let l_comment: Vec<String> = (0..num_rows).map(|i| format!("comment_{}", i)).collect();

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

// ============================================================================
// Helper Functions
// ============================================================================

async fn write_to_parquet(batch: &RecordBatch, path: &std::path::Path) -> anyhow::Result<()> {
    use parquet::arrow::ArrowWriter;
    use std::fs::File;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(batch)?;
    writer.close()?;
    Ok(())
}

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

// ============================================================================
// TPC-H Queries
// ============================================================================

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

const SIMPLE_SCAN: &str = "SELECT COUNT(*) FROM lineitem";

const FILTER_SCAN: &str = r#"
SELECT COUNT(*) FROM lineitem 
WHERE l_quantity > 25 AND l_discount < 0.05
"#;

// ============================================================================
// Benchmarks
// ============================================================================

/// Benchmark data writing performance
fn bench_write(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("write");

    for size in [1_000, 10_000, 50_000] {
        let batch = generate_lineitem_data(size);
        let bytes = batch.get_array_memory_size();

        group.throughput(Throughput::Bytes(bytes as u64));

        group.bench_with_input(BenchmarkId::new("parquet", size), &batch, |b, batch| {
            b.to_async(&rt).iter(|| async {
                let temp_dir = TempDir::new().unwrap();
                let path = temp_dir.path().join("test.parquet");
                write_to_parquet(batch, &path).await.unwrap();
                black_box(path)
            });
        });

        // TODO: Add VortexLake write benchmark here when TableProvider is ready
        // group.bench_with_input(BenchmarkId::new("vortexlake", size), &batch, |b, batch| {
        //     b.to_async(&rt).iter(|| async {
        //         // VortexLake write
        //     });
        // });
    }

    group.finish();
}

/// Benchmark TPC-H Q1 (heavy aggregation)
fn bench_tpch_q1(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("tpch_q1");

    for size in [10_000, 50_000] {
        let batch = generate_lineitem_data(size);

        group.bench_with_input(BenchmarkId::new("parquet", size), &batch, |b, batch| {
            b.to_async(&rt).iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let path = temp_dir.path().join("lineitem.parquet");
                    rt.block_on(write_to_parquet(batch, &path)).unwrap();
                    let ctx = rt.block_on(setup_parquet_session(&path)).unwrap();
                    (temp_dir, ctx)
                },
                |(_temp_dir, ctx)| async move {
                    let df = ctx.sql(TPCH_Q1).await.unwrap();
                    let results = df.collect().await.unwrap();
                    black_box(results)
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Benchmark TPC-H Q6 (filter + aggregation)
fn bench_tpch_q6(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("tpch_q6");

    for size in [10_000, 50_000] {
        let batch = generate_lineitem_data(size);

        group.bench_with_input(BenchmarkId::new("parquet", size), &batch, |b, batch| {
            b.to_async(&rt).iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let path = temp_dir.path().join("lineitem.parquet");
                    rt.block_on(write_to_parquet(batch, &path)).unwrap();
                    let ctx = rt.block_on(setup_parquet_session(&path)).unwrap();
                    (temp_dir, ctx)
                },
                |(_temp_dir, ctx)| async move {
                    let df = ctx.sql(TPCH_Q6).await.unwrap();
                    let results = df.collect().await.unwrap();
                    black_box(results)
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Benchmark simple scan (COUNT(*))
fn bench_simple_scan(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("simple_scan");

    for size in [10_000, 50_000, 100_000] {
        let batch = generate_lineitem_data(size);

        group.bench_with_input(BenchmarkId::new("parquet", size), &batch, |b, batch| {
            b.to_async(&rt).iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let path = temp_dir.path().join("lineitem.parquet");
                    rt.block_on(write_to_parquet(batch, &path)).unwrap();
                    let ctx = rt.block_on(setup_parquet_session(&path)).unwrap();
                    (temp_dir, ctx)
                },
                |(_temp_dir, ctx)| async move {
                    let df = ctx.sql(SIMPLE_SCAN).await.unwrap();
                    let results = df.collect().await.unwrap();
                    black_box(results)
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Benchmark filter scan
fn bench_filter_scan(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("filter_scan");

    for size in [10_000, 50_000] {
        let batch = generate_lineitem_data(size);

        group.bench_with_input(BenchmarkId::new("parquet", size), &batch, |b, batch| {
            b.to_async(&rt).iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let path = temp_dir.path().join("lineitem.parquet");
                    rt.block_on(write_to_parquet(batch, &path)).unwrap();
                    let ctx = rt.block_on(setup_parquet_session(&path)).unwrap();
                    (temp_dir, ctx)
                },
                |(_temp_dir, ctx)| async move {
                    let df = ctx.sql(FILTER_SCAN).await.unwrap();
                    let results = df.collect().await.unwrap();
                    black_box(results)
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_write,
    bench_simple_scan,
    bench_filter_scan,
    bench_tpch_q1,
    bench_tpch_q6,
);

criterion_main!(benches);

