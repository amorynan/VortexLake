//! Compression benchmark example
//!
//! This example benchmarks different compression algorithms
//! and Vortex storage performance.

use anyhow::Result;
use std::time::Instant;
use vortexlake_core::compression::{CompressionCodec, ZstdCodec, Lz4Codec, get_codec};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::init();

    println!("VortexLake Compression Benchmark");
    println!("=================================");

    // Generate test data
    let test_data = generate_test_data(10 * 1024 * 1024); // 10MB
    println!("Test data size: {} MB", test_data.len() / (1024 * 1024));

    // Benchmark compression algorithms
    let codecs = vec![
        ("zstd-1", get_codec("zstd", 1)?),
        ("zstd-3", get_codec("zstd", 3)?),
        ("zstd-9", get_codec("zstd", 9)?),
        ("lz4-1", get_codec("lz4", 1)?),
        ("lz4-9", get_codec("lz4", 9)?),
        ("none", get_codec("none", 0)?),
    ];

    println!("\nCompression Benchmark Results:");
    println!("{: <10} {: <10} {: <12} {: <10} {: <12}",
             "Codec", "Level", "Compressed", "Ratio", "Time");

    for (name, codec) in codecs {
        let start = Instant::now();
        let compressed = codec.compress(&test_data).await?;
        let compress_time = start.elapsed();

        let start = Instant::now();
        let decompressed = codec.decompress(&compressed).await?;
        let decompress_time = start.elapsed();

        // Verify correctness
        assert_eq!(test_data, decompressed);

        let ratio = compressed.len() as f64 / test_data.len() as f64;
        println!("{: <10} {: <10} {: <12} {:.3}      {:.2}ms/{:.2}ms",
                 name, codec.level(), format!("{}KB", compressed.len() / 1024),
                 ratio, compress_time.as_millis(), decompress_time.as_millis());
    }

    // Vortex storage benchmark (placeholder)
    println!("\nVortex Storage Performance (TODO):");
    println!("- Fragment creation time");
    println!("- Query performance");
    println!("- Memory usage");
    println!("- Disk I/O patterns");

    Ok(())
}

fn generate_test_data(size: usize) -> Vec<u8> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut data = Vec::with_capacity(size);

    // Generate semi-compressible data (similar to real-world data)
    for i in 0..size {
        let mut hasher = DefaultHasher::new();
        (i % 1000).hash(&mut hasher);
        let hash = hasher.finish();
        data.push((hash % 256) as u8);
    }

    data
}
