//! LZ4 compression codec

use anyhow::Result;
use async_trait::async_trait;

use super::CompressionCodec;

/// LZ4 compression codec
pub struct Lz4Codec {
    level: u8,
}

impl Lz4Codec {
    /// Create a new LZ4 codec with the given compression level
    /// Note: LZ4 has limited level control, higher values may not improve compression much
    pub fn new(level: u8) -> Self {
        Self {
            level: level.min(16), // LZ4 levels are typically 1-16
        }
    }
}

#[async_trait]
impl CompressionCodec for Lz4Codec {
    async fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let data = data.to_vec();

        tokio::task::spawn_blocking(move || {
            // Use fast compression for LZ4
            lz4::block::compress(&data, None, false)
                .map_err(|e| anyhow::anyhow!("LZ4 compression failed: {:?}", e))
        })
        .await?
    }

    async fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let data = data.to_vec();

        tokio::task::spawn_blocking(move || {
            lz4::block::decompress(&data, None)
                .map_err(|e| anyhow::anyhow!("LZ4 decompression failed: {:?}", e))
        })
        .await?
    }

    fn name(&self) -> &str {
        "lz4"
    }

    fn level(&self) -> u8 {
        self.level
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_lz4_compression() {
        let codec = Lz4Codec::new(1);
        let data = b"Hello, World! This is a test string for compression.";

        let compressed = codec.compress(data).await.unwrap();
        // LZ4 may not compress text much, but should still work
        assert!(compressed.len() <= data.len() + 10); // Allow some overhead

        let decompressed = codec.decompress(&compressed).await.unwrap();
        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn test_lz4_binary_data() {
        let codec = Lz4Codec::new(1);
        let data = vec![0u8; 1000]; // Highly compressible

        let compressed = codec.compress(&data).await.unwrap();
        assert!(compressed.len() < data.len()); // Should compress well

        let decompressed = codec.decompress(&compressed).await.unwrap();
        assert_eq!(decompressed, data);
    }
}
