//! Zstandard compression codec

use anyhow::Result;
use async_trait::async_trait;

use super::CompressionCodec;

/// Zstandard compression codec
pub struct ZstdCodec {
    level: u8,
}

impl ZstdCodec {
    /// Create a new Zstd codec with the given compression level
    pub fn new(level: u8) -> Self {
        Self {
            level: level.min(22), // Zstd max level is 22
        }
    }
}

#[async_trait]
impl CompressionCodec for ZstdCodec {
    async fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        // Use tokio::task::spawn_blocking for CPU-intensive compression
        let data = data.to_vec();
        let level = self.level as i32;

        tokio::task::spawn_blocking(move || {
            zstd::encode_all(std::io::Cursor::new(data), level)
        })
        .await?
        .map_err(Into::into)
    }

    async fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let data = data.to_vec();

        tokio::task::spawn_blocking(move || {
            zstd::decode_all(std::io::Cursor::new(data))
        })
        .await?
        .map_err(Into::into)
    }

    fn name(&self) -> &str {
        "zstd"
    }

    fn level(&self) -> u8 {
        self.level
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_zstd_compression() {
        let codec = ZstdCodec::new(3);
        let data = b"Hello, World! This is a test string for compression.";

        let compressed = codec.compress(data).await.unwrap();
        assert!(compressed.len() < data.len()); // Should be smaller

        let decompressed = codec.decompress(&compressed).await.unwrap();
        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn test_zstd_different_levels() {
        let data = vec![0u8; 10000]; // Compressible data

        for level in [1, 5, 10, 22] {
            let codec = ZstdCodec::new(level);
            let compressed = codec.compress(&data).await.unwrap();
            let decompressed = codec.decompress(&compressed).await.unwrap();
            assert_eq!(decompressed, data);
        }
    }
}
