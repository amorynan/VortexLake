//! Pluggable compression strategies for VortexLake
//!
//! Provides various compression algorithms that can be applied
//! at the fragment level for optimal storage efficiency.

pub mod zstd;
pub mod lz4;

use anyhow::Result;
use async_trait::async_trait;

/// Compression codec trait
#[async_trait]
pub trait CompressionCodec: Send + Sync {
    /// Compress data
    async fn compress(&self, data: &[u8]) -> Result<Vec<u8>>;

    /// Decompress data
    async fn decompress(&self, data: &[u8]) -> Result<Vec<u8>>;

    /// Get codec name
    fn name(&self) -> &str;

    /// Get compression level (0-9, higher = better compression)
    fn level(&self) -> u8;
}

/// Get a compression codec by name
pub fn get_codec(name: &str, level: u8) -> Result<Box<dyn CompressionCodec>> {
    match name.to_lowercase().as_str() {
        "zstd" => Ok(Box::new(zstd::ZstdCodec::new(level))),
        "lz4" => Ok(Box::new(lz4::Lz4Codec::new(level))),
        "none" => Ok(Box::new(NoCompression)),
        _ => Err(anyhow::anyhow!("Unknown compression codec: {}", name)),
    }
}

/// No compression codec (pass-through)
pub struct NoCompression;

#[async_trait]
impl CompressionCodec for NoCompression {
    async fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    async fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn name(&self) -> &str {
        "none"
    }

    fn level(&self) -> u8 {
        0
    }
}

/// Compression configuration
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// Codec name
    pub codec: String,
    /// Compression level
    pub level: u8,
    /// Whether to compress metadata separately
    pub compress_metadata: bool,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            codec: "zstd".to_string(),
            level: 3,
            compress_metadata: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_no_compression() {
        let codec = NoCompression;
        let data = b"Hello, World!";

        let compressed = codec.compress(data).await.unwrap();
        assert_eq!(compressed, data);

        let decompressed = codec.decompress(&compressed).await.unwrap();
        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn test_get_codec() {
        let codec = get_codec("none", 0).unwrap();
        assert_eq!(codec.name(), "none");

        let codec = get_codec("zstd", 3).unwrap();
        assert_eq!(codec.name(), "zstd");
        assert_eq!(codec.level(), 3);
    }

    #[tokio::test]
    async fn test_unknown_codec() {
        assert!(get_codec("unknown", 0).is_err());
    }
}
