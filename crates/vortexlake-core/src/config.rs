//! VortexLake write and layout configuration
//!
//! This module provides configuration options for controlling how data is written
//! to VortexLake, including Layout strategies and compression settings.

/// VortexLake write configuration
///
/// Controls how data is organized and compressed when writing to VortexLake.
/// These settings affect both write performance and query performance.
#[derive(Debug, Clone)]
pub struct VortexLakeWriteConfig {
    /// Row block size (Zone Map granularity)
    /// 
    /// This determines how many rows are grouped together for statistics.
    /// Smaller values provide finer-grained pruning but increase metadata overhead.
    /// Default: 8192 rows
    pub row_block_size: usize,

    /// Minimum block size in bytes before triggering compaction
    /// 
    /// Blocks smaller than this will be buffered until they reach this size.
    /// Default: 1MB (1 << 20)
    pub block_size_minimum: u64,

    /// Whether to enable dictionary encoding
    /// 
    /// Dictionary encoding is effective for columns with low cardinality.
    /// Default: true
    pub enable_dict_encoding: bool,

    /// Dictionary encoding threshold
    /// 
    /// If (distinct_count / row_count) < threshold, dictionary encoding is used.
    /// Default: 0.01 (1%)
    pub dict_threshold: f64,

    /// Maximum buffer size in rows before auto-flush
    /// 
    /// Default: 100_000 rows
    pub max_buffer_rows: usize,

    /// Target fragment size in bytes
    /// 
    /// Fragments will be split when they exceed this size.
    /// Default: 128MB
    pub target_fragment_size: u64,
}

impl Default for VortexLakeWriteConfig {
    fn default() -> Self {
        Self {
            row_block_size: 8192,
            block_size_minimum: 1 << 20,  // 1MB
            enable_dict_encoding: true,
            dict_threshold: 0.01,
            max_buffer_rows: 100_000,
            target_fragment_size: 128 << 20,  // 128MB
        }
    }
}

impl VortexLakeWriteConfig {
    /// Create a new configuration with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the row block size
    pub fn with_row_block_size(mut self, size: usize) -> Self {
        self.row_block_size = size;
        self
    }

    /// Set the minimum block size
    pub fn with_block_size_minimum(mut self, size: u64) -> Self {
        self.block_size_minimum = size;
        self
    }

    /// Enable or disable dictionary encoding
    pub fn with_dict_encoding(mut self, enabled: bool) -> Self {
        self.enable_dict_encoding = enabled;
        self
    }

    /// Set the dictionary encoding threshold
    pub fn with_dict_threshold(mut self, threshold: f64) -> Self {
        self.dict_threshold = threshold;
        self
    }

    /// Set the maximum buffer size in rows
    pub fn with_max_buffer_rows(mut self, rows: usize) -> Self {
        self.max_buffer_rows = rows;
        self
    }

    /// Set the target fragment size
    pub fn with_target_fragment_size(mut self, size: u64) -> Self {
        self.target_fragment_size = size;
        self
    }
}

/// Configuration optimized for time-series data
/// 
/// Uses larger zone sizes and assumes sorted timestamps.
pub fn timeseries_config() -> VortexLakeWriteConfig {
    VortexLakeWriteConfig {
        row_block_size: 65536,  // 64K rows per zone for time range queries
        block_size_minimum: 4 << 20,  // 4MB
        enable_dict_encoding: false,  // Time series rarely benefit from dict encoding
        dict_threshold: 0.001,
        max_buffer_rows: 500_000,
        target_fragment_size: 256 << 20,  // 256MB
    }
}

/// Configuration optimized for vector/embedding columns
/// 
/// Reduces compression overhead for high-dimensional vectors.
pub fn vector_config() -> VortexLakeWriteConfig {
    VortexLakeWriteConfig {
        row_block_size: 4096,  // Smaller zones for vector queries
        block_size_minimum: 4 << 20,  // 4MB
        enable_dict_encoding: false,  // Vectors don't benefit from dict encoding
        dict_threshold: 0.0,
        max_buffer_rows: 50_000,
        target_fragment_size: 64 << 20,  // 64MB
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = VortexLakeWriteConfig::default();
        assert_eq!(config.row_block_size, 8192);
        assert_eq!(config.block_size_minimum, 1 << 20);
        assert!(config.enable_dict_encoding);
    }

    #[test]
    fn test_builder_pattern() {
        let config = VortexLakeWriteConfig::new()
            .with_row_block_size(4096)
            .with_dict_encoding(false)
            .with_max_buffer_rows(200_000);

        assert_eq!(config.row_block_size, 4096);
        assert!(!config.enable_dict_encoding);
        assert_eq!(config.max_buffer_rows, 200_000);
    }

    #[test]
    fn test_timeseries_config() {
        let config = timeseries_config();
        assert_eq!(config.row_block_size, 65536);
        assert!(!config.enable_dict_encoding);
    }

    #[test]
    fn test_vector_config() {
        let config = vector_config();
        assert_eq!(config.row_block_size, 4096);
        assert!(!config.enable_dict_encoding);
    }
}

