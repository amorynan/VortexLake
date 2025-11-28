
use vortexlake_core::manifest::FragmentMetadata;

/// Helper struct for managing VortexLake file sources
/// 
/// This struct holds the fragment metadata after Manifest-level pruning,
/// and can be used to construct file lists for vortex-datafusion.
#[derive(Debug, Clone)]
pub struct VortexLakeFileList {
    /// Fragments after Manifest-level pruning
    pub fragments: Vec<FragmentMetadata>,
    /// Base path for the database
    pub base_path: std::path::PathBuf,
}

impl VortexLakeFileList {
    /// Create a new file list from pruned fragments
    pub fn new(
        fragments: Vec<FragmentMetadata>,
        base_path: std::path::PathBuf,
    ) -> Self {
        Self {
            fragments,
            base_path,
        }
    }

    /// Get the number of files in this list
    pub fn len(&self) -> usize {
        self.fragments.len()
    }

    /// Check if the file list is empty
    pub fn is_empty(&self) -> bool {
        self.fragments.is_empty()
    }

    /// Get file paths as strings
    pub fn file_paths(&self) -> Vec<String> {
        self.fragments
            .iter()
            .map(|f| {
                self.base_path
                    .join(&f.path)
                    .to_string_lossy()
                    .to_string()
            })
            .collect()
    }

    /// Get total row count across all fragments
    pub fn total_row_count(&self) -> usize {
        self.fragments.iter().map(|f| f.row_count).sum()
    }

    /// Get total size in bytes across all fragments
    pub fn total_size_bytes(&self) -> u64 {
        self.fragments.iter().map(|f| f.size_bytes).sum()
    }

    /// Convert to ObjectMeta list for DataFusion
    pub fn to_object_metas(&self) -> Vec<object_store::ObjectMeta> {
        self.fragments
            .iter()
            .map(|f| {
                let path = self.base_path.join(&f.path);
                object_store::ObjectMeta {
                    location: object_store::path::Path::from(path.to_string_lossy().as_ref()),
                    last_modified: f.created_at.into(),
                    size: f.size_bytes,
                    e_tag: None,
                    version: None,
                }
            })
            .collect()
    }
}

/// Statistics for pruning results
#[derive(Debug, Clone, Default)]
pub struct PruningStats {
    /// Total fragments before pruning
    pub total_fragments: usize,
    /// Fragments remaining after pruning
    pub remaining_fragments: usize,
    /// Fragments pruned
    pub pruned_fragments: usize,
    /// Total rows before pruning
    pub total_rows: usize,
    /// Rows remaining after pruning
    pub remaining_rows: usize,
}

impl PruningStats {
    /// Calculate the pruning ratio (0.0 = no pruning, 1.0 = all pruned)
    pub fn pruning_ratio(&self) -> f64 {
        if self.total_fragments == 0 {
            0.0
        } else {
            self.pruned_fragments as f64 / self.total_fragments as f64
        }
    }

    /// Create stats from before/after fragment lists
    pub fn from_fragments(
        before: &[FragmentMetadata],
        after: &[FragmentMetadata],
    ) -> Self {
        let total_fragments = before.len();
        let remaining_fragments = after.len();
        let total_rows: usize = before.iter().map(|f| f.row_count).sum();
        let remaining_rows: usize = after.iter().map(|f| f.row_count).sum();

        Self {
            total_fragments,
            remaining_fragments,
            pruned_fragments: total_fragments - remaining_fragments,
            total_rows,
            remaining_rows,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::collections::HashMap;

    fn create_test_fragment(id: &str, row_count: usize) -> FragmentMetadata {
        FragmentMetadata {
            id: id.to_string(),
            table_name: "test".to_string(),
            path: format!("data/{}.vortex", id),
            row_count,
            size_bytes: (row_count * 100) as u64,
            created_at: Utc::now(),
            column_stats: HashMap::new(),
        }
    }

    #[test]
    fn test_file_list_basic() {
        let fragments = vec![
            create_test_fragment("frag1", 1000),
            create_test_fragment("frag2", 2000),
        ];
        let file_list = VortexLakeFileList::new(
            fragments,
            std::path::PathBuf::from("/tmp/db"),
        );

        assert_eq!(file_list.len(), 2);
        assert_eq!(file_list.total_row_count(), 3000);
        assert!(!file_list.is_empty());
    }

    #[test]
    fn test_pruning_stats() {
        let before = vec![
            create_test_fragment("f1", 1000),
            create_test_fragment("f2", 2000),
            create_test_fragment("f3", 3000),
        ];
        let after = vec![
            create_test_fragment("f2", 2000),
        ];

        let stats = PruningStats::from_fragments(&before, &after);
        
        assert_eq!(stats.total_fragments, 3);
        assert_eq!(stats.remaining_fragments, 1);
        assert_eq!(stats.pruned_fragments, 2);
        assert!((stats.pruning_ratio() - 0.666).abs() < 0.01);
    }
}

