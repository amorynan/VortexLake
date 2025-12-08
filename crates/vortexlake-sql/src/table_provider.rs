//! VortexLake TableProvider - Two-Layer Pruning Architecture
//!
//! This module implements a DataFusion TableProvider that uses a two-layer
//! pruning strategy:
//!
//! 1. **Manifest Layer Pruning**: Uses fragment-level statistics stored in the
//!    VortexLake manifest to skip entire files before opening them.
//!
//! 2. **Vortex Zone Layer Pruning**: Delegates to vortex-datafusion for fine-grained
//!    Zone Map pruning within each file.
//!
//! This architecture combines VortexLake's Manifest awareness with Vortex's
//! advanced compression and query capabilities.

use std::any::Any;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
// Use DataFusion's re-exported Arrow types to avoid version conflicts
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::DataFusionError;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::PartitionedFile;
use datafusion_execution::object_store::ObjectStoreUrl;
// Use object_store directly (version 0.12 matches DataFusion 51)
use object_store::local::LocalFileSystem;
use object_store::{ObjectMeta, ObjectStore};
use vortex::VortexSessionDefault;
// VortexFormat and VortexOptions are re-exported at vortex_datafusion root
use vortex_datafusion::{VortexFormat, VortexOptions};
// FileFormat trait needed for create_physical_plan
use datafusion_datasource::file_format::FileFormat;
use vortex_session::VortexSession;
use vortexlake_core::manifest::FragmentMetadata;
use vortexlake_core::VortexLake;

use crate::file_source::PruningStats;

/// VortexLake TableProvider with two-layer pruning
///
/// This provider implements:
/// - Layer 1: Manifest-based fragment pruning using column statistics
/// - Layer 2: Delegates to vortex-datafusion for Zone Map pruning
#[derive(Clone)]
pub struct VortexLakeTableProvider {
    /// VortexLake database instance
    db: VortexLake,
    /// Table name
    table_name: String,
    /// Cached Arrow schema (using DataFusion's Arrow version)
    schema: SchemaRef,
    /// Database base path
    base_path: PathBuf,
    /// Vortex session for file operations
    vortex_session: VortexSession,
    /// Vortex format for DataFusion integration
    vortex_format: Arc<VortexFormat>,
}

impl Debug for VortexLakeTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VortexLakeTableProvider")
            .field("table_name", &self.table_name)
            .field("schema", &self.schema)
            .field("base_path", &self.base_path)
            .finish()
    }
}

impl VortexLakeTableProvider {
    /// Create a new table provider
    pub async fn new(db_path: &str, table_name: &str) -> Result<Self> {
        let db = VortexLake::new(db_path).await?;
        let reader = db.reader(table_name)?;
        
        // Convert vortexlake-core schema (arrow 56) to DataFusion schema (arrow 57)
        // by rebuilding via IPC serialization
        let core_schema = reader.schema().to_arrow();
        let schema = Self::convert_schema(&core_schema)?;
        let base_path = PathBuf::from(db_path);

        // Initialize Vortex session and format
        let vortex_session = VortexSession::default();
        let opts = VortexOptions::default();
        let vortex_format = Arc::new(VortexFormat::new_with_options(
            vortex_session.clone(),
            opts,
        ));

        Ok(Self {
            db,
            table_name: table_name.to_string(),
            schema,
            base_path,
            vortex_session,
            vortex_format,
        })
    }

    /// Convert schema from arrow 56 to arrow 57 via IPC serialization
    fn convert_schema(core_schema: &arrow::datatypes::Schema) -> Result<SchemaRef> {
        use datafusion::arrow::datatypes::{Field, Schema};
        
        // Manually rebuild schema fields
        let fields: Vec<Arc<Field>> = core_schema
            .fields()
            .iter()
            .map(|f| {
                Arc::new(Field::new(
                    f.name(),
                    Self::convert_data_type(f.data_type()),
                    f.is_nullable(),
                ))
            })
            .collect();

        Ok(Arc::new(Schema::new(fields)))
    }

    /// Convert DataType from arrow 56 to arrow 57， maybe not needed
    fn convert_data_type(dt: &arrow::datatypes::DataType) -> datafusion::arrow::datatypes::DataType {
        use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
        
        match dt {
            arrow::datatypes::DataType::Null => DataType::Null,
            arrow::datatypes::DataType::Boolean => DataType::Boolean,
            arrow::datatypes::DataType::Int8 => DataType::Int8,
            arrow::datatypes::DataType::Int16 => DataType::Int16,
            arrow::datatypes::DataType::Int32 => DataType::Int32,
            arrow::datatypes::DataType::Int64 => DataType::Int64,
            arrow::datatypes::DataType::UInt8 => DataType::UInt8,
            arrow::datatypes::DataType::UInt16 => DataType::UInt16,
            arrow::datatypes::DataType::UInt32 => DataType::UInt32,
            arrow::datatypes::DataType::UInt64 => DataType::UInt64,
            arrow::datatypes::DataType::Float16 => DataType::Float16,
            arrow::datatypes::DataType::Float32 => DataType::Float32,
            arrow::datatypes::DataType::Float64 => DataType::Float64,
            arrow::datatypes::DataType::Utf8 => DataType::Utf8,
            arrow::datatypes::DataType::LargeUtf8 => DataType::LargeUtf8,
            arrow::datatypes::DataType::Binary => DataType::Binary,
            arrow::datatypes::DataType::LargeBinary => DataType::LargeBinary,
            arrow::datatypes::DataType::FixedSizeBinary(size) => DataType::FixedSizeBinary(*size),
            arrow::datatypes::DataType::List(f) => {
                DataType::List(Arc::new(Field::new(
                    f.name(),
                    Self::convert_data_type(f.data_type()),
                    f.is_nullable(),
                )))
            }
            arrow::datatypes::DataType::LargeList(f) => {
                DataType::LargeList(Arc::new(Field::new(
                    f.name(),
                    Self::convert_data_type(f.data_type()),
                    f.is_nullable(),
                )))
            }
            arrow::datatypes::DataType::FixedSizeList(f, size) => {
                DataType::FixedSizeList(
                    Arc::new(Field::new(
                        f.name(),
                        Self::convert_data_type(f.data_type()),
                        f.is_nullable(),
                    )),
                    *size,
                )
            }
            arrow::datatypes::DataType::Struct(fields) => {
                let new_fields: Vec<Arc<Field>> = fields
                    .iter()
                    .map(|f| {
                        Arc::new(Field::new(
                            f.name(),
                            Self::convert_data_type(f.data_type()),
                            f.is_nullable(),
                        ))
                    })
                    .collect();
                DataType::Struct(new_fields.into())
            }
            arrow::datatypes::DataType::Date32 => DataType::Date32,
            arrow::datatypes::DataType::Date64 => DataType::Date64,
            arrow::datatypes::DataType::Timestamp(tu, tz) => {
                let new_tu = match tu {
                    arrow::datatypes::TimeUnit::Second => TimeUnit::Second,
                    arrow::datatypes::TimeUnit::Millisecond => TimeUnit::Millisecond,
                    arrow::datatypes::TimeUnit::Microsecond => TimeUnit::Microsecond,
                    arrow::datatypes::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
                };
                DataType::Timestamp(new_tu, tz.clone())
            }
            // Decimal types
            arrow::datatypes::DataType::Decimal128(precision, scale) => {
                DataType::Decimal128(*precision, *scale)
            }
            arrow::datatypes::DataType::Decimal256(precision, scale) => {
                DataType::Decimal256(*precision, *scale)
            }
            // String view types (Arrow 2.0)
            arrow::datatypes::DataType::Utf8View => DataType::Utf8View,
            arrow::datatypes::DataType::BinaryView => DataType::BinaryView,
            // Time types
            arrow::datatypes::DataType::Time32(tu) => {
                let new_tu = match tu {
                    arrow::datatypes::TimeUnit::Second => TimeUnit::Second,
                    arrow::datatypes::TimeUnit::Millisecond => TimeUnit::Millisecond,
                    arrow::datatypes::TimeUnit::Microsecond => TimeUnit::Microsecond,
                    arrow::datatypes::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
                };
                DataType::Time32(new_tu)
            }
            arrow::datatypes::DataType::Time64(tu) => {
                let new_tu = match tu {
                    arrow::datatypes::TimeUnit::Second => TimeUnit::Second,
                    arrow::datatypes::TimeUnit::Millisecond => TimeUnit::Millisecond,
                    arrow::datatypes::TimeUnit::Microsecond => TimeUnit::Microsecond,
                    arrow::datatypes::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
                };
                DataType::Time64(new_tu)
            }
            arrow::datatypes::DataType::Duration(tu) => {
                let new_tu = match tu {
                    arrow::datatypes::TimeUnit::Second => TimeUnit::Second,
                    arrow::datatypes::TimeUnit::Millisecond => TimeUnit::Millisecond,
                    arrow::datatypes::TimeUnit::Microsecond => TimeUnit::Microsecond,
                    arrow::datatypes::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
                };
                DataType::Duration(new_tu)
            }
            // Interval type
            arrow::datatypes::DataType::Interval(iu) => {
                use datafusion::arrow::datatypes::IntervalUnit;
                let new_iu = match iu {
                    arrow::datatypes::IntervalUnit::YearMonth => IntervalUnit::YearMonth,
                    arrow::datatypes::IntervalUnit::DayTime => IntervalUnit::DayTime,
                    arrow::datatypes::IntervalUnit::MonthDayNano => IntervalUnit::MonthDayNano,
                };
                DataType::Interval(new_iu)
            }
            // Default to Utf8 for unsupported types (with warning)
            other => {
                tracing::warn!("Unsupported data type {:?}, falling back to Utf8", other);
                DataType::Utf8
            }
        }
    }

    /// Get the underlying VortexLake database
    pub fn database(&self) -> &VortexLake {
        &self.db
    }

    /// Get table name
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Get database path
    pub fn base_path(&self) -> &PathBuf {
        &self.base_path
    }

    /// Get schema
    pub fn get_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Layer 1: Prune fragments using Manifest statistics
    ///
    /// This method evaluates filter expressions against fragment-level statistics
    /// to eliminate entire files from the scan.
    fn prune_fragments_by_manifest(
        &self,
        fragments: &[FragmentMetadata],
        filters: &[Expr],
    ) -> (Vec<FragmentMetadata>, PruningStats) {
        if filters.is_empty() {
            let stats = PruningStats {
                total_fragments: fragments.len(),
                remaining_fragments: fragments.len(),
                pruned_fragments: 0,
                total_rows: fragments.iter().map(|f| f.row_count).sum(),
                remaining_rows: fragments.iter().map(|f| f.row_count).sum(),
            };
            return (fragments.to_vec(), stats);
        }

        let pruned: Vec<FragmentMetadata> = fragments
            .iter()
            .filter(|fragment| !self.can_prune_fragment(fragment, filters))
            .cloned()
            .collect();

        let stats = PruningStats::from_fragments(fragments, &pruned);
        (pruned, stats)
    }

    /// Check if a fragment can be pruned based on filter expressions
    fn can_prune_fragment(&self, fragment: &FragmentMetadata, filters: &[Expr]) -> bool {
        for (i, filter) in filters.iter().enumerate() {
            let can_prune = self.filter_can_prune(fragment, filter);
            tracing::debug!(
                "  Fragment '{}' Filter {}: {:?} -> {}",
                fragment.id,
                i + 1,
                filter,
                if can_prune { "CAN PRUNE (skip)" } else { "CANNOT PRUNE (keep)" }
            );
            if can_prune {
                return true;
            }
        }
        false
    }

    /// Evaluate a single filter expression against fragment statistics
    fn filter_can_prune(&self, fragment: &FragmentMetadata, filter: &Expr) -> bool {
        match filter {
            Expr::BinaryExpr(binary) => {
                // Try to extract column, operator, and literal value
                if let Some((column, op, value)) = Self::extract_comparison(&binary.left, &binary.op, &binary.right) {
                    let can_prune = fragment.can_prune(&column, &op, &value);
                    // Log detailed pruning decision
                    if let Some(stats) = fragment.column_stats.get(&column) {
                        tracing::debug!(
                            "    Column '{}': min={:?}, max={:?} | Filter: {} {} {:?} -> {}",
                            column,
                            stats.min_value,
                            stats.max_value,
                            column,
                            op,
                            value,
                            if can_prune { "PRUNE" } else { "KEEP" }
                        );
                    } else {
                        tracing::debug!(
                            "    Column '{}': NO STATS available -> KEEP (conservative)",
                            column
                        );
                    }
                    return can_prune;
                }
                // Also try reversed order (e.g., 5 < x)
                if let Some((column, op, value)) = Self::extract_comparison_reversed(&binary.left, &binary.op, &binary.right) {
                    let can_prune = fragment.can_prune(&column, &op, &value);
                    if let Some(stats) = fragment.column_stats.get(&column) {
                        tracing::debug!(
                            "    Column '{}': min={:?}, max={:?} | Filter: {} {} {:?} -> {}",
                            column,
                            stats.min_value,
                            stats.max_value,
                            column,
                            op,
                            value,
                            if can_prune { "PRUNE" } else { "KEEP" }
                        );
                    } else {
                        tracing::debug!(
                            "    Column '{}': NO STATS available -> KEEP (conservative)",
                            column
                        );
                    }
                    return can_prune;
                }
                tracing::debug!("    BinaryExpr not a simple comparison -> KEEP (conservative)");
                false
            }
            Expr::Not(_inner) => {
                // NOT can sometimes be handled
                false // Conservative: don't prune
            }
            Expr::IsNull(inner) => {
                // Check if column has no nulls
                if let Expr::Column(col) = inner.as_ref() {
                    if let Some(stats) = fragment.column_stats.get(col.name()) {
                        // If null_count is 0, IS NULL will never match
                        return stats.null_count == 0;
                    }
                }
                false
            }
            Expr::IsNotNull(inner) => {
                // Check if column is all nulls
                if let Expr::Column(col) = inner.as_ref() {
                    if let Some(stats) = fragment.column_stats.get(col.name()) {
                        // If null_count == row_count, IS NOT NULL will never match
                        return stats.null_count == fragment.row_count;
                    }
                }
                false
            }
            _ => false, // Conservative: don't prune for complex expressions
        }
    }

    /// Extract (column_name, operator, value) from a binary expression
    fn extract_comparison(
        left: &Expr,
        op: &datafusion::logical_expr::Operator,
        right: &Expr,
    ) -> Option<(String, String, serde_json::Value)> {
        use datafusion::logical_expr::Operator;

        let column = match left {
            Expr::Column(col) => col.name().to_string(),
            _ => return None,
        };

        // TODO: why here use expr_to_json? why not use right directly?
        let value = Self::expr_to_json(right)?;

        let op_str = match op {
            Operator::Eq => "=",
            Operator::NotEq => "!=",
            Operator::Lt => "<",
            Operator::LtEq => "<=",
            Operator::Gt => ">",
            Operator::GtEq => ">=",
            _ => return None,
        };

        Some((column, op_str.to_string(), value))
    }

    /// Extract comparison with reversed operand order (e.g., 5 < x becomes x > 5)
    fn extract_comparison_reversed(
        left: &Expr,
        op: &datafusion::logical_expr::Operator,
        right: &Expr,
    ) -> Option<(String, String, serde_json::Value)> {
        use datafusion::logical_expr::Operator;

        let column = match right {
            Expr::Column(col) => col.name().to_string(),
            _ => return None,
        };

        let value = Self::expr_to_json(left)?;

        // Reverse the operator
        let op_str = match op {
            Operator::Eq => "=",
            Operator::NotEq => "!=",
            Operator::Lt => ">",   // 5 < x means x > 5
            Operator::LtEq => ">=",
            Operator::Gt => "<",   // 5 > x means x < 5
            Operator::GtEq => "<=",
            _ => return None,
        };

        Some((column, op_str.to_string(), value))
    }

    /// Convert a literal expression to JSON value
    fn expr_to_json(expr: &Expr) -> Option<serde_json::Value> {
        match expr {
            Expr::Literal(scalar, _metadata) => {
                use datafusion::scalar::ScalarValue;
                match scalar {
                    ScalarValue::Int8(Some(v)) => Some(serde_json::json!(*v)),
                    ScalarValue::Int16(Some(v)) => Some(serde_json::json!(*v)),
                    ScalarValue::Int32(Some(v)) => Some(serde_json::json!(*v)),
                    ScalarValue::Int64(Some(v)) => Some(serde_json::json!(*v)),
                    ScalarValue::UInt8(Some(v)) => Some(serde_json::json!(*v)),
                    ScalarValue::UInt16(Some(v)) => Some(serde_json::json!(*v)),
                    ScalarValue::UInt32(Some(v)) => Some(serde_json::json!(*v)),
                    ScalarValue::UInt64(Some(v)) => Some(serde_json::json!(*v)),
                    ScalarValue::Float32(Some(v)) => Some(serde_json::json!(*v)),
                    ScalarValue::Float64(Some(v)) => Some(serde_json::json!(*v)),
                    ScalarValue::Utf8(Some(v)) => Some(serde_json::json!(v)),
                    ScalarValue::Boolean(Some(v)) => Some(serde_json::json!(*v)),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// Build PartitionedFile list from pruned fragments
    fn build_partitioned_files(&self, fragments: &[FragmentMetadata]) -> Vec<PartitionedFile> {
        use object_store::path::Path as ObjectPath;
        
        fragments
            .iter()
            .map(|frag| {
                let file_path = self.base_path.join(&frag.path);
                // For LocalFileSystem, ObjectPath should NOT have leading /
                // LocalFileSystem expects paths relative to filesystem root
                let path_str = file_path.to_string_lossy();
                // Parse instead of from() to handle special characters correctly
                let location = ObjectPath::parse(&path_str).unwrap_or_else(|_| {
                    ObjectPath::from(path_str.strip_prefix('/').unwrap_or(&path_str))
                });
                
                // CRITICAL: Get actual file size from disk, NOT frag.size_bytes (which is memory size)
                // vortex-file uses file_size to calculate footer position
                let actual_file_size = std::fs::metadata(&file_path)
                    .map(|m| m.len())
                    .unwrap_or(frag.size_bytes); // Fallback to manifest size if file not accessible
                
                tracing::debug!(
                    path = %location, 
                    manifest_size = frag.size_bytes, 
                    actual_size = actual_file_size,
                    "Building PartitionedFile"
                );
                
                PartitionedFile {
                    object_meta: ObjectMeta {
                        location,
                        last_modified: frag.created_at.into(),
                        size: actual_file_size,
                        e_tag: None,
                        version: None,
                    },
                    partition_values: vec![],
                    range: None,
                    statistics: None,
                    extensions: None,
                    metadata_size_hint: None,
                }
            })
            .collect()
    }

    /// Build file groups from partitioned files
    fn build_file_groups(&self, files: Vec<PartitionedFile>) -> Vec<FileGroup> {
        // Each file as a separate group for parallel execution
        files.into_iter().map(|f| FileGroup::new(vec![f])).collect()
    }
}

#[async_trait]
impl TableProvider for VortexLakeTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Indicate which filters can be pushed down to the TableProvider.
    /// 
    /// This enables DataFusion's filter pushdown optimization, which moves
    /// filter predicates from the Filter operator into the TableScan.
    /// 
    /// We return `Inexact` for supported filters because:
    /// 1. Manifest-level pruning may skip entire fragments
    /// 2. Zone-map pruning within Vortex files may skip row groups
    /// 3. But we don't guarantee all non-matching rows are filtered
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion_common::Result<Vec<TableProviderFilterPushDown>> {
        use datafusion::logical_expr::Operator;
        
        filters
            .iter()
            .map(|expr| {
                match expr {
                    // Support basic comparison operators
                    Expr::BinaryExpr(binary) => {
                        match binary.op {
                            // Equality and inequality
                            Operator::Eq | Operator::NotEq |
                            // Range comparisons
                            Operator::Lt | Operator::LtEq |
                            Operator::Gt | Operator::GtEq => {
                                // Check if one side is a column reference
                                let has_column = matches!(binary.left.as_ref(), Expr::Column(_))
                                    || matches!(binary.right.as_ref(), Expr::Column(_));
                                if has_column {
                                    Ok(TableProviderFilterPushDown::Inexact)
                                } else {
                                    Ok(TableProviderFilterPushDown::Unsupported)
                                }
                            }
                            // AND/OR can be partially supported
                            Operator::And | Operator::Or => {
                                Ok(TableProviderFilterPushDown::Inexact)
                            }
                            _ => Ok(TableProviderFilterPushDown::Unsupported)
                        }
                    }
                    // Support IS NULL / IS NOT NULL
                    Expr::IsNull(_) | Expr::IsNotNull(_) => {
                        Ok(TableProviderFilterPushDown::Inexact)
                    }
                    // Support BETWEEN
                    Expr::Between(_) => {
                        Ok(TableProviderFilterPushDown::Inexact)
                    }
                    // Support IN list
                    Expr::InList(_) => {
                        Ok(TableProviderFilterPushDown::Inexact)
                    }
                    // Support NOT expressions
                    Expr::Not(_) => {
                        Ok(TableProviderFilterPushDown::Inexact)
                    }
                    // Support LIKE patterns (useful for string filtering)
                    Expr::Like(_) | Expr::SimilarTo(_) => {
                        Ok(TableProviderFilterPushDown::Inexact)
                    }
                    // Unsupported: complex expressions, subqueries, etc.
                    _ => Ok(TableProviderFilterPushDown::Unsupported)
                }
            })
            .collect()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // ============================================================
        // Filter Pushdown Visibility - Log received filters
        // ============================================================
        if filters.is_empty() {
            tracing::info!(
                "[{}] Filter Pushdown: NO filters received (check supports_filters_pushdown implementation)",
                self.table_name
            );
        } else {
            tracing::info!(
                "[{}] Filter Pushdown SUCCESS: Received {} filter(s) from DataFusion optimizer:",
                self.table_name,
                filters.len()
            );
            for (i, filter) in filters.iter().enumerate() {
                tracing::info!("  [{}] Filter {}: {:?}", self.table_name, i + 1, filter);
            }
        }

        // Get all fragments from manifest using public API
        let all_fragments = self.db
            .get_fragments(&self.table_name)
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        // Layer 1: Manifest-based pruning
        let (pruned_fragments, pruning_stats) = self.prune_fragments_by_manifest(&all_fragments, filters);

        tracing::info!(
            "[{}] Manifest Pruning (Layer 1): {} -> {} fragments ({:.1}% pruned, {} rows skipped)",
            self.table_name,
            pruning_stats.total_fragments,
            pruning_stats.remaining_fragments,
            pruning_stats.pruning_ratio() * 100.0,
            pruning_stats.total_rows - pruning_stats.remaining_rows
        );
        
        // Explain if no pruning occurred
        if pruning_stats.pruned_fragments == 0 && !filters.is_empty() {
            tracing::info!(
                "[{}] Note: 0% pruned because filter ranges overlap with all fragment min/max stats.",
                self.table_name
            );
            tracing::info!(
                "[{}] Row filtering will happen at Layer 2 (Zone Map) and Layer 3 (DataFusion Filter).",
                self.table_name
            );
        }

        if pruned_fragments.is_empty() {
            // Return empty result
            return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                self.schema.clone(),
            )));
        }

        // Build partitioned files from pruned fragments
        let partitioned_files = self.build_partitioned_files(&pruned_fragments);
        let file_groups = self.build_file_groups(partitioned_files);

        // Register local filesystem object store with root prefix
        // This allows ObjectPath without leading / to be resolved as absolute paths
        let object_store_url = ObjectStoreUrl::local_filesystem();
        let local_fs: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix("/")
                .map_err(|e| DataFusionError::Execution(format!("Failed to create LocalFileSystem: {e}")))?
        );
        
        state.runtime_env().register_object_store(
            <ObjectStoreUrl as AsRef<url::Url>>::as_ref(&object_store_url),
            local_fs,
        );

        // ============================================================
        // NEW: 步骤 1 - 将逻辑表达式转换为物理表达式
        // ============================================================
        use datafusion::physical_expr::PhysicalExpr;
        use datafusion_common::DFSchema;
        
        let physical_filters: Vec<Arc<dyn PhysicalExpr>> = if !filters.is_empty() {
            // 将 SchemaRef 转换为 DFSchema
            let df_schema = DFSchema::try_from(self.schema.clone())
                .map_err(|e| DataFusionError::Execution(format!("Failed to convert schema: {}", e)))?;
            
            let mut converted_filters = Vec::new();
            for filter in filters {
                match state.create_physical_expr(filter.clone(), &df_schema) {
                    Ok(physical_expr) => {
                        converted_filters.push(physical_expr);
                        tracing::debug!("Converted filter to physical expression: {:?}", filter);
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to convert filter to physical expression: {} - {:?}",
                            e,
                            filter
                        );
                        // 继续处理其他 filters
                    }
                }
            }
            
            if !converted_filters.is_empty() {
                tracing::info!(
                    "Converted {}/{} filters to physical expressions",
                    converted_filters.len(),
                    filters.len()
                );
            }
            
            converted_filters
        } else {
            Vec::new()
        };

        // ============================================================
        // NEW: 步骤 2 - 下推 filters 到 FileSource
        // ============================================================
        let file_source = self.vortex_format.file_source();
        let config_options = state.config_options();
        
        let file_source_with_filters = if !physical_filters.is_empty() {
            match file_source.try_pushdown_filters(physical_filters.clone(), config_options) {
                Ok(pushdown_result) => {
                    // 检查有多少 filters 被成功下推
                    use datafusion_physical_plan::filter_pushdown::PushedDown;
                    let pushed_count = pushdown_result
                        .filters
                        .iter()
                        .filter(|result| matches!(result, PushedDown::Yes))
                        .count();
                    
                    if pushed_count > 0 {
                        tracing::info!(
                            "Pushed down {}/{} filters to VortexFormat for Zone Map pruning",
                            pushed_count,
                            physical_filters.len()
                        );
                    } else {
                        tracing::debug!("No filters were pushed down to VortexFormat (may not be supported yet)");
                    }
                    
                    // 使用更新后的 FileSource（如果有），否则使用原来的
                    pushdown_result.updated_node.unwrap_or_else(|| file_source.clone())
                }
                Err(e) => {
                    tracing::warn!("Failed to pushdown filters: {}", e);
                    file_source.clone()
                }
            }
        } else {
            file_source.clone()
        };

        // ============================================================
        // NEW: 步骤 3 - 使用新的 FileSource（如果 VortexFormat 支持）
        // ============================================================
        // 注意：如果 FileSource 有变化，我们需要使用新的 FileSource
        // 但是 VortexFormat 可能没有直接的方法来更新 FileSource
        // 目前先使用原来的 VortexFormat，filters 会在 FileSource 层面处理
        
        // Build FileScanConfig using the builder
        let config_builder = FileScanConfigBuilder::new(
            object_store_url,
            self.schema.clone(),
            file_source_with_filters,
        )
        .with_file_groups(file_groups)
        .with_limit(limit);

        // Apply projection if specified
        let config_builder = if let Some(proj) = projection {
            config_builder.with_projection(Some(proj.clone()))
        } else {
            config_builder
        };

        let file_scan_config = config_builder.build();

        // Layer 2: Delegate to VortexFormat for Zone Map pruning
        // VortexFormat::create_physical_plan handles:
        // - Opening Vortex files
        // - Zone Map pruning within files (now with pushed-down filters)
        // - Efficient columnar scanning
        self.vortex_format
            .create_physical_plan(state, file_scan_config)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::collections::HashMap;
    use tempfile::tempdir;
    use vortexlake_core::manifest::ColumnStats;

    // Helper function to create test FragmentMetadata
    fn create_test_fragment(
        id: &str,
        column_stats: HashMap<String, ColumnStats>,
        row_count: usize,
    ) -> FragmentMetadata {
        FragmentMetadata {
            id: id.to_string(),
            table_name: "test".to_string(),
            path: format!("data/{}.vortex", id),
            row_count,
            size_bytes: (row_count * 100) as u64,
            created_at: Utc::now(),
            column_stats,
        }
    }

    // Helper function to create column stats for numeric column
    fn create_numeric_stats(min: i64, max: i64) -> ColumnStats {
        ColumnStats {
            min_value: Some(serde_json::json!(min)),
            max_value: Some(serde_json::json!(max)),
            null_count: 0,
            distinct_count: None,
            is_sorted: false,
            is_constant: min == max,
        }
    }

    #[tokio::test]
    async fn test_table_provider_creation() {
        let temp_dir = tempdir().unwrap();

        // This will fail because no database exists, but tests the code path
        let result = VortexLakeTableProvider::new(
            temp_dir.path().to_string_lossy().as_ref(),
            "test_table",
        )
        .await;

        assert!(result.is_err());
    }

    // ============================================================
    // expr_to_json Tests
    // ============================================================

    #[test]
    fn test_expr_to_json_integers() {
        use datafusion::scalar::ScalarValue;

        // Test Int8
        let expr = Expr::Literal(ScalarValue::Int8(Some(8)), None);
        let json = VortexLakeTableProvider::expr_to_json(&expr);
        assert_eq!(json, Some(serde_json::json!(8)));

        // Test Int16
        let expr = Expr::Literal(ScalarValue::Int16(Some(16)), None);
        let json = VortexLakeTableProvider::expr_to_json(&expr);
        assert_eq!(json, Some(serde_json::json!(16)));

        // Test Int32
        let expr = Expr::Literal(ScalarValue::Int32(Some(42)), None);
        let json = VortexLakeTableProvider::expr_to_json(&expr);
        assert_eq!(json, Some(serde_json::json!(42)));

        // Test Int64
        let expr = Expr::Literal(ScalarValue::Int64(Some(1000000)), None);
        let json = VortexLakeTableProvider::expr_to_json(&expr);
        assert_eq!(json, Some(serde_json::json!(1000000)));
    }

    #[test]
    fn test_expr_to_json_unsigned_integers() {
        use datafusion::scalar::ScalarValue;

        // Test UInt8
        let expr = Expr::Literal(ScalarValue::UInt8(Some(255)), None);
        let json = VortexLakeTableProvider::expr_to_json(&expr);
        assert_eq!(json, Some(serde_json::json!(255)));

        // Test UInt16
        let expr = Expr::Literal(ScalarValue::UInt16(Some(65535)), None);
        let json = VortexLakeTableProvider::expr_to_json(&expr);
        assert_eq!(json, Some(serde_json::json!(65535)));

        // Test UInt32
        let expr = Expr::Literal(ScalarValue::UInt32(Some(1000)), None);
        let json = VortexLakeTableProvider::expr_to_json(&expr);
        assert_eq!(json, Some(serde_json::json!(1000)));

        // Test UInt64
        let expr = Expr::Literal(ScalarValue::UInt64(Some(1000000)), None);
        let json = VortexLakeTableProvider::expr_to_json(&expr);
        assert_eq!(json, Some(serde_json::json!(1000000)));
    }

    #[test]
    fn test_expr_to_json_floats() {
        use datafusion::scalar::ScalarValue;

        // Test Float32
        let expr = Expr::Literal(ScalarValue::Float32(Some(3.14)), None);
        let json = VortexLakeTableProvider::expr_to_json(&expr);
        assert!(json.is_some());
        let val = json.unwrap().as_f64().unwrap();
        assert!((val - 3.14).abs() < 0.001);

        // Test Float64
        let expr = Expr::Literal(ScalarValue::Float64(Some(2.71828)), None);
        let json = VortexLakeTableProvider::expr_to_json(&expr);
        assert!(json.is_some());
        let val = json.unwrap().as_f64().unwrap();
        assert!((val - 2.71828).abs() < 0.00001);
    }

    #[test]
    fn test_expr_to_json_string_and_bool() {
        use datafusion::scalar::ScalarValue;

        // Test string
        let expr = Expr::Literal(ScalarValue::Utf8(Some("hello".to_string())), None);
        let json = VortexLakeTableProvider::expr_to_json(&expr);
        assert_eq!(json, Some(serde_json::json!("hello")));

        // Test boolean true
        let expr = Expr::Literal(ScalarValue::Boolean(Some(true)), None);
        let json = VortexLakeTableProvider::expr_to_json(&expr);
        assert_eq!(json, Some(serde_json::json!(true)));

        // Test boolean false
        let expr = Expr::Literal(ScalarValue::Boolean(Some(false)), None);
        let json = VortexLakeTableProvider::expr_to_json(&expr);
        assert_eq!(json, Some(serde_json::json!(false)));
    }

    #[test]
    fn test_expr_to_json_null_values() {
        use datafusion::scalar::ScalarValue;

        // Test null Int32
        let expr = Expr::Literal(ScalarValue::Int32(None), None);
        let json = VortexLakeTableProvider::expr_to_json(&expr);
        assert!(json.is_none());

        // Test null String
        let expr = Expr::Literal(ScalarValue::Utf8(None), None);
        let json = VortexLakeTableProvider::expr_to_json(&expr);
        assert!(json.is_none());
    }

    #[test]
    fn test_expr_to_json_non_literal() {
        use datafusion::logical_expr::col;

        // Column expression is not a literal
        let expr = col("name");
        let json = VortexLakeTableProvider::expr_to_json(&expr);
        assert!(json.is_none());
    }

    // ============================================================
    // extract_comparison Tests
    // ============================================================

    #[test]
    fn test_extract_comparison_column_op_literal() {
        use datafusion::logical_expr::{col, lit, Operator};

        // Test: column > literal (x > 5)
        let left = col("x");
        let right = lit(5i32);
        let result = VortexLakeTableProvider::extract_comparison(
            &left,
            &Operator::Gt,
            &right,
        );
        assert!(result.is_some());
        let (col_name, op, value) = result.unwrap();
        assert_eq!(col_name, "x");
        assert_eq!(op, ">");
        assert_eq!(value, serde_json::json!(5));
    }

    #[test]
    fn test_extract_comparison_all_operators() {
        use datafusion::logical_expr::{col, lit, Operator};

        let column = col("x");
        let value = lit(10i32);

        // Test Eq (=)
        let result = VortexLakeTableProvider::extract_comparison(&column, &Operator::Eq, &value);
        assert_eq!(result.unwrap().1, "=");

        // Test NotEq (!=)
        let result = VortexLakeTableProvider::extract_comparison(&column, &Operator::NotEq, &value);
        assert_eq!(result.unwrap().1, "!=");

        // Test Lt (<)
        let result = VortexLakeTableProvider::extract_comparison(&column, &Operator::Lt, &value);
        assert_eq!(result.unwrap().1, "<");

        // Test LtEq (<=)
        let result = VortexLakeTableProvider::extract_comparison(&column, &Operator::LtEq, &value);
        assert_eq!(result.unwrap().1, "<=");

        // Test Gt (>)
        let result = VortexLakeTableProvider::extract_comparison(&column, &Operator::Gt, &value);
        assert_eq!(result.unwrap().1, ">");

        // Test GtEq (>=)
        let result = VortexLakeTableProvider::extract_comparison(&column, &Operator::GtEq, &value);
        assert_eq!(result.unwrap().1, ">=");
    }

    #[test]
    fn test_extract_comparison_unsupported_operator() {
        use datafusion::logical_expr::{col, lit, Operator};

        let column = col("x");
        let value = lit(10i32);

        // Test unsupported operator (And, Or, etc.)
        let result = VortexLakeTableProvider::extract_comparison(&column, &Operator::And, &value);
        assert!(result.is_none());
    }

    // ============================================================
    // extract_comparison_reversed Tests
    // ============================================================

    #[test]
    fn test_extract_comparison_reversed() {
        use datafusion::logical_expr::{col, lit, Operator};

        // Test: 5 < x becomes x > 5
        let left = lit(5i32);
        let right = col("x");
        let result = VortexLakeTableProvider::extract_comparison_reversed(
            &left,
            &Operator::Lt,
            &right,
        );
        assert!(result.is_some());
        let (col_name, op, value) = result.unwrap();
        assert_eq!(col_name, "x");
        assert_eq!(op, ">"); // Reversed from <
        assert_eq!(value, serde_json::json!(5));

        // Test: 5 > x becomes x < 5
        let result = VortexLakeTableProvider::extract_comparison_reversed(
            &left,
            &Operator::Gt,
            &right,
        );
        assert!(result.is_some());
        let (_, op, _) = result.unwrap();
        assert_eq!(op, "<"); // Reversed from >

        // Test: 5 <= x becomes x >= 5
        let result = VortexLakeTableProvider::extract_comparison_reversed(
            &left,
            &Operator::LtEq,
            &right,
        );
        assert!(result.is_some());
        let (_, op, _) = result.unwrap();
        assert_eq!(op, ">="); // Reversed from <=

        // Test: 5 >= x becomes x <= 5
        let result = VortexLakeTableProvider::extract_comparison_reversed(
            &left,
            &Operator::GtEq,
            &right,
        );
        assert!(result.is_some());
        let (_, op, _) = result.unwrap();
        assert_eq!(op, "<="); // Reversed from >=

        // Test: 5 = x stays = (symmetric)
        let result = VortexLakeTableProvider::extract_comparison_reversed(
            &left,
            &Operator::Eq,
            &right,
        );
        assert!(result.is_some());
        let (_, op, _) = result.unwrap();
        assert_eq!(op, "=");
    }

    // ============================================================
    // PruningStats Tests (via VortexLakeTableProvider)
    // ============================================================

    #[test]
    fn test_pruning_stats_calculation() {
        // Create fragments with different ranges
        let mut stats1 = HashMap::new();
        stats1.insert("age".to_string(), create_numeric_stats(0, 30));
        let f1 = create_test_fragment("f1", stats1, 1000);

        let mut stats2 = HashMap::new();
        stats2.insert("age".to_string(), create_numeric_stats(31, 60));
        let f2 = create_test_fragment("f2", stats2, 2000);

        let mut stats3 = HashMap::new();
        stats3.insert("age".to_string(), create_numeric_stats(61, 100));
        let f3 = create_test_fragment("f3", stats3, 3000);

        let all_fragments = vec![f1, f2, f3];
        let pruned_fragments = vec![all_fragments[1].clone()]; // Only f2 remains

        let stats = PruningStats::from_fragments(&all_fragments, &pruned_fragments);

        assert_eq!(stats.total_fragments, 3);
        assert_eq!(stats.remaining_fragments, 1);
        assert_eq!(stats.pruned_fragments, 2);
        assert_eq!(stats.total_rows, 6000);
        assert_eq!(stats.remaining_rows, 2000);
        assert!((stats.pruning_ratio() - 0.666).abs() < 0.01);
    }

    #[test]
    fn test_pruning_stats_no_pruning() {
        let mut stats = HashMap::new();
        stats.insert("age".to_string(), create_numeric_stats(0, 100));
        let f1 = create_test_fragment("f1", stats, 1000);

        let all_fragments = vec![f1.clone()];
        let pruned_fragments = all_fragments.clone();

        let stats = PruningStats::from_fragments(&all_fragments, &pruned_fragments);

        assert_eq!(stats.pruned_fragments, 0);
        assert_eq!(stats.pruning_ratio(), 0.0);
    }

    #[test]
    fn test_pruning_stats_all_pruned() {
        let mut stats = HashMap::new();
        stats.insert("age".to_string(), create_numeric_stats(0, 30));
        let f1 = create_test_fragment("f1", stats, 1000);

        let all_fragments = vec![f1];
        let pruned_fragments: Vec<FragmentMetadata> = vec![];

        let stats = PruningStats::from_fragments(&all_fragments, &pruned_fragments);

        assert_eq!(stats.remaining_fragments, 0);
        assert_eq!(stats.pruning_ratio(), 1.0);
    }

    // ============================================================
    // Data Type Conversion Tests
    // ============================================================

    #[test]
    fn test_convert_data_type_primitives() {
        use arrow::datatypes::DataType as ArrowDT;
        use datafusion::arrow::datatypes::DataType as DfDT;

        // Test primitive types
        assert!(matches!(
            VortexLakeTableProvider::convert_data_type(&ArrowDT::Boolean),
            DfDT::Boolean
        ));
        assert!(matches!(
            VortexLakeTableProvider::convert_data_type(&ArrowDT::Int32),
            DfDT::Int32
        ));
        assert!(matches!(
            VortexLakeTableProvider::convert_data_type(&ArrowDT::Int64),
            DfDT::Int64
        ));
        assert!(matches!(
            VortexLakeTableProvider::convert_data_type(&ArrowDT::Float32),
            DfDT::Float32
        ));
        assert!(matches!(
            VortexLakeTableProvider::convert_data_type(&ArrowDT::Float64),
            DfDT::Float64
        ));
        assert!(matches!(
            VortexLakeTableProvider::convert_data_type(&ArrowDT::Utf8),
            DfDT::Utf8
        ));
        assert!(matches!(
            VortexLakeTableProvider::convert_data_type(&ArrowDT::Binary),
            DfDT::Binary
        ));
    }

    #[test]
    fn test_convert_data_type_dates() {
        use arrow::datatypes::DataType as ArrowDT;
        use datafusion::arrow::datatypes::DataType as DfDT;

        assert!(matches!(
            VortexLakeTableProvider::convert_data_type(&ArrowDT::Date32),
            DfDT::Date32
        ));
        assert!(matches!(
            VortexLakeTableProvider::convert_data_type(&ArrowDT::Date64),
            DfDT::Date64
        ));
    }

    #[test]
    fn test_convert_data_type_lists() {
        use arrow::datatypes::{DataType as ArrowDT, Field as ArrowField};
        use datafusion::arrow::datatypes::DataType as DfDT;

        // Test List type
        let inner_field = Arc::new(ArrowField::new("item", ArrowDT::Int32, true));
        let list_type = ArrowDT::List(inner_field);
        let converted = VortexLakeTableProvider::convert_data_type(&list_type);
        
        assert!(matches!(converted, DfDT::List(_)));

        // Test FixedSizeList type
        let inner_field = Arc::new(ArrowField::new("item", ArrowDT::Float32, false));
        let fsl_type = ArrowDT::FixedSizeList(inner_field, 128);
        let converted = VortexLakeTableProvider::convert_data_type(&fsl_type);
        
        assert!(matches!(converted, DfDT::FixedSizeList(_, 128)));
    }
}
