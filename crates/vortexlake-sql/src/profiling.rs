//! Comprehensive Query Profiling Tools
//!
//! This module provides detailed query performance analysis including:
//! - Execution plan tree structure
//! - Per-operator metrics (time, rows, I/O)
//! - Bottleneck identification
//! - Comparison between different storage formats

use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::prelude::SessionContext;
use datafusion_physical_plan::{
    ExecutionPlan,
    metrics::MetricsSet,
};
use futures::TryStreamExt;

use crate::metrics_config::{VortexMetricsConfig};

/// Comprehensive query profile with detailed operator-level metrics
#[derive(Debug, Clone)]
pub struct QueryProfile {
    /// origin sql 
    pub origin_sql: String,

    /// Overall query phases
    pub phases: Vec<PhaseMetrics>,
    
    /// Execution plan tree with operator-level metrics
    pub plan_tree: PlanNode,
    
    /// Summary statistics
    pub summary: ProfileSummary,
    
    /// Raw DataFusion physical plan string (native format with full operator details)
    pub raw_physical_plan: String,
}

/// Metrics for a single execution phase
#[derive(Debug, Clone)]
pub struct PhaseMetrics {
    pub name: String,
    pub duration: Duration,
    pub percentage: f64,
}

/// Tree representation of execution plan with metrics
#[derive(Debug, Clone)]
pub struct PlanNode {
    pub operator_type: String,
    pub operator_name: String,
    pub metrics: OperatorMetrics,
    pub children: Vec<PlanNode>,
    pub depth: usize,
}

/// Detailed metrics for a single operator
#[derive(Debug, Clone, Default)]
/// Metrics collected for each physical operator in the execution plan.
/// These metrics help analyze query performance and identify bottlenecks.
///
/// ## Metrics Categories:
/// - **Standard DataFusion Metrics**: Basic operator statistics (CPU time, row counts)
/// - **I/O Metrics**: Storage access patterns (bytes scanned, pushdown pruning)
/// - **Vortex-Specific Metrics**: ZoneMap pruning and internal optimization details
/// - **Time Metrics**: Detailed timing breakdown for performance analysis
///
/// ## Key Vortex vs Parquet Differences:
/// - **ZoneMap Pruning**: Vortex uses logical zones (8k rows) vs Parquet's physical row groups
/// - **Segment I/O**: Vortex tracks segment-level access vs Parquet's page-level access
/// - **Metrics Mapping**: rows_pruned_by_statistics ≈ Parquet's row_groups_pruned_statistics
///
/// ## Usage Notes:
/// - All metrics are optional (Option<T>) as not all operators provide all metrics
/// - Metrics are accumulated across partitions for distributed execution
/// - Some metrics are specific to certain storage formats (Parquet vs Vortex)
/// - Vortex-specific metrics only appear in DataSourceExec operators using Vortex
/// - Standard DataFusion metrics appear in most operators (FilterExec, AggregateExec, etc.)
/// - ⚠️ Four fields marked as "POTENTIALLY UNUSED" should be reviewed for removal
///
/// ## Operator-Specific Metrics:
/// - **DataSourceExec**: All I/O and Vortex-specific metrics
/// - **FilterExec**: input_rows, output_rows, rows_filtered, selectivity
/// - **AggregateExec**: output_rows (input_rows may be from children)
/// - **JoinExec**: output_rows (may have left/right input tracking)
/// - **SortExec**: output_rows (input_rows = output_rows for sorting)
///
/// ## Future Improvements:
/// - Remove unused fields (start_time, end_time, memory_usage, partition)
/// - Add memory tracking integration with DataFusion
/// - Standardize Vortex metrics with DataFusion's metric system
pub struct OperatorMetrics {
    // === Standard DataFusion Metrics ===

    /// CPU time spent in this operator (from DataFusion's baseline metrics)
    /// Used for: Performance profiling, operator cost estimation
    pub elapsed_compute: Option<Duration>,

    /// Total rows produced as output by this operator
    /// Used for: Understanding data flow, verifying correctness
    pub output_rows: Option<usize>,

    /// Input rows fed into this operator (calculated from children, not directly measured)
    /// Used for: Filter selectivity calculation, data reduction analysis
    pub input_rows: Option<usize>,

    /// Rows filtered out by FilterExec (input_rows - output_rows)
    /// Used for: Filter effectiveness measurement
    pub rows_filtered: Option<usize>,

    /// Filter selectivity ratio (output_rows / input_rows)
    /// Used for: Filter efficiency analysis (higher = more selective)
    pub selectivity: Option<f64>,

    /// Bytes spilled to disk during execution (for memory-intensive operators)
    /// Used for: Memory pressure analysis, spill detection
    pub spilled_bytes: Option<usize>,

    /// Rows spilled to disk during execution
    /// Used for: Memory management analysis
    pub spilled_rows: Option<usize>,

    // === I/O and Scan Metrics (from Parquet/Vortex) ===

    /// Total bytes scanned from storage (physical I/O)
    /// Used for: I/O efficiency analysis, storage access patterns
    pub bytes_scanned: Option<usize>,

    /// Rows pruned by pushdown predicates (before data materialization)
    /// Used for: Predicate pushdown effectiveness
    pub pushdown_rows_pruned: Option<usize>,

    /// Rows that matched pushdown predicates (and were kept)
    /// Used for: Pushdown selectivity analysis
    pub pushdown_rows_matched: Option<usize>,

    /// Row groups pruned by file-level statistics (Parquet/Vortex)
    /// Used for: File pruning effectiveness
    pub row_groups_pruned_statistics: Option<usize>,

    /// Row groups that matched file-level statistics
    /// Used for: File-level selectivity analysis
    pub row_groups_matched_statistics: Option<usize>,

    /// Time spent evaluating row-level pushdown filters
    /// Used for: Pushdown performance cost analysis
    pub row_pushdown_eval_time: Option<Duration>,

    /// Time spent loading metadata (schema, statistics, etc.)
    /// Used for: Metadata loading overhead analysis
    pub metadata_load_time: Option<Duration>,

    /// Rows pruned by page-level indexes (Parquet specific)
    /// Used for: Fine-grained pruning analysis
    pub page_index_rows_pruned: Option<usize>,
    
    // === Vortex Logical Pruning Metrics (核心差异层) ===
    // Vortex使用逻辑ZoneMap (每8k行) 而不是物理segment统计
    // 这些metrics展示ZoneMap剪枝的详细效果

    /// Total logical row windows participating in scan (total_rows / ZONE_LEN rounded up)
    /// ZONE_LEN = 8192 (8k rows per zone)
    /// Used for: Understanding ZoneMap coverage, pruning denominator
    pub logical_windows_total: Option<usize>,

    /// Logical windows pruned by ZoneMapLayout min/max statistics
    /// Equivalent to Parquet's row_groups_pruned_statistics
    /// Used for: ZoneMap pruning effectiveness measurement
    pub logical_windows_pruned_statistics: Option<usize>,

    /// Final logical windows that need to be scanned after pruning
    /// Formula: logical_windows_selected = logical_windows_total - logical_windows_pruned_statistics
    /// Used for: Post-pruning scan scope analysis
    pub logical_windows_selected: Option<usize>,

    /// Theoretical rows skipped by ZoneMap pruning (estimated)
    /// **Source**: vortex-scan/src/tasks.rs:118 `rows_pruned_by_statistics.add(pruned_rows)`
    /// Formula: pruned_rows = original_mask_len - remaining_true_count
    /// Used for: Quantifying pruning benefit in row count, ZoneMap effectiveness measurement
    pub rows_pruned_by_statistics: Option<usize>,

    // === Vortex Segment / IO Metrics (Vortex 特有) ===
    // Vortex的物理存储单元是segments，展示实际I/O模式

    /// Number of segments that were touched (at least one slice read)
    /// **Source**: vortex-scan/src/tasks.rs:160 `segments_touched.add(1)` during filter evaluation
    /// Vortex segments are physical storage units (compressed data blocks)
    /// Used for: Physical I/O distribution analysis, measuring segment access efficiency
    pub segments_touched: Option<usize>,

    /// Number of segment slice read operations executed
    /// **Source**: vortex-scan/src/tasks.rs:192 `segment_slices_read.add(1)` after projection_evaluation
    /// A segment can be divided into multiple slices for parallel processing
    /// Example: segments_touched:1, segment_slices_read:2 means 1 segment split into 2 slices
    /// Used for: Parallel I/O analysis, slice efficiency measurement
    pub segment_slices_read: Option<usize>,

    /// Bytes actually read from segments (compressed state)
    /// **Source**: vortex-scan/src/tasks.rs:196 `bytes_read_from_segments.add(estimated_bytes)`
    /// Equivalent to Parquet's bytes_scanned
    /// Used for: Physical I/O volume measurement, compression ratio analysis
    pub bytes_read_from_segments: Option<usize>,

    /// Estimated bytes skipped due to logical pruning (no physical I/O)
    /// Formula: bytes_skipped ≈ rows_pruned * avg_bytes_per_row
    /// Used for: Pruning efficiency in I/O savings
    pub bytes_skipped_by_zonemap: Option<usize>,

    // === Vortex Decode / Materialization Metrics ===
    // 从压缩存储到Arrow Array的转换过程

    /// Rows actually decoded into Arrow Arrays (after compression)
    /// **Source**: vortex-scan/src/tasks.rs:200 `rows_decoded.add(array.len())`
    /// **Lifecycle**:
    ///   1. **Creation**: tasks.rs:49 `let rows_decoded = ctx.metrics.counter("rows_decoded")`
    ///   2. **Recording**: tasks.rs:200 `rows_decoded.add(array.len())` after projection_evaluation
    ///   3. **Extraction**: profiling.rs:495-498 collected from VortexMetricsFinder
    ///   4. **Display**: profiling.rs:849-852 shown in DataSourceExec profile
    ///
    /// May differ significantly from output_rows due to:
    /// - **Pushdown filtering**: Decode first, filter second (vortex-scan pruning logic)
    /// - **ZoneMap granularity**: Must decode entire 8k-row blocks (8192 rows each)
    /// - **Query complexity**: Subqueries/JOINs need broader data access
    /// - **Batch processing**: Arrow RecordBatch boundaries cause over-counting
    ///
    /// ⚠️ **Can be >> output_rows** (e.g., 28,596 decoded → 15,000 output after filtering)
    /// Used for: Measuring decode overhead vs final result size, ZoneMap efficiency analysis
    pub rows_decoded: Option<usize>,

    /// Number of RecordBatches emitted by ScanExec
    /// Used for: Batch size and memory efficiency analysis
    pub record_batches_emitted: Option<usize>,

    // === Vortex Time Metrics ===
    // 详细的时间分解，便于性能瓶颈识别

    /// Time spent on ZoneMapLayout + predicate evaluation (nanoseconds)
    /// **Source**: vortex-scan/src/tasks.rs:112 `time_pruning_ns.update(pruning_start.elapsed())`
    /// Used for: Pruning overhead analysis (should be minimal compared to I/O time)
    pub time_pruning_ns: Option<Duration>,

    /// Time spent on segment slice reads (nanoseconds)
    /// Used for: I/O time vs CPU time analysis
    pub time_io_ns: Option<Duration>,

    /// Time spent on FlatLayout decoding (nanoseconds)
    /// Used for: Codec performance analysis
    pub time_decode_ns: Option<Duration>,

    // === Miscellaneous Metrics ===

    /// All other metrics not covered above (name -> value as string)
    /// Used for: Capturing additional operator-specific metrics
    pub other_metrics: Vec<(String, String)>,

    // === POTENTIALLY UNUSED FIELDS ===
    // These fields are defined but may not be actively used in current profiling

    /// Start timestamp of operator execution
    /// ⚠️ POTENTIALLY UNUSED: Defined but not populated or used in profiling logic
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,

    /// End timestamp of operator execution
    /// ⚠️ POTENTIALLY UNUSED: Defined but not populated or used in profiling logic
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,

    /// Memory usage during operator execution
    /// ⚠️ POTENTIALLY UNUSED: Defined but not populated from DataFusion metrics
    pub memory_usage: Option<usize>,

    /// Partition information for distributed execution
    /// ⚠️ POTENTIALLY UNUSED: Defined but not populated or used in current implementation
    pub partition: Option<usize>,
}

/// Summary statistics for the entire query
#[derive(Debug, Clone, Default)]
pub struct ProfileSummary {
    pub total_time: Duration,
    pub total_rows_processed: usize,
    pub total_bytes_read: usize,
    pub total_bytes_written: usize,
    pub operator_count: usize,
    pub max_depth: usize,
    pub slowest_operator: Option<String>,
    pub largest_operator: Option<String>,
    
    // Filter statistics
    pub total_rows_filtered: usize,
    pub filter_selectivity: Option<f64>,
}

impl QueryProfile {
    pub fn new() -> Self {
        Self {
            origin_sql: String::new(),
            phases: Vec::new(),
            plan_tree: PlanNode::new("Root", "Root"),
            summary: ProfileSummary::default(),
            raw_physical_plan: String::new(),
        }
    }
    
    /// Set the raw DataFusion physical plan string
    pub fn set_raw_physical_plan(&mut self, plan: impl Into<String>) {
        self.raw_physical_plan = plan.into();
    }
    
    pub fn set_origin_sql(&mut self, sql: impl Into<String>) {
        self.origin_sql = sql.into().trim().to_string();
    }
    
    /// Add a phase metric
    pub fn add_phase(&mut self, name: impl Into<String>, duration: Duration) {
        self.phases.push(PhaseMetrics {
            name: name.into(),
            duration,
            percentage: 0.0, // Will be calculated later
        });
    }
    
    /// Calculate percentages for all phases
    pub fn finalize(&mut self) {
        let total: Duration = self.phases.iter().map(|p| p.duration).sum();
        for phase in &mut self.phases {
            if total.as_secs_f64() > 0.0 {
                phase.percentage = (phase.duration.as_secs_f64() / total.as_secs_f64()) * 100.0;
            }
        }
        self.summary.total_time = total;
    }
    
    /// Build plan tree from execution plan (after execution, metrics are available)
    pub fn build_plan_tree(&mut self, plan: &Arc<dyn ExecutionPlan>, _root_metrics: Option<&MetricsSet>) {
        // Recursively build tree and collect metrics from each node
        self.plan_tree = Self::build_node(plan, 0);
        self.summary.max_depth = self.plan_tree.max_depth();
        self.summary.operator_count = self.plan_tree.count_nodes();
        self.analyze_bottlenecks();
    }
    
    fn build_node(plan: &Arc<dyn ExecutionPlan>, depth: usize) -> PlanNode {
        // Use the name() method from ExecutionPlan trait
        let operator_name = plan.name().to_string();
        let operator_type = format!("{:?}", plan.as_any().type_id());
        
        let mut node_metrics = OperatorMetrics::default();
        
        // Extract metrics from this node (available after execution)
        if let Some(metrics) = plan.metrics() {
            // Standard metrics
            node_metrics.output_rows = metrics.output_rows();
            node_metrics.spilled_bytes = metrics.spilled_bytes();
            node_metrics.spilled_rows = metrics.spilled_rows();

            // Extract elapsed_compute time (returns nanoseconds as usize)
            if let Some(nanos) = metrics.elapsed_compute() {
                node_metrics.elapsed_compute = Some(Duration::from_nanos(nanos as u64));
            }

            // Extract all other metrics from the MetricsSet
            Self::extract_all_metrics(&metrics, &mut node_metrics);
        }

        // Extract Vortex-specific metrics using VortexMetricsFinder： very details for every metric
        use vortex_datafusion::metrics::VortexMetricsFinder;
        let vortex_metrics_sets = VortexMetricsFinder::find_all(&**plan);
        // println!("🔍 VORTEX METRICS DEBUG: Found {} Vortex metrics sets", vortex_metrics_sets.len());

        // Debug: Check if Vortex metrics are being found
        if vortex_metrics_sets.is_empty() {
            tracing::warn!("⚠️ No Vortex metrics sets found for DataSourceExec node");
        } else {
            // println!("✅ Found {} Vortex metrics sets with total {} metrics",
            //     vortex_metrics_sets.len(),
            //     vortex_metrics_sets.iter().map(|set| set.iter().count()).sum::<usize>());

            // Debug: Print all metric names
            // for (i, metrics_set) in vortex_metrics_sets.iter().enumerate() {
            //     println!("  Metrics set {} contains:", i);
            //     for metric in metrics_set.iter() {
            //         println!("    - {}: {:?}", metric.value().name(), metric.value());
            //     }
            // }
        }

        for (i, metrics_set) in vortex_metrics_sets.iter().enumerate() {
            Self::extract_all_metrics(&metrics_set, &mut node_metrics);
        }
        
        // Build children recursively
        let children: Vec<PlanNode> = plan
            .children()
            .iter()
            .map(|child| Self::build_node(child, depth + 1))
            .collect();
        
        // Calculate rows statistics for various operators
        // Input rows = find the first valid output_rows in the subtree
        // (RepartitionExec and some other operators don't expose output_rows)

        // For FilterExec - standard filter statistics
        if operator_name.contains("Filter") && !children.is_empty() {
            let input_rows = Self::find_input_rows_in_subtree(&children);

            if input_rows > 0 {
                node_metrics.input_rows = Some(input_rows);

                if let Some(output) = node_metrics.output_rows {
                    node_metrics.rows_filtered = Some(input_rows.saturating_sub(output));
                    node_metrics.selectivity = Some(output as f64 / input_rows as f64);
                }
            }
        }

        // For DataSource/Scan operations - show scan output even without explicit filter
        else if operator_name.contains("DataSource") || operator_name.contains("Scan") {
            // For scan operations, we can show output_rows as a basic metric
            // This helps identify if data is being read but not filtered
            if let Some(output) = node_metrics.output_rows {
                tracing::debug!("Scan operation produced {} output rows", output);
            }
        }

        // For Join operations - show join statistics
        else if operator_name.contains("Join") && !children.is_empty() {
            // Try to get input rows from children for join statistics
            let left_rows = children.get(0).and_then(|c| c.metrics.output_rows).unwrap_or(0);
            let right_rows = children.get(1).and_then(|c| c.metrics.output_rows).unwrap_or(0);

            if left_rows > 0 || right_rows > 0 {
                tracing::debug!("Join operation: left={} rows, right={} rows, output={:?}",
                    left_rows, right_rows, node_metrics.output_rows);
            }
        }
        
        PlanNode {
            operator_type,
            operator_name,
            metrics: node_metrics,
            children,
            depth,
        }
    }
    
    /// Extract all available metrics from MetricsSet
    fn extract_all_metrics(metrics: &MetricsSet, node_metrics: &mut OperatorMetrics) {
        use datafusion_physical_plan::metrics::MetricValue;
        
        // Track scan time components for aggregation
        let mut scan_total_nanos: u64 = 0;
        
        for metric in metrics.iter() {
            let _name = metric.value().name();
            match metric.value() {
                MetricValue::Count { name, count } => {
                    let value = count.value();
                    match name.as_ref() {
                        "bytes_scanned" => {
                            // Accumulate bytes_scanned from all partitions
                            let current = node_metrics.bytes_scanned.unwrap_or(0);
                            node_metrics.bytes_scanned = Some(current + value);
                        }
                        "pushdown_rows_pruned" => {
                            let current = node_metrics.pushdown_rows_pruned.unwrap_or(0);
                            node_metrics.pushdown_rows_pruned = Some(current + value);
                        }
                        "pushdown_rows_matched" => {
                            let current = node_metrics.pushdown_rows_matched.unwrap_or(0);
                            node_metrics.pushdown_rows_matched = Some(current + value);
                        }
                        "row_groups_pruned_statistics" => {
                            let current = node_metrics.row_groups_pruned_statistics.unwrap_or(0);
                            node_metrics.row_groups_pruned_statistics = Some(current + value);
                        }
                        "row_groups_matched_statistics" => {
                            let current = node_metrics.row_groups_matched_statistics.unwrap_or(0);
                            node_metrics.row_groups_matched_statistics = Some(current + value);
                        }
                        "page_index_rows_pruned" => {
                            let current = node_metrics.page_index_rows_pruned.unwrap_or(0);
                            node_metrics.page_index_rows_pruned = Some(current + value);
                        }
                        // Vortex Logical Pruning Metrics
                        "logical_windows_total" => {
                            let current = node_metrics.logical_windows_total.unwrap_or(0);
                            node_metrics.logical_windows_total = Some(current + value);
                        }
                        "logical_windows_pruned_statistics" => {
                            let current = node_metrics.logical_windows_pruned_statistics.unwrap_or(0);
                            node_metrics.logical_windows_pruned_statistics = Some(current + value);
                            // Also map to DataFusion's row_groups_pruned_statistics
                            let current_df = node_metrics.row_groups_pruned_statistics.unwrap_or(0);
                            node_metrics.row_groups_pruned_statistics = Some(current_df + value);
                        }
                        "logical_windows_selected" => {
                            let current = node_metrics.logical_windows_selected.unwrap_or(0);
                            node_metrics.logical_windows_selected = Some(current + value);
                        }
                        "rows_pruned_by_statistics" => {
                            let current: usize = node_metrics.rows_pruned_by_statistics.unwrap_or(0);
                            node_metrics.rows_pruned_by_statistics = Some(current + value);
                            // Also map to DataFusion's pushdown_rows_pruned
                            let current_df = node_metrics.pushdown_rows_pruned.unwrap_or(0);
                            node_metrics.pushdown_rows_pruned = Some(current_df + value);
                        }
                        // Vortex Segment / IO Metrics
                        "segments_touched" => {
                            let current = node_metrics.segments_touched.unwrap_or(0);
                            node_metrics.segments_touched = Some(current + value);
                        }
                        "segment_slices_read" => {
                            let current = node_metrics.segment_slices_read.unwrap_or(0);
                            node_metrics.segment_slices_read = Some(current + value);
                        }
                        "bytes_read_from_segments" => {
                            let current = node_metrics.bytes_read_from_segments.unwrap_or(0);
                            node_metrics.bytes_read_from_segments = Some(current + value);
                        }
                        "bytes_skipped_by_zonemap" => {
                            let current = node_metrics.bytes_skipped_by_zonemap.unwrap_or(0);
                            node_metrics.bytes_skipped_by_zonemap = Some(current + value);
                        }
                        // Vortex Decode / Materialization Metrics
                        "rows_decoded" => {
                            let current = node_metrics.rows_decoded.unwrap_or(0);
                            node_metrics.rows_decoded = Some(current + value);
                        }
                        "record_batches_emitted" => {
                            let current = node_metrics.record_batches_emitted.unwrap_or(0);
                            node_metrics.record_batches_emitted = Some(current + value);
                        }
                        _ => {
                            // Skip other count metrics to reduce noise
                        }
                    }
                }
                MetricValue::Time { name, time } => {
                    let nanos = time.value();
                    let duration = Duration::from_nanos(nanos as u64);
                    match name.as_ref() {
                        "row_pushdown_eval_time" => node_metrics.row_pushdown_eval_time = Some(duration),
                        "metadata_load_time" => {
                            // Accumulate metadata_load_time
                            let current = node_metrics.metadata_load_time.unwrap_or(Duration::ZERO);
                            node_metrics.metadata_load_time = Some(current + duration);
                        }
                        "time_elapsed_scanning_total" => {
                            // Accumulate scan time from all partitions (use max instead of sum for parallel)
                            scan_total_nanos = scan_total_nanos.max(nanos as u64);
                        }
                        // Vortex Time Metrics
                        "time_pruning_ns" => {
                            let current = node_metrics.time_pruning_ns.unwrap_or(Duration::ZERO);
                            node_metrics.time_pruning_ns = Some(current + duration);
                        }
                        "time_io_ns" => {
                            let current = node_metrics.time_io_ns.unwrap_or(Duration::ZERO);
                            node_metrics.time_io_ns = Some(current + duration);
                        }
                        "time_decode_ns" => {
                            let current = node_metrics.time_decode_ns.unwrap_or(Duration::ZERO);
                            node_metrics.time_decode_ns = Some(current + duration);
                        }
                        _ => {
                            // Skip other time metrics to reduce noise
                        }
                    }
                }
                MetricValue::Gauge { .. } => {
                    // Skip gauge metrics
                }
                MetricValue::StartTimestamp { .. } | MetricValue::EndTimestamp { .. } => {
                    // Skip timestamps
                }
                _ => {
                    // Skip other types
                }
            }
        }
        
        // Store aggregated scan time
        if scan_total_nanos > 0 {
            node_metrics.other_metrics.push((
                "scan_time".to_string(),
                format!("{:.2}ms", scan_total_nanos as f64 / 1_000_000.0)
            ));
        }
    }
    
    /// Recursively find the first valid output_rows in the subtree
    /// This is needed because operators like RepartitionExec don't expose output_rows
    fn find_input_rows_in_subtree(children: &[PlanNode]) -> usize {
        for child in children {
            // First check if this child has output_rows
            if let Some(rows) = child.metrics.output_rows {
                return rows;
            }
            // If not, recursively check its children
            let nested_rows = Self::find_input_rows_in_subtree(&child.children);
            if nested_rows > 0 {
                return nested_rows;
            }
        }
        0
    }
    
    fn analyze_bottlenecks(&mut self) {
        let mut slowest_time = Duration::ZERO;
        let mut slowest_name = None;
        let mut largest_rows = 0;
        let mut largest_name = None;
        let mut total_filtered = 0usize;
        let mut total_input_rows = 0usize;
        let mut total_output_rows = 0usize;
        
        self.plan_tree.visit(&mut |node| {
            if let Some(time) = node.metrics.elapsed_compute {
                if time > slowest_time {
                    slowest_time = time;
                    slowest_name = Some(node.operator_name.clone());
                }
            }
            if let Some(rows) = node.metrics.output_rows {
                if rows > largest_rows {
                    largest_rows = rows;
                    largest_name = Some(node.operator_name.clone());
                }
            }
            
            // Collect filter statistics
            if node.operator_name.contains("Filter") {
                if let Some(filtered) = node.metrics.rows_filtered {
                    total_filtered += filtered;
                }
                if let Some(input) = node.metrics.input_rows {
                    total_input_rows += input;
                }
                if let Some(output) = node.metrics.output_rows {
                    total_output_rows += output;
                }
            }
        });
        
        self.summary.slowest_operator = slowest_name;
        self.summary.largest_operator = largest_name;
        self.summary.total_rows_processed = largest_rows;
        self.summary.total_rows_filtered = total_filtered;
        
        if total_input_rows > 0 {
            self.summary.filter_selectivity = Some(total_output_rows as f64 / total_input_rows as f64);
        }
    }
    
    /// Print detailed profile report
    pub fn print(&self, metrics_config: &VortexMetricsConfig) {
        println!("Origin SQL:\n {}", self.origin_sql);
        self.print_phases();
        self.print_raw_physical_plan();
        self.print_plan_tree(metrics_config);
        self.print_summary();
    }
    
    fn print_raw_physical_plan(&self) {
        println!("\n{}", "=".repeat(80));
        println!("DataFusion Physical Plan (Native Format)");
        println!("{}", "=".repeat(80));
        println!("{}", self.raw_physical_plan);
    }
    
    fn print_phases(&self) {
        println!("\n{}", "=".repeat(80));
        println!("Execution Phases");
        println!("{}", "=".repeat(80));
        println!("{:<40} {:>15} {:>10} {:>10}", "Phase", "Time (ms)", "Percentage", "Cumulative");
        println!("{}", "-".repeat(80));
        
        let mut cumulative = Duration::ZERO;
        for phase in &self.phases {
            cumulative += phase.duration;
            let cumulative_pct = if self.summary.total_time.as_secs_f64() > 0.0 {
                (cumulative.as_secs_f64() / self.summary.total_time.as_secs_f64()) * 100.0
            } else {
                0.0
            };
            println!("{:<40} {:>15.2} {:>9.1}% {:>9.1}%",
                phase.name,
                phase.duration.as_secs_f64() * 1000.0,
                phase.percentage,
                cumulative_pct
            );
        }
        println!("{}", "-".repeat(80));
        println!("{:<40} {:>15.2} {:>9.1}% {:>9.1}%",
            "TOTAL",
            self.summary.total_time.as_secs_f64() * 1000.0,
            100.0,
            100.0
        );
    }
    
    fn print_plan_tree(&self, metrics_config: &VortexMetricsConfig) {
        println!("\n{}", "=".repeat(80));
        println!("Execution Plan Tree");
        println!("{}", "=".repeat(80));
        self.plan_tree.print_tree(metrics_config);
    }
    
    fn print_summary(&self) {
        println!("\n{}", "=".repeat(80));
        println!("Profile Summary");
        println!("{}", "=".repeat(80));
        println!("Total Execution Time:     {:.2} ms", 
            self.summary.total_time.as_secs_f64() * 1000.0);
        println!("Total Operators:           {}", self.summary.operator_count);
        println!("Plan Depth:                {}", self.summary.max_depth);
        println!("Total Rows Processed:      {}", self.summary.total_rows_processed);
        
        // Filter statistics
        if self.summary.total_rows_filtered > 0 {
            println!("\n--- Filter Statistics (Layer 3: DataFusion Filter) ---");
            println!("Rows Filtered:             {}", self.summary.total_rows_filtered);
            if let Some(selectivity) = self.summary.filter_selectivity {
                println!("Filter Selectivity:        {:.2}% pass rate", selectivity * 100.0);
            }
        }
        
        if let Some(ref slowest) = self.summary.slowest_operator {
            println!("\nSlowest Operator:          {}", slowest);
        }
        if let Some(ref largest) = self.summary.largest_operator {
            println!("Largest Operator (rows):   {}", largest);
        }
    }
}

impl PlanNode {
    fn new(operator_type: impl Into<String>, operator_name: impl Into<String>) -> Self {
        Self {
            operator_type: operator_type.into(),
            operator_name: operator_name.into(),
            metrics: OperatorMetrics::default(),
            children: Vec::new(),
            depth: 0,
        }
    }
    
    fn max_depth(&self) -> usize {
        let child_max = self.children.iter()
            .map(|c| c.max_depth())
            .max()
            .unwrap_or(0);
        self.depth.max(child_max)
    }
    
    fn count_nodes(&self) -> usize {
        1 + self.children.iter().map(|c| c.count_nodes()).sum::<usize>()
    }
    
    fn visit<F>(&self, f: &mut F)
    where
        F: FnMut(&PlanNode),
    {
        f(self);
        for child in &self.children {
            child.visit(f);
        }
    }
    
    fn print_tree(&self, metrics_config: &VortexMetricsConfig) {
        self.print_node("", true, metrics_config);
    }
    
    fn print_node(&self, prefix: &str, is_last: bool, metrics_config: &VortexMetricsConfig) {
        let connector = if is_last { "└── " } else { "├── " };
        let next_prefix = if is_last { "    " } else { "│   " };
        
        // Build metrics string
        let mut metrics_parts = Vec::new();
        
        // For Filter operators, show filter statistics
        if self.operator_name.contains("Filter") {
            if let (Some(input), Some(output)) = (self.metrics.input_rows, self.metrics.output_rows) {
                let filtered = input.saturating_sub(output);
                let selectivity = if input > 0 { 
                    (output as f64 / input as f64) * 100.0 
                } else { 
                    100.0 
                };
                metrics_parts.push(format!("in:{} -> out:{} (filtered:{}, {:.1}% pass)", 
                    input, output, filtered, selectivity));
            } else if let Some(rows) = self.metrics.output_rows {
                metrics_parts.push(format!("rows:{}", rows));
            }
        } else {
            if let Some(rows) = self.metrics.output_rows {
                metrics_parts.push(format!("rows:{}", rows));
            }
        }
        
        // I/O metrics for DataSourceExec (Parquet/Vortex scan)
        if self.operator_name.contains("DataSource") || self.operator_name.contains("Scan") {
            // Show scan time (most important I/O metric)
            for (name, value) in &self.metrics.other_metrics {
                if name == "scan_time" {
                    metrics_parts.push(format!("scan:{}", value));
                }
            }
            if let Some(bytes) = self.metrics.bytes_scanned {
                if bytes > 0 {
                    metrics_parts.push(format!("read:{}", Self::format_bytes(bytes)));
                }
            }
            if let Some(pruned) = self.metrics.pushdown_rows_pruned {
                if pruned > 0 {
                    let matched = self.metrics.pushdown_rows_matched.unwrap_or(0);
                    let total = pruned + matched;
                    let pct = if total > 0 { (pruned as f64 / total as f64) * 100.0 } else { 0.0 };
                    metrics_parts.push(format!("pruned:{} ({:.1}%)", pruned, pct));
                }
            }
            if let Some(rg_pruned) = self.metrics.row_groups_pruned_statistics {
                if rg_pruned > 0 {
                    let rg_matched = self.metrics.row_groups_matched_statistics.unwrap_or(0);
                    metrics_parts.push(format!("rg_pruned:{}/{}", rg_pruned, rg_pruned + rg_matched));
                }
            }
            
            // Vortex Logical Pruning Metrics
            if let Some(windows_total) = self.metrics.logical_windows_total {
                if let Some(windows_pruned) = self.metrics.logical_windows_pruned_statistics {
                    if windows_pruned > 0 {
                        let windows_selected = self.metrics.logical_windows_selected.unwrap_or(0);
                        let prune_pct = if windows_total > 0 {
                            (windows_pruned as f64 / windows_total as f64) * 100.0
                        } else {
                            0.0
                        };
                        metrics_parts.push(format!(
                            "logical_windows:{}/{} pruned ({}%), selected:{}",
                            windows_pruned, windows_total, prune_pct as u32, windows_selected
                        ));
                    }
                }
            }
            
            if let Some(rows_pruned) = self.metrics.rows_pruned_by_statistics {
                if rows_pruned > 0 {
                    metrics_parts.push(format!("rows_pruned_by_zonemap:{}", rows_pruned));
                }
            }
            
            // Vortex Segment / IO Metrics
            if let Some(segments) = self.metrics.segments_touched {
                if segments > 0 {
                    metrics_parts.push(format!("segments_touched:{}", segments));
                }
            }
            
            if let Some(slices) = self.metrics.segment_slices_read {
                if slices > 0 {
                    metrics_parts.push(format!("segment_slices_read:{}", slices));
                }
            }
            
            if let Some(bytes) = self.metrics.bytes_read_from_segments {
                if bytes > 0 {
                    metrics_parts.push(format!("bytes_read_from_segments:{}", Self::format_bytes(bytes)));
                }
            }
            
            if let Some(bytes_skipped) = self.metrics.bytes_skipped_by_zonemap {
                if bytes_skipped > 0 {
                    metrics_parts.push(format!("bytes_skipped_by_zonemap:{}", Self::format_bytes(bytes_skipped)));
                }
            }
            
            // Vortex Decode Metrics - configurable isolation mode
            if let Some(rows_decoded) = self.metrics.rows_decoded {
                if rows_decoded > 0 {
                    let suffix = metrics_config.metrics_name_suffix();
                    let help_text = metrics_config.metrics_help_text();
                    metrics_parts.push(format!("rows_decoded{}:{}{}", suffix, rows_decoded, help_text));
                }
            }
            
            if let Some(batches) = self.metrics.record_batches_emitted {
                if batches > 0 {
                    metrics_parts.push(format!("record_batches_emitted:{}", batches));
                }
            }
            
            // Vortex Time Metrics
            if let Some(time) = self.metrics.time_pruning_ns {
                if time.as_nanos() > 0 {
                    metrics_parts.push(format!("time_pruning:{:.2}ms", time.as_secs_f64() * 1000.0));
                }
            }
            
            if let Some(time) = self.metrics.time_io_ns {
                if time.as_nanos() > 0 {
                    metrics_parts.push(format!("time_io:{:.2}ms", time.as_secs_f64() * 1000.0));
                }
            }
            
            if let Some(time) = self.metrics.time_decode_ns {
                if time.as_nanos() > 0 {
                    metrics_parts.push(format!("time_decode:{:.2}ms", time.as_secs_f64() * 1000.0));
                }
            }
        }
        
        if let Some(bytes) = self.metrics.spilled_bytes {
            if bytes > 0 {
                metrics_parts.push(format!("spilled:{}", Self::format_bytes(bytes)));
            }
        }
        if let Some(time) = self.metrics.elapsed_compute {
            metrics_parts.push(format!("time:{:.2}ms", time.as_secs_f64() * 1000.0));
        }
        
        let metrics_str = if metrics_parts.is_empty() {
            String::new()
        } else {
            format!(" [{}]", metrics_parts.join(", "))
        };
        
        println!("{}{}{}{}", prefix, connector, self.operator_name, metrics_str);
        
        // Print children
        for (i, child) in self.children.iter().enumerate() {
            let is_last_child = i == self.children.len() - 1;
            child.print_node(&format!("{}{}", prefix, next_prefix), is_last_child, metrics_config);
        }
    }
    
    /// Format bytes in human readable format
    fn format_bytes(bytes: usize) -> String {
        if bytes >= 1024 * 1024 * 1024 {
            format!("{:.2}GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
        } else if bytes >= 1024 * 1024 {
            format!("{:.2}MB", bytes as f64 / (1024.0 * 1024.0))
        } else if bytes >= 1024 {
            format!("{:.2}KB", bytes as f64 / 1024.0)
        } else {
            format!("{}B", bytes)
        }
    }
    
    fn write_tree<W: std::io::Write>(&self, writer: &mut W, prefix: &str, is_last: bool, metrics_config: &VortexMetricsConfig) -> std::io::Result<()> {
        let connector = if is_last { "└── " } else { "├── " };
        let next_prefix = if is_last { "    " } else { "│   " };
        
        // Build metrics string
        let mut metrics_parts = Vec::new();
        
        // For Filter operators, show filter statistics
        if self.operator_name.contains("Filter") {
            if let (Some(input), Some(output)) = (self.metrics.input_rows, self.metrics.output_rows) {
                let filtered = input.saturating_sub(output);
                let selectivity = if input > 0 { 
                    (output as f64 / input as f64) * 100.0 
                } else { 
                    100.0 
                };
                metrics_parts.push(format!("in:{} -> out:{} (filtered:{}, {:.1}% pass)", 
                    input, output, filtered, selectivity));
            } else if let Some(rows) = self.metrics.output_rows {
                metrics_parts.push(format!("rows:{}", rows));
            }
        } else {
            if let Some(rows) = self.metrics.output_rows {
                metrics_parts.push(format!("rows:{}", rows));
            }
        }
        
        // I/O metrics for DataSourceExec (Parquet/Vortex scan)
        if self.operator_name.contains("DataSource") || self.operator_name.contains("Scan") {
            // Show scan time (most important I/O metric)
            for (name, value) in &self.metrics.other_metrics {
                if name == "scan_time" {
                    metrics_parts.push(format!("scan:{}", value));
                }
            }
            if let Some(bytes) = self.metrics.bytes_scanned {
                if bytes > 0 {
                    metrics_parts.push(format!("read:{}", Self::format_bytes(bytes)));
                }
            }
            if let Some(pruned) = self.metrics.pushdown_rows_pruned {
                if pruned > 0 {
                    let matched = self.metrics.pushdown_rows_matched.unwrap_or(0);
                    let total = pruned + matched;
                    let pct = if total > 0 { (pruned as f64 / total as f64) * 100.0 } else { 0.0 };
                    metrics_parts.push(format!("pruned:{} ({:.1}%)", pruned, pct));
                }
            }
            if let Some(rg_pruned) = self.metrics.row_groups_pruned_statistics {
                if rg_pruned > 0 {
                    let rg_matched = self.metrics.row_groups_matched_statistics.unwrap_or(0);
                    metrics_parts.push(format!("rg_pruned:{}/{}", rg_pruned, rg_pruned + rg_matched));
                }
            }
            
            // Vortex Logical Pruning Metrics
            if let Some(windows_total) = self.metrics.logical_windows_total {
                if let Some(windows_pruned) = self.metrics.logical_windows_pruned_statistics {
                    if windows_pruned > 0 {
                        let windows_selected = self.metrics.logical_windows_selected.unwrap_or(0);
                        let prune_pct = if windows_total > 0 {
                            (windows_pruned as f64 / windows_total as f64) * 100.0
                        } else {
                            0.0
                        };
                        metrics_parts.push(format!(
                            "logical_windows:{}/{} pruned ({}%), selected:{}",
                            windows_pruned, windows_total, prune_pct as u32, windows_selected
                        ));
                    }
                }
            }
            
            if let Some(rows_pruned) = self.metrics.rows_pruned_by_statistics {
                if rows_pruned > 0 {
                    metrics_parts.push(format!("rows_pruned_by_zonemap:{}", rows_pruned));
                }
            }
            
            // Vortex Segment / IO Metrics
            if let Some(segments) = self.metrics.segments_touched {
                if segments > 0 {
                    metrics_parts.push(format!("segments_touched:{}", segments));
                }
            }
            
            if let Some(slices) = self.metrics.segment_slices_read {
                if slices > 0 {
                    metrics_parts.push(format!("segment_slices_read:{}", slices));
                }
            }
            
            if let Some(bytes) = self.metrics.bytes_read_from_segments {
                if bytes > 0 {
                    metrics_parts.push(format!("bytes_read_from_segments:{}", Self::format_bytes(bytes)));
                }
            }
            
            if let Some(bytes_skipped) = self.metrics.bytes_skipped_by_zonemap {
                if bytes_skipped > 0 {
                    metrics_parts.push(format!("bytes_skipped_by_zonemap:{}", Self::format_bytes(bytes_skipped)));
                }
            }
            
            // Vortex Decode Metrics - configurable isolation mode
            if let Some(rows_decoded) = self.metrics.rows_decoded {
                if rows_decoded > 0 {
                    let suffix = metrics_config.metrics_name_suffix();
                    let help_text = metrics_config.metrics_help_text();
                    metrics_parts.push(format!("rows_decoded{}:{}{}", suffix, rows_decoded, help_text));
                }
            }
            
            if let Some(batches) = self.metrics.record_batches_emitted {
                if batches > 0 {
                    metrics_parts.push(format!("record_batches_emitted:{}", batches));
                }
            }
            
            // Vortex Time Metrics
            if let Some(time) = self.metrics.time_pruning_ns {
                if time.as_nanos() > 0 {
                    metrics_parts.push(format!("time_pruning:{:.2}ms", time.as_secs_f64() * 1000.0));
                }
            }
            
            if let Some(time) = self.metrics.time_io_ns {
                if time.as_nanos() > 0 {
                    metrics_parts.push(format!("time_io:{:.2}ms", time.as_secs_f64() * 1000.0));
                }
            }
            
            if let Some(time) = self.metrics.time_decode_ns {
                if time.as_nanos() > 0 {
                    metrics_parts.push(format!("time_decode:{:.2}ms", time.as_secs_f64() * 1000.0));
                }
            }
        }
        
        if let Some(bytes) = self.metrics.spilled_bytes {
            if bytes > 0 {
                metrics_parts.push(format!("spilled:{}", Self::format_bytes(bytes)));
            }
        }
        if let Some(time) = self.metrics.elapsed_compute {
            metrics_parts.push(format!("time:{:.2}ms", time.as_secs_f64() * 1000.0));
        }
        
        let metrics_str = if metrics_parts.is_empty() {
            String::new()
        } else {
            format!(" [{}]", metrics_parts.join(", "))
        };
        
        writeln!(writer, "{}{}{}{}", prefix, connector, self.operator_name, metrics_str)?;
        
        // Write children
        for (i, child) in self.children.iter().enumerate() {
            let is_last_child = i == self.children.len() - 1;
            child.write_tree(writer, &format!("{}{}", prefix, next_prefix), is_last_child, metrics_config)?;
        }
        
        Ok(())
    }
}

/// Execute query with comprehensive profiling
pub async fn execute_with_full_profile(
    ctx: &SessionContext,
    sql: &str,
) -> anyhow::Result<(Vec<arrow::record_batch::RecordBatch>, QueryProfile)> {
    let mut profile = QueryProfile::new();
    profile.set_origin_sql(sql);
    
    // Phase 1: SQL Parsing
    let parse_start = Instant::now();
    let logical_plan = ctx.state().create_logical_plan(sql).await?;
    let parse_time = parse_start.elapsed();
    profile.add_phase("1. Parse SQL", parse_time);
    
    // Phase 2: Logical Plan Optimization
    let opt_start = Instant::now();
    let optimized_plan = ctx.state().optimize(&logical_plan)?;
    let opt_time = opt_start.elapsed();
    profile.add_phase("2. Optimize Logical Plan", opt_time);
    
    // Phase 3: Physical Plan Creation
    let phys_start = Instant::now();
    let physical_plan = ctx.state().create_physical_plan(&optimized_plan).await?;
    let phys_time = phys_start.elapsed();
    profile.add_phase("3. Create Physical Plan", phys_time);
    
    // Capture raw physical plan string (DataFusion native format with full operator details)
    use datafusion_physical_plan::displayable;
    let raw_plan = displayable(physical_plan.as_ref()).indent(true).to_string();
    profile.set_raw_physical_plan(raw_plan);
    
    // Phase 4: Execution (with detailed metrics collection)
    let exec_start = Instant::now();
    let task_ctx = ctx.task_ctx();
    
    // Execute and collect results
    let stream = physical_plan.execute(0, task_ctx)?;
    let results: Vec<arrow::record_batch::RecordBatch> = stream.try_collect().await?;
    
    let exec_time = exec_start.elapsed();
    profile.add_phase("4. Execute Query", exec_time);
    
    // Extract metrics from physical plan
    let metrics = physical_plan.metrics();
    profile.build_plan_tree(&physical_plan, metrics.as_ref());
    
    // Finalize profile
    profile.finalize();
    
    Ok((results, profile))
}

/// Compare two profiles side by side
pub fn compare_profiles(profile1: &QueryProfile, name1: &str, profile2: &QueryProfile, name2: &str) {
    println!("\n{}", "=".repeat(100));
    println!("Profile Comparison: {} vs {}", name1, name2);
    println!("{}", "=".repeat(100));
    
    // Compare phases
    println!("\n--- Phase Comparison ---");
    println!("{:<40} {:>15} {:>15} {:>15} {:>15}",
        "Phase", 
        format!("{} (ms)", name1),
        format!("{} (ms)", name2),
        "Ratio",
        "Difference"
    );
    println!("{}", "-".repeat(100));
    
    for phase1 in &profile1.phases {
        if let Some(phase2) = profile2.phases.iter().find(|p| p.name == phase1.name) {
            let ratio = if phase2.duration.as_secs_f64() > 0.0 {
                phase1.duration.as_secs_f64() / phase2.duration.as_secs_f64()
            } else {
                0.0
            };
            let diff = phase1.duration.as_secs_f64() - phase2.duration.as_secs_f64();
            let diff_pct = if phase2.duration.as_secs_f64() > 0.0 {
                (diff / phase2.duration.as_secs_f64()) * 100.0
            } else {
                0.0
            };
            
            println!("{:<40} {:>15.2} {:>15.2} {:>14.2}x {:>14.2} ms ({:+.1}%)",
                phase1.name,
                phase1.duration.as_secs_f64() * 1000.0,
                phase2.duration.as_secs_f64() * 1000.0,
                ratio,
                diff * 1000.0,
                diff_pct
            );
        }
    }
    
    // Compare summary
    println!("\n--- Summary Comparison ---");
    println!("{:<40} {:>15} {:>15} {:>15}",
        "Metric",
        name1,
        name2,
        "Ratio"
    );
    println!("{}", "-".repeat(85));
    
    let time1 = profile1.summary.total_time.as_secs_f64() * 1000.0;
    let time2 = profile2.summary.total_time.as_secs_f64() * 1000.0;
    let time_ratio = if time2 > 0.0 { time1 / time2 } else { 0.0 };
    
    println!("{:<40} {:>15.2} {:>15.2} {:>14.2}x",
        "Total Time (ms)", time1, time2, time_ratio);
    
    println!("{:<40} {:>15} {:>15} {:>14.2}x",
        "Operator Count",
        profile1.summary.operator_count,
        profile2.summary.operator_count,
        if profile2.summary.operator_count > 0 {
            profile1.summary.operator_count as f64 / profile2.summary.operator_count as f64
        } else {
            0.0
        }
    );
    
    println!("{:<40} {:>15} {:>15} {:>14.2}x",
        "Plan Depth",
        profile1.summary.max_depth,
        profile2.summary.max_depth,
        if profile2.summary.max_depth > 0 {
            profile1.summary.max_depth as f64 / profile2.summary.max_depth as f64
        } else {
            0.0
        }
    );
    
    println!("{:<40} {:>15} {:>15} {:>14.2}x",
        "Rows Processed",
        profile1.summary.total_rows_processed,
        profile2.summary.total_rows_processed,
        if profile2.summary.total_rows_processed > 0 {
            profile1.summary.total_rows_processed as f64 / profile2.summary.total_rows_processed as f64
        } else {
            0.0
        }
    );
}

/// Export profile to a text file
pub fn export_profile_to_file(profile: &QueryProfile, file_path: &str, metrics_config: &VortexMetricsConfig) -> std::io::Result<()> {
    use std::io::Write;
    
    let mut file = std::fs::File::create(file_path)?;
    
    // Write phases
    writeln!(file, "{}", "=".repeat(80))?;
    writeln!(file, "Execution Phases")?;
    writeln!(file, "{}", "=".repeat(80))?;
    writeln!(file, "{:<40} {:>15} {:>10} {:>10}", "Phase", "Time (ms)", "Percentage", "Cumulative")?;
    writeln!(file, "{}", "-".repeat(80))?;
    
    let mut cumulative = Duration::ZERO;
    for phase in &profile.phases {
        cumulative += phase.duration;
        let cumulative_pct = if profile.summary.total_time.as_secs_f64() > 0.0 {
            (cumulative.as_secs_f64() / profile.summary.total_time.as_secs_f64()) * 100.0
        } else {
            0.0
        };
        writeln!(file, "{:<40} {:>15.2} {:>9.1}% {:>9.1}%",
            phase.name,
            phase.duration.as_secs_f64() * 1000.0,
            phase.percentage,
            cumulative_pct
        )?;
    }
    writeln!(file, "{}", "-".repeat(80))?;
    writeln!(file, "{:<40} {:>15.2} {:>9.1}% {:>9.1}%",
        "TOTAL",
        profile.summary.total_time.as_secs_f64() * 1000.0,
        100.0,
        100.0
    )?;
    
    // Write execution plan tree
    writeln!(file, "\n{}", "=".repeat(80))?;
    writeln!(file, "Execution Plan Tree")?;
    writeln!(file, "{}", "=".repeat(80))?;
    profile.plan_tree.write_tree(&mut file, "", true, metrics_config)?;
    
    // Write summary
    writeln!(file, "\n{}", "=".repeat(80))?;
    writeln!(file, "Profile Summary")?;
    writeln!(file, "{}", "=".repeat(80))?;
    writeln!(file, "Total Execution Time:     {:.2} ms", 
        profile.summary.total_time.as_secs_f64() * 1000.0)?;
    writeln!(file, "Total Operators:           {}", profile.summary.operator_count)?;
    writeln!(file, "Plan Depth:                {}", profile.summary.max_depth)?;
    writeln!(file, "Total Rows Processed:      {}", profile.summary.total_rows_processed)?;
    if let Some(ref slowest) = profile.summary.slowest_operator {
        writeln!(file, "Slowest Operator:          {}", slowest)?;
    }
    if let Some(ref largest) = profile.summary.largest_operator {
        writeln!(file, "Largest Operator (rows):  {}", largest)?;
    }
    
    Ok(())
}

/// Export profile to JSON for further analysis
pub fn export_profile_json(profile: &QueryProfile) -> serde_json::Value {
    serde_json::json!({
        "phases": profile.phases.iter().map(|p| {
            serde_json::json!({
                "name": p.name,
                "duration_ms": p.duration.as_secs_f64() * 1000.0,
                "percentage": p.percentage
            })
        }).collect::<Vec<_>>(),
        "summary": {
            "total_time_ms": profile.summary.total_time.as_secs_f64() * 1000.0,
            "operator_count": profile.summary.operator_count,
            "max_depth": profile.summary.max_depth,
            "total_rows_processed": profile.summary.total_rows_processed,
            "slowest_operator": profile.summary.slowest_operator,
            "largest_operator": profile.summary.largest_operator,
        }
    })
}

