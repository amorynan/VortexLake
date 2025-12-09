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
pub struct OperatorMetrics {
    /// CPU time spent in this operator
    pub elapsed_compute: Option<Duration>,
    
    /// Total output rows
    pub output_rows: Option<usize>,
    
    /// Input rows (if available) - used to calculate filter selectivity
    pub input_rows: Option<usize>,
    
    /// Rows filtered out (for FilterExec: input_rows - output_rows)
    pub rows_filtered: Option<usize>,
    
    /// Filter selectivity ratio (output_rows / input_rows)
    pub selectivity: Option<f64>,
    
    /// Spilled bytes to disk
    pub spilled_bytes: Option<usize>,
    
    /// Spilled rows to disk
    pub spilled_rows: Option<usize>,
    
    // === I/O and Scan Metrics (from Parquet/Vortex) ===
    
    /// Bytes scanned from storage
    pub bytes_scanned: Option<usize>,
    
    /// Rows pruned by pushdown predicates
    pub pushdown_rows_pruned: Option<usize>,
    
    /// Rows matched by pushdown predicates
    pub pushdown_rows_matched: Option<usize>,
    
    /// Row groups pruned by statistics
    pub row_groups_pruned_statistics: Option<usize>,
    
    /// Row groups matched by statistics
    pub row_groups_matched_statistics: Option<usize>,
    
    /// Time spent evaluating row-level pushdown filters
    pub row_pushdown_eval_time: Option<Duration>,
    
    /// Time spent loading metadata
    pub metadata_load_time: Option<Duration>,
    
    /// Page index rows pruned
    pub page_index_rows_pruned: Option<usize>,
    
    /// All other metrics (name -> value as string)
    pub other_metrics: Vec<(String, String)>,
    
    /// Start timestamp
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,
    
    /// End timestamp
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,
    
    /// Memory usage (if available)
    pub memory_usage: Option<usize>,
    
    /// Partition information
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
        
        // Build children recursively
        let children: Vec<PlanNode> = plan
            .children()
            .iter()
            .map(|child| Self::build_node(child, depth + 1))
            .collect();
        
        // Calculate filter statistics for FilterExec
        // Input rows = find the first valid output_rows in the subtree
        // (RepartitionExec and some other operators don't expose output_rows)
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
            let name = metric.value().name();
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
    pub fn print(&self) {
        println!("Origin SQL:\n {}", self.origin_sql);
        self.print_phases();
        self.print_raw_physical_plan();
        self.print_plan_tree();
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
    
    fn print_plan_tree(&self) {
        println!("\n{}", "=".repeat(80));
        println!("Execution Plan Tree");
        println!("{}", "=".repeat(80));
        self.plan_tree.print_tree();
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
    
    fn print_tree(&self) {
        self.print_node("", true);
    }
    
    fn print_node(&self, prefix: &str, is_last: bool) {
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
            child.print_node(&format!("{}{}", prefix, next_prefix), is_last_child);
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
    
    fn write_tree<W: std::io::Write>(&self, writer: &mut W, prefix: &str, is_last: bool) -> std::io::Result<()> {
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
            child.write_tree(writer, &format!("{}{}", prefix, next_prefix), is_last_child)?;
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
pub fn export_profile_to_file(profile: &QueryProfile, file_path: &str) -> std::io::Result<()> {
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
    profile.plan_tree.write_tree(&mut file, "", true)?;
    
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

