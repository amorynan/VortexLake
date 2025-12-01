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
    
    /// Input rows (if available)
    pub input_rows: Option<usize>,
    
    /// Spilled bytes to disk
    pub spilled_bytes: Option<usize>,
    
    /// Spilled rows to disk
    pub spilled_rows: Option<usize>,
    
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
}

impl QueryProfile {
    pub fn new() -> Self {
        Self {
            origin_sql: String::new(),
            phases: Vec::new(),
            plan_tree: PlanNode::new("Root", "Root"),
            summary: ProfileSummary::default(),
        }
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
            node_metrics.output_rows = metrics.output_rows();
            node_metrics.spilled_bytes = metrics.spilled_bytes();
            node_metrics.spilled_rows = metrics.spilled_rows();
            
            // Extract elapsed_compute time (returns nanoseconds as usize)
            if let Some(nanos) = metrics.elapsed_compute() {
                node_metrics.elapsed_compute = Some(Duration::from_nanos(nanos as u64));
            }
        }
        
        // Build children recursively
        let children: Vec<PlanNode> = plan
            .children()
            .iter()
            .map(|child| Self::build_node(child, depth + 1))
            .collect();
        
        PlanNode {
            operator_type,
            operator_name,
            metrics: node_metrics,
            children,
            depth,
        }
    }
    
    fn analyze_bottlenecks(&mut self) {
        let mut slowest_time = Duration::ZERO;
        let mut slowest_name = None;
        let mut largest_rows = 0;
        let mut largest_name = None;
        
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
        });
        
        self.summary.slowest_operator = slowest_name;
        self.summary.largest_operator = largest_name;
        self.summary.total_rows_processed = largest_rows;
    }
    
    /// Print detailed profile report
    pub fn print(&self) {
        println!("Origin SQL:\n {}", self.origin_sql);
        self.print_phases();
        self.print_plan_tree();
        self.print_summary();
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
        if let Some(ref slowest) = self.summary.slowest_operator {
            println!("Slowest Operator:          {}", slowest);
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
        if let Some(rows) = self.metrics.output_rows {
            metrics_parts.push(format!("rows:{}", rows));
        }
        if let Some(bytes) = self.metrics.spilled_bytes {
            if bytes > 0 {
                metrics_parts.push(format!("spilled:{}B", bytes));
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
    
    fn write_tree<W: std::io::Write>(&self, writer: &mut W, prefix: &str, is_last: bool) -> std::io::Result<()> {
        let connector = if is_last { "└── " } else { "├── " };
        let next_prefix = if is_last { "    " } else { "│   " };
        
        // Build metrics string
        let mut metrics_parts = Vec::new();
        if let Some(rows) = self.metrics.output_rows {
            metrics_parts.push(format!("rows:{}", rows));
        }
        if let Some(bytes) = self.metrics.spilled_bytes {
            if bytes > 0 {
                metrics_parts.push(format!("spilled:{}B", bytes));
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

