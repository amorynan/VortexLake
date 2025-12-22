/// Configuration for VortexMetrics isolation behavior
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsIsolationMode {
    /// Default: Session-level accumulated metrics
    /// Same metrics instance shared across all DataSourceExec in a session
    /// Useful for monitoring file access hotspots and cache optimization
    SessionAccumulated,

    /// Per-DataSource metrics isolation
    /// Each DataSourceExec gets its own metrics scope
    /// Shows accurate per-scan consumption
    PerDataSource,
}

impl Default for MetricsIsolationMode {
    fn default() -> Self {
        Self::SessionAccumulated
    }
}

impl std::fmt::Display for MetricsIsolationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SessionAccumulated => write!(f, "SessionAccumulated"),
            Self::PerDataSource => write!(f, "PerDataSource"),
        }
    }
}

/// VortexMetrics configuration
#[derive(Debug, Clone)]
pub struct VortexMetricsConfig {
    pub isolation_mode: MetricsIsolationMode,
}

impl Default for VortexMetricsConfig {
    fn default() -> Self {
        Self {
            isolation_mode: MetricsIsolationMode::default(),
        }
    }
}

impl VortexMetricsConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_isolation_mode(mut self, mode: MetricsIsolationMode) -> Self {
        self.isolation_mode = mode;
        self
    }

    /// Get display suffix for metrics names based on isolation mode
    pub fn metrics_name_suffix(&self) -> &'static str {
        match self.isolation_mode {
            MetricsIsolationMode::SessionAccumulated => "_session_total",
            MetricsIsolationMode::PerDataSource => "_this_scan",
        }
    }

    /// Get help text explaining the metrics
    pub fn metrics_help_text(&self) -> &'static str {
        match self.isolation_mode {
            MetricsIsolationMode::SessionAccumulated =>
                " [SESSION ACCUMULATED - sum across all scans of this file in current session]",
            MetricsIsolationMode::PerDataSource =>
                " [PER SCAN - metrics for this specific DataSourceExec only]",
        }
    }
}
