use std::sync::Once;

use std::path::PathBuf;
use tracing_subscriber::{fmt, EnvFilter};

static INIT: Once = Once::new();
static mut GUARD: Option<tracing_appender::non_blocking::WorkerGuard> = None;

/// Initialize test logging once, writing to `target/logs/{log_file}`.
/// Honors `RUST_LOG` if set; otherwise defaults to a modest filter to avoid flooding stdout.
pub fn init_test_logging(log_file: &str) {
    INIT.call_once(|| {
        let file_appender = tracing_appender::rolling::never("target/logs", log_file);
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

        let default_filter = std::env::var("RUST_LOG").unwrap_or_else(|_| {
            "datafusion_datasource=debug,vortex_datafusion=debug,vortexlake_sql=info".to_string()
        });

        let _ = fmt()
            .with_env_filter(EnvFilter::new(default_filter))
            .with_writer(non_blocking)
            .with_ansi(false)
            .with_target(false)
            .with_file(true)
            .with_line_number(true)
            .without_time()
            .try_init();

        // Keep guard alive to flush logs
        unsafe { GUARD = Some(guard); }
    });
}

/// Resolve the persistent test data directory (absolute path) under `target/test_data`.
/// If the directory does not exist, it will be created.
pub fn get_test_data_dir() -> PathBuf {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("target/test_data");

    std::fs::create_dir_all(&base).ok();

    base.canonicalize().unwrap_or(base)
}

