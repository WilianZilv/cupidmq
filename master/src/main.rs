use anyhow::Result;
use clap::Parser;
use cupidmq::config::{self, CliOverrides, FileConfig};
use cupidmq::server::{RelayConfig, run};
use std::path::PathBuf;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "cupidmq")]
struct Args {
    /// Config file (default: ./cupidmq.conf or CUPIDMQ_CONFIG)
    #[arg(long, env = "CUPIDMQ_CONFIG", default_value = "cupidmq.conf")]
    config: String,

    /// Bind host for control + metrics listeners
    #[arg(long, env = "CUPIDMQ_HOST")]
    host: Option<String>,

    /// TCP control plane port (producers + consumers)
    #[arg(long, env = "CUPIDMQ_CONTROL_PORT")]
    control_port: Option<u16>,

    /// HTTP metrics + dashboard port
    #[arg(long, env = "CUPIDMQ_METRICS_PORT")]
    metrics_port: Option<u16>,

    #[arg(long, env = "CUPIDMQ_HISTORY_INTERVAL_MS")]
    history_interval_ms: Option<u64>,

    #[arg(long, env = "CUPIDMQ_HISTORY_CAP")]
    history_cap: Option<usize>,

    /// Built dashboard static files (Vite `dist/`)
    #[arg(long, env = "CUPIDMQ_DASHBOARD_DIR")]
    dashboard_dir: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let config_path = PathBuf::from(&args.config);

    let file_cfg = if config_path.is_file() {
        Some(FileConfig::load(&config_path)?)
    } else {
        info!(path = %config_path.display(), "config file not found, using defaults/CLI/env");
        None
    };

    let resolved = config::resolve(
        file_cfg.as_ref(),
        if config_path.is_file() {
            Some(config_path.clone())
        } else {
            None
        },
        CliOverrides {
            host: args.host,
            control_port: args.control_port,
            metrics_port: args.metrics_port,
            history_interval_ms: args.history_interval_ms,
            history_cap: args.history_cap,
            dashboard_dir: args.dashboard_dir,
        },
    );

    let variant = if cfg!(feature = "embed-dashboard") {
        "cupidmq"
    } else {
        "cupidmq-headless"
    };
    info!(
        variant,
        config = resolved.config_path.as_ref().map(|p| p.display().to_string()),
        host = %resolved.host,
        control_port = resolved.control_port,
        metrics_port = resolved.metrics_port,
        "cupidmq master starting"
    );

    run(RelayConfig {
        control_addr: resolved.control,
        metrics_addr: resolved.metrics,
        history_interval_ms: resolved.history_interval_ms,
        history_cap: resolved.history_cap,
        dashboard_dir: resolved.dashboard_dir.map(PathBuf::from),
    })
    .await
}
