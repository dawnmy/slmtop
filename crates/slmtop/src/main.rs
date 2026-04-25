use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use slmtop_slurm::BackendConfig;
use slmtop_slurm_cli::CliSlurmBackend;
use slmtop_tui::TuiOptions;

#[derive(Debug, Parser)]
#[command(name = "slmtop", version, about = "Realtime Slurm cluster monitor")]
struct Args {
    #[arg(long, default_value_t = 3.0, help = "Refresh interval in seconds")]
    refresh_interval: f64,

    #[arg(long, default_value_t = 4.0, help = "Per-command timeout in seconds")]
    command_timeout: f64,

    #[arg(long, default_value_t = 100, help = "Recent sacct rows to keep")]
    accounting_limit: usize,

    #[arg(long, help = "Override the current username used for owner filters")]
    user: Option<String>,

    #[arg(long, help = "Print version and exit without starting the TUI")]
    version_only: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .compact()
        .try_init()
        .ok();

    let args = Args::parse();
    if args.version_only {
        println!("slmtop {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    let mut config = BackendConfig {
        refresh_interval: duration_from_secs(args.refresh_interval),
        command_timeout: duration_from_secs(args.command_timeout),
        accounting_limit: args.accounting_limit,
        ..BackendConfig::default()
    };
    if let Some(user) = args.user {
        config.current_user = user;
    }

    let backend = CliSlurmBackend::new(config.command_timeout);
    slmtop_tui::run(backend, &config, TuiOptions::default())?;
    Ok(())
}

fn duration_from_secs(seconds: f64) -> Duration {
    Duration::from_secs_f64(seconds.max(0.1))
}
