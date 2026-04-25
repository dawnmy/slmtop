use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use slmtop_slurm::BackendConfig;
use slmtop_slurm_cli::CliSlurmBackend;
use slmtop_tui::{ThemeName, TuiOptions};

#[derive(Debug, Parser)]
#[command(name = "slmtop", version, about = "Realtime Slurm cluster monitor")]
struct Args {
    #[arg(short = 'i', long, default_value_t = 3.0, help = "Refresh interval in seconds")]
    refresh_interval: f64,

    #[arg(short = 't', long, default_value_t = 4.0, help = "Per-command timeout in seconds")]
    command_timeout: f64,

    #[arg(short = 'l', long, default_value_t = 100, help = "Recent sacct rows to keep")]
    accounting_limit: usize,

    #[arg(short = 'u', long, help = "Override the current username used for owner filters")]
    user: Option<String>,

    #[arg(short = 'V', long, help = "Print version and exit without starting the TUI")]
    version_only: bool,

    #[arg(short = 'T', long, default_value = "catppuccin", help = "Color theme: catppuccin, monokai, tokyonight, dracula, onedark, nightowl, classic")]
    theme: String,
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

    let theme = ThemeName::from_str(&args.theme);
    let options = TuiOptions {
        theme,
        ..TuiOptions::default()
    };

    let backend = CliSlurmBackend::new(config.command_timeout);
    slmtop_tui::run(backend, &config, options)?;
    Ok(())
}

fn duration_from_secs(seconds: f64) -> Duration {
    Duration::from_secs_f64(seconds.max(0.1))
}
