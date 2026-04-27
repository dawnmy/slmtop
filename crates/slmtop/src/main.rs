use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use slmtop_slurm::BackendConfig;
use slmtop_slurm_cli::CliSlurmBackend;
use slmtop_tui::{ThemeName, TuiOptions};

#[derive(Debug, Parser)]
#[command(name = "slmtop", version, about = "Realtime Slurm cluster monitor")]
struct Args {
    #[arg(
        short = 'i',
        long,
        default_value_t = 3.0,
        help = "Refresh interval in seconds"
    )]
    refresh_interval: f64,

    #[arg(
        short = 't',
        long,
        default_value_t = 4.0,
        help = "Per-command timeout in seconds"
    )]
    command_timeout: f64,

    #[arg(
        short = 'd',
        long,
        default_value_t = 180.0,
        help = "Disk usage scan timeout in seconds; use --disk-usage-no-timeout to keep usage scans running in the background until complete"
    )]
    disk_usage_timeout: f64,

    #[arg(
        short = 'n',
        long,
        help = "Disable timeout for disk usage scans; scans keep running in the background and results are cached when they finish"
    )]
    disk_usage_no_timeout: bool,

    #[arg(
        short = 'l',
        long,
        default_value_t = 100,
        help = "Recent sacct rows to keep"
    )]
    accounting_limit: usize,

    #[arg(
        short = 'u',
        long,
        help = "Override the current username used for owner filters"
    )]
    user: Option<String>,

    #[arg(
        short = 'T',
        long,
        default_value = "catppuccin",
        help = "Color theme: catppuccin, monokai, tokyonight, dracula, onedark, nightowl, classic"
    )]
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
    let mut config = BackendConfig {
        refresh_interval: duration_from_secs(args.refresh_interval),
        command_timeout: duration_from_secs(args.command_timeout),
        disk_usage_timeout: if args.disk_usage_no_timeout {
            None
        } else {
            Some(duration_from_secs(args.disk_usage_timeout))
        },
        accounting_limit: args.accounting_limit,
        ..BackendConfig::default()
    };
    if let Some(user) = args.user {
        config.current_user = user;
    }

    let theme = ThemeName::parse(&args.theme);
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
