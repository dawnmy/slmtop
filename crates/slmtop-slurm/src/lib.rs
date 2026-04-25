//! Backend traits for collecting and controlling Slurm state.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use slmtop_core::{AccountingRecord, ClusterSnapshot, Job, Node};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SlurmError {
    #[error("backend command timed out after {timeout:?}: {command}")]
    Timeout { command: String, timeout: Duration },
    #[error("backend command failed: {command} (exit {exit_code:?}) stderr={stderr}")]
    CommandFailed {
        command: String,
        exit_code: Option<i32>,
        stderr: String,
    },
    #[error("backend IO error while running {command}: {source}")]
    Io {
        command: String,
        #[source]
        source: std::io::Error,
    },
    #[error("backend parse error: {0}")]
    Parse(String),
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, SlurmError>;

#[derive(Debug, Clone)]
pub struct BackendConfig {
    pub command_timeout: Duration,
    pub refresh_interval: Duration,
    pub accounting_limit: usize,
    pub current_user: String,
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            command_timeout: Duration::from_secs(4),
            refresh_interval: Duration::from_secs(3),
            accounting_limit: 100,
            current_user: std::env::var("USER").unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CommandTelemetry {
    pub command: String,
    pub elapsed: Duration,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SnapshotTelemetry {
    pub elapsed: Duration,
    pub commands: Vec<CommandTelemetry>,
}

#[derive(Debug, Clone)]
pub struct SnapshotEnvelope {
    pub snapshot: ClusterSnapshot,
    pub telemetry: SnapshotTelemetry,
}

#[async_trait]
pub trait SlurmBackend: Send + Sync {
    async fn jobs(&self) -> Result<(Vec<Job>, CommandTelemetry)>;
    async fn nodes(&self) -> Result<(Vec<Node>, CommandTelemetry)>;
    async fn accounting(&self, limit: usize) -> Result<(Vec<AccountingRecord>, CommandTelemetry)>;
}

#[async_trait]
pub trait JobControl: Send + Sync {
    async fn cancel_job(&self, job_id: &str) -> Result<String>;
    async fn hold_job(&self, job_id: &str) -> Result<String>;
    async fn release_job(&self, job_id: &str) -> Result<String>;
    async fn requeue_job(&self, job_id: &str) -> Result<String>;
}

#[async_trait]
pub trait SlurmClient: SlurmBackend + JobControl {}

impl<T> SlurmClient for T where T: SlurmBackend + JobControl {}

pub struct SnapshotCollector<B> {
    backend: B,
    config: BackendConfig,
}

impl<B> SnapshotCollector<B>
where
    B: SlurmBackend,
{
    #[must_use]
    pub const fn new(backend: B, config: BackendConfig) -> Self {
        Self { backend, config }
    }

    /// Refreshes the cluster snapshot through the configured backend.
    ///
    /// # Errors
    ///
    /// Returns a [`SlurmError`] when required live commands fail. Accounting
    /// failures are downgraded to snapshot warnings by [`refresh_backend`].
    pub async fn refresh(&self) -> Result<SnapshotEnvelope> {
        refresh_backend(&self.backend, &self.config).await
    }

    #[must_use]
    pub const fn backend(&self) -> &B {
        &self.backend
    }
}

/// Collects jobs, nodes, and recent accounting into a single immutable snapshot.
///
/// # Errors
///
/// Returns a [`SlurmError`] when required live job or node data cannot be
/// collected. Recent accounting failures are reported as warnings so live
/// monitoring can continue when `sacct` is unavailable or slow.
pub async fn refresh_backend<B>(backend: &B, config: &BackendConfig) -> Result<SnapshotEnvelope>
where
    B: SlurmBackend + ?Sized,
{
    let started = Instant::now();
    let (jobs_result, nodes_result, accounting_result) = tokio::join!(
        backend.jobs(),
        backend.nodes(),
        backend.accounting(config.accounting_limit)
    );

    let mut commands = Vec::new();
    let (jobs, job_telemetry) = jobs_result?;
    commands.push(job_telemetry);
    let (nodes, node_telemetry) = nodes_result?;
    commands.push(node_telemetry);
    let (accounting, accounting_telemetry) = match accounting_result {
        Ok((rows, telemetry)) => {
            commands.push(telemetry);
            (rows, None)
        }
        Err(error) => (Vec::new(), Some(format!("sacct unavailable: {error}"))),
    };

    let mut warnings = Vec::new();
    for command in &commands {
        warnings.extend(command.warnings.iter().cloned());
    }
    if let Some(warning) = accounting_telemetry {
        warnings.push(warning);
    }

    Ok(SnapshotEnvelope {
        snapshot: ClusterSnapshot::new(jobs, nodes, accounting, &config.current_user, warnings),
        telemetry: SnapshotTelemetry {
            elapsed: started.elapsed(),
            commands,
        },
    })
}

#[must_use]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
