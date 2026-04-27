//! Slurm CLI backend implementation.

use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use slmtop_core::{AccountingRecord, DiskInfo, DiskUserUsage, Job, Node};
use slmtop_parsers::{
    parse_df, parse_du_user_usage, parse_lfs_quota_user_usage, parse_sacct, parse_sinfo,
    parse_squeue,
};
use slmtop_slurm::{CommandTelemetry, JobControl, Result, SlurmBackend, SlurmError};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::time::timeout;

#[derive(Debug, Clone)]
pub struct CliSlurmBackend {
    timeout: Duration,
}

impl CliSlurmBackend {
    #[must_use]
    pub const fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    async fn run(
        &self,
        program: &str,
        args: &[&str],
        allow_failure: bool,
    ) -> Result<CommandOutput> {
        self.run_with_timeout(program, args, allow_failure, self.timeout)
            .await
    }

    async fn run_with_timeout(
        &self,
        program: &str,
        args: &[&str],
        allow_failure: bool,
        command_timeout: Duration,
    ) -> Result<CommandOutput> {
        self.run_with_optional_timeout(program, args, allow_failure, Some(command_timeout))
            .await
    }

    async fn run_with_optional_timeout(
        &self,
        program: &str,
        args: &[&str],
        allow_failure: bool,
        command_timeout: Option<Duration>,
    ) -> Result<CommandOutput> {
        let command = command_label(program, args);
        let started = Instant::now();
        let mut child = Command::new(program);
        child
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let output = if let Some(command_timeout) = command_timeout {
            timeout(command_timeout, child.output())
                .await
                .map_err(|_| SlurmError::Timeout {
                    command: command.clone(),
                    timeout: command_timeout,
                })?
        } else {
            child.output().await
        }
        .map_err(|source| SlurmError::Io {
            command: command.clone(),
            source,
        })?;

        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if !output.status.success() && !allow_failure {
            return Err(SlurmError::CommandFailed {
                command,
                exit_code: output.status.code(),
                stderr,
            });
        }

        Ok(CommandOutput {
            stdout,
            telemetry: CommandTelemetry {
                command,
                elapsed: started.elapsed(),
                warnings: if output.status.success() || stderr.is_empty() {
                    Vec::new()
                } else {
                    vec![stderr]
                },
            },
        })
    }

    async fn run_action(&self, program: &str, args: &[&str]) -> Result<String> {
        let output = self.run(program, args, false).await?;
        let message = output.stdout.trim();
        if message.is_empty() {
            Ok(format!("{} completed", output.telemetry.command))
        } else {
            Ok(message.to_string())
        }
    }
}

#[async_trait]
impl SlurmBackend for CliSlurmBackend {
    async fn jobs(&self) -> Result<(Vec<Job>, CommandTelemetry)> {
        let output = self
            .run(
                "squeue",
                &["-a", "-h", "-o", "%i|%u|%T|%P|%j|%D|%C|%m|%b|%M|%N|%R|%l"],
                false,
            )
            .await?;
        let parsed = parse_squeue(&output.stdout);
        let mut telemetry = output.telemetry;
        telemetry.warnings.extend(parsed.warnings);
        Ok((parsed.rows, telemetry))
    }

    async fn nodes(&self) -> Result<(Vec<Node>, CommandTelemetry)> {
        let output = self
            .run(
                "sinfo",
                &[
                    "-N",
                    "-h",
                    "-O",
                    "NodeHost:|,StateCompact:|,CPUs:|,CPUsState:|,Memory:|,FreeMem:|,Gres:|,Reason:|,GresUsed:500",
                ],
                false,
            )
            .await?;
        let parsed = parse_sinfo(&output.stdout);
        let mut telemetry = output.telemetry;
        telemetry.warnings.extend(parsed.warnings);
        Ok((parsed.rows, telemetry))
    }

    async fn accounting(&self, limit: usize) -> Result<(Vec<AccountingRecord>, CommandTelemetry)> {
        let output = self
            .run(
                "sacct",
                &[
                    "-X",
                    "-n",
                    "-P",
                    "-a",
                    "--starttime=now-12hours",
                    "--format=JobIDRaw,User,State,Partition,JobName,AllocCPUS,ReqMem,Elapsed,Start,End",
                ],
                false,
            )
            .await?;
        let mut parsed = parse_sacct(&output.stdout);
        parsed.rows.truncate(limit);
        let mut telemetry = output.telemetry;
        telemetry.warnings.extend(parsed.warnings);
        Ok((parsed.rows, telemetry))
    }

    async fn disk_info(&self) -> Result<Vec<DiskInfo>> {
        let output = self
            .run(
                "df",
                &["-h", "--output=source,fstype,size,used,avail,pcent,target"],
                true,
            )
            .await?;
        Ok(parse_df(&output.stdout))
    }

    async fn disk_user_usage(
        &self,
        mount: &str,
        user: &str,
        scan_timeout: Option<Duration>,
    ) -> Result<Vec<DiskUserUsage>> {
        if let Ok(output) = self
            .run_with_timeout(
                "lfs",
                &["quota", "-u", user, "-h", mount],
                true,
                Duration::from_secs(3),
            )
            .await
        {
            let rows = parse_lfs_quota_user_usage(&output.stdout, user);
            if !rows.is_empty() {
                return Ok(rows);
            }
        }

        for path in user_disk_paths(mount, user) {
            let output = match self
                .run_with_optional_timeout("du", &["-sxB1", &path], true, scan_timeout)
                .await
            {
                Ok(output) => output,
                Err(SlurmError::Timeout { timeout, .. }) => return Err(disk_scan_timeout(timeout)),
                Err(_) => continue,
            };
            let rows = parse_du_user_usage(&output.stdout, user);
            if !rows.is_empty() {
                return Ok(rows);
            }
        }

        stream_find_current_user_usage(mount, user, scan_timeout)
            .await
            .map_err(disk_scan_error)
    }
}

#[async_trait]
impl JobControl for CliSlurmBackend {
    async fn cancel_job(&self, job_id: &str) -> Result<String> {
        self.run_action("scancel", &[job_id]).await
    }

    async fn hold_job(&self, job_id: &str) -> Result<String> {
        self.run_action("scontrol", &["hold", job_id]).await
    }

    async fn release_job(&self, job_id: &str) -> Result<String> {
        self.run_action("scontrol", &["release", job_id]).await
    }

    async fn requeue_job(&self, job_id: &str) -> Result<String> {
        self.run_action("scontrol", &["requeue", job_id]).await
    }
}

#[derive(Debug)]
struct CommandOutput {
    stdout: String,
    telemetry: CommandTelemetry,
}

fn command_label(program: &str, args: &[&str]) -> String {
    let mut words = Vec::with_capacity(args.len() + 1);
    words.push(program.to_string());
    words.extend(args.iter().map(|arg| (*arg).to_string()));
    shell_words::join(words)
}

fn user_disk_paths(mount: &str, user: &str) -> Vec<String> {
    if user.is_empty() || user.contains('/') || user.contains('\0') {
        return Vec::new();
    }
    let mount = Path::new(mount);
    [
        mount.join(user),
        mount.join("users").join(user),
        mount.join("home").join(user),
    ]
    .into_iter()
    .filter(|path| path.is_dir())
    .filter_map(|path| path.into_os_string().into_string().ok())
    .collect()
}

fn disk_scan_timeout(timeout: Duration) -> SlurmError {
    SlurmError::Other(format!(
        "Usage scan timed out after {}s. This filesystem is too large to scan directly, and no quick user directory result was available.",
        timeout.as_secs()
    ))
}

async fn stream_find_current_user_usage(
    mount: &str,
    user: &str,
    scan_timeout: Option<Duration>,
) -> Result<Vec<DiskUserUsage>> {
    let scan = async {
        let command = "disk usage scan".to_string();
        let mut child = Command::new("find");
        child
            .args([mount, "-xdev", "-user", user, "-printf", "%b\n"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let mut child = child.spawn().map_err(|source| SlurmError::Io {
            command: command.clone(),
            source,
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            SlurmError::Other("Usage scan did not provide a readable stream.".to_string())
        })?;
        let mut lines = BufReader::new(stdout).lines();
        let mut row = DiskUserUsage {
            user: user.to_string(),
            bytes: 0,
            entries: 0,
        };
        while let Some(line) = lines.next_line().await.map_err(|source| SlurmError::Io {
            command: command.clone(),
            source,
        })? {
            let Ok(blocks) = line.trim().parse::<u64>() else {
                continue;
            };
            row.bytes = row.bytes.saturating_add(blocks.saturating_mul(512));
            row.entries = row.entries.saturating_add(1);
        }
        let _status = child
            .wait()
            .await
            .map_err(|source| SlurmError::Io { command, source })?;
        if row.entries == 0 {
            Ok(Vec::new())
        } else {
            Ok(vec![row])
        }
    };

    if let Some(scan_timeout) = scan_timeout {
        timeout(scan_timeout, scan)
            .await
            .map_err(|_| disk_scan_timeout(scan_timeout))?
    } else {
        scan.await
    }
}

fn disk_scan_error(error: SlurmError) -> SlurmError {
    match error {
        SlurmError::Io { source, .. } => SlurmError::Other(format!(
            "Usage scan could not start: {source}. The command details are hidden because this is a background scan."
        )),
        SlurmError::CommandFailed { stderr, .. } if !stderr.is_empty() => {
            SlurmError::Other(stderr)
        }
        SlurmError::CommandFailed { .. } => {
            SlurmError::Other("Usage scan failed before producing a result.".to_string())
        }
        other => other,
    }
}

#[must_use]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
