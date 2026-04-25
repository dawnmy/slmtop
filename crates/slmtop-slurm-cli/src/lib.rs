//! Slurm CLI backend implementation.

use std::process::Stdio;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use slmtop_core::{AccountingRecord, Job, Node};
use slmtop_parsers::{parse_sacct, parse_sinfo, parse_squeue};
use slmtop_slurm::{CommandTelemetry, JobControl, Result, SlurmBackend, SlurmError};
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
        let command = command_label(program, args);
        let started = Instant::now();
        let mut child = Command::new(program);
        child
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let output = timeout(self.timeout, child.output())
            .await
            .map_err(|_| SlurmError::Timeout {
                command: command.clone(),
                timeout: self.timeout,
            })?
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
                &["-a", "-h", "-o", "%i|%u|%T|%P|%j|%D|%C|%m|%b|%M|%R"],
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
                &["-N", "-h", "-o", "%n|%t|%c|%C|%m|%e|%G|%E"],
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

#[must_use]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
