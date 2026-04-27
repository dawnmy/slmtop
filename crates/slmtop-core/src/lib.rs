//! Core domain model and pure state transformations for slmtop.

use std::collections::BTreeMap;
use std::fmt;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type GpuMap = BTreeMap<String, u64>;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("unknown panel: {0}")]
    UnknownPanel(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SortDirection {
    #[default]
    Asc,
    Desc,
}

impl SortDirection {
    #[must_use]
    pub const fn toggled(self) -> Self {
        match self {
            Self::Asc => Self::Desc,
            Self::Desc => Self::Asc,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub struct MemoryMb(pub u64);

impl MemoryMb {
    #[must_use]
    pub const fn zero() -> Self {
        Self(0)
    }

    #[must_use]
    pub const fn saturating_sub(self, rhs: Self) -> Self {
        Self(self.0.saturating_sub(rhs.0))
    }

    #[must_use]
    pub fn human(self) -> String {
        let mb = self.0;
        if mb < 1024 {
            return format!("{mb}M");
        }
        if mb < 1024 * 1024 {
            let tenths = mb.saturating_mul(10) / 1024;
            return format!("{}.{:01}G", tenths / 10, tenths % 10);
        }
        let hundredths = mb.saturating_mul(100) / (1024 * 1024);
        format!("{}.{:02}T", hundredths / 100, hundredths % 100)
    }
}

impl fmt::Display for MemoryMb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.human())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CpuCounts {
    pub total: u64,
    pub allocated: u64,
    pub idle: u64,
    pub other: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub user: String,
    pub state: String,
    pub partition: String,
    pub name: String,
    pub nodes: String,
    pub node_list: String,
    pub cpus: u64,
    pub memory: MemoryMb,
    pub gpus: GpuMap,
    pub gres_raw: String,
    pub time_used: String,
    pub time_limit: String,
    pub reason: Option<String>,
}

impl Job {
    #[must_use]
    pub fn gpu_total(&self) -> u64 {
        self.gpus.values().sum()
    }

    #[must_use]
    pub fn state_rank(&self) -> u8 {
        state_rank(&self.state)
    }

    #[must_use]
    pub fn id_sort_key(&self) -> (u64, u64) {
        numeric_job_id(&self.id)
    }

    #[must_use]
    pub fn searchable_text(&self) -> String {
        format!(
            "{} {} {} {} {} {} {} {} {}",
            self.id,
            self.user,
            self.state,
            self.partition,
            self.name,
            self.nodes,
            self.node_list,
            self.gres_raw,
            self.reason.as_deref().unwrap_or_default()
        )
        .to_lowercase()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub name: String,
    pub state: String,
    pub cpus: CpuCounts,
    pub memory_total: MemoryMb,
    pub memory_reserved: MemoryMb,
    pub memory_free: MemoryMb,
    pub gpus: GpuMap,
    pub gpus_allocated: GpuMap,
    pub gres_raw: String,
    pub reason: Option<String>,
}

impl Node {
    #[must_use]
    pub fn gpu_total(&self) -> u64 {
        self.gpus.values().sum()
    }

    #[must_use]
    pub fn gpu_allocated(&self) -> u64 {
        self.gpus_allocated.values().sum()
    }

    #[must_use]
    pub fn searchable_text(&self) -> String {
        format!(
            "{} {} {} {}",
            self.name,
            self.state,
            self.gres_raw,
            self.reason.as_deref().unwrap_or_default()
        )
        .to_lowercase()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountingRecord {
    pub job_id: String,
    pub user: String,
    pub state: String,
    pub partition: String,
    pub name: String,
    pub cpus: u64,
    pub memory: MemoryMb,
    pub elapsed: String,
    pub start: String,
    pub end: String,
}

impl AccountingRecord {
    #[must_use]
    pub fn searchable_text(&self) -> String {
        format!(
            "{} {} {} {} {} {} {}",
            self.job_id, self.user, self.state, self.partition, self.name, self.elapsed, self.end
        )
        .to_lowercase()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct JobBucket {
    pub jobs: u64,
    pub cpus: u64,
    pub memory: MemoryMb,
    pub gpus: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct OwnerSummary {
    pub running: JobBucket,
    pub pending: JobBucket,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct JobSummary {
    pub all: OwnerSummary,
    pub me: OwnerSummary,
    pub others: OwnerSummary,
    pub users: std::collections::HashMap<String, OwnerSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GpuTypeSummary {
    pub total: u64,
    pub active: u64,
    pub reserved: u64,
    pub free_estimate: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GpuSummary {
    pub total: u64,
    pub active: u64,
    pub reserved: u64,
    pub free_estimate: u64,
    pub by_type: BTreeMap<String, GpuTypeSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskInfo {
    pub mount: String,
    pub fstype: String,
    pub size: String,
    pub used: String,
    pub avail: String,
    pub use_percent: u8,
    pub label: DiskLabel,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DiskUserUsage {
    pub user: String,
    pub bytes: u64,
    pub entries: u64,
}

impl DiskUserUsage {
    #[must_use]
    pub fn human_bytes(&self) -> String {
        human_bytes(self.bytes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiskLabel {
    Ssd,
    Hdd,
    Nfs,
    ParallelFs,
    Unknown,
}

impl DiskLabel {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ssd => "SSD",
            Self::Hdd => "HDD",
            Self::Nfs => "NFS",
            Self::ParallelFs => "PFS",
            Self::Unknown => "---",
        }
    }
}

impl fmt::Display for DiskLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[must_use]
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "K", "M", "G", "T", "P"];
    if bytes < 1024 {
        return format!("{bytes}B");
    }

    let mut unit_idx = 0;
    let mut divisor = 1_u64;
    while unit_idx + 1 < UNITS.len() && bytes / divisor >= 1024 {
        divisor = divisor.saturating_mul(1024);
        unit_idx += 1;
    }

    let tenths = bytes.saturating_mul(10) / divisor;
    format!("{}.{:01}{}", tenths / 10, tenths % 10, UNITS[unit_idx])
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSnapshot {
    pub captured_at: SystemTime,
    pub jobs: Vec<Job>,
    pub nodes: Vec<Node>,
    pub accounting: Vec<AccountingRecord>,
    pub disk_info: Vec<DiskInfo>,
    pub job_summary: JobSummary,
    pub gpu_summary: GpuSummary,
    pub warnings: Vec<String>,
}

impl ClusterSnapshot {
    #[must_use]
    pub fn new(
        jobs: Vec<Job>,
        nodes: Vec<Node>,
        accounting: Vec<AccountingRecord>,
        disk_info: Vec<DiskInfo>,
        current_user: &str,
        warnings: Vec<String>,
    ) -> Self {
        let job_summary = summarize_jobs(&jobs, current_user);
        let gpu_summary = summarize_gpus(&nodes, &jobs);
        Self {
            captured_at: SystemTime::now(),
            jobs,
            nodes,
            accounting,
            disk_info,
            job_summary,
            gpu_summary,
            warnings,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum PanelId {
    #[default]
    Jobs,
    Nodes,
    Gpus,
    Disks,
    Summary,
}

impl PanelId {
    pub const ALL: [Self; 5] = [
        Self::Jobs,
        Self::Nodes,
        Self::Gpus,
        Self::Disks,
        Self::Summary,
    ];

    #[must_use]
    pub const fn title(self) -> &'static str {
        match self {
            Self::Jobs => "Jobs",
            Self::Nodes => "Nodes",
            Self::Gpus => "GPUs / Resources",
            Self::Disks => "Disks",
            Self::Summary => "Summary / Accounting",
        }
    }

    #[must_use]
    pub const fn index(self) -> usize {
        match self {
            Self::Jobs => 0,
            Self::Nodes => 1,
            Self::Gpus => 2,
            Self::Disks => 3,
            Self::Summary => 4,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum JobColumn {
    #[default]
    State,
    JobId,
    User,
    Partition,
    Name,
    Nodes,
    Cpus,
    Gpus,
    Memory,
    Time,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum NodeColumn {
    #[default]
    State,
    Name,
    CpusTotal,
    CpusFree,
    MemoryTotal,
    MemoryFree,
    GpusTotal,
    GpusFree,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum AccountingColumn {
    #[default]
    JobId,
    User,
    State,
    Partition,
    Cpus,
    Memory,
    Elapsed,
    End,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FilterExpression {
    pub query: String,
    pub owner: Option<String>,
    pub state: Option<String>,
    pub partition: Option<String>,
    pub gpu_type: Option<String>,
    pub node_state: Option<String>,
}

impl FilterExpression {
    #[must_use]
    pub fn parse(input: &str) -> Self {
        let mut expression = Self::default();
        let mut free_text = Vec::new();
        for token in input.split_whitespace() {
            let Some((key, value)) = token.split_once('=') else {
                free_text.push(token.to_string());
                continue;
            };
            let key = key.trim().to_lowercase();
            let value = value.trim().to_string();
            match key.as_str() {
                "owner" | "user" => expression.owner = Some(value),
                "state" => expression.state = Some(value),
                "part" | "partition" => expression.partition = Some(value),
                "gpu" | "gpu_type" | "gres" => expression.gpu_type = Some(value),
                "node_state" | "nstate" => expression.node_state = Some(value),
                _ => free_text.push(token.to_string()),
            }
        }
        expression.query = free_text.join(" ").to_lowercase();
        expression
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.query.is_empty()
            && self.owner.is_none()
            && self.state.is_none()
            && self.partition.is_none()
            && self.gpu_type.is_none()
            && self.node_state.is_none()
    }
}

#[must_use]
pub fn numeric_job_id(job_id: &str) -> (u64, u64) {
    let parts: Vec<&str> = job_id
        .split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty())
        .collect();
    let base = parts
        .first()
        .and_then(|s| s.parse().ok())
        .unwrap_or(u64::MAX);
    let task = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    (base, task)
}

#[must_use]
pub fn state_rank(state: &str) -> u8 {
    let state = state.to_ascii_uppercase();
    if state.starts_with('R') {
        0
    } else if state.starts_with("CG") || state.starts_with("COMPLET") {
        1
    } else if state.starts_with('P') {
        2
    } else if state.starts_with("CONFIG") {
        3
    } else {
        4
    }
}

#[must_use]
pub fn parse_slurm_time(time_str: &str) -> u64 {
    let mut days = 0;
    let mut rest = time_str;
    if let Some((d_str, t_str)) = time_str.split_once('-') {
        days = d_str.parse::<u64>().unwrap_or(0);
        rest = t_str;
    }
    let parts: Vec<&str> = rest.split(':').collect();
    let mut total_seconds = days * 86400;
    if parts.len() == 3 {
        let h = parts[0].parse::<u64>().unwrap_or(0);
        let m = parts[1].parse::<u64>().unwrap_or(0);
        let s = parts[2].parse::<u64>().unwrap_or(0);
        total_seconds += h * 3600 + m * 60 + s;
    } else if parts.len() == 2 {
        let m = parts[0].parse::<u64>().unwrap_or(0);
        let s = parts[1].parse::<u64>().unwrap_or(0);
        total_seconds += m * 60 + s;
    }
    total_seconds
}

#[must_use]
pub fn sort_jobs(mut jobs: Vec<Job>, column: JobColumn, direction: SortDirection) -> Vec<Job> {
    jobs.sort_by(|a, b| {
        let ordering = match column {
            JobColumn::State => {
                (a.state_rank(), a.id_sort_key()).cmp(&(b.state_rank(), b.id_sort_key()))
            }
            JobColumn::JobId => a.id_sort_key().cmp(&b.id_sort_key()),
            JobColumn::User => a.user.to_lowercase().cmp(&b.user.to_lowercase()),
            JobColumn::Partition => a.partition.to_lowercase().cmp(&b.partition.to_lowercase()),
            JobColumn::Name => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
            JobColumn::Nodes => a.nodes.cmp(&b.nodes),
            JobColumn::Cpus => a.cpus.cmp(&b.cpus),
            JobColumn::Gpus => a.gpu_total().cmp(&b.gpu_total()),
            JobColumn::Memory => a.memory.cmp(&b.memory),
            JobColumn::Time => parse_slurm_time(&a.time_used).cmp(&parse_slurm_time(&b.time_used)),
        };
        match direction {
            SortDirection::Asc => ordering,
            SortDirection::Desc => ordering.reverse(),
        }
    });
    jobs
}

#[must_use]
pub fn sort_nodes(mut nodes: Vec<Node>, column: NodeColumn, direction: SortDirection) -> Vec<Node> {
    nodes.sort_by(|a, b| {
        let ordering = match column {
            NodeColumn::State => a.state.to_lowercase().cmp(&b.state.to_lowercase()),
            NodeColumn::Name => a.name.cmp(&b.name),
            NodeColumn::CpusTotal => a.cpus.total.cmp(&b.cpus.total),
            NodeColumn::CpusFree => a.cpus.idle.cmp(&b.cpus.idle),
            NodeColumn::MemoryTotal => a.memory_total.cmp(&b.memory_total),
            NodeColumn::MemoryFree => a.memory_free.cmp(&b.memory_free),
            NodeColumn::GpusTotal => a.gpu_total().cmp(&b.gpu_total()),
            NodeColumn::GpusFree => (a.gpu_total().saturating_sub(a.gpu_allocated()))
                .cmp(&(b.gpu_total().saturating_sub(b.gpu_allocated()))),
        };
        match direction {
            SortDirection::Asc => ordering,
            SortDirection::Desc => ordering.reverse(),
        }
    });
    nodes
}

#[must_use]
pub fn sort_accounting(
    mut rows: Vec<AccountingRecord>,
    column: AccountingColumn,
    direction: SortDirection,
) -> Vec<AccountingRecord> {
    rows.sort_by(|a, b| {
        let ordering = match column {
            AccountingColumn::JobId => numeric_job_id(&a.job_id).cmp(&numeric_job_id(&b.job_id)),
            AccountingColumn::User => a.user.to_lowercase().cmp(&b.user.to_lowercase()),
            AccountingColumn::State => a.state.to_lowercase().cmp(&b.state.to_lowercase()),
            AccountingColumn::Partition => {
                a.partition.to_lowercase().cmp(&b.partition.to_lowercase())
            }
            AccountingColumn::Cpus => a.cpus.cmp(&b.cpus),
            AccountingColumn::Memory => a.memory.cmp(&b.memory),
            AccountingColumn::Elapsed => a.elapsed.cmp(&b.elapsed),
            AccountingColumn::End => a.end.cmp(&b.end),
        };
        match direction {
            SortDirection::Asc => ordering,
            SortDirection::Desc => ordering.reverse(),
        }
    });
    rows
}

#[must_use]
pub fn filter_jobs<'a>(
    jobs: &'a [Job],
    filter: &FilterExpression,
    current_user: &str,
) -> Vec<&'a Job> {
    jobs.iter()
        .filter(|job| {
            if let Some(owner) = &filter.owner {
                let owner = owner.to_lowercase();
                let matches_owner = match owner.as_str() {
                    "me" => job.user == current_user,
                    "others" | "other" => job.user != current_user,
                    "all" => true,
                    user => job.user.eq_ignore_ascii_case(user),
                };
                if !matches_owner {
                    return false;
                }
            }
            if let Some(state) = &filter.state {
                let wanted = state.to_lowercase();
                let state = job.state.to_lowercase();
                let matches_state = match wanted.as_str() {
                    "running" | "run" | "r" => state.starts_with('r'),
                    "pending" | "pend" | "pd" | "p" => state.starts_with('p'),
                    other => state.contains(other),
                };
                if !matches_state {
                    return false;
                }
            }
            if let Some(partition) = &filter.partition {
                if !job.partition.eq_ignore_ascii_case(partition) {
                    return false;
                }
            }
            if let Some(gpu_type) = &filter.gpu_type {
                let gpu_type = gpu_type.to_lowercase();
                if !job
                    .gpus
                    .keys()
                    .any(|key| key.to_lowercase().contains(&gpu_type))
                {
                    return false;
                }
            }
            filter.query.is_empty() || job.searchable_text().contains(&filter.query)
        })
        .collect()
}

#[must_use]
pub fn filter_nodes<'a>(nodes: &'a [Node], filter: &FilterExpression) -> Vec<&'a Node> {
    nodes
        .iter()
        .filter(|node| {
            if let Some(state) = &filter.node_state {
                if !node.state.to_lowercase().contains(&state.to_lowercase()) {
                    return false;
                }
            }
            if let Some(gpu_type) = &filter.gpu_type {
                let gpu_type = gpu_type.to_lowercase();
                if !node
                    .gpus
                    .keys()
                    .any(|key| key.to_lowercase().contains(&gpu_type))
                {
                    return false;
                }
            }
            filter.query.is_empty() || node.searchable_text().contains(&filter.query)
        })
        .collect()
}

#[must_use]
pub fn filter_accounting<'a>(
    rows: &'a [AccountingRecord],
    filter: &FilterExpression,
    current_user: &str,
) -> Vec<&'a AccountingRecord> {
    rows.iter()
        .filter(|row| {
            if let Some(owner) = &filter.owner {
                let owner = owner.to_lowercase();
                let matches_owner = match owner.as_str() {
                    "me" => row.user == current_user,
                    "others" | "other" => row.user != current_user,
                    "all" => true,
                    user => row.user.eq_ignore_ascii_case(user),
                };
                if !matches_owner {
                    return false;
                }
            }
            if let Some(state) = &filter.state {
                if !row.state.to_lowercase().contains(&state.to_lowercase()) {
                    return false;
                }
            }
            if let Some(partition) = &filter.partition {
                if !row.partition.eq_ignore_ascii_case(partition) {
                    return false;
                }
            }
            filter.query.is_empty() || row.searchable_text().contains(&filter.query)
        })
        .collect()
}

#[must_use]
pub fn summarize_jobs(jobs: &[Job], current_user: &str) -> JobSummary {
    let mut summary = JobSummary::default();
    for job in jobs {
        let state = job.state.to_ascii_uppercase();
        let target = if state.starts_with('R') {
            Some("running")
        } else if state.starts_with('P') {
            Some("pending")
        } else {
            None
        };
        let Some(target) = target else {
            continue;
        };
        let owner_bucket = if job.user == current_user {
            "me"
        } else {
            "others"
        };
        add_job_bucket(&mut summary.all, target, job);
        let user_bucket = summary.users.entry(job.user.clone()).or_default();
        add_job_bucket(user_bucket, target, job);

        if owner_bucket == "me" {
            add_job_bucket(&mut summary.me, target, job);
        } else {
            add_job_bucket(&mut summary.others, target, job);
        }
    }
    summary
}

fn add_job_bucket(summary: &mut OwnerSummary, target: &str, job: &Job) {
    let bucket = if target == "running" {
        &mut summary.running
    } else {
        &mut summary.pending
    };
    bucket.jobs += 1;
    bucket.cpus += job.cpus;
    bucket.memory.0 += job.memory.0;
    bucket.gpus += job.gpu_total();
}

#[must_use]
pub fn summarize_gpus(nodes: &[Node], jobs: &[Job]) -> GpuSummary {
    let mut summary = GpuSummary::default();
    for node in nodes {
        for (gpu_type, count) in &node.gpus {
            let bucket = summary.by_type.entry(gpu_type.clone()).or_default();
            bucket.total += count;
            summary.total += count;
        }
        for (gpu_type, count) in &node.gpus_allocated {
            let bucket = summary.by_type.entry(gpu_type.clone()).or_default();
            bucket.active += count;
            summary.active += count;
        }
    }
    for job in jobs {
        let state = job.state.to_ascii_uppercase();
        if state.starts_with('P') {
            let job_total = job.gpu_total();
            summary.reserved += job_total;
            for (gpu_type, count) in &job.gpus {
                let bucket = summary.by_type.entry(gpu_type.clone()).or_default();
                bucket.reserved += count;
            }
        }
    }
    summary.free_estimate = summary.total.saturating_sub(summary.active);
    for bucket in summary.by_type.values_mut() {
        bucket.free_estimate = bucket.total.saturating_sub(bucket.active);
    }
    summary
}

#[must_use]
pub fn bucket_display(bucket: &JobBucket) -> String {
    format!(
        "{} jobs / {} GPU / {} CPU / {}",
        bucket.jobs, bucket.gpus, bucket.cpus, bucket.memory
    )
}

#[must_use]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
