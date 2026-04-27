//! Parsers for Slurm command output.

use std::collections::BTreeMap;

use regex::Regex;
use slmtop_core::{
    AccountingRecord, CpuCounts, DiskInfo, DiskLabel, DiskUserUsage, GpuMap, Job, MemoryMb, Node,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parsed<T> {
    pub rows: Vec<T>,
    pub warnings: Vec<String>,
}

impl<T> Parsed<T> {
    #[must_use]
    pub const fn new(rows: Vec<T>, warnings: Vec<String>) -> Self {
        Self { rows, warnings }
    }
}

#[must_use]
pub fn parse_squeue(output: &str) -> Parsed<Job> {
    let mut jobs = Vec::new();
    let mut warnings = Vec::new();
    for (idx, line) in output.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || looks_like_header(line, "JOBID") {
            continue;
        }
        let parts: Vec<_> = line.split('|').map(str::trim).collect();
        if parts.len() < 11 {
            warnings.push(format!(
                "squeue line {} has {} fields: {line}",
                idx + 1,
                parts.len()
            ));
            continue;
        }
        jobs.push(Job {
            id: parts[0].to_string(),
            user: parts[1].to_string(),
            state: parts[2].to_string(),
            partition: parts[3].to_string(),
            name: parts[4].to_string(),
            nodes: parts[5].to_string(),
            cpus: parse_u64(parts[6]),
            memory: parse_memory_mb(parts[7]),
            gpus: parse_gpu_map(parts[8]),
            gres_raw: parts[8].to_string(),
            time_used: parts[9].to_string(),
            node_list: parts[10].to_string(),
            reason: parts
                .get(11)
                .filter(|value| !value.is_empty())
                .map(|value| (*value).to_string()),
            time_limit: parts.get(12).map_or_else(String::new, |s| (*s).to_string()),
        });
    }
    Parsed::new(jobs, warnings)
}

#[must_use]
pub fn parse_sinfo(output: &str) -> Parsed<Node> {
    let mut nodes = Vec::new();
    let mut warnings = Vec::new();
    for (idx, line) in output.lines().enumerate() {
        let line = line.trim();
        if line.is_empty()
            || looks_like_header(line, "NODELIST")
            || looks_like_header(line, "HOSTNAMES")
        {
            continue;
        }
        let parts: Vec<_> = line.split('|').map(str::trim).collect();
        if parts.len() < 7 {
            warnings.push(format!(
                "sinfo line {} has {} fields: {line}",
                idx + 1,
                parts.len()
            ));
            continue;
        }
        let cpus = parse_cpu_state(parts[3], parse_u64(parts[2]));
        let total = parse_memory_mb(parts[4]);
        let free = parse_memory_mb(parts[5]);
        let reserved = total.saturating_sub(free);
        nodes.push(Node {
            name: parts[0].to_string(),
            state: parts[1].to_string(),
            cpus,
            memory_total: total,
            memory_reserved: reserved,
            memory_free: free,
            gpus: parse_gpu_map(parts[6]),
            gpus_allocated: parts
                .get(8)
                .map_or_else(GpuMap::default, |s| parse_gpu_map(s)),
            gres_raw: parts[6].to_string(),
            reason: parts
                .get(7)
                .filter(|value| !value.is_empty())
                .map(|value| (*value).to_string()),
        });
    }
    Parsed::new(nodes, warnings)
}

#[must_use]
pub fn parse_sacct(output: &str) -> Parsed<AccountingRecord> {
    let mut rows = Vec::new();
    let mut warnings = Vec::new();
    for (idx, line) in output.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || looks_like_header(line, "JobID") {
            continue;
        }
        let parts: Vec<_> = line.split('|').map(str::trim).collect();
        if parts.len() < 10 {
            warnings.push(format!(
                "sacct line {} has {} fields: {line}",
                idx + 1,
                parts.len()
            ));
            continue;
        }
        rows.push(AccountingRecord {
            job_id: parts[0].to_string(),
            user: parts[1].to_string(),
            state: parts[2].to_string(),
            partition: parts[3].to_string(),
            name: parts[4].to_string(),
            cpus: parse_u64(parts[5]),
            memory: parse_memory_mb(parts[6]),
            elapsed: parts[7].to_string(),
            start: parts[8].to_string(),
            end: parts[9].to_string(),
        });
    }
    Parsed::new(rows, warnings)
}

#[must_use]
pub fn parse_memory_mb(value: &str) -> MemoryMb {
    let value = value.trim();
    if value.is_empty()
        || matches!(
            value.to_ascii_lowercase().as_str(),
            "n/a" | "(null)" | "unknown"
        )
    {
        return MemoryMb::zero();
    }

    let mut number = String::new();
    let mut unit = None;
    for ch in value.chars() {
        if ch.is_ascii_digit() || ch == '.' {
            number.push(ch);
        } else if ch.is_ascii_alphabetic() {
            unit = Some(ch.to_ascii_uppercase());
            break;
        }
    }

    MemoryMb(decimal_memory_to_mb(&number, unit.unwrap_or('M')))
}

#[must_use]
pub fn parse_gpu_map(value: &str) -> GpuMap {
    let text = value.trim().to_ascii_lowercase();
    if text.is_empty() || matches!(text.as_str(), "n/a" | "(null)" | "none") {
        return BTreeMap::new();
    }

    let Ok(regex) = Regex::new(r"gpu(?::([^,=():]+))?[=:](\d+)") else {
        return BTreeMap::new();
    };
    let mut map = BTreeMap::new();
    for capture in regex.captures_iter(&text) {
        let gpu_type = capture
            .get(1)
            .map_or("generic", |match_| match_.as_str().trim())
            .trim_matches('/');
        let gpu_type = if gpu_type.is_empty() {
            "generic"
        } else {
            gpu_type
        };
        let count = capture
            .get(2)
            .and_then(|match_| match_.as_str().parse::<u64>().ok())
            .unwrap_or(0);
        if count > 0 {
            *map.entry(gpu_type.to_string()).or_insert(0) += count;
        }
    }
    map
}

#[must_use]
pub fn parse_cpu_state(value: &str, total: u64) -> CpuCounts {
    let mut counts = CpuCounts {
        total,
        ..CpuCounts::default()
    };
    let parts: Vec<_> = value.split('/').collect();
    if let Some(allocated) = parts.first() {
        counts.allocated = parse_u64(allocated);
    }
    if let Some(idle) = parts.get(1) {
        counts.idle = parse_u64(idle);
    }
    if let Some(other) = parts.get(2) {
        counts.other = parse_u64(other);
    }
    if counts.total == 0 {
        counts.total = counts.allocated + counts.idle + counts.other;
    }
    counts
}

#[must_use]
pub fn parse_u64(value: &str) -> u64 {
    value
        .trim()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .unwrap_or(0)
}

fn decimal_memory_to_mb(number: &str, unit: char) -> u64 {
    let (whole, fraction, scale) = parse_decimal_parts(number);
    match unit {
        'K' => whole / 1024,
        'G' => whole
            .saturating_mul(1024)
            .saturating_add(fraction.saturating_mul(1024) / scale),
        'T' => whole
            .saturating_mul(1024 * 1024)
            .saturating_add(fraction.saturating_mul(1024 * 1024) / scale),
        _ => whole.saturating_add(fraction / scale),
    }
}

fn parse_decimal_parts(number: &str) -> (u64, u64, u64) {
    let Some((whole, fraction)) = number.split_once('.') else {
        return (number.parse().unwrap_or(0), 0, 1);
    };
    let whole = whole.parse().unwrap_or(0);
    let fraction_digits: String = fraction.chars().filter(char::is_ascii_digit).collect();
    if fraction_digits.is_empty() {
        return (whole, 0, 1);
    }
    let scale = 10_u64.saturating_pow(u32::try_from(fraction_digits.len()).unwrap_or(0));
    (whole, fraction_digits.parse().unwrap_or(0), scale)
}

fn looks_like_header(line: &str, marker: &str) -> bool {
    line.starts_with(marker)
        || line
            .to_ascii_uppercase()
            .starts_with(&marker.to_ascii_uppercase())
}

#[must_use]
pub fn parse_df(output: &str) -> Vec<DiskInfo> {
    let mut disks = Vec::new();
    for line in output.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with("Filesystem") || line.starts_with("filesystem") {
            continue;
        }
        let parts: Vec<_> = line.split_whitespace().collect();
        // Expected: source fstype size used avail pcent target
        if parts.len() < 7 {
            continue;
        }
        let percent_str = parts[5].trim_end_matches('%');
        let use_percent = percent_str.parse::<u8>().unwrap_or(0);
        let fstype = parts[1].to_lowercase();
        let source = parts[0].to_lowercase();
        let label = classify_disk(&source, &fstype);
        // Skip pseudo filesystems
        if matches!(
            fstype.as_str(),
            "tmpfs"
                | "devtmpfs"
                | "sysfs"
                | "proc"
                | "devpts"
                | "cgroup"
                | "cgroup2"
                | "securityfs"
                | "pstore"
                | "efivarfs"
                | "bpf"
                | "autofs"
                | "hugetlbfs"
                | "mqueue"
                | "debugfs"
                | "tracefs"
                | "fusectl"
                | "configfs"
                | "ramfs"
                | "rpc_pipefs"
                | "nsfs"
                | "overlay"
        ) {
            continue;
        }
        disks.push(DiskInfo {
            mount: parts[6..].join(" "),
            fstype: parts[1].to_string(),
            size: parts[2].to_string(),
            used: parts[3].to_string(),
            avail: parts[4].to_string(),
            use_percent,
            label,
        });
    }
    disks
}

#[must_use]
pub fn parse_find_user_usage(output: &str) -> Vec<DiskUserUsage> {
    let mut usage: BTreeMap<String, DiskUserUsage> = BTreeMap::new();
    for line in output.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split('\t');
        let Some(user) = fields.next().map(str::trim).filter(|user| !user.is_empty()) else {
            continue;
        };
        let blocks = fields
            .next()
            .map(str::trim)
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        let entry = usage
            .entry(user.to_string())
            .or_insert_with(|| DiskUserUsage {
                user: user.to_string(),
                bytes: 0,
                entries: 0,
            });
        entry.bytes = entry.bytes.saturating_add(blocks.saturating_mul(512));
        entry.entries = entry.entries.saturating_add(1);
    }

    let mut rows = usage.into_values().collect::<Vec<_>>();
    rows.sort_by(|a, b| b.bytes.cmp(&a.bytes).then_with(|| a.user.cmp(&b.user)));
    rows
}

#[must_use]
pub fn parse_find_current_user_usage(output: &str, user: &str) -> Vec<DiskUserUsage> {
    let mut row = DiskUserUsage {
        user: user.to_string(),
        bytes: 0,
        entries: 0,
    };
    for line in output.lines() {
        let Ok(blocks) = line.trim().parse::<u64>() else {
            continue;
        };
        row.bytes = row.bytes.saturating_add(blocks.saturating_mul(512));
        row.entries = row.entries.saturating_add(1);
    }
    if row.entries == 0 {
        Vec::new()
    } else {
        vec![row]
    }
}

#[must_use]
pub fn parse_du_user_usage(output: &str, user: &str) -> Vec<DiskUserUsage> {
    let Some(bytes) = output
        .lines()
        .find_map(|line| line.split_whitespace().next()?.parse::<u64>().ok())
    else {
        return Vec::new();
    };
    vec![DiskUserUsage {
        user: user.to_string(),
        bytes,
        entries: 0,
    }]
}

#[must_use]
pub fn parse_lfs_quota_user_usage(output: &str, user: &str) -> Vec<DiskUserUsage> {
    for line in output.lines() {
        let line = line.trim();
        if line.is_empty()
            || line.starts_with("Disk quotas")
            || line.starts_with("Filesystem")
            || line.starts_with("none")
        {
            continue;
        }
        let tokens = line.split_whitespace().collect::<Vec<_>>();
        let offset = usize::from(tokens.first().is_some_and(|token| token.starts_with('/')));
        let used_token = tokens.get(offset).copied();
        let entries = tokens
            .get(offset + 4)
            .and_then(|token| parse_quota_count(token))
            .unwrap_or(0);
        if let Some(bytes) = used_token.and_then(quota_size_to_bytes) {
            return vec![DiskUserUsage {
                user: user.to_string(),
                bytes,
                entries,
            }];
        }
    }
    Vec::new()
}

fn parse_quota_count(value: &str) -> Option<u64> {
    let digits = value
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

fn quota_size_to_bytes(value: &str) -> Option<u64> {
    let mut number = String::new();
    let mut unit = None;
    for ch in value.trim().chars() {
        if ch.is_ascii_digit() || ch == '.' {
            number.push(ch);
        } else if ch.is_ascii_alphabetic() {
            unit = Some(ch.to_ascii_uppercase());
            break;
        }
    }
    if number.is_empty() {
        return None;
    }
    let (whole, fraction, scale) = decimal_parts_u64(&number);
    let multiplier = match unit.unwrap_or('B') {
        'K' => 1024,
        'M' => 1024_u64.pow(2),
        'G' => 1024_u64.pow(3),
        'T' => 1024_u64.pow(4),
        'P' => 1024_u64.pow(5),
        _ => 1,
    };
    Some(
        whole
            .saturating_mul(multiplier)
            .saturating_add(fraction.saturating_mul(multiplier) / scale),
    )
}

fn decimal_parts_u64(number: &str) -> (u64, u64, u64) {
    let mut parts = number.splitn(2, '.');
    let whole = parts
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let fraction_text = parts.next().unwrap_or("");
    let fraction = fraction_text.parse::<u64>().unwrap_or(0);
    let scale = 10_u64.saturating_pow(u32::try_from(fraction_text.len()).unwrap_or(0));
    (whole, fraction, scale.max(1))
}
fn classify_disk(source: &str, fstype: &str) -> DiskLabel {
    match fstype {
        "nfs" | "nfs4" | "nfs3" | "cifs" | "smb" | "smbfs" => DiskLabel::Nfs,
        "lustre" | "gpfs" | "beegfs" | "orangefs" | "pvfs2" | "cephfs" | "glusterfs" => {
            DiskLabel::ParallelFs
        }
        _ => {
            if source.contains("nvme") {
                DiskLabel::Ssd
            } else if source.starts_with("/dev/sd") {
                // Try to detect via sysfs — fall back to HDD for spinning disks
                let dev = source
                    .trim_start_matches("/dev/")
                    .chars()
                    .take_while(char::is_ascii_alphabetic)
                    .collect::<String>();
                let rotational_path = format!("/sys/block/{dev}/queue/rotational");
                match std::fs::read_to_string(&rotational_path) {
                    Ok(val) if val.trim() == "0" => DiskLabel::Ssd,
                    Ok(_) => DiskLabel::Hdd,
                    Err(_) => DiskLabel::Unknown,
                }
            } else {
                DiskLabel::Unknown
            }
        }
    }
}

#[must_use]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn parses_memory_units() {
        assert_eq!(parse_memory_mb("1024").0, 1024);
        assert_eq!(parse_memory_mb("1G").0, 1024);
        assert_eq!(parse_memory_mb("1.5T").0, 1_572_864);
        assert_eq!(parse_memory_mb("512K").0, 0);
        assert_eq!(parse_memory_mb("4000Mc").0, 4000);
    }

    #[test]
    fn parses_gpu_strings() {
        let parsed = parse_gpu_map("gpu:a100:2,gpu:h100=1,gres/gpu:4");
        assert_eq!(parsed.get("a100"), Some(&2));
        assert_eq!(parsed.get("h100"), Some(&1));
        assert_eq!(parsed.get("generic"), Some(&4));
    }

    #[test]
    fn parses_squeue_rows() {
        let output = "123|alice|RUNNING|gpu|train|1|8|32G|gpu:a100:2|01:02:03|node001|None\n";
        let parsed = parse_squeue(output);
        assert!(parsed.warnings.is_empty());
        assert_eq!(parsed.rows.len(), 1);
        assert_eq!(parsed.rows[0].gpu_total(), 2);
        assert_eq!(parsed.rows[0].memory.0, 32 * 1024);
        assert_eq!(parsed.rows[0].node_list, "node001");
    }

    #[test]
    fn parses_sinfo_rows() {
        let output = "node001|idle|64|4/58/2/64|257000|128000|gpu:a100:4|healthy\n";
        let parsed = parse_sinfo(output);
        assert!(parsed.warnings.is_empty());
        assert_eq!(parsed.rows[0].cpus.allocated, 4);
        assert_eq!(parsed.rows[0].gpu_total(), 4);
        assert_eq!(parsed.rows[0].memory_reserved.0, 129_000);
    }

    #[test]
    fn parses_sacct_rows() {
        let output = "123|alice|COMPLETED|cpu|analysis|16|8G|00:05:00|2026-01-01T00:00:00|2026-01-01T00:05:00\n";
        let parsed = parse_sacct(output);
        assert_eq!(parsed.rows[0].job_id, "123");
        assert_eq!(parsed.rows[0].cpus, 16);
        assert_eq!(parsed.rows[0].memory.0, 8192);
    }

    #[test]
    fn parses_find_user_usage() {
        let rows = parse_find_user_usage("alice\t2\nbob\t4\nalice\t3\n");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].user, "alice");
        assert_eq!(rows[0].bytes, 5 * 512);
        assert_eq!(rows[0].entries, 2);
        assert_eq!(rows[1].user, "bob");
        assert_eq!(rows[1].bytes, 4 * 512);
    }

    #[test]
    fn parses_find_current_user_usage() {
        let rows = parse_find_current_user_usage("2\n0\n4\ninvalid\n", "alice");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].user, "alice");
        assert_eq!(rows[0].bytes, 6 * 512);
        assert_eq!(rows[0].entries, 3);
    }

    #[test]
    fn parses_du_user_usage() {
        let rows = parse_du_user_usage("4096\t/vol/projects/alice\n", "alice");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].user, "alice");
        assert_eq!(rows[0].bytes, 4096);
        assert_eq!(rows[0].entries, 0);
    }

    #[test]
    fn parses_lfs_quota_user_usage() {
        let output = "\
Disk quotas for usr alice (uid 1000):
     Filesystem  kbytes   quota   limit   grace   files   quota   limit   grace
/vol/projects
                 1.5G       0       0       -       10       0       0       -
";
        let rows = parse_lfs_quota_user_usage(output, "alice");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].user, "alice");
        assert_eq!(rows[0].bytes, 1_610_612_736);
        assert_eq!(rows[0].entries, 10);
    }
}
