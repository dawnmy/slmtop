# slmtop

`slmtop` is a terminal dashboard developed with Rust for realtime Slurm monitoring. It is inspired by `htop` and `slurm-monitor-top`, and built for Slurm-specific jobs, nodes, GPU resources, disks and recent accounting data.
<img width="1728" height="1048" alt="image" src="https://github.com/user-attachments/assets/10f1cda4-637f-4ab7-a247-8cef99cc7ad9" />


## Features

- Four focusable TUI panels: jobs, nodes, GPUs/resources, disks and summary/accounting.
- Realtime refresh from Slurm CLI commands with per-command timeouts and structured errors.
- Sortable tables by keyboard or mouse header click.
- Search and typed filters per panel, for example `owner=me state=running gpu=a100`.
- Panel focus, resize, show/hide, row selection, and column visibility controls.
- Job details with guarded `cancel`, `hold`, `release`, and `requeue` actions.
- Typed parsers and resource aggregation for memory, CPUs, GPUs, GRES, and `sacct` rows.
- Backend trait layout ready for a future `slurmrestd` implementation.

## Requirements

- Rust toolchain with Cargo.
- Slurm CLI commands in `PATH`: `squeue`, `sinfo`, `scontrol`, `scancel`, and optionally `sacct`.


## Installation

Download the latest precompiled binary for your platform from the [releases page](https://github.com/dawnmy/slmtop/releases), or build from source as described below.

```bash
cargo build --release
./target/release/slmtop
```

Help:

```bash
Realtime Slurm cluster monitor

Usage: slmtop [OPTIONS]

Options:
  -i, --refresh-interval <REFRESH_INTERVAL>
          Refresh interval in seconds [default: 3]
  -t, --command-timeout <COMMAND_TIMEOUT>
          Per-command timeout in seconds [default: 4]
  -l, --accounting-limit <ACCOUNTING_LIMIT>
          Recent sacct rows to keep [default: 100]
  -u, --user <USER>
          Override the current username used for owner filters
  -T, --theme <THEME>
          Color theme: catppuccin, monokai, tokyonight, dracula, onedark, nightowl, classic [default: catppuccin]
  -h, --help
          Print help
  -V, --version
          Print version
```


Useful options:

```bash
slmtop --refresh-interval 2 --command-timeout 5 --accounting-limit 200 -T nightowl
slmtop --user bob
```

## Keybindings

- `q`: quit.
- `?`: help.
- `r`: refresh now.
- `t`: switch theme.
- `Tab` / `Shift-Tab` or `1`-`4`: change focused panel.
- Arrow keys or `j` / `k`: move selected row.
- `s`: cycle sort column for the focused panel.
- `d`: toggle sort direction.
- Mouse left-click on a table header: sort by that column.
- `/`: search the focused panel.
- `f`: set a typed filter on the focused panel.
- `c`: toggle optional columns in the focused panel.
- `x`: hide the focused panel.
- `v`: show the next hidden panel.
- `[` / `]`: resize panel width split.
- `{` / `}`: resize panel height split.
- `Enter` on a job: open job details.

In the job details popup:

- `c`: cancel job.
- `h`: hold job.
- `u`: release job.
- `r`: requeue job.
- `Esc`: close popup.

Job actions require a `y` confirmation before Slurm is changed.

## Filters

Filters are whitespace-separated tokens. Free text searches within the panel row, and key-value tokens narrow the result set:

```text
owner=me state=running
owner=others part=gpu gpu=a100 train
state=pending
node_state=drain gpu=h100
```

Supported keys include `owner`, `user`, `state`, `part`, `partition`, `gpu`, `gpu_type`, `gres`, and `node_state`.

## Workspace Layout

- `crates/slmtop-core`: domain models, sorting, filtering, search, summaries, and GPU aggregation.
- `crates/slmtop-parsers`: pure parsers for `squeue`, `sinfo`, `sacct`, memory, CPU, and GPU/GRES strings.
- `crates/slmtop-slurm`: backend traits, refresh orchestration, telemetry, and job-control contracts.
- `crates/slmtop-slurm-cli`: Slurm CLI backend.
- `crates/slmtop-tui`: Ratatui/Crossterm terminal UI.
- `crates/slmtop`: `slmtop` binary.

## Validation

```bash
cargo fmt --check
cargo test
cargo clippy --workspace --all-targets
cargo run -p slmtop -- --version-only
```
