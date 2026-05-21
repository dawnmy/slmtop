//! Terminal UI for slmtop.

use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::{self, Stdout};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event as CrosstermEvent, KeyCode, KeyEvent,
    KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, TableState, Wrap};
use ratatui::{Frame, Terminal};
use serde::{Deserialize, Serialize};
use slmtop_core::{
    filter_jobs, filter_nodes, human_bytes, parse_slurm_time, sort_jobs, sort_nodes,
    AccountingColumn, ClusterSnapshot, DiskInfo, DiskUserUsage, FilterExpression, GpuSummary, Job,
    JobColumn, Node, NodeColumn, OwnerSummary, PanelId, SortDirection,
};
use slmtop_slurm::{
    refresh_backend, BackendConfig, DiskUsageProgress, DiskUsageProgressCallback,
    DiskUsageProgressStage, SlurmClient, SlurmError, SnapshotEnvelope,
};
use thiserror::Error;
use time::{OffsetDateTime, UtcOffset};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;

type TerminalBackend = CrosstermBackend<Stdout>;

#[derive(Debug, Error)]
pub enum TuiError {
    #[error("terminal IO error: {0}")]
    Io(#[from] io::Error),
    #[error("slurm backend error: {0}")]
    Slurm(#[from] SlurmError),
}

pub type Result<T> = std::result::Result<T, TuiError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThemeName {
    Catppuccin,
    TokyoNight,
    Dracula,
    OneDarkPro,
    Monokai,
    NightOwl,
    Classic,
}

impl ThemeName {
    #[must_use]
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "monokai" => Self::Monokai,
            "onedark" | "onedarkpro" => Self::OneDarkPro,
            "nightowl" | "night_owl" => Self::NightOwl,
            "tokyonight" | "tokyo_night" => Self::TokyoNight,
            "dracula" => Self::Dracula,
            "classic" => Self::Classic,
            _ => Self::Catppuccin, // Default theme
        }
    }

    const ALL: [Self; 7] = [
        Self::Catppuccin,
        Self::TokyoNight,
        Self::Dracula,
        Self::OneDarkPro,
        Self::Monokai,
        Self::NightOwl,
        Self::Classic,
    ];

    fn cycle(self) -> Self {
        let idx = Self::ALL.iter().position(|t| *t == self).unwrap_or(0);
        Self::ALL[(idx + 1) % Self::ALL.len()]
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Classic => "Classic",
            Self::Monokai => "Monokai",
            Self::Catppuccin => "Catppuccin Mocha",
            Self::OneDarkPro => "One Dark Pro",
            Self::NightOwl => "Night Owl",
            Self::TokyoNight => "Tokyo Night",
            Self::Dracula => "Dracula",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Theme {
    border_focused: Style,
    border_unfocused: Style,
    header_style: Style,
    highlight: Style,
    status_badge: Style,
    state_running: Style,
    state_pending: Style,
    state_failed: Style,
    node_idle: Style,
    node_down: Style,
    node_mixed: Style,
    summary_all: Style,
    summary_me: Style,
    summary_others: Style,
    _bar_filled: Color,
    _bar_empty: Color,
    warning_border: Style,
}

fn selected_row_style() -> Style {
    Style::default()
        .fg(Color::Rgb(8, 20, 24))
        .bg(Color::Rgb(0, 188, 188))
}

impl Theme {
    fn from_name(name: ThemeName) -> Self {
        match name {
            ThemeName::Classic => Self::classic(),
            ThemeName::Monokai => Self::monokai(),
            ThemeName::Catppuccin => Self::catppuccin(),
            ThemeName::OneDarkPro => Self::onedark(),
            ThemeName::NightOwl => Self::nightowl(),
            ThemeName::TokyoNight => Self::tokyonight(),
            ThemeName::Dracula => Self::dracula(),
        }
    }

    fn classic() -> Self {
        Self {
            border_focused: Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
            border_unfocused: Style::default().fg(Color::DarkGray),
            header_style: Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
            highlight: selected_row_style(),
            status_badge: Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
            state_running: Style::default().fg(Color::Green),
            state_pending: Style::default().fg(Color::Yellow),
            state_failed: Style::default().fg(Color::Red),
            node_idle: Style::default().fg(Color::Green),
            node_down: Style::default().fg(Color::Red),
            node_mixed: Style::default().fg(Color::Yellow),
            summary_all: Style::default().fg(Color::Cyan),
            summary_me: Style::default().fg(Color::Green),
            summary_others: Style::default().fg(Color::Yellow),
            _bar_filled: Color::Cyan,
            _bar_empty: Color::DarkGray,
            warning_border: Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        }
    }

    fn monokai() -> Self {
        let green = Color::Rgb(166, 226, 46);
        let orange = Color::Rgb(253, 151, 31);
        let magenta = Color::Rgb(249, 38, 114);
        let cyan = Color::Rgb(102, 217, 239);
        let bg_sel = Color::Rgb(73, 72, 62);
        Self {
            border_focused: Style::default().fg(cyan).add_modifier(Modifier::BOLD),
            border_unfocused: Style::default().fg(Color::Rgb(117, 113, 94)),
            header_style: Style::default()
                .fg(Color::Black)
                .bg(cyan)
                .add_modifier(Modifier::BOLD),
            highlight: selected_row_style(),
            status_badge: Style::default()
                .fg(Color::Black)
                .bg(green)
                .add_modifier(Modifier::BOLD),
            state_running: Style::default().fg(green),
            state_pending: Style::default().fg(orange),
            state_failed: Style::default().fg(magenta),
            node_idle: Style::default().fg(green),
            node_down: Style::default().fg(magenta),
            node_mixed: Style::default().fg(orange),
            summary_all: Style::default().fg(cyan),
            summary_me: Style::default().fg(green),
            summary_others: Style::default().fg(orange),
            _bar_filled: green,
            _bar_empty: bg_sel,
            warning_border: Style::default().fg(orange).add_modifier(Modifier::BOLD),
        }
    }

    fn catppuccin() -> Self {
        let mauve = Color::Rgb(203, 166, 247);
        let green = Color::Rgb(166, 227, 161);
        let peach = Color::Rgb(250, 179, 135);
        let red = Color::Rgb(243, 139, 168);
        let teal = Color::Rgb(148, 226, 213);
        let surface1 = Color::Rgb(69, 71, 90);
        Self {
            border_focused: Style::default().fg(mauve).add_modifier(Modifier::BOLD),
            border_unfocused: Style::default().fg(surface1),
            header_style: Style::default()
                .fg(Color::Rgb(30, 30, 46))
                .bg(mauve)
                .add_modifier(Modifier::BOLD),
            highlight: selected_row_style(),
            status_badge: Style::default()
                .fg(Color::Rgb(30, 30, 46))
                .bg(mauve)
                .add_modifier(Modifier::BOLD),
            state_running: Style::default().fg(green),
            state_pending: Style::default().fg(peach),
            state_failed: Style::default().fg(red),
            node_idle: Style::default().fg(green),
            node_down: Style::default().fg(red),
            node_mixed: Style::default().fg(peach),
            summary_all: Style::default().fg(teal),
            summary_me: Style::default().fg(green),
            summary_others: Style::default().fg(peach),
            _bar_filled: teal,
            _bar_empty: surface1,
            warning_border: Style::default().fg(peach).add_modifier(Modifier::BOLD),
        }
    }

    fn onedark() -> Self {
        let blue = Color::Rgb(97, 175, 239);
        let green = Color::Rgb(152, 195, 121);
        let orange = Color::Rgb(209, 154, 102);
        let red = Color::Rgb(224, 108, 117);
        let purple = Color::Rgb(198, 120, 221);
        let gutter = Color::Rgb(76, 82, 99);
        Self {
            border_focused: Style::default().fg(blue).add_modifier(Modifier::BOLD),
            border_unfocused: Style::default().fg(gutter),
            header_style: Style::default()
                .fg(Color::Rgb(40, 44, 52))
                .bg(blue)
                .add_modifier(Modifier::BOLD),
            highlight: selected_row_style(),
            status_badge: Style::default()
                .fg(Color::Rgb(40, 44, 52))
                .bg(blue)
                .add_modifier(Modifier::BOLD),
            state_running: Style::default().fg(green),
            state_pending: Style::default().fg(orange),
            state_failed: Style::default().fg(red),
            node_idle: Style::default().fg(green),
            node_down: Style::default().fg(red),
            node_mixed: Style::default().fg(orange),
            summary_all: Style::default().fg(purple),
            summary_me: Style::default().fg(green),
            summary_others: Style::default().fg(orange),
            _bar_filled: blue,
            _bar_empty: gutter,
            warning_border: Style::default().fg(orange).add_modifier(Modifier::BOLD),
        }
    }

    fn nightowl() -> Self {
        let cyan = Color::Rgb(127, 219, 202);
        let orange = Color::Rgb(239, 143, 82);
        let yellow = Color::Rgb(255, 203, 107);
        let red = Color::Rgb(239, 83, 80);
        let blue = Color::Rgb(130, 170, 255);
        let surface = Color::Rgb(30, 50, 80);
        Self {
            border_focused: Style::default().fg(cyan).add_modifier(Modifier::BOLD),
            border_unfocused: Style::default().fg(Color::Rgb(68, 98, 130)),
            header_style: Style::default()
                .fg(Color::Rgb(1, 22, 39))
                .bg(cyan)
                .add_modifier(Modifier::BOLD),
            highlight: selected_row_style(),
            status_badge: Style::default()
                .fg(Color::Rgb(1, 22, 39))
                .bg(cyan)
                .add_modifier(Modifier::BOLD),
            state_running: Style::default().fg(cyan),
            state_pending: Style::default().fg(yellow),
            state_failed: Style::default().fg(red),
            node_idle: Style::default().fg(cyan),
            node_down: Style::default().fg(red),
            node_mixed: Style::default().fg(yellow),
            summary_all: Style::default().fg(blue),
            summary_me: Style::default().fg(cyan),
            summary_others: Style::default().fg(orange),
            _bar_filled: cyan,
            _bar_empty: surface,
            warning_border: Style::default().fg(orange).add_modifier(Modifier::BOLD),
        }
    }

    fn tokyonight() -> Self {
        let blue = Color::Rgb(122, 162, 247);
        let green = Color::Rgb(158, 206, 106);
        let orange = Color::Rgb(255, 158, 100);
        let red = Color::Rgb(247, 118, 142);
        let purple = Color::Rgb(187, 154, 247);
        let bg_highlight = Color::Rgb(41, 46, 66);
        Self {
            border_focused: Style::default().fg(blue).add_modifier(Modifier::BOLD),
            border_unfocused: Style::default().fg(bg_highlight),
            header_style: Style::default()
                .fg(Color::Rgb(26, 27, 38))
                .bg(blue)
                .add_modifier(Modifier::BOLD),
            highlight: selected_row_style(),
            status_badge: Style::default()
                .fg(Color::Rgb(26, 27, 38))
                .bg(blue)
                .add_modifier(Modifier::BOLD),
            state_running: Style::default().fg(green),
            state_pending: Style::default().fg(orange),
            state_failed: Style::default().fg(red),
            node_idle: Style::default().fg(green),
            node_down: Style::default().fg(red),
            node_mixed: Style::default().fg(orange),
            summary_all: Style::default().fg(purple),
            summary_me: Style::default().fg(green),
            summary_others: Style::default().fg(orange),
            _bar_filled: blue,
            _bar_empty: bg_highlight,
            warning_border: Style::default().fg(orange).add_modifier(Modifier::BOLD),
        }
    }

    fn dracula() -> Self {
        let purple = Color::Rgb(189, 147, 249);
        let green = Color::Rgb(80, 250, 123);
        let orange = Color::Rgb(255, 184, 108);
        let red = Color::Rgb(255, 85, 85);
        let cyan = Color::Rgb(139, 233, 253);
        let bg_highlight = Color::Rgb(68, 71, 90);
        Self {
            border_focused: Style::default().fg(purple).add_modifier(Modifier::BOLD),
            border_unfocused: Style::default().fg(bg_highlight),
            header_style: Style::default()
                .fg(Color::Rgb(40, 42, 54))
                .bg(purple)
                .add_modifier(Modifier::BOLD),
            highlight: selected_row_style(),
            status_badge: Style::default()
                .fg(Color::Rgb(40, 42, 54))
                .bg(purple)
                .add_modifier(Modifier::BOLD),
            state_running: Style::default().fg(green),
            state_pending: Style::default().fg(orange),
            state_failed: Style::default().fg(red),
            node_idle: Style::default().fg(green),
            node_down: Style::default().fg(red),
            node_mixed: Style::default().fg(orange),
            summary_all: Style::default().fg(cyan),
            summary_me: Style::default().fg(green),
            summary_others: Style::default().fg(orange),
            _bar_filled: cyan,
            _bar_empty: bg_highlight,
            warning_border: Style::default().fg(orange).add_modifier(Modifier::BOLD),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TuiOptions {
    pub tick_rate: Duration,
    pub theme: ThemeName,
}

impl Default for TuiOptions {
    fn default() -> Self {
        Self {
            tick_rate: Duration::from_millis(80),
            theme: ThemeName::Catppuccin,
        }
    }
}

/// Starts the interactive terminal UI.
///
/// # Errors
///
/// Returns an error when terminal setup, drawing, event handling, or terminal
/// restoration fails.
pub fn run<B>(backend: B, config: &BackendConfig, options: TuiOptions) -> Result<()>
where
    B: SlurmClient + 'static,
{
    let backend = Arc::new(backend);
    let mut terminal = setup_terminal()?;
    let result = run_app(&mut terminal, &backend, config, options);
    restore_terminal(&mut terminal)?;
    result
}

fn run_app<B>(
    terminal: &mut Terminal<TerminalBackend>,
    backend: &Arc<B>,
    config: &BackendConfig,
    options: TuiOptions,
) -> Result<()>
where
    B: SlurmClient + 'static,
{
    let (tx, mut rx) = mpsc::channel(16);
    spawn_refresh_loop(Arc::clone(backend), config.clone(), tx.clone());
    let mut app = AppState::new(
        config.current_user.clone(),
        config.refresh_interval,
        config.disk_usage_timeout,
        options.theme,
    );

    loop {
        while let Ok(message) = rx.try_recv() {
            app.apply_message(message);
        }
        terminal.draw(|frame| app.draw(frame))?;

        if event::poll(options.tick_rate)? {
            match event::read()? {
                CrosstermEvent::Key(key) => {
                    if app.handle_key(key, Arc::clone(backend), config, tx.clone()) {
                        return Ok(());
                    }
                }
                CrosstermEvent::Mouse(mouse) => app.handle_mouse(mouse, backend, &tx),
                CrosstermEvent::Resize(_, _)
                | CrosstermEvent::FocusGained
                | CrosstermEvent::FocusLost
                | CrosstermEvent::Paste(_) => {}
            }
        }
    }
}

fn setup_terminal() -> Result<Terminal<TerminalBackend>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;
    Ok(terminal)
}

fn restore_terminal(terminal: &mut Terminal<TerminalBackend>) -> Result<()> {
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;
    Ok(())
}

fn spawn_refresh_loop<B>(backend: Arc<B>, config: BackendConfig, tx: mpsc::Sender<UiMessage>)
where
    B: SlurmClient + 'static,
{
    tokio::spawn(async move {
        loop {
            let message = match refresh_backend(backend.as_ref(), &config).await {
                Ok(envelope) => UiMessage::Snapshot(Box::new(envelope)),
                Err(error) => UiMessage::Error(error.to_string()),
            };
            if tx.send(message).await.is_err() {
                break;
            }
            sleep(config.refresh_interval).await;
        }
    });
}

fn spawn_one_refresh<B>(backend: Arc<B>, config: BackendConfig, tx: mpsc::Sender<UiMessage>)
where
    B: SlurmClient + 'static,
{
    tokio::spawn(async move {
        let message = match refresh_backend(backend.as_ref(), &config).await {
            Ok(envelope) => UiMessage::Snapshot(Box::new(envelope)),
            Err(error) => UiMessage::Error(error.to_string()),
        };
        let _ = tx.send(message).await;
    });
}

fn spawn_action<B>(
    backend: Arc<B>,
    action: JobAction,
    job_id: String,
    tx: mpsc::Sender<UiMessage>,
    config: BackendConfig,
) where
    B: SlurmClient + 'static,
{
    tokio::spawn(async move {
        let result = match action {
            JobAction::Cancel => backend.cancel_job(&job_id).await,
            JobAction::Hold => backend.hold_job(&job_id).await,
            JobAction::Release => backend.release_job(&job_id).await,
            JobAction::Requeue => backend.requeue_job(&job_id).await,
        };
        let _ = tx
            .send(match result {
                Ok(message) => {
                    UiMessage::ActionResult(format!("{} {}: {message}", action.label(), job_id))
                }
                Err(error) => {
                    UiMessage::Error(format!("{} {} failed: {error}", action.label(), job_id))
                }
            })
            .await;
        spawn_one_refresh(backend, config, tx);
    });
}

fn spawn_disk_usage<B>(
    backend: Arc<B>,
    mount: String,
    user: String,
    scan_id: u64,
    scan_timeout: Option<Duration>,
    tx: mpsc::Sender<UiMessage>,
) -> JoinHandle<()>
where
    B: SlurmClient + 'static,
{
    tokio::spawn(async move {
        let progress_tx = tx.clone();
        let progress_mount = mount.clone();
        let progress_user = user.clone();
        let progress: DiskUsageProgressCallback = Arc::new(move |progress: DiskUsageProgress| {
            let _ = progress_tx.try_send(UiMessage::DiskUsageProgress {
                mount: progress_mount.clone(),
                user: progress_user.clone(),
                scan_id,
                progress,
            });
        });
        let result = backend
            .disk_user_usage(&mount, &user, scan_timeout, Some(progress))
            .await
            .map_err(|error| friendly_disk_usage_error(&error.to_string()));
        let _ = tx
            .send(UiMessage::DiskUsage {
                mount,
                user,
                scan_id,
                result,
            })
            .await;
    })
}

#[derive(Debug)]
enum UiMessage {
    Snapshot(Box<SnapshotEnvelope>),
    Error(String),
    ActionResult(String),
    DiskUsage {
        mount: String,
        user: String,
        scan_id: u64,
        result: std::result::Result<Vec<DiskUserUsage>, String>,
    },
    DiskUsageProgress {
        mount: String,
        user: String,
        scan_id: u64,
        progress: DiskUsageProgress,
    },
}

#[derive(Debug, Clone, Copy)]
enum JobAction {
    Cancel,
    Hold,
    Release,
    Requeue,
}

impl JobAction {
    const fn label(self) -> &'static str {
        match self {
            Self::Cancel => "cancel",
            Self::Hold => "hold",
            Self::Release => "release",
            Self::Requeue => "requeue",
        }
    }
}

#[derive(Debug, Clone)]
struct PendingAction {
    action: JobAction,
    job_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputMode {
    Normal,
    Search,
    Filter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GpuColumn {
    Type,
    Total,
    Active,
    Reserved,
    Free,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiskColumn {
    Usage,
    Path,
    Type,
    Space,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UserJobColumn {
    JobId,
    Name,
    Node,
    Cpus,
    Gpus,
    Memory,
    Time,
    Limit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UserJobSection {
    Running,
    Pending,
}

impl UserJobSection {
    const fn toggled(self) -> Self {
        match self {
            Self::Running => Self::Pending,
            Self::Pending => Self::Running,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiskUsageColumn {
    User,
    Used,
    Entries,
}

#[derive(Debug, Clone)]
struct DiskDetails {
    mount: String,
    user: String,
}

impl DiskDetails {
    fn key(&self) -> DiskUsageKey {
        DiskUsageKey {
            mount: self.mount.clone(),
            user: self.user.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct DiskUsageKey {
    mount: String,
    user: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiskUsageCacheEntry {
    rows: Vec<DiskUserUsage>,
    captured_at: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiskUsageCacheFile {
    version: u8,
    entries: Vec<DiskUsageCacheFileEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiskUsageCacheFileEntry {
    key: DiskUsageKey,
    value: DiskUsageCacheEntry,
}

struct DiskUsageScan {
    id: u64,
    started_at: Instant,
    timeout: Option<Duration>,
    progress: Option<DiskUsageProgress>,
    handle: JoinHandle<()>,
}

#[derive(Debug, Clone)]
struct DiskUsageError {
    message: String,
    occurred_at: SystemTime,
}

#[derive(Debug, Clone)]
struct DiskUsageView {
    rows: Vec<DiskUserUsage>,
    captured_at: Option<SystemTime>,
    scan_started_at: Option<Instant>,
    scan_timeout: Option<Duration>,
    progress: Option<DiskUsageProgress>,
    error: Option<DiskUsageError>,
}

impl DiskUsageView {
    const fn is_loading(&self) -> bool {
        self.scan_started_at.is_some()
    }
}

#[derive(Debug, Clone)]
struct PanelUiState {
    visible: bool,
    filter: FilterExpression,
    columns: Vec<bool>,
}

impl PanelUiState {
    fn new(column_count: usize) -> Self {
        Self {
            visible: true,
            filter: FilterExpression::default(),
            columns: vec![true; column_count],
        }
    }

    fn toggle_next_optional_column(&mut self) {
        if self.columns.len() <= 2 {
            return;
        }
        let hidden = self.columns.iter().position(|visible| !*visible);
        if let Some(idx) = hidden {
            self.columns[idx] = true;
            return;
        }
        if let Some(idx) = self
            .columns
            .iter()
            .enumerate()
            .rev()
            .find_map(
                |(idx, visible)| {
                    if idx > 0 && *visible {
                        Some(idx)
                    } else {
                        None
                    }
                },
            )
        {
            self.columns[idx] = false;
        }
    }
}

const DOUBLE_CLICK_MAX_INTERVAL: Duration = Duration::from_millis(450);

#[derive(Debug, Clone, Copy)]
struct RowClick {
    panel: PanelId,
    row: usize,
    at: Instant,
}

impl RowClick {
    fn matches(self, panel: PanelId, row: usize, at: Instant) -> bool {
        self.panel == panel
            && self.row == row
            && at.saturating_duration_since(self.at) <= DOUBLE_CLICK_MAX_INTERVAL
    }
}

struct AppState {
    current_user: String,
    refresh_interval: Duration,
    disk_usage_timeout: Option<Duration>,
    snapshot: Option<SnapshotEnvelope>,
    last_error: Option<String>,
    status: String,
    focus: PanelId,
    mode: InputMode,
    input: String,
    show_help: bool,
    details_job: Option<String>,
    details_node: Option<String>,
    details_gpu: Option<String>,
    details_user: Option<String>,
    details_disk: Option<DiskDetails>,
    pending_action: Option<PendingAction>,
    left_percent: u16,
    top_percent: u16,
    panels: [PanelUiState; 5],
    jobs_sort: JobColumn,
    nodes_sort: NodeColumn,
    gpu_sort: GpuColumn,
    disk_sort: DiskColumn,
    accounting_sort: AccountingColumn,
    user_details_sort: UserJobColumn,
    disk_usage_sort: DiskUsageColumn,
    directions: [SortDirection; 5],
    user_details_direction: SortDirection,
    disk_usage_direction: SortDirection,
    user_details_section: UserJobSection,
    jobs_table: TableState,
    nodes_table: TableState,
    gpus_table: TableState,
    disks_table: TableState,
    summary_table: TableState,
    user_running_table: TableState,
    user_pending_table: TableState,
    disk_usage_table: TableState,
    disk_usage_cache: HashMap<DiskUsageKey, DiskUsageCacheEntry>,
    disk_usage_scans: HashMap<DiskUsageKey, DiskUsageScan>,
    disk_usage_errors: HashMap<DiskUsageKey, DiskUsageError>,
    next_disk_usage_scan_id: u64,
    panel_areas: [Option<Rect>; 5],
    header_hits: Vec<HeaderHit>,
    modal_header_hits: Vec<ModalHeaderHit>,
    modal_table_hits: Vec<ModalTableHit>,
    last_row_click: Option<RowClick>,
    theme_name: ThemeName,
    theme: Theme,
}

impl AppState {
    fn new(
        current_user: String,
        refresh_interval: Duration,
        disk_usage_timeout: Option<Duration>,
        theme_name: ThemeName,
    ) -> Self {
        let mut jobs_table = TableState::default();
        jobs_table.select(Some(0));
        let mut nodes_table = TableState::default();
        nodes_table.select(Some(0));
        let mut gpus_table = TableState::default();
        gpus_table.select(Some(0));
        let mut disks_table = TableState::default();
        disks_table.select(Some(0));
        let mut summary_table = TableState::default();
        summary_table.select(Some(0));
        let mut user_running_table = TableState::default();
        user_running_table.select(Some(0));
        let mut user_pending_table = TableState::default();
        user_pending_table.select(Some(0));
        let mut disk_usage_table = TableState::default();
        disk_usage_table.select(Some(0));
        Self {
            current_user,
            refresh_interval,
            disk_usage_timeout,
            snapshot: None,
            last_error: None,
            status: "Starting Slurm refresh...".to_string(),
            focus: PanelId::Jobs,
            mode: InputMode::Normal,
            input: String::new(),
            show_help: false,
            details_job: None,
            details_node: None,
            details_gpu: None,
            details_user: None,
            details_disk: None,
            pending_action: None,
            left_percent: 60,
            top_percent: 68,
            panels: [
                PanelUiState::new(12),
                PanelUiState::new(8),
                PanelUiState::new(5),
                PanelUiState::new(4),
                PanelUiState::new(8),
            ],
            jobs_sort: JobColumn::JobId,
            nodes_sort: NodeColumn::State,
            gpu_sort: GpuColumn::Type,
            disk_sort: DiskColumn::Space,
            accounting_sort: AccountingColumn::JobId,
            user_details_sort: UserJobColumn::JobId,
            disk_usage_sort: DiskUsageColumn::Used,
            directions: {
                let mut dirs = [SortDirection::Asc; 5];
                dirs[PanelId::Jobs.index()] = SortDirection::Desc;
                dirs[PanelId::Disks.index()] = SortDirection::Desc;
                dirs
            },
            user_details_direction: SortDirection::Desc,
            disk_usage_direction: SortDirection::Desc,
            user_details_section: UserJobSection::Running,
            jobs_table,
            nodes_table,
            gpus_table,
            disks_table,
            summary_table,
            user_running_table,
            user_pending_table,
            disk_usage_table,
            disk_usage_cache: load_disk_usage_cache(),
            disk_usage_scans: HashMap::new(),
            disk_usage_errors: HashMap::new(),
            next_disk_usage_scan_id: 1,
            panel_areas: [None, None, None, None, None],
            header_hits: Vec::new(),
            modal_header_hits: Vec::new(),
            modal_table_hits: Vec::new(),
            last_row_click: None,
            theme_name,
            theme: Theme::from_name(theme_name),
        }
    }

    fn apply_message(&mut self, message: UiMessage) {
        match message {
            UiMessage::Snapshot(envelope) => {
                let jobs = envelope.snapshot.jobs.len();
                let nodes = envelope.snapshot.nodes.len();
                let elapsed = envelope.telemetry.elapsed.as_millis();
                self.status = format!(
                    "Refreshed {jobs} jobs, {nodes} nodes in {elapsed}ms; interval {}s",
                    self.refresh_interval.as_secs_f32()
                );
                if envelope.snapshot.warnings.is_empty() {
                    self.last_error = None;
                } else {
                    self.last_error = Some(envelope.snapshot.warnings.join(" | "));
                }
                self.snapshot = Some(*envelope);
            }
            UiMessage::Error(error) => {
                self.last_error = Some(error.clone());
                self.status = format!("Refresh failed: {error}");
            }
            UiMessage::ActionResult(message) => {
                self.status = message;
                self.pending_action = None;
            }
            UiMessage::DiskUsage {
                mount,
                user,
                scan_id,
                result,
            } => {
                let key = DiskUsageKey {
                    mount: mount.clone(),
                    user: user.clone(),
                };
                if self
                    .disk_usage_scans
                    .get(&key)
                    .is_none_or(|scan| scan.id != scan_id)
                {
                    return;
                }
                self.disk_usage_scans.remove(&key);
                match result {
                    Ok(mut rows) => {
                        if is_inconclusive_zero_usage(&rows) {
                            rows.clear();
                        }
                        self.disk_usage_errors.remove(&key);
                        self.disk_usage_cache.insert(
                            key.clone(),
                            DiskUsageCacheEntry {
                                rows,
                                captured_at: SystemTime::now(),
                            },
                        );
                        persist_disk_usage_cache(&self.disk_usage_cache);
                        if self
                            .details_disk
                            .as_ref()
                            .is_some_and(|details| details.key() == key)
                        {
                            let len = self
                                .disk_usage_cache
                                .get(&key)
                                .map_or(0, |cache| cache.rows.len());
                            clamp_selection(&mut self.disk_usage_table, len);
                        }
                        self.status = format!("Disk usage scan finished for {user} on {mount}");
                    }
                    Err(error) => {
                        self.disk_usage_errors.insert(
                            key,
                            DiskUsageError {
                                message: error.clone(),
                                occurred_at: SystemTime::now(),
                            },
                        );
                        self.status =
                            format!("Disk usage scan failed for {user} on {mount}: {error}");
                    }
                }
            }
            UiMessage::DiskUsageProgress {
                mount,
                user,
                scan_id,
                progress,
            } => {
                let key = DiskUsageKey { mount, user };
                if let Some(scan) = self.disk_usage_scans.get_mut(&key) {
                    if scan.id == scan_id {
                        scan.progress = Some(progress);
                    }
                }
            }
        }
    }

    fn draw(&mut self, frame: &mut Frame<'_>) {
        self.header_hits.clear();
        self.modal_header_hits.clear();
        self.modal_table_hits.clear();
        let root = frame.area();
        let vertical = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(6), Constraint::Length(2)])
            .split(root);
        self.panel_areas = compute_panel_areas(
            vertical[0],
            &self.panels,
            self.left_percent,
            self.top_percent,
        );

        if self.snapshot.is_none() {
            let block = Block::default()
                .title("slmtop")
                .borders(Borders::ALL)
                .border_style(self.theme.border_focused);
            let paragraph = Paragraph::new("Waiting for Slurm data...")
                .block(block)
                .style(Style::default().fg(Color::Gray));
            frame.render_widget(paragraph, vertical[0]);
        } else {
            self.draw_jobs(frame);
            self.draw_nodes(frame);
            self.draw_gpus(frame);
            self.draw_disks(frame);
            self.draw_summary(frame);
        }

        self.draw_status(frame, vertical[1]);
        if self.show_help {
            self.draw_help(frame, centered_rect(74, 70, root));
        }
        if let Some(job_id) = self.details_job.clone() {
            self.draw_job_details(frame, &job_id, centered_rect(78, 62, root));
        }
        if let Some(node_name) = self.details_node.clone() {
            self.draw_node_details(frame, &node_name, centered_rect(72, 55, root));
        }
        if let Some(gpu_type) = self.details_gpu.clone() {
            self.draw_gpu_details(frame, &gpu_type, centered_rect(72, 55, root));
        }
        if let Some(user) = self.details_user.clone() {
            self.draw_user_details(frame, &user, centered_rect(85, 75, root));
        }
        if self.details_disk.is_some() {
            self.draw_disk_details(frame, centered_rect(74, 68, root));
        }
        if let Some(pending) = self.pending_action.clone() {
            self.draw_confirmation(frame, &pending, centered_rect(62, 28, root));
        }
        if self.mode != InputMode::Normal {
            self.draw_input(frame, centered_rect(70, 15, root));
        }
    }

    fn draw_jobs(&mut self, frame: &mut Frame<'_>) {
        let Some(area) = self.panel_areas[PanelId::Jobs.index()] else {
            return;
        };
        let rows = self.visible_jobs();
        clamp_selection(&mut self.jobs_table, rows.len());
        let headers = [
            "JOBID", "USER", "STATE", "PART", "QOS", "PRI", "NAME", "NODE", "CPU", "GPU", "MEM",
            "TIME",
        ];
        let sort_idx = job_column_to_index(self.jobs_sort);
        let direction = self.directions[PanelId::Jobs.index()];
        let visible = self.visible_columns(PanelId::Jobs, headers.len());
        let theme = self.theme;
        let table_rows = rows.iter().map(|job| {
            let values = [
                job.id.clone(),
                job.user.clone(),
                job.state.clone(),
                job.partition.clone(),
                job.qos.clone(),
                job.priority.to_string(),
                job.name.clone(),
                job.nodes.clone(),
                job.cpus.to_string(),
                job.gpu_total().to_string(),
                job.memory.to_string(),
                job.time_used.clone(),
            ];
            Row::new(select_visible(values, &visible)).style(themed_state_style(&theme, &job.state))
        });
        let mut constraints = Vec::new();
        for (i, &is_visible) in visible.iter().enumerate() {
            if is_visible {
                constraints.push(match headers[i] {
                    "JOBID" | "USER" | "PART" => Constraint::Percentage(7),
                    "STATE" => Constraint::Percentage(6),
                    "QOS" | "MEM" => Constraint::Percentage(9),
                    "PRI" | "CPU" | "GPU" => Constraint::Percentage(5),
                    "NAME" => Constraint::Percentage(16),
                    "NODE" => Constraint::Percentage(6),
                    "TIME" => Constraint::Percentage(12),
                    _ => Constraint::Percentage(7),
                });
            }
        }
        self.add_header_hits(area, PanelId::Jobs, &constraints);
        let title = self.panel_title(
            PanelId::Jobs,
            format!(
                "rows={} filter={}",
                rows.len(),
                filter_label(&self.panels[PanelId::Jobs.index()].filter)
            ),
        );
        let header_cells = decorate_headers(&headers, sort_idx, direction, &visible);
        let table = Table::new(table_rows, constraints)
            .header(Row::new(header_cells).style(self.theme.header_style))
            .block(self.themed_panel_block(title, self.focus == PanelId::Jobs))
            .row_highlight_style(self.theme.highlight);
        frame.render_stateful_widget(table, area, &mut self.jobs_table);
    }

    fn draw_nodes(&mut self, frame: &mut Frame<'_>) {
        let Some(area) = self.panel_areas[PanelId::Nodes.index()] else {
            return;
        };
        let rows = self.visible_nodes();
        clamp_selection(&mut self.nodes_table, rows.len());
        let headers = [
            "NODE", "STATE", "CPU(T)", "CPU(F)", "MEM(T)", "MEM(F)", "GPU(T)", "GPU(F)",
        ];
        let sort_idx = node_column_to_index(self.nodes_sort);
        let direction = self.directions[PanelId::Nodes.index()];
        let visible = self.visible_columns(PanelId::Nodes, headers.len());
        let theme = self.theme;
        let table_rows = rows.iter().map(|node| {
            let values = [
                node.name.clone(),
                node.state.clone(),
                node.cpus.total.to_string(),
                node.cpus.idle.to_string(),
                node.memory_total.to_string(),
                node.memory_free.to_string(),
                node.gpu_total().to_string(),
                (node.gpu_total().saturating_sub(node.gpu_allocated())).to_string(),
            ];
            Row::new(select_visible(values, &visible)).style(themed_node_style(&theme, &node.state))
        });
        let mut constraints = Vec::new();
        for (i, &is_visible) in visible.iter().enumerate() {
            if is_visible {
                constraints.push(match headers[i] {
                    "NODE" => Constraint::Percentage(20),
                    _ => Constraint::Percentage(10),
                });
            }
        }
        self.add_header_hits(area, PanelId::Nodes, &constraints);
        let title = self.panel_title(
            PanelId::Nodes,
            format!(
                "rows={} filter={}",
                rows.len(),
                filter_label(&self.panels[PanelId::Nodes.index()].filter)
            ),
        );
        let header_cells = decorate_headers(&headers, sort_idx, direction, &visible);
        let table = Table::new(table_rows, constraints)
            .header(Row::new(header_cells).style(self.theme.header_style))
            .block(self.themed_panel_block(title, self.focus == PanelId::Nodes))
            .row_highlight_style(self.theme.highlight);
        frame.render_stateful_widget(table, area, &mut self.nodes_table);
    }

    fn draw_gpus(&mut self, frame: &mut Frame<'_>) {
        let Some(area) = self.panel_areas[PanelId::Gpus.index()] else {
            return;
        };
        let rows = self.gpu_rows();
        clamp_selection(&mut self.gpus_table, rows.len());
        let headers = ["TYPE", "TOTAL", "ACTIVE", "RESERVED", "FREE"];
        let sort_idx = gpu_column_to_index(self.gpu_sort);
        let direction = self.directions[PanelId::Gpus.index()];
        let visible = self.visible_columns(PanelId::Gpus, headers.len());
        let table_rows = rows.iter().map(|row| {
            let values = [
                row.gpu_type.clone(),
                row.total.to_string(),
                row.active.to_string(),
                row.reserved.to_string(),
                row.free.to_string(),
            ];
            Row::new(select_visible(values, &visible))
        });
        let constraints = equal_widths(visible.iter().filter(|is_visible| **is_visible).count());
        self.add_header_hits(area, PanelId::Gpus, &constraints);
        let title = self.panel_title(PanelId::Gpus, format!("rows={}", rows.len()));
        let header_cells = decorate_headers(&headers, sort_idx, direction, &visible);
        let table = Table::new(table_rows, constraints)
            .header(Row::new(header_cells).style(self.theme.header_style))
            .block(self.themed_panel_block(title, self.focus == PanelId::Gpus))
            .row_highlight_style(self.theme.highlight);
        frame.render_stateful_widget(table, area, &mut self.gpus_table);
    }

    fn draw_disks(&mut self, frame: &mut Frame<'_>) {
        let Some(area) = self.panel_areas[PanelId::Disks.index()] else {
            return;
        };
        let disks = self.disk_rows();
        clamp_selection(&mut self.disks_table, disks.len());
        let mut table_rows: Vec<Row<'static>> = Vec::new();
        let bar_width = 12_usize;
        let headers = ["USAGE", "PATH", "TYPE", "SPACE"];
        let sort_idx = disk_column_to_index(self.disk_sort);
        let direction = self.directions[PanelId::Disks.index()];
        let visible = self.visible_columns(PanelId::Disks, headers.len());
        for disk in &disks {
            let filled = (usize::from(disk.use_percent) * bar_width) / 100;
            let empty = bar_width.saturating_sub(filled);

            let color = match disk.label {
                slmtop_core::DiskLabel::Ssd => Color::LightGreen,
                slmtop_core::DiskLabel::Hdd => Color::LightYellow,
                slmtop_core::DiskLabel::Nfs => Color::LightBlue,
                slmtop_core::DiskLabel::ParallelFs => Color::LightCyan,
                slmtop_core::DiskLabel::Unknown => Color::Gray,
            };

            let bar_text = format!(
                "[{}{}] {:>3}%",
                "█".repeat(filled),
                " ".repeat(empty),
                disk.use_percent
            );
            let cells = [
                Cell::from(Line::from(Span::styled(
                    bar_text,
                    Style::default().fg(color),
                ))),
                Cell::from(disk.mount.clone()),
                Cell::from(disk.label.as_str()),
                Cell::from(format!("{}/{}", disk.used, disk.size)),
            ];
            table_rows.push(Row::new(select_visible_cells(cells, &visible)));
        }
        let title = self.panel_title(PanelId::Disks, format!("disks={}", disks.len()));
        let constraints = select_visible_constraints(
            &[
                Constraint::Length(20),
                Constraint::Percentage(40),
                Constraint::Length(6),
                Constraint::Percentage(40),
            ],
            &visible,
        );
        self.add_header_hits(area, PanelId::Disks, &constraints);
        let header_cells = decorate_headers(&headers, sort_idx, direction, &visible);
        let table = Table::new(table_rows, constraints)
            .header(Row::new(header_cells).style(self.theme.header_style))
            .block(self.themed_panel_block(title, self.focus == PanelId::Disks))
            .row_highlight_style(self.theme.highlight);
        frame.render_stateful_widget(table, area, &mut self.disks_table);
    }

    fn draw_summary(&mut self, frame: &mut Frame<'_>) {
        let Some(area) = self.panel_areas[PanelId::Summary.index()] else {
            return;
        };
        let headers = [
            "", "R-JOB", "R-CPU", "R-GPU", "│", "P-JOB", "P-CPU", "P-GPU",
        ];
        let rows = self.summary_display_rows();
        let table_rows = rows
            .iter()
            .map(|row| summary_table_row(row, summary_row_style(self.theme, row.kind)))
            .collect::<Vec<_>>();
        clamp_selection(&mut self.summary_table, rows.len().max(1));
        let title = self.panel_title(PanelId::Summary, "job stats");
        let constraints = vec![
            Constraint::Length(8),
            Constraint::Percentage(15),
            Constraint::Percentage(15),
            Constraint::Percentage(15),
            Constraint::Length(1),
            Constraint::Percentage(15),
            Constraint::Percentage(15),
            Constraint::Percentage(15),
        ];
        let table = Table::new(table_rows, constraints)
            .header(Row::new(headers.map(Cell::from)).style(self.theme.header_style))
            .block(self.themed_panel_block(title, self.focus == PanelId::Summary))
            .row_highlight_style(self.theme.highlight);
        frame.render_stateful_widget(table, area, &mut self.summary_table);
    }

    fn draw_status(&self, frame: &mut Frame<'_>, area: Rect) {
        let mode = match self.mode {
            InputMode::Normal => "NORMAL",
            InputMode::Search => "SEARCH",
            InputMode::Filter => "FILTER",
        };
        let error = self
            .last_error
            .as_ref()
            .map_or(String::new(), |error| format!(" | warning: {error}"));
        let text = Line::from(vec![
            Span::styled(" slmtop ", self.theme.status_badge),
            Span::raw(format!(
                " {mode} focus={} theme={} | Tab / s d f c x v [] t ? q | {}{}",
                self.focus.title(),
                self.theme_name.label(),
                self.status,
                error
            )),
        ]);
        frame.render_widget(
            Paragraph::new(text).style(Style::default().fg(Color::White)),
            area,
        );
    }

    fn draw_help(&self, frame: &mut Frame<'_>, area: Rect) {
        frame.render_widget(Clear, area);
        let text = vec![
            Line::from("slmtop help"),
            Line::from("Tab/Shift-Tab or 1-4: focus panels. Arrow keys or j/k: move row."),
            Line::from("s: cycle sort column, d: toggle direction, click header to sort."),
            Line::from("/: search, f: filter (owner=me state=running gpu=a100)."),
            Line::from("c: toggle column, x: hide panel, v: show panel, [ ] { } resize."),
            Line::from("Enter on job: details (c cancel, h hold, u release, r requeue)."),
            Line::from("Enter on node: node specs popup."),
            Line::from(
                "Enter on disk: current-user usage cache; u/r refresh, n no-timeout in popup.",
            ),
            Line::from("t: cycle theme, ? or Esc: close help, q: quit."),
        ];
        frame.render_widget(
            Paragraph::new(text)
                .wrap(Wrap { trim: true })
                .block(self.themed_panel_block("Help".to_string(), true)),
            area,
        );
    }

    fn draw_input(&self, frame: &mut Frame<'_>, area: Rect) {
        frame.render_widget(Clear, area);
        let prompt = match self.mode {
            InputMode::Search => "Search",
            InputMode::Filter => "Filter",
            InputMode::Normal => "",
        };
        let help = if self.mode == InputMode::Filter {
            "Examples: owner=me state=running part=gpu gpu=a100 free text"
        } else {
            "Enter applies to the focused panel; Esc cancels"
        };
        let text = vec![
            Line::from(format!("{prompt}: {}", self.input)),
            Line::from(help),
        ];
        frame.render_widget(
            Paragraph::new(text).block(self.themed_panel_block(prompt.to_string(), true)),
            area,
        );
    }

    fn draw_job_details(&self, frame: &mut Frame<'_>, job_id: &str, area: Rect) {
        let Some(job) = self.find_job(job_id) else {
            return;
        };
        frame.render_widget(Clear, area);
        let gpu_text = if job.gpus.is_empty() {
            "0".to_string()
        } else {
            job.gpus
                .iter()
                .map(|(gpu_type, count)| format!("{gpu_type}:{count}"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let mut lines = vec![
            Line::from(format!("Job ID    : {}", job.id)),
            Line::from(format!("User      : {}", job.user)),
            Line::from(format!("State     : {}", job.state)),
            Line::from(format!("Reason    : {}", job.status_reason())),
            Line::from(format!("Partition : {}", job.partition)),
            Line::from(format!("QOS       : {}", job.qos)),
            Line::from(format!("Priority  : {}", job.priority)),
            Line::from(format!("Name      : {}", job.name)),
            Line::from(format!("Nodes     : {}", job.nodes)),
        ];
        if !job.node_list.is_empty() {
            lines.push(Line::from(format!("Node List : {}", job.node_list)));
        }
        lines.extend([
            Line::from(format!("CPUs      : {}", job.cpus)),
            Line::from(format!("GPUs      : {gpu_text}")),
            Line::from(format!("Memory    : {}", job.memory)),
            Line::from(format!("Time Used  : {}", slurm_time_display(&job.time_used))),
            Line::from(format!("Time Limit : {}", slurm_time_display(&job.time_limit))),
            Line::from(""),
            Line::from("c cancel | h hold | u release | r requeue | Esc close"),
        ]);
        frame.render_widget(
            Paragraph::new(lines)
                .wrap(Wrap { trim: true })
                .block(self.themed_panel_block("Job Details".to_string(), true)),
            area,
        );
    }

    fn draw_confirmation(&self, frame: &mut Frame<'_>, pending: &PendingAction, area: Rect) {
        frame.render_widget(Clear, area);
        let irreversible = matches!(pending.action, JobAction::Cancel | JobAction::Requeue);
        let mut lines = vec![
            Line::from(Span::styled("⚠  WARNING", self.theme.warning_border)),
            Line::from(""),
            Line::from(format!(
                "Confirm {} for job {}?",
                pending.action.label(),
                pending.job_id
            )),
        ];
        if irreversible {
            lines.push(Line::from(Span::styled(
                "This action cannot be undone.",
                self.theme.state_failed,
            )));
        }
        lines.push(Line::from(""));
        lines.push(Line::from("y: confirm | n/Esc: cancel"));
        let block = Block::default()
            .title("⚠ Confirm Job Action")
            .borders(Borders::ALL)
            .border_style(self.theme.warning_border);
        frame.render_widget(Paragraph::new(lines).block(block), area);
    }

    fn handle_key<B>(
        &mut self,
        key: KeyEvent,
        backend: Arc<B>,
        config: &BackendConfig,
        tx: mpsc::Sender<UiMessage>,
    ) -> bool
    where
        B: SlurmClient + 'static,
    {
        if key.kind == KeyEventKind::Release {
            return false;
        }
        if self.pending_action.is_some() {
            return self.handle_confirmation_key(key, backend, config, tx);
        }
        if self.show_help {
            match key.code {
                KeyCode::Esc | KeyCode::Char('?') => self.show_help = false,
                _ => {}
            }
            return false;
        }
        if self.mode != InputMode::Normal {
            self.handle_input_key(key);
            return false;
        }
        if self.details_node.is_some() {
            match key.code {
                KeyCode::Esc | KeyCode::Char('q') => self.details_node = None,
                _ => {}
            }
            return false;
        }
        if self.details_gpu.is_some() {
            match key.code {
                KeyCode::Esc | KeyCode::Char('q') => self.details_gpu = None,
                _ => {}
            }
            return false;
        }
        if self.details_user.is_some() {
            self.handle_user_details_key(key);
            return false;
        }
        if self.details_disk.is_some() {
            self.handle_disk_details_key(key, backend, tx);
            return false;
        }
        if self.details_job.is_some() {
            return self.handle_details_key(key);
        }

        match key.code {
            KeyCode::Char('q') => return true,
            KeyCode::Char('?') => self.show_help = true,
            KeyCode::Char('r') => {
                self.status = "Manual refresh requested".to_string();
                spawn_one_refresh(backend, config.clone(), tx);
            }
            KeyCode::Tab => self.focus_next(),
            KeyCode::BackTab => self.focus_previous(),
            KeyCode::Char('1') => self.focus_visible(PanelId::Jobs),
            KeyCode::Char('2') => self.focus_visible(PanelId::Nodes),
            KeyCode::Char('3') => self.focus_visible(PanelId::Gpus),
            KeyCode::Char('4') => self.focus_visible(PanelId::Summary),
            KeyCode::Char('5') => self.focus_visible(PanelId::Disks),
            KeyCode::Char('/') => self.begin_input(InputMode::Search),
            KeyCode::Char('f') => self.begin_input(InputMode::Filter),
            KeyCode::Char('s') => self.cycle_sort(),
            KeyCode::Char('d') => self.toggle_direction(),
            KeyCode::Char('c') => self.toggle_column(),
            KeyCode::Char('x') => self.hide_focused_panel(),
            KeyCode::Char('v') => self.show_next_hidden_panel(),
            KeyCode::Char('[') => self.resize_width(false),
            KeyCode::Char(']') => self.resize_width(true),
            KeyCode::Char('{') => self.resize_height(false),
            KeyCode::Char('}') => self.resize_height(true),
            KeyCode::Char('t') => {
                self.theme_name = self.theme_name.cycle();
                self.theme = Theme::from_name(self.theme_name);
                self.status = format!("Theme: {}", self.theme_name.label());
            }
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::Enter => self.open_focused_details(backend, tx),
            _ => {}
        }
        false
    }

    fn open_focused_details<B>(&mut self, backend: Arc<B>, tx: mpsc::Sender<UiMessage>)
    where
        B: SlurmClient + 'static,
    {
        match self.focus {
            PanelId::Jobs => self.details_job = self.selected_job().map(|job| job.id),
            PanelId::Nodes => self.details_node = self.selected_node().map(|node| node.name),
            PanelId::Gpus => self.details_gpu = self.selected_gpu().map(|row| row.gpu_type),
            PanelId::Summary => self.open_selected_summary_user(),
            PanelId::Disks => {
                if let Some(disk) = self.selected_disk() {
                    self.open_disk_details(disk.mount, backend, tx);
                }
            }
        }
    }

    fn open_selected_summary_user(&mut self) {
        let Some(user) = self.selected_summary_user() else {
            return;
        };
        if user == "All" || user == "Others" {
            return;
        }
        let (running, pending) = self.user_detail_jobs(&user);
        self.details_user = Some(user);
        self.user_running_table.select(Some(0));
        self.user_pending_table.select(Some(0));
        self.user_details_section = if running.is_empty() && !pending.is_empty() {
            UserJobSection::Pending
        } else {
            UserJobSection::Running
        };
    }

    fn handle_details_key(&mut self, key: KeyEvent) -> bool {
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => self.details_job = None,
            KeyCode::Char('c') => self.queue_job_action(JobAction::Cancel),
            KeyCode::Char('h') => self.queue_job_action(JobAction::Hold),
            KeyCode::Char('u') => self.queue_job_action(JobAction::Release),
            KeyCode::Char('r') => self.queue_job_action(JobAction::Requeue),
            _ => {}
        }
        false
    }

    fn handle_user_details_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => self.details_user = None,
            KeyCode::Tab | KeyCode::BackTab => {
                self.user_details_section = self.user_details_section.toggled();
            }
            KeyCode::Char('s') => {
                self.user_details_sort = next_user_job_column(self.user_details_sort);
                reset_table_to_top(self.active_user_details_table_mut());
            }
            KeyCode::Char('d') => {
                self.user_details_direction = self.user_details_direction.toggled();
                reset_table_to_top(self.active_user_details_table_mut());
            }
            KeyCode::Down | KeyCode::Char('j') => self.move_user_details_selection(1),
            KeyCode::Up | KeyCode::Char('k') => self.move_user_details_selection(-1),
            KeyCode::PageDown => self.move_user_details_selection(10),
            KeyCode::PageUp => self.move_user_details_selection(-10),
            KeyCode::Home => self.set_user_details_selection(0),
            KeyCode::End => {
                if let Some(len) = self.active_user_details_len() {
                    self.set_user_details_selection(len.saturating_sub(1));
                }
            }
            _ => {}
        }
    }

    fn handle_disk_details_key<B>(
        &mut self,
        key: KeyEvent,
        backend: Arc<B>,
        tx: mpsc::Sender<UiMessage>,
    ) where
        B: SlurmClient + 'static,
    {
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => self.details_disk = None,
            KeyCode::Char('u' | 'r') => {
                if let Some(key) = self.details_disk.as_ref().map(DiskDetails::key) {
                    self.start_disk_usage_scan(key, backend, tx, self.disk_usage_timeout, false);
                }
            }
            KeyCode::Char('n') => {
                if let Some(key) = self.details_disk.as_ref().map(DiskDetails::key) {
                    self.start_disk_usage_scan(key, backend, tx, None, true);
                }
            }
            KeyCode::Char('s') => {
                self.disk_usage_sort = next_disk_usage_column(self.disk_usage_sort);
                reset_table_to_top(&mut self.disk_usage_table);
            }
            KeyCode::Char('d') => {
                self.disk_usage_direction = self.disk_usage_direction.toggled();
                reset_table_to_top(&mut self.disk_usage_table);
            }
            KeyCode::Down | KeyCode::Char('j') => self.move_disk_usage_selection(1),
            KeyCode::Up | KeyCode::Char('k') => self.move_disk_usage_selection(-1),
            KeyCode::PageDown => self.move_disk_usage_selection(10),
            KeyCode::PageUp => self.move_disk_usage_selection(-10),
            KeyCode::Home => self.disk_usage_table.select(Some(0)),
            KeyCode::End => {
                if let Some(len) = self.active_disk_usage_len() {
                    self.disk_usage_table.select(Some(len.saturating_sub(1)));
                }
            }
            _ => {}
        }
    }

    fn handle_confirmation_key<B>(
        &mut self,
        key: KeyEvent,
        backend: Arc<B>,
        config: &BackendConfig,
        tx: mpsc::Sender<UiMessage>,
    ) -> bool
    where
        B: SlurmClient + 'static,
    {
        match key.code {
            KeyCode::Char('y') => {
                if let Some(pending) = self.pending_action.take() {
                    self.status =
                        format!("Running {} for {}", pending.action.label(), pending.job_id);
                    self.details_job = None;
                    spawn_action(backend, pending.action, pending.job_id, tx, config.clone());
                }
            }
            KeyCode::Char('n') | KeyCode::Esc => self.pending_action = None,
            _ => {}
        }
        false
    }

    fn handle_input_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                self.mode = InputMode::Normal;
                self.input.clear();
            }
            KeyCode::Enter => {
                let panel_idx = self.focus.index();
                let mut filter = FilterExpression::parse(&self.input);
                if self.mode == InputMode::Search {
                    filter = self.panels[panel_idx].filter.clone();
                    filter.query = self.input.to_lowercase();
                }
                self.panels[panel_idx].filter = filter;
                self.mode = InputMode::Normal;
                self.input.clear();
            }
            KeyCode::Backspace => {
                self.input.pop();
            }
            KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.input.push(ch);
            }
            _ => {}
        }
    }

    fn handle_mouse<B>(&mut self, mouse: MouseEvent, backend: &Arc<B>, tx: &mpsc::Sender<UiMessage>)
    where
        B: SlurmClient + 'static,
    {
        if self.handle_modal_mouse(mouse) {
            return;
        }
        if !matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
            return;
        }
        if let Some(hit) = self
            .header_hits
            .iter()
            .find(|hit| hit.contains(mouse.column, mouse.row))
            .copied()
        {
            self.focus_visible(hit.panel);
            self.apply_header_sort(hit.panel, hit.column);
            self.last_row_click = None;
            return;
        }
        for panel in PanelId::ALL {
            if let Some(area) = self.panel_areas[panel.index()] {
                if contains(area, mouse.column, mouse.row) {
                    self.focus_visible(panel);
                    if let Some(row) = table_body_row_at(area, mouse.column, mouse.row) {
                        if let Some(selected) = self.select_visible_row(panel, row) {
                            if self.register_row_click(panel, selected) {
                                self.open_focused_details(Arc::clone(backend), tx.clone());
                            }
                        } else {
                            self.last_row_click = None;
                        }
                    } else {
                        self.last_row_click = None;
                    }
                    return;
                }
            }
        }
        self.last_row_click = None;
    }

    fn handle_modal_mouse(&mut self, mouse: MouseEvent) -> bool {
        if self.details_user.is_some() {
            self.handle_user_details_mouse(mouse);
            return true;
        }
        if self.details_disk.is_some() {
            self.handle_disk_details_mouse(mouse);
            return true;
        }
        self.pending_action.is_some()
            || self.show_help
            || self.mode != InputMode::Normal
            || self.details_job.is_some()
            || self.details_node.is_some()
            || self.details_gpu.is_some()
    }

    fn handle_user_details_mouse(&mut self, mouse: MouseEvent) {
        match mouse.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                if let Some(hit) = self
                    .modal_header_hits
                    .iter()
                    .find(|hit| hit.contains(mouse.column, mouse.row))
                    .copied()
                {
                    if let Some(section) = hit.table.user_section() {
                        self.user_details_section = section;
                        self.set_user_details_sort(user_job_column_from_index(hit.column));
                    }
                    return;
                }
                if let Some(hit) = self.modal_table_at(mouse.column, mouse.row) {
                    if let Some(section) = hit.table.user_section() {
                        self.user_details_section = section;
                        if let Some(row) = table_body_row_at(hit.area, mouse.column, mouse.row) {
                            self.select_visible_user_details_row(section, row);
                        }
                    }
                }
            }
            MouseEventKind::ScrollUp => {
                self.set_user_details_section_under_mouse(mouse.column, mouse.row);
                self.move_user_details_selection(-3);
            }
            MouseEventKind::ScrollDown => {
                self.set_user_details_section_under_mouse(mouse.column, mouse.row);
                self.move_user_details_selection(3);
            }
            _ => {}
        }
    }

    fn handle_disk_details_mouse(&mut self, mouse: MouseEvent) {
        match mouse.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                if let Some(hit) = self
                    .modal_header_hits
                    .iter()
                    .find(|hit| hit.contains(mouse.column, mouse.row))
                    .copied()
                {
                    if hit.table == ModalTable::DiskUsage {
                        self.set_disk_usage_sort(disk_usage_column_from_index(hit.column));
                    }
                    return;
                }
                if let Some(hit) = self.modal_table_at(mouse.column, mouse.row) {
                    if hit.table == ModalTable::DiskUsage {
                        if let Some(row) = table_body_row_at(hit.area, mouse.column, mouse.row) {
                            self.select_visible_disk_usage_row(row);
                        }
                    }
                }
            }
            MouseEventKind::ScrollUp => self.move_disk_usage_selection(-3),
            MouseEventKind::ScrollDown => self.move_disk_usage_selection(3),
            _ => {}
        }
    }

    fn begin_input(&mut self, mode: InputMode) {
        self.mode = mode;
        self.input.clear();
        if mode == InputMode::Search {
            self.input = self.panels[self.focus.index()].filter.query.clone();
        }
    }

    fn visible_jobs(&self) -> Vec<Job> {
        let Some(snapshot) = self.snapshot.as_ref() else {
            return Vec::new();
        };
        let filter = &self.panels[PanelId::Jobs.index()].filter;
        let jobs = filter_jobs(&snapshot.snapshot.jobs, filter, &self.current_user)
            .into_iter()
            .cloned()
            .collect();
        sort_jobs(jobs, self.jobs_sort, self.directions[PanelId::Jobs.index()])
    }

    fn visible_nodes(&self) -> Vec<Node> {
        let Some(snapshot) = self.snapshot.as_ref() else {
            return Vec::new();
        };
        let filter = &self.panels[PanelId::Nodes.index()].filter;
        let nodes = filter_nodes(&snapshot.snapshot.nodes, filter)
            .into_iter()
            .cloned()
            .collect();
        sort_nodes(
            nodes,
            self.nodes_sort,
            self.directions[PanelId::Nodes.index()],
        )
    }

    fn gpu_rows(&self) -> Vec<GpuRow> {
        let Some(snapshot) = self.snapshot.as_ref() else {
            return Vec::new();
        };
        let mut rows = gpu_rows_from_summary(&snapshot.snapshot.gpu_summary);
        let direction = self.directions[PanelId::Gpus.index()];
        rows.sort_by(|a, b| {
            let ordering = match self.gpu_sort {
                GpuColumn::Type => a.gpu_type.cmp(&b.gpu_type),
                GpuColumn::Total => a.total.cmp(&b.total),
                GpuColumn::Active => a.active.cmp(&b.active),
                GpuColumn::Reserved => a.reserved.cmp(&b.reserved),
                GpuColumn::Free => a.free.cmp(&b.free),
            };
            if direction == SortDirection::Asc {
                ordering
            } else {
                ordering.reverse()
            }
        });
        rows
    }

    fn disk_rows(&self) -> Vec<DiskInfo> {
        let Some(snapshot) = self.snapshot.as_ref() else {
            return Vec::new();
        };
        let mut rows = snapshot.snapshot.disk_info.clone();
        let direction = self.directions[PanelId::Disks.index()];
        rows.sort_by(|a, b| {
            let ordering = match self.disk_sort {
                DiskColumn::Usage => a.use_percent.cmp(&b.use_percent),
                DiskColumn::Path => a.mount.to_lowercase().cmp(&b.mount.to_lowercase()),
                DiskColumn::Type => a.label.as_str().cmp(b.label.as_str()),
                DiskColumn::Space => disk_size_bytes(&a.size).cmp(&disk_size_bytes(&b.size)),
            }
            .then_with(|| a.mount.cmp(&b.mount));
            match direction {
                SortDirection::Asc => ordering,
                SortDirection::Desc => ordering.reverse(),
            }
        });
        rows
    }

    fn user_detail_jobs(&self, user: &str) -> (Vec<Job>, Vec<Job>) {
        let Some(snapshot) = self.snapshot.as_ref() else {
            return (Vec::new(), Vec::new());
        };
        let mut running = Vec::new();
        let mut pending = Vec::new();
        for job in &snapshot.snapshot.jobs {
            if !job.user.eq_ignore_ascii_case(user) {
                continue;
            }
            let state = job.state.to_ascii_uppercase();
            if state.starts_with('R') {
                running.push(job.clone());
            } else if state.starts_with('P') {
                pending.push(job.clone());
            }
        }
        (
            sort_user_jobs(running, self.user_details_sort, self.user_details_direction),
            sort_user_jobs(pending, self.user_details_sort, self.user_details_direction),
        )
    }

    fn sorted_disk_usage_rows(&self) -> Vec<DiskUserUsage> {
        let mut rows = self.active_disk_usage_view().rows;
        rows.sort_by(|a, b| {
            let ordering = match self.disk_usage_sort {
                DiskUsageColumn::User => a.user.to_lowercase().cmp(&b.user.to_lowercase()),
                DiskUsageColumn::Used => a.bytes.cmp(&b.bytes),
                DiskUsageColumn::Entries => a.entries.cmp(&b.entries),
            }
            .then_with(|| a.user.cmp(&b.user));
            match self.disk_usage_direction {
                SortDirection::Asc => ordering,
                SortDirection::Desc => ordering.reverse(),
            }
        });
        rows
    }

    fn active_disk_usage_view(&self) -> DiskUsageView {
        let Some(key) = self.details_disk.as_ref().map(DiskDetails::key) else {
            return DiskUsageView {
                rows: Vec::new(),
                captured_at: None,
                scan_started_at: None,
                scan_timeout: None,
                progress: None,
                error: None,
            };
        };
        let cache = self.disk_usage_cache.get(&key);
        let scan = self.disk_usage_scans.get(&key);
        DiskUsageView {
            rows: cache.map_or_else(Vec::new, |cache| cache.rows.clone()),
            captured_at: cache.map(|cache| cache.captured_at),
            scan_started_at: scan.map(|scan| scan.started_at),
            scan_timeout: scan.and_then(|scan| scan.timeout),
            progress: scan.and_then(|scan| scan.progress),
            error: self.disk_usage_errors.get(&key).cloned(),
        }
    }

    fn selected_job(&self) -> Option<Job> {
        let selected = self.jobs_table.selected()?;
        self.visible_jobs().get(selected).cloned()
    }

    fn find_job(&self, job_id: &str) -> Option<Job> {
        self.snapshot
            .as_ref()?
            .snapshot
            .jobs
            .iter()
            .find(|job| job.id == job_id)
            .cloned()
    }

    fn queue_job_action(&mut self, action: JobAction) {
        let Some(job_id) = self.details_job.clone() else {
            return;
        };
        self.pending_action = Some(PendingAction { action, job_id });
    }

    fn focus_next(&mut self) {
        for offset in 1..=PanelId::ALL.len() {
            let candidate = PanelId::ALL[(self.focus.index() + offset) % PanelId::ALL.len()];
            if self.panels[candidate.index()].visible {
                self.focus = candidate;
                break;
            }
        }
    }

    fn focus_previous(&mut self) {
        for offset in 1..=PanelId::ALL.len() {
            let idx = (self.focus.index() + PanelId::ALL.len() - offset) % PanelId::ALL.len();
            let candidate = PanelId::ALL[idx];
            if self.panels[candidate.index()].visible {
                self.focus = candidate;
                break;
            }
        }
    }

    fn focus_visible(&mut self, panel: PanelId) {
        if self.panels[panel.index()].visible {
            self.focus = panel;
        }
    }

    fn hide_focused_panel(&mut self) {
        let visible_count = self.panels.iter().filter(|panel| panel.visible).count();
        if visible_count <= 1 {
            self.status = "At least one panel must remain visible".to_string();
            return;
        }
        self.panels[self.focus.index()].visible = false;
        self.focus_next();
    }

    fn show_next_hidden_panel(&mut self) {
        for panel in PanelId::ALL {
            if !self.panels[panel.index()].visible {
                self.panels[panel.index()].visible = true;
                self.focus = panel;
                return;
            }
        }
        self.status = "All panels are already visible".to_string();
    }

    fn resize_width(&mut self, grow_left: bool) {
        if grow_left {
            self.left_percent = (self.left_percent + 5).min(85);
        } else {
            self.left_percent = self.left_percent.saturating_sub(5).max(15);
        }
    }

    fn resize_height(&mut self, grow_top: bool) {
        if grow_top {
            self.top_percent = (self.top_percent + 5).min(85);
        } else {
            self.top_percent = self.top_percent.saturating_sub(5).max(15);
        }
    }

    fn move_selection(&mut self, delta: isize) {
        let len = self.panel_row_count(self.focus);
        let table = self.focused_table_mut();
        move_table_selection(table, len, delta);
    }

    fn select_visible_row(&mut self, panel: PanelId, visible_row: usize) -> Option<usize> {
        let len = self.panel_row_count(panel);
        let table = self.panel_table_mut(panel);
        select_visible_table_row(table, len, visible_row)
    }

    fn register_row_click(&mut self, panel: PanelId, row: usize) -> bool {
        let at = Instant::now();
        let is_double_click = self
            .last_row_click
            .is_some_and(|click| click.matches(panel, row, at));
        self.last_row_click = if is_double_click {
            None
        } else {
            Some(RowClick { panel, row, at })
        };
        is_double_click
    }

    fn panel_row_count(&self, panel: PanelId) -> usize {
        let Some(snapshot) = self.snapshot.as_ref() else {
            return 0;
        };
        match panel {
            PanelId::Jobs => filter_jobs(
                &snapshot.snapshot.jobs,
                &self.panels[PanelId::Jobs.index()].filter,
                &self.current_user,
            )
            .len(),
            PanelId::Nodes => filter_nodes(
                &snapshot.snapshot.nodes,
                &self.panels[PanelId::Nodes.index()].filter,
            )
            .len(),
            PanelId::Gpus => snapshot.snapshot.gpu_summary.by_type.len() + 1,
            PanelId::Summary => self.summary_display_rows().len(),
            PanelId::Disks => snapshot.snapshot.disk_info.len(),
        }
    }

    fn panel_table_mut(&mut self, panel: PanelId) -> &mut TableState {
        match panel {
            PanelId::Jobs => &mut self.jobs_table,
            PanelId::Nodes => &mut self.nodes_table,
            PanelId::Gpus => &mut self.gpus_table,
            PanelId::Summary => &mut self.summary_table,
            PanelId::Disks => &mut self.disks_table,
        }
    }

    fn move_user_details_selection(&mut self, delta: isize) {
        let Some(len) = self.active_user_details_len() else {
            return;
        };
        let table = self.active_user_details_table_mut();
        move_table_selection(table, len, delta);
    }

    fn set_user_details_selection(&mut self, row: usize) {
        let Some(len) = self.active_user_details_len() else {
            return;
        };
        if len == 0 {
            self.active_user_details_table_mut().select(None);
        } else {
            self.active_user_details_table_mut()
                .select(Some(row.min(len - 1)));
        }
    }

    fn select_visible_user_details_row(&mut self, section: UserJobSection, visible_row: usize) {
        let len = self.user_details_len(section).unwrap_or(0);
        let table = self.user_details_table_mut(section);
        select_visible_table_row(table, len, visible_row);
    }

    fn active_user_details_len(&self) -> Option<usize> {
        self.user_details_len(self.user_details_section)
    }

    fn user_details_len(&self, section: UserJobSection) -> Option<usize> {
        let user = self.details_user.as_ref()?;
        let (running, pending) = self.user_detail_jobs(user);
        Some(match section {
            UserJobSection::Running => running.len(),
            UserJobSection::Pending => pending.len(),
        })
    }

    fn active_user_details_table_mut(&mut self) -> &mut TableState {
        self.user_details_table_mut(self.user_details_section)
    }

    fn user_details_table_mut(&mut self, section: UserJobSection) -> &mut TableState {
        match section {
            UserJobSection::Running => &mut self.user_running_table,
            UserJobSection::Pending => &mut self.user_pending_table,
        }
    }

    fn move_disk_usage_selection(&mut self, delta: isize) {
        let len = self.active_disk_usage_len().unwrap_or(0);
        move_table_selection(&mut self.disk_usage_table, len, delta);
    }

    fn select_visible_disk_usage_row(&mut self, visible_row: usize) {
        let len = self.active_disk_usage_len().unwrap_or(0);
        select_visible_table_row(&mut self.disk_usage_table, len, visible_row);
    }

    fn active_disk_usage_len(&self) -> Option<usize> {
        self.details_disk
            .as_ref()
            .map(|_| self.active_disk_usage_view().rows.len())
    }

    fn focused_table_mut(&mut self) -> &mut TableState {
        self.panel_table_mut(self.focus)
    }

    fn toggle_direction(&mut self) {
        let idx = self.focus.index();
        self.directions[idx] = self.directions[idx].toggled();
        let focus = self.focus;
        reset_table_to_top(self.panel_table_mut(focus));
    }

    fn toggle_column(&mut self) {
        self.panels[self.focus.index()].toggle_next_optional_column();
    }

    fn cycle_sort(&mut self) {
        match self.focus {
            PanelId::Jobs => self.jobs_sort = next_job_column(self.jobs_sort),
            PanelId::Nodes => self.nodes_sort = next_node_column(self.nodes_sort),
            PanelId::Gpus => self.gpu_sort = next_gpu_column(self.gpu_sort),
            PanelId::Summary => self.accounting_sort = next_accounting_column(self.accounting_sort),
            PanelId::Disks => self.disk_sort = next_disk_column(self.disk_sort),
        }
        let focus = self.focus;
        reset_table_to_top(self.panel_table_mut(focus));
    }

    fn apply_header_sort(&mut self, panel: PanelId, visible_column: usize) {
        let actual = self.actual_column(panel, visible_column);
        match panel {
            PanelId::Jobs => self.jobs_sort = job_column_from_index(actual),
            PanelId::Nodes => self.nodes_sort = node_column_from_index(actual),
            PanelId::Gpus => self.gpu_sort = gpu_column_from_index(actual),
            PanelId::Summary => self.accounting_sort = accounting_column_from_index(actual),
            PanelId::Disks => self.disk_sort = disk_column_from_index(actual),
        }
        self.directions[panel.index()] = self.directions[panel.index()].toggled();
        reset_table_to_top(self.panel_table_mut(panel));
    }

    fn set_user_details_sort(&mut self, column: UserJobColumn) {
        if self.user_details_sort == column {
            self.user_details_direction = self.user_details_direction.toggled();
        } else {
            self.user_details_sort = column;
            self.user_details_direction = default_user_job_direction(column);
        }
        reset_table_to_top(self.active_user_details_table_mut());
    }

    fn set_disk_usage_sort(&mut self, column: DiskUsageColumn) {
        if self.disk_usage_sort == column {
            self.disk_usage_direction = self.disk_usage_direction.toggled();
        } else {
            self.disk_usage_sort = column;
            self.disk_usage_direction = default_disk_usage_direction(column);
        }
        reset_table_to_top(&mut self.disk_usage_table);
    }

    fn set_user_details_section_under_mouse(&mut self, x: u16, y: u16) {
        if let Some(hit) = self.modal_table_at(x, y) {
            if let Some(section) = hit.table.user_section() {
                self.user_details_section = section;
            }
        }
    }

    fn modal_table_at(&self, x: u16, y: u16) -> Option<ModalTableHit> {
        self.modal_table_hits
            .iter()
            .find(|hit| contains(hit.area, x, y))
            .copied()
    }

    fn actual_column(&self, panel: PanelId, visible_column: usize) -> usize {
        let mut seen = 0;
        let columns = &self.panels[panel.index()].columns;
        for idx in 0..50 {
            let is_visible = *columns.get(idx).unwrap_or(&true);
            if is_visible {
                if seen == visible_column {
                    return idx;
                }
                seen += 1;
            }
        }
        0
    }

    fn visible_columns(&self, panel: PanelId, len: usize) -> Vec<bool> {
        let columns = &self.panels[panel.index()].columns;
        (0..len)
            .map(|idx| *columns.get(idx).unwrap_or(&true))
            .collect()
    }

    fn add_header_hits(&mut self, area: Rect, panel: PanelId, constraints: &[Constraint]) {
        if constraints.is_empty() || area.width <= 2 || area.height <= 2 {
            return;
        }
        let table_area = Rect {
            x: area.x + 1,
            y: area.y + 1,
            width: area.width.saturating_sub(2),
            height: area.height.saturating_sub(2),
        };
        let columns = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(constraints)
            .spacing(1)
            .split(table_area);

        for (idx, col_area) in columns.iter().enumerate() {
            self.header_hits.push(HeaderHit {
                panel,
                column: idx,
                x_start: col_area.x,
                x_end: col_area.x + col_area.width + 1,
                y: table_area.y,
            });
        }
    }

    fn add_modal_header_hits(&mut self, area: Rect, table: ModalTable, constraints: &[Constraint]) {
        if constraints.is_empty() || area.width <= 2 || area.height <= 2 {
            return;
        }
        let table_area = Rect {
            x: area.x + 1,
            y: area.y + 1,
            width: area.width.saturating_sub(2),
            height: area.height.saturating_sub(2),
        };
        let columns = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(constraints)
            .spacing(1)
            .split(table_area);

        for (idx, col_area) in columns.iter().enumerate() {
            self.modal_header_hits.push(ModalHeaderHit {
                table,
                column: idx,
                x_start: col_area.x,
                x_end: col_area.x + col_area.width + 1,
                y: table_area.y,
            });
        }
    }

    fn panel_title(&self, panel: PanelId, suffix: impl std::fmt::Display) -> String {
        let marker = if self.focus == panel { "*" } else { " " };
        format!("{marker} {} | {suffix}", panel.title())
    }

    fn selected_node(&self) -> Option<Node> {
        let selected = self.nodes_table.selected()?;
        self.visible_nodes().get(selected).cloned()
    }

    fn selected_gpu(&self) -> Option<GpuRow> {
        let selected = self.gpus_table.selected()?;
        self.gpu_rows().get(selected).cloned()
    }

    fn selected_disk(&self) -> Option<DiskInfo> {
        let selected = self.disks_table.selected()?;
        self.disk_rows().get(selected).cloned()
    }

    fn open_disk_details<B>(&mut self, mount: String, backend: Arc<B>, tx: mpsc::Sender<UiMessage>)
    where
        B: SlurmClient + 'static,
    {
        let user = self.current_user.trim().to_string();
        if user.is_empty() {
            let key = DiskUsageKey {
                mount: mount.clone(),
                user,
            };
            self.disk_usage_errors.insert(
                key.clone(),
                DiskUsageError {
                    message: "Current user is unknown; pass --user or set USER.".to_string(),
                    occurred_at: SystemTime::now(),
                },
            );
            self.details_disk = Some(DiskDetails {
                mount: key.mount,
                user: key.user,
            });
            return;
        }
        let key = DiskUsageKey {
            mount: mount.clone(),
            user: user.clone(),
        };
        self.details_disk = Some(DiskDetails { mount, user });
        self.disk_usage_table.select(Some(0));
        if !self.disk_usage_cache.contains_key(&key) && !self.disk_usage_scans.contains_key(&key) {
            self.start_disk_usage_scan(key, backend, tx, self.disk_usage_timeout, false);
        } else if let Some(cache) = self.disk_usage_cache.get(&key) {
            self.status = format!(
                "Loaded cached disk usage for {} on {} from {}",
                key.user,
                key.mount,
                format_timestamp(cache.captured_at)
            );
        }
    }

    fn start_disk_usage_scan<B>(
        &mut self,
        key: DiskUsageKey,
        backend: Arc<B>,
        tx: mpsc::Sender<UiMessage>,
        scan_timeout: Option<Duration>,
        replace_timed_scan: bool,
    ) where
        B: SlurmClient + 'static,
    {
        if let Some(scan) = self.disk_usage_scans.get(&key) {
            let replace_running =
                replace_timed_scan && scan.timeout.is_some() && scan_timeout.is_none();
            if !replace_running {
                self.status = format!(
                    "Disk usage scan already running for {} on {} ({})",
                    key.user,
                    key.mount,
                    disk_usage_timeout_label(scan.timeout)
                );
                return;
            }
        }
        if let Some(scan) = self.disk_usage_scans.remove(&key) {
            scan.handle.abort();
            self.status = format!(
                "Restarting disk usage scan for {} on {} without timeout",
                key.user, key.mount
            );
        }
        self.disk_usage_errors.remove(&key);
        let scan_id = self.next_disk_usage_scan_id;
        self.next_disk_usage_scan_id = self.next_disk_usage_scan_id.wrapping_add(1);
        let handle = spawn_disk_usage(
            backend,
            key.mount.clone(),
            key.user.clone(),
            scan_id,
            scan_timeout,
            tx,
        );
        let mount = key.mount.clone();
        let user = key.user.clone();
        self.disk_usage_scans.insert(
            key,
            DiskUsageScan {
                id: scan_id,
                started_at: Instant::now(),
                timeout: scan_timeout,
                progress: None,
                handle,
            },
        );
        self.status = format!(
            "Scanning disk usage for {} on {} ({})",
            user,
            mount,
            disk_usage_timeout_label(scan_timeout)
        );
    }

    fn summary_display_rows(&self) -> Vec<SummaryDisplayRow> {
        let Some(snapshot) = self.snapshot.as_ref() else {
            return Vec::new();
        };
        let summary = &snapshot.snapshot.job_summary;
        let mut rows = vec![
            SummaryDisplayRow {
                label: "All".to_string(),
                summary: summary.all.clone(),
                kind: SummaryRowKind::All,
            },
            SummaryDisplayRow {
                label: "Me".to_string(),
                summary: summary.me.clone(),
                kind: SummaryRowKind::Me,
            },
        ];

        let mut users = summary
            .users
            .iter()
            .filter(|(user, _)| user.as_str() != self.current_user.as_str())
            .collect::<Vec<_>>();
        users.sort_by(|a, b| {
            summary_total_jobs(b.1)
                .cmp(&summary_total_jobs(a.1))
                .then_with(|| a.0.cmp(b.0))
        });

        rows.extend(
            users
                .iter()
                .take(10)
                .map(|(user, stats)| SummaryDisplayRow {
                    label: (*user).clone(),
                    summary: (*stats).clone(),
                    kind: SummaryRowKind::User,
                }),
        );

        let overflow =
            users
                .iter()
                .skip(10)
                .fold(OwnerSummary::default(), |mut total, (_, stats)| {
                    merge_owner_summary(&mut total, stats);
                    total
                });
        if users.len() > 10 || summary_total_jobs(&overflow) > 0 {
            rows.push(SummaryDisplayRow {
                label: "Others".to_string(),
                summary: overflow,
                kind: SummaryRowKind::Others,
            });
        }
        rows
    }

    fn selected_summary_user(&self) -> Option<String> {
        let selected = self.summary_table.selected()?;
        let row = self.summary_display_rows().get(selected)?.clone();
        match row.kind {
            SummaryRowKind::All => Some("All".to_string()),
            SummaryRowKind::Me => Some(self.current_user.clone()),
            SummaryRowKind::User => Some(row.label),
            SummaryRowKind::Others => Some("Others".to_string()),
        }
    }

    fn find_node(&self, name: &str) -> Option<Node> {
        self.snapshot
            .as_ref()?
            .snapshot
            .nodes
            .iter()
            .find(|node| node.name == name)
            .cloned()
    }

    fn draw_node_details(&self, frame: &mut Frame<'_>, node_name: &str, area: Rect) {
        let Some(node) = self.find_node(node_name) else {
            return;
        };
        frame.render_widget(Clear, area);
        let gpu_text = if node.gpus.is_empty() {
            "0".to_string()
        } else {
            node.gpus
                .iter()
                .map(|(gpu_type, count)| format!("{gpu_type}:{count}"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let mut lines = vec![
            Line::from(format!("Node      : {}", node.name)),
            Line::from(format!("State     : {}", node.state)),
            Line::from(""),
            Line::from("── CPU ──────────────────────"),
            Line::from(format!("  Total     : {}", node.cpus.total)),
            Line::from(format!("  Allocated : {}", node.cpus.allocated)),
            Line::from(format!("  Idle      : {}", node.cpus.idle)),
            Line::from(format!("  Other     : {}", node.cpus.other)),
            Line::from(""),
            Line::from("── Memory ───────────────────"),
            Line::from(format!("  Total     : {}", node.memory_total)),
            Line::from(format!("  Reserved  : {}", node.memory_reserved)),
            Line::from(format!("  Free      : {}", node.memory_free)),
            Line::from(""),
            Line::from("── GPU ──────────────────────"),
            Line::from(format!("  GPUs      : {gpu_text}")),
            Line::from(format!("  GRES      : {}", node.gres_raw)),
        ];
        if let Some(reason) = &node.reason {
            lines.push(Line::from(""));
            lines.push(Line::from(format!("Reason    : {reason}")));
        }
        lines.push(Line::from(""));
        lines.push(Line::from("Esc: close"));
        frame.render_widget(
            Paragraph::new(lines)
                .wrap(Wrap { trim: true })
                .block(self.themed_panel_block(format!("Node Details: {node_name}"), true)),
            area,
        );
    }

    fn draw_gpu_details(&self, frame: &mut Frame<'_>, gpu_type: &str, area: Rect) {
        let Some(snapshot) = self.snapshot.as_ref() else {
            return;
        };
        frame.render_widget(Clear, area);

        let mut lines = vec![
            Line::from(format!("GPU Type  : {gpu_type}")),
            Line::from(""),
            Line::from("Node Distribution:"),
            Line::from(format!("{:<15} | {:<5} | {:<5}", "Node", "Total", "Free")),
            Line::from("-".repeat(31)),
        ];

        let mut matched_nodes = Vec::new();
        for node in &snapshot.snapshot.nodes {
            if let Some(&total) = node.gpus.get(gpu_type) {
                let allocated = node.gpus_allocated.get(gpu_type).copied().unwrap_or(0);
                let free = total.saturating_sub(allocated);
                matched_nodes.push((node.name.clone(), total, free));
            }
        }

        matched_nodes.sort_by(|a, b| a.0.cmp(&b.0));

        for (node_name, total, free) in matched_nodes {
            lines.push(Line::from(format!(
                "{node_name:<15} | {total:<5} | {free:<5}"
            )));
        }

        lines.push(Line::from(""));
        lines.push(Line::from("Esc close"));

        frame.render_widget(
            Paragraph::new(lines)
                .wrap(Wrap { trim: true })
                .block(self.themed_panel_block(format!("GPU Details: {gpu_type}"), true)),
            area,
        );
    }

    fn draw_user_details(&mut self, frame: &mut Frame<'_>, user: &str, area: Rect) {
        frame.render_widget(Clear, area);
        let (running, pending) = self.user_detail_jobs(user);
        clamp_selection(&mut self.user_running_table, running.len());
        clamp_selection(&mut self.user_pending_table, pending.len());

        let block = self.themed_panel_block(
            format!(
                "User: {} | R:{} P:{} | Tab table | s/d sort | Esc close",
                user,
                running.len(),
                pending.len()
            ),
            true,
        );
        frame.render_widget(block.clone(), area);
        let inner = block.inner(area);

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(inner);

        let headers = [
            "JOBID", "NAME", "NODE", "CPU", "GPU", "MEM", "TIME", "LIMIT",
        ];
        let constraints = vec![
            Constraint::Percentage(10),
            Constraint::Percentage(18),
            Constraint::Percentage(12),
            Constraint::Percentage(6),
            Constraint::Percentage(18),
            Constraint::Percentage(8),
            Constraint::Percentage(14),
            Constraint::Percentage(14),
        ];
        let header_cells = decorate_headers(
            &headers,
            user_job_column_to_index(self.user_details_sort),
            self.user_details_direction,
            &[true; 8],
        );
        let header_style = self.theme.header_style;
        let highlight = self.theme.highlight;
        let running_border = if self.user_details_section == UserJobSection::Running {
            self.theme.border_focused
        } else {
            self.theme.border_unfocused
        };
        let pending_border = if self.user_details_section == UserJobSection::Pending {
            self.theme.border_focused
        } else {
            self.theme.border_unfocused
        };

        let running_title = format!("Running ({})", running.len());
        self.modal_table_hits.push(ModalTableHit {
            table: ModalTable::UserRunning,
            area: chunks[0],
        });
        self.add_modal_header_hits(chunks[0], ModalTable::UserRunning, &constraints);
        let running_table = Table::new(user_job_rows(&running), constraints.clone())
            .header(Row::new(header_cells.clone()).style(header_style))
            .block(
                Block::default()
                    .title(running_title)
                    .borders(Borders::ALL)
                    .border_style(running_border),
            )
            .row_highlight_style(highlight);
        frame.render_stateful_widget(running_table, chunks[0], &mut self.user_running_table);

        let pending_title = format!("Pending ({})", pending.len());
        self.modal_table_hits.push(ModalTableHit {
            table: ModalTable::UserPending,
            area: chunks[1],
        });
        self.add_modal_header_hits(chunks[1], ModalTable::UserPending, &constraints);
        let pending_table = Table::new(user_job_rows(&pending), constraints)
            .header(Row::new(header_cells).style(header_style))
            .block(
                Block::default()
                    .title(pending_title)
                    .borders(Borders::ALL)
                    .border_style(pending_border),
            )
            .row_highlight_style(highlight);
        frame.render_stateful_widget(pending_table, chunks[1], &mut self.user_pending_table);
    }

    fn draw_disk_details(&mut self, frame: &mut Frame<'_>, area: Rect) {
        let Some(details) = self.details_disk.clone() else {
            return;
        };
        frame.render_widget(Clear, area);
        let view = self.active_disk_usage_view();
        let rows = self.sorted_disk_usage_rows();
        clamp_selection(&mut self.disk_usage_table, rows.len());

        let status = Self::disk_usage_status(&view);
        let block = self.themed_panel_block(
            format!(
                "Disk Usage: {} | user={} | {status} | u refresh | n no-timeout | Esc close",
                details.mount, details.user,
            ),
            true,
        );
        frame.render_widget(block.clone(), area);
        let inner = block.inner(area);

        if let Some(error) = view.error.as_ref().filter(|_| rows.is_empty()) {
            draw_disk_usage_message(frame, inner, disk_usage_error_lines(error));
            return;
        }

        if view.is_loading() && rows.is_empty() {
            draw_disk_usage_message(
                frame,
                inner,
                disk_usage_loading_lines(view.scan_timeout, view.progress),
            );
            return;
        }

        if rows.is_empty() {
            draw_disk_usage_message(frame, inner, disk_usage_empty_lines(&details.user));
            return;
        }

        let table_area = if view.is_loading() {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(2), Constraint::Min(3)])
                .split(inner);
            frame.render_widget(
                Paragraph::new(disk_usage_scan_progress_lines(
                    view.scan_timeout,
                    view.progress,
                ))
                .wrap(Wrap { trim: true }),
                chunks[0],
            );
            chunks[1]
        } else {
            inner
        };

        let headers = ["USER", "USED", "ENTRIES"];
        let constraints = vec![
            Constraint::Percentage(45),
            Constraint::Percentage(25),
            Constraint::Percentage(30),
        ];
        self.modal_table_hits.push(ModalTableHit {
            table: ModalTable::DiskUsage,
            area: table_area,
        });
        self.add_modal_header_hits(table_area, ModalTable::DiskUsage, &constraints);
        let header_cells = decorate_headers(
            &headers,
            disk_usage_column_to_index(self.disk_usage_sort),
            self.disk_usage_direction,
            &[true; 3],
        );
        let table = Table::new(disk_usage_rows(&rows), constraints)
            .header(Row::new(header_cells).style(self.theme.header_style))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(self.theme.border_focused),
            )
            .row_highlight_style(self.theme.highlight);
        frame.render_stateful_widget(table, table_area, &mut self.disk_usage_table);
    }

    fn disk_usage_status(view: &DiskUsageView) -> String {
        if let Some(started_at) = view.scan_started_at {
            let elapsed = started_at.elapsed().as_secs();
            let status = match view.scan_timeout {
                Some(limit) => format!(
                    "{} scanning {}s/{}s",
                    spinner(started_at),
                    elapsed,
                    limit.as_secs()
                ),
                None => format!("{} scanning {}s/no timeout", spinner(started_at), elapsed),
            };
            let status = if let Some(progress) = view.progress {
                format!("{status}; {}", disk_usage_progress_summary(progress))
            } else {
                status
            };
            return view.captured_at.map_or(status.clone(), |captured_at| {
                format!("{status}; showing cached {}", format_timestamp(captured_at))
            });
        }
        if view.error.is_some() && view.captured_at.is_some() {
            return view.captured_at.map_or_else(
                || "cached; update failed".to_string(),
                |captured_at| format!("cached at {}; update failed", format_timestamp(captured_at)),
            );
        }
        if view.error.is_some() {
            return "error".to_string();
        }
        view.captured_at.map_or_else(
            || "not scanned".to_string(),
            |captured_at| format!("cached at {}", format_timestamp(captured_at)),
        )
    }

    fn themed_panel_block(&self, title: String, focused: bool) -> Block<'static> {
        let style = if focused {
            self.theme.border_focused
        } else {
            self.theme.border_unfocused
        };
        Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(style)
    }
}

#[derive(Debug, Clone, Copy)]
struct HeaderHit {
    panel: PanelId,
    column: usize,
    x_start: u16,
    x_end: u16,
    y: u16,
}

impl HeaderHit {
    const fn contains(self, x: u16, y: u16) -> bool {
        y == self.y && x >= self.x_start && x < self.x_end
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModalTable {
    UserRunning,
    UserPending,
    DiskUsage,
}

impl ModalTable {
    const fn user_section(self) -> Option<UserJobSection> {
        match self {
            Self::UserRunning => Some(UserJobSection::Running),
            Self::UserPending => Some(UserJobSection::Pending),
            Self::DiskUsage => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ModalHeaderHit {
    table: ModalTable,
    column: usize,
    x_start: u16,
    x_end: u16,
    y: u16,
}

impl ModalHeaderHit {
    const fn contains(self, x: u16, y: u16) -> bool {
        y == self.y && x >= self.x_start && x < self.x_end
    }
}

#[derive(Debug, Clone, Copy)]
struct ModalTableHit {
    table: ModalTable,
    area: Rect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SummaryRowKind {
    All,
    Me,
    User,
    Others,
}

#[derive(Debug, Clone)]
struct SummaryDisplayRow {
    label: String,
    summary: OwnerSummary,
    kind: SummaryRowKind,
}

#[derive(Debug, Clone)]
struct GpuRow {
    gpu_type: String,
    total: u64,
    active: u64,
    reserved: u64,
    free: u64,
}

fn gpu_rows_from_summary(summary: &GpuSummary) -> Vec<GpuRow> {
    let mut rows = vec![GpuRow {
        gpu_type: "ALL".to_string(),
        total: summary.total,
        active: summary.active,
        reserved: summary.reserved,
        free: summary.free_estimate,
    }];
    rows.extend(summary.by_type.iter().map(|(gpu_type, stats)| GpuRow {
        gpu_type: gpu_type.clone(),
        total: stats.total,
        active: stats.active,
        reserved: stats.reserved,
        free: stats.free_estimate,
    }));
    rows
}

fn sort_user_jobs(jobs: Vec<Job>, column: UserJobColumn, direction: SortDirection) -> Vec<Job> {
    match column {
        UserJobColumn::JobId => sort_jobs(jobs, JobColumn::JobId, direction),
        UserJobColumn::Name => sort_jobs(jobs, JobColumn::Name, direction),
        UserJobColumn::Node => sort_jobs(jobs, JobColumn::Nodes, direction),
        UserJobColumn::Cpus => sort_jobs(jobs, JobColumn::Cpus, direction),
        UserJobColumn::Gpus => sort_jobs(jobs, JobColumn::Gpus, direction),
        UserJobColumn::Memory => sort_jobs(jobs, JobColumn::Memory, direction),
        UserJobColumn::Time => sort_jobs(jobs, JobColumn::Time, direction),
        UserJobColumn::Limit => sort_user_jobs_by_limit(jobs, direction),
    }
}

fn sort_user_jobs_by_limit(mut jobs: Vec<Job>, direction: SortDirection) -> Vec<Job> {
    jobs.sort_by(|a, b| {
        let ordering = parse_slurm_time(&a.time_limit)
            .cmp(&parse_slurm_time(&b.time_limit))
            .then_with(|| a.id_sort_key().cmp(&b.id_sort_key()));
        match direction {
            SortDirection::Asc => ordering,
            SortDirection::Desc => ordering.reverse(),
        }
    });
    jobs
}

fn user_job_rows(jobs: &[Job]) -> Vec<Row<'static>> {
    jobs.iter()
        .map(|job| {
            let gpu_str = gpu_map_display(job);
            let node_display = if job.node_list.is_empty() {
                job.nodes.clone()
            } else {
                job.node_list.clone()
            };
            Row::new(vec![
                Cell::from(job.id.clone()),
                Cell::from(truncate_text(&job.name, 18)),
                Cell::from(node_display),
                Cell::from(job.cpus.to_string()),
                Cell::from(gpu_str),
                Cell::from(job.memory.to_string()),
                Cell::from(job.time_used.clone()),
                Cell::from(job.time_limit.clone()),
            ])
        })
        .collect()
}

fn disk_usage_rows(rows: &[DiskUserUsage]) -> Vec<Row<'static>> {
    rows.iter()
        .map(|row| {
            let entries = if row.entries == 0 {
                "-".to_string()
            } else {
                row.entries.to_string()
            };
            Row::new(vec![
                Cell::from(row.user.clone()),
                Cell::from(row.human_bytes()),
                Cell::from(entries),
            ])
        })
        .collect()
}

fn disk_usage_error_lines(error: &DiskUsageError) -> Vec<Line<'static>> {
    vec![
        Line::from("Per-user usage scan failed."),
        Line::from(error.message.clone()),
        Line::from(format!("Failed at {}", format_timestamp(error.occurred_at))),
        Line::from(""),
        Line::from("Press u to retry, or n to retry without a timeout."),
        Line::from("Esc close"),
    ]
}

fn disk_usage_loading_lines(
    timeout: Option<Duration>,
    progress: Option<DiskUsageProgress>,
) -> Vec<Line<'static>> {
    vec![
        Line::from(disk_usage_progress_detail(progress)),
        Line::from(format!(
            "Timeout mode: {}",
            disk_usage_timeout_label(timeout)
        )),
    ]
}

fn disk_usage_scan_progress_lines(
    timeout: Option<Duration>,
    progress: Option<DiskUsageProgress>,
) -> Vec<Line<'static>> {
    let mut lines = vec![Line::from(format!(
        "Refreshing in background; timeout {}.",
        disk_usage_timeout_label(timeout)
    ))];
    lines.push(progress.map_or_else(
        || Line::from("Preparing disk usage scan."),
        |progress| Line::from(disk_usage_progress_detail(Some(progress))),
    ));
    lines
}

fn disk_usage_progress_detail(progress: Option<DiskUsageProgress>) -> String {
    progress.map_or_else(
        || "Preparing disk usage scan.".to_string(),
        |progress| match progress.stage {
            DiskUsageProgressStage::Starting => "Preparing disk usage scan.".to_string(),
            DiskUsageProgressStage::Quota => {
                "Checking filesystem quota for the current user.".to_string()
            }
            DiskUsageProgressStage::UserDirectory => {
                "Measuring the user's directory with du; item counts are not available for this fast path.".to_string()
            }
            DiskUsageProgressStage::Traversal => {
                if progress.scanned_entries == 0 {
                    "Scanning filesystem tree for files owned by the current user.".to_string()
                } else {
                    format!(
                        "Scanned {} items; matched {} owned entries ({}) so far.",
                        progress.scanned_entries,
                        progress.matched_entries,
                        human_bytes(progress.bytes)
                    )
                }
            }
        },
    )
}

fn disk_usage_progress_summary(progress: DiskUsageProgress) -> String {
    match progress.stage {
        DiskUsageProgressStage::Starting => "starting".to_string(),
        DiskUsageProgressStage::Quota => "checking quota".to_string(),
        DiskUsageProgressStage::UserDirectory => "measuring user directory with du".to_string(),
        DiskUsageProgressStage::Traversal => {
            if progress.scanned_entries == 0 {
                "traversing filesystem".to_string()
            } else {
                format!(
                    "scanned {} items; matched {} ({})",
                    progress.scanned_entries,
                    progress.matched_entries,
                    human_bytes(progress.bytes)
                )
            }
        }
    }
}

fn disk_usage_empty_lines(user: &str) -> Vec<Line<'static>> {
    vec![
        Line::from(format!(
            "No files owned by {user} were found on this mount."
        )),
        Line::from("The scan only includes directories the current process can traverse."),
    ]
}

fn draw_disk_usage_message(frame: &mut Frame<'_>, area: Rect, lines: Vec<Line<'static>>) {
    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: true }), area);
}

fn spinner(started_at: Instant) -> &'static str {
    const FRAMES: [&str; 4] = ["|", "/", "-", "\\"];
    let frame = (started_at.elapsed().as_millis() / 200) as usize % FRAMES.len();
    FRAMES[frame]
}

fn disk_usage_timeout_label(timeout: Option<Duration>) -> String {
    timeout.map_or_else(
        || "no timeout".to_string(),
        |timeout| format!("{}s", timeout.as_secs()),
    )
}

fn load_disk_usage_cache() -> HashMap<DiskUsageKey, DiskUsageCacheEntry> {
    let Some(path) = disk_usage_cache_path() else {
        return HashMap::new();
    };
    let Ok(text) = fs::read_to_string(path) else {
        return HashMap::new();
    };
    let Ok(cache_file) = serde_json::from_str::<DiskUsageCacheFile>(&text) else {
        return HashMap::new();
    };
    if cache_file.version != 1 {
        return HashMap::new();
    }
    let mut dropped_inconclusive = false;
    let cache = cache_file
        .entries
        .into_iter()
        .filter_map(|entry| {
            if is_inconclusive_zero_usage(&entry.value.rows) {
                dropped_inconclusive = true;
                None
            } else {
                Some((entry.key, entry.value))
            }
        })
        .collect();
    if dropped_inconclusive {
        persist_disk_usage_cache(&cache);
    }
    cache
}

fn persist_disk_usage_cache(cache: &HashMap<DiskUsageKey, DiskUsageCacheEntry>) {
    let Some(path) = disk_usage_cache_path() else {
        return;
    };
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let cache_file = DiskUsageCacheFile {
        version: 1,
        entries: cache
            .iter()
            .map(|(key, value)| DiskUsageCacheFileEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect(),
    };
    if let Ok(json) = serde_json::to_string_pretty(&cache_file) {
        let _ = fs::write(path, json);
    }
}

fn disk_usage_cache_path() -> Option<PathBuf> {
    if let Ok(cache_home) = env::var("XDG_CACHE_HOME") {
        if !cache_home.is_empty() {
            return Some(
                PathBuf::from(cache_home)
                    .join("slmtop")
                    .join("disk_usage.json"),
            );
        }
    }
    env::var("HOME")
        .ok()
        .filter(|home| !home.is_empty())
        .map(|home| {
            PathBuf::from(home)
                .join(".cache")
                .join("slmtop")
                .join("disk_usage.json")
        })
}

fn is_inconclusive_zero_usage(rows: &[DiskUserUsage]) -> bool {
    rows.len() == 1 && rows[0].bytes == 0 && rows[0].entries == 0
}

fn friendly_disk_usage_error(error: &str) -> String {
    let lower = error.to_ascii_lowercase();
    if lower.contains("timed out") || lower.contains("timeout") {
        return "Usage scan timed out. This filesystem is too large to scan directly, and no quick user-directory result was available. Press n in this popup, or start with --disk-usage-no-timeout, to let scans run until completion.".to_string();
    }
    if lower.contains("find ") || lower.contains(" du ") || lower.contains("backend command") {
        return "Usage scan failed in the background. Command details are hidden; try a narrower user/project directory or a filesystem quota tool.".to_string();
    }
    error.to_string()
}

fn format_timestamp(time: SystemTime) -> String {
    let utc = OffsetDateTime::from(time);
    let offset = UtcOffset::local_offset_at(utc).unwrap_or(UtcOffset::UTC);
    let local = utc.to_offset(offset);
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02} {}",
        local.year(),
        u8::from(local.month()),
        local.day(),
        local.hour(),
        local.minute(),
        format_utc_offset(offset)
    )
}

fn format_utc_offset(offset: UtcOffset) -> String {
    let seconds = offset.whole_seconds();
    if seconds == 0 {
        return "UTC".to_string();
    }
    let sign = if seconds < 0 { '-' } else { '+' };
    let abs_seconds = seconds.unsigned_abs();
    let hours = abs_seconds / 3_600;
    let minutes = (abs_seconds % 3_600) / 60;
    format!("UTC{sign}{hours:02}:{minutes:02}")
}

fn summary_table_row(row: &SummaryDisplayRow, style: Style) -> Row<'static> {
    Row::new(vec![
        Cell::from(row.label.clone()),
        Cell::from(row.summary.running.jobs.to_string()),
        Cell::from(row.summary.running.cpus.to_string()),
        Cell::from(row.summary.running.gpus.to_string()),
        Cell::from("│"),
        Cell::from(row.summary.pending.jobs.to_string()),
        Cell::from(row.summary.pending.cpus.to_string()),
        Cell::from(row.summary.pending.gpus.to_string()),
    ])
    .style(style)
}

fn summary_row_style(theme: Theme, kind: SummaryRowKind) -> Style {
    match kind {
        SummaryRowKind::All => theme.summary_all,
        SummaryRowKind::Me => theme.summary_me,
        SummaryRowKind::User | SummaryRowKind::Others => theme.summary_others,
    }
}

fn summary_total_jobs(summary: &OwnerSummary) -> u64 {
    summary.running.jobs + summary.pending.jobs
}

fn merge_owner_summary(total: &mut OwnerSummary, stats: &OwnerSummary) {
    total.running.jobs += stats.running.jobs;
    total.running.cpus += stats.running.cpus;
    total.running.gpus += stats.running.gpus;
    total.running.memory.0 += stats.running.memory.0;
    total.pending.jobs += stats.pending.jobs;
    total.pending.cpus += stats.pending.cpus;
    total.pending.gpus += stats.pending.gpus;
    total.pending.memory.0 += stats.pending.memory.0;
}

#[must_use]
fn slurm_time_display(value: &str) -> String {
    let value = value.trim();
    if value.is_empty()
        || matches!(
            value.to_ascii_lowercase().as_str(),
            "none" | "n/a" | "(null)" | "null"
        )
    {
        "—".to_string()
    } else {
        value.to_string()
    }
}

fn gpu_map_display(job: &Job) -> String {
    if job.gpus.is_empty() {
        return "0".to_string();
    }
    job.gpus
        .iter()
        .map(|(gpu_type, count)| format!("{gpu_type}:{count}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn truncate_text(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let truncated: String = text.chars().take(max_chars.saturating_sub(1)).collect();
    format!("{truncated}\u{2026}")
}

fn compute_panel_areas(
    area: Rect,
    panels: &[PanelUiState; 5],
    left_percent: u16,
    top_percent: u16,
) -> [Option<Rect>; 5] {
    let visible: Vec<_> = PanelId::ALL
        .into_iter()
        .filter(|panel| panels[panel.index()].visible)
        .collect();
    let mut areas = [None, None, None, None, None];
    if visible.is_empty() {
        return areas;
    }
    if visible.len() == 1 {
        areas[visible[0].index()] = Some(area);
        return areas;
    }

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(top_percent),
            Constraint::Percentage(100 - top_percent),
        ])
        .split(area);
    place_row(
        &mut areas,
        rows[0],
        &[
            (PanelId::Jobs, panels[PanelId::Jobs.index()].visible),
            (PanelId::Nodes, panels[PanelId::Nodes.index()].visible),
        ],
        left_percent,
    );
    place_row(
        &mut areas,
        rows[1],
        &[
            (PanelId::Gpus, panels[PanelId::Gpus.index()].visible),
            (PanelId::Disks, panels[PanelId::Disks.index()].visible),
            (PanelId::Summary, panels[PanelId::Summary.index()].visible),
        ],
        left_percent,
    );

    let top_empty =
        areas[PanelId::Jobs.index()].is_none() && areas[PanelId::Nodes.index()].is_none();
    let bottom_empty = areas[PanelId::Gpus.index()].is_none()
        && areas[PanelId::Summary.index()].is_none()
        && areas[PanelId::Disks.index()].is_none();
    if top_empty || bottom_empty {
        areas.fill(None);
        place_row(
            &mut areas,
            area,
            &[
                (PanelId::Jobs, panels[PanelId::Jobs.index()].visible),
                (PanelId::Nodes, panels[PanelId::Nodes.index()].visible),
                (PanelId::Gpus, panels[PanelId::Gpus.index()].visible),
                (PanelId::Disks, panels[PanelId::Disks.index()].visible),
                (PanelId::Summary, panels[PanelId::Summary.index()].visible),
            ],
            left_percent,
        );
    }
    areas
}

fn place_row(
    areas: &mut [Option<Rect>; 5],
    row: Rect,
    panels: &[(PanelId, bool)],
    left_percent: u16,
) {
    let visible: Vec<_> = panels
        .iter()
        .filter_map(|(panel, visible)| visible.then_some(*panel))
        .collect();
    if visible.is_empty() {
        return;
    }
    if visible.len() == 1 {
        areas[visible[0].index()] = Some(row);
        return;
    }
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(left_percent),
            Constraint::Percentage(100 - left_percent),
        ])
        .split(row);
    for (idx, panel) in visible.iter().take(2).enumerate() {
        areas[panel.index()] = Some(chunks[idx]);
    }
    if visible.len() > 2 {
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(vec![
                Constraint::Ratio(
                    1,
                    u32::try_from(visible.len()).unwrap_or(1)
                );
                visible.len()
            ])
            .split(row);
        for (idx, panel) in visible.iter().enumerate() {
            areas[panel.index()] = Some(chunks[idx]);
        }
    }
}

fn themed_state_style(theme: &Theme, state: &str) -> Style {
    let state = state.to_ascii_uppercase();
    if state.starts_with('R') {
        theme.state_running
    } else if state.starts_with('P') {
        theme.state_pending
    } else if state.starts_with('F') || state.starts_with("CANCEL") {
        theme.state_failed
    } else {
        Style::default()
    }
}

fn themed_node_style(theme: &Theme, state: &str) -> Style {
    let state = state.to_ascii_lowercase();
    if state.contains("idle") {
        theme.node_idle
    } else if state.contains("down") || state.contains("drain") {
        theme.node_down
    } else {
        theme.node_mixed
    }
}

fn sort_indicator(direction: SortDirection) -> &'static str {
    match direction {
        SortDirection::Asc => " ▲",
        SortDirection::Desc => " ▼",
    }
}

fn decorate_headers<const N: usize>(
    headers: &[&str; N],
    sort_column: usize,
    direction: SortDirection,
    visible: &[bool],
) -> Vec<Cell<'static>> {
    headers
        .iter()
        .enumerate()
        .filter(|(idx, _)| *visible.get(*idx).unwrap_or(&true))
        .map(|(idx, header)| {
            if idx == sort_column {
                Cell::from(format!("{header}{}", sort_indicator(direction)))
            } else {
                Cell::from((*header).to_string())
            }
        })
        .collect()
}

fn job_column_to_index(col: JobColumn) -> usize {
    match col {
        JobColumn::JobId => 0,
        JobColumn::User => 1,
        JobColumn::State => 2,
        JobColumn::Partition => 3,
        JobColumn::Qos => 4,
        JobColumn::Priority => 5,
        JobColumn::Name => 6,
        JobColumn::Nodes => 7,
        JobColumn::Cpus => 8,
        JobColumn::Gpus => 9,
        JobColumn::Memory => 10,
        JobColumn::Time => 11,
    }
}

fn node_column_to_index(col: NodeColumn) -> usize {
    match col {
        NodeColumn::Name => 0,
        NodeColumn::State => 1,
        NodeColumn::CpusTotal => 2,
        NodeColumn::CpusFree => 3,
        NodeColumn::MemoryTotal => 4,
        NodeColumn::MemoryFree => 5,
        NodeColumn::GpusTotal => 6,
        NodeColumn::GpusFree => 7,
    }
}

fn gpu_column_to_index(col: GpuColumn) -> usize {
    match col {
        GpuColumn::Type => 0,
        GpuColumn::Total => 1,
        GpuColumn::Active => 2,
        GpuColumn::Reserved => 3,
        GpuColumn::Free => 4,
    }
}

fn equal_widths(count: usize) -> Vec<Constraint> {
    let count = count.max(1);
    vec![Constraint::Ratio(1, u32::try_from(count).unwrap_or(1)); count]
}

fn select_visible<T: ToString, const N: usize>(
    values: [T; N],
    visible: &[bool],
) -> Vec<Cell<'static>> {
    values
        .into_iter()
        .enumerate()
        .filter(|(idx, _)| *visible.get(*idx).unwrap_or(&true))
        .map(|(_, value)| Cell::from(value.to_string()))
        .collect()
}

fn select_visible_cells<const N: usize>(
    values: [Cell<'static>; N],
    visible: &[bool],
) -> Vec<Cell<'static>> {
    values
        .into_iter()
        .enumerate()
        .filter(|(idx, _)| *visible.get(*idx).unwrap_or(&true))
        .map(|(_, value)| value)
        .collect()
}

fn select_visible_constraints(values: &[Constraint], visible: &[bool]) -> Vec<Constraint> {
    values
        .iter()
        .enumerate()
        .filter(|(idx, _)| *visible.get(*idx).unwrap_or(&true))
        .map(|(_, value)| *value)
        .collect()
}

fn clamp_selection(state: &mut TableState, len: usize) {
    if len == 0 {
        state.select(None);
        return;
    }
    let idx = state.selected().unwrap_or(0).min(len - 1);
    state.select(Some(idx));
}

fn move_table_selection(state: &mut TableState, len: usize, delta: isize) {
    if len == 0 {
        state.select(None);
        return;
    }
    let current = state.selected().unwrap_or(0).min(len - 1);
    let next = if delta.is_negative() {
        current.saturating_sub(delta.unsigned_abs())
    } else {
        current
            .saturating_add(delta.unsigned_abs())
            .min(len.saturating_sub(1))
    };
    state.select(Some(next));
}

fn select_visible_table_row(
    state: &mut TableState,
    len: usize,
    visible_row: usize,
) -> Option<usize> {
    if len == 0 {
        state.select(None);
        return None;
    }
    let selected = state.offset().saturating_add(visible_row);
    if selected < len {
        state.select(Some(selected));
        Some(selected)
    } else {
        None
    }
}

fn reset_table_to_top(state: &mut TableState) {
    *state.offset_mut() = 0;
    state.select(Some(0));
}

fn table_body_row_at(area: Rect, x: u16, y: u16) -> Option<usize> {
    let content_left = area.x.saturating_add(1);
    let content_right = area.x.saturating_add(area.width).saturating_sub(1);
    let first_row_y = area.y.saturating_add(2);
    let after_last_row_y = area.y.saturating_add(area.height).saturating_sub(1);
    if x < content_left || x >= content_right || y < first_row_y || y >= after_last_row_y {
        return None;
    }
    Some(usize::from(y.saturating_sub(first_row_y)))
}

#[must_use]
fn disk_size_bytes(value: &str) -> u128 {
    let value = value.trim();
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
    let (whole, fraction, scale) = decimal_parts_u128(&number);
    let multiplier = match unit.unwrap_or('B') {
        'K' => 1024_u128,
        'M' => 1024_u128.pow(2),
        'G' => 1024_u128.pow(3),
        'T' => 1024_u128.pow(4),
        'P' => 1024_u128.pow(5),
        'E' => 1024_u128.pow(6),
        _ => 1,
    };
    whole
        .saturating_mul(multiplier)
        .saturating_add(fraction.saturating_mul(multiplier) / scale)
}

fn decimal_parts_u128(number: &str) -> (u128, u128, u128) {
    let Some((whole, fraction)) = number.split_once('.') else {
        return (number.parse().unwrap_or(0), 0, 1);
    };
    let fraction_digits = fraction
        .chars()
        .filter(char::is_ascii_digit)
        .collect::<String>();
    if fraction_digits.is_empty() {
        return (whole.parse().unwrap_or(0), 0, 1);
    }
    let scale = 10_u128.saturating_pow(u32::try_from(fraction_digits.len()).unwrap_or(0));
    (
        whole.parse().unwrap_or(0),
        fraction_digits.parse().unwrap_or(0),
        scale,
    )
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1]);
    horizontal[1]
}

fn contains(area: Rect, x: u16, y: u16) -> bool {
    x >= area.x && x < area.x + area.width && y >= area.y && y < area.y + area.height
}

fn filter_label(filter: &FilterExpression) -> String {
    if filter.is_empty() {
        return "none".to_string();
    }
    let mut parts = Vec::new();
    if !filter.query.is_empty() {
        parts.push(format!("q={}", filter.query));
    }
    if let Some(owner) = &filter.owner {
        parts.push(format!("owner={owner}"));
    }
    if let Some(state) = &filter.state {
        parts.push(format!("state={state}"));
    }
    if let Some(partition) = &filter.partition {
        parts.push(format!("part={partition}"));
    }
    if let Some(gpu) = &filter.gpu_type {
        parts.push(format!("gpu={gpu}"));
    }
    if let Some(state) = &filter.node_state {
        parts.push(format!("node_state={state}"));
    }
    parts.join(",")
}

fn next_job_column(column: JobColumn) -> JobColumn {
    match column {
        JobColumn::State => JobColumn::JobId,
        JobColumn::JobId => JobColumn::User,
        JobColumn::User => JobColumn::Partition,
        JobColumn::Partition => JobColumn::Qos,
        JobColumn::Qos => JobColumn::Priority,
        JobColumn::Priority => JobColumn::Name,
        JobColumn::Name => JobColumn::Nodes,
        JobColumn::Nodes => JobColumn::Cpus,
        JobColumn::Cpus => JobColumn::Gpus,
        JobColumn::Gpus => JobColumn::Memory,
        JobColumn::Memory => JobColumn::Time,
        JobColumn::Time => JobColumn::State,
    }
}

fn next_node_column(column: NodeColumn) -> NodeColumn {
    match column {
        NodeColumn::State => NodeColumn::Name,
        NodeColumn::Name => NodeColumn::CpusTotal,
        NodeColumn::CpusTotal => NodeColumn::CpusFree,
        NodeColumn::CpusFree => NodeColumn::MemoryTotal,
        NodeColumn::MemoryTotal => NodeColumn::MemoryFree,
        NodeColumn::MemoryFree => NodeColumn::GpusTotal,
        NodeColumn::GpusTotal => NodeColumn::GpusFree,
        NodeColumn::GpusFree => NodeColumn::State,
    }
}

fn next_gpu_column(column: GpuColumn) -> GpuColumn {
    match column {
        GpuColumn::Type => GpuColumn::Total,
        GpuColumn::Total => GpuColumn::Active,
        GpuColumn::Active => GpuColumn::Reserved,
        GpuColumn::Reserved => GpuColumn::Free,
        GpuColumn::Free => GpuColumn::Type,
    }
}

fn next_disk_column(column: DiskColumn) -> DiskColumn {
    match column {
        DiskColumn::Usage => DiskColumn::Path,
        DiskColumn::Path => DiskColumn::Type,
        DiskColumn::Type => DiskColumn::Space,
        DiskColumn::Space => DiskColumn::Usage,
    }
}

fn next_user_job_column(column: UserJobColumn) -> UserJobColumn {
    match column {
        UserJobColumn::JobId => UserJobColumn::Name,
        UserJobColumn::Name => UserJobColumn::Node,
        UserJobColumn::Node => UserJobColumn::Cpus,
        UserJobColumn::Cpus => UserJobColumn::Gpus,
        UserJobColumn::Gpus => UserJobColumn::Memory,
        UserJobColumn::Memory => UserJobColumn::Time,
        UserJobColumn::Time => UserJobColumn::Limit,
        UserJobColumn::Limit => UserJobColumn::JobId,
    }
}

fn next_disk_usage_column(column: DiskUsageColumn) -> DiskUsageColumn {
    match column {
        DiskUsageColumn::User => DiskUsageColumn::Used,
        DiskUsageColumn::Used => DiskUsageColumn::Entries,
        DiskUsageColumn::Entries => DiskUsageColumn::User,
    }
}

fn next_accounting_column(column: AccountingColumn) -> AccountingColumn {
    match column {
        AccountingColumn::JobId => AccountingColumn::User,
        AccountingColumn::User => AccountingColumn::State,
        AccountingColumn::State => AccountingColumn::Partition,
        AccountingColumn::Partition => AccountingColumn::Cpus,
        AccountingColumn::Cpus => AccountingColumn::Memory,
        AccountingColumn::Memory => AccountingColumn::Elapsed,
        AccountingColumn::Elapsed => AccountingColumn::End,
        AccountingColumn::End => AccountingColumn::JobId,
    }
}

fn job_column_from_index(idx: usize) -> JobColumn {
    [
        JobColumn::JobId,
        JobColumn::User,
        JobColumn::State,
        JobColumn::Partition,
        JobColumn::Qos,
        JobColumn::Priority,
        JobColumn::Name,
        JobColumn::Nodes,
        JobColumn::Cpus,
        JobColumn::Gpus,
        JobColumn::Memory,
        JobColumn::Time,
    ]
    .get(idx)
    .copied()
    .unwrap_or(JobColumn::State)
}

fn node_column_from_index(idx: usize) -> NodeColumn {
    [
        NodeColumn::Name,
        NodeColumn::State,
        NodeColumn::CpusTotal,
        NodeColumn::CpusFree,
        NodeColumn::MemoryTotal,
        NodeColumn::MemoryFree,
        NodeColumn::GpusTotal,
        NodeColumn::GpusFree,
    ]
    .get(idx)
    .copied()
    .unwrap_or(NodeColumn::State)
}

fn gpu_column_from_index(idx: usize) -> GpuColumn {
    [
        GpuColumn::Type,
        GpuColumn::Total,
        GpuColumn::Active,
        GpuColumn::Reserved,
        GpuColumn::Free,
    ]
    .get(idx)
    .copied()
    .unwrap_or(GpuColumn::Type)
}

fn disk_column_to_index(col: DiskColumn) -> usize {
    match col {
        DiskColumn::Usage => 0,
        DiskColumn::Path => 1,
        DiskColumn::Type => 2,
        DiskColumn::Space => 3,
    }
}

fn disk_column_from_index(idx: usize) -> DiskColumn {
    [
        DiskColumn::Usage,
        DiskColumn::Path,
        DiskColumn::Type,
        DiskColumn::Space,
    ]
    .get(idx)
    .copied()
    .unwrap_or(DiskColumn::Space)
}

fn user_job_column_to_index(col: UserJobColumn) -> usize {
    match col {
        UserJobColumn::JobId => 0,
        UserJobColumn::Name => 1,
        UserJobColumn::Node => 2,
        UserJobColumn::Cpus => 3,
        UserJobColumn::Gpus => 4,
        UserJobColumn::Memory => 5,
        UserJobColumn::Time => 6,
        UserJobColumn::Limit => 7,
    }
}

fn user_job_column_from_index(idx: usize) -> UserJobColumn {
    [
        UserJobColumn::JobId,
        UserJobColumn::Name,
        UserJobColumn::Node,
        UserJobColumn::Cpus,
        UserJobColumn::Gpus,
        UserJobColumn::Memory,
        UserJobColumn::Time,
        UserJobColumn::Limit,
    ]
    .get(idx)
    .copied()
    .unwrap_or(UserJobColumn::JobId)
}

fn disk_usage_column_to_index(col: DiskUsageColumn) -> usize {
    match col {
        DiskUsageColumn::User => 0,
        DiskUsageColumn::Used => 1,
        DiskUsageColumn::Entries => 2,
    }
}

fn disk_usage_column_from_index(idx: usize) -> DiskUsageColumn {
    [
        DiskUsageColumn::User,
        DiskUsageColumn::Used,
        DiskUsageColumn::Entries,
    ]
    .get(idx)
    .copied()
    .unwrap_or(DiskUsageColumn::Used)
}

const fn default_user_job_direction(column: UserJobColumn) -> SortDirection {
    match column {
        UserJobColumn::JobId
        | UserJobColumn::Cpus
        | UserJobColumn::Gpus
        | UserJobColumn::Memory
        | UserJobColumn::Time
        | UserJobColumn::Limit => SortDirection::Desc,
        UserJobColumn::Name | UserJobColumn::Node => SortDirection::Asc,
    }
}

const fn default_disk_usage_direction(column: DiskUsageColumn) -> SortDirection {
    match column {
        DiskUsageColumn::User => SortDirection::Asc,
        DiskUsageColumn::Used | DiskUsageColumn::Entries => SortDirection::Desc,
    }
}

fn accounting_column_from_index(idx: usize) -> AccountingColumn {
    [
        AccountingColumn::JobId,
        AccountingColumn::User,
        AccountingColumn::State,
        AccountingColumn::Partition,
        AccountingColumn::Cpus,
        AccountingColumn::Memory,
        AccountingColumn::Elapsed,
        AccountingColumn::End,
    ]
    .get(idx)
    .copied()
    .unwrap_or(AccountingColumn::JobId)
}

#[must_use]
pub fn snapshot_age(snapshot: &ClusterSnapshot) -> Option<Duration> {
    SystemTime::now().duration_since(snapshot.captured_at).ok()
}

#[must_use]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_row_style_uses_truecolor_dark_text_on_teal() {
        let style = selected_row_style();
        assert_eq!(style.fg, Some(Color::Rgb(8, 20, 24)));
        assert_eq!(style.bg, Some(Color::Rgb(0, 188, 188)));
        assert!(!style.add_modifier.contains(Modifier::BOLD));
    }

    #[test]
    fn table_body_row_at_excludes_borders_and_header() {
        let area = Rect::new(10, 5, 20, 8);

        assert_eq!(table_body_row_at(area, 11, 5), None);
        assert_eq!(table_body_row_at(area, 11, 6), None);
        assert_eq!(table_body_row_at(area, 10, 7), None);
        assert_eq!(table_body_row_at(area, 29, 7), None);
        assert_eq!(table_body_row_at(area, 11, 12), None);
        assert_eq!(table_body_row_at(area, 11, 7), Some(0));
        assert_eq!(table_body_row_at(area, 11, 11), Some(4));
    }

    #[test]
    fn visible_table_row_selection_preserves_scroll_offset() {
        let mut state = TableState::default()
            .with_offset(10)
            .with_selected(Some(12));

        let selected = select_visible_table_row(&mut state, 50, 3);

        assert_eq!(selected, Some(13));
        assert_eq!(state.offset(), 10);
        assert_eq!(state.selected(), Some(13));
    }

    #[test]
    fn visible_table_row_selection_ignores_clicks_past_rows() {
        let mut state = TableState::default()
            .with_offset(10)
            .with_selected(Some(11));

        let selected = select_visible_table_row(&mut state, 12, 3);

        assert_eq!(selected, None);
        assert_eq!(state.selected(), Some(11));
    }

    #[test]
    fn row_click_matches_same_row_within_double_click_interval() {
        let at = Instant::now();
        let click = RowClick {
            panel: PanelId::Jobs,
            row: 7,
            at,
        };

        assert!(click.matches(PanelId::Jobs, 7, at + Duration::from_millis(449)));
        assert!(!click.matches(PanelId::Nodes, 7, at + Duration::from_millis(1)));
        assert!(!click.matches(PanelId::Jobs, 8, at + Duration::from_millis(1)));
        assert!(!click.matches(
            PanelId::Jobs,
            7,
            at + DOUBLE_CLICK_MAX_INTERVAL + Duration::from_millis(1)
        ));
    }

    #[test]
    fn reset_table_to_top_clears_scroll_offset_and_selection() {
        let mut state = TableState::default()
            .with_offset(42)
            .with_selected(Some(47));

        reset_table_to_top(&mut state);

        assert_eq!(state.offset(), 0);
        assert_eq!(state.selected(), Some(0));
    }
}
