//! Terminal UI for slmtop.

use std::io::{self, Stdout};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event as CrosstermEvent, KeyCode, KeyEvent,
    KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
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
use slmtop_core::{
    bucket_display, filter_accounting, filter_jobs, filter_nodes, sort_accounting, sort_jobs,
    sort_nodes, AccountingColumn, AccountingRecord, ClusterSnapshot, FilterExpression, GpuSummary,
    Job, JobColumn, Node, NodeColumn, PanelId, SortDirection,
};
use slmtop_slurm::{refresh_backend, BackendConfig, SlurmClient, SlurmError, SnapshotEnvelope};
use thiserror::Error;
use tokio::sync::mpsc;
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

#[derive(Debug, Clone, Copy)]
pub struct TuiOptions {
    pub tick_rate: Duration,
}

impl Default for TuiOptions {
    fn default() -> Self {
        Self {
            tick_rate: Duration::from_millis(80),
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
    let mut app = AppState::new(config.current_user.clone(), config.refresh_interval);

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
                CrosstermEvent::Mouse(mouse) => app.handle_mouse(mouse),
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

#[derive(Debug)]
enum UiMessage {
    Snapshot(Box<SnapshotEnvelope>),
    Error(String),
    ActionResult(String),
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

struct AppState {
    current_user: String,
    refresh_interval: Duration,
    snapshot: Option<SnapshotEnvelope>,
    last_error: Option<String>,
    status: String,
    focus: PanelId,
    mode: InputMode,
    input: String,
    show_help: bool,
    details_job: Option<String>,
    pending_action: Option<PendingAction>,
    left_percent: u16,
    top_percent: u16,
    panels: [PanelUiState; 4],
    jobs_sort: JobColumn,
    nodes_sort: NodeColumn,
    gpu_sort: GpuColumn,
    accounting_sort: AccountingColumn,
    directions: [SortDirection; 4],
    jobs_table: TableState,
    nodes_table: TableState,
    gpus_table: TableState,
    summary_table: TableState,
    panel_areas: [Option<Rect>; 4],
    header_hits: Vec<HeaderHit>,
}

impl AppState {
    fn new(current_user: String, refresh_interval: Duration) -> Self {
        let mut jobs_table = TableState::default();
        jobs_table.select(Some(0));
        let mut nodes_table = TableState::default();
        nodes_table.select(Some(0));
        let mut gpus_table = TableState::default();
        gpus_table.select(Some(0));
        let mut summary_table = TableState::default();
        summary_table.select(Some(0));
        Self {
            current_user,
            refresh_interval,
            snapshot: None,
            last_error: None,
            status: "Starting Slurm refresh...".to_string(),
            focus: PanelId::Jobs,
            mode: InputMode::Normal,
            input: String::new(),
            show_help: false,
            details_job: None,
            pending_action: None,
            left_percent: 62,
            top_percent: 68,
            panels: [
                PanelUiState::new(10),
                PanelUiState::new(9),
                PanelUiState::new(5),
                PanelUiState::new(8),
            ],
            jobs_sort: JobColumn::State,
            nodes_sort: NodeColumn::State,
            gpu_sort: GpuColumn::Type,
            accounting_sort: AccountingColumn::JobId,
            directions: [SortDirection::Asc; 4],
            jobs_table,
            nodes_table,
            gpus_table,
            summary_table,
            panel_areas: [None, None, None, None],
            header_hits: Vec::new(),
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
        }
    }

    fn draw(&mut self, frame: &mut Frame<'_>) {
        self.header_hits.clear();
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
                .border_style(Style::default().fg(Color::Cyan));
            let paragraph = Paragraph::new("Waiting for Slurm data...")
                .block(block)
                .style(Style::default().fg(Color::Gray));
            frame.render_widget(paragraph, vertical[0]);
        } else {
            self.draw_jobs(frame);
            self.draw_nodes(frame);
            self.draw_gpus(frame);
            self.draw_summary(frame);
        }

        self.draw_status(frame, vertical[1]);
        if self.show_help {
            Self::draw_help(frame, centered_rect(74, 70, root));
        }
        if let Some(job_id) = self.details_job.clone() {
            self.draw_job_details(frame, &job_id, centered_rect(78, 62, root));
        }
        if let Some(pending) = self.pending_action.clone() {
            Self::draw_confirmation(frame, &pending, centered_rect(62, 24, root));
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
            "JOBID", "USER", "STATE", "PART", "NAME", "NODES", "CPUS", "GPUS", "MEM", "TIME",
        ];
        let visible = self.visible_columns(PanelId::Jobs, headers.len());
        let table_rows = rows.iter().map(|job| {
            let values = [
                job.id.clone(),
                job.user.clone(),
                job.state.clone(),
                job.partition.clone(),
                job.name.clone(),
                job.nodes.clone(),
                job.cpus.to_string(),
                job.gpu_total().to_string(),
                job.memory.to_string(),
                job.time_used.clone(),
            ];
            Row::new(select_visible(values, &visible)).style(state_style(&job.state))
        });
        self.add_header_hits(area, PanelId::Jobs, visible.len());
        let title = self.panel_title(
            PanelId::Jobs,
            format!(
                "sort={:?} {:?} rows={} filter={}",
                self.jobs_sort,
                self.directions[PanelId::Jobs.index()],
                rows.len(),
                filter_label(&self.panels[PanelId::Jobs.index()].filter)
            ),
        );
        let table = Table::new(table_rows, equal_widths(visible.len()))
            .header(Row::new(select_visible(headers, &visible)).style(header_style()))
            .block(panel_block(title, self.focus == PanelId::Jobs))
            .row_highlight_style(
                Style::default()
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            );
        frame.render_stateful_widget(table, area, &mut self.jobs_table);
    }

    fn draw_nodes(&mut self, frame: &mut Frame<'_>) {
        let Some(area) = self.panel_areas[PanelId::Nodes.index()] else {
            return;
        };
        let rows = self.visible_nodes();
        clamp_selection(&mut self.nodes_table, rows.len());
        let headers = [
            "NODE", "STATE", "CPU(T)", "CPU(A)", "CPU(I)", "MEM(T)", "MEM(R)", "MEM(F)", "GPU",
        ];
        let visible = self.visible_columns(PanelId::Nodes, headers.len());
        let table_rows = rows.iter().map(|node| {
            let values = [
                node.name.clone(),
                node.state.clone(),
                node.cpus.total.to_string(),
                node.cpus.allocated.to_string(),
                node.cpus.idle.to_string(),
                node.memory_total.to_string(),
                node.memory_reserved.to_string(),
                node.memory_free.to_string(),
                node.gpu_total().to_string(),
            ];
            Row::new(select_visible(values, &visible)).style(node_style(&node.state))
        });
        self.add_header_hits(area, PanelId::Nodes, visible.len());
        let title = self.panel_title(
            PanelId::Nodes,
            format!(
                "sort={:?} {:?} rows={} filter={}",
                self.nodes_sort,
                self.directions[PanelId::Nodes.index()],
                rows.len(),
                filter_label(&self.panels[PanelId::Nodes.index()].filter)
            ),
        );
        let table = Table::new(table_rows, equal_widths(visible.len()))
            .header(Row::new(select_visible(headers, &visible)).style(header_style()))
            .block(panel_block(title, self.focus == PanelId::Nodes))
            .row_highlight_style(
                Style::default()
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            );
        frame.render_stateful_widget(table, area, &mut self.nodes_table);
    }

    fn draw_gpus(&mut self, frame: &mut Frame<'_>) {
        let Some(area) = self.panel_areas[PanelId::Gpus.index()] else {
            return;
        };
        let rows = self.gpu_rows();
        clamp_selection(&mut self.gpus_table, rows.len());
        let headers = ["TYPE", "TOTAL", "ACTIVE", "RESERVED", "FREE"];
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
        self.add_header_hits(area, PanelId::Gpus, visible.len());
        let title = self.panel_title(
            PanelId::Gpus,
            format!(
                "sort={:?} {:?} rows={}",
                self.gpu_sort,
                self.directions[PanelId::Gpus.index()],
                rows.len()
            ),
        );
        let table = Table::new(table_rows, equal_widths(visible.len()))
            .header(Row::new(select_visible(headers, &visible)).style(header_style()))
            .block(panel_block(title, self.focus == PanelId::Gpus))
            .row_highlight_style(
                Style::default()
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            );
        frame.render_stateful_widget(table, area, &mut self.gpus_table);
    }

    fn draw_summary(&mut self, frame: &mut Frame<'_>) {
        let Some(area) = self.panel_areas[PanelId::Summary.index()] else {
            return;
        };
        let rows = self.visible_accounting();
        clamp_selection(&mut self.summary_table, rows.len() + 3);
        let headers = [
            "JOBID", "USER", "STATE", "PART", "CPUS", "MEM", "ELAPSED", "END",
        ];
        let visible = self.visible_columns(PanelId::Summary, headers.len());
        let mut table_rows = Vec::new();
        if let Some(snapshot) = self.snapshot.as_ref() {
            let summary = &snapshot.snapshot.job_summary;
            table_rows.push(
                Row::new(vec![
                    Cell::from("All"),
                    Cell::from(bucket_display(&summary.all.running)),
                    Cell::from(bucket_display(&summary.all.pending)),
                ])
                .style(Style::default().fg(Color::Cyan)),
            );
            table_rows.push(
                Row::new(vec![
                    Cell::from("Me"),
                    Cell::from(bucket_display(&summary.me.running)),
                    Cell::from(bucket_display(&summary.me.pending)),
                ])
                .style(Style::default().fg(Color::Green)),
            );
            table_rows.push(
                Row::new(vec![
                    Cell::from("Others"),
                    Cell::from(bucket_display(&summary.others.running)),
                    Cell::from(bucket_display(&summary.others.pending)),
                ])
                .style(Style::default().fg(Color::Yellow)),
            );
        }
        table_rows.extend(rows.iter().map(|row| {
            let values = [
                row.job_id.clone(),
                row.user.clone(),
                row.state.clone(),
                row.partition.clone(),
                row.cpus.to_string(),
                row.memory.to_string(),
                row.elapsed.clone(),
                row.end.clone(),
            ];
            Row::new(select_visible(values, &visible))
        }));
        self.add_header_hits(area, PanelId::Summary, visible.len());
        let title = self.panel_title(
            PanelId::Summary,
            format!(
                "running/pending + sacct sort={:?} {:?} rows={}",
                self.accounting_sort,
                self.directions[PanelId::Summary.index()],
                rows.len()
            ),
        );
        let table = Table::new(table_rows, equal_widths(visible.len().max(3)))
            .header(Row::new(select_visible(headers, &visible)).style(header_style()))
            .block(panel_block(title, self.focus == PanelId::Summary))
            .row_highlight_style(
                Style::default()
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            );
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
            Span::styled(" slmtop ", Style::default().fg(Color::Black).bg(Color::Cyan).add_modifier(Modifier::BOLD)),
            Span::raw(format!(
                " {mode} focus={} | Tab focus  / search  f filter  s sort  d dir  c column  x hide  v show  [] resize  ? help  q quit | {}{}",
                self.focus.title(), self.status, error
            )),
        ]);
        frame.render_widget(
            Paragraph::new(text).style(Style::default().fg(Color::White)),
            area,
        );
    }

    fn draw_help(frame: &mut Frame<'_>, area: Rect) {
        frame.render_widget(Clear, area);
        let text = vec![
            Line::from("slmtop help"),
            Line::from("Tab/Shift-Tab or 1-4: focus panels. Arrow keys: move selected row."),
            Line::from("s: cycle sort column, d: toggle direction, mouse-click table headers to sort."),
            Line::from("/: search focused panel, f: typed filters like owner=me state=running gpu=a100."),
            Line::from("c: hide/show the next optional column in the focused panel."),
            Line::from("x: hide focused panel, v: show next hidden panel, [ ] resize width, { } resize height."),
            Line::from("Enter on a job: details. In details: c cancel, h hold, u release, r requeue, Esc close."),
            Line::from("? or Esc: close this help."),
        ];
        frame.render_widget(
            Paragraph::new(text)
                .wrap(Wrap { trim: true })
                .block(panel_block("Help".to_string(), true)),
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
            Paragraph::new(text).block(panel_block(prompt.to_string(), true)),
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
        let lines = vec![
            Line::from(format!("Job ID    : {}", job.id)),
            Line::from(format!("User      : {}", job.user)),
            Line::from(format!("State     : {}", job.state)),
            Line::from(format!("Partition : {}", job.partition)),
            Line::from(format!("Name      : {}", job.name)),
            Line::from(format!("Nodes     : {}", job.nodes)),
            Line::from(format!("CPUs      : {}", job.cpus)),
            Line::from(format!("GPUs      : {gpu_text}")),
            Line::from(format!("Memory    : {}", job.memory)),
            Line::from(format!("Time      : {}", job.time_used)),
            Line::from(""),
            Line::from("c cancel | h hold | u release | r requeue | Esc close"),
        ];
        frame.render_widget(
            Paragraph::new(lines)
                .wrap(Wrap { trim: true })
                .block(panel_block("Job Details".to_string(), true)),
            area,
        );
    }

    fn draw_confirmation(frame: &mut Frame<'_>, pending: &PendingAction, area: Rect) {
        frame.render_widget(Clear, area);
        let lines = vec![
            Line::from(format!(
                "Confirm {} for job {}?",
                pending.action.label(),
                pending.job_id
            )),
            Line::from("y: confirm | n/Esc: cancel"),
        ];
        frame.render_widget(
            Paragraph::new(lines).block(panel_block("Confirm Job Action".to_string(), true)),
            area,
        );
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
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::Enter => {
                if self.focus == PanelId::Jobs {
                    self.details_job = self.selected_job().map(|job| job.id);
                }
            }
            _ => {}
        }
        false
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

    fn handle_mouse(&mut self, mouse: MouseEvent) {
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
            return;
        }
        for panel in PanelId::ALL {
            if let Some(area) = self.panel_areas[panel.index()] {
                if contains(area, mouse.column, mouse.row) {
                    self.focus_visible(panel);
                    let row = mouse.row.saturating_sub(area.y + 2) as usize;
                    self.select_row(panel, row);
                    return;
                }
            }
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

    fn visible_accounting(&self) -> Vec<AccountingRecord> {
        let Some(snapshot) = self.snapshot.as_ref() else {
            return Vec::new();
        };
        let filter = &self.panels[PanelId::Summary.index()].filter;
        let rows = filter_accounting(&snapshot.snapshot.accounting, filter, &self.current_user)
            .into_iter()
            .cloned()
            .collect();
        sort_accounting(
            rows,
            self.accounting_sort,
            self.directions[PanelId::Summary.index()],
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
        let table = self.focused_table_mut();
        let current = table.selected().unwrap_or(0);
        let next = if delta.is_negative() {
            current.saturating_sub(delta.unsigned_abs())
        } else {
            current.saturating_add(delta.unsigned_abs())
        };
        table.select(Some(next));
    }

    fn select_row(&mut self, panel: PanelId, row: usize) {
        match panel {
            PanelId::Jobs => self.jobs_table.select(Some(row)),
            PanelId::Nodes => self.nodes_table.select(Some(row)),
            PanelId::Gpus => self.gpus_table.select(Some(row)),
            PanelId::Summary => self.summary_table.select(Some(row)),
        }
    }

    fn focused_table_mut(&mut self) -> &mut TableState {
        match self.focus {
            PanelId::Jobs => &mut self.jobs_table,
            PanelId::Nodes => &mut self.nodes_table,
            PanelId::Gpus => &mut self.gpus_table,
            PanelId::Summary => &mut self.summary_table,
        }
    }

    fn toggle_direction(&mut self) {
        let idx = self.focus.index();
        self.directions[idx] = self.directions[idx].toggled();
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
        }
    }

    fn apply_header_sort(&mut self, panel: PanelId, visible_column: usize) {
        let actual = self.actual_column(panel, visible_column);
        match panel {
            PanelId::Jobs => self.jobs_sort = job_column_from_index(actual),
            PanelId::Nodes => self.nodes_sort = node_column_from_index(actual),
            PanelId::Gpus => self.gpu_sort = gpu_column_from_index(actual),
            PanelId::Summary => self.accounting_sort = accounting_column_from_index(actual),
        }
        self.toggle_direction();
    }

    fn actual_column(&self, panel: PanelId, visible_column: usize) -> usize {
        let mut seen = 0;
        for (idx, visible) in self.panels[panel.index()].columns.iter().enumerate() {
            if *visible {
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

    fn add_header_hits(&mut self, area: Rect, panel: PanelId, visible_columns: usize) {
        if visible_columns == 0 || area.width <= 2 || area.height <= 2 {
            return;
        }
        let y = area.y + 1;
        let x = area.x + 1;
        let width = area.width.saturating_sub(2);
        let visible_columns = u16::try_from(visible_columns).unwrap_or(u16::MAX).max(1);
        let col_width = (width / visible_columns).max(1);
        for idx in 0..visible_columns {
            let start = x + idx * col_width;
            let end = if idx + 1 == visible_columns {
                x + width
            } else {
                start + col_width
            };
            self.header_hits.push(HeaderHit {
                panel,
                column: usize::from(idx),
                x_start: start,
                x_end: end,
                y,
            });
        }
    }

    fn panel_title(&self, panel: PanelId, suffix: impl std::fmt::Display) -> String {
        let marker = if self.focus == panel { "*" } else { " " };
        format!("{marker} {} | {suffix}", panel.title())
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

fn compute_panel_areas(
    area: Rect,
    panels: &[PanelUiState; 4],
    left_percent: u16,
    top_percent: u16,
) -> [Option<Rect>; 4] {
    let visible: Vec<_> = PanelId::ALL
        .into_iter()
        .filter(|panel| panels[panel.index()].visible)
        .collect();
    let mut areas = [None, None, None, None];
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
            (PanelId::Summary, panels[PanelId::Summary.index()].visible),
        ],
        left_percent,
    );

    let top_empty =
        areas[PanelId::Jobs.index()].is_none() && areas[PanelId::Nodes.index()].is_none();
    let bottom_empty =
        areas[PanelId::Gpus.index()].is_none() && areas[PanelId::Summary.index()].is_none();
    if top_empty || bottom_empty {
        areas.fill(None);
        place_row(
            &mut areas,
            area,
            &[
                (PanelId::Jobs, panels[PanelId::Jobs.index()].visible),
                (PanelId::Nodes, panels[PanelId::Nodes.index()].visible),
                (PanelId::Gpus, panels[PanelId::Gpus.index()].visible),
                (PanelId::Summary, panels[PanelId::Summary.index()].visible),
            ],
            left_percent,
        );
    }
    areas
}

fn place_row(
    areas: &mut [Option<Rect>; 4],
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

fn panel_block(title: String, focused: bool) -> Block<'static> {
    let style = if focused {
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(style)
}

fn header_style() -> Style {
    Style::default()
        .fg(Color::Black)
        .bg(Color::Cyan)
        .add_modifier(Modifier::BOLD)
}

fn state_style(state: &str) -> Style {
    let state = state.to_ascii_uppercase();
    if state.starts_with('R') {
        Style::default().fg(Color::Green)
    } else if state.starts_with('P') {
        Style::default().fg(Color::Yellow)
    } else if state.starts_with('F') || state.starts_with("CANCEL") {
        Style::default().fg(Color::Red)
    } else {
        Style::default()
    }
}

fn node_style(state: &str) -> Style {
    let state = state.to_ascii_lowercase();
    if state.contains("idle") {
        Style::default().fg(Color::Green)
    } else if state.contains("down") || state.contains("drain") {
        Style::default().fg(Color::Red)
    } else {
        Style::default().fg(Color::Yellow)
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

fn clamp_selection(state: &mut TableState, len: usize) {
    if len == 0 {
        state.select(None);
        return;
    }
    let idx = state.selected().unwrap_or(0).min(len - 1);
    state.select(Some(idx));
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
        JobColumn::Partition => JobColumn::Name,
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
        NodeColumn::CpusTotal => NodeColumn::CpusAllocated,
        NodeColumn::CpusAllocated => NodeColumn::CpusIdle,
        NodeColumn::CpusIdle => NodeColumn::MemoryTotal,
        NodeColumn::MemoryTotal => NodeColumn::MemoryReserved,
        NodeColumn::MemoryReserved => NodeColumn::MemoryFree,
        NodeColumn::MemoryFree => NodeColumn::Gpus,
        NodeColumn::Gpus => NodeColumn::State,
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
        NodeColumn::CpusAllocated,
        NodeColumn::CpusIdle,
        NodeColumn::MemoryTotal,
        NodeColumn::MemoryReserved,
        NodeColumn::MemoryFree,
        NodeColumn::Gpus,
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
