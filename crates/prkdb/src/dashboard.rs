//! Lightweight read-only dashboard server you can attach to any `PrkDb` instance.
//! Shows collections, partitioning, live metrics, logs, and traces.
use axum::{
    extract::State,
    response::{sse::Event, sse::Sse, Html},
    routing::{get, post},
    Json, Router,
};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};
use sysinfo::{Networks, System};
use tokio::sync::broadcast;
use tokio::time::interval;
use tokio_stream::wrappers::{BroadcastStream, IntervalStream};
use tokio_stream::StreamExt;
use tracing::{Event as TracingEvent, Subscriber};
use tracing_subscriber::{layer::Context, Layer};

use crate::db::PrkDb;
use crate::partitioning::PartitionId;
use crate::pipeline;

#[derive(Clone)]
pub struct DashboardState {
    db: Arc<PrkDb>,
    log_tx: broadcast::Sender<LogEntry>,
    traces: Arc<RwLock<VecDeque<TraceEntry>>>,
    sys: Arc<Mutex<System>>,
    networks: Arc<Mutex<Networks>>,
}

const PEER_TTL_SECS: u64 = 15;
const MAX_LOG_HISTORY: usize = 100;
const MAX_TRACE_HISTORY: usize = 100;

#[derive(Debug, Serialize, Clone)]
pub struct LogEntry {
    pub timestamp: String,
    pub level: String,
    pub target: String,
    pub message: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct TraceEntry {
    pub timestamp: String,
    pub name: String,
    pub duration_ms: u64,
    pub fields: String,
}

#[derive(Debug, Serialize)]
pub struct CollectionInfo {
    pub name: String,
    pub partitions: u32,
}

#[derive(Debug, Serialize, Default)]
pub struct SystemMetrics {
    pub cpu_usage_percent: f32,
    pub memory_used_mb: u64,
    pub memory_total_mb: u64,
    pub open_fds: u64,
    pub network_rx_bytes: u64,
    pub network_tx_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct MetricsSnapshot {
    pub puts: u64,
    pub deletes: u64,
    pub outbox_saved: u64,
    pub outbox_drift: i64,
    pub dlq_saved: u64,
    pub partition_stats: crate::metrics::PartitionAggregateStats,
    pub system: SystemMetrics,
}

#[derive(Debug, Serialize)]
pub struct PartitionFlow {
    pub partition_id: PartitionId,
    pub produced: u64,
    pub consumed: u64,
    pub lag: u64,
    pub max_latency_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct PipelineFlow {
    pub name: String,
    pub source: String,
    pub sink: String,
    pub delivered: u64,
    pub failed: u64,
    pub retries: u64,
}

#[derive(Debug, Serialize)]
pub struct FlowSnapshot {
    pub metrics: MetricsSnapshot,
    pub partitions: Vec<PartitionFlow>,
    pub pipelines: Vec<PipelineFlow>,
    pub dbs: Vec<DbSummary>,
}

#[derive(Debug, Serialize, Clone)]
pub struct DbSummary {
    pub id: String,
    pub collections: usize,
    pub partitions: u64,
    pub puts: u64,
    pub outbox_drift: i64,
    pub last_seen_ms: u64,
    pub source: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DbRegistration {
    pub id: String,
    pub collections: usize,
    pub partitions: u64,
    pub puts: u64,
    pub outbox_drift: i64,
}

pub struct DashboardConfig<'a> {
    pub addr: &'a str,
}

impl<'a> Default for DashboardConfig<'a> {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:8088",
        }
    }
}

/// Global shared state for logs and traces to be accessed by layers
struct SharedObservability {
    log_tx: broadcast::Sender<LogEntry>,
    traces: Arc<RwLock<VecDeque<TraceEntry>>>,
}

static SHARED_OBSERVABILITY: OnceLock<SharedObservability> = OnceLock::new();

fn get_shared_observability() -> &'static SharedObservability {
    SHARED_OBSERVABILITY.get_or_init(|| {
        let (tx, _) = broadcast::channel(MAX_LOG_HISTORY);
        SharedObservability {
            log_tx: tx,
            traces: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_TRACE_HISTORY))),
        }
    })
}

/// Layer to capture logs and broadcast them to the dashboard
pub struct LogCaptureLayer;

impl<S> Layer<S> for LogCaptureLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &TracingEvent<'_>, _ctx: Context<'_, S>) {
        let obs = get_shared_observability();
        // Only do work if there are subscribers
        if obs.log_tx.receiver_count() > 0 {
            let mut visitor = MessageVisitor::default();
            event.record(&mut visitor);

            let entry = LogEntry {
                timestamp: chrono::Local::now().format("%H:%M:%S%.3f").to_string(),
                level: event.metadata().level().to_string(),
                target: event.metadata().target().to_string(),
                message: visitor.message,
            };
            let _ = obs.log_tx.send(entry);
        }
    }
}

#[derive(Default)]
struct MessageVisitor {
    message: String,
}

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{:?}", value);
        }
    }
}

/// Layer to capture slow traces
pub struct TraceCaptureLayer {
    threshold: Duration,
}

impl TraceCaptureLayer {
    pub fn new(threshold_ms: u64) -> Self {
        Self {
            threshold: Duration::from_millis(threshold_ms),
        }
    }
}

struct TraceTiming {
    start: Instant,
    name: String,
    fields: String,
}

impl<S> Layer<S> for TraceCaptureLayer
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::Id,
        ctx: Context<'_, S>,
    ) {
        let span = ctx.span(id).expect("span not found");
        let mut extensions = span.extensions_mut();

        // Simple field capture (could be improved)
        let fields = format!("{:?}", attrs);

        extensions.insert(TraceTiming {
            start: Instant::now(),
            name: span.name().to_string(),
            fields,
        });
    }

    fn on_close(&self, id: tracing::Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).expect("span not found");
        let extensions = span.extensions();

        if let Some(timing) = extensions.get::<TraceTiming>() {
            let duration = timing.start.elapsed();
            if duration >= self.threshold {
                let obs = get_shared_observability();
                let mut traces = obs.traces.write().unwrap();
                if traces.len() >= MAX_TRACE_HISTORY {
                    traces.pop_front();
                }
                traces.push_back(TraceEntry {
                    timestamp: chrono::Local::now().format("%H:%M:%S%.3f").to_string(),
                    name: timing.name.clone(),
                    duration_ms: duration.as_millis() as u64,
                    fields: timing.fields.clone(),
                });
            }
        }
    }
}

/// Start a read-only dashboard for the provided database. Returns when the server stops.
pub async fn serve(db: Arc<PrkDb>, cfg: DashboardConfig<'_>) -> anyhow::Result<()> {
    let obs = get_shared_observability();
    let mut sys = System::new_all();
    sys.refresh_all();
    let networks = Networks::new_with_refreshed_list();

    let state = DashboardState {
        db,
        log_tx: obs.log_tx.clone(),
        traces: obs.traces.clone(),
        sys: Arc::new(Mutex::new(sys)),
        networks: Arc::new(Mutex::new(networks)),
    };

    let router = Router::new()
        .route("/", get(index))
        .route("/api/collections", get(list_collections))
        .route("/api/metrics", get(metrics))
        .route("/api/dbs", get(list_dbs))
        .route("/api/traces", get(list_traces))
        .route("/api/register", post(register_peer))
        .route("/events", get(events_stream))
        .route("/logs", get(logs_stream))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(cfg.addr).await?;
    tracing::info!("Dashboard listening on http://{}", cfg.addr);
    axum::serve(listener, router).await?;
    Ok(())
}

/// Spawn the dashboard server if the `PRKDB_DASHBOARD` env var is set (default port 8088).
/// Intended for examples and CLI commands so you don't have to wire a separate dashboard per demo.
pub fn spawn_if_env(db: Arc<PrkDb>) -> Option<tokio::task::JoinHandle<()>> {
    if std::env::var("PRKDB_DASHBOARD").ok().as_deref() != Some("1") {
        return None;
    }
    let addr = std::env::var("PRKDB_DASHBOARD_ADDR").unwrap_or_else(|_| "127.0.0.1:8088".into());
    tracing::info!(%addr, "starting dashboard (read-only)");
    Some(tokio::spawn(async move {
        if let Err(err) = serve(db, DashboardConfig { addr: &addr }).await {
            tracing::warn!(%err, "dashboard server exited");
        }
    }))
}

/// If `PRKDB_DASHBOARD_REGISTRY` is set, periodically send this DB's summary to the registry
/// so a shared dashboard instance can list it.
pub fn spawn_peer_heartbeat(db: Arc<PrkDb>) -> Option<tokio::task::JoinHandle<()>> {
    let registry = std::env::var("PRKDB_DASHBOARD_REGISTRY").ok()?;
    let client = match reqwest::Client::builder().build() {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(%e, "failed to build reqwest client for dashboard heartbeat");
            return None;
        }
    };
    Some(tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(1));
        loop {
            ticker.tick().await;
            let summary = db_summary(&DashboardState {
                db: db.clone(),
                log_tx: broadcast::channel(1).0, // Dummy
                traces: Arc::new(RwLock::new(VecDeque::new())), // Dummy
                sys: Arc::new(Mutex::new(System::new())), // Dummy
                networks: Arc::new(Mutex::new(Networks::new())), // Dummy
            });
            let res = client
                .post(format!("{registry}/api/register"))
                .json(&DbRegistration {
                    id: summary.id.clone(),
                    collections: summary.collections,
                    partitions: summary.partitions,
                    puts: summary.puts,
                    outbox_drift: summary.outbox_drift,
                })
                .send()
                .await;
            if let Err(e) = res {
                tracing::debug!(%e, "dashboard heartbeat failed");
            }
        }
    }))
}

async fn list_collections(State(state): State<DashboardState>) -> Json<Vec<CollectionInfo>> {
    let mut colls = Vec::new();
    for entry in state.db.collection_registry.iter() {
        let name = entry.value().clone();
        // best effort partition count lookup
        let partitions = state
            .db
            .partitioning_registry
            .get(entry.key())
            .map(|p| p.num_partitions)
            .unwrap_or(1);
        colls.push(CollectionInfo { name, partitions });
    }
    Json(colls)
}

async fn metrics(State(state): State<DashboardState>) -> Json<MetricsSnapshot> {
    Json(snapshot(&state))
}

async fn list_dbs(State(state): State<DashboardState>) -> Json<Vec<DbSummary>> {
    Json(all_db_summaries(&state))
}

async fn list_traces(State(state): State<DashboardState>) -> Json<Vec<TraceEntry>> {
    let traces = state.traces.read().unwrap();
    Json(traces.iter().cloned().collect())
}

async fn register_peer(
    State(_state): State<DashboardState>,
    Json(reg): Json<DbRegistration>,
) -> Json<&'static str> {
    let entry = DbSummary {
        id: reg.id,
        collections: reg.collections,
        partitions: reg.partitions,
        puts: reg.puts,
        outbox_drift: reg.outbox_drift,
        last_seen_ms: 0,
        source: "peer".into(),
    };
    peers().insert(entry.id.clone(), (entry, Instant::now()));
    Json("ok")
}

async fn events_stream(
    State(state): State<DashboardState>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let stream = IntervalStream::new(interval(Duration::from_millis(750)))
        .map(move |_| flow_snapshot(&state))
        .map(|snap| {
            let ev = Event::default()
                .json_data(&snap)
                .expect("serialize snapshot for SSE");
            Ok(ev)
        });
    Sse::new(stream)
}

async fn logs_stream(
    State(state): State<DashboardState>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let rx = state.log_tx.subscribe();
    let stream = BroadcastStream::new(rx)
        .filter_map(|res| res.ok())
        .map(|entry| {
            let ev = Event::default()
                .json_data(&entry)
                .expect("serialize log entry for SSE");
            Ok(ev)
        });
    Sse::new(stream)
}

fn snapshot(state: &DashboardState) -> MetricsSnapshot {
    let m = state.db.metrics();

    // Collect system metrics
    let mut sys_metrics = SystemMetrics::default();
    if let Ok(mut sys) = state.sys.try_lock() {
        sys.refresh_cpu();
        sys.refresh_memory();

        sys_metrics.cpu_usage_percent = sys.global_cpu_info().cpu_usage();
        sys_metrics.memory_used_mb = sys.used_memory() / 1024 / 1024;
        sys_metrics.memory_total_mb = sys.total_memory() / 1024 / 1024;

        // Open FDs - skipped for now
        sys_metrics.open_fds = 0;
    }

    if let Ok(mut nets) = state.networks.try_lock() {
        nets.refresh();
        for (_interface_name, data) in nets.iter() {
            sys_metrics.network_rx_bytes += data.received();
            sys_metrics.network_tx_bytes += data.transmitted();
        }
    }

    MetricsSnapshot {
        puts: m.puts(),
        deletes: m.deletes(),
        outbox_saved: m.outbox_saved(),
        outbox_drift: m.outbox_drift(),
        dlq_saved: m.dlq_saved(),
        partition_stats: m.get_partition_aggregate_stats(),
        system: sys_metrics,
    }
}

fn flow_snapshot(state: &DashboardState) -> FlowSnapshot {
    let metrics = snapshot(state);
    let partitions = state
        .db
        .metrics()
        .partition_metrics()
        .all_metrics()
        .into_iter()
        .map(|pm| PartitionFlow {
            partition_id: pm.partition_id(),
            produced: pm.events_produced(),
            consumed: pm.events_consumed(),
            lag: pm.consumer_lag(),
            max_latency_ms: pm.max_consume_latency_ms(),
        })
        .collect();

    let pipelines = pipeline::telemetry_snapshots()
        .into_iter()
        .map(|p| PipelineFlow {
            name: p.name,
            source: p.source,
            sink: p.sink,
            delivered: p.delivered,
            failed: p.failed,
            retries: p.retries,
        })
        .collect();

    FlowSnapshot {
        metrics,
        partitions,
        pipelines,
        dbs: all_db_summaries(state),
    }
}

fn all_db_summaries(state: &DashboardState) -> Vec<DbSummary> {
    let mut out = vec![db_summary(state)];
    let now = Instant::now();
    peers().retain(|_, (_, seen)| now.duration_since(*seen).as_secs() < PEER_TTL_SECS);
    for entry in peers().iter() {
        let mut summary = entry.value().0.clone();
        summary.last_seen_ms = now.saturating_duration_since(entry.value().1).as_millis() as u64;
        out.push(summary);
    }
    out
}

fn db_summary(state: &DashboardState) -> DbSummary {
    let collections = state.db.collection_registry.len();
    let partitions: u64 = state
        .db
        .partitioning_registry
        .iter()
        .map(|p| p.num_partitions as u64)
        .sum::<u64>()
        .max(collections as u64); // at least one per collection
    let m = state.db.metrics();
    DbSummary {
        id: "default".to_string(),
        collections,
        partitions,
        puts: m.puts(),
        outbox_drift: m.outbox_drift(),
        last_seen_ms: 0,
        source: "local".into(),
    }
}

fn peers() -> &'static DashMap<String, (DbSummary, Instant)> {
    static PEERS: OnceLock<DashMap<String, (DbSummary, Instant)>> = OnceLock::new();
    PEERS.get_or_init(DashMap::new)
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

const INDEX_HTML: &str = r#"<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>PrkDB Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
      :root {
        --bg: #0f172a;
        --panel: #1e293b;
        --text: #e2e8f0;
        --muted: #94a3b8;
        --accent: #38bdf8;
        --accent-2: #818cf8;
        --success: #4ade80;
        --warn: #fbbf24;
        --error: #f87171;
        --border: #334155;
      }
      body { font-family: "Inter", system-ui, sans-serif; margin: 0; padding: 0; background: var(--bg); color: var(--text); height: 100vh; display: flex; flex-direction: column; overflow: hidden; }
      
      /* Header */
      header { padding: 0 24px; height: 60px; border-bottom: 1px solid var(--border); background: rgba(15, 23, 42, 0.9); display: flex; align-items: center; justify-content: space-between; }
      h1 { margin: 0; font-size: 18px; font-weight: 600; display: flex; align-items: center; gap: 12px; color: #fff; }
      .logo { width: 28px; height: 28px; background: linear-gradient(135deg, var(--accent), var(--accent-2)); border-radius: 6px; }
      
      nav { display: flex; gap: 8px; background: #0f172a; padding: 4px; border-radius: 8px; }
      nav button { background: transparent; border: none; color: var(--muted); padding: 6px 16px; border-radius: 6px; cursor: pointer; font-size: 13px; font-weight: 500; transition: all 0.2s; }
      nav button:hover { color: var(--text); background: rgba(255,255,255,0.05); }
      nav button.active { background: var(--panel); color: var(--accent); box-shadow: 0 1px 2px rgba(0,0,0,0.2); }

      /* Main Layout */
      main { flex: 1; padding: 20px; overflow-y: auto; display: flex; flex-direction: column; gap: 20px; }
      .view { display: none; height: 100%; }
      .view.active { display: flex; flex-direction: column; gap: 20px; }

      /* Dashboard Grid */
      .dashboard-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; min-height: min-content; }
      .card { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 16px; display: flex; flex-direction: column; }
      .card-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; }
      .card-title { font-size: 13px; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; }
      
      /* KPI Cards */
      .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; }
      .kpi-card { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 16px; position: relative; overflow: hidden; }
      .kpi-card::before { content: ''; position: absolute; left: 0; top: 0; bottom: 0; width: 4px; background: var(--accent); }
      .kpi-card.warn::before { background: var(--warn); }
      .kpi-card.error::before { background: var(--error); }
      .kpi-value { font-size: 24px; font-weight: 700; color: #fff; margin: 8px 0 4px; }
      .kpi-label { font-size: 12px; color: var(--muted); }

      /* Charts */
      .chart-container { position: relative; height: 200px; width: 100%; }
      
      /* Tables */
      table { width: 100%; border-collapse: collapse; font-size: 13px; }
      th, td { text-align: left; padding: 10px 12px; border-bottom: 1px solid var(--border); }
      th { color: var(--muted); font-weight: 500; font-size: 12px; background: rgba(0,0,0,0.2); }
      tr:last-child td { border-bottom: none; }
      .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
      
      /* Logs */
      #logs-container { font-family: ui-monospace, monospace; font-size: 12px; flex: 1; overflow-y: auto; background: #0b1120; border-radius: 8px; padding: 12px; border: 1px solid var(--border); }
      .log-entry { margin-bottom: 4px; display: flex; gap: 12px; line-height: 1.4; }
      .log-time { color: var(--muted); min-width: 90px; }
      .log-level { font-weight: bold; min-width: 50px; }
      .log-target { color: var(--accent-2); min-width: 140px; }
      .log-msg { color: var(--text); white-space: pre-wrap; word-break: break-all; }
      .level-INFO { color: var(--accent); }
      .level-WARN { color: var(--warn); }
      .level-ERROR { color: var(--error); }
      .level-DEBUG { color: var(--muted); }

      /* Traces */
      .trace-entry { cursor: pointer; transition: background 0.1s; }
      .trace-entry:hover { background: rgba(255,255,255,0.05); }
      .duration-warn { color: var(--warn); font-weight: 600; }
      .duration-bad { color: var(--error); font-weight: 700; }
    </style>
  </head>
  <body>
    <header>
      <h1><div class="logo"></div> PrkDB Dashboard</h1>
      <nav>
        <button class="active" onclick="switchView('overview')">Overview</button>
        <button onclick="switchView('logs')">Live Logs</button>
        <button onclick="switchView('traces')">Slow Traces</button>
      </nav>
    </header>
    
    <main>
      <!-- Overview Tab -->
      <div id="view-overview" class="view active">
        <!-- KPI Row -->
        <div class="kpi-grid">
          <div class="kpi-card" id="kpi-throughput">
            <div class="kpi-label">Throughput</div>
            <div class="kpi-value">0 ops/s</div>
          </div>
          <div class="kpi-card" id="kpi-latency">
            <div class="kpi-label">Avg Latency</div>
            <div class="kpi-value">0 ms</div>
          </div>
          <div class="kpi-card" id="kpi-lag">
            <div class="kpi-label">Consumer Lag</div>
            <div class="kpi-value">0</div>
          </div>
          <div class="kpi-card" id="kpi-partitions">
            <div class="kpi-label">Active Partitions</div>
            <div class="kpi-value">0</div>
          </div>
          <div class="kpi-card error" id="kpi-offline">
             <div class="kpi-label">Offline Partitions</div>
             <div class="kpi-value">0</div>
          </div>
        </div>

        <!-- Charts Row -->
        <div class="dashboard-grid">
          <div class="card">
            <div class="card-header"><div class="card-title">Broker Metrics</div></div>
            <div class="chart-container"><canvas id="chart-broker"></canvas></div>
          </div>
          <div class="card">
            <div class="card-header"><div class="card-title">Producer Metrics</div></div>
            <div class="chart-container"><canvas id="chart-producer"></canvas></div>
          </div>
          <div class="card">
            <div class="card-header"><div class="card-title">Consumer Metrics</div></div>
            <div class="chart-container"><canvas id="chart-consumer"></canvas></div>
          </div>
          <div class="card">
            <div class="card-header"><div class="card-title">System Metrics</div></div>
            <div class="chart-container"><canvas id="chart-system"></canvas></div>
          </div>
        </div>

        <!-- Tables Row -->
        <div class="dashboard-grid" style="grid-template-columns: 1fr 1fr;">
           <div class="card">
             <div class="card-header"><div class="card-title">Collections</div></div>
             <table id="collections"></table>
           </div>
           <div class="card">
             <div class="card-header"><div class="card-title">Pipelines</div></div>
             <table id="pipelines"></table>
           </div>
        </div>
      </div>

      <!-- Logs Tab -->
      <div id="view-logs" class="view">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
           <div class="card-title">Live Server Logs</div>
           <button onclick="clearLogs()" style="background: #334155; border: none; color: #fff; padding: 6px 12px; border-radius: 4px; cursor: pointer; font-size: 12px;">Clear Logs</button>
        </div>
        <div id="logs-container"></div>
      </div>

      <!-- Traces Tab -->
      <div id="view-traces" class="view">
        <div class="card" style="flex: 1;">
          <div class="card-header"><div class="card-title">Slow Operations (>10ms)</div></div>
          <table id="traces">
            <thead><tr><th>Time</th><th>Operation</th><th>Duration</th><th>Details</th></tr></thead>
            <tbody id="traces-body"></tbody>
          </table>
        </div>
      </div>
    </main>

    <script>
      // --- Chart Setup ---
      Chart.defaults.color = '#94a3b8';
      Chart.defaults.borderColor = '#334155';
      Chart.defaults.font.family = '"Inter", system-ui, sans-serif';
      
      const chartConfig = (label, color) => ({
        type: 'line',
        data: {
          labels: Array(30).fill(''),
          datasets: [{
            label: label,
            data: Array(30).fill(0),
            borderColor: color,
            backgroundColor: color + '20',
            borderWidth: 2,
            tension: 0.3,
            fill: true,
            pointRadius: 0
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false } },
          scales: {
            x: { display: false },
            y: { beginAtZero: true, grid: { borderDash: [4, 4] } }
          },
          animation: { duration: 0 }
        }
      });

      const charts = {
        broker: new Chart(document.getElementById('chart-broker'), chartConfig('Throughput (ops/s)', '#38bdf8')),
        producer: new Chart(document.getElementById('chart-producer'), chartConfig('Bytes Out (KB/s)', '#818cf8')),
        consumer: new Chart(document.getElementById('chart-consumer'), chartConfig('Lag', '#fbbf24')),
        system: new Chart(document.getElementById('chart-system'), chartConfig('CPU %', '#4ade80'))
      };

      // --- Data Handling ---
      let lastMetrics = null;

      function updateChart(chart, value) {
        const data = chart.data.datasets[0].data;
        data.shift();
        data.push(value);
        chart.update();
      }

      async function fetchMetrics() {
        try {
          const res = await fetch('/api/metrics');
          const m = await res.json();
          
          // Calculate rates
          const now = Date.now();
          if (lastMetrics) {
             const dt = (now - lastMetrics.time) / 1000;
             if (dt > 0) {
               const opsPerSec = (m.puts - lastMetrics.puts) / dt;
               const bytesPerSec = (m.system.network_tx_bytes - lastMetrics.system.network_tx_bytes) / dt;
               
               updateChart(charts.broker, opsPerSec);
               updateChart(charts.producer, bytesPerSec / 1024);
               
               // KPIs
               document.querySelector('#kpi-throughput .kpi-value').innerText = opsPerSec.toFixed(1) + ' ops/s';
             }
          }
          
          // Instant values
          updateChart(charts.consumer, m.partition_stats.total_consumer_lag);
          updateChart(charts.system, m.system.cpu_usage_percent);
          
          // KPIs
          document.querySelector('#kpi-lag .kpi-value').innerText = m.partition_stats.total_consumer_lag;
          document.querySelector('#kpi-partitions .kpi-value').innerText = m.partition_stats.total_partitions;
          document.querySelector('#kpi-latency .kpi-value').innerText = m.partition_stats.avg_consume_latency_ms.toFixed(2) + ' ms';
          
          lastMetrics = { ...m, time: now };
        } catch (e) { console.error(e); }
      }

      async function fetchMetadata() {
        try {
          const [colsRes, pipesRes] = await Promise.all([
            fetch('/api/collections'),
            fetch('/api/collections') // TODO: Pipeline endpoint
          ]);
          
          const cols = await colsRes.json();
          document.getElementById('collections').innerHTML = 
            '<thead><tr><th>Name</th><th>Partitions</th></tr></thead><tbody>' +
            cols.map(c => `<tr><td>${c.name}</td><td>${c.partitions}</td></tr>`).join('') + '</tbody>';
            
          // Mock pipelines for now as endpoint might be missing
          document.getElementById('pipelines').innerHTML = 
            '<thead><tr><th>Name</th><th>Status</th></tr></thead><tbody>' +
            '<tr><td>main_pipeline</td><td><span style="color:var(--success)">Running</span></td></tr></tbody>';

        } catch (e) { console.error(e); }
      }
      
      async function fetchTraces() {
         try {
           const res = await fetch('/api/traces');
           const traces = await res.json();
           document.getElementById('traces-body').innerHTML = traces.map(t => {
             const durClass = t.duration_ms > 100 ? 'duration-bad' : 'duration-warn';
             return `<tr class="trace-entry" title="${t.fields}">
               <td class="mono">${t.timestamp}</td>
               <td>${t.name}</td>
               <td class="${durClass}">${t.duration_ms}ms</td>
               <td style="font-size: 11px; color: var(--muted); max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap">${t.fields}</td>
             </tr>`;
           }).join('');
         } catch (e) { console.error(e); }
      }

      // --- Logs ---
      const logsContainer = document.getElementById('logs-container');
      function appendLog(entry) {
        const div = document.createElement('div');
        div.className = 'log-entry';
        div.innerHTML = `
          <span class="log-time">${entry.timestamp}</span>
          <span class="log-level level-${entry.level}">${entry.level}</span>
          <span class="log-target">${entry.target}</span>
          <span class="log-msg">${entry.message}</span>
        `;
        logsContainer.appendChild(div);
        if (logsContainer.children.length > 500) logsContainer.removeChild(logsContainer.firstChild);
        logsContainer.scrollTop = logsContainer.scrollHeight;
      }
      function clearLogs() { logsContainer.innerHTML = ''; }

      // --- Initialization ---
      function switchView(viewName) {
        document.querySelectorAll('.view').forEach(el => el.classList.remove('active'));
        document.getElementById('view-' + viewName).classList.add('active');
        document.querySelectorAll('nav button').forEach(el => el.classList.remove('active'));
        event.target.classList.add('active');
      }

      // Start loops
      setInterval(fetchMetrics, 1000);
      setInterval(fetchMetadata, 5000);
      setInterval(fetchTraces, 2000);
      fetchMetrics(); fetchMetadata(); fetchTraces();

      // SSE for Logs
      const evtSource = new EventSource("/logs");
      evtSource.onmessage = (e) => appendLog(JSON.parse(e.data));
    </script>
  </body>
"#;
