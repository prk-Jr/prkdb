use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, warn};

/// A source produces items to be forwarded in a pipeline.
#[async_trait]
pub trait Source<T>: Send + Sync + 'static {
    async fn fetch_batch(&self, max_items: usize) -> Result<Vec<T>, String>;
}

/// A sink consumes items from a pipeline.
#[async_trait]
pub trait Sink<T>: Send + Sync + 'static {
    async fn consume_batch(&self, items: &[T]) -> Result<(), String>;
}

/// Basic pipeline metrics.
#[derive(Default, Debug, Clone)]
pub struct PipelineMetrics {
    pub delivered: u64,
    pub failed: u64,
    pub retries: u64,
}

#[derive(Debug, Clone)]
pub struct PipelineTelemetry {
    pub name: String,
    pub source: String,
    pub sink: String,
    pub delivered: u64,
    pub failed: u64,
    pub retries: u64,
}

#[derive(Debug, Default)]
pub struct PipelineCounters {
    delivered: AtomicU64,
    failed: AtomicU64,
    retries: AtomicU64,
}

#[derive(Debug, Clone)]
struct PipelineEntry {
    source: String,
    sink: String,
    counters: Arc<PipelineCounters>,
}

fn registry() -> &'static DashMap<String, PipelineEntry> {
    static REGISTRY: OnceLock<DashMap<String, PipelineEntry>> = OnceLock::new();
    REGISTRY.get_or_init(DashMap::new)
}

/// Register a pipeline by name so dashboards can pick up live telemetry.
pub fn register_pipeline(
    name: impl Into<String>,
    source: impl Into<String>,
    sink: impl Into<String>,
) -> Arc<PipelineCounters> {
    let name = name.into();
    let entry = PipelineEntry {
        source: source.into(),
        sink: sink.into(),
        counters: Arc::new(PipelineCounters::default()),
    };
    let counters = entry.counters.clone();
    registry().insert(name, entry);
    counters
}

/// Return a snapshot of all registered pipelines and their counters.
pub fn telemetry_snapshots() -> Vec<PipelineTelemetry> {
    registry()
        .iter()
        .map(|entry| PipelineTelemetry {
            name: entry.key().clone(),
            source: entry.value().source.clone(),
            sink: entry.value().sink.clone(),
            delivered: entry.value().counters.delivered.load(Ordering::Relaxed),
            failed: entry.value().counters.failed.load(Ordering::Relaxed),
            retries: entry.value().counters.retries.load(Ordering::Relaxed),
        })
        .collect()
}

/// Pipeline runner that pulls from a Source and pushes into a Sink.
pub struct Pipeline<S, D, T>
where
    S: Source<T>,
    D: Sink<T>,
{
    pub source: S,
    pub sink: D,
    pub batch_size: usize,
    pub retry_backoff: Duration,
    pub max_retries: usize,
    _marker: std::marker::PhantomData<T>,
    name: Option<String>,
    source_label: Option<String>,
    sink_label: Option<String>,
    counters: Option<Arc<PipelineCounters>>,
}

impl<S, D, T> Pipeline<S, D, T>
where
    S: Source<T>,
    D: Sink<T>,
{
    pub fn new(source: S, sink: D) -> Self {
        Self {
            source,
            sink,
            batch_size: 100,
            retry_backoff: Duration::from_millis(50),
            max_retries: 3,
            _marker: std::marker::PhantomData,
            name: None,
            source_label: None,
            sink_label: None,
            counters: None,
        }
    }

    /// Instrument this pipeline with a name/source/sink label and register telemetry.
    pub fn instrument(
        mut self,
        name: impl Into<String>,
        source: impl Into<String>,
        sink: impl Into<String>,
    ) -> Self {
        let source_label = source.into();
        let sink_label = sink.into();
        let name_val = name.into();
        let counters =
            register_pipeline(name_val.clone(), source_label.clone(), sink_label.clone());
        self.name = Some(name_val);
        self.source_label = Some(source_label);
        self.sink_label = Some(sink_label);
        self.counters = Some(counters);
        self
    }

    /// Run a single iteration: fetch a batch and deliver to sink with retries.
    pub async fn tick(&self, metrics: &mut PipelineMetrics) -> Result<(), String> {
        let batch = self.source.fetch_batch(self.batch_size).await?;
        if batch.is_empty() {
            return Ok(());
        }
        debug!(batch_size = batch.len(), "fetched batch");

        let mut attempt = 0;
        loop {
            match self.sink.consume_batch(&batch).await {
                Ok(()) => {
                    metrics.delivered += batch.len() as u64;
                    if let Some(c) = &self.counters {
                        c.delivered.fetch_add(batch.len() as u64, Ordering::Relaxed);
                    }
                    if attempt > 0 {
                        debug!(
                            retries = attempt,
                            delivered = metrics.delivered,
                            "delivered after retry"
                        );
                    }
                    return Ok(());
                }
                Err(e) => {
                    metrics.failed += batch.len() as u64;
                    if let Some(c) = &self.counters {
                        c.failed.fetch_add(batch.len() as u64, Ordering::Relaxed);
                    }
                    if attempt >= self.max_retries {
                        warn!(
                            attempts = attempt + 1,
                            delivered = metrics.delivered,
                            failed = metrics.failed,
                            "sink failed after retries"
                        );
                        return Err(format!("sink failed after retries: {e}"));
                    }
                    metrics.retries += 1;
                    if let Some(c) = &self.counters {
                        c.retries.fetch_add(1, Ordering::Relaxed);
                    }
                    attempt += 1;
                    warn!(attempt, failed = metrics.failed, "sink failed, backing off");
                    sleep(self.retry_backoff).await;
                }
            }
        }
    }
}
