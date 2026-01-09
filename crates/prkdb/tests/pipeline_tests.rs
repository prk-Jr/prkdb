use prkdb::pipeline::{Pipeline, PipelineMetrics, Sink, Source};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Clone)]
struct InMemorySource {
    inner: Arc<Mutex<VecDeque<String>>>,
}

impl InMemorySource {
    fn new(items: Vec<String>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(items.into())),
        }
    }

    async fn is_empty(&self) -> bool {
        self.inner.lock().await.is_empty()
    }
}

#[async_trait::async_trait]
impl Source<String> for InMemorySource {
    async fn fetch_batch(&self, max_items: usize) -> Result<Vec<String>, String> {
        let mut guard = self.inner.lock().await;
        let mut batch = Vec::new();
        for _ in 0..max_items {
            if let Some(item) = guard.pop_front() {
                batch.push(item);
            } else {
                break;
            }
        }
        Ok(batch)
    }
}

#[derive(Clone, Default)]
struct CollectSink {
    inner: Arc<Mutex<Vec<String>>>,
}

impl CollectSink {
    async fn items(&self) -> Vec<String> {
        self.inner.lock().await.clone()
    }
}

#[async_trait::async_trait]
impl Sink<String> for CollectSink {
    async fn consume_batch(&self, items: &[String]) -> Result<(), String> {
        let mut guard = self.inner.lock().await;
        guard.extend(items.iter().cloned());
        Ok(())
    }
}

#[derive(Clone)]
struct FlakySink {
    inner: CollectSink,
    fail_once: Arc<Mutex<bool>>,
}

impl FlakySink {
    fn new() -> Self {
        Self {
            inner: CollectSink::default(),
            fail_once: Arc::new(Mutex::new(true)),
        }
    }

    async fn items(&self) -> Vec<String> {
        self.inner.items().await
    }
}

#[async_trait::async_trait]
impl Sink<String> for FlakySink {
    async fn consume_batch(&self, items: &[String]) -> Result<(), String> {
        let mut first = self.fail_once.lock().await;
        if *first {
            *first = false;
            return Err("synthetic failure".into());
        }
        self.inner.consume_batch(items).await
    }
}

#[tokio::test]
async fn pipeline_successfully_drains_in_memory_source() {
    let source = InMemorySource::new(vec![
        "a".to_string(),
        "b".to_string(),
        "c".to_string(),
        "d".to_string(),
    ]);
    let sink = CollectSink::default();
    let mut pipeline = Pipeline::new(source.clone(), sink.clone());
    pipeline.batch_size = 2;
    pipeline.max_retries = 1;
    pipeline.retry_backoff = Duration::from_millis(1);

    let mut metrics = PipelineMetrics::default();
    while !source.is_empty().await {
        pipeline.tick(&mut metrics).await.unwrap();
    }

    let items = sink.items().await;
    assert_eq!(items, vec!["a", "b", "c", "d"]);
    assert_eq!(metrics.delivered, 4);
    assert_eq!(metrics.failed, 0);
    assert_eq!(metrics.retries, 0);
}

#[tokio::test]
async fn pipeline_retries_then_succeeds() {
    let source = InMemorySource::new(vec!["x".to_string(), "y".to_string()]);
    let sink = FlakySink::new();
    let mut pipeline = Pipeline::new(source.clone(), sink.clone());
    pipeline.batch_size = 5;
    pipeline.max_retries = 2;
    pipeline.retry_backoff = Duration::from_millis(1);

    let mut metrics = PipelineMetrics::default();
    pipeline.tick(&mut metrics).await.unwrap();

    let items = sink.items().await;
    assert_eq!(items, vec!["x", "y"]);
    assert_eq!(metrics.delivered, 2);
    assert_eq!(metrics.retries, 1);
    assert_eq!(metrics.failed, 2);
}
