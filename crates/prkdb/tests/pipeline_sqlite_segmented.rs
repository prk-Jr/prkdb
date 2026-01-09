use prkdb::pipeline::{Pipeline, PipelineMetrics, Sink, Source};
use prkdb::prelude::StorageAdapter;
use prkdb_storage_segmented::SegmentedLogAdapter;
use prkdb_storage_sql::SqliteAdapter;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

#[derive(Clone, Debug)]
struct OutboxEvent {
    id: String,
    payload: Vec<u8>,
}

#[derive(Clone, Debug)]
struct KvEntry {
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Clone)]
struct SqliteOutboxSource {
    adapter: SqliteAdapter,
}

impl SqliteOutboxSource {
    fn new(adapter: SqliteAdapter) -> Self {
        Self { adapter }
    }

    async fn pending(&self) -> usize {
        self.adapter
            .outbox_list()
            .await
            .map(|v| v.len())
            .unwrap_or(0)
    }
}

#[async_trait::async_trait]
impl Source<OutboxEvent> for SqliteOutboxSource {
    async fn fetch_batch(&self, max_items: usize) -> Result<Vec<OutboxEvent>, String> {
        let mut rows = self
            .adapter
            .outbox_list()
            .await
            .map_err(|e| format!("outbox_list: {e}"))?;
        rows.truncate(max_items);
        Ok(rows
            .into_iter()
            .map(|(id, payload)| OutboxEvent { id, payload })
            .collect())
    }
}

#[derive(Clone)]
struct SqliteKvSource {
    cache: Arc<tokio::sync::Mutex<Vec<KvEntry>>>,
}

impl SqliteKvSource {
    async fn load(adapter: &SqliteAdapter) -> Result<Self, String> {
        let rows = adapter
            .scan_prefix(&[])
            .await
            .map_err(|e| format!("scan_prefix: {e}"))?;
        let cache = rows
            .into_iter()
            .map(|(k, v)| KvEntry { key: k, value: v })
            .collect();
        Ok(Self {
            cache: Arc::new(tokio::sync::Mutex::new(cache)),
        })
    }
}

#[async_trait::async_trait]
impl Source<KvEntry> for SqliteKvSource {
    async fn fetch_batch(&self, max_items: usize) -> Result<Vec<KvEntry>, String> {
        let mut guard = self.cache.lock().await;
        if guard.is_empty() {
            return Ok(Vec::new());
        }
        let len = guard.len();
        let split_at = len.saturating_sub(max_items);
        let batch = guard.split_off(split_at);
        Ok(batch)
    }
}

#[derive(Clone)]
struct SegmentedSink {
    dest: Arc<SegmentedLogAdapter>,
    ack: SqliteAdapter,
}

impl SegmentedSink {
    fn new(dest: Arc<SegmentedLogAdapter>, ack: SqliteAdapter) -> Self {
        Self { dest, ack }
    }
}

#[async_trait::async_trait]
impl Sink<OutboxEvent> for SegmentedSink {
    async fn consume_batch(&self, items: &[OutboxEvent]) -> Result<(), String> {
        for evt in items {
            self.dest
                .put(evt.id.as_bytes(), &evt.payload)
                .await
                .map_err(|e| format!("put: {e}"))?;
            self.ack
                .outbox_remove(&evt.id)
                .await
                .map_err(|e| format!("outbox_remove: {e}"))?;
        }
        self.dest.flush().await.map_err(|e| format!("flush: {e}"))?;
        Ok(())
    }
}

#[derive(Clone)]
struct SegmentedKvSink {
    dest: Arc<SegmentedLogAdapter>,
}

impl SegmentedKvSink {
    fn new(dest: Arc<SegmentedLogAdapter>) -> Self {
        Self { dest }
    }
}

#[async_trait::async_trait]
impl Sink<KvEntry> for SegmentedKvSink {
    async fn consume_batch(&self, items: &[KvEntry]) -> Result<(), String> {
        for kv in items {
            self.dest
                .put(&kv.key, &kv.value)
                .await
                .map_err(|e| format!("put: {e}"))?;
        }
        self.dest.flush().await.map_err(|e| format!("flush: {e}"))?;
        Ok(())
    }
}

#[tokio::test]
async fn pipeline_moves_outbox_to_segmented() {
    let sqlite = SqliteAdapter::connect("sqlite::memory:").await.unwrap();
    sqlite.outbox_save("o1", b"alpha").await.unwrap();
    sqlite.outbox_save("o2", b"beta").await.unwrap();
    sqlite.put(b"k1", b"v1").await.unwrap();
    sqlite.put(b"k2", b"v2").await.unwrap();

    let dir: TempDir = tempfile::tempdir().unwrap();
    let segmented = Arc::new(
        SegmentedLogAdapter::new(dir.path(), 1024 * 1024, Some(2), 10)
            .await
            .unwrap(),
    );

    let source = SqliteOutboxSource::new(sqlite.clone());
    let sink = SegmentedSink::new(segmented.clone(), sqlite.clone());

    let mut pipeline = Pipeline::new(source.clone(), sink);
    pipeline.batch_size = 10;
    pipeline.retry_backoff = Duration::from_millis(1);

    let mut metrics = PipelineMetrics::default();
    while source.pending().await > 0 {
        pipeline.tick(&mut metrics).await.unwrap();
    }

    assert_eq!(metrics.delivered, 2);
    assert_eq!(source.pending().await, 0);
    assert_eq!(segmented.get(b"o1").await.unwrap().unwrap(), b"alpha");
    assert_eq!(segmented.get(b"o2").await.unwrap().unwrap(), b"beta");

    // Run a second pipeline for KV rows and ensure they land in segmented as well.
    let kv_source = SqliteKvSource::load(&sqlite).await.unwrap();
    let kv_sink = SegmentedKvSink::new(segmented.clone());
    let mut kv_pipeline = Pipeline::new(kv_source, kv_sink);
    kv_pipeline.batch_size = 4;
    kv_pipeline.retry_backoff = Duration::from_millis(1);

    let mut kv_metrics = PipelineMetrics::default();
    kv_pipeline.tick(&mut kv_metrics).await.unwrap();
    assert_eq!(segmented.get(b"k1").await.unwrap().unwrap(), b"v1");
    assert_eq!(segmented.get(b"k2").await.unwrap().unwrap(), b"v2");
}
