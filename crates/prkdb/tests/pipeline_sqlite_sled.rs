use prkdb::pipeline::{Pipeline, PipelineMetrics, Sink, Source};
use prkdb::prelude::StorageAdapter;
use prkdb_storage_sled::SledAdapter;
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
struct SledSink {
    dest: SledAdapter,
    ack: SqliteAdapter,
}

impl SledSink {
    fn new(dest: SledAdapter, ack: SqliteAdapter) -> Self {
        Self { dest, ack }
    }
}

#[async_trait::async_trait]
impl Sink<OutboxEvent> for SledSink {
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
        Ok(())
    }
}

#[derive(Clone)]
struct SledKvSink {
    dest: SledAdapter,
}

impl SledKvSink {
    fn new(dest: SledAdapter) -> Self {
        Self { dest }
    }
}

#[async_trait::async_trait]
impl Sink<KvEntry> for SledKvSink {
    async fn consume_batch(&self, items: &[KvEntry]) -> Result<(), String> {
        for kv in items {
            self.dest
                .put(&kv.key, &kv.value)
                .await
                .map_err(|e| format!("put: {e}"))?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn pipeline_sqlite_to_sled_outbox_and_kv() {
    let sqlite = SqliteAdapter::connect("sqlite::memory:").await.unwrap();
    sqlite.outbox_save("o1", b"alpha").await.unwrap();
    sqlite.outbox_save("o2", b"beta").await.unwrap();
    sqlite.put(b"k1", b"v1").await.unwrap();
    sqlite.put(b"k2", b"v2").await.unwrap();

    let dir: TempDir = tempfile::tempdir().unwrap();
    let sled = SledAdapter::open(dir.path()).unwrap();

    let out_source = SqliteOutboxSource::new(sqlite.clone());
    let out_sink = SledSink::new(sled.clone(), sqlite.clone());
    let mut out_pipeline = Pipeline::new(out_source, out_sink);
    out_pipeline.batch_size = 4;
    out_pipeline.retry_backoff = Duration::from_millis(1);

    let mut out_metrics = PipelineMetrics::default();
    while out_metrics.delivered < 2 {
        out_pipeline.tick(&mut out_metrics).await.unwrap();
    }

    let kv_source = SqliteKvSource::load(&sqlite).await.unwrap();
    let kv_sink = SledKvSink::new(sled.clone());
    let mut kv_pipeline = Pipeline::new(kv_source, kv_sink);
    kv_pipeline.batch_size = 4;
    kv_pipeline.retry_backoff = Duration::from_millis(1);

    let mut kv_metrics = PipelineMetrics::default();
    while kv_metrics.delivered < 2 {
        kv_pipeline.tick(&mut kv_metrics).await.unwrap();
    }

    assert_eq!(sled.get(b"o1").await.unwrap().unwrap(), b"alpha");
    assert_eq!(sled.get(b"o2").await.unwrap().unwrap(), b"beta");
    assert_eq!(sled.get(b"k1").await.unwrap().unwrap(), b"v1");
    assert_eq!(sled.get(b"k2").await.unwrap().unwrap(), b"v2");
}
