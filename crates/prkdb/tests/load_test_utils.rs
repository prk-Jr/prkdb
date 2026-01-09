use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use prkdb_types::storage::StorageAdapter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Tracks latency percentiles for performance analysis
pub struct LatencyTracker {
    samples: Arc<Mutex<Vec<Duration>>>,
}

impl LatencyTracker {
    pub fn new() -> Self {
        Self {
            samples: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn record(&self, latency: Duration) {
        self.samples.lock().await.push(latency);
    }

    pub async fn percentile(&self, p: f64) -> Duration {
        let mut samples = self.samples.lock().await;
        if samples.is_empty() {
            return Duration::from_secs(0);
        }

        samples.sort();
        let index = ((samples.len() as f64) * p / 100.0) as usize;
        samples[index.min(samples.len() - 1)]
    }

    #[allow(dead_code)]
    pub async fn count(&self) -> usize {
        self.samples.lock().await.len()
    }

    #[allow(dead_code)]
    pub async fn clear(&self) {
        self.samples.lock().await.clear();
    }
}

/// Collects and aggregates performance metrics
pub struct MetricsCollector {
    pub total_ops: AtomicU64,
    pub successful_ops: AtomicU64,
    pub failed_ops: AtomicU64,
    pub latency_tracker: LatencyTracker,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            total_ops: AtomicU64::new(0),
            successful_ops: AtomicU64::new(0),
            failed_ops: AtomicU64::new(0),
            latency_tracker: LatencyTracker::new(),
        }
    }

    pub fn record_success(&self, _latency: Duration) {
        self.total_ops.fetch_add(1, Ordering::Relaxed);
        self.successful_ops.fetch_add(1, Ordering::Relaxed);
        // We'll record latency afterwards to avoid blocking
    }

    pub fn record_failure(&self) {
        self.total_ops.fetch_add(1, Ordering::Relaxed);
        self.failed_ops.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_total_ops(&self) -> u64 {
        self.total_ops.load(Ordering::Relaxed)
    }

    pub fn get_successful_ops(&self) -> u64 {
        self.successful_ops.load(Ordering::Relaxed)
    }

    pub fn get_failed_ops(&self) -> u64 {
        self.failed_ops.load(Ordering::Relaxed)
    }

    pub async fn print_summary(&self, duration: Duration) {
        let total = self.get_total_ops();
        let successful = self.get_successful_ops();
        let failed = self.get_failed_ops();
        let ops_per_sec = total as f64 / duration.as_secs_f64();

        let p50 = self.latency_tracker.percentile(50.0).await;
        let p95 = self.latency_tracker.percentile(95.0).await;
        let p99 = self.latency_tracker.percentile(99.0).await;

        println!("\n=== Performance Summary ===");
        println!("Duration: {:.2}s", duration.as_secs_f64());
        println!("Total operations: {}", total);
        println!("Successful: {}", successful);
        println!("Failed: {}", failed);
        println!("Throughput: {:.2} ops/sec", ops_per_sec);
        println!("Latency p50: {:?}", p50);
        println!("Latency p95: {:?}", p95);
        println!("Latency p99: {:?}", p99);
        println!("==========================\n");
    }
}

/// Generates realistic workload data
pub struct WorkloadGenerator {
    counter: AtomicU64,
}

impl WorkloadGenerator {
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(0),
        }
    }

    pub fn next_key(&self) -> Vec<u8> {
        let id = self.counter.fetch_add(1, Ordering::Relaxed);
        format!("key_{}", id).into_bytes()
    }

    pub fn next_value(&self, size: usize) -> Vec<u8> {
        vec![b'x'; size]
    }

    pub fn key_for_id(&self, id: u64) -> Vec<u8> {
        format!("key_{}", id).into_bytes()
    }
}

/// Harness for setting up and running load tests
pub struct LoadTestHarness {
    pub adapter: WalStorageAdapter,
    pub workload_gen: Arc<WorkloadGenerator>,
    pub metrics: Arc<MetricsCollector>,
}

impl LoadTestHarness {
    pub async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        let adapter = WalStorageAdapter::new(config).unwrap();

        Self {
            adapter,
            workload_gen: Arc::new(WorkloadGenerator::new()),
            metrics: Arc::new(MetricsCollector::new()),
        }
    }

    pub async fn run_concurrent_writes(&self, num_ops: usize, concurrency: usize) -> Duration {
        let start = Instant::now();
        let sem = Arc::new(tokio::sync::Semaphore::new(concurrency));

        let mut tasks = vec![];
        for _ in 0..num_ops {
            let adapter = self.adapter.clone();
            let workload_gen = self.workload_gen.clone();
            let metrics = self.metrics.clone();
            let sem = sem.clone();

            let task = tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                let key = workload_gen.next_key();
                let value = workload_gen.next_value(100);

                let op_start = Instant::now();
                match adapter.put(&key, &value).await {
                    Ok(_) => {
                        let latency = op_start.elapsed();
                        metrics.record_success(latency);
                        metrics.latency_tracker.record(latency).await;
                    }
                    Err(_) => metrics.record_failure(),
                }
            });
            tasks.push(task);
        }

        for task in tasks {
            task.await.unwrap();
        }

        start.elapsed()
    }

    pub async fn run_concurrent_reads(&self, num_ops: usize, concurrency: usize) -> Duration {
        // First, populate some data
        for i in 0..num_ops.min(1000) {
            let key = self.workload_gen.key_for_id(i as u64);
            let value = self.workload_gen.next_value(100);
            self.adapter.put(&key, &value).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        let start = Instant::now();
        let sem = Arc::new(tokio::sync::Semaphore::new(concurrency));

        let mut tasks = vec![];
        for i in 0..num_ops {
            let adapter = self.adapter.clone();
            let workload_gen = self.workload_gen.clone();
            let metrics = self.metrics.clone();
            let sem = sem.clone();
            let key_id = (i % 1000) as u64;

            let task = tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                let key = workload_gen.key_for_id(key_id);

                let op_start = Instant::now();
                match adapter.get(&key).await {
                    Ok(_) => {
                        let latency = op_start.elapsed();
                        metrics.record_success(latency);
                        metrics.latency_tracker.record(latency).await;
                    }
                    Err(_) => metrics.record_failure(),
                }
            });
            tasks.push(task);
        }

        for task in tasks {
            task.await.unwrap();
        }

        start.elapsed()
    }

    pub async fn run_mixed_workload(
        &self,
        num_ops: usize,
        read_ratio: f64,
        concurrency: usize,
    ) -> Duration {
        // Populate initial data
        for i in 0..1000 {
            let key = self.workload_gen.key_for_id(i);
            let value = self.workload_gen.next_value(100);
            self.adapter.put(&key, &value).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        let start = Instant::now();
        let sem = Arc::new(tokio::sync::Semaphore::new(concurrency));

        let mut tasks = vec![];
        for i in 0..num_ops {
            let adapter = self.adapter.clone();
            let workload_gen = self.workload_gen.clone();
            let metrics = self.metrics.clone();
            let sem = sem.clone();

            let is_read = (i as f64 / num_ops as f64) < read_ratio;

            let task = tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();

                let op_start = Instant::now();
                let result = if is_read {
                    let key = workload_gen.key_for_id((i % 1000) as u64);
                    adapter.get(&key).await.map(|_| ())
                } else {
                    let key = workload_gen.next_key();
                    let value = workload_gen.next_value(100);
                    adapter.put(&key, &value).await
                };

                match result {
                    Ok(_) => {
                        let latency = op_start.elapsed();
                        metrics.record_success(latency);
                        metrics.latency_tracker.record(latency).await;
                    }
                    Err(_) => metrics.record_failure(),
                }
            });
            tasks.push(task);
        }

        for task in tasks {
            task.await.unwrap();
        }

        start.elapsed()
    }
}
