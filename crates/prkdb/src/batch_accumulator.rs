//! The async batching window behind `CollectionHandle::with_batching` (STO-07).
//!
//! Producers send items down one FIFO channel to a single worker task, which groups them
//! into batches (by `linger_ms` or `max_batch_size`) and runs the executor on each batch.
//! `flush` sends a marker down the same channel, so the worker answers it only after every
//! item sent before it has been executed: it is a sequence barrier, and it returns the first
//! executor error since the previous flush.
//!
//! Memory is bounded by bytes, not items: each item holds `item_size` permits of a
//! `max_buffer_bytes` semaphore while it waits in the channel, and the worker releases them
//! as it moves the item into its batch. The batch being executed is bounded by
//! `max_batch_size`, so the permits bound the memory that could otherwise grow without limit,
//! and a buffer smaller than one batch cannot deadlock.

use prkdb_core::batch_config::BatchConfig;
use prkdb_types::collection::Collection;
use prkdb_types::error::StorageError;
use serde::Serialize;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tokio::time::{sleep_until, Duration, Instant};

/// Result type for batch operations
type BatchResult<T> = Result<T, StorageError>;

enum Msg<C> {
    Item(C, OwnedSemaphorePermit),
    /// Answered after every item sent before it has been executed; carries the first
    /// executor error since the previous flush (then clears it).
    Flush(oneshot::Sender<BatchResult<()>>),
}

/// The bincode-encoded size of `item`, counted without allocating the encoding. `add_put`
/// charges this many bytes of the buffer budget per item.
pub(crate) fn item_size<T: Serialize>(item: &T) -> usize {
    struct Counter(usize);
    impl bincode::enc::write::Writer for Counter {
        fn write(&mut self, bytes: &[u8]) -> Result<(), bincode::error::EncodeError> {
            self.0 += bytes.len();
            Ok(())
        }
    }
    let mut counter = Counter(0);
    // An item that cannot be encoded will fail in the executor, where the error is reported;
    // here it is charged what was counted before the failure.
    let _ = bincode::serde::encode_into_writer(item, &mut counter, bincode::config::standard());
    counter.0
}

/// Accumulates operations and executes them in batches.
///
/// A batch is executed when `linger_ms` has elapsed since its first item, when it holds
/// `max_batch_size` items, or when `flush` is called.
pub struct BatchAccumulator<C: Collection> {
    tx: mpsc::UnboundedSender<Msg<C>>,
    budget: Arc<Semaphore>,
    /// Permits in `budget`: the most one item may be charged.
    max_permits: usize,
}

impl<C: Collection> BatchAccumulator<C> {
    /// Create an accumulator and spawn its worker; must be called inside a Tokio runtime.
    pub fn new<F, Fut>(config: BatchConfig, executor: F) -> Self
    where
        F: Fn(Vec<C>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = BatchResult<()>> + Send + 'static,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        // `acquire_many_owned` takes a u32 and a zero budget would admit nothing.
        let max_permits = config.max_buffer_bytes.clamp(1, u32::MAX as usize);
        let worker = Worker {
            executor,
            linger: Duration::from_millis(config.linger_ms),
            max_batch_size: config.max_batch_size.max(1),
            batch: Vec::new(),
            first_error: None,
        };
        tokio::spawn(worker.run(rx));
        Self {
            tx,
            budget: Arc::new(Semaphore::new(max_permits)),
            max_permits,
        }
    }

    /// Buffer a PUT. Returns once the item is queued, not once it is written; waits while
    /// the queued items already hold `max_buffer_bytes`. Write failures surface in `flush`.
    pub async fn add_put(&self, item: C) -> BatchResult<()> {
        let permits = item_size(&item).min(self.max_permits) as u32;
        // Fast path while the buffer has room; otherwise wait for the worker to drain it.
        let permit = match Arc::clone(&self.budget).try_acquire_many_owned(permits) {
            Ok(permit) => permit,
            Err(_) => Arc::clone(&self.budget)
                .acquire_many_owned(permits)
                .await
                .map_err(|_| StorageError::Internal("batch accumulator buffer closed".into()))?,
        };
        self.tx
            .send(Msg::Item(item, permit))
            .map_err(|_| StorageError::Internal("batch accumulator worker stopped".into()))
    }

    /// Execute every PUT buffered before this call and wait for it. Returns the first
    /// executor error since the previous flush.
    pub async fn flush(&self) -> BatchResult<()> {
        let stopped = || StorageError::Internal("batch accumulator worker stopped".into());
        let (answer, answered) = oneshot::channel();
        self.tx.send(Msg::Flush(answer)).map_err(|_| stopped())?;
        answered.await.map_err(|_| stopped())?
    }
}

struct Worker<C, F> {
    executor: F,
    linger: Duration,
    max_batch_size: usize,
    batch: Vec<C>,
    first_error: Option<StorageError>,
}

impl<C, F, Fut> Worker<C, F>
where
    C: Send + 'static,
    F: Fn(Vec<C>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = BatchResult<()>> + Send + 'static,
{
    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Msg<C>>) {
        // When the current batch must be executed even if no more messages arrive.
        let mut deadline = Instant::now();
        loop {
            let msg = if self.batch.is_empty() {
                rx.recv().await
            } else {
                tokio::select! {
                    biased;
                    msg = rx.recv() => msg,
                    _ = sleep_until(deadline) => {
                        self.execute().await;
                        continue;
                    }
                }
            };
            match msg {
                Some(Msg::Item(item, permit)) => {
                    // The item leaves the channel: its bytes no longer count against the budget.
                    drop(permit);
                    if self.batch.is_empty() {
                        deadline = Instant::now() + self.linger;
                    }
                    self.batch.push(item);
                    // A steady stream keeps `recv` ready, so the deadline is checked here too.
                    if self.batch.len() >= self.max_batch_size || Instant::now() >= deadline {
                        self.execute().await;
                    }
                }
                Some(Msg::Flush(answer)) => {
                    self.execute().await;
                    let result = self.first_error.take().map_or(Ok(()), Err);
                    // A flush caller that gave up waiting has nothing to receive the answer.
                    let _ = answer.send(result);
                }
                None => {
                    // Every handle is gone: execute what is left. Nobody can receive an error
                    // any more, so it is logged.
                    self.execute().await;
                    if let Some(err) = self.first_error.take() {
                        tracing::error!(error = %err, "batched writes failed after the last flush");
                    }
                    return;
                }
            }
        }
    }

    async fn execute(&mut self) {
        if self.batch.is_empty() {
            return;
        }
        let batch = std::mem::take(&mut self.batch);
        if let Err(err) = (self.executor)(batch).await {
            match self.first_error {
                None => self.first_error = Some(err),
                // The next flush reports the first error only; later ones are logged so they
                // are not lost.
                Some(_) => tracing::error!(error = %err, "batched write failed"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prkdb_core::batch_config::BatchConfig;
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestItem {
        id: String,
        value: i32,
    }

    impl prkdb_types::collection::Collection for TestItem {
        type Id = String;
        fn id(&self) -> &Self::Id {
            &self.id
        }
    }

    #[tokio::test]
    async fn accumulator_executes_batch() {
        let executed_count = Arc::new(AtomicUsize::new(0));
        let count_clone = Arc::clone(&executed_count);

        let config = BatchConfig {
            linger_ms: 10,
            max_batch_size: 3,
            max_buffer_bytes: 1024 * 1024,
            compression: Default::default(),
        };

        let accumulator = Arc::new(BatchAccumulator::new(
            config,
            move |items: Vec<TestItem>| {
                let count = Arc::clone(&count_clone);
                async move {
                    count.store(items.len(), Ordering::SeqCst);
                    Ok(())
                }
            },
        ));

        // Add 3 items - should trigger immediate flush
        for i in 0..3 {
            let acc = Arc::clone(&accumulator);
            let item = TestItem {
                id: format!("test{}", i),
                value: i,
            };
            acc.add_put(item).await.unwrap();
        }

        // Give flush a moment to execute
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Verify batch was executed with 3 items
        assert_eq!(executed_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn flush_stays_pending_while_the_executor_is_blocked() {
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let (s, r) = (started.clone(), release.clone());
        let acc = BatchAccumulator::new(
            BatchConfig {
                linger_ms: 1,
                max_batch_size: 1,
                ..Default::default()
            },
            move |_: Vec<TestItem>| {
                let (s, r) = (s.clone(), r.clone());
                async move {
                    s.notify_one();
                    r.notified().await;
                    Ok(())
                }
            },
        );
        acc.add_put(TestItem {
            id: "a".into(),
            value: 1,
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .unwrap();
        let flush = acc.flush();
        tokio::pin!(flush);
        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut flush)
                .await
                .is_err(),
            "flush returned while the executor was still running"
        );
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), flush)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn flush_returns_the_executor_error() {
        let acc = BatchAccumulator::new(
            BatchConfig {
                linger_ms: 1,
                max_batch_size: 10,
                ..Default::default()
            },
            |_: Vec<TestItem>| async { Err(StorageError::Internal("disk on fire".into())) },
        );
        acc.add_put(TestItem {
            id: "a".into(),
            value: 1,
        })
        .await
        .unwrap();
        let err = acc
            .flush()
            .await
            .expect_err("an executor error must reach flush");
        assert!(err.to_string().contains("disk on fire"), "{err}");
        acc.flush()
            .await
            .expect("the error is reported once, then cleared");
    }

    /// Bytes waiting in the channel are bounded; the item being executed no longer counts.
    /// Every wait is under a timeout, so a broken bound fails the test instead of hanging it.
    #[tokio::test]
    async fn admission_is_bounded_by_bytes() {
        const T: Duration = Duration::from_secs(5);
        let started = Arc::new(tokio::sync::Semaphore::new(0));
        // One permit per batch the executor may finish.
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let (st, g) = (started.clone(), gate.clone());
        let big = || TestItem {
            id: "x".repeat(100),
            value: 0,
        };
        let size = item_size(&big()); // the same counting-sink size add_put uses, ~102 bytes
        let acc = Arc::new(BatchAccumulator::new(
            // Room for exactly two queued items, not three.
            BatchConfig {
                linger_ms: 1,
                max_batch_size: 1,
                max_buffer_bytes: 2 * size + size / 2,
                ..Default::default()
            },
            move |_: Vec<TestItem>| {
                let (st, g) = (st.clone(), g.clone());
                async move {
                    st.add_permits(1);
                    g.acquire().await.unwrap().forget();
                    Ok(())
                }
            },
        ));
        tokio::time::timeout(T, acc.add_put(big()))
            .await
            .unwrap()
            .unwrap();
        // Wait until the worker has taken item 1 into its batch (its permit is released) and is
        // blocked executing it; only then is the channel empty, whatever the scheduling.
        tokio::time::timeout(T, started.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        tokio::time::timeout(T, acc.add_put(big()))
            .await
            .unwrap()
            .unwrap(); // queued: 1 x size
        tokio::time::timeout(T, acc.add_put(big()))
            .await
            .unwrap()
            .unwrap(); // queued: 2 x size
        let blocked = {
            let acc = acc.clone();
            tokio::spawn(async move { acc.add_put(big()).await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !blocked.is_finished(),
            "a third queued item must wait: the byte budget is used up"
        );
        gate.add_permits(16); // let every batch finish
        tokio::time::timeout(T, blocked)
            .await
            .expect("admission never resumed")
            .unwrap()
            .unwrap();
        tokio::time::timeout(T, acc.flush()).await.unwrap().unwrap();
    }
}
