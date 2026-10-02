//! The async batching window behind `CollectionHandle::with_batching` (STO-07).
//!
//! Producers send items down one FIFO channel to a single worker task, which groups them
//! into batches (by `linger_ms` or `max_batch_size`) and runs the executor on each batch.
//! `flush` sends a marker down the same channel, so the worker answers it only after every
//! item sent before it has been executed: it is a sequence barrier.
//!
//! # Which failures a flush reports
//!
//! The worker numbers items in the order it receives them. A failed batch is remembered
//! with the position just past its last item. A flush reports a failure if the failed batch
//! holds an item after the *low mark* the flush read when it was sent: the position of the
//! last flush that had been answered by then. So every flush in flight over a failed batch
//! fails (several cloned handles flushing at once are all told), and a flush sent after an
//! answered one starts clean. A flush whose caller stopped waiting was not answered, so it
//! does not move the low mark and its failure stays for the next flush. A failure is
//! dropped once no outstanding or future flush can have a low mark before it.
//!
//! # Memory
//!
//! Memory is bounded by bytes, not items: each item holds `item_size` permits of a
//! `max_buffer_bytes` semaphore while it waits in the channel, and the worker releases them
//! as it moves the item into its batch. The batch being executed is bounded by
//! `max_batch_size`, so the permits bound the memory that could otherwise grow without limit,
//! and a buffer smaller than one batch cannot deadlock.
//!
//! # When the worker stops
//!
//! When the last handle is dropped the worker executes what is left and logs, with
//! `tracing::error!`, every failure no flush reported.
//!
//! The executor usually holds the database (`CollectionHandle::with_batching` gives it a
//! `PrkDb`), and the worker drops it only when it notices the closed channel, on a runtime
//! thread, some time later. So when nothing is left to execute (every item added has been
//! executed to completion, which a `flush` that returned guarantees for the items before
//! it), dropping the last handle takes the executor out of the worker and drops it right
//! there: the database it held is released before the drop returns (STO-13). Dropping the
//! last handle with items still unexecuted leaves the executor to the worker, which needs
//! it to write them; flush first for a prompt release. If the worker panics (the executor
//! panicked), later `add_put` and `flush` calls fail with "batch accumulator worker stopped"
//! and the panic message is not carried over. If the runtime shuts down before the worker
//! has drained the channel, the queued items are lost without a log line.

use prkdb_core::batch_config::BatchConfig;
use prkdb_types::collection::Collection;
use prkdb_types::error::StorageError;
use serde::Serialize;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tokio::time::{Duration, Instant};

/// Result type for batch operations
type BatchResult<T> = Result<T, StorageError>;

enum Msg<C> {
    Item(C, OwnedSemaphorePermit),
    /// Answered after every item sent before it has been executed, with the first failure
    /// among the items after `low` (see the module docs).
    Flush {
        low: u64,
        answer: oneshot::Sender<BatchResult<()>>,
    },
}

/// Flush bookkeeping shared by `flush` callers and the worker.
#[derive(Default)]
struct FlushState {
    /// Position covered by the last flush whose answer was delivered.
    answered: u64,
    /// Low marks of flushes sent but not yet answered, with their counts.
    outstanding: BTreeMap<u64, usize>,
    /// Failed batches as (position just past the batch, error), in position order.
    failures: VecDeque<(u64, StorageError)>,
}

#[derive(Default)]
struct Flushes(Mutex<FlushState>);

impl Flushes {
    fn lock(&self) -> MutexGuard<'_, FlushState> {
        // The state is a few counters updated without panicking code in between: a poisoned
        // lock still holds consistent values.
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Register a flush about to be sent; returns its low mark.
    fn register(&self) -> u64 {
        let mut state = self.lock();
        let low = state.answered;
        *state.outstanding.entry(low).or_default() += 1;
        low
    }

    fn unregister(state: &mut FlushState, low: u64) {
        if let Some(count) = state.outstanding.get_mut(&low) {
            *count -= 1;
            if *count == 0 {
                state.outstanding.remove(&low);
            }
        }
    }
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
    flushes: Arc<Flushes>,
    /// Items added and not yet executed to completion (the executor's future for their
    /// batch has finished and been dropped).
    unexecuted: Arc<AtomicU64>,
    /// Takes the executor out of the worker and drops it (see the module docs).
    release_executor: Box<dyn Fn() + Send + Sync>,
}

impl<C: Collection> Drop for BatchAccumulator<C> {
    fn drop(&mut self) {
        // The last handle: no item can be added after this. If every one added has been
        // executed, the worker will never call the executor again.
        if self.unexecuted.load(Ordering::Acquire) == 0 {
            (self.release_executor)();
        }
    }
}

fn stopped() -> StorageError {
    StorageError::Internal("batch accumulator worker stopped".into())
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
        let flushes = Arc::new(Flushes::default());
        let unexecuted = Arc::new(AtomicU64::new(0));
        let executor = Arc::new(parking_lot::Mutex::new(Some(executor)));
        let slot = Arc::clone(&executor);
        let release_executor = Box::new(move || {
            // User-owned executor captures may run arbitrary destructors. Release
            // the worker slot before dropping them.
            let executor = slot.lock().take();
            drop(executor);
        });
        let worker = Worker {
            executor,
            unexecuted: Arc::clone(&unexecuted),
            linger: Duration::from_millis(config.linger_ms),
            max_batch_size: config.max_batch_size.max(1),
            batch: Vec::new(),
            received: 0,
            flushes: Arc::clone(&flushes),
        };
        tokio::spawn(worker.run(rx));
        Self {
            tx,
            budget: Arc::new(Semaphore::new(max_permits)),
            max_permits,
            flushes,
            unexecuted,
            release_executor,
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
        // Counted before it is sent, so the worker never executes an uncounted item.
        self.unexecuted.fetch_add(1, Ordering::AcqRel);
        self.tx.send(Msg::Item(item, permit)).map_err(|_| {
            self.unexecuted.fetch_sub(1, Ordering::AcqRel);
            stopped()
        })
    }

    /// Execute every PUT buffered before this call and wait for it. Fails if a batch
    /// holding any item after the last flush answered before this call failed.
    pub async fn flush(&self) -> BatchResult<()> {
        let (answer, answered) = oneshot::channel();
        let low = self.flushes.register();
        if self.tx.send(Msg::Flush { low, answer }).is_err() {
            Flushes::unregister(&mut self.flushes.lock(), low);
            return Err(stopped());
        }
        answered.await.map_err(|_| stopped())?
    }

    /// Failed batches still kept for a flush that may report them.
    #[cfg(test)]
    fn retained_failures(&self) -> usize {
        self.flushes.lock().failures.len()
    }
}

struct Worker<C, F> {
    /// `None` once the last handle released it (nothing was left to execute).
    executor: Arc<parking_lot::Mutex<Option<F>>>,
    unexecuted: Arc<AtomicU64>,
    linger: Duration,
    max_batch_size: usize,
    batch: Vec<C>,
    /// Items received so far: the position just past the newest one.
    received: u64,
    flushes: Arc<Flushes>,
}

impl<C, F, Fut> Worker<C, F>
where
    C: Send + 'static,
    F: Fn(Vec<C>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = BatchResult<()>> + Send + 'static,
{
    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Msg<C>>) {
        let mut msgs = Vec::with_capacity(self.max_batch_size);
        // When the current batch must be executed even if no more messages arrive; reset
        // whenever a batch gets its first item.
        let linger = tokio::time::sleep(Duration::ZERO);
        tokio::pin!(linger);
        loop {
            let received = if self.batch.is_empty() {
                rx.recv_many(&mut msgs, self.max_batch_size).await
            } else {
                tokio::select! {
                    biased;
                    n = rx.recv_many(&mut msgs, self.max_batch_size) => n,
                    () = &mut linger => {
                        self.execute().await;
                        continue;
                    }
                }
            };
            if received == 0 {
                return self.shut_down().await;
            }
            // The permits of the items taken out of the channel, released together: before
            // each execute and at the end of the chunk.
            let mut taken: Option<OwnedSemaphorePermit> = None;
            for msg in msgs.drain(..) {
                match msg {
                    Msg::Item(item, permit) => {
                        match taken.as_mut() {
                            Some(taken) => taken.merge(permit),
                            None => taken = Some(permit),
                        }
                        if self.batch.is_empty() {
                            linger.as_mut().reset(Instant::now() + self.linger);
                        }
                        self.batch.push(item);
                        self.received += 1;
                        if self.batch.len() >= self.max_batch_size {
                            drop(taken.take());
                            self.execute().await;
                        }
                    }
                    Msg::Flush { low, answer } => {
                        drop(taken.take());
                        self.execute().await;
                        self.answer(low, answer);
                    }
                }
            }
            drop(taken);
            // A steady stream keeps `recv_many` ready, so the deadline is checked here too.
            if !self.batch.is_empty() && Instant::now() >= linger.deadline() {
                self.execute().await;
            }
        }
    }

    async fn execute(&mut self) {
        if self.batch.is_empty() {
            return;
        }
        let batch = std::mem::take(&mut self.batch);
        let items = batch.len() as u64;
        // The lock is held only to start the future, never across the await.
        let started = self
            .executor
            .lock()
            .as_ref()
            .map(|executor| executor(batch));
        let result = match started {
            Some(future) => future.await,
            // Unreachable: the executor is released only when no item is unexecuted.
            None => Err(stopped()),
        };
        // After the future (and whatever it held) is dropped.
        self.unexecuted.fetch_sub(items, Ordering::AcqRel);
        if let Err(err) = result {
            self.flushes.lock().failures.push_back((self.received, err));
        }
    }

    /// Answer a flush with the first failure after its low mark, then drop the failures
    /// no outstanding or future flush can report.
    fn answer(&mut self, low: u64, answer: oneshot::Sender<BatchResult<()>>) {
        let mut state = self.flushes.lock();
        let result = match state.failures.iter().find(|(end, _)| *end > low) {
            Some((_, err)) => Err(err.clone()),
            None => Ok(()),
        };
        // A caller that stopped waiting was not told: the low mark stays, so the next
        // flush reports the failure instead.
        if answer.send(result).is_ok() {
            state.answered = state.answered.max(self.received);
        }
        Flushes::unregister(&mut state, low);
        let floor = state
            .outstanding
            .keys()
            .next()
            .map_or(state.answered, |&oldest| oldest.min(state.answered));
        while state.failures.front().is_some_and(|(end, _)| *end <= floor) {
            state.failures.pop_front();
        }
    }

    /// Every handle is gone: execute what is left and log what no flush reported.
    async fn shut_down(&mut self) {
        self.execute().await;
        let state = self.flushes.lock();
        for (_, err) in state
            .failures
            .iter()
            .filter(|(end, _)| *end > state.answered)
        {
            tracing::error!(error = %err, "batched writes failed after the last flush");
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

    /// A flush whose caller stopped waiting was never answered, so its error stays for the
    /// next flush instead of being lost.
    #[tokio::test]
    async fn an_abandoned_flush_leaves_its_error_for_the_next_flush() {
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
                    Err(StorageError::BackendError("disk on fire".into()))
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
        assert!(
            tokio::time::timeout(Duration::from_millis(50), acc.flush())
                .await
                .is_err(),
            "the executor is blocked, so this flush must time out"
        );
        release.notify_one();
        let err = tokio::time::timeout(Duration::from_secs(2), acc.flush())
            .await
            .unwrap()
            .expect_err("the abandoned flush's error must reach the next flush");
        assert!(err.to_string().contains("disk on fire"), "{err}");
    }

    /// Two callers flush at once over a failed batch: both are told, not just the first.
    #[tokio::test]
    async fn every_concurrent_flush_over_a_failed_batch_fails() {
        let acc = BatchAccumulator::new(
            BatchConfig {
                linger_ms: 1,
                max_batch_size: 10,
                ..Default::default()
            },
            |_: Vec<TestItem>| async { Err(StorageError::BackendError("disk on fire".into())) },
        );
        acc.add_put(TestItem {
            id: "a".into(),
            value: 1,
        })
        .await
        .unwrap();
        let (a, b) = tokio::join!(acc.flush(), acc.flush());
        assert!(a.is_err() && b.is_err(), "{a:?} {b:?}");
        acc.flush()
            .await
            .expect("a flush sent after both were answered has nothing to report");
    }

    /// Failures are kept only while a flush that must report them may still be answered.
    #[tokio::test]
    async fn reported_failures_are_pruned() {
        let acc = BatchAccumulator::new(
            BatchConfig {
                linger_ms: 1,
                max_batch_size: 1,
                ..Default::default()
            },
            |_: Vec<TestItem>| async { Err(StorageError::BackendError("x".into())) },
        );
        for i in 0..100 {
            acc.add_put(TestItem {
                id: i.to_string(),
                value: i,
            })
            .await
            .unwrap();
            acc.flush().await.unwrap_err();
        }
        acc.flush().await.unwrap();
        assert_eq!(acc.retained_failures(), 0);
    }
}
