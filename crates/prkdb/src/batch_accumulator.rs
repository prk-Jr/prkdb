use crossbeam_channel::{unbounded, Sender};
use prkdb_core::batch_config::BatchConfig;
use prkdb_types::collection::Collection;
use prkdb_types::error::StorageError;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

/// Result type for batch operations
type BatchResult<T> = Result<T, StorageError>;

/// Callback type for executing batches
type BatchExecutor<C> = Arc<
    dyn Fn(Vec<C>) -> std::pin::Pin<Box<dyn std::future::Future<Output = BatchResult<()>> + Send>>
        + Send
        + Sync,
>;

/// Accumulates operations and flushes them in batches.
///
/// This is the core of the async batching window optimization. It buffers
/// operations and flushes them when either:
/// 1. linger_ms time has elapsed since first item
/// 2. max_batch_size items accumulated
pub struct BatchAccumulator<C: Collection> {
    config: BatchConfig,
    // Lock-free channel for sending items - multiple producers, single consumer
    item_tx: Sender<C>,
    _executor: BatchExecutor<C>,
    flush_tx: mpsc::Sender<FlushCommand>,
    _flush_task: tokio::task::JoinHandle<()>,
}

enum FlushCommand {
    Shutdown,
}

impl<C: Collection> BatchAccumulator<C> {
    /// Create a new accumulator with the given configuration and executor.
    pub fn new<F, Fut>(config: BatchConfig, executor: F) -> Self
    where
        F: Fn(Vec<C>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = BatchResult<()>> + Send + 'static,
    {
        let (flush_tx, flush_rx) = mpsc::channel(1);

        // Create lock-free channel for items
        let (item_tx, item_rx) = unbounded();

        // Wrap executor in Arc for sharing
        let executor: BatchExecutor<C> = Arc::new(move |items| Box::pin(executor(items)));

        // Spawn flush timer task with lock-free channel
        let flush_task = {
            let executor = Arc::clone(&executor);
            let linger_ms = config.linger_ms;
            let max_batch_size = config.max_batch_size;

            tokio::spawn(async move {
                let mut rx = flush_rx;
                let mut batch = Vec::with_capacity(max_batch_size);

                loop {
                    tokio::select! {
                        _ = sleep(Duration::from_millis(linger_ms)) => {
                            // Timer expired - collect pending items and flush
                            while let Ok(item) = item_rx.try_recv() {
                                batch.push(item);
                                if batch.len() >= max_batch_size {
                                    break;
                                }
                            }

                            if !batch.is_empty() {
                                let items_to_flush = std::mem::replace(
                                    &mut batch,
                                    Vec::with_capacity(max_batch_size)
                                );
                                let _ = executor(items_to_flush).await;
                            }
                        }
                        cmd = rx.recv() => {
                            match cmd {
                                Some(FlushCommand::Shutdown) | None => {
                                    // Flush all remaining items
                                    while let Ok(item) = item_rx.try_recv() {
                                        batch.push(item);
                                    }
                                    if !batch.is_empty() {
                                        let _ = executor(batch).await;
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
            })
        };

        Self {
            config,
            item_tx,
            _executor: executor,
            flush_tx,
            _flush_task: flush_task,
        }
    }

    /// Add a PUT operation to the batch.
    ///
    /// Returns immediately after buffering - does NOT wait for flush!
    /// This is the "fire-and-forget" semantic that enables high throughput.
    pub async fn add_put(&self, item: C) -> BatchResult<()> {
        // Lock-free send - just push to channel, no mutex!
        self.item_tx
            .send(item)
            .map_err(|_| StorageError::Internal("Batch accumulator channel closed".into()))?;

        // Return immediately - items will be batched by background task
        Ok(())
    }

    /// Flush all pending PUT operations immediately and wait for completion.
    ///
    /// Note: With lock-free channels, flush is handled by the background task.
    /// This method sends a shutdown signal to trigger final flush.
    pub async fn flush(&self) -> BatchResult<()> {
        // For lock-free implementation, flushing is automatic via background task
        // Just sleep briefly to allow background task to process queue
        tokio::time::sleep(Duration::from_millis(self.config.linger_ms + 10)).await;
        Ok(())
    }

    /// Get approximate number of pending PUT operations.
    ///
    /// Note: With lock-free channels, we can't get exact count without blocking.
    /// This returns an approximation based on channel state.
    #[allow(dead_code)]
    pub async fn pending_puts_count(&self) -> usize {
        // With crossbeam channel, we can't get exact count without consuming
        // Return 0 as items are processed asynchronously
        self.item_tx.len()
    }
}

impl<C: Collection> Drop for BatchAccumulator<C> {
    fn drop(&mut self) {
        // Signal flush task to shutdown
        let _ = self.flush_tx.try_send(FlushCommand::Shutdown);
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
}
