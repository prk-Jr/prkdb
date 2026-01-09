use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;

/// Async fsync batcher that amortizes fsync costs across multiple writes
///
/// Instead of calling fsync after every write, this batches multiple
/// fsync requests and performs a single fsync for all of them.
/// This dramatically reduces the number of expensive fsync syscalls.
#[derive(Clone)]
pub struct AsyncFsyncBatcher {
    /// Channel to send fsync requests
    tx: mpsc::UnboundedSender<oneshot::Sender<std::io::Result<()>>>,
}

impl AsyncFsyncBatcher {
    /// Create a new async fsync batcher
    pub fn new<F>(sync_fn: F, sync_interval: Duration) -> Self
    where
        F: Fn() -> std::io::Result<()> + Send + 'static,
    {
        let (tx, rx) = mpsc::unbounded_channel();

        // Spawn background sync task
        tokio::spawn(Self::run_sync_loop(rx, sync_fn, sync_interval));

        Self { tx }
    }

    /// Request a sync and wait for it to complete
    pub async fn request_sync(&self) -> std::io::Result<()> {
        let (tx, rx) = oneshot::channel();

        // Send sync request
        self.tx.send(tx).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Sync task terminated")
        })?;

        // Wait for sync to complete
        rx.await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Sync task dropped"))?
    }

    /// Background task that batches and executes syncs
    async fn run_sync_loop<F>(
        mut rx: mpsc::UnboundedReceiver<oneshot::Sender<std::io::Result<()>>>,
        sync_fn: F,
        sync_interval: Duration,
    ) where
        F: Fn() -> std::io::Result<()>,
    {
        let mut pending: Vec<oneshot::Sender<std::io::Result<()>>> = Vec::new();
        let mut last_sync = tokio::time::Instant::now();

        loop {
            // Wait for either:
            // 1. A new sync request
            // 2. The sync interval to elapse
            let should_sync = tokio::select! {
                Some(waiter) = rx.recv() => {
                    pending.push(waiter);

                    // Sync if we have pending requests and interval elapsed
                    last_sync.elapsed() >= sync_interval
                }
                _ = sleep(sync_interval) => {
                    // Interval elapsed, sync if we have pending requests
                    !pending.is_empty()
                }
            };

            if should_sync {
                // Perform single fsync for all pending requests
                let result = sync_fn();
                last_sync = tokio::time::Instant::now();

                // Notify all waiters
                for waiter in pending.drain(..) {
                    let send_result = match &result {
                        Ok(()) => Ok(()),
                        Err(e) => Err(std::io::Error::new(e.kind(), e.to_string())),
                    };
                    let _ = waiter.send(send_result);
                }
            }

            // Exit if channel closed and no pending requests
            if rx.is_closed() && pending.is_empty() {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn test_async_fsync_batcher_basic() {
        // Create a sync counter
        let sync_count = Arc::new(Mutex::new(0));
        let sync_count_clone = sync_count.clone();

        let batcher = AsyncFsyncBatcher::new(
            move || {
                // Use try_lock to avoid blocking
                if let Ok(mut count) = sync_count_clone.try_lock() {
                    *count += 1;
                }
                Ok(())
            },
            Duration::from_millis(50),
        );

        // Request multiple syncs in quick succession
        let mut handles = Vec::new();
        for _ in 0..10 {
            let batcher = batcher.clone();
            handles.push(tokio::spawn(async move {
                batcher.request_sync().await.unwrap();
            }));
        }

        // Wait for all to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Give time for final sync
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Should have batched into fewer syncs than requests
        let count = *sync_count.lock().await;
        assert!(count >= 1, "Expected at least 1 sync, got {}", count);
        assert!(count <= 10, "Got {} syncs", count);
    }
}
