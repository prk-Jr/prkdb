/// Lock-free write queue for dedicated WAL writer thread
///
/// This module provides a high-performance write queue that allows async tasks
/// to enqueue WAL write requests without blocking, while a dedicated OS thread
/// handles batched I/O operations.
///
/// # Architecture
///
/// ```text
/// Async Tasks (many)
///   ↓
/// WriteQueue (lock-free enqueue)
///   ↓
/// Dedicated Writer Thread (blocking I/O batching)
///   ↓
/// WAL Write + Fsync
///   ↓
/// Oneshot Ack
/// ```
use crossbeam_channel::{unbounded, Receiver, Sender};
use prkdb_core::wal::log_record::LogRecord;
use prkdb_types::error::StorageError;
use tokio::sync::oneshot;

/// A request to write a log record to the WAL
pub struct WriteRequest {
    /// The log record to write
    pub record: LogRecord,
    /// Channel to send the write result (offset or error)
    pub ack: oneshot::Sender<Result<u64, StorageError>>,
}

/// Lock-free write queue using crossbeam channels
///
/// This queue allows multiple async tasks to enqueue writes concurrently
/// without blocking, while a single dedicated thread dequeues and processes
/// batches.
pub struct WriteQueue {
    _tx: Sender<WriteRequest>,
    _rx: Receiver<WriteRequest>,
}

impl WriteQueue {
    /// Create a sender/receiver pair for a write queue
    ///
    /// Returns a tuple of (sender, receiver) for the queue.
    /// The sender can be cloned and shared across async tasks.
    /// The receiver should be used by the dedicated writer thread.
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> (Sender<WriteRequest>, Receiver<WriteRequest>) {
        unbounded()
    }
}

impl Default for WriteQueue {
    fn default() -> Self {
        let (tx, rx) = unbounded();
        Self { _tx: tx, _rx: rx }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prkdb_core::wal::log_record::LogOperation;

    #[tokio::test]
    async fn test_write_queue_send_receive() {
        let (tx, rx) = WriteQueue::new();

        let record = LogRecord::new(LogOperation::Put {
            collection: "test".to_string(),
            id: vec![1, 2, 3],
            data: vec![4, 5, 6],
        });

        let (ack_tx, ack_rx) = oneshot::channel();

        tx.send(WriteRequest {
            record,
            ack: ack_tx,
        })
        .unwrap();

        let req = rx.recv().unwrap();
        assert!(matches!(req.record.operation, LogOperation::Put { .. }));

        // Simulate successful write
        req.ack.send(Ok(42)).unwrap();
        assert_eq!(ack_rx.await.unwrap().unwrap(), 42);
    }

    #[tokio::test]
    async fn test_write_queue_multiple() {
        let (tx, rx) = WriteQueue::new();

        // Send 3 requests
        for i in 0..3 {
            let record = LogRecord::new(LogOperation::Put {
                collection: "test".to_string(),
                id: vec![i],
                data: vec![i * 2],
            });

            let (ack_tx, _ack_rx) = oneshot::channel();
            tx.send(WriteRequest {
                record,
                ack: ack_tx,
            })
            .unwrap();
        }

        // Receive 3 requests
        for _ in 0..3 {
            let _req = rx.recv().unwrap();
        }
    }
}
