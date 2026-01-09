use std::time::{Duration, Instant};

/// Batch accumulator that collects items and flushes periodically or when full
pub struct BatchAccumulator<T> {
    /// Pending items
    buffer: Vec<T>,

    /// Maximum batch size (number of items)
    max_batch_size: usize,

    /// Maximum time to wait before flushing (milliseconds)
    flush_interval: Duration,

    /// Last flush time
    last_flush: Instant,
}

impl<T> BatchAccumulator<T> {
    /// Create a new batch accumulator
    pub fn new(max_batch_size: usize, flush_interval_ms: u64) -> Self {
        Self {
            buffer: Vec::with_capacity(max_batch_size),
            max_batch_size,
            flush_interval: Duration::from_millis(flush_interval_ms),
            last_flush: Instant::now(),
        }
    }

    /// Add an item to the batch
    /// Returns true if batch should be flushed
    pub fn add(&mut self, item: T) -> bool {
        self.buffer.push(item);
        self.should_flush()
    }

    /// Check if batch should be flushed
    pub fn should_flush(&self) -> bool {
        self.buffer.len() >= self.max_batch_size
            || (!self.buffer.is_empty() && self.last_flush.elapsed() >= self.flush_interval)
    }

    /// Flush the batch and return items
    pub fn flush(&mut self) -> Vec<T> {
        let batch = std::mem::take(&mut self.buffer);
        self.last_flush = Instant::now();
        batch
    }

    /// Get current batch size
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Get time since last flush
    pub fn time_since_flush(&self) -> Duration {
        self.last_flush.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{LogOperation, LogRecord};
    use std::thread::sleep;

    #[test]
    fn test_accumulator_creation() {
        let acc = BatchAccumulator::<LogRecord>::new(100, 10);
        assert_eq!(acc.len(), 0);
        assert!(acc.is_empty());
    }

    #[test]
    fn test_accumulator_add() {
        let mut acc = BatchAccumulator::new(100, 10);

        let record = LogRecord::new(LogOperation::Put {
            collection: "test".to_string(),
            id: b"key1".to_vec(),
            data: b"value1".to_vec(),
        });

        let should_flush = acc.add(record);
        assert!(!should_flush); // Not full yet
        assert_eq!(acc.len(), 1);
    }

    #[test]
    fn test_accumulator_flush_when_full() {
        let mut acc = BatchAccumulator::new(3, 10000); // Large timeout

        for i in 0..3 {
            let record = LogRecord::new(LogOperation::Put {
                collection: "test".to_string(),
                id: format!("key{}", i).into_bytes(),
                data: b"value".to_vec(),
            });
            acc.add(record);
        }

        assert!(acc.should_flush());
        assert_eq!(acc.len(), 3);

        let batch = acc.flush();
        assert_eq!(batch.len(), 3);
        assert_eq!(acc.len(), 0);
    }

    #[test]
    fn test_accumulator_flush_on_timeout() {
        let mut acc = BatchAccumulator::new(100, 50); // 50ms timeout

        let record = LogRecord::new(LogOperation::Put {
            collection: "test".to_string(),
            id: b"key1".to_vec(),
            data: b"value1".to_vec(),
        });
        acc.add(record);

        // Initially shouldn't flush (just added)
        assert!(!acc.should_flush());

        // Wait for timeout
        sleep(Duration::from_millis(60));

        // Now should flush due to timeout
        assert!(acc.should_flush());

        let batch = acc.flush();
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn test_accumulator_time_since_flush() {
        let mut acc = BatchAccumulator::<LogRecord>::new(100, 10);

        let initial_time = acc.time_since_flush();
        assert!(initial_time < Duration::from_millis(10));

        sleep(Duration::from_millis(20));

        let elapsed = acc.time_since_flush();
        assert!(elapsed >= Duration::from_millis(20));

        acc.flush();

        let after_flush = acc.time_since_flush();
        assert!(after_flush < Duration::from_millis(10));
    }

    #[test]
    fn test_accumulator_multiple_flushes() {
        let mut acc = BatchAccumulator::new(2, 10000);

        // First batch
        for i in 0..2 {
            let record = LogRecord::new(LogOperation::Put {
                collection: "test".to_string(),
                id: format!("key{}", i).into_bytes(),
                data: b"value".to_vec(),
            });
            acc.add(record);
        }

        let batch1 = acc.flush();
        assert_eq!(batch1.len(), 2);
        assert_eq!(acc.len(), 0);

        // Second batch
        for i in 2..4 {
            let record = LogRecord::new(LogOperation::Put {
                collection: "test".to_string(),
                id: format!("key{}", i).into_bytes(),
                data: b"value".to_vec(),
            });
            acc.add(record);
        }

        let batch2 = acc.flush();
        assert_eq!(batch2.len(), 2);
        assert_eq!(acc.len(), 0);
    }
}
