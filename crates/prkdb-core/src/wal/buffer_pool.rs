use std::sync::Arc;
use tokio::sync::Mutex;

/// Buffer pool for reusing serialization buffers
///
/// Reduces allocations in hot path by reusing Vec<u8> buffers
/// across multiple serialization operations.
pub struct BufferPool {
    buffers: Arc<Mutex<Vec<Vec<u8>>>>,
    buffer_size: usize,
    max_pool_size: usize,
}

impl BufferPool {
    /// Create a new buffer pool
    pub fn new(buffer_size: usize, max_pool_size: usize) -> Self {
        Self {
            buffers: Arc::new(Mutex::new(Vec::with_capacity(max_pool_size))),
            buffer_size,
            max_pool_size,
        }
    }

    /// Acquire a buffer from the pool (or create new if empty)
    pub async fn acquire(&self) -> Vec<u8> {
        let mut buffers = self.buffers.lock().await;
        buffers
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(self.buffer_size))
    }

    /// Release a buffer back to the pool
    pub async fn release(&self, mut buffer: Vec<u8>) {
        buffer.clear();

        let mut buffers = self.buffers.lock().await;
        if buffers.len() < self.max_pool_size {
            buffers.push(buffer);
        }
        // Otherwise drop the buffer
    }

    /// Get a clone of the pool for sharing
    pub fn clone_pool(&self) -> Self {
        Self {
            buffers: self.buffers.clone(),
            buffer_size: self.buffer_size,
            max_pool_size: self.max_pool_size,
        }
    }
}

impl Clone for BufferPool {
    fn clone(&self) -> Self {
        self.clone_pool()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_buffer_pool_acquire_release() {
        let pool = BufferPool::new(1024, 10);

        // Acquire a buffer
        let mut buf = pool.acquire().await;
        assert!(buf.capacity() >= 1024);

        // Use it
        buf.extend_from_slice(b"hello world");
        assert_eq!(buf.len(), 11);

        // Release it back
        pool.release(buf).await;

        // Acquire again - should get the same buffer (cleared)
        let buf2 = pool.acquire().await;
        assert_eq!(buf2.len(), 0);
        assert!(buf2.capacity() >= 1024);
    }

    #[tokio::test]
    async fn test_buffer_pool_max_size() {
        let pool = BufferPool::new(1024, 2);

        // Acquire and release 3 buffers
        for _ in 0..3 {
            let buf = pool.acquire().await;
            pool.release(buf).await;
        }

        // Pool should have at most 2 buffers (may have less due to timing)
        let buffers = pool.buffers.lock().await;
        assert!(buffers.len() <= 2);
        assert!(buffers.len() >= 1); // At least one should be pooled
    }

    #[tokio::test]
    async fn test_buffer_pool_concurrent() {
        let pool = BufferPool::new(1024, 10);

        // Spawn multiple tasks
        let mut handles = Vec::new();
        for i in 0..20 {
            let pool = pool.clone();
            handles.push(tokio::spawn(async move {
                let mut buf = pool.acquire().await;
                buf.extend_from_slice(&[i as u8; 100]);
                pool.release(buf).await;
            }));
        }

        // Wait for all
        for handle in handles {
            handle.await.unwrap();
        }

        // Pool should have up to max_pool_size buffers
        let buffers = pool.buffers.lock().await;
        assert!(buffers.len() <= 10);
    }
}
