use std::cell::RefCell;

thread_local! {
    static BUFFER_POOL: RefCell<Vec<Vec<u8>>> = RefCell::new(Vec::new());
}

/// A pooled buffer that returns itself to the pool when dropped
pub struct PooledBuffer {
    buffer: Option<Vec<u8>>,
}

impl PooledBuffer {
    /// Acquire a buffer from the thread-local pool
    pub fn acquire() -> Self {
        let buffer = BUFFER_POOL.with(|pool| {
            pool.borrow_mut()
                .pop()
                .unwrap_or_else(|| Vec::with_capacity(4096))
        });

        Self {
            buffer: Some(buffer),
        }
    }

    /// Get a mutable reference to the underlying buffer
    pub fn as_mut_vec(&mut self) -> &mut Vec<u8> {
        self.buffer.as_mut().expect("buffer should be present")
    }

    /// Get a slice view of the buffer
    pub fn as_slice(&self) -> &[u8] {
        self.buffer
            .as_ref()
            .expect("buffer should be present")
            .as_slice()
    }

    /// Convenience method for bincode serialization
    pub fn encode<T: bincode::Encode>(
        &mut self,
        value: &T,
        config: bincode::config::Configuration,
    ) -> Result<&[u8], bincode::error::EncodeError> {
        let buffer = self.as_mut_vec();
        buffer.clear();
        bincode::encode_into_std_write(value, buffer, config)?;
        Ok(buffer.as_slice())
    }

    /// Take ownership of the buffer, consuming this PooledBuffer without returning it to the pool
    pub fn into_vec(mut self) -> Vec<u8> {
        self.buffer.take().expect("buffer should be present")
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(mut buffer) = self.buffer.take() {
            buffer.clear();

            // Don't pool overly large buffers to prevent memory bloat
            if buffer.capacity() <= 65536 {
                BUFFER_POOL.with(|pool| {
                    let mut pool = pool.borrow_mut();
                    // Limit pool size to 16 buffers per thread
                    if pool.len() < 16 {
                        pool.push(buffer);
                    }
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_acquire_and_reuse() {
        let buf1 = PooledBuffer::acquire();
        let ptr1 = buf1.as_slice().as_ptr();
        drop(buf1);

        let buf2 = PooledBuffer::acquire();
        let ptr2 = buf2.as_slice().as_ptr();

        assert_eq!(ptr1, ptr2, "Buffer should be reused from pool");
    }

    #[test]
    fn test_buffer_encode() {
        let mut buf = PooledBuffer::acquire();
        let config = bincode::config::standard();

        let data = vec![1u8, 2, 3, 4, 5];
        let encoded = buf.encode(&data, config).expect("encode should succeed");

        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_large_buffer_not_pooled() {
        let mut buf = PooledBuffer::acquire();
        let vec = buf.as_mut_vec();
        vec.resize(100_000, 0u8); // Make it large

        let ptr1 = vec.as_ptr();
        drop(buf);

        let buf2 = PooledBuffer::acquire();
        let ptr2 = buf2.as_slice().as_ptr();

        assert_ne!(ptr1, ptr2, "Large buffer should not be pooled");
    }
}
