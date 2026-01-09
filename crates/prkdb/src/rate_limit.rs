//! Rate Limiting for PrkDB operations
//!
//! Simple token bucket rate limiter for controlling operation throughput.
//!
//! # Example
//!
//! ```rust,ignore
//! use prkdb::rate_limit::RateLimiter;
//!
//! let limiter = RateLimiter::new(100, Duration::from_secs(1));  // 100 ops/sec
//!
//! // Wait for permission before operation
//! limiter.acquire().await;
//! db.insert(&record).await?;
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Token bucket rate limiter
pub struct RateLimiter {
    /// Maximum tokens (burst capacity)
    capacity: u64,
    /// Current tokens available
    tokens: AtomicU64,
    /// Tokens added per refill
    refill_rate: u64,
    /// Refill interval
    refill_interval: Duration,
    /// Last refill time
    last_refill: Mutex<Instant>,
}

impl RateLimiter {
    /// Create a new rate limiter
    ///
    /// # Arguments
    /// * `ops_per_second` - Maximum operations per second
    ///
    /// # Example
    /// ```rust,ignore
    /// let limiter = RateLimiter::per_second(100);  // 100 ops/sec
    /// ```
    pub fn per_second(ops_per_second: u64) -> Self {
        Self {
            capacity: ops_per_second,
            tokens: AtomicU64::new(ops_per_second),
            refill_rate: ops_per_second,
            refill_interval: Duration::from_secs(1),
            last_refill: Mutex::new(Instant::now()),
        }
    }

    /// Create a new rate limiter with custom capacity and interval
    pub fn new(capacity: u64, refill_interval: Duration) -> Self {
        Self {
            capacity,
            tokens: AtomicU64::new(capacity),
            refill_rate: capacity,
            refill_interval,
            last_refill: Mutex::new(Instant::now()),
        }
    }

    /// Create a rate limiter with burst capacity
    ///
    /// # Arguments
    /// * `sustained_rate` - Normal operations per interval
    /// * `burst_capacity` - Maximum burst capacity
    /// * `interval` - Refill interval
    pub fn with_burst(sustained_rate: u64, burst_capacity: u64, interval: Duration) -> Self {
        Self {
            capacity: burst_capacity,
            tokens: AtomicU64::new(burst_capacity),
            refill_rate: sustained_rate,
            refill_interval: interval,
            last_refill: Mutex::new(Instant::now()),
        }
    }

    /// Refill tokens based on elapsed time
    async fn refill(&self) {
        let mut last = self.last_refill.lock().await;
        let now = Instant::now();
        let elapsed = now.duration_since(*last);

        if elapsed >= self.refill_interval {
            let refills = elapsed.as_millis() / self.refill_interval.as_millis();
            let tokens_to_add = (refills as u64) * self.refill_rate;

            let current = self.tokens.load(Ordering::Relaxed);
            let new_tokens = (current + tokens_to_add).min(self.capacity);
            self.tokens.store(new_tokens, Ordering::Relaxed);

            *last = now;
        }
    }

    /// Try to acquire a token without waiting
    /// Returns true if acquired, false if rate limited
    pub async fn try_acquire(&self) -> bool {
        self.refill().await;

        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            if current == 0 {
                return false;
            }

            if self
                .tokens
                .compare_exchange(current, current - 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Acquire a token, waiting if necessary
    pub async fn acquire(&self) {
        loop {
            if self.try_acquire().await {
                return;
            }
            // Wait a bit before retrying
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Acquire multiple tokens
    pub async fn acquire_n(&self, n: u64) {
        for _ in 0..n {
            self.acquire().await;
        }
    }

    /// Get current available tokens
    pub fn available(&self) -> u64 {
        self.tokens.load(Ordering::Relaxed)
    }

    /// Get capacity
    pub fn capacity(&self) -> u64 {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rate_limiter_basic() {
        let limiter = RateLimiter::per_second(10);

        // Should be able to acquire up to capacity
        for _ in 0..10 {
            assert!(limiter.try_acquire().await);
        }

        // Should be rate limited after
        assert!(!limiter.try_acquire().await);
    }

    #[tokio::test]
    async fn test_rate_limiter_refill() {
        let limiter = RateLimiter::new(5, Duration::from_millis(50));

        // Exhaust tokens
        for _ in 0..5 {
            assert!(limiter.try_acquire().await);
        }
        assert!(!limiter.try_acquire().await);

        // Wait for refill
        tokio::time::sleep(Duration::from_millis(60)).await;

        // Should have tokens again
        assert!(limiter.try_acquire().await);
    }
}
