//! The caller's time bound on the WAL write path.
//!
//! # The property this exists to enforce
//!
//! Every queued write is a promise to the caller. Before the liveness work, a write handed
//! to a flush loop that had stopped publishing waited forever: callers blocked, no error
//! was returned and nothing outside the process could tell (cargo-mutants found it by
//! replacing the flush function with `()`; the suite hung, run 31505589348).
//!
//! # Two bounds, owned by two layers
//!
//! Since Task 2.8a the writer is the single `Wal`'s own thread, and detection and the
//! caller's answer are separate:
//!
//! - **Stall detection is the WAL's.** `Wal::health` reports `Stalled` once requests are
//!   queued and no batch has completed for `max(1 s, 100 × sync_interval_ms)`. That is what
//!   `write_path_health` (and the `writer_healthy` / `writer_stalls_total` metrics) report.
//!   A panic on the writer thread poisons the log instead, and every queued request is
//!   answered.
//! - **The caller's bound is here.** [`LivenessBounds::client_bound`] is how long one write
//!   waits for admission (then `WriteBackpressure`: nothing was queued) and, once queued,
//!   for its answer (then `WriteNotConfirmed`: the writer holds it and it may still land).
//!   It derives from `max_flush_ms`, not from the WAL's stall bound, so the two are not
//!   ordered by construction: with the defaults (`max_flush_ms` 50, `sync_interval_ms` 10)
//!   the client bound is 6.4 s against a 1 s stall bound, but a small `max_flush_ms` can
//!   make a caller give up before the WAL would call the writer stalled.
//!
//! Liveness has no non-temporal observation — a test can only ever see "not yet" — so the
//! caller's bound is necessarily time-based, and it means *not confirmed*, not *failed*.
//!
//! Spec: `docs/superpowers/specs/2026-08-11-wal-writer-liveness.md`.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// How many configured flush intervals a caller waits, at each of its two steps, before it
/// is answered without the writer's result.
///
/// A multiple of `max_flush_ms` rather than a wall-clock constant, so a deployment that
/// raises its flush interval raises its bound with it. 128 (it was 16 intervals for the
/// old watchdog's threshold, times 8 of margin above it) is a margin, not a measurement:
/// a merely slow writer must not be reported to its callers as one that may have lost
/// their writes.
const CLIENT_BOUND_FLUSH_INTERVALS: u32 = 128;

/// A flush interval of zero would make the bound zero. Clamping is a sanity floor on a
/// misconfiguration, not a tuning choice.
const MIN_FLUSH_INTERVAL_MS: u64 = 1;

/// The caller's time bound on the write path; see the module doc for how it relates to
/// the WAL's own stall detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LivenessBounds {
    /// How long a client waits for admission, and then for its answer, before giving up
    /// with `WriteBackpressure` or `WriteNotConfirmed` respectively.
    pub client_bound: Duration,
}

impl LivenessBounds {
    /// Derive the bound from the configured maximum flush interval.
    ///
    /// The *configured maximum*, so the bound does not tighten under load, which is exactly
    /// when latency is highest.
    pub fn from_max_flush_ms(max_flush_ms: u64) -> Self {
        let interval = Duration::from_millis(max_flush_ms.max(MIN_FLUSH_INTERVAL_MS));
        Self {
            client_bound: interval * CLIENT_BOUND_FLUSH_INTERVALS,
        }
    }
}

/// Milliseconds since the Unix epoch, for the "last successful publish" gauge.
///
/// Wall-clock rather than monotonic because this one is read by humans and dashboards
/// against other wall-clock timestamps.
pub fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `unix_millis` is a wall-clock reading. A broken one (zero, or a monotonic clock's
    /// arbitrary origin) would put the last publish in January 1970 and "time since last
    /// publish" unboundedly large — the alarm this gauge feeds would fire permanently and
    /// mean nothing.
    ///
    /// Bracketed by two readings of the same clock rather than compared to a fixed date,
    /// so the test does not acquire an expiry.
    #[test]
    fn unix_millis_reads_the_wall_clock() {
        let millis_now = || {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("the system clock is after the Unix epoch")
                .as_millis() as u64
        };

        let before = millis_now();
        let stamped = unix_millis();
        let after = millis_now();

        assert!(
            (before..=after).contains(&stamped),
            "unix_millis returned {stamped}, which is not a wall-clock reading taken \
             between {before} and {after}"
        );
    }

    #[test]
    fn the_bound_derives_from_the_flush_interval_rather_than_a_constant() {
        assert_eq!(
            LivenessBounds::from_max_flush_ms(10).client_bound,
            Duration::from_millis(1_280)
        );
        assert_eq!(
            LivenessBounds::from_max_flush_ms(50).client_bound,
            Duration::from_millis(6_400),
            "the default max_flush_ms gives callers 6.4s"
        );
    }

    #[test]
    fn a_zero_flush_interval_does_not_produce_a_zero_bound() {
        assert!(LivenessBounds::from_max_flush_ms(0).client_bound > Duration::ZERO);
    }
}
