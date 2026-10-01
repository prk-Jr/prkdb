//! Time bounds for the WAL write path.
//!
//! # The property this exists to enforce
//!
//! Every queued write is a promise to the caller. Before the liveness work, a write handed
//! to a flush loop that had stopped publishing waited forever: callers blocked, no error
//! was returned and nothing outside the process could tell (cargo-mutants found it by
//! replacing the flush function with `()`; the suite hung, run 31505589348).
//!
//! Since Task 2.8a the writer is the single `Wal`'s own thread. A panic in it poisons the
//! log and answers every queued request; a writer that is alive but completes nothing is
//! reported by `Wal::health` as stalled. What stays here is the caller's side: how long a
//! write waits for admission and then for its answer before it is told
//! `WriteBackpressure` or `WriteNotConfirmed`. Liveness has no non-temporal observation —
//! a test can only ever see "not yet" — so the bound is necessarily time-based, and it
//! means *not confirmed* rather than *failed*.
//!
//! Spec: `docs/superpowers/specs/2026-08-11-wal-writer-liveness.md`.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// How many flush intervals an unpublished write may sit before the writer is considered
/// stalled.
///
/// A multiple of the configured flush interval rather than a wall-clock constant, because a
/// constant would be correct only for whichever configuration it happened to be tuned
/// against — the magic number the spec rejects, moved one level out. A deployment that
/// raises `max_flush_ms` raises its own bounds with it. 16 is a margin, not a measurement:
/// it keeps a loaded CI box from being reported as a stalled database.
const STALL_FLUSH_INTERVALS: u32 = 16;

/// How much longer than the stall threshold a client waits before giving up on its own.
///
/// Deliberately far above the threshold, so a merely slow writer is not reported to its
/// callers as one that may have lost their writes.
const CLIENT_BOUND_STALL_MULTIPLE: u32 = 8;

/// A flush interval of zero would make every bound zero. Clamping is a sanity floor on a
/// misconfiguration, not a tuning choice.
const MIN_FLUSH_INTERVAL_MS: u64 = 1;

/// The two time bounds the write path runs on, both derived from the configured flush
/// interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LivenessBounds {
    /// How long an unpublished write may sit before the writer is considered stalled.
    pub stall_threshold: Duration,
    /// How long a client waits for admission, and then for its answer, before giving up
    /// with `WriteBackpressure` or `WriteNotConfirmed` respectively.
    pub client_bound: Duration,
}

impl LivenessBounds {
    /// Derive both bounds from the accumulator's configured maximum flush interval.
    ///
    /// The *configured maximum*, so the bounds do not tighten under load, which is exactly
    /// when latency is highest.
    pub fn from_max_flush_ms(max_flush_ms: u64) -> Self {
        let interval = Duration::from_millis(max_flush_ms.max(MIN_FLUSH_INTERVAL_MS));
        let stall_threshold = interval * STALL_FLUSH_INTERVALS;
        Self {
            stall_threshold,
            client_bound: stall_threshold * CLIENT_BOUND_STALL_MULTIPLE,
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
    fn bounds_derive_from_the_flush_interval_rather_than_a_constant() {
        let fast = LivenessBounds::from_max_flush_ms(10);
        let slow = LivenessBounds::from_max_flush_ms(100);

        assert_eq!(fast.stall_threshold, Duration::from_millis(160));
        assert_eq!(slow.stall_threshold, Duration::from_millis(1600));
        assert_eq!(fast.client_bound, Duration::from_millis(1280));
        assert!(
            slow.stall_threshold > fast.stall_threshold,
            "a slower configured flush must get a proportionally later bound"
        );

        // The client's bound sits above the stall threshold, so a merely slow writer is
        // not reported to its callers as one that may have lost their writes.
        assert!(fast.client_bound > fast.stall_threshold);
        assert!(slow.client_bound > slow.stall_threshold);
    }

    #[test]
    fn a_zero_flush_interval_does_not_produce_a_zero_threshold() {
        let bounds = LivenessBounds::from_max_flush_ms(0);
        assert!(bounds.stall_threshold > Duration::ZERO);
        assert!(bounds.client_bound > Duration::ZERO);
    }
}
