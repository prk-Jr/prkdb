//! Progress accounting and failure state for the WAL write path.
//!
//! # The property this exists to enforce
//!
//! Every queued write is a promise to the caller, and before this module nothing was
//! responsible for keeping it. `put_many` and the `append_raft_entry*` family push a
//! `PendingWrite` into the accumulator and `await` a `oneshot` with no deadline. The
//! sender half lives *inside* the accumulator until the flush loop takes the batch out. If
//! the flush loop stops taking batches out, the senders sit there alive and unfired
//! forever: callers block, no error is returned, no metric moves, and nothing outside the
//! process can tell.
//!
//! cargo-mutants found it by replacing `flush_accumulator_inner` with `()`. The whole
//! workspace suite hung and the mutant was reported `TIMEOUT` after burning its full 300s
//! budget (run 31505589348). That mutant is not a nuisance — it is a faithful simulation
//! of a flush loop that is alive but no longer publishing, which is what a swallowed error
//! inside the loop body produces in production.
//!
//! # Why a `JoinHandle` is not enough
//!
//! Supervising the writer task catches a panic, an early return, and a cancellation. It
//! does not catch this failure, because under it the task is alive, looping, and its
//! `JoinHandle` never resolves. Liveness — "the write is eventually published" — has no
//! non-temporal observation: a test can only ever see "not yet". So the detection is
//! necessarily time-based, and the only design question is where the bound lives and what
//! it means. It lives here, and it means *not confirmed* rather than *failed*.
//!
//! Spec: `docs/superpowers/specs/2026-08-11-wal-writer-liveness.md`.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use prkdb_types::error::StorageError;
use prkdb_types::storage::WritePathHealth;

/// How many flush intervals an unpublished write may sit before the writer is declared
/// stalled.
///
/// The threshold is a multiple of the configured flush interval rather than a wall-clock
/// constant, because a constant would be correct only for whichever configuration it
/// happened to be tuned against — the same magic number the spec rejects, moved one level
/// out. A deployment that raises `max_flush_ms` raises its own stall bound with it.
///
/// 16 is a margin, not a measurement. The flush loop wakes every 2ms whenever the
/// accumulator is non-empty, so a healthy writer publishes two orders of magnitude faster
/// than this bound at the 50ms default. The margin is there so that a loaded CI box is not
/// reported as a stalled database.
const STALL_FLUSH_INTERVALS: u32 = 16;

/// How much longer than the stall threshold a client waits before giving up on its own.
///
/// This bound is a backstop, not the fix. It sits deliberately far above the watchdog's,
/// so that under the failure this spec is about the caller is discharged by
/// [`WritePathProgress::fail`] with a named cause rather than by its own timer with a
/// generic one. It only becomes the operative bound for writes the writer has *already*
/// taken out of the accumulator, which the watchdog cannot reach.
///
/// A blanket timeout on `rx.await` as the primary fix was considered and rejected: it
/// changes the durability contract in order to make a CI signal pass.
const CLIENT_BOUND_STALL_MULTIPLE: u32 = 8;

/// A flush interval of zero would make the stall threshold zero and fire the watchdog on
/// the first tick. Clamping is a sanity floor on a misconfiguration, not a tuning choice.
const MIN_FLUSH_INTERVAL_MS: u64 = 1;

/// Why the write path stopped being able to confirm writes.
///
/// Rendered into the error the waiters receive and into the reason on the health endpoint,
/// so the wording is operator-facing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriterFailure {
    /// The writer task ended. Carries what `JoinError` said, or "returned" for a clean
    /// exit — a clean exit is still a failure here, because the loop is supposed to run
    /// for the life of the adapter.
    Exited(String),
    /// The writer is alive but has published nothing for longer than the stall threshold.
    Stalled {
        queue_depth: u64,
        oldest_age_ms: u64,
        threshold_ms: u64,
    },
}

impl std::fmt::Display for WriterFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriterFailure::Exited(cause) => {
                write!(f, "WAL writer task exited ({cause})")
            }
            WriterFailure::Stalled {
                queue_depth,
                oldest_age_ms,
                threshold_ms,
            } => write!(
                f,
                "WAL writer stalled: {queue_depth} write(s) queued, oldest unpublished for \
                 {oldest_age_ms}ms with no publication progress (threshold {threshold_ms}ms)"
            ),
        }
    }
}

impl WriterFailure {
    /// The error handed to every waiter discharged because of this failure.
    ///
    /// Always [`StorageError::WriteNotConfirmed`], never `Internal` and never anything
    /// that reads as "failed". A panic mid-`flush_accumulator_inner` can happen *after* the
    /// WAL append returned, so even the panic case cannot honestly claim the write did not
    /// land.
    /// The error for a write this failure **discarded**, as opposed to one merely left
    /// waiting.
    ///
    /// Definite, and deliberately not `WriteNotConfirmed`. Both call sites reach writes
    /// that were never handed to the writer: the watchdog discharging what is still in the
    /// accumulator, and `refuse_if_failed` declining a write once the writer has exited.
    /// Nothing was appended for either, so "may still be published" would be false — and
    /// false in the lossy direction, because a caller who believes a write may have landed
    /// will not retry it.
    ///
    /// A write the writer already holds is a different case and keeps `WriteNotConfirmed`:
    /// there the outcome is genuinely unknown, and `await_write`'s bound is what answers.
    pub fn to_error(&self) -> StorageError {
        StorageError::WriteAbandoned(self.to_string())
    }
}

/// Monotonic accounting for writes in flight, plus the terminal failure state.
///
/// Every field is an atomic and every read is lock-free except [`Self::failure`], which
/// takes an uncontended `RwLock` around a small enum. Health probes read this, and a probe
/// that can block is worse than no probe.
#[derive(Debug)]
pub struct WritePathProgress {
    /// Zero point for the two "when" fields below. `Instant` is not representable as an
    /// atomic, so times are stored as nanoseconds since this base.
    base: Instant,

    enqueued: AtomicU64,
    published: AtomicU64,

    /// Nanoseconds-since-`base` at which the oldest unpublished write was enqueued.
    /// `0` means nothing is pending.
    oldest_unpublished_nanos: AtomicU64,

    /// Nanoseconds-since-`base` of the last publication. `0` means none yet.
    last_publish_nanos: AtomicU64,

    /// Set once the writer is declared failed. Kept behind a lock rather than an
    /// `AtomicPtr` because it is written at most a handful of times in a process lifetime
    /// and read only by probes.
    failure: RwLock<Option<WriterFailure>>,

    /// Mirrors `failure.is_some()` so the hot path (`refuse_if_failed`) never takes the
    /// lock at all.
    failed: AtomicBool,
}

impl Default for WritePathProgress {
    fn default() -> Self {
        Self::new()
    }
}

impl WritePathProgress {
    pub fn new() -> Self {
        Self {
            base: Instant::now(),
            enqueued: AtomicU64::new(0),
            published: AtomicU64::new(0),
            oldest_unpublished_nanos: AtomicU64::new(0),
            last_publish_nanos: AtomicU64::new(0),
            failure: RwLock::new(None),
            failed: AtomicBool::new(false),
        }
    }

    fn now_nanos(&self) -> u64 {
        // Saturates at ~584 years of uptime. Adding a branch for that is not worth it.
        self.base.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
    }

    /// Record `count` writes entering the accumulator.
    ///
    /// Starts the clock on the oldest unpublished write if it was not already running.
    /// `compare_exchange` rather than a plain store: a second concurrent enqueue must not
    /// push the oldest write's timestamp forward, which would let a queue that never
    /// drains keep resetting its own age and never trip the watchdog.
    /// Returns whether this call took the queue from empty to non-empty.
    ///
    /// The answer costs nothing to produce: `oldest_unpublished_nanos` is zero exactly
    /// while the queue is empty — `resolve` stores 0 whenever it drains — so the
    /// compare-exchange below already distinguishes the two cases, and only reported
    /// success by discarding it. The caller uses it to wake a supervisor that would
    /// otherwise be asleep, which is the whole reason the supervisor no longer polls.
    ///
    /// Deliberately *this* transition and not every enqueue. A notify per write moves the
    /// cost from once per second while idle to once per write while busy, on the hot path
    /// of a database whose main job is writes — the wrong direction. This fires only when
    /// a queue that was empty stops being empty, and there is nothing to wake otherwise.
    pub fn record_enqueued(&self, count: u64) -> bool {
        if count == 0 {
            return false;
        }
        self.enqueued.fetch_add(count, Ordering::SeqCst);
        let now = self.now_nanos().max(1);
        self.oldest_unpublished_nanos
            .compare_exchange(0, now, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
    }

    /// Record `count` writes resolved *by the writer*.
    ///
    /// "Published" here means the writer reached them: it took the batch out, ran it
    /// through the WAL, and answered every caller. A batch whose WAL append returned an
    /// error counts too, because the property being tracked is that queued work resolves,
    /// not that it succeeds. A writer that keeps returning errors is broken, but it is not
    /// stalled, and calling it stalled would name the wrong problem to whoever is paged.
    ///
    /// Because it means the writer is demonstrably running, this also clears a stall. That
    /// is the *only* way a stall clears — see [`record_discharged`](Self::record_discharged)
    /// for the counterpart that deliberately does not.
    ///
    /// The oldest-write clock restarts at *now* if anything is still queued. That
    /// understates the age of whatever remains, which is the safe direction: the watchdog
    /// fires later than it strictly could, never earlier. Tracking true per-item ages would
    /// need a second ordered structure alongside the accumulator, to buy nothing but a
    /// tighter bound on an error path.
    pub fn record_published(&self, count: u64) {
        if count == 0 {
            return;
        }
        self.resolve(count);
        self.last_publish_nanos
            .store(self.now_nanos().max(1), Ordering::SeqCst);
        if self.failed.load(Ordering::SeqCst) {
            self.clear_stall();
        }
    }

    /// Record `count` writes resolved by *someone other than the writer* — the supervisor
    /// discharging the queue after a failure.
    ///
    /// It moves the same counter, because for queue-depth purposes these writes are gone
    /// either way and leaving them counted would keep the watchdog firing on an empty
    /// queue. It does not clear a stall, because handing out errors is not evidence that
    /// the writer came back; the writer is exactly as stuck as it was a moment ago, and an
    /// adapter that flipped itself back to healthy by giving up would be lying in the most
    /// misleading direction available.
    pub fn record_discharged(&self, count: u64) {
        self.resolve(count);
    }

    fn resolve(&self, count: u64) {
        if count == 0 {
            return;
        }
        self.published.fetch_add(count, Ordering::SeqCst);
        let now = self.now_nanos().max(1);

        if self.queue_depth() == 0 {
            self.oldest_unpublished_nanos.store(0, Ordering::SeqCst);
        } else {
            self.oldest_unpublished_nanos.store(now, Ordering::SeqCst);
        }
    }

    /// Writes enqueued but not yet discharged.
    ///
    /// Saturating because the two counters are bumped independently: a reader can observe
    /// `published` after an increment and `enqueued` before it.
    pub fn queue_depth(&self) -> u64 {
        self.enqueued
            .load(Ordering::SeqCst)
            .saturating_sub(self.published.load(Ordering::SeqCst))
    }

    pub fn enqueued_total(&self) -> u64 {
        self.enqueued.load(Ordering::SeqCst)
    }

    pub fn published_total(&self) -> u64 {
        self.published.load(Ordering::SeqCst)
    }

    /// How long the oldest unpublished write has been waiting, or `None` if the queue is
    /// empty.
    pub fn oldest_unpublished_age(&self) -> Option<Duration> {
        match self.oldest_unpublished_nanos.load(Ordering::SeqCst) {
            0 => None,
            at => Some(Duration::from_nanos(self.now_nanos().saturating_sub(at))),
        }
    }

    /// Time since the last publication, or `None` if nothing has published yet.
    pub fn since_last_publish(&self) -> Option<Duration> {
        match self.last_publish_nanos.load(Ordering::SeqCst) {
            0 => None,
            at => Some(Duration::from_nanos(self.now_nanos().saturating_sub(at))),
        }
    }

    /// Declare the write path failed, unless it already is.
    ///
    /// Returns `true` for the caller that made the transition, so logging and metric
    /// updates happen once rather than on every watchdog tick.
    pub fn fail(&self, failure: WriterFailure) -> bool {
        let mut slot = self.failure.write().expect("write path failure lock");
        if slot.is_some() {
            return false;
        }
        *slot = Some(failure);
        self.failed.store(true, Ordering::SeqCst);
        true
    }

    /// Clear a stall after the writer resumes publishing.
    ///
    /// Only stalls clear. [`WriterFailure::Exited`] is terminal: the task is gone and
    /// nothing will restart it, so an adapter that reported itself unhealthy for that
    /// reason must not later claim otherwise.
    pub fn clear_stall(&self) -> bool {
        let mut slot = self.failure.write().expect("write path failure lock");
        if matches!(*slot, Some(WriterFailure::Stalled { .. })) {
            *slot = None;
            self.failed.store(false, Ordering::SeqCst);
            return true;
        }
        false
    }

    pub fn failure(&self) -> Option<WriterFailure> {
        if !self.failed.load(Ordering::SeqCst) {
            return None;
        }
        self.failure
            .read()
            .expect("write path failure lock")
            .clone()
    }

    /// The error to return instead of queueing a new write, once the writer is gone.
    ///
    /// Only [`WriterFailure::Exited`] refuses. There is provably nobody left to drain the
    /// queue, so accepting a write would be making a promise in the knowledge that it
    /// cannot be kept.
    ///
    /// A stall deliberately does **not** refuse. The writer is still there and may resume;
    /// turning a latency problem into a total write outage — and one that never lifts,
    /// since refusing writes guarantees no publication will ever clear the stall — would
    /// be a worse failure than the one being guarded against. A stalled adapter reports
    /// itself unhealthy, discharges what it cannot confirm, and sheds new load through
    /// backpressure once the queue fills. It does not slam the door.
    pub fn refuse_if_failed(&self) -> Option<StorageError> {
        if !self.failed.load(Ordering::SeqCst) {
            return None;
        }
        match self.failure() {
            Some(failure @ WriterFailure::Exited(_)) => Some(failure.to_error()),
            _ => None,
        }
    }

    /// Snapshot for health and readiness probes.
    pub fn health(&self) -> WritePathHealth {
        let failure = self.failure();
        WritePathHealth {
            healthy: failure.is_none(),
            reason: failure.map(|f| f.to_string()),
            queue_depth: self.queue_depth(),
            oldest_unpublished_age_ms: self
                .oldest_unpublished_age()
                .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
                .unwrap_or(0),
            last_publish_age_ms: self
                .since_last_publish()
                .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64),
            publishes_total: self.published_total(),
        }
    }
}

/// The two time bounds the write path runs on, both derived from the configured flush
/// interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LivenessBounds {
    /// How long an unpublished write may sit before the writer is declared stalled.
    pub stall_threshold: Duration,
    /// How long a client waits on its `oneshot` before giving up with *not confirmed*.
    pub client_bound: Duration,
}

impl LivenessBounds {
    /// Derive both bounds from the accumulator's configured maximum flush interval.
    ///
    /// The *configured maximum* rather than the accumulator's current interval on purpose.
    /// `maybe_adjust_parameters` shortens the live interval when batches fill quickly —
    /// that is, under load, which is exactly when latency is highest. Deriving from it
    /// would tighten the stall bound at the moment the system most needs slack, so the
    /// watchdog would fire hardest on a database that is merely busy.
    pub fn from_max_flush_ms(max_flush_ms: u64) -> Self {
        let interval = Duration::from_millis(max_flush_ms.max(MIN_FLUSH_INTERVAL_MS));
        let stall_threshold = interval * STALL_FLUSH_INTERVALS;
        Self {
            stall_threshold,
            client_bound: stall_threshold * CLIENT_BOUND_STALL_MULTIPLE,
        }
    }

    /// How often the supervisor re-checks while writes are in flight.
    ///
    /// A quarter of the threshold, so detection latency is dominated by the threshold
    /// rather than by the sampling — capped at 250ms so that a deployment with a long
    /// stall bound still gets a queue-depth gauge that moves.
    ///
    /// # Why capping this is not the magic number the spec rejects
    ///
    /// This is sampling resolution, not the detection threshold. Firing still requires the
    /// oldest unpublished write to have aged past the full derived threshold, and no
    /// sampling rate changes that — sampling faster only means the gauges are fresher and
    /// the answer arrives closer to the moment it became true. The number that decides
    /// *whether* a writer is stalled is [`Self::stall_threshold`], and that one derives
    /// entirely from the configured flush interval.
    pub fn active_tick(&self) -> Duration {
        (self.stall_threshold / 4).clamp(Duration::from_millis(1), Duration::from_millis(250))
    }
}

// `idle_tick` used to live here, answering "how often to re-check when nothing is queued".
// The watchdog now waits for a write instead of re-checking on a timer, so the honest
// answer became "never" and a function returning an interval had nothing left to say.
// Deleted rather than left unused: the three mutants that survived on the branch choosing
// between it and `active_tick` are gone with it.

/// Milliseconds since the Unix epoch, for the "last successful publish" gauge.
///
/// Wall-clock rather than monotonic because this one is read by humans and dashboards
/// against other wall-clock timestamps. The stall detection above uses `Instant` and is
/// unaffected by clock steps.
pub fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

/// Convenience alias: the progress tracker is shared between the adapter, the writer and
/// the supervisor.
pub type SharedProgress = Arc<WritePathProgress>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_depth_tracks_enqueue_and_publish() {
        let progress = WritePathProgress::new();
        assert_eq!(progress.queue_depth(), 0);

        progress.record_enqueued(3);
        assert_eq!(progress.queue_depth(), 3);

        progress.record_published(2);
        assert_eq!(progress.queue_depth(), 1);

        progress.record_published(1);
        assert_eq!(progress.queue_depth(), 0);
    }

    /// The running totals, not just the difference between them.
    ///
    /// Mutation run 31575909551 missed `published_total -> u64 with 1`. Every test here
    /// asserted `queue_depth`, which is `enqueued - published` and so is unchanged by a
    /// constant that both sides of the subtraction never see. But the watchdog does not
    /// read the depth to decide whether the writer is stalled — it reads
    /// `published_total` on consecutive ticks and asks whether it moved. A total stuck at
    /// a constant reports "no progress" on every tick after the first, so a busy writer
    /// working perfectly through a queue that never empties gets declared stalled and has
    /// its pending writes discharged with errors.
    /// `unix_millis` reports the wall clock, not a number.
    ///
    /// Mutation run 31575909551 missed `unix_millis -> u64 with 1`. The liveness tests
    /// only ever asked whether `writer_last_publish_unix_ms` was `Some`, which any
    /// constant satisfies. The gauge exists to be read against other wall-clock
    /// timestamps on a dashboard, so a constant makes every publish look like it happened
    /// in January 1970 and "time since last publish" unboundedly large — the alarm this
    /// gauge feeds would fire permanently and mean nothing.
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
    fn the_running_totals_count_what_actually_happened() {
        let progress = WritePathProgress::new();
        assert_eq!(progress.enqueued_total(), 0);
        assert_eq!(progress.published_total(), 0);

        progress.record_enqueued(3);
        assert_eq!(progress.enqueued_total(), 3);
        assert_eq!(progress.published_total(), 0);

        progress.record_published(2);
        assert_eq!(progress.enqueued_total(), 3);
        assert_eq!(
            progress.published_total(),
            2,
            "the publish total must move with each publish; the watchdog reads it across \
             ticks and treats a total that does not move as a stall"
        );

        progress.record_published(1);
        assert_eq!(progress.published_total(), 3);
    }

    #[test]
    fn the_oldest_write_clock_starts_on_the_first_enqueue_and_stops_when_the_queue_drains() {
        let progress = WritePathProgress::new();
        assert_eq!(progress.oldest_unpublished_age(), None);

        progress.record_enqueued(1);
        let first = progress
            .oldest_unpublished_age()
            .expect("a queued write has an age");

        std::thread::sleep(Duration::from_millis(5));

        // A later enqueue must not restart the clock: the oldest write is still the first.
        progress.record_enqueued(1);
        let after = progress
            .oldest_unpublished_age()
            .expect("still queued")
            .as_millis();
        assert!(
            after >= 5,
            "second enqueue reset the oldest-write clock ({after}ms, started at {first:?})"
        );

        progress.record_published(2);
        assert_eq!(
            progress.oldest_unpublished_age(),
            None,
            "an empty queue has no oldest write"
        );
        assert!(progress.since_last_publish().is_some());
    }

    #[test]
    fn a_partial_publish_restarts_the_clock_but_does_not_stop_it() {
        let progress = WritePathProgress::new();
        progress.record_enqueued(4);
        std::thread::sleep(Duration::from_millis(5));

        progress.record_published(1);

        assert_eq!(progress.queue_depth(), 3);
        let age = progress
            .oldest_unpublished_age()
            .expect("three writes are still queued");
        assert!(
            age < Duration::from_millis(5),
            "publishing progress should reset the age, got {age:?}"
        );
    }

    #[test]
    fn failure_is_recorded_once_and_names_itself() {
        let progress = WritePathProgress::new();
        assert!(progress.failure().is_none());
        assert!(progress.refuse_if_failed().is_none());

        assert!(progress.fail(WriterFailure::Exited("panicked: boom".into())));
        assert!(
            !progress.fail(WriterFailure::Exited("second".into())),
            "only the first transition reports true, so logging happens once"
        );

        let err = progress.refuse_if_failed().expect("failed path refuses");
        assert!(
            err.is_write_abandoned(),
            "a refused write was never enqueued"
        );
        assert!(err.to_string().contains("boom"));
        assert!(!progress.health().healthy);
    }

    #[test]
    fn a_stall_clears_when_the_writer_resumes_but_an_exit_never_does() {
        let stalled = WritePathProgress::new();
        stalled.fail(stall());
        assert!(!stalled.health().healthy);
        assert!(stalled.clear_stall());
        assert!(stalled.health().healthy);

        let exited = WritePathProgress::new();
        exited.fail(WriterFailure::Exited("returned".into()));
        assert!(
            !exited.clear_stall(),
            "a task that has exited cannot come back, so its state must not clear"
        );
        assert!(!exited.health().healthy);
    }

    fn stall() -> WriterFailure {
        WriterFailure::Stalled {
            queue_depth: 1,
            oldest_age_ms: 900,
            threshold_ms: 800,
        }
    }

    /// The distinction the whole recovery rule turns on: only the writer publishing again
    /// counts as recovery. Handing out errors is the supervisor giving up, and an adapter
    /// that reported itself healthy again for that would be lying in the most misleading
    /// direction available.
    #[test]
    fn discharging_the_queue_resolves_it_without_claiming_the_writer_recovered() {
        let progress = WritePathProgress::new();
        progress.record_enqueued(4);
        progress.fail(stall());

        progress.record_discharged(4);
        assert_eq!(progress.queue_depth(), 0);
        assert!(
            !progress.health().healthy,
            "the writer is exactly as stuck as it was"
        );

        // The other direction: a real publication does clear it.
        progress.record_enqueued(1);
        progress.record_published(1);
        assert!(progress.health().healthy);
    }

    /// A stalled writer may still come back, so new writes are not refused; a writer that
    /// has exited will not, so they are.
    #[test]
    fn only_an_exited_writer_refuses_new_writes() {
        let stalled = WritePathProgress::new();
        stalled.fail(stall());
        assert!(
            stalled.refuse_if_failed().is_none(),
            "refusing writes during a stall guarantees no publication ever clears it"
        );

        let exited = WritePathProgress::new();
        exited.fail(WriterFailure::Exited("panicked: boom".into()));
        let err = exited.refuse_if_failed().expect("nobody is left to drain");
        assert!(
            err.is_write_abandoned(),
            "a refused write was never enqueued"
        );
        assert!(
            err.denies_durability(),
            "both variants must deny durability"
        );
    }

    /// A discharge is not a publication, so it must not move the "last successful publish"
    /// clock a dashboard is reading to decide whether writes are landing.
    #[test]
    fn discharging_does_not_count_as_a_publication() {
        let progress = WritePathProgress::new();
        progress.record_enqueued(1);
        progress.record_discharged(1);
        assert_eq!(progress.since_last_publish(), None);

        progress.record_enqueued(1);
        progress.record_published(1);
        assert!(progress.since_last_publish().is_some());
    }

    #[test]
    fn bounds_derive_from_the_flush_interval_rather_than_a_constant() {
        let fast = LivenessBounds::from_max_flush_ms(10);
        let slow = LivenessBounds::from_max_flush_ms(100);

        assert_eq!(fast.stall_threshold, Duration::from_millis(160));
        assert_eq!(slow.stall_threshold, Duration::from_millis(1600));
        assert!(
            slow.stall_threshold > fast.stall_threshold,
            "a slower configured flush must get a proportionally later bound"
        );

        // The client's bound sits above the watchdog's, so under a stall the caller is
        // discharged with a named cause rather than by its own timer.
        assert!(fast.client_bound > fast.stall_threshold);
        assert!(slow.client_bound > slow.stall_threshold);
    }

    #[test]
    fn a_zero_flush_interval_does_not_produce_a_zero_threshold() {
        let bounds = LivenessBounds::from_max_flush_ms(0);
        assert!(bounds.stall_threshold > Duration::ZERO);
        assert!(bounds.active_tick() > Duration::ZERO);
    }

    /// Sampling is bounded on both sides; the detection threshold is not. A very long
    /// stall bound must still refresh its gauges, and a very short one must still sample
    /// often enough to be the reason detection is late rather than the sampling.
    #[test]
    fn sampling_is_bounded_but_the_threshold_it_measures_against_is_not() {
        let patient = LivenessBounds::from_max_flush_ms(60_000);
        assert_eq!(patient.stall_threshold, Duration::from_secs(960));
        assert_eq!(
            patient.active_tick(),
            Duration::from_millis(250),
            "a long stall bound must not mean a frozen queue-depth gauge"
        );
        let brisk = LivenessBounds::from_max_flush_ms(4);
        assert_eq!(brisk.stall_threshold, Duration::from_millis(64));
        assert_eq!(brisk.active_tick(), Duration::from_millis(16));
    }

    #[test]
    fn health_reports_the_numbers_that_separate_busy_from_stuck() {
        let progress = WritePathProgress::new();
        let idle = progress.health();
        assert!(idle.healthy);
        assert_eq!(idle.queue_depth, 0);
        assert_eq!(idle.oldest_unpublished_age_ms, 0);
        assert_eq!(idle.last_publish_age_ms, None);

        progress.record_enqueued(5);
        std::thread::sleep(Duration::from_millis(3));
        let busy = progress.health();
        assert!(busy.healthy, "a deep queue on its own is not unhealthy");
        assert_eq!(busy.queue_depth, 5);
        assert!(busy.oldest_unpublished_age_ms >= 2);
    }
}
