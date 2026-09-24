//! Phase 1 gate tests (Task 1.8): prove the crash/restart harness can itself
//! fail, that the blocking profile is green on current code, and that the
//! discovery profile reproduces STO-01 (checkpoint recovery drops
//! pre-checkpoint keys).
//!
//! These are NOT the harness unit self-tests in `self_test.rs` (which prove
//! the runner/checker/minimizer correctly detect and shrink each class of
//! finding using small, fast, deterministic wrappers). This file exercises
//! the real WAL storage end to end and is the actual Phase 1 gate.

use prkdb_verify::model::{Key, Value};
use prkdb_verify::ops::{Op, Profile};
use prkdb_verify::runner::{run_seeds, Failure, Outcome};
use prkdb_verify::sut::{Sut, WalSut};

/// Wraps a `WalSut`, silently dropping every 10th `put` (acking it to the
/// caller/model without ever writing it to storage) — a lossy implementation
/// the harness must catch. This is the Phase 1 gate's meta-test: if the
/// harness can't catch this, it can't be trusted to catch anything else.
struct LossySut {
    inner: WalSut,
    put_count: u64,
}

#[async_trait::async_trait]
impl Sut for LossySut {
    async fn put(&mut self, k: &Key, v: &Value) -> anyhow::Result<()> {
        self.put_count += 1;
        if self.put_count.is_multiple_of(10) {
            // Ack without writing: the model believes this put succeeded, but
            // storage never saw it.
            return Ok(());
        }
        self.inner.put(k, v).await
    }
    async fn delete(&mut self, k: &Key) -> anyhow::Result<()> {
        self.inner.delete(k).await
    }
    async fn get(&mut self, k: &Key) -> anyhow::Result<Option<Value>> {
        self.inner.get(k).await
    }
    async fn reopen(&mut self) -> anyhow::Result<()> {
        self.inner.reopen().await
    }
    async fn crash(&mut self) -> anyhow::Result<()> {
        self.inner.crash().await
    }
    async fn checkpoint(&mut self) -> anyhow::Result<()> {
        self.inner.checkpoint().await
    }
}

/// The Phase 1 gate's meta-test: a SUT that drops every 10th put must be
/// caught by the harness as a finding. If this test can't fail, the harness
/// can't be trusted to gate anything else.
#[tokio::test(flavor = "multi_thread")]
async fn meta_harness_catches_a_lossy_sut() {
    let make = || async {
        Ok(LossySut {
            inner: WalSut::new().await?,
            put_count: 0,
        })
    };
    let report = run_seeds(make, 0, 30, 60, Profile::Blocking)
        .await
        .expect("harness error");
    let failure = report
        .failure
        .expect("expected the lossy SUT (drops every 10th put) to be caught as a finding");
    assert!(
        matches!(failure.outcome, Outcome::Mismatch { .. }),
        "expected a Mismatch finding for the dropped put, got {:?}",
        failure.outcome
    );
}

/// The blocking profile (Put/Delete/Reopen/Crash, no Checkpoint) must be
/// green on current code: no findings, and a non-vacuous number of checks
/// performed.
#[tokio::test(flavor = "multi_thread")]
async fn blocking_profile_is_green_on_current_code() {
    let report = run_seeds(WalSut::new, 0, 20, 60, Profile::Blocking)
        .await
        .expect("harness error");
    assert!(
        report.failure.is_none(),
        "blocking profile found a failure on current code: {:?}",
        report.failure
    );
    assert!(
        report.checks > 0,
        "vacuous run: no key comparisons were performed"
    );
}

/// True only for STO-01's own shape: a minimized sequence containing a
/// `Checkpoint` after which an acknowledged key comes back missing. A checkpoint
/// that errors, or restores a stale value, is a different bug and must not keep
/// this tripwire green after STO-01 is fixed.
fn is_sto01(failure: &Failure) -> bool {
    failure.ops.iter().any(|op| matches!(op, Op::Checkpoint))
        && matches!(
            &failure.outcome,
            Outcome::Mismatch { mismatch, .. }
                if mismatch.expected.is_some() && mismatch.actual.is_none()
        )
}

/// Searches the discovery profile (which, unlike blocking, includes
/// Checkpoint) across successive seed ranges for an STO-01-shaped finding.
/// Advances past other findings rather than stopping at them, since
/// `run_seeds` returns at the first finding it hits. Both the seed range and
/// the number of skipped findings are bounded, so once STO-01 is fixed this
/// fails with the inversion message instead of a nextest timeout.
async fn find_sto01_finding() -> Option<Failure> {
    const OPS_LEN: usize = 60;
    const CHUNK: u64 = 25;
    // STO-01 reproduces on nearly every seed; 300 keeps a wide margin while a
    // fixed build exhausts the budget in seconds, not minutes.
    const MAX_SEED: u64 = 300;
    const MAX_SKIPPED_FINDINGS: usize = 20;

    let mut start = 0u64;
    let mut skipped = 0usize;
    while start < MAX_SEED {
        let report = run_seeds(WalSut::new, start, CHUNK, OPS_LEN, Profile::Discovery)
            .await
            .expect("harness error");
        match report.failure {
            Some(failure) if is_sto01(&failure) => return Some(failure),
            Some(failure) => {
                skipped += 1;
                assert!(
                    skipped <= MAX_SKIPPED_FINDINGS,
                    "discovery keeps finding non-STO-01 failures (last: seed {} {:?}); \
                     these are new findings, record them in the ledger",
                    failure.seed,
                    failure.outcome
                );
                start = failure.seed + 1;
            }
            None => start += CHUNK,
        }
    }
    None
}

/// STO-01 tripwire: the discovery profile must reproduce checkpoint recovery
/// dropping pre-checkpoint keys, with a minimized sequence containing a
/// `Checkpoint`. This test is meant to fail the moment STO-01 is fixed, at
/// which point it must be inverted into a regression test (see
/// docs/remediation/ledger.toml, STO-01).
#[tokio::test(flavor = "multi_thread")]
async fn sto01_discovery_profile_finds_checkpoint_loss_tripwire() {
    match find_sto01_finding().await {
        // Keep the reproducer visible so inverting this tripwire has it ready.
        Some(failure) => eprintln!(
            "STO-01 still reproduces: seed {} ops {:?}",
            failure.seed, failure.ops
        ),
        None => panic!("STO-01 appears fixed: invert this tripwire"),
    }
}
