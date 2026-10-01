//! Phase 1 gate tests (Task 1.8): prove the crash/restart harness can itself
//! fail, that the blocking profile is green on current code, and that the
//! discovery profile no longer finds STO-01 (checkpoint recovery dropped
//! pre-checkpoint keys until Task 2.8a).
//!
//! These are NOT the harness unit self-tests in `self_test.rs` (which prove
//! the runner/checker/minimizer correctly detect and shrink each class of
//! finding using small, fast, deterministic wrappers). This file exercises
//! the real WAL storage end to end and is the actual Phase 1 gate. From Task
//! 2.10b it runs the WAL adapter on `FaultFs` (`FaultSut`), so the blocking
//! profile's `PowerLoss` ops are real simulated power cuts, in both Durable and
//! Fast mode.

use prkdb_verify::faultfs::Tear;
use prkdb_verify::model::{Key, Mode, Value};
use prkdb_verify::ops::{Op, Profile};
use prkdb_verify::runner::{run, run_seeds, Failure, Outcome, RunConfig};
use prkdb_verify::sut::{FaultSut, Sut};

/// Wraps a `FaultSut`, silently dropping every 10th `put` (acking it to the
/// caller/model without ever writing it to storage) — a lossy implementation
/// the harness must catch. This is the Phase 1 gate's meta-test: if the
/// harness can't catch this, it can't be trusted to catch anything else.
struct LossySut {
    inner: FaultSut,
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
    async fn power_loss(&mut self, tear: Tear, fault_seed: u64) -> anyhow::Result<()> {
        self.inner.power_loss(tear, fault_seed).await
    }
}

/// The Phase 1 gate's meta-test: a SUT that drops every 10th put must be
/// caught by the harness as a finding. If this test can't fail, the harness
/// can't be trusted to gate anything else.
#[tokio::test(flavor = "multi_thread")]
async fn meta_harness_catches_a_lossy_sut() {
    let make = || async {
        Ok(LossySut {
            inner: FaultSut::new(Mode::Durable).await?,
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

/// The blocking profile (Put/Delete/Reopen/Crash/PowerLoss, no Checkpoint)
/// must be green on current code in Durable mode: no findings, a non-vacuous
/// number of checks performed, and at least one power loss survived.
#[tokio::test(flavor = "multi_thread")]
async fn blocking_profile_is_green_on_current_code() {
    let cfg = RunConfig {
        first_seed: 0,
        seeds: 20,
        ops: 60,
        profile: Profile::Blocking,
        mode: Mode::Durable,
        repro_attempts: 1,
    };
    let report = run(|| FaultSut::new(Mode::Durable), &cfg)
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
    assert!(
        report.op_counts.get("PowerLoss").copied().unwrap_or(0) > 0,
        "PowerLoss never ran: {:?}",
        report.op_counts
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

/// STO-01 regression (was the discovery tripwire): the discovery profile, which includes
/// `Checkpoint`, finds no checkpoint-shaped loss.
#[tokio::test(flavor = "multi_thread")]
async fn discovery_profile_checkpoint_keeps_every_key() {
    // Discovery includes `PowerLoss`, which only the `FaultFs`-backed SUT supports.
    let report = run_seeds(
        || FaultSut::new(Mode::Durable),
        0,
        100,
        60,
        Profile::Discovery,
    )
    .await
    .expect("harness error");
    if let Some(f) = &report.failure {
        assert!(
            !is_sto01(f),
            "STO-01 is back: seed {} ops {:?}",
            f.seed,
            f.ops
        );
        panic!("discovery found a different failure; record it in the ledger: {f:?}");
    }
    assert!(report.checks > 0, "vacuous run");
}

/// Op coverage is reported: a green run must show which op kinds actually
/// executed, so a disabled op can't hide behind a passing report.
#[tokio::test(flavor = "multi_thread")]
async fn report_counts_ops_per_kind() {
    let report = run_seeds(
        || FaultSut::new(Mode::Durable),
        0,
        20,
        60,
        Profile::Blocking,
    )
    .await
    .expect("harness error");
    assert!(report.failure.is_none(), "{:?}", report.failure);
    assert!(report.op_counts.get("Put").copied().unwrap_or(0) > 0);
    assert!(report.op_counts.get("Reopen").copied().unwrap_or(0) > 0);
    assert!(
        report.missing_op_kinds(Profile::Blocking).is_empty(),
        "blocking kinds never executed: {:?} (counts {:?})",
        report.missing_op_kinds(Profile::Blocking),
        report.op_counts
    );
    // Discovery also enables Checkpoint, which a blocking run never executes.
    assert_eq!(
        report.missing_op_kinds(Profile::Discovery),
        vec!["Checkpoint"]
    );
    let formatted = report.format_op_counts();
    assert!(formatted.starts_with("Put:"), "{formatted}");
    assert!(!formatted.contains("Checkpoint"), "{formatted}");
}

/// A Fast-mode SUT that loses data it had already synced must be caught: the checker
/// may only accept prefixes that start at the last durable point.
struct ForgetsSyncedKey {
    inner: FaultSut,
}

#[async_trait::async_trait]
impl Sut for ForgetsSyncedKey {
    async fn put(&mut self, k: &Key, v: &Value) -> anyhow::Result<()> {
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
    async fn power_loss(&mut self, tear: Tear, fault_seed: u64) -> anyhow::Result<()> {
        self.inner.power_loss(tear, fault_seed).await?;
        // Simulates a WAL that discarded synced data: key 0 vanishes whatever the model says.
        self.inner.delete(&prkdb_verify::ops::key(0)).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn fast_mode_checker_catches_lost_synced_data() {
    let cfg = RunConfig {
        first_seed: 0,
        seeds: 40,
        ops: 60,
        profile: Profile::Blocking,
        mode: Mode::Fast,
        repro_attempts: 1,
    };
    let report = run(
        || async {
            Ok(ForgetsSyncedKey {
                inner: FaultSut::new(Mode::Fast).await?,
            })
        },
        &cfg,
    )
    .await
    .expect("harness error");
    let f = report
        .failure
        .expect("a SUT that loses synced data must be caught in Fast mode");
    assert!(
        f.ops.iter().any(|o| matches!(o, Op::PowerLoss { .. })),
        "{:?}",
        f.ops
    );
}

#[test]
fn blocking_profile_includes_power_loss() {
    assert!(
        (0..20).any(|s| prkdb_verify::ops::generate(s, 200, Profile::Blocking)
            .iter()
            .any(|o| matches!(o, Op::PowerLoss { .. })))
    );
    assert!(
        (0..50).all(|s| !prkdb_verify::ops::generate(s, 200, Profile::Core)
            .iter()
            .any(|o| matches!(o, Op::PowerLoss { .. }))),
        "Core stays the Phase 1 op set"
    );
}

/// The blocking profile must be green in Fast mode too: whatever a power loss
/// takes, what survives is one prefix of the acknowledged writes, no shorter
/// than the last point the model knows was synced.
#[tokio::test(flavor = "multi_thread")]
async fn blocking_profile_is_green_in_fast_mode() {
    let cfg = RunConfig {
        first_seed: 0,
        seeds: 20,
        ops: 60,
        profile: Profile::Blocking,
        mode: Mode::Fast,
        repro_attempts: 1,
    };
    let report = run(|| FaultSut::new(Mode::Fast), &cfg)
        .await
        .expect("harness error");
    assert!(report.failure.is_none(), "{:?}", report.failure);
    assert!(
        report.op_counts.get("PowerLoss").copied().unwrap_or(0) > 0,
        "PowerLoss never ran: {:?}",
        report.op_counts
    );
}
