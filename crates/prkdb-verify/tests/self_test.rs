//! Unit-level self-tests of the runner/checker/minimizer: wrap the real
//! `WalSut` with a deliberately injected bug and confirm `run_seeds` reports
//! the right kind of finding, with a short minimized reproduction.
//!
//! These are NOT the Phase 1 gate tests (Task 1.8's meta-test and
//! blocking/discovery runs) — those exercise the real WAL storage end to end.
//! These self-tests exist purely to prove the harness itself (runner,
//! checker, minimizer) correctly detects and shrinks each class of finding,
//! using small, fast, deterministic wrappers.

use prkdb_verify::checker::Mismatch;
use prkdb_verify::faultfs::Tear;
use prkdb_verify::model::{Key, Mode, Value};
use prkdb_verify::ops::{generate, key, Op, Profile, KEY_SPACE};
use prkdb_verify::runner::{
    run, run_ops, run_seeds, run_seeds_with, Outcome, RunConfig, FINAL_REOPEN,
};
use prkdb_verify::sut::{FaultSut, Sut, Unsupported, WalSut};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Wraps a `WalSut`, deleting a fixed key straight out from under it on every
/// `crash`, regardless of what the model thinks happened — simulating an
/// implementation that loses a key across a crash.
struct LosesKeyOnCrash {
    inner: WalSut,
    lost: Key,
}

#[async_trait::async_trait]
impl Sut for LosesKeyOnCrash {
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
        self.inner.crash().await?;
        // Simulate the crash losing durability of `lost`, independent of
        // whatever the model still expects it to be.
        self.inner.delete(&self.lost).await
    }
    async fn checkpoint(&mut self) -> anyhow::Result<()> {
        self.inner.checkpoint().await
    }
}

/// Wraps a `WalSut`, re-writing a fixed key's last-known value back into
/// storage on every `crash` — simulating an implementation that resurrects a
/// deleted key after a crash.
struct ResurrectsDeletedKey {
    inner: WalSut,
    target: Key,
    last_value: Option<Value>,
}

#[async_trait::async_trait]
impl Sut for ResurrectsDeletedKey {
    async fn put(&mut self, k: &Key, v: &Value) -> anyhow::Result<()> {
        if *k == self.target {
            self.last_value = Some(v.clone());
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
        self.inner.crash().await?;
        if let Some(v) = self.last_value.clone() {
            self.inner.put(&self.target, &v).await?;
        }
        Ok(())
    }
    async fn checkpoint(&mut self) -> anyhow::Result<()> {
        self.inner.checkpoint().await
    }
}

/// Wraps a `WalSut`, always returning the *first* value ever written to a
/// fixed key instead of the latest one — simulating an implementation that
/// serves a stale value after an overwrite.
struct ReturnsStaleValue {
    inner: WalSut,
    target: Key,
    first_value: Option<Value>,
}

#[async_trait::async_trait]
impl Sut for ReturnsStaleValue {
    async fn put(&mut self, k: &Key, v: &Value) -> anyhow::Result<()> {
        if *k == self.target && self.first_value.is_none() {
            self.first_value = Some(v.clone());
        }
        self.inner.put(k, v).await
    }
    async fn delete(&mut self, k: &Key) -> anyhow::Result<()> {
        self.inner.delete(k).await
    }
    async fn get(&mut self, k: &Key) -> anyhow::Result<Option<Value>> {
        if *k == self.target {
            Ok(self.first_value.clone())
        } else {
            self.inner.get(k).await
        }
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

/// Wraps a `WalSut` whose `reopen` always fails — simulating an
/// implementation that can't come back up.
struct FailsToReopen {
    inner: WalSut,
}

#[async_trait::async_trait]
impl Sut for FailsToReopen {
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
        anyhow::bail!("simulated reopen failure")
    }
    async fn crash(&mut self) -> anyhow::Result<()> {
        self.inner.crash().await
    }
    async fn checkpoint(&mut self) -> anyhow::Result<()> {
        self.inner.checkpoint().await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn detects_and_minimizes_lost_key_on_crash() {
    let lost = key(0);
    let make = {
        let lost = lost.clone();
        move || {
            let lost = lost.clone();
            async move {
                Ok(LosesKeyOnCrash {
                    inner: WalSut::new().await?,
                    lost,
                })
            }
        }
    };
    let report = run_seeds(make, 0, 30, 20, Profile::Core)
        .await
        .expect("harness error");
    let failure = report.failure.expect("expected a finding");
    match &failure.outcome {
        Outcome::Mismatch { mismatch, .. } => assert_eq!(mismatch.key, lost),
        other => panic!("expected a Mismatch on the lost key, got {other:?}"),
    }
    assert!(
        failure.ops.len() <= 4,
        "expected the minimizer to shrink to <= 4 ops, got {}: {:?}",
        failure.ops.len(),
        failure.ops
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn detects_resurrected_deleted_key() {
    let target = key(1);
    let make = {
        let target = target.clone();
        move || {
            let target = target.clone();
            async move {
                Ok(ResurrectsDeletedKey {
                    inner: WalSut::new().await?,
                    target,
                    last_value: None,
                })
            }
        }
    };
    let report = run_seeds(make, 0, 30, 20, Profile::Core)
        .await
        .expect("harness error");
    let failure = report.failure.expect("expected a finding");
    match &failure.outcome {
        Outcome::Mismatch { mismatch, .. } => assert_eq!(mismatch.key, target),
        other => panic!("expected a Mismatch on the resurrected key, got {other:?}"),
    }
    assert!(!failure.ops.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn detects_stale_value() {
    let target = key(2);
    let make = {
        let target = target.clone();
        move || {
            let target = target.clone();
            async move {
                Ok(ReturnsStaleValue {
                    inner: WalSut::new().await?,
                    target,
                    first_value: None,
                })
            }
        }
    };
    let report = run_seeds(make, 0, 30, 20, Profile::Core)
        .await
        .expect("harness error");
    let failure = report.failure.expect("expected a finding");
    match &failure.outcome {
        Outcome::Mismatch { mismatch, .. } => assert_eq!(mismatch.key, target),
        other => panic!("expected a Mismatch on the stale key, got {other:?}"),
    }
    assert!(!failure.ops.is_empty());
}

/// Wraps a `WalSut`, deleting whichever key was *last put* right before a
/// crash — unlike `LosesKeyOnCrash`'s fixed key, the buggy key here depends
/// on the op sequence itself, so a naive shrink that changes what was last
/// put before a crash would drift onto a mismatch for a *different* key.
struct CorruptsLastPutOnCrash {
    inner: WalSut,
    last_put: Option<Key>,
}

#[async_trait::async_trait]
impl Sut for CorruptsLastPutOnCrash {
    async fn put(&mut self, k: &Key, v: &Value) -> anyhow::Result<()> {
        self.last_put = Some(k.clone());
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
        self.inner.crash().await?;
        if let Some(k) = self.last_put.clone() {
            self.inner.delete(&k).await?;
        }
        Ok(())
    }
    async fn checkpoint(&mut self) -> anyhow::Result<()> {
        self.inner.checkpoint().await
    }
}

/// Wraps a `WalSut`, losing a fixed key on crash only every *other* time
/// `crash` is called globally (a shared counter, not per-instance) —
/// simulating a flaky, nondeterministic bug: fresh SUT instances built by the
/// minimizer's `reproduces` won't all see the bug fire.
struct FlakyLosesKeyOnCrash {
    inner: WalSut,
    lost: Key,
    counter: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Sut for FlakyLosesKeyOnCrash {
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
        self.inner.crash().await?;
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        if n.is_multiple_of(2) {
            self.inner.delete(&self.lost).await?;
        }
        Ok(())
    }
    async fn checkpoint(&mut self) -> anyhow::Result<()> {
        self.inner.checkpoint().await
    }
}

/// Wraps a `WalSut`, always failing a `put` for one fixed key.
struct FailsPutOnKey {
    inner: WalSut,
    target: Key,
}

#[async_trait::async_trait]
impl Sut for FailsPutOnKey {
    async fn put(&mut self, k: &Key, v: &Value) -> anyhow::Result<()> {
        if *k == self.target {
            anyhow::bail!("simulated put failure on target key");
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

/// N1 drift guard: the minimizer must never accept a shrunk sequence that
/// reproduces a *different* finding (here, a mismatch on a different key, or
/// the same key with a different present/missing shape) than the original.
/// `CorruptsLastPutOnCrash`'s buggy key moves depending on the op sequence,
/// so a minimizer without this guard could easily drift.
///
/// This compares the *reported* failure against the ORIGINAL, un-minimized
/// outcome (recovered independently by re-running the generator's own
/// sequence for `failure.seed`) rather than against `failure.outcome`
/// itself — comparing a value against itself would trivially pass even if
/// the minimizer had drifted.
#[tokio::test(flavor = "multi_thread")]
async fn minimizer_never_accepts_a_drifted_finding() {
    let len = 30;
    let make = || async {
        Ok(CorruptsLastPutOnCrash {
            inner: WalSut::new().await?,
            last_put: None,
        })
    };
    let report = run_seeds(make, 0, 30, len, Profile::Core)
        .await
        .expect("harness error");
    let failure = report.failure.expect("expected a finding");

    let original_ops = generate(failure.seed, len, Profile::Core);
    let mut original_sut = CorruptsLastPutOnCrash {
        inner: WalSut::new().await.expect("fresh SUT"),
        last_put: None,
    };
    let original_outcome = run_ops(&mut original_sut, &original_ops, Mode::Durable)
        .await
        .expect("harness error");
    let (original_key, original_shape) = match &original_outcome {
        Outcome::Mismatch { mismatch, .. } => (
            mismatch.key.clone(),
            (mismatch.expected.is_some(), mismatch.actual.is_some()),
        ),
        other => panic!(
            "expected the original (un-minimized) sequence for seed {} to reproduce a Mismatch, got {other:?}",
            failure.seed
        ),
    };

    match &failure.outcome {
        Outcome::Mismatch { mismatch, .. } => {
            assert_eq!(
                mismatch.key, original_key,
                "minimizer drifted onto a mismatch for a different key"
            );
            assert_eq!(
                (mismatch.expected.is_some(), mismatch.actual.is_some()),
                original_shape,
                "minimizer drifted onto a mismatch with a different shape"
            );
        }
        other => panic!("expected a Mismatch, got {other:?}"),
    }
}

/// Unit test of `Outcome::same_kind` itself, independent of the runner: pins
/// down exactly which pairs of findings count as "the same bug" (N1).
#[test]
fn same_kind_requires_matching_key_and_shape_or_op_discriminant() {
    let key_a = key(1);
    let key_b = key(2);

    let missing_expected_present_actual = Outcome::Mismatch {
        at: 0,
        after: Op::Reopen,
        mismatch: Mismatch {
            key: key_a.clone(),
            expected: None,
            acceptable: vec![None],
            actual: Some(b"v".to_vec()),
        },
    };
    let same_key_different_shape = Outcome::Mismatch {
        at: 0,
        after: Op::Reopen,
        mismatch: Mismatch {
            key: key_a.clone(),
            expected: Some(b"v".to_vec()),
            acceptable: vec![Some(b"v".to_vec())],
            actual: None,
        },
    };
    assert!(
        !missing_expected_present_actual.same_kind(&same_key_different_shape),
        "same key but different (expected, actual) presence shape must not be the same kind"
    );

    let different_key_same_shape = Outcome::Mismatch {
        at: 0,
        after: Op::Reopen,
        mismatch: Mismatch {
            key: key_b,
            expected: None,
            acceptable: vec![None],
            actual: Some(b"v".to_vec()),
        },
    };
    assert!(
        !missing_expected_present_actual.same_kind(&different_key_same_shape),
        "different key must not be the same kind even with the same shape"
    );

    let sut_error_put = Outcome::SutError {
        at: 0,
        op: Op::Put(key_a.clone(), b"v".to_vec()),
        error: "boom".to_string(),
    };
    let sut_error_reopen = Outcome::SutError {
        at: 0,
        op: Op::Reopen,
        error: "boom".to_string(),
    };
    assert!(
        !sut_error_put.same_kind(&sut_error_reopen),
        "a SutError on Put must not be the same kind as a SutError on Reopen"
    );

    let same_everything = Outcome::Mismatch {
        at: 7,
        after: Op::Crash,
        mismatch: Mismatch {
            key: key_a.clone(),
            expected: None,
            acceptable: vec![None],
            actual: Some(b"different-value".to_vec()),
        },
    };
    assert!(
        missing_expected_present_actual.same_kind(&same_everything),
        "same key and same shape must be the same kind, regardless of `at`/`after`/value"
    );

    // The acceptable set's presence shape counts too: "absent was the only
    // acceptable answer" is a different bug from "absent or present were both
    // acceptable" — but how many candidates there were is not (a shrink can
    // change how many prefixes were pending).
    let wider_acceptable = Outcome::Mismatch {
        at: 0,
        after: Op::Reopen,
        mismatch: Mismatch {
            key: key_a.clone(),
            expected: None,
            acceptable: vec![None, Some(b"old".to_vec())],
            actual: Some(b"v".to_vec()),
        },
    };
    assert!(
        !missing_expected_present_actual.same_kind(&wider_acceptable),
        "a different acceptable-set presence shape must not be the same kind"
    );
    let more_candidates_same_shape = Outcome::Mismatch {
        at: 0,
        after: Op::Reopen,
        mismatch: Mismatch {
            key: key_a,
            expected: None,
            acceptable: vec![None, None],
            actual: Some(b"v".to_vec()),
        },
    };
    assert!(
        missing_expected_present_actual.same_kind(&more_candidates_same_shape),
        "the number of acceptable candidates alone must not change the kind"
    );
}

/// A nondeterministic SUT bug, checked with `repro_attempts > 1`: some
/// `reproduces()` attempts inevitably won't see the bug fire (see
/// `FlakyLosesKeyOnCrash`). The minimizer must not panic over this — it
/// should simply treat those attempts as "didn't reproduce" and fall back to
/// keeping the op, eventually returning the original kind of failure.
#[tokio::test(flavor = "multi_thread")]
async fn nondeterministic_failure_with_repro_attempts_does_not_panic() {
    let lost = key(3);
    let counter = Arc::new(AtomicUsize::new(0));
    let make = {
        let lost = lost.clone();
        let counter = counter.clone();
        move || {
            let lost = lost.clone();
            let counter = counter.clone();
            async move {
                Ok(FlakyLosesKeyOnCrash {
                    inner: WalSut::new().await?,
                    lost,
                    counter,
                })
            }
        }
    };
    let report = run_seeds_with(make, 0, 30, 20, Profile::Core, 3)
        .await
        .expect("harness error should not panic or propagate");
    let failure = report
        .failure
        .expect("expected the flaky bug to be caught at least once");
    match &failure.outcome {
        Outcome::Mismatch { mismatch, .. } => assert_eq!(mismatch.key, lost),
        other => panic!("expected a Mismatch on the flaky lost key, got {other:?}"),
    }
}

/// A `put` that errors mid-sequence is reported as a `SutError` (with the
/// seed that found it) and minimizes down to essentially just that op.
#[tokio::test(flavor = "multi_thread")]
async fn detects_and_minimizes_put_sut_error() {
    let target = key(4);
    let make = {
        let target = target.clone();
        move || {
            let target = target.clone();
            async move {
                Ok(FailsPutOnKey {
                    inner: WalSut::new().await?,
                    target,
                })
            }
        }
    };
    let report = run_seeds(make, 0, 30, 20, Profile::Core)
        .await
        .expect("harness error");
    let failure = report.failure.expect("expected a finding");
    // The seed that found the bug is carried on the (minimized) failure.
    assert!(failure.seed < 30);
    match &failure.outcome {
        Outcome::SutError { op, .. } => {
            assert!(matches!(op, Op::Put(k, _) if *k == target));
        }
        other => panic!("expected a SutError on Put, got {other:?}"),
    }
    assert_eq!(
        failure.ops.len(),
        1,
        "expected the minimizer to shrink to exactly the failing Put, got {:?}",
        failure.ops
    );
}

/// Wraps a `WalSut`, deleting one fixed key on every `crash` — used to prove
/// the checker actually *compares* a touched, out-of-`KEY_SPACE` key rather
/// than merely counting it (see `checks_a_touched_key_outside_key_space`).
struct DropsTouchedKeyOnCrash {
    inner: WalSut,
    target: Key,
}

#[async_trait::async_trait]
impl Sut for DropsTouchedKeyOnCrash {
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
        self.inner.crash().await?;
        self.inner.delete(&self.target).await
    }
    async fn checkpoint(&mut self) -> anyhow::Result<()> {
        self.inner.checkpoint().await
    }
}

/// The checker must compare a key the model has `touched` even when that key
/// falls outside the generator's nominal `KEY_SPACE` — otherwise a bug that
/// only reaches an out-of-space key could never be caught. Feeds
/// `Model::touched` by hand-crafting an `Op::Put` for a key `key(i)` can
/// never produce (`i` is a `u8`, but `key_space` only ever draws `0..16`).
#[tokio::test(flavor = "multi_thread")]
async fn checks_a_touched_key_outside_key_space() {
    let outside: Key = vec![b'z', 200];
    assert!(
        !(0..KEY_SPACE).map(key).any(|k| k == outside),
        "test key must actually be outside the generator's key space"
    );
    let ops = || vec![Op::Put(outside.clone(), b"v".to_vec()), Op::Crash];

    // Passing case: a plain WalSut has no bug, so this passes. There are two
    // checkpoints (the mid-sequence Crash, and the implicit trailing Reopen
    // every sequence gets), each comparing the full nominal KEY_SPACE plus
    // the one touched out-of-space key — pin the exact count, not just a
    // lower bound, so this can't pass by coincidence.
    let mut sut = WalSut::new().await.expect("fresh SUT");
    let outcome = run_ops(&mut sut, &ops(), Mode::Durable)
        .await
        .expect("harness error");
    match outcome {
        Outcome::Pass { checks } => assert_eq!(
            checks,
            2 * (KEY_SPACE as usize + 1),
            "expected 2 checkpoints x (KEY_SPACE + the touched key)"
        ),
        other => panic!("expected Pass, got {other:?}"),
    }

    // Failing case: a SUT that loses the touched-but-out-of-space key across
    // a crash must be caught. If the checker only unioned `touched` into the
    // *count* without actually comparing those keys' values, this would
    // wrongly report Pass instead of Mismatch.
    let mut sut = DropsTouchedKeyOnCrash {
        inner: WalSut::new().await.expect("fresh SUT"),
        target: outside.clone(),
    };
    let outcome = run_ops(&mut sut, &ops(), Mode::Durable)
        .await
        .expect("harness error");
    match outcome {
        Outcome::Mismatch { mismatch, .. } => assert_eq!(mismatch.key, outside),
        other => panic!("expected a Mismatch on the touched out-of-space key, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn detects_reopen_failure() {
    let make = || async {
        Ok(FailsToReopen {
            inner: WalSut::new().await?,
        })
    };
    // A `reopen` that always fails must be caught even on an empty sequence,
    // because every run ends with an implicit trailing reopen.
    let report = run_seeds(make, 0, 1, 1, Profile::Core)
        .await
        .expect("harness error");
    let failure = report.failure.expect("expected a finding");
    match &failure.outcome {
        Outcome::SutError { op, .. } => {
            assert!(matches!(op, prkdb_verify::ops::Op::Reopen))
        }
        other => panic!("expected a SutError on Reopen, got {other:?}"),
    }
}

/// Wraps a `FaultSut` whose `checkpoint` is not implemented — a SUT the
/// discovery profile (which generates `Checkpoint`) asks too much of. It
/// forwards `power_loss`, so `Checkpoint` is the only op it cannot do.
struct NoCheckpoint {
    inner: FaultSut,
}

#[async_trait::async_trait]
impl Sut for NoCheckpoint {
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
        Err(Unsupported("checkpoint").into())
    }
    async fn power_loss(&mut self, tear: Tear, fault_seed: u64) -> anyhow::Result<()> {
        self.inner.power_loss(tear, fault_seed).await
    }
}

/// A profile asking for an op the SUT cannot do is a harness error (the run
/// is misconfigured), never a finding against the SUT: `run` returns `Err`
/// instead of a `Report` carrying a `Failure`.
#[tokio::test(flavor = "multi_thread")]
async fn an_unsupported_op_is_a_harness_error_not_a_finding() {
    let make = || async {
        Ok(NoCheckpoint {
            inner: FaultSut::new(Mode::Durable).await?,
        })
    };
    let config = RunConfig {
        first_seed: 0,
        seeds: 20,
        ops: 200,
        profile: Profile::Discovery,
        mode: Mode::Durable,
        repro_attempts: 1,
    };
    let err = run(make, &config)
        .await
        .expect_err("an Unsupported op must surface as a harness error, not a Report");
    let text = format!("{err:#}");
    assert!(
        text.contains("does not support checkpoint"),
        "unexpected harness error text: {text}"
    );
    assert!(
        text.contains("profile mismatch at op"),
        "harness error lost its context: {text}"
    );
    // The typed cause survives the added context, so callers can match on it.
    let cause = err
        .downcast_ref::<Unsupported>()
        .expect("the harness error must keep Unsupported as its typed cause");
    assert_eq!(cause.0, "checkpoint");
}

/// `WalSut` (on the real filesystem) cannot lose power, and the blocking
/// profile generates `PowerLoss` from Task 2.10b on: running it there is a
/// misconfigured run (a harness error), never a finding against the SUT.
#[tokio::test(flavor = "multi_thread")]
async fn a_sut_without_power_loss_is_a_harness_error_under_blocking() {
    let config = RunConfig {
        first_seed: 0,
        seeds: 20,
        ops: 200,
        profile: Profile::Blocking,
        mode: Mode::Durable,
        repro_attempts: 1,
    };
    let err = run(WalSut::new, &config)
        .await
        .expect_err("an unsupported power_loss must be a harness error, not a finding");
    let text = format!("{err:#}");
    assert!(
        text.contains("does not support power_loss"),
        "unexpected harness error text: {text}"
    );
    assert_eq!(
        err.downcast_ref::<Unsupported>().map(|u| u.0),
        Some("power_loss")
    );
}

/// What a [`MemSut`] keeps of its unsynced mutations when it loses power.
#[derive(Clone, Copy)]
enum Loss {
    /// Keeps exactly the first `n` unsynced mutations (a correct WAL).
    KeepPrefix(usize),
    /// Drops the first unsynced mutation and keeps every later one: a hole,
    /// which no WAL that writes in order can produce.
    Hole,
    /// Keeps the first `n` unsynced mutations, then overwrites `key(i)` with
    /// garbage: a value no prefix ever held.
    KeepPrefixCorrupting(usize, u8),
}

/// A tiny in-memory SUT with an explicit unsynced window, so the Fast-mode
/// power-loss check can be tested against exact, hand-picked survivals.
/// Restarts (`reopen`, `crash`) and `checkpoint` make everything durable.
struct MemSut {
    durable: BTreeMap<Key, Value>,
    unsynced: Vec<(Key, Option<Value>)>,
    loss: Loss,
}

impl MemSut {
    fn new(loss: Loss) -> Self {
        Self {
            durable: BTreeMap::new(),
            unsynced: Vec::new(),
            loss,
        }
    }

    fn apply(kv: &mut BTreeMap<Key, Value>, muts: &[(Key, Option<Value>)]) {
        for (k, v) in muts {
            match v {
                Some(v) => kv.insert(k.clone(), v.clone()),
                None => kv.remove(k),
            };
        }
    }

    fn sync(&mut self) {
        let muts = std::mem::take(&mut self.unsynced);
        Self::apply(&mut self.durable, &muts);
    }
}

#[async_trait::async_trait]
impl Sut for MemSut {
    async fn put(&mut self, k: &Key, v: &Value) -> anyhow::Result<()> {
        self.unsynced.push((k.clone(), Some(v.clone())));
        Ok(())
    }
    async fn delete(&mut self, k: &Key) -> anyhow::Result<()> {
        self.unsynced.push((k.clone(), None));
        Ok(())
    }
    async fn get(&mut self, k: &Key) -> anyhow::Result<Option<Value>> {
        let mut kv = self.durable.clone();
        Self::apply(&mut kv, &self.unsynced);
        Ok(kv.get(k).cloned())
    }
    async fn reopen(&mut self) -> anyhow::Result<()> {
        self.sync();
        Ok(())
    }
    async fn crash(&mut self) -> anyhow::Result<()> {
        self.sync();
        Ok(())
    }
    async fn checkpoint(&mut self) -> anyhow::Result<()> {
        self.sync();
        Ok(())
    }
    async fn power_loss(&mut self, _tear: Tear, _fault_seed: u64) -> anyhow::Result<()> {
        let muts = std::mem::take(&mut self.unsynced);
        let kept: Vec<_> = match self.loss {
            Loss::KeepPrefix(n) => muts.into_iter().take(n).collect(),
            Loss::Hole => muts.into_iter().skip(1).collect(),
            Loss::KeepPrefixCorrupting(n, i) => muts
                .into_iter()
                .take(n)
                .chain(std::iter::once((key(i), Some(b"garbage".to_vec()))))
                .collect(),
        };
        Self::apply(&mut self.durable, &kept);
        Ok(())
    }
    fn unsynced_acked(&self) -> Option<u64> {
        Some(self.unsynced.len() as u64)
    }
}

fn power_loss_op() -> Op {
    Op::PowerLoss {
        tear: Tear::None,
        fault_seed: 0,
    }
}

/// Task 2.10a review (a): the Fast check is ONE prefix over the whole
/// snapshot. Here key B's earlier write is lost while key A's later write
/// survives. Each key on its own matches *some* prefix (A from the full one,
/// B from the empty one), so a per-key check would accept it; no single
/// prefix gives both, so it must be a mismatch.
#[tokio::test(flavor = "multi_thread")]
async fn fast_check_needs_one_global_prefix_not_one_per_key() {
    let (a, b) = (key(1), key(2));
    let ops = vec![
        Op::Put(b.clone(), b"b1".to_vec()),
        Op::Put(a.clone(), b"a1".to_vec()),
        power_loss_op(),
    ];
    let mut sut = MemSut::new(Loss::Hole);
    let outcome = run_ops(&mut sut, &ops, Mode::Fast)
        .await
        .expect("harness error");
    match &outcome {
        Outcome::Mismatch {
            at: 2,
            after: Op::PowerLoss { .. },
            mismatch,
        } => {
            assert_eq!(
                mismatch.key, b,
                "B is the key that diverges from the full state"
            );
            assert_eq!(mismatch.actual, None);
            assert!(
                mismatch.acceptable.contains(&None),
                "B alone is acceptable in some prefix: {mismatch:?}"
            );
            assert!(mismatch.no_prefix_fits(), "{mismatch:?}");
        }
        other => panic!("a hole must be a mismatch at the power loss, got {other:?}"),
    }

    // The same SUT keeping a true prefix (B only) passes, and the model settles
    // on that prefix: the trailing reopen expects exactly B.
    for n in 0..=2 {
        let mut sut = MemSut::new(Loss::KeepPrefix(n));
        let outcome = run_ops(&mut sut, &ops, Mode::Fast)
            .await
            .expect("harness error");
        assert!(outcome.is_pass(), "prefix {n}: {outcome:?}");
    }
}

/// In Durable mode every ack is durable, so a power loss that keeps only a
/// prefix of the acknowledged writes is a finding, not an acceptable outcome.
#[tokio::test(flavor = "multi_thread")]
async fn durable_power_loss_check_requires_every_ack() {
    let ops = vec![
        Op::Put(key(1), b"a1".to_vec()),
        Op::Put(key(2), b"b1".to_vec()),
        power_loss_op(),
    ];
    let mut sut = MemSut::new(Loss::KeepPrefix(1));
    let outcome = run_ops(&mut sut, &ops, Mode::Durable)
        .await
        .expect("harness error");
    match &outcome {
        Outcome::Mismatch { mismatch, .. } => {
            assert_eq!(mismatch.key, key(2));
            assert_eq!(mismatch.acceptable, vec![Some(b"b1".to_vec())]);
        }
        other => panic!("expected a mismatch, got {other:?}"),
    }
    let mut sut = MemSut::new(Loss::KeepPrefix(2));
    assert!(run_ops(&mut sut, &ops, Mode::Durable)
        .await
        .expect("harness error")
        .is_pass());
}

/// Fast mode: a power loss right after a sync point (`Reopen`) may not take
/// anything from before it. Losing a write the model knows was synced is a
/// finding even though "nothing survived since the sync" is otherwise fine.
#[tokio::test(flavor = "multi_thread")]
async fn fast_check_never_goes_below_the_last_sync() {
    /// Forgets everything, synced or not, on power loss.
    struct ForgetsAll(MemSut);
    #[async_trait::async_trait]
    impl Sut for ForgetsAll {
        async fn put(&mut self, k: &Key, v: &Value) -> anyhow::Result<()> {
            self.0.put(k, v).await
        }
        async fn delete(&mut self, k: &Key) -> anyhow::Result<()> {
            self.0.delete(k).await
        }
        async fn get(&mut self, k: &Key) -> anyhow::Result<Option<Value>> {
            self.0.get(k).await
        }
        async fn reopen(&mut self) -> anyhow::Result<()> {
            self.0.reopen().await
        }
        async fn crash(&mut self) -> anyhow::Result<()> {
            self.0.crash().await
        }
        async fn checkpoint(&mut self) -> anyhow::Result<()> {
            self.0.checkpoint().await
        }
        async fn power_loss(&mut self, _tear: Tear, _fault_seed: u64) -> anyhow::Result<()> {
            self.0.durable.clear();
            self.0.unsynced.clear();
            Ok(())
        }
    }
    let ops = vec![
        Op::Put(key(1), b"a1".to_vec()),
        Op::Reopen,
        Op::Put(key(2), b"b1".to_vec()),
        power_loss_op(),
    ];
    let mut sut = ForgetsAll(MemSut::new(Loss::KeepPrefix(0)));
    let outcome = run_ops(&mut sut, &ops, Mode::Fast)
        .await
        .expect("harness error");
    match &outcome {
        Outcome::Mismatch { mismatch, .. } => {
            assert_eq!(mismatch.key, key(1));
            assert_eq!(mismatch.actual, None);
            assert!(!mismatch.acceptable.contains(&None), "{mismatch:?}");
            assert!(!mismatch.no_prefix_fits());
        }
        other => panic!("expected a mismatch on the synced key, got {other:?}"),
    }
}

/// The implicit trailing reopen every sequence gets is counted separately
/// from explicit `Reopen` ops, so a generator that stops emitting `Reopen`
/// is caught as vacuous for it instead of being masked by the final reopen.
#[tokio::test(flavor = "multi_thread")]
async fn trailing_reopen_does_not_mask_missing_explicit_reopens() {
    // A seed whose single generated op is not a Reopen.
    let seed = (0..100)
        .find(|&s| generate(s, 1, Profile::Core)[0] != Op::Reopen)
        .expect("some seed starts with a non-Reopen op");
    let config = RunConfig {
        first_seed: seed,
        seeds: 1,
        ops: 1,
        profile: Profile::Core,
        mode: Mode::Durable,
        repro_attempts: 1,
    };
    let report = run(WalSut::new, &config).await.expect("harness error");
    assert!(report.failure.is_none(), "{:?}", report.failure);
    assert_eq!(report.op_counts.get(FINAL_REOPEN).copied(), Some(1));
    assert_eq!(report.op_counts.get("Reopen"), None);
    assert!(
        report.missing_op_kinds(Profile::Core).contains(&"Reopen"),
        "a run with no explicit Reopen must be vacuous for Reopen: {:?}",
        report.op_counts
    );
    assert!(
        report
            .format_op_counts()
            .ends_with(&format!("{FINAL_REOPEN}:1")),
        "{}",
        report.format_op_counts()
    );
}

/// Task 2.10a review (d): a "no prefix fits" finding (every key's value is
/// acceptable on its own, but no single prefix gives them all) is about the
/// whole snapshot, so which key it reports depends on which prefix a shrink
/// leaves. `same_kind` matches such findings with each other regardless of
/// key, and never with an ordinary single-key mismatch.
#[test]
fn same_kind_treats_no_prefix_fits_findings_as_one_kind() {
    let lost = |k: Key, acceptable: Vec<Option<Value>>| Outcome::Mismatch {
        at: 0,
        after: power_loss_op(),
        mismatch: Mismatch {
            key: k,
            expected: Some(b"v".to_vec()),
            acceptable,
            actual: None,
        },
    };
    let hole_on_a = lost(key(1), vec![None, Some(b"v".to_vec())]);
    let hole_on_b = lost(key(2), vec![None, Some(b"v".to_vec())]);
    let plain_loss_on_a = lost(key(1), vec![Some(b"v".to_vec())]);

    let Outcome::Mismatch { mismatch, .. } = &hole_on_a else {
        unreachable!()
    };
    assert!(mismatch.no_prefix_fits());
    assert!(
        hole_on_a.same_kind(&hole_on_b),
        "two no-prefix-fits findings are the same kind whatever key they report"
    );
    assert!(
        !hole_on_a.same_kind(&plain_loss_on_a) && !plain_loss_on_a.same_kind(&hole_on_a),
        "a no-prefix-fits finding is not the same kind as a lost key on that key"
    );
}

/// Review M2: when a power loss both drops a legitimate unsynced suffix and
/// corrupts a key, the finding must name the corrupted key (its value is in
/// no prefix: single-key evidence), not the first legitimately-lost key.
#[tokio::test(flavor = "multi_thread")]
async fn fast_check_reports_the_key_no_prefix_explains() {
    let ops = vec![
        Op::Put(key(1), b"a1".to_vec()),
        Op::Put(key(2), b"b1".to_vec()),
        Op::Put(key(3), b"c1".to_vec()),
        power_loss_op(),
    ];
    // Keeps only k1 (legitimately losing k2 and k3), then corrupts k3.
    let mut sut = MemSut::new(Loss::KeepPrefixCorrupting(1, 3));
    let outcome = run_ops(&mut sut, &ops, Mode::Fast)
        .await
        .expect("harness error");
    match &outcome {
        Outcome::Mismatch { mismatch, .. } => {
            assert_eq!(mismatch.key, key(3), "{mismatch:?}");
            assert_eq!(mismatch.actual, Some(b"garbage".to_vec()));
            assert!(!mismatch.no_prefix_fits(), "{mismatch:?}");
        }
        other => panic!("expected a mismatch on the corrupted key, got {other:?}"),
    }
}

/// A WAL whose power loss reverts to the last clean reopen, so it loses
/// what its own close (in `crash`) had made durable, while claiming through
/// `unsynced_acked` that the close synced everything.
struct LosesCrashSyncedData {
    inner: MemSut,
    /// What really survives a power cut: the state as of the last reopen.
    really_durable: BTreeMap<Key, Value>,
    report_watermark: bool,
}

#[async_trait::async_trait]
impl Sut for LosesCrashSyncedData {
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
        self.inner.reopen().await?;
        self.really_durable = self.inner.durable.clone();
        Ok(())
    }
    async fn crash(&mut self) -> anyhow::Result<()> {
        self.inner.crash().await
    }
    async fn checkpoint(&mut self) -> anyhow::Result<()> {
        self.inner.checkpoint().await?;
        self.really_durable = self.inner.durable.clone();
        Ok(())
    }
    async fn power_loss(&mut self, _tear: Tear, _fault_seed: u64) -> anyhow::Result<()> {
        self.inner.durable = self.really_durable.clone();
        self.inner.unsynced.clear();
        Ok(())
    }
    fn unsynced_acked(&self) -> Option<u64> {
        if self.report_watermark {
            self.inner.unsynced_acked()
        } else {
            None
        }
    }
}

/// Review M1: the lower bound follows the SUT's own sync points. A `Crash`
/// closes the log (a sync), so a later power loss may not take what was
/// written before it. With the SUT reporting `unsynced_acked` this is caught;
/// without it the model only knows of `Reopen`/`Checkpoint` and must accept
/// the loss, which is why `FaultSut` reports it.
#[tokio::test(flavor = "multi_thread")]
async fn a_loss_of_data_synced_by_a_crash_is_caught() {
    let ops = vec![
        Op::Put(key(1), b"a1".to_vec()),
        Op::Crash,
        Op::Put(key(2), b"b1".to_vec()),
        power_loss_op(),
    ];
    let make = |report_watermark| LosesCrashSyncedData {
        inner: MemSut::new(Loss::KeepPrefix(0)),
        really_durable: BTreeMap::new(),
        report_watermark,
    };

    let outcome = run_ops(&mut make(true), &ops, Mode::Fast)
        .await
        .expect("harness error");
    match &outcome {
        Outcome::Mismatch {
            after: Op::PowerLoss { .. },
            mismatch,
            ..
        } => {
            assert_eq!(mismatch.key, key(1));
            assert_eq!(mismatch.actual, None);
            assert!(!mismatch.acceptable.contains(&None), "{mismatch:?}");
        }
        other => panic!("losing crash-synced data must be caught, got {other:?}"),
    }

    let blind = run_ops(&mut make(false), &ops, Mode::Fast)
        .await
        .expect("harness error");
    assert!(
        blind.is_pass(),
        "without the SUT's watermark the model cannot know the crash synced: {blind:?}"
    );
}
