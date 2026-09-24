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
use prkdb_verify::model::{Key, Value};
use prkdb_verify::ops::{generate, key, Op, Profile, KEY_SPACE};
use prkdb_verify::runner::{run_ops, run_seeds, run_seeds_with, Outcome};
use prkdb_verify::sut::{Sut, WalSut};
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
    let report = run_seeds(make, 0, 30, 20, Profile::Blocking)
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
    let report = run_seeds(make, 0, 30, 20, Profile::Blocking)
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
    let report = run_seeds(make, 0, 30, 20, Profile::Blocking)
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
    let report = run_seeds(make, 0, 30, len, Profile::Blocking)
        .await
        .expect("harness error");
    let failure = report.failure.expect("expected a finding");

    let original_ops = generate(failure.seed, len, Profile::Blocking);
    let mut original_sut = CorruptsLastPutOnCrash {
        inner: WalSut::new().await.expect("fresh SUT"),
        last_put: None,
    };
    let original_outcome = run_ops(&mut original_sut, &original_ops)
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
            actual: Some(b"v".to_vec()),
        },
    };
    let same_key_different_shape = Outcome::Mismatch {
        at: 0,
        after: Op::Reopen,
        mismatch: Mismatch {
            key: key_a.clone(),
            expected: Some(b"v".to_vec()),
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
            key: key_a,
            expected: None,
            actual: Some(b"different-value".to_vec()),
        },
    };
    assert!(
        missing_expected_present_actual.same_kind(&same_everything),
        "same key and same shape must be the same kind, regardless of `at`/`after`/value"
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
    let report = run_seeds_with(make, 0, 30, 20, Profile::Blocking, 3)
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
    let report = run_seeds(make, 0, 30, 20, Profile::Blocking)
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
    let outcome = run_ops(&mut sut, &ops()).await.expect("harness error");
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
    let outcome = run_ops(&mut sut, &ops()).await.expect("harness error");
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
    let report = run_seeds(make, 0, 1, 1, Profile::Blocking)
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
