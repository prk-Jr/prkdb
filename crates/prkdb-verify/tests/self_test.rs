//! Unit-level self-tests of the runner/checker/minimizer: wrap the real
//! `WalSut` with a deliberately injected bug and confirm `run_seeds` reports
//! the right kind of finding, with a short minimized reproduction.
//!
//! These are NOT the Phase 1 gate tests (Task 1.8's meta-test and
//! blocking/discovery runs) — those exercise the real WAL storage end to end.
//! These self-tests exist purely to prove the harness itself (runner,
//! checker, minimizer) correctly detects and shrinks each class of finding,
//! using small, fast, deterministic wrappers.

use prkdb_verify::model::{Key, Value};
use prkdb_verify::ops::{key, Profile};
use prkdb_verify::runner::{run_seeds, Outcome};
use prkdb_verify::sut::{Sut, WalSut};

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
