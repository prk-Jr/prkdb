# Mutation Survivors and h2 Advisory Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Kill the three WAL mutation survivors repeated by nightly CI and upgrade the vulnerable `h2` lockfile entry so mutation and security jobs pass.

**Architecture:** Keep production WAL behavior unchanged and strengthen its private unit-test contract at the exact constructor and publication seams the mutants alter. Update only the transitive `h2` lockfile resolution, with cargo-deny and cargo-audit as acceptance checks.

**Tech Stack:** Rust 1.95, Tokio, cargo-mutants, cargo-deny, cargo-audit, Cargo workspaces.

---

## File structure

- Modify `crates/prkdb/src/storage/wal_adapter.rs`: add private unit regression tests beside the existing WAL adapter tests; do not refactor production code unless a test reveals a real defect.
- Modify `Cargo.lock`: resolve `h2` to a patched 0.4.x release accepted by the existing manifests.
- Reference `deny.toml`: do not add an exception; remove an existing ignored advisory only if the lockfile update makes cargo-deny identify it as stale.

### Task 1: Prove the replication constructor preserves its WAL configuration

**Files:**
- Modify: `crates/prkdb/src/storage/wal_adapter.rs` in the `#[cfg(test)] mod tests` block
- Test: `crates/prkdb/src/storage/wal_adapter.rs`

- [ ] **Step 1: Add the focused constructor regression test**

Add the import `use prkdb_types::replication::ReplicationConfig;` to the test module, then add:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn replication_constructor_uses_the_supplied_wal_config() {
    let dir = tempfile::tempdir().expect("temporary root");
    let log_dir = dir.path().join("replicated-wal");
    let config = WalConfig {
        log_dir: log_dir.clone(),
        segment_count: 2,
        ..WalConfig::test_config()
    };
    let replication = ReplicationManager::new(ReplicationConfig::test_config())
        .await
        .expect("empty replica list needs no network");

    let adapter = WalStorageAdapter::new_with_replication(config, replication)
        .await
        .expect("replicated adapter opens");

    assert_eq!(adapter.inner._config.wal.log_dir, log_dir);
    assert_eq!(adapter.inner._config.wal.segment_count, 2);
    assert!(log_dir.join("mmap_segment_0").is_dir());
    assert!(log_dir.join("mmap_segment_1").is_dir());
}
```

- [ ] **Step 2: Run the exact constructor mutant and verify RED**

Use cargo-mutants so the source edit and the mutant's default relative `prkdb_data` directory stay
inside cargo-mutants' isolated build copy. First prove the filter selects only the reported mutant,
then run it:

```bash
cargo mutants --list \
  --package prkdb \
  --file crates/prkdb/src/storage/wal_adapter.rs \
  --re 'delete field wal.*new_with_replication' \
  --shard 0/3
cargo mutants --no-shuffle --timeout 600 \
  --package prkdb \
  --file crates/prkdb/src/storage/wal_adapter.rs \
  --re 'delete field wal.*new_with_replication' \
  --shard 0/3 \
  -- --lib \
  storage::wal_adapter::tests::replication_constructor_uses_the_supplied_wal_config \
  -- --exact
```

Expected: the list contains exactly the reported `wal` field-deletion mutant, and the run reports
it CAUGHT because the new test fails when the stored WAL path becomes `prkdb_data`.

- [ ] **Step 3: Verify GREEN on the correct implementation**

Run from the repository root:

```bash
cargo test -p prkdb --lib \
  storage::wal_adapter::tests::replication_constructor_uses_the_supplied_wal_config -- --exact
```

Expected: PASS.

- [ ] **Step 4: Commit the constructor regression**

```bash
git add crates/prkdb/src/storage/wal_adapter.rs
git commit -m "test: cover replicated WAL configuration"
```

### Task 2: Prove queued puts and deletes retain collection identity

**Files:**
- Modify: `crates/prkdb/src/storage/wal_adapter.rs` in the `#[cfg(test)] mod tests` block
- Test: `crates/prkdb/src/storage/wal_adapter.rs`

- [ ] **Step 1: Add the collection-routing regression test**

Add this test near `a_mixed_batch_applies_its_puts_and_deletes_in_order`:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn queued_puts_and_deletes_keep_their_collection_names() {
    let dir = tempfile::tempdir().expect("temporary WAL directory");
    let adapter = WalStorageAdapter::new(WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    })
    .expect("adapter opens");

    {
        let tasks = adapter.inner.writer.get().expect("the writer was spawned");
        tasks.supervisor.abort();
        tasks.flush_loop.abort();
    }

    let (put, _put_rx) = PendingWrite::new(LogRecord::new(LogOperation::Put {
        collection: "orders".to_string(),
        id: b"order-1".to_vec(),
        data: b"open".to_vec(),
    }));
    let (delete, _delete_rx) = PendingWrite::new(LogRecord::new(LogOperation::Delete {
        collection: "archive".to_string(),
        id: b"old-1".to_vec(),
    }));

    assert_eq!(
        WalStorageAdapter::publish_batch(&adapter.inner, vec![put, delete]).await,
        2
    );

    let records = adapter.inner.wal.scan().await.expect("scan published WAL");
    let mut routed = records
        .into_iter()
        .map(|(_, record)| match record.operation {
            LogOperation::PutBatch { collection, .. } => ("put", collection),
            LogOperation::DeleteBatch { collection, .. } => ("delete", collection),
            operation => panic!("unexpected published operation: {operation:?}"),
        })
        .collect::<Vec<_>>();
    routed.sort();

    assert_eq!(
        routed,
        vec![
            ("delete", "archive".to_string()),
            ("put", "orders".to_string()),
        ]
    );
}
```

- [ ] **Step 2: Run the exact put-routing mutant and verify RED**

First prove the filter selects only the reported mutant, then run it in cargo-mutants' isolated
copy:

```bash
cargo mutants --list \
  --package prkdb \
  --file crates/prkdb/src/storage/wal_adapter.rs \
  --re 'delete match arm LogOperation::Put.*publish_batch' \
  --shard 3/4
cargo mutants --no-shuffle --timeout 600 \
  --package prkdb \
  --file crates/prkdb/src/storage/wal_adapter.rs \
  --re 'delete match arm LogOperation::Put.*publish_batch' \
  --shard 3/4 \
  -- --lib \
  storage::wal_adapter::tests::queued_puts_and_deletes_keep_their_collection_names \
  -- --exact
```

Expected: exactly one listed mutant and a CAUGHT result caused by the put being published under
the empty collection.

- [ ] **Step 3: Run the exact delete-routing mutant and verify RED**

Repeat with the delete-arm filter:

```bash
cargo mutants --list \
  --package prkdb \
  --file crates/prkdb/src/storage/wal_adapter.rs \
  --re 'delete match arm LogOperation::Delete.*publish_batch' \
  --shard 3/4
cargo mutants --no-shuffle --timeout 600 \
  --package prkdb \
  --file crates/prkdb/src/storage/wal_adapter.rs \
  --re 'delete match arm LogOperation::Delete.*publish_batch' \
  --shard 3/4 \
  -- --lib \
  storage::wal_adapter::tests::queued_puts_and_deletes_keep_their_collection_names \
  -- --exact
```

Expected: exactly one listed mutant and a CAUGHT result caused by the delete being published under
the empty collection.

- [ ] **Step 4: Verify GREEN on the correct implementation**

```bash
cargo test -p prkdb --lib \
  storage::wal_adapter::tests::queued_puts_and_deletes_keep_their_collection_names -- --exact
```

Expected: PASS.

- [ ] **Step 5: Run both new regressions together**

```bash
cargo test -p prkdb --lib supplied_wal_config
cargo test -p prkdb --lib keep_their_collection_names
```

Expected: both commands PASS.

- [ ] **Step 6: Commit the routing regression**

```bash
git add crates/prkdb/src/storage/wal_adapter.rs
git commit -m "test: preserve collections in queued WAL batches"
```

### Task 3: Upgrade the vulnerable h2 lockfile entry

**Files:**
- Modify: `Cargo.lock`
- Possibly modify: `deny.toml` only if cargo-deny reports an existing ignore as stale after the update

- [ ] **Step 1: Confirm the vulnerable baseline**

```bash
cargo tree -i h2@0.4.13 --locked
cargo deny check advisories
```

Expected: the tree contains `h2 0.4.13`, and cargo-deny reports `RUSTSEC-2026-0258`.

- [ ] **Step 2: Update only h2**

```bash
cargo update -p h2@0.4.13 --precise 0.4.16
```

Expected: `Cargo.lock` changes from `h2 0.4.13` to `h2 0.4.16` plus only dependency metadata
required by that resolution. Do not accept unrelated package upgrades.

- [ ] **Step 3: Verify the patched resolution and scanners**

```bash
cargo tree -i h2@0.4.16 --locked
cargo deny check
cargo audit
```

Expected: `h2 0.4.16` is selected; `RUSTSEC-2026-0258` is absent; both scanners exit zero. If
cargo-deny reports an ignored advisory as stale, remove only that stale ignore from `deny.toml`
and rerun both scanners.

- [ ] **Step 4: Commit the dependency remediation**

```bash
git add Cargo.lock deny.toml
git commit -m "fix: update h2 for RUSTSEC-2026-0258"
```

If `deny.toml` is unchanged, omit it from `git add`.

### Task 4: Verify the complete branch

**Files:**
- Verify: `crates/prkdb/src/storage/wal_adapter.rs`
- Verify: `Cargo.lock`
- Verify: `deny.toml` if changed

- [ ] **Step 1: Format and lint**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: both commands exit zero with no warnings.

- [ ] **Step 2: Run the relevant library suite**

```bash
cargo test -p prkdb --lib
```

Expected: PASS.

- [ ] **Step 3: Run the workspace suite**

```bash
cargo test --workspace
```

Expected: PASS.

- [ ] **Step 4: Inspect final scope**

```bash
git status --short
git diff main...HEAD --stat
git diff main...HEAD --check
```

Expected: only the reviewed spec, this plan, WAL regression tests, `Cargo.lock`, and possibly the
single justified `deny.toml` cleanup are changed. The pre-existing untracked watchdog plan remains
unmodified and uncommitted.

- [ ] **Step 5: Commit any formatting-only adjustment**

If formatting changed tracked source after the earlier commits:

```bash
git add crates/prkdb/src/storage/wal_adapter.rs
git commit -m "style: format WAL regression tests"
```

Otherwise, do not create an empty commit.
