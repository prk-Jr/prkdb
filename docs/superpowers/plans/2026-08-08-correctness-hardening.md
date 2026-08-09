# Correctness Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every correctness claim in PrkDB provable by a test that can actually fail, and make CI fail when the repository is broken.

**Architecture:** Work outside-in. First install the safety net (CI timeouts, pinned toolchain, green clippy) so subsequent work has trustworthy feedback. Then fix the tests that report green while testing nothing — starting with a meta-test that proves the linearizability checker can fail before we trust anything it says. Then remove the flakiness that makes the suite untrustworthy, then close hygiene gaps.

**Tech Stack:** Rust 2021, tokio, tonic, criterion, cargo-llvm-cov, cargo-deny, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-08-08-correctness-and-production-readiness.md` (revision 6)

**Decisions already made** (spec §0, resolved 2026-08-08) — no blockers remain:
- **D1 — Raft stays.** Tasks 3-7 and 17 are all in scope. Full ~9-day correctness effort.
- **D6 — in-process Wing & Gong checker**, not Elle. Task 4 builds it; no JVM in CI.
- **D5 — write an in-process cluster harness**, not a CI binary build. This is **new work and a
  prerequisite**, inserted as Task 4b below. Tasks 5, 17, and 19 all depend on it.

> **Revision 6 note.** Tasks 17-19 were added after a self-review. Task 1's job count and Tasks 5
> and 17's use of `TestCluster` were corrected after an independent review found they assumed
> APIs and a job count that do not match the repository. A third pass extended Task 13's scope to
> the storage-adapter crates, where `prkdb-storage-segmented/src/lib.rs:210-221` panics on a
> truncated segment rather than erroring. Revision 5 applies the §0 decisions: Task 4b (in-process
> harness) is new, and Task 4 now commits to WGL rather than offering a choice.

---

## Sequencing

Tasks 1-2 are the safety net and must land first — everything after depends on trustworthy CI.
Tasks 3-6 are the heart of the plan (R1). Tasks 7-16 can be parallelized once 1-2 are green.

**Task 4b (in-process cluster harness) is a hard prerequisite** for Tasks 5, 7, 17, and 19. It is
new work from D5 and there is no way around it — every cluster test below either uses it or stays
`#[ignore]`d, which is the state this plan exists to end.

Tasks 17-19 are appended rather than interleaved, to avoid renumbering. Their real position in
the order:

| Task | Belongs after | Why |
|---|---|---|
| **17** — verify the linearizable read mode (R14) | Task 5 | Needs the real checker and the cluster harness. |
| **18** — performance claim methodology (R15) | Task 16 | Extends the same `xtask` drift collector. |
| **19** — `#[ignore]` discipline (R16) | Task 9 | Task 9 surfaces the failures that Task 19 then classifies. |

Commit after every task. Do not batch.

---

## Task 1: CI safety net (R2)

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/chaos-tests.yml`
- Create: `rust-toolchain.toml`
- Modify: `Cargo.toml` (workspace package)

- [x] **Step 1: Verify the problem exists**

```bash
grep -c 'timeout-minutes' .github/workflows/ci.yml
```
Expected: `0` — no job can be interrupted, so a hang burns GitHub's 6-hour default.

- [x] **Step 2: Add a timeout to every job that can take one**

`ci.yml` defines **13** jobs. Twelve run steps directly and take `timeout-minutes` under
`runs-on`:

`repo-status-snapshot`, `check`, `fmt`, `clippy`, `test`, `schema-integration`,
`client-features-integration`, `client-features-integration-ts`,
`client-features-integration-go`, `mixed-client-integration`, `benchmark`,
`cross-lang-benchmark`.

Use 20 for the fast jobs, 45 for `test` and the integration jobs, 90 for the benchmark jobs.

```yaml
  test:
    name: Test
    runs-on: ubuntu-latest
    timeout-minutes: 45
    steps:
```

The thirteenth, `chaos-tests` (line 213), calls a reusable workflow:

```yaml
  chaos-tests:
    uses: ./.github/workflows/chaos-tests.yml
```

**GitHub Actions does not accept `timeout-minutes` on a job that uses `uses:`.** Adding it there
is a workflow syntax error. Timeouts for that job live inside `chaos-tests.yml`, which already
declares four of them — verify rather than add:

```bash
grep -c 'timeout-minutes' .github/workflows/chaos-tests.yml
```
Expected: `4`.

- [x] **Step 3: Verify**

```bash
grep -c 'timeout-minutes' .github/workflows/ci.yml
```
Expected: `12` at this point; `13` after Step 6 adds `nightly-check`. Never on `chaos-tests`.

> This count changes as later tasks add jobs (`nightly-check` in Step 6, `coverage` in Task 14,
> `security-audit` in Task 15, the bare-`#[ignore]` guard in Task 19). Treat 12 as the check for
> *this* task, not a permanent invariant. The durable rule is: every job with a `steps:` key has
> a `timeout-minutes`.

- [x] **Step 4: Pin the toolchain**

Create `rust-toolchain.toml`:

```toml
[toolchain]
channel = "1.95.0"
components = ["rustfmt", "clippy"]
```

- [x] **Step 5: Declare the MSRV**

In `Cargo.toml` under `[workspace.package]`, add:

```toml
rust-version = "1.95"
```

Then update the README badge from `Rust-1.70+` to `Rust-1.95+` — the old claim was never
verified by anything and is almost certainly false given the 2021 edition features in use.

- [x] **Step 6: Add a nightly early-warning job**

Append to `ci.yml`:

```yaml
  nightly-check:
    name: Nightly (advisory)
    runs-on: ubuntu-latest
    timeout-minutes: 30
    continue-on-error: true      # advisory only — never blocks a PR
    steps:
      - uses: actions/checkout@v4
      - name: Install Protoc
        run: sudo apt-get install -y protobuf-compiler
      - uses: dtolnay/rust-toolchain@nightly
        with:
          components: clippy
      - uses: Swatinem/rust-cache@v2
      - run: cargo clippy --workspace --all-targets -- -D warnings
```

> This is the *only* place `continue-on-error` is legitimate: it warns about a future toolchain
> without gating today's work. Task 9 removes the illegitimate use of it.

- [x] **Step 7: Commit**

```bash
git add .github/workflows/ci.yml rust-toolchain.toml Cargo.toml README.md
git commit -m "ci: add job timeouts, pin toolchain, declare MSRV"
```

---

## Task 2: Make clippy pass (R2)

**Files:**
- Modify: various (mechanical)

- [x] **Step 1: Confirm the failure**

```bash
cargo clippy --workspace --all-targets -- -D warnings ; echo "exit=$?"
```
Expected: `exit=101`, with 22 warnings — 15 `explicit call to .into_iter()`, 5 `this if can be
collapsed into the outer match`, 2 `consider using sort_by_key`.

- [x] **Step 2: Apply the mechanical fixes**

```bash
cargo clippy --fix --workspace --all-targets --allow-dirty
```

- [x] **Step 3: Re-run and fix the remainder by hand**

```bash
cargo clippy --workspace --all-targets -- -D warnings ; echo "exit=$?"
```
Expected: `exit=0`. Anything `--fix` could not resolve is a collapsible-if or a `sort_by_key`;
resolve each by reading the suggestion, not by adding `#[allow]`.

- [x] **Step 4: Confirm nothing broke**

```bash
cargo test --workspace --no-fail-fast -- --test-threads=4
```
Expected: all pass. Use `--test-threads=4` until Task 11 lands — the default hangs.

- [x] **Step 5: Commit**

```bash
git add -A
git commit -m "fix: resolve 22 clippy warnings on rustc 1.95"
```

---

## Task 3: Prove the linearizability checker is broken (R1)

> This is the most important task in the plan. Before replacing the checker we write the test
> that shows the current one cannot fail. That test then becomes the permanent guard on the
> replacement.

**Files:**
- Test: `crates/prkdb/tests/helpers/jepsen_checker.rs` (append to `mod tests`)

- [x] **Step 1: Write the failing test**

Append to the `#[cfg(test)] mod tests` block at the bottom of `jepsen_checker.rs`:

```rust
/// A stale read is THE canonical linearizability violation: a read that returns an old
/// value after a newer write has already completed, in real time. A checker that cannot
/// fail this test cannot verify linearizability at all.
#[test]
fn detects_stale_read_after_completed_write() {
    let history = OperationHistory::new();
    let t0 = Instant::now();
    let t1 = t0 + std::time::Duration::from_millis(10);
    let t2 = t0 + std::time::Duration::from_millis(20);
    let t3 = t0 + std::time::Duration::from_millis(30);
    let t4 = t0 + std::time::Duration::from_millis(40);
    let t5 = t0 + std::time::Duration::from_millis(50);

    // W1: write "v1", completes at t1
    history.record(Operation {
        kind: OpKind::Write,
        key: b"k".to_vec(),
        write_value: Some(b"v1".to_vec()),
        read_value: None,
        start_time: t0,
        end_time: t1,
        result: OpResult::Ok(None),
        client_id: 1,
    });

    // W2: write "v2", starts at t2, completes at t3 — strictly after W1
    history.record(Operation {
        kind: OpKind::Write,
        key: b"k".to_vec(),
        write_value: Some(b"v2".to_vec()),
        read_value: None,
        start_time: t2,
        end_time: t3,
        result: OpResult::Ok(None),
        client_id: 1,
    });

    // R1: reads "v1" at t4..t5 — entirely AFTER W2 completed. There is no valid
    // linearization: W2 must precede R1 in real time, so R1 must observe "v2".
    history.record(Operation {
        kind: OpKind::Read,
        key: b"k".to_vec(),
        write_value: None,
        read_value: Some(b"v1".to_vec()),
        start_time: t4,
        end_time: t5,
        result: OpResult::Ok(Some(b"v1".to_vec())),
        client_id: 2,
    });

    match history.is_linearizable() {
        LinearizabilityResult::NotLinearizable { .. } => {}
        LinearizabilityResult::Linearizable => {
            panic!("checker reported a stale read as linearizable — it cannot detect violations")
        }
    }
}
```

- [x] **Step 2: Run it and watch it fail**

```bash
cargo test -p prkdb --test jepsen_consistency_tests detects_stale_read -- --nocapture
```
Expected: FAIL — `checker reported a stale read as linearizable`. The existing checker only
requires `w.start_time < read.end_time`, which `W1` satisfies.

- [x] **Step 3: Commit the failing test**

```bash
git add crates/prkdb/tests/helpers/jepsen_checker.rs
git commit -m "test: add failing meta-test proving the checker cannot detect stale reads"
```

> Committing a known-failing test is deliberate here — it documents the defect precisely, and
> Task 4 is defined as "make this pass". If CI must stay green, mark it
> `#[ignore = "fails until the real checker lands in Task 4"]` and un-ignore it in Task 4.

---

## Task 4: Implement a real linearizability checker (R1)

**Files:**
- Create: `crates/prkdb/tests/helpers/wgl.rs`
- Modify: `crates/prkdb/tests/helpers/jepsen_checker.rs`
- Modify: `crates/prkdb/tests/helpers/mod.rs`

> **D6 settled this: in-process Wing & Gong.** Elle/EDN offline checking was considered and
> rejected — a stronger claim, but it puts a JVM and Clojure in CI and moves failures out of the
> PR feedback loop, which for a solo maintainer means they get looked at less.
>
> Three properties the implementation must have, all of which the old checker lacked:
> **bounded search** (Step 1), **errors modelled as indeterminate rather than skipped**
> (Step 2), and **its own correctness tested** (Task 3's meta-test). Do not trust a single
> result from this checker before that meta-test passes.

- [x] **Step 1: Add the bounded-history guard**

WGL's search is exponential in the worst case. Add to `OperationHistory`:

```rust
/// Maximum operations the linearizability search will accept. The Wing & Gong search is
/// exponential in the worst case; beyond this we would hang rather than answer.
pub const MAX_CHECKABLE_OPS: usize = 200;
```

- [x] **Step 2: Implement the checker**

Create `crates/prkdb/tests/helpers/wgl.rs` implementing Wing & Gong linear search over a
single-register model:

- Model state is `Option<Vec<u8>>` (the register's value).
- Walk the history; at each step, try to linearize any operation whose interval is currently
  "open" (started, not yet forced to complete).
- A `Write(v)` linearizes unconditionally and sets state to `Some(v)`.
- A `Read(v)` linearizes only if `state == Some(v)`.
- On success, recurse with the operation removed. On failure, backtrack.
- Memoize on `(sorted_remaining_op_ids, state)` to prune the search.
- Return `NotLinearizable` when the search exhausts with operations remaining.

Model errored operations as *indeterminate*: a `Timeout` write may or may not have taken
effect, so the search must try both branches. This is what the old checker got wrong by
skipping them.

- [x] **Step 3: Route `is_linearizable` through it**

Replace the body of `is_linearizable()` with a call into `wgl::check`, keeping the existing
`LinearizabilityResult` return type so callers do not change.

- [x] **Step 4: Run the meta-test**

```bash
cargo test -p prkdb --test jepsen_consistency_tests detects_stale_read -- --nocapture
```
Expected: PASS.

- [x] **Step 5: Confirm valid histories still pass**

```bash
cargo test -p prkdb --test jepsen_consistency_tests test_linearizability_simple -- --nocapture
```
Expected: PASS. A checker that rejects everything is as useless as one that accepts everything —
both tests must hold.

- [x] **Step 6: Add a concurrent-is-fine case**

Add a test where a read overlaps an in-flight write and returns either the old or new value.
Both must be reported `Linearizable` — concurrency is not a violation.

- [x] **Step 7: Commit**

```bash
git add crates/prkdb/tests/helpers/
git commit -m "feat(test): implement Wing & Gong linearizability checker"
```

---


> **Complete 2026-08-09.** `crates/prkdb/tests/helpers/in_process_cluster.rs`, driven by
> `crates/prkdb/tests/in_process_cluster.rs`. Needs no prebuilt binary.
## Task 4b: Build an in-process cluster harness (R1, R4, R14, R16 prerequisite; D5)

> **New in revision 5. A prerequisite for Tasks 5, 7, 17, and 19 of this plan — and for
> Plan B Task 2's cluster tests.** Skipping it does not just shrink Plan A; it silently blocks
> the gRPC authorization work in the other plan.
>
> The existing `TestCluster` spawns real `prkdb-server` child processes
> (`test_cluster.rs:139-176`) and panics with *"Binary not found … Run 'cargo build --bin
> prkdb-server --release' first"* if the binary is absent. That is why
> `raft_chaos_tests.rs:256` carries `#[ignore] // Requires server binary`, and why re-ignoring
> these tests is the path of least resistance every time CI gets slow.
>
> D5 chose to fix the cause rather than add a build step.

**Files:**
- Create: `crates/prkdb/tests/helpers/in_process_cluster.rs`
- Modify: `crates/prkdb/tests/helpers/mod.rs`

- [x] **Step 1: Confirm the constraint**

```bash
sed -n '135,180p' crates/prkdb/tests/helpers/test_cluster.rs
grep -rn '#\[ignore.*[Bb]inary' crates/prkdb/tests/
```
Expected: the binary-path resolution and panic, and at least one test ignored because of it.

- [x] **Step 2: Write the failing test for the harness itself**

```rust
/// The harness must form a working cluster with no child processes and no prebuilt binary.
/// If this needs `cargo build` first, it has not solved the problem it exists to solve.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn in_process_cluster_elects_a_leader() {
    let cluster = InProcessCluster::new(3).await.expect("cluster starts");

    helpers::await_condition("a leader is elected", Duration::from_secs(10), || async {
        cluster.leader().await.is_some()
    })
    .await;

    assert_eq!(
        cluster.leaders_in_current_term().await.len(),
        1,
        "exactly one leader per term"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn in_process_cluster_partitions_and_heals() {
    let cluster = InProcessCluster::new(3).await.expect("cluster starts");
    helpers::await_condition("initial leader", Duration::from_secs(10), || async {
        cluster.leader().await.is_some()
    })
    .await;

    cluster.partition(vec![1], vec![2, 3]).await;
    helpers::await_condition("majority side re-elects", Duration::from_secs(10), || async {
        matches!(cluster.leader_among(&[2, 3]).await, Some(_))
    })
    .await;

    cluster.heal_partitions().await;
    helpers::await_condition("cluster reconverges", Duration::from_secs(10), || async {
        cluster.leaders_in_current_term().await.len() == 1
    })
    .await;
}
```

- [x] **Step 3: Run and watch it fail**

```bash
cargo test -p prkdb --test in_process_cluster -- --nocapture --test-threads=1
```
Expected: FAIL — `InProcessCluster` does not exist.

- [x] **Step 4: Implement the harness**

Required surface, mirroring the process harness so tests can migrate mechanically:

```rust
pub struct InProcessCluster { /* nodes, network simulator, temp dirs */ }

impl InProcessCluster {
    pub async fn new(n: usize) -> anyhow::Result<Self>;
    pub async fn leader(&self) -> Option<u64>;
    pub async fn leader_among(&self, ids: &[u64]) -> Option<u64>;
    pub async fn leaders_in_current_term(&self) -> Vec<u64>;
    pub async fn partition(&self, g1: Vec<u64>, g2: Vec<u64>);
    pub async fn heal_partitions(&self);
    pub async fn stop_node(&self, id: u64);
    pub async fn restart_node(&self, id: u64) -> anyhow::Result<()>;
    pub async fn put(&self, k: &[u8], v: &[u8], c: ReadConsistency) -> anyhow::Result<()>;
    pub async fn get(&self, k: &[u8], c: ReadConsistency) -> anyhow::Result<Option<Vec<u8>>>;
    pub async fn all_nodes_have(&self, k: &[u8], v: &[u8]) -> bool;
}
```

Construct `PartitionManager` / `RaftNode` directly, exactly as `distributed_writes.rs:7-46`
already does — that file proves in-process nodes work; it just does it inline instead of behind
a reusable type. Bind every port with `helpers::free_port()` from Task 10.

- [x] **Step 5: Verify it needs no binary**

```bash
cargo clean -p prkdb
cargo test -p prkdb --test in_process_cluster -- --nocapture --test-threads=1
```
Expected: PASS **without** any `cargo build --bin prkdb-server` step. That is the whole point —
if it fails here, the harness still depends on the binary.

- [x] **Step 6: Keep the process harness for what it actually tests**

Do not delete `TestCluster`. It is the right tool for binary-level behaviour: startup, config
parsing, signal handling, graceful shutdown. Add a module doc to each saying which to reach for.

- [x] **Step 7: Commit**

```bash
git add crates/prkdb/tests/helpers/
git commit -m "test: add in-process cluster harness requiring no prebuilt binary"
```

---


> **Complete 2026-08-09.** `a_replicated_register_is_linearizable` and
> `..._across_a_partition` in `jepsen_consistency_tests.rs`. The partitioned variant
> found **S-06** — `read_index` served reads without confirming leadership.
## Task 5: Point the register test at a real cluster (R1)

**Files:**
- Modify: `crates/prkdb/tests/jepsen_consistency_tests.rs:80-145`

- [x] **Step 1: Read the existing cluster harness — and learn its cost**

```bash
sed -n '1,80p' crates/prkdb/tests/helpers/test_cluster.rs
sed -n '135,180p' crates/prkdb/tests/helpers/test_cluster.rs
```

The harness exists and is unused by this test. But it is **process-based**: `TestCluster` spawns
real `prkdb-server` children and panics with *"Binary not found … Run 'cargo build --bin
prkdb-server --release' first"* if the binary is missing. Two consequences:

1. This task and its CI job must run `cargo build --bin prkdb-server --release` first.
2. `TestCluster` is not `Clone` (it owns a `TempDir` and `Child` handles). Share via `Arc`.

**D5 chose the in-process harness**, built in Task 4b. Use `InProcessCluster` here rather than
`TestCluster` — no binary build, and it is what lets Tasks 17 and 19 drop most `#[ignore]`s.

- [x] **Step 2: Replace the single-node adapter**

`test_linearizable_register` currently constructs a local `WalStorageAdapter`
(`jepsen_consistency_tests.rs:15`). Replace with a 3-node cluster from `test_cluster.rs`,
issuing reads and writes through the cluster client so the operations traverse Raft.

- [x] **Step 3: Inject a partition mid-run**

Halfway through the operation loop, apply a `NetworkSimulator::partition` splitting one node
from the other two, then heal it before the run ends. A consistency test with no faults tests
nothing that a single-threaded test would not.

- [x] **Step 4: Run it**

```bash
cargo test -p prkdb --test jepsen_consistency_tests test_linearizable_register -- --nocapture --test-threads=1
```
Expected: PASS, and the log shows the partition being applied and healed.

> If it FAILS, that is a real finding about PrkDB's consistency — not a test bug. Stop, capture
> the history, and open an issue before changing the test.

- [x] **Step 5: Commit**

```bash
git add crates/prkdb/tests/jepsen_consistency_tests.rs
git commit -m "test: run linearizable register against a 3-node cluster under partition"
```

---

## Task 6: Make the bank invariant test touch the database (R1)

**Files:**
- Modify: `crates/prkdb/tests/helpers/jepsen_checker.rs:224-268` (`BankAccounts`)
- Modify: `crates/prkdb/tests/jepsen_consistency_tests.rs:171-236`

- [x] **Step 1: Confirm the current test proves nothing**

```bash
grep -n 'accounts.lock().unwrap()' crates/prkdb/tests/helpers/jepsen_checker.rs
```
Expected: hits inside `transfer` and `check_total_invariant` — the invariant is computed over an
in-process `HashMap`, never over stored state.

- [x] **Step 2: Back `BankAccounts` with a storage adapter**

Change `BankAccounts` to hold `Arc<dyn StorageAdapter>` instead of
`Arc<Mutex<HashMap<String, i64>>>`. `transfer` becomes a real transaction:

```rust
pub async fn transfer(&self, from: &str, to: &str, amount: i64) -> Result<(), String> {
    let mut tx = self.storage.begin_transaction();
    let from_bal = read_balance(&mut tx, from).await?;
    if from_bal < amount {
        tx.rollback();
        return Err("Insufficient funds".to_string());
    }
    write_balance(&mut tx, from, from_bal - amount)?;
    let to_bal = read_balance(&mut tx, to).await?;
    write_balance(&mut tx, to, to_bal + amount)?;
    tx.commit().await.map_err(|e| e.to_string())
}
```

Use `TransactionConfig` with `IsolationLevel::Serializable` — the repo already supports this
(`crates/prkdb/src/transaction.rs`) and it is the isolation level the invariant needs.

- [x] **Step 3: Compute the invariant from storage**

`check_total_invariant` reads every account balance back out of the adapter and sums those.

- [x] **Step 4: Run it**

```bash
cargo test -p prkdb --test jepsen_consistency_tests test_bank_transfer_invariant -- --nocapture --test-threads=1
```
Expected: PASS. Conflict-induced transaction aborts are expected and fine — the invariant is
about the total, not about every transfer succeeding.

- [x] **Step 5: Commit**

```bash
git add crates/prkdb/tests/
git commit -m "test: run bank transfer invariant through real serializable transactions"
```

---


> **Complete 2026-08-09.** `distributed_writes.rs` rewritten; the vacuous
> `get_leader().is_some()` assertions are gone. Election safety lives in
> `election_safety.rs`.
## Task 7: Fix the Raft leadership assertions (R4)

**Files:**
- Modify: `crates/prkdb/tests/distributed_writes.rs:49-160`

- [x] **Step 1: Confirm the assertion is vacuous**

```bash
sed -n '391,399p' crates/prkdb/src/raft/node.rs
```
`get_leader()` returns `Some(local_id)` when this node leads, **or** the known leader id
otherwise. Followers return `Some` too, so `nodeN_is_leader` is true for every node once any
election succeeds.

- [x] **Step 2: Write the election-safety test first**

```rust
/// Raft's election safety property: at most one leader per term.
/// This is the invariant worth regression-testing; "someone knows a leader" is not.
#[tokio::test(flavor = "multi_thread")]
async fn test_election_safety_at_most_one_leader_per_term() {
    let (nodes, _dirs) = spawn_cluster(3).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let mut leaders_by_term: HashMap<u64, Vec<u64>> = HashMap::new();
        for n in &nodes {
            if n.get_state().await == RaftState::Leader {
                leaders_by_term
                    .entry(n.current_term().await)
                    .or_default()
                    .push(n.id());
            }
        }
        for (term, leaders) in &leaders_by_term {
            assert!(
                leaders.len() <= 1,
                "election safety violated: term {} had leaders {:?}",
                term, leaders
            );
        }
        if !leaders_by_term.is_empty() {
            break;                       // an election completed; property held
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no leader elected within 10s"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
```

- [x] **Step 3: Run it**

```bash
cargo test -p prkdb --test distributed_writes test_election_safety -- --nocapture --test-threads=1
```
Expected: PASS. If `current_term()` or `id()` are not public on `RaftNode`, add
`pub(crate)` accessors — do not weaken the test to fit the API.

- [x] **Step 4: Fix `test_raft_leader_election`**

Replace `get_leader().await.is_some()` with `get_state().await == RaftState::Leader`, and poll
until a leader exists or a deadline expires rather than `sleep(3s)`-then-assert.

- [x] **Step 5: Fix `test_raft_propose`**

Select the propose target by `get_state() == RaftState::Leader`, not by the first node returning
`Some` from `get_leader()`. Today it picks node 1 regardless of who leads, so the test named for
leader proposal usually exercises follower forwarding.

- [x] **Step 6: Run the whole file**

```bash
cargo test -p prkdb --test distributed_writes -- --nocapture --test-threads=1
```
Expected: all pass.

- [x] **Step 7: Commit**

```bash
git add crates/prkdb/tests/distributed_writes.rs
git commit -m "test: assert Raft election safety instead of leader-known"
```

---

## Task 8: Feature-gate chaos injection out of release builds (R6)

**Files:**
- Modify: `crates/prkdb/Cargo.toml`
- Modify: `crates/prkdb/src/raft/rpc_client.rs:22-89`
- Modify: `.github/workflows/chaos-tests.yml`

- [x] **Step 1: Confirm it ships today**

```bash
grep -A4 '^\[features\]' crates/prkdb/Cargo.toml
```
Expected: only `default = ["metrics"]` and `metrics = []`. There is no gate, so
`check_chaos` — an env-var read plus a file read plus a JSON parse — runs on every
`get_client()` call in release builds.

- [x] **Step 2: Add the feature**

```toml
[features]
default = ["metrics"]
metrics = []
# Fault injection for chaos tests. MUST NOT be enabled in production builds:
# it lets anyone who can write CHAOS_CONFIG_PATH partition the cluster.
chaos = []
```

- [x] **Step 3: Gate the code**

Annotate `ChaosRule` (line 22), `check_chaos` (line 44), and its call site (line 89) with
`#[cfg(feature = "chaos")]`. Provide a no-op `#[cfg(not(feature = "chaos"))]` shim so
`get_client` compiles unchanged, or gate the call site itself.

- [x] **Step 4: Move the call past the cache lookup**

`check_chaos` currently runs *before* the connection-cache read (`rpc_client.rs:89`), so even
when disabled it sits ahead of the hot path. Move it after the cache hit returns.

- [x] **Step 5: Verify it is gone from release builds**

```bash
cargo build -p prkdb --release
strings target/release/libprkdb.rlib 2>/dev/null | grep -c CHAOS_CONFIG_PATH
```
Expected: `0`.

- [x] **Step 6: Verify it still works when enabled**

```bash
cargo test -p prkdb --features chaos --test raft_chaos_tests -- --ignored --nocapture --test-threads=1
```
Expected: chaos rules take effect.

- [x] **Step 7: Update the chaos workflow**

Add `--features chaos` to every `cargo test` invocation in `chaos-tests.yml`.

- [x] **Step 8: Commit**

```bash
git add crates/prkdb/Cargo.toml crates/prkdb/src/raft/rpc_client.rs .github/workflows/chaos-tests.yml
git commit -m "fix: gate chaos fault injection behind a feature flag"
```

---

## Task 9: Make chaos tests actually gate (R7)

**Files:**
- Modify: `.github/workflows/chaos-tests.yml:52`
- Modify: `.github/workflows/ci.yml:213-217`
- Modify: `README.md:6`

- [x] **Step 1: Confirm the tests do not gate today**

```bash
grep -n 'continue-on-error' .github/workflows/chaos-tests.yml
grep -n "if: github.event_name == 'pull_request'" .github/workflows/ci.yml
```
Expected: the "Run All Raft Chaos Tests" step swallows failures, and the whole suite is skipped
on pushes to `main`.

- [x] **Step 2: Remove `continue-on-error`**

Delete line 52 of `chaos-tests.yml`.

- [x] **Step 3: Run chaos on `main` too**

Change the `chaos-tests` job condition in `ci.yml` so it runs on `push` to `main` as well as on
PRs.

- [x] **Step 4: Fix or quarantine what now fails**

Removing the swallow will surface real failures. For each: fix it, or mark it
`#[ignore = "<specific reason and tracking issue>"]`. A bare `#[ignore]` is not acceptable —
the repo already has 14 of those and they are why the suite proves nothing.

- [x] **Step 5: Replace the fake badge**

`README.md:6` is `[![Chaos Tests](https://img.shields.io/badge/Chaos%20Tests-19%20passing-blue)]()`
— a hardcoded string pointing nowhere. Replace with a real workflow-status badge:

```markdown
[![Chaos Tests](https://github.com/prk-Jr/prkdb/actions/workflows/chaos-tests.yml/badge.svg)](https://github.com/prk-Jr/prkdb/actions/workflows/chaos-tests.yml)
```

Do the same for the Benchmarks badge on line 5.

- [x] **Step 6: Commit**

```bash
git add .github/workflows/ README.md
git commit -m "ci: enforce chaos test results and replace hardcoded badges"
```

---

## Task 10: Eliminate hardcoded test ports (R3)

**Files:**
- Modify: all files under `crates/prkdb/tests/` that hardcode a port

> **Observed in practice on 2026-08-08 — this is not theoretical.** During execution of Task 16,
> `test_raft_propose` began failing with "No leader elected" and kept failing 3/3, including at
> the pre-session baseline commit. The cause was not any code change:
>
> ```
> $ lsof -nP -iTCP -sTCP:LISTEN | grep -E ':(5008[0-9])'
> distribut 80307 prk-jr TCP 127.0.0.1:50081 (LISTEN)
> distribut 80307 prk-jr TCP 127.0.0.1:50082 (LISTEN)
> distribut 80307 prk-jr TCP 127.0.0.1:50083 (LISTEN)
> ```
>
> PID 80307 was a `distributed_writes` binary orphaned by an **earlier run that was killed**
> while hung. SIGTERM reached the `cargo test` parent; the test binary survived and kept its
> listeners. Because the ports are hardcoded, every later run of that test bound nothing, elected
> no leader, and failed — permanently red until someone thinks to look for a stray PID.
>
> Two things this proves, both of which justify the work below:
>
> 1. **Hardcoded ports turn a transient hang into a persistent failure.** With `:0` the orphan
>    would have been harmless.
> 2. **The failure is maximally misleading.** "No leader elected" points at Raft. Roughly twenty
>    minutes were spent bisecting three commits before checking `lsof`. A CI agent would have
>    reverted good work.
>
> Normal completion does *not* leak — back-to-back runs are clean. It is specifically the
> killed-while-hung path, which is exactly what the Task 1 CI timeouts now cause on a hang.
> Task 11's deadlines and this task's ephemeral ports are what stop that from poisoning the
> next run.

- [x] **Step 1: Find them**

```bash
grep -rnoE '127\.0\.0\.1:[0-9]{4,5}' crates/prkdb/tests/ | awk -F: '{print $NF}' | sort | uniq -c | sort -rn
```
Expected: ports 9010, 8084, 8081, and 50001 each appear 3 times — those collide when cargo runs
test binaries concurrently, which is the default.

- [x] **Step 2: Read the correct pattern**

```bash
sed -n '20,35p' crates/prkdb/tests/admin_rpc_tests.rs
```
This file already binds `127.0.0.1:0` and reads the assigned port back. Copy it.

- [x] **Step 3: Add a shared helper**

In `crates/prkdb/tests/helpers/mod.rs`:

```rust
/// Bind an ephemeral port and return it. The listener is dropped, so there is a small
/// TOCTOU window — acceptable for tests and far better than a fixed port that always
/// collides under `cargo test --workspace`.
pub async fn free_port() -> u16 {
    tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("binding an ephemeral port cannot fail")
        .local_addr()
        .expect("a bound listener always has a local address")
        .port()
}
```

- [x] **Step 4: Convert every hardcoded port**

Work file by file. `distributed_writes.rs` (50071-50073, 50081-50083) first, since it is the one
observed to hang.

- [x] **Step 5: Verify**

```bash
grep -rcE '127\.0\.0\.1:[0-9]{4,5}' crates/prkdb/tests/ | grep -v ':0$' || echo "none remaining"
```
Expected: `none remaining`.

- [x] **Step 6: Commit**

```bash
git add crates/prkdb/tests/
git commit -m "test: replace hardcoded ports with ephemeral allocation"
```

---


> **Complete 2026-08-09.** `helpers::await_condition`, `helpers::within`, and
> `TestCluster::await_ready` replace the sleep-then-assert pattern. Remaining `sleep`
> calls are soak durations, not races.
## Task 11: Put a deadline on every cluster test (R3)

**Files:**
- Modify: `crates/prkdb/tests/helpers/mod.rs`
- Modify: every test that forms a cluster

- [x] **Step 1: Reproduce the hang**

```bash
cargo test --workspace --no-fail-fast
```
Observed on this baseline: run 1 exits 0; a second identical run hangs in `distributed_writes`
for >10 minutes. Serialized in isolation it passes 5/5 in ~6s. The cause is CPU starvation of
Raft election timers when ~40 test binaries run concurrently.

- [x] **Step 2: Add a poll-until-condition helper**

```rust
/// Poll `cond` until it returns true or `timeout` elapses. Panics with `desc` on timeout.
/// Replaces the `sleep(N)`-then-assert pattern, which is flaky under CI load and silently
/// slow when it works.
pub async fn await_condition<F, Fut>(
    desc: &str,
    timeout: std::time::Duration,
    mut cond: F,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if cond().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out after {:?} waiting for: {}",
            timeout,
            desc
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}
```

- [x] **Step 3: Replace every `sleep`-then-assert**

```bash
grep -rn 'sleep(Duration::from_secs' crates/prkdb/tests/ | wc -l
```
Convert each to `await_condition` with a description naming what is being waited on. The
description is what turns a mystery hang into a diagnosable failure.

- [x] **Step 4: Wrap whole tests**

Any test that starts a cluster gets an outer `tokio::time::timeout(Duration::from_secs(60), ...)`
so it fails rather than hangs.

> **Known flake to fix here, found during execution on 2026-08-08.**
>
> `xtask/tests/render_fixtures.rs::snapshot_json_is_machine_readable_and_written_to_deterministic_target_path`
> passes 3/3 in isolation and fails intermittently under `cargo test --workspace`.
> Observed failing during Task 16 verification and again during Task 19, and passing
> in the Task 10 run — so it is independent of all three.
>
> The assertion is that two consecutive `repo-status snapshot` runs produce identical
> stdout. When it fails, the first run reports `stale_repo_status_summary` and the
> second does not, meaning **the first run changed the state its own audit reads**.
> The test uses `temp_fixture_copy`, so it looks hermetic; the fixture nonetheless
> ships a committed `target/repo-status/repo-status.snapshot.json`, and the test
> shells out to `cargo run -p xtask` while an outer `cargo test` already holds the
> workspace build lock.
>
> Two candidate root causes, in order of likelihood:
> 1. `repo-status snapshot` is not idempotent — it writes an artifact that its next
>    invocation treats as input. If so, snapshot should be read-only and only
>    `render` should write.
> 2. The nested `cargo run` contends with the outer `cargo test` for the target
>    directory, so the fixture's `target/` is not the isolated state the test assumes.
>
> Fix the tool if (1); give the test its own `CARGO_TARGET_DIR` if (2). Do not paper
> over it by relaxing the determinism assertion — a snapshot tool whose output depends
> on how many times it has run is a real defect in the drift detector, which is one of
> the better things in this repository.

- [x] **Step 5: Verify determinism**

```bash
for i in $(seq 1 10); do
  printf "run %s: " "$i"
  timeout 900 cargo test --workspace --no-fail-fast >/tmp/ws_$i.log 2>&1 \
    && echo OK || echo "FAIL/HANG (see /tmp/ws_$i.log)"
done
```
Expected: 10 × `OK`. This is the acceptance criterion for R3 — anything less means the flake is
still there.

- [x] **Step 6: Commit**

```bash
git add crates/prkdb/tests/
git commit -m "test: add explicit deadlines to all cluster-forming tests"
```

---


> **Complete 2026-08-09.** `cargo test --doc -p prkdb`: 70 passed, 0 ignored.
## Task 12: Make the documentation compile (R5)

**Files:**
- Modify: `crates/prkdb/src/indexed_storage.rs` (59 of the 67)
- Modify: remaining files with `ignore` fences
- Modify: `crates/prkdb/src/lib.rs`

- [x] **Step 1: Measure the baseline**

```bash
cargo test --doc -p prkdb 2>&1 | grep 'test result:'
```
Expected: `3 passed; 0 failed; 67 ignored`.

- [x] **Step 2: Convert `ignore` to `no_run`**

```bash
grep -rc '```ignore' crates/prkdb/src/indexed_storage.rs
```
Expected: 59. Change ```` ```ignore ```` to ```` ```rust,no_run ````. `no_run` compiles and
type-checks without executing — which catches every signature drift at no runtime cost.

Examples that genuinely cannot compile (pseudo-code, shell) become ```` ```text ````, not
`ignore`. `ignore` means "nobody will ever check this"; `text` means "this is not Rust".

- [x] **Step 3: Fix what fails to compile**

```bash
cargo test --doc -p prkdb 2>&1 | tail -40
```
Every failure here is a documented API that no longer exists or changed shape. These are real
bugs in the docs, found for the first time. Fix the example to match the code.

- [x] **Step 4: Repeat for the remaining files**

`rate_limit.rs`, `collection_handle.rs`, `ttl.rs`, `transaction.rs`, `cache.rs`.

- [x] **Step 5: Compile the README**

Add to the top of `crates/prkdb/src/lib.rs`:

```rust
#![doc = include_str!("../../../README.md")]
```

Then fix the fallout — 38 Rust fences that nothing has ever compiled. Fences that are shell,
output, or config get retagged ```` ```text ````.

- [x] **Step 6: Verify**

```bash
cargo test --doc -p prkdb 2>&1 | grep 'test result:'
```
Expected: ≥60 passing, 0 failed.

- [x] **Step 7: Commit**

```bash
git add crates/prkdb/src/ README.md
git commit -m "docs: compile doctests and README examples"
```

---


> **Complete 2026-08-09.** Zero ```ignore fences remain in `crates/prkdb/src`.
## Task 12b: Convert the remaining 63 ignored doc fences (R5)

> **Filed 2026-08-08 during execution of Task 12.** Task 12 established and verified the
> conversion templates on four representative examples; this task applies them to the
> rest. Split out because it is bulk mechanical work — 56 of the 63 sit in a single file
> — and because a half-finished conversion leaves docs worse than either end state.

**Starting point:** `cargo test --doc -p prkdb` reports **7 passed, 63 ignored**
(was 3 passed, 67 ignored before Task 12).

| File | Remaining |
|---|---|
| `indexed_storage.rs` | 56 |
| `cache.rs`, `transaction.rs`, `ttl.rs`, `collection_handle.rs` | 1 each |
| `storage/{collection_partitioned_adapter,partitioned_streaming_adapter,streaming_adapter}.rs` | 1 each |

### Three templates, each verified against the compiler

**A — no setup needed** (`rate_limit.rs`, verified passing):

```rust
/// ```rust
/// use prkdb::rate_limit::RateLimiter;
///
/// let limiter = RateLimiter::per_second(100); // 100 ops/sec
/// ```
```

**B — needs a `PrkDb` and a `Collection`** (`collection_handle.rs`, verified passing):

```rust
/// ```rust
/// use prkdb::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Collection, Serialize, Deserialize, Clone, Debug)]
/// struct MyItem {
///     #[id]
///     id: u64,
/// }
///
/// let db = PrkDb::builder()
///     .with_storage(prkdb::storage::InMemoryAdapter::new())
///     .register_collection::<MyItem>()
///     .build()
///     .unwrap();
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// // ... the example ...
/// # });
/// ```
```

**C — needs an `IndexedStorage`** (`indexed_storage.rs`, verified passing). This is the
one that unlocks the other 55:

```rust
/// ```rust
/// use prkdb::indexed_storage::IndexedStorage;
/// use prkdb::prelude::*;
/// use prkdb::storage::InMemoryAdapter;
/// use serde::{Deserialize, Serialize};
/// use std::sync::Arc;
///
/// #[derive(Collection, Serialize, Deserialize, Clone, Debug)]
/// struct User {
///     #[id]
///     id: String,
///     #[index]
///     age: u32,
///     created_at: u64,
/// }
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let db = IndexedStorage::new(Arc::new(InMemoryAdapter::new()));
/// // ... the example ...
/// # });
/// ```
```

### Three traps the compiler found, so nobody rediscovers them

1. **`Collection` requires `Debug`.** `#[derive(Collection, Serialize, Deserialize, Clone)]`
   compiles on its own but fails inside the machinery. Always add `Debug`.
2. **`with_batching` panics without a Tokio reactor** — "there is no reactor running". The
   documented example would have panicked for anyone who copied it outside async. Wrap
   anything that spawns in `block_on`.
3. **`?` does not work in a doctest body** unless the block returns a `Result`. Use
   `.unwrap()`; these are examples, not production code.

- [x] **Step 1: Confirm the baseline**

```bash
cargo test --doc -p prkdb 2>&1 | grep 'test result:'
```
Expected: `7 passed; 63 ignored`.

- [x] **Step 2: Convert the seven scattered fences first**

One each in `cache.rs`, `transaction.rs`, `ttl.rs`, `collection_handle.rs`, and the three
`storage/*` adapters. They exercise all three templates and are a cheap confidence check
before the bulk.

- [x] **Step 3: Convert `indexed_storage.rs` in batches**

Fifty-six fences, nearly all template C with a per-method body. Work in batches of ten and
run `cargo test --doc -p prkdb indexed_storage` after each — a batch that compiles as a
whole but fails as a unit is much harder to bisect.

> Where an example genuinely cannot compile — pseudo-code, shell, output samples — retag it
> ```` ```text ````, never ```` ```ignore ````. `text` says "this is not Rust"; `ignore`
> says "nobody will ever check this", which is how all 67 got here.

- [x] **Step 4: Compile the README too**

Add to `crates/prkdb/src/lib.rs`:

```rust
#![doc = include_str!("../../../README.md")]
```

The README holds 38 Rust fences that nothing has ever compiled. Expect fallout; retag the
shell and output blocks as `text`.

- [x] **Step 5: Verify**

```bash
cargo test --doc -p prkdb 2>&1 | grep 'test result:'
./scripts/plan_status.sh | grep doctests
```
Expected: ≥60 passing, 0 failed, and the tracker's doctest check green.

- [x] **Step 6: Commit**

```bash
git add crates/prkdb/src README.md
git commit -m "docs: compile the remaining doc examples"
```

---

## Task 13: Remove unwraps from durability paths (R8)

> **Scope is production code only.** Raw directory totals are misleading: `prkdb-core/src/wal/`
> contains 82 `.unwrap()` calls and `prkdb/src/storage/` contains 176, but the great majority are
> inside `#[cfg(test)]` modules and are fine there. The production-only counts below are what this
> task targets — ~27 sites total.

**Files:**
- Modify: `crates/prkdb-core/src/wal/write_ahead_log.rs` (11 production)
- Modify: `crates/prkdb-core/src/wal/log_segment.rs` (6 production)
- Modify: `crates/prkdb/src/storage/wal_adapter.rs` (7 production)
- Modify: `crates/prkdb-storage-segmented/src/lib.rs` (3 production — **do these first**)
- Modify: `crates/prkdb-core/src/lib.rs`

> **Start with the segmented adapter.** `prkdb-storage-segmented/src/lib.rs:210-221` is the worst
> of the set and was outside this task's scope until a third review pass looked at the adapter
> crates:
>
> ```rust
> let key_len = u32::from_le_bytes(cursor[1..5].try_into().unwrap()) as usize;
> let val_len = u32::from_le_bytes(cursor[5..9].try_into().unwrap()) as usize;
> let crc     = u32::from_le_bytes(cursor[crc_start..crc_start + 4].try_into().unwrap());
> ```
>
> These read length prefixes and a CRC from bytes loaded off disk during recovery. On a truncated
> segment — exactly what this code exists to survive — the slice index panics before `try_into` is
> even reached. Corrupt data becomes a process crash instead of an error. It is also the precise
> scenario the three `#[ignore]`d tests in `corruption_tests.rs` were written for, so fixing it
> lets Task 19 un-ignore them.
>
> `prkdb-core/src/io/` (mmap, io_uring) was checked in the same pass and has zero production
> unwraps — no work needed there.

- [x] **Step 0: Establish the production-only baseline**

Do not use a bare `grep -c '.unwrap()'` — it counts test modules and will send you chasing 258
sites instead of 24. Count outside `#[cfg(test)]`:

```bash
python3 - <<'EOF'
import re, pathlib, collections
per = collections.Counter()
for root in ("crates/prkdb-core/src/wal", "crates/prkdb/src/storage"):
    for p in pathlib.Path(root).rglob("*.rs"):
        in_test, depth, started = False, 0, False
        for ln in p.read_text(errors="ignore").splitlines():
            if not in_test and re.search(r"#\[cfg\(test\)\]", ln):
                in_test, depth, started = True, 0, False
                continue
            if in_test:
                depth += ln.count("{") - ln.count("}")
                if "{" in ln:
                    started = True
                if started and depth <= 0:
                    in_test = False
                continue
            if ".unwrap()" in ln:
                per[str(p)] += ln.count(".unwrap()")
for f, c in per.most_common():
    print(f"{c:4}  {f}")
print("TOTAL:", sum(per.values()))
EOF
```
Record the total. That number, not the raw grep, is what must reach zero.

- [x] **Step 1: Write a test that a WAL error surfaces as an error**

Pick one `unwrap()` in `write_ahead_log.rs`, construct the condition that trips it (a truncated
segment, a bad path, a full disk simulated by a read-only dir), and assert the API returns
`Err`, not that the process panics.

- [x] **Step 2: Run it and watch it panic**

```bash
cargo test -p prkdb-core wal_returns_error_on -- --nocapture
```
Expected: FAIL via panic, not via a returned `Err`.

- [x] **Step 3: Convert to typed errors**

Use the existing `thiserror` error enum in `prkdb-core`. Add variants rather than stringly-typed
errors.

- [x] **Step 4: Verify**

```bash
cargo test -p prkdb-core -- --nocapture
```
Expected: PASS, and the new test now observes an `Err`.

- [x] **Step 5: Repeat for the remaining 23 sites**

- [x] **Step 6: Lock it in**

At the top of `crates/prkdb-core/src/lib.rs`:

```rust
#![deny(clippy::unwrap_used, clippy::expect_used)]
```

Test modules are exempt via `#![cfg_attr(test, allow(clippy::unwrap_used))]`.

- [x] **Step 7: Document the metrics unwraps**

The 60 in `prometheus_metrics.rs` and 20 in `prkdb-metrics/src/exporter.rs` are startup-only
registration. Convert to `.expect("metric registration uses static names and cannot collide")`.
A panic message that explains the invariant is worth more than a silent `?`.

- [x] **Step 8: Commit**

```bash
git add crates/prkdb-core/ crates/prkdb/src/storage/ crates/prkdb/src/prometheus_metrics.rs
git commit -m "fix: replace unwraps on durability paths with typed errors"
```

---

## Task 14: Measure coverage (R9)

**Files:**
- Modify: `.github/workflows/ci.yml`

- [x] **Step 1: Install and measure**

```bash
cargo install cargo-llvm-cov
cargo llvm-cov --workspace --summary-only -- --test-threads=4
```
Record the line-coverage percentage. This number is the deliverable — nothing in the repo has
ever produced it.

- [x] **Step 2: Add the CI job**

```yaml
  coverage:
    name: Coverage
    runs-on: ubuntu-latest
    timeout-minutes: 45
    steps:
      - uses: actions/checkout@v4
      - name: Install Protoc
        run: sudo apt-get install -y protobuf-compiler
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: llvm-tools-preview
      - uses: taiki-e/install-action@cargo-llvm-cov
      - uses: Swatinem/rust-cache@v2
      - run: cargo llvm-cov --workspace --fail-under-lines <MEASURED_BASELINE>
```

Set `<MEASURED_BASELINE>` to the Step 1 number, rounded down. Ratchet it upward in later PRs;
never lower it.

- [x] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: measure line coverage with a ratcheting floor"
```

---

## Task 15: Scan dependencies (R10)

**Files:**
- Create: `deny.toml`
- Create: `.github/dependabot.yml`
- Modify: `.github/workflows/ci.yml`

- [x] **Step 1: Confirm nothing scans today**

```bash
cargo audit 2>&1 | tail -3
```
Expected: an error — the installed `cargo-audit` cannot parse CVSS 4.0 advisories, so it has
never completed a scan. 481 crates in `Cargo.lock` are unchecked.

- [x] **Step 2: Reinstall the tools**

```bash
cargo install --force cargo-audit cargo-deny
cargo audit
```
Expected: a completed scan. Triage anything it reports.

- [x] **Step 3: Create `deny.toml`**

```toml
[advisories]
version = 2
yanked = "deny"

[licenses]
version = 2
allow = ["Apache-2.0", "MIT", "BSD-2-Clause", "BSD-3-Clause", "ISC", "Unicode-3.0", "Zlib"]

[bans]
multiple-versions = "warn"
wildcards = "deny"

[sources]
unknown-registry = "deny"
unknown-git = "deny"
```

- [x] **Step 4: Run it and resolve**

```bash
cargo deny check
```
Add explicit exceptions with a comment explaining each, rather than widening `allow`.

- [x] **Step 5: Add Dependabot**

```yaml
version: 2
updates:
  - package-ecosystem: cargo
    directory: "/"
    schedule:
      interval: weekly
    open-pull-requests-limit: 5
  - package-ecosystem: github-actions
    directory: "/"
    schedule:
      interval: monthly
```

- [x] **Step 6: Add the CI job**

A `security-audit` job with `timeout-minutes: 15` running `cargo deny check` and `cargo audit`.

- [x] **Step 7: Commit**

```bash
git add deny.toml .github/
git commit -m "ci: add cargo-deny, cargo-audit, and Dependabot"
```

---

## Task 16: Remove dead code and close doc drift (R11)

**Files:**
- Delete: `crates/prkdb/src/storage_old_inmemory.rs`, `crates/prkdb/src/security.rs`
- Modify: `crates/prkdb/src/lib.rs:28`
- Modify: `xtask/src/repo_status/collectors/`
- Modify: `docs/guide/roadmap.md`

- [x] **Step 1: Confirm what is actually dead — one of the two is not**

```bash
wc -c crates/prkdb/src/security.rs
grep -rn 'mod security' crates/prkdb/src/lib.rs || echo "security.rs declared nowhere"
grep -rn 'storage_old_inmemory\|InMemoryAdapter' crates/prkdb/src/lib.rs crates/prkdb/src/builder.rs crates/prkdb/src/storage/mod.rs
grep -rln 'InMemoryAdapter' crates/prkdb/tests/ | wc -l
```

Expected:
- `security.rs` is **0 bytes and declared nowhere** — genuinely dead, delete it.
- `storage_old_inmemory` is **live**: `builder.rs:269` constructs it as the default storage,
  `storage/mod.rs:14` and `lib.rs:96` re-export `InMemoryAdapter` as public API, `lib.rs:87`
  documents it, and 10+ integration tests use it.

> **The audit was wrong about this one.** It read the name and the comment "Renamed from
> storage.rs" as a legacy artifact. Deleting the module fails the build and removes a published
> type. Do not delete it.

- [x] **Step 2: Delete the genuine orphan**

```bash
rm crates/prkdb/src/security.rs
cargo check --workspace
```
Expected: clean. Nothing referenced it.

- [x] **Step 2b: Rename the misleadingly-named module**

The defect is the name, not the code. `git mv` it into the storage module where it belongs:

```bash
git mv crates/prkdb/src/storage_old_inmemory.rs crates/prkdb/src/storage/in_memory.rs
```

Then in `crates/prkdb/src/lib.rs` drop `mod storage_old_inmemory;` (line 28), and in
`crates/prkdb/src/storage/mod.rs` replace the cross-module re-export with a normal declaration:

```rust
mod in_memory;
pub use in_memory::InMemoryAdapter;
```

Update `builder.rs:269` from `crate::storage_old_inmemory::InMemoryAdapter` to
`crate::storage::InMemoryAdapter`.

**The public paths must not change.** `prkdb::storage::InMemoryAdapter` and
`prkdb::InMemoryAdapter` both still resolve — this is an internal rename only:

```bash
cargo check --workspace
cargo test --workspace --no-fail-fast -- --test-threads=4
grep -rn 'storage_old_inmemory' crates/ --include='*.rs' || echo "name is gone"
```

- [x] **Step 3: Add a version-drift collector to xtask** — `collectors/versions.rs`

Extend `xtask/src/repo_status/collectors/` with a check that fails when the workspace version
disagrees with the version named in `docs/guide/roadmap.md`. Today the workspace is `0.6.0`
while the roadmap says "v2.0-clean" — the drift detector you already built should have caught
this.

- [x] **Step 4: Add a roadmap-vs-CI collector** — the existing check was made
      section-aware; it previously matched anywhere in the file, so the fix it asked
      for could not satisfy it

Fail when the roadmap lists something under "Future" that CI already tests. Today it lists
"Native Clients: Go and Python" as future work while five CI jobs exercise generated Go, Python,
and TypeScript clients.

- [x] **Step 5: Reconcile the roadmap** — version now matches the manifest, client SDKs
      moved out of Future, and the duplicated performance table replaced by a pointer to
      `docs/benchmarks/methodology.md`

Move shipped items out of Future. Reconcile the two performance tables — the roadmap says 199K
writes/sec, the README says 894K queries/sec, and nothing explains that these are different
operations.

- [x] **Step 6: Verify the detector catches drift** — confirmed by editing the roadmap
      version and watching it fire, then restoring

```bash
cargo run -p xtask -- repo-status snapshot --fail-on-objective-drift
```
Expected: exit 0 after the reconciliation. Then deliberately edit the roadmap version, re-run,
and confirm it exits non-zero — a drift detector that never fires is not a detector.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "chore: delete dead modules and extend drift detection to docs"
```

---

## Task 16b: Fix the xtask snapshot determinism flake (R3)

> **Filed 2026-08-08 during execution.** This started as a note inside Task 11 and is
> promoted to its own task because it now **blocks Task 14** — `cargo llvm-cov
> --workspace` cannot complete while it fails.

**Files:**
- Investigate: `xtask/src/repo_status/mod.rs`, `xtask/tests/render_fixtures.rs`

**Symptom.** `snapshot_json_is_machine_readable_and_written_to_deterministic_target_path`
asserts two consecutive `repo-status snapshot` runs produce identical stdout. When it
fails, run 1 reports `stale_repo_status_summary` and run 2 does not — the first run
changes the state its own audit reads.

**What has been ruled out**, so nobody repeats the work:

| Hypothesis | Verdict |
|---|---|
| Caused by the Task 16 module rename | No — failed at the pre-session baseline commit too |
| Caused by Task 19's `#[ignore]` changes | No — failed before those landed |
| Non-hermetic fixture | No — the test uses `temp_fixture_copy` into a temp directory |
| Target-directory dependent | No — passes 2/2 in isolation under both the default and the `llvm-cov-target` directory |

**What is left.** It fails only when the whole workspace runs concurrently, and passes
3/3 in isolation every time. The test shells out to `cargo run -p xtask` while an outer
`cargo test` already holds the workspace build lock, so the nested invocation races the
parent. Two candidates:

1. `repo-status snapshot` is not idempotent — it writes an artifact that its next
   invocation treats as input. If so, `snapshot` should be read-only and only `render`
   should write. This is the likelier cause and the more valuable fix.
2. The nested `cargo run` blocks on, or observes partial state from, the outer build.

- [x] **Step 1: Reproduce deterministically**

```bash
cargo llvm-cov --workspace --summary-only
```
Expected today: fails in `xtask`, taking the coverage baseline down with it.

- [x] **Step 2: Establish which candidate it is**

Copy the fixture, run the tool twice by hand, and look for writes between runs:

```bash
REPO=$(pwd)
rm -rf /tmp/fx && cp -r xtask/tests/fixtures/docs_contract_mismatch /tmp/fx
cd /tmp/fx
cargo run -q -p xtask --manifest-path "$REPO/Cargo.toml" -- repo-status snapshot > /tmp/a.json
find . -newer /tmp/a.json -type f          # any output here means candidate 1
cargo run -q -p xtask --manifest-path "$REPO/Cargo.toml" -- repo-status snapshot > /tmp/b.json
diff /tmp/a.json /tmp/b.json
```

- [x] **Step 3: Fix the cause, not the assertion**

If candidate 1: make `snapshot` read-only, so only `render` writes.
If candidate 2: give the test its own `CARGO_TARGET_DIR` and invoke the already-built
binary directly instead of going through `cargo run`.

> **Do not relax the determinism assertion.** A snapshot tool whose output depends on how
> many times it has run is a real defect in the drift detector, which is one of the better
> things in this repository.

- [x] **Step 4: Verify**

```bash
cargo llvm-cov --workspace --summary-only
for i in $(seq 1 5); do cargo test --workspace || break; done
```
Expected: coverage completes, and five consecutive workspace runs pass.

- [x] **Step 5: Commit**

```bash
git add xtask/
git commit -m "fix(xtask): make repo-status snapshot idempotent"
```

---


> **Complete 2026-08-09.** `read_consistency_modes.rs`, including
> `an_isolated_leader_refuses_a_linearizable_read` (S-06).
## Task 17: Verify the shipped linearizable read mode (R14)

> **Depends on Tasks 3-5.** The checker must be real before pointing it at anything.
>
> PrkDB ships `ReadConsistency::Linearizable` as a public API — `prkdb-client/src/client.rs:53`,
> `prkdb-cli/src/commands/data.rs:80`, dispatched at `grpc_service.rs:200`. That is a correctness
> guarantee offered to users. The only tests exercising `ReadMode::Linearizable` are
> `raft_chaos_tests.rs:714` and `:761` — **both inside `#[ignore]`d tests**. The guarantee has
> zero enforced coverage.

**Files:**
- Create: `crates/prkdb/tests/read_consistency_modes.rs`
- Modify: `.github/workflows/ci.yml`

- [x] **Step 1: Confirm the gap**

```bash
grep -rn 'ReadMode::Linearizable\|ReadConsistency::Linearizable' crates/prkdb/tests/
```
Expected: only `raft_chaos_tests.rs:714` and `:761`, both under `#[ignore]`.

- [x] **Step 2: Read the real `TestCluster` surface before writing anything**

```bash
grep -n 'pub fn \|pub async fn \|pub struct ' crates/prkdb/tests/helpers/test_cluster.rs
sed -n '135,180p' crates/prkdb/tests/helpers/test_cluster.rs
```

Two things the pseudo-code in older drafts got wrong, and that you will get wrong too if you
skip this step:

| Assumption | Reality |
|---|---|
| `helpers::spawn_cluster(3)` | `TestCluster::new(3).await?` then `start_all().await?` |
| `cluster.heal()` | `heal_partitions()` |
| `cluster.clone()` | `TestCluster` holds a `TempDir` and `Child` handles — **not `Clone`**. Share it as `Arc<TestCluster>`, or drive the partition inline rather than in a spawned task. |
| in-process nodes | **Process-based.** It spawns real `prkdb-server` children (`test_cluster.rs:139-176`) and panics with *"Binary not found … Run 'cargo build --bin prkdb-server --release' first"*. |

`leader()`, `all_nodes_have()`, and a workload driver do **not** exist. Building them is part of
this task, not a given.

- [x] **Step 3: Build the binary the harness needs**

```bash
cargo build --bin prkdb-server --release
```

Without this the test panics before asserting anything. The CI job added in Step 6 needs the
same step. This is the constraint that has kept `raft_chaos_tests.rs:256` marked
`#[ignore] // Requires server binary`. Task 4b removes that constraint — prefer
`InProcessCluster` and skip the binary build entirely.

- [x] **Step 4: Add the missing cluster helpers**

In `tests/helpers/test_cluster.rs`:

- `pub async fn leader(&self) -> Option<u64>` — polls each node's status endpoint and returns
  the one reporting `RaftState::Leader`. Reuse the state accessor from Task 7.
- `pub async fn put(&self, key, value, consistency) -> anyhow::Result<()>` and a matching
  `get`, both going through `prkdb-client` so the request traverses the real read path.
- `pub async fn all_nodes_have(&self, key, value) -> bool`.

- [x] **Step 5: Write the failing test**

```rust
//! The read-consistency levels PrkDB advertises must actually differ, and the linearizable
//! one must actually be linearizable. Regression guard for R14.
//!
//! Requires: cargo build --bin prkdb-server --release

mod helpers;

use helpers::{LinearizabilityResult, OperationHistory, TestCluster};
use prkdb_client::ReadConsistency;
use std::sync::Arc;
use std::time::Duration;

/// Drive reads through the mode users are told is linearizable, under a partition, and check
/// the recorded history with the real checker from Task 4.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn linearizable_mode_produces_linearizable_history() {
    let mut cluster = TestCluster::new(3).await.expect("cluster builds");
    cluster.start_all().await.expect("nodes start");
    let cluster = Arc::new(cluster);          // TestCluster is not Clone
    let history = OperationHistory::new();

    let chaos = tokio::spawn({
        let cluster = Arc::clone(&cluster);
        async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            cluster.partition(vec![1], vec![2, 3]).await;
            tokio::time::sleep(Duration::from_secs(2)).await;
            cluster.heal_partitions().await;   // not `heal()`
        }
    });

    helpers::mixed_read_write_load(
        Arc::clone(&cluster),
        ReadConsistency::Linearizable,
        history.clone(),
    )
    .await;
    chaos.await.expect("chaos task completes");

    match history.is_linearizable() {
        LinearizabilityResult::Linearizable => {}
        LinearizabilityResult::NotLinearizable { reason } => {
            panic!("ReadConsistency::Linearizable produced a non-linearizable history: {reason}")
        }
    }
}

/// A consistency level that behaves identically to the weaker one is a bug, and nothing in the
/// repo currently would detect it. Stale reads are *expected* here — this test fails if the
/// modes are indistinguishable, not if staleness occurs.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn stale_mode_is_observably_weaker_than_linearizable() {
    let mut cluster = TestCluster::new(3).await.expect("cluster builds");
    cluster.start_all().await.expect("nodes start");
    let cluster = Arc::new(cluster);
    let history = OperationHistory::new();

    cluster.partition(vec![1], vec![2, 3]).await;
    helpers::mixed_read_write_load(
        Arc::clone(&cluster),
        ReadConsistency::Stale,
        history.clone(),
    )
    .await;
    cluster.heal_partitions().await;

    assert!(
        matches!(history.is_linearizable(), LinearizabilityResult::NotLinearizable { .. }),
        "Stale reads produced a linearizable history under partition — either the mode is not \
         wired through, or the workload is too weak to distinguish the modes. Investigate \
         before weakening this assertion."
    );
}
```

- [x] **Step 6: Run and watch it fail, then pass**

```bash
cargo build --bin prkdb-server --release
cargo test -p prkdb --test read_consistency_modes -- --nocapture --test-threads=1
```
First run FAILs on the missing helpers; after Step 4 both PASS.

> **If `linearizable_mode_produces_linearizable_history` fails:** that is a finding about PrkDB,
> not about the test. See spec §6 — capture the history, do not weaken the assertion.
>
> **If `stale_mode_is_observably_weaker_than_linearizable` fails:** the two modes may not be
> wired through distinctly. Check `execute_read_mode` at `grpc_service.rs:174` before assuming
> the test is wrong.

- [x] **Step 7: Gate it in CI**

Add to the `consistency-tests` job in `chaos-tests.yml`, **including the binary build**:

```yaml
      - name: Build server binary (required by TestCluster)
        run: cargo build --bin prkdb-server --release
      - name: Verify read consistency modes
        run: cargo test -p prkdb --test read_consistency_modes -- --nocapture --test-threads=1
```

Not behind `#[ignore]`.

- [x] **Step 8: Commit**

```bash
git add crates/prkdb/tests/read_consistency_modes.rs .github/workflows/
git commit -m "test: verify the shipped linearizable read mode under partition"
```

---


> **Complete 2026-08-09.** `docs/benchmarks/methodology.md`; `plan_status.sh` rejects
> a bare throughput figure in a doc comment.
## Task 18: Attach methodology to every performance claim (R15)

**Files:**
- Modify: `README.md`
- Modify: `crates/prkdb/src/indexed_storage.rs:7,71`
- Modify: `docs/guide/roadmap.md`
- Modify: `xtask/src/repo_status/collectors/`

- [x] **Step 1: Inventory the claims**

```bash
grep -rnE '[0-9]+(\.[0-9]+)?[KMB]? ?(ops|queries|writes|reads)/s' README.md docs/guide/ crates/*/src --include='*.rs' --include='*.md'
```
Expected: `894K queries/sec` at `README.md:15`, `indexed_storage.rs:7`, `indexed_storage.rs:71`;
a separate table in `docs/guide/roadmap.md` claiming 199K writes/sec, 8.5M reads/sec,
1.56B routing ops/sec, 10.4M cache hits/sec.

- [x] **Step 2: For each, find or reproduce the source**

```bash
ls crates/prkdb/benches/
cargo bench -p prkdb --bench query_bench
```
Every number either gets a link to the bench that produced it plus the hardware it ran on, or it
is deleted. A number nobody can reproduce is worse than no number.

- [x] **Step 3: Reconcile the two tables**

README says 894K queries/sec; the roadmap says 8.5M reads/sec. These may be different operations
(indexed secondary-key query vs. primary-key lookup) — if so, say which, explicitly. Today a
reader cannot tell whether one supersedes the other.

- [x] **Step 4: Preserve the good caveat**

`README.md:876` already states that the PrkDB and Kafka benchmark runs are not apples-to-apples.
That paragraph is the most trustworthy sentence in the README. Keep it, and make the feature
bullets consistent with it rather than the reverse.

- [x] **Step 5: Teach the drift detector**

Extend the `xtask` collector from Task 16 to fail when a performance claim in README or crate
docs has no adjacent source link.

- [x] **Step 6: Verify**

```bash
cargo run -p xtask -- repo-status snapshot --fail-on-objective-drift
```
Expected: exit 0. Then add an unsourced number, re-run, confirm non-zero.

- [x] **Step 7: Commit**

```bash
git add README.md docs/guide/roadmap.md crates/prkdb/src/indexed_storage.rs xtask/
git commit -m "docs: attach reproducible methodology to every performance claim"
```

---

## Task 19: Make every `#[ignore]` justify itself (R16)

**Files:**
- Modify: all files containing `#[ignore]`
- Modify: `.github/workflows/ci.yml`

- [x] **Step 1: Inventory**

```bash
grep -rn '#\[ignore' crates/ --include='*.rs'
```
Expected: 14 — 7 in `raft_chaos_tests.rs`, 3 in `corruption_tests.rs`, 2 in `load_tests.rs`,
2 in `chaos_tests.rs`. Most are bare `#[ignore]` with no reason.

- [x] **Step 2: Classify each**

Three buckets, and each has a different fix:

| Bucket | Fix |
|---|---|
| **Slow, but correct** (`load_tests.rs`) | Move to a nightly job that actually runs them. An ignore that means "slow" is a test you have silently deleted. |
| **Needs a running binary/cluster** (`raft_chaos_tests.rs:256`) | Wire the harness so it can run, or move to the chaos workflow with `--ignored`. |
| **Known-broken** (`chaos_tests.rs:117` — "integration harness still diverges from WAL recovery unit tests") | This one already has a reason, which is the standard. File an issue, reference it in the string. |

- [x] **Step 3: Give every ignore a reason**

```rust
// Before
#[ignore]

// After
#[ignore = "requires a running prkdb-server binary; runs in the chaos workflow — see #NN"]
```

- [x] **Step 4: Add the CI guard**

```yaml
      - name: Reject bare #[ignore]
        run: |
          if grep -rn '#\[ignore\]' crates/ --include='*.rs'; then
            echo "::error::bare #[ignore] found — use #[ignore = \"reason\"]"
            exit 1
          fi
```

Note the pattern matches `#[ignore]` exactly, so `#[ignore = "..."]` passes.

- [x] **Step 5: Verify**

```bash
grep -rn '#\[ignore\]' crates/ --include='*.rs' || echo "all ignores carry a reason"
```

- [x] **Step 6: Commit**

```bash
git add crates/ .github/workflows/ci.yml
git commit -m "test: require a documented reason on every #[ignore]"
```

---

## Definition of done

- [x] `cargo clippy --workspace --all-targets -- -D warnings` exits 0
- [x] `cargo test --workspace --no-fail-fast` completes 10 consecutive runs without hanging
- [x] `detects_stale_read_after_completed_write` passes against the new checker
- [x] The register test runs on a ≥3-node cluster under an injected partition
- [x] The bank invariant is computed from stored state
- [x] Election safety (≤1 leader per term) is asserted
- [x] `strings` on a release build finds no `CHAOS_CONFIG_PATH`
- [x] `chaos-tests.yml` contains no `continue-on-error`
- [x] `cargo test --doc -p prkdb` reports ≥60 passing
- [x] Zero `unwrap()` **outside `#[cfg(test)]`** in `prkdb-core/src/wal/` and `prkdb/src/storage/` (per the Task 13 Step 0 counter, not a raw grep)
- [x] `cargo deny check` and `cargo audit` pass in CI
- [x] Every CI job declares `timeout-minutes`
- [x] No badge in the README points at a hardcoded string
- [x] `ReadConsistency::Linearizable` is verified under partition, and `Stale` is shown to differ from it
- [x] Every performance number links to a reproducible benchmark, or is gone
- [x] Zero bare `#[ignore]`; CI rejects them
