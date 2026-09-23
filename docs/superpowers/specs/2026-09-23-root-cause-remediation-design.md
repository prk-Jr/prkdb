# Root-Cause Remediation Program — Design Spec

**Date:** 2026-09-23
**Status:** proposed — awaiting maintainer review
**Baseline commit:** 6ce73a8 (main)
**Program branch:** `remediation/root-cause`
**Toolchain:** rustc 1.98.1
**Supersedes for scheduling:** `2026-08-08-correctness-and-production-readiness.md` (its requirements R1–R16 stay in force)
**Absorbs:** `docs/reviews/2026-09-07-senior-review.md` (R01–R12)
**Phase 6 defers to:** `2026-09-07-ai-database-design.md`, `2026-09-07-embedded-ai-state-design.md`

---

## 0. Decisions

Resolved with the maintainer on 2026-09-23.

| # | Decision | Consequence |
|---|---|---|
| **D1** | **Harness first.** Build a model-based crash/restart harness before fixing individual findings. | First production fix lands ~1 week later than a bug-list approach, but every fix is proven by a checker that also finds unknown bugs. |
| **D2** | **Raft: openraft spike, gated on correctness and performance.** | 2–3 day spike in Phase 4. Adopt openraft if it passes the gates in Phase 4b; otherwise repair in place. Outcome recorded in the ledger. |
| **D3** | **Change the data-directory layout once, then lock it.** | One "format v2" change in Phase 2 with a version marker. Old directories are refused with a clear error. No converter now. |
| **D4** | **A migration path must exist for future format changes.** | v2 ships a migration registry and a `prkdb-cli migrate` command (empty today) so v2→v3 can ship a working migrator. |
| **D5** | **Serializable is the default transaction isolation.** | Matches what docs already promise. ReadCommitted remains opt-in and gains write-write conflict detection. |
| **D6** | **Performance is a gate in every phase, not a final check.** | Baseline recorded in Phase 1; every task change records a local benchmark delta; the deterministic regression gate runs on every phase PR; regressions need a durability reason in the ledger. |
| **D7** | **One spec, one plan, one program branch, one machine-checked ledger.** | Tracking cannot silently drift from code (§4). |
| **D8** | **Clustering is labelled experimental until Phase 4 exits.** | Honest scope while Raft is unsafe; removes the label only on evidence. |
| **D9** | **Commit locally; push to the public repo once per phase, together with that phase's fixes.** | No unfixed security finding is ever public; no CI runs for spec-only commits; private remote holds backups (§5.1). |

---

## 1. Why green CI did not mean correct

On 2026-09-23 `cargo test --workspace` passed 948 tests with 0 failures. The same day, a 20-line program using only the public API lost four of five keys:

```
before restart k0 = Some([118])
after checkpoint+restart k0 = None      # k0..k3 lost
after checkpoint+restart k4 = Some([118])
after restart w/o checkpoint k0 = Some([118])
```

This is the third time this pattern has appeared:

| Round | Claimed | Reality |
|---|---|---|
| 2026-08-08 spec §5 | "checkpoint recovery … the strongest production story in the repo" | Checkpoint recovery drops every key written before the checkpoint (STO-01). |
| 2026-08-09 S-05 | Reopen-loses-everything fixed | Fixed for the no-checkpoint path only. |
| repo-status page | `docs_coverage: green` | Most feature-page samples do not compile (DOC-03). |

**Root causes, not symptoms:**

1. **Tests assert surfaces, not invariants.** The checkpoint test checks that `checkpoint.json` exists. No test states "state after recovery equals state before crash" and tries to break it.
2. **No failure is ever injected where failures happen.** Nothing restarts after a checkpoint, restarts a majority, crosses 10k Raft entries, restarts a consumer, or cuts power. The chaos test tolerates 20 % of acknowledged writes missing (`raft_chaos_tests.rs:1066`).
3. **Identity and ordering are process-local.** Outbox sequence (`static AtomicU64`), partition hash (`AHasher::default()`), Raft term/vote (in-memory), and un-namespaced keys all look fine until a restart or a second node.
4. **Too many implementations of the same thing.** Four WAL implementations (`WriteAheadLog`, `ParallelWal`, `AsyncParallelWal`, `MmapParallelWal`) and multiple key layouts. Fixes land in one and not the others.
5. **Status is asserted, not derived.** Plans and status pages record "done" by hand. Nothing checks that a "fixed" finding has a regression test that exists and runs.

Every requirement below maps to one of these five causes.

---

## 2. Scope

**In:** every finding in §3; the harness and tooling that prove them fixed; the docs that describe them; the AI work in Phase 6 (scoped by the Sep 7 specs).

**Out:** new database features not needed to fix a finding (vector search, graph, query language) until Phase 6; managed/hosted offering; Windows support changes.

---

## 3. Finding ledger (initial contents)

Sources: **A** = 2026-09-23 correctness audit (A#n), **R** = 2026-09-07 senior review (Rnn), **D** = 2026-09-23 docs audit. "Verified" = maintainer session re-ran or re-read the code; "reported" = audit claim, confirm with a failing test before fixing.

### 3.1 Storage and WAL (STO)

| ID | Sev | Finding | Evidence | Status of claim | Phase |
|---|---|---|---|---|---|
| STO-01 | CRIT | Checkpoint recovery replays only from `max_offset`; index is memory-only, so pre-checkpoint keys vanish. Compactor saves this checkpoint automatically; `truncate_before` is a stub. | `storage/wal_adapter.rs:1050-1058`, `storage/checkpoint.rs:44-51`, `wal/mmap_parallel_wal.rs:310`, `wal_adapter.rs:1737` (A#1) | Verified (repro) | 2 |
| STO-02 | CRIT | Writes acked after memcpy into mmap; `flush_async` every 10th batch; no fsync. `SyncMode::Durable` never read. | `wal/mmap_log_segment.rs:279-281`, `wal_adapter.rs:497` (A#11) | Reported | 2 |
| STO-03 | MED | WAL append outside `publish_barrier`; unconditional index insert → live index may point at older offset than recovery. | `wal_adapter.rs:478-509` (A#17) | Reported, not reproduced | 2 |
| STO-04 | MED | `scan_mmap` skips CRC on open; records after a torn record become invisible; `repair_segment` never called; directories never fsynced. | `wal/mmap_log_segment.rs:676-720` (A#18) | Reported | 2 |
| STO-05 | MED | WAL routing uses std `DefaultHasher` (unstable across Rust releases); replay by segment id, not global order. | `wal/mmap_parallel_wal.rs:407-418` (A#19) | Reported | 2 |
| STO-06 | HIGH | Four WAL implementations in use; fixes do not propagate. The "parallel" WAL gives no parallelism for data: shards are chosen by hashing the collection name, but every data write passes `collection: String::new()` (`wal_adapter.rs:470,540`), so all user data lands in one shard and only `__raft_log` goes elsewhere. | `ParallelWal`, `AsyncParallelWal`, `MmapParallelWal`, `WriteAheadLog` referenced outside `wal/` | Verified | 2 |
| STO-07 | HIGH | `BatchAccumulator::flush()` sleeps `linger_ms+10` and returns Ok; executor errors dropped; queue unbounded. | `batch_accumulator.rs:49,81,92,131-135`, `collection_handle.rs:177-187` (R06) | Reported | 2 |

### 3.2 Keys, indexes, partitioning (KEY)

| ID | Sev | Finding | Evidence | Claim | Phase |
|---|---|---|---|---|---|
| KEY-01 | HIGH | Primary keys not namespaced by collection: `User{id:1}` and `Order{id:1}` overwrite each other. | `indexed_storage.rs:4280-4287`, `5273-5276` (A#13, R01) | Reported ×2 | 2 |
| KEY-02 | HIGH | `upsert` removes index entries of the new record, not the old; `query_by` never re-checks; `#[index(unique)]` never enforced. | `indexed_storage.rs:4733` (A#14) | Spot-checked | 2 |
| KEY-03 | HIGH | `AHasher::default()` is randomly seeded per process; same key → different partition after restart or on another node. | `partitioning.rs:51` (A#12) | Verified | 2 |

### 3.3 Events, outbox, consumers (EVT)

| ID | Sev | Finding | Evidence | Claim | Phase |
|---|---|---|---|---|---|
| EVT-01 | CRIT | `static OUTBOX_SEQ = 1` resets on restart; persisted consumer offsets then skip new events; sled/SQL `ON CONFLICT DO UPDATE` overwrites unconsumed rows. | `outbox.rs:118` (A#9; R "Event replay") | Verified | 2 |
| EVT-02 | HIGH | Default WAL adapter keeps outbox in memory only; `collection_partitioned_adapter` discards outbox writes and returns Ok. | `wal_adapter.rs:2482`, `collection_partitioned_adapter.rs:525` (A#10) | Reported | 2 |
| EVT-03 | HIGH | Mixed-partition batch routes all events to the first item's partition; outbox built from all items, not committed subset. | `collection_handle.rs:400-414,544-558`, `consumer.rs:304-319` (R08) | Reported | 3 |
| EVT-04 | HIGH | Sled "atomic" outbox methods apply two independent tree batches. | `prkdb-storage-sled/src/lib.rs:263-268,285-290` (R05) | Reported | 3 |
| EVT-05 | HIGH | Any `put_with_outbox` error falls back to a non-atomic path; real errors hidden. | `collection_handle.rs:282-299,608-625` (R07) | Reported | 3 |
| EVT-06 | HIGH | Auto-commit commits inside `poll()` before processing (at-most-once); no generation fencing; unassigned consumer falls back to partition 0; `let _ = self.commit()`. | `consumer.rs:71,422,435,667` (A#16) | Reported | 3 |

### 3.4 Transactions and TTL (TXN, TTL)

| ID | Sev | Finding | Evidence | Claim | Phase |
|---|---|---|---|---|---|
| TXN-01 | HIGH | Commit writes puts then deletes in separate `append_batch` calls; crash between leaves torn commit; readers see intermediate state. | `transaction.rs:415-422`, `wal_adapter.rs:482-508,547-563` (A#15, R02) | Reported ×2 | 3 |
| TXN-02 | HIGH | Indexed "atomic" transaction loops over independent puts. | `indexed_storage.rs:3308-3322,3354-3362` (R03) | Reported | 3 |
| TXN-03 | HIGH | Serializable read set keeps only last read hash; ReadCommitted writers bypass the barrier → lost updates. | `transaction.rs:275-281,370-374,417,422` (A#15, R04) | Reported ×2 | 3 |
| TXN-04 | MED | Default isolation is ReadCommitted; D5 makes it Serializable. (Docs half is DOC-11.) | `transaction.rs:131` | Verified | 3 |
| TTL-01 | MED | Expiry deletes without re-check (can delete a fresh `put_with_ttl`); value and TTL metadata written non-atomically; expiry requires undocumented `start_cleanup()`. | `ttl.rs:180,197,270` (A#21, D) | Reported | 3 |

### 3.5 Raft and cluster (RFT)

| ID | Sev | Finding | Evidence | Claim | Phase |
|---|---|---|---|---|---|
| RFT-01 | CRIT | `current_term`/`voted_for` start at 0/None; never persisted → double vote → two leaders. | `raft/node.rs:278-279`; no persistence found | Verified | 4 |
| RFT-02 | CRIT | Raft log never reloaded; `append_raft_entry` stores entries under random UUID without index/term. | `raft/node.rs:285`, `wal_adapter.rs:964` (A#3) | Reported | 4 |
| RFT-03 | CRIT | Commit index `sort(); indices[len/2]` ascending: 4-node commits with 2/4, 2-node leader commits alone. | `raft/node.rs:829-830` (A#4) | Verified | 4 |
| RFT-04 | CRIT | After compaction (>10k entries) or snapshot install, `log_start_index` ignored; indexes reused; apply loop advances `last_applied` over gaps. | `raft/node.rs:824,1355,1446,1461,1797` (A#5) | Reported | 4 |
| RFT-05 | HIGH | Commit waiters keyed by index only, not failed on step-down; deposed leader's client gets Ok for another leader's entry. | `raft/node.rs:378-396` (A#6) | Reported | 4 |
| RFT-06 | HIGH | ReadIndex heartbeat sends `prev_log_index:0` with `leader_commit`; no no-op on election → linearizable read can miss acked write. | `raft/node.rs:500-510,552-559` (A#7) | Reported | 4 |
| RFT-07 | HIGH | Failed InstallSnapshot counts toward commit; `restore` never clears old keys; stale snapshots accepted. | `raft/service.rs:188`, `state_machine.rs:219` (A#8) | Reported | 4 |
| RFT-08 | MED | RPC client never sends `x-prkdb-cluster-secret`; mTLS mode configures no server TLS → cluster cannot elect (outage, fails closed). | `raft/rpc_client.rs`, `bin/prkdb-server.rs` (A#20) | Reported | 4 |
| RFT-09 | MED | `a_committed_write_replicates_to_every_node` failed unmutated baseline in CI run 34021601202. | `tests/in_process_cluster.rs:87` (R11) | Reported | 4 |
| RFT-10 | HIGH | `CLUSTER_NODES` is parsed with `parse::<SocketAddr>()`, so hostnames (`node1:50051`) are rejected and the 3-node `docker-compose.yml` cannot start any node; the listen address doubles as the bind address. | `bin/prkdb-server.rs:366` (Batch E quality review, 2026-09-23) | Verified | 4 |

### 3.6 Schema, release, security (SCH, REL)

| ID | Sev | Finding | Evidence | Claim | Phase |
|---|---|---|---|---|---|
| SCH-01 | HIGH (sec) | Schema collection name joined into a path: `../../escaped` writes outside the registry dir. Admin-only route. | `prkdb-schema/src/storage.rs:240-245,295-307` (R09) | Reproduced by R09 probe | 0 |
| SCH-02 | HIGH | Schema reload accepts missing descriptors; non-atomic writes; concurrent registrations can reuse a version. | `prkdb-schema/src/storage.rs:198-222,283-319` (R10) | Reported | 2 |
| REL-01 | HIGH | `prkdb-client` path dep has no version → packaging fails mid-release; dry-run failures suppressed; `validate_all.sh` prints success on failure. | `release.yml:86-104`, `scripts/validate_all.sh` (R12) | Reported | 5 |

### 3.7 Tests and CI (TST)

| ID | Sev | Finding | Phase |
|---|---|---|---|
| TST-01 | HIGH | Chaos monkey tolerates 20 % of acknowledged writes missing (`raft_chaos_tests.rs:1066`). Fixed in Phase 4 by asserting `missing == 0` once RFT-02/RFT-04 are fixed; random loss cannot serve as a deterministic tripwire earlier. | 4 |
| TST-02 | MED | Linearizability workloads: 1 writer, 1 reader, ~25 ops, failed reads dropped. | 4 |
| TST-03 | MED | No restart/crash testing against a reference model. | 1 |
| TST-04 | LOW | `e2e_throughput_bench` not declared `harness = false`; Criterion `main` likely never runs. | 1 |
| TST-05 | MED | No power-loss (unsynced-data) testing; needs the WAL routed through `Vfs` (2a). | 2 |
| TST-06 | MED | No deterministic simulation of the cluster. | 4 |
| TST-07 | LOW | No fuzzing of WAL record, segment, snapshot, and proto decoding. | 2 |

### 3.8 Documentation (DOC)

| ID | Sev | Finding | Phase |
|---|---|---|---|
| DOC-01 | HIGH | Cluster setup and both compose files fail: `PRKDB_CLUSTER_SECRET`/`PRKDB_TLS_CLIENT_CA` and bootstrap token undocumented; metrics bind 127.0.0.1 in container; healthcheck gets 401; simple compose port mismatch. Fix docs and compose files. | 0 |
| DOC-02 | HIGH | Every Rust client sample uses `PrkDbClient::new` without credentials; `connect_with_credential` undocumented; Python client has no credential parameter. | 5 |
| DOC-03 | HIGH | Transactions, TTL, secondary-index, custom-adapter, ORM samples do not compile (wrong APIs, module paths, missing derives). | 5 |
| DOC-04 | MED | Global `--credential` ignored by `schema`/`codegen` (code bug, `main.rs:283`, `schema.rs:97`). | 5 |
| DOC-05 | MED | README CLI commands (`reset-offset`, `replication add`, `status`) and binary name `prkdb` do not exist; missing examples referenced. | 5 |
| DOC-06 | LOW | Rust version stated as 1.75+, 1.95+, actual 1.98. | 0 |
| DOC-07 | MED | `prkdb_writer_healthy` alert only exported by `prkdb-cli serve`; `/metrics` needs Admin; prkdb-server vs serve capability split undocumented. | 5 |
| DOC-08 | MED | No pages: consumer groups, Docker, CLI reference, Raft ops, troubleshooting, Python client, upgrade. | 5 |
| DOC-09 | LOW | Unsourced claims: "10x less resources", "~10 MB binary", "<1 s startup", "99.4 % write success". | 0 |
| DOC-10 | LOW | `ignoreDeadLinks: true`; methodology/status pages orphaned. | 5 |
| DOC-11 | MED | Transactions page claims Serializable is the default; code defaults to ReadCommitted (TXN-04). | 0 |
| DOC-12 | MED | Nothing in CI runs the documented deploy recipes (`docker compose up`, 3-node setup). | 5 |

---

## 4. Tracking system (D7)

Tracking must be impossible to mark "done" without evidence. Hand-edited checklists are what let §1 happen.

### 4.1 Ledger file

`docs/remediation/ledger.toml` — the single source of truth. One entry per finding in §3, plus phase gates.

```toml
[[finding]]
id = "STO-01"
title = "Checkpoint recovery drops pre-checkpoint keys"
area = "storage"
severity = "critical"          # critical | high | medium | low
phase = 2
status = "open"                # open | in_progress | fixed | verified | wont_fix | duplicate
sources = ["audit-2026-09-23#1"]
evidence = ["crates/prkdb/src/storage/wal_adapter.rs:1050"]
tripwire = ""                  # "path/to/file.rs::test_fn" asserting the bug still exists (open/in_progress only)
regression_tests = []          # evidence targets, required for fixed/verified; one of:
                               #   "test:path/to/file.rs::test_fn"
                               #   "script:scripts/check_x.sh"   (exits non-zero on regression)
                               #   "ci-job:<workflow file>/<job id>"
                               #   "xtask:<subcommand>"
changes = []                   # commit SHAs or PR URLs — required for fixed/verified
harness = ""                   # required for verified in harness areas: "<commit> seeds=<n> mode=<durable|fast> profile=<phase> run=<gate-job URL>"
ci_evidence = ""               # required for verified: public CI run URL where the regression tests passed
perf_note = ""                 # required if the fix regressed a tracked benchmark
decision = ""                  # required for wont_fix: link to decision row
duplicate_of = ""              # required for duplicate

[[phase]]
id = 2
title = "Format v2 and single-node root fixes"
status = "not_started"         # not_started | in_progress | gate_passed
gate = ["harness passes 10k seeds durable+fast", "perf within budget", "storage-compat check on"]
gate_evidence = []             # CI run URLs
```

**Status meanings:**
- `fixed` — code committed to the program branch with a regression test that failed before the fix (the former tripwire, inverted).
- `verified` — additionally: regression tests passed in a **public CI run** (`ci_evidence`), and for **harness areas** (STO, KEY, EVT, TXN, TTL, RFT) the harness passed at the recorded commit with the op profile that covers the finding (§7.1). Non-harness areas (SCH, REL, TST, DOC) need only `ci_evidence`. KEY-03 is proven by golden hash vectors, not by a harness profile, so it is also exempt from `harness`.

**Tripwires.** Every open finding that can be reproduced gets a tripwire test asserting the *buggy* behaviour (e.g. `sto01_checkpoint_drops_keys_tripwire` asserts k0 is `None` after checkpoint+reopen). Tripwires pass while the bug exists, so CI stays green without hiding the bug, and they **fail the moment the bug is fixed**, which forces the fixer to invert the tripwire into the regression test and update the ledger. No `#[ignore]` and no expected-failure allowlist is needed.

### 4.2 `cargo xtask remediation`

Extends the existing `xtask` crate next to `repo-status`.

| Subcommand | Behavior |
|---|---|
| `check` | Validates schema and invariants; exits non-zero on any violation (below). Runs in `pre-push-check.sh` and in CI on every PR. |
| `render` | Writes `docs/status/remediation.md`: per-phase progress, open criticals first, links to tests and PRs. |
| `render --check` | Fails if the committed page differs from a fresh render (same pattern as the repo-status fingerprint). |

**Invariants enforced by `check`:**
1. Every `id` is unique and matches `^(STO|KEY|EVT|TXN|TTL|RFT|SCH|REL|TST|DOC)-\d{2}$`.
2. `fixed`/`verified` ⇒ non-empty `regression_tests` and `changes`. Every target must resolve: `test:` — file exists and contains the named `fn`; `script:` — file exists and is executable; `ci-job:` — the workflow file defines that job id; `xtask:` — the subcommand is dispatched in `xtask/src/main.rs`. These checks prove the evidence *exists*, not that it ran; `ci_evidence` covers the run. `tripwire` is empty or names a function that no longer exists.
3. `open`/`in_progress` with a non-empty `tripwire` ⇒ that function exists.
4. No listed regression test or tripwire carries `#[ignore]` (reuses `scripts/check_ignore_reasons.sh` parsing).
5. `verified` ⇒ `ci_evidence`; plus `harness` if the area is STO, KEY (except KEY-03), EVT, TXN, TTL, or RFT.
6. `wont_fix` ⇒ `decision`; `duplicate` ⇒ `duplicate_of` names an existing id.
7. Each finding has exactly one integer `phase`. A finding that spans phases is split into separate IDs.
8. A phase can be `gate_passed` only if every finding with that `phase` is `verified`, `wont_fix`, or `duplicate`, and `gate_evidence` is non-empty.

### 4.3 Surfacing

- `docs/status/remediation.md` added to the VitePress sidebar under Status.
- `repo-status` reads the ledger: any open `critical` sets `verification` to red. This fixes the "status is asserted, not derived" root cause for the status page itself.
- A GitHub tracking issue mirroring the ledger is **optional** and needs maintainer approval (public). The ledger remains authoritative.

---

## 5. Branch and change workflow

```
main ──●──────────────────────●─────────────────●──▶
        \                    / phase PR          /
         remediation/root-cause ── commits ── commits (local + private backup)
```

- **Program branch** `remediation/root-cause`, cut from `main` at 6ce73a8. Holds this spec, the plan, and the ledger.
- **Task work** happens as commits (or short-lived local branches `remediation/<ID>-<slug>` merged into the program branch). No public task PRs, per §5.1.
- **Commits:** conventional commits, no attribution trailers. Each task commit series: tripwire → failing regression test → fix → ledger update, with the local benchmark delta in the final commit message body.
- **One finding per commit series** where practical; one series may close several findings only if they share one root fix (e.g. STO-01 + STO-06).
- **Keeping current:** merge `main` into the program branch after Dependabot merges (no force-push on the shared branch).
- **Phase PR sequence** (the only public CI runs):
  1. `pre-push-check.sh` green locally.
  2. Push program branch to `origin`; open PR `remediation/root-cause → main`.
  3. CI green, including the deterministic perf gate. Then the maintainer runs the **`remediation-gate` workflow** (`workflow_dispatch` on the PR head ref): the phase's blocking profile at the gate seed count (10k; sharded across a matrix to stay within job time limits) in every mode that phase requires, plus the simulator from Phase 4. This workflow is the only valid source for `harness` and for `gate_evidence` of Phases 1–4, because the scheduled nightly runs on `main`, which lacks the phase's code until after the merge. GitHub only dispatches workflows whose file exists on the default branch, so `remediation-gate.yml` ships in Phase 0 and reaches `main` with the Phase 0 merge. Phases 0 and 5 have no harness profile: their `gate_evidence` is the ordinary phase-PR CI run URL.
  4. Commit `ci_evidence` / `gate_evidence` URLs to the ledger and flip findings to `verified` and the phase to `gate_passed`, as a final commit on the PR (this last commit must not use `[skip ci]`).
  5. Maintainer merges with the admin override (solo-maintainer rule).

### 5.1 Publishing policy (D9)

**Security findings become public only together with their fixes.** Correctness findings become public at the first push, next to the experimental labels, so users can judge risk.

- **Local until Phase 0 is done.** The program branch, this spec, the Sep 7 review, and the ledger stay off the public `origin` until Phase 0 is complete locally. Phase 0 includes the SCH-01 fix, so no unfixed security finding is ever pushed publicly.
- **One public push per phase**, following the phase PR sequence in §5.
- **Private backup remote.** A separate **private repository** (not a GitHub fork, since forks of public repos cannot be private), created by the maintainer, receives regular pushes of the program branch. **GitHub Actions disabled** on it; it is a backup, not a CI target.
- **Local verification before any push.** `scripts/pre-push-check.sh` runs `cargo fmt --check`, `cargo clippy --workspace -D warnings`, the workspace tests (`cargo test` in Phase 0, `cargo nextest run` from Phase 1), `cargo xtask verify` with the current blocking profile (§7.1, from Phase 1), and `cargo xtask remediation check`. The deterministic perf gate needs Valgrind and runs only in Linux CI; locally, Criterion wall-clock deltas are recorded instead.
- **Public ledger scope.** The rendered status page lists every finding, except that security findings (currently SCH-01) appear only once `fixed`.

---

## 6. Performance gate (D6)

### 6.1 Baselines

**Phase 1 baseline** — `main` @ 6ce73a8, committed to `docs/benchmarks/baseline-2026-09.toml`:

| Metric | Source |
|---|---|
| Single-node put throughput, p50/p99 latency (1 KiB, 64 KiB values) | `crates/prkdb/benches` e2e + WAL benches |
| Batch put throughput (batch 100) | same |
| Point get / index query latency | same |
| Recovery time for a 1 GiB WAL | new bench |
| 3-node write throughput and p99 (in-process cluster) | new bench |
| Consumer poll throughput | existing |

This baseline is **not a target**: its write numbers are inflated by missing fsync (STO-02). It measures each fix's cost.

**Phase 2 durable baseline** — the same metrics in `Durable` and `Fast` modes at the Phase 2 gate, committed as `docs/benchmarks/baseline-format-v2.toml`. Later phases (including the Raft decision in 4b) compare against this like-for-like durable baseline.

### 6.2 Gate mechanics

- **Deterministic gate (phase PRs, Linux CI):** instruction-count benchmarks via `iai-callgrind` (check at implementation time whether the successor crate `gungraun` should be used) for put, get, batch, index update, and WAL append/encode. The base commit and the PR head are benchmarked **in the same job**. The gate fails if any tracked benchmark regresses **> 5 %**, unless the responsible ledger entry has a `perf_note` with a durability or correctness reason.
- **Local per-task deltas:** Criterion runs before/after on the maintainer machine; the delta goes in the commit message body. Informative, not gating.
- **Wall-clock trend:** Criterion runs nightly on `main`; results appended to a trend file. Informative only; shared runners are noisy.
- **Durability modes** (from Phase 2a; both published in docs with numbers):
  - `Durable` (default for new data dirs): ack after the batch is fsynced. Group commit amortizes one `fdatasync` across every write in the batch.
  - `Fast`: ack after write to the OS; background sync every `sync_interval` (default 10 ms). A power cut can lose up to `sync_interval` of acknowledged writes. Docs must say so in those words.

---

## 7. Phases

Estimates are working days for one engineer with agent assistance.

### 7.1 Harness op profiles

The harness always runs a **blocking profile** (must be green; gates pushes) and a **discovery profile** (full op set, nightly on the public repo after the first push and on demand locally; failures create ledger entries, never block). A profile grows only when the findings it would trip are `fixed`.

| From | Blocking profile adds | Modes | Findings it proves |
|---|---|---|---|
| Phase 1 | `Put`, `Delete`, `Reopen` (clean close), `Crash` (in-process drop; nightly: subprocess `SIGKILL`) | current behaviour (Durable config) | TST-03 |
| Phase 2 (after 2d) | `Checkpoint`, `Compact`, multi-collection keys | Durable | STO-01, STO-03, STO-07, KEY-01, KEY-02 |
| Phase 2 (after 2a) | `PowerLoss` (via `Vfs`) | Durable + Fast | STO-02, STO-04, TST-05 |
| Phase 2 (after 2e) | Event emission, restart then read events | Durable + Fast | EVT-01, EVT-02 |
| Phase 3 | `Txn`, concurrent clients, `Poll`, `Commit`, consumer restart, `AdvanceClock` | Durable + Fast | TXN-01..04, EVT-03..06, TTL-01 |
| Phase 4 | Cluster ops in the simulator (§ 4c) | Durable | RFT-01..09, TST-02, TST-06 |

### Phase 0 — Honesty and tracking (≈2 days)

1. Ledger (`docs/remediation/ledger.toml`) populated from §3; `cargo xtask remediation check|render`; CI job; status page in sidebar. Add `.cargo/config.toml` alias `xtask = "run -p xtask --"` and a `toml` dependency to `xtask`.
2. README + docs: clustering, sharding, consumer groups marked **experimental**, linking to the status page.
3. Fix claims that actively mislead:
   - DOC-11 interim wording: *"Transactions currently default to `ReadCommitted`, which does not detect conflicts. Pass `IsolationLevel::Serializable` for conflict detection. Serializable becomes the default in an upcoming release."*
   - DOC-01: document `PRKDB_CLUSTER_SECRET` / `PRKDB_TLS_CLIENT_CA` and the bootstrap token; fix both compose files (peer secret, metrics bind, healthcheck auth, simple-compose port).
   - DOC-06 Rust version; DOC-09 unsourced claims removed or sourced.
4. SCH-01: validate schema collection names against an allowlist (reject separators, `..`, absolute paths, control characters), reject malformed descriptors before any write.
5. `scripts/pre-push-check.sh` (§5.1).
6. Tripwires for every finding that can be reproduced cheaply without the harness (at least STO-01, KEY-01, KEY-03, EVT-01, RFT-03, TXN-04).
7. `.github/workflows/remediation-gate.yml` (`workflow_dispatch`, inputs `ref` and `phase`): runs `remediation check`, the workspace tests, and, when the phase has a profile (§7.1), the sharded harness at the gate seed count. With no profile it runs only the first two. It must be on `main` after Phase 0 so it can be dispatched against later phase PRs.

**Gate:** `remediation check` and `pre-push-check.sh` green locally → phase PR sequence (§5) → Phase 0 findings `verified` via `ci_evidence`. The status page goes live with this push.

### Phase 1 — Harness and baseline (≈7–8 days)

New workspace crate `crates/prkdb-verify` (`publish = false`).

**Components:**

| Unit | Purpose | Depends on |
|---|---|---|
| `model` | Pure reference model: per-collection `BTreeMap`, event log with durable sequence, consumer offsets, TTL clock. No I/O. | nothing |
| `ops` | Operation enum + seeded generator over the **full** op set (§7.1), filtered by profile. | `model` types |
| `sut` | Drives the public embedded API; maps results back to model observations. | `prkdb` |
| `vfs` (in `prkdb-core`) | New **synchronous** filesystem trait covering files *and* directories: `open`, `create`, `write_at`, `read_at`, `set_len`, `sync_data`, `rename`, `remove`, `read_dir`, `sync_dir`. `StdVfs` is the production implementation. Defined in Phase 1; the WAL is routed through it in Phase 2a. (The existing `PlatformIO` trait covers only one open file, has no directory operations, and is unused by the WAL, so it is not a sufficient seam.) | `std::fs` |
| `faultfs` | `Vfs` implementation that tracks synced vs unsynced bytes per file and synced vs unsynced directory entries. `PowerLoss` drops unsynced data and entries, optionally tearing the last write at a random byte. Unit-tested in Phase 1; wired into the harness when 2a lands. | `vfs` |
| `checker` | After each restart op: **Durable** — SUT state equals model state for every acknowledged op. **Fast** (from 2a) — SUT state equals the model at some prefix no earlier than the last completed sync. Events: no gaps, no duplicates beyond at-least-once, offsets monotonic. | `model`, `sut` |
| `runner` | `cargo xtask verify [--profile blocking|discovery] [--seed N] [--seeds K] [--mode durable|fast] [--ops M]`; prints the failing seed and a minimized op sequence; reports how many checks were actually compared (a run that compared nothing fails). | all |

**What really runs on `main` in Phase 1:** the Phase 1 blocking profile (`Put`/`Delete`/`Reopen`/`Crash`). With no fsync, a process crash still leaves mmap writes in the page cache, so the Durable checker is valid for these ops today. `PowerLoss` and Fast mode wait for 2a. This is expected, not proven: STO-07 (acknowledgement before the WAL write) or STO-03 may trip even these ops. **Rule:** if a blocking-profile op trips a known finding, that op (or op combination) moves to the discovery profile, the finding gets a tripwire, and the op returns to the blocking profile when the finding is `fixed`.

**Also in Phase 1:**
- **Checker can fail:** the discovery profile (with `Checkpoint`) reproduces STO-01 on `main`. Meta-test: a deliberately broken SUT wrapper (drops every 10th put) must be caught by the blocking profile.
- Switch to `cargo nextest` with a `ci` profile (test groups for process-spawning tests, slow-timeout, retries reported not hidden).
- TST-04 and the §6.1 Phase 1 baseline.
- CI: blocking profile 200 seeds per PR (time-boxed ~5 min); nightly 20k seeds blocking + 2k discovery (non-blocking).

**Gate:** blocking profile green; discovery reproduces STO-01; meta-test green; baseline committed; nextest in CI; then phase PR sequence.

### Phase 2 — Format v2 and single-node root fixes (≈12–15 days)

**2a. One WAL (STO-06, STO-02, STO-04, STO-05, TST-05, TST-07).**
- Consolidate to **one WAL implementation that is a single, globally ordered log** per data directory. The current hash-routed parallel shards go away. Every record gets one monotonically increasing global offset. That is what makes transaction atomicity (one batch record), event identity (2c), read-set versions (Phase 3), and recovery order (STO-05) straightforward. Partitions become logical (a field on the record, indexed for consumers), not separate physical logs. Throughput comes from group commit on the single writer, the way Postgres, RocksDB, and SQLite WAL mode do it. This gives up no parallelism that was actually in use (STO-06: all data writes already hit one shard). Segment and `Vfs` code stay shard-agnostic, so shards can return if measurement shows the single writer is the bottleneck, done properly: a global sequence assigned at commit, one commit record per transaction, and replay merged by sequence.
- Segment files remain (rolling, CRC per record). **Writes go through the `Vfs` trait (`pwrite` + `fdatasync` per group-commit batch)**; reads use mmap or `pread`. Rationale: `msync` on shared mmap gives weaker, platform-dependent guarantees, and routing writes through `Vfs` makes durability testable with `faultfs`. **Routing the WAL through `Vfs` is an explicit 2a deliverable; `PowerLoss` joins the blocking profile when it lands.**
- `SyncMode` becomes real: `Durable` and `Fast` as defined in §6.2.
- `Vfs` is synchronous. The WAL gets a **dedicated group-commit writer thread** (`std::thread`, not a tokio task) that owns the active segment, performs `pwrite` + `fdatasync`, and completes callers' oneshot channels. The async API never blocks the runtime, and the thread's liveness is covered by the existing writer-liveness design (`2026-08-11-wal-writer-liveness.md`).
- **First task is a 1–2 day spike:** single-log group-commit `pwrite` path vs the current parallel mmap path, measured against the Phase 1 baseline. Escalate to the maintainer before proceeding if `Fast` mode loses > 15 % put throughput, or if the single log is the bottleneck. In that case, the fallback to evaluate is sharded logs with a global sequence number assigned at commit.
- CRC verified on every open; torn tail truncated at the first bad record and logged; directory fsync after segment create/rename/remove.
- Delete the other WAL implementations after migrating their callers.
- TST-07: `cargo-fuzz` targets for WAL record decode, segment scan, checkpoint/snapshot load, and proto decode (nightly, public repo).

**2b. Format v2 (D3, D4).**
- `FORMAT` file at data-dir root: `format = 2`, `created_by = "<version>"`, written atomically (temp + fsync + rename + dir fsync).
- Open rules: empty dir → create v2; `FORMAT` = 2 → open; missing on non-empty dir or unknown version → refuse: *"data directory was created by an older/newer PrkDB (format N); this version reads format 2. See docs/guide/upgrade."*
- Migration registry `trait Migration { fn from(&self) -> u32; fn to(&self) -> u32; fn run(&self, dir: &Path) -> Result<()> }`, empty in v2; `prkdb-cli migrate --data-dir` lists and runs applicable migrations (today: "no migrations available for format 2").
- Raft storage lives under `raft/` with its **own** format marker, frozen at the Phase 4 gate (clustering is experimental until then).

**2c. Stable identity (KEY-01, KEY-03, STO-05, EVT-01).**
- Key codec (own module, extracted from `indexed_storage.rs`): `[namespace_len][namespace][collection_id: u32][key bytes]`. `collection_id` assigned from a persisted catalog keyed by the collection's **persisted name** (`#[collection(name = "...")]`, defaulting to the snake-case type name *recorded at first registration*; renaming a Rust type does not change it). All CRUD, index, transaction, scan, backup paths use the codec.
- Partitioner: fixed-seed `seahash` (already a dependency in `crates/prkdb`). Golden test vectors are committed, so any change to key→partition mapping fails CI.
- Event identity (single-node): an event's sequence **is** the global WAL offset of its commit record (2a). No separate counter exists that could reset. Consumer offsets are global offsets; a per-partition consumer simply skips records of other partitions. Sled/SQL backends use a persisted monotonic sequence row updated in the same transaction as the data.
- Event identity (cluster mode, Phase 4): the sequence is the **Raft log index** of the committed entry, identical on every node, so consumer offsets survive leader failover, snapshot install, and compaction. The event API exposes an opaque, ordered `EventSeq`, so the node-local WAL offset (single-node) and the Raft index (cluster) never leak as different types.

**2d. Recovery (STO-01, STO-03, STO-07).**
- Invariant: `recover(checkpoint, wal) == recover(∅, wal)` for every WAL — property test in `prkdb-verify`.
- Checkpoint = atomically written index snapshot (key → offset) at `max_offset` plus the offset. Recovery loads the snapshot, then replays WAL after it. No snapshot or bad CRC → full replay.
- Compaction rewrites live records into new segments, fsyncs, then removes old segments; `truncate_before` becomes real or is deleted.
- WAL append and index publish happen under one ordering point so the live index can never point at an older offset than recovery would.
- `BatchAccumulator::flush()` waits on a sequence-numbered barrier and returns the first executor error; bounded admission by bytes.

**2e. Index correctness (KEY-02), outbox persistence (EVT-02), schema (SCH-02).**
- `upsert` reads the prior record under the same lock and removes *its* index entries; `#[index(unique)]` enforced at commit (violation → typed error, no partial write).
- Outbox written in the same WAL batch as the data it describes; partitioned adapter persists or returns `UnsupportedCapability`, never Ok-and-drop.
- Schema registry: fail closed on missing/corrupt descriptor; atomic write; version allocation serialized.

**Storage-compat check.** From 2b: a committed **golden v2 data directory** (written by the program branch, covering every record type) must be readable by every build, in `pre-push-check.sh` and CI. After the Phase 2 PR merges to `main`, a nightly job adds the Iggy-style check: the `main` binary writes a data dir, the HEAD binary reads it. This is not enabled earlier, because before that merge `main` writes format v1, which HEAD correctly refuses. A `breaking:storage` label is the only escape hatch, and it requires a registered migration (D4).

**Gate:** all Phase 2 findings `verified`; Phase 2 blocking profile (§7.1) at 10k seeds green in both modes; golden-dir compat check green; Phase 2 durable baseline (§6.1) committed; perf gate green or `perf_note`s recorded.

### Phase 3 — Semantics (≈7 days)

**Transactions (TXN-01..04).**
- One WAL batch record per transaction containing all puts, deletes, and outbox events, closed by a commit marker; recovery applies only complete batches.
- Serializable (default): optimistic concurrency with **versions, not hashes** — read set records the global WAL offset of the version seen per key at first read (absent = 0). Validation and publish happen under one commit lock. ReadCommitted takes the same commit lock and performs write-write conflict detection.
- Indexed transactions use the same batch path; no per-op loop.

**Consumers (EVT-03..06).**
- Default at-least-once: auto-commit commits the positions of the *previous* poll's records, not the current poll's.
- Consumer groups carry a generation id; commits from a stale generation are rejected.
- Unassigned consumer → error, never partition 0. Commit errors propagate.
- Batches are partition-aware; only committed items emit events. Sled outbox uses a single transaction across trees. Atomic-required callers never fall back silently; the best-effort path, if kept, is a separately named API.

**TTL (TTL-01).** Value and expiry in one record; expiry deletes conditionally on the version it observed; background expiry starts automatically with the database (documented config to disable).

**Gate:** Phase 3 findings `verified`; Phase 3 blocking profile (§7.1) at 10k seeds green in both modes; existing WGL/Jepsen workloads green; perf gate green or `perf_note`s recorded.

### Phase 4 — Raft (≈12–18 days)

**4a. Spike (2–3 days).** Pin the latest stable openraft release at spike start that provides the storage-v2 traits (`RaftLogStorage`, `RaftStateMachine`) and `openraft::testing::Suite` (0.9.x at time of writing; module paths have moved between releases). Implement those traits and wire the gRPC transport as openraft's `RaftNetwork`. The **Raft log is a separate store under `raft/`**, not the state machine's WAL, because Raft needs suffix truncation of conflicting uncommitted entries and prefix purge after snapshots. It reuses the Phase 2 segment and `Vfs` code, adding `truncate_after(index)` and `purge_before(index)`. Each node's state machine applies committed entries into its own single-node WAL/index, as in Phase 2.

**4b. Decision gate.** Compare like with like: durable replicated vs the **Phase 2 durable single-node baseline** (§6.1), not the unsafe Phase 1 numbers. Adopt openraft if all hold:
1. `openraft::testing::Suite` passes against our storage.
2. 3-node in-process durable write throughput ≥ 60 % of the Phase 2 single-node `Durable` throughput, and p99 ≤ 2.5× single-node p99. (Replication adds at least one follower round-trip plus a follower fsync per group commit.)
3. No blocker in trait fit (auth, TLS, read-index, membership).

If criterion 2 alone fails, record the numbers and escalate to the maintainer before choosing the repair path, because the repaired in-house path pays the same round-trip and fsync costs.

Otherwise: record numbers in the ledger and repair in place (hard-state persistence, indexed term-tagged log with reload, correct majority, `log_start_index` everywhere, waiter failure on step-down, no-op on election, snapshot restore that clears state). Either path fixes RFT-01..07.

**4c. Deterministic simulation (TST-06; ≈4 of the phase's days).** `madsim` with seeded partitions, crashes, disk faults, and clock skew. madsim is not fully drop-in: it needs `cfg(madsim)` builds, `madsim-tokio` / `madsim-tonic` substituted across the affected crates, and its own filesystem shim. Disk faults therefore come from running `faultfs` behind `Vfs` inside the simulation, not from madsim's fs. If substitution proves too invasive, fall back to `turmoil` with the transport behind a trait. Histories feed the existing WGL checker. Rules borrowed from Iggy's simulator: separate PRNG streams per concern, seed printed on failure, `--seed` replay, and a report of how many operations the checker actually compared (an empty run must fail).

**4d.** RFT-08 (peer auth actually sent; mTLS server config), RFT-10 (resolve hostnames in `CLUSTER_NODES`, bind `0.0.0.0:<port>`, advertise the configured name), RFT-09 (root-cause the flaky baseline with the simulator, no timeout bumps), TST-02 (multi-writer, multi-reader workloads, failed reads classified not dropped), TST-01 (chaos monkey asserts `missing == 0`).

**Gate:** Phase 4 findings `verified`; simulation 10k seeds green; `raft/` format frozen and added to storage-compat; experimental label removed from clustering only if all of the above hold.

### Phase 5 — Documentation and release (≈5 days)

- Every guide sample becomes a compiled example (`examples/docs/*`) or doctest included into the page; CI fails if a sample does not build (DOC-03).
- Credentialed clients everywhere; Python client gains credential support or is removed from docs (DOC-02); global `--credential` honored by all subcommands (DOC-04).
- `docker compose up` and the 3-node recipe run in CI as a smoke test (DOC-12).
- CLI reference generated from clap definitions; README commands corrected (DOC-05, DOC-07).
- New pages: consumer groups, Docker, Raft operations, troubleshooting, upgrade/format policy, durability modes with numbers (DOC-08).
- `ignoreDeadLinks: false`; orphan pages linked (DOC-10).
- REL-01: versioned internal deps; package every crate before publishing any; failures never suppressed.

**Gate:** all DOC/REL `verified`; docs build strict; compose smoke green.

### Phase 6 — AI layer

Scope and architecture are defined by `2026-09-07-ai-database-design.md` and `2026-09-07-embedded-ai-state-design.md` (separate AI crates, typed capability contracts, SQLite-first AI profile, native backend only after conformance). This program adds two constraints:

1. `prkdb-ai-native` may be advertised only after Phases 2–3 gates pass.
2. The durable, replayable agent event log ("replay to step N", "fork a run", "tail a run") is the headline differentiator; Python bindings (PyO3), LangGraph checkpointer, and MCP server follow the Sep 7 package boundaries.

A separate plan is written at phase start.

---

## 8. Error handling principles (all phases)

- **Fail before mutation.** Unsupported capability, invalid name, unique violation, stale generation → typed error with no side effects.
- **Never downgrade silently.** No automatic fallback from an atomic path to a non-atomic one.
- **No `let _ =` on durability, commit, or offset paths.** Enforced by a clippy lint allowlist review in Phase 2.
- **Refuse to open, rather than misread.** Unknown format, bad checkpoint CRC with no fallback, missing schema descriptor → clear error naming the file and the fix.

---

## 9. Code structure changes (targeted only)

Touch only what the fixes require:

- Extract the key codec, index maintenance, and transaction commit path out of `indexed_storage.rs` (7.6k lines) into focused modules as they are rewritten.
- `wal_adapter.rs` (4.5k lines): recovery and checkpoint logic move to `storage/recovery.rs` / `storage/checkpoint.rs`, which already exist.
- `raft/node.rs` (2.6k lines) is replaced (openraft) or split into log store, election, replication, read-index (repair path).

No unrelated refactoring.

---

## 10. Risks

| Risk | Mitigation |
|---|---|
| Durable-mode throughput drops sharply once fsync is real | Group commit; `Fast` mode with documented window; publish both; escalate at 2a spike if > 15 % in `Fast`. |
| Harness finds many more bugs than listed | Expected and desirable. New findings enter the ledger with IDs; phase scope grows; estimates revised in the plan. |
| openraft fit worse than expected | Decision gate 4b; repair path fully specified. |
| Program branch drifts from main | Merge main after Dependabot merges; phase PRs keep divergence ≤ one phase. |
| Maintainer bandwidth for admin merges | One merge per phase to main; task work is local commits on the program branch (§5). |

---

## 11. Effort summary

| Phase | Days |
|---|---|
| 0 Honesty and tracking | 2 |
| 1 Harness and baseline | 7–8 |
| 2 Format v2 + single-node | 12–15 |
| 3 Semantics | 7 |
| 4 Raft | 12–18 |
| 5 Docs and release | 5 |
| **Total before AI** | **45–55 (~9–11 weeks)** |
| 6 AI | per Sep 7 plan |

---

## 12. Revision history

| Rev | Date | Change |
|---|---|---|
| 1 | 2026-09-23 | Initial spec from the 2026-09-23 audits and the 2026-09-07 review; decisions D1–D9 (D9: publishing policy). |
| 2 | 2026-09-23 | Spec review pass 1: `Vfs` seam defined in Phase 1, WAL routed through it in 2a; per-area `verified`; tripwires instead of expected-failure lists; single-phase IDs (DOC-11, DOC-12, TST-05..07 split out); harness op profiles per phase (§7.1); Fast mode from 2a; commit-based workflow with phase-PR CI sequence; private backup repo with Actions off; storage-compat timing; single globally ordered WAL; like-for-like Raft gate; madsim budget; DOC-11 interim wording. |
| 6 | 2026-09-23 | Execution: RFT-10 added (hostnames rejected in `CLUSTER_NODES`), found by the Batch E docs review. |
| 5 | 2026-09-23 | Plan review: TST-01 moved to Phase 4 (random chaos loss cannot be a deterministic tripwire, and the test is `#[ignore]`d for needing a server binary). |
| 4 | 2026-09-23 | Spec review pass 3: `remediation-gate.yml` ships in Phase 0 so it is dispatchable from `main`; Phases 0 and 5 use the phase-PR CI run as `gate_evidence`; noted that evidence checks prove existence, not execution. |
| 3 | 2026-09-23 | Spec review pass 2: evidence kinds for non-code findings (`script:`, `ci-job:`, `xtask:`); TST-01 redefined as the allowance; `remediation-gate` workflow as the only source of gate evidence; cluster event identity = Raft index behind opaque `EventSeq`; Raft log as separate `raft/` store with suffix truncation; blocking-op demotion rule; dedicated WAL writer thread; KEY-03 harness exemption. |
