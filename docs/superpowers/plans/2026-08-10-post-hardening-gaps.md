# Post-Hardening Gaps Implementation Plan

> **For agentic workers:** Steps use checkbox (`- [ ]`) syntax for tracking. Every task states
> what must be *observed*, not merely written — a task is done when the check it installs has
> been seen to fail for the right reason and then pass.

> **Status, 2026-08-11: Tasks 1–9 shipped.** Merged as #45, #46, #47, #48, #49 and #50.
> Two items remain unticked in Task 10, and the "not in this plan" section below is
> unchanged. This header exists because a plan whose boxes no longer match the repository
> is the same defect the plan was written to expose — a document asserting a state the code
> does not have.
>
> **Three of the four E1–E4 decisions were not decisions.** Reading the code answered them:
> single-node already takes the local path because `main.rs` builds Raft options only with
> `--peers`; bootstrap must stay local because it runs ~320 lines before Raft starts; and
> `propose` already returned `NotLeader` immediately. Only E1 was open, and researching it
> surfaced a fourth option the plan had not listed — letting principals hash like any other
> key — which was rejected because availability would then depend on which partition a
> *name* hashes to.
>
> **What the plan missed.** Task 5 said "principals in the state machine". That alone would
> have shipped a revoke that reports success and revokes nothing, because `resolve` reads an
> in-memory map loaded at startup. Replicating the durable write is half the job.
>
> **What following it turned up.** Task 7's HTTPS test found `--tls-cert` panicking a worker
> on every connection while reporting success. Task 1's investigation found the cluster
> suite starving itself on threads. The Task 10 mutation work found 11 survivors that were
> unreachable code — a writer thread nothing ever sent to, spawned once per adapter.

**Goal:** Close the gaps a verified audit found after the correctness-hardening effort shipped,
without destabilising what that effort made trustworthy.

**Architecture:** Stabilise before widening. The audit's headline fix is *turning on tests that
have never run*, and there is currently one chaos test failing intermittently for reasons nobody
has diagnosed. Enabling more tests on top of an unexplained intermittent makes every subsequent
red ambiguous — new breakage, or the same ghost? So the sequence is: make failures diagnosable,
then enable, then correct false claims, then take on the one item with real design risk.

**Tech Stack:** Rust 2021 (1.95.0 pinned), tokio, tonic, cargo-mutants, GitHub Actions.

**Source:** Audit of `docs/superpowers/specs/2026-08-08-correctness-and-production-readiness.md`
performed 2026-08-10 against the code rather than against the spec's own checkboxes. Every finding
below was independently re-verified before being written down; the verification command is
recorded with each task.

---

## Decisions required before Task 5

Task 5 (principals through Raft) cannot start until these are answered. Everything before it can.

| # | Question | Options | Recommendation |
|---|---|---|---|
| **E1** | Which partition owns the authz keyspace? | (a) partition 0 by convention; (b) a dedicated metadata Raft group; (c) replicate to every partition | **(a)** — smallest change, no new Raft group to operate. (b) is correct long-term but is a second consensus group to elect, snapshot, and monitor. |
| **E2** | What happens to admin writes when that partition has no leader? | (a) refuse with a clear error; (b) queue; (c) fall back to local write | **(a)** — (c) is what the code does today and is the bug. Refusing is honest and matches how every other write behaves. |
| **E3** | Do existing single-node deployments keep working unchanged? | (a) yes, local persist stays the path when `nodes.len() == 1`; (b) always propose | **(a)** — single-node users must not need a Raft round-trip to add a principal, and `authz_persistence.rs` already proves that path. |
| **E4** | Is a bootstrap admin proposed or written locally? | (a) locally, before serving; (b) proposed | **(a)** — bootstrap runs before the node joins a cluster; proposing would deadlock waiting for a leader that needs the credential to exist. |

---

## Sequencing

```
Task 1  diagnosable chaos failures        ── unblocks everything, no dependencies
Task 2  enable the two dead tests         ── depends on 1 (needs diagnosable output)
Task 3  close the cfg-gate class          ── with 2, same file
Task 4  correct the false claims          ── independent, zero risk
Task 5  principals through Raft           ── needs E1-E4; the only item with design risk
Task 6+ small independent fixes           ── any order, any time
```

Tasks 1–4 and 6+ are safe to parallelise across PRs. Task 5 is not.

---

## Task 1: Make a chaos failure say why (no requirement id — new)

**Evidence.** On run 31407756063 (PR #43, which changed only the mutation job and cannot affect
Raft), `an_isolated_leader_refuses_a_linearizable_read` failed:

```
crates/prkdb/tests/read_consistency_modes.rs:308:5:
an isolated leader served a ReadIndex; it cannot know its commit index is current
```

Re-running the same commit with no change turned it green. It passed on the previous five main
runs, and 20/20 locally including 12 under saturated CPU. **It is non-deterministic and
undiagnosed.**

Two possibilities, and the current output cannot distinguish them:

1. the harness races — the partition is not yet in force when `read_index_on` is called
2. `ReadIndex` has a real rare window where an isolated leader confirms leadership

One hypothesis is already eliminated: a cached connection bypassing the partition. The chaos check
runs on the cache-hit path (`crates/prkdb/src/raft/rpc_client.rs:136`).

This matters beyond one test. Retrying until green is the same shape as the failures this
repository has spent a release removing — a red signal made to go away rather than understood.
The difference between a flaky test and a rare correctness bug is exactly what a linearizability
suite exists to tell you.

- [x] Assert the partition is in force before the read, rather than assuming it: query the chaos
      rules and fail with a distinct message if the expected rules are absent
- [x] On failure, report the leader's term, whether it still believes itself leader, and how many
      peer acks `confirm_leadership` collected — so the next occurrence distinguishes (1) from (2)
      without a re-run
- [x] Run the test 50× locally and 3× in CI; record the observed failure rate in the test's doc
      comment, even if it is 0
- [x] If a failure reproduces, follow it to a root cause before closing this task

**Acceptance:** a future failure of this test is diagnosable from its own output alone.

**Effort:** 2–4 hours. **Blocks:** Task 2.

---

## Task 2: Run the two acceptance tests that have never executed (R1.2, R12.16)

**Evidence, verified by execution not inspection:**

```
$ cargo test -p prkdb --test jepsen_consistency_tests -- --list | grep -c ': test'
18
$ cargo test -p prkdb --features chaos --test jepsen_consistency_tests -- --list | grep -c ': test'
19          # a_replicated_register_is_linearizable_across_a_partition

$ cargo test -p prkdb --test peer_authz -- --list | grep -c ': test'
17
$ cargo test -p prkdb --features chaos --test peer_authz -- --list | grep -c ': test'
18          # a_cluster_elects_and_replicates_with_peer_auth_configured
```

`.github/workflows/chaos-tests.yml:105` runs `jepsen_consistency_tests` **without**
`--features chaos`. `peer_authz` is named in **no workflow step at all**.

So R1 acceptance #2 — which the spec calls the most important test in its family, and which is the
workload that found S-06 — has never run in CI. Neither has R12 acceptance #16, the regression the
spec says "would otherwise be found in production."

The neighbouring steps at `chaos-tests.yml:117,120,123` *do* pass `--features chaos`. This was a
miss, not a decision.

- [x] Add `--features chaos` to the `jepsen_consistency_tests` step
- [x] Add a step running `peer_authz` with `--features chaos`
- [x] **Run each 10× before merging.** A test executing for the first time in its life may fail,
      and it must not first do so as a required check blocking an unrelated PR
- [x] If either fails, treat it as a finding and fix the cause — do not disable it again

**Acceptance:** both tests appear in CI output and have been observed to pass repeatedly.

**Effort:** 30 minutes, plus whatever the first real run surfaces. **Depends on:** Task 1.

---

## Task 3: Close the `cfg(feature = "chaos")` coverage cliff (no requirement id — new)

R16 solved this exact problem for `#[ignore]`: every one must carry a reason from a closed
category, enforced by `scripts/check_ignore_reasons.sh`, because a switched-off test is invisible.

Feature-gating hides a test **more** thoroughly. An `#[ignore]`d test still appears in `--list`
output as `ignored`. A `cfg`-gated one leaves no trace at all — which is why `plan_status.sh`
reports `55/55 complete` while two named acceptance tests have never executed.

There are eight chaos-gated tests across six files. Six are named by a workflow step. Two are not,
and nothing in the repository would tell you which.

- [x] Add a check enumerating every test function under `#[cfg(feature = "chaos")]`
- [x] Assert each containing file is named by a workflow step that passes `--features chaos`
- [x] Fail with the specific file and test name, not a count
- [x] Verify it fails when the `--features chaos` is removed from one step, then passes

**Acceptance:** removing `--features chaos` from any step turns the check red.

**Effort:** 2–3 hours. **Pairs with:** Task 2.

---

## Task 4: Correct two artefacts that claim a property the code lacks (no requirement id — new)

Both are false **today**, independently of when Task 5 lands, and both are what a reviewer reads
instead of the code.

- `crates/prkdb/src/authz/store.rs:14-16` — "replicated by Raft when the node is clustered,
  captured by `take_snapshot`". Neither is true on the path both binaries use.
- `crates/prkdb/src/authz/store.rs:428` `principals_round_trip_through_a_snapshot` — a **serde**
  round-trip over an in-memory `Vec`. It never touches `handle_install_snapshot`, yet its doc
  comment cites the spec's §6 abort criterion, which makes it read as coverage it does not provide.

- [x] Rewrite the module doc to state what is actually true: each node persists its own principals
      locally and reloads them on restart; there is no cross-node replication yet
- [x] Rename the test to describe what it tests (serde round-trip), or extend it to genuinely
      exercise `handle_install_snapshot`
- [x] Cross-reference Task 5 so the limitation is discoverable

**Acceptance:** no doc comment or test name in `authz/` asserts replication that does not exist.

**Effort:** 30 minutes. **Independent.**

---

## Task 5: Replicate principals through Raft (spec §6 stop-before-shipping)

**Evidence.** `crates/prkdb/src/raft/state_machine.rs` contains **zero** occurrences of `principal`
or `authz`. Both binaries write principals through `db.storage()` — the standalone `meta` adapter
built at `crates/prkdb/src/db.rs:132-138` — with a plain `put`, never a proposal:
`crates/prkdb-cli/src/commands/serve.rs:242`, `crates/prkdb-cli/src/admin_principals.rs:147`,
`crates/prkdb/src/bin/prkdb-server.rs:92`.

**Consequences on a real cluster:** a principal created via `PUT /admin/principals` exists only on
the node that served the request; a revoke on node 1 leaves the credential live on nodes 2 and 3;
`install_snapshot` carries no credential state.

**Mitigating, and worth stating plainly:** each node *does* persist its own principals and reload
them on restart — proven by `crates/prkdb/tests/authz_persistence.rs`. The failure mode is
cross-node divergence, not credential loss. That is materially less bad than the spec feared.

Requires **E1–E4** answered first.

- [x] `Command::UpsertPrincipal` / `Command::RevokePrincipal` in the state machine
- [x] Route admin writes through the proposal path when clustered; keep the local path for
      single-node (E3)
- [x] Include the authz keyspace in `snapshot` and `handle_install_snapshot`
- [x] **A cross-node test**: create a principal on node 1, authenticate with it on node 2; revoke
      on node 1, confirm node 2 refuses it. No such test exists — `grep principal
      crates/prkdb/tests/*.rs` returns nothing cluster-scoped
- [x] A snapshot test that installs a snapshot and confirms principals arrive with it
- [x] Verify by reverting the replication and observing both new tests fail

**Acceptance:** a credential revoked on one node is refused on every node.

**Effort:** 2–4 days. **Blocked on:** E1–E4.

---

## Task 6: Source or remove the README's headline performance claim (R15.1, R15.3)

`README.md:15` claims **894K queries/sec** with no link. `docs/benchmarks/methodology.md:47`
records that exact figure as *"Unverified. Against in-memory storage, if reproducible at all."*
`README.md:819` similarly carries `# 199K writes, 7.3M reads`.

Neither existing check catches it: `scripts/plan_status.sh:117-122` scans only `crates/*/src` doc
comments and matches only the literal `ops/sec`, so "queries/sec" in a README passes both filters.
`xtask/src/repo_status/collectors/docs.rs:33-63` only checks the Kafka caveat string.

- [x] Link every README performance figure to the methodology page, or delete it
- [x] Extend the drift collector to fail on an unlinked performance claim in `README.md` (R15.3,
      currently NOT STARTED)
- [x] Verify the collector fails on a planted unlinked claim

**Effort:** 2–4 hours. **Independent.**

---

## Task 7: Prove the HTTPS listener works and rejects plaintext (R13.3)

HTTP TLS is wired (`crates/prkdb-cli/src/commands/serve.rs:652-667`, `axum_server::bind_rustls`),
but `crates/prkdb-cli/tests/tls_integration.rs` contains only two tests: `--help` advertises the
flags, and a half-configured `--tls-cert` is rejected. Its own module doc concedes it establishes
only "that the capability is reachable from the command line."

The Raft half **is** covered — `crates/prkdb/tests/peer_mtls.rs` drives `RpcClientPool` and asserts
a plaintext pool fails against a TLS listener. The HTTP half has no equivalent. This is the
S-02/S-10 shape the spec warns about twice: a capability exercised on the side that works.

- [x] Serve over HTTPS, complete a real request, assert plaintext is refused
- [x] Verify by pointing the client at `http://` and observing the failure

**Effort:** 3–6 hours. **Independent.**

---

## Task 8: Consolidate the duplicate reqwest (R10)

`crates/prkdb/Cargo.toml:48` uses reqwest 0.12; `crates/prkdb-cli/Cargo.toml:54` uses 0.11.
`deny.toml:104-109` documents the deferral and keeps `multiple-versions = "warn"` — which is the
"suppress rather than fix" outcome R10's note said not to take. Two HTTP stacks, two TLS
configurations, two CVE surfaces.

- [x] Hoist reqwest into `[workspace.dependencies]` at 0.12
- [x] Fix the CLI's `blocking` feature usage
- [x] Tighten `deny.toml` once the duplicate is gone

**Effort:** 2–4 hours. **Independent.**

---

## Task 9: Audit logging (spec §5)

No record of who performed which admin operation. This got cheaper and more valuable since the
spec: there are now named principals, and `crates/prkdb-cli/src/admin_principals.rs` is a live
credential-minting surface with no log of who used it.

- [x] Log principal name, operation, target, and outcome for every admin mutation
- [x] Never log the credential or its digest
- [x] Test that a denied attempt is logged as well as a permitted one

**Effort:** 0.5–1 day. **Independent.**

---

## Task 10: Smaller items

- [x] **gRPC backpressure (P11).** `RateLimiter` is wired only as an axum layer
      (`crates/prkdb-cli/src/probes.rs:113`). The gRPC data plane — including `fetch_segment`,
      which streams raw WAL — has no rate limit and no in-flight bound.
- [ ] **Upgrade path (P10).** The prerequisite landed (`log_segment.rs:13,17` define
      `PRKDB_WAL_MAGIC` and `FORMAT_VERSION`, and refuse a future version), but no rolling-upgrade
      procedure or compatibility policy exists in `docs/`.
- [x] **Stale exemption.** `scripts/check_wrapper_completeness.sh:56` still exempts
      `get_changes_since`, which `collection_partitioned_adapter.rs:702` now implements. The check
      silently skips a method it would pass, weakening what it attests.
- [ ] **`plan_status.sh` file-existence checks.** Nine checks are `test -f <path>`. The script's
      own comment at line 208 identifies this trap for the gRPC layer and correctly checks
      registration separately — the same reasoning was not applied to test files. `test -f
      crates/prkdb/tests/read_consistency_modes.rs` says nothing about whether CI runs it.

---

## Explicitly not in this plan

- **Multi-tenancy (P9).** `builder.rs:98` `with_namespace` is one namespace per DB instance. Now
  the single row between PrkDB and the FalkorDB FREE tier, since R12 closed access control. Large;
  needs its own spec.
- **Distributed tracing (P8).** No `opentelemetry` dependency anywhere. 1–2 days, low urgency.
- **R3.4 (10 consecutive clean runs).** The mechanism is in place — ephemeral ports,
  poll-until-deadline, `timeout-minutes` everywhere — but nothing records or enforces the result.
  Could not be verified either way; listed so it is not mistaken for done.
- **Vector search (spec §4).** Deliberately unplanned; gets its own spec. Its sequencing gate
  ("after R1–R8") now appears clear, but that is a reason to write the spec, not to start coding.
- **Publishing to crates.io.** A decision, not debt. The concrete blocker is named in
  `deny.toml:110-114`: workspace path dependencies carry no `version` field.

---

## What this plan assumes, and how it could be wrong

The audit was performed against the code, and its three highest-severity findings were
independently re-verified by execution before being written here. The remainder were verified by
the auditing agent but **not** re-checked by hand — Tasks 6–10 rest on citations that should be
confirmed at the point of doing the work, not trusted because they appear in this document.

That caveat is the same lesson the hardening effort produced: a plan is a claim about the
repository, and claims about this repository have a poor track record until something executes
them.
