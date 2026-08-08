# Correctness & Production Readiness — Design Spec

**Date:** 2026-08-08
**Revision:** 6 (four review passes, decisions applied; see §0 and §9)
**Baseline commit:** c839ef2 (main)
**Toolchain:** rustc 1.95.0
**Extends:** `2026-03-27-production-hardening-round-2.md`

---

## 0. Decisions

All blocking questions were resolved on **2026-08-08**. This section is the record; the
requirements below assume these answers.

| # | Decision | Consequence |
|---|---|---|
| **D1** | **Raft stays**, and gets verified properly | Full correctness scope. R1, R4, R6, R7, R14 are all live. ~9 days. The audit's alternative — cut Raft, ship single-node — was considered and rejected. |
| **D2** | **Users and per-collection roles**, not a single API token | R12 grows from a 2-day token gate into a real authorization model: user store, role model, per-collection permissions. ~6 days. |
| **D3** | **No interim mitigation** for S-01 — go straight to RBAC | The data planes stay unauthenticated until the role model lands. See the risk note below. |
| **D4** | **`Health` public, `Metadata` authenticated** | Orchestrator probes work; cluster topology is not disclosed to unauthenticated callers. Mirrors the HTTP `PUBLIC_PATHS` list. |
| **D5** | **In-process cluster harness**, not a CI binary build | ~2 days more than the cheap path, but every cluster test becomes runnable under plain `cargo test` — which is what stops them being `#[ignore]`d again. Unblocks most of R16. |
| **D6** | **In-process Wing & Gong checker**, not Elle | ~300 lines, no JVM in CI, failures arrive on the PR rather than on a schedule. The Task 3 meta-test is what guards the checker's own correctness. |
| **D7** | **mTLS client certificates** for Raft peer identity | Reuses the R13 TLS machinery, adds encryption in transit, no shared secret to rotate. Cluster cannot run without TLS configured. |

> **Risk accepted under D3.** S-01 leaves both data planes readable and writable, and
> `fetch_segment` streaming raw WAL, until the role model ships — weeks rather than days. This is
> defensible here specifically because PrkDB is not deployed anywhere reachable: nothing is
> published to crates.io, the repo has 8 stars and 0 forks, and there is no managed offering. The
> exposure is theoretical while that stays true.
>
> **It stops being theoretical the moment anyone runs `prkdb-cli serve` on a routable
> interface.** If that becomes likely before R12 lands, revisit D3 — the single-token interceptor
> is ~2 days and is not throwaway work, since the role model reuses its interceptor,
> constant-time comparison, and client plumbing.

D2 and D5 together add 6 days to the original ~20-day estimate. See §7 — the rows sum to 26.5.

---

## 1. Purpose

The 2026-08-08 staff audit found PrkDB's *implementation* substantially ahead of its *evidence*.
This spec converts every audit finding into a testable requirement, adds the security findings
discovered while scoping production readiness, and answers two strategic questions: whether to
add vector search, and what is missing for a production tier.

**Guiding principle:** a database earns trust by what it can prove. Every requirement below is
written so a failing state is detectable by CI.

---

## 2. Security findings (not in the original audit)

Both are more severe than anything the audit found. **S-01 was understated in revision 1** — see
§9.

### S-01 — Both data planes are unauthenticated

PrkDB exposes its data through two transports. Neither authenticates.

**gRPC** (`crates/prkdb/src/raft/grpc_service.rs`). `PrkDbService` declares 25 RPCs. Fifteen
correctly call `validate_admin_token` (lines 417, 446, 466, 487, 530, 617, 670, 714, 752, 788,
824, 879, 902, 1080, 1181). The other ten do not:

| RPC | Line | Exposure |
|---|---|---|
| `put` | 238 | arbitrary write |
| `get` | 260 | arbitrary read |
| `delete` | 288 | arbitrary delete |
| `batch_put` | 379 | bulk write |
| `watch` | 924 | live change stream of any collection |
| `fetch_segment` | 959 | **streams raw WAL segments** |
| `get_schema` | 1129 | schema disclosure |
| `check_compatibility` | 1222 | schema probing |
| `health` | 303 | intentional — stays public (D4) |
| `metadata` | 322 | **not** intentional — discloses node addresses and partition layout; requires `Read` (D4) |

`fetch_segment` is the most serious: a complete data-exfiltration primitive requiring no
credential.

`get_schema` and `check_compatibility` are the clearest evidence this was oversight rather than
design — their siblings `register_schema` (1080) and `list_schemas` (1181) *are* gated. Nobody
decided that reading a schema should be public while listing schemas is not.

The admin gate itself is real and correctly written: `validate_admin_token`
(`grpc_service.rs:1269-1282`) even denies everything when no token is configured, which is the
right default. It was simply never applied beyond the admin surface.

**Structural cause:** the admin RPCs carry `admin_token` as a *field in the request message*
(`raft.proto`). `PutRequest` (`raft.proto:182-185`), `GetRequest`, and the rest have no such
field, so there was nothing to check. The fix cannot be per-message; it has to be a transport-
level interceptor.

**HTTP** (`crates/prkdb-cli/src/commands/serve.rs:226-260`). The axum router declares **10**
routes and is built with no authentication layer at all:

```
GET    /                                → service root
GET    /health                          → probe
GET    /collections                     → list every collection
GET    /collections/:name               → collection metadata
GET    /collections/:name/data          → full collection read
PUT    /collections/:name/data          → arbitrary write
GET    /collections/:name/data/:id      → single-item read
DELETE /collections/:name/data/:id      → arbitrary delete
GET    /collections/:name/count
GET    /collections/:name/schema
GET    /ws/collections/:name            → when --websockets
GET    /metrics                         → when --prometheus
```

The only auth anywhere in the HTTP surface is an optional WebSocket token
(`serve.rs:954-956`), checked *after* the upgrade — see R12.10, because gating the
upgrade request is a behaviour change for existing WS clients.

**A third surface:** `crates/prkdb/src/bin/prkdb-server.rs:87-114` binds a *separate*
unauthenticated metrics server on port `9090 + node_id`, independent of `prkdb-cli serve`.

This matters more than a normal missing-auth bug because the generated cross-language clients
target these APIs. The README states: *"Generated clients target the HTTP API exposed by
`prkdb-cli serve`."* Every Python, TypeScript, and Go client the codegen produces speaks to an
unauthenticated read/write endpoint.

> **Constraint that shapes the fix:** all five inter-node Raft RPCs — `RequestVote`, `PreVote`,
> `AppendEntries`, `InstallSnapshot`, `ReadIndex` — are registered on the **same tonic server and
> port** as the client API (`prkdb-server.rs:155-158` comments this explicitly). They do at least
> live on a *separate gRPC service* (`RaftService`, `raft.proto:5-20`), so tonic can apply a
> different interceptor to each. Peer traffic must still be authenticated by a peer
> identity — **D7 chose mTLS client certificates**, which R13 delivers. Leaving `RaftService`
> open because "only peers call it" lets any client forge `AppendEntries` and rewrite the log.

### S-02 — TLS exists but no shipped binary can turn it on

`crates/prkdb/src/raft/server.rs:38-66` implements `start_raft_server_tls` with full mTLS
(server identity plus optional client CA). Its only caller in the entire workspace is
`crates/prkdb/examples/raft_node.rs:142`.

Neither `prkdb-server` nor `prkdb-cli serve` references `TlsConfig` or `start_raft_server_tls`.
The axum HTTP server has no TLS path at all. A `certs/` directory containing a generated CA,
server, and client keypair sits in the repo, implying TLS is available. It is not, from any
binary a user would run.

> **On `certs/` — an earlier revision of this spec was wrong about this.** It claimed the repo
> contained *committed* private keys and that they "remain in git history". Verified on
> 2026-08-08: `git log --all -- certs/` returns **0 commits**, and `/certs/` has been ignored at
> `.gitignore:12` all along. The keys are local dev fixtures that were never tracked. No
> remediation and no history rewrite is required — this was a phantom finding, and the claim is
> corrected here rather than quietly deleted.

---

## 3. Requirements

Each has an ID, the finding it closes, and a **falsifiable acceptance test** — what CI runs to
prove it holds. Effort is a rough solo-developer estimate.

### R1 — Consistency claims must be backed by a checker that can fail

| | |
|---|---|
| **Closes** | F-01, F-02, F-03 |
| **Effort** | 3 days |
| **Problem** | `is_linearizable()` (`tests/helpers/jepsen_checker.rs:77-127`) is a value-provenance check: it verifies only that a read's value matches *some* write to that key which started before the read ended. It cannot fail on a stale read. The bank test never touches the database. The register test runs single-node. |

**Requirement:** PrkDB either (a) verifies linearizability with a real algorithm against a real
cluster under real faults, or (b) makes no linearizability claim in test names, docs, CI job
names, or badges.

**Acceptance:**
1. A deliberately-injected stale read makes `is_linearizable()` return `NotLinearizable`.
   **This is the most important test in the spec — it tests the tester.** Without it, every
   other consistency result is unfalsifiable.
2. The register test runs against a ≥3-node cluster from `tests/helpers/test_cluster.rs`, with
   at least one `NetworkSimulator` partition applied mid-run.

   > **This requires new harness work (D5).** The existing `TestCluster` is **process-based**: it
   > spawns real `prkdb-server` children (`test_cluster.rs:139-176`) and panics with *"Binary not
   > found … Run 'cargo build --bin prkdb-server --release' first"* if the binary is absent. That
   > is why `raft_chaos_tests.rs:256` carries `#[ignore] // Requires server binary`.
   >
   > **D5 chose to build an in-process harness** rather than add a binary-build step to CI. It
   > costs ~2 days more, and it is the reason those tests can stop being `#[ignore]`d rather than
   > being re-ignored the next time CI is slow. Requirements:
   >
   > - Spawn `RaftNode` / `PartitionManager` instances directly in the test process, on ephemeral
   >   ports, with no child processes and no dependency on a built binary.
   > - Expose the same fault-injection surface the process harness has: `partition(g1, g2)` and
   >   `heal_partitions()` backed by `NetworkSimulator`.
   > - Every cluster test must run under plain `cargo test` with no prerequisite build step.
   >
   > Keep the process-based `TestCluster` for the tests that genuinely exercise the binary
   > (startup, config parsing, signal handling). Both can coexist; they test different things.
3. The bank invariant is computed by reading balances back out of the storage adapter, not from
   an in-process `HashMap`.
4. **Scope of the naming cleanup:** no *test name*, *doc comment*, *CI job name*, or *badge*
   claims linearizability unless covered by (1)–(3).
   This explicitly **does not** apply to `ReadConsistency::Linearizable`,
   `ReadMode::Linearizable`, or `ReadConsistencyCli::Linearizable`
   (`prkdb-client/src/client.rs:53`, `prkdb-cli/src/commands/data.rs:80`,
   `grpc_service.rs:200`) — those are correct product APIs naming a real read mode. Their
   verification is R14, not a rename.

**Design note — how the checker is built (D6).** In-process Wing & Gong. Port the
P-compositionality linear-search algorithm that
[Porcupine](https://github.com/anishathalye/porcupine) implements: search over linearization
points with memoization on (state, pending-set). ~300 lines.

Three properties this must have, all of which the old checker lacked:

1. **Bounded search.** Cap histories at 200 operations — the search is exponential in the worst
   case, and a checker that hangs is a checker that gets deleted.
2. **Errors are indeterminate, not absent.** A timed-out write may or may not have taken effect.
   The search must try both branches. The old checker skipped errored operations outright, which
   is a large part of why it could not fail.
3. **Its own correctness is tested.** Acceptance (1) — the injected stale read — is what proves
   the checker works. Do not trust a single result from it before that test passes.

Elle/EDN offline checking was considered and rejected: it is a stronger claim, but it puts a JVM
and Clojure in CI and moves failures out of the PR feedback loop, which for a solo maintainer
means they get looked at less.

### R2 — CI must fail when the repository is broken

| | |
|---|---|
| **Closes** | F-05, F-06 |
| **Effort** | 0.5 day |
| **Problem** | `cargo clippy --workspace --all-targets -- -D warnings` exits 101 on rustc 1.95 (22 warnings). No CI job declares `timeout-minutes`, so a hang consumes GitHub's 6-hour default. The toolchain is unpinned, so breakage arrives on someone else's schedule. |

**Acceptance:**
1. `cargo clippy --workspace --all-targets -- -D warnings` exits 0.
2. Every job in `ci.yml` declares `timeout-minutes`.
3. `rust-toolchain.toml` pins a specific stable version; `rust-version` is set in
   `[workspace.package]` and matches the README badge. The current "Rust 1.70+" claim is
   verified by nothing.
4. A scheduled `@nightly` job is allowed to fail — future-stable breakage surfaces as a warning,
   never as a red PR. This is the only legitimate use of `continue-on-error` in the repo.

### R3 — No test may hang

| | |
|---|---|
| **Closes** | F-06, F-12 |
| **Effort** | 1.5 days |
| **Problem** | Two identical `cargo test --workspace` runs, same commit, same machine: the first exited 0; the second hung >10 minutes in `distributed_writes`. Serialized in isolation it passes 5/5 in 6.04s. Cause is CPU starvation of Raft election timers when ~40 test binaries run concurrently, compounded by `sleep(3s)`-then-assert timing and hardcoded ports. |

**Acceptance:**
1. Every cluster-forming test is wrapped in `tokio::time::timeout` with an explicit deadline that
   names the condition it was waiting on.
2. Zero hardcoded ports under `crates/prkdb/tests/`. All bind `127.0.0.1:0`, following the
   pattern already in `admin_rpc_tests.rs`, `client_server_integration.rs`, `security_tests.rs`.
3. `sleep(N)`-then-assert is replaced by poll-until-condition-or-deadline.
4. `cargo test --workspace --no-fail-fast` completes **10 consecutive times** without hanging.

### R4 — Raft tests must assert Raft properties

| | |
|---|---|
| **Closes** | F-07 |
| **Effort** | 1 day |
| **Problem** | `get_leader()` (`raft/node.rs:391-399`) returns `Some` for followers that merely know who the leader is. So `node1_is_leader \|\| node2_is_leader \|\| node3_is_leader` is satisfied by any node having received a heartbeat, and `test_raft_propose` selects node 1 as "the leader" regardless of who leads. |

**Acceptance:**
1. Leadership assertions use `get_state() == RaftState::Leader`, never `get_leader().is_some()`.
2. A test asserts **election safety**: at most one leader per term. This is the property Raft
   guarantees and the one worth regression-testing.
3. Proposal targets are selected by state. If follower-forwarding is worth testing, it gets its
   own named test.

### R5 — Documentation must compile

| | |
|---|---|
| **Closes** | F-08 |
| **Effort** | 1.5 days |
| **Problem** | 67 of 70 doctests in `prkdb` are ignored (61 ```` ```ignore ```` + 6 ```` ```rust,ignore ````); `indexed_storage.rs` holds 59. The README's 38 Rust fences are compiled by nothing. |

**Acceptance:**
1. Zero ```` ```ignore ```` fences on public API items in `crates/prkdb/src`. Examples that
   cannot execute use `no_run`, which still type-checks. Non-Rust content uses ```` ```text ````.
2. `#![doc = include_str!("../../../README.md")]` on the `prkdb` crate root.
3. `cargo test --doc -p prkdb` reports ≥60 passing.

### R6 — Chaos injection must not ship in release builds

| | |
|---|---|
| **Closes** | F-04 |
| **Effort** | 0.5 day |
| **Problem** | `check_chaos()` (`raft/rpc_client.rs:44-89`) is behind no `cfg`. In a release build it reads an env var, then reads and JSON-parses a file, on **every** `get_client()` call — before the connection-cache lookup. Anyone who can set `CHAOS_CONFIG_PATH` or write that file can partition a live cluster. |

**Acceptance:**
1. `ChaosRule`, `check_chaos`, and the call site are behind `#[cfg(feature = "chaos")]`.
2. A release build contains no reference to `CHAOS_CONFIG_PATH`.
3. The chaos workflow builds with `--features chaos` and its tests still pass.
4. When enabled, the check sits *after* the cache lookup so it is never on the hot path.

### R7 — Chaos and consistency tests must be enforced

| | |
|---|---|
| **Closes** | F-09 (badge), CI wiring |
| **Effort** | 0.5 day + whatever the newly-visible failures cost |
| **Problem** | All 7 Raft chaos tests are `#[ignore]`d. The chaos workflow runs only on `pull_request`, and its "Run All Raft Chaos Tests" step is `continue-on-error: true` (`chaos-tests.yml:52`). The README badge (`README.md:6`) claims "Chaos Tests — 19 passing" via a hardcoded shields.io string pointing nowhere. |

**Acceptance:**
1. `continue-on-error` is removed from `chaos-tests.yml`.
2. Chaos tests gate something on pushes to `main`, not only on PRs.
3. Both hardcoded badges (`README.md:5-6`) are replaced with real workflow-status URLs, or
   removed until the workflows they describe actually gate.

### R8 — Durability paths must not panic

| | |
|---|---|
| **Closes** | F-10 |
| **Effort** | 1 day |
| **Problem** | 201 `.unwrap()` calls **outside** `#[cfg(test)]` (572 inside, which is fine). Most of the 201 are metrics registration — startup-only and defensible. But 11 are in `prkdb-core/src/wal/write_ahead_log.rs`, 6 in `wal/log_segment.rs`, 7 in `prkdb/src/storage/wal_adapter.rs`, and 3 in `prkdb-storage-segmented/src/lib.rs`. A panic there is a durability event. |

> **The three in `prkdb-storage-segmented/src/lib.rs:210-221` are the worst of them**, and were
> outside this requirement's scope until an independent review looked at the adapter crates:
>
> ```rust
> let key_len = u32::from_le_bytes(cursor[1..5].try_into().unwrap()) as usize;
> let val_len = u32::from_le_bytes(cursor[5..9].try_into().unwrap()) as usize;
> let crc     = u32::from_le_bytes(cursor[crc_start..crc_start + 4].try_into().unwrap());
> ```
>
> These parse length prefixes and a CRC out of bytes read from disk during recovery. On a
> truncated segment — precisely the condition this code exists to survive — the slice index
> panics before `try_into` is even reached. It is a corrupt-data-to-panic path in a storage
> adapter, and it is the exact scenario the three `#[ignore]`d tests in `corruption_tests.rs`
> were written for. Fix alongside Plan B Task 4 (format versioning), which adds the header these
> reads should be validating against.

**Acceptance:**
1. Zero `.unwrap()` **outside `#[cfg(test)]` modules** in `crates/prkdb-core/src/wal/`,
   `crates/prkdb/src/storage/`, and `crates/prkdb-storage-{sled,sql,segmented}/src/`. (Raw
   directory totals are misleading — 82 and 176 for the first two — because the great majority
   are test-module unwraps and are explicitly out of scope. Production-only totals are ~24 and
   3 respectively.)
1b. Recovery paths that read length or checksum fields from disk return a typed error on a short
   or malformed buffer, and a test feeds them a deliberately truncated segment. `prkdb-core/src/io/`
   was checked and has zero production unwraps — no work needed there.
2. `#![deny(clippy::unwrap_used, clippy::expect_used)]` in `prkdb-core/src/lib.rs`, with
   `#![cfg_attr(test, allow(...))]` for test modules.
3. Metrics-registration unwraps become `.expect("<invariant>")` stating why they cannot fail. A
   panic message that explains itself beats a silent conversion.

### R9 — Coverage must be measured before it is claimed

| | |
|---|---|
| **Closes** | F-11 |
| **Effort** | 0.5 day |
| **Problem** | 389 test functions, no coverage tool anywhere. Given R1, raw test counts are actively misleading — some of those tests cover nothing. |

**Acceptance:** `cargo-llvm-cov` runs in CI with `--fail-under-lines` at the measured baseline,
ratcheting upward, never downward. The first report is the deliverable.

### R10 — Dependencies must be scanned

| | |
|---|---|
| **Closes** | F-09 |
| **Effort** | 0.5 day |
| **Problem** | 481 entries in `Cargo.lock`. No `deny.toml`, no `cargo-deny`, no Dependabot, no audit step. The locally installed `cargo-audit` cannot parse CVSS 4.0 advisories, so nothing anywhere is scanning. |

**Acceptance:** `cargo deny check` and `cargo audit` both run in CI and pass;
`.github/dependabot.yml` covers the cargo and github-actions ecosystems.

> **Known duplicate to resolve first.** The workspace pulls **two major versions of `reqwest`** —
> 0.12 in `crates/prkdb/Cargo.toml:43` and 0.11 in `crates/prkdb-cli/Cargo.toml:48`. `deny.toml`
> sets `multiple-versions = "warn"`, so this surfaces the moment R10 lands. Consolidate on 0.12
> and hoist it into `[workspace.dependencies]` rather than suppressing the warning; two HTTP
> stacks in one workspace is duplicated TLS configuration and duplicated CVE exposure.

### R11 — Dead code and documentation drift must be removed

| | |
|---|---|
| **Closes** | F-13, F-14 |
| **Effort** | 0.5 day |

**Acceptance:**
1. `mod storage_old_inmemory;` (`lib.rs:28`) and the 0-byte orphan `src/security.rs` are deleted.
2. `xtask repo-status` gains collectors for version consistency and roadmap-vs-CI drift. Today it
   catches neither that the workspace is `0.6.0` while `docs/guide/roadmap.md` says "v2.0-clean",
   nor that the roadmap lists Go/Python clients as future work while five CI jobs test them.
   The drift detector already exists; it just does not look at these.

### R12 — Authorization on **both** data planes

| | |
|---|---|
| **Closes** | S-01 |
| **Effort** | 6 days |
| **Decision** | D2 — users and per-collection roles, not a single API token. D3 — no interim token gate; the hole stays open until this lands. |

> **Revision 1 scoped this to HTTP only.** That would have shipped a "security hardening"
> release with `fetch_segment` still streaming raw WAL to anyone. Both transports are in scope.

#### The model

Three concepts, kept as small as an authorization model can be while still being one:

```
Principal  — a named identity holding one credential (a token; rotatable)
Role       — a named set of grants
Grant      — (collection-pattern, permission)
Permission — Read | Write | Admin
```

`Admin` on `*` reproduces today's `PRKDB_ADMIN_TOKEN` behaviour exactly, which is the migration
path: the existing token becomes a bootstrap principal holding an admin role.

Deliberately excluded, because each is a separate project and none is needed to close S-01:
groups, permission inheritance, row-level or field-level rules, external identity providers,
and token expiry. Add them when something demands them.

#### Where the model lives

The principal store must survive restart and be consistent across the cluster. It is small,
rarely written, and read on every request — so it belongs in the Raft state machine, not in a
config file. Two consequences worth stating before implementation:

- A cold cluster has no principals and cannot authenticate anyone, including the operator. A
  bootstrap path is required: `PRKDB_BOOTSTRAP_TOKEN` creates a single admin principal on first
  start and is refused once any principal exists.
- Authorization checks are on the hot path. Cache the resolved principal-to-grants map in memory
  and invalidate on the Raft apply that changes it. Do **not** read through to storage per
  request.

**Acceptance — model:**
1. A principal with `Read` on `users` can `get` from `users` and is denied `put` to `users`.
2. A principal with `Write` on `logs/*` is denied any access to `users`.
3. A principal with `Admin` on `*` can perform every currently-admin-gated RPC.
4. Bootstrap creates exactly one admin principal, and is refused once any principal exists.
5. Revoking a role takes effect without restarting the node.
6. The permission decision is covered by table-driven tests over (principal, collection, action)
   — this is logic where an exhaustive table is cheaper than reasoning about cases.

**Acceptance — HTTP:**

`serve.rs:226-260` declares **10** routes. Every one needs a bucket:

| Route | Required permission |
|---|---|
| `/collections` | `Read` on any collection (filtered to what the caller may see) |
| `/collections/:name` | `Read` on `:name` |
| `/collections/:name/data` GET | `Read` on `:name` |
| `/collections/:name/data` PUT | `Write` on `:name` |
| `/collections/:name/data/:id` GET | `Read` on `:name` |
| `/collections/:name/data/:id` DELETE | `Write` on `:name` |
| `/collections/:name/count` | `Read` on `:name` |
| `/collections/:name/schema` | `Read` on `:name` |
| `/ws/collections/:name` | `Read` on `:name` — **breaking, see below** |
| `/metrics` | `Admin`, or bound to a separate interface |
| `/health` | public (D4) |
| `/` | public — service-info root, discloses nothing |

7. `prkdb-cli serve` refuses to start with no principals configured unless `--allow-anonymous`
   is passed, and logs a prominent warning when it is.
8. `/health` and the new `/livez`, `/readyz` stay open — orchestrators probe them before any
   client could hold a credential.
9. `/metrics` requires `Admin` or a separate interface. This covers **both** metrics servers:
   `prkdb-cli serve --prometheus` and the independent one at `prkdb-server.rs:87-114` on port
   `9090 + node_id`.
10. **The WebSocket route is a behaviour change, not just a new gate.** `/ws/collections/:name`
    has its own optional token today (`serve.rs:954-956`), checked *after* the upgrade. A
    middleware layer gates the upgrade request itself, so existing WS clients passing a token by
    query parameter start getting 401s. Decide whether the layer also accepts that parameter, and
    note the break in the changelog either way. Silently breaking a working client is worse than
    the gap being closed.

**Acceptance — gRPC:**

`raft.proto` declares **two** services, which makes this cleaner than one interceptor would be:

| Service | Lines | RPCs | Who calls it |
|---|---|---|---|
| `RaftService` | 5–20 | `RequestVote`, `PreVote`, `AppendEntries`, `InstallSnapshot`, `ReadIndex` | peers only |
| `PrkDbService` | 23–118 | data plane + 15 admin RPCs + `Health`, `Metadata` | clients |

Both are registered on the same tonic server and port (`prkdb-server.rs:155-158`), but tonic
applies interceptors per service, so each gets its own policy.

11. An interceptor on **`PrkDbService`** resolves the caller's principal from request
    **metadata**, not from a message field, and enforces the permission each RPC requires. The
    data-plane messages have no token field (`raft.proto:182-185`) and adding one to each is the
    wrong shape.
12. All eight currently-unprotected client RPCs are covered: `put` and `batch_put` need `Write`;
    `get`, `watch`, `get_schema`, `check_compatibility` need `Read`; `delete` needs `Write`;
    `fetch_segment` needs **`Admin`** — it streams raw WAL across every collection, so no
    per-collection `Read` grant is sufficient authority for it.
13. The 15 already-gated admin RPCs move from the `admin_token` field to the same principal
    model, requiring `Admin`. The `admin_token` request field is deprecated, and honoured for one
    release with a warning so existing clients do not break atomically.
14. **`RaftService` is authenticated too, by mTLS peer identity (D7).** All five of its RPCs —
    including `PreVote` and `ReadIndex`, which are easy to overlook — require a client
    certificate signed by the cluster CA. `ReadIndex` is the mechanism behind linearizable
    follower reads (R14); forging it breaks the guarantee. Leaving `RaftService` open because
    "only peers call it" lets any client forge `AppendEntries` and rewrite the log.
15. **`Health` is public, `Metadata` requires `Read` (D4).** `Metadata` discloses node addresses
    and partition layout, which is reconnaissance value for no probe benefit.
16. An integration test asserts a 3-node cluster still elects a leader and replicates with both
    policies active. This is the regression that would otherwise be found in production.

**Acceptance — both:**
17. Every credential comparison is constant-time (`subtle::ConstantTimeEq`). All three current
    comparisons use `!=` on `String`/`&str`: `grpc_service.rs:1277`, `serve.rs:956`, and the new
    interceptor. The existing empty-token behaviour — deny all admin ops when unconfigured — is
    correct and must be preserved as the "no principals" case.
18. Generated Python, TypeScript, and Go clients send credentials; the mixed-client integration
    test passes against an authorized server.
19. Integration tests assert 401/`UNAUTHENTICATED` with no credential, 403/`PERMISSION_DENIED`
    with a valid credential lacking the grant, and success with the right one — for every route
    and RPC above. **The middle case is the one that matters**: authentication without
    authorization is the bug this requirement exists to prevent.

### R13 — TLS reachable from shipped binaries

| | |
|---|---|
| **Closes** | S-02 |
| **Effort** | 1.5 days |

**Acceptance:**
1. `prkdb-server` and `prkdb-cli serve` both accept `--tls-cert`, `--tls-key`, and optional
   `--tls-client-ca`, applying them to the Raft transport and the HTTP surface.
2. All three refuse to start if the files are unreadable — fail loudly rather than silently
   serve plaintext.
3. An integration test starts the server with TLS, connects with a client trusting the test CA,
   and asserts a plaintext client is rejected.
4. `certs/` is untracked; `scripts/gen_certs.sh` generates fixtures on demand.

### R14 — The shipped linearizable read mode must be verified

| | |
|---|---|
| **Closes** | gap found in self-review — not in the original audit |
| **Effort** | 1 day (on top of R1) |
| **Problem** | PrkDB ships `ReadConsistency::Linearizable` as a public API (`prkdb-client/src/client.rs:53`, `prkdb-cli/src/commands/data.rs:80`, dispatched at `grpc_service.rs:200`). That is a correctness guarantee offered to users. The only tests that exercise `ReadMode::Linearizable` are `raft_chaos_tests.rs:714` and `:761` — **both inside `#[ignore]`d tests**. The guarantee has zero enforced coverage. |

This is distinct from R1. R1 fixes the *checker*; R14 points it at the *product API that makes
the promise*.

**Acceptance:**
1. A test drives reads through `ReadConsistency::Linearizable` against a ≥3-node cluster under
   an injected partition, records the history, and checks it with the R1 checker.
2. The same test run with `ReadConsistency::Stale` is **expected** to produce stale reads —
   asserting the modes actually differ. A consistency level that behaves identically to the
   weaker one is a bug, and nothing currently would detect it.
3. Both run in a gating CI job, not behind `#[ignore]`.

### R15 — Performance claims must carry methodology

| | |
|---|---|
| **Closes** | gap — audit flagged the claim, revision 1 dropped the requirement |
| **Effort** | 0.5 day |
| **Problem** | `README.md:15` asserts "894K queries/sec" as a feature bullet with no methodology; the same number is repeated in `indexed_storage.rs:7,71`. `docs/guide/roadmap.md` gives a different table (199K writes/sec) and nothing reconciles them. The README's benchmark *section* does carry an honest caveat about Kafka comparisons — that honesty just never reached the bullets. |

**Acceptance:**
1. Every performance number in the README, crate docs, or roadmap either links to the benchmark
   that produced it (hardware, dataset, command) or is removed.
2. The roadmap and README tables are reconciled, or explicitly state that they measure different
   operations.
3. The `xtask` drift collector from R11 fails when a performance claim has no linked source.

### R16 — Every `#[ignore]` must justify itself

| | |
|---|---|
| **Closes** | gap — revision 1 covered only the chaos ignores |
| **Effort** | 0.5 day |
| **Problem** | 14 `#[ignore]` attributes exist: 7 in `raft_chaos_tests.rs`, 3 in `corruption_tests.rs`, 2 in `load_tests.rs`, 2 in `chaos_tests.rs`. Most are bare `#[ignore]` with no reason. A bare ignore is a test that will never run again and nobody will remember why. |

**Acceptance:**
1. Every `#[ignore]` carries `#[ignore = "<reason>"]` naming why and what would re-enable it.
2. Ignores that exist only because a test is *slow* are moved to a nightly job that runs them,
   rather than left permanently unrun.
3. A CI check rejects bare `#[ignore]` with no reason string.

---

## 4. Should PrkDB become a vector database?

**Short answer: add vector search as a feature; do not become a vector database.**

### Current state

Verified: no vector, embedding, HNSW, IVF, or similarity code anywhere in the workspace. Every
grep hit for "vector" is a Rust `Vec`. This would be built from zero.

### Why not a vector database

The category is closed. pgvector, Qdrant, Milvus, Weaviate, LanceDB, turbopuffer, plus
sqlite-vec and DuckDB VSS at the embedded end. LanceDB in particular is Rust, embedded, and
columnar — the exact niche PrkDB would reach for, with years of head start and a team.

More fundamentally: **pgvector settled this.** Vector search is now a feature storage systems
have, not a category you enter. Shipping "PrkDB, a vector database" in 2026 invites a benchmark
comparison against Qdrant that PrkDB loses, on a dimension that is not where its advantage lies.

### D1 weakens this argument — stated plainly

This section was written before D1, and it leaned on the audit's pivot thesis: that PrkDB's real
position is a durable, typed, replayable event log for agent systems, with Raft cut. **D1
rejected cutting Raft**, so that premise no longer holds and the recommendation cannot rest on
it.

What changes, honestly:

| | With Raft cut (rejected) | With Raft kept (D1) |
|---|---|---|
| Competitors | sqlite-vec, LanceDB, DuckDB VSS — embedded, small field | Qdrant, Milvus, Weaviate — distributed, funded, benchmarked |
| The pitch | "embeddable typed event log with similarity search" — no other entrant | "distributed vector store" — a crowded field entered late |
| Verdict | clear niche | **materially weaker** |

So D1 makes vector search a *less* compelling bet, not a more compelling one. The case below
still stands on the query-coverage argument, but it is now a feature that rounds out the engine
rather than a positioning play. **Sequence it accordingly: after Plan A, and without expecting it
to differentiate the product.**

If the agent-event-log niche later looks more attractive than distributed streaming, that is a
reason to revisit D1 — not a reason to ship vector search on top of a distributed database and
hope the positioning follows.

### Why vector search is still the right feature

Independent of positioning: ask what queries an event-shaped workload actually issues, and check
which the engine can serve.

| Query | PrkDB today |
|---|---|
| Last N events for session X | ✅ offset scan + consumer groups |
| Events in time window W | ✅ `windowing.rs` |
| Events where field F = V | ✅ secondary indexes, `#[index]` macro |
| Events expiring after D | ✅ TTL |
| **k most similar past events to this one** | ❌ **missing** |

Similarity search is the *one* query type in that set the engine cannot serve. That makes it
justified — as the fourth index type, beside primary, secondary, and time. It is a gap in query
coverage, which is a real reason to build something. It is not, after D1, a market position.

### Concrete shape

Extend existing machinery rather than building a parallel system:

```rust
#[derive(Collection, Serialize, Deserialize)]
struct AgentTrace {
    #[id]        id: String,
    #[index]     session_id: String,
                 content: String,
    #[vector(dim = 1536, metric = "cosine")]
                 embedding: Vec<f32>,
}

let similar = traces.query_similar(&embedding, 10)
    .filter(|t| t.session_id == sid)     // reuses the secondary index
    .await?;
```

**Do not implement HNSW.** Use [`hnsw_rs`](https://crates.io/crates/hnsw_rs) (0.3.4) or
[`instant-distance`](https://crates.io/crates/instant-distance) (0.6.1) — both verified live on
the registry as of 2026-08-08. Index construction is not where the differentiation is. The
differentiated part is that the vector index is *durable through the same WAL* and *replayable
from the same log* as everything else — precisely what a bolted-on vector store cannot offer.

### Sequencing

**Vector search comes after R1–R8, not before.** Not process purity:

- A new index type is new state that must survive crash recovery. Building it on a WAL guarded
  by 11 `unwrap()` calls makes every future vector bug indistinguishable from a durability bug.
- The suite currently hangs non-deterministically (R3). Adding a subsystem to a suite that
  cannot be trusted to terminate means you will not know whether a vector test is failing or
  merely stuck.
- If the checker cannot fail (R1), no consistency claim about the vector index means anything
  either.

### Positioning

Ship it as **"typed event log with similarity search"**, not "vector database". The first is a
category with no other entrant. The second is one where the comparison is already lost.

---

## 5. Production readiness gap

Mapped against the FalkorDB tier matrix, plus rows their matrix omits. Every mark is verified
against code.

### FalkorDB's rows

| Capability | PrkDB | Evidence |
|---|---|---|
| Multi-tenancy | ❌ | `builder.rs:92` `with_namespace()` sets **one** namespace per DB instance. No per-tenant isolation, quotas, or auth scoping. |
| Access control | ❌ → ✅ after R12 | **Today:** a single shared `PRKDB_ADMIN_TOKEN` covering 15 admin RPCs; both data planes unauthorized (S-01). **After R12 (D2):** users, roles, and per-collection permissions — this row is the one gap in the table that the plans actually close. |
| TLS | ❌ | Implemented but unreachable from any shipped binary (S-02). |
| VPC | ❌ | Deployment-tier concern; no managed offering exists. |
| Cluster deployment | ⚠️ | Raft implemented; correctness unverified (R1, R4, R14). |
| High availability | ⚠️ | Pre-Vote, leader election, ReadIndex all present. The tests that would prove them assert the wrong thing (R4). |
| Multi-zone | ❌ | No zone/region awareness. Cross-region replication is a roadmap item. |
| Scalability | ⚠️ | Consistent hashing, range partitioning, and `dynamic_rebalancing_tests.rs` exist. Unproven at scale; no published methodology (R15). |
| Continuous persistence | ✅ | mmap WAL, CRC32 per record (`log_record.rs:101`), checkpoint recovery, snapshots. **The strongest production story in the repo.** |
| Automated backups | ⚠️ | **More exists than an earlier revision of this spec claimed.** `prkdb backup` and `prkdb restore` are implemented (`prkdb-cli/src/commands/backup.rs`, 113 lines, wired at `main.rs:132-135,252-253`) with gzip/none compression and a `--force` guard. Genuinely missing: checksum verification, format-version validation, remote targets, retention policy, and scheduling guidance. |
| Advanced monitoring | ⚠️ | A Prometheus metrics module (`prometheus_metrics.rs`) plus provisioned Grafana dashboards and datasources under `docker/grafana/`. No alert rules, no SLO definitions, no distributed tracing. |
| Support / SLA / account mgmt | ❌ | Not applicable to a solo project. |

### Rows the matrix omits that matter more

| Gap | State | Why it blocks production |
|---|---|---|
| **On-disk format versioning** | ❌ | `log_record.rs` has CRC32 integrity but **no magic number and no format version field**. No way to detect a version mismatch, refuse a future format, or migrate an old one. The hardest thing here to retrofit — every day of data written without it is data you cannot safely evolve. |
| **Rate limiting** | ❌ wired | `rate_limit.rs` implements a `RateLimiter`. Nothing in `prkdb-cli` or `prkdb-server` constructs one. A complete feature, unreachable. |
| **Distributed tracing** | ❌ | No `opentelemetry` or OTLP dependency. `tracing` is used for logs only. Debugging a multi-node write path means correlating logs by hand. |
| **Readiness vs. liveness** | ❌ | Only `/health` (`serve.rs:230`). Kubernetes needs `/readyz` to separate "process is up" from "this node has replayed its log and can serve reads" — without it, traffic routes to nodes that will fail. |
| **Audit logging** | ❌ | No record of who performed which admin operation. Required for any compliance posture. |
| **Secrets handling** | ⚠️ | Tokens arrive by env var, held as plain `String`, compared with `!=`. No zeroization, no constant-time compare (R12.8). |
| **Backpressure surface** | ❌ | `batch_accumulator.rs` and `write_queue.rs` exist, but no configurable bound on in-flight writes, no load-shed path, no documented overload behaviour. |
| **Release engineering** | ❌ | No CHANGELOG, no release workflow, no tags, no MSRV (R2.3), nothing published to crates.io. Users cannot pin a version because there are none. |
| **Upgrade path** | ❌ | Follows from format versioning. No rolling-upgrade procedure, no compatibility policy. |

### What this means

Against the FalkorDB matrix, PrkDB sits **below their FREE tier** today — that tier requires both
multi-tenancy and access control, and PrkDB has neither in usable form.

**R12 (D2) closes one of the two.** After it lands, access control is genuinely present — users,
roles, per-collection permissions — and PrkDB is one row from that tier, with multi-tenancy the
only remaining blocker. That is a meaningful move, and it is the only row in the whole matrix
these plans actually change.

Less damning than it sounds. FalkorDB is selling a managed cloud service; the tier matrix is a
*hosting* product, and most of those rows (VPC, multi-zone, 24/7 support, account management)
are properties of an operations team, not a database. Not reachable by a solo maintainer and
not a goal.

Reachable, and worth doing, in order:

1. **Data-plane authorization** (S-01) — actively dangerous today. `fetch_segment` alone.
2. **Format versioning** — cheapest now, hardest to retrofit later.
3. **TLS wiring** (S-02) — the code is written; only plumbing is missing.
4. **Harden backup + restore** — the commands exist and work; they need checksum verification, format-version validation, and scheduling guidance.
5. **Readiness endpoint + rate limiting** — both small, both already half-built.
6. **Release engineering** — what converts 76k lines into something anyone can use.

---

## 6. What happens if a requirement fails

Several acceptance criteria can fail in a way that is a finding about PrkDB rather than a bug in
the test. Decide the response now, not while staring at a red CI run — because the expedient fix
in each row below is the one that destroys the value of the requirement.

| Trigger | Meaning | Response |
|---|---|---|
| **R1.2 fails** — the register test finds a real violation under partition | PrkDB is not linearizable under the conditions it claims | **Stop.** Capture the history, file it, do not weaken the test. Either fix the consensus bug or remove the linearizability claim from the product (which makes R14 moot and strengthens the case for cutting Raft — see §0). |
| **R14.2 fails** — `Stale` reads behave identically to `Linearizable` | Either the read modes are not wired through, or the test cannot distinguish them | Determine which before changing anything. If the modes genuinely do not differ, `ReadConsistency` is a false API and must be fixed or removed. |
| **R7 surfaces failures once `continue-on-error` is removed** | Chaos tests were failing all along, silently | Expected. Triage each: fix, or `#[ignore = "<reason>"]` per R16. Do not restore `continue-on-error`. |
| **R3.4 still hangs after fixes** | The flake has a second cause beyond timers and ports | Do not raise the timeout. Capture a stack dump (`SIGQUIT` / `rust-gdb`) from the hung binary and find it. |
| **R12.16 fails** — the cluster stops electing or replicating once interceptors are active | Peer authentication is rejecting real peer traffic | **Do not exempt `RaftService`** to make it pass — that is the shortcut that reintroduces the vulnerability. Debug the certificate chain instead. If mTLS proves unworkable, fall back to the cluster-secret option (D7's rejected alternative) and record the downgrade. |
| **Principals do not survive `install_snapshot`** (Task 0) | The authorization store is not in the snapshot path | **Stop before shipping.** A cluster that loses its own credentials during recovery is worse than one with no authorization — it fails closed on the operator too, and the recovery path is a `--allow-anonymous` restart. Fix the snapshot before wiring any interceptor to the store. |
| **D5's in-process harness cannot reproduce a fault the process harness can** | The in-process nodes share state the real ones do not | Keep the process-based test for that specific fault and mark it clearly. Do not delete coverage to make the new harness look complete. |

---

## 7. Effort summary

Reflects the §0 decisions. D2 (RBAC over a single token) and D5 (in-process harness) are what
moved this from the ~20 days originally estimated.

| Group | Requirements | Effort | Changed by |
|---|---|---|---|
| CI safety net | R2, R3 | 2 days | |
| Consistency evidence | R1, R4, R14 | 5 days | D1 keeps this live |
| **In-process cluster harness** | prerequisite for R1, R4, R14, R16 | **2 days** | **D5** |
| Build hygiene | R5, R6, R7, R8, R16 | 4 days | |
| Tooling & drift | R9, R10, R11, R15 | 2 days | |
| **Authorization (RBAC)** | **R12** | **6 days** | **D2** — was 2 days as a token gate |
| TLS | R13 | 1.5 days | D7 depends on it landing first |
| Production primitives | format versioning, backup/restore, readiness, release | 4 days | |
| **Total** | | **26.5 days** solo | |

**Ordering constraint introduced by D7:** peer authentication uses mTLS client certificates, so
R13 (TLS reachable from shipped binaries) must land *before* the `RaftService` half of R12. It is
listed after R12 by requirement number but comes first in execution.

**Ordering constraint introduced by D5:** the in-process harness is a prerequisite, not a
follow-up. R1.2, R4, R14, and most of R16 cannot be demonstrated without it.

---

## 8. Execution plans

| Plan | Covers | Tasks | File |
|---|---|---|---|
| **A — Correctness hardening** | R1–R11, R14–R16 | 20 | `docs/superpowers/plans/2026-08-08-correctness-hardening.md` |
| **B — Production security** | R12, R13, format versioning, backups, readiness, release | 8 | `docs/superpowers/plans/2026-08-08-production-security.md` |

**Execution order is not task-number order.** Two decisions reordered things:

```
Plan A:  1, 2 (CI safety net)  ->  3, 4 (checker)  ->  4b (in-process harness)  ->  5, 7, 17, 19
Plan B:  0 (authz model)  ->  3 (TLS)  ->  1 (HTTP)  ->  2 (gRPC)  ->  4, 5, 6, 7
```

- Plan A Tasks 1–2 land first; everything depends on trustworthy CI feedback.
- **Plan A Task 4b (in-process cluster harness, D5) is a hard prerequisite** for Tasks 5, 7, 17,
  19, and for Plan B Task 2's cluster tests. It is new work, not a refactor.
- **Plan B Task 3 (TLS) precedes Task 2** because D7 authenticates Raft peers by client
  certificate, which needs the `--tls-client-ca` plumbing Task 3 builds.
- Plan B Task 1 and Plan A Task 8 both edit `rpc_client.rs` / server wiring — sequence them.

Vector search is deliberately unplanned here. It gets its own spec once Plan A is green.

---

## 9. Revision history

**Revision 2 (2026-08-08)** — self-review against the code found the following defects in
revision 1:

| # | Defect | Fix |
|---|---|---|
| 1 | **S-01 materially understated.** Revision 1 stated the gRPC admin surface "is guarded", implying gRPC was safe. In fact `put`, `get`, `delete`, `batch_put`, `watch`, and `fetch_segment` have no auth at all — `fetch_segment` streams raw WAL. R12 was scoped to axum middleware only, so implementing revision 1 would have shipped a "security hardening" release with the larger hole still open. | S-01 rewritten to cover both transports and the third metrics server; R12 rewritten with gRPC interceptor criteria and the same-port Raft constraint. |
| 2 | **R1 acceptance #4 would have broken a shipping API.** It demanded no occurrence of "linearizab" survive anywhere, but `ReadConsistency::Linearizable` is a correct product API. | Rescoped to test/doc/CI/badge naming, with an explicit carve-out. |
| 3 | **The shipped linearizable read mode had no requirement at all.** Its only test coverage is inside two `#[ignore]`d tests. | Added R14. |
| 4 | **R8 acceptance was unachievable as written.** "Zero unwrap in `wal/` and `storage/`" ignored `#[cfg(test)]`; raw totals are 82 and 176, production-only is ~24. | Qualified to "outside `#[cfg(test)]`" with the totals stated. |
| 5 | **"34 Prometheus metrics" was a grep artifact**, not a count of distinct metrics. | Replaced with a qualitative description. |
| 6 | No requirement covered benchmark methodology, despite the audit flagging it. | Added R15. |
| 7 | No rule that `#[ignore]` carry a reason; 14 exist, mostly bare. | Added R16. |
| 8 | No abort criteria for acceptance tests that fail because PrkDB is genuinely wrong. | Added §6. |
| 9 | No effort estimates; the audit had them and revision 1 dropped them. | Added §7. |
| 10 | The keep-or-cut-Raft decision gated ~60% of the work but sat in an appendix. | Promoted to §0. |

Verified as correct in revision 1 and unchanged: helper `#[cfg(test)]` modules **do** execute in
integration test binaries (confirmed by `cargo test --test jepsen_consistency_tests -- --list`,
which lists `helpers::jepsen_checker::tests::*`), so the R1.1 meta-test is placed correctly. All
spot-checked line references were accurate.

**Revision 3 (2026-08-08)** — independent review against `raft.proto` and the test harness:

| # | Finding | Fix |
|---|---|---|
| 11 | **R12.6 listed only three peer RPCs.** `RaftService` has five: `RequestVote`, `PreVote`, `AppendEntries`, `InstallSnapshot`, `ReadIndex` (`raft.proto:5-20`). `ReadIndex` is the mechanism behind linearizable follower reads — omitting it from peer auth would undermine R14. | R12.6 lists all five and calls out `ReadIndex` specifically. |
| 12 | **The interceptor problem was overstated.** Revision 2 implied peer and client RPCs share one service and the interceptor must disambiguate. They are two services — `RaftService` (5-20) and `PrkDbService` (23-118) — and tonic applies interceptors per service. | R12 rewritten around per-service policy. The warning not to leave `RaftService` open stands; the mechanism is simpler than described. |
| 13 | **`Health` and `Metadata` were unaccounted for.** Both sit on `PrkDbService`, so an interceptor blocks them — potentially breaking orchestrator probes and client bootstrap. | Added R12.7 and open question 6. |
| 14 | **`TestCluster` is process-based and needs a pre-built binary.** `test_cluster.rs:139-176` spawns real `prkdb-server` children and panics if the binary is missing. R1.2 and R14 both assume the harness is usable from plain `cargo test`. It is not. This is the same reason `raft_chaos_tests.rs:256` is ignored. | Added the feasibility constraint under R1.2 and open question 5. |
| 15 | **Two further ungated RPCs, found by making the arithmetic reconcile.** Revision 2 listed six unprotected client RPCs. `PrkDbService` has 25 RPCs and 15 admin-gated ones, so 6 + 2 policy cases left two unaccounted. Auditing every handler found `get_schema` (1129) and `check_compatibility` (1222) also unprotected — while their siblings `register_schema` (1080) and `list_schemas` (1181) are gated. Oversight, not design. | S-01 table and R12.5 now list all eight, plus `health`/`metadata` as explicit decisions. |

> The method that found #15 is worth repeating: count the declared RPCs, count the protected
> ones, and require the arithmetic to balance. Enumerating from the interface rather than from
> the list of known problems is what surfaces the unknown ones.

**Revision 4 (2026-08-08)** — applied that same method to the surfaces it had not yet been
applied to. Three reconciliations came back clean; two did not.

| # | Finding | Fix |
|---|---|---|
| 16 | **R12 enumerated 4 HTTP routes; `serve.rs` declares 10.** `/`, `/collections`, `/collections/:name`, `/collections/:name/count`, `/collections/:name/schema`, and `/ws/collections/:name` had no stated policy. | R12 HTTP acceptance now buckets all ten. |
| 17 | **The WebSocket route is a breaking change, not just a new gate.** `/ws/collections/:name` checks its own optional token *after* the upgrade (`serve.rs:954-956`). An axum layer gates the upgrade request itself, so existing WS clients passing a token by query parameter start getting 401s. | Added R12.5 requiring an explicit decision and a changelog note. |
| 18 | **`prkdb-storage-segmented/src/lib.rs:210-221` has three production unwraps in a recovery path**, outside R8's stated scope. They slice-index and `try_into().unwrap()` on bytes read from disk — a truncated segment panics rather than erroring. Same scenario as the three `#[ignore]`d `corruption_tests.rs` tests. | R8 scope extended to the adapter crates, with the corrupt-data path called out and tied to Plan B Task 4. |

Reconciled and **clean** — recorded so the next pass does not redo them:

- All 14 audit findings F-01 … F-14 are referenced by at least one requirement.
- `#[ignore]` count is 14 repo-wide, all under `crates/` — the R16 inventory is complete.
- The three extra binaries (`compaction_load_test`, `test_metrics`, `verify_cluster`) bind no
  listeners and expose no network surface. Not additional attack surface.
- `prkdb-core/src/io/` (mmap, io_uring) has zero production unwraps.

Also corrected in the plans (not spec-level): Plan A Task 1 assumed 12 CI jobs — there are 13,
and the `chaos-tests` job uses a reusable workflow, where GitHub Actions does not accept
`timeout-minutes` at the caller (its four internal timeouts already cover it). Plan A Task 17 and
Plan B Task 2 both used helper APIs that do not exist; those are now marked as work to build,
with the real `TestCluster` surface documented.

---

**Revision 6 (2026-08-08)** — fourth pass, widened beyond interface reconciliation to internal
consistency and to claims made while writing revision 5.

| # | Finding | Fix |
|---|---|---|
| 19 | **§4 argued for vector search from a premise §0 rejected.** It opened with "the audit's pivot thesis: PrkDB's real position is an event log for agent systems" — which assumed Raft was cut. D1 kept Raft, so the reasoning chain had a broken link, and the honest conclusion is that D1 makes vector search a *weaker* bet: it moves the comparison from sqlite-vec/LanceDB to Qdrant/Milvus. | §4 states the weakening explicitly, with a before/after table, and rests the remaining case on query coverage rather than positioning. |
| 20 | **Plan B Task 0 used `#[rstest]`, which is not a workspace dependency.** An agent executing it hits a compile error on the first step. `proptest 1.4` is present but is the wrong tool for a fixed enumerable input space. | Added Step 0 to install `rstest 0.23` as a dev-dependency. |
| 21 | **The Plan A ↔ Plan B dependency was one-directional.** Plan B Task 2 declares it needs Plan A Task 4b; Plan A Task 4b said nothing about Plan B. Skipping 4b would silently block the gRPC authorization work in the other plan. | Task 4b's note now names Plan B Task 2. |
| 22 | **Two major versions of `reqwest` in one workspace** — 0.12 in `prkdb`, 0.11 in `prkdb-cli`. `deny.toml`'s `multiple-versions = "warn"` surfaces this the moment R10 lands, and two HTTP stacks means duplicated TLS config and duplicated CVE exposure. | Noted under R10 with instruction to consolidate rather than suppress. |
| 23 | **§6 covered four failure modes, none from the new decisions.** RBAC and mTLS each have an expedient wrong fix — exempting `RaftService` to make replication pass; shipping an authorization store that does not survive `install_snapshot`. | Added three rows, each naming the shortcut not to take. |
| 24 | **§5's "below FREE tier" verdict ignored that R12 changes it.** Access control is the one row in the whole matrix these plans actually close. | Row and verdict now show today vs. after-R12. |

| 25 | **Requirement-to-task traceability did not exist.** Ten of the sixteen requirements — R2, R4, R5, R6, R7, R8, R9, R10, R11, R13 — were not named anywhere in either plan. The *work* was covered; the mapping was only in §8's plan-level table, so no one could verify coverage per task or tell which requirement a task satisfies. | Every task heading now carries its requirement ID, e.g. `## Task 13: Remove unwraps from durability paths (R8)`. Coverage is now mechanically checkable. |

| 26 | **External crate claims were asserted, not checked.** Verified against the registry and docs.rs on 2026-08-08: `rstest` was cited as `0.23` but is at **0.26.1** (three minor releases stale). `subtle 2.6`, `hnsw_rs 0.3.4`, `instant-distance 0.6.1` were correct. `axum-server 0.8` is compatible with axum 0.7 — axum is only a *dev*-dependency there; it couples to `hyper ^1.4` / `tower-service ^0.3` — so the version must **not** be "corrected" down to 0.7. `tonic::Request::peer_certs()` exists but requires the **`server` + `tls`** features and returns raw `CertificateDer`; the rustls layer has already validated the chain, so the interceptor extracts identity rather than verifying trust. | Versions corrected and each claim annotated with what was checked and when. |

> **Three claims in earlier drafts of this section were wrong, and the corrections belong here
> rather than in a silent edit.** The draft asserted that the effort table summed to its stated
> total and that all 16 requirements were referenced by a plan task. Neither had been checked:
> the rows sum to **26.5**, not 26, and ten requirements were unreferenced (finding 25).
>
> This is the same failure the whole review sequence has been correcting — asserting a
> reconciliation instead of running it. The lesson generalizes: *a claim that something balances
> is worth nothing unless the arithmetic is in the transcript.*

Reconciled and **actually verified** in this pass: effort rows sum to 26.5 days against a stated
"~26"; all 16 requirements are now named by at least one task; step numbering is contiguous
across all 28 tasks; no decision-pending language survives anywhere.

---

## 10. Decision log

All questions that blocked this spec were resolved on 2026-08-08 and are recorded in §0 as
D1–D7. Nothing here is open.

Decisions that should be revisited if circumstances change:

| Decision | Revisit when |
|---|---|
| **D3** — no interim mitigation for S-01 | Anyone runs `prkdb-cli serve` on a routable interface, or the project is published/deployed. The exposure is theoretical only while PrkDB has no users. |
| **D6** — in-process WGL checker | The single-register model stops being enough — e.g. transactions across keys need checking, where Elle's cycle detection finds anomalies WGL cannot. |
| **D7** — mTLS peer identity | Operating cert rotation proves heavier than expected for a solo maintainer. The shared-secret fallback remains available. |
| **D1** — Raft stays | The correctness work reveals a consensus bug that is not economically fixable solo. §6 covers this case: it is a legitimate outcome, not a failure. |

New questions raised by the decisions themselves, to answer during implementation rather than
before it:

1. **Where does the principal store live in the Raft log?** R12 argues for the state machine over
   a config file. Confirm the snapshot/restore path carries principals before writing the code —
   an authorization store that does not survive `install_snapshot` is a cluster that loses its
   own credentials during recovery.
2. **Does the deprecated `admin_token` field coexist cleanly with metadata credentials?** R12.13
   allows one release of overlap. Verify a client sending both, and a client sending neither,
   behave sensibly.
