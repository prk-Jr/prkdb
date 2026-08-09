# Correctness & Production Readiness — Design Spec

**Date:** 2026-08-08
**Revision:** 7 (four review passes, decisions applied, execution findings; see §0 and §9)
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

> **Status: fixed 2026-08-09.** HTTP was closed by `prkdb-cli`'s `authz_layer`; the gRPC
> client API by `AuthzGrpcLayer`, registered in both binaries and proven end to end by
> `crates/prkdb/tests/grpc_authz.rs`; and `RaftService` by `PeerAuthInterceptor`, likewise
> registered in both binaries and proven by `crates/prkdb/tests/peer_authz.rs`. All three
> surfaces are now closed.
>
> The gRPC half spent a while in the most dangerous state available to a security
> control: **implemented, unit-tested, and not installed on anything.** The policy object
> was correct and its tests passed; `fetch_segment` went on streaming raw WAL to
> uncredentialed callers the entire time. Nothing that tested the policy could detect
> this. `scripts/plan_status.sh` therefore checks the registration in `serve.rs`
> separately from the module's existence, and the new tests drive a real tonic server
> over a real socket rather than calling the policy directly.

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

### S-03 — No snapshot reads outside a transaction

**Found 2026-08-09. Diagnosed and resolved the same day. Severity: medium — a read
property, not a durability one.**

> **Status: fixed 2026-08-09.** A batch is now published into the index and cache under
> `publish_barrier`, so it becomes visible as a unit, and `snapshot_get_many` holds that
> barrier for a whole multi-key read. `batch_atomicity.rs` asserts zero torn reads through
> that path; removing the barrier reproduces roughly 7,000 torn reads over the same
> workload, so the assertion is load-bearing rather than decorative.
>
> **What was deliberately not fixed:** two separate `get()` calls are still not a
> snapshot. No barrier can span two independent calls without MVCC, and pretending
> otherwise would be worse than stating it. The test asserts that this is still true, so
> the limitation cannot quietly become an assumption.

> **This entry originally claimed "Serializable transactions lose writes under
> contention" and called it the most serious finding of the hardening work. That was
> wrong, and the correction matters more than the original claim.** No writes are lost.
> Committed state is correct at all times. What happens is that a reader *outside* a
> transaction can observe a multi-key commit half-applied.

#### What was actually seen

`test_bank_transfer_invariant` failed once during a full workspace run:

```
Invariant violated during worker 4 iteration 20
  left: Failed { reason: "total balance mismatch: expected 10000, storage holds 9983" }
```

It looked exactly like seventeen units of money vanishing.

#### What it really was

`WalStorageAdapter::put_batch_impl` appends every record to the WAL as a batch, then
inserts them into the in-memory index **one key at a time**, then updates the cache.
`StorageAdapter::get` takes no transaction barrier. So between the first and second index
insert, a concurrent reader sees one leg of a two-key transfer and not the other.

The invariant check was reading each balance with a plain `storage.get()`, so it caught a
commit in flight and compared halves of two different states.

`crates/prkdb/tests/batch_atomicity.rs` demonstrates the mechanism directly and pins both
facts down:

```
final_sum == 200          durable state is always correct — nothing is lost
torn_reads == 194         out of ~400 transfers, a non-transactional reader saw
                          inconsistent state roughly half the time
```

#### The fix

`BankAccounts::check_total_invariant` now reads inside a `Serializable` transaction and
only trusts the sum when that transaction commits cleanly. Conflict detection runs *before*
the empty-write-set early return in `Transaction::commit`, so a read-only Serializable
transaction that commits successfully has certified that nothing it read changed while it
was reading. That is the snapshot.

Retries on conflict, up to 50 attempts, then fails loudly rather than quietly reporting
`Passed` — an invariant checker that gives up and says "fine" is worse than one that
errors.

#### The real limitation this exposed

**PrkDB has no snapshot read for callers outside a transaction.** Any client reading
several keys with `get()` and expecting them to agree can observe a torn state. That is
worth knowing and worth documenting for users; it is not a bug in the sense the original
entry implied.

Two ways to close it properly, neither required to fix the test:

1. Apply the whole batch to the index atomically — swap in a prepared map rather than
   inserting key by key.
2. Give `get` a read guard on the transaction barrier, so reads and commits interleave
   cleanly. Costs contention on every read.

Option 1 is narrower and does not slow the read path.

#### Why this is recorded rather than deleted

The mistaken diagnosis was reasonable from the symptom and wrong on the evidence. A
missing-money report is the right thing to escalate on sight — and the right thing to
verify before believing. Writing "17 units vanished" into a spec and then proving it did
not is the more useful artifact of the two.

The bank test earned its keep either way. Before Task 6 it operated on an in-process
`HashMap` and could only ever have caught a bug in `std::sync::Mutex`. Two days after being
pointed at real storage it surfaced a genuine, documented limitation.

### S-04 — `prkdb backup` fails on any database opened with `--database`

**Found 2026-08-09 by the first backup round-trip test ever run. Severity: high — the
backup command does not work in its normal configuration.**

> **Status: fixed 2026-08-09, and it was concealing something much worse.**
> `CollectionPartitionedAdapter` now implements `take_snapshot`, merging its
> per-collection WALs into a single archive keyed `collection:id` — option 1 below, as
> recommended. Restore needed no change: it re-`put`s each entry and the storage layer
> routes by key prefix.
>
> Fixing the missing `take_snapshot` made backup *succeed* while producing an archive of
> **zero entries**. Chasing that produced [S-05](#s-05--the-database-lost-every-write-when-it-was-reopened),
> which is the real headline: reopening a data directory destroyed it. S-04 was a symptom.
> The lesson is the one this whole document keeps repeating — the round-trip test found in
> one run what code review had missed for the life of the repository, because it was the
> first thing to ever *use* the output.

```
$ prkdb-cli --database ./data backup --output snapshot.bin
Error: Snapshot failed: Storage error: Failed to access underlying store:
       take_snapshot not supported
```

#### Cause

`PrkDb::builder().with_data_dir()` produces a `CollectionPartitionedAdapter`
(`builder.rs:258`), which holds **one `WalStorageAdapter` per collection** in a
`DashMap<String, Arc<WalStorageAdapter>>`. It does not implement `take_snapshot`, so the
call falls through to the `StorageAdapter` trait default at `prkdb-types/src/storage.rs:177`,
which returns `"take_snapshot not supported"`.

`WalStorageAdapter` *does* implement it (`wal_adapter.rs:1967`). Only the wrapper is
missing, and the wrapper is what the default builder path constructs.

#### Why it went unnoticed

The commands existed and were wired into the CLI, so nothing looked absent. The production
gap analysis recorded backup as "⚠️ more exists than an earlier revision claimed —
`prkdb backup` and `prkdb restore` are implemented", which was true of the code and false
of the behaviour. **No test had ever run a backup.** That is the entire argument for the
round-trip test: a backup nobody has restored is not a backup.

#### Not a one-line fix

Forwarding is not enough. The adapter partitions data across N independent WALs, so a
single snapshot has to merge N sources into one archive and the restore has to redistribute
them. Decisions needed:

1. **One archive containing per-collection sections**, with the collection name in the
   entry key. Restore replays into whichever collection each key names — which the existing
   `handle_restore` already does, since it re-`put`s through the public API and the storage
   layer routes by the `collection:id` key prefix. This is the smaller change.
2. **One archive per collection.** Simpler to write, worse to operate: a restore becomes N
   invocations, and a partial restore is silently possible.

Option 1 is preferable, and the fact that restore already routes by key prefix means most
of the work is on the write side.

#### Regression test already exists

`crates/prkdb-cli/tests/backup_restore.rs` has the round-trip and the
refuses-without-`--force` cases, both `#[ignore]`d against this finding with the reason
stated inline. They pass the moment the fix lands. `restore_rejects_a_corrupt_archive`
passes today and is not ignored.

#### What must not happen

Do not close this by making `backup` require a single-collection database, or by having the
wrapper snapshot only its first adapter. A backup that silently captures part of the
database is worse than one that refuses.

---

### S-05 — The database lost every write when it was reopened

**Found 2026-08-09 while fixing S-04. Fixed the same day. Severity: critical — PrkDB did
not persist data across a restart, which is the one property a database cannot be without.**

```
$ # write, close, reopen, read
DIAG same-handle read: Some("one")
DIAG reopen read:      None
```

Verified present at `c839ef2`, the commit this hardening work branched from, so it is
long-standing and not a regression introduced by it.

#### Three independent causes, each sufficient alone

1. **Opening the database truncated its log.** `MmapLogSegment::create` opens with
   `.truncate(true)`. `WalStorageAdapter::new_with_config` called
   `MmapParallelWal::create` unconditionally, and that is the constructor
   `PrkDb::builder().with_data_dir()` reaches. A correct non-destructive `open` existed
   alongside it and was reachable only from `WalStorageAdapter::open`/`open_async`, which
   nothing on the user-facing path calls.
2. **The index was never rebuilt.** `open` and `open_async` both call
   `rebuild_index_async`; `new_with_config` did not. So even after (1) was fixed and the
   log survived, every key in it remained invisible — `get_all_keys()` returned 0 against
   a segment that had just recovered 96 bytes of valid records.
3. **Collections on disk were never discovered.** `CollectionPartitionedAdapter` creates
   collections lazily on first key access and never enumerated `collections/`, so a
   freshly opened database reported an empty collection set. This is what made `backup`
   produce a valid, well-formed, empty archive.

They masked each other. Fixing the truncation changed no observable behaviour until the
index rebuild landed, and neither was visible through `backup` until collection discovery
was added. A partial fix would have looked like no fix at all — which is the most likely
reason this survived so long.

#### Why nothing caught it

Every test in the suite built a database in a fresh `tempdir`, used it through a single
handle, and dropped it. **No test had ever reopened a data directory and read from it.**
The property was not so much untested as unconsidered: with a fresh directory every time,
case (1) is unreachable, (2) has nothing to rebuild, and (3) has nothing to discover. The
suite was large and green and could not have failed.

#### Fix

`open_or_create` at both the segment and parallel-WAL levels, used by every path that
means "open a database" — including `sharded_wal_adapter` and `streaming_adapter`, which
had the same defect. `new_with_config` and `new_with_replication` now rebuild the index as
the other constructors always did, and recovered records restore `max_offset` so a
snapshot taken straight after a reopen does not claim offset 0.
`CollectionPartitionedAdapter::load_all_collections` reads the directory.

`create` keeps its truncating semantics: benchmarks legitimately want a fresh segment.
The bug was never that `create` truncates — it is that opening a database called it.

#### Regression tests

`crates/prkdb/tests/durability.rs` covers values surviving a reopen across several
collections, repeated reopens not discarding data, deletes staying deleted, writes after a
reopen appending rather than overwriting, and snapshotting a reopened database without
touching a key first. `scripts/plan_status.sh` additionally asserts that no
database-open path calls `MmapParallelWal::create`.

#### What must not happen

Do not "fix" a future recurrence by having the open path recreate missing segments and
call it recovery. The distinction that matters is between a segment that is absent and one
that is present but unread; conflating them is how a truncating `create` came to be the
default open in the first place.

---

### S-06 — `ReadIndex` served linearizable reads without confirming leadership

**Found 2026-08-09 by the R1 register test on its first partitioned run. Fixed the same
day. Severity: high — the linearizable read mode was not linearizable.**

```
key "users:register": stale read returned "v1" after write(s) ["v0"] had already
completed in real time — no ordering of linearization points explains it
```

Reproduced on roughly **two runs in five**.

#### Cause

`RaftNode::read_index` returned the local commit index on the strength of local state
alone. The code said so:

```rust
// Note: For full linearizability, we should confirm leadership
// with a heartbeat round to majority. For now, we trust our
// leadership status which is good enough for most cases.
```

It is not good enough for the case that matters. A leader partitioned away from its
cluster **does not know it has been deposed** — it holds `RaftState::Leader` until it hears
a higher term, and until then it answered ReadIndex from a log the rest of the cluster had
already moved past. Every read built on that index was stale while being advertised as
linearizable, through `ReadConsistency::Linearizable` on three public surfaces.

Raft §6.4 requires the leader to exchange a round of heartbeats with a majority before
serving a read. That step was absent.

#### Why nothing caught it

`read_consistency_modes.rs` existed and passed. It read through the leader in scenarios
where the leader was on the majority side, which is the case that works. The bug needs a
leader that is *isolated but still believes it leads*, and nothing constructed that until
the register test drove a real workload across a partition.

This is the second time in this work that a test which looked like coverage was not: the
first was the linearizability checker that could not fail. The pattern is the same —
exercising the happy path of a safety property proves nothing about the property.

#### Fix

`read_index` now captures the commit index, then requires a majority to acknowledge a
heartbeat in the current term before returning it. A leader that cannot reach a majority
fails the read instead of answering it wrongly. A peer reporting a higher term ends it
immediately. Single-node clusters are their own majority and return without a round trip.

The cost is one round trip per linearizable read. That is the price of the guarantee;
`ReadConsistency::Stale` remains for callers who would rather not pay it, and it promises
nothing.

#### Regression tests

- `read_consistency_modes::an_isolated_leader_refuses_a_linearizable_read` — pins the
  mechanism, so a regression names itself instead of appearing as a flaky checker.
- `jepsen_consistency_tests::a_replicated_register_is_linearizable_across_a_partition` —
  the workload that found it, now green across repeated runs.

#### What must not happen

Do not "fix" a future recurrence by loosening the checker or by treating an isolated
leader's read as acceptable because it is *usually* current. The whole value of a
linearizable mode is that it is never *usually*.

---

### S-07 — `scan_prefix` was unsupported on the default storage adapter

**Found 2026-08-09 while wiring principal persistence. Fixed the same day. Severity:
medium — every feature built on a prefix scan silently failed.**

```
DIAG scan_prefix: Err(BackendError("scan_prefix not supported"))
DIAG scan meta:col: Err(BackendError("scan_prefix not supported"))
```

#### Cause

Identical in shape to [S-04](#s-04--prkdb-backup-fails-on-any-database-opened-with---database):
`CollectionPartitionedAdapter` — what `PrkDb::builder().with_data_dir()` constructs — did
not implement `scan_prefix`, so calls reached the trait default that refuses.
`WalStorageAdapter` implements it; only the wrapper did not.

The visible consequences were `PrkDb::list_collections`, which scans `meta:col:`, and
loading persisted principals. Both returned an error rather than data, on the adapter the
default builder path produces.

#### Why it kept happening

This is the third method missing from the same wrapper, after `take_snapshot` (S-04) and
the collection discovery that made backup archive nothing. The pattern is a trait with
`Result`-returning defaults that say "not supported": a wrapper that forgets a method
compiles cleanly, and the failure appears only at runtime in whichever feature happened to
call it.

A default that returns an error is a reasonable design for genuinely optional capability.
It is a poor one for a method the wrapper's own siblings implement, because nothing marks
the wrapper as incomplete.

#### Fix

`CollectionPartitionedAdapter::scan_prefix` routes by the `collection:id` key layout: a
prefix containing the delimiter names one collection and only that one is scanned; a
prefix without it is matched against every collection name. Results carry full
`collection:id` keys and are sorted, because per-collection iteration order is not stable
and callers that page or diff need it to be.

#### Worth doing next

Audit `StorageAdapter` for every method with an error-returning default, and check each
wrapper implements the ones its inner adapters do. Three have been found by accident; the
fourth should be found on purpose.

---

### S-08 / S-09 — the audit of `StorageAdapter`'s error-returning defaults

**Found 2026-08-09 by deliberately auditing the trait, after S-07 made it three
accidental discoveries in a row. S-08 fixed; S-09 partially fixed and documented.**

#### The pattern

`StorageAdapter` has 18 methods. Three are required; ten have a default body that returns
`"not supported"`; five have a benign default that loops over the single-key operation.

A wrapper that omits a method with an **error-returning** default compiles cleanly and
fails at runtime, in whichever feature happens to call it. The compiler cannot help,
because inheriting a default is exactly what the language is for.

`CollectionPartitionedAdapter` — what `PrkDb::builder().with_data_dir()` produces — had
now shed four such methods:

| | Method | Symptom | How it was found |
|---|---|---|---|
| S-04 | `take_snapshot` | `prkdb backup` refused | first backup test ever run |
| S-05 | collection discovery | backup archived **zero entries** | chasing S-04's fix |
| S-07 | `scan_prefix` | `list_collections` and principal loading failed | wiring persistence |
| S-08 | `scan_range` | `CollectionHandle::scan_range_by_id_bytes` failed | **this audit** |

Three of four were found by someone using the feature. That is not a detection strategy.

#### S-08 — `scan_range`

`CollectionHandle::scan_range_by_id_bytes` is public API and calls `storage.scan_range`.
On any `--database` database it returned `"scan_range not supported"`.

Implemented on the wrapper. Bounds are full `collection:id` keys; a range naming one
collection on both sides is narrowed to it, otherwise every collection is scanned and
results are filtered against the original bounds. Reasoning about which collections an
arbitrary range spans is not worth the subtlety when the filter is exact.

#### S-09 — `get_changes_since`, and a swallowed error

The wrapper does not implement it, and this one is **not** a forwarding fix: it returns an
ordered change stream, and offsets from N independent per-collection WALs are not
comparable. Defining a merged order is a design decision.

What was fixable is the failure mode. `fetch_segment` called it like this:

```rust
Err(e) => {
    tracing::error!("FetchSegment scan error: {}", e);
}
```

It logged, ended the stream, and returned **success**. A caller replicating from that
segment received an empty stream and concluded there was nothing to replicate — on every
`--database` database, guaranteed, because the call always failed. An empty log and an
unreadable one looked identical.

The error is now surfaced as `Status::internal`. The limitation remains, and
`durability.rs` pins it so that inverting the assertion is the first step of fixing it.

#### The check

`scripts/check_wrapper_completeness.sh` compares the wrapper's trait impl against its
inner adapter's, restricted to methods whose default returns an error — flagging the
benign ones too would report three performance fallbacks beside every real defect, and a
check that cries wolf gets muted. Exemptions require a stated reason and a test pinning
current behaviour; `get_changes_since` is the only one.

Verified load-bearing by removing `scan_prefix`, `scan_range` and `take_snapshot` in turn
and confirming each is flagged. Runs in CI and in `plan_status.sh`.

#### Also found

`ShardedWalAdapter` is re-exported from `storage::mod` and implements **none** of the ten
optional methods, so anything built on it has no prefix queries, no range scans, no
backup, no outbox, and no replication stream. It is constructed nowhere outside its own
tests and the builder cannot produce one. Documented as such rather than removed, since
removing a `pub use` is a breaking change for a type someone may have named.

---

### S-10 — mTLS peer authentication could be selected but never worked

**Found 2026-08-09 while trying to demonstrate mTLS at cluster scale. Fixed the same day.
Severity: high — the recommended secure configuration silently prevented a cluster from
forming. Introduced by this work.**

#### Cause

`RpcClientPool::get_client` built the peer endpoint as:

```rust
let endpoint = format!("http://{}", addr);
```

unconditionally. There was no TLS on the peer *client* at all — the module contained no
reference to TLS of any kind.

So a cluster configured with `--tls-client-ca` had servers demanding a client certificate
and peers dialling plaintext at them. The handshake failed, no `AppendEntries` ever landed,
and no leader could be elected.

`PeerIdentity::from_config` preferred `MutualTls` whenever a CA was configured, so this was
the configuration an operator following the security guidance would reach. **The node
started cleanly and reported nothing wrong**; only replication was broken. That is the
worst shape a fault can take.

#### This is the S-02 pattern, repeated

S-02 was "TLS is implemented and no shipped binary can turn it on". This is "peer mTLS is
implemented on the server and the client cannot speak it". Both are a capability that
exists on one side of a connection only, and both survived because the tests exercised the
side that worked.

Every test in `peer_mtls.rs` before this drove a *client* against a TLS server. None used
`RpcClientPool`, which is the thing a real cluster uses.

#### Fix

`PeerTls` on the pool: certificate, key, CA and expected domain. `get_client` dials
`https://` with a `ClientTlsConfig` when it is set, `http://` when it is not — matching how
the peer listens, which is the property that was missing.

`serve` now refuses `--tls-client-ca` without `--tls-cert`/`--tls-key`, because mTLS needs
this node to present a certificate too, and `PeerIdentity::from_config` only selects
`MutualTls` when that material is actually available. A policy that cannot be honoured is
not offered.

#### Regression test

`peer_mtls::peers_dial_each_other_over_mtls` drives the pool itself. It asserts a plaintext
pool **fails** against the TLS listener — otherwise the server is not really requiring TLS
and the rest proves nothing — and that a TLS pool succeeds.

The first version of that test asserted only that the error text lacked certain words, and
**passed with the fix reverted**. It was tightened to require success and re-checked in
both directions. A regression test that does not fail without the fix is not a regression
test, which is the lesson S-07 taught and this nearly repeated.

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
2. ~~`#![doc = include_str!("../../../README.md")]` on the `prkdb` crate root.~~
   **Not done — see below.**
3. `cargo test --doc -p prkdb` reports ≥60 passing.

> **Status: 1 and 3 done 2026-08-09.** `cargo test --doc -p prkdb` reports **70 passed, 0
> failed, 0 ignored**, from 7 passed / 63 ignored. Converting the fences found eight real
> API drifts that no test could have caught, among them `Transaction::insert` documented as
> `async` when it is synchronous, `create_compound_index` shown with one generic argument
> where the method takes two, and `PartitionedStreamingAdapter::new` shown with a partition
> count it has not accepted for some time. Those examples had been wrong for as long as
> they had been `ignore`d, which is the argument for the requirement.
>
> **Acceptance 2 was attempted and deliberately reverted.** Including the README compiles
> its 38 Rust fences: 1 was a box-drawing diagram mistagged as `rust` (fixed — it now says
> `text`), and 37 are real code, of which roughly thirty are two- and three-line fragments
> like `let count = db.count::<User>().await?;`. Making those compile requires a hidden
> `# ` setup preamble on each. Hidden lines are invisible in rustdoc but **render literally
> on GitHub**, where the README is mostly read — so satisfying this item would put ~120
> lines of `# use …` noise into the project's front page to benefit a doctest run.
>
> The requirement's purpose is that documentation cannot drift from the API without
> something failing. That is now true of all 70 in-crate doctests. It is still **not** true
> of the README, which remains unverified; the honest way to close that is to rewrite its
> fragments as self-contained runnable examples, which is a documentation rewrite rather
> than a test change and is not attempted here.

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
| **Problem** | 201 `.unwrap()` calls outside `#[cfg(test)]`. Most are metrics registration — startup-only and defensible. 25 sit on durability paths, of which 17 are lock acquisition and the rest are logic invariants. See the correction below: this is smaller and less alarming than an earlier revision claimed. |

> **CORRECTION — an earlier revision of this spec was wrong about these.** It claimed
> the three `try_into().unwrap()` calls at `prkdb-storage-segmented/src/lib.rs:210-221`
> were a corrupt-data-to-panic path on WAL recovery, and said the same of
> `storage/streaming_adapter.rs:152,160`. Read in context on 2026-08-08, all five are
> **guarded and cannot panic**:
>
> ```rust
> // segmented/src/lib.rs
> if cursor.len() < 13 { break; }                       // guards [1..5] and [5..9]
> if cursor.len() < 13 + key_len + val_len { break; }   // guards the CRC slice
>
> // streaming_adapter.rs
> if data.len() < 8 { return Err(...); }                // guards [0..4]
> if data.len() < key_end + 4 { return Err(...); }      // guards [key_end..key_end+4]
> ```
>
> Each `unwrap` is on `TryInto<[u8; 4]>` for a slice whose length the preceding check
> has already established. The conversion is infallible at that point; `.expect()` with
> a stated invariant would document it better, but there is no bug.
>
> **The wider claim was overstated too.** Recounting on 2026-08-08 with doc-comment
> lines excluded (the earlier count wrongly treated `/// ... .unwrap()` inside doctests
> as production code), the durability paths hold **25** real production unwraps, and
> **17 of them are lock acquisition** — `self.active_segment.read().unwrap()`,
> `self.log_file.lock().unwrap()`. Those only fail if another thread panicked while
> holding the lock, which is a different failure class from "a panic here loses data".
>
> What this means for the requirement: the WAL recovery path is **more defensive than
> the audit assumed** — bounds-checked, CRC-verified, and returning typed errors. R8 is
> therefore a hygiene and documentation task, not the durability emergency an earlier
> revision described. Scope it accordingly.

**Acceptance:**
1. Lock acquisitions use `.expect("<why this cannot be poisoned>")` rather than bare
   `.unwrap()`. A panic message that names the invariant is worth more than a silent
   conversion, and converting `LockResult` to a typed error throughout would be a large
   refactor for little benefit — a poisoned lock means another thread already panicked.
2. Infallible conversions guarded by a preceding length check use `.expect()` naming the
   guard, so a future edit that removes the guard is visibly wrong.
3. Genuine logic invariants — `segments.keys().last().unwrap()` in
   `write_ahead_log.rs:87-88`, which assumes at least one segment exists — either return
   a typed error or `.expect()` the invariant explicitly.
4. `#![deny(clippy::unwrap_used)]` in `prkdb-core/src/lib.rs`, with
   `#![cfg_attr(test, allow(...))]` for test modules, so the durability crate cannot
   regress.
5. Any counting done for this requirement excludes doc-comment lines. The original count
   treated `/// ... .unwrap()` inside doctests as production code.

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
1. The 0-byte orphan `src/security.rs` is deleted. It is declared nowhere and referenced by
   nothing.
2. `storage_old_inmemory` is **renamed, not deleted** — see the correction below.

> **The audit called `storage_old_inmemory` dead code. It is not.** Verified 2026-08-08:
> `builder.rs:269` constructs `InMemoryAdapter::new()` as the default storage path,
> `storage/mod.rs:14` re-exports it, `lib.rs:96` re-exports it again as public API, `lib.rs:87`
> documents it in the crate-level example, and **10+ integration tests use it**. Deleting the
> module would fail the build and remove a published type.
>
> The real defect is the *name*: "old" plus a comment reading "Renamed from storage.rs to allow
> storage/ directory" describes a refactoring artifact, not the module's purpose. It is the
> in-memory storage adapter and should be `storage/in_memory.rs`, with the re-export in
> `storage/mod.rs` collapsing to a normal `mod` declaration. `prkdb::storage::InMemoryAdapter`
> and `prkdb::InMemoryAdapter` must keep resolving — the rename is internal only.
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
