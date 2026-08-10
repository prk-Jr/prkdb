# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

A correctness and security hardening pass. The theme is that PrkDB's implementation was
substantially ahead of its evidence: several claims the repository made about itself were
not enforced by anything, and several tests reported green while testing nothing.

### Security

- **Authorization model** — principals, roles, grants, and a `Read < Write < Admin`
  permission ordering. `Admin` on `*` reproduces the previous single `PRKDB_ADMIN_TOKEN`,
  which is the migration path rather than a coincidence. Credential comparison is
  constant-time.
- **HTTP data plane now requires authorization.** Every `/collections/*` route, including
  `PUT .../data` and `DELETE .../data/:id`, was previously reachable with no credential.
  The generated Python, TypeScript, and Go clients all target this API.
- `prkdb-cli serve` **refuses to start** with no principals configured unless
  `--allow-anonymous` is passed explicitly, and warns loudly when it is.
- **TLS is reachable from the shipped binary** via `--tls-cert`, `--tls-key`, and
  `--tls-client-ca`. The mTLS implementation already existed but its only caller anywhere
  in the workspace was an example, so no binary a user runs could enable it.
- **Chaos fault injection no longer ships in release builds.** It is behind a `chaos`
  feature; previously anyone able to set `CHAOS_CONFIG_PATH` could partition a live
  cluster, and the check ran a file read and JSON parse on every Raft RPC.
- Supply-chain scanning via `cargo-deny` and `cargo-audit` in CI. The first scan fixed
  eight vulnerabilities by version bump, including `tar` (symlink chmod, used by backup)
  and `memmap2` (unsound pointer offset, the WAL is mmap-based).
- Seven of fifteen workspace crates declared **no license at all**. All now resolve to
  Apache-2.0.

### Added

- **Benchmark methodology** (`docs/benchmarks/methodology.md`) with the command, hardware
  and results, plus a register of the ~40 published performance figures and their status.
  Several did not survive contact with a benchmark: "800x faster" for batched writes
  measures **95x** here (822 → 78.4K ops/sec), and the 1.2M ops/sec "Legendary" preset is
  produced by nothing in the repository. Unverified numbers are now labelled as such, and
  `plan_status.sh` rejects a bare throughput figure in a doc comment.
- **Version and roadmap drift detection** in `xtask repo-status`. The roadmap announced
  "v2.0-clean" while the workspace was `0.6.0`, and listed client SDKs as future work while
  five CI jobs exercised them. The pre-existing feature-drift check matched anywhere in the
  file, so the very fix it demanded left it firing; it is now section-aware.
- **An in-process cluster harness** (`InProcessCluster`). Raft tests no longer need a
  prebuilt `prkdb-server` binary — the dependency that produced
  `#[ignore] // Requires server binary` on the tests of a consensus implementation. It
  forms real clusters, partitions them, and stops nodes, all inside the test process.
- **Election safety is asserted**: at most one leader per term, sampled across partitions,
  heals, and leader loss. Stated per term deliberately — two simultaneous leaders in
  *different* terms is correct Raft behaviour, so the obvious assertion is the wrong one.
- **The read consistency modes are verified to differ.** `ReadConsistency::Linearizable` is
  public API on three surfaces and had no enforced coverage. A linearizable history is now
  checked with the Wing & Gong search, and a partition demonstrates a stale read genuinely
  lagging — without which a "linearizable" mode that was merely the stale code path would
  pass every test.
- **Backups carry a manifest**: length, SHA-256, entry count, and format version, written
  beside the archive. `restore` verifies before writing anything, and reports truncation as
  truncation rather than as an opaque digest mismatch. `--skip-verify` exists for salvage.
- Backup scheduling and verification guidance in the deployment guide, with systemd-timer
  and cron examples. The retention glob deletes each archive's manifest with it.
- WAL segments carry a **magic number and format version**. A future format is refused
  rather than misparsed; segments written before headers existed are still read.
- `/livez` and `/readyz` as distinct probes. Liveness touches nothing; readiness reports
  whether WAL replay finished and a leader is known, naming the unmet condition on 503.
- `--rate-limit` sheds excess requests with 429 and `Retry-After`. Probe endpoints are
  exempt: rate-limiting a liveness check gets the node killed under the load where it most
  needs to stay up.
- Coverage measurement in CI with a ratcheting floor. First measurement in the project's
  history: 55.46% lines.
- `scripts/plan_status.sh`, which reports progress by inspecting the repository rather
  than by reading checkboxes.

### Fixed

- **The CLI could not talk to a server that enforced authorization.** Every remote
  subcommand built its client as `PrkDbClient::new(servers).await?.with_admin_token(token)`,
  but `new` fetches cluster metadata before returning and `Metadata` requires `Read`. The
  credential arrived one step after the call that needed it, so `prkdb-cli schema list`
  against a server started with `PRKDB_BOOTSTRAP_TOKEN` failed with `Failed to fetch
  metadata from any bootstrap server` — a message naming the network and never mentioning
  authorization. Twelve call sites now route through one `remote_client::connect`, and a
  global `--credential` / `PRKDB_CREDENTIAL` lets a non-admin principal use the CLI. The
  bug survived local testing because every script that exercised the CLI ran it against an
  anonymous server; it surfaced only once the mixed-client integration test was switched
  to an authorized one.
- **A server started with `--allow-anonymous` could be written to by anyone and
  administered by no one.** The deprecated-`admin_token` check denied every admin RPC when
  the server had neither a token nor the authorization layer. That is right when the layer
  went missing by accident and wrong when the operator asked for it: `ListSchemas`
  answered every caller with `Unauthenticated` on a server whose collections were already
  open to unauthenticated reads and writes. The two cases are now distinguished, and the
  accidental one still denies.
- **The generated Go client sent no credential on reads.** `ListRaw` called
  `http.Client.Get`, which takes no headers and so skipped `authorize()` entirely, while
  writes — which build their request explicitly — were authenticated. Reads against a
  secured server failed with `prkdb: not authenticated`. `authorize`'s own doc comment
  claimed "every request is built through this, so a new method cannot silently ship
  unauthenticated"; that was an aspiration with nothing enforcing it, and a test now
  asserts the dispatched-request count equals the authorized-request count.
- **The database lost every write when it was reopened.** Three independent defects, each
  sufficient on its own, and each masking the others:
  - `MmapLogSegment::create` opens with `truncate(true)`, and the path
    `PrkDb::builder().with_data_dir()` reaches called `create` unconditionally — so
    opening a data directory zeroed its write-ahead log. A correct `open` existed and was
    never called. Replaced with `open_or_create` on every database-open path.
  - `WalStorageAdapter::new_with_config` never rebuilt the in-memory index. `open` and
    `open_async` both did; the constructor a user actually reaches did not, so even a
    recovered log stayed invisible.
  - `CollectionPartitionedAdapter` never discovered collections already on disk, because
    collections open lazily. A freshly opened database reported an empty collection set.

  No test had ever reopened a data directory and read from it. `crates/prkdb/tests/durability.rs`
  now does, including that a write after a reopen appends rather than overwriting.
- **`prkdb backup` silently backed up nothing** (S-04). It failed outright with
  "take_snapshot not supported" on any database opened with `--database`, and once that
  was fixed it produced a valid archive containing zero entries because of the collection
  discovery bug above. `CollectionPartitionedAdapter` now implements `take_snapshot`,
  merging its per-collection WALs into one archive keyed `collection:id` so the existing
  restore routes each entry back without needing to know about collections. The
  round-trip test is no longer `#[ignore]`d.
- **`ReadIndex` did not confirm leadership, so linearizable reads were not linearizable**
  (S-06). A leader partitioned away from its cluster keeps believing it leads, and served
  reads from a commit index the rest of the cluster had moved past — through the API that
  advertises linearizability. The code carried a comment saying a heartbeat round "should"
  happen and that trusting local state was "good enough for most cases"; the case it is not
  good enough for is the only one the guarantee exists for. `read_index` now requires a
  majority to acknowledge a heartbeat in the current term before returning an index, per
  Raft §6.4. Found by the register test's first partitioned run, reproducing about two
  runs in five.
- **A correct test was switched off for finding a real bug.**
  `chaos_test_rapid_recovery` carried
  `#[ignore = "Manual investigation: integration harness still diverges from WAL recovery
  unit tests"]`. The harness did not diverge — `WalStorageAdapter::new` truncated the WAL,
  so every reopen destroyed the previous cycle, and the test said so: *"Lost data from
  cycle 0 key 0"*. It passes now, and reverting `open_or_create` reproduces the original
  failure. `scripts/check_ignore_reasons.sh` closes the category list so a test can be
  skipped for being slow or needing a binary, never for failing.
- **Mutation testing in CI**, scoped to the authorization model and the storage wrapper.
  Coverage says a line ran; it does not say an assertion would have noticed the line being
  wrong, and this repository has had the second without the first three times. The manual
  version of this — delete the fix, see whether anything complains — is what found S-07's
  missing test.
- Coverage floor ratcheted 55 → 58 (measured 59.03% lines, from a 55.46% baseline).
- **`scan_range` was unsupported on the default storage adapter** (S-08), so
  `CollectionHandle::scan_range_by_id_bytes` — public API — failed on every database
  opened with `--database`. Found by auditing the trait rather than by hitting it.
- **`fetch_segment` reported success while streaming nothing** (S-09). It called
  `get_changes_since`, which the partitioned adapter does not implement, then logged the
  error and ended the stream — so a caller replicating from that segment concluded there
  was nothing to replicate. An empty log and an unreadable one looked identical. The error
  is now surfaced; the missing method is a design decision (offsets are not comparable
  across per-collection WALs) and is documented rather than guessed at.
- **`scripts/check_wrapper_completeness.sh`** compares a wrapper's trait implementation
  against the adapter it wraps, restricted to methods whose default returns an error. Four
  such methods had gone missing from `CollectionPartitionedAdapter`, three of them found
  only when someone used the affected feature. Runs in CI.
- **`scan_prefix` was unsupported on the default storage adapter** (S-07), with no
  regression test until a verification pass removed the fix and found the suite still
  green. `durability.rs` now covers it directly against the wrapper — a test reaching for
  `WalStorageAdapter` or `SledAdapter` passes whether or not the wrapper forwards anything,
  which is why the gap existed. Originally: so
  `list_collections` and principal loading failed on any database opened with
  `--database`. The third method missing from `CollectionPartitionedAdapter` after
  `take_snapshot` and collection discovery — a trait default that returns "not supported"
  lets an incomplete wrapper compile clean and fail at runtime.
- **Principals are persisted** (R12). `PrincipalStore` was an in-memory map, so every
  credential vanished on restart, a restarted node authenticated nobody including the
  operator, and `PRKDB_BOOTSTRAP_TOKEN` had to stay set — turning a one-time bootstrap into
  a permanent back door. Principals now go through the storage adapter under the reserved
  `__prkdb_metadata:` prefix, inheriting the WAL, Raft replication, snapshot and restore.
  **Only a SHA-256 digest of each credential is stored**, so a backup archive or a
  `fetch_segment` stream is no longer a credential dump.
- **No shipped client could talk to a secured server.** `PrkDbClient` sent its
  `admin_token` as a *message field*, on the admin RPCs that declare one. The data plane —
  `put`, `get`, `delete`, `batch_put`, `watch` — has no such field, so once the server
  enforced authorization every data call from the official client returned
  `unauthenticated`. Securing the server without this was shipping a lock and no key. All
  18 request sites now go through one helper that attaches the credential;
  `connect_with_credential` exists because `Metadata` requires `Read`, so a credential set
  after `new()` arrives too late to bootstrap.
- **Generated Python, TypeScript and Go clients** now send a credential and raise distinct
  errors for 401 and 403. Previously none of the three sent one, and all collapsed both
  statuses into "request failed" — which makes a client retry a permission error forever.
- **Principals can be administered at runtime.** `GET`/`PUT /admin/principals` and
  `DELETE /admin/principals/:name`, all requiring `Admin` on `*` — enforced by path prefix,
  because the default method mapping would have let any `Read` holder enumerate credentials
  and any `Write` holder mint an admin. Revocation takes effect on the next request,
  without a restart. The last admin cannot be revoked.
- **`GET /collections` is filtered to the caller's grants**, with the total corrected to
  match; an unfiltered total discloses how many collections are being hidden. A caller
  entitled to nothing gets `200 []`, not `403` — the request was permitted, the answer is
  empty.
- **Both metrics servers require `Admin`.** `prkdb-server` exposed `/metrics` on its own
  port with no authentication at all, on the assumption that a metrics port is private.
  That is a deployment property, not a property of the binary.
- **WebSocket authentication decided.** `PRKDB_WS_TOKEN` and the authorization layer both
  read the same `Authorization` header, so with principals configured a client presenting
  its own credential failed the shared-token comparison and was refused a connection it was
  entitled to. The variable is now **ignored when authorization is enabled** (with a
  warning) and still honoured under `--allow-anonymous`, so no deployment loses a gate. Its
  comparison was also non-constant-time; it now uses `subtle`.
- **`admin_token` deprecation.** The field is still accepted, now compared in constant time
  — it used `!=`, leaking the token's prefix through response timing. An `Admin` principal
  no longer needs it: the layer has already required `Admin` to reach the handler. The
  relaxation is gated on authorization actually being enforced, so a server with neither
  the layer nor a token still denies.
- **A three-node cluster now elects and replicates with mTLS active end to end**, via
  `InProcessCluster::new_with_mtls`. Every node serves TLS to its peers and dials TLS to
  them simultaneously, which is where mTLS actually has to work and what none of the
  single-connection tests could establish. Verified load-bearing: removing the pool's TLS
  while the servers still require it reproduces the S-10 failure, "no leader elected
  within 20s".
- **mTLS peer authentication could be selected but never worked** (S-10, introduced by
  this work). `RpcClientPool` built `http://{addr}` unconditionally — the module contained
  no TLS of any kind — so a cluster configured with `--tls-client-ca` had servers demanding
  a client certificate and peers dialling plaintext at them. No `AppendEntries` landed and
  no leader could be elected, while the node started cleanly and reported nothing wrong.
  `PeerIdentity::from_config` preferred mTLS whenever a CA was present, so this was the
  configuration the security guidance led to. The pool now dials `https://` with a client
  identity when configured, `serve` refuses `--tls-client-ca` without a certificate and
  key, and the policy is only selected when the material to honour it exists.
- **Raft peer RPCs are now authenticated** (S-01, final part). `RaftService` carries
  `AppendEntries`, `RequestVote` and `ReadIndex` and is exempt from the client-API layer by
  design, because peers present certificates rather than bearer credentials — which meant
  nothing checked it at all. `PeerAuthInterceptor` is registered on both binaries: mTLS
  when a cluster CA is configured, a shared secret otherwise. A node **with peers refuses
  to start** unauthenticated unless `--allow-unauthenticated-peers` is passed; a
  single-node instance does not, since requiring certificates to run `serve` locally would
  make the opt-out the default habit.
- **The gRPC data plane is now authorized** (S-01). The policy was implemented and
  unit-tested but *never registered on the server*, so `fetch_segment` continued to stream
  raw WAL segments to any caller. `AuthzGrpcLayer` is a tower layer — a tonic `Interceptor`
  receives `Request<()>` and cannot see the method name, so it could not distinguish
  `Health` from `FetchSegment`. Peer `RaftService` traffic is excluded and still
  authenticates by mTLS. `scripts/plan_status.sh` checks the registration separately from
  the file's existence, since "exists but unregistered" is exactly the state this sat in.
- **Batched writes are now published atomically** (S-03). A batch was appended to the WAL
  as a unit but inserted into the index one key at a time, so a concurrent reader could
  observe half a commit. Publication now happens under a dedicated barrier, and
  `snapshot_get_many` holds it for the duration of a multi-key read. Nothing was ever lost;
  this was always a read-visibility property, not a durability one.
- **The QueryBuilder DSL was documented but implemented for nothing.** The `Collection`
  derive emitted a `{Struct}QueryExt` trait whose `where_*` methods all carried default
  bodies, and then implemented it for no type, so
  `db.query::<User>().where_role_eq("admin")` — shown in the README as "type-safe, fluent
  query API with macro-generated field methods" — compiled for no one. Nothing had ever
  compiled the README, and the working `where_*_eq` methods elsewhere in the workspace
  come from `prkdb-orm-macros`, a different derive. The missing impl is now emitted.
- **Every README example compiles.** The skip list is empty, from nine entries. Each was a
  real defect: a struct whose own fence read a field it did not declare, an aggregate
  closure with an uninferable return type, `start_auto_sync` on an immutable binding, an
  `LruCache` keyed by the wrong type, `take_while` comparing a `String` against an integer,
  `get_local`/`get_follower_read` shown on an `IndexedStorage` when they are `PrkDb`
  methods, `ConsistentHashRing::get_partition` (the method is `get_partition_for_key`), and
  a fence tagged `rust` containing `...` placeholders.
- **`test_leader_crash_during_write` failed in CI on a property it does not test.** The
  read helper treated "Not leader. Leader is None" as terminal, though a cluster mid
  election is transient in exactly the way a transport error is, and the post-restart read
  allowed three retries where its neighbours allow thirty. The test asserts data
  consistency and was failing on election timing. It surfaced now because this is the first
  branch on which the chaos job actually runs.
- **All 70 documentation examples in `prkdb` now compile**, from 7 passing and 63
  `#[ignore]`d. Converting them surfaced eight API drifts that no test could have caught,
  including `Transaction::insert` documented as `async` when it is synchronous,
  `create_compound_index` shown with one generic argument where it takes two, and
  `PartitionedStreamingAdapter::new` shown with a partition-count argument it does not
  take. A README diagram mistagged as `rust` is now `text`.
- **The linearizability checker could not fail.** It asked only whether *some* write of the
  same value had started before the read ended — a condition any earlier write satisfies.
  Replaced with Wing & Gong linear search, guarded by a meta-test that injects the
  canonical stale read.
- **The bank invariant test never touched the database.** It operated on an
  `Arc<Mutex<HashMap>>`, asserting a property of `std::sync::Mutex`. Now runs real
  `Serializable` transactions against real storage.
- **Chaos test results were discarded.** The workflow never built `prkdb-server`, which
  every test needs, and `continue-on-error` hid the resulting failure while a README badge
  advertised "19 passing".
- **The mutation-testing job could not finish, so it reported nothing.** Its first step
  alone found 100 mutants and spent ~133s on each, needing ~3.7 hours against a 45-minute
  limit; run 31329241574 was cancelled having tested 7. Its own comment already warned
  that "a job people cancel is a job that tests nothing" — it arrived there by timeout
  instead. Now sharded ten ways. The seven mutants it did reach all survived, every one an
  accessor (`Grant::pattern`, `Role::name`, `Role::grants`, `Principal::grants`,
  `Principal::credential_hash`) whose only callers are in `prkdb-cli` while the run was
  `--package prkdb` — a scoping artifact rather than a logic gap, but the consequences are
  real enough (`credential_hash` returning `""` writes a principal no credential can
  match) that they are now pinned by direct tests instead of by widening the scope.
- Three WAL corruption tests were `#[ignore]`d for no stated reason and pass in 0.7s.
- CI jobs had no `timeout-minutes`, so a hung test consumed GitHub's six-hour default.
- `cargo clippy --all-targets -- -D warnings` did not pass on the pinned toolchain.
- Hardcoded ports across the test suite; `compaction_test` and `read_index_test` both bound
  50001, and an orphaned process could poison every later run.

### Changed

- `storage_old_inmemory` renamed to `storage/in_memory.rs`. It was listed as dead code but
  is the default storage backend, re-exported as public API and used by 18 test files.
- Toolchain pinned via `rust-toolchain.toml`; MSRV declared as 1.95. The README previously
  claimed 1.70+, which nothing verified.
- README badges point at real workflow status rather than hardcoded strings.
- `Cargo.lock` is now tracked. The workspace ships binaries, and both CI and the release
  workflow pass `--locked`, which cannot succeed without a committed lockfile.

### Known issues

- **27 of the README's 37 Rust examples are compile-checked.** The remaining 10 are listed
  in `xtask/src/readme_tests.rs` with a specific defect each — not "does not compile",
  which is the finding rather than the excuse.
- The README's query and aggregate examples call `query`/`count`/`sum` on a `db` that a
  reader following the quick-start will hold as a `PrkDb`. Those methods live on
  `IndexedStorage`. The examples are correct for the type they mean and never say which
  type that is.

- Two keys read with two separate `get()` calls are still not a snapshot; use
  `snapshot_get_many` or a transaction. This is a property of the API the caller picks,
  not a defect, and `batch_atomicity.rs` asserts it so it stays explicit.

## [0.6.0]

Baseline. See the git history prior to this changelog.

[Unreleased]: https://github.com/prk-Jr/prkdb/compare/v0.6.0...HEAD
[0.6.0]: https://github.com/prk-Jr/prkdb/releases/tag/v0.6.0
