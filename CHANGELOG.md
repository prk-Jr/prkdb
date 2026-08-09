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

- **`RaftService` is unauthenticated.** Peers are exempt from the client-API layer by
  design and `PeerAuthInterceptor` is written but not installed, so a caller who can reach
  the port can still forge `AppendEntries`. Tracked as Task 2b Step 4.
- The README's 37 Rust examples are still compiled by nothing. Including it as crate docs
  was attempted and reverted: most are two-line fragments needing hidden `# ` setup, which
  renders literally on GitHub where the README is actually read. Closing this properly
  means rewriting the fragments as self-contained examples.
- Two keys read with two separate `get()` calls are still not a snapshot; use
  `snapshot_get_many` or a transaction. This is a property of the API the caller picks,
  not a defect, and `batch_atomicity.rs` asserts it so it stays explicit.

## [0.6.0]

Baseline. See the git history prior to this changelog.

[Unreleased]: https://github.com/prk-Jr/prkdb/compare/v0.6.0...HEAD
[0.6.0]: https://github.com/prk-Jr/prkdb/releases/tag/v0.6.0
