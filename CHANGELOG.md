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

### Known issues

- **S-01 (partial)** — the gRPC data plane is still unauthenticated. `fetch_segment`
  streams raw WAL segments to any caller. The authorization policy is implemented and
  tested; registering it on the running server is tracked as Task 2b.
- **S-03** — no snapshot read outside a transaction. A client reading several keys with
  `get()` can observe a multi-key commit half-applied. Committed state is always correct.
- **S-04** — `prkdb backup` fails on any database opened with `--database`.
  `CollectionPartitionedAdapter` does not implement `take_snapshot`.
- 63 documentation examples are still `#[ignore]`d and do not compile.

## [0.6.0]

Baseline. See the git history prior to this changelog.

[Unreleased]: https://github.com/prk-Jr/prkdb/compare/v0.6.0...HEAD
[0.6.0]: https://github.com/prk-Jr/prkdb/releases/tag/v0.6.0
