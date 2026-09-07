# Embedded AI State Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver independently usable SQLite-backed sessions, atomic checkpoint/event commits and idempotent retries without a PrkDB daemon.

**Architecture:** Separate domain contracts, orchestration and SQLite persistence. Every compound mutation uses one SQLite transaction; all future backends must pass the same domain conformance tests. Follow [M1 specification](../specs/2026-09-07-embedded-ai-state-design.md); broader graph/server/retrieval work stays in the [product roadmap](../specs/2026-09-07-ai-database-design.md).

**Tech Stack:** Rust workspace toolchain, serde/serde_json, async-trait, Tokio, SQLx SQLite, tempfile and child-process recovery tests. Use workspace-pinned dependency versions where suitable; avoid inheriting full runtime/network feature sets unnecessarily.

Status: implementation not started. This plan does not claim the audited native database defects are fixed.

## File map

| Files | Purpose |
| --- | --- |
| `crates/prkdb-ai-types/src/{lib,scope,state,error,capabilities}.rs` | Public validated types, operation models, errors and capabilities |
| `crates/prkdb-ai/src/{lib,store,session,validation,canonical}.rs` | Backend port, scoped handles, request validation/canonicalization |
| `crates/prkdb-ai-sqlite/src/{lib,open,migrate,commit,read,close}.rs` | SQLite adapter with explicit connection and lifecycle ownership |
| `crates/prkdb-ai-sqlite/migrations/0001_state.sql` | Versioned schema, foreign keys and query indexes |
| `crates/prkdb-ai/tests/contracts.rs` | Validation and port behavior tests |
| `crates/prkdb-ai-sqlite/tests/{state,concurrency,recovery,pagination,migrations,lifecycle}.rs` | Backend acceptance tests |
| `crates/prkdb-ai-sqlite/tests/support/{mod,conformance,faults}.rs` | Reusable behavioral cases and deterministic fault hooks |
| `crates/prkdb-ai-sqlite/examples/resume_session.rs` | Executable local restart demonstration |
| `tests/ai-consumer/{Cargo.toml,src/main.rs}` | Standalone packaging/feature/dependency fixture, excluded from workspace |
| `scripts/check_ai_dependency_boundary.sh` | Assert forbidden server crates are absent from minimal consumer graph |
| `.github/workflows/ci.yml` | Focused contract/recovery/package jobs |
| `docs/ai/embedded-state.md` | Tested usage, guarantees, limitations and error recovery |

Do not put domain methods in the existing `StorageAdapter`, reuse IndexedStorage transactions, or depend on the main `prkdb` crate. Do not opportunistically repair unrelated native APIs in these commits.

## Task 1 — define public types and dependency boundary

- [ ] Create manifests for `prkdb-ai-types`, `prkdb-ai`, and `prkdb-ai-sqlite`; register them in root `Cargo.toml`. Give internal publishable dependencies both path and version. Use explicit minimal Tokio/SQLx features.
- [ ] Define `Scope`, `SessionId`, `OperationKey`, checked `Revision`/`Sequence`, JSON checkpoint/event request types, receipts, snapshot/page types and typed errors from the spec. Expose constructors that enforce size/nonempty rules; do not expose unchecked deserialization as a trusted scope constructor.
- [ ] Write table-driven validation cases for empty/overlong UTF-8 IDs, request/event limits, sequence/revision overflow and error serialization before implementing constructors. Run `cargo test -p prkdb-ai-types`; first confirm relevant new tests fail, then implement and require them to pass.
- [ ] Define an object-safe async state backend port with create/open/load/commit/events and an owned store lifecycle. Choose handle ownership explicitly so closing the store affects all clones. Add an unsupported-capability fake proving rejection occurs before mutation.
- [ ] Run `cargo check -p prkdb-ai --no-default-features` and inspect `cargo tree -p prkdb-ai`; expected: no `prkdb`, `prkdb-proto`, tonic, axum, ORM or model SDK.
- [ ] Commit the contracts and passing focused tests.

## Task 2 — file creation, schema migration and durable configuration

- [ ] Write `migrations.rs` cases for fresh creation, reopen, unknown newer format, corrupt existing file, migration interruption and two concurrent opens. Use temp directories; never test against user databases.
- [ ] Add a termination test at the schema/version boundary: both must recover together. Write format-version metadata in the same transaction as schema changes, never in a postcommit update.
- [ ] Create the four tables from the spec with composite primary keys and foreign keys. Include revision/next-sequence/usage counters and persisted canonical request/receipt columns. Write explicit constraints for valid counter ranges.
- [ ] Implement open/configuration and serialized transaction-based migration. Set WAL, FULL synchronous, foreign keys per connection and bounded busy admission. Verify pragmas after setting them. Do not turn a read/parse failure into a new database.
- [ ] Run `cargo test -p prkdb-ai-sqlite --test migrations`; confirm each new acceptance test fails before its behavior exists and passes after implementation.
- [ ] Commit schema/open behavior and tests.

## Task 3 — scoped sessions and snapshot reads

- [ ] Write tests in `state.rs` for revision-zero creation, duplicate create (`AlreadyExists`), missing open/load, and identical session IDs in two tenants and namespaces.
- [ ] Implement create/open and bound scoped handles using SQL parameters. Include scope columns in every lookup and foreign key.
- [ ] Implement a snapshot load returning checkpoint, revision and last sequence together. Test against a concurrent writer with a deterministic barrier once commit support is available; mark the dependency in the test implementation rather than silently skipping it.
- [ ] Run `cargo test -p prkdb-ai-sqlite --test state`; require passing create/read/isolation cases. Commit.

## Task 4 — canonical requests and atomic commit

- [ ] Write canonicalization tests in `contracts.rs`: object key order equivalent, array/event order significant, changed expected revision/schema version significant, unsupported numbers rejected. Use exact canonical bytes for duplicate equality.
- [ ] Write this behavioral sequence in `state.rs`: create session; commit expected revision 0 with two events and key `a`; assert revision 1 and sequences 1/2; retry `a` unchanged and get the original receipt; reuse `a` with changed checkpoint and get `IdempotencyConflict`; new key with expected revision 0 gets `RevisionConflict`.
- [ ] Add fresh checkpoint absence versus committed JSON null tests. Assert exact receipt fields (committed revision, optional inclusive sequence range, resulting last sequence), including absent range for checkpoint-only commits. Add independent scopes/keys tests. Run the focused tests and confirm missing behavior fails.
- [ ] Implement transaction admission, existing-operation lookup first, conditional revision update, sequence allocation, event inserts, checkpoint replacement, quota counters and receipt storage on one connection. Roll back every precommit failure. Handle numeric overflow before mutation.
- [ ] Test exact logical byte/event/operation quota boundaries, rejection without mutation, checkpoint replacement subtraction, returned usage counters and identical retries at full quota. Use the precise accounting formula from the spec; do not count SQLite overhead as logical payload.
- [ ] Enforce encoded byte/event/session quotas including canonical request storage; identical retries do not consume counters again. Persist receipts with stable fields, excluding nondeterministic per-call durations.
- [ ] Run `cargo test -p prkdb-ai --test contracts` and `cargo test -p prkdb-ai-sqlite --test state`. Commit only with passing focused tests.

## Task 5 — concurrency, failure and retry semantics

- [ ] In `concurrency.rs`, start two commits at revision 0 with different keys behind a start barrier with adequate admission budget and no infrastructure faults: exactly one succeeds and one conflicts. Repeat same-key/same-request race: both return one identical receipt and only one event batch exists.
- [ ] Test busy admission with an independent write connection and a short caller deadline. The total elapsed budget includes busy waiting and any retry; verify a typed error and unchanged state.
- [ ] Add test-only fault points before each statement, immediately before commit and after commit before response. Fault controls must not be compiled into production-default builds or controllable through ambient environment variables.
- [ ] In `recovery.rs`, execute a child process that commits and is terminated at controlled boundaries. Reopen and assert checkpoint/events/receipt agree on either old or complete new state. Test retry resolution after an intentionally lost postcommit response.
- [ ] Propagate one absolute operation deadline through admission, reads/statements and commit confirmation. Add deterministic deadline tests before commit (DeadlineExceeded and rollback), after commit attempt without confirmation (OutcomeUnknown), and confirmed commit at expiry (return original receipt). Keep unresolved connections unavailable until cleanup/commit resolution; test that they cannot be borrowed prematurely and can be used again after resolution.
- [ ] Test future cancellation/connection cleanup and subsequent successful admission; use barrier notifications instead of sleeping to infer completion.
- [ ] Run `cargo test -p prkdb-ai-sqlite --test concurrency --test recovery`. Commit implementation corrections and deterministic tests.

## Task 6 — bounded event pagination

- [ ] Write tests for after-sequence exclusivity, captured upper bound, continuation under appends, wrong scope/session cursor, invalid cursor version, byte-limited pages, maximum limits and empty/exhausted pages.
- [ ] Implement SQL reads with full scope predicates, upper sequence bound, sequence ordering and row limit. Enforce encoded byte allowance while retaining a correct next cursor when more rows remain.
- [ ] Validate cursors as data, never SQL. In M1, cursor tampering may select a position only within the already authorized session; it cannot select another scope.
- [ ] Run `cargo test -p prkdb-ai-sqlite --test pagination`; commit.

## Task 7 — lifecycle, observability and reusable conformance

- [ ] Add `lifecycle.rs` tests: close stops new admission, waits for an in-flight operation, reports errors, and closing one store affects its handles. Verify Drop has no falsely advertised awaited durability guarantee.
- [ ] Implement admission tracking and async close. Add structured operation hooks without logging payloads, canonical bytes or sensitive scope values.
- [ ] Move backend-independent assertions into `tests/support/conformance.rs` using a factory so future backends run the same cases. Keep SQLite-specific pragma/migration/child-process checks separate.
- [ ] Run all three new crates' tests and default/no-default feature checks. Commit.

## Task 8 — executable example, package validation and CI

- [ ] Implement `resume_session.rs`: accept a temp file path, create/open one session, retry a fixed operation, close/reopen, and verify exact checkpoint/events. Run twice against the same file to demonstrate retry safety. No model key or server.
- [ ] Create an excluded standalone consumer fixture with explicit package versions. Build it against locally packaged crates via a temporary registry/vendor staging mechanism; do not count workspace path resolution as proof of publishability.
- [ ] Add `check_ai_dependency_boundary.sh` using Cargo's structured dependency graph. Test it with a known bad fixture containing a forbidden server dependency; it must fail.
- [ ] Add focused CI steps for new-crate fmt/clippy/tests, Linux/macOS embedded integration and clean consumer/dependency verification. Add child-process recovery to the appropriate slower lane with artifacts on failure. Preserve nonzero status.
- [ ] Write `docs/ai/embedded-state.md` from the running example, including OutcomeUnknown retry behavior, limits, durability, quotas, and that graph/retrieval are separate milestones.
- [ ] Run `cargo fmt --all -- --check`, focused Clippy/tests, example twice, and standalone consumer verification. Run existing affected workspace checks once the new dependencies/manifests are final. Record exact results and limitations, then commit.

## Native correctness work alongside M1

The [review](../../reviews/2026-09-07-senior-review.md) is the M0 backlog. Split it into focused implementation specs/PRs: (a) namespace codec and migration, (b) atomic commit/concurrency contract, (c) durable outbox and completion barriers, (d) schema validation/persistence, (e) CI baseline diagnosis/release preflight. Each requires its own fault or interleaving regression and compatibility analysis. Do not bundle the entire database rewrite into M1.

## Completion criteria

M1 is complete only when all ten acceptance categories in its specification have evidence, the clean consumer works, no server/model dependency is required, and the example resumes correctly after restart and duplicate requests. Report unsupported native/graph/remote capabilities explicitly. A complete M1 is not a claim that the complete database roadmap or existing P1 findings are resolved.
