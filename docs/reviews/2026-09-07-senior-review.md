# PrkDB implementation review — 2026-09-07

Reviewed revision: `adc101fb` (short SHA `adc101f`). Work branch: `codex/ai-database-foundation`.

## Assessment

PrkDB has substantial working infrastructure: modular Rust crates, storage backends, a WAL, Raft, typed collections, an ORM, generated clients, authorization layers, and CI. The next priority is to make the guarantees consistent across those surfaces. Current code does not justify an unqualified ACID or production-readiness claim across all adapters and APIs.

The product direction is a complete database with an independently usable AI library. Graph support belongs in that direction, but adding retrieval over unreliable writes or incorrectly scoped records would compound existing problems. Build one capability contract and test suite, then expose it through embedded, server, and AI interfaces.

This is a targeted senior review, not a proof that all code is correct. It covers primary storage and indexed transactions, collection batching/outbox, schema persistence, package boundaries, and CI/release behavior. It does not establish full Raft correctness, a security certification, or benchmark performance. Older audit documents were context, not evidence that a defect exists or has been fixed.

## Prioritized findings

P1 means address before trusting the affected guarantee in production. P2 means a significant correctness or validation gap. Static findings below were traced in the current implementation; probes and remote observations are identified separately.

### R01 — P1: indexed collections overwrite each other's records

**Code:** `crates/prkdb/src/indexed_storage.rs:4280–4287`, `5273–5276`.

`insert<T>` scopes index metadata by collection, but serializes only the record ID for primary storage. `get<T>` uses the same unscoped ID. Insert User(1), then Project(1): Project replaces User. Compatible JSON shapes silently deserialize as the wrong type; incompatible shapes fail on read. The ordinary collection handle uses a different key layout, so the two APIs do not share a coherent namespace contract.

**Required correction:** versioned keys containing an explicit stable collection identifier and tenant/namespace. Apply the same codec to all CRUD, transaction, index, scan, backup, and migration paths. A Rust module/type rename must not rename persisted collections. Legacy indexed keys may already have lost their collection identity; do not guess during migration. Require an explicit mapping or report ambiguity.

**Acceptance:** identical IDs in two collection types stay independent through get/query/update/delete and restart. See diagnostic `probes/core_review.rs`.

### R02 — P1: mixed WAL transaction commits are not atomic

**Code:** `crates/prkdb/src/transaction.rs:415–422`; `storage/wal_adapter.rs:482–508`, `547–563`.

Commit writes all puts, then all deletes, through separate WAL appends and publication barriers. Failure or cancellation between them leaves a partially committed transaction. A snapshot reader can observe new records before deleted records disappear. A Serializable writer lock does not combine these two publication steps.

**Required correction:** one mixed operation transaction with a durable commit boundary, recovery rules, and one publication boundary. A mutex alone cannot solve crash atomicity.

**Acceptance:** replace A with B in one transaction; readers and reopened storage observe either A or B, never the intermediate combination. Inject failure/cancellation at every append, commit, and publication boundary.

### R03 — P1: indexed transactions execute independent writes

**Code:** `crates/prkdb/src/indexed_storage.rs:3308–3322`, `3354–3362`.

The method documented as atomic loops over operations, performs each backend mutation, then changes in-memory indexes. If the second operation fails, the first record and indexes remain changed while commit returns an error. This affects put-only transactions too and is separate from R02.

**Required correction:** require an atomic transaction capability and publish indexes consistently with committed records. Backends without it must return a typed unsupported-capability error before mutation.

**Acceptance:** fail the second operation in a multi-record transaction; no partial primary or index changes remain. Exercise insert, replacement, delete, uniqueness violations, and recovery.

### R04 — P1: Serializable validation can accept nonserializable histories

**Code:** `crates/prkdb/src/transaction.rs:275–281`, `370–374`, `417`, `422`; `storage/wal_adapter.rs:2179`, `2198`, `2235`.

Two defects require separate regression tests:

1. Every read replaces the previous read-set hash. T reads x=0, another writer commits x=1, T reads x=1, and T can commit because validation retained only the last value. No serial placement explains both observations.
2. ReadCommitted transactions call unlocked write methods without taking either side of the Serializable transaction barrier. They can change a key between another transaction's validation and publication.

**Required correction:** retain first-read versions or provide snapshots; all write paths must participate in one concurrency protocol. Include absent-key reads and ABA changes in the version design; comparing only value hashes is insufficient to represent mutation history.

**Acceptance:** controlled two-read history conflicts or returns a stable snapshot. A default transaction cannot publish while the exclusive validation guard is held. Verify the actual validation/write interleaving, not just lock acquisition.

### R05 — P1: Sled atomic outbox methods use two independent tree batches

**Code:** `crates/prkdb-storage-sled/src/lib.rs:263–268`, `285–290`.

KV and outbox updates are applied independently. Failure or termination between them can leave a primary mutation without its event. Flushing afterwards does not turn independent batches into a transaction.

**Required correction:** use a transaction spanning both trees and define durability at acknowledgment. Propagate transaction errors without downgrading to non-atomic writes.

**Acceptance:** failure injection for put and delete proves primary data and its event appear together or not at all, including after reopening.

### R06 — P1: collection flush does not wait for persistence or report failures

**Code:** `crates/prkdb/src/batch_accumulator.rs:49`, `81`, `92`, `131–135`; `collection_handle.rs:177–187`.

`flush()` sleeps for `linger_ms + 10` and returns success. An executor can still be blocked or the queue can contain multiple batches. Executor errors are discarded; the collection executor also logs per-item failures and returns success. The queue is unbounded despite `max_buffer_bytes`, and reaching `max_batch_size` does not wake the task immediately.

**Required correction:** bounded admission, sequence-based flush barriers with acknowledgments, retained error state, and explicit async close. Define whether `put()` means admitted or durable; provide a durable operation for checkpoints. Size/byte thresholds must trigger actual work.

**Acceptance:** block the executor with a notification and prove flush remains pending; release it and observe success. Inject executor failure and require flush to return it. Test several queued batches, byte limits, shutdown, and cancellation. See `probes/core_review.rs`.

### R07 — P1: collection writes downgrade every atomic-operation error

**Code:** `crates/prkdb/src/collection_handle.rs:282–299`, `608–625`; `crates/prkdb-types/src/storage.rs:159–188`.

Any error from `put_with_outbox` or `delete_with_outbox` triggers a non-atomic fallback. Unsupported capability, disk failure, and transaction conflict are indistinguishable. Even an adapter that attempted an atomic operation can have its failure hidden by a second mutation path.

**Required correction:** typed capability negotiation before writes; preserve real storage/transaction errors. Atomic-required callers must never silently fall back. If a best-effort API is retained, name and document its weaker outcome explicitly.

**Acceptance:** inject a real atomic-operation error and assert neither fallback method is invoked. Test supported, unsupported, transient, and uncertain-commit outcomes separately.

### R08 — P1: mixed-partition collection batches route all events to the first partition

**Code:** `crates/prkdb/src/collection_handle.rs:400–414`, `544–558`; `consumer.rs:304–319`; `outbox.rs:65–128`.

A batch chooses its first item's partition for the entire outbox record. Consumers filter records by that partition prefix, so another partition's consumer misses its events while the first receives records it does not own. The put path also builds outbox/handler/broadcast inputs from all original items rather than the successfully serialized subset.

**Required correction:** partition-aware batches preserving per-item results, only committed items emitted, and atomic data/event writes. Define whether cross-partition batches are atomic or explicitly independent.

**Acceptance:** deterministic two-partition batch with consumers assigned separately, covering puts/deletes and partial serialization failures.

### R09 — P1: schema collection names escape the configured storage directory

**Code:** `crates/prkdb-schema/src/storage.rs:240–245`, `295–307`; `registry.rs:38–107`; `crates/prkdb/src/raft/grpc_service.rs:1139–1173`.

The file backend joins the collection string directly into a path. `../../escaped` writes `v1.binpb` outside the registry directory. The gRPC registration route forwards that value without path validation. The route is administrative, so this is not a claim of unauthenticated exploitation; an authorized registration can cross the intended filesystem boundary. First registration also accepts arbitrary descriptor bytes without decoding them.

**Evidence:** `probes/schema_review.rs` reproduced traversal within a disposable temporary directory.

**Required correction:** validated logical names and an encoded filesystem mapping, prohibit traversal/absolute paths, validate descriptors before allocation, and account for symlinks in the storage-root policy.

**Acceptance:** reject traversal, absolute paths, separators, malformed descriptors, and invalid names before any write; valid Unicode names round-trip through the chosen encoding.

### R10 — P1: schema persistence accepts missing data and lacks an atomic commit protocol

**Code:** `crates/prkdb-schema/src/storage.rs:198–222`, `283–285`, `295–319`; `crates/prkdb/src/raft/grpc_service.rs:95–100`.

Metadata deliberately excludes descriptors, but reload accepts a missing descriptor and puts the empty value into cache. A probe confirmed this. Writes replace descriptor/index files directly without a commit protocol; startup logs load errors and continues with a fresh registry. An interrupted write can consequently turn metadata loss into apparently successful startup.

**Required correction:** fail closed on malformed/incomplete committed metadata, provide explicit recovery tooling, and make schema registration atomic. Version allocation, compatibility check, and insertion also need serialization: separate `get_latest`, `next_version`, and `put` calls currently permit concurrent registrations to reuse a version.

**Acceptance:** missing/corrupt descriptors and indexes fail startup with actionable errors; interrupted registration yields old or new complete state; simultaneous registrations never overwrite a version. Do not advertise cluster-wide schema consistency until registration participates in replication.

### R11 — P1: latest remote CI failed its unmutated baseline

**Evidence:** [CI run 34021601202, mutation shard 13](https://github.com/prk-Jr/prkdb/actions/runs/34021601202/job/101455090281), scheduled September 6, 2026, at local HEAD `adc101fb`.

The shard reported `FAILED Unmutated baseline` and `no mutants were tested`. `a_committed_write_replicates_to_every_node` failed at `crates/prkdb/tests/in_process_cluster.rs:87`: a committed value did not reach all nodes within ten seconds. Runs at the same revision passed September 3–5.

**Required investigation:** capture leader/term, commit/applied indexes, peer progress, task failures, and runtime load on timeout; reproduce the baseline under shard concurrency. Logs alone do not distinguish a replication defect from a load-sensitive test. Do not weaken assertions or simply extend timeouts to make CI green.

**Acceptance:** explain and reproduce the cause, add a deterministic regression where possible, then rerun the original baseline and shard. A successful unrelated local test is not evidence this issue is fixed.

### R12 — P1/P2: release and validation can misreport publish readiness

**Code:** `.github/workflows/release.yml:86–88`, `103–104`; root `Cargo.toml:73`; `crates/prkdb-client/Cargo.toml:10`; `scripts/validate_all.sh:59–63`, `138–143`, `157–169`, `188–199`.

Three distinct changes are needed:

1. **P1:** Client packaging fails because the path dependency on `prkdb-proto` has no version requirement. The release loop can publish predecessors before failing on the client. Offline `cargo package -p prkdb-client --offline --allow-dirty --no-verify` exited 101 with that exact error.
2. **P2:** Every release dry-run failure is suppressed as an expected unpublished-dependency issue. This also hides malformed manifests and compilation failures.
3. **P2:** `validate_all.sh` suppresses several failures and prints successful quality/documentation/package/deployment claims anyway. Its `cargo package --dry-run` command is invalid.

**Required correction:** version all publishable internal dependencies, verify every package before publishing any, distinguish expected registry sequencing from real failures, and produce status-derived summaries with a nonzero exit for required failed checks. Tag-signature and environment-approval comments are not proof of configured repository protection; verify those settings separately before release.

**Acceptance:** deliberately broken manifest/build/doc checks fail preflight and final status; no publish job starts. Validate clean packaged consumers independently of workspace path resolution.

## Missing capabilities relevant to the product

| Area | Current gap | Required direction |
| --- | --- | --- |
| Adapter contracts | Optional operations return generic backend errors; batch defaults are loops | Typed capabilities; atomic conditional commit and bounded scans |
| Stable identity | Raw IDs in indexed storage; Rust type names elsewhere | Versioned, explicit tenant/collection/key codec |
| AI memory | No dedicated agent-memory/checkpoint package in the workspace | Independently installable library with sessions, provenance, revisions and retry safety |
| Graph | No graph contract or graph package in the workspace | Typed nodes/edges, transactional adjacency, bounded traversal |
| Retrieval | Index helpers are not a complete semantic retrieval contract | Text, optional embedding providers, filtering, provenance, bounded context |
| Isolation | Namespace prefixes alone are not authorization | Trusted scope binding across records, events, indexes, graph and checkpoints |
| Event replay | Process-local outbox sequence in `outbox.rs:131–144`; namespace absent from event IDs | Durable scoped sequence and explicit replay/retention semantics; restart tests |
| Packaging | Main crate unconditionally pulls server/network/ORM dependencies | AI contracts must not depend on the full `prkdb` crate |
| Confidence reporting | Repo status explicitly says verification is unknown | Track observed checks by commit; never infer correctness from docs consistency |

The event identity issue is a follow-up audit target: persistent outbox records and committed offsets can outlive the process-local sequence. Namespace isolation also needs an end-to-end audit because primary keys include a namespace while outbox IDs and consumer offset keys do not. Do not treat the current namespace builder as a tenant security boundary.

## Verification ledger

- Original checkout and isolated worktree: `cargo test --offline -p prkdb-schema --lib` — 12 tests passed.
- Temporary schema diagnostic target — 2 probes passed, confirming traversal and missing-descriptor defects. Assertions deliberately describe defective behavior; these are not regression acceptance tests.
- Core diagnostic target and final outcomes are recorded in `probes/README.md`.
- CI reviewer read latest six GitHub runs and failed-job logs; no workflow was triggered or rerun.
- `bash scripts/check_ignore_reasons.sh` passed.
- Offline client packaging failed as described in R12.
- No full workspace/chaos matrix, power-loss suite, or performance benchmark was run. `actionlint` was not available.

Production source and workflow fixes are not part of this review artifact. The accompanying specifications define their acceptance gates and staged implementation order.
