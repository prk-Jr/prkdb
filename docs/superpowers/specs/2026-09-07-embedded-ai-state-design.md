# Embedded AI state — M1 specification

Date: 2026-09-07. Status: ready for implementation planning after spec review. Parent: [product architecture](2026-09-07-ai-database-design.md). This specification covers one milestone: durable sessions, event append and resume checkpoints in an embedded SQLite file. It does not implement graph/search/framework integrations.

## Outcome

An application creates a scoped session, atomically appends events and replaces its checkpoint, closes, reopens, and resumes from the exact committed revision. Repeating a request after a lost response does not append duplicates. Two workers cannot silently overwrite each other's checkpoints. Users do not need a PrkDB server or model provider.

## Public contract

Proposed API names, not current compiling examples:

```text
store = SqliteStateStore.open(path, options)
session = store.scoped(trusted_scope).create_session(session_id)
receipt = session.commit(expected_revision=0, idempotency_key="step-1",
                         events=[user_message], checkpoint=resume_state)
snapshot = session.load()
page = session.events(after_sequence=0, limit=100, cursor=None)
store.close()
```

`Scope` contains validated tenant and namespace strings. Empty values are rejected. Session IDs and idempotency keys are bounded opaque UTF-8 strings, not path fragments. Defaults: name/ID max 128 UTF-8 bytes, idempotency key max 128 bytes; payload limits below are checked before transaction entry. The trusted application chooses the scope handle; request payloads cannot override it.

`create_session` returns revision 0 for a newly created session. An existing session returns `AlreadyExists` without changing it; use `open_session` to attach. `load` returns checkpoint, revision and last event sequence from one snapshot; a missing session is `NotFound`. A fresh session has `checkpoint=None`; a committed JSON `null` is `checkpoint=Some(null)`. Both are distinct from a missing session. A snapshot includes revision, last event sequence, checkpoint schema version when present, and usage counters.

`commit` always requires an expected revision and an idempotency key. It atomically appends zero or more events and stores an explicitly supplied JSON checkpoint. Zero events is allowed for a checkpoint-only update. Event-only convenience methods are deferred to keep one mutation contract. Revision increases by exactly one per successful new commit, independently of event count. Each event gets a monotonically increasing sequence within the session starting at 1; sequence 0 is the before-first cursor.

A persisted receipt contains `committed_revision`, `appended_sequence_range` (inclusive first/last, or absent for zero events), and `last_sequence`. A retry returns these exact original fields even after later commits; per-call tracing/timing is not part of receipt equality.

Event fields: event kind (validated string), JSON payload, optional caller correlation ID, assigned sequence, operation revision, timestamp. Checkpoint fields: JSON payload and caller schema-version string. The adapter treats contents as opaque validated data; it never executes instructions or infers a schema migration. Stored format has an independent adapter format version.

## Idempotency and concurrency

The unique operation identity is `(tenant, namespace, session_id, idempotency_key)`. Canonical request bytes include expected revision, ordered events, checkpoint and caller checkpoint schema version. Canonicalization recursively sorts JSON object keys, preserves array order, normalizes serialization through the supported JSON representation and rejects nonfinite/out-of-range numeric inputs. Store canonical bytes for exact equality; a hash may accelerate lookup but is not the sole collision check.

Inside one SQLite transaction:

1. Read an existing idempotency record. Equal canonical request returns its original receipt without consulting the session's now-advanced revision. Different bytes return `IdempotencyConflict`.
2. Read and conditionally update the session at `expected_revision`; if absent, return `NotFound`, otherwise mismatch returns `RevisionConflict` carrying current revision but no checkpoint payload.
3. Allocate consecutive event sequences from session state, append events, replace checkpoint, and persist the receipt and canonical request.
4. Commit. Return success only after SQLite confirms commit under the configured durability mode.

All steps share a single connection and transaction. Validate revision/sequence overflow before writing. Version counters use a documented signed-64-bit-compatible range; exceeding it returns `ResourceExhausted`, never wrapping.

For operations admitted and completed without infrastructure failure, concurrent same-key/same-request calls converge on one receipt. Concurrent different operations using one expected revision yield exactly one success and one revision conflict. Admission/IO failures may instead return Busy, DeadlineExceeded, or a storage error; they do not authorize a second successful CAS. On a busy/deadlocked admission, return bounded `Busy`/`DeadlineExceeded`; do not silently retry indefinitely. Callers may retry the exact request/key.

Cancellation before commit rolls back through connection/transaction cleanup. Cancellation or connection failure near commit can leave an uncertain outcome; return `OutcomeUnknown` when detectable and document that a dropped future provides no outcome. Retrying the same operation resolves committed-vs-not-committed state. This provides idempotent state mutation, not exactly-once external tool execution.

Retain idempotency records for the full session lifetime in M1. No silent time-based eviction that makes old retry keys unsafe. Session deletion/TTL/compaction is out of scope for M1 and must later define retry invalidation explicitly. Enforce configurable per-session event/operation/byte quotas before writes; exceeding a quota returns `ResourceExhausted` with no mutation. Expose usage so the application can provision capacity.

## Storage layout

Use structured columns and bound parameters, never concatenated scope keys in raw SQL or filesystem names.

| Table | Primary/unique identity | Payload |
| --- | --- | --- |
| `ai_meta` | format key | schema version and migration metadata |
| `ai_sessions` | tenant, namespace, session ID | revision, next sequence, checkpoint, checkpoint schema version, timestamps, usage counters |
| `ai_events` | tenant, namespace, session ID, sequence | event kind, payload, correlation ID, operation revision, timestamp |
| `ai_operations` | tenant, namespace, session ID, idempotency key | canonical request bytes and original receipt |

Foreign keys bind events/operations to their session. Index events for scope/session/sequence access. Data and receipt commit together. Store timestamps as integer UTC milliseconds for display/provenance; revisions and sequences are authoritative ordering.

Create migrations transactionally, serialize concurrent open/migration, and reject a newer unknown format. Write the applied format version in the same transaction as the schema changes; both become visible together on commit. Never update the version in a separate postcommit write. An unreadable/corrupt existing database must error rather than be recreated. The application supplies the SQLite filename; the adapter never derives it from a session/tenant ID.

## Durability and resource behavior

Default persistent profile: SQLite WAL journal and `synchronous=FULL`, foreign keys enabled on every connection. Verify configured pragmas and fail opening if required settings cannot be established. Document filesystem/hardware limits of SQLite's durability guarantee; an in-memory test backend explicitly advertises no restart durability.

Default operation deadline: 5 seconds, covering admission, busy waiting, reads/statements and waiting for commit confirmation. Avoid nested retries that multiply the deadline. Every operation accepts an optional tighter deadline. Expiry before commit is attempted returns DeadlineExceeded after rollback/cleanup; once commit has been attempted, an unconfirmed outcome returns OutcomeUnknown rather than claiming rollback. If commit is confirmed, return its receipt even when the deadline has just elapsed. Cleanup can outlive the response deadline and must keep the affected connection unavailable until resolved; document this distinction from a hard wall-clock cancellation guarantee. `close` stops new admission, awaits admitted operations, closes connections and reports failures; synchronous Drop is best effort and is not a durable-close guarantee. No background fire-and-forget state writer in M1.

Initial request limits: checkpoint 1 MiB encoded, each event 64 KiB encoded, at most 100 events and 4 MiB aggregate encoded request per commit. Read pages default 100 events, maximum 1,000 and 4 MiB encoded response. A single stored event cannot exceed a page's maximum byte allowance. Default per-session quotas: 100,000 events, 100,000 distinct operation keys and 256 MiB accounted payload/request bytes; trusted configuration may change quotas. Define accounted bytes as: current checkpoint UTF-8 canonical JSON bytes + retained events' canonical JSON envelope bytes (kind/payload/correlation ID, excluding assigned metadata) + all retained canonical request bytes. Keys, receipt metadata and SQLite/index overhead are excluded, so this is a logical quota, not an on-disk size cap. Replacing a checkpoint subtracts its previous encoded size; canonical operation records and events accumulate. Store each contribution's byte count, update counters in the transaction, and expose event_count, operation_count and accounted_bytes in load/usage results. Identical retries are checked before quota admission, consume no quota twice, and succeed even when quotas are full. A new commit exceeding any postcommit limit rejects without mutation.

## Reads and pagination

`load` is one snapshot of checkpoint/revision/last sequence. Initial `events` captures an upper sequence bound and returns records after the supplied sequence through that bound. Continuation cursors contain format version, bound scope/session, last sequence, and upper sequence. Reject cursors used with a different scope/session or conflicting request filters. No caller-supplied cursor field grants authorization.

In the trusted embedded API, cursor tampering can only select a position within the authorized session; validate all fields and do not accept SQL fragments. Remote cursor signing is a later transport concern. Appends after the initial upper bound do not appear in that traversal. `next_cursor` is absent only when the snapshot is exhausted; byte-limited pages retain continuation. Return explicit snapshot/sequence metadata so callers do not mistake the end of one snapshot for a permanently complete stream.

## Errors and observability

Typed errors: `InvalidArgument`, `NotFound`, `AlreadyExists`, `RevisionConflict`, `IdempotencyConflict`, `UnsupportedCapability`, `Busy`, `DeadlineExceeded`, `ResourceExhausted`, `CorruptData`, `UnsupportedFormat`, `OutcomeUnknown`, `Closed`, and storage I/O errors with sources. No generic success/empty result on corruption or unsupported operations.

Emit optional operation spans with operation name, duration, outcome and retry/conflict counts. Do not log checkpoint/event content, credentials, canonical request bytes or sensitive scope strings by default. No mandatory metrics HTTP server. Provide lightweight counters/hooks for the host application.

## Conformance and acceptance tests

1. Fresh file, create/open session, absent checkpoint vs committed JSON null, checkpoint-only receipt with absent range, append receipt with exact range, close/reopen and exact state recovery.
2. Same IDs and retry keys in different scopes/sessions remain independent across reads, events and receipts.
3. Concurrent writers using the same expected revision with adequate admission budget and no injected infrastructure failure: exactly one commit and one conflict; no lost update or partial event append. Separate contention tests allow typed admission failures but never two CAS successes.
4. Same-key identical retries before/after reopen return identical receipts; changed payload/order/revision conflicts.
5. Deterministic failure at each transaction step leaves either the prior state or complete new state, including receipt. Child-process termination around commit tests recovery without relying solely on mocked exceptions.
6. Cancellation and busy contention follow whole-operation deadline/cleanup semantics; unconfirmed commit returns OutcomeUnknown and resolves through the same-key retry contract.
7. Paged reads remain bounded and ordered while another task appends; cursor scope/version validation and byte-limit continuation work.
8. Invalid/oversized input, exact quota boundaries and numeric overflow reject before mutation; replacing checkpoints updates byte usage exactly, and identical retries succeed at full quota; corrupt/newer files fail opening.
9. Migration failure, termination at version/schema commit boundaries and concurrent open cannot expose schema/version disagreement. Close waits for admitted work and rejects new requests.
10. A clean external consumer builds with default and no-default features without `prkdb`, Raft, tonic, axum or a model SDK; examples execute without network access.

Use deterministic barriers/fault hooks for interleavings, not sleeps as completion proof. Keep diagnostic probes from the review separate: acceptance tests assert desired guarantees and must fail against an intentionally broken backend. Execute the same logical conformance suite on any future native/remote implementation, with explicit transport/durability differences reported.

## Delivery boundary

M1 deliverables are the contracts, SQLite implementation, migrations, conformance suite, dependency/package checks, and runnable restart example. Graph, vector search, LLM providers, MCP, Python packaging and full database hardening are separate milestones in the parent architecture. No existing storage format is rewritten by this new library.
