# PrkDB database and AI product architecture

Date: 2026-09-07. Status: proposed architecture, decisions delegated by the user; implementation remains staged. Companion: [implementation review](../../reviews/2026-09-07-senior-review.md).

## Product decision

Build PrkDB as a complete database with an independently usable AI state and retrieval layer. Users can start with an embedded file, use the same logical API against a server later, and adopt graph/retrieval without managing Raft on day one. “Complete” means a coherent, documented set of guarantees across storage, query, recovery, access control, and operations; it does not mean shipping every database feature in the first release.

The first supported AI storage profile is SQLite. This is an architectural choice to establish a testable transaction contract using an existing embedded engine while repairing the native engine. It is not an endorsement of the current generic SQLite adapter as sufficient for checkpoints: that adapter lacks the new conditional transaction contract.

Three approaches considered:

| Approach | Benefit | Cost | Decision |
| --- | --- | --- | --- |
| Put AI and graph directly in `prkdb` | Immediate access to current facilities | Forces embedded users to compile server/ORM/network stack; inherits inconsistent contracts | Reject as package boundary |
| Add only an MCP wrapper to current CRUD | Quick assistant access | Still requires server semantics; no reliable memory/checkpoint/retrieval contract | Optional interface later |
| Independent domain contracts, embedded backend, optional server/framework/MCP adapters | Small adoption path; shared conformance tests; preserves full database direction | Requires explicit capability and transaction work | Adopt |

## Intended users and first useful outcomes

- An agent application persists session state, resumes after restart, and retries a checkpoint without duplicating events.
- A knowledge application stores source-backed memories, retrieves bounded relevant context, and follows relationships between entities.
- A database user operates the native engine/server with explicit consistency, backup, recovery, and access-control contracts.

First public demo: a local application writes two session messages and a checkpoint, terminates, reopens the file, and resumes with the same revision and no duplicated write. It requires no PrkDB daemon, model key, Docker, or embedding service. A second demonstration later adds search and graph relationships. AI integration is useful even without automatic inference.

## Package boundaries

Names below are proposed new packages, not existing install commands.

| Package | Responsibility | Allowed dependencies |
| --- | --- | --- |
| `prkdb-ai-types` | Scope, IDs, errors, revisions, request/response and capability contracts | Serialization and minimal domain utilities; no database/network/runtime implementation |
| `prkdb-ai` | Session/checkpoint/memory operations over explicit ports | AI types, async/runtime utilities; no `prkdb`, tonic, ORM, Raft or model SDK |
| `prkdb-ai-sqlite` | Embedded transactions, migrations, bounded queries | AI contracts + SQLx SQLite only |
| `prkdb-ai-native` | Optional translation to corrected native engine | AI contracts + `prkdb`; advertised only after conformance |
| `prkdb-ai-client` | Optional remote implementation of the same contracts | AI contracts + network protocol/client |
| `prkdb-graph` | Graph semantics and query limits | Atomic domain backend contract; no dependency on LLM inference |
| `prkdb-ai-python` | Thin Python bindings, published wheels | One Rust implementation of semantics; no independent Python transaction logic |
| `prkdb-ai-mcp` | Optional local tool transport | Public AI API; no raw storage bypass |

Do not extend `StorageAdapter` with dozens of AI methods. Keep byte storage separate from memory, graph, and retrieval semantics. Use explicit optional ports for graph, semantic search and replay. Absence yields `UnsupportedCapability` before side effects, not an empty successful response.

The existing `prkdb-types` can remain the low-level byte-storage boundary. Extract shared concepts only when both sides need them; do not make the standalone AI path depend on `prkdb-core` just to obtain a small type. The current core crate includes WAL/compression/runtime dependencies.

## Data model and consistency

Every operation binds to a trusted scope: tenant, namespace and optional session. End users cannot override the authenticated tenant through a tool argument. Embedded callers are trusted application code; a file/namespace is not an OS security boundary.

Use explicit stable IDs, UTF-8 logical names, versioned encodings and structured scope columns. Never concatenate arbitrary names into filesystem paths or derive persisted identity from Rust type names. Store created/updated/expiry timestamps with a documented clock policy. CAS revisions and durable event sequences determine ordering, not timestamps.

Memory documents hold content, metadata, provenance and revision. Session events are ordered records, while checkpoints are versioned resume state. Node and edge properties reference the same stable identity model. Embeddings and text indexes are derived data tied to source revisions; a stale index must never return deleted, expired, unauthorized or superseded content.

Atomicity, durable acknowledgment, snapshot reads and conditional writes are independent capabilities with explicit tests. A caller requiring a capability must fail before mutation if unsupported. Local single-file transactions and distributed multi-partition transactions are different profiles; the server must not pretend a per-partition batch is globally atomic.

The SQLite baseline uses transactions for compound domain changes. `BEGIN IMMEDIATE` is one candidate for acquiring write admission before validating state; contention can return `SQLITE_BUSY` and must be bounded and typed. This follows SQLite's documented transaction behavior, not a guarantee of lock-free concurrency. [SQLite transaction reference](https://www.sqlite.org/lang_transaction.html).

## Graph support decision

Ship a property graph after the durable state milestone. Graph is a first-class capability, not an LLM-generated blob or an obligation to implement Cypher/Gremlin immediately.

Initial operations: create/update/delete node; create/update/delete directed edge; get node; neighbors; bounded breadth-first traversal. Nodes have `(scope, node_id, labels, properties, revision)`. Edges have `(scope, edge_id, source_id, target_id, relation, properties, revision)`. An edge's endpoints must exist in the same scope. Multiple edges and self-edges are permitted through distinct edge IDs; idempotency is explicit. Edge IDs are unique within scope.

Maintain outgoing and incoming indexes atomically with edge records. Deleting a node defaults to `Conflict` when incident edges exist; an explicit cascade operation deletes the node and its edges atomically within the backend's documented transaction limits. Oversized cascades fail before mutation; bulk lifecycle jobs are a separate future contract.

Traversal must specify direction, allowed relations, max depth, max visited nodes, max edges and deadline. Initial defaults: depth 2, 1,000 nodes, 5,000 edges, 1 second; hard maxima configurable only by trusted application configuration. Deduplicate visited nodes to terminate cycles. Return paths/source references and an explicit truncation reason. Stable neighbor ordering is `(relation, target/source node ID, edge ID)`; pagination cursors bind scope, filters and snapshot revision.

Initial SQLite implementation uses indexed node/edge tables and bounded traversal. Benchmark it before selecting a specialized graph engine. A pluggable graph backend is supported through capabilities later; automatic distributed graph traversal, graph query languages, path optimization and graph analytics are deferred.

Extracting entities/edges with a model is optional application logic. Store inferred relationships with source references and extractor/model version; do not silently elevate generated links to verified facts. Users can add relationships manually without an LLM.

**Graph release gates:** isolated identical IDs across tenants; no dangling edges after failed writes/restart; CAS conflicts; adjacency/index consistency; cycle termination; deterministic limits/cursors; explicit cascade behavior; source deletion handling; conformance across advertised graph backends.

## Retrieval and AI interface decision

Deliver exact lookup and filtered listing first, then lexical retrieval, then semantic retrieval. Embedding generation is an optional provider invoked explicitly; ordinary writes must not send content to a remote service. Persist provider/model/version/dimension/metric with each embedding. Reject dimension or model-space mismatches and nonfinite values.

Start vector correctness with bounded exact search for a documented dataset size. Add an ANN implementation only after recall/latency/memory evidence warrants it. Score semantics and embedding versions are part of the public contract. Do not advertise existing text-index helpers as semantic search.

Hybrid retrieval combines lexical and vector candidates, applies trusted scope and metadata constraints, then optionally expands graph neighbors under traversal limits. Revalidate source revision, expiry and authorization before emitting results. Return source IDs, revisions, scores with score-kind labels, and provenance. A context builder enforces byte/item limits; exact model token budgeting requires an explicit tokenizer, otherwise label counts as estimates.

The first language is Rust because the codebase is Rust. Python bindings follow the stable embedded contract so common AI applications can use it without a daemon. TypeScript initially uses the remote client; a native JS binding is not an MVP dependency. Choose one framework integration only after the core contract and packaging work; do not build three wrappers before there is a reliable shared API.

MCP is an optional transport after the library: expose `memory_get`, `memory_search`, `memory_put` and bounded graph queries through the same validated API. Start with local stdio and a host-configured scope. Keep writes disabled unless enabled by trusted configuration. Do not expose arbitrary SQL, filesystem paths, admin tokens, or unrestricted tenant selection. MCP tool annotations are not authorization. Protocol version and SDK compatibility must be pinned and verified when implemented. [Official MCP tools specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/main/docs/specification/2026-07-28/server/tools.mdx).

## Native database hardening

Before exposing the native backend through the AI contract:

1. Repair the review's primary key, transaction, outbox, flush and schema defects with migration plans and fault tests.
2. Establish one key codec, capability registry, error taxonomy and shared backend conformance suite.
3. Verify WAL recovery at mixed transaction boundaries, index rebuilds, backup restore, expiry and retention under restart.
4. Explain the failed replication baseline, then validate elections, partitions, recovery and advertised read modes. Do not turn a test timeout increase into a consistency claim.
5. Make per-partition limits explicit; route graph/checkpoint operations to one atomic domain or reject unsupported distributed transactions.
6. Derive readiness from active engine state, and publish tested support levels per API/backend, not a single blanket ACID label.

Existing data migrations require inventory/export, dry-run collision detection, backup and recovery verification, then a versioned codec migration. No implicit rewrite of old data when opening the database.

## Delivery milestones

| Milestone | Deliverable | Exit criteria |
| --- | --- | --- |
| M0 — trustworthy baseline | Review findings fixed in bounded PRs; accurate CI/package status | Reproductions converted into failing-before/passing-after regressions; latest baseline failure explained |
| M1 — embedded AI state | [Detailed checkpoint/session spec](2026-09-07-embedded-ai-state-design.md) implemented on SQLite | Restart/CAS/idempotency/isolation tests; installable minimal crate; runnable local example |
| M2 — usable memory retrieval | Documents, provenance, TTL, lexical search, Python bindings | Real installed wheel example; deletion/expiry/index correctness; bounded queries |
| M3 — graph | Nodes/edges, atomic adjacency, bounded traversal | Graph conformance and recovery tests; source-backed relationship example |
| M4 — semantic retrieval | Optional embedding provider, vector/hybrid search, bounded context | Deterministic retrieval fixtures, measured recall and resource limits; no implicit network egress |
| M5 — integrations and server parity | One framework, local MCP, native/remote adapters | Same core conformance suites; correct auth scope and retry semantics; documented capability differences |
| M6 — production database release | Recovery/operations, migration, reliability evidence, release packaging | Reproducible clean install; restore rehearsal; CI/chaos gates; no unresolved P1 affecting advertised guarantees |

M0 work that blocks native correctness may proceed independently of M1's SQLite domain backend, but neither is a reason to advertise the native engine as already fixed. Each milestone has a separate implementation plan and review. This document is the product roadmap, not one giant implementation task.

## CI and measurement requirements

Use fast PR gates for fmt/lint/unit/contract/package checks and deterministic fault tests. Keep slower restart/chaos/platform/packaging/benchmark jobs, with commit-linked artifacts and honest status. Validate SQLite and native separately; capability skips must be visible. Mutation jobs must distinguish baseline failure from surviving mutations.

Add a dependency-graph assertion proving the embedded AI consumer does not pull in `prkdb`, `prkdb-proto`, tonic, axum, or Raft. Test a clean consumer outside this workspace, so local path dependencies and feature unification cannot hide packaging defects. Record compile time, artifact size, throughput and p50/p95/p99 on declared hardware/dataset/durability settings. No absolute performance promises until measured.

Release preflight must preserve errors, check versioned internal dependencies and packaged examples, and run before irreversible publishing. Verify actual protected-environment and tag settings rather than relying on workflow comments.

## Deferred scope

No automatic multi-partition ACID, full SQL/graph query language, model hosting, autonomous database tuning, or mandatory vector service in the first AI release. These can become separate proposals after the foundational guarantees and user adoption are demonstrated.
