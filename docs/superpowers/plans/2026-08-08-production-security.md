# Production Security & Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the two critical security holes (unauthorized data planes on **both** HTTP and gRPC, unreachable TLS) and add the production primitives PrkDB is missing: on-disk format versioning, backup/restore, readiness signalling, and release engineering.

**Architecture:** Build the authorization model first, then apply it to every surface. PrkDB exposes data through three unprotected surfaces today — the axum router in `prkdb-cli serve`, **eight** gRPC RPCs including `fetch_segment` (which streams raw WAL), and a second metrics server inside `prkdb-server`. Task 0 builds the principal/role/grant model; Tasks 1 and 2 apply it, separated only because the mechanisms differ (axum middleware vs. tonic interceptor). Then on-disk format versioning, the cheapest thing to add now and the most expensive to retrofit later. Then the operational primitives that are already half-built and only need wiring.

**Tech Stack:** Rust 2021, axum 0.7.9, tonic 0.12, subtle, tokio, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-08-08-correctness-and-production-readiness.md` (revision 6)

**Execution order** — not task number order. D7 makes TLS a prerequisite of peer authentication:

```
Task 0 (model)  ->  Task 3 (TLS)  ->  Task 1 (HTTP)  ->  Task 2 (gRPC)  ->  4, 5, 6, 7
```

**Depends on:** Plan A Tasks 1-2 (CI safety net, green clippy) for trustworthy feedback, and Plan
A Task 4b (in-process cluster harness) for Task 2's cluster tests. Tasks 1-3 all edit server
wiring — land Plan A Task 8 first to avoid conflicts in `rpc_client.rs`.

**Decisions already made** (spec §0, resolved 2026-08-08):
- **D2 — users and per-collection roles**, not a single API token. Tasks 1 and 2 are no longer a
  2-day token gate; they implement an authorization model. ~6 days. See spec R12 for the model.
- **D7 — mTLS client certificates** for Raft peer identity. **Task 3 (TLS) must land before the
  `RaftService` half of Task 2** — the peer check depends on the client-CA plumbing.
- **D4 — `Health` public, `Metadata` requires `Read`.**
- **D3 — no interim token gate.** The data planes stay open until the role model ships. Accepted
  because PrkDB is not deployed anywhere reachable; revisit the moment that changes.

> **Revision history.**
>
> **Rev 1** covered only the HTTP surface, on the mistaken belief that gRPC was already guarded.
>
> **Rev 2** added Task 2 after finding `put`, `get`, `delete`, `batch_put`, `watch`, and
> `fetch_segment` unauthenticated.
>
> **Rev 3** — an independent review made the RPC arithmetic balance (25 declared in
> `PrkDbService`, 15 admin-gated) and found two more: `get_schema` (1129) and
> `check_compatibility` (1222), whose siblings `register_schema` and `list_schemas` *are* gated.
> Eight RPCs need the interceptor, not six. Rev 3 also corrected the axum 0.6 middleware
> signatures, the crates.io publish order, and helper APIs that did not exist.
>
> **Rev 4** — applied the same reconciliation to the HTTP surface: `serve.rs` declares **10**
> routes, and Task 1 had a policy for four. Also found that gating `/ws/collections/:name` is a
> **breaking change** for existing WebSocket clients, not merely a new gate.
>
> **Rev 5** — the §0 decisions landed. D2 replaces the single API token with users and
> per-collection roles, which reshapes Tasks 1 and 2 from a gate into an authorization model and
> triples their effort. D7 makes Task 3 (TLS) a prerequisite of Task 2 rather than a follow-on.

---

## Task 0: Build the authorization model (R12, D2)

> **New in revision 5, and a prerequisite for Tasks 1 and 2.** D2 chose users and per-collection
> roles over a single API token, so there is a model to build before there is anything for the
> middleware and interceptor to consult. Spec R12 defines it.

**Files:**
- Create: `crates/prkdb/src/authz/mod.rs`, `authz/model.rs`, `authz/store.rs`
- Modify: `crates/prkdb/src/raft/state_machine.rs`
- Test: `crates/prkdb/tests/authz_model.rs`

- [x] **Step 0: Add `rstest` — it is not a dependency yet**

```bash
grep -rn 'rstest' Cargo.toml crates/*/Cargo.toml || echo "confirmed absent"
```

The table below uses `#[rstest]`, which the workspace does not currently have. `proptest 1.4` is
present (`crates/prkdb/Cargo.toml:79`) but is the wrong tool here — this is a fixed, enumerable
input space, not a property to sample. Add to `[workspace.dependencies]`:

```toml
rstest = "0.26"
```

and `rstest = { workspace = true }` to `crates/prkdb`'s `[dev-dependencies]`.

> Version checked against the registry on 2026-08-08: latest is **0.26.1**. An earlier draft of
> this plan said `0.23`, which was three minor releases stale.

- [x] **Step 1: Write the permission table test first**

Authorization is decision logic over a small input space, so an exhaustive table is cheaper than
reasoning about cases — and it is the artifact you will reread in a year:

```rust
//! Permission decisions, exhaustively. Regression guard for spec R12 acceptance 1-3.

#[rstest]
//        grants                              collection  action   expected
#[case(&[("users", Read)],                    "users",    Read,    true )]
#[case(&[("users", Read)],                    "users",    Write,   false)]
#[case(&[("users", Read)],                    "orders",   Read,    false)]
#[case(&[("logs/*", Write)],                  "logs/app", Write,   true )]
#[case(&[("logs/*", Write)],                  "users",    Read,    false)]
#[case(&[("*", Admin)],                       "anything", Write,   true )]
#[case(&[],                                   "users",    Read,    false)]
fn permits(
    #[case] grants: &[(&str, Permission)],
    #[case] collection: &str,
    #[case] action: Permission,
    #[case] expected: bool,
) {
    let p = Principal::with_grants(grants);
    assert_eq!(p.permits(collection, action), expected);
}
```

- [x] **Step 2: Run and watch it fail**

```bash
cargo test -p prkdb --test authz_model -- --nocapture
```

- [x] **Step 3: Implement `Principal`, `Role`, `Grant`, `Permission`**

Keep it to what spec R12 defines. Explicitly **not** in scope: groups, permission inheritance,
row/field rules, external identity providers, token expiry.

- [ ] **Step 4: Put the store in the Raft state machine**

The principal store must survive restart and agree across the cluster, so it belongs in the
state machine — not a config file. Two failure modes to test explicitly:

```rust
/// An authorization store that does not survive install_snapshot is a cluster that loses
/// its own credentials during recovery. This is spec section 10, implementation question 1.
#[tokio::test]
async fn principals_survive_snapshot_and_restore() { /* ... */ }

/// A cold cluster has no principals and can authenticate nobody, including the operator.
#[tokio::test]
async fn bootstrap_creates_one_admin_then_refuses() { /* ... */ }
```

`PRKDB_BOOTSTRAP_TOKEN` creates a single admin principal on first start and is refused once any
principal exists.

- [x] **Step 5: Cache resolved grants in memory**

Authorization is on the hot path. Resolve principal-to-grants into an in-memory map, invalidated
by the Raft apply that changes it. Do not read through to storage per request.

- [x] **Step 6: Verify**

```bash
cargo test -p prkdb --test authz_model -- --nocapture
```
Expected: the whole table passes, plus snapshot and bootstrap tests.

- [ ] **Step 7: Commit**

```bash
git add crates/prkdb/src/authz/ crates/prkdb/src/raft/state_machine.rs crates/prkdb/tests/authz_model.rs
git commit -m "feat: add principal, role, and grant authorization model"
```

---

## Task 1: Authorize the HTTP data plane (R12)

> **Severity: critical.** `crates/prkdb-cli/src/commands/serve.rs:226-249` builds the router with
> no auth layer. `PUT /collections/:name/data` and `DELETE /collections/:name/data/:id` are open
> to anyone who can reach the port. This is the API the generated Python, TypeScript, and Go
> clients target.
>
> **Depends on Task 0** (the `Principal` / `Role` / `Grant` model) and **Task 3** (TLS — credentials
> must not cross the wire in plaintext).

**Files:**
- Create: `crates/prkdb-cli/src/authz_layer.rs`
- Modify: `crates/prkdb-cli/src/commands/serve.rs:226-270`
- Modify: `crates/prkdb-cli/src/main.rs`
- Modify: `crates/prkdb/src/bin/prkdb-server.rs:87-114`
- Test: `crates/prkdb-cli/tests/http_authz.rs`

- [ ] **Step 1: Write the failing tests**

Five cases, and **the third is the one that matters** — authentication without authorization is
the bug this task exists to prevent, and it is the case a token-only design cannot express.

```rust
//! HTTP authorization. Regression guard for S-01 and spec R12.

mod common;

#[tokio::test]
async fn rejects_write_with_no_credential() {
    let srv = common::spawn_authorized_server().await;
    let resp = srv.put_data("users", json!({"id":"1"})).send().await.unwrap();
    assert_eq!(resp.status(), 401, "no credential must be 401");
}

#[tokio::test]
async fn rejects_write_with_unknown_credential() {
    let srv = common::spawn_authorized_server().await;
    let resp = srv.put_data("users", json!({"id":"1"}))
        .bearer_auth("not-a-real-credential").send().await.unwrap();
    assert_eq!(resp.status(), 401, "unknown credential must be 401");
}

/// A valid principal without the grant must be 403, not 401. 401 says "who are you";
/// 403 says "I know who you are and you may not do this". Conflating them is how
/// authorization bugs hide.
#[tokio::test]
async fn rejects_write_from_read_only_principal() {
    let srv = common::spawn_authorized_server().await;
    let reader = srv.principal_with(&[("users", Permission::Read)]).await;

    let resp = srv.put_data("users", json!({"id":"1"}))
        .bearer_auth(&reader.credential).send().await.unwrap();
    assert_eq!(resp.status(), 403, "valid principal lacking Write must be 403");

    let resp = srv.get_data("users").bearer_auth(&reader.credential).send().await.unwrap();
    assert!(resp.status().is_success(), "the same principal may still read");
}

#[tokio::test]
async fn scopes_grants_per_collection() {
    let srv = common::spawn_authorized_server().await;
    let p = srv.principal_with(&[("logs/*", Permission::Write)]).await;

    assert!(srv.put_data("logs/app", json!({"id":"1"}))
        .bearer_auth(&p.credential).send().await.unwrap().status().is_success());
    assert_eq!(srv.put_data("users", json!({"id":"1"}))
        .bearer_auth(&p.credential).send().await.unwrap().status(), 403);
}

#[tokio::test]
async fn probes_are_reachable_without_a_credential() {
    let srv = common::spawn_authorized_server().await;
    for path in ["/health", "/livez", "/readyz", "/"] {
        let resp = reqwest::get(format!("{}{path}", srv.base_url)).await.unwrap();
        assert!(resp.status().is_success(), "{path} must stay open (D4)");
    }
}
```

- [ ] **Step 2: Run and watch them fail**

```bash
cargo test -p prkdb-cli --test http_authz
```
Expected: FAIL — every request succeeds today, because nothing checks anything.

- [ ] **Step 3: Add `subtle` for constant-time comparison**

In `Cargo.toml` under `[workspace.dependencies]`:

```toml
subtle = "2.6"
```

Then add `subtle = { workspace = true }` to `crates/prkdb-cli/Cargo.toml` and
`crates/prkdb/Cargo.toml`.

- [ ] **Step 4: Map every route to a required permission**

`serve.rs:226-260` declares **10** routes. All ten need a bucket — the four named in the severity
note are not the whole surface:

| Route | Required |
|---|---|
| `GET /collections` | `Read` on any collection — **filter the response** to what the caller may see |
| `GET /collections/:name` | `Read` on `:name` |
| `GET /collections/:name/data` | `Read` on `:name` |
| `PUT /collections/:name/data` | `Write` on `:name` |
| `GET /collections/:name/data/:id` | `Read` on `:name` |
| `DELETE /collections/:name/data/:id` | `Write` on `:name` |
| `GET /collections/:name/count` | `Read` on `:name` |
| `GET /collections/:name/schema` | `Read` on `:name` |
| `GET /ws/collections/:name` | `Read` on `:name` — **breaking, see Step 6** |
| `GET /metrics` | `Admin`, or a separate interface |
| `GET /health` | public (D4) |
| `GET /` | public — service-info root, discloses nothing |

```bash
grep -c '\.route(' crates/prkdb-cli/src/commands/serve.rs
```
Expected: `10`. Higher means a route was added since this plan was written — bucket it first.

> `GET /collections` is the subtle one. Denying it outright to a principal with narrow grants is
> wrong; so is listing every collection. Filter the response to collections the caller holds
> `Read` on, and return `200 []` rather than `403` when that set is empty — otherwise the status
> code itself discloses whether collections exist.

- [ ] **Step 5: Write the authorization layer**

> **axum version matters.** This repo uses axum **0.7.9** (`Cargo.toml:44`). In 0.7 the `Next<B>`
> / `Request<B>` generics of 0.6 were removed — `Next` and `Request` are concrete. Code copied
> from an axum 0.6 example will not compile.

```rust
use axum::{
    extract::{Request, State},
    http::{Method, StatusCode},
    middleware::Next,
    response::Response,
};
use prkdb::authz::{Permission, PrincipalStore};

/// Reachable without credentials (D4). Orchestrators probe these before any client could
/// hold a credential, so requiring one here breaks deployment.
const PUBLIC_PATHS: &[&str] = &["/", "/health", "/livez", "/readyz"];

#[derive(Clone)]
pub struct Authz {
    store: Option<PrincipalStore>,   // None == --allow-anonymous
}

/// Map an HTTP method to the permission it needs. Anything not read-only is a write;
/// defaulting the other way is how a new route silently ships unprotected.
fn required_permission(method: &Method) -> Permission {
    match *method {
        Method::GET | Method::HEAD => Permission::Read,
        _ => Permission::Write,
    }
}

pub async fn authorize(
    State(authz): State<Authz>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let Some(store) = &authz.store else {
        return Ok(next.run(req).await);
    };
    if PUBLIC_PATHS.contains(&req.uri().path()) {
        return Ok(next.run(req).await);
    }

    let credential = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(StatusCode::UNAUTHORIZED)?;

    // Constant-time lookup: a byte-wise `!=` leaks the credential prefix through timing.
    let principal = store
        .resolve(credential)
        .ok_or(StatusCode::UNAUTHORIZED)?;      // 401: who are you

    match collection_from_path(req.uri().path()) {
        Some(c) if !principal.permits(&c, required_permission(req.method())) => {
            Err(StatusCode::FORBIDDEN)          // 403: known, but not allowed
        }
        _ => Ok(next.run(req).await),
    }
}
```

Wire it in `serve.rs` after `.with_state(state)` and **before** the CORS layer:

```rust
let app = app.layer(axum::middleware::from_fn_with_state(
    authz.clone(),
    crate::authz_layer::authorize,
));
```

- [ ] **Step 6: Handle the WebSocket break deliberately**

`/ws/collections/:name` checks its own optional token *after* the upgrade
(`serve.rs:954-956`). A middleware layer gates the **upgrade request**, so existing WS clients
passing a token by query parameter start receiving 401s.

Decide one, and write it down:
- accept the existing query parameter in `authorize` as well as `Authorization`, or
- require the header and note the break.

Either way it goes in `CHANGELOG.md`. Silently breaking a working client is worse than the gap
being closed.

- [ ] **Step 7: Refuse to start unprotected**

```rust
let authz = match (store.principal_count().await, args.allow_anonymous) {
    (0, false) => anyhow::bail!(
        "No principals are configured. Set PRKDB_BOOTSTRAP_TOKEN to create an admin \
         principal, or pass --allow-anonymous to serve without authorization (development only)."
    ),
    (0, true) => {
        eprintln!(
            "WARNING: serving with --allow-anonymous. Every collection is readable and \
             writable by anyone who can reach this port."
        );
        Authz { store: None }
    }
    (_, _) => Authz { store: Some(store) },
};
```

Add `#[arg(long)] pub allow_anonymous: bool` to the serve args.

- [ ] **Step 8: Close the second metrics server**

`prkdb-cli serve --prometheus` is not the only HTTP surface.
`crates/prkdb/src/bin/prkdb-server.rs:87-114` binds its **own** axum server on port
`9090 + node_id` with a bare `/metrics` route and no auth:

```bash
grep -n 'metrics_addr\|Router::new' crates/prkdb/src/bin/prkdb-server.rs
```

Apply the same layer requiring `Admin`, or bind it to `127.0.0.1` only and document that
operators scrape through a sidecar. Either is defensible; exposing it on `0.0.0.0` is not.

- [ ] **Step 9: Run the tests**

```bash
cargo test -p prkdb-cli --test http_authz
```
Expected: all five PASS, including the 403 case.

- [ ] **Step 10: Update the generated clients**

The codegen templates in `crates/prkdb-cli/src/commands/codegen.rs` must emit clients that send
`Authorization: Bearer` and surface 403 distinctly from 401 — a caller needs to know whether to
re-authenticate or request a grant. Update all three languages, then:

```bash
./scripts/test_mixed_client_integration.sh
```

- [ ] **Step 11: Commit**

```bash
git add crates/prkdb-cli/ crates/prkdb/src/bin/prkdb-server.rs Cargo.toml
git commit -m "feat: authorize the HTTP data plane with per-collection permissions"
```

---

## Task 2: Authorize the gRPC data plane (R12)

> **Severity: critical, and larger than Task 1.** `PrkDbService` declares 25 RPCs. Fifteen call
> `validate_admin_token`. These eight do not:
>
> `put` (238), `get` (260), `delete` (288), `batch_put` (379), `watch` (924),
> `fetch_segment` (959), `get_schema` (1129), `check_compatibility` (1222).
>
> Plus `health` (303) and `metadata` (322), settled by D4.
>
> `fetch_segment` streams **raw WAL segments** — a complete data-exfiltration primitive requiring
> no credential. `get_schema` and `check_compatibility` are plain oversight: their siblings
> `register_schema` (1080) and `list_schemas` (1181) *are* gated.
>
> Fixing only Task 1 ships a "security hardening" release with all of this wide open.
>
> **Depends on Task 0** (the model) and **Task 3** (TLS — D7 authenticates peers by client
> certificate, so the `--tls-client-ca` plumbing must exist first).

**Files:**
- Create: `crates/prkdb/src/raft/authz_interceptor.rs`, `raft/peer_auth.rs`
- Modify: `crates/prkdb/src/raft/grpc_service.rs`
- Modify: `crates/prkdb/src/bin/prkdb-server.rs`
- Modify: `crates/prkdb-cli/src/commands/serve.rs`
- Modify: `crates/prkdb-client/src/client.rs`
- Test: `crates/prkdb/tests/grpc_authz.rs`

- [ ] **Step 1: Confirm the gap**

Do not check only the RPCs you already suspect — enumerate every handler. That is how
`get_schema` and `check_compatibility` were found:

```bash
for fn in $(grep -oE '^    async fn [a-z_]+' crates/prkdb/src/raft/grpc_service.rs | awk '{print $3}'); do
  ln=$(grep -n "    async fn $fn(" crates/prkdb/src/raft/grpc_service.rs | head -1 | cut -d: -f1)
  n=$(sed -n "${ln},$((ln+30))p" crates/prkdb/src/raft/grpc_service.rs | grep -c 'validate_admin_token')
  [ "$n" -eq 0 ] && echo "UNGATED: $fn (line $ln)"
done
```

Expected output — 10 real RPCs plus one false positive:

```
UNGATED: follower_reads_do_not_fallback_to_leader_reads (line 213)  <- a test fn, not an RPC
UNGATED: put (238)             UNGATED: watch (924)
UNGATED: get (260)             UNGATED: fetch_segment (959)
UNGATED: delete (288)          UNGATED: get_schema (1129)
UNGATED: health (303)          UNGATED: check_compatibility (1222)
UNGATED: metadata (322)
UNGATED: batch_put (379)
```

- [ ] **Step 2: Understand why a per-message fix is wrong**

```bash
sed -n '182,200p' crates/prkdb-proto/proto/raft.proto
```

`PutRequest` is `{ bytes key = 1; bytes value = 2; }` — no token field. Admin RPCs carry
`admin_token` *in the message*; data RPCs do not. Adding a credential field to every data message
would bloat the wire format, still leave `fetch_segment` streaming, and give three languages of
generated client a field to forget. The fix is a transport-level interceptor reading gRPC
**metadata**, which also matches the `Authorization` header Task 1 uses.

- [ ] **Step 3: Build the test helpers first — they do not exist**

`spawn_authorized_grpc_server`, `raw_client`, `client_as`, and `principal_with` are **all new**.
Write them in `tests/helpers/` before the test file, modelled on the in-process server setup in
`admin_rpc_tests.rs:20-35` (which already binds `127.0.0.1:0` correctly).

The cluster test in Step 4 uses `InProcessCluster` from **Plan A Task 4b**. If that has not
landed, do this task's single-node half first and the cluster half after.

- [ ] **Step 4: Write the failing tests**

```rust
//! gRPC authorization. Regression guard for S-01 and spec R12 acceptance 11-16.

mod helpers;

#[tokio::test]
async fn rejects_put_without_metadata_credential() {
    let srv = helpers::spawn_authorized_grpc_server().await;
    let status = srv.raw_client().await
        .put(tonic::Request::new(PutRequest { key: b"k".into(), value: b"v".into() }))
        .await
        .expect_err("unauthenticated put must be rejected");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
}

/// A valid principal lacking the grant must be PERMISSION_DENIED, not UNAUTHENTICATED.
/// This is the case a single shared token cannot express, and the reason for D2.
#[tokio::test]
async fn rejects_put_from_read_only_principal() {
    let srv = helpers::spawn_authorized_grpc_server().await;
    let reader = srv.principal_with(&[("users", Permission::Read)]).await;

    let status = srv.client_as(&reader).await
        .put(tonic::Request::new(PutRequest { key: b"users/1".into(), value: b"v".into() }))
        .await
        .expect_err("Read-only principal must not write");
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
}

/// The most important case in this file. fetch_segment streams raw WAL across every
/// collection, so a per-collection Read grant is NOT sufficient authority for it.
#[tokio::test]
async fn fetch_segment_requires_admin_not_read() {
    let srv = helpers::spawn_authorized_grpc_server().await;
    let reader = srv.principal_with(&[("*", Permission::Read)]).await;

    let status = srv.client_as(&reader).await
        .fetch_segment(tonic::Request::new(FetchSegmentRequest::default()))
        .await
        .expect_err("Read on * must not be enough to stream raw WAL");
    assert_eq!(status.code(), tonic::Code::PermissionDenied);

    let admin = srv.principal_with(&[("*", Permission::Admin)]).await;
    srv.client_as(&admin).await
        .fetch_segment(tonic::Request::new(FetchSegmentRequest::default()))
        .await
        .expect("Admin may stream segments");
}

#[tokio::test]
async fn health_is_public_and_metadata_is_not() {
    let srv = helpers::spawn_authorized_grpc_server().await;
    let mut anon = srv.raw_client().await;

    anon.health(tonic::Request::new(HealthRequest::default()))
        .await
        .expect("health is public (D4)");

    let status = anon.metadata(tonic::Request::new(MetadataRequest::default()))
        .await
        .expect_err("metadata discloses topology and must require Read (D4)");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
}

/// Raft peer RPCs share the same server and port as the client API. An interceptor that
/// rejects them breaks replication -- which would surface in production, not here,
/// unless this test exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cluster_still_elects_and_replicates_with_authz_enabled() {
    let cluster = helpers::InProcessCluster::authorized(3).await.expect("cluster starts");

    helpers::await_condition("a leader is elected", Duration::from_secs(10), || async {
        cluster.leader().await.is_some()
    })
    .await;

    let admin = cluster.principal_with(&[("*", Permission::Admin)]).await;
    cluster.put_as(&admin, b"k", b"v").await.expect("authorized write succeeds");

    helpers::await_condition("value replicated to all nodes", Duration::from_secs(10), || async {
        cluster.all_nodes_have(b"k", b"v").await
    })
    .await;
}

/// A client without a cluster certificate must not be able to forge log entries.
#[tokio::test]
async fn client_cannot_forge_append_entries() {
    let cluster = helpers::InProcessCluster::authorized(3).await.expect("cluster starts");
    let status = cluster.raw_raft_client(1).await
        .append_entries(tonic::Request::new(AppendEntriesRequest::default()))
        .await
        .expect_err("a non-peer must not reach RaftService");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
}
```

- [ ] **Step 5: Run and watch them fail**

```bash
cargo test -p prkdb --test grpc_authz -- --test-threads=1
```
Expected: every `rejects_*` and `cannot_forge` test FAILs — the calls succeed today.

- [ ] **Step 6: Write the peer-auth policy for `RaftService` (D7)**

D7 chose mTLS client certificates, so this depends on Task 3's `--tls-client-ca` plumbing.

```rust
// tonic 0.12.3 — requires the "server" AND "tls" features.
// This workspace has tls (Cargo.toml:49) and default features, so `server` is present.
pub fn peer_certs(&self) -> Option<Arc<Vec<CertificateDer<'static>>>>
```

**Read the contract precisely, because it is easy to over- or under-build here:**

- `peer_certs()` returns `Some` **only** on the server side of a TLS connection. `None` means
  either no TLS or no client certificate — both must be rejected for `RaftService`.
- The rustls layer has **already validated the chain** against the CA configured by
  `--tls-client-ca`. You are not verifying trust here; you are extracting *identity* from an
  already-trusted cert. Do not hand-roll chain validation — and do not skip configuring the
  client CA, because without it any self-signed cert produces a `Some`.
- The certs come back as raw DER (`CertificateDer`). Extract the subject CN or a SAN to map to a
  node id if per-node identity is wanted; membership in the cluster CA is the minimum bar.

All **five** `RaftService` RPCs are covered — `RequestVote`, `PreVote`, `AppendEntries`,
`InstallSnapshot`, `ReadIndex`. `PreVote` and `ReadIndex` are the easy ones to miss, and
`ReadIndex` is the mechanism behind linearizable follower reads (R14): forging it breaks the
guarantee R14 exists to prove.

**Do not exempt `RaftService` from authentication.** That is the tempting shortcut, and it lets
any client forge `AppendEntries` and rewrite the log.

- [ ] **Step 7: Write the client interceptor for `PrkDbService`**

```rust
use prkdb::authz::{Permission, PrincipalStore};
use tonic::{Request, Status};

/// Permission required per RPC. `fetch_segment` needs Admin because it streams raw WAL
/// across every collection -- no per-collection grant is sufficient authority.
fn required(rpc: &str) -> Option<Permission> {
    match rpc {
        "Health" => None,                                   // public (D4)
        "Put" | "BatchPut" | "Delete" => Some(Permission::Write),
        "Get" | "Watch" | "GetSchema" | "CheckCompatibility" | "Metadata" => Some(Permission::Read),
        "FetchSegment" => Some(Permission::Admin),
        _ => Some(Permission::Admin),                       // admin RPCs, and safe default
    }
}
```

The `_ => Admin` default is deliberate: a newly added RPC that nobody classified fails closed.
An `Option::None` default would ship the next `fetch_segment` unprotected.

Resolve the credential from metadata, look up the `Principal` via the Task 0 store using a
constant-time comparison, then check `principal.permits(collection, required(rpc))`. Return
`Unauthenticated` when the credential is unknown and `PermissionDenied` when it is known but
insufficient.

- [ ] **Step 8: Apply each policy to its own service**

`raft.proto` declares two services, and tonic applies interceptors per service — so this is
cleaner than one interceptor trying to disambiguate:

```
RaftService   (raft.proto:5-20)    -> peer_auth (mTLS client cert)
PrkDbService  (raft.proto:23-118)  -> ApiAuthzInterceptor
```

Both binaries construct these — `prkdb-server.rs:147-172` and `serve.rs:328-331`. Update both.

- [ ] **Step 9: Migrate the 15 admin RPCs off the message field**

They currently read `admin_token` from the request message. Move them to the same principal
model, requiring `Admin`. Deprecate the `admin_token` field but keep honouring it for **one
release** with a warning, so existing clients do not break atomically.

Test both a client sending the deprecated field and one sending metadata, plus one sending
neither — spec §10 implementation question 2.

- [ ] **Step 10: Teach the Rust client to send credentials**

`crates/prkdb-client/src/client.rs` has `with_admin_token`. Add `with_credential` that attaches
`authorization: Bearer <credential>` metadata to **every** request, not only admin ones, and map
`PermissionDenied` to a distinct error variant so callers can tell "re-authenticate" from
"ask for a grant".

- [ ] **Step 11: Verify no RPC was missed**

```bash
awk '/^service /{s=$2} /^[[:space:]]*rpc /{gsub(/\(.*/,"",$2); print s"\t"$2}' \
  crates/prkdb-proto/proto/raft.proto
```

Expected: **5** under `RaftService` and **25** under `PrkDbService`. Every one must land in
exactly one bucket, and the arithmetic must balance:

| Bucket | Count | Members |
|---|---|---|
| Peer-authenticated (`RaftService`, mTLS) | 5 | `RequestVote`, `PreVote`, `AppendEntries`, `InstallSnapshot`, `ReadIndex` |
| `Admin` | 16 | the 15 previously admin-gated, plus `FetchSegment` |
| `Write` | 3 | `Put`, `BatchPut`, `Delete` |
| `Read` | 5 | `Get`, `Watch`, `GetSchema`, `CheckCompatibility`, `Metadata` |
| Public | 1 | `Health` |

`PrkDbService`: 16 + 3 + 5 + 1 = **25** OK, plus `RaftService`: 5 = **30 total** OK

**If the arithmetic does not balance, an RPC is unprotected.** That check is what surfaced
`get_schema` and `check_compatibility` — auditing from the interface, not from the list of known
problems. Do not proceed past a mismatch.

- [ ] **Step 12: Run the existing suites for regressions**

```bash
cargo test -p prkdb --test admin_rpc_tests --test client_server_integration --test distributed_writes -- --test-threads=1
./scripts/test_mixed_client_integration.sh
```

- [ ] **Step 13: Commit**

```bash
git add crates/prkdb/src/raft/ crates/prkdb/src/bin/ crates/prkdb-cli/src/ crates/prkdb-client/src/ crates/prkdb/tests/grpc_authz.rs
git commit -m "feat: authorize the gRPC data plane and authenticate Raft peers by mTLS"
```

---

## Task 3: Make TLS reachable (R13)

> `crates/prkdb/src/raft/server.rs:38` implements full mTLS. Its only caller in the workspace is
> `crates/prkdb/examples/raft_node.rs:142`. No shipped binary can enable it.

**Files:**
- Modify: `crates/prkdb-cli/src/commands/serve.rs`
- Modify: `crates/prkdb/src/bin/prkdb-server.rs`
- Test: `crates/prkdb-cli/tests/tls_integration.rs`
- Modify: `.gitignore`
- Delete: `certs/`

- [x] **Step 1: Confirm TLS is unreachable**

```bash
grep -rn 'start_raft_server_tls\|TlsConfig' crates/prkdb-cli/src crates/prkdb/src/bin --include='*.rs' \
  || echo "no binary references TLS"
```
Expected: `no binary references TLS`.

- [x] **Step 2: Write the failing test**

`crates/prkdb-cli/tests/tls_integration.rs`: generate a CA and server cert into a `TempDir` via
`scripts/gen_certs.sh`, start the server with `--tls-cert`/`--tls-key`, then assert:
1. a client trusting the test CA connects and reads successfully;
2. a plaintext HTTP client is rejected.

- [x] **Step 3: Run it and watch it fail**

```bash
cargo test -p prkdb-cli --test tls_integration -- --nocapture
```
Expected: FAIL — the flags do not exist.

- [x] **Step 4: Add the CLI flags**

`--tls-cert <PATH>`, `--tls-key <PATH>`, `--tls-client-ca <PATH>` (optional, enables mTLS) on
both `prkdb-cli serve` and `prkdb-server`. All three refuse to start if the files are unreadable
— fail loudly at startup rather than silently serving plaintext.

- [x] **Step 5: Wire the Raft transport**

Where `start_raft_server` is called, branch to `start_raft_server_tls` when TLS args are present.
The `TlsConfig` struct already exists at `server.rs:11`.

- [x] **Step 6: Wire the HTTP surface**

Use `axum-server` with its `rustls` feature — the smaller change than terminating TLS via
`tokio-rustls` by hand.

```toml
axum-server = { version = "0.8", features = ["tls-rustls"] }
```

> **Do not "correct" this to 0.7 to match axum 0.7.** Checked on 2026-08-08: `axum-server 0.8.0`
> lists axum only as a **dev**-dependency; its normal deps are `hyper ^1.4`, `hyper-util ^0.1.18`,
> and `tower-service ^0.3`. It couples to the tower/hyper stack, not to an axum major version, so
> it serves any `tower::Service` — which is what `app.into_make_service()` produces on axum 0.7.

- [x] **Step 7: Run the test**

```bash
cargo test -p prkdb-cli --test tls_integration -- --nocapture
```
Expected: PASS.

- [x] **Step 8: Confirm `certs/` is already clean — no work required**

An earlier revision of this plan called for removing committed private keys. That was a phantom
finding. Verify rather than act:

```bash
git log --all --oneline -- certs/ | wc -l    # expect 0 — never committed
grep -n 'certs' .gitignore                   # expect /certs/ at line 12
git ls-files certs/ | wc -l                  # expect 0 — not tracked
```

The keys under `certs/` are local dev fixtures produced by `scripts/gen_certs.sh`. They have
never been in the repository and `/certs/` has been ignored all along. Nothing to remove, no
history to rewrite. Tick this box once the three commands confirm it.

- [x] **Step 9: Document it**

Add a TLS section to `docs/guide/deployment.md` covering cert generation, the flags, and mTLS
between Raft peers.

- [x] **Step 10: Commit**

```bash
git add -A
git commit -m "feat: expose TLS configuration from both server binaries"
```

---

## Task 4: Version the on-disk format (production primitive)

> Highest-urgency, lowest-effort item in the spec. `log_record.rs` has CRC32 per record
> (`log_record.rs:101`) but no magic number and no format version. Every day of data written
> without one is data that cannot be safely evolved.

**Files:**
- Modify: `crates/prkdb-core/src/wal/log_record.rs`
- Modify: `crates/prkdb-core/src/wal/log_segment.rs`
- Test: `crates/prkdb-core/tests/format_version.rs`

- [ ] **Step 1: Write the failing test**

```rust
//! The WAL must identify its own format and refuse formats it does not understand.

#[test]
fn rejects_a_segment_with_an_unknown_magic() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("000001.wal");
    std::fs::write(&path, b"NOTAPRKDBSEGMENTATALL\x00\x00\x00\x00").unwrap();

    let err = LogSegment::open(&path).expect_err("must refuse an unrecognised file");
    assert!(
        err.to_string().contains("magic"),
        "error must name the problem, got: {err}"
    );
}

#[test]
fn rejects_a_future_format_version() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("000001.wal");

    let mut header = Vec::new();
    header.extend_from_slice(&PRKDB_WAL_MAGIC);
    header.extend_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
    std::fs::write(&path, &header).unwrap();

    let err = LogSegment::open(&path).expect_err("must refuse a newer format");
    assert!(
        err.to_string().contains("version"),
        "error must name the problem, got: {err}"
    );
}

#[test]
fn round_trips_the_current_format() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("000001.wal");

    let mut seg = LogSegment::create(&path).unwrap();
    seg.append(&LogRecord::new(1, b"hello".to_vec())).unwrap();
    drop(seg);

    let reopened = LogSegment::open(&path).expect("current format must reopen");
    assert_eq!(reopened.format_version(), FORMAT_VERSION);
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p prkdb-core --test format_version -- --nocapture
```
Expected: FAIL — `PRKDB_WAL_MAGIC` and `FORMAT_VERSION` do not exist.

- [ ] **Step 3: Define the header**

In `log_segment.rs`:

```rust
/// Magic bytes at the start of every WAL segment. Lets us reject a file that is not ours
/// before interpreting a single byte of it as a record.
pub const PRKDB_WAL_MAGIC: [u8; 8] = *b"PRKDBWAL";

/// On-disk format version. Bump on any change to record framing or segment layout.
/// A reader refuses versions above its own rather than misparsing them.
pub const FORMAT_VERSION: u32 = 1;

/// Magic (8) + version (4) + reserved (4, zero — room for flags without a version bump).
const SEGMENT_HEADER_LEN: usize = 16;
```

- [ ] **Step 4: Write the header on create, validate on open**

`LogSegment::create` writes the 16-byte header before any record. `LogSegment::open` reads it
and returns a typed error on magic mismatch or `version > FORMAT_VERSION`. All existing record
offsets shift by `SEGMENT_HEADER_LEN` — check every offset computation in `log_segment.rs` and
`mmap_log_segment.rs`.

- [ ] **Step 5: Run the tests**

```bash
cargo test -p prkdb-core --test format_version -- --nocapture
cargo test -p prkdb-core -- --nocapture
```
Expected: all PASS. The existing WAL tests must still pass — if offsets were missed, they fail
here.

- [ ] **Step 6: Handle pre-header segments**

Any WAL written before this change has no header. Choose one and document it in
`docs/guide/deployment.md`:
- **Recommended:** treat a missing magic as format 0, read it, and rewrite with a header on next
  compaction. Existing data keeps working.
- Or: refuse to open, and ship a `prkdb migrate` command.

Given nothing is published to crates.io and there are no external users, the second is
defensible and simpler. Say which was chosen and why.

- [ ] **Step 7: Run the corruption suite**

```bash
cargo test -p prkdb --test corruption_tests -- --ignored --nocapture --test-threads=1
```
These three tests are currently `#[ignore]`d. This task is the right moment to un-ignore them —
they test exactly this layer.

- [ ] **Step 8: Commit**

```bash
git add crates/prkdb-core/
git commit -m "feat: add magic number and format version to WAL segments"
```

---

## Task 5: Backup and restore (production primitive)

> **This task is smaller than an earlier revision claimed.** `prkdb backup` and `prkdb restore`
> already exist — `crates/prkdb-cli/src/commands/backup.rs` (113 lines), wired into the CLI at
> `main.rs:132-135` and `:252-253`. They support gzip/none compression and `restore --force`.
> Read the file before writing anything.
>
> What is genuinely missing: **verification**. There is no checksum manifest, no format-version
> check, no round-trip test, no retention policy, and no scheduling guidance. A backup nobody has
> restored is not a backup — and nothing here has ever been restored in a test.

**Files:**
- Modify: `crates/prkdb-cli/src/commands/backup.rs`
- Test: `crates/prkdb-cli/tests/backup_restore.rs` *(new — this is the real gap)*

- [ ] **Step 1: Read what already exists**

```bash
sed -n '1,113p' crates/prkdb-cli/src/commands/backup.rs
grep -n 'Backup\|Restore' crates/prkdb-cli/src/main.rs
```

Note the design: `handle_backup` calls `db.take_snapshot(...)`; `handle_restore` iterates the
snapshot and re-`put`s each entry. That is a **logical** restore, not a byte-level one.

- [ ] **Step 2: Write the failing round-trip test**

Write records, `backup` to a temp path, wipe the data dir, `restore`, and assert every key reads
back with its original value.

> **Assert logical equivalence, not byte-identity.** Because restore replays entries through
> `put`, the rebuilt WAL will differ byte-for-byte from the original — different offsets,
> different segment boundaries. An earlier revision of this plan demanded a byte-identical
> round-trip, which this design can never satisfy. The property that matters is that every key
> reads back with the value it had.

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p prkdb-cli --test backup_restore -- --nocapture
```

- [ ] **Step 3: Implement `prkdb backup`**

```
prkdb backup --data-dir <DIR> --out <PATH> [--compress]
```

Reuse `storage/snapshot.rs` for the consistent point-in-time read. `flate2` and `tar` are already
workspace dependencies — use them rather than adding an archive crate.

- [ ] **Step 4: Implement `prkdb restore`**

```
prkdb restore --archive <PATH> --data-dir <DIR>
```

Refuse to overwrite a non-empty data dir without `--force`. Validate the format version from
Task 3 before writing anything.

- [ ] **Step 5: Run the round-trip test**

Expected: PASS.

- [ ] **Step 6: Add a checksum manifest**

The archive carries a manifest of per-file checksums and the format version. `restore` verifies
before extracting. A silently corrupt backup is worse than no backup.

- [ ] **Step 7: Document scheduling**

Add a `docs/guide/deployment.md` section showing a cron/systemd-timer example. Do **not** build a
scheduler into the database — that is the operator's layer.

- [ ] **Step 8: Commit**

```bash
git add crates/prkdb-cli/ docs/guide/deployment.md
git commit -m "feat: add backup and restore commands with checksum verification"
```

---

## Task 6: Readiness, liveness, and rate limiting (production primitive)

> Only `/health` exists (`serve.rs:230`). Kubernetes needs to distinguish "process is up" from
> "this node has caught up and can serve reads". Separately, `crates/prkdb/src/rate_limit.rs`
> implements a `RateLimiter` that no binary ever constructs.

**Files:**
- Modify: `crates/prkdb-cli/src/commands/serve.rs`
- Test: `crates/prkdb-cli/tests/readiness.rs`

- [ ] **Step 1: Write the failing test**

Assert `/livez` returns 200 as soon as the process is listening, and `/readyz` returns 503 while
the node is still replaying its WAL or has no leader, then 200 once it can serve.

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p prkdb-cli --test readiness -- --nocapture
```

- [ ] **Step 3: Implement the endpoints**

- `/livez` — 200 whenever the process is listening. Never touches storage; a liveness probe that
  can hang causes the restart loop it was meant to prevent.
- `/readyz` — 200 only when WAL replay is complete and, in cluster mode, a leader is known.
  503 with a body naming which condition is unmet.
- `/health` stays as an alias for `/livez` for backward compatibility.

Add all three to `PUBLIC_PATHS` in `auth.rs` (Task 1).

- [ ] **Step 4: Wire the rate limiter**

```bash
grep -rn 'RateLimiter' crates/prkdb-cli/src crates/prkdb/src/bin --include='*.rs' || echo "not wired"
```
Expected: `not wired`. Add `--rate-limit <PER_SECOND>` to serve, construct the existing
`RateLimiter`, apply as an axum layer returning 429 with a `Retry-After` header.

- [ ] **Step 5: Test the limiter**

Assert that exceeding the configured rate returns 429, and that the limit resets.

- [ ] **Step 6: Commit**

```bash
git add crates/prkdb-cli/
git commit -m "feat: add liveness/readiness endpoints and wire rate limiting"
```

---

## Task 7: Release engineering (production primitive)

> No CHANGELOG, no tags, no release workflow, and nothing published. 76k lines nobody can
> `cargo add`.

**Files:**
- Create: `CHANGELOG.md`
- Create: `.github/workflows/release.yml`
- Modify: `crates/prkdb-types/Cargo.toml`, `crates/prkdb-proto/Cargo.toml`, `crates/prkdb-client/Cargo.toml`

- [ ] **Step 1: Verify nothing is published**

```bash
for c in prkdb prkdb-core prkdb-client prkdb-types; do
  printf "%s: " "$c"
  curl -s "https://crates.io/api/v1/crates/$c" | grep -q '"crate"' && echo published || echo "NOT PUBLISHED"
done
```
Expected: all `NOT PUBLISHED`.

- [ ] **Step 2: Add publishing metadata**

Each crate to be published needs `description`, `readme`, `keywords`, `categories`, and
`documentation` in its `Cargo.toml`. crates.io rejects publishes without a description.

- [ ] **Step 3: Give every path dependency a version**

```bash
grep -n 'path = "crates/' Cargo.toml
```

`[workspace.dependencies]` declares inter-crate deps as `{ path = "..." }` with no `version`.
crates.io rejects a publish whose dependency has only a path. Every crate in the publish set
needs `version = "0.6.0"` alongside its path:

```toml
prkdb-types = { path = "crates/prkdb-types", version = "0.6.0" }
prkdb-proto = { path = "crates/prkdb-proto", version = "0.6.0" }
```

- [ ] **Step 4: Determine the real dependency order**

```bash
grep -A6 '^\[dependencies\]' crates/prkdb-client/Cargo.toml
```

`prkdb-client` is **not** a leaf — it depends on `prkdb-proto`. crates.io requires every
dependency to already exist at the declared version, so the order is fixed:

```
prkdb-types  →  prkdb-proto  →  prkdb-client
```

- [ ] **Step 5: Dry-run in that order**

```bash
cargo publish -p prkdb-types  --dry-run
cargo publish -p prkdb-proto  --dry-run
cargo publish -p prkdb-client --dry-run
```

`prkdb-proto` and `prkdb-client` dry-runs will fail until their dependencies are actually
published — `--dry-run` cannot resolve a version that does not exist on the registry yet. That
is expected. Publish `prkdb-types` for real first, then re-run the next dry run.

Start with these three: small, stable, low-risk, and they establish the namespace. The engine
crates (`prkdb`, `prkdb-core`) can follow once their public APIs settle after Plan A.

- [ ] **Step 6: Write the CHANGELOG**

Keep-a-Changelog format. Reconstruct entries for 0.6.0 from `git log`.

- [ ] **Step 7: Add the release workflow**

Triggered on `v*` tags: verify the tag matches the workspace version, run the full test suite,
then `cargo publish` each crate in the order from Step 4.

- [ ] **Step 8: Tag and publish 0.6.0**

```bash
git tag -s v0.6.0 -m "v0.6.0"
git push origin v0.6.0
```

> Signed tags: this repo is configured for SSH signing, so `-s` works and the tag verifies on
> GitHub alongside the commits.

- [ ] **Step 9: Commit**

```bash
git add CHANGELOG.md .github/workflows/release.yml crates/*/Cargo.toml
git commit -m "chore: add release workflow, changelog, and publishing metadata"
```

---

## Deliberately out of scope

Listed so nobody assumes they were forgotten:

- **Multi-tenancy** — `with_namespace()` (`builder.rs:92`) is one namespace per instance. Real
  multi-tenancy means per-tenant isolation, quotas, and storage accounting. The authorization
  model from Task 0 is a prerequisite for it but is not the same thing: roles scope *who may do
  what*, tenancy scopes *whose data this is*. Separate project.
- **Groups, permission inheritance, row- and field-level rules** — the model is deliberately
  three concepts (principal, role, grant). Each of these is a real feature; none is needed to
  close S-01.
- **External identity providers (OIDC, LDAP), token expiry and rotation** — the credential is a
  long-lived opaque string. Rotation is manual: create a new principal, migrate, delete the old.
- **VPC, multi-zone, managed hosting** — properties of an operations team, not of a database.
  Not reachable by a solo maintainer and not a goal.
- **Distributed tracing (OTLP)** — worth doing, but after Plan A. Debugging a multi-node write
  path is only useful once the multi-node tests are trustworthy.
- **Vector search** — see spec §4. Explicitly sequenced after Plan A.

---

## Definition of done

**Authorization model (Task 0)**
- [ ] `Principal`, `Role`, `Grant`, `Permission` implemented per spec R12
- [ ] The permission table test passes exhaustively
- [ ] Principals live in the Raft state machine and survive `install_snapshot`
- [ ] `PRKDB_BOOTSTRAP_TOKEN` creates exactly one admin principal and is refused thereafter
- [ ] Resolved grants are cached in memory and invalidated by the Raft apply that changes them
- [ ] Revoking a role takes effect without a restart

**HTTP (Task 1)**
- [ ] All eight non-public routes require a credential and the right permission
- [ ] A valid principal lacking the grant gets **403**, not 401
- [ ] `GET /collections` filters to what the caller may see and returns `200 []`, never 403
- [ ] `prkdb-cli serve` refuses to start with no principals unless `--allow-anonymous`
- [ ] `/`, `/health`, `/livez`, `/readyz` stay reachable without credentials (D4)
- [ ] Both metrics servers are covered — `serve --prometheus` and `prkdb-server.rs:87-114`
- [ ] The `/ws/collections/:name` break is decided and recorded in `CHANGELOG.md`

**gRPC (Task 2)**
- [ ] All eight previously unprotected RPCs enforce a permission
- [ ] `fetch_segment` requires **`Admin`** — a `Read` grant on `*` is not sufficient, and a test asserts it
- [ ] `Health` is public; `Metadata` requires `Read` (D4)
- [ ] All five `RaftService` RPCs authenticate by mTLS peer certificate, including `PreVote` and `ReadIndex`
- [ ] A non-peer client cannot call `append_entries`
- [ ] A 3-node cluster still elects a leader and replicates with both policies active
- [ ] The RPC arithmetic balances: 16 Admin + 3 Write + 5 Read + 1 public = 25 `PrkDbService`, plus 5 `RaftService`
- [ ] The deprecated `admin_token` message field still works for one release, with a warning

**Both**
- [ ] Every credential comparison uses `subtle::ConstantTimeEq`
- [ ] Generated Python, TypeScript, and Go clients send credentials and distinguish 403 from 401
- [ ] `prkdb-client` attaches the credential to every request, not only admin calls
- [ ] The mixed-client integration test passes against an authorized server

**Everything else**
- [ ] TLS is enabled by CLI flags on both binaries, covered by an integration test
- [ ] `certs/` is no longer tracked
- [ ] WAL segments carry a magic number and format version; unknown formats are refused
- [ ] `backup` → wipe → `restore` round-trips byte-identically with checksum verification
- [ ] `/livez` and `/readyz` are distinct and correct; both are auth-exempt
- [ ] `--rate-limit` returns 429 with `Retry-After`
- [ ] `prkdb-types`, `prkdb-proto`, and `prkdb-client` are published to crates.io
- [ ] `CHANGELOG.md` exists and a signed `v0.6.0` tag is pushed
