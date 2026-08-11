//! Principal management over HTTP.
//!
//! # Why this exists
//!
//! Before it, the only way to obtain a principal was `PRKDB_BOOTSTRAP_TOKEN`, which mints
//! exactly one admin and refuses thereafter. That left the authorization model with no way
//! to add a second user, narrow anyone's grants, or revoke a leaked credential short of
//! stopping the process and editing storage by hand — and a credential that cannot be
//! revoked without downtime will not be revoked promptly.
//!
//! # Every route requires `Admin` on `*`
//!
//! Enforced in `authz_layer::authorize` by path prefix rather than by HTTP method. Method
//! is the wrong axis here: `GET /admin/principals` lists credentials' *names and grants*,
//! which is reconnaissance, and the default mapping would have allowed it to anyone
//! holding `Read` on any single collection.
//!
//! # Revocation is immediate
//!
//! `PrincipalStore` is an `Arc<RwLock<…>>` shared with both the HTTP layer and the gRPC
//! layer, so a delete is visible to the next request on either surface without a restart.
//! The durable write happens first: a cache that forgets a principal the storage layer
//! still holds would resurrect it on restart, which is the failure mode that matters for
//! a revoked credential.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use prkdb::authz::{Grant, Permission, Principal, PrincipalStore};
use serde::{Deserialize, Serialize};
use serde_json::json;

/// What the admin routes need: the live store, and storage to persist through.
#[derive(Clone)]
pub struct PrincipalAdmin {
    store: PrincipalStore,
}

impl PrincipalAdmin {
    pub fn new(store: PrincipalStore) -> Self {
        Self { store }
    }
}

#[derive(Debug, Deserialize)]
pub struct GrantSpec {
    /// Collection name, or `*` for all. A trailing `/*` matches a path prefix.
    pub collection: String,
    /// One of `read`, `write`, `admin`.
    pub permission: String,
}

#[derive(Debug, Deserialize)]
pub struct CreatePrincipal {
    pub name: String,
    pub credential: String,
    pub grants: Vec<GrantSpec>,
}

#[derive(Debug, Serialize)]
pub struct PrincipalView {
    pub name: String,
    pub grants: Vec<GrantView>,
}

#[derive(Debug, Serialize)]
pub struct GrantView {
    pub collection: String,
    pub permission: String,
}

fn parse_permission(value: &str) -> Option<Permission> {
    match value.to_ascii_lowercase().as_str() {
        "read" => Some(Permission::Read),
        "write" => Some(Permission::Write),
        "admin" => Some(Permission::Admin),
        _ => None,
    }
}

fn permission_name(permission: Permission) -> &'static str {
    match permission {
        Permission::Read => "read",
        Permission::Write => "write",
        Permission::Admin => "admin",
    }
}

fn view(principal: &Principal) -> PrincipalView {
    PrincipalView {
        name: principal.name().to_string(),
        // Deliberately no credential and no digest. The digest is not usable as a
        // credential, but publishing it hands an attacker an offline target.
        grants: principal
            .grants()
            .iter()
            .map(|g| GrantView {
                collection: g.pattern().to_string(),
                permission: permission_name(g.permission()).to_string(),
            })
            .collect(),
    }
}

/// List principals. Names and grants only.
pub async fn list(State(admin): State<PrincipalAdmin>) -> Response {
    let mut principals: Vec<PrincipalView> = admin.store.snapshot().iter().map(view).collect();
    principals.sort_by(|a, b| a.name.cmp(&b.name));
    (StatusCode::OK, Json(json!({ "principals": principals }))).into_response()
}

/// Create or replace a principal.
pub async fn upsert(
    axum::Extension(actor): axum::Extension<Principal>,
    State(admin): State<PrincipalAdmin>,
    Json(body): Json<CreatePrincipal>,
) -> Response {
    if body.name.trim().is_empty() {
        return bad_request("name must not be empty");
    }
    if body.credential.is_empty() {
        return bad_request("credential must not be empty");
    }

    let mut grants = Vec::with_capacity(body.grants.len());
    for spec in &body.grants {
        let Some(permission) = parse_permission(&spec.permission) else {
            return bad_request(&format!(
                "unknown permission {:?}; expected read, write, or admin",
                spec.permission
            ));
        };
        grants.push(Grant::new(&spec.collection, permission));
    }

    let principal = Principal::new(&body.name, &body.credential, grants);

    let db = match crate::database_manager::get_db_instance().await {
        Ok(db) => db,
        Err(e) => return server_error(&format!("storage unavailable: {e}")),
    };

    // Clustered: propose, so every node updates its durable copy *and* its in-memory
    // cache in log order. Authentication reads that cache, so a write that reached only
    // this node's storage would be invisible everywhere else until a restart.
    //
    // Single node: no partition manager exists, so the local write is the only path
    // (decision E3). `replicates_authz` is the same condition the write path uses.
    if db.replicates_authz() {
        let encoded = match serde_json::to_vec(&principal) {
            Ok(bytes) => bytes,
            Err(e) => return server_error(&format!("failed to encode principal: {e}")),
        };
        let command = prkdb::raft::command::Command::UpsertPrincipal {
            name: body.name.clone(),
            encoded,
        };
        if let Err(e) = db.propose_authz(command).await {
            audit(Some(&actor), "upsert", &body.name, "failed: not replicated");
            // Refused rather than written locally (decision E2). A local write here is
            // the bug this replaces: it succeeds on one node and leaves every other node
            // disagreeing about who may do what.
            return server_error(&format!(
                "failed to replicate principal: {e}. No principal was created; retry \
                 against the partition-0 leader."
            ));
        }
    } else if let Err(e) = admin.store.persist(db.storage().as_ref(), principal).await {
        audit(Some(&actor), "upsert", &body.name, "failed: not persisted");
        // Storage before cache: a principal the cache admits but storage never recorded
        // disappears on restart, and someone will have already been told it was created.
        return server_error(&format!("failed to persist principal: {e}"));
    }

    audit(Some(&actor), "upsert", &body.name, "created");
    (StatusCode::CREATED, Json(json!({ "created": body.name }))).into_response()
}

/// Revoke a principal.
pub async fn revoke(
    axum::Extension(actor): axum::Extension<Principal>,
    State(admin): State<PrincipalAdmin>,
    Path(name): Path<String>,
) -> Response {
    if admin.store.resolve_by_name(&name).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("no principal named {name}") })),
        )
            .into_response();
    }

    // Refuse to remove the last admin. A cluster with no admin cannot be administered
    // again without stopping it and editing storage, and the operator who does this will
    // be doing it at speed during an incident.
    if admin.store.admin_count() <= 1 && admin.store.is_admin(&name) {
        return (
            StatusCode::CONFLICT,
            Json(json!({
                "error": format!(
                    "{name} is the only admin; create another before revoking it, or the \
                     cluster becomes unadministrable"
                )
            })),
        )
            .into_response();
    }

    let db = match crate::database_manager::get_db_instance().await {
        Ok(db) => db,
        Err(e) => return server_error(&format!("storage unavailable: {e}")),
    };

    if db.replicates_authz() {
        let command = prkdb::raft::command::Command::RevokePrincipal { name: name.clone() };
        if let Err(e) = db.propose_authz(command).await {
            audit(Some(&actor), "revoke", &name, "failed: not replicated");
            // Refusing matters more here than on create: reporting a revoke that did not
            // replicate tells an operator a credential is dead while it still works on
            // every other node.
            return server_error(&format!(
                "failed to replicate revocation: {e}. The credential is still valid; \
                 retry against the partition-0 leader."
            ));
        }
    } else if let Err(e) = admin.store.forget(db.storage().as_ref(), &name).await {
        audit(Some(&actor), "revoke", &name, "failed: not persisted");
        return server_error(&format!("failed to revoke principal: {e}"));
    }

    audit(Some(&actor), "revoke", &name, "revoked");
    (StatusCode::OK, Json(json!({ "revoked": name }))).into_response()
}

/// Record an administrative mutation.
///
/// # Why this exists and what it must never contain
///
/// `/admin/principals` mints and revokes credentials. Until now nothing recorded who used
/// it, so "who granted this principal Admin on `*`?" had no answer — the spec lists audit
/// logging as a production gap, and it got cheaper and more valuable once principals had
/// names.
///
/// Denied attempts are logged as well as permitted ones. A rejected mutation is the more
/// interesting record: one refusal is a typo, a hundred is someone trying credentials.
///
/// **The credential and its digest are never logged.** `Principal`'s `Debug` is not used
/// here for that reason — only the actor's name, the operation, the target, and the
/// outcome. A log that leaks the thing it is auditing is worse than no log, because it
/// ships the secret to wherever logs are shipped.
fn audit(actor: Option<&Principal>, operation: &str, target: &str, outcome: &str) {
    tracing::info!(
        target: "prkdb::audit",
        actor = actor.map(|p| p.name()).unwrap_or("<unauthenticated>"),
        operation,
        target_principal = target,
        outcome,
        "admin principal mutation"
    );
}

fn bad_request(message: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": message }))).into_response()
}

fn server_error(message: &str) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": message })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_names_round_trip() {
        for permission in [Permission::Read, Permission::Write, Permission::Admin] {
            assert_eq!(
                parse_permission(permission_name(permission)),
                Some(permission)
            );
        }
    }

    #[test]
    fn permission_parsing_is_case_insensitive_and_rejects_nonsense() {
        assert_eq!(parse_permission("ADMIN"), Some(Permission::Admin));
        assert_eq!(parse_permission("Read"), Some(Permission::Read));
        assert_eq!(parse_permission("superuser"), None);
        assert_eq!(parse_permission(""), None);
    }

    /// The view must never carry credential material, however encoded.
    #[test]
    fn the_view_exposes_no_credential() {
        let principal = Principal::new(
            "alice",
            "alice-secret-token",
            vec![Grant::new("users", Permission::Read)],
        );
        let rendered = serde_json::to_string(&view(&principal)).unwrap();
        assert!(!rendered.contains("alice-secret-token"));
        assert!(!rendered.contains(principal.credential_hash()));
        assert!(rendered.contains("alice") && rendered.contains("users"));
    }

    /// The error helpers must carry their status codes.
    ///
    /// # What this catches
    ///
    /// Mutation run 31411280726, shard 18, replaced both `bad_request` and `server_error`
    /// with `Default::default()` and no test noticed. A `Response` defaults to `200 OK`
    /// with an empty body, so under those mutants every rejected admin request — an empty
    /// name, an unparseable permission, a storage failure — answered **200 OK**.
    ///
    /// A client cannot distinguish that from success. It is the worst shape an error path
    /// can take: the caller believes a principal was created when none was, or that a
    /// revoke succeeded while the credential stays live.
    ///
    /// This surface was only mutated at all because #43 brought `prkdb-cli`'s
    /// authorization files into scope; before that it had never been checked.
    #[test]
    fn the_error_helpers_carry_their_status_codes() {
        assert_eq!(
            bad_request("nope").status(),
            StatusCode::BAD_REQUEST,
            "a rejected request must not answer 200; a client cannot tell that from success"
        );
        assert_eq!(
            server_error("boom").status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "a failed operation must not answer 200"
        );

        // And they are distinguishable from each other: a caller retries a 500 and fixes
        // its input on a 400.
        assert_ne!(bad_request("a").status(), server_error("b").status());
    }
}
