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

    // Storage before cache: a principal the cache admits but storage never recorded
    // disappears on restart, and someone will have already been told it was created.
    if let Err(e) = admin.store.persist(db.storage().as_ref(), principal).await {
        return server_error(&format!("failed to persist principal: {e}"));
    }

    (StatusCode::CREATED, Json(json!({ "created": body.name }))).into_response()
}

/// Revoke a principal.
pub async fn revoke(State(admin): State<PrincipalAdmin>, Path(name): Path<String>) -> Response {
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

    if let Err(e) = admin.store.forget(db.storage().as_ref(), &name).await {
        return server_error(&format!("failed to revoke principal: {e}"));
    }

    (StatusCode::OK, Json(json!({ "revoked": name }))).into_response()
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
}
