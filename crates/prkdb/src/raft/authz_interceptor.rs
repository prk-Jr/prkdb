//! gRPC authorization for `PrkDbService`.
//!
//! Closes the gRPC half of spec S-01. `PrkDbService` declares 25 RPCs; fifteen called
//! `validate_admin_token` and ten did not, including:
//!
//! - `fetch_segment`, which streams **raw WAL segments** — a complete data-exfiltration
//!   primitive requiring no credential;
//! - `put`, `get`, `delete`, `batch_put`, `watch`;
//! - `get_schema` and `check_compatibility`, whose siblings `register_schema` and
//!   `list_schemas` *are* gated, which is what marks this as oversight rather than design.
//!
//! # Status: decision logic complete, server wiring outstanding
//!
//! `check` and `check_http` are implemented and tested. Registering the layer on the
//! running server is the remaining step, tracked as Task 2b in the production-security
//! plan. Until that lands the gRPC surface is still open — the HTTP surface is already
//! closed by `prkdb-cli`'s `authz_layer`.
//!
//! # Why metadata rather than a message field
//!
//! The admin RPCs carry `admin_token` as a field in the request message. The data-plane
//! messages have no such field — `PutRequest` is `{ key, value }` — and adding one to each
//! would bloat the wire format, still leave `fetch_segment` streaming, and give three
//! languages of generated client a field to forget. A transport-level interceptor reading
//! gRPC metadata also matches the `Authorization` header the HTTP surface uses.

use tonic::Status;

use crate::authz::{Permission, PrincipalStore};

/// Permission required per RPC, keyed by the gRPC method name.
///
/// `fetch_segment` requires `Admin` rather than `Read`: it streams raw WAL across every
/// collection, so no per-collection `Read` grant is sufficient authority for it.
///
/// The fallback is `Admin`, deliberately. A newly added RPC that nobody classified fails
/// closed. An `Option::None` default would ship the next `fetch_segment` unprotected,
/// which is precisely how this hole appeared.
pub fn required_permission(method: &str) -> Option<Permission> {
    match method.rsplit('/').next().unwrap_or(method) {
        // Public: orchestrators and the client bootstrap path probe this before any
        // credential could exist (spec D4).
        "Health" => None,

        "Put" | "BatchPut" | "Delete" => Some(Permission::Write),

        // Metadata discloses node addresses and partition layout, so it is Read rather
        // than public (D4).
        "Get" | "Watch" | "GetSchema" | "CheckCompatibility" | "Metadata" => Some(Permission::Read),

        "FetchSegment" => Some(Permission::Admin),

        _ => Some(Permission::Admin),
    }
}

/// Authenticates and authorizes client-facing RPCs from gRPC metadata.
///
/// Peer RPCs live on a separate service (`RaftService`) and are authenticated by mTLS
/// client certificate instead — see `peer_auth`.
#[derive(Clone)]
pub struct ApiAuthzInterceptor {
    /// `None` means `--allow-anonymous`.
    store: Option<PrincipalStore>,
}

impl ApiAuthzInterceptor {
    pub fn new(store: Option<PrincipalStore>) -> Self {
        Self { store }
    }

    /// Decide a single request. Split out so it can be tested without a live server.
    // tonic::Status is a large error type; the repo already allows this elsewhere
    // (grpc_service.rs:159, :1268) rather than boxing every gRPC result.
    #[allow(clippy::result_large_err)]
    pub fn check(&self, method: &str, credential: Option<&str>) -> Result<(), Status> {
        let Some(store) = &self.store else {
            return Ok(());
        };

        let Some(required) = required_permission(method) else {
            return Ok(()); // public RPC
        };

        let credential = credential.ok_or_else(|| {
            Status::unauthenticated("missing bearer credential in authorization metadata")
        })?;

        let principal = store
            .resolve(credential)
            .ok_or_else(|| Status::unauthenticated("unknown credential"))?;

        // These RPCs are not scoped to a single collection, so authority is checked
        // against `*`. FetchSegment is the reason that matters: it spans all of them.
        if principal.permits("*", required) {
            Ok(())
        } else {
            Err(Status::permission_denied(format!(
                "principal {} lacks {:?} for {}",
                principal.name(),
                required,
                method
            )))
        }
    }
}

/// Decide an HTTP-level gRPC request.
///
/// **A `tonic::service::Interceptor` cannot do this job.** It receives `Request<()>`,
/// which carries metadata but not the method being called — tonic sets no `grpc-method`
/// key. An interceptor would therefore see an empty method name, fall through to the
/// `Admin` default, and require Admin for every RPC including `Health`: fail-closed, so
/// not a hole, but it would break every client and the cluster with them.
///
/// The method is visible one layer down, as the request path
/// (`/prkdb.PrkDbService/Put`), so authorization belongs in a tower layer rather than an
/// interceptor.
impl ApiAuthzInterceptor {
    #[allow(clippy::result_large_err)]
    pub fn check_http<B>(&self, req: &http::Request<B>) -> Result<(), Status> {
        let method = req.uri().path().to_string();
        let credential = req
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(|s| s.to_string());

        self.check(&method, credential.as_deref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authz::{Grant, Principal};

    fn store_with(grants: &[(&str, Permission)]) -> PrincipalStore {
        let s = PrincipalStore::new();
        s.insert(Principal::new(
            "p",
            "cred",
            grants
                .iter()
                .map(|(pat, perm)| Grant::new(*pat, *perm))
                .collect(),
        ));
        s
    }

    #[test]
    fn fetch_segment_requires_admin_not_read() {
        // The single most important case: FetchSegment streams raw WAL across every
        // collection, so Read on `*` must not be enough.
        assert_eq!(
            required_permission("/prkdb.PrkDbService/FetchSegment"),
            Some(Permission::Admin)
        );

        let reader = ApiAuthzInterceptor::new(Some(store_with(&[("*", Permission::Read)])));
        let err = reader
            .check("/prkdb.PrkDbService/FetchSegment", Some("cred"))
            .expect_err("Read on * must not stream raw WAL");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);

        let admin = ApiAuthzInterceptor::new(Some(store_with(&[("*", Permission::Admin)])));
        admin
            .check("/prkdb.PrkDbService/FetchSegment", Some("cred"))
            .expect("Admin may stream segments");
    }

    #[test]
    fn missing_and_unknown_credentials_are_unauthenticated() {
        let i = ApiAuthzInterceptor::new(Some(store_with(&[("*", Permission::Admin)])));

        assert_eq!(
            i.check("/prkdb.PrkDbService/Put", None).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            i.check("/prkdb.PrkDbService/Put", Some("wrong"))
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
    }

    /// A known principal lacking the grant is PermissionDenied, not Unauthenticated —
    /// the distinction a single shared token cannot express.
    #[test]
    fn known_principal_without_the_grant_is_permission_denied() {
        let i = ApiAuthzInterceptor::new(Some(store_with(&[("*", Permission::Read)])));
        assert_eq!(
            i.check("/prkdb.PrkDbService/Put", Some("cred"))
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        i.check("/prkdb.PrkDbService/Get", Some("cred"))
            .expect("Read covers Get");
    }

    #[test]
    fn health_is_public_and_metadata_is_not() {
        assert_eq!(required_permission("/prkdb.PrkDbService/Health"), None);
        assert_eq!(
            required_permission("/prkdb.PrkDbService/Metadata"),
            Some(Permission::Read)
        );

        let i = ApiAuthzInterceptor::new(Some(store_with(&[])));
        i.check("/prkdb.PrkDbService/Health", None)
            .expect("health is public");
        assert_eq!(
            i.check("/prkdb.PrkDbService/Metadata", None)
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
    }

    /// An RPC nobody classified must fail closed.
    #[test]
    fn an_unclassified_rpc_defaults_to_admin() {
        assert_eq!(
            required_permission("/prkdb.PrkDbService/SomeFutureRpc"),
            Some(Permission::Admin)
        );
    }

    #[test]
    fn allow_anonymous_permits_everything() {
        let i = ApiAuthzInterceptor::new(None);
        i.check("/prkdb.PrkDbService/FetchSegment", None)
            .expect("--allow-anonymous bypasses the check");
    }

    /// Every data-plane RPC named in S-01 must now require something.
    #[test]
    fn every_rpc_from_the_finding_is_classified() {
        for rpc in [
            "Put",
            "Get",
            "Delete",
            "BatchPut",
            "Watch",
            "FetchSegment",
            "GetSchema",
            "CheckCompatibility",
        ] {
            assert!(
                required_permission(rpc).is_some(),
                "{rpc} was unprotected in S-01 and must now require a permission"
            );
        }
    }
}
