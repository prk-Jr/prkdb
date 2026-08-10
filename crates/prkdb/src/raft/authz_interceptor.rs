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
//! # Status: closed
//!
//! [`AuthzGrpcLayer`] is registered on the server in `prkdb-cli`'s `serve` command, and
//! `crates/prkdb/tests/grpc_authz.rs` drives a real tonic server to prove it.
//!
//! This was worth stating explicitly because for a while the policy below was implemented
//! and unit-tested while **nothing installed it** — a state indistinguishable from working
//! if you only read the tests for this file. `scripts/plan_status.sh` now checks the
//! registration separately from the file's existence.
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

/// Path prefix of the client-facing service.
///
/// Both services share the `raft` proto package, so the service name is what separates
/// them: `/raft.PrkDbService/Put` versus `/raft.RaftService/AppendEntries`.
const CLIENT_SERVICE_PREFIX: &str = "/raft.PrkDbService/";

/// Registers [`ApiAuthzInterceptor`] on a tonic server.
///
/// # Why this only guards `PrkDbService`
///
/// `RaftService` is multiplexed onto the same port, and peers authenticate with mTLS
/// client certificates rather than bearer credentials (see `peer_auth`). Applying the
/// credential check to peer traffic would send every `AppendEntries` through the
/// `_ => Admin` fallback with no credential attached, and the cluster would stop electing
/// leaders. Requests outside the client service pass through untouched.
#[derive(Clone)]
pub struct AuthzGrpcLayer {
    interceptor: ApiAuthzInterceptor,
}

impl AuthzGrpcLayer {
    pub fn new(store: Option<PrincipalStore>) -> Self {
        Self {
            interceptor: ApiAuthzInterceptor::new(store),
        }
    }
}

impl<S> tower_layer::Layer<S> for AuthzGrpcLayer {
    type Service = AuthzGrpcService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthzGrpcService {
            inner,
            interceptor: self.interceptor.clone(),
        }
    }
}

#[derive(Clone)]
pub struct AuthzGrpcService<S> {
    inner: S,
    interceptor: ApiAuthzInterceptor,
}

impl<S, B> tower_service::Service<http::Request<B>> for AuthzGrpcService<S>
where
    S: tower_service::Service<http::Request<B>, Response = http::Response<tonic::body::BoxBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future =
        futures::future::Either<S::Future, std::future::Ready<Result<S::Response, S::Error>>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        if !req.uri().path().starts_with(CLIENT_SERVICE_PREFIX) {
            return futures::future::Either::Left(self.inner.call(req));
        }

        match self.interceptor.check_http(&req) {
            Ok(()) => futures::future::Either::Left(self.inner.call(req)),
            // Rejection is returned as a gRPC status response rather than a transport
            // error, so clients see `unauthenticated`/`permission_denied` instead of a
            // dropped connection.
            Err(status) => {
                futures::future::Either::Right(std::future::ready(Ok(status.into_http())))
            }
        }
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

    /// A `Write` grant must actually be sufficient to write.
    ///
    /// # What this catches
    ///
    /// Every other test here checks that insufficient authority is *refused*. Nothing
    /// checked that sufficient authority is *admitted*, so the classification could drift
    /// tighter without failing anything: deleting the
    /// `"Put" | "BatchPut" | "Delete" => Write` arm drops those three RPCs through to the
    /// `_ => Admin` fallback, locking out every writer, and the whole suite stayed green.
    ///
    /// Mutation testing found this (run 31358158012, shard 3). It is the direction a
    /// refusal-only suite is blind to, and it is the one that pages you at 3am: nobody
    /// notices a permission that got stricter until writes start failing in production.
    #[test]
    fn a_write_grant_admits_writes_and_nothing_more() {
        for method in ["Put", "BatchPut", "Delete"] {
            assert_eq!(
                required_permission(&format!("/prkdb.PrkDbService/{method}")),
                Some(Permission::Write),
                "{method} must require Write — not Admin, which would lock out writers"
            );
        }

        let writer = ApiAuthzInterceptor::new(Some(store_with(&[("*", Permission::Write)])));
        for method in ["Put", "BatchPut", "Delete"] {
            writer
                .check(&format!("/prkdb.PrkDbService/{method}"), Some("cred"))
                .unwrap_or_else(|e| panic!("a Write grant must admit {method}: {e}"));
        }

        // Write is not Admin: the same principal is still refused the admin surface.
        let err = writer
            .check("/prkdb.PrkDbService/FetchSegment", Some("cred"))
            .expect_err("Write on * must not stream raw WAL");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    /// A `Read` grant admits reads. The counterpart of the test above, for the same
    /// reason: nothing asserted that `Get`/`Watch`/`Metadata` stay readable by a reader.
    #[test]
    fn a_read_grant_admits_reads() {
        let reader = ApiAuthzInterceptor::new(Some(store_with(&[("*", Permission::Read)])));
        for method in [
            "Get",
            "Watch",
            "GetSchema",
            "CheckCompatibility",
            "Metadata",
        ] {
            assert_eq!(
                required_permission(&format!("/prkdb.PrkDbService/{method}")),
                Some(Permission::Read),
                "{method} must require Read"
            );
            reader
                .check(&format!("/prkdb.PrkDbService/{method}"), Some("cred"))
                .unwrap_or_else(|e| panic!("a Read grant must admit {method}: {e}"));
        }

        // Read is not Write.
        let err = reader
            .check("/prkdb.PrkDbService/Put", Some("cred"))
            .expect_err("Read on * must not write");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
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
