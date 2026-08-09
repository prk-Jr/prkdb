//! HTTP authorization middleware.
//!
//! Closes the HTTP half of spec S-01, where every `/collections/*` route — including
//! `PUT .../data` and `DELETE .../data/:id` — was reachable with no credential at all.
//! The generated Python, TypeScript and Go clients all target this API, so every one of
//! them was speaking to an unauthenticated read/write endpoint.
//!
//! # 401 and 403 are different answers
//!
//! `401` means "I do not know who you are". `403` means "I know who you are and you may
//! not do this". Collapsing them is how authorization bugs hide: a caller cannot tell
//! whether to re-authenticate or to ask for a grant, and a test cannot tell whether the
//! permission check ran at all.

use axum::{
    extract::{Request, State},
    http::{Method, StatusCode},
    middleware::Next,
    response::Response,
};
use prkdb::authz::{Permission, PrincipalStore};

/// Reachable without credentials.
///
/// Orchestrators probe these before any client could hold a credential, so requiring one
/// here breaks deployment rather than securing anything. `/` is the service-info root and
/// discloses nothing.
const PUBLIC_PATHS: &[&str] = &["/", "/health", "/livez", "/readyz"];

/// Authorization state shared with the router.
#[derive(Clone)]
pub struct Authz {
    /// `None` means `--allow-anonymous`: every request is permitted.
    store: Option<PrincipalStore>,
}

impl Authz {
    pub fn enabled(store: PrincipalStore) -> Self {
        Self { store: Some(store) }
    }

    pub fn anonymous() -> Self {
        Self { store: None }
    }
}

/// Map an HTTP method to the permission it requires.
///
/// Anything that is not plainly read-only counts as a write. Defaulting the other way is
/// how a newly added route ships unprotected: an unrecognised method would be treated as
/// harmless rather than as a mutation.
fn required_permission(method: &Method) -> Permission {
    match *method {
        Method::GET | Method::HEAD | Method::OPTIONS => Permission::Read,
        _ => Permission::Write,
    }
}

/// Extract the collection name from a `/collections/{name}/...` path.
///
/// Returns `None` for paths that name no collection, which are then subject only to
/// authentication.
fn collection_from_path(path: &str) -> Option<String> {
    let rest = path
        .strip_prefix("/collections/")
        .or_else(|| path.strip_prefix("/ws/collections/"))?;
    let name = rest.split('/').next().unwrap_or(rest);
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

/// Reject a request that carries no valid credential, or whose principal lacks the
/// permission the route requires.
pub async fn authorize(
    State(authz): State<Authz>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let Some(store) = &authz.store else {
        return Ok(next.run(req).await);
    };

    let path = req.uri().path().to_string();
    if PUBLIC_PATHS.contains(&path.as_str()) {
        return Ok(next.run(req).await);
    }

    let credential = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(StatusCode::UNAUTHORIZED)?;

    // 401: no principal holds this credential.
    let principal = store.resolve(credential).ok_or(StatusCode::UNAUTHORIZED)?;

    let required = required_permission(req.method());
    let allowed = match collection_from_path(&path) {
        Some(collection) => principal.permits(&collection, required),
        // Routes naming no collection — /collections (the list) and /metrics — need a
        // credential but are not scoped to one collection. /metrics carries every
        // collection's data, so it takes Admin.
        None if path == "/metrics" => principal.permits("*", Permission::Admin),
        // Principal management is Admin regardless of method. Without this, `required_permission`
        // would let any principal holding Read *anywhere* list credentials, and any holding
        // Write *anywhere* mint one — which is privilege escalation to full admin in one call.
        None if path.starts_with("/admin/") => principal.permits("*", Permission::Admin),
        None => principal
            .grants()
            .iter()
            .any(|g| g.permission().satisfies(required)),
    };

    // 403: the principal is known, and may not do this.
    if !allowed {
        return Err(StatusCode::FORBIDDEN);
    }

    // Hand the principal to the handler. Routes that return a *set* of things — the
    // collection listing above all — must narrow it to what this caller may see, and a
    // handler cannot do that if the middleware resolves the principal and drops it.
    // Re-resolving from the header downstream would duplicate the credential comparison,
    // and a second implementation is a second thing to get wrong.
    let mut req = req;
    req.extensions_mut().insert(principal);
    Ok(next.run(req).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_paths_are_not_mistaken_for_collections() {
        // /admin/... must reach the Admin arm, not the "any grant satisfies" fallback.
        assert_eq!(collection_from_path("/admin/principals"), None);
        assert_eq!(collection_from_path("/admin/principals/alice"), None);
    }

    #[test]
    fn extracts_the_collection_from_every_route_shape() {
        assert_eq!(
            collection_from_path("/collections/users"),
            Some("users".into())
        );
        assert_eq!(
            collection_from_path("/collections/users/data"),
            Some("users".into())
        );
        assert_eq!(
            collection_from_path("/collections/users/data/42"),
            Some("users".into())
        );
        assert_eq!(
            collection_from_path("/ws/collections/events"),
            Some("events".into())
        );
        assert_eq!(collection_from_path("/collections"), None);
        assert_eq!(collection_from_path("/metrics"), None);
        assert_eq!(collection_from_path("/health"), None);
    }

    #[test]
    fn only_read_only_methods_count_as_reads() {
        assert_eq!(required_permission(&Method::GET), Permission::Read);
        assert_eq!(required_permission(&Method::HEAD), Permission::Read);
        assert_eq!(required_permission(&Method::PUT), Permission::Write);
        assert_eq!(required_permission(&Method::POST), Permission::Write);
        assert_eq!(required_permission(&Method::DELETE), Permission::Write);
        assert_eq!(required_permission(&Method::PATCH), Permission::Write);
    }

    #[test]
    fn probe_paths_are_public() {
        for p in ["/", "/health", "/livez", "/readyz"] {
            assert!(PUBLIC_PATHS.contains(&p), "{p} must stay reachable");
        }
        assert!(!PUBLIC_PATHS.contains(&"/collections"));
        assert!(!PUBLIC_PATHS.contains(&"/metrics"));
    }
}
