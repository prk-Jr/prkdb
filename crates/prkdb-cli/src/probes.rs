//! Liveness, readiness, and request rate limiting.
//!
//! # Why liveness and readiness are separate
//!
//! Before this the server had only `/health`, which orchestrators must then use for both
//! questions — and the two have opposite failure modes:
//!
//! - **Liveness** answers "is this process alive". It must never touch storage or take a
//!   lock. A liveness probe that can hang causes the restart loop it exists to prevent.
//! - **Readiness** answers "can this node serve traffic *now*". It must check that WAL
//!   replay finished and, in cluster mode, that a leader is known. Without it, Kubernetes
//!   routes traffic to nodes that will fail every request.
//!
//! Conflating them means either restarting a node that was merely still replaying, or
//! sending traffic to one that cannot answer.

use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::sync::Arc;

use prkdb::rate_limit::RateLimiter;

/// Liveness: the process is running and its event loop is turning.
///
/// Deliberately touches nothing. If this can block, it is not a liveness probe.
pub async fn livez_handler() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({ "status": "alive" })))
}

/// Readiness: this node can serve traffic.
///
/// Returns 503 with the unmet condition named, so an operator reading probe output learns
/// *why* rather than only that.
pub async fn readyz_handler() -> Response {
    let db = match crate::database_manager::get_db_instance().await {
        Ok(db) => db,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "status": "not_ready",
                    "reason": "storage is not open",
                    "detail": e.to_string(),
                })),
            )
                .into_response()
        }
    };

    // A node whose WAL writer has stopped publishing cannot serve writes, so it must not
    // be sent traffic — even though the process is alive and `/livez` will keep saying so.
    // This is exactly the split the module doc describes: not a restart condition (a
    // restart loses the queued writes), a "stop routing here" condition.
    let write_path = db.storage().write_path_health();
    if !write_path.healthy {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "not_ready",
                "reason": "the storage write path is not confirming writes",
                "detail": write_path.reason,
                "queue_depth": write_path.queue_depth,
                "oldest_unpublished_age_ms": write_path.oldest_unpublished_age_ms,
            })),
        )
            .into_response();
    }

    // In cluster mode a node with no known leader cannot serve a linearizable read, so it
    // is not ready however healthy the process is.
    if let Some(pm) = &db.partition_manager {
        let stats = pm.get_statistics().await;
        if stats.leaders_count == 0 {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "status": "not_ready",
                    "reason": "no partition leader elected yet",
                    "partitions": stats.total_partitions,
                    "leaders_ready": stats.leaders_count,
                })),
            )
                .into_response();
        }

        return (
            StatusCode::OK,
            Json(json!({
                "status": "ready",
                "partitions": stats.total_partitions,
                "leaders_ready": stats.leaders_count,
            })),
        )
            .into_response();
    }

    (StatusCode::OK, Json(json!({ "status": "ready" }))).into_response()
}

/// Shared limiter state. `None` disables limiting.
#[derive(Clone)]
pub struct RateLimit {
    limiter: Option<Arc<RateLimiter>>,
}

impl RateLimit {
    pub fn disabled() -> Self {
        Self { limiter: None }
    }

    pub fn per_second(ops: u64) -> Self {
        Self {
            limiter: Some(Arc::new(RateLimiter::per_second(ops))),
        }
    }
}

/// Shed load rather than queueing it.
///
/// `try_acquire` returns immediately instead of waiting for a token. Waiting would convert
/// an overload into unbounded latency and memory growth — the caller cannot tell a slow
/// server from a hung one, so it retries, which makes the overload worse.
///
/// Probe endpoints are exempt: rate-limiting a liveness check gets the node killed under
/// exactly the load where it most needs to stay up.
pub async fn limit(State(rl): State<RateLimit>, req: Request, next: Next) -> Response {
    const EXEMPT: &[&str] = &["/", "/health", "/livez", "/readyz"];

    let Some(limiter) = &rl.limiter else {
        return next.run(req).await;
    };
    if EXEMPT.contains(&req.uri().path()) {
        return next.run(req).await;
    }

    if limiter.try_acquire().await {
        next.run(req).await
    } else {
        (
            StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", "1")],
            Json(json!({
                "error": "rate limit exceeded",
                "retry_after_seconds": 1,
            })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_limiter_admits_its_budget_then_sheds() {
        let rl = RateLimit::per_second(5);
        let limiter = rl.limiter.expect("configured");

        let mut admitted = 0;
        for _ in 0..20 {
            if limiter.try_acquire().await {
                admitted += 1;
            }
        }

        assert!(
            (1..=5).contains(&admitted),
            "a 5/sec limiter admitted {admitted} of 20 immediate requests; it must shed \
             the rest rather than queue them"
        );
    }

    #[tokio::test]
    async fn a_disabled_limiter_holds_nothing() {
        assert!(RateLimit::disabled().limiter.is_none());
    }

    #[tokio::test]
    async fn livez_never_touches_storage() {
        // No database is initialised in this test; a liveness probe must still answer.
        let response = livez_handler().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
