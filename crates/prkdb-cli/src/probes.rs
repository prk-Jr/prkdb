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
use prkdb_types::storage::WritePathHealth;

/// Liveness: the process is running and its event loop is turning.
///
/// Deliberately touches nothing. If this can block, it is not a liveness probe.
pub async fn livez_handler() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({ "status": "alive" })))
}

/// The objection the storage write path raises to serving traffic, if it raises one.
///
/// A node whose WAL writer has stopped publishing cannot serve writes, so it must not be
/// sent traffic — even though the process is alive and `/livez` will keep saying so. This
/// is exactly the split the module doc describes: not a restart condition (a restart loses
/// the queued writes), a "stop routing here" condition.
///
/// Split out of `readyz_handler` so the condition is testable at all. The handler reaches
/// this through a process-global database, and no test can arrange for that database's
/// writer to be stalled. Mutation run 31575909551 deleted the `!` from the inline version
/// and nothing noticed — that mutant makes `/readyz` report every healthy node as
/// unavailable and the one broken node as ready, which is worse than having no probe:
/// traffic is drained from the nodes that work and routed to the one that cannot accept a
/// write.
fn write_path_objection(write_path: &WritePathHealth) -> Option<Response> {
    if write_path.healthy {
        return None;
    }

    Some(
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "not_ready",
                "reason": "the storage write path is not confirming writes",
                "detail": write_path.reason,
                "queue_depth": write_path.queue_depth,
                "oldest_unpublished_age_ms": write_path.oldest_unpublished_age_ms,
            })),
        )
            .into_response(),
    )
}

/// Readiness for a node running in cluster mode.
///
/// A node with no known leader cannot serve a linearizable read, so it is not ready
/// however healthy the process is.
///
/// Takes the two counts rather than the statistics struct so it can be exercised without
/// standing up a partition manager. Extracted for the same reason as
/// `write_path_objection`, and after the same finding: `replace == with != in
/// readyz_handler` survived here too, and inverted it reports a node that has elected
/// leaders as having none, and a node with none as ready to serve.
fn partition_readiness(total_partitions: usize, leaders_ready: usize) -> Response {
    if leaders_ready == 0 {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "not_ready",
                "reason": "no partition leader elected yet",
                "partitions": total_partitions,
                "leaders_ready": leaders_ready,
            })),
        )
            .into_response();
    }

    (
        StatusCode::OK,
        Json(json!({
            "status": "ready",
            "partitions": total_partitions,
            "leaders_ready": leaders_ready,
        })),
    )
        .into_response()
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

    if let Some(not_ready) = write_path_objection(&db.storage().write_path_health()) {
        return not_ready;
    }

    if let Some(pm) = &db.partition_manager {
        let stats = pm.get_statistics().await;
        return partition_readiness(stats.total_partitions, stats.leaders_count);
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

    /// Readiness answers 503 rather than panicking when there is no storage at all.
    ///
    /// `replace readyz_handler -> Response with Default::default()` survived mutation run
    /// 31575909551 because nothing called the handler. `Response::default()` is a 200 with
    /// an empty body, so the mutant is a readiness probe that reports every node ready
    /// unconditionally — including one whose database never opened.
    ///
    /// Relies on no database manager being initialised in this test binary, the same
    /// condition `livez_never_touches_storage` above depends on.
    #[tokio::test]
    async fn readyz_is_not_ready_before_storage_opens() {
        let response = readyz_handler().await;
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "a node whose storage is not open must not be routed traffic"
        );
    }

    /// The write-path condition, in both directions.
    ///
    /// Only the false case can go wrong quietly: a probe that says 503 too often drains a
    /// node and someone notices, while one that says 200 on a stalled writer routes writes
    /// to a node that will not confirm them. Both are pinned because a single-direction
    /// assertion is satisfied by a function that always answers the same way.
    #[test]
    fn a_write_path_that_is_not_confirming_writes_takes_the_node_out_of_rotation() {
        let healthy = WritePathHealth {
            healthy: true,
            reason: None,
            queue_depth: 0,
            oldest_unpublished_age_ms: 0,
            last_publish_age_ms: Some(5),
        };
        assert!(
            write_path_objection(&healthy).is_none(),
            "a working writer must not take its node out of rotation"
        );

        let stalled = WritePathHealth {
            healthy: false,
            reason: Some("WAL writer stalled".to_string()),
            queue_depth: 12,
            oldest_unpublished_age_ms: 900,
            last_publish_age_ms: Some(900),
        };
        let objection = write_path_objection(&stalled)
            .expect("a stalled writer must object to being sent traffic");
        assert_eq!(objection.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// The cluster-mode condition, in both directions.
    ///
    /// Found by running the mutation job locally against the refactor above rather than by
    /// CI, which mutates only the lines a pull request touches and had never reached this
    /// one. It is the same inversion as `write_path_objection`'s, one branch further down:
    /// a node with a leader reported as having none takes a working cluster out of
    /// rotation, and a node with none reported ready is sent linearizable reads it cannot
    /// answer.
    #[test]
    fn a_node_with_no_elected_leader_is_not_ready() {
        assert_eq!(
            partition_readiness(4, 0).status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "no leader means no linearizable read, however healthy the process is"
        );
        assert_eq!(
            partition_readiness(4, 4).status(),
            StatusCode::OK,
            "a cluster with leaders elected must be routed traffic"
        );
    }
}
