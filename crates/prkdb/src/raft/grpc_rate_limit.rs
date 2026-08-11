//! Rate limiting for the gRPC data plane.
//!
//! # Why the HTTP limiter was not enough
//!
//! `--rate-limit` built a [`RateLimiter`] and mounted it as an axum layer, so it guarded
//! the HTTP surface and nothing else. The gRPC surface — which carries `Put`, `BatchPut`,
//! `Get`, `Watch`, and `FetchSegment`, the last of which streams raw WAL — had no limit and
//! no in-flight bound at all. An operator who passed `--rate-limit` had every reason to
//! believe the server was protected.
//!
//! Raft's `max_in_flight` is not this: it windows peer replication, not client load.
//!
//! # What is exempt, and why
//!
//! `Health` is exempt. Shedding a liveness probe gets the node killed by its orchestrator
//! under exactly the load where it most needs to stay up — the same reasoning that exempts
//! the probe endpoints on the HTTP side.
//!
//! Peer RPCs (`RaftService`) are exempt too, and that matters more. Rate-limiting
//! `AppendEntries` under client load would make a busy leader look unreachable to its
//! followers, triggering an election that removes the leader that was merely busy. Shedding
//! client traffic is backpressure; shedding consensus traffic is self-inflicted failover.

use std::sync::Arc;

use crate::rate_limit::RateLimiter;

/// Client-facing service prefix. Everything else is a peer RPC or a health probe.
const CLIENT_SERVICE_PREFIX: &str = "/prkdb.PrkDbService/";
const HEALTH_METHOD: &str = "/prkdb.PrkDbService/Health";

/// Sheds client gRPC calls above a configured rate.
#[derive(Clone)]
pub struct GrpcRateLimitLayer {
    limiter: Arc<RateLimiter>,
}

impl GrpcRateLimitLayer {
    pub fn per_second(ops: u64) -> Self {
        Self {
            limiter: Arc::new(RateLimiter::per_second(ops)),
        }
    }
}

impl<S> tower_layer::Layer<S> for GrpcRateLimitLayer {
    type Service = GrpcRateLimitService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcRateLimitService {
            inner,
            limiter: self.limiter.clone(),
        }
    }
}

#[derive(Clone)]
pub struct GrpcRateLimitService<S> {
    inner: S,
    limiter: Arc<RateLimiter>,
}

/// Whether this path is subject to shedding.
fn is_sheddable(path: &str) -> bool {
    path.starts_with(CLIENT_SERVICE_PREFIX) && path != HEALTH_METHOD
}

impl<S, B> tower_service::Service<http::Request<B>> for GrpcRateLimitService<S>
where
    S: tower_service::Service<http::Request<B>, Response = http::Response<tonic::body::BoxBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let sheddable = is_sheddable(req.uri().path());
        let limiter = self.limiter.clone();

        // `Clone` then `std::mem::replace`: the cloned service is the one that was polled
        // ready, and calling the original after cloning is the classic tower footgun.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            if sheddable && !limiter.try_acquire().await {
                // ResourceExhausted, not Unavailable: gRPC clients retry Unavailable
                // aggressively, which turns shedding into a retry storm against a server
                // that is already over capacity.
                let status = tonic::Status::resource_exhausted(
                    "rate limit exceeded; retry after backing off",
                );
                return Ok(status.into_http());
            }
            inner.call(req).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_is_never_shed() {
        assert!(
            !is_sheddable(HEALTH_METHOD),
            "shedding a liveness probe gets the node killed under the load where it most \
             needs to stay up"
        );
    }

    #[test]
    fn peer_rpcs_are_never_shed() {
        // Rate-limiting AppendEntries under client load makes a busy leader look
        // unreachable to its followers, which is self-inflicted failover.
        for path in [
            "/prkdb.RaftService/AppendEntries",
            "/prkdb.RaftService/RequestVote",
            "/prkdb.RaftService/InstallSnapshot",
            "/prkdb.RaftService/ReadIndex",
        ] {
            assert!(!is_sheddable(path), "{path} must not be shed");
        }
    }

    #[test]
    fn client_data_calls_are_shed() {
        for method in ["Put", "BatchPut", "Get", "Watch", "FetchSegment", "Delete"] {
            let path = format!("{CLIENT_SERVICE_PREFIX}{method}");
            assert!(
                is_sheddable(&path),
                "{path} carries client load and must be sheddable"
            );
        }
    }

    /// Drive `call` itself, not just its two ingredients.
    ///
    /// Diff mutation found `sheddable && !acquired` surviving both `&&` -> `||` and a
    /// deleted `!`. The unit tests above check `is_sheddable` and the limiter separately,
    /// and neither notices how they are combined:
    ///
    ///   `||`         sheds a client call whenever a token *is* available, and sheds
    ///                Health once the bucket empties — the exemption is gone
    ///   deleted `!`  sheds only while tokens remain and passes everything once the
    ///                bucket is empty, which is the limiter backwards
    ///
    /// Only exercising the composed path distinguishes them.
    #[tokio::test]
    async fn call_sheds_client_traffic_and_never_health() {
        use tower_service::Service;

        // An inner service that records how many requests reached it.
        #[derive(Clone)]
        struct Inner(std::sync::Arc<std::sync::atomic::AtomicUsize>);
        impl Service<http::Request<()>> for Inner {
            type Response = http::Response<tonic::body::BoxBody>;
            type Error = std::convert::Infallible;
            type Future = std::future::Ready<Result<Self::Response, Self::Error>>;
            fn poll_ready(
                &mut self,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), Self::Error>> {
                std::task::Poll::Ready(Ok(()))
            }
            fn call(&mut self, _: http::Request<()>) -> Self::Future {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                std::future::ready(Ok(tonic::Status::ok("").into_http()))
            }
        }

        let reached = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut svc = GrpcRateLimitService {
            inner: Inner(reached.clone()),
            limiter: Arc::new(RateLimiter::per_second(2)),
        };

        let req = |path: &str| {
            http::Request::builder()
                .uri(format!("http://x{path}"))
                .body(())
                .unwrap()
        };
        let put = format!("{CLIENT_SERVICE_PREFIX}Put");

        // Two client calls fit in the bucket and reach the inner service.
        svc.call(req(&put)).await.unwrap();
        svc.call(req(&put)).await.unwrap();
        assert_eq!(reached.load(std::sync::atomic::Ordering::SeqCst), 2);

        // The third is shed, and never reaches the inner service.
        svc.call(req(&put)).await.unwrap();
        assert_eq!(
            reached.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "a call past the limit reached the inner service; nothing is being shed"
        );

        // Health still passes with an empty bucket — the exemption that keeps an
        // overloaded node from being killed by its own orchestrator.
        svc.call(req(HEALTH_METHOD)).await.unwrap();
        assert_eq!(
            reached.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "Health was shed once the bucket emptied; that gets the node killed under \
             exactly the load where it most needs to stay up"
        );
    }

    #[tokio::test]
    async fn the_limiter_actually_runs_out() {
        let limiter = RateLimiter::per_second(2);
        assert!(limiter.try_acquire().await);
        assert!(limiter.try_acquire().await);
        assert!(
            !limiter.try_acquire().await,
            "a limiter that never refuses is not a limit"
        );
    }
}
