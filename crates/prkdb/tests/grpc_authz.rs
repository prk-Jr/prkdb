//! The gRPC data plane refuses uncredentialed callers.
//!
//! # What was open
//!
//! `PrkDbService` declares 25 RPCs. Fifteen checked `admin_token` as a message field; ten
//! checked nothing at all, among them `put`, `get`, `delete`, `watch`, and — worst —
//! `fetch_segment`, which streams **raw WAL segments** across every collection. Anyone who
//! could reach the port could copy the database.
//!
//! # Why these tests exist rather than only the layer's unit tests
//!
//! `ApiAuthzInterceptor` was implemented and unit-tested well before it protected
//! anything: the decision logic was correct and simply *was not registered on the server*.
//! Unit tests of a policy object cannot distinguish that state from a working one. These
//! tests drive a real tonic server over a real socket, so they fail if the layer is
//! dropped from the stack.
//!
//! `serve.rs` registering the layer is checked separately by `scripts/plan_status.sh`;
//! together they cover both "the layer works" and "the binary installs it".

use prkdb::authz::{Grant, Permission, Principal, PrincipalStore};
use prkdb::raft::authz_interceptor::AuthzGrpcLayer;
use prkdb::raft::grpc_service::PrkDbGrpcService;
use prkdb::storage::InMemoryAdapter;
use prkdb::PrkDb;
use prkdb_proto::raft::prk_db_service_client::PrkDbServiceClient;
use prkdb_proto::raft::prk_db_service_server::PrkDbServiceServer;
use prkdb_proto::raft::{FetchSegmentRequest, GetRequest, HealthRequest, PutRequest};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::Code;

const ADMIN_CREDENTIAL: &str = "grpc-authz-admin-credential";
const READER_CREDENTIAL: &str = "grpc-authz-reader-credential";

/// Admin on `*` plus a read-only principal, so "denied" can be shown to mean insufficient
/// permission rather than merely an unknown credential.
fn store() -> PrincipalStore {
    let s = PrincipalStore::new();
    s.insert(Principal::new(
        "admin",
        ADMIN_CREDENTIAL,
        vec![Grant::new("*", Permission::Admin)],
    ));
    s.insert(Principal::new(
        "reader",
        READER_CREDENTIAL,
        vec![Grant::new("*", Permission::Read)],
    ));
    s
}

/// Starts the server exactly as `prkdb-cli serve` does: the authorization layer wraps the
/// whole stack rather than being applied per service.
async fn start_server(store: Option<PrincipalStore>) -> (String, oneshot::Sender<()>) {
    let db = Arc::new(
        PrkDb::builder()
            .with_storage(InMemoryAdapter::new())
            .build()
            .unwrap(),
    );
    let service = PrkDbGrpcService::new(db, "unused-admin-token".to_string());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (tx, rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        Server::builder()
            .layer(AuthzGrpcLayer::new(store))
            .add_service(PrkDbServiceServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                rx.await.ok();
            })
            .await
            .unwrap();
    });

    for _ in 0..40 {
        if let Ok(ch) = tonic::transport::Channel::from_shared(url.clone()) {
            if ch.connect().await.is_ok() {
                return (url, tx);
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("test gRPC server failed to start");
}

fn with_credential<T>(message: T, credential: &str) -> tonic::Request<T> {
    let mut req = tonic::Request::new(message);
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {credential}").parse().unwrap(),
    );
    req
}

/// The finding that mattered most: a raw WAL stream with no credential.
#[tokio::test]
async fn fetch_segment_refuses_an_uncredentialed_caller() {
    let (url, _shutdown) = start_server(Some(store())).await;
    let mut client = PrkDbServiceClient::connect(url).await.unwrap();

    let status = client
        .fetch_segment(tonic::Request::new(FetchSegmentRequest {
            segment_id: 0,
            start_offset: 0,
            max_bytes: 1,
        }))
        .await
        .expect_err("streaming raw WAL segments must require a credential");

    assert_eq!(
        status.code(),
        Code::Unauthenticated,
        "expected unauthenticated, got {status:?}"
    );
}

/// A valid credential without Admin is still refused: `fetch_segment` spans every
/// collection, so no per-collection Read grant is sufficient authority for it.
#[tokio::test]
async fn fetch_segment_refuses_a_reader() {
    let (url, _shutdown) = start_server(Some(store())).await;
    let mut client = PrkDbServiceClient::connect(url).await.unwrap();

    let status = client
        .fetch_segment(with_credential(
            FetchSegmentRequest {
                segment_id: 0,
                start_offset: 0,
                max_bytes: 1,
            },
            READER_CREDENTIAL,
        ))
        .await
        .expect_err("a Read principal must not be able to stream the WAL");

    assert_eq!(
        status.code(),
        Code::PermissionDenied,
        "a known credential with insufficient authority must be denied, not rejected as \
         unauthenticated: {status:?}"
    );
}

#[tokio::test]
async fn the_data_plane_refuses_uncredentialed_reads_and_writes() {
    let (url, _shutdown) = start_server(Some(store())).await;
    let mut client = PrkDbServiceClient::connect(url).await.unwrap();

    let status = client
        .put(tonic::Request::new(PutRequest {
            key: b"users:alpha".to_vec(),
            value: b"one".to_vec(),
        }))
        .await
        .expect_err("put must require a credential");
    assert_eq!(status.code(), Code::Unauthenticated, "put: {status:?}");

    let status = client
        .get(tonic::Request::new(GetRequest {
            key: b"users:alpha".to_vec(),
            // READ_MODE_LINEARIZABLE
            read_mode: 0,
        }))
        .await
        .expect_err("get must require a credential");
    assert_eq!(status.code(), Code::Unauthenticated, "get: {status:?}");
}

/// A write principal can actually write. Without this the suite would pass just as well if
/// the layer rejected everything.
#[tokio::test]
async fn an_authorized_caller_is_admitted() {
    let (url, _shutdown) = start_server(Some(store())).await;
    let mut client = PrkDbServiceClient::connect(url).await.unwrap();

    client
        .put(with_credential(
            PutRequest {
                key: b"users:alpha".to_vec(),
                value: b"one".to_vec(),
            },
            ADMIN_CREDENTIAL,
        ))
        .await
        .expect("an Admin principal must be able to write");

    let response = client
        .get(with_credential(
            GetRequest {
                key: b"users:alpha".to_vec(),
                // READ_MODE_LINEARIZABLE
                read_mode: 0,
            },
            ADMIN_CREDENTIAL,
        ))
        .await
        .expect("an Admin principal must be able to read");

    assert_eq!(response.into_inner().value, b"one".to_vec());
}

/// Health must stay reachable without a credential: orchestrators probe it before any
/// credential could exist, and a node that fails its own health check gets restarted.
#[tokio::test]
async fn health_remains_public() {
    let (url, _shutdown) = start_server(Some(store())).await;
    let mut client = PrkDbServiceClient::connect(url).await.unwrap();

    client
        .health(tonic::Request::new(HealthRequest {}))
        .await
        .expect("health must not require a credential");
}

/// `--allow-anonymous` must still serve everything, or development setups break.
#[tokio::test]
async fn anonymous_mode_admits_everything() {
    let (url, _shutdown) = start_server(None).await;
    let mut client = PrkDbServiceClient::connect(url).await.unwrap();

    client
        .put(tonic::Request::new(PutRequest {
            key: b"users:alpha".to_vec(),
            value: b"one".to_vec(),
        }))
        .await
        .expect("anonymous mode must admit an uncredentialed write");
}
