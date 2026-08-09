//! The shipped Rust client can talk to a server that enforces authorization.
//!
//! # The gap this closes
//!
//! `PrkDbClient` carried an `admin_token` and sent it as a **message field**, on the
//! handful of admin RPCs that declare one. The data plane — `put`, `get`, `delete`,
//! `batch_put`, `watch` — has no such field and never did, so once the server started
//! enforcing authorization (spec S-01) every data call from the official client came back
//! `unauthenticated`.
//!
//! Securing a server while leaving its own client unable to authenticate is shipping a
//! lock and no key. These tests exist so that cannot happen quietly again: they drive a
//! real tonic server with the authorization layer installed, over a real socket.

mod helpers;

use prkdb::authz::{Grant, Permission, Principal, PrincipalStore};
use prkdb::raft::authz_interceptor::AuthzGrpcLayer;
use prkdb::raft::grpc_service::PrkDbGrpcService;
use prkdb::storage::InMemoryAdapter;
use prkdb::PrkDb;
use prkdb_client::PrkDbClient;
use prkdb_proto::raft::prk_db_service_server::PrkDbServiceServer;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

const ADMIN: &str = "client-credentials-admin";
const READER: &str = "client-credentials-reader";

fn store() -> PrincipalStore {
    let store = PrincipalStore::new();
    store.insert(Principal::admin("admin", ADMIN));
    store.insert(Principal::new(
        "reader",
        READER,
        vec![Grant::new("*", Permission::Read)],
    ));
    store
}

async fn start_anon() -> (String, oneshot::Sender<()>) {
    start_with(None).await
}

/// Start a server with authorization enforced, as `prkdb-cli serve` does.
async fn start() -> (String, oneshot::Sender<()>) {
    start_with(Some(store())).await
}

async fn start_with(store: Option<PrincipalStore>) -> (String, oneshot::Sender<()>) {
    let db = Arc::new(
        PrkDb::builder()
            .with_storage(InMemoryAdapter::new())
            .build()
            .expect("build"),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let url = format!("http://{addr}");

    // The client bootstraps from Metadata, which only names a node if the service was
    // told its own id and public address — as `prkdb-cli serve` does. Without them the
    // client reports "Failed to fetch metadata from any bootstrap server", which looks
    // like a network fault rather than a misconfigured service.
    let service = PrkDbGrpcService::new(db, "unused".to_string())
        .with_local_node_id(1)
        .with_public_address(url.clone());

    let (tx, rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        Server::builder()
            .layer(AuthzGrpcLayer::new(store))
            .add_service(PrkDbServiceServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                rx.await.ok();
            })
            .await
            .expect("server runs");
    });

    for _ in 0..40 {
        if let Ok(channel) = tonic::transport::Channel::from_shared(url.clone()) {
            if channel.connect().await.is_ok() {
                return (url, tx);
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("test server did not start");
}

/// Control: with authorization disabled the client connects, so a failure in the tests
/// below is about credentials rather than about the metadata RPC or the harness.
///
/// Kept because it earned its place — the first run of this file failed here, and without
/// this test the cause looked like a credential bug rather than a service missing its
/// advertised address.
#[tokio::test(flavor = "multi_thread")]
async fn an_anonymous_server_admits_an_uncredentialed_client() {
    let (url, _shutdown) = start_anon().await;
    PrkDbClient::new(vec![url])
        .await
        .expect("connect to an anonymous server");
}

/// A credentialed client can write and read. This is the case that was broken.
#[tokio::test(flavor = "multi_thread")]
async fn a_credentialed_client_can_use_the_data_plane() {
    let (url, _shutdown) = start().await;

    let client = PrkDbClient::connect_with_credential(vec![url], ADMIN)
        .await
        .expect("connect");

    client
        .put(b"users:alpha", b"one")
        .await
        .expect("a credentialed put must be accepted");

    let value = client
        .get(b"users:alpha")
        .await
        .expect("a credentialed get must be accepted");
    assert_eq!(value.as_deref(), Some(b"one".as_slice()));
}

/// Without a credential the client cannot even bootstrap, so the test above is not
/// passing because the server forgot to check.
///
/// The refusal lands earlier than one might expect: `Metadata` requires `Read` (spec D4),
/// and the client fetches metadata during construction. So a secured cluster rejects an
/// uncredentialed client at connect rather than at first write.
///
/// That is the correct behaviour, and worth pinning: the error text mentions only
/// metadata, so anyone debugging it will suspect the network unless they know that
/// bootstrap is itself an authorized call. `connect_with_credential` exists for this
/// reason and its doc comment says so.
#[tokio::test(flavor = "multi_thread")]
async fn an_uncredentialed_client_cannot_bootstrap() {
    let (url, _shutdown) = start().await;

    let err = PrkDbClient::new(vec![url])
        .await
        .err()
        .expect("a secured cluster must refuse an uncredentialed client");

    assert!(
        err.to_string().contains("metadata"),
        "expected the bootstrap fetch to be what fails, got: {err}"
    );
}

/// `with_admin_token` keeps working: an admin token is a credential, and an existing
/// deployment that configured one must not break when the server starts enforcing.
#[tokio::test(flavor = "multi_thread")]
async fn an_admin_token_still_authenticates_the_data_plane() {
    let (url, _shutdown) = start().await;

    let client = PrkDbClient::connect_with_credential(vec![url], ADMIN)
        .await
        .expect("connect")
        .with_admin_token(ADMIN);

    client
        .put(b"users:beta", b"two")
        .await
        .expect("with_admin_token must also authenticate data-plane calls");
}

/// A principal with only `Read` is refused a write, and the error says *permission*
/// rather than *authentication* — a client that cannot tell them apart will retry a
/// permission failure forever.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_credential_is_refused_a_write() {
    let (url, _shutdown) = start().await;

    let client = PrkDbClient::connect_with_credential(vec![url], READER)
        .await
        .expect("connect");

    client
        .get(b"users:alpha")
        .await
        .expect("a Read principal must be able to read");

    let err = client
        .put(b"users:alpha", b"one")
        .await
        .expect_err("a Read principal must not be able to write");
    let text = err.to_string();
    assert!(
        text.contains("ermission") || text.contains("denied"),
        "a permission failure must be distinguishable from an authentication failure, \
         got: {text}"
    );
}
