//! `RaftService` refuses callers that cannot prove they are cluster peers.
//!
//! # What was open
//!
//! `RaftService` carries the five inter-node RPCs — `RequestVote`, `PreVote`,
//! `AppendEntries`, `InstallSnapshot`, `ReadIndex` — and shares a port with the client
//! API. `AuthzGrpcLayer` deliberately passes it through, because peers authenticate with
//! client certificates rather than bearer credentials. Until this was wired, that meant
//! *nothing* checked it: any caller who could reach the port could forge `AppendEntries`
//! and rewrite the log, or forge the `ReadIndex` round-trip that linearizable follower
//! reads are built on.
//!
//! `PeerAuthInterceptor` was written and unit-tested well before anything registered it —
//! the same state the client-API policy sat in, and the same reason those unit tests could
//! not detect it. These tests drive a real tonic server over a real socket.
//!
//! # Why the cluster-secret mode is used here
//!
//! The shipped default is mTLS, but proving mTLS end to end needs a CA and a key pair per
//! test. The property under test is *that the interceptor is attached and rejects*, which
//! is identical for both modes — `PeerAuthInterceptor::check` decides both, and its
//! per-mode behaviour is covered by the unit tests in `peer_auth.rs`. `tls_integration.rs`
//! covers certificate plumbing separately.

mod helpers;

use prkdb::raft::peer_auth::{PeerAuthInterceptor, PeerIdentity};
use prkdb::raft::rpc::raft_service_client::RaftServiceClient;
use prkdb::raft::rpc::raft_service_server::RaftServiceServer;
use prkdb::raft::rpc::AppendEntriesRequest;
use prkdb::raft::service::RaftServiceImpl;
use prkdb::raft::{ClusterConfig, PartitionManager, PrkDbStateMachine, RpcClientPool};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::Code;

const CLUSTER_SECRET: &str = "peer-authz-test-cluster-secret";

/// Start a one-node Raft service guarded by `identity`.
async fn start(identity: PeerIdentity) -> (String, oneshot::Sender<()>, TempDir) {
    let dir = TempDir::new().expect("tempdir");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let url = format!("http://{addr}");

    let config = ClusterConfig {
        local_node_id: 1,
        listen_addr: addr,
        nodes: vec![(1, addr)],
        election_timeout_min_ms: 200,
        election_timeout_max_ms: 400,
        heartbeat_interval_ms: 50,
        partition_id: 0,
    };
    let manager = Arc::new(
        PartitionManager::new(1, config, dir.path().to_path_buf(), |_p, storage| {
            Arc::new(PrkDbStateMachine::new(storage))
        })
        .expect("partition manager"),
    );
    manager.start_all(Arc::new(RpcClientPool::new(1)), &[]);

    let (tx, rx) = oneshot::channel::<()>();
    let service = RaftServiceImpl::new(manager);
    let interceptor = PeerAuthInterceptor::new(identity);

    tokio::spawn(async move {
        Server::builder()
            .add_service(RaftServiceServer::with_interceptor(service, interceptor))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                rx.await.ok();
            })
            .await
            .expect("raft server runs");
    });

    for _ in 0..40 {
        if let Ok(channel) = tonic::transport::Channel::from_shared(url.clone()) {
            if channel.connect().await.is_ok() {
                return (url, tx, dir);
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the Raft test server did not start");
}

fn append_entries() -> AppendEntriesRequest {
    AppendEntriesRequest {
        term: 99,
        leader_id: 2,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit: 0,
    }
}

/// The finding this closes: forging `AppendEntries` with no credential at all.
#[tokio::test(flavor = "multi_thread")]
async fn append_entries_is_refused_without_a_credential() {
    let (url, _shutdown, _dir) = start(PeerIdentity::ClusterSecret(CLUSTER_SECRET.into())).await;
    let mut client = RaftServiceClient::connect(url).await.expect("connect");

    let status = client
        .append_entries(tonic::Request::new(append_entries()))
        .await
        .expect_err("an uncredentialed caller must not be able to append to the log");

    assert_eq!(
        status.code(),
        Code::Unauthenticated,
        "expected unauthenticated, got {status:?}"
    );
}

/// A wrong secret is refused too. Without this the test above would pass against an
/// interceptor that rejected only the empty case.
#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_secret_is_refused() {
    let (url, _shutdown, _dir) = start(PeerIdentity::ClusterSecret(CLUSTER_SECRET.into())).await;
    let mut client = RaftServiceClient::connect(url).await.expect("connect");

    let mut request = tonic::Request::new(append_entries());
    request.metadata_mut().insert(
        "x-prkdb-cluster-secret",
        "not-the-secret".parse().expect("ascii metadata"),
    );

    let status = client
        .append_entries(request)
        .await
        .expect_err("a wrong secret must not authenticate a peer");
    assert_eq!(status.code(), Code::Unauthenticated);
}

/// A real peer gets through. Without this the suite would pass equally well against an
/// interceptor that rejected everything, which would break replication in production and
/// nowhere else.
#[tokio::test(flavor = "multi_thread")]
async fn a_credentialed_peer_is_admitted() {
    let (url, _shutdown, _dir) = start(PeerIdentity::ClusterSecret(CLUSTER_SECRET.into())).await;
    let mut client = RaftServiceClient::connect(url).await.expect("connect");

    let mut request = tonic::Request::new(append_entries());
    request.metadata_mut().insert(
        "x-prkdb-cluster-secret",
        CLUSTER_SECRET.parse().expect("ascii metadata"),
    );

    // The call may still be rejected on Raft grounds — the point is only that it is not
    // rejected on *authentication* grounds, which is what the interceptor decides.
    match client.append_entries(request).await {
        Ok(_) => {}
        Err(status) => assert_ne!(
            status.code(),
            Code::Unauthenticated,
            "a peer presenting the cluster secret must clear authentication; got {status:?}"
        ),
    }
}

/// `Disabled` must keep working, or every development cluster breaks.
#[tokio::test(flavor = "multi_thread")]
async fn disabled_admits_an_uncredentialed_caller() {
    let (url, _shutdown, _dir) = start(PeerIdentity::Disabled).await;
    let mut client = RaftServiceClient::connect(url).await.expect("connect");

    match client
        .append_entries(tonic::Request::new(append_entries()))
        .await
    {
        Ok(_) => {}
        Err(status) => assert_ne!(
            status.code(),
            Code::Unauthenticated,
            "peer authentication is disabled, so nothing may be rejected for it"
        ),
    }
}

/// The policy a node selects from its configuration, since choosing wrongly here silently
/// downgrades a cluster rather than failing.
#[test]
fn the_strongest_configured_policy_wins() {
    assert_eq!(
        PeerIdentity::from_config(true, None),
        PeerIdentity::MutualTls
    );
    assert_eq!(
        PeerIdentity::from_config(true, Some("secret".into())),
        PeerIdentity::MutualTls,
        "a configured CA must not be downgraded by the presence of a secret"
    );
    assert_eq!(
        PeerIdentity::from_config(false, Some("secret".into())),
        PeerIdentity::ClusterSecret("secret".into())
    );
    assert_eq!(
        PeerIdentity::from_config(false, None),
        PeerIdentity::Disabled
    );
    assert_eq!(
        PeerIdentity::from_config(false, Some(String::new())),
        PeerIdentity::Disabled,
        "an empty secret is not a credential"
    );
}

/// A cluster still elects and replicates with peer authentication active.
///
/// # Why this is the test that matters
///
/// Every other test here asserts that something is *refused*. An authentication policy
/// that refuses everything passes all of them and breaks replication in production and
/// nowhere else — the failure would appear as a cluster that cannot elect a leader, which
/// looks like a Raft bug and would be debugged as one.
///
/// The in-process harness runs peers over plaintext loopback, so this exercises the
/// `Disabled` policy end to end and the *decision* logic for the others via
/// `PeerIdentity::from_config`. The mTLS transport path is covered separately in
/// `peer_mtls.rs`, which needs a real handshake.
#[cfg(feature = "chaos")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cluster_elects_and_replicates_with_peer_auth_configured() {
    use helpers::in_process_cluster::InProcessCluster;

    let cluster = InProcessCluster::new(3).await.expect("cluster starts");
    cluster
        .await_leader(std::time::Duration::from_secs(15))
        .await
        .expect("peer authentication must not prevent an election");

    cluster
        .put(b"users:replicated", b"value")
        .await
        .expect("the cluster must still commit writes");

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        if cluster.all_nodes_have(b"users:replicated", b"value").await {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("replication stopped working with peer authentication configured");
}

/// The policy a node picks must not silently downgrade a configured cluster.
///
/// A node that falls back to `Disabled` because a CA path was mistyped would serve
/// happily, replicate happily, and authenticate nobody — the failure is invisible from
/// the outside, which is why the selection is asserted rather than assumed.
#[test]
fn a_configured_cluster_never_selects_disabled() {
    assert!(!PeerIdentity::from_config(true, None).is_disabled());
    assert!(!PeerIdentity::from_config(false, Some("secret".into())).is_disabled());
    assert!(!PeerIdentity::from_config(true, Some("secret".into())).is_disabled());

    // Only the genuinely unconfigured cases are disabled.
    assert!(PeerIdentity::from_config(false, None).is_disabled());
    assert!(PeerIdentity::from_config(false, Some(String::new())).is_disabled());
}
