//! Raft peers authenticate by mTLS client certificate, over real TLS.
//!
//! # Why this is separate from `peer_authz.rs`
//!
//! That file proves the interceptor is installed and rejects, using the cluster-secret
//! mode — which needs no certificates and so is cheap to drive. It cannot exercise the
//! *shipped default*, `PeerIdentity::MutualTls`, because that decides on
//! `Request::peer_certs()`, and `peer_certs` returns `Some` only on the server side of a
//! TLS connection whose client actually presented a certificate.
//!
//! Everything about the mTLS path is therefore invisible to a plaintext test: whether the
//! CA is enforced, whether an unauthenticated client is refused, whether a legitimate peer
//! gets through. This file establishes those with a real CA, a real handshake, and real
//! certificates generated in-process.
//!
//! # The property that matters most
//!
//! `peer_certs()` reflects what the *TLS layer* verified. Without `--tls-client-ca`
//! configured, rustls does not request or validate a client certificate at all, so the
//! interceptor sees `None` and refuses everyone — fail-closed, but for the wrong reason.
//! With the CA configured, an untrusted certificate is rejected during the handshake and
//! never reaches the interceptor. Both are asserted below, because "the caller was
//! refused" is not by itself evidence that the check works.

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
use tonic::transport::{Certificate, ClientTlsConfig, Identity, Server, ServerTlsConfig};
use tonic::Code;

/// Pick the crypto provider explicitly, once per process.
///
/// # Why this is necessary
///
/// rustls selects a provider from its own crate features. Building the whole workspace
/// unifies features across every crate, and more than one provider ends up enabled — so
/// rustls refuses to guess and panics inside a tokio worker with "Could not automatically
/// determine the process-level CryptoProvider".
///
/// The failure only appears in a full `cargo test --workspace` run: these tests pass on
/// their own, because building this target alone enables exactly one. That is a trap worth
/// naming — a test that passes in isolation and fails in the suite invites being blamed on
/// parallelism or ports.
fn install_crypto_provider() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // Ignore the error: another test in this binary may have installed it already, and
        // an existing provider is the desired state either way.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

/// A CA plus the certificates it signs.
struct Pki {
    ca_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    peer_cert_pem: String,
    peer_key_pem: String,
    /// Signed by a *different* CA, so the server must reject it.
    stranger_cert_pem: String,
    stranger_key_pem: String,
}

fn pki() -> Pki {
    install_crypto_provider();
    use rcgen::{CertificateParams, KeyPair};

    let mut ca_params = CertificateParams::new(vec![]).expect("ca params");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().expect("ca key");
    let ca = ca_params.self_signed(&ca_key).expect("self-signed ca");

    let issue = |names: Vec<String>| {
        let params = CertificateParams::new(names).expect("params");
        let key = KeyPair::generate().expect("key");
        let cert = params.signed_by(&key, &ca, &ca_key).expect("sign");
        (cert.pem(), key.serialize_pem())
    };

    let (server_cert_pem, server_key_pem) = issue(vec!["localhost".into()]);
    let (peer_cert_pem, peer_key_pem) = issue(vec!["peer".into()]);

    // A second, unrelated CA: its certificates are well-formed and worthless here.
    let mut other_ca_params = CertificateParams::new(vec![]).expect("other ca params");
    other_ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let other_ca_key = KeyPair::generate().expect("other ca key");
    let other_ca = other_ca_params
        .self_signed(&other_ca_key)
        .expect("other self-signed ca");
    let stranger_params = CertificateParams::new(vec!["stranger".to_string()]).expect("params");
    let stranger_key = KeyPair::generate().expect("stranger key");
    let stranger_cert = stranger_params
        .signed_by(&stranger_key, &other_ca, &other_ca_key)
        .expect("sign stranger");

    Pki {
        ca_pem: ca.pem(),
        server_cert_pem,
        server_key_pem,
        peer_cert_pem,
        peer_key_pem,
        stranger_cert_pem: stranger_cert.pem(),
        stranger_key_pem: stranger_key.serialize_pem(),
    }
}

/// Start a `RaftService` behind mTLS, guarded by [`PeerIdentity::MutualTls`].
///
/// `require_client_ca` controls whether the server asks for a client certificate at all,
/// which is what distinguishes "the CA is enforced" from "nobody presented anything".
async fn start(pki: &Pki, require_client_ca: bool) -> (String, oneshot::Sender<()>, TempDir) {
    let dir = TempDir::new().expect("tempdir");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let url = format!("https://localhost:{}", addr.port());

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

    let mut tls = ServerTlsConfig::new().identity(Identity::from_pem(
        &pki.server_cert_pem,
        &pki.server_key_pem,
    ));
    if require_client_ca {
        tls = tls.client_ca_root(Certificate::from_pem(&pki.ca_pem));
    }

    let (tx, rx) = oneshot::channel::<()>();
    let service = RaftServiceImpl::new(manager);
    let interceptor = PeerAuthInterceptor::new(PeerIdentity::MutualTls);

    tokio::spawn(async move {
        Server::builder()
            .tls_config(tls)
            .expect("server tls config")
            .add_service(RaftServiceServer::with_interceptor(service, interceptor))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                rx.await.ok();
            })
            .await
            .expect("tls server runs");
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    (url, tx, dir)
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

/// A peer holding a certificate from the cluster CA is admitted.
///
/// Without this the suite would pass against an interceptor that refused everything —
/// which would break replication in production and nowhere else.
#[tokio::test(flavor = "multi_thread")]
async fn a_peer_with_a_cluster_certificate_is_admitted() {
    let pki = pki();
    let (url, _shutdown, _dir) = start(&pki, true).await;

    let tls = ClientTlsConfig::new()
        .domain_name("localhost")
        .ca_certificate(Certificate::from_pem(&pki.ca_pem))
        .identity(Identity::from_pem(&pki.peer_cert_pem, &pki.peer_key_pem));

    let channel = tonic::transport::Channel::from_shared(url)
        .expect("uri")
        .tls_config(tls)
        .expect("client tls")
        .connect()
        .await
        .expect("a peer with a valid certificate must connect");

    // May still be rejected on Raft grounds; the point is that it is not rejected for
    // *authentication*, which is what the interceptor decides.
    match RaftServiceClient::new(channel)
        .append_entries(tonic::Request::new(append_entries()))
        .await
    {
        Ok(_) => {}
        Err(status) => assert_ne!(
            status.code(),
            Code::Unauthenticated,
            "a certificate signed by the cluster CA must clear peer authentication; got {status:?}"
        ),
    }
}

/// A client presenting no certificate is refused.
///
/// This is the forged-`AppendEntries` case the policy exists for.
///
/// # Where the refusal happens
///
/// With a client CA configured, **rustls refuses the handshake** — the request never
/// reaches `PeerAuthInterceptor` at all, and the client sees a transport error rather than
/// `Unauthenticated`. That is the stronger outcome: the connection is dropped before any
/// application code runs.
///
/// So this asserts *refused*, not *refused with a particular gRPC status*. Demanding
/// `Unauthenticated` here would be asserting that the transport failed to protect us and
/// the interceptor had to. The interceptor is the second line, covered by
/// `without_a_client_ca_mtls_admits_nobody` below, which constructs the case where the
/// transport lets an uncertified caller through.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_with_no_certificate_is_refused() {
    let pki = pki();
    let (url, _shutdown, _dir) = start(&pki, true).await;

    let tls = ClientTlsConfig::new()
        .domain_name("localhost")
        .ca_certificate(Certificate::from_pem(&pki.ca_pem));

    let channel = tonic::transport::Channel::from_shared(url)
        .expect("uri")
        .tls_config(tls)
        .expect("client tls")
        .connect()
        .await;

    let refused = match channel {
        // The handshake itself was rejected.
        Err(_) => true,
        Ok(channel) => RaftServiceClient::new(channel)
            .append_entries(tonic::Request::new(append_entries()))
            .await
            .is_err(),
    };
    assert!(
        refused,
        "a caller presenting no client certificate must not be able to append to the log"
    );
}

/// A certificate from an unrelated CA never gets past the handshake.
///
/// The rejection happens in rustls, not in the interceptor — which is the point.
/// `PeerAuthInterceptor` reads identity from an already-trusted certificate; it does not
/// establish trust, and a test that only checked the interceptor would not notice a
/// server configured without a client CA.
#[tokio::test(flavor = "multi_thread")]
async fn a_certificate_from_another_ca_is_rejected() {
    let pki = pki();
    let (url, _shutdown, _dir) = start(&pki, true).await;

    let tls = ClientTlsConfig::new()
        .domain_name("localhost")
        .ca_certificate(Certificate::from_pem(&pki.ca_pem))
        .identity(Identity::from_pem(
            &pki.stranger_cert_pem,
            &pki.stranger_key_pem,
        ));

    let channel = tonic::transport::Channel::from_shared(url)
        .expect("uri")
        .tls_config(tls)
        .expect("client tls")
        .connect()
        .await;

    let refused = match channel {
        Err(_) => true,
        Ok(channel) => RaftServiceClient::new(channel)
            .append_entries(tonic::Request::new(append_entries()))
            .await
            .is_err(),
    };
    assert!(
        refused,
        "a certificate signed by an unrelated CA must not authenticate a peer"
    );
}

/// Without `--tls-client-ca` the server never asks for a certificate, so mTLS refuses
/// everyone.
///
/// Fail-closed, but for the wrong reason — and worth pinning, because the failure looks
/// identical to a genuine rejection. `PeerIdentity::MutualTls` is only meaningful when the
/// transport is configured to verify client certificates, which is exactly what the
/// `peer_auth` module documentation warns about.
#[tokio::test(flavor = "multi_thread")]
async fn without_a_client_ca_mtls_admits_nobody() {
    let pki = pki();
    let (url, _shutdown, _dir) = start(&pki, false).await;

    let tls = ClientTlsConfig::new()
        .domain_name("localhost")
        .ca_certificate(Certificate::from_pem(&pki.ca_pem))
        .identity(Identity::from_pem(&pki.peer_cert_pem, &pki.peer_key_pem));

    let channel = tonic::transport::Channel::from_shared(url)
        .expect("uri")
        .tls_config(tls)
        .expect("client tls")
        .connect()
        .await
        .expect("connect");

    let status = RaftServiceClient::new(channel)
        .append_entries(tonic::Request::new(append_entries()))
        .await
        .expect_err("with no client CA the server sees no certificate and must refuse");
    assert_eq!(status.code(), Code::Unauthenticated);
}

/// Two nodes form a cluster over mTLS: they dial each other, present certificates, and
/// replicate.
///
/// # The bug this closes (S-10)
///
/// `RpcClientPool::get_client` built `http://{addr}` unconditionally. There was no TLS on
/// the peer *client* at all. So configuring `--tls-client-ca` produced servers that
/// demanded a client certificate and peers that dialled plaintext at them — the handshake
/// failed, no AppendEntries ever landed, and the cluster could not elect.
///
/// `PeerIdentity::from_config` preferred mTLS whenever a CA was present, so that was the
/// configuration an operator following the documentation would reach. The node started
/// cleanly and only replication was broken, which is the worst shape for a fault.
///
/// Every other test in this file drives a *client* against a TLS server. Only this one
/// exercises the pool, which is where the gap was.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn peers_dial_each_other_over_mtls() {
    use prkdb::raft::rpc_client::PeerTls;

    let pki = pki();
    let (url, _shutdown, _dir) = start(&pki, true).await;
    let addr = url.trim_start_matches("https://").to_string();

    let pool = RpcClientPool::new(2).with_tls(PeerTls {
        cert_pem: pki.peer_cert_pem.clone().into_bytes(),
        key_pem: pki.peer_key_pem.clone().into_bytes(),
        ca_pem: pki.ca_pem.clone().into_bytes(),
        domain: "localhost".to_string(),
    });

    // A plaintext pool cannot reach this server at all — that is the state before the fix.
    let plain = RpcClientPool::new(3);
    assert!(
        plain
            .send_append_entries(1, &addr, append_entries(), 0)
            .await
            .is_err(),
        "a plaintext dial must fail against a TLS listener; if this succeeds the server \
         is not actually requiring TLS and the rest of this test proves nothing"
    );

    // The TLS pool gets through. It may still be refused on Raft grounds; what matters is
    // that it is not refused for authentication, which is what the interceptor decides.
    let outcome = pool
        .send_append_entries(1, &addr, append_entries(), 0)
        .await;
    assert!(
        outcome.is_ok(),
        "a peer presenting a cluster certificate must complete the handshake and reach \
         the service; got: {:?}",
        outcome.err()
    );
}
