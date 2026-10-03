//! TLS reachability from the shipped binary.
//!
//! Regression guard for spec S-02: `start_raft_server_tls` implemented full mTLS, but its
//! only caller in the workspace was an example, so no binary a user actually runs could
//! enable it. A `certs/` directory in the repository implied otherwise.
//!
//! `prkdb-cli` has no library target, so the argument-validation logic is unit-tested
//! inside `src/tls.rs`. What only an integration test can establish is the thing S-02 was
//! actually about: that the capability is reachable from the command line.

/// A struct field nothing parses is precisely the shape of the S-02 defect — capability
/// present in the source, unreachable in practice.
#[test]
fn serve_exposes_the_tls_flags() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_prkdb-cli"))
        .args(["serve", "--help"])
        .output()
        .expect("the CLI binary must run");

    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    for flag in ["--tls-cert", "--tls-key", "--tls-client-ca"] {
        assert!(
            text.contains(flag),
            "`serve --help` must advertise {flag}; without it the TLS implementation is \
             unreachable, which is the defect S-02 records"
        );
    }
}

/// `--tls-cert` without `--tls-key` must be rejected by argument parsing rather than
/// producing a server that quietly serves plaintext.
#[test]
fn a_half_configured_pair_is_rejected_at_the_command_line() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_prkdb-cli"))
        .args(["serve", "--tls-cert", "/nonexistent/server.crt"])
        .output()
        .expect("the CLI binary must run");

    assert!(
        !out.status.success(),
        "a certificate with no key must fail rather than start"
    );

    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        text.contains("--tls-key"),
        "the error should name the missing flag, got: {text}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// The HTTPS listener actually serving (R13.3)
//
// Everything above establishes that the flags exist and are validated. None of it starts
// a server. The module doc concedes as much: it establishes "that the capability is
// reachable from the command line".
//
// The Raft half is properly covered — `crates/prkdb/tests/peer_mtls.rs` drives
// `RpcClientPool` against a TLS listener and asserts a plaintext pool fails. The HTTP half
// had no equivalent, which is the S-02/S-10 shape twice over: a capability exercised on
// the side that works.
// ═══════════════════════════════════════════════════════════════════════════

use std::time::Duration;

#[path = "common/server.rs"]
pub mod server;

struct Server {
    _process: server::ServerProcess,
    port: u16,
}

/// A self-signed certificate for 127.0.0.1, written to disk because `serve` takes paths.
fn write_cert(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let cert =
        rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string(), "localhost".into()])
            .expect("generate a self-signed certificate");
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).expect("write cert");
    std::fs::write(&key_path, cert.key_pair.serialize_pem()).expect("write key");
    (cert_path, key_path)
}

async fn spawn_https(dir: &std::path::Path) -> Server {
    use prkdb_proto::raft::{prk_db_service_client::PrkDbServiceClient, MetadataRequest};
    use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

    let (cert, key) = write_cert(dir);
    let mut process = server::ServerProcess::spawn(&[
        "--port",
        "0",
        "--grpc-port",
        "0",
        "--tls-cert",
        cert.to_str().unwrap(),
        "--tls-key",
        key.to_str().unwrap(),
    ]);
    let listening = process.listening().await.expect("TLS listening record");
    assert_eq!(
        listening.http_url,
        format!("https://{}", listening.http_address)
    );
    assert_eq!(
        listening.grpc_url,
        format!("https://{}", listening.grpc_address)
    );
    tokio::time::timeout(Duration::from_secs(30), async {
        let channel = Endpoint::from_shared(listening.grpc_url.clone())
            .unwrap()
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(std::fs::read(&cert).unwrap()))
                    .domain_name("localhost"),
            )
            .unwrap()
            .connect()
            .await
            .expect("the reported gRPC endpoint must speak TLS");
        let metadata = PrkDbServiceClient::new(channel)
            .metadata(MetadataRequest { topics: vec![] })
            .await
            .expect("gRPC metadata over TLS")
            .into_inner();
        assert_eq!(metadata.nodes.len(), 1);
        assert_eq!(metadata.nodes[0].address, listening.grpc_url);
    })
    .await
    .expect("TLS endpoint must serve within the startup deadline");
    Server {
        _process: process,
        port: listening.http_address.port(),
    }
}

/// The listener speaks TLS, and a plaintext client is refused.
///
/// Both halves matter. Serving over HTTPS alone would pass against a server that also
/// accepted plaintext — which is not TLS, it is TLS-optional, and an eavesdropper picks
/// the option. The plaintext assertion is what makes the first one mean something.
#[tokio::test]
async fn the_https_listener_serves_tls_and_refuses_plaintext() {
    let dir = tempfile::tempdir().expect("tempdir");

    let server = spawn_https(dir.path()).await;

    let tls = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let response = tls
        .get(format!("https://127.0.0.1:{}/health", server.port))
        .send()
        .await
        .expect("an HTTPS request must succeed against a TLS listener");
    assert!(
        response.status().is_success(),
        "HTTPS /health returned {}",
        response.status()
    );

    // The same port over plaintext must not answer. A TLS listener handed an unencrypted
    // request fails the handshake; anything else means traffic a user believes is
    // encrypted is not.
    let plain = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let outcome = plain
        .get(format!("http://127.0.0.1:{}/health", server.port))
        .send()
        .await;
    assert!(
        outcome.is_err(),
        "a plaintext request succeeded against the TLS listener; the transport is not \
         actually encrypted"
    );
}
