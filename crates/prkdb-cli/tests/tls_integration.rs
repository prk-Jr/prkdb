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

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Server {
    child: Child,
    port: u16,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("binding an ephemeral port cannot fail")
        .local_addr()
        .expect("a bound listener has an address")
        .port()
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

fn spawn_https(dir: &std::path::Path) -> Option<Server> {
    let (cert, key) = write_cert(dir);
    let port = free_port();
    let data = dir.join("db");

    let mut child = Command::new(env!("CARGO_BIN_EXE_prkdb-cli"))
        .args(["--database", data.to_str().unwrap(), "serve", "--port"])
        .arg(port.to_string())
        .arg("--grpc-port")
        .arg(free_port().to_string())
        .args(["--tls-cert", cert.to_str().unwrap()])
        .args(["--tls-key", key.to_str().unwrap()])
        // Anonymous on purpose: this test is about the transport, and a credential
        // requirement would make a 401 indistinguishable from a TLS failure.
        .arg("--allow-anonymous")
        .env_remove("PRKDB_BOOTSTRAP_TOKEN")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("the CLI binary must launch");

    let client = reqwest::blocking::Client::builder()
        .danger_accept_invalid_certs(true) // self-signed
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build a TLS client");

    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(Some(_)) = child.try_wait() {
            return None; // port taken between free_port and bind; caller retries
        }
        if client
            .get(format!("https://127.0.0.1:{port}/health"))
            .send()
            .is_ok()
        {
            return Some(Server { child, port });
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    let _ = child.kill();
    let _ = child.wait();
    None
}

/// The listener speaks TLS, and a plaintext client is refused.
///
/// Both halves matter. Serving over HTTPS alone would pass against a server that also
/// accepted plaintext — which is not TLS, it is TLS-optional, and an eavesdropper picks
/// the option. The plaintext assertion is what makes the first one mean something.
#[test]
fn the_https_listener_serves_tls_and_refuses_plaintext() {
    let dir = tempfile::tempdir().expect("tempdir");

    let mut server = None;
    for _ in 0..3 {
        server = spawn_https(dir.path());
        if server.is_some() {
            break;
        }
    }
    let server = server.expect("the HTTPS listener must come up within 3 attempts");

    let tls = reqwest::blocking::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let response = tls
        .get(format!("https://127.0.0.1:{}/health", server.port))
        .send()
        .expect("an HTTPS request must succeed against a TLS listener");
    assert!(
        response.status().is_success(),
        "HTTPS /health returned {}",
        response.status()
    );

    // The same port over plaintext must not answer. A TLS listener handed an unencrypted
    // request fails the handshake; anything else means traffic a user believes is
    // encrypted is not.
    let plain = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let outcome = plain
        .get(format!("http://127.0.0.1:{}/health", server.port))
        .send();
    assert!(
        outcome.is_err(),
        "a plaintext request succeeded against the TLS listener; the transport is not \
         actually encrypted"
    );
}
