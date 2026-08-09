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
