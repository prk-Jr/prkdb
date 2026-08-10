//! The CLI must be usable against a server that enforces authorization.
//!
//! # What this guards
//!
//! Every remote subcommand built its client as
//!
//! ```rust,ignore
//! PrkDbClient::new(servers).await?.with_admin_token(token)
//! ```
//!
//! but `new` fetches cluster metadata before returning, and `Metadata` requires `Read`. The
//! credential therefore arrived after the call it was needed for, and the whole remote CLI
//! failed against any secured cluster with
//!
//! ```text
//! Error: Failed to fetch metadata from any bootstrap server or cached node
//! ```
//!
//! which names the network and never mentions authorization.
//!
//! It survived every existing test because they all ran the CLI against an anonymous
//! server. This one starts an authorized server, so reverting `remote_client::connect`
//! back to `new(..).with_admin_token(..)` fails it.
//!
//! The negative case is asserted alongside: a subcommand with no credential must still be
//! refused. Without it, the test would also pass against a server that authorizes nothing.

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const BOOTSTRAP: &str = "cli-authorized-bootstrap-credential";

struct Server {
    child: Child,
    grpc: String,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// An ephemeral port, subject to the usual race: the listener is dropped so the child can
/// bind it, and anything else on the machine may take it in between. [`spawn_authorized`]
/// handles that by retrying rather than by pretending the window does not exist.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("binding an ephemeral port cannot fail")
        .local_addr()
        .expect("a bound listener has an address")
        .port()
}

/// Whether the gRPC surface is actually serving, established by making a real
/// authenticated call rather than by connecting a socket.
///
/// A TCP connect proves a listener exists, not that the service behind it answers. This
/// runs the same subcommand the tests run, with a known-good credential, and treats
/// success as readiness.
fn grpc_serving(grpc: &str) -> bool {
    Command::new(env!("CARGO_BIN_EXE_prkdb-cli"))
        .args(["schema", "--server", grpc, "list"])
        .env_remove("PRKDB_ADMIN_TOKEN")
        .env("PRKDB_CREDENTIAL", BOOTSTRAP)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Start `prkdb-cli serve` with a bootstrap credential, so authorization is enforced.
///
/// # Why readiness is a real gRPC call
///
/// The first version polled `/health` only and returned as soon as HTTP answered — but
/// the subcommands under test speak **gRPC**, on a second port. A server not yet serving
/// gRPC therefore read as "ready", and the subcommand failed with `Failed to fetch
/// metadata from any bootstrap server`: character-for-character the error this file
/// exists to catch. The harness could report the very bug it guards against, from a cause
/// with nothing to do with credentials, and in CI it did —
/// `an_admin_token_authenticates_a_subcommand` failed while the other two passed, with the
/// fix under test present and working.
///
/// **The root cause is not proven.** The obvious candidate, a gap between HTTP-ready and
/// gRPC-ready, measures under 10ms locally even with every core saturated, which does not
/// explain it. So readiness is established the only way that is robust regardless of
/// mechanism: by running the subcommand with a known-good credential until it succeeds.
/// A socket connect would only prove a listener exists.
///
/// The spawn is retried because `free_port` is racy by construction: the listener is
/// dropped so the child can bind it, and anything on the machine may take it in between.
fn spawn_authorized() -> Server {
    for attempt in 1..=3 {
        if let Some(server) = try_spawn_authorized() {
            return server;
        }
        eprintln!("server did not come up on attempt {attempt}; retrying with fresh ports");
    }
    panic!(
        "the server never served gRPC to a known-good credential across 3 attempts. \
         If `--credential` itself regressed, this is how it presents — readiness uses \
         that path, so the break surfaces here rather than in \
         `a_bearer_credential_authenticates_a_subcommand`."
    );
}

fn try_spawn_authorized() -> Option<Server> {
    let http_port = free_port();
    let grpc_port = free_port();
    let dir = std::env::temp_dir().join(format!(
        "prkdb-cli-authz-{}-{}",
        std::process::id(),
        http_port
    ));
    let _ = std::fs::create_dir_all(&dir);

    let mut child = Command::new(env!("CARGO_BIN_EXE_prkdb-cli"))
        .args(["--database", dir.to_str().unwrap(), "serve", "--port"])
        .arg(http_port.to_string())
        .arg("--grpc-port")
        .arg(grpc_port.to_string())
        .env("PRKDB_BOOTSTRAP_TOKEN", BOOTSTRAP)
        // Inherited values would silently authorize the negative case.
        .env_remove("PRKDB_ADMIN_TOKEN")
        .env_remove("PRKDB_CREDENTIAL")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("the CLI binary must launch");

    let health = format!("http://127.0.0.1:{http_port}/health");
    // 15s, not 30: the server is ready in about a second, and this deadline is paid three
    // times over on the failure path.
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if let Ok(Some(_status)) = child.try_wait() {
            // Almost always a port taken between `free_port` and the bind. The caller
            // retries with fresh ports; panicking here would turn a race into a failure.
            return None;
        }
        // Both surfaces, not just HTTP: the subcommands under test speak gRPC.
        let grpc = format!("http://127.0.0.1:{grpc_port}");
        if reqwest::blocking::get(&health).is_ok() && grpc_serving(&grpc) {
            return Some(Server { child, grpc });
        }
        std::thread::sleep(Duration::from_millis(150));
    }

    let _ = child.kill();
    let _ = child.wait();
    None
}

/// Run `prkdb-cli schema --server <grpc> list`, optionally authenticated.
fn schema_list(server: &Server, credential_env: Option<(&str, &str)>) -> std::process::Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_prkdb-cli"));
    cmd.args(["schema", "--server", &server.grpc, "list"])
        .env_remove("PRKDB_ADMIN_TOKEN")
        .env_remove("PRKDB_CREDENTIAL");

    if let Some((key, value)) = credential_env {
        cmd.env(key, value);
    }

    cmd.output().expect("the CLI binary must run")
}

/// `--admin-token` reaches the metadata fetch, not just the calls after it.
#[test]
fn an_admin_token_authenticates_a_subcommand() {
    let server = spawn_authorized();
    let out = schema_list(&server, Some(("PRKDB_ADMIN_TOKEN", BOOTSTRAP)));

    assert!(
        out.status.success(),
        "schema list with an admin token must succeed against an authorized server.\n\
         stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// `--credential` works for a principal that is not claiming admin authority.
///
/// Partly tautological, and deliberately so: readiness establishes that the server serves
/// gRPC by making this very call, because no weaker probe (a socket connect, an HTTP
/// health check) distinguishes "listener exists" from "service answers". A regression in
/// this path therefore fails the spawn rather than this assertion — see the panic message
/// in `spawn_authorized`. The assertion stays because the failure it names is the one a
/// reader will look for.
#[test]
fn a_bearer_credential_authenticates_a_subcommand() {
    let server = spawn_authorized();
    let out = schema_list(&server, Some(("PRKDB_CREDENTIAL", BOOTSTRAP)));

    assert!(
        out.status.success(),
        "schema list with a bearer credential must succeed.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Without this the two tests above would pass against a server enforcing nothing.
#[test]
fn no_credential_is_still_refused() {
    let server = spawn_authorized();
    let out = schema_list(&server, None);

    assert!(
        !out.status.success(),
        "an unauthenticated schema list must fail against an authorized server; \
         it succeeded, so the server is not enforcing authorization and the positive \
         tests prove nothing.\nstdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
}
