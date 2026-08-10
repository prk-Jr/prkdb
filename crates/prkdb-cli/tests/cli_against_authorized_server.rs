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

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("binding an ephemeral port cannot fail")
        .local_addr()
        .expect("a bound listener has an address")
        .port()
}

/// Start `prkdb-cli serve` with a bootstrap credential, so authorization is enforced.
fn spawn_authorized() -> Server {
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
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(Some(status)) = child.try_wait() {
            panic!("server exited during startup: {status}");
        }
        if reqwest::blocking::get(&health).is_ok() {
            return Server {
                child,
                grpc: format!("http://127.0.0.1:{grpc_port}"),
            };
        }
        std::thread::sleep(Duration::from_millis(150));
    }

    let _ = child.kill();
    let _ = child.wait();
    panic!("server did not become healthy within 30s");
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
