//! HTTP authorization, end to end against the built binary.
//!
//! Regression guard for the HTTP half of spec S-01, where `PUT /collections/:name/data`
//! and `DELETE /collections/:name/data/:id` were reachable with no credential at all.

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const BOOTSTRAP: &str = "test-bootstrap-credential";

/// A `prkdb-cli serve` child, killed on drop.
struct Server {
    child: Child,
    base: String,
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

/// Start a server, optionally with a bootstrap credential.
///
/// Returns `None` when the process exits during startup, which is how the
/// refuses-to-start case is asserted.
fn spawn(bootstrap: Option<&str>, extra: &[&str]) -> Option<Server> {
    spawn_seeded(bootstrap, extra, &[])
}

/// As [`spawn`], but writes `seed` principals into the data directory first.
///
/// Possible only because principals are now persisted through the storage layer: before
/// that, the sole way to obtain one was `PRKDB_BOOTSTRAP_TOKEN`, which always mints an
/// admin — so a principal with *insufficient* authority could not be constructed at all,
/// and 403 was untestable.
fn spawn_seeded(
    bootstrap: Option<&str>,
    extra: &[&str],
    seed: &[prkdb::authz::Principal],
) -> Option<Server> {
    let port = free_port();
    let dir = std::env::temp_dir().join(format!("prkdb-authz-{}-{}", std::process::id(), port));
    let _ = std::fs::create_dir_all(&dir);

    if !seed.is_empty() {
        // Seed through the same adapter `prkdb-cli serve` opens in single-node mode.
        // Seeding with `with_data_dir` instead builds a WAL adapter over the same path —
        // a different store entirely, so the server would find nothing and the test would
        // fail for a reason that has nothing to do with authorization.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        runtime.block_on(async {
            let storage = prkdb_storage_sled::SledAdapter::open(dir.to_str().unwrap())
                .expect("open the data directory to seed principals");
            let store = prkdb::authz::PrincipalStore::new();
            for principal in seed {
                store
                    .persist(&storage, principal.clone())
                    .await
                    .expect("seed a principal");
            }
        });
    }

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_prkdb-cli"));
    cmd.args(["--database", dir.to_str().unwrap(), "serve", "--port"])
        .arg(port.to_string())
        .args(extra)
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    match bootstrap {
        Some(token) => {
            cmd.env("PRKDB_BOOTSTRAP_TOKEN", token);
        }
        None => {
            cmd.env_remove("PRKDB_BOOTSTRAP_TOKEN");
        }
    }

    let mut child = cmd.spawn().expect("the CLI binary must launch");
    let base = format!("http://127.0.0.1:{port}");

    // Poll /health rather than sleeping a fixed interval.
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if let Ok(Some(status)) = child.try_wait() {
            // The process is already reaped by try_wait; this is the refuses-to-start
            // path, which the caller asserts on.
            assert!(!status.success() || bootstrap.is_none());
            return None;
        }
        if reqwest::blocking::get(format!("{base}/health")).is_ok() {
            return Some(Server { child, base });
        }
        std::thread::sleep(Duration::from_millis(150));
    }

    let _ = child.kill();
    let _ = child.wait();
    panic!("server did not become healthy within 20s");
}

fn client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("building a blocking HTTP client cannot fail")
}

/// The whole point of S-01: an unauthenticated write must not succeed.
#[test]
fn rejects_a_write_with_no_credential() {
    let Some(srv) = spawn(Some(BOOTSTRAP), &[]) else {
        panic!("server with a bootstrap credential must start");
    };

    let resp = client()
        .put(format!("{}/collections/users/data", srv.base))
        .json(&serde_json::json!({"id": "1", "name": "Alice"}))
        .send()
        .expect("the request must reach the server");
    assert_eq!(
        resp.status(),
        401,
        "an unauthenticated write must be rejected"
    );
}

#[test]
fn rejects_an_unknown_credential() {
    let Some(srv) = spawn(Some(BOOTSTRAP), &[]) else {
        panic!("server must start");
    };

    let resp = client()
        .put(format!("{}/collections/users/data", srv.base))
        .bearer_auth("not-a-real-credential")
        .json(&serde_json::json!({"id": "1"}))
        .send()
        .expect("the request must reach the server");
    assert_eq!(resp.status(), 401, "an unknown credential must be rejected");
}

/// The bootstrap principal holds Admin on `*`, so it may write.
#[test]
fn accepts_the_bootstrap_credential() {
    let Some(srv) = spawn(Some(BOOTSTRAP), &[]) else {
        panic!("server must start");
    };

    let resp = client()
        .get(format!("{}/collections", srv.base))
        .bearer_auth(BOOTSTRAP)
        .send()
        .expect("the request must reach the server");
    assert!(
        resp.status().is_success(),
        "the admin credential must be accepted, got {}",
        resp.status()
    );
}

/// Orchestrators probe these before any client could hold a credential.
#[test]
fn probe_endpoints_stay_public() {
    let Some(srv) = spawn(Some(BOOTSTRAP), &[]) else {
        panic!("server must start");
    };

    let resp = client()
        .get(format!("{}/health", srv.base))
        .send()
        .expect("the request must reach the server");
    assert!(
        resp.status().is_success(),
        "/health must stay reachable without credentials, got {}",
        resp.status()
    );
}

/// Refusing to start unprotected is what stops S-01 recurring by omission.
#[test]
fn refuses_to_start_without_principals() {
    assert!(
        spawn(None, &[]).is_none(),
        "serving with no principals and no --allow-anonymous must abort"
    );
}

/// The escape hatch still exists, and is explicit.
#[test]
fn allow_anonymous_serves_without_credentials() {
    let Some(srv) = spawn(None, &["--allow-anonymous"]) else {
        panic!("--allow-anonymous must start");
    };

    let resp = client()
        .get(format!("{}/collections", srv.base))
        .send()
        .expect("the request must reach the server");
    assert!(
        resp.status().is_success(),
        "--allow-anonymous must serve without a credential, got {}",
        resp.status()
    );
}

/// A valid credential without the right grant must get **403, not 401**.
///
/// The distinction is not cosmetic. 401 tells a client "your credential was not
/// recognised", which invites it to retry with different credentials or re-authenticate;
/// 403 tells it "you are who you say, and it is not enough". Collapsing the two makes a
/// permission bug look like an authentication bug and sends operators looking in the
/// wrong place.
#[test]
fn a_known_credential_without_the_grant_gets_403() {
    use prkdb::authz::{Grant, Permission, Principal};

    let reader = Principal::new(
        "reader",
        "reader-credential",
        vec![Grant::new("users", Permission::Read)],
    );

    let Some(srv) = spawn_seeded(Some(BOOTSTRAP), &[], std::slice::from_ref(&reader)) else {
        panic!("server must start");
    };

    // A read this principal *is* granted must succeed, or the 403 below could just as
    // well mean the credential was never loaded.
    let resp = client()
        .get(format!("{}/collections/users/data", srv.base))
        .bearer_auth("reader-credential")
        .send()
        .expect("the request must reach the server");
    assert!(
        resp.status().is_success(),
        "the seeded principal must be recognised and permitted to read; got {}",
        resp.status()
    );

    // The write it is not granted must be refused as a permission failure.
    let resp = client()
        .put(format!("{}/collections/users/data", srv.base))
        .bearer_auth("reader-credential")
        .json(&serde_json::json!({"id": "1", "name": "Alice"}))
        .send()
        .expect("the request must reach the server");
    assert_eq!(
        resp.status(),
        403,
        "a recognised principal lacking Write must get 403, not {}",
        resp.status()
    );
}

/// Principals seeded into a data directory are still there when the server opens it.
///
/// The end-to-end counterpart of `authz_persistence.rs`: that file proves the store round
/// trips, this proves the shipped binary actually loads it at startup.
#[test]
fn the_server_loads_persisted_principals_at_startup() {
    use prkdb::authz::Principal;

    let admin = Principal::admin("seeded-admin", "seeded-admin-credential");
    let Some(srv) = spawn_seeded(Some(BOOTSTRAP), &[], std::slice::from_ref(&admin)) else {
        panic!("server must start");
    };

    let resp = client()
        .put(format!("{}/collections/users/data", srv.base))
        .bearer_auth("seeded-admin-credential")
        .json(&serde_json::json!({"id": "1", "name": "Alice"}))
        .send()
        .expect("the request must reach the server");
    assert!(
        resp.status().is_success(),
        "a principal persisted before startup must authenticate afterwards; got {}",
        resp.status()
    );
}
