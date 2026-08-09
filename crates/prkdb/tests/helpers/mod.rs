// Test helper modules
//
// These modules provide infrastructure for chaos and consistency testing.
// Many items are reserved for future test scenarios.
#![allow(dead_code)]

pub mod in_process_cluster;
pub mod jepsen_checker;
pub mod leader_redirect;
pub mod network_simulator;
pub mod test_cluster;
pub mod wgl;

/// Bind an ephemeral port and return it.
///
/// Test binaries in a workspace run concurrently by default, so a hardcoded port is
/// a collision waiting to happen — `compaction_test` and `read_index_test` both used
/// 50001. Worse, a test binary orphaned by a killed run keeps its listeners: on
/// 2026-08-08 a stray `distributed_writes` process held 50081-50083 and made every
/// later run of that test report "No leader elected", which cost twenty minutes of
/// bisecting before anyone ran `lsof`.
///
/// The listener is dropped before the port is returned, so there is a small TOCTOU
/// window. That is acceptable for tests and strictly better than a fixed port, which
/// collides deterministically rather than occasionally.
pub async fn free_port() -> u16 {
    tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("binding an ephemeral port on loopback cannot fail")
        .local_addr()
        .expect("a bound listener always has a local address")
        .port()
}

/// Allocate `n` distinct ephemeral ports.
///
/// All listeners are held until every port is chosen, so the OS cannot hand out the
/// same port twice within one call — which it otherwise can, since each is released
/// as soon as it is probed.
pub async fn free_ports(n: usize) -> Vec<u16> {
    let mut listeners = Vec::with_capacity(n);
    for _ in 0..n {
        listeners.push(
            tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("binding an ephemeral port on loopback cannot fail"),
        );
    }
    listeners
        .iter()
        .map(|l| {
            l.local_addr()
                .expect("a bound listener always has a local address")
                .port()
        })
        .collect()
}

/// Poll `condition` until it holds, or fail naming what was being waited for.
///
/// # Why this exists
///
/// The pattern it replaces is `sleep(Duration::from_secs(3))` followed by an assertion.
/// That is wrong in both directions at once: on a loaded CI runner three seconds is not
/// enough and the test fails for no reason, while on a fast machine it wastes three
/// seconds every run. It also reports "assertion failed" rather than "no leader was
/// elected within 10s", which is the sentence someone reading CI output actually needs.
///
/// The failure message names the condition, so a timeout says what did not happen.
pub async fn await_condition<F, Fut>(what: &str, within: std::time::Duration, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + within;
    loop {
        if condition().await {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out after {within:?} waiting for: {what}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// Run `op`, giving up after `within`.
///
/// Cluster operations against a partitioned node do not fail fast — they wait on RPC
/// timeouts per unreachable peer, which turned one register test from three seconds into
/// ninety. A test that means "try this, and treat not finishing as a failure of the
/// operation" should say so rather than inherit whatever the transport's timeouts add up
/// to.
pub async fn within<T, Fut>(limit: std::time::Duration, op: Fut) -> Result<T, String>
where
    Fut: std::future::Future<Output = T>,
{
    match tokio::time::timeout(limit, op).await {
        Ok(value) => Ok(value),
        Err(_) => Err(format!("operation did not complete within {limit:?}")),
    }
}

#[allow(unused_imports)]
pub use jepsen_checker::{
    BankAccounts, InvariantResult, LinearizabilityResult, OpKind, OpResult, Operation,
    OperationHistory,
};
// Each test binary compiles this module in full, so a re-export any one of them does
// not use is a warning there. Same reason as the other re-exports below.
#[allow(unused_imports)]
pub use in_process_cluster::{InProcessCluster, ReadConsistency};
pub use network_simulator::NetworkSimulator;
#[allow(unused_imports)]
pub use test_cluster::TestCluster;
