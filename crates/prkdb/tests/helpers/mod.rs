// Test helper modules
//
// These modules provide infrastructure for chaos and consistency testing.
// Many items are reserved for future test scenarios.
#![allow(dead_code)]

pub mod jepsen_checker;
pub mod leader_redirect;
pub mod network_simulator;
pub mod test_cluster;

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

#[allow(unused_imports)]
pub use jepsen_checker::{
    BankAccounts, InvariantResult, LinearizabilityResult, OpKind, OpResult, Operation,
    OperationHistory,
};
pub use network_simulator::NetworkSimulator;
#[allow(unused_imports)]
pub use test_cluster::TestCluster;
