use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Command {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    CreateCollection {
        name: String,
        num_partitions: u32,
        replication_factor: u32,
    },
    DropCollection {
        name: String,
    },
    /// Create or replace an authorization principal.
    ///
    /// # Why principals are a command rather than a `Put`
    ///
    /// A `Put` would replicate the *storage* write and nothing else. Authentication reads
    /// `PrincipalStore`'s in-memory map (`resolve` walks `inner.read()`), which is loaded
    /// once at startup — so a principal replicated only into storage would be invisible to
    /// every node but the one that served the request until it restarted, and a revoke
    /// would leave the credential live everywhere else.
    ///
    /// Applying a dedicated command lets the state machine update the durable copy and the
    /// in-memory copy together, on every node, in log order.
    ///
    /// The principal travels JSON-encoded — the same encoding `PrincipalStore::persist`
    /// writes — so the value in the log and the value on disk are byte-identical, and only
    /// the SHA-256 of the credential is ever carried.
    UpsertPrincipal {
        name: String,
        encoded: Vec<u8>,
    },
    /// Remove an authorization principal on every node.
    RevokePrincipal {
        name: String,
    },
}

impl Command {
    pub fn serialize(&self) -> Vec<u8> {
        bincode::serde::encode_to_vec(self, bincode::config::standard()).unwrap()
    }

    pub fn deserialize(data: &[u8]) -> Option<Self> {
        let (command, _len): (Self, usize) =
            bincode::serde::decode_from_slice(data, bincode::config::standard()).ok()?;
        Some(command)
    }
}
