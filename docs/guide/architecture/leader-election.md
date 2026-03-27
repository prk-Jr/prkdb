# Leader Election & Raft

At the heart of PrkDB's distributed architecture is the **Raft Consensus Protocol**. Raft ensures that all nodes in your cluster agree on the state of the data, providing strong consistency and high availability even in the face of network partitions or server crashes.

## How It Works

A PrkDB cluster consists of multiple nodes (typically 3, 5, or 7). For each data partition, one node is selected as the **Leader**, and the others act as **Followers**.

1. **Client Writes**: All write operations are routed to the partition Leader.
2. **Replication**: The Leader appends the write to its local Write-Ahead Log (WAL) and concurrently sends it to the Followers.
3. **Quorum Commit**: Once a majority (quorum) of the nodes have safely written the entry, the Leader commits the write and responds to the client.

## Pre-Vote Protocol

PrkDB implements the Raft **Pre-Vote protocol** to stabilize the cluster and prevent disruptive elections. 

Normally, if a follower gets disconnected by a brief network partition, its election timer expires, and it starts a new term. When the network heals, its higher term forces the current leader to step down, disrupting operations.

With Pre-Vote, the isolated node first asks the cluster if it *would* win an election before actually disrupting the current term. If the rest of the cluster is still happily communicating with the active leader, they reject the pre-vote, and the isolated node simply rejoins as a follower once the network heals.

## Split-Brain Protection

Because PrkDB requires a strict majority quorum to commit writes or elect a leader, it is mathematically impossible for the cluster to suffer from "split-brain" (where two nodes both think they are the leader and accept divergent writes).

If a 5-node cluster splits into a 3-node group and a 2-node group:
- The 3-node group maintains a majority and continues operating normally.
- The 2-node group loses its majority. If it contains the old leader, it will be unable to commit new writes and will cleanly timeout.

## Read Consistency Levels

Because replication takes time, PrkDB offers configurable read modes to balance latency against strict consistency:

### 1. Linearizable (Default)
Ensures that a read always reflects the most recent acknowledged write. This requires contacting the leader, which may need to verify its leadership with a majority of followers before responding.

### 2. Follower Reads (Linearizable via ReadIndex)
Permits reads from a follower node, while still using `ReadIndex` coordination with the leader to preserve linearizability. This can reduce leader load without weakening correctness guarantees.

### 3. Stale Reads
Reads directly from local state without `ReadIndex`. This is fastest, but can lag behind the latest committed write.

```rust
use prkdb_client::{PrkDbClient, ReadConsistency};

let client = PrkDbClient::new(vec!["http://127.0.0.1:8080".to_string()]).await?;

let latest = client.get(b"account_balance").await?;
let follower = client
    .get_with_consistency(b"user_profile", ReadConsistency::Follower)
    .await?;
let stale = client
    .get_with_consistency(b"user_profile", ReadConsistency::Stale)
    .await?;
```
