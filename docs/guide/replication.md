# PrkDB Replication Guide

PrkDB v2 uses **Multi-Raft Consensus** to provide strong consistency, high availability, and horizontal scalability.

## Overview

Unlike traditional primary-replica systems, PrkDB uses the Raft consensus algorithm to ensure that data is safely replicated across a quorum of nodes before it is considered committed.

- **Strong Consistency**: Writes are acknowledged only after being replicated to a majority of nodes.
- **Automatic Failover**: If a leader node fails, the cluster automatically elects a new leader without human intervention.
- **Member Changes**: Nodes can be added or removed dynamically (planned feature).

## Architecture

PrkDB partitions data into multiple **Raft Groups** (Shards). Each partition has its own Raft consensus group, allowing the cluster to scale writes horizontally.

```mermaid
graph TD
    Client -->|Put key="user:1"| Proxy
    Proxy -->|Hash("user:1")| Partition1[Partition 1 Leader]

    subgraph "Partition 1 (Raft Group)"
        Partition1 -->|Replicate| P1Follower1[Follower A]
        Partition1 -->|Replicate| P1Follower2[Follower B]
    end
```

### Roles

- **Leader**: Handles all writes and linearizable reads for a specific partition.
- **Follower**: Replicates logs from the leader. Can serve stale reads or linearizable reads (via `ReadIndex`).
- **Candidate**: A node trying to become a leader during an election.

## Consistency Modes

PrkDB supports tunable consistency levels for reads, allowing you to balance performance and correctness.

| Mode                       | Description                                                                                      | Latency | Consistency |
| -------------------------- | ------------------------------------------------------------------------------------------------ | ------- | ----------- |
| **Linearizable** (Default) | Reads from Leader. Guarantees you see the latest committed data.                                 | Medium  | Strong      |
| **Follower**               | Reads from Follower using `ReadIndex`. Guarantees linearizability but offloads work from Leader. | Medium  | Strong      |
| **Stale**                  | Reads locally from any node. Fastest, but data might be slightly outdated.                       | Low     | Eventual    |

### Code Example

```rust
use prkdb::client::{PrkDbClient, ReadMode};

let client = PrkDbClient::new(vec!["http://localhost:8080".to_string()]).await?;

// Linearizable Read (Default)
let val = client.get(b"key").await?;

// Stale Read (Low Latency)
let val = client.get_opts(b"key", ReadMode::Stale).await?;
```

## Network Transport

Replication traffic uses **gRPC** for high performance and strict typing.

- **Heartbeats**: Leaders send periodic heartbeats to maintain authority.
- **AppendEntries**: Log entries are batched and streamed to followers.
- **Snapshots**: Compacted logs are sent as snapshots to slow followers.

## Monitoring

Key metrics to watch:

- `prkdb_raft_current_term`: High churn indicates frequent elections (instability).
- `prkdb_raft_commit_index`: Should closely track `last_log_index`.
- `prkdb_raft_voted_for`: Who this node voted for.

See [Metrics Guide](./metrics.md) for a full list.

## Roadmap Status

### Completed (v2.0)

- [x] Multi-Raft Consensus (Vote, AppendEntries, Heartbeat)
- [x] Log Compaction & Snapshotting
- [x] Linearizable & Follower Reads
- [x] gRPC Transport

### Upcoming

- [ ] TLS Mutual Authentication for Intra-cluster communication
- [ ] Multi-region federation
- [ ] Dynamic Split/Merge of partitions
