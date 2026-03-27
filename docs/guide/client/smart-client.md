# Smart Client

When developing external microservices that interact with a PrkDB database deployment, you typically do not want to embed the massive storage, consensus, and WAL engine logic directly into your app.

For these situations, you will utilize the standalone `prkdb-client`. The Smart Client contains zero storage logic, compiling exclusively the wire protocol, routing, and high-availability modules.

## Initialization

The Client library only accepts a cluster's seed URLs. Once connected, the client performs a background cluster metadata exchange, learning about your cluster's partition routing, current leaders, and replicas.

```rust
use prkdb_client::{ClientConfig, PrkDbClient};

let config = ClientConfig {
    max_retries: 5,
    max_connections_per_node: 8,
    unhealthy_threshold: 3,
    ..Default::default()
};

// Provide a seed node. The client will discover the rest!
let client = PrkDbClient::with_config(vec!["http://127.0.0.1:8080".into()], config).await?;
```

Use any dialable `prkdb-server` node as the seed. For local development with `prkdb-cli serve`, use its gRPC endpoint such as `http://127.0.0.1:50051`.

## Features and Resilience

### Topology Routing Middleware
The smart client caches the topological layout of the entire cluster. When you request a write mapped to `Partition 2`, the client automatically directs the gRPC request straight to the **Leader of Partition 2**, avoiding secondary network hops from blind proxies.

### Head-Of-Line Blocking Prevention
Rather than utilizing single synchronous pipes, the smart client spawns and manages a distinct asynchronous connection pool (`max_connections_per_node`) for every peer in the cluster, multiplying available network throughput to Kafka-beating levels.

### Dynamic Health Checks
The client actively monitors the success rates of its node connections. If a node suddenly crash-loops or drops below the `unhealthy_threshold`, the client temporarily blocks routes to that address, automatically hunting for the new Raft leader until the topology stabilizes.

## Read Consistency Modes

While write operations are exclusively forwarded to partition leaders, read operations give you fine-grained control over network load using `ReadConsistency`.

```rust
use prkdb_client::ReadConsistency;

// Linearizable read from the leader
let latest = client.get(b"user_profile").await?;

// Linearizable follower read using ReadIndex under the hood
let replica_read = client
    .get_with_consistency(b"user_profile", ReadConsistency::Follower)
    .await?;

// Fast local read that may be stale
let stale = client
    .get_with_consistency(b"user_profile", ReadConsistency::Stale)
    .await?;
```
