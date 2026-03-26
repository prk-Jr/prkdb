# Partitions & Sharding

PrkDB uses advanced partitioning strategies to distribute data across the cluster. This allows the database to scale horizontally, handling massive throughput and storage requirements that exceed the capacity of a single node.

## Partitioning Strategies

When initializing a new `PrkDb` instance, you can choose between two main partitioning strategies:

### 1. Consistent Hashing (Default)
Consistent Hashing minimizes data movement when nodes are added or removed from the cluster. It distributes data using a hash ring (often with virtual nodes to ensure even distribution).

```rust
use prkdb::raft::{ConsistentHashRing, PartitionStrategy};

// Create a ring with 3 partitions and 150 virtual nodes per partition
let ring = ConsistentHashRing::new(3, 150);

let db = PrkDb::builder()
    .with_data_dir("./data")
    .with_partition_strategy(Box::new(ring))
    .build()
    .unwrap();
```
**Best for**: Highly distributed workloads where keys are accessed uniformly and cluster topology might change.

### 2. Range Partitioning
Range Partitioning assigns contiguous blocks of keys to specific partitions. This strategy is extremely efficient for time-series data or when you frequently perform **range queries** (e.g., fetching all users with an age between 18 and 30).

```rust
use prkdb::raft::{RangePartitioner, PartitionStrategy};

// Create a partitioner with 3 initial partitions
let mut partitioner = RangePartitioner::new(3);

let db = PrkDb::builder()
    .with_data_dir("./data")
    .with_partition_strategy(Box::new(partitioner))
    .build()
    .unwrap();
```
**Best for**: Ordered access patterns and heavy range-scan workloads.

## Managing Hotspots

Sometimes, a specific subset of your data becomes significantly more popular than the rest, leading to a "hotspot" that overwhelms a single partition.

When using the `RangePartitioner`, you can dynamically split a busy partition to distribute the load:

```rust
// Split partition 0 at the key "middle_key"
partitioner.split_partition(0, b"middle_key").unwrap();
```

## Client-Side Routing

To avoid unnecessary network hops (where a node receives a write request for a partition it doesn't own and has to proxy it), PrkDB's Smart Client (`prkdb-client`) leverages **topology caching**.

The client fetches the partition map from the cluster and automatically routes point-lookup and write requests directly to the leader of the correct partition.

```rust
// The client knows which node holds the partition for "user:123"
let user = client.get("user:123").await?; 
```
