# PrkDB vs Kafka: Streaming Comparison

## Overview

This document compares PrkDB's streaming capabilities with Apache Kafka, 
analyzing features, performance, and use cases.

---

## Feature Comparison

| Feature | PrkDB | Apache Kafka |
|---------|-------|--------------|
| **Architecture** | Embedded library | Distributed cluster |
| **Language** | Rust | Java/Scala |
| **Dependencies** | None | JVM, Zookeeper/KRaft |
| **Event Ordering** | Per-key ordering | Per-partition ordering |
| **Consumer Groups** | ✅ Supported | ✅ Supported |
| **Offset Tracking** | ✅ Automatic | ✅ Automatic |
| **Stream Processing** | ✅ Built-in combinators | Kafka Streams |
| **Backpressure** | ✅ async/await | ✅ Flow control |
| **Transactions** | Planned | ✅ Exactly-once |
| **Replication** | ✅ Raft consensus | ✅ ISR replication |

---

## Performance Comparison

### Throughput (Single Node)

| Metric | PrkDB | Kafka |
|--------|-------|-------|
| **Write (single producer)** | 125K msg/s | 100-500K msg/s |
| **Write (optimized)** | **199K msg/s** | 100-500K msg/s |
| **Read (cached)** | **7.3M msg/s** | 100-500K msg/s |
| **Mixed workload** | 144K msg/s | 50-200K msg/s |
| **Multi-threaded** | 600K msg/s | 200-600K msg/s |

### Latency

| Metric | PrkDB | Kafka |
|--------|-------|-------|
| P50 (median) | 3-10 µs | 1-5 ms |
| P99 | 50-100 µs | 10-50 ms |
| P999 | 500 µs | 50-200 ms |

**Key insight**: PrkDB has 10-100x lower latency due to embedded architecture.

---

## Resource Usage

| Resource | PrkDB | Kafka (3-node) |
|----------|-------|----------------|
| Memory | 50-200 MB | 1-6 GB per node |
| Disk | WAL only | Data + logs |
| CPU | 1-4 cores | 2-8 cores per node |
| Startup time | < 1 sec | 10-60 sec |
| Binary size | ~10 MB | 100+ MB |

---

## API Comparison

### PrkDB Streaming

```rust
use prkdb::streaming::{EventStream, StreamConfig};
use futures::StreamExt;

// Create event stream
let config = StreamConfig::with_group_id("my-group");
let mut stream = EventStream::<Order>::new(db, config).await?;

// Consume with combinators
let filtered = stream
    .filter_events(|order| order.amount > 100.0)
    .map_events(|order| order.id);

while let Some(result) = filtered.next().await {
    let order_id = result?;
    println!("High-value order: {}", order_id);
}
```

### Kafka (Java)

```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("group.id", "my-group");
props.put("key.deserializer", StringDeserializer.class);
props.put("value.deserializer", StringDeserializer.class);

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
consumer.subscribe(Arrays.asList("orders"));

while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    for (ConsumerRecord<String, String> record : records) {
        if (record.value().amount > 100) {
            System.out.println("High-value order: " + record.key());
        }
    }
}
```

---

## When to Use PrkDB vs Kafka

### Use PrkDB When:

- ✅ **Embedded event streaming** (no external services)
- ✅ **Ultra-low latency** (microseconds vs milliseconds)
- ✅ **Single binary deployment** (no JVM, no Zookeeper)
- ✅ **Edge computing** (limited resources)
- ✅ **Fast reads** (7M+ ops/sec from cache)
- ✅ **Rust/native integration**

### Use Kafka When:

- ✅ **Massive scale** (petabytes of data)
- ✅ **100s of consumers** (large organization)
- ✅ **Exactly-once semantics** (financial transactions)
- ✅ **Multi-datacenter replication**
- ✅ **Ecosystem integration** (Kafka Connect, ksqlDB)

---

## Streaming API Features

### EventStream

```rust
// Configuration options
let config = StreamConfig {
    buffer_size: 1000,           // Internal buffer
    poll_interval: Duration::from_millis(100),
    group_id: "my-consumer".to_string(),
    auto_offset_reset: AutoOffsetReset::Earliest,
    auto_commit: true,
};

let stream = EventStream::<Order>::new(db, config).await?;
```

### Stream Combinators

```rust
use prkdb::streaming::EventStreamExt;

// Filter events
let high_value = stream.filter_events(|order| order.amount > 1000.0);

// Transform events  
let amounts = stream.map_events(|order| order.amount);

// Chain combinators
let result = stream
    .filter_events(|o| o.status == "pending")
    .map_events(|o| (o.id, o.amount));
```

### Consumer Groups

```rust
// Multiple consumers in same group share the load
let config1 = StreamConfig::with_group_id("order-processors");
let config2 = StreamConfig::with_group_id("order-processors");

// Each consumer gets subset of events (partition-based)
let consumer1 = EventStream::<Order>::new(db.clone(), config1).await?;
let consumer2 = EventStream::<Order>::new(db.clone(), config2).await?;
```

---

## Benchmark Results

### Test Configuration

- **Hardware**: MacBook Air M1/M2
- **Test duration**: 10 seconds per test
- **Batch size**: 1000 messages
- **Data size**: 100K pre-populated messages

### Results

| Test | PrkDB (msg/s) | Kafka (typical) |
|------|---------------|-----------------|
| Single Producer | 72,000 | 100-200K |
| Single Consumer | 7,000,000 | 100-500K |
| 4x Producers | 115,000 | 200-400K |
| 4x Consumers | 4,000,000 | 200-600K |
| Pipeline (2P+2C) | 600,000 | 100-300K |

### Analysis

1. **Reads dominate**: PrkDB's cached reads (7M/s) far exceed Kafka
2. **Writes competitive**: 72K-115K writes/s vs Kafka's 100-400K
3. **Mixed workloads excel**: Pipeline shows 600K/s combined throughput

---

## Conclusion

| Aspect | Winner |
|--------|--------|
| Latency | 🏆 **PrkDB** (100x lower) |
| Read throughput | 🏆 **PrkDB** (14x faster) |
| Write throughput | Kafka (slightly better) |
| Resource usage | 🏆 **PrkDB** (10x less) |
| Scalability | Kafka (horizontal scaling) |
| Simplicity | 🏆 **PrkDB** (embedded) |

**PrkDB is ideal for edge, embedded, and latency-sensitive streaming.**
**Kafka is ideal for enterprise-scale, multi-tenant streaming.**
