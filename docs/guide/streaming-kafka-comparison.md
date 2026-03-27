# PrkDB and Kafka: Streaming Comparison

## Overview

This document compares PrkDB's streaming capabilities with Apache Kafka,
focusing on architecture, operational tradeoffs, and use cases.

---

## Feature Comparison

| Feature               | PrkDB                   | Apache Kafka           |
| --------------------- | ----------------------- | ---------------------- |
| **Architecture**      | Embedded library        | Distributed cluster    |
| **Language**          | Rust                    | Java/Scala             |
| **Dependencies**      | None                    | JVM, Zookeeper/KRaft   |
| **Event Ordering**    | Per-key ordering        | Per-partition ordering |
| **Consumer Groups**   | ✅ Supported            | ✅ Supported           |
| **Offset Tracking**   | ✅ Automatic            | ✅ Automatic           |
| **Stream Processing** | ✅ Built-in combinators | Kafka Streams          |
| **Backpressure**      | ✅ async/await          | ✅ Flow control        |
| **Transactions**      | Planned                 | ✅ Exactly-once        |
| **Replication**       | ✅ Raft consensus       | ✅ ISR replication     |

---

## Benchmark Notes

The repo CI publishes two separate kinds of measurements:

- local PrkDB storage-engine benchmarks written in native Rust
- single-broker Kafka reference runs using the official perf tools

Those results are useful for tracking regressions inside this repo, but they are not a fair head-to-head system comparison. Kafka numbers include a networked broker and protocol/tooling overhead; the PrkDB numbers are local storage-engine paths. Use the raw artifacts as reference points, not as proof that one system universally outperforms the other.

### 🔬 Methodology

- **Records**: 1,000,000 (standard) / 10,000,000 (sustained)
- **Record Size**: 100 bytes
- **Batch Size**: 10,000
- **Environment**: GitHub Actions (`ubuntu-latest`)
- **Kafka lane**: official `kafka-producer-perf-test` and `kafka-consumer-perf-test` against a single broker
- **PrkDB lane**: native Rust local benchmarks over mmap/WAL storage
- **Interpretation**: suitable for internal trend tracking, not apples-to-apples product claims

---

## Resource Usage

| Resource     | PrkDB     | Kafka (3-node)     |
| ------------ | --------- | ------------------ |
| Memory       | 50-200 MB | 1-6 GB per node    |
| Disk         | WAL only  | Data + logs        |
| CPU          | 1-4 cores | 2-8 cores per node |
| Startup time | < 1 sec   | 10-60 sec          |
| Binary size  | ~10 MB    | 100+ MB            |

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
- ✅ **Low-latency local pipelines**
- ✅ **Single binary deployment** (no JVM, no Zookeeper)
- ✅ **Edge computing** (limited resources)
- ✅ **Fast local reads and replay**
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

## Conclusion

| Aspect           | Winner                                   |
| ---------------- | ---------------------------------------- |
| Local deployment | 🏆 **PrkDB** (embedded or single binary) |
| Resource usage   | 🏆 **PrkDB** (10x less)                  |
| Scalability      | Kafka (horizontal scaling)               |
| Simplicity       | 🏆 **PrkDB** (embedded or single binary) |

**PrkDB is ideal for high-performance local streaming and embedded deployments.**
**Kafka remains the standard for massive, multi-tenant enterprise data hubs.**
