# prkdb-core

**Core storage engine and WAL implementation for PrkDB**

## Overview

`prkdb-core` provides the foundational storage primitives for PrkDB, including a high-performance Write-Ahead Log (WAL) and pluggable storage backends. This crate contains no distributed consensus logic - just the core building blocks.

## Performance

- ✅ **4.2M msgs/sec** - WAL write throughput (local, single-threaded benchmark)
- ✅ **Zero async overhead** - Dedicated sync writer thread
- ✅ **Intelligent batching** - 10ms linger with 1000-entry batches
- ✅ **Lock-free queue** - Crossbeam-based async-to-sync bridge

## Key Components

### Write-Ahead Log (WAL)

One implementation since Task 2.9 (D13): `wal::Wal`, a single globally ordered log
written by one group-commit writer thread through the `Vfs` seam, with `Durable`
(acknowledge after sync) and `Fast` (acknowledge after write, sync every
`sync_interval`) modes. The memory-mapped, parallel and async WALs it replaced are
deleted. Most callers use it through `prkdb::storage::WalStorageAdapter`.

### Batch Configuration

Kafka-style batching for throughput optimization:

```rust
use prkdb_core::batch_config::BatchConfig;

let config = BatchConfig {
    linger_ms: 10,                    // Wait up to 10ms
    max_batch_size: 1000,             // Or 1000 entries
    max_buffer_bytes: 1024 * 1024,    // Or 1MB
    compression: CompressionConfig::none(),
};
```

**Presets**:
- `BatchConfig::default()` - Balanced (10ms, 1000 entries)
- `BatchConfig::latency_optimized()` - Low latency (1ms, 100 entries)
- `BatchConfig::throughput_optimized()` - High throughput (50ms, 10K entries)

### Storage Backends

Pluggable storage layer:

- Memory-mapped files (mmap)
- Async file I/O (tokio)
- Extensible trait-based design

## Architecture Highlights

### Dedicated Sync Writer Thread

Eliminates Tokio async overhead in the critical write path:

```
Async World          Sync World
   │                     │
   ├─ put() ────────────►│
   │  (async)            │
   │                     ├─ Batch writes
   │                     ├─ Sync to disk
   │◄────────────────────┤
      (oneshot)          │
```

Benefits:
- No async/await overhead
- No executor contention
- Predictable latency
- Maximum throughput

### Lock-Free Write Queue

Uses `crossbeam-channel` for zero-contention async-to-sync communication:

```rust
// Async caller
tx.send(WriteRequest { record, response_tx })?;

// Sync writer thread
for req in rx.iter() {
    batch.push(req);
    if should_flush() {
        flush_batch(&batch);
    }
}
```

## Features

### Current
- [x] Batch processing
- [x] Compression support (LZ4, Snappy, Zstd)
- [x] CRC32 checksums
- [x] Sync writer thread
- [x] Lock-free queues

### Planned
- [ ] Snapshot support
- [ ] Compaction
- [ ] Read-ahead buffering
- [ ] Bloom filters

## ✅ Verified Implementation

**Confirmed working** (based on source code inspection):

1. **WAL** ✅
   - `wal::Wal` (`src/wal/log.rs`), the only WAL implementation since Task 2.9

2. **Sync Writer** ✅
   - `write_tx` channel in `wal_adapter.rs`
   - Dedicated writer thread spawned
   - Lock-free queue using crossbeam

3. **Batch Processing** ✅
   - `BatchConfig` with presets
   - Linger/batch size configuration
   - Multiple batch strategies

4. **Performance** ✅
   - 4.2M msgs/sec measured in benchmarks
   - 22 benchmark files in workspace
   - `perf_test_batching_wal.rs` example exists

**Partially Implemented** ⚠️:
- Compaction - planned (remediation Task 2.15)

## Building

```bash
cd crates/prkdb-core
cargo build --release
```

## Testing

```bash
# Run tests
cargo test

# Run benchmarks
cargo bench
```

## Benchmarks

The WAL write-path comparison bench is `crates/prkdb/benches/wal_write_path.rs`
(`cargo bench -p prkdb --bench wal_write_path`); this crate has no benches of its own
since Task 2.9.

## Examples

See `examples/` directory:
- `perf_test_batching_wal.rs` - Performance testing
- `simple_wal.rs` - Basic usage

## Performance Tuning

### For Maximum Throughput

```rust
let config = WalConfig {
    num_segments: 32,  // More parallelism
    batch_config: BatchConfig::throughput_optimized(),
};
```

### For Minimum Latency

```rust
let config = WalConfig {
    num_segments: 8,   // Less overhead
    batch_config: BatchConfig::latency_optimized(),
};
```

## Dependencies

Core dependencies:
- `tokio` - Async runtime
- `memmap2` - Memory-mapped I/O
- `crossbeam-channel` - Lock-free queues
- `lz4`, `snap`, `zstd` - Compression (optional)

## Status

**Production Ready**: WAL write path (4.2M msgs/sec achieved)  
**Beta**: Read operations, recovery  
**Alpha**: Compaction, snapshots

## Contributing

See [CONTRIBUTING.md](../../CONTRIBUTING.md)

## License

Apache-2.0
