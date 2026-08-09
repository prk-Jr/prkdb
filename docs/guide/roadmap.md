# PrkDB v2 Development Roadmap & Summary

## Status Check

- **Current Version**: 0.6.0
  <br>Must match `workspace.package.version`; `xtask repo-status` fails if they drift.
- **Build Status**: see the CI badge in the README rather than a hardcoded claim here
- **Test Coverage**: measured in CI with a ratcheting floor — see the coverage job

---

## 🚀 Completed Features (v2.0)

### 1. Raft Consensus

- [x] **Pre-Vote Protocol**: Prevents disruptive elections from partitioned nodes (verified by chaos tests).
- [x] **Replication Modes**:
  - `Linearizable`: Strong consistency (Leader read)
  - `Stale`: High availability (Local read)
  - `Follower`: Balances load (ReadIndex)

### 2. Advanced Sharding

- [x] **Consistent Hashing**: `ConsistentHashRing` with virtual nodes for minimal rebalancing.
- [x] **Range Partitioning**: `RangePartitioner` for ordered key access patterns.
- [x] **Performance**: routing is a pure hash lookup with no I/O. The "1.56B ops/sec"
      figure previously quoted here is unverified — see
      [benchmark methodology](../benchmarks/methodology.md).

### 3. Client SDKs

- [x] **Generated clients**: TypeScript, Python, and Go, produced by `prkdb-cli codegen`
      and exercised by dedicated CI jobs on every push. These were listed as future work
      long after they shipped, which is the drift `xtask repo-status` now catches.

### 4. Infrastructure

- [x] **Cleanup**: Removed ~70 redundant files; repo size optimized.
- [x] **Testing**: Fast unit tests (<1s for core); comprehensive chaos suite.

---

## 🔮 Future Roadmap (v2.1+)

### Performance Optimization

- [ ] **SIMD Vectorization**: Optimize range scans and aggregations.
- [ ] **Zero-Copy Networking**: Use `io_uring` for faster replication.
- [ ] **Compaction Strategies**: Leveled compaction for segmented logs.

### Distributed Features

- [ ] **Dynamic Rebalancing**: Auto-move partitions based on load.
- [ ] **Cross-Region Replication**: Asynchronous geo-replication.
- [ ] **Distributed Transactions**: 2PC over Raft for multi-partition atomic commits.

### Developer Experience

- [ ] **Web Dashboard**: React/Next.js admin UI for cluster management.
- [ ] **SQL Layer**: Expand `prkdb-orm` with more SQL dialect support.

---

## Performance Baselines

Numbers live in one place — [benchmark methodology](../benchmarks/methodology.md) — with
the command and hardware that produced them.

They used to be duplicated here and in the README, in different units, for different
operations, with nothing saying so: this page claimed 199K writes/sec while the README
claimed 894K queries/sec, and a reader had no way to know those measure different things
on different hardware. Keeping one table means the two cannot disagree.

---

## Getting Started

```bash
# Run unit tests
cargo test

# Start a local cluster
./scripts/start_cluster.sh
```
