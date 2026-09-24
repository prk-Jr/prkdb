//! 3-node in-process cluster write throughput (spec §6.1: "3-node write throughput and
//! p99 (in-process cluster)").
//!
//! Reuses `crates/prkdb/tests/helpers/in_process_cluster.rs` (built for Raft-level
//! integration tests: elections, replication, partitions) rather than duplicating a
//! second cluster harness here. The cluster is started once, outside the measured
//! region; each measured iteration is one `put` proposed to the current leader and
//! committed by a quorum, which is the same path `distributed_writes.rs` exercises.
//!
//! Four tokio worker threads, not eight: see the "Thread pressure" note in
//! `in_process_cluster.rs` — three Raft nodes plus a benchmark driver oversubscribe an
//! eight-worker runtime on a shared CI box, and this bench does not need more than the
//! integration tests do.

#[path = "../tests/helpers/mod.rs"]
mod helpers;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use helpers::in_process_cluster::InProcessCluster;
use std::time::Duration;
use tokio::runtime::Builder;

fn bench_cluster_write(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let cluster = rt.block_on(async {
        let cluster = InProcessCluster::new(3)
            .await
            .expect("3-node in-process cluster starts");
        cluster
            .await_leader(Duration::from_secs(15))
            .await
            .expect("a leader is elected before the benchmark runs");
        cluster
    });

    let mut group = c.benchmark_group("cluster_write");
    group.throughput(Throughput::Elements(1));
    // Committing through 3 Raft nodes is much slower than a local WAL append; a small
    // sample size keeps this within the harness's overall time budget.
    group.sample_size(10);

    let value = vec![b'x'; 1024];
    let mut counter: u64 = 0;

    group.bench_function("put_3_node_quorum", |b| {
        b.to_async(&rt).iter(|| {
            counter += 1;
            let key = format!("cluster-bench-key-{counter}").into_bytes();
            let value = value.clone();
            let cluster = &cluster;
            async move {
                cluster
                    .put(&key, &value)
                    .await
                    .expect("write commits to the cluster");
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_cluster_write);
criterion_main!(benches);
