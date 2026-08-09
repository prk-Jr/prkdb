# Performance methodology

Every performance number PrkDB publishes should say where it came from and how to get it
again. This page is the register of those numbers, and the reason it exists is that until
2026-08-09 none of them did.

## How to reproduce

```bash
cargo bench -p prkdb --bench storage_bench
cargo bench -p prkdb-core --bench wal_bench
cargo bench -p prkdb --bench e2e_throughput_bench
```

Benchmarks use [Criterion]. The defaults sample for long enough to give a confidence
interval; the numbers below were taken with a shortened run
(`-- --sample-size 10 --measurement-time 3`) and are therefore indicative rather than
publication-grade. The command is recorded with the results so the two are not confused.

[Criterion]: https://bheisler.github.io/criterion.rs/book/

## Measured, 2026-08-09

| Benchmark | Result | Notes |
|---|---|---|
| `storage_put/single_put` | **822 ops/sec** | One `put` per iteration, each awaiting durability |
| `storage_batch/batch_put_100` | **78.4 K ops/sec** | 100-key batch |
| `storage_get/single_get` | **8.3 M ops/sec** | Cache-resident read |
| `storage_mixed/mixed_70_30` | **22.1 K ops/sec** | 70 % read, 30 % write |
| `storage_cache/cache_hit_rate` | **10.0 M ops/sec** | Pure cache hit |

Environment: Apple M3, 8 cores, 16 GiB, macOS 26.5.1, rustc 1.95.0, release profile.
A laptop under thermal management is not a server. Treat these as a floor and a shape, not
as a spec sheet.

## Claims register

The repository carried roughly forty performance numbers in doc comments. None linked to a
benchmark, so none could be checked, and several disagree with what the benchmarks
actually produce.

| Claim | Where | Status |
|---|---|---|
| Batched puts ~300 K ops/sec, "800x faster" | `prkdb-types/src/storage.rs` | **Corrected.** Measured 78.4 K and **95x**, not 800x |
| `Legendary`: 1.2 M+ ops/sec | `prkdb/src/builder.rs` | **Unverified.** No benchmark produces this |
| `Balanced`: 200–400 K, `Throughput`: 500–800 K ops/sec | `prkdb/src/builder.rs` | **Unverified.** No benchmark separates the presets |
| 214 K ops/sec "proven" | `storage/sharded_wal_adapter.rs` | **Unverified.** The word "proven" was not earned |
| `insert_batch` 878 K, `query_by` 894 K ops/sec | `prkdb/src/indexed_storage.rs` | **Unverified.** Against in-memory storage, if reproducible at all |
| Batch insert "76x faster" | `prkdb/src/indexed_storage.rs` | **Unverified** |
| Collection partitioning 3x/5x scaling | `storage/collection_partitioned_adapter.rs` | **Unverified.** Plausible by design; unmeasured |
| 21.8x faster than Kafka, 24.5x faster consumer | March audit notes | **Not published.** Recorded here only so the claim is not silently reused |

"Unverified" means exactly that: the number may well be right on the hardware where it was
first taken. It is not a measurement anyone can currently reproduce from this repository,
so it must not be read as one.

## What benchmarks a mock rather than the database

`kv_bench.rs` and `consumer_bench.rs` measure a `MockKvStore` built on `HashMap`. They are
useful as a control — they show what the surrounding harness costs — but a number taken
from them describes `std::collections::HashMap`, not PrkDB. No published claim may come
from these two.

`joins_bench.rs`, `partitioning_bench.rs`, `streaming_bench.rs` and `windowing_bench.rs`
construct no storage adapter at all.

## The rule

A performance number in documentation must name the benchmark that produces it. If there
is no such benchmark, the number does not go in — or it is labelled unverified, as above.
A number with no provenance is a marketing claim wearing a lab coat.
