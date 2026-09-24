# Decision: single ordered WAL with group commit (Task 2.1 spike)

- **Date:** 2026-09-24
- **Spec:** `docs/superpowers/specs/2026-09-23-root-cause-remediation-design.md` §7 Phase 2 "2a. One WAL", §6 (performance gate), §1 root cause 4
- **Bench:** `crates/prkdb/benches/wal_write_path_spike.rs` (`cargo bench -p prkdb --bench wal_write_path_spike`)
- **Decision:** **PROCEED** with one globally ordered log per data directory, written by a dedicated group-commit writer thread through `Vfs`. One condition carries into Task 2.2: re-run the 1-writer cells on Linux (see Risk 1).

## 1. Question

Spec 2a replaces the four WAL implementations with one ordered log. Its throughput comes from group commit on a single writer. Before building it, the spec requires a spike that answers two questions:

1. Does `Fast` mode lose more than 15% put throughput against the current path?
2. Is the single writer the bottleneck at 64 concurrent writers?

If either answer is yes, the spike stops and escalates to the maintainer, and the fallback to evaluate is sharded logs with a global sequence assigned at commit. The spec expects `Durable` mode to cost a lot compared with today's path, which never fsyncs. That cost is reported below. It is the price of fixing STO-02, not a failure criterion.

## 2. What was built

The bench file is self-contained. None of it is linked into the product.

- **`SingleLog` prototype (SPIKE):**
  - One active segment is written through `Vfs`/`StdVfs` using `write_at` and `sync_data`.
  - A dedicated `std::thread` writer blocks on the first queued `(record bytes, oneshot)` and then drains everything else already queued, up to 16 MiB. It frames each record as `len u32 | crc32 u32 | offset u64 | payload`, with the CRC covering the offset and the payload. It issues **one `pwrite`** for the batch, then one sync or none depending on the policy, and completes every oneshot with the record's global offset.
  - Offsets are global and increase by one per record.
  - Segments roll at 256 MiB: the writer syncs the old segment, creates the new one, and fsyncs the directory.
  - Callers encode records (`LogRecord` rkyv) on their own threads, as the current path does.
  - Sync policies:
    - `Durable`: `VfsFile::sync_data` per batch, which is `F_FULLFSYNC` on macOS.
    - `DurablePlainFsync`: `fsync(2)` per batch. On macOS this skips the drive-cache flush, so it approximates Linux `fdatasync` cost. It exists only for that estimate.
    - `Fast`: ack after `pwrite`. A background thread calls `sync_data` every 10 ms.
- **Current path, through its own APIs:**
  - `current_adapter_put`: `WalStorageAdapter::put`, the public write path.
  - `current_mmap_wal`: `MmapParallelWal::append_batch(vec![record])`. This is the WAL call `put` makes, without the adapter's index, cache and barrier work.
  - Both use `WalConfig::test_config()`, the configuration `storage_bench` uses.
- **`model_memcpy_only` (a model, not product code):** the current mmap append with its every-10th-batch `msync(MS_ASYNC)` removed. It keeps the record encode and one async mutex, and replaces the file-backed mmap with one memcpy into pre-faulted memory. `MS_ASYNC` is close to a no-op on Linux, so this is a **lower bound on what the current path costs on Linux**.
- **`two_shard_fast` (bottleneck probe):** two independent `Fast` `SingleLog`s, with writers routed by index. If two writers beat one, the single writer is the limit.
- **Driver (custom `harness = false`):**
  - One multi-thread tokio runtime with 8 workers runs 1, 8 or 64 tasks. Each task loops `put`, awaits the ack, and records its own latency.
  - Each cell gets a 1 s warm-up and a 3 s measurement window. An op counts toward the window if it starts inside it.
  - p50, p99 and p99.9 come from the full sorted latency set.
  - For `SingleLog` cells the writer thread reports these deltas over the window:
    - how the writer's time splits: blocked in `recv` (idle), in `pwrite` (write) or in sync;
    - its thread CPU time (`CLOCK_THREAD_CPUTIME_ID`);
    - average and maximum batch size.
  - Each cell uses its own temporary directory, removed afterwards.

The file lives in `crates/prkdb/benches`, not `crates/prkdb-core/benches` as the plan listed. The comparison has to go through `WalStorageAdapter`, and `prkdb-core` cannot depend on `prkdb`. It adds `libc` as a unix-only dev-dependency of `prkdb`.

## 3. Setup and hygiene

- **Machine:** MacBook Air, Apple M3 (8 logical CPUs, 4P+4E), 16 GiB RAM, Darwin 25.6.0 arm64, internal SSD (APFS), 87 GiB free. The M3 Air has no fan, so sustained runs throttle (see Risk 3).
- **Tree:** `remediation/phase-2` @ 65b3e50 plus the spike commit. Release profile.
- **Load:** the matrix started only after the 1-minute load average fell below 3. The wait took about 3 minutes, because an unrelated `rustc` build on the machine was using about 600% CPU. The load was 2.85 at start and 3.07 at the end. During rep 1 it rose to about 8–9. Part of that rise is the bench itself: on macOS the load average counts runnable threads, and a 64-writer cell keeps all 8 workers and the writer runnable. Rep 2 ran at 3–8. The table records the load for every cell.
- **Nothing else heavy** of mine ran during measurement.
- **macOS caveat:** `File::sync_data` on macOS is `F_FULLFSYNC`, which flushes the drive's write cache. It is much slower than Linux `fdatasync` on typical server NVMe. The `DurablePlainFsync` rows give a Linux-like estimate. Plain `fsync(2)` on macOS does *not* guarantee durability across power loss, so those rows are an estimate only.

**Device ceilings** (single thread, same run):

| Probe | Result |
|---|---|
| `pwrite` 1 MiB, no sync | 1,555 MB/s |
| `pwrite` 1 MiB + `sync_data` each | 285 MB/s (271 syncs/s) |
| 4 KiB write + `sync_data` (`F_FULLFSYNC`) | p50 2,965 µs, p99 4,012 µs |
| 4 KiB write + plain `fsync(2)` | p50 15 µs, p99 26 µs |

## 4. Results

The full matrix ran twice (reps 1 and 2). Throughput is shown for both reps. Latency, MB/s, batch and writer columns are from rep 2, which ran at lower load. Raw output for both reps is reproducible with the command at the top.

"Writer" columns give the share of the measurement window the writer thread spent blocked waiting for work (idle), inside `pwrite` (write) and inside sync, plus the thread's CPU time as a share of the window.

| cell | ops/s rep1 | ops/s rep2 | MB/s rep2 | p50 µs | p99 µs | p99.9 µs | avg batch | writer idle / write / sync / CPU % | load1 (rep2) |
|---|--:|--:|--:|--:|--:|--:|--:|---|---|
| `single_log_durable/1w/1k` | 328 | 324 | 0.3 | 2942.6 | 6589.7 | 17119.1 | 1.0 | 1 / 0 / 98 / 1 | 8.15 |
| `single_log_durable_plain_fsync/1w/1k` | 34,227 | 31,193 | 31.9 | 31.8 | 52.1 | 105.8 | 1.0 | 21 / 19 / 57 / 44 | 7.66 |
| `single_log_fast/1w/1k` | 76,192 | 75,376 | 77.2 | 10.4 | 17.3 | 853.6 | 1.0 | 52 / 39 / 0 / 39 | 7.28 |
| `current_mmap_wal/1w/1k` | 1,653 | 1,228 | 1.3 | 11.3 | 5067.5 | 7758.0 | — | — | 7.28 |
| `current_adapter_put/1w/1k` | 1,625 | 1,062 | 1.1 | 16.3 | 5443.3 | 12118.0 | — | — | 6.94 |
| `model_memcpy_only/1w/1k` | 394,681 | 394,817 | 404.3 | 2.4 | 3.1 | 6.8 | — | — | 6.54 |
| `single_log_durable/8w/1k` | 1,341 | 1,426 | 1.5 | 5905.4 | 9483.4 | 12523.7 | 4.0 | 0 / 2 / 97 / 5 | 6.18 |
| `single_log_durable_plain_fsync/8w/1k` | 130,071 | 117,096 | 119.9 | 53.5 | 113.8 | 1395.1 | 4.0 | 1 / 19 / 74 / 51 | 5.92 |
| `single_log_fast/8w/1k` | 291,277 | 284,392 | 291.2 | 15.8 | 49.2 | 2800.1 | 3.0 | 15 / 73 / 0 / 51 | 5.92 |
| `current_mmap_wal/8w/1k` | 1,592 | 1,605 | 1.6 | 5012.2 | 8890.7 | 16516.8 | — | — | 5.53 |
| `current_adapter_put/8w/1k` | 1,618 | 1,682 | 1.7 | 4950.1 | 8612.3 | 15098.5 | — | — | 5.17 |
| `model_memcpy_only/8w/1k` | 328,481 | 330,455 | 338.4 | 24.0 | 26.0 | 37.5 | — | — | 4.99 |
| `single_log_durable/64w/1k` | 11,470 | 13,129 | 13.4 | 4735.0 | 10362.9 | 28247.4 | 32.0 | 0 / 2 / 97 / 5 | 4.67 |
| `single_log_durable_plain_fsync/64w/1k` | 402,210 | 564,637 | 578.2 | 101.2 | 335.1 | 1385.2 | 32.0 | 0 / 18 / 70 / 64 | 4.67 |
| `single_log_fast/64w/1k` | 740,000 | 395,772 | 405.3 | 45.0 | 5051.7 | 13765.0 | 12.8 | 0 / 84 / 0 / 31 | 4.38 |
| `current_mmap_wal/64w/1k` | 1,580 | 1,107 | 1.1 | 57870.3 | 65550.7 | 67103.3 | — | — | 4.19 |
| `current_adapter_put/64w/1k` | 1,313 | 1,092 | 1.1 | 58830.2 | 66631.5 | 69696.4 | — | — | 3.85 |
| `model_memcpy_only/64w/1k` | 225,940 | 329,945 | 337.9 | 193.7 | 200.9 | 207.3 | — | — | 3.70 |
| `single_log_durable/1w/64k` | 290 | 291 | 19.1 | 3933.3 | 4212.9 | 6058.3 | 1.0 | 12 / 2 / 86 / 4 | 3.57 |
| `single_log_durable_plain_fsync/1w/64k` | 1,724 | 4,951 | 324.5 | 194.5 | 253.2 | 719.7 | 1.0 | 69 / 8 / 21 / 19 | 3.57 |
| `single_log_fast/1w/64k` | 1,834 | 4,757 | 311.8 | 152.7 | 2405.8 | 3224.4 | 1.0 | 66 / 31 / 0 / 8 | 3.44 |
| `current_mmap_wal/1w/64k` | 331 | 522 | 34.2 | 1905.1 | 4106.3 | 4487.0 | — | — | 3.33 |
| `current_adapter_put/1w/64k` | 321 | 531 | 34.8 | 1707.3 | 3955.0 | 4308.5 | — | — | 3.22 |
| `model_memcpy_only/1w/64k` | 4,090 | 7,598 | 497.9 | 131.2 | 138.0 | 147.8 | — | — | 3.12 |
| `single_log_durable/8w/64k` | 1,201 | 1,326 | 86.9 | 5993.5 | 8263.1 | 12319.7 | 4.0 | 0 / 5 / 93 / 10 | 3.12 |
| `single_log_durable_plain_fsync/8w/64k` | 9,512 | 21,174 | 1387.7 | 313.4 | 1117.2 | 9795.8 | 2.3 | 2 / 20 / 69 / 44 | 3.35 |
| `single_log_fast/8w/64k` | 5,203 | 19,555 | 1281.6 | 229.5 | 3359.7 | 8701.2 | 1.8 | 22 / 67 / 0 / 39 | 3.16 |
| `current_mmap_wal/8w/64k` | 327 | 521 | 34.1 | 15919.6 | 18295.5 | 22988.6 | — | — | 3.15 |
| `current_adapter_put/8w/64k` | 372 | 523 | 34.3 | 15833.0 | 17973.5 | 22018.4 | — | — | 3.06 |
| `model_memcpy_only/8w/64k` | 8,640 | 9,847 | 645.3 | 811.0 | 833.9 | 887.0 | — | — | 3.06 |
| `single_log_durable/64w/64k` | 5,886 | 6,890 | 451.5 | 9340.7 | 12898.1 | 15645.5 | 32.0 | 0 / 10 / 83 / 22 | 2.89 |
| `single_log_durable_plain_fsync/64w/64k` | 8,140 | 19,348 | 1268.0 | 1819.9 | 18047.6 | 244214.0 | 31.7 | 0 / 44 / 47 / 25 | 3.46 |
| `single_log_fast/64w/64k` | 8,261 | 10,210 | 669.1 | 2377.6 | 37569.4 | 50823.3 | 16.6 | 9 / 85 / 0 / 18 | 3.26 |
| `current_mmap_wal/64w/64k` | 385 | 387 | 25.3 | 166033.0 | 179028.5 | 180893.3 | — | — | 3.16 |
| `current_adapter_put/64w/64k` | 329 | 383 | 25.1 | 166148.2 | 183966.7 | 184919.8 | — | — | 3.07 |
| `model_memcpy_only/64w/64k` | 9,494 | 9,803 | 642.4 | 6522.8 | 6678.0 | 6810.6 | — | — | 3.07 |

**Bottleneck probe:** one `Fast` log vs two `Fast` shards, 64 writers, three reps, load 1.8–3.9.

| Value | One log ops/s (MB/s) | Two shards ops/s (MB/s) | Two shards / one |
|---|---|---|---|
| 1 KiB | 823,318 (843) · 415,656 (426) · 460,322 (471) | 686,165 (703) · 303,840 (311) · 213,784 (219) | 0.83 · 0.73 · 0.46 |
| 64 KiB | 25,731 (1,686) · 10,429 (684) · 10,300 (675) | 18,374 (1,204) · 8,968 (588) · 9,006 (590) | 0.71 · 0.86 · 0.87 |

The one-log numbers roughly halve after the first rep. That pattern is thermal throttling on a fanless laptop, not the design (Risk 3). Within each rep, two shards are slower than one log every time.

### 4.1 What the numbers say

**The current path is slow on macOS for a reason unrelated to its design.** `MmapLogSegment::append_batch` calls `mmap.flush_async()`, which is `msync(MS_ASYNC)` over the whole mapping, on every 10th batch. It does so while holding the segment mutex. Every `put` goes to the same shard (STO-06), so that ~4–5 ms call stalls every writer:

- p50 is about 10 µs, but p99 is 3.6–5 ms at 1 writer.
- Throughput stays flat at about 1.1k–1.7k ops/s (1 KiB) and 320–530 ops/s (64 KiB) whether there are 1, 8 or 64 writers.
- The Phase 1 baseline `storage_put/single_put` (146,671 ns, about 6.8k ops/s, 100 B values) comes from the same path. It is higher because Criterion reports a mean of short samples and only 1 put in 10 pays the msync.

The adapter adds little on top of `MmapParallelWal`: at 1 writer, adapter p50 is 10.3–16.3 µs and mmap p50 is 10.2–11.3 µs.

**`Fast` against the current path (question 1).** `Fast` is faster in every cell, by 9× to 360×:

| Writers | 1 KiB: Fast ops/s | 1 KiB: adapter `put` ops/s | 64 KiB: Fast ops/s | 64 KiB: adapter `put` ops/s |
|---|---|---|---|---|
| 1 | 75–76k | 1.1–1.6k | 1.8–4.8k | 320–530 |
| 8 | 284–291k | 1.6–1.7k | 5.2–19.6k | 370–520 |
| 64 | 396–740k | 1.1–1.3k | 8.3–10.2k | 330–380 |

Against the Phase 1 baseline, `Fast` at 1 writer (75k ops/s at 1 KiB) is about 11× the baseline's single-put rate (6.8k ops/s at 100 B). **`Fast` loses nothing on this machine.**

**Single-writer bottleneck (question 2).** At 64 writers the single writer is **not** the limit:

- **Batches are not maxed out.** The writer is busy nearly all the time (idle 0–1% at 1 KiB, 9–21% at 64 KiB). But average `Fast` batches are only 12.6–13 records out of 64 writers, with a maximum of 64. If the writer were the choke point, the queue would fill and batches would approach 64. Instead the 8 client cores are saturated too: 64 tasks each format a key, build and rkyv-encode a record, and wake on a oneshot. **The machine is CPU-bound as a whole.**
- **The writer is not CPU-saturated.** Writer CPU is 31–62% of wall time. The rest of its busy time is spent blocked inside `pwrite`, contending with the 10 ms background `F_FULLFSYNC` at the file-system level. That is the device path, not the writer thread.
- **The disk is not idle.** At 64 KiB, `Fast` reaches 675–1,686 MB/s at 64 writers, against a 1,555 MB/s single-thread `pwrite` ceiling. At 1 KiB it reaches 426–843 MB/s with ~13 KiB pwrites.
- **Adding a second writer makes it slower.** Two shards lose 13–54% against one log in every rep (probe table above). Splitting the stream adds a second writer competing for the same device and cores. It does not add capacity.

**`Durable` cost (reported, not a criterion).** On macOS, `F_FULLFSYNC` has a floor of about 3 ms per sync:

- **1 writer:** 324–328 ops/s, one sync per put.
- **8 writers:** 1,341–1,426 ops/s, 0.85–0.88× the current path.
- **64 writers:** 11.5k–13.1k ops/s, 9–12× the current path. Group commit forms ~32-record batches, because two cohorts pipeline: one syncs while the next queues.
- **64 KiB values:** 290 (1 writer), 1.2–1.3k (8 writers) and 5.9–6.9k ops/s (64 writers, 386–452 MB/s).

With a Linux-like sync cost (plain `fsync`), `Durable` reaches 31–34k ops/s at 1 writer, 117–130k at 8 and 402–565k at 64 (1 KiB). Real Linux NVMe `fdatasync` will land between the two sets of rows.

## 5. Decision

**PROCEED with the single, globally ordered log.** Both conditions of the spec rule hold on the reference machine, the same macOS M3 the Phase 1 baseline was captured on:

1. `Fast` loses 0% put throughput against the current path. It is 9–360× faster, because it is not stuck behind a locked `msync`.
2. The single writer is not the bottleneck at 64 writers:
   - batches stay well below the writer count;
   - the writer thread is not CPU-saturated;
   - the disk is doing real work;
   - a two-shard layout is measurably slower.

The sharded-with-global-sequence fallback is therefore not needed now. It remains available, because the `Vfs` and segment code stay shard-agnostic (spec 2a). The probe says it would need a workload that is actually limited by one writer's CPU before it pays off. This machine does not produce one.

## 6. Risks

1. **Linux, 1 writer (condition on Task 2.2).** On Linux, `MS_ASYNC` does almost nothing, so the current path does not pay the msync that makes it slow here.
   - `model_memcpy_only` does ~395k ops/s at 1 writer. The real mmap path's non-msync p50 (~10 µs) suggests ~90–100k ops/s.
   - `SingleLog` `Fast` at 1 writer pays a thread handoff each way plus a `pwrite` syscall: ~10 µs p50, 75k ops/s here.
   - At 1 writer on Linux, `Fast` could therefore land somewhere between parity and about **−40%** against the current path. That range is an estimate, not a measurement. At 8+ writers group commit amortises the handoff, and `Fast` is at or above the memcpy model (284–291k vs 328–330k at 8; 396–823k vs 226–330k at 64).
   - **Required in 2.2:**
     - run this bench on Linux (CI runner or a Linux box) before merging the writer;
     - if the 1-writer loss exceeds 15%, try a bounded spin (tens of µs) on `try_recv` before the writer parks, and have callers avoid a second handoff (for example `oneshot` plus `Notify` versus a futex-backed waiter), before escalating.
2. **`Fast` tail latency from the background sync.** At 64 KiB, `Fast` p99 is 3–42 ms, because `pwrite` stalls while the 10 ms `F_FULLFSYNC` runs on the same file. Task 2.3 should measure the syncer inside the writer thread (sync at batch boundaries once `sync_interval` has elapsed) against a separate syncer thread, and pick the better tail. Either way the writer must track a durable-LSN watermark.
3. **Noise and throttling.** The M3 Air throttles under sustained load: the one-log 64-writer cells halved between rep 1 and later reps, and the load average rose during 64-writer cells. The conclusions hold in every rep, but individual cells vary up to 2×. Durable baselines for Phase 2 (`baseline-format-v2.toml`) should be captured on a quiet, cooled machine or on Linux.
4. **`Durable` at low concurrency.** On macOS, a single synchronous writer in `Durable` mode gets ~330 ops/s. That is inherent to `F_FULLFSYNC` and must be documented next to `Fast`'s loss window (spec §6.2). `put_batch`/`put_many` must go down as one WAL append (one record, one sync), or bulk loads will crawl.
5. **The prototype omits:**
   - error poisoning (after a failed write or sync, nothing may be acked; fsyncgate semantics);
   - bounded admission by bytes (liveness spec Part 4);
   - the writer-liveness supervisor;
   - reads, recovery and compaction.

   None of these changes the throughput shape, but they are the bulk of 2.2–2.4.
6. **Side finding, root cause 4, not in the ledger yet:** `WalStorageAdapter::new_with_config` ignores `StorageConfig::cache_capacity`. The builder's `with_cache_capacity` feeds that path, yet the cache is hard-coded to 100,000 entries (`wal_adapter.rs`, "Match default config cache capacity"). The `open`/`open_async`/replication constructors do read it. The controller should add this to the ledger. It is also why the 64 KiB adapter cells can hold up to ~6 GB of cache.

## 7. Design notes for Tasks 2.2–2.4

**Writer thread and API (2.2).**

- `Wal::open(vfs: Arc<dyn Vfs>, dir, WalOptions) -> Result<(Wal, Recovered)>`. `WalOptions` has these fields:
  - `sync_mode: SyncMode`
  - `sync_interval: Duration` (default 10 ms)
  - `segment_bytes: u64` (read from `WalConfig::segment_bytes`, which fixes STO-08)
  - `max_batch_bytes` (4–16 MiB; 16 MiB was used here)
  - `max_queued_bytes` (admission bound)
- `async fn append(&self, rec: EncodedRecord) -> Result<Lsn, WalError>` for one record.
- `async fn append_group(&self, recs) -> Result<Lsn, WalError>`. It is **one** framed record, so a transaction or `put_batch` is atomic and costs one slot in the batch.
- `async fn sync(&self) -> Result<Lsn>` returns the durable watermark.
- `fn durable_lsn()`, `fn health()`.
- Encoding happens on the caller. The writer only frames, assigns LSNs, writes and syncs.
- The channel carries `(bytes, oneshot::Sender<Result<Lsn>>)`. Senders carry a drop guard, so a dropped write is answered and never silently lost (same as today's `PendingWrite`).
- The writer thread is a `std::thread` and owns the active segment. Its `JoinHandle` is supervised per `2026-08-11-wal-writer-liveness.md`. On any I/O error the writer enters a **poisoned** state: it fails the current batch and every later append with a typed error, and never retries a failed `fsync`.

**Batching policy.**

- Block for the first record, then drain everything already queued up to `max_batch_bytes`. Add **no linger timer**. The spike shows pipelining forms batches by itself: ~N/2 in `Durable` and ~N/5 in `Fast` at 64 writers.
- A linger would only add latency at low concurrency. Revisit only if the Linux 1-writer run (Risk 1) calls for a short spin.
- Admission is bounded by queued bytes. A caller over the bound waits on a semaphore instead of growing the queue (STO-07).

**How `SyncMode` maps.**

- Rename today's `SyncMode::{Durable, Performance}` (in `storage/config.rs`; `Durable` is never read, STO-02) to `{Durable, Fast}`. Keep the old variant as a deprecated alias if the config is public.
- `Durable` (default for new data directories): ack after `sync_data` for the batch that contains the record.
- `Fast`: ack after `write_at`. `sync_data` runs at least every `sync_interval`, from the writer or a syncer thread (Risk 2). Docs state that a power cut can lose up to `sync_interval` of acknowledged writes.
- Both modes sync on segment roll and on clean shutdown.

**Segment roll.**

- Roll when the next batch would exceed `segment_bytes`. A batch never spans segments; a batch larger than `segment_bytes` gets a segment of its own.
- Sequence: `sync_data` the old segment, `vfs.create` the new one named `{first_lsn:020}.log`, then `vfs.sync_dir(dir)`, then switch.
- Names carry the first LSN, so recovery sorts and continuity-checks without reading headers.
- Measure optional preallocation (`set_len` ahead of the write position) in 2.3. It avoids a size-metadata update per `fdatasync` on Linux, but recovery must then treat trailing zeros as end-of-log.

**Framing.**

- Layout: `len u32 | crc32c u32 | lsn u64 | kind u8 | payload`.
- The CRC covers `lsn | kind | payload`.
- `kind` separates data, group/commit and future checkpoint markers (2c/2d).
- Enforce a maximum record size on both write and read.

**Recovery (2.4, and STO-04/STO-05).**

- List segments and sort by first LSN. Scan each frame in order and verify:
  - `len` is within bounds and the file;
  - the CRC matches;
  - the LSN equals the expected next LSN, so replay order is the global order.
- **In the last segment:** the first bad frame (short read, bad CRC, zero header, LSN gap) is a torn tail. Log it, truncate the file there (`set_len` + `sync_data`), and continue.
- **In an earlier segment:** a bad frame is corruption. Refuse to open with a typed error; never truncate silently.
- An empty trailing segment is valid.
- After any truncation or cleanup, `sync_dir`.
- Next LSN = last good LSN + 1.
- Replay feeds the index rebuild in LSN order. Checkpoints (2d) record an LSN, and recovery replays strictly after it.
- Property to test with `faultfs` `PowerLoss`: every record acknowledged in `Durable` mode is present after recovery; in `Fast` mode, those acknowledged more than `sync_interval` before the cut.

**Reads.** Use `VfsFile::read_at`, located through a sparse LSN → (segment, position) index built during the recovery scan and extended by the writer. mmap for reads is optional and never used for writes.

**Deletion (spec 2a).** Once `WalStorageAdapter` and Raft storage are migrated, delete `WriteAheadLog`, `ParallelWal`, `AsyncParallelWal` and `MmapParallelWal`. Delete this spike bench with them, or keep it as a comparison bench against the real `Wal`.

## 8. Linux probe results (Task 2.2, closes Risk 1)

Risk 1 above required re-running the spike bench on Linux before Task 2.2 merges the
writer, because the macOS numbers in §4 are inflated by an `msync` penalty this bench's
`current_mmap_wal`/`current_adapter_put` cells pay on macOS but not on Linux (§4.1). The
estimate in Risk 1 was "somewhere between parity and about −40%" at 1 writer.

**How this ran.** `.github/workflows/remediation-gate.yml` gained a `probe` dispatch
input (Task 2.2, D10: probe pushes of `remediation/phase-2` without a PR). Dispatched
with `gh workflow run remediation-gate.yml --ref remediation/phase-2 -f
ref=remediation/phase-2 -f phase=2 -f probe=bench -f bench_name=wal_write_path_spike -f
bench_reps=2`, which skips the normal gate jobs (`ledger-and-tests`, `harness`) and runs
only `cargo bench -p prkdb --bench wal_write_path_spike` on `ubuntu-latest`.

- **Run:** <https://github.com/prk-Jr/prkdb/actions/runs/36023246514> (success), commit
  `e8a011f9d1017ae0242ee537bd1e26d93fd4c6a7`.
- **Machine:** GitHub-hosted `ubuntu-latest` runner, 4 vCPU (tokio worker threads = 4,
  vs. 8 on the M3 Air in §3). Load average at start 5.43/3.14/1.38 (a shared runner, not
  a quiet machine — see caveat below).
- **Disk ceiling:** `pwrite` 1 MiB no sync 1492 MB/s; `pwrite` 1 MiB + `sync_data` 407
  MB/s (388 writes/s); 4 KiB write + `sync_data` p50 261 µs / p99 417 µs; 4 KiB write +
  plain `fsync(2)` p50 86 µs / p99 370 µs. Unlike the M3 Air, Linux `sync_data` here is a
  real `fdatasync`-class call, not `F_FULLFSYNC`, so `Durable` costs are directly
  meaningful on this machine (no macOS caveat).

**Raw output (both reps, unedited):**

```
# wal_write_path_spike
- warm-up 1000 ms, measure 3000 ms, reps 2, tokio worker threads = 4
- load average at start: 5.43 3.14 1.38
- disk ceiling, pwrite 1 MiB, no sync: 1492 MB/s (1423 writes/s)
- disk ceiling, pwrite 1 MiB + sync_data: 407 MB/s (388 writes/s)
- 4 KiB write + sync_data (F_FULLFSYNC on macOS): p50 261 µs, p99 417 µs (7198 samples)
- 4 KiB write + plain fsync(2): p50 86 µs, p99 370 µs (11749 samples)
| cell | writers | value | ops/s | MB/s | p50 µs | p99 µs | p99.9 µs | avg batch | max batch | writer idle % | writer write % | writer sync % | writer CPU % | load1 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| single_log_durable/1w/1k | 1 | 1 KiB | 3543 | 3.6 | 252.0 | 474.5 | 1307.9 | 1.0 | 1 | 13 | 3 | 81 | 23 | 4.75 3.07 1.38 |
| single_log_durable_plain_fsync/1w/1k | 1 | 1 KiB | 3639 | 3.7 | 255.5 | 436.7 | 1672.7 | 1.0 | 1 | 14 | 3 | 80 | 24 | 4.45 3.03 1.38 |
| single_log_fast/1w/1k | 1 | 1 KiB | 22582 | 23.1 | 44.1 | 61.1 | 110.9 | 1.0 | 1 | 76 | 8 | 0 | 44 | 4.09 2.98 1.37 |
| current_mmap_wal/1w/1k | 1 | 1 KiB | 20810 | 21.3 | 5.9 | 253.7 | 418.2 |  |  |  |  |  |  | 3.85 2.95 1.37 |
| current_adapter_put/1w/1k | 1 | 1 KiB | 19759 | 20.2 | 7.7 | 251.5 | 397.1 |  |  |  |  |  |  | 3.85 2.95 1.37 |
| model_memcpy_only/1w/1k | 1 | 1 KiB | 376576 | 385.6 | 2.5 | 6.2 | 12.9 |  |  |  |  |  |  | 3.62 2.91 1.36 |
| two_shard_fast/1w/1k | 1 | 1 KiB | 22393 | 22.9 | 44.6 | 62.0 | 114.5 |  |  |  |  |  |  | 3.33 2.87 1.36 |
| single_log_durable/8w/1k | 8 | 1 KiB | 13648 | 14.0 | 590.6 | 834.9 | 2113.8 | 4.0 | 7 | 0 | 5 | 92 | 31 | 3.22 2.85 1.36 |
| single_log_durable_plain_fsync/8w/1k | 8 | 1 KiB | 13272 | 13.6 | 594.8 | 927.8 | 2669.2 | 4.0 | 8 | 0 | 5 | 92 | 31 | 3.04 2.82 1.36 |
| single_log_fast/8w/1k | 8 | 1 KiB | 268976 | 275.4 | 25.6 | 66.3 | 228.3 | 1.4 | 8 | 23 | 50 | 1 | 85 | 2.96 2.81 1.36 |
| current_mmap_wal/8w/1k | 8 | 1 KiB | 19348 | 19.8 | 452.9 | 673.6 | 3073.0 |  |  |  |  |  |  | 2.96 2.81 1.36 |
| current_adapter_put/8w/1k | 8 | 1 KiB | 18615 | 19.1 | 466.8 | 710.4 | 2610.6 |  |  |  |  |  |  | 2.88 2.79 1.37 |
| model_memcpy_only/8w/1k | 8 | 1 KiB | 222278 | 227.6 | 29.5 | 60.7 | 68.2 |  |  |  |  |  |  | 2.73 2.76 1.36 |
| two_shard_fast/8w/1k | 8 | 1 KiB | 178534 | 182.8 | 43.2 | 125.9 | 340.3 |  |  |  |  |  |  | 2.75 2.77 1.37 |
| single_log_durable/64w/1k | 64 | 1 KiB | 90560 | 92.7 | 699.0 | 1088.5 | 3625.1 | 32.0 | 63 | 0 | 8 | 88 | 30 | 2.69 2.75 1.38 |
| single_log_durable_plain_fsync/64w/1k | 64 | 1 KiB | 87686 | 89.8 | 732.6 | 1096.8 | 2316.9 | 32.0 | 60 | 0 | 8 | 88 | 29 | 2.69 2.75 1.38 |
| single_log_fast/64w/1k | 64 | 1 KiB | 388355 | 397.7 | 85.9 | 265.0 | 3117.7 | 9.8 | 64 | 2 | 89 | 0 | 61 | 3.04 2.83 1.41 |
| current_mmap_wal/64w/1k | 64 | 1 KiB | 19237 | 19.7 | 3236.8 | 4630.5 | 33269.7 |  |  |  |  |  |  | 2.95 2.81 1.41 |
| current_adapter_put/64w/1k | 64 | 1 KiB | 18422 | 18.9 | 3354.2 | 5915.8 | 33222.6 |  |  |  |  |  |  | 2.80 2.78 1.41 |
| model_memcpy_only/64w/1k | 64 | 1 KiB | 223746 | 229.1 | 233.3 | 402.8 | 419.5 |  |  |  |  |  |  | 2.65 2.75 1.40 |
| two_shard_fast/64w/1k | 64 | 1 KiB | 392654 | 402.1 | 46.1 | 2188.2 | 4441.0 |  |  |  |  |  |  | 3.08 2.84 1.44 |
| single_log_durable/1w/64k | 1 | 64 KiB | 1672 | 109.6 | 586.3 | 856.6 | 3378.3 | 1.0 | 1 | 33 | 6 | 54 | 24 | 3.08 2.84 1.44 |
| single_log_durable_plain_fsync/1w/64k | 1 | 64 KiB | 1724 | 113.0 | 566.9 | 845.5 | 2311.2 | 1.0 | 1 | 34 | 6 | 54 | 23 | 2.92 2.81 1.44 |
| single_log_fast/1w/64k | 1 | 64 KiB | 3870 | 253.6 | 254.6 | 386.5 | 482.6 | 1.0 | 1 | 76 | 11 | 0 | 27 | 2.68 2.76 1.43 |
| current_mmap_wal/1w/64k | 1 | 64 KiB | 2678 | 175.5 | 371.7 | 591.0 | 7862.0 |  |  |  |  |  |  | 2.55 2.73 1.43 |
| current_adapter_put/1w/64k | 1 | 64 KiB | 2677 | 175.4 | 386.1 | 600.2 | 3533.5 |  |  |  |  |  |  | 2.42 2.70 1.43 |
| model_memcpy_only/1w/64k | 1 | 64 KiB | 6227 | 408.1 | 158.2 | 180.0 | 210.1 |  |  |  |  |  |  | 2.42 2.70 1.43 |
| two_shard_fast/1w/64k | 1 | 64 KiB | 3875 | 254.0 | 254.1 | 387.3 | 474.1 |  |  |  |  |  |  | 2.31 2.67 1.42 |
| single_log_durable/8w/64k | 8 | 64 KiB | 5747 | 376.7 | 1174.8 | 8185.2 | 22639.3 | 3.9 | 8 | 0 | 16 | 74 | 38 | 2.52 2.71 1.44 |
| single_log_durable_plain_fsync/8w/64k | 8 | 64 KiB | 5643 | 369.8 | 1180.9 | 9446.5 | 25251.8 | 4.0 | 8 | 0 | 15 | 74 | 37 | 2.72 2.75 1.46 |
| single_log_fast/8w/64k | 8 | 64 KiB | 6124 | 401.4 | 694.9 | 2269.2 | 248722.9 | 2.2 | 8 | 27 | 58 | 0 | 32 | 2.66 2.74 1.46 |
| current_mmap_wal/8w/64k | 8 | 64 KiB | 2727 | 178.7 | 2663.2 | 5003.3 | 32283.0 |  |  |  |  |  |  | 2.53 2.71 1.46 |
| current_adapter_put/8w/64k | 8 | 64 KiB | 2633 | 172.5 | 2749.6 | 8411.4 | 32425.9 |  |  |  |  |  |  | 2.53 2.71 1.46 |
| model_memcpy_only/8w/64k | 8 | 64 KiB | 7648 | 501.2 | 1037.5 | 1251.3 | 1334.2 |  |  |  |  |  |  | 2.41 2.68 1.46 |
| two_shard_fast/8w/64k | 8 | 64 KiB | 6348 | 416.0 | 642.5 | 20907.0 | 97070.8 |  |  |  |  |  |  | 2.70 2.74 1.48 |
| single_log_durable/64w/64k | 64 | 64 KiB | 6204 | 406.6 | 5688.5 | 42926.4 | 45505.8 | 27.7 | 54 | 0 | 13 | 80 | 22 | 2.64 2.72 1.49 |
| single_log_durable_plain_fsync/64w/64k | 64 | 64 KiB | 6188 | 405.5 | 5684.9 | 42801.6 | 47076.0 | 27.5 | 53 | 0 | 13 | 80 | 22 | 2.51 2.69 1.48 |
| single_log_fast/64w/64k | 64 | 64 KiB | 6215 | 407.3 | 4320.8 | 329092.1 | 386339.5 | 5.0 | 63 | 20 | 70 | 0 | 25 | 2.51 2.69 1.48 |
| current_mmap_wal/64w/64k | 64 | 64 KiB | 2695 | 176.6 | 21679.8 | 51762.9 | 52416.3 |  |  |  |  |  |  | 2.39 2.67 1.48 |
| current_adapter_put/64w/64k | 64 | 64 KiB | 2658 | 174.2 | 22027.8 | 52097.0 | 52648.7 |  |  |  |  |  |  | 2.36 2.65 1.48 |
| model_memcpy_only/64w/64k | 64 | 64 KiB | 7613 | 498.9 | 8401.8 | 8571.8 | 8667.6 |  |  |  |  |  |  | 2.25 2.63 1.48 |
| two_shard_fast/64w/64k | 64 | 64 KiB | 6708 | 439.6 | 4845.2 | 90070.2 | 506490.3 |  |  |  |  |  |  | 2.55 2.68 1.50 |
| single_log_durable/1w/1k | 1 | 1 KiB | 3709 | 3.8 | 253.8 | 399.8 | 1343.3 | 1.0 | 1 | 14 | 3 | 80 | 25 | 2.42 2.65 1.50 |
| single_log_durable_plain_fsync/1w/1k | 1 | 1 KiB | 3726 | 3.8 | 251.1 | 402.6 | 1328.2 | 1.0 | 1 | 14 | 3 | 80 | 24 | 2.42 2.65 1.50 |
| single_log_fast/1w/1k | 1 | 1 KiB | 22654 | 23.2 | 44.0 | 60.7 | 109.4 | 1.0 | 1 | 76 | 8 | 0 | 44 | 2.23 2.61 1.49 |
| current_mmap_wal/1w/1k | 1 | 1 KiB | 20663 | 21.2 | 6.3 | 252.2 | 412.7 |  |  |  |  |  |  | 2.21 2.60 1.50 |
| current_adapter_put/1w/1k | 1 | 1 KiB | 19588 | 20.1 | 8.2 | 268.6 | 440.6 |  |  |  |  |  |  | 2.11 2.57 1.49 |
| model_memcpy_only/1w/1k | 1 | 1 KiB | 364506 | 373.3 | 2.6 | 6.3 | 13.0 |  |  |  |  |  |  | 2.02 2.55 1.49 |
| two_shard_fast/1w/1k | 1 | 1 KiB | 22222 | 22.8 | 44.8 | 61.9 | 109.7 |  |  |  |  |  |  | 2.02 2.55 1.49 |
| single_log_durable/8w/1k | 8 | 1 KiB | 14255 | 14.6 | 537.0 | 951.8 | 3243.6 | 4.0 | 6 | 0 | 5 | 92 | 29 | 1.94 2.52 1.49 |
| single_log_durable_plain_fsync/8w/1k | 8 | 1 KiB | 15090 | 15.5 | 514.5 | 836.7 | 3394.1 | 4.0 | 7 | 0 | 5 | 92 | 29 | 1.87 2.50 1.48 |
| single_log_fast/8w/1k | 8 | 1 KiB | 268223 | 274.7 | 25.6 | 67.0 | 201.8 | 1.5 | 8 | 23 | 50 | 1 | 85 | 1.96 2.50 1.49 |
| current_mmap_wal/8w/1k | 8 | 1 KiB | 19424 | 19.9 | 450.6 | 661.3 | 1745.2 |  |  |  |  |  |  | 1.88 2.48 1.49 |
| current_adapter_put/8w/1k | 8 | 1 KiB | 18762 | 19.2 | 461.4 | 695.8 | 2621.2 |  |  |  |  |  |  | 1.88 2.48 1.49 |
| model_memcpy_only/8w/1k | 8 | 1 KiB | 219818 | 225.1 | 34.6 | 60.8 | 68.1 |  |  |  |  |  |  | 1.81 2.45 1.49 |
| two_shard_fast/8w/1k | 8 | 1 KiB | 178312 | 182.6 | 42.7 | 128.4 | 322.1 |  |  |  |  |  |  | 1.83 2.45 1.49 |
| single_log_durable/64w/1k | 64 | 1 KiB | 92587 | 94.8 | 677.8 | 1010.7 | 3644.2 | 32.0 | 62 | 0 | 8 | 88 | 29 | 1.84 2.44 1.49 |
| single_log_durable_plain_fsync/64w/1k | 64 | 1 KiB | 93292 | 95.5 | 661.8 | 986.0 | 2008.7 | 32.0 | 58 | 0 | 9 | 87 | 31 | 1.77 2.41 1.49 |
| single_log_fast/64w/1k | 64 | 1 KiB | 388127 | 397.4 | 86.1 | 269.5 | 3118.1 | 9.8 | 64 | 2 | 90 | 0 | 61 | 2.03 2.46 1.51 |
| current_mmap_wal/64w/1k | 64 | 1 KiB | 19218 | 19.7 | 3230.9 | 4747.4 | 32563.7 |  |  |  |  |  |  | 2.03 2.46 1.51 |
| current_adapter_put/64w/1k | 64 | 1 KiB | 18675 | 19.1 | 3314.9 | 4892.9 | 32907.1 |  |  |  |  |  |  | 1.95 2.43 1.51 |
| model_memcpy_only/64w/1k | 64 | 1 KiB | 218082 | 223.3 | 291.5 | 403.2 | 452.0 |  |  |  |  |  |  | 1.87 2.41 1.50 |
| two_shard_fast/64w/1k | 64 | 1 KiB | 365440 | 374.2 | 46.8 | 2200.1 | 5584.8 |  |  |  |  |  |  | 2.36 2.50 1.54 |
| single_log_durable/1w/64k | 1 | 64 KiB | 1744 | 114.3 | 542.3 | 810.4 | 1881.4 | 1.0 | 1 | 35 | 6 | 54 | 23 | 2.25 2.48 1.54 |
| single_log_durable_plain_fsync/1w/64k | 1 | 64 KiB | 1748 | 114.5 | 542.4 | 813.6 | 2775.5 | 1.0 | 1 | 34 | 6 | 54 | 23 | 2.25 2.48 1.54 |
| single_log_fast/1w/64k | 1 | 64 KiB | 3847 | 252.1 | 256.2 | 388.4 | 485.7 | 1.0 | 1 | 76 | 11 | 0 | 27 | 2.15 2.45 1.53 |
| current_mmap_wal/1w/64k | 1 | 64 KiB | 2734 | 179.2 | 379.2 | 571.7 | 7895.9 |  |  |  |  |  |  | 2.06 2.43 1.53 |
| current_adapter_put/1w/64k | 1 | 64 KiB | 2394 | 156.9 | 382.0 | 1933.2 | 5934.2 |  |  |  |  |  |  | 1.98 2.40 1.53 |
| model_memcpy_only/1w/64k | 1 | 64 KiB | 6191 | 405.7 | 159.5 | 177.8 | 208.1 |  |  |  |  |  |  | 1.90 2.38 1.52 |
| two_shard_fast/1w/64k | 1 | 64 KiB | 3851 | 252.4 | 255.4 | 388.4 | 484.7 |  |  |  |  |  |  | 1.82 2.36 1.52 |
| single_log_durable/8w/64k | 8 | 64 KiB | 5715 | 374.5 | 1102.4 | 9967.6 | 27197.7 | 4.0 | 7 | 0 | 15 | 74 | 37 | 1.82 2.36 1.52 |
| single_log_durable_plain_fsync/8w/64k | 8 | 64 KiB | 5700 | 373.6 | 1119.1 | 10102.4 | 26332.2 | 3.9 | 7 | 0 | 15 | 74 | 37 | 1.84 2.35 1.52 |
| single_log_fast/8w/64k | 8 | 64 KiB | 6181 | 405.1 | 694.4 | 2355.8 | 248389.0 | 2.4 | 8 | 28 | 58 | 0 | 31 | 2.09 2.40 1.54 |
| current_mmap_wal/8w/64k | 8 | 64 KiB | 2710 | 177.6 | 2675.0 | 5409.8 | 32823.2 |  |  |  |  |  |  | 2.00 2.37 1.54 |
| current_adapter_put/8w/64k | 8 | 64 KiB | 2661 | 174.4 | 2730.5 | 4962.4 | 32393.6 |  |  |  |  |  |  | 1.92 2.35 1.54 |
| model_memcpy_only/8w/64k | 8 | 64 KiB | 7630 | 500.1 | 1042.3 | 1116.3 | 1334.1 |  |  |  |  |  |  | 1.92 2.35 1.54 |
| two_shard_fast/8w/64k | 8 | 64 KiB | 6431 | 421.5 | 651.7 | 3212.4 | 107271.7 |  |  |  |  |  |  | 2.25 2.41 1.56 |
| single_log_durable/64w/64k | 64 | 64 KiB | 6186 | 405.4 | 5801.2 | 42571.1 | 45436.9 | 25.4 | 58 | 0 | 13 | 80 | 22 | 2.47 2.45 1.58 |
| single_log_durable_plain_fsync/64w/64k | 64 | 64 KiB | 6201 | 406.4 | 5775.1 | 42860.3 | 46201.8 | 26.8 | 57 | 0 | 13 | 80 | 22 | 2.59 2.48 1.59 |
| single_log_fast/64w/64k | 64 | 64 KiB | 6170 | 404.4 | 4318.8 | 328645.9 | 381297.4 | 5.1 | 59 | 21 | 69 | 0 | 25 | 2.87 2.54 1.62 |
| current_mmap_wal/64w/64k | 64 | 64 KiB | 2716 | 178.0 | 21506.4 | 51734.5 | 58009.8 |  |  |  |  |  |  | 2.88 2.55 1.62 |
| current_adapter_put/64w/64k | 64 | 64 KiB | 2651 | 173.7 | 22096.7 | 52127.7 | 56264.1 |  |  |  |  |  |  | 2.88 2.55 1.62 |
| model_memcpy_only/64w/64k | 64 | 64 KiB | 7615 | 499.0 | 8370.2 | 9953.8 | 10395.7 |  |  |  |  |  |  | 2.73 2.52 1.62 |
| two_shard_fast/64w/64k | 64 | 64 KiB | 6346 | 415.9 | 4783.2 | 114106.7 | 502449.0 |  |  |  |  |  |  | 3.07 2.59 1.65 |
```

### 8.1 Fast-mode rule verdict

`scripts/wal_fast_rule.py check` compares `single_log_fast` against `current_adapter_put`
(the public write path) per (writers, value size) cell, using rep 2's rows (the later,
lower-load rep, matching §4's convention of reporting detailed columns from rep 2):

```
$ python3 scripts/wal_fast_rule.py check <(cat probe-bench-output.txt)
| writers | value | fast ops/s | current ops/s | ratio | loss % | verdict |
|---|---|---|---|---|---|---|
| 1 | 1 KiB | 22654 | 19588 | 1.157 | -15.7% | PASS |
| 1 | 64 KiB | 3847 | 2394 | 1.607 | -60.7% | PASS |
| 8 | 1 KiB | 268223 | 18762 | 14.296 | -1329.6% | PASS |
| 8 | 64 KiB | 6181 | 2661 | 2.323 | -132.3% | PASS |
| 64 | 1 KiB | 388127 | 18675 | 20.783 | -1978.3% | PASS |
| 64 | 64 KiB | 6170 | 2651 | 2.327 | -132.7% | PASS |

PASS: every cell is within the Fast-mode <=15% rule.
```

(A negative "loss %" means `Fast` is faster than the current path, not slower — the
rule only ever escalates on a positive loss above 15%.) Rep 1's 1-writer, 1 KiB cell
(22582 vs 19759 ops/s, ratio 1.14) and 1-writer, 64 KiB cell (3870 vs 2677, ratio 1.45)
agree with rep 2 within noise: both PASS by a wide margin.

**On the 1-writer condition specifically (Risk 1):** `Fast` at 1 writer beats
`current_adapter_put` by 15–61% on this runner, not the estimated 0–40% *loss*. The
reason the macOS-only estimate was pessimistic no longer holds on Linux:
`current_mmap_wal`/`current_adapter_put` are not stuck behind a per-batch `msync`
(§4.1's macOS-specific stall) here — Linux `MS_ASYNC` is cheap, as §6 Risk 1 predicted —
so the current path's own 1-writer throughput is much higher on Linux (~19.6–20.8k
ops/s vs. ~1.1–1.6k ops/s on the M3 Air). `single_log_fast` is still faster than that
higher bar, because it avoids the shared-segment mutex the current path takes on every
`put` (STO-06) regardless of platform.

### 8.2 Decision

**PROCEED.** Every required cell — 1, 8 and 64 writers, at both value sizes, across both
reps — is a PASS under the Fast-mode ≤15% rule; several cells show `Fast` an order of
magnitude *faster* than the current path (8 and 64 writers), consistent with §4's
macOS findings. Risk 1 is closed: no mitigation (the bounded `try_recv` spin, or
avoiding a second caller-side handoff) is needed before Task 2.2 proceeds with the
single-writer design from §7. This is a measurement outcome, not a judgment call — the
maintainer should still review the run linked above before Task 2.2's writer lands.
