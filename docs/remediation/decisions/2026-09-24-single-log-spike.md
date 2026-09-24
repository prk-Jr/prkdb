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
