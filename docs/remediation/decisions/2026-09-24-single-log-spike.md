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

## 8. Linux probe smoke test (Task 2.2 dispatch verification)

> Numbering note: the plan's Task 2.6, step 9 says to record its own Linux re-run
> under a new heading `## 8. Linux re-run (Task 2.6)`, written on the assumption that
> this document still ended at §7. Since this section now occupies §8, Task 2.6 should
> file its section as **§9** instead of the plan's literal `## 8.` — whoever executes
> Task 2.6 should check this file's current section count before adding it.

This section is **not** a re-measurement of Risk 1. It verifies that Task 2.2's Linux
probe dispatch path (`remediation-gate.yml`'s `probe` workflow_dispatch input) works
end to end, using the still-unrenamed spike bench as the smoke-test payload: the bench
still emits `single_log_fast`, not the `wal_fast` cell name `scripts/wal_fast_rule.py`
looks for (that rename is Task 2.6's). The **formal** Risk-1 verdict — does `Fast` mode
lose more than 15% put throughput against the current path, on Linux, once the real
writer exists — is Task 2.6 step 9's job, against the renamed `wal_fast`/
`current_adapter_put` cells and the actual `Wal`, not this spike's prototype.

**How this ran.** Dispatched per the plan's Task 2.2 step 8: `gh workflow run
remediation-gate.yml --ref remediation/phase-2 -f ref=remediation/phase-2 -f phase=2 -f
probe=wal-bench` (no `base_ref`).

- **Run:** <https://github.com/prk-Jr/prkdb/actions/runs/36026407910> (success), commit
  `52a252348f26d71953dfcf97645d2ada0b3a7479`.
- **Result matched the plan's prediction exactly:** only `resolve`, `probe-wal-bench`
  and `harness-result` ran (`ledger-and-tests`, `harness`, `probe-iai` all skipped); the
  job summary carries the head table; `wal_fast_rule.py --self-test` passed; the
  Fast-rule step then printed `no comparable cells: expected wal_fast +
  current_mmap_wal, or current_adapter_put in both runs` (no `wal_fast` cells exist
  until Task 2.6 renames `single_log_fast`). That step's script has no `set -o
  pipefail` (matching the plan's own YAML for this step), so the job itself still
  reports success — the "no comparable cells" outcome is visible in the step's log and
  summary, not as a red job.
- **Machine:** GitHub-hosted `ubuntu-latest` runner, 4 vCPU (tokio worker threads = 4).
  Host detail (`nproc`/`lscpu`/`df`) went to the job summary, not the step log; see the
  run linked above.

**Raw output (`head.md`, 3 reps, unedited — this is what `wal_fast_rule.py --head`
would parse once the bench's cells are renamed; it is kept here as a data point, not a
verdict):**

```text
# wal_write_path_spike
- warm-up 1000 ms, measure 3000 ms, reps 3, tokio worker threads = 4
- load average at start: 5.63 3.30 1.41
- disk ceiling, pwrite 1 MiB, no sync: 1500 MB/s (1431 writes/s)
- disk ceiling, pwrite 1 MiB + sync_data: 394 MB/s (375 writes/s)
- 4 KiB write + sync_data (F_FULLFSYNC on macOS): p50 196 µs, p99 342 µs (9533 samples)
- 4 KiB write + plain fsync(2): p50 68 µs, p99 324 µs (15434 samples)
| cell | writers | value | ops/s | MB/s | p50 µs | p99 µs | p99.9 µs | avg batch | max batch | writer idle % | writer write % | writer sync % | writer CPU % | load1 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| single_log_durable/1w/1k | 1 | 1 KiB | 4565 | 4.7 | 196.3 | 390.3 | 563.5 | 1.0 | 1 | 14 | 3 | 81 | 19 | 4.92 3.22 1.41 |
| single_log_durable_plain_fsync/1w/1k | 1 | 1 KiB | 4970 | 5.1 | 193.5 | 311.1 | 489.5 | 1.0 | 1 | 14 | 3 | 80 | 19 | 4.60 3.18 1.40 |
| single_log_fast/1w/1k | 1 | 1 KiB | 30029 | 30.7 | 32.2 | 50.3 | 82.7 | 1.0 | 1 | 79 | 8 | 0 | 35 | 4.32 3.15 1.40 |
| current_mmap_wal/1w/1k | 1 | 1 KiB | 27180 | 27.8 | 5.2 | 194.6 | 321.6 |  |  |  |  |  |  | 4.05 3.11 1.40 |
| current_adapter_put/1w/1k | 1 | 1 KiB | 24823 | 25.4 | 7.2 | 206.1 | 313.3 |  |  |  |  |  |  | 3.81 3.08 1.40 |
| model_memcpy_only/1w/1k | 1 | 1 KiB | 330590 | 338.5 | 2.9 | 6.1 | 9.4 |  |  |  |  |  |  | 3.81 3.08 1.40 |
| two_shard_fast/1w/1k | 1 | 1 KiB | 29648 | 30.4 | 32.7 | 50.5 | 84.4 |  |  |  |  |  |  | 3.58 3.04 1.39 |
| single_log_durable/8w/1k | 8 | 1 KiB | 20757 | 21.3 | 375.8 | 578.9 | 899.4 | 4.0 | 8 | 0 | 3 | 93 | 23 | 3.37 3.01 1.39 |
| single_log_durable_plain_fsync/8w/1k | 8 | 1 KiB | 21025 | 21.5 | 373.5 | 569.5 | 871.2 | 4.0 | 8 | 0 | 3 | 93 | 23 | 3.18 2.97 1.39 |
| single_log_fast/8w/1k | 8 | 1 KiB | 311265 | 318.7 | 27.9 | 51.2 | 122.0 | 1.6 | 8 | 43 | 33 | 1 | 72 | 3.17 2.97 1.40 |
| current_mmap_wal/8w/1k | 8 | 1 KiB | 25873 | 26.5 | 335.8 | 510.0 | 713.0 |  |  |  |  |  |  | 3.17 2.97 1.40 |
| current_adapter_put/8w/1k | 8 | 1 KiB | 24433 | 25.0 | 345.5 | 582.9 | 812.1 |  |  |  |  |  |  | 2.99 2.94 1.40 |
| model_memcpy_only/8w/1k | 8 | 1 KiB | 218201 | 223.4 | 37.6 | 54.2 | 64.1 |  |  |  |  |  |  | 2.83 2.91 1.39 |
| two_shard_fast/8w/1k | 8 | 1 KiB | 242105 | 247.9 | 31.1 | 82.0 | 337.7 |  |  |  |  |  |  | 2.77 2.89 1.40 |
| single_log_durable/64w/1k | 64 | 1 KiB | 109406 | 112.0 | 572.9 | 844.9 | 1174.2 | 32.0 | 60 | 0 | 7 | 88 | 26 | 2.95 2.93 1.42 |
| single_log_durable_plain_fsync/64w/1k | 64 | 1 KiB | 106686 | 109.2 | 584.1 | 870.4 | 1363.4 | 32.0 | 64 | 0 | 7 | 88 | 24 | 2.95 2.93 1.42 |
| single_log_fast/64w/1k | 64 | 1 KiB | 380024 | 389.1 | 93.2 | 279.6 | 2729.6 | 3.1 | 64 | 4 | 78 | 0 | 62 | 3.19 2.98 1.44 |
| current_mmap_wal/64w/1k | 64 | 1 KiB | 25378 | 26.0 | 2493.1 | 3168.1 | 6987.9 |  |  |  |  |  |  | 3.02 2.95 1.44 |
| current_adapter_put/64w/1k | 64 | 1 KiB | 24080 | 24.7 | 2592.4 | 3598.2 | 8826.9 |  |  |  |  |  |  | 2.85 2.91 1.44 |
| model_memcpy_only/64w/1k | 64 | 1 KiB | 219729 | 225.0 | 290.4 | 382.0 | 420.4 |  |  |  |  |  |  | 2.71 2.88 1.43 |
| two_shard_fast/64w/1k | 64 | 1 KiB | 365074 | 373.8 | 52.8 | 2023.2 | 31601.5 |  |  |  |  |  |  | 3.13 2.97 1.47 |
| single_log_durable/1w/64k | 1 | 64 KiB | 2121 | 139.0 | 457.4 | 635.9 | 932.4 | 1.0 | 1 | 41 | 5 | 52 | 16 | 3.13 2.97 1.47 |
| single_log_durable_plain_fsync/1w/64k | 1 | 64 KiB | 2137 | 140.1 | 449.8 | 640.7 | 892.6 | 1.0 | 1 | 41 | 5 | 52 | 16 | 2.96 2.93 1.47 |
| single_log_fast/1w/64k | 1 | 64 KiB | 4485 | 293.9 | 218.4 | 297.4 | 570.3 | 1.0 | 1 | 86 | 10 | 0 | 16 | 2.80 2.90 1.46 |
| current_mmap_wal/1w/64k | 1 | 64 KiB | 3062 | 200.7 | 335.0 | 482.5 | 914.5 |  |  |  |  |  |  | 2.66 2.87 1.46 |
| current_adapter_put/1w/64k | 1 | 64 KiB | 2902 | 190.2 | 341.3 | 651.4 | 1550.9 |  |  |  |  |  |  | 2.52 2.84 1.46 |
| model_memcpy_only/1w/64k | 1 | 64 KiB | 5892 | 386.2 | 168.9 | 180.7 | 193.5 |  |  |  |  |  |  | 2.52 2.84 1.46 |
| two_shard_fast/1w/64k | 1 | 64 KiB | 4479 | 293.5 | 219.0 | 299.3 | 550.0 |  |  |  |  |  |  | 2.40 2.81 1.45 |
| single_log_durable/8w/64k | 8 | 64 KiB | 5622 | 368.5 | 919.6 | 17469.6 | 34043.9 | 3.9 | 7 | 0 | 13 | 82 | 25 | 2.53 2.83 1.47 |
| single_log_durable_plain_fsync/8w/64k | 8 | 64 KiB | 5661 | 371.0 | 912.3 | 18150.7 | 33629.4 | 3.9 | 7 | 0 | 13 | 82 | 25 | 2.41 2.80 1.47 |
| single_log_fast/8w/64k | 8 | 64 KiB | 6302 | 413.0 | 489.6 | 1699.4 | 362437.3 | 2.1 | 8 | 26 | 75 | 0 | 18 | 2.38 2.78 1.47 |
| current_mmap_wal/8w/64k | 8 | 64 KiB | 2949 | 193.2 | 2465.2 | 3067.4 | 31080.5 |  |  |  |  |  |  | 2.34 2.77 1.47 |
| current_adapter_put/8w/64k | 8 | 64 KiB | 2786 | 182.6 | 2578.2 | 3898.7 | 30951.8 |  |  |  |  |  |  | 2.34 2.77 1.47 |
| model_memcpy_only/8w/64k | 8 | 64 KiB | 7338 | 480.9 | 1088.3 | 1142.9 | 1283.2 |  |  |  |  |  |  | 2.24 2.74 1.47 |
| two_shard_fast/8w/64k | 8 | 64 KiB | 6582 | 431.4 | 414.9 | 1889.0 | 157371.5 |  |  |  |  |  |  | 2.46 2.78 1.49 |
| single_log_durable/64w/64k | 64 | 64 KiB | 6169 | 404.3 | 4791.4 | 47275.1 | 49237.2 | 29.8 | 55 | 0 | 13 | 80 | 21 | 2.34 2.75 1.48 |
| single_log_durable_plain_fsync/64w/64k | 64 | 64 KiB | 6202 | 406.4 | 4718.7 | 46348.1 | 49303.7 | 29.6 | 58 | 0 | 13 | 81 | 21 | 2.31 2.74 1.49 |
| single_log_fast/64w/64k | 64 | 64 KiB | 6254 | 409.9 | 3847.8 | 378202.4 | 443894.1 | 7.4 | 64 | 22 | 77 | 0 | 19 | 2.31 2.74 1.49 |
| current_mmap_wal/64w/64k | 64 | 64 KiB | 2986 | 195.7 | 19674.5 | 47670.0 | 47916.0 |  |  |  |  |  |  | 2.21 2.71 1.48 |
| current_adapter_put/64w/64k | 64 | 64 KiB | 2818 | 184.7 | 20877.0 | 48729.7 | 49433.8 |  |  |  |  |  |  | 2.11 2.68 1.48 |
| model_memcpy_only/64w/64k | 64 | 64 KiB | 7185 | 470.9 | 8897.5 | 9420.1 | 9472.0 |  |  |  |  |  |  | 2.02 2.65 1.48 |
| two_shard_fast/64w/64k | 64 | 64 KiB | 7072 | 463.5 | 3479.1 | 66436.8 | 833276.6 |  |  |  |  |  |  | 2.18 2.67 1.49 |
| single_log_durable/1w/1k | 1 | 1 KiB | 5012 | 5.1 | 191.1 | 304.6 | 572.9 | 1.0 | 1 | 15 | 3 | 80 | 19 | 2.09 2.64 1.49 |
| single_log_durable_plain_fsync/1w/1k | 1 | 1 KiB | 5080 | 5.2 | 190.2 | 301.3 | 483.6 | 1.0 | 1 | 14 | 2 | 81 | 19 | 2.09 2.64 1.49 |
| single_log_fast/1w/1k | 1 | 1 KiB | 30104 | 30.8 | 32.2 | 49.6 | 83.4 | 1.0 | 1 | 79 | 8 | 0 | 35 | 1.92 2.60 1.48 |
| current_mmap_wal/1w/1k | 1 | 1 KiB | 28088 | 28.8 | 4.9 | 186.1 | 306.5 |  |  |  |  |  |  | 1.93 2.59 1.48 |
| current_adapter_put/1w/1k | 1 | 1 KiB | 25095 | 25.7 | 6.9 | 207.4 | 302.7 |  |  |  |  |  |  | 1.93 2.58 1.49 |
| model_memcpy_only/1w/1k | 1 | 1 KiB | 329384 | 337.3 | 2.9 | 6.3 | 9.2 |  |  |  |  |  |  | 1.86 2.55 1.48 |
| two_shard_fast/1w/1k | 1 | 1 KiB | 29372 | 30.1 | 32.9 | 51.2 | 84.8 |  |  |  |  |  |  | 1.86 2.55 1.48 |
| single_log_durable/8w/1k | 8 | 1 KiB | 18123 | 18.6 | 431.2 | 663.6 | 982.8 | 4.0 | 7 | 0 | 3 | 94 | 20 | 1.79 2.53 1.48 |
| single_log_durable_plain_fsync/8w/1k | 8 | 1 KiB | 20554 | 21.0 | 377.6 | 614.8 | 892.7 | 4.0 | 6 | 0 | 3 | 93 | 23 | 1.72 2.50 1.48 |
| single_log_fast/8w/1k | 8 | 1 KiB | 308827 | 316.2 | 28.3 | 51.3 | 131.9 | 1.7 | 8 | 44 | 32 | 1 | 72 | 1.75 2.49 1.48 |
| current_mmap_wal/8w/1k | 8 | 1 KiB | 26523 | 27.2 | 325.3 | 491.4 | 701.7 |  |  |  |  |  |  | 1.77 2.49 1.48 |
| current_adapter_put/8w/1k | 8 | 1 KiB | 24905 | 25.5 | 344.3 | 565.7 | 806.8 |  |  |  |  |  |  | 1.77 2.49 1.48 |
| model_memcpy_only/8w/1k | 8 | 1 KiB | 222936 | 228.3 | 37.2 | 52.5 | 61.5 |  |  |  |  |  |  | 1.71 2.46 1.48 |
| two_shard_fast/8w/1k | 8 | 1 KiB | 241401 | 247.2 | 31.2 | 79.4 | 338.0 |  |  |  |  |  |  | 1.81 2.47 1.49 |
| single_log_durable/64w/1k | 64 | 1 KiB | 129142 | 132.2 | 467.6 | 735.0 | 981.6 | 32.0 | 38 | 0 | 8 | 85 | 29 | 1.91 2.48 1.50 |
| single_log_durable_plain_fsync/64w/1k | 64 | 1 KiB | 127315 | 130.4 | 472.5 | 750.0 | 961.2 | 32.0 | 64 | 0 | 8 | 86 | 29 | 1.91 2.47 1.50 |
| single_log_fast/64w/1k | 64 | 1 KiB | 382502 | 391.7 | 93.8 | 267.3 | 2730.3 | 3.0 | 64 | 4 | 76 | 0 | 63 | 1.91 2.47 1.50 |
| current_mmap_wal/64w/1k | 64 | 1 KiB | 26113 | 26.7 | 2417.6 | 2967.9 | 3310.5 |  |  |  |  |  |  | 1.84 2.45 1.50 |
| current_adapter_put/64w/1k | 64 | 1 KiB | 24246 | 24.8 | 2592.1 | 3494.3 | 6657.6 |  |  |  |  |  |  | 1.77 2.42 1.49 |
| model_memcpy_only/64w/1k | 64 | 1 KiB | 222399 | 227.7 | 287.5 | 372.8 | 410.1 |  |  |  |  |  |  | 1.71 2.40 1.49 |
| two_shard_fast/64w/1k | 64 | 1 KiB | 367020 | 375.8 | 52.9 | 2055.2 | 32274.3 |  |  |  |  |  |  | 2.29 2.51 1.53 |
| single_log_durable/1w/64k | 1 | 64 KiB | 2159 | 141.5 | 443.3 | 643.7 | 906.4 | 1.0 | 1 | 41 | 5 | 51 | 16 | 2.19 2.48 1.53 |
| single_log_durable_plain_fsync/1w/64k | 1 | 64 KiB | 2151 | 141.0 | 445.8 | 630.2 | 773.2 | 1.0 | 1 | 42 | 5 | 51 | 16 | 2.19 2.48 1.53 |
| single_log_fast/1w/64k | 1 | 64 KiB | 4537 | 297.3 | 216.3 | 286.7 | 522.0 | 1.0 | 1 | 86 | 10 | 0 | 16 | 2.10 2.46 1.53 |
| current_mmap_wal/1w/64k | 1 | 64 KiB | 3069 | 201.1 | 323.8 | 489.1 | 991.4 |  |  |  |  |  |  | 2.17 2.47 1.53 |
| current_adapter_put/1w/64k | 1 | 64 KiB | 2870 | 188.1 | 348.1 | 657.5 | 1237.0 |  |  |  |  |  |  | 2.07 2.44 1.53 |
| model_memcpy_only/1w/64k | 1 | 64 KiB | 5901 | 386.7 | 168.8 | 183.3 | 195.4 |  |  |  |  |  |  | 1.99 2.42 1.53 |
| two_shard_fast/1w/64k | 1 | 64 KiB | 4455 | 292.0 | 219.9 | 300.3 | 557.7 |  |  |  |  |  |  | 1.99 2.42 1.53 |
| single_log_durable/8w/64k | 8 | 64 KiB | 5677 | 372.0 | 905.6 | 18251.1 | 34060.4 | 3.8 | 8 | 0 | 13 | 82 | 25 | 1.99 2.41 1.53 |
| single_log_durable_plain_fsync/8w/64k | 8 | 64 KiB | 5685 | 372.6 | 907.4 | 18253.6 | 33476.7 | 3.8 | 8 | 0 | 13 | 83 | 25 | 2.23 2.45 1.55 |
| single_log_fast/8w/64k | 8 | 64 KiB | 6231 | 408.4 | 476.6 | 1768.5 | 371039.8 | 2.0 | 8 | 25 | 75 | 0 | 18 | 2.53 2.51 1.57 |
| current_mmap_wal/8w/64k | 8 | 64 KiB | 2995 | 196.3 | 2441.2 | 2891.9 | 30282.3 |  |  |  |  |  |  | 2.41 2.49 1.57 |
| current_adapter_put/8w/64k | 8 | 64 KiB | 2823 | 185.0 | 2564.5 | 3165.6 | 30457.7 |  |  |  |  |  |  | 2.30 2.46 1.57 |
| model_memcpy_only/8w/64k | 8 | 64 KiB | 7393 | 484.5 | 1079.6 | 1111.3 | 1174.8 |  |  |  |  |  |  | 2.30 2.46 1.57 |
| two_shard_fast/8w/64k | 8 | 64 KiB | 7537 | 494.0 | 346.6 | 1594.9 | 252185.5 |  |  |  |  |  |  | 2.43 2.49 1.58 |
| single_log_durable/64w/64k | 64 | 64 KiB | 6200 | 406.3 | 4782.5 | 46120.8 | 48428.4 | 27.8 | 60 | 0 | 13 | 81 | 21 | 2.64 2.53 1.60 |
| single_log_durable_plain_fsync/64w/64k | 64 | 64 KiB | 6233 | 408.5 | 5137.6 | 44981.8 | 47503.1 | 29.3 | 56 | 0 | 14 | 79 | 23 | 2.83 2.57 1.62 |
| single_log_fast/64w/64k | 64 | 64 KiB | 5433 | 356.1 | 3754.3 | 395936.6 | 670431.1 | 6.6 | 64 | 19 | 72 | 0 | 16 | 2.76 2.56 1.62 |
| current_mmap_wal/64w/64k | 64 | 64 KiB | 2946 | 193.0 | 19988.1 | 48153.0 | 48823.2 |  |  |  |  |  |  | 2.62 2.54 1.62 |
| current_adapter_put/64w/64k | 64 | 64 KiB | 2798 | 183.3 | 20991.2 | 49362.8 | 50521.0 |  |  |  |  |  |  | 2.62 2.54 1.62 |
| model_memcpy_only/64w/64k | 64 | 64 KiB | 7177 | 470.3 | 8894.4 | 9384.6 | 9546.8 |  |  |  |  |  |  | 2.49 2.51 1.61 |
| two_shard_fast/64w/64k | 64 | 64 KiB | 6496 | 425.7 | 3425.1 | 112459.7 | 633263.0 |  |  |  |  |  |  | 2.93 2.60 1.65 |
| single_log_durable/1w/1k | 1 | 1 KiB | 5038 | 5.2 | 191.8 | 296.9 | 506.6 | 1.0 | 1 | 15 | 3 | 80 | 19 | 2.78 2.57 1.64 |
| single_log_durable_plain_fsync/1w/1k | 1 | 1 KiB | 4892 | 5.0 | 193.0 | 319.0 | 501.8 | 1.0 | 1 | 15 | 3 | 80 | 20 | 2.71 2.56 1.65 |
| single_log_fast/1w/1k | 1 | 1 KiB | 29834 | 30.6 | 32.5 | 50.1 | 82.0 | 1.0 | 1 | 79 | 8 | 0 | 34 | 2.71 2.56 1.65 |
| current_mmap_wal/1w/1k | 1 | 1 KiB | 28222 | 28.9 | 5.0 | 183.3 | 329.6 |  |  |  |  |  |  | 2.66 2.55 1.65 |
| current_adapter_put/1w/1k | 1 | 1 KiB | 25268 | 25.9 | 7.0 | 207.1 | 319.2 |  |  |  |  |  |  | 2.60 2.54 1.65 |
| model_memcpy_only/1w/1k | 1 | 1 KiB | 331016 | 339.0 | 2.9 | 6.0 | 9.0 |  |  |  |  |  |  | 2.48 2.52 1.65 |
| two_shard_fast/1w/1k | 1 | 1 KiB | 29647 | 30.4 | 32.6 | 50.6 | 82.5 |  |  |  |  |  |  | 2.36 2.49 1.64 |
| single_log_durable/8w/1k | 8 | 1 KiB | 17799 | 18.2 | 439.4 | 677.3 | 980.1 | 4.0 | 8 | 0 | 3 | 94 | 20 | 2.36 2.49 1.64 |
| single_log_durable_plain_fsync/8w/1k | 8 | 1 KiB | 20728 | 21.2 | 375.2 | 598.3 | 855.9 | 4.0 | 8 | 0 | 3 | 93 | 23 | 2.33 2.49 1.64 |
| single_log_fast/8w/1k | 8 | 1 KiB | 310185 | 317.6 | 28.2 | 51.0 | 125.5 | 1.7 | 8 | 43 | 32 | 1 | 72 | 2.38 2.49 1.65 |
| current_mmap_wal/8w/1k | 8 | 1 KiB | 26117 | 26.7 | 335.1 | 503.4 | 766.1 |  |  |  |  |  |  | 2.35 2.49 1.65 |
| current_adapter_put/8w/1k | 8 | 1 KiB | 24942 | 25.5 | 339.4 | 567.6 | 838.2 |  |  |  |  |  |  | 2.32 2.48 1.66 |
| model_memcpy_only/8w/1k | 8 | 1 KiB | 221206 | 226.5 | 37.3 | 53.1 | 61.9 |  |  |  |  |  |  | 2.32 2.48 1.66 |
| two_shard_fast/8w/1k | 8 | 1 KiB | 242044 | 247.9 | 31.2 | 79.4 | 349.0 |  |  |  |  |  |  | 2.22 2.45 1.65 |
| single_log_durable/64w/1k | 64 | 1 KiB | 125427 | 128.4 | 479.5 | 765.2 | 1042.2 | 32.0 | 61 | 0 | 8 | 86 | 28 | 2.28 2.46 1.66 |
| single_log_durable_plain_fsync/64w/1k | 64 | 1 KiB | 125446 | 128.5 | 469.8 | 782.7 | 991.2 | 32.0 | 62 | 0 | 8 | 86 | 29 | 2.50 2.50 1.68 |
| single_log_fast/64w/1k | 64 | 1 KiB | 383612 | 392.8 | 92.8 | 277.3 | 2991.2 | 3.0 | 64 | 4 | 78 | 0 | 63 | 2.46 2.50 1.68 |
| current_mmap_wal/64w/1k | 64 | 1 KiB | 25277 | 25.9 | 2495.1 | 3025.2 | 3899.3 |  |  |  |  |  |  | 2.34 2.47 1.68 |
| current_adapter_put/64w/1k | 64 | 1 KiB | 24537 | 25.1 | 2553.9 | 3343.0 | 6707.3 |  |  |  |  |  |  | 2.34 2.47 1.68 |
| model_memcpy_only/64w/1k | 64 | 1 KiB | 218611 | 223.9 | 298.4 | 381.2 | 418.4 |  |  |  |  |  |  | 2.31 2.46 1.68 |
| two_shard_fast/64w/1k | 64 | 1 KiB | 361228 | 369.9 | 53.4 | 2014.0 | 32902.7 |  |  |  |  |  |  | 2.85 2.57 1.72 |
| single_log_durable/1w/64k | 1 | 64 KiB | 2142 | 140.4 | 443.2 | 644.0 | 925.8 | 1.0 | 1 | 42 | 5 | 51 | 16 | 2.70 2.54 1.71 |
| single_log_durable_plain_fsync/1w/64k | 1 | 64 KiB | 2122 | 139.1 | 450.9 | 655.8 | 882.6 | 1.0 | 1 | 42 | 5 | 51 | 16 | 2.65 2.54 1.71 |
| single_log_fast/1w/64k | 1 | 64 KiB | 4451 | 291.7 | 220.2 | 300.9 | 559.8 | 1.0 | 1 | 86 | 10 | 0 | 16 | 2.65 2.54 1.71 |
| current_mmap_wal/1w/64k | 1 | 64 KiB | 3035 | 198.9 | 332.9 | 487.3 | 751.2 |  |  |  |  |  |  | 2.51 2.51 1.71 |
| current_adapter_put/1w/64k | 1 | 64 KiB | 2876 | 188.5 | 349.2 | 650.6 | 932.5 |  |  |  |  |  |  | 2.39 2.48 1.71 |
| model_memcpy_only/1w/64k | 1 | 64 KiB | 5886 | 385.7 | 169.1 | 182.8 | 193.0 |  |  |  |  |  |  | 2.28 2.46 1.70 |
| two_shard_fast/1w/64k | 1 | 64 KiB | 4487 | 294.1 | 218.4 | 301.1 | 583.7 |  |  |  |  |  |  | 2.18 2.44 1.70 |
| single_log_durable/8w/64k | 8 | 64 KiB | 5707 | 374.0 | 885.4 | 18302.8 | 34421.3 | 3.8 | 7 | 0 | 13 | 82 | 25 | 2.40 2.48 1.72 |
| single_log_durable_plain_fsync/8w/64k | 8 | 64 KiB | 5699 | 373.5 | 893.6 | 18172.5 | 33557.6 | 3.8 | 7 | 0 | 13 | 82 | 26 | 2.40 2.48 1.72 |
| single_log_fast/8w/64k | 8 | 64 KiB | 6389 | 418.7 | 482.7 | 1728.2 | 369682.5 | 2.1 | 8 | 26 | 75 | 0 | 19 | 2.61 2.52 1.74 |
| current_mmap_wal/8w/64k | 8 | 64 KiB | 3028 | 198.5 | 2424.2 | 2884.4 | 30250.6 |  |  |  |  |  |  | 2.48 2.49 1.73 |
| current_adapter_put/8w/64k | 8 | 64 KiB | 2810 | 184.1 | 2569.1 | 3181.6 | 30782.4 |  |  |  |  |  |  | 2.44 2.49 1.73 |
| model_memcpy_only/8w/64k | 8 | 64 KiB | 7315 | 479.4 | 1092.3 | 1126.2 | 1295.1 |  |  |  |  |  |  | 2.33 2.46 1.73 |
| two_shard_fast/8w/64k | 8 | 64 KiB | 7092 | 464.8 | 407.2 | 1602.8 | 63043.2 |  |  |  |  |  |  | 2.33 2.46 1.73 |
| single_log_durable/64w/64k | 64 | 64 KiB | 6205 | 406.7 | 4738.1 | 46773.0 | 50208.7 | 29.6 | 60 | 0 | 13 | 81 | 20 | 2.22 2.44 1.72 |
| single_log_durable_plain_fsync/64w/64k | 64 | 64 KiB | 6204 | 406.6 | 4842.5 | 46181.5 | 49601.7 | 30.1 | 59 | 0 | 13 | 80 | 21 | 2.12 2.41 1.72 |
| single_log_fast/64w/64k | 64 | 64 KiB | 6286 | 411.9 | 3749.2 | 387123.3 | 396776.1 | 6.7 | 64 | 22 | 77 | 0 | 18 | 2.11 2.41 1.72 |
| current_mmap_wal/64w/64k | 64 | 64 KiB | 2961 | 194.1 | 19855.8 | 48408.5 | 49055.7 |  |  |  |  |  |  | 2.02 2.38 1.72 |
| current_adapter_put/64w/64k | 64 | 64 KiB | 2793 | 183.0 | 20972.7 | 49238.1 | 49872.3 |  |  |  |  |  |  | 1.94 2.36 1.71 |
| model_memcpy_only/64w/64k | 64 | 64 KiB | 7174 | 470.2 | 8916.8 | 9090.8 | 9297.3 |  |  |  |  |  |  | 1.94 2.36 1.71 |
| two_shard_fast/64w/64k | 64 | 64 KiB | 7982 | 523.1 | 3134.4 | 64748.2 | 768948.6 |  |  |  |  |  |  | 2.43 2.45 1.75 |
```

Informally, this run's `current_adapter_put`/`current_mmap_wal` numbers again look
nothing like the M3 Air's (§4): e.g. `current_adapter_put/1w/1k` is ~24.8k ops/s here
vs. ~1.1–1.6k ops/s on the M3 Air, consistent with the earlier smoke run (see the prior
probe dispatch, run 36023246514) and with §4.1's diagnosis that the current path's slow
macOS numbers come from a platform-specific `msync` stall rather than the design. That
is background color for Task 2.6, not this task's decision.

### 8.1 Decision

No decision is made here. Task 2.2's scope is the probe dispatch path and the
`wal_fast_rule.py` tooling, both verified working above. Risk 1 remains open until
Task 2.6 step 9 runs the real rule against the real `Wal` on Linux; that task records
the PROCEED/STOP call and, per its own step 9, the raw bench rows it captures.
