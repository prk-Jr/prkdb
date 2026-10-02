# Streaming log on the single `Wal` (Task 2.15b, D13)

**Status:** proposed, for maintainer review before any code.
**Plan:** `docs/superpowers/plans/2026-09-23-root-cause-remediation.md`, "Task 2.15b".
**Spec:** `2026-09-23-root-cause-remediation-design.md`, D3, D4, D11, D13, §6, §7.1.
**Base:** `remediation/phase-2` @ b2a5359 (compaction, Task 2.15, merged).

This note answers the design questions the plan lists for Task 2.15b, records what we take
from Kafka, Redpanda and Iggy, and ends with the questions only the maintainer can settle
(§13) and a task breakdown (§14). Nothing here is implemented yet.

---

## 1. What exists today, and what it lacks for a stream

Everything a stream needs is already in `prkdb-core::wal`, with five gaps.

| Piece | Where | Use for a stream |
|---|---|---|
| One writer thread, group commit, `Durable`/`Fast` ack contract | `wal/log.rs` | The append path, unchanged. |
| Bounded admission (`max_queued_bytes`), `RecordTooLarge`, poisoning on I/O error | `wal/log.rs` | Producer backpressure and fsyncgate, unchanged. |
| Contiguous frame LSNs, `{first_lsn:020}.wal` segments, CRC per frame, torn-tail truncation | `wal/frame.rs`, `wal/segment.rs` | Offsets, and the crash rules. |
| `acked_lsn` / `durable_lsn`, `scan_from` / `scan_durable_from` | `wal/log.rs` | The two read bounds. |
| `LOG_STATE.log_start`, `set_log_start` → `remove_leading_segments`, the open-time leftover rule | `wal/log_state.rs`, `wal/log.rs` | The durable retention floor. |
| `StorageError::CompactedCursor { cursor, floor }` | `prkdb-types/src/error.rs` | The "below the floor" error. |
| `OffsetStore`, `StorageOffsetStore`, `AutoOffsetReset`, `ConsumerGroupCoordinator` | `prkdb-types/src/consumer.rs`, `prkdb/src/consumer.rs` | Consumer offsets and partition assignment. |
| `FORMAT`, `LOCK`, container rule (a container holds no WAL and no `FORMAT`) | `prkdb/src/storage/{format,lock}.rs`, Task 2.11 | Data-directory and partition layout. |

The five gaps:

1. **Releasing live segments.** `set_log_start` and `remove_leading_segments` refuse any
   segment that still holds a non-`Elided` frame, and `Wal::open` treats such a leftover
   below `log_start` as corruption. That is right for the keyed log, where compaction is the
   only thing that removes segments. A stream's retention removes live data on purpose.
2. **Seeking.** `scan_from_capped` walks every segment from the oldest and decodes every
   frame, dropping those below `from`. That is fine for compaction. For a consumer near the
   tail of a 100 GiB stream it is O(log size) per poll. There is no early stop either.
3. **Waiting for new data.** Nothing notifies a reader when `acked_lsn` moves.
4. **Time.** Frames carry no timestamp, and the writer rolls only on size. Retention by age
   needs both a time per segment and a way to seal a quiet active segment.
5. **A record frame.** `Batch` is the keyed op list. A stream needs a record batch: N
   records, an append time, optional keys and headers.

The deleted adapters (`git show 01caedd^:crates/prkdb/src/storage/streaming_adapter.rs`)
show what to avoid as well as what to keep. Their `next_offset` started at 0 on every open,
so offsets were reused after a restart. They ran on the mmap WAL, which acknowledged writes
it had not synced, and that is where the "2x faster than Kafka" claim came from. They had no
retention and no consumer offsets. The API shape is worth keeping: `append_batch(records) ->
first offset`, `read_from(offset, max)`, `sync()`, and a partitioned wrapper with round-robin
or key-hash routing.

---

## 2. Prior art and what we adopt

| System | Model | What we take | What we leave |
|---|---|---|---|
| **Kafka** | A partition is a directory of segment files named by base offset (`.log`, plus sparse `.index` and `.timeindex`). `segment.bytes` (default 1 GiB) and `segment.ms` (7 days) roll the active segment. `retention.ms` / `retention.bytes` (per partition) delete **whole closed segments**: "retention and cleaning is always done a file at a time" [K1]. The log start offset moves forward on deletion. A fetch below it is out of range, and `auto.offset.reset` (`earliest`/`latest`/`none`, default `latest`) decides what the consumer does [K2]. `cleanup.policy=compact` keeps the last value per key, and `delete.retention.ms` keeps tombstones that long [K1]. Committed group offsets live in `__consumer_offsets` and mean "next offset to read". Durability comes from replication: `flush.messages`/`flush.ms` default to never, so an ack does not imply fsync [K1]. | Whole-segment retention by age and size, with the active segment exempt. A durable log start. Out-of-range as an error. Committed offset = next to read. A per-segment max timestamp for age retention. A sparse in-memory offset index. | Silent `auto.offset.reset=latest` on out-of-range (we raise an error by default). Compacted topics (the keyed adapter already does that). Ack-without-fsync as the default. |
| **Redpanda** | Kafka API and segment model, defaults `segment.bytes` 128 MiB and `retention.ms` 7 days. It removes whole closed segments [R1]. With `acks=all` it fsyncs before acking by default. `write.caching` is an opt-in relaxed mode that acks before fsync and flushes on `flush.ms`/`flush.bytes` [R1][R2]. | The durability framing: fsync-before-ack by default (our `Durable`), with an explicit, documented relaxed mode bounded by time (our `Fast`, `sync_interval_ms`). A smaller default segment size, so retention is finer-grained. | Replication (D13: Raft only, Phase 4). |
| **Apache Iggy** | Streams → topics → partitions → segments. Each partition is a directory of `.log` + `.index` (offset and timestamp) files, "named by their 20-digit start offset". Segments seal at 1 GiB [I1]. Per-topic retention: `max_topic_size` deletes the oldest sealed segments past the size, and `message_expiry` deletes sealed segments older than the expiry. "The active segment is never touched." A background cleaner runs every `interval` (1 min) [I2]. Server-side consumer and consumer-group offsets are stored under `offsets/` [I1]. Polling by offset, by timestamp, first/last N, or "next N for this consumer" [I3]. | The same file naming we already use (`{first_lsn:020}.wal`). A periodic best-effort cleaner over sealed segments only. Offsets kept server-side, next to the data. Poll by offset and by timestamp. Iggy's storage-compat check is already the model for spec §7 2e. | Thread-per-core with io_uring. Our single writer thread is measured (decision record 2026-09-24) and stays. |

Sources:
- [K1] Kafka topic configs: <https://kafka.apache.org/43/configuration/topic-configs/>
- [K2] Kafka consumer configs (`auto.offset.reset`, `isolation.level`): <https://kafka.apache.org/43/configuration/consumer-configs/>
- [R1] Redpanda topic properties: <https://docs.redpanda.com/current/reference/properties/topic-properties/>
- [R2] Redpanda write caching: <https://www.redpanda.com/blog/write-caching-performance-benchmark>, <https://docs.redpanda.com/current/develop/config-topics/>
- [I1] Iggy architecture: <https://iggy.apache.org/docs/introduction/architecture/>
- [I2] Iggy server config (`[data_maintenance.messages]`, retention comment): <https://github.com/apache/iggy/blob/master/core/server/config.toml>
- [I3] Iggy overview: <https://iggy.apache.org/docs/introduction/about/>

One difference from all three shapes several sections below. Their offsets are **dense**:
every record is offset + 1. Ours are **sparse** (§4): `EventSeq = lsn << 16 | idx`. Lag in
records cannot be computed by subtraction, and "offset + 1" is a valid resume position but
usually not a record.

---

## 3. Layout (D11, Task 2.11)

### 3.1 One stream = one data directory

```
<stream dir>/
  FORMAT            format = 2, created_by = "...", kind = "stream"
  LOCK              Task 2.11b, held for the stream's lifetime
  LOG_STATE         log_start = the retention floor (deletes_compacted_through stays 0)
  00000000000000000001.wal
  00000000000000004711.wal
  ...
```

There is no `checkpoints/` directory and no key index. A stream directory is a data
directory in the D3/D11 sense: one WAL, one `FORMAT`, one lock.

**`FORMAT` gains a `kind` key**: `kind = "kv"` (the default when the key is absent, so
every existing directory and the golden fixture keep their bytes) or `kind = "stream"`.
`ensure_format` takes the expected kind and refuses a mismatch before anything is written:
`data directory {dir} is a stream, not a key/value store` and the reverse. Without this, a
keyed adapter opened on a stream directory would fail deep inside replay (unknown frame
kind) with a corruption message, and a stream opened on a keyed directory would fail on
`Batch` frames. Today's parser ignores unknown keys. That tolerance is exactly why the
refusal has to be explicit, and why it must land before the format freezes (§5.4).

### 3.2 A partitioned stream = a container of N stream directories

```
<root>/                       container: no FORMAT, no WAL (Task 2.11's rule)
  STREAM                      manifest: version, partitions = N, created_by
  partition_0/                a stream data directory (3.1)
  partition_1/
  ...
  __offsets/                  optional: a kv data directory for consumer offsets (§7)
```

- **The partition count is durable.** Key-hash routing depends on N, so `STREAM` records
  it. Opening with a different N is refused: repartitioning is not supported (§13 Q11).
- **Creation order:** create every `partition_<i>/` (each through `ensure_format`), then
  write `STREAM` atomically (tmp → `sync_data` → rename → `sync_dir(root)`).
  - On open, a root that has partition directories but no `STREAM` was cut off during
    creation. If every partition directory holds no segment with a frame, creation is
    finished. Otherwise the open is refused, naming the directories.
- `partition_<n>` matches the multi-raft layout's naming (Task 2.11).
- Stream names given by users go through `catalog::validate_name` (SCH-01's allowlist)
  before they become path components.
- Routing: explicit partition, round-robin, or key hash through Task 2.13's fixed-seed
  `seahash` partitioner, so key → partition has golden vectors.

**Not in scope:** nesting streams inside a `PrkDb` keyed data directory. A data directory
holds exactly one WAL, so streams sit next to it under a container root, never inside it.
`PrkDb` integration (`db.stream("name")`) is §13 Q12.

---

## 4. Records and offsets

### 4.1 Decision: a new `FrameKind::Records = 3`, not a `Batch` op tag

| | New frame kind (chosen) | `Batch` op tag 6 |
|---|---|---|
| What separates the two directory kinds | The frame header. The stream reader refuses `Batch`, and the keyed replay refuses `Records`, before decoding. | Both decode through `Batch::decode`. Every keyed consumer of `Batch` (replay, compaction liveness, `get_changes_since`, checkpoint) must learn to skip or refuse tag 6. |
| Record-shaped layout (append time once per batch, optional key, headers) | Free to design (4.2). | Squeezed into the op-list shape: a tag byte per record, `Put`-like framing. |
| Reading a timestamp at open without decompressing | Yes: the time sits in the uncompressed header. | No: ops sit inside the possibly compressed body. |
| Shared code | Reuses `compress`/`decompress_bounded` and the `version | codec | raw_len` envelope. | Reuses all of `batch.rs`. |
| Cost | One arm in `FrameKind::from_u8` and `decode_frame`, a new codec file, and fuzz targets. | A new tag, plus guards in four keyed paths. |

A stream directory therefore holds only `Records` frames. `Elided` would only come from
compaction, which never runs on streams (4.4). Anything else in a stream directory is
`CorruptSegment` naming the frame.

### 4.2 `Records` payload (little-endian), `crates/prkdb-core/src/wal/records.rs`

```text
version          u8    1
codec            u8    CompressionType discriminant actually used (as batch.rs)
raw_len          u32   length of the uncompressed body
append_time_ms   i64   wall clock at encode, ms since the Unix epoch
count            u32   1..=65_536 records
body                   compressed when codec != 0
  per record, in idx order (idx = position, 0-based):
    flags        u8    bit0 has_key, bit1 has_headers; other bits must be 0
    [key_len u32 | key]                          if has_key
    value_len    u32 | value
    [header_count u16 | (name_len u16 | name UTF-8 | val_len u32 | val)*]   if has_headers
```

Decoding rejects an unknown version or codec, `count` of 0 or above 65,536, `raw_len`
above `MAX_PAYLOAD_LEN` (checked before decompressing, as in `batch.rs`), set reserved
flag bits, invalid UTF-8 header names, lengths that run past the end, and trailing bytes.
It is a fuzz target (§10.4).

**Headers are in version 1.** Format 2 freezes at Task 2.24, so adding them later would cost
`FORMAT_VERSION + 1` and a migration (D3/D4). The `flags` byte costs one byte per record, and
a record without headers carries nothing else. See §13 Q6.

**Timestamps.** One `append_time_ms` per append, stamped by `StreamLog::append` when it
encodes the payload. That happens before admission, so concurrent appenders can land
slightly out of time order. The skew is bounded by queueing time. Retention uses each
segment's **maximum** time, as Kafka does, so out-of-order times are harmless there.
Seek-by-time returns the first frame whose time is ≥ the target, scanning forward from the
sparse index (§6.3). Producer-supplied create times are §13 Q7.

### 4.3 Offsets: `EventSeq = lsn << 16 | idx`

- An append of `n` records (1 ≤ `n` ≤ 65,536) is one frame at LSN `L`. Record `i` has
  offset `L << 16 | i`, so the append returns every record's offset without a frame per
  record. `AppendAck { lsn, count }` exposes `offset(i)`, `first()` and `last()`.
- **Type.** `prkdb_types::event::EventSeq`, as Task 2.20 specifies it (`from_wal(lsn,
  idx)`, `raw()`, `Ord`, a 20-digit `Display`). If 2.15b.2 lands before 2.20, it creates
  the type with exactly that spec and 2.20 reuses it, so there is one offset type across
  events and streams.
- **Limits.** LSNs must be < 2⁴⁸. `append` refuses with `StorageError::Validation` when
  `wal.next_lsn() >= 1 << 48`. That is unreachable in practice (2.8 × 10¹⁴ appends), but it
  is checked rather than left to wrap.
- **Sparse, never reused, never moved.** Offsets jump by 2¹⁶ between frames. Retention
  removes old segments but never renumbers, and LSNs continue across reopen from the
  recovered `next_lsn`. The one exception is `Fast` power loss, which can drop acked frames
  so their LSNs are reissued (§7.3).
- **Resume positions.** "Next offset after record `r`" is `r.raw() + 1`, which is valid even
  when `idx + 1` does not exist. `read_from(o)` starts at frame `o >> 16` and skips records
  with `idx < (o & 0xFFFF)` in that one frame. If the low bits are `0xFFFF`, `+ 1` carries
  into the next LSN, which is correct. `consumer.rs` already commits `last + 1`, so its
  convention carries over unchanged.
- `earliest()` = `log_start << 16`. `next_offset()` = `(acked_lsn + 1) << 16` (the high
  watermark consumers read up to). `durable_end()` = `(durable_lsn + 1) << 16`.

### 4.4 Streams are never compacted

`idx` is positional because a stream frame is never rewritten. That is the difference from
Task 2.20's `Event` op, which stores `idx` explicitly because compaction drops events from
frames. A compacted stream (Kafka's `cleanup.policy=compact`) would need explicit `idx`,
which is a format change. The keyed adapter is the compacted store. `StreamLog` never calls
`replace_segment`, and its directory kind keeps `WalStorageAdapter::compact` away from it.

---

## 5. D3/D4 and Task 2.24

### 5.1 What the stream changes on disk

1. A new frame kind (3) and its payload codec.
2. A new `FORMAT` key (`kind`) and the refusal rule for it.
3. `LOG_STATE` semantics: `log_start` may move past segments holding live frames, in
   stream directories only. The bytes and the layout do not change.
4. A new container manifest (`STREAM`).

### 5.2 Recommendation: land 5.1 (1)–(4) before Task 2.24

Task 2.24 freezes format 2: after it, any byte change a v2 build writes or reads needs
`FORMAT_VERSION + 1` and a registered migration. If the stream lands after the freeze:

- Every v2 keyed directory would need a 2 → 3 migration that only rewrites `FORMAT`, plus
  segment headers that carry the format number (`write_segment_header` writes
  `FORMAT_VERSION` into every segment). That means rewriting every segment header, or
  accepting both numbers in sealed segments. Either way it is real migration code for a
  feature that adds nothing to keyed directories.
- The golden directory would be regenerated once more. Spec revision 11 moved the freeze to
  2.24 precisely to avoid that.

So 2.15b.1–2.15b.3 (§14) come before 2.24, and 2.24's generator gains `out/stream/`: a
two-partition stream with headers, keys, LZ4, at least three segments, one retention run
that moved `log_start`, and a `STREAM` manifest, with `expected-stream.json` listing
`(partition, offset, key, value, headers)`. Retention (2.15b.4) and consumers (2.15b.6)
write nothing new to disk and can land after 2.24. The consumer offset value encoding (§7.3)
is the one exception: it must also land before 2.24 if it changes what `StorageOffsetStore`
writes.

### 5.3 A hardening to land with it: an unknown frame kind is never a torn tail

Today `decode_frame` checks the kind byte **before** the CRC, and `scan_segment` reports
`UnknownKind` as a frame fault. In the **last** segment, `Wal::open` treats any frame fault
as a torn tail and truncates there. A build that does not know kind 3 would therefore
truncate a stream's active segment at its first `Records` frame. The segment header's
format number prevents that across format versions. Within one version it protects
nothing, and adding kind 3 is exactly a same-version change before the freeze. Fix: check
the CRC first. A CRC-valid frame with an unknown kind is `WalError::UnsupportedFormat`
(refuse, never truncate). A CRC-invalid one stays `BadCrc` (torn). Add a test that a frame
with kind 99 and a valid CRC in the last segment refuses the open and leaves the file's
length unchanged.

(Minor: the doc comment on `frame::Lsn` still says "its byte offset within its segment".
An LSN is the frame's sequence number. Fix it in passing.)

### 5.4 If the maintainer prefers to defer the stream past 2.24

Then 2.15b ships with `FORMAT_VERSION = 3` and a registered migration `2 → 3` (FORMAT
rewrite, plus every segment header's format field rewritten in place with `sync_data` per
segment, then `FORMAT` last), and 2.24's compat check gains a v2 fixture that must migrate.
It is doable, and it costs about a day plus permanent test surface. §13 Q2.

---

## 6. Reads

### 6.1 Contract

```rust
pub fn read_from(&self, from: StartAt, limits: ReadLimits) -> Result<ReadBatch, StorageError>;
pub fn read_durable_from(&self, from: StartAt, limits: ReadLimits) -> Result<ReadBatch, StorageError>;
```

- **Default bound: acked** (`Wal::scan_from`'s cap `acked_lsn`), the same as
  `get_changes_since`. In `Durable` mode acked equals durable. In `Fast` mode a reader sees
  an acked record before it syncs, and a power cut can still take it away (§7.3).
- **`read_durable_from`** caps at `durable_lsn`, for consumers that must never act on a
  record that could still vanish: exactly-once sinks, and replicas built on the stream.
- **`StartAt`**: `Offset(EventSeq)`, `Earliest`, `Latest` (= `next_offset()`, returns
  nothing until new data arrives), `Timestamp(i64)`.
- **Below the floor:** `Offset(o)` with `o < earliest()` returns
  `StorageError::CompactedCursor { cursor: o.raw(), floor: earliest().raw() }`. That
  includes `o = 0` on a fresh log, since the first offset is `1 << 16`. Nothing is skipped
  silently, and no offset is special-cased. "From the beginning" is spelled `Earliest`. The
  floor is read before the scan. A segment that retention removes during the scan is still
  read through the cloned handle (on Unix an unlinked file stays readable). The records
  returned were real and valid when the read started, so unlike compaction no
  after-the-scan check is needed.
- **Above the end:** `Offset(o)` with `o > next_offset()` returns
  `StorageError::Validation("offset {o} is past the end of the stream ({next})")`. This is
  how a consumer whose committed position outran a `Fast` power loss finds out (§7.3), as
  long as no new appends have refilled those LSNs.
- **`ReadLimits { max_records, max_bytes }`**: the batch stops at whichever comes first,
  but always returns at least one record if one exists (Kafka's rule: a record larger than
  `max_bytes` would otherwise wedge the consumer). `ReadBatch { records, next: EventSeq,
  high_watermark: EventSeq }`, where `next` is the resume position.
- A read is synchronous `pread` I/O through `Vfs`, bounded by `limits`, like `Wal::read`
  and `get_changes_since` today. Async callers with large `max_bytes` use `spawn_blocking`
  (§13 Q13).

### 6.2 `Wal` changes needed (2.15b.1)

1. **Start at the right segment.** `scan_from_capped` picks the start through
   `segments.range(..=from_lsn).next_back()` instead of scanning from the oldest. The
   keyed `get_changes_since` gets the same speed-up.
2. **Early stop.** The visitor returns `ControlFlow<()>` (or a new `scan_from_bounded`
   wraps a `max_frames`/`max_bytes` budget), so a 1 MiB read never decodes the whole tail.
3. **Start at a byte offset.** `scan_segment_from(file, path, first_lsn, start: (Lsn, u64),
   visit)` begins at a known frame boundary, supplied by the sparse index (6.3), instead of
   the segment header. The LSN-continuity check starts from that LSN.
4. **Wake readers.** `Wal::subscribe_acked() -> tokio::sync::watch::Receiver<Lsn>`, sent
   once per batch after `commit_batch`'s reply loop, which raises `acked_lsn` per item
   right before each `Ok` reply, so a woken reader's `scan_from` cap already includes the
   frame. `StreamLog::wait_for(after, timeout)` awaits it. Commit
   hooks are not used for this: they run before `acked_lsn` moves.

### 6.3 Sparse offset index (memory only)

Per segment, a `Vec<(Lsn, u64 /*byte offset*/, i64 /*append_time_ms*/)>` with one entry per
64 KiB of frames, plus the segment's max append time. That is about 256 KiB of index per
GiB of log. It is built during `Wal::open`'s replay (every frame is visited anyway to check
its CRC, and the time sits in the uncompressed header) and extended by each append's commit
hook, which receives the `RecordLoc`. The time is captured in the hook's closure. Nothing
about the index is written to disk, and that is deliberate. Kafka's and Iggy's index files
exist to make open fast, and our open already scans every segment for STO-04. A persisted
index can come later as a measured optimisation, with its own format entry.

---

## 7. Consumers (reuse `consumer.rs`)

### 7.1 What is reused, and what is new

- **Reused:** the `OffsetStore` trait and `StorageOffsetStore` (key
  `__consumer_offset:{group}:{stream}:{partition}`), `AutoOffsetReset`,
  `ConsumerGroupCoordinator` + `AssignmentStrategy` for assigning partitions to group
  members in-process, and the "committed = next to read = last + 1" convention that
  `PrkConsumer::poll` already uses.
- **Not reused:** `PrkConsumer<C>` and the `Consumer<C: Collection>` trait. Both are typed
  to a `Collection` and read the outbox (`outbox_list`, ids `Type:partition:seq`). A stream
  consumer reads bytes from `StreamLog::read_from`. Bending `Consumer<C>` around bytes would
  either change a public trait or wrap every record in a fake `Collection`.
- **New:** `StreamConsumer`, holding an `Arc<PartitionedStream>`, an
  `Arc<dyn OffsetStore>`, a `StreamConsumerConfig` (group id, assigned partitions or a
  coordinator, `auto_offset_reset`, `on_out_of_range`, `max_poll_records`, `max_poll_bytes`,
  `read_bound: Acked | Durable`, auto-commit interval), and `poll`, `commit`,
  `commit_offsets`, `seek`, `position`, `committed`.

### 7.2 Where offsets live

The stream API takes an `Arc<dyn OffsetStore>`. `PartitionedStream::default_offset_store()`
opens a `WalStorageAdapter` at `<root>/__offsets/`: a kv data directory, its own `FORMAT`
and `LOCK`, a sibling of the partitions, like Kafka's `__consumer_offsets` and Iggy's
`offsets/`. A commit is therefore as durable as that adapter's `SyncMode`. Delivery is
at-least-once: process, then commit. §13 Q9.

### 7.3 Out-of-range and `Fast` power loss

- **No committed offset:** `auto_offset_reset` decides (`Earliest`, `Latest`, or `None`,
  which is an error), as today.
- **Committed offset below `earliest()`** (retention ran past an idle group):
  `on_out_of_range` decides. The default is `Error`: the `CompactedCursor` propagates and
  the caller chooses. `ResetToEarliest` is opt-in and logs the number of skipped LSNs.
  Kafka's silent `latest` reset is not offered as a default, per the plan's "never silent
  skip".
- **Offset reuse after a `Fast` power loss.** In `Fast` mode, acked frames newer than the
  last sync can vanish. The next appends reissue those LSNs with different records. A
  consumer that read the lost records with the acked bound and committed past them now
  holds a position that **looks valid** once the log has refilled. It would silently skip
  the new records. Kafka handles the same hazard (truncation after leader change) by
  validating positions against leader epochs. Our equivalent is cheap:
  - A commit stores `(next: EventSeq, last_frame_crc: u32)`: the CRC of the frame holding
    the last consumed record.
  - On resume, `StreamConsumer` reads that one frame header. A missing frame (past the end)
    or a different CRC is `StorageError::OffsetDiverged { committed, reason }`, handled like
    out-of-range (`Error` by default).
  - In `Durable` mode the check always passes (acked = durable). It costs one header read
    per partition per consumer start.
  - It needs the offset store to hold 12 bytes instead of an `Offset`. Either
    `StorageOffsetStore` stores a small versioned struct under the same key, or a sibling
    key `__consumer_offset_check:...` holds the CRC. §13 Q8.
  - Reading with `read_bound: Durable` avoids the hazard entirely, at the cost of up to
    `sync_interval_ms` extra latency.

---

## 8. Retention

### 8.1 Policy

```rust
pub struct RetentionPolicy {
    pub max_age: Option<Duration>,    // Kafka retention.ms, Iggy message_expiry
    pub max_bytes: Option<u64>,       // per partition; Kafka retention.bytes, Iggy max_topic_size (per topic)
}
pub struct StreamConfig {
    pub wal: WalConfig,               // log_dir = the stream dir; sync_mode etc.
    pub retention: RetentionPolicy,   // default: both None (keep everything)
    pub segment_max_age: Option<Duration>, // Kafka segment.ms; default = max_age / 4 when max_age is set
    pub retention_interval: Duration, // background check, default 60 s (Iggy's cleaner interval)
    pub clock: Arc<dyn Clock>,        // injectable, for the harness's AdvanceClock
}
```

The default segment size for streams is **128 MiB** (Redpanda's default), not
`WalConfig`'s 1 GiB. Retention is segment-granular, and 1 GiB segments make `max_bytes`
overshoot by up to 1 GiB per partition. §13 Q16.

### 8.2 What retention removes

Retention removes **sealed segments only, oldest first, never the active one**. It stops
at the first segment that is not eligible, so the removed set is always a prefix and the
log stays contiguous. A segment is eligible if either:

- **age:** its max `append_time_ms` < `now - max_age`; or
- **size:** total `log_bytes()` minus this segment's length is still ≥ `max_bytes`. The log
  ends at or above `max_bytes` and overshoots by less than one segment, as in Kafka.

**Rolling a quiet segment.** If the active segment holds frames and its oldest frame's time
is older than `segment_max_age`, retention asks the writer to roll: a new `Request::Roll`,
which syncs the active segment and opens the next one, exactly what `roll_segment` does on
size. Without it, a stream that stopped receiving writes would keep its last segment
forever.

**Unlike keyed compaction, this deletes data.** Removed records are gone for every reader.
The floor `earliest() = log_start << 16` rises. Offsets above it keep their meaning. Readers
below it get `CompactedCursor`. Retention ignores consumers, like Kafka and Iggy: a slow
group loses unread data and learns it through 7.3. §13 Q10.

### 8.3 Mechanism: Task 2.15's two calls, generalised

`WalOptions` gains `front_release: FrontRelease`, fixed at `Wal::open`:

- `ElidedOnly` (keyed directories, the default): today's behaviour, unchanged.
- `Retention` (stream directories): `set_log_start(upto)` skips `check_fully_elided`, and
  so does `remove_leading_segments`. They still require sealed segments, `upto` to be the
  first LSN of a later segment, and `remove_leading_segments(upto) ≤ log_start`. `Wal::open`
  removes leftovers below `log_start` whatever their frames. A leftover that does not end at
  or before `log_start` is still `CorruptSegment`.

The open mode comes from `FORMAT`'s `kind`, so a keyed directory can never be opened in
`Retention` mode and lose live data to a mis-set option.

A run, under the stream's retention mutex:

1. Compute the eligible prefix `[s_0 .. s_k)` from the sealed segments and the in-memory
   max times. The next segment's first LSN is `upto`.
2. `wal.set_log_start(upto)`: `LOG_STATE` written atomically. **From here the floor is
   durable.** A crash leaves leftovers that the next open removes.
3. `wal.remove_leading_segments(upto)`: `remove` + `sync_dir` per segment, oldest first.
4. Drop the removed segments' sparse-index entries.

Crash points: before 2, nothing changed. Between 2 and 3, or in the middle of 3, `open`
finishes the removal. After 3, done. In every case the floor never goes backwards and no
offset at or above it changes.

**Windows.** `remove` of a file another handle holds fails unless it was opened with
`FILE_SHARE_DELETE`. Readers clone handles (6.1), so on Windows a removal may fail
transiently. That must not poison the log. It is an error from the run, retried at the next
interval, and the floor stays where step 2 put it. §13 Q14.

---

## 9. Durability, backpressure, limits

- **Durability:** `WalConfig.sync_mode`, with the keyed path's exact contract (spec §6.2).
  In `Durable`, `append` resolves after the group-commit batch holding the frame is
  `fdatasync`ed. In `Fast`, it resolves after the write reaches the OS, and a sync follows
  within `sync_interval_ms`. An ack covers all `n` records of the append, because they are
  one frame. `StreamLog::sync()` = `Wal::sync`, returning `durable_end()`. A failed fsync
  poisons the stream until reopen (D12). Docs say this in the same words as for the keyed
  path.
- **Producer backpressure:** `Wal` admission bounded by `max_queued_bytes`. `append`
  awaits permits. `append_timeout(records, d)` returns `WriteNotConfirmed` on timeout
  (D12's meaning: maybe written, not confirmed; a retry can duplicate). There is no
  unbounded producer queue anywhere.
- **Reader bounds:** every read is bounded by `ReadLimits`. `wait_for` is a watch, not a
  poll loop.
- **Limits:** at most 65,536 records and `MAX_PAYLOAD_LEN` (64 MiB) of encoded payload per
  append. Above that, `append` refuses with `Validation` / `RecordTooLarge` before
  admission, and callers split. Zero records is `Validation`.
- **Disk full:** `ENOSPC` on `write_at` poisons the log like any I/O error. Retention does
  not free space for a poisoned log until it is reopened. Documented as an operational
  rule: size `max_bytes` below the volume.

---

## 10. API surface (`prkdb::stream_log`; codec in `prkdb_core::wal::records`)

The module is named `stream_log` because `prkdb::streaming` (the `futures::Stream` consumer
adapter) and `prkdb::retention` (outbox TTL) already exist.

```rust
// prkdb-core
pub mod records {
    pub struct Record { pub key: Option<Vec<u8>>, pub value: Vec<u8>, pub headers: Vec<(String, Vec<u8>)> }
    pub struct RecordBatch { pub append_time_ms: i64, pub records: Vec<Record> }
    impl RecordBatch {
        pub fn encode(&self, c: &CompressionConfig) -> Result<Vec<u8>, WalError>;
        pub fn decode(bytes: &[u8]) -> Result<RecordBatch, WalError>;
        /// Header only: (append_time_ms, count), no decompression.
        pub fn peek_header(bytes: &[u8]) -> Result<(i64, u32), WalError>;
    }
}

// prkdb
pub mod stream_log {
    pub use prkdb_core::wal::records::Record;
    pub use prkdb_types::event::EventSeq;

    pub struct StoredRecord { pub offset: EventSeq, pub append_time_ms: i64,
                              pub key: Option<Vec<u8>>, pub value: Vec<u8>,
                              pub headers: Vec<(String, Vec<u8>)> }
    pub enum StartAt { Offset(EventSeq), Earliest, Latest, Timestamp(i64) }
    pub struct ReadLimits { pub max_records: usize, pub max_bytes: usize }
    pub struct ReadBatch { pub records: Vec<StoredRecord>, pub next: EventSeq, pub high_watermark: EventSeq }
    pub struct AppendAck { pub lsn: u64, pub count: u32 }
    impl AppendAck { pub fn first(&self) -> EventSeq; pub fn last(&self) -> EventSeq; pub fn offset(&self, i: u32) -> EventSeq; }
    pub struct RetentionPolicy { /* 8.1 */ }
    pub struct StreamConfig { /* 8.1 */ }
    pub struct RetentionReport { pub segments_removed: usize, pub bytes_removed: u64,
                                 pub earliest_before: EventSeq, pub earliest_after: EventSeq, pub rolled: bool }
    pub trait Clock: Send + Sync { fn now_ms(&self) -> i64; }

    pub struct StreamLog { /* Wal, sparse index, retention task handle */ }
    impl StreamLog {
        pub async fn open(cfg: StreamConfig) -> Result<StreamLog, StorageError>;
        pub async fn open_with_vfs(vfs: Arc<dyn Vfs>, cfg: StreamConfig) -> Result<StreamLog, StorageError>;
        pub async fn append(&self, records: Vec<Record>) -> Result<AppendAck, StorageError>;
        pub async fn append_timeout(&self, records: Vec<Record>, d: Duration) -> Result<AppendAck, StorageError>;
        pub fn read_from(&self, from: StartAt, limits: ReadLimits) -> Result<ReadBatch, StorageError>;
        pub fn read_durable_from(&self, from: StartAt, limits: ReadLimits) -> Result<ReadBatch, StorageError>;
        pub async fn wait_for(&self, after: EventSeq, timeout: Duration) -> Result<bool, StorageError>;
        pub fn earliest(&self) -> EventSeq;
        pub fn next_offset(&self) -> EventSeq;
        pub fn durable_end(&self) -> EventSeq;
        pub fn offset_for_timestamp(&self, ts_ms: i64) -> Result<Option<EventSeq>, StorageError>;
        pub async fn sync(&self) -> Result<EventSeq, StorageError>;
        pub fn apply_retention(&self) -> Result<RetentionReport, StorageError>;
        pub fn health(&self) -> WalHealth;
        pub fn log_bytes(&self) -> Result<u64, StorageError>;
        pub fn close(self) -> Result<(), StorageError>;
    }

    pub enum Route<'a> { Partition(u32), RoundRobin, Key(&'a [u8]) }
    pub struct PartitionedStream { /* Vec<StreamLog>, STREAM manifest, round-robin counter */ }
    impl PartitionedStream {
        pub async fn open(root: &Path, partitions: u32, cfg: StreamConfig) -> Result<Self, StorageError>; // refuses a different N
        pub async fn append(&self, route: Route<'_>, records: Vec<Record>) -> Result<(u32, AppendAck), StorageError>;
        pub fn partition(&self, p: u32) -> Option<&StreamLog>;
        pub fn partitions(&self) -> u32;
        pub async fn default_offset_store(&self) -> Result<Arc<dyn OffsetStore>, StorageError>;
        pub fn close(self) -> Result<(), StorageError>;
    }

    pub enum OnOutOfRange { Error, ResetToEarliest }
    pub enum ReadBound { Acked, Durable }
    pub struct StreamConsumerConfig { pub group_id: String, pub auto_offset_reset: AutoOffsetReset,
        pub on_out_of_range: OnOutOfRange, pub read_bound: ReadBound,
        pub max_poll_records: usize, pub max_poll_bytes: usize, pub auto_commit_interval: Option<Duration> }
    pub struct StreamConsumer { /* ... */ }
    impl StreamConsumer {
        pub async fn new(stream: Arc<PartitionedStream>, name: &str, partitions: Vec<u32>,
                         offsets: Arc<dyn OffsetStore>, cfg: StreamConsumerConfig) -> Result<Self, StorageError>;
        pub async fn poll(&mut self, timeout: Duration) -> Result<Vec<(u32, StoredRecord)>, StorageError>;
        pub async fn commit(&mut self) -> Result<(), StorageError>;
        pub fn seek(&mut self, partition: u32, to: StartAt) -> Result<(), StorageError>;
        pub fn position(&self, partition: u32) -> Option<EventSeq>;
        pub async fn committed(&self, partition: u32) -> Result<Option<EventSeq>, StorageError>;
    }
}
```

`StorageError` additions: `OffsetDiverged { committed: u64, reason: String }`.
`CompactedCursor`'s text is generalised (the variant is unchanged) to
`offset {cursor} is below the log's floor {floor}: the records before it were removed (by
retention or compaction); resume at or above {floor}`. The keyed-path advice ("resynchronise
from a snapshot") moves into `get_changes_since`'s docs. §13 Q3.

---

## 11. Performance: targets and how they are measured

### 11.1 Cells (Linux `probe=wal-bench`, `crates/prkdb/benches/wal_write_path.rs`)

| Cell | What | Why |
|---|---|---|
| `stream_append/{durable,fast}/{1,64}w/{1,64}k/b1` | `StreamLog::append` of 1 record per call | Same shape as `wal_{durable,fast}`, so the record codec's overhead is isolated. |
| `stream_append/{durable,fast}/{1,64}w/{1,64}k/b100` | 100 records per call | The streaming case: client-side batching. Reported as records/s and MB/s. |
| `stream_read/tail/1k` and `stream_read/cold/64k` | One reader, `read_from` with 1 MiB limits, from the page cache (tail) and after `posix_fadvise(DONTNEED)` (cold) | Read path, including the seek. |
| `stream_append_retention/durable/64w/1k/b1` | `stream_append` on 1 MiB segments with `max_bytes` = 8 MiB and retention looping | Append p99 while segments are being removed. |
| `device: pread 1 MiB` (new ceiling row) | Sequential read ceiling | Denominator for the read targets. |

Each row prints the existing columns (ops/s, MB/s, p50/p99/p99.9, batch and writer
columns) plus records/s.

### 11.2 Targets (relative, checked in the decision record; not promises in docs)

- **T1** `stream_append …/b1` ops/s ≥ 0.95 × `wal_{mode}` in the same cell. The stream adds
  one encode and one hook.
- **T2** `stream_append/durable/1w/1k/b100` records/s ≥ 50 × `…/b1` records/s. One fsync
  amortised over 100 records. Real Linux `Durable` 1w/1k is ~3–5k ops/s (decision record
  §9), so this asks for ≥ 150k records/s.
- **T3** `stream_read/tail` MB/s ≥ 0.5 × the `pread` ceiling. `stream_read/cold` ≥ 0.3 ×.
- **T4** `stream_append_retention` p99 ≤ 1.2 × the matching `stream_append` p99.
- **T5** Open time for a 1 GiB stream ≤ the keyed `recovery_bench` open of a 1 GiB log. Both
  are scan-bound, and the stream skips index replay.

A missed target is recorded with the cause and goes back to the maintainer, the same as the
2.1 spike's 15 % rule. It is not hidden or tuned away in the bench.

### 11.3 Deterministic gate

`iai_hot_paths.rs` gains `bench_stream_append_100` (100 records of 1 KiB, one append,
`Fast`, so instruction counts are not dominated by sync waits) and
`bench_stream_read_100`, using the same `whole_process()` setup as `bench_wal_put_100`.
Their first recorded counts become the floor, and the > 5 % rule applies (spec §6.2).

### 11.4 Kafka: we will not claim a comparison

A fair comparison needs the same machine and disk, Kafka with replication factor 1, a matched
ack/fsync contract (`flush.messages=1` against `Durable`; Kafka's default, which never
fsyncs, against `Fast` with its loss window stated), the same record sizes and batching,
and, the part that cannot be matched, the same transport. Kafka is measured over a socket
by `kafka-producer-perf-test`. `StreamLog` is embedded in-process. Any number from that
comparison measures the network stack and serialisation as much as the log. So 2.15b
publishes **measured PrkDB numbers only** (both modes, the cells above, the machine named),
and docs make no "faster than Kafka" or "Kafka-class" statement. If a comparison is wanted
later, it belongs with a networked stream API (Phase 4+), run through Kafka's own perf tool
against a PrkDB server on the same box. §13 Q15.

---

## 12. Test plan

### 12.1 Unit and integration (failing first, per task)

- **Codec:** round trip, every bit flip and truncation refused, reserved flags, count 0 and
  65,537, a `raw_len` bomb, invalid UTF-8 header names.
- **Frame kind:** a `Records` frame in a keyed directory is refused at replay, and a `Batch`
  frame in a stream directory is refused at open. An unknown kind with a valid CRC in the
  last segment refuses and does not truncate (5.3).
- **FORMAT kind:** a stream opened on a kv directory, and the reverse, refuse with nothing
  written. A missing `kind` reads as `kv`.
- **Offsets:** a 3-record append returns `L<<16|0..2`. Offsets continue across reopen.
  `read_from(last + 1)` is the next frame. `0xFFFF + 1` carries. `o < earliest()` is
  `CompactedCursor` naming the floor. `o > next_offset()` is an error. `Earliest`, `Latest`
  and `Timestamp` resolve correctly.
- **Read bounds:** in `Fast`, an acked-but-unsynced record is visible to `read_from` and
  not to `read_durable_from`.
- **Seek cost:** a read near the tail of a 64-segment log visits only the last segment.
  Assert with a counting `Vfs`.
- **Retention:** age (with an injected `Clock`), size, both together. The active segment is
  never removed, and a quiet segment is rolled. The floor survives reopen. With
  `ElidedOnly`, a keyed `set_log_start` still refuses live segments.
- **Crash points in retention** (FaultFs, as Task 2.15's `CompactionStep` hooks): after
  `LOG_STATE`, after each `remove`, and before `sync_dir`. Each reopen gives the same
  floor, no lost record at or above it, and no leftover segment.
- **Partitions:** creation cut before `STREAM` (all empty: finished; data present:
  refused). A different N is refused. Key routing matches the 2.13 golden vectors.
- **Consumers:** commit/resume; `auto_offset_reset` with no commit; `on_out_of_range`
  after retention; `OffsetDiverged` after a `Fast` power loss that drops consumed frames and
  then refills their LSNs; a group of two consumers gets disjoint partitions through
  `ConsumerGroupCoordinator`.
- **`LOCK`:** a second open of a stream directory is `Locked`.

### 12.2 Harness (`prkdb-verify`)

A stream SUT next to the kv one, sharing the runner, FaultFs, PowerLoss and the seed and
minimisation machinery.

- **Model:** per partition, a list of acknowledged appends `(lsn, records)`, a floor, and a
  clock.
- **Ops:** `StreamAppend { partition, n: 1..=200, size: 1 KiB | 64 KiB | mixed, keys, headers }`,
  `StreamRead { partition, from: Earliest | Offset(model-chosen, incl. below floor and past end) }`,
  `Retain`, `AdvanceClock(ms)`, `Roll`, plus the existing `Reopen`, `Crash` and
  `PowerLoss { tear, fault_seed }`. Small segments (4–64 KiB) so rolls and retention happen
  every few ops.
- **Checker:**
  - `Durable`: every acked record at or above the floor is present with the same offset and
    bytes. Nothing unacked is visible past the next write.
  - `Fast`: per partition, the survivors equal the acked appends up to some prefix no
    earlier than the last completed sync (Task 2.10a's acceptable-prefix rule, per
    partition).
  - Always: the floor never decreases, `read_from` below it errors, offsets are strictly
    increasing, and no offset maps to two different records except LSNs reissued after a
    `Fast` loss, where the checker expects `OffsetDiverged` from a consumer that consumed
    them.
- **Profiles:** the stream ops join the discovery profile first. They move to the blocking
  profile (both modes) when 2.15b.7 is green at 1,000 seeds per mode, which satisfies the
  exit criterion "PowerLoss in both modes, including across segment rolls and retention".

### 12.3 Fuzz (Task 2.23's `fuzz/`)

- New `records_decode` target (`RecordBatch::decode`, with a corpus seeded from the golden
  stream fixture).
- `frame_decode` and `segment_scan` corpora gain kind-3 frames.
- `format_parse` gains `kind` lines.
- New `stream_manifest_parse` target for `STREAM`.
- Nightly, as the others.

---

## 13. Open questions for the maintainer

1. **Frame kind.** Confirm `FrameKind::Records = 3` (recommended, §4.1) over a `Batch` op
   tag.
2. **Timing against 2.24.** Land the on-disk parts (frame kind, codec, `FORMAT` kind,
   `STREAM` manifest, the unknown-kind hardening) before Task 2.24 freezes format 2
   (recommended, §5.2)? The alternative is format 3 plus a migration (§5.4).
3. **Error.** Reuse `CompactedCursor` with generalised text (recommended; the plan's note
   from Task 2.15 asks for no second error), or add a distinct
   `OffsetOutOfRange { offset, earliest, latest }`, which reads better for retention?
4. **Offset 0.** Treat every offset below `earliest()`, including 0, as out of range, with
   `StartAt::Earliest` for "from the start" (recommended)? `get_changes_since` treats 0 as
   special. Streams would not.
5. **Sparse offsets.** Accept that offsets have gaps of 2¹⁶ between appends, and that lag is
   reported in frames and bytes (`next_offset().lsn - position.lsn`, `log_bytes`), not in
   records?
6. **Headers in record format v1.** Include them now (recommended, because the format
   freezes), or keep records key/value only and accept a format bump later?
7. **Timestamps.** Append time stamped at encode, one per append (recommended), or a
   producer-supplied create time per record? Is seek-by-time needed in v1?
8. **`Fast` offset reuse.** Adopt the frame-CRC position check (§7.3)? If so, extend what
   `StorageOffsetStore` stores (a versioned struct under the same key) or add a sibling key?
   Changing the stored bytes must land before 2.24.
9. **Offset store location.** A kv data directory `<root>/__offsets/` by default
   (recommended), or always caller-provided?
10. **Retention vs consumers.** Ignore consumers like Kafka and Iggy (recommended), or offer
    an option to hold retention for the slowest committed group (Pulsar-style)?
11. **Repartitioning.** Partition count fixed at creation and refused on mismatch
    (recommended for 2.15b)?
12. **`PrkDb` integration.** Defer `db.stream(..)` and a container layout for the builder,
    or do it in 2.15b?
13. **Read path.** Synchronous bounded `read_from` (recommended, like `Wal::read`), or
    async with `spawn_blocking` inside?
14. **Windows.** Is retention on Windows a release requirement now (FILE_SHARE_DELETE in
    `StdVfs`), or documented as best-effort until a Windows CI job exists?
15. **Kafka.** Confirm no Kafka comparison claim in 2.15b (§11.4).
16. **Defaults.** Streams default to 128 MiB segments and no retention (keep everything)?
    Or 7 days, like Kafka and Redpanda?

---

## 14. Implementation breakdown

Each task is TDD, a standalone commit series on a feature branch, with the harness green in
both modes before merge. 2.15b.1–.3 precede Task 2.24.

| Task | Content | Depends on |
|---|---|---|
| **2.15b.1** `Wal` prerequisites | Start-segment lookup + early stop in `scan_from_capped` (keyed `get_changes_since` benefits); `scan_segment_from` at a byte offset; `subscribe_acked`; `Request::Roll`; `WalOptions::front_release` (`set_log_start`/`remove_leading_segments`/open leftover rule); CRC-before-kind with unknown kind = `UnsupportedFormat` (5.3); fix the `Lsn` doc. FaultFs crash tests for `Retention` release. | — |
| **2.15b.2** Record codec + `EventSeq` | `FrameKind::Records`, `wal/records.rs`, `peek_header`; `prkdb_types::event::EventSeq` per Task 2.20's spec if 2.20 has not landed; `records_decode` fuzz target; corpus updates. | — |
| **2.15b.3** `StreamLog` core | `FORMAT` `kind` (+ refusals both ways), `LOCK`, open/append/read_from/read_durable_from/wait_for/sync/close, sparse index, offsets and `StartAt`, generalised `CompactedCursor` text. | .1, .2, 2.11, 2.11b |
| **2.15b.4** Retention | `RetentionPolicy`, `Clock`, eligibility, quiet-segment roll, background task with stop-on-drop (as compaction's), crash-point tests, `RetentionReport`. | .3 |
| **2.15b.5** Partitions | `STREAM` manifest (+ fuzz target), `PartitionedStream`, routing through the 2.13 partitioner, creation crash rules. | .3, 2.13 |
| **2.15b.6** Consumers | `StreamConsumer` on `OffsetStore`/`ConsumerGroupCoordinator`, `__offsets` default store, out-of-range policy, `OffsetDiverged` check (Q8). | .5 |
| **2.15b.7** Harness | Stream SUT/model/ops/checker, discovery then blocking profile, 1,000 seeds per mode, ledger entries. | .4, .5, .6 |
| **2.15b.8** Performance + docs | `wal_write_path` cells (§11.1), `pread` ceiling row, iai benches + floor, Linux probe run, results and T1–T5 verdicts in a decision record, user docs with measured numbers only. | .4, .5 |
| **2.15b.9** Golden fixture | `out/stream/` + `expected-stream.json` in Task 2.24's generator (folded into 2.24 if 2.24 has not run yet). | .3, .4, .5 |
