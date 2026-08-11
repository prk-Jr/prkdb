# WAL writer liveness: discharging the write obligation

**Status:** proposed
**Found by:** mutation run 31505589348, `mutants-diff` on PR #53
**Blocks:** the standing `TIMEOUT` exemption in `.cargo/mutants.toml`

## The defect

Every queued write is a promise to the caller. Nothing in the system is responsible for
keeping it.

`WalStorageAdapter::put_many`, `append_raft_entry`, and `append_raft_entries_batch` each
build a `PendingWrite { record, tx }`, push it into `inner.accumulator`, notify the flush
loop, and then `rx.await` — with no deadline. The sender half lives inside the accumulator
until `flush_accumulator_inner` calls `acc.flush()` and takes the batch out.

If the writer stops taking batches out, the senders sit there, alive and unfired, forever.
Callers block forever. No error, no metric, no timeout, no way to tell from the outside.

The existing error path is dead code:

```rust
rx.await
    .map_err(|_| StorageError::Internal("oneshot canceled".into()))?
```

`RecvError` fires only when the sender is *dropped*. No path guarantees a queued
`PendingWrite` is ever dropped or fired. The handler is written, correct, and unreachable.

`run_writer_loop` is spawned fire-and-forget and re-acquires its state through
`weak_inner.upgrade()` each iteration. No `JoinHandle` is retained. If that task panics,
returns early, or is dropped, nothing observes it.

### How it was found

cargo-mutants replaced `flush_accumulator_inner` with `()`. Every write in the workspace
test suite hung; the job hit its 300s per-mutant budget and reported:

```
TIMEOUT crates/prkdb/src/storage/wal_adapter.rs:1119:9:
  replace WalStorageAdapter::flush_accumulator_inner with () in 72s build + 300s test
```

That reads as a mutation-testing nuisance. It is not. The mutant is a faithful simulation
of a flush loop that is alive but no longer publishing — which is what a swallowed error
inside the loop body would produce in production. The tests did detect it. They detected it
by hanging, which is precisely the symptom a user would get.

### What this is not

It is not a missing test, and no test can fix it. The property being violated is
**liveness** — "the write is eventually published" — and there is no non-temporal
observation of *never*. A test can only ever observe "not yet". Any correct detection is
time-based; the design question is only where the bound lives and what it means.

Two fixes were considered and rejected before this spec:

- **A blanket timeout on `rx.await`.** Changes the durability contract to make a CI signal
  pass. A timed-out write may still be published afterward, so the caller cannot know what
  happened — and see below, `Failed` is the wrong word for that.
- **A `Drop` guard on `PendingWrite`** that sends `Err` when dropped unsent. This is a
  genuine improvement and is included in Part 1 below, but it does not address this defect:
  the write is not dropped, it is *abandoned in place* inside a live accumulator.

## The fix

Four parts. They cover different failure modes and are not substitutes for one another.

### Part 1 — Supervise the writer

Retain the writer's `JoinHandle`. On exit — clean return, panic, or cancellation —
transition the adapter to a terminal failed state and drain every pending waiter with an
error naming the cause.

Add the `Drop` guard on `PendingWrite` here: a write that is dropped without a result sends
`Err` rather than silently closing the channel. Together these make the existing
`oneshot canceled` path reachable for the first time.

Covers: panic in the loop body, early return, task cancellation, runtime shutdown.
Does not cover: a task that is alive and looping but publishing nothing.

### Part 2 — Account for progress

This is the part that catches the defect above, and Part 1 cannot substitute for it: under
this failure the task is alive and its `JoinHandle` never resolves.

The accumulator maintains:

- a monotonic count of writes enqueued and writes published,
- the enqueue timestamp of the oldest unpublished write.

A watchdog fires when the oldest unpublished write has been queued longer than a small
multiple of `flush_interval` while the published count has not advanced. On firing: mark
the adapter unhealthy, discharge all pending waiters with a distinct error, and surface the
condition on the health endpoint.

The threshold derives from `flush_interval`, not from a constant. A magic number here would
be the same hack in a different place.

### Part 3 — Bound the client wait, honestly

Even with Parts 1 and 2, a client call takes a deadline, because the caller's SLA is not
the database's business.

The error on expiry must mean **not confirmed**, never **failed**. The write may still be
published afterward. An API that reports `Failed` for a write that later commits will cause
double-writes in every caller that retries on it — trading a hang for silent data
corruption, which is a strictly worse bug. This needs its own `StorageError` variant; it
must not reuse `Internal`.

### Part 4 — Backpressure

If the writer stalls, the accumulator must refuse new writes rather than buffer without
bound. Otherwise a stall becomes an OOM, and Parts 1–3 report the stall correctly right up
until the process dies.

### Observability

Queue depth, age of the oldest unpublished write, publishes per second, timestamp of last
successful publish. Exported through `PartitionMetrics` alongside the existing
`set_cache_size_bytes`. A stall must be visible on a dashboard before a client feels it.

## Acceptance

1. A writer task that panics discharges every pending waiter with an error naming the panic.
   No caller blocks.
2. A writer task that is alive but publishes nothing is detected within a bounded multiple
   of `flush_interval`; pending waiters receive the not-confirmed error and the health
   endpoint reports unhealthy.
3. The not-confirmed error is a distinct variant, and its docs state that the write may
   still be published. No call site maps it to a failure.
4. With the accumulator at capacity and the writer stalled, new writes are refused with
   backpressure rather than queued. Memory stays bounded.
5. `cargo mutants` on `flush_accumulator_inner` reports **caught**, not `TIMEOUT`, and does
   so within one watchdog interval rather than the full per-mutant budget.

Acceptance 5 is the exit condition for the `mutants.toml` exemption this spec blocks. When
it holds, that entry is deleted.

## Sequencing

Parts 1 and 2 are the permanent fix; 3 and 4 harden the surface around them. Part 3 depends
on Part 2 for its error semantics — without progress accounting, the bound has nothing
principled to derive from and degenerates into the blanket timeout already rejected.

This work does not belong in PR #53, which fixes `scan_prefix` skipping compressed records
and is unrelated. #53 carries a tracked exemption naming this spec.
