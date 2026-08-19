# Mutation Survivors and h2 Advisory Design

## Goal

Make the repeated nightly CI failures actionable and green by adding regression coverage for
the three surviving WAL mutations and upgrading the vulnerable `h2` lockfile entry, without
changing unrelated production behavior.

## Evidence and scope

Runs `32216234002`, `32099522120`, `31995210376`, `31926883919`, and `31864476429` all test
commit `bed798b`. They repeat the same failures rather than representing five separate
regressions:

- Shard 10 misses deletion of the `wal` field from the `StorageConfig` expression in
  `WalStorageAdapter::new_with_replication`.
- Shard 14 misses deletion of the `LogOperation::Put` collection-routing arm in
  `WalStorageAdapter::publish_batch`.
- Shard 14 misses deletion of the `LogOperation::Delete` collection-routing arm in
  `WalStorageAdapter::publish_batch`.
- The newest run also fails `cargo deny` because `Cargo.lock` contains `h2 0.4.13`, affected
  by `RUSTSEC-2026-0258`; the advisory identifies `0.4.16` as the first patched release.

The current implementation behaves correctly. The mutation failures show that the test suite
does not prove those behaviors. The implementation should remain unchanged unless writing the
tests exposes an actual defect.

## Test design

### Replication constructor configuration

Add a unit test beside the existing WAL adapter tests. Construct a `ReplicationManager` with
no follower addresses, pass a unique temporary `WalConfig` to
`WalStorageAdapter::new_with_replication`, and prove that the adapter creates and uses that
directory. The test must fail when the exact `wal: config` field initializer is deleted, because
the mutant falls back to the default `prkdb_data` path.

The test will use a temporary directory owned by the test and will not depend on networking.

### Collection routing during queued publication

Add focused unit coverage that stops the background writer, queues single-record `Put` and
`Delete` operations with non-empty collection names, flushes them through `publish_batch`, and
inspects the resulting WAL operations. The assertions must prove that both operation types retain
their original collection names.

Non-empty and distinct names are essential: existing mixed-batch coverage uses the empty default
collection, so routing a mutated operation into `String::new()` is observationally identical.
The regression coverage should exercise both arms independently enough that deleting either one
causes a clear failure.

### Mutation verification

For each survivor, temporarily apply the exact reported mutation and run the narrow regression
test. A valid regression test must fail for the expected behavioral reason. Restore the correct
source before proceeding, then run the same test successfully. Temporary mutant edits must never
be committed.

## Dependency remediation

Update only the `h2` package selected by the existing dependency graph, targeting a patched
version accepted by current constraints. Do not add an advisory ignore. `Cargo.lock` is the
expected changed artifact; manifests should change only if the existing constraints cannot select
a fixed release.

Verify with `cargo deny check` and `cargo audit`. Existing advisory exceptions remain out of scope
unless the dependency update makes one provably stale, in which case remove only that stale entry.

## Verification

Run, in increasing scope:

1. Each new WAL regression test.
2. The relevant `prkdb` library test target.
3. `cargo deny check` and `cargo audit`.
4. `cargo fmt --check` and workspace Clippy with warnings denied.
5. The workspace test suite.
6. Targeted `cargo mutants` checks for the three reported mutation sites when practical; the
   explicit apply-mutation RED checks are the deterministic acceptance criterion.

Success means all three reported mutants are killed, the vulnerable `h2 0.4.13` entry is absent,
the security scanners pass, and no existing test or lint regresses.

## Non-goals

- Refactoring WAL constructors or collection routing when tests alone close the gaps.
- Broad dependency upgrades.
- Changing mutation shard layout, timeouts, or workflow behavior.
- Modifying the pre-existing untracked watchdog plan.
