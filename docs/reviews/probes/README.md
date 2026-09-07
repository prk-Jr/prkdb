# Review diagnostic probes

These probes assert **observed defective behavior** at `adc101fb`. A pass confirms a finding. They are deliberately outside the ordinary test suite and must not become permanent tests requiring bugs to remain present. For fixes, convert each into a regression asserting the opposite, desired guarantee.

Run from a disposable worktree; do not overwrite existing files with the same names:

```sh
mkdir -p crates/prkdb-schema/tests
cp docs/reviews/probes/schema_review.rs crates/prkdb-schema/tests/schema_review.rs
cargo test --offline -p prkdb-schema --lib --test schema_review -- --nocapture
cp docs/reviews/probes/core_review.rs crates/prkdb/tests/core_review.rs
cargo test --offline -p prkdb --test core_review -- --nocapture
```

Remove only the two copied test files after running them. The core probe includes the production batch-accumulator module by relative path; that path is correct after copying it into `crates/prkdb/tests/`.

Observed September 7, 2026, isolated branch based on `adc101fb`:

| Check | Result | Meaning |
| --- | --- | --- |
| Existing schema library suite | 12 passed | Existing tests pass; not proof of failure-path correctness |
| Schema traversal probe | Passed | Collection name escaped configured registry directory within a disposable temp directory |
| Missing descriptor probe | Passed | Reload accepted a missing descriptor as empty |
| Indexed collection identity probe | Passed | Reading User returned Project data with the same ID |
| Batch flush barrier probe | Passed | Flush returned while executor remained blocked |
| Included existing batch unit test | Passed | Existing timing-based test does not detect flush defect |

Both commands exited 0. These checks are a small targeted subset, not a complete workspace validation. No production source was modified for the probes.
