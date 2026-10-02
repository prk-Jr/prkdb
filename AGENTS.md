# Working on prkdb as a coding agent

This file is for any coding agent (Codex, Claude Code, or another) working in this
repository. Several agents work on the remediation programme in parallel and hand
work to each other when one hits a usage limit. The rules below keep their work from
colliding and let any agent pick up where another stopped.

## 1. What the work is

prkdb is an embedded Rust database being rebuilt so its durability and consistency
guarantees are true and machine-checked. The work is a phased remediation programme.
Three documents define it, and when they disagree the earlier one in this list wins:

1. **Spec:** `docs/superpowers/specs/2026-09-23-root-cause-remediation-design.md`.
   Decisions D1–D13, the finding tables (§3), the branch workflow (§5), the perf gate
   (§6) and the harness (§7).
2. **Plan:** `docs/superpowers/plans/2026-09-23-root-cause-remediation.md`. One section
   per task (`### Task 2.15b.3: …`) with files, test-first steps, commands and the
   commit message. Read its "Conventions" section before your first task.
3. **Ledger:** `docs/remediation/ledger.toml`. Every finding (`STO-12`, `KEY-06`, …)
   with its status, evidence, regression tests and fixing commits.
   `docs/status/remediation.md` is rendered from it; never edit that file by hand.

Design notes for individual areas live in `docs/superpowers/specs/` (for example
`2026-10-02-streaming-log-design.md` for Task 2.15b). Decision records live in
`docs/remediation/decisions/`.

The current phase is **Phase 2** on branch **`remediation/phase-2`**.

## 2. Hard rules

These have no exceptions unless the maintainer (Prakash) says so for a specific case.

- **Never push, open a PR, or touch `main`.** Pushes happen only at phase gates and are
  done by the maintainer or an agent he has explicitly told to push. Security findings
  must not become public before their fix.
- **Never rewrite shared history:** no force-push, no rebase or amend of a commit that
  is on `remediation/phase-2` or that another agent may have built on.
- **Commits:** conventional commits (`feat:`, `fix:`, `test:`, `docs:`, `chore:`,
  `ci:`, `perf:`, `refactor:`). **No `Co-Authored-By` or any other attribution
  trailer.** Never `--no-verify`.
- **Fix the root cause.** No workarounds, retries, sleeps or wider timeouts to make a
  test pass. If the real fix is out of your task's scope, stop and report it.
- **Never weaken a check to get green.** Do not add `#[ignore]` (without a reason that
  `scripts/check_ignore_reasons.sh` accepts), lower a perf floor, raise a threshold,
  shrink a seed count, or delete a failing test.
- **No performance regressions.** The maintainer's rule is "we can't compromise on
  performance". Hot paths are `crates/prkdb-core/src/wal/`, `crates/prkdb/src/storage/`,
  `indexed_storage.rs` and `transaction.rs`. When you touch them, run the relevant
  Criterion bench before and after and put the delta in the commit body (plan,
  "Perf note").
- **If a tool is missing, ask the maintainer to install it.** Do not substitute a
  weaker check and call the step done. Name the tool and the install command.
- **STOP steps are real.** When a plan step says STOP, or the work shows the plan is
  wrong, stop and report. Do not improvise a different design.
- **Stay in your task's scope.** If you must touch a file outside the task's
  **Files:** line, say so in the report.

## 3. Machine constraints

- Disk is tight. Every worktree builds its own `target/` (7–20 GB).
  - Build with `CARGO_INCREMENTAL=0`.
  - Never build in release mode unless a step requires it.
  - When your task is finished and reported, run `cargo clean` in your worktree.
- The toolchain is pinned to 1.98.1 by `rust-toolchain.toml`. Fuzzing uses nightly
  (`cargo +nightly fuzz …`).
- The instruction-count perf gate (iai/gungraun) needs Linux and Valgrind. It cannot
  run on this Mac, so never report instruction counts you did not measure. Linux probes
  run through the `remediation-gate` workflow, which needs a push: ask the maintainer.

## 4. Claiming a task

Before starting, check what is already taken:

```bash
git worktree list                     # every active worktree and its branch
git branch --list 'wip-*'             # task branches, merged or not
ls "$(git rev-parse --path-format=absolute --git-common-dir)/../.agents/handoff/"
```

A task is **taken** if a `wip-<task>` branch exists that is not yet merged into
`remediation/phase-2` (`git branch --no-merged remediation/phase-2 --list 'wip-*'`) or
a handoff note for it says `status: in-progress`. Take a different task, or continue
that one only if its note says `status: paused` (see §6).

Also check the task's ordering in the plan. Some tasks must land before others (for
example, every task marked *before Task 2.24* lands before the format freeze; 2.18 and
2.19 come after 2.17). Do not start a task whose prerequisites are not merged into
`remediation/phase-2`.

To claim one, create a worktree outside the main checkout and write a handoff note
(§6) with `status: in-progress` straight away:

```bash
cd /Users/prk-jr/Desktop/opensource/prkdb/output/prkdb-clean
git worktree add -b wip-<task> ../prkdb-worktrees/wip-<task> remediation/phase-2
cd ../prkdb-worktrees/wip-<task>
```

Use the plan's task number in the branch name: `wip-2.15b.3`, `wip-2.24b`. For work
that is not a plan task, use a short slug: `wip-ci-gates`.

## 5. Doing the task

Follow the task's steps in order. The usual shape is:

1. **Read** the task section, the spec sections and design note it cites, and the code
   it names. Note the exact test names and commit messages the plan gives.
2. **Test first.** Write the failing tests and run them to see them fail for the
   expected reason. For a finding: the regression test must fail before the fix.
3. **Fix** at the root, in the smallest change that makes the tests pass.
4. **Ledger.** For a fixed finding, set `status = "fixed"` and fill `regression_tests`
   (`"test:<path>::<test_name>"`) and `changes` (the fixing commit SHAs). For a new
   finding, add a `[[finding]]` entry, a row in the spec's §3 table, and a line in the
   spec's revision table. See "Numbering" below.
5. **Verify.** All of these must pass before you report done:

   ```bash
   export CARGO_INCREMENTAL=0
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets -- -D warnings
   cargo nextest run --workspace          # or the crates/tests the task names, plus
                                          # the workspace once before reporting
   cargo test --workspace --doc
   cargo xtask remediation check
   cargo xtask remediation render --check # if it fails: cargo xtask remediation render
   ```

   Run these too when your change could affect them:

   ```bash
   cargo xtask verify --profile blocking --seeds 200 --mode durable   # storage/WAL
   cargo xtask verify --profile blocking --seeds 200 --mode fast      # changes
   bash scripts/check_single_wal.sh
   bash scripts/check_perf_gate_floors.sh                             # benches
   cargo +nightly fuzz run <target> -- -max_total_time=60             # decoders
   ```

   `scripts/pre-push-check.sh` runs the full set; run it before reporting a task that
   touches storage, the WAL or CI.
6. **Commit** with the plan's message. Commit small and often. A half-done task should
   always be in commits plus a handoff note, never only in the working tree.
7. **Report** (§7). Do not merge into `remediation/phase-2` yourself unless the
   maintainer asks you to integrate. The integrator reviews and merges.

### Numbering

Several branches add findings and spec revisions at the same time, so numbers
collide.

- **New finding IDs:** take the next free number in the area across *all* branches,
  not only yours:

  ```bash
  for b in $(git branch --format='%(refname:short)'); do
    git show "$b:docs/remediation/ledger.toml" 2>/dev/null
  done | grep -o 'id = "[A-Z]*-[0-9]*"' | sort -u
  ```

- **Spec revision numbers:** use the next number on your branch. The integrator
  renumbers on merge. Mention the number you used in your report.

## 6. Handoff notes

Any agent can be cut off by a usage limit at any moment, so the state of every task
lives on disk, not in an agent's context.

Notes live in the main checkout's `.agents/handoff/` directory, which git ignores. One
file per branch: `.agents/handoff/wip-<task>.md`. From a worktree, that directory is:

```bash
"$(git rev-parse --path-format=absolute --git-common-dir)/../.agents/handoff/"
```

Update the note **every time you commit**, and whenever you stop. Format:

```markdown
# wip-2.15b.3: StreamLog core
status: in-progress | paused | done | blocked
agent: codex | claude
worktree: /Users/prk-jr/Desktop/opensource/prkdb/output/prkdb-worktrees/wip-2.15b.3
base: <SHA of remediation/phase-2 the branch was cut from>
updated: 2026-10-02 18:40 IST

## Done
- <commit SHA> <what it does>

## In progress
<what is half-written, which files, uncommitted or not>

## Next
1. <the very next concrete step>
2. ...

## Findings and decisions
<anything learned that is not in the plan: a finding ID you added, a spec revision
number you used, a deviation from the plan and why, a STOP and its reason>

## Verification so far
<which commands passed, with counts; which were not run and why>
```

Set `status: paused` when you stop before the task is finished (for example, you are
about to run out of budget): commit what you have first, even as a `wip:` commit on
your own branch, and write the exact next step. Set `status: done` when you report.
Set `status: blocked` when you need the maintainer.

### Continuing someone else's task

1. Read the handoff note.
2. `cd` into its worktree. Run `git status`, and `git log --oneline <base>..HEAD`.
3. Check the note against reality: uncommitted changes, the last commit, failing tests.
   Run the task's tests before changing anything, so you know the starting state.
4. Set the note to `status: in-progress` and `agent:` to yourself, then continue
   from "Next". Do not redo committed work, and do not discard uncommitted work you
   did not write. Commit it as `wip:` first if it builds, or describe it in the note if
   it does not.

## 7. The report

End every task, or every stop, with a report to the maintainer containing:

- the branch, worktree and base SHA, and the commits (SHA and one line each);
- what changed and why, at the level of behaviour, not a file list;
- verification: each command run and its result (counts), and what was not run and
  why;
- perf: Criterion before/after for hot-path changes;
- ledger: findings added or updated, spec revision numbers used;
- deviations from the plan, files touched outside scope, and anything left for a later
  task;
- anything the maintainer must decide or install.

Copy the report into the handoff note's "Done" and "Findings and decisions" sections
as well, so the integrator can read it there if your session is gone.
