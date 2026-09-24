# Root-Cause Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make PrkDB's durability, identity, and consistency guarantees true and machine-proven, phase by phase, per `docs/superpowers/specs/2026-09-23-root-cause-remediation-design.md`.

**Architecture:** A finding ledger checked by `xtask` is the single tracking source. A model-based crash/restart harness (`crates/prkdb-verify`) proves fixes and finds unknown bugs. A new `Vfs` seam in `prkdb-core` makes power loss testable. Fixes land as local commits on `remediation/root-cause`; each phase ends in one public PR.

**Tech Stack:** Rust 1.98.1, tokio, `toml` + `serde` (ledger), `rand` 0.8 + `rand_chacha` 0.3 (seeded ops), `cargo-nextest`, `iai-callgrind` (or successor `gungraun`), Criterion, GitHub Actions, VitePress.

**Spec sections are referenced as §N.** When this plan and the spec disagree, the spec wins; fix the plan.

---

## Status

| Phase | Detail level | Status |
|---|---|---|
| 0 Honesty and tracking | Full (code-level) | gate passed — squash-merged as bc50b8e (PR #79) |
| 1 Harness and baseline | Full (code-level) | gate passed — squash-merged as 2a7dcdb (PR #80) |
| 2 Format v2 + single-node | Full (code-level), expanded 2026-09-24 after the Task 2.1 spike (decision: PROCEED) | in progress (2.1 done) |
| 3 Semantics | Outline — expand at phase start | — |
| 4 Raft | Outline — expand after the 4a spike | — |
| 5 Docs and release | Outline — expand at phase start | — |
| 6 AI | Separate plan per Sep 7 specs | — |

---

## Conventions (apply to every task)

- **Branch:** each phase has its own branch `remediation/phase-<n>` (Phase 0 used `remediation/root-cause`), cut from the previous phase branch and synced with `main` after each phase merge. Never push to `origin` except in a phase-gate task (§5.1) or an explicitly approved STOP step.
- **Merges are squash merges and head branches are auto-deleted.** Per-commit SHAs stay reachable only via `refs/pull/<n>/head`, so every fixed finding's `changes` must also include its PR URL (added in the phase-gate evidence commit).
- **Commits:** conventional commits (`feat:`, `fix:`, `test:`, `docs:`, `chore:`, `ci:`). **No `Co-Authored-By` or other attribution trailers.**
- **Finding workflow:** tripwire exists → change tripwire into a failing regression test → fix → test passes → ledger entry updated (`status`, `regression_tests`, `changes`) → `cargo xtask remediation check` passes → commit.
- **Perf note:** for any task touching `crates/prkdb-core/src/wal/`, `crates/prkdb/src/storage/`, `indexed_storage.rs`, or `transaction.rs`, run the relevant Criterion bench before and after (`cargo bench -p prkdb --bench <name> -- --save-baseline before` / `--baseline before`) and put the delta in the commit body.
- **Test command:** Phase 0 uses `cargo test`; from Task 1.1 use `cargo nextest run`.
- **The hook `block-no-verify` rejects any shell command that contains `git commit` together with a `--no-…` flag or a `-n` anywhere in the same command text (e.g. `sed -n`, `grep -n`).** Run `git commit` as its own command; use `git commit --amend -C HEAD` to reuse a message.
- **CI preamble.** Every new workflow job that builds `prkdb` (anything beyond `xtask`) starts with exactly the `test` job's setup from `.github/workflows/ci.yml`:
  ```yaml
      - name: Free disk space
        run: |
          sudo rm -rf /usr/share/dotnet
          sudo rm -rf /usr/local/lib/android
          sudo rm -rf /opt/ghc
      - uses: actions/checkout@v4          # add `with: ref: ${{ inputs.ref }}` in remediation-gate.yml
      - name: Install Protoc
        run: sudo apt-get install -y protobuf-compiler
      - uses: dtolnay/rust-toolchain@1.98.1   # never @stable: it overrides rust-toolchain.toml
      - uses: Swatinem/rust-cache@v2
  ```
  Jobs that only run `xtask` need checkout, `dtolnay/rust-toolchain@1.98.1`, and rust-cache. **`xtask` must never depend on `prkdb` or `prkdb-verify`**: `prkdb-proto`'s build script needs `protoc`, and the existing xtask-only jobs (`repo-status-snapshot`, `fmt`, `repo-audit.yml`) do not install it.

---

## File structure

### Phase 0

| Path | Responsibility |
|---|---|
| `.cargo/config.toml` (create) | `cargo xtask` alias |
| `xtask/Cargo.toml` (modify) | add `toml`, dev-dep `tempfile` |
| `xtask/src/main.rs` (modify) | dispatch `remediation` subcommands |
| `xtask/src/remediation/mod.rs` (create) | entry points: `check`, `render`, `render --check` |
| `xtask/src/remediation/model.rs` (create) | ledger types, parsing |
| `xtask/src/remediation/evidence.rs` (create) | resolve `test:`/`script:`/`ci-job:`/`xtask:` targets |
| `xtask/src/remediation/check.rs` (create) | invariants 1–8 (§4.2) |
| `xtask/src/remediation/render.rs` (create) | status page markdown |
| `docs/remediation/ledger.toml` (create) | the ledger |
| `docs/status/remediation.md` (generated) | public status page |
| `docs/.vitepress/config.mts` (modify) | sidebar entry |
| `.github/workflows/ci.yml` (modify) | `remediation` job |
| `.github/workflows/remediation-gate.yml` (create) | dispatchable gate workflow |
| `crates/prkdb-schema/src/names.rs` (create) | collection-name validation (SCH-01) |
| `crates/prkdb-schema/tests/collection_names.rs` (create) | SCH-01 regression tests |
| `crates/prkdb/tests/tripwires.rs` (create) | tripwires for open findings |
| `crates/prkdb/src/raft/node.rs` (modify) | extract `majority_match_index` + RFT-03 tripwire |
| `scripts/pre-push-check.sh` (create) | local gate before any push |
| README, `docs/guide/**`, compose files (modify) | honesty edits |

### Phase 1

| Path | Responsibility |
|---|---|
| `.config/nextest.toml` (create) | nextest profiles |
| `crates/prkdb-core/src/vfs/mod.rs` (create) | `Vfs` / `VfsFile` traits |
| `crates/prkdb-core/src/vfs/std_vfs.rs` (create) | production `StdVfs` |
| `crates/prkdb-verify/` (create crate) | harness |
| `crates/prkdb-verify/src/faultfs.rs` | fault-injecting `Vfs` |
| `crates/prkdb-verify/src/model.rs` | reference model |
| `crates/prkdb-verify/src/ops.rs` | op enum, profiles, seeded generator |
| `crates/prkdb-verify/src/sut.rs` | driver over the public API |
| `crates/prkdb-verify/src/checker.rs` | durable checker |
| `crates/prkdb-verify/src/runner.rs` | seed loop, minimizer, report |
| `crates/prkdb-verify/src/bin/crash_child.rs` | subprocess for `SIGKILL` crash |
| `crates/prkdb-verify/tests/*.rs` | meta-test, discovery-finds-STO-01, profile test |
| `xtask/src/verify.rs` (create) | `cargo xtask verify` |
| `scripts/capture_baseline.sh` (create) | writes `docs/benchmarks/baseline-2026-09.toml` |

---

# Phase 0 — Honesty and tracking

### Task 0.1: `cargo xtask` alias and ledger model

**Files:**
- Create: `.cargo/config.toml`
- Modify: `xtask/Cargo.toml`, `xtask/src/main.rs`
- Create: `xtask/src/remediation/mod.rs`, `xtask/src/remediation/model.rs`

- [ ] **Step 1: Add the alias**

`.cargo/config.toml`:
```toml
[alias]
xtask = "run -p xtask --"
```

- [ ] **Step 2: Add dependencies**

In `xtask/Cargo.toml` add under `[dependencies]` `toml = "0.8"`, and add:
```toml
[dev-dependencies]
tempfile = "3"
```

- [ ] **Step 3: Write the failing model test**

`xtask/src/remediation/model.rs`:
```rust
//! Ledger schema (spec §4.1). Unknown fields are rejected so a typo cannot silently
//! drop evidence.

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ledger {
    #[serde(default)]
    pub finding: Vec<Finding>,
    #[serde(default)]
    pub phase: Vec<Phase>,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Open,
    InProgress,
    Fixed,
    Verified,
    WontFix,
    Duplicate,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Critical,
    High,
    Medium,
    Low,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PhaseStatus {
    NotStarted,
    InProgress,
    GatePassed,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Finding {
    pub id: String,
    pub title: String,
    pub area: String,
    pub severity: Severity,
    pub phase: u8,
    pub status: Status,
    #[serde(default)]
    pub security: bool,
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub evidence: Vec<String>,
    #[serde(default)]
    pub tripwire: String,
    #[serde(default)]
    pub regression_tests: Vec<String>,
    #[serde(default)]
    pub changes: Vec<String>,
    #[serde(default)]
    pub harness: String,
    #[serde(default)]
    pub ci_evidence: String,
    #[serde(default)]
    pub perf_note: String,
    #[serde(default)]
    pub decision: String,
    #[serde(default)]
    pub duplicate_of: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Phase {
    pub id: u8,
    pub title: String,
    pub status: PhaseStatus,
    #[serde(default)]
    pub gate: Vec<String>,
    #[serde(default)]
    pub gate_evidence: Vec<String>,
}

impl Ledger {
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        Ok(toml::from_str(text)?)
    }
}

impl Finding {
    /// Prefix before the dash, e.g. "STO" for "STO-01".
    pub fn prefix(&self) -> &str {
        self.id.split('-').next().unwrap_or("")
    }

    /// Spec §4.1: harness evidence is required for these areas, except KEY-03,
    /// which is proven by golden hash vectors.
    pub fn needs_harness(&self) -> bool {
        matches!(self.prefix(), "STO" | "KEY" | "EVT" | "TXN" | "TTL" | "RFT") && self.id != "KEY-03"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
[[finding]]
id = "STO-01"
title = "Checkpoint recovery drops pre-checkpoint keys"
area = "storage"
severity = "critical"
phase = 2
status = "open"
tripwire = "test:crates/prkdb/tests/tripwires.rs::sto01_checkpoint_drops_pre_checkpoint_keys_tripwire"

[[phase]]
id = 2
title = "Format v2"
status = "not_started"
"#;

    #[test]
    fn parses_sample_ledger() {
        let ledger = Ledger::parse(SAMPLE).unwrap();
        assert_eq!(ledger.finding.len(), 1);
        assert_eq!(ledger.finding[0].status, Status::Open);
        assert!(ledger.finding[0].needs_harness());
        assert_eq!(ledger.phase[0].status, PhaseStatus::NotStarted);
    }

    #[test]
    fn rejects_unknown_field() {
        let bad = SAMPLE.replace("tripwire =", "tripwrie =");
        assert!(Ledger::parse(&bad).is_err());
    }

    #[test]
    fn key03_is_exempt_from_harness() {
        let text = SAMPLE.replace("STO-01", "KEY-03");
        assert!(!Ledger::parse(&text).unwrap().finding[0].needs_harness());
    }
}
```

`xtask/src/remediation/mod.rs` (initial):
```rust
pub mod model;
```

Add `mod remediation;` to the top of `xtask/src/main.rs`.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p xtask remediation::model`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add .cargo/config.toml xtask/Cargo.toml xtask/src/main.rs xtask/src/remediation Cargo.lock
git commit -m "chore: add cargo xtask alias and remediation ledger model"
```

---

### Task 0.2: Evidence resolution

**Files:**
- Create: `xtask/src/remediation/evidence.rs`
- Modify: `xtask/src/remediation/mod.rs` (add `pub mod evidence;`)

- [ ] **Step 1: Write failing tests and the implementation skeleton**

`xtask/src/remediation/evidence.rs`:
```rust
//! Resolves evidence targets (spec §4.2 invariant 2). These checks prove evidence
//! *exists*; `ci_evidence` proves it ran.

use std::path::Path;

#[derive(Debug, PartialEq, Eq)]
pub enum Target<'a> {
    Test { file: &'a str, func: &'a str },
    Script(&'a str),
    CiJob { workflow: &'a str, job: &'a str },
    Xtask(&'a str),
}

pub fn parse(target: &str) -> Result<Target<'_>, String> {
    if let Some(rest) = target.strip_prefix("test:") {
        let (file, func) = rest
            .rsplit_once("::")
            .ok_or_else(|| format!("`{target}`: expected test:<file>::<fn>"))?;
        return Ok(Target::Test { file, func });
    }
    if let Some(rest) = target.strip_prefix("script:") {
        return Ok(Target::Script(rest));
    }
    if let Some(rest) = target.strip_prefix("ci-job:") {
        let (workflow, job) = rest
            .split_once('/')
            .ok_or_else(|| format!("`{target}`: expected ci-job:<workflow file>/<job id>"))?;
        return Ok(Target::CiJob { workflow, job });
    }
    if let Some(rest) = target.strip_prefix("xtask:") {
        return Ok(Target::Xtask(rest));
    }
    Err(format!("`{target}`: unknown evidence kind (test:, script:, ci-job:, xtask:)"))
}

/// Returns Ok if the target exists under `root`.
pub fn resolve(root: &Path, target: &str) -> Result<(), String> {
    match parse(target)? {
        Target::Test { file, func } => {
            let text = read(root, file)?;
            if find_fn(&text, func).is_none() {
                return Err(format!("`{target}`: fn `{func}` not found in {file}"));
            }
            Ok(())
        }
        Target::Script(path) => {
            let full = root.join(path);
            let meta = std::fs::metadata(&full).map_err(|_| format!("`{target}`: {path} missing"))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if meta.permissions().mode() & 0o111 == 0 {
                    return Err(format!("`{target}`: {path} is not executable"));
                }
            }
            let _ = meta;
            Ok(())
        }
        Target::CiJob { workflow, job } => {
            let text = read(root, &format!(".github/workflows/{workflow}"))?;
            let needle = format!("  {job}:");
            if !text.lines().any(|l| l.trim_end() == needle) {
                return Err(format!("`{target}`: job `{job}` not defined in {workflow}"));
            }
            Ok(())
        }
        Target::Xtask(sub) => {
            let text = read(root, "xtask/src/main.rs")?;
            if !text.contains(&format!("\"{sub}\"")) {
                return Err(format!("`{target}`: xtask subcommand `{sub}` not dispatched"));
            }
            Ok(())
        }
    }
}

/// Line index of `fn <name>(` or `fn <name><`, if present.
pub fn find_fn(text: &str, name: &str) -> Option<usize> {
    let a = format!("fn {name}(");
    let b = format!("fn {name}<");
    text.lines().position(|l| l.contains(&a) || l.contains(&b))
}

/// True if an `#[ignore` attribute appears in the 6 lines above the fn.
pub fn is_ignored(root: &Path, target: &str) -> bool {
    let Ok(Target::Test { file, func }) = parse(target) else { return false };
    let Ok(text) = read(root, file) else { return false };
    let lines: Vec<&str> = text.lines().collect();
    let Some(idx) = find_fn(&text, func) else { return false };
    lines[idx.saturating_sub(6)..idx].iter().any(|l| l.trim_start().starts_with("#[ignore"))
}

fn read(root: &Path, rel: &str) -> Result<String, String> {
    std::fs::read_to_string(root.join(rel)).map_err(|_| format!("{rel} missing"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn repo() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("t")).unwrap();
        fs::write(
            dir.path().join("t/a.rs"),
            "#[test]\nfn good() {}\n#[test]\n#[ignore = \"slow: x\"]\nfn skipped() {}\n",
        )
        .unwrap();
        fs::create_dir_all(dir.path().join(".github/workflows")).unwrap();
        fs::write(dir.path().join(".github/workflows/ci.yml"), "jobs:\n  remediation:\n    runs-on: x\n").unwrap();
        fs::create_dir_all(dir.path().join("xtask/src")).unwrap();
        fs::write(dir.path().join("xtask/src/main.rs"), "[\"verify\"] => x,").unwrap();
        dir
    }

    #[test]
    fn resolves_each_kind() {
        let r = repo();
        assert!(resolve(r.path(), "test:t/a.rs::good").is_ok());
        assert!(resolve(r.path(), "test:t/a.rs::missing").is_err());
        assert!(resolve(r.path(), "ci-job:ci.yml/remediation").is_ok());
        assert!(resolve(r.path(), "ci-job:ci.yml/nope").is_err());
        assert!(resolve(r.path(), "xtask:verify").is_ok());
        assert!(resolve(r.path(), "script:nope.sh").is_err());
        assert!(resolve(r.path(), "bogus:x").is_err());
    }

    #[test]
    fn detects_ignored_tests() {
        let r = repo();
        assert!(is_ignored(r.path(), "test:t/a.rs::skipped"));
        assert!(!is_ignored(r.path(), "test:t/a.rs::good"));
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p xtask remediation::evidence`
Expected: 2 passed.

- [ ] **Step 3: Commit**

```bash
git add xtask/src/remediation
git commit -m "feat: resolve remediation evidence targets"
```

---

### Task 0.3: Ledger invariants (`check`)

**Files:**
- Create: `xtask/src/remediation/check.rs`
- Modify: `xtask/src/remediation/mod.rs`

- [ ] **Step 1: Write `check.rs` with tests per invariant**

```rust
//! Spec §4.2 invariants. Returns every violation, not just the first.

use super::evidence;
use super::model::{Ledger, PhaseStatus, Status};
use std::collections::HashSet;
use std::path::Path;

const PREFIXES: [&str; 10] = ["STO", "KEY", "EVT", "TXN", "TTL", "RFT", "SCH", "REL", "TST", "DOC"];

pub fn check(ledger: &Ledger, root: &Path) -> Vec<String> {
    let mut errs = Vec::new();
    let mut seen = HashSet::new();
    let ids: HashSet<&str> = ledger.finding.iter().map(|f| f.id.as_str()).collect();

    for f in &ledger.finding {
        let id = &f.id;
        // 1. unique, well-formed id
        if !seen.insert(id.as_str()) {
            errs.push(format!("{id}: duplicate id"));
        }
        let well_formed = id.len() == 6
            && PREFIXES.contains(&f.prefix())
            && id.as_bytes()[3] == b'-'
            && id[4..].bytes().all(|b| b.is_ascii_digit());
        if !well_formed {
            errs.push(format!("{id}: id must match ^(STO|KEY|EVT|TXN|TTL|RFT|SCH|REL|TST|DOC)-\\d{{2}}$"));
        }

        let done = matches!(f.status, Status::Fixed | Status::Verified);
        // 2. fixed/verified need resolvable evidence and changes; tripwire gone
        if done {
            if f.regression_tests.is_empty() {
                errs.push(format!("{id}: {:?} requires regression_tests", f.status));
            }
            if f.changes.is_empty() {
                errs.push(format!("{id}: {:?} requires changes", f.status));
            }
            for t in &f.regression_tests {
                if let Err(e) = evidence::resolve(root, t) {
                    errs.push(format!("{id}: {e}"));
                }
            }
            if !f.tripwire.is_empty() && evidence::resolve(root, &f.tripwire).is_ok() {
                errs.push(format!("{id}: tripwire still exists; invert it into the regression test"));
            }
        }
        // 3. open tripwires must exist
        if matches!(f.status, Status::Open | Status::InProgress) && !f.tripwire.is_empty() {
            if let Err(e) = evidence::resolve(root, &f.tripwire) {
                errs.push(format!("{id}: tripwire {e}"));
            }
        }
        // 4. nothing listed may be #[ignore]d
        for t in f.regression_tests.iter().chain(std::iter::once(&f.tripwire)) {
            if !t.is_empty() && evidence::is_ignored(root, t) {
                errs.push(format!("{id}: `{t}` is #[ignore]d"));
            }
        }
        // 5. verified needs CI evidence, plus harness in harness areas
        if f.status == Status::Verified {
            if f.ci_evidence.is_empty() {
                errs.push(format!("{id}: verified requires ci_evidence"));
            }
            if f.needs_harness() && f.harness.is_empty() {
                errs.push(format!("{id}: verified requires harness"));
            }
        }
        // 6. wont_fix / duplicate
        if f.status == Status::WontFix && f.decision.is_empty() {
            errs.push(format!("{id}: wont_fix requires decision"));
        }
        if f.status == Status::Duplicate && !ids.contains(f.duplicate_of.as_str()) {
            errs.push(format!("{id}: duplicate_of must name an existing id"));
        }
        // 7. phase is a single integer by construction (u8); range-check it
        if f.phase > 6 {
            errs.push(format!("{id}: phase must be 0..=6"));
        }
    }

    // 8. gates
    for p in &ledger.phase {
        if p.status != PhaseStatus::GatePassed {
            continue;
        }
        if p.gate_evidence.is_empty() {
            errs.push(format!("phase {}: gate_passed requires gate_evidence", p.id));
        }
        for f in ledger.finding.iter().filter(|f| f.phase == p.id) {
            if !matches!(f.status, Status::Verified | Status::WontFix | Status::Duplicate) {
                errs.push(format!("phase {}: {} is {:?}, not verified", p.id, f.id, f.status));
            }
        }
    }
    errs
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn root() -> tempfile::TempDir {
        let d = tempfile::tempdir().unwrap();
        fs::create_dir_all(d.path().join("t")).unwrap();
        fs::write(d.path().join("t/a.rs"), "fn trip() {}\nfn reg() {}\n#[ignore = \"slow: x\"]\nfn ign() {}\n").unwrap();
        d
    }

    fn one(extra: &str) -> Ledger {
        Ledger::parse(&format!(
            "[[finding]]\nid = \"STO-01\"\ntitle = \"t\"\narea = \"storage\"\nseverity = \"critical\"\nphase = 2\n{extra}\n"
        ))
        .unwrap()
    }

    #[test]
    fn open_with_existing_tripwire_is_clean() {
        let l = one("status = \"open\"\ntripwire = \"test:t/a.rs::trip\"");
        assert!(check(&l, root().path()).is_empty());
    }

    #[test]
    fn fixed_without_evidence_fails() {
        let e = check(&one("status = \"fixed\""), root().path());
        assert!(e.iter().any(|m| m.contains("requires regression_tests")));
        assert!(e.iter().any(|m| m.contains("requires changes")));
    }

    #[test]
    fn fixed_with_live_tripwire_fails() {
        let l = one("status = \"fixed\"\ntripwire = \"test:t/a.rs::trip\"\nregression_tests = [\"test:t/a.rs::reg\"]\nchanges = [\"abc123\"]");
        assert!(check(&l, root().path()).iter().any(|m| m.contains("tripwire still exists")));
    }

    #[test]
    fn ignored_regression_test_fails() {
        let l = one("status = \"fixed\"\nregression_tests = [\"test:t/a.rs::ign\"]\nchanges = [\"abc\"]");
        assert!(check(&l, root().path()).iter().any(|m| m.contains("#[ignore]d")));
    }

    #[test]
    fn verified_harness_area_needs_harness() {
        let l = one("status = \"verified\"\nregression_tests = [\"test:t/a.rs::reg\"]\nchanges = [\"abc\"]\nci_evidence = \"https://x\"");
        assert!(check(&l, root().path()).iter().any(|m| m.contains("requires harness")));
    }

    #[test]
    fn gate_passed_with_open_finding_fails() {
        let mut text = String::from("[[finding]]\nid = \"DOC-06\"\ntitle = \"t\"\narea = \"docs\"\nseverity = \"low\"\nphase = 0\nstatus = \"open\"\n");
        text.push_str("[[phase]]\nid = 0\ntitle = \"p\"\nstatus = \"gate_passed\"\ngate_evidence = [\"https://x\"]\n");
        let e = check(&Ledger::parse(&text).unwrap(), root().path());
        assert!(e.iter().any(|m| m.contains("not verified")));
    }

    #[test]
    fn malformed_id_fails() {
        let l = Ledger::parse("[[finding]]\nid = \"STO-1a\"\ntitle = \"t\"\narea = \"s\"\nseverity = \"low\"\nphase = 0\nstatus = \"open\"\n").unwrap();
        assert!(check(&l, root().path()).iter().any(|m| m.contains("id must match")));
    }
}
```

Add `pub mod check;` to `mod.rs`.

- [ ] **Step 2: Run tests**

Run: `cargo test -p xtask remediation::check`
Expected: 7 passed.

- [ ] **Step 3: Commit**

```bash
git add xtask/src/remediation
git commit -m "feat: enforce remediation ledger invariants"
```

---

### Task 0.4: Render and CLI wiring

**Files:**
- Create: `xtask/src/remediation/render.rs`
- Modify: `xtask/src/remediation/mod.rs`, `xtask/src/main.rs`

- [ ] **Step 1: Write `render.rs`**

```rust
//! Renders docs/status/remediation.md. Security findings are hidden until fixed (§5.1).

use super::model::{Ledger, Status};
use std::fmt::Write;

pub fn render(ledger: &Ledger) -> String {
    let mut out = String::new();
    writeln!(out, "# Remediation status\n").unwrap();
    writeln!(out, "<!-- Generated by `cargo xtask remediation render`. Do not edit by hand. -->\n").unwrap();
    writeln!(out, "Tracks the root-cause remediation program. A finding is **verified** only when its regression tests passed in public CI (and, for storage, keys, events, transactions, TTL and Raft, the crash/restart harness).\n").unwrap();

    writeln!(out, "## Phases\n\n| Phase | Title | Status | Findings verified |\n|---|---|---|---|").unwrap();
    for p in &ledger.phase {
        let all: Vec<_> = ledger.finding.iter().filter(|f| f.phase == p.id).collect();
        let done = all.iter().filter(|f| matches!(f.status, Status::Verified | Status::WontFix | Status::Duplicate)).count();
        writeln!(out, "| {} | {} | {:?} | {}/{} |", p.id, p.title, p.status, done, all.len()).unwrap();
    }

    let mut visible: Vec<_> = ledger
        .finding
        .iter()
        .filter(|f| !f.security || matches!(f.status, Status::Fixed | Status::Verified))
        .collect();
    visible.sort_by(|a, b| (is_closed(a.status), a.severity, &a.id).cmp(&(is_closed(b.status), b.severity, &b.id)));

    writeln!(out, "\n## Findings\n\n| ID | Severity | Phase | Status | Title |\n|---|---|---|---|---|").unwrap();
    for f in visible {
        writeln!(out, "| {} | {:?} | {} | {:?} | {} |", f.id, f.severity, f.phase, f.status, f.title.replace('|', "\\|")).unwrap();
    }
    out
}

fn is_closed(s: Status) -> bool {
    matches!(s, Status::Verified | Status::WontFix | Status::Duplicate)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hides_unfixed_security_findings() {
        let l = Ledger::parse(
            "[[finding]]\nid = \"SCH-01\"\ntitle = \"secret\"\narea = \"schema\"\nseverity = \"high\"\nphase = 0\nstatus = \"open\"\nsecurity = true\n",
        )
        .unwrap();
        assert!(!render(&l).contains("SCH-01"));
    }

    #[test]
    fn open_criticals_sort_first() {
        let l = Ledger::parse(concat!(
            "[[finding]]\nid = \"DOC-06\"\ntitle = \"low\"\narea = \"docs\"\nseverity = \"low\"\nphase = 0\nstatus = \"open\"\n",
            "[[finding]]\nid = \"STO-01\"\ntitle = \"crit\"\narea = \"storage\"\nseverity = \"critical\"\nphase = 2\nstatus = \"open\"\n",
        ))
        .unwrap();
        let text = render(&l);
        assert!(text.find("STO-01").unwrap() < text.find("DOC-06").unwrap());
    }
}
```

- [ ] **Step 2: Wire `mod.rs` entry points**

```rust
pub mod check;
pub mod evidence;
pub mod model;
pub mod render;

use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};

const LEDGER: &str = "docs/remediation/ledger.toml";
const PAGE: &str = "docs/status/remediation.md";

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().to_path_buf()
}

fn load() -> Result<model::Ledger> {
    let path = root().join(LEDGER);
    let text = std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    model::Ledger::parse(&text)
}

pub fn run_check() -> Result<()> {
    let errs = check::check(&load()?, &root());
    if errs.is_empty() {
        println!("remediation ledger: ok");
        return Ok(());
    }
    for e in &errs {
        eprintln!("  ✗ {e}");
    }
    bail!("{} ledger violation(s)", errs.len())
}

pub fn run_render(check_only: bool) -> Result<()> {
    let fresh = render::render(&load()?);
    let path = root().join(PAGE);
    if check_only {
        let committed = std::fs::read_to_string(&path).unwrap_or_default();
        if committed != fresh {
            bail!("{PAGE} is stale; run `cargo xtask remediation render`");
        }
        return Ok(());
    }
    std::fs::write(&path, fresh)?;
    println!("wrote {PAGE}");
    Ok(())
}
```

In `xtask/src/main.rs` add match arms and usage lines:
```rust
        ["remediation", "check"] => remediation::run_check(),
        ["remediation", "render"] => remediation::run_render(false),
        ["remediation", "render", "--check"] => remediation::run_render(true),
```
Usage text: `cargo xtask remediation <check|render> [--check]`.

- [ ] **Step 3: Run tests**

Run: `cargo test -p xtask`
Expected: all pass (existing + 14 new).

- [ ] **Step 4: Commit**

```bash
git add xtask
git commit -m "feat: render remediation status page"
```

---

### Task 0.5: Populate the ledger

**Files:**
- Create: `docs/remediation/ledger.toml`
- Create (generated): `docs/status/remediation.md`

- [ ] **Step 1: Transcribe every finding in spec §3.1–§3.8 into `ledger.toml`**

One `[[finding]]` per row, all `status = "open"`. Use: `area` = storage / keys / events / transactions / ttl / raft / schema / release / tests / docs; `severity` from the Sev column (CRIT→critical); `phase` from the Phase column; `sources` from the evidence citation (`"audit-2026-09-23#1"`, `"review-2026-09-07#R01"`, `"docs-audit-2026-09-23"`); `evidence` = file:line strings. SCH-01 gets `security = true`. Add the six `[[phase]]` blocks 0–5 with `gate` strings copied from each phase's **Gate** line in §7, all `status = "not_started"`. Leave `tripwire` empty for now (Task 0.8 fills it).

Example of the first entry:
```toml
[[finding]]
id = "STO-01"
title = "Checkpoint recovery drops pre-checkpoint keys"
area = "storage"
severity = "critical"
phase = 2
status = "open"
sources = ["audit-2026-09-23#1"]
evidence = [
  "crates/prkdb/src/storage/wal_adapter.rs:1050",
  "crates/prkdb/src/storage/checkpoint.rs:44",
  "crates/prkdb-core/src/wal/mmap_parallel_wal.rs:310",
]
```

- [ ] **Step 2: Check the count and invariants**

Run: `grep -c '^\[\[finding\]\]' docs/remediation/ledger.toml && cargo xtask remediation check`
Expected: count equals the number of rows in §3 (STO 7, KEY 3, EVT 6, TXN 4, TTL 1, RFT 9, SCH 2, REL 1, TST 7, DOC 12 = **52**), then `remediation ledger: ok`.

- [ ] **Step 3: Render**

Run: `cargo xtask remediation render`
Expected: `wrote docs/status/remediation.md`; the file does not contain `SCH-01`.

- [ ] **Step 4: Commit**

```bash
git add docs/remediation/ledger.toml docs/status/remediation.md
git commit -m "docs: add remediation finding ledger"
```

---

### Task 0.6: SCH-01 — schema collection names cannot escape the registry

**Files:**
- Create: `crates/prkdb-schema/src/names.rs`
- Modify: `crates/prkdb-schema/src/lib.rs`, `crates/prkdb-schema/src/error.rs`, `crates/prkdb-schema/src/registry.rs:36-47`, `crates/prkdb-schema/src/storage.rs:293-300`
- Test: `crates/prkdb-schema/tests/collection_names.rs`

Design note: the spec says "validate and encode to filesystem names". An allowlist of `[A-Za-z0-9_.-]` makes every accepted name filesystem-safe as-is, so no encoding layer is needed. Update spec §7 Phase 0 item 4 to say "validate against an allowlist" in the same commit.

- [ ] **Step 1: Write the failing tests**

`crates/prkdb-schema/tests/collection_names.rs`:
```rust
use prkdb_schema::{CompatibilityMode, FileSchemaStorage, SchemaError, SchemaRegistry};
use prost::Message;
use prost_types::FileDescriptorProto;
use std::sync::Arc;

fn valid_proto() -> Vec<u8> {
    FileDescriptorProto { name: Some("user.proto".into()), ..Default::default() }.encode_to_vec()
}

#[tokio::test]
async fn sch01_traversal_names_are_rejected_before_any_write() {
    let root = tempfile::tempdir().unwrap();
    let base = root.path().join("registry");
    let registry = SchemaRegistry::new(Arc::new(FileSchemaStorage::new(base.clone())));
    for bad in ["../../escaped", "..", ".", "a/b", "a\\b", "/abs", "", ".hidden", "nul\0byte", &"x".repeat(129)] {
        let err = registry.register(bad, valid_proto(), CompatibilityMode::Backward, None).await.unwrap_err();
        assert!(matches!(err, SchemaError::InvalidCollectionName(_)), "{bad:?} gave {err:?}");
    }
    assert!(!root.path().join("escaped").exists());
    assert!(!base.exists(), "nothing may be written for rejected names");
}

#[tokio::test]
async fn sch01_malformed_descriptor_is_rejected_before_any_write() {
    let root = tempfile::tempdir().unwrap();
    let base = root.path().join("registry");
    let registry = SchemaRegistry::new(Arc::new(FileSchemaStorage::new(base.clone())));
    let err = registry.register("users", vec![1, 2, 3], CompatibilityMode::Backward, None).await.unwrap_err();
    assert!(matches!(err, SchemaError::InvalidDescriptor(_)), "{err:?}");
    assert!(!base.exists());
}

#[tokio::test]
async fn sch01_valid_names_still_register() {
    let root = tempfile::tempdir().unwrap();
    let registry = SchemaRegistry::new(Arc::new(FileSchemaStorage::new(root.path().into())));
    for good in ["users", "user_events", "v2.orders", "a-b"] {
        registry.register(good, valid_proto(), CompatibilityMode::Backward, None).await.unwrap();
    }
}
```

Add `prost-types` is already a dependency; add to `[dev-dependencies]` nothing new (tempfile present).

- [ ] **Step 2: Run to see it fail**

Run: `cargo test -p prkdb-schema --test collection_names`
Expected: compile error `no variant named InvalidCollectionName`.

- [ ] **Step 3: Implement**

`error.rs` — add variant:
```rust
    /// Collection name is not a safe logical name
    #[error("Invalid collection name {0:?}: use 1-128 characters from [A-Za-z0-9_.-], not starting with '.'")]
    InvalidCollectionName(String),
```

`names.rs`:
```rust
//! Collection names become directory names in FileSchemaStorage, so they are
//! restricted to a filesystem-safe allowlist (SCH-01).

use crate::error::{SchemaError, SchemaResult};

pub const MAX_COLLECTION_NAME_LEN: usize = 128;

pub fn validate_collection_name(name: &str) -> SchemaResult<()> {
    let ok = !name.is_empty()
        && name.len() <= MAX_COLLECTION_NAME_LEN
        && !name.starts_with('.')
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'));
    if ok {
        Ok(())
    } else {
        Err(SchemaError::InvalidCollectionName(name.to_string()))
    }
}

pub fn validate_descriptor(bytes: &[u8]) -> SchemaResult<()> {
    use prost::Message;
    prost_types::FileDescriptorProto::decode(bytes)
        .map(|_| ())
        .map_err(|e| SchemaError::InvalidDescriptor(e.to_string()))
}
```

`lib.rs`: `pub mod names;` and `pub use names::validate_collection_name;`.

`registry.rs` — first lines of `register`, before the `info!`:
```rust
        crate::names::validate_collection_name(collection)?;
        crate::names::validate_descriptor(&schema_proto)?;
```

`storage.rs` — first line of `FileSchemaStorage::put` (defense in depth for direct storage callers):
```rust
        crate::names::validate_collection_name(&schema.collection)?;
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p prkdb-schema && cargo test -p prkdb --test schema_tests`
Expected: all pass (the existing tests use real `FileDescriptorProto` bytes).

- [ ] **Step 5: Update ledger and commit**

In `ledger.toml` for SCH-01: `status = "fixed"`, `regression_tests = ["test:crates/prkdb-schema/tests/collection_names.rs::sch01_traversal_names_are_rejected_before_any_write", "test:crates/prkdb-schema/tests/collection_names.rs::sch01_malformed_descriptor_is_rejected_before_any_write"]`, `changes = ["<code commit SHA>"]` (set after the code commit).

Code commit first, ledger as a separate follow-up commit (spec §5: fix → ledger update), so the recorded SHA stays valid:
```bash
git add crates/prkdb-schema docs/superpowers/specs/2026-09-23-root-cause-remediation-design.md
git commit -m "fix: reject unsafe schema collection names and malformed descriptors"
git rev-parse --short HEAD        # put this SHA into SCH-01 `changes`
cargo xtask remediation check
cargo xtask remediation render
git add docs/remediation/ledger.toml docs/status/remediation.md
git commit -m "docs: mark SCH-01 fixed in remediation ledger"
```
Never amend a commit whose SHA is recorded in the ledger. This pattern applies to every finding in this plan.

---

### Task 0.7: RFT-03 — extract the majority computation

**Files:**
- Modify: `crates/prkdb/src/raft/node.rs:817-830`

This is a pure refactor with identical behaviour, so the bug becomes testable.

- [ ] **Step 1: Extract the function**

Above `impl RaftNode` (module level) add:
```rust
/// Match index replicated on a majority. Kept byte-for-byte equivalent to the
/// previous inline code so the RFT-03 tripwire documents current behaviour.
pub(crate) fn majority_match_index(indices: &mut [u64]) -> u64 {
    indices.sort_unstable();
    indices[indices.len() / 2]
}
```
In `update_commit_index`, replace the three lines from `indices.sort_unstable();` through `let new_commit_index = indices[majority_idx];` with:
```rust
        let new_commit_index = majority_match_index(&mut indices);
```

- [ ] **Step 2: Add the tripwire at the bottom of `node.rs`**

```rust
#[cfg(test)]
mod rft03_tripwire {
    use super::majority_match_index;

    /// RFT-03 tripwire: asserts the BUG. 4 nodes, only 2 hold index 5, yet 5 is
    /// reported as majority-replicated (correct answer: 1). When this fails, the
    /// bug is fixed: invert it into a regression test and update the ledger.
    #[test]
    fn rft03_even_cluster_commits_without_majority_tripwire() {
        assert_eq!(majority_match_index(&mut [1, 1, 5, 5]), 5);
        // 2-node cluster: leader alone "commits".
        assert_eq!(majority_match_index(&mut [0, 7]), 7);
    }
}
```

- [ ] **Step 3: Run**

Run: `cargo test -p prkdb --lib rft03 && cargo test -p prkdb --test election_safety`
Expected: tripwire passes; election tests unchanged.

- [ ] **Step 4: Commit**

```bash
git add crates/prkdb/src/raft/node.rs
git commit -m "test: extract raft majority computation and add RFT-03 tripwire"
```

---

### Task 0.8: Tripwires for reproducible findings

**Files:**
- Create: `crates/prkdb/tests/tripwires.rs`
- Modify: `docs/remediation/ledger.toml` (tripwire fields)

- [ ] **Step 1: Write the tripwires**

```rust
//! Tripwires assert that a known bug STILL EXISTS. They pass today and fail the
//! moment the bug is fixed, forcing the fixer to invert them into regression tests
//! and update docs/remediation/ledger.toml. See spec §4.1.
//!
//! Tests that need a fresh process re-run this test binary as a child with
//! PRKDB_TRIPWIRE_CHILD set; the child prints `CHILD_RESULT=<value>`.

use prkdb::indexed_storage::IndexedStorage;
use prkdb::storage::{InMemoryAdapter, WalStorageAdapter};
use prkdb_core::wal::WalConfig;
use prkdb_macros::Collection;
use prkdb_types::storage::StorageAdapter;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const CHILD_ENV: &str = "PRKDB_TRIPWIRE_CHILD";

fn child_result(test_name: &str) -> String {
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([test_name, "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, "1")
        .output()
        .expect("spawn child");
    let stdout = String::from_utf8_lossy(&out.stdout);
    stdout
        .lines()
        .find_map(|l| l.strip_prefix("CHILD_RESULT="))
        .map(str::to_owned)
        .unwrap_or_else(|| panic!("child printed no result; stdout:\n{stdout}\nstderr:\n{}", String::from_utf8_lossy(&out.stderr)))
}

fn is_child() -> bool {
    std::env::var_os(CHILD_ENV).is_some()
}

fn wal_config(dir: &std::path::Path) -> WalConfig {
    WalConfig { log_dir: dir.to_path_buf(), ..WalConfig::test_config() }
}

/// STO-01: keys written before a checkpoint vanish after reopen.
#[tokio::test(flavor = "multi_thread")]
async fn sto01_checkpoint_drops_pre_checkpoint_keys_tripwire() {
    let dir = tempfile::tempdir().unwrap();
    {
        let a = WalStorageAdapter::new(wal_config(dir.path())).unwrap();
        for i in 0..5u8 {
            a.put(&[b'k', i], b"v").await.unwrap();
        }
        a.flush().await.unwrap();
        a.save_checkpoint().unwrap();
    }
    let b = WalStorageAdapter::open_async(wal_config(dir.path())).await.unwrap();
    assert_eq!(b.get(&[b'k', 0]).await.unwrap(), None, "STO-01 appears fixed: invert this tripwire");
    assert_eq!(b.get(&[b'k', 4]).await.unwrap(), Some(b"v".to_vec()));
}

#[derive(Collection, Serialize, Deserialize, Clone, Debug)]
struct TwUser {
    #[id]
    id: u64,
    #[index]
    name: String,
}

#[derive(Collection, Serialize, Deserialize, Clone, Debug)]
struct TwProject {
    #[id]
    id: u64,
    #[index]
    name: String,
}

/// KEY-01: two collections with the same id overwrite each other.
#[tokio::test]
async fn key01_collections_share_primary_keys_tripwire() {
    let db = IndexedStorage::new(Arc::new(InMemoryAdapter::new()));
    db.insert(&TwUser { id: 1, name: "Alice".into() }).await.unwrap();
    db.insert(&TwProject { id: 1, name: "Project".into() }).await.unwrap();
    let user = db.get::<TwUser>(&1).await.unwrap().unwrap();
    assert_eq!(user.name, "Project", "KEY-01 appears fixed: invert this tripwire");
}

/// KEY-03: the default partitioner is seeded per process.
#[test]
fn key03_partition_differs_across_processes_tripwire() {
    use prkdb::partitioning::{DefaultPartitioner, Partitioner};
    if is_child() {
        let p = DefaultPartitioner::<String>::new().partition(&"user-42".to_string(), 1_000_000);
        println!("CHILD_RESULT={p}");
        return;
    }
    let results: Vec<String> = (0..3).map(|_| child_result("key03_partition_differs_across_processes_tripwire")).collect();
    assert!(
        !(results[0] == results[1] && results[1] == results[2]),
        "KEY-03 appears fixed (stable across processes: {results:?}): invert this tripwire"
    );
}

#[derive(Collection, Serialize, Deserialize, Clone, Debug)]
struct TwEvent {
    #[id]
    id: u64,
}

/// EVT-01: the outbox sequence restarts at 1 in every process.
#[test]
fn evt01_outbox_sequence_restarts_per_process_tripwire() {
    if is_child() {
        println!("CHILD_RESULT={}", prkdb::outbox::make_outbox_id_for_type::<TwEvent>(None));
        return;
    }
    let first = child_result("evt01_outbox_sequence_restarts_per_process_tripwire");
    let second = child_result("evt01_outbox_sequence_restarts_per_process_tripwire");
    assert_eq!(first, second, "EVT-01 appears fixed: invert this tripwire");
}

/// TXN-04: default isolation is ReadCommitted (D5 makes it Serializable).
#[test]
fn txn04_default_isolation_is_read_committed_tripwire() {
    use prkdb::transaction::{IsolationLevel, TransactionConfig};
    assert_eq!(
        TransactionConfig::default().isolation_level,
        IsolationLevel::ReadCommitted,
        "TXN-04 appears fixed: invert this tripwire"
    );
}
```

If an import path does not compile, find the real path with `grep -rn "pub use\|pub mod" crates/prkdb/src/lib.rs crates/prkdb/src/storage/mod.rs` and fix the `use` line only; do not change behaviour.

- [ ] **Step 2: Run**

Run: `cargo test -p prkdb --test tripwires`
Expected: 5 passed.

- [ ] **Step 3: Record tripwires in the ledger**

Set `tripwire` for STO-01, KEY-01, KEY-03, EVT-01, TXN-04 to `"test:crates/prkdb/tests/tripwires.rs::<fn>"` and RFT-03 to `"test:crates/prkdb/src/raft/node.rs::rft03_even_cluster_commits_without_majority_tripwire"`.

Run: `cargo xtask remediation check && cargo xtask remediation render`
Expected: ok.

- [ ] **Step 4: Commit**

```bash
git add crates/prkdb/tests/tripwires.rs docs/remediation/ledger.toml docs/status/remediation.md
git commit -m "test: add tripwires for STO-01, KEY-01, KEY-03, EVT-01, TXN-04"
```

---

### Task 0.9: Honesty edits in docs and compose files

**Files:**
- Modify: `README.md`, `docs/index.md`, `docs/guide/getting-started.md`, `docs/guide/features/transactions.md`, `docs/guide/deployment.md`, `docs/guide/replication.md`, `docs/guide/streaming-kafka-comparison.md`, `docker-compose.yml`, `docker-compose-simple.yml`, `docs/.vitepress/config.mts`

- [ ] **Step 1: Experimental labels**

In `README.md` feature list, append ` *(experimental — see [status](https://prk-jr.github.io/prkdb/status/remediation))*` to the Raft consensus, Advanced Sharding, Read Consistency Levels, and Kafka-style consumers bullets. Add the same one-line admonition at the top of `docs/guide/replication.md`, `docs/guide/architecture/partitions.md`, and `docs/guide/architecture/leader-election.md`:
```md
::: warning Experimental
Clustering is experimental until the remediation program's Raft phase completes. See [Remediation status](/status/remediation).
:::
```

- [ ] **Step 2: DOC-11 — transactions page**

Replace `docs/guide/features/transactions.md:5` with:
```md
Transactions currently default to `ReadCommitted`, which does not detect conflicts. Pass `IsolationLevel::Serializable` for conflict detection. Serializable becomes the default in an upcoming release.
```
Also fix line 69 ("under Serializable Isolation") to say "when `IsolationLevel::Serializable` is set".

- [ ] **Step 3: DOC-06 — Rust version**

`README.md:7` badge → `Rust-1.98+`; `docs/guide/getting-started.md:7` → `Rust 1.98+`.

- [ ] **Step 4: DOC-09 — unsourced claims**

Delete "10x less" (including "(10x less)" at ~181), "~10 MB binary", "<1 sec startup" from `docs/guide/streaming-kafka-comparison.md` (lines ~52-56 and ~181) and "99.4%" from `README.md` (two occurrences, ~923 and ~933), or replace each with a link to a measured result in `docs/benchmarks/methodology.md`. Keep the benchmark caveat sentences that `xtask/src/repo_status/collectors/docs.rs` requires in README and the streaming doc. Add ` *(known issue: [STO-01](https://prk-jr.github.io/prkdb/status/remediation))*` to the README "Checkpoint Recovery" bullet (~26). Run `grep -rn "10x less\|10 MB\|99.4" README.md docs/guide` — expected: no matches.

- [ ] **Step 5: DOC-01 — deployment docs and compose**

In `docs/guide/deployment.md` 3-node section, add the required variables:
```md
Every multi-node cluster must authenticate its Raft peers, or `prkdb-server` refuses to start:

| Variable | Purpose |
|---|---|
| `PRKDB_CLUSTER_SECRET` | Shared secret sent on every Raft RPC (same value on all nodes) |
| `PRKDB_TLS_CLIENT_CA` | Alternative: mutual TLS for peers |
| `PRKDB_BOOTSTRAP_TOKEN` | Creates the first admin principal on an empty data directory |
| `PRKDB_METRICS_ADDR` | Metrics bind address; use `0.0.0.0:<port>` inside containers |

`/metrics` requires an Admin bearer token: `curl -H "Authorization: Bearer $PRKDB_BOOTSTRAP_TOKEN" http://localhost:9091/metrics`.
```
Remove the client-side `PRKDB_BOOTSTRAP_TOKEN` export (lines ~94, ~102). Apply the same variables to the README cluster section (~757-773).

`docker-compose.yml` — for each node's `environment:` add (port matches that node's metrics port):
```yaml
      - PRKDB_CLUSTER_SECRET=${PRKDB_CLUSTER_SECRET:?set PRKDB_CLUSTER_SECRET}
      - PRKDB_BOOTSTRAP_TOKEN=${PRKDB_BOOTSTRAP_TOKEN:?set PRKDB_BOOTSTRAP_TOKEN}
      - PRKDB_METRICS_ADDR=0.0.0.0:9091
```
and each healthcheck:
```yaml
      test: ["CMD-SHELL", "curl -f -H \"Authorization: Bearer $$PRKDB_BOOTSTRAP_TOKEN\" http://localhost:9091/metrics"]
```
`docker-compose-simple.yml` — add `CLUSTER_NODES=1@127.0.0.1:50051`, `PRKDB_BOOTSTRAP_TOKEN=${PRKDB_BOOTSTRAP_TOKEN:?set PRKDB_BOOTSTRAP_TOKEN}`.

- [ ] **Step 6: Verify compose parses and docs build**

Run: `PRKDB_CLUSTER_SECRET=x PRKDB_BOOTSTRAP_TOKEN=y docker compose -f docker-compose.yml config -q && PRKDB_BOOTSTRAP_TOKEN=y docker compose -f docker-compose-simple.yml config -q && (cd docs && npx vitepress build)`
Expected: no output from compose; VitePress build succeeds. (If Docker is not installed locally, note it; CI covers it in Phase 5.)

- [ ] **Step 7: Sidebar**

In `docs/.vitepress/config.mts` add a sidebar group:
```ts
      {
        text: 'Status',
        items: [{ text: 'Remediation status', link: '/status/remediation' }],
      },
```

- [ ] **Step 8: Ledger and commit**

Mark DOC-01, DOC-06, DOC-09, DOC-11 `fixed`, with `regression_tests` = `["script:scripts/check_doc_claims.sh"]` — create that script:
```bash
#!/usr/bin/env bash
# Fails if known-false documentation claims return (DOC-01, DOC-06, DOC-09, DOC-11).
set -euo pipefail
cd "$(dirname "$0")/.."
fail=0
check() { if grep -rqn -- "$1" $2; then echo "  ✗ found forbidden claim: $1"; fail=1; fi; }
check "Serializable\*\* isolation mode by default" docs/guide
check "Rust-1.95" README.md
check "Rust 1.75" docs/guide
check "99.4%" README.md
check "10x less" docs/guide
grep -q "PRKDB_CLUSTER_SECRET" docs/guide/deployment.md || { echo "  ✗ deployment.md lacks PRKDB_CLUSTER_SECRET"; fail=1; }
grep -q "PRKDB_CLUSTER_SECRET" docker-compose.yml || { echo "  ✗ docker-compose.yml lacks PRKDB_CLUSTER_SECRET"; fail=1; }
exit $fail
```
`chmod +x scripts/check_doc_claims.sh && scripts/check_doc_claims.sh` → exit 0.

```bash
git add README.md docs docker-compose.yml docker-compose-simple.yml scripts/check_doc_claims.sh
git commit -m "docs: mark clustering experimental and correct misleading claims"
```

---

### Task 0.10: CI job, gate workflow, pre-push script

**Files:**
- Modify: `.github/workflows/ci.yml` (add job after `ignore-discipline`)
- Create: `.github/workflows/remediation-gate.yml`
- Create: `scripts/pre-push-check.sh`

- [ ] **Step 1: CI job**

```yaml
  remediation:
    name: Remediation Ledger
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@1.98.1
      - uses: Swatinem/rust-cache@v2
      # The ledger is the only record of what is fixed. A finding cannot be marked
      # fixed without a regression test that exists and is not ignored (spec §4.2).
      - run: cargo xtask remediation check
      - run: cargo xtask remediation render --check
      - run: bash scripts/check_doc_claims.sh
```
This is an xtask-only job: no protoc needed (see Conventions).

- [ ] **Step 2: Gate workflow**

`.github/workflows/remediation-gate.yml`:
```yaml
name: Remediation Gate

# Dispatched by the maintainer against a phase PR's head ref (spec §5, step 3).
# Must exist on main to be dispatchable, which is why it ships in Phase 0.
on:
  workflow_dispatch:
    inputs:
      ref:
        description: Git ref to verify (phase PR head)
        required: true
      phase:
        description: Phase number (0-5)
        required: true

jobs:
  ledger-and-tests:
    runs-on: ubuntu-latest
    timeout-minutes: 60
    steps:
      - name: Free disk space
        run: |
          sudo rm -rf /usr/share/dotnet
          sudo rm -rf /usr/local/lib/android
          sudo rm -rf /opt/ghc
      - uses: actions/checkout@v4
        with:
          ref: ${{ inputs.ref }}
      - name: Install Protoc
        run: sudo apt-get install -y protobuf-compiler
      - uses: dtolnay/rust-toolchain@1.98.1
      - uses: Swatinem/rust-cache@v2
      - run: cargo xtask remediation check
      - run: cargo test --workspace --no-fail-fast
  # Phase 1 adds a sharded `harness` job here (Task 1.14).
```

- [ ] **Step 3: Pre-push script**

`scripts/pre-push-check.sh`:
```bash
#!/usr/bin/env bash
# Run before any push to origin (spec §5.1). Public CI should confirm, not discover.
set -euo pipefail
cd "$(dirname "$0")/.."
step() { printf '\n==> %s\n' "$*"; }
step fmt;          cargo fmt --all -- --check
step clippy;       cargo clippy --workspace --all-targets -- -D warnings
step tests
if command -v cargo-nextest >/dev/null; then cargo nextest run --workspace; else cargo test --workspace; fi
if [ -d crates/prkdb-verify ]; then step harness; cargo xtask verify --profile blocking --seeds 200 --mode durable; fi
# Never chain with `&&`: under `set -e` a failing non-final command in an `&&` list is ignored.
step ledger;        cargo xtask remediation check
step ledger-render; cargo xtask remediation render --check
step repo-status;  cargo xtask repo-status snapshot --fail-on-objective-drift
step readme-tests; cargo xtask readme-tests --check
step doc-claims;   bash scripts/check_doc_claims.sh
echo; echo "pre-push-check: all green"
```
`chmod +x scripts/pre-push-check.sh`.

- [ ] **Step 4: Run it**

Run: `scripts/pre-push-check.sh`
Expected: `pre-push-check: all green`. Fix any fmt/clippy fallout in files this phase touched.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yml .github/workflows/remediation-gate.yml scripts/pre-push-check.sh
git commit -m "ci: add remediation ledger job, gate workflow, and pre-push check"
```

---

### Task 0.10b: `repo-status` reads the ledger (spec §4.3)

**Files:**
- Create: `xtask/src/repo_status/collectors/ledger.rs`
- Modify: `xtask/src/repo_status/collectors/mod.rs`, `xtask/src/repo_status/mod.rs:237-244`

Emits a **Warning** (not Error), so the Verification dimension turns red without tripping `--fail-on-objective-drift`, which counts only Error findings and would otherwise keep CI red until Phase 2.

- [ ] **Step 1: Collector**

```rust
//! Verification dimension from the remediation ledger (spec §4.3): open critical
//! findings make it red. Warning severity: visible, never fails objective-drift CI.

use super::super::model::{Confidence, DimensionId, Evidence, Finding, Severity};
use crate::remediation::model::{Ledger, Severity as LSev, Status};
use std::path::Path;

pub(in super::super) fn collect(repo_root: &Path) -> Vec<Finding> {
    let Ok(text) = std::fs::read_to_string(repo_root.join("docs/remediation/ledger.toml")) else { return vec![] };
    let Ok(ledger) = Ledger::parse(&text) else { return vec![] };
    let open: Vec<_> = ledger
        .finding
        .iter()
        .filter(|f| f.severity == LSev::Critical && !matches!(f.status, Status::Verified | Status::WontFix | Status::Duplicate))
        .collect();
    if open.is_empty() {
        return vec![];
    }
    vec![Finding {
        id: "open-critical-remediation-findings".into(),
        dimension: DimensionId::Verification,
        severity: Severity::Warning,
        confidence: Confidence::High,
        message: format!(
            "{} critical remediation finding(s) not yet verified: {}",
            open.len(),
            open.iter().map(|f| f.id.as_str()).collect::<Vec<_>>().join(", ")
        ),
        evidence: vec![Evidence::new("docs/remediation/ledger.toml", "see docs/status/remediation.md")],
    }]
}
```
Register it: in `collectors/mod.rs` add `pub(super) mod ledger;` and in `collect_findings` add `findings.extend(ledger::collect(repo_root));`.

- [ ] **Step 2: Dimension rule** — in `build_dimension_report`, replace the unconditional Verification early return with:
```rust
    if id == DimensionId::Verification {
        let open = findings.iter().filter(|f| f.dimension == DimensionId::Verification).count();
        return DimensionReport {
            id,
            status: if open > 0 { Status::Red } else { Status::Unknown },
            confidence: if open > 0 { Confidence::High } else { Confidence::Low },
            summary: if open > 0 {
                "Open critical remediation findings; see docs/status/remediation.md.".to_owned()
            } else {
                passing_summary.to_owned()
            },
        };
    }
```

- [ ] **Step 3: Test** — add a unit test in `ledger.rs` with a tempdir ledger holding one open critical and one verified critical; assert exactly one Warning finding naming only the open id. Run `cargo test -p xtask repo_status`, then `cargo xtask repo-status snapshot --fail-on-objective-drift` (exit 0), then `cargo xtask repo-status render` and include the regenerated `docs/status/repo-status.md`.

- [ ] **Step 4: Commit** — `feat: derive repo-status verification from the remediation ledger`.

---

### Task 0.11: Phase 0 gate (maintainer + agent)

- [ ] **Step 1 (maintainer):** create a **private repository** (not a fork), disable Actions on it, then `git remote add backup <url> && git push backup remediation/root-cause`.
- [ ] **Step 2:** `scripts/pre-push-check.sh` green.
- [ ] **Step 3 (maintainer approves the push):** `git push -u origin remediation/root-cause`, then open PR `remediation/root-cause → main` titled `Phase 0: honesty, tracking, SCH-01`. PR body lists ledger changes and ends with a test plan checklist.
- [ ] **Step 4:** wait for CI green. Record the run URL as `ci_evidence` on each Phase 0 finding (SCH-01, DOC-01, DOC-06, DOC-09, DOC-11), set them `verified`, set phase 0 `status = "gate_passed"` with `gate_evidence = ["<run URL>"]`, render, commit (no `[skip ci]`), push.
- [ ] **Step 5 (maintainer):** merge with admin override.

---

# Phase 1 — Harness and baseline

### Task 1.1: nextest

**Files:**
- Create: `.config/nextest.toml`
- Modify: `.github/workflows/ci.yml` (`test` job)

- [ ] **Step 1: Config**

```toml
[profile.default]
slow-timeout = { period = "60s", terminate-after = 5 }

[profile.ci]
retries = 0            # flaky tests are findings; a retry would turn a flake green
failure-output = "immediate-final"
fail-fast = false

[test-groups]
serial-servers = { max-threads = 1 }

[[profile.default.overrides]]
filter = "test(/_tripwire$/) | binary(raft_chaos_tests) | binary(in_process_cluster)"
test-group = "serial-servers"
```

- [ ] **Step 2: Run locally**

Run: `cargo install cargo-nextest --locked && cargo nextest run --workspace`
Expected: same pass count as `cargo test` (doctests excluded — keep a separate `cargo test --doc --workspace` step in CI).

- [ ] **Step 3: CI**

In the `test` job replace `cargo test --workspace` with:
```yaml
      - uses: taiki-e/install-action@nextest
      - run: cargo nextest run --workspace --profile ci
      - run: cargo test --doc --workspace
```

- [ ] **Step 4: Commit** — `ci: run tests with cargo-nextest`

---

### Task 1.2: `Vfs` trait and `StdVfs`

> **As built (review-hardened, commits b112104 + 33e419e):** the real API differs from the code below — `fn exists(&self, path: &Path) -> io::Result<bool>` (never hides I/O errors), `fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn VfsFile>>` with `OpenMode::{Read, ReadWrite}`, and every method documents its durability contract. `crates/prkdb-core/src/vfs/` is the source of truth; later tasks must implement/consume that API, not the snippet below. The nextest group in Task 1.1 was also widened and renamed `serial-servers` (all prkdb-cli test binaries + every `mod helpers` binary + tripwires).

**Files:**
- Create: `crates/prkdb-core/src/vfs/mod.rs`, `crates/prkdb-core/src/vfs/std_vfs.rs`
- Modify: `crates/prkdb-core/src/lib.rs` (`pub mod vfs;`)

- [ ] **Step 1: Write the trait and tests**

`vfs/mod.rs`:
```rust
//! Synchronous filesystem seam (spec §7 Phase 1). Production uses `StdVfs`; the
//! harness uses a fault-injecting implementation to model power loss. The WAL is
//! routed through this trait in Phase 2a.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod std_vfs;
pub use std_vfs::StdVfs;

pub trait VfsFile: Send + Sync {
    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()>;
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;
    fn set_len(&self, len: u64) -> io::Result<()>;
    fn len(&self) -> io::Result<u64>;
    fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }
    /// Durably persist file contents written so far (fdatasync).
    fn sync_data(&self) -> io::Result<()>;
}

pub trait Vfs: Send + Sync {
    fn open(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>>;
    /// Create or truncate. The new directory entry is not durable until `sync_dir`.
    fn create(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>>;
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    fn remove(&self, path: &Path) -> io::Result<()>;
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>>;
    fn exists(&self, path: &Path) -> bool;
    /// Durably persist directory entries (creates, renames, removes) in `dir`.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;
}

/// Shared conformance tests; every `Vfs` implementation must pass them.
#[cfg(any(test, feature = "vfs-conformance"))]
pub mod conformance {
    use super::*;

    pub fn run(vfs: &dyn Vfs, root: &Path) {
        let dir = root.join("d");
        vfs.create_dir_all(&dir).unwrap();
        let p = dir.join("a.log");
        let f = vfs.create(&p).unwrap();
        f.write_at(0, b"hello").unwrap();
        f.write_at(5, b" world").unwrap();
        f.sync_data().unwrap();
        vfs.sync_dir(&dir).unwrap();
        let mut buf = [0u8; 11];
        assert_eq!(vfs.open(&p).unwrap().read_at(0, &mut buf).unwrap(), 11);
        assert_eq!(&buf, b"hello world");
        f.set_len(5).unwrap();
        assert_eq!(f.len().unwrap(), 5);
        let q = dir.join("b.log");
        vfs.rename(&p, &q).unwrap();
        assert!(!vfs.exists(&p) && vfs.exists(&q));
        assert_eq!(vfs.read_dir(&dir).unwrap(), vec![q.clone()]);
        vfs.remove(&q).unwrap();
        assert!(!vfs.exists(&q));
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn std_vfs_conformance() {
        let tmp = tempfile::tempdir().unwrap();
        super::conformance::run(&super::StdVfs, tmp.path());
    }
}
```

`vfs/std_vfs.rs`:
```rust
use super::{Vfs, VfsFile};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Default, Clone, Copy)]
pub struct StdVfs;

struct StdFile(File);

impl VfsFile for StdFile {
    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.0.write_all_at(buf, offset)
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            let mut done = 0;
            while done < buf.len() {
                done += self.0.seek_write(&buf[done..], offset + done as u64)?;
            }
            Ok(())
        }
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            let mut n = 0;
            while n < buf.len() {
                let r = self.0.read_at(&mut buf[n..], offset + n as u64)?;
                if r == 0 {
                    break;
                }
                n += r;
            }
            Ok(n)
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            self.0.seek_read(buf, offset)
        }
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.0.set_len(len)
    }
    fn len(&self) -> io::Result<u64> {
        Ok(self.0.metadata()?.len())
    }
    fn sync_data(&self) -> io::Result<()> {
        self.0.sync_data()
    }
}

impl Vfs for StdVfs {
    fn open(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>> {
        Ok(Arc::new(StdFile(OpenOptions::new().read(true).write(true).open(path)?)))
    }
    fn create(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>> {
        Ok(Arc::new(StdFile(
            OpenOptions::new().read(true).write(true).create(true).truncate(true).open(path)?,
        )))
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }
    fn remove(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }
    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let mut v: Vec<PathBuf> = fs::read_dir(path)?.map(|e| e.map(|e| e.path())).collect::<Result<_, _>>()?;
        v.sort();
        Ok(v)
    }
    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        #[cfg(unix)]
        {
            File::open(dir)?.sync_all()
        }
        #[cfg(not(unix))]
        {
            let _ = dir;
            Ok(())
        }
    }
}
```

Add to `crates/prkdb-core/Cargo.toml` `[features]`: `vfs-conformance = []`.

- [ ] **Step 2: Run** — `cargo nextest run -p prkdb-core vfs` → 1 passed.
- [ ] **Step 3: Commit** — `feat: add synchronous Vfs seam with StdVfs`

---

### Task 1.3: `prkdb-verify` crate and `faultfs`

> **API note:** implement the as-built `Vfs` (see Task 1.2 note): `exists` returns `io::Result<bool>`, `open` takes `OpenMode` (writes through a `Read` handle must return an error), and `FaultFs` must honour exactly the durability rules documented on each trait method.

**Files:**
- Create: `crates/prkdb-verify/Cargo.toml`, `crates/prkdb-verify/src/lib.rs`, `crates/prkdb-verify/src/faultfs.rs`
- Modify: root `Cargo.toml` (`members += "crates/prkdb-verify"`)

- [ ] **Step 1: Crate manifest**

```toml
[package]
name = "prkdb-verify"
version.workspace = true
edition.workspace = true
license.workspace = true
publish = false
description = "Model-based crash/restart verification harness for PrkDB (not published)"

[dependencies]
prkdb = { workspace = true }
prkdb-core = { workspace = true, features = ["vfs-conformance"] }
prkdb-types = { workspace = true }
tokio = { workspace = true }
anyhow = { workspace = true }
rand = "0.8"
rand_chacha = "0.3"
tempfile = "3"
parking_lot = "0.12"
```

`src/lib.rs`:
```rust
//! Model-based crash/restart harness (spec §7 Phase 1).
pub mod checker;
pub mod faultfs;
pub mod model;
pub mod ops;
pub mod runner;
pub mod sut;
```
(Create empty `checker.rs`, `model.rs`, `ops.rs`, `runner.rs`, `sut.rs` so it compiles; each later task fills one.)

- [ ] **Step 2: Write `faultfs.rs` with tests**

```rust
//! In-memory `Vfs` that separates written from synced state, so `power_loss`
//! can discard everything not yet made durable (spec §7 Phase 1).

use parking_lot::Mutex;
use prkdb_core::vfs::{Vfs, VfsFile};
use rand::Rng;
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Default, Clone)]
struct Content {
    written: Vec<u8>,
    synced: Vec<u8>,
}

#[derive(Default)]
struct State {
    /// inode -> content
    inodes: BTreeMap<u64, Content>,
    /// live directory entries: path -> inode
    live: BTreeMap<PathBuf, u64>,
    /// durable directory entries per directory (as of last sync_dir)
    durable: BTreeMap<PathBuf, BTreeMap<PathBuf, u64>>,
    dirs: BTreeSet<PathBuf>,
    next_inode: u64,
}

#[derive(Clone, Default)]
pub struct FaultFs {
    state: Arc<Mutex<State>>,
}

struct FaultFile {
    state: Arc<Mutex<State>>,
    inode: u64,
}

fn parent(p: &Path) -> PathBuf {
    p.parent().map(Path::to_path_buf).unwrap_or_default()
}

fn not_found(p: &Path) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, p.display().to_string())
}

impl FaultFs {
    pub fn new() -> Self {
        Self::default()
    }

    /// Simulate power loss: unsynced bytes and unsynced directory entries vanish.
    /// With `tear`, a random prefix of the unsynced tail of each file survives.
    pub fn power_loss(&self, rng: &mut impl Rng, tear: bool) {
        let mut s = self.state.lock();
        let mut live = BTreeMap::new();
        for entries in s.durable.values() {
            live.extend(entries.iter().map(|(p, i)| (p.clone(), *i)));
        }
        s.live = live;
        for c in s.inodes.values_mut() {
            let mut next = c.synced.clone();
            if tear && c.written.len() > c.synced.len() && c.written.starts_with(&c.synced) {
                let extra = rng.gen_range(0..=c.written.len() - c.synced.len());
                next.extend_from_slice(&c.written[c.synced.len()..c.synced.len() + extra]);
            }
            c.written = next;
        }
    }
}

impl VfsFile for FaultFile {
    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        let mut s = self.state.lock();
        let c = s.inodes.get_mut(&self.inode).expect("inode");
        let end = offset as usize + buf.len();
        if c.written.len() < end {
            c.written.resize(end, 0);
        }
        c.written[offset as usize..end].copy_from_slice(buf);
        Ok(())
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let s = self.state.lock();
        let c = &s.inodes[&self.inode];
        let start = (offset as usize).min(c.written.len());
        let n = buf.len().min(c.written.len() - start);
        buf[..n].copy_from_slice(&c.written[start..start + n]);
        Ok(n)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.state.lock().inodes.get_mut(&self.inode).unwrap().written.resize(len as usize, 0);
        Ok(())
    }
    fn len(&self) -> io::Result<u64> {
        Ok(self.state.lock().inodes[&self.inode].written.len() as u64)
    }
    fn sync_data(&self) -> io::Result<()> {
        let mut s = self.state.lock();
        let c = s.inodes.get_mut(&self.inode).unwrap();
        c.synced = c.written.clone();
        Ok(())
    }
}

impl Vfs for FaultFs {
    fn open(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>> {
        let inode = *self.state.lock().live.get(path).ok_or_else(|| not_found(path))?;
        Ok(Arc::new(FaultFile { state: self.state.clone(), inode }))
    }
    fn create(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>> {
        let mut s = self.state.lock();
        if !s.dirs.contains(&parent(path)) {
            return Err(not_found(&parent(path)));
        }
        let inode = s.next_inode;
        s.next_inode += 1;
        s.inodes.insert(inode, Content::default());
        s.live.insert(path.to_path_buf(), inode);
        Ok(Arc::new(FaultFile { state: self.state.clone(), inode }))
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut s = self.state.lock();
        let inode = s.live.remove(from).ok_or_else(|| not_found(from))?;
        s.live.insert(to.to_path_buf(), inode);
        Ok(())
    }
    fn remove(&self, path: &Path) -> io::Result<()> {
        self.state.lock().live.remove(path).map(|_| ()).ok_or_else(|| not_found(path))
    }
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        let mut s = self.state.lock();
        for a in path.ancestors() {
            s.dirs.insert(a.to_path_buf());
        }
        Ok(())
    }
    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let s = self.state.lock();
        Ok(s.live.keys().filter(|p| parent(p) == path).cloned().collect())
    }
    fn exists(&self, path: &Path) -> bool {
        let s = self.state.lock();
        s.live.contains_key(path) || s.dirs.contains(path)
    }
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        let mut s = self.state.lock();
        let entries: BTreeMap<PathBuf, u64> =
            s.live.iter().filter(|(p, _)| parent(p) == dir).map(|(p, i)| (p.clone(), *i)).collect();
        s.durable.insert(dir.to_path_buf(), entries);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    #[test]
    fn conformance() {
        prkdb_core::vfs::conformance::run(&FaultFs::new(), Path::new("/r"));
    }

    #[test]
    fn power_loss_drops_unsynced_bytes() {
        let fs = FaultFs::new();
        fs.create_dir_all(Path::new("/d")).unwrap();
        let f = fs.create(Path::new("/d/a")).unwrap();
        fs.sync_dir(Path::new("/d")).unwrap();
        f.write_at(0, b"durable").unwrap();
        f.sync_data().unwrap();
        f.write_at(7, b"-lost").unwrap();
        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(1), false);
        assert_eq!(fs.open(Path::new("/d/a")).unwrap().len().unwrap(), 7);
    }

    #[test]
    fn power_loss_drops_unsynced_directory_entries() {
        let fs = FaultFs::new();
        fs.create_dir_all(Path::new("/d")).unwrap();
        let f = fs.create(Path::new("/d/new")).unwrap();
        f.write_at(0, b"x").unwrap();
        f.sync_data().unwrap(); // data synced, but directory entry never was
        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(1), false);
        assert!(!fs.exists(Path::new("/d/new")));
    }

    #[test]
    fn torn_write_keeps_a_prefix() {
        let fs = FaultFs::new();
        fs.create_dir_all(Path::new("/d")).unwrap();
        let f = fs.create(Path::new("/d/a")).unwrap();
        fs.sync_dir(Path::new("/d")).unwrap();
        f.write_at(0, b"0123456789").unwrap();
        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(7), true);
        let len = fs.open(Path::new("/d/a")).unwrap().len().unwrap();
        assert!(len <= 10);
    }
}
```

- [ ] **Step 3: Run** — `cargo nextest run -p prkdb-verify faultfs` → 4 passed.
- [ ] **Step 4: Commit** — `feat: add prkdb-verify crate with fault-injecting Vfs`

---

### Task 1.4: Reference model

**Files:** `crates/prkdb-verify/src/model.rs`

- [ ] **Step 1: Write the model and tests**

```rust
//! Pure reference model: what the database must contain after any sequence of
//! acknowledged operations. No I/O.

use std::collections::BTreeMap;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Model {
    pub kv: BTreeMap<Key, Value>,
}

impl Model {
    pub fn put(&mut self, k: Key, v: Value) {
        self.kv.insert(k, v);
    }
    pub fn delete(&mut self, k: &Key) {
        self.kv.remove(k);
    }
    pub fn get(&self, k: &Key) -> Option<&Value> {
        self.kv.get(k)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_overwrite_delete() {
        let mut m = Model::default();
        m.put(b"a".to_vec(), b"1".to_vec());
        m.put(b"a".to_vec(), b"2".to_vec());
        assert_eq!(m.get(&b"a".to_vec()), Some(&b"2".to_vec()));
        m.delete(&b"a".to_vec());
        assert!(m.kv.is_empty());
    }
}
```
Event log, consumer offsets, and TTL clock are added to the model in the tasks that enable those ops (Phase 2e and Phase 3), not now (YAGNI).

- [ ] **Step 2: Run and commit** — `cargo nextest run -p prkdb-verify model`; commit `feat: add harness reference model`.

---

### Task 1.5: Ops, profiles, seeded generator

**Files:** `crates/prkdb-verify/src/ops.rs`

- [ ] **Step 1: Write**

```rust
//! Operation vocabulary and profiles (spec §7.1). The generator draws only ops
//! enabled by the selected profile. Seeds are stable: ChaCha8 is portable.

use crate::model::{Key, Value};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Put(Key, Value),
    Delete(Key),
    /// Clean close (flush) and reopen.
    Reopen,
    /// Drop the adapter without flushing, then reopen.
    Crash,
    Checkpoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    /// Phase 1 blocking profile.
    Blocking,
    /// Everything implemented so far; failures are findings, not gates.
    Discovery,
}

impl Profile {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "blocking" => Some(Self::Blocking),
            "discovery" => Some(Self::Discovery),
            _ => None,
        }
    }
}

pub fn generate(seed: u64, len: usize, profile: Profile) -> Vec<Op> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let keys = 16u8; // small key space so overwrites and deletes collide
    (0..len)
        .map(|i| {
            let k = vec![b'k', rng.gen_range(0..keys)];
            let roll = rng.gen_range(0..100);
            match (profile, roll) {
                (_, 0..=59) => Op::Put(k, format!("v{seed}-{i}").into_bytes()),
                (_, 60..=79) => Op::Delete(k),
                (_, 80..=89) => Op::Reopen,
                (Profile::Discovery, 90..=94) => Op::Checkpoint,
                _ => Op::Crash,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_ops() {
        assert_eq!(generate(42, 100, Profile::Blocking), generate(42, 100, Profile::Blocking));
    }

    #[test]
    fn blocking_never_checkpoints() {
        for seed in 0..50 {
            assert!(!generate(seed, 200, Profile::Blocking).contains(&Op::Checkpoint));
        }
    }

    #[test]
    fn discovery_does_checkpoint() {
        assert!((0..20).any(|s| generate(s, 200, Profile::Discovery).contains(&Op::Checkpoint)));
    }
}
```

- [ ] **Step 2: Run and commit** — 3 passed; `feat: add harness ops and profiles`.

---

### Task 1.6: SUT driver

**Files:** `crates/prkdb-verify/src/sut.rs`

- [ ] **Step 1: Write**

```rust
//! Drives the real embedded storage through its public API.

use crate::model::{Key, Value};
use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use prkdb_types::storage::StorageAdapter;
use std::path::PathBuf;

#[async_trait::async_trait]
pub trait Sut: Send {
    async fn put(&mut self, k: &Key, v: &Value) -> anyhow::Result<()>;
    async fn delete(&mut self, k: &Key) -> anyhow::Result<()>;
    async fn get(&mut self, k: &Key) -> anyhow::Result<Option<Value>>;
    async fn reopen(&mut self) -> anyhow::Result<()>;
    async fn crash(&mut self) -> anyhow::Result<()>;
    async fn checkpoint(&mut self) -> anyhow::Result<()>;
}

pub struct WalSut {
    dir: PathBuf,
    _tmp: tempfile::TempDir,
    db: Option<WalStorageAdapter>,
}

fn config(dir: &std::path::Path) -> WalConfig {
    WalConfig { log_dir: dir.to_path_buf(), ..WalConfig::test_config() }
}

impl WalSut {
    pub async fn new() -> anyhow::Result<Self> {
        let tmp = tempfile::tempdir()?;
        let dir = tmp.path().to_path_buf();
        let db = WalStorageAdapter::new(config(&dir))?;
        Ok(Self { dir, _tmp: tmp, db: Some(db) })
    }
    fn db(&self) -> &WalStorageAdapter {
        self.db.as_ref().expect("open")
    }
    async fn open(&mut self) -> anyhow::Result<()> {
        self.db = Some(WalStorageAdapter::open_async(config(&self.dir)).await?);
        Ok(())
    }
}

#[async_trait::async_trait]
impl Sut for WalSut {
    async fn put(&mut self, k: &Key, v: &Value) -> anyhow::Result<()> {
        Ok(self.db().put(k, v).await?)
    }
    async fn delete(&mut self, k: &Key) -> anyhow::Result<()> {
        Ok(self.db().delete(k).await?)
    }
    async fn get(&mut self, k: &Key) -> anyhow::Result<Option<Value>> {
        Ok(self.db().get(k).await?)
    }
    async fn reopen(&mut self) -> anyhow::Result<()> {
        self.db().flush().await?;
        self.db = None;
        self.open().await
    }
    async fn crash(&mut self) -> anyhow::Result<()> {
        // In-process "crash": drop without an explicit flush. NOTE: today's
        // WalStorageAdapter::drop runs flush_on_last_handle_drop, so this behaves
        // like a clean reopen. Real crash coverage comes from the SIGKILL test
        // (Task 1.10) and PowerLoss (Task 2.5); do not read a green run as more.
        self.db = None;
        self.open().await
    }
    async fn checkpoint(&mut self) -> anyhow::Result<()> {
        self.db().flush().await?;
        Ok(self.db().save_checkpoint()?)
    }
}
```
Add `async-trait = { workspace = true }` to the crate manifest.

- [ ] **Step 2: Build** — `cargo build -p prkdb-verify`.
- [ ] **Step 3: Commit** — `feat: add WAL SUT driver for harness`.

---

### Task 1.7: Checker and runner

**Files:** `crates/prkdb-verify/src/checker.rs`, `crates/prkdb-verify/src/runner.rs`

- [ ] **Step 1: Checker**

```rust
//! Durable-mode check: after a restart, every key's value equals the model's.

use crate::model::Model;
use crate::sut::Sut;

#[derive(Debug)]
pub struct Mismatch {
    pub key: Vec<u8>,
    pub expected: Option<Vec<u8>>,
    pub actual: Option<Vec<u8>>,
}

/// Returns (number of keys compared, first mismatch).
pub async fn check_durable(model: &Model, sut: &mut dyn Sut, key_space: u8) -> anyhow::Result<(usize, Option<Mismatch>)> {
    let mut compared = 0;
    for b in 0..key_space {
        let key = vec![b'k', b];
        let expected = model.get(&key).cloned();
        let actual = sut.get(&key).await?;
        compared += 1;
        if expected != actual {
            return Ok((compared, Some(Mismatch { key, expected, actual })));
        }
    }
    Ok((compared, None))
}
```

- [ ] **Step 2: Runner with minimizer**

```rust
//! Runs op sequences against a fresh SUT, checks after every restart op, and
//! shrinks failing sequences.

use crate::checker::{check_durable, Mismatch};
use crate::model::Model;
use crate::ops::{generate, Op, Profile};
use crate::sut::Sut;
use std::future::Future;

pub const KEY_SPACE: u8 = 16;

#[derive(Debug)]
pub struct Failure {
    pub seed: u64,
    pub ops: Vec<Op>,
    pub mismatch: Mismatch,
}

#[derive(Debug, Default)]
pub struct Report {
    pub seeds: u64,
    pub checks: usize,
    pub failure: Option<Failure>,
}

/// Returns Ok(checks) or the mismatch.
pub async fn run_ops(sut: &mut dyn Sut, ops: &[Op]) -> anyhow::Result<Result<usize, Mismatch>> {
    let mut model = Model::default();
    let mut checks = 0;
    for op in ops {
        match op {
            Op::Put(k, v) => {
                sut.put(k, v).await?;
                model.put(k.clone(), v.clone());
            }
            Op::Delete(k) => {
                sut.delete(k).await?;
                model.delete(k);
            }
            Op::Checkpoint => sut.checkpoint().await?,
            Op::Reopen | Op::Crash => {
                if matches!(op, Op::Reopen) { sut.reopen().await? } else { sut.crash().await? }
                let (n, m) = check_durable(&model, sut, KEY_SPACE).await?;
                checks += n;
                if let Some(m) = m {
                    return Ok(Err(m));
                }
            }
        }
    }
    // Final check after a clean reopen, so every sequence verifies at least once.
    sut.reopen().await?;
    let (n, m) = check_durable(&model, sut, KEY_SPACE).await?;
    Ok(match m {
        Some(m) => Err(m),
        None => Ok(checks + n),
    })
}

pub async fn run_seeds<F, Fut, S>(make: F, first_seed: u64, seeds: u64, len: usize, profile: Profile) -> anyhow::Result<Report>
where
    F: Fn() -> Fut,
    Fut: Future<Output = anyhow::Result<S>>,
    S: Sut,
{
    let mut report = Report::default();
    for seed in first_seed..first_seed + seeds {
        let ops = generate(seed, len, profile);
        let mut sut = make().await?;
        report.seeds += 1;
        match run_ops(&mut sut, &ops).await? {
            Ok(n) => report.checks += n,
            Err(_) => {
                let (ops, mismatch) = minimize(&make, ops).await?;
                report.failure = Some(Failure { seed, ops, mismatch });
                return Ok(report);
            }
        }
    }
    Ok(report)
}

/// Greedy one-at-a-time deletion: keep removing ops while the failure persists.
async fn minimize<F, Fut, S>(make: &F, mut ops: Vec<Op>) -> anyhow::Result<(Vec<Op>, Mismatch)>
where
    F: Fn() -> Fut,
    Fut: Future<Output = anyhow::Result<S>>,
    S: Sut,
{
    let mut i = 0;
    while i < ops.len() {
        let mut candidate = ops.clone();
        candidate.remove(i);
        let mut sut = make().await?;
        if run_ops(&mut sut, &candidate).await?.is_err() {
            ops = candidate;
        } else {
            i += 1;
        }
    }
    let mut sut = make().await?;
    let mismatch = run_ops(&mut sut, &ops).await?.expect_err("minimized sequence still fails");
    Ok((ops, mismatch))
}
```

- [ ] **Step 3: Build and commit** — `cargo build -p prkdb-verify`; `feat: add harness checker, runner, and minimizer`.

---

### Task 1.8: Meta-test, blocking-profile test, discovery reproduces STO-01

**Files:**
- Create: `crates/prkdb-verify/tests/harness.rs`

- [ ] **Step 1: Write the tests**

```rust
use prkdb_verify::model::{Key, Value};
use prkdb_verify::ops::Profile;
use prkdb_verify::runner::run_seeds;
use prkdb_verify::sut::{Sut, WalSut};

/// Wraps a real SUT and silently drops every 10th put. The harness must catch it,
/// otherwise a green run means nothing.
struct Lossy {
    inner: WalSut,
    puts: u64,
}

#[async_trait::async_trait]
impl Sut for Lossy {
    async fn put(&mut self, k: &Key, v: &Value) -> anyhow::Result<()> {
        self.puts += 1;
        if self.puts % 10 == 0 { Ok(()) } else { self.inner.put(k, v).await }
    }
    async fn delete(&mut self, k: &Key) -> anyhow::Result<()> { self.inner.delete(k).await }
    async fn get(&mut self, k: &Key) -> anyhow::Result<Option<Value>> { self.inner.get(k).await }
    async fn reopen(&mut self) -> anyhow::Result<()> { self.inner.reopen().await }
    async fn crash(&mut self) -> anyhow::Result<()> { self.inner.crash().await }
    async fn checkpoint(&mut self) -> anyhow::Result<()> { self.inner.checkpoint().await }
}

#[tokio::test(flavor = "multi_thread")]
async fn meta_harness_catches_a_lossy_sut() {
    let r = run_seeds(|| async { Ok(Lossy { inner: WalSut::new().await?, puts: 0 }) }, 0, 20, 60, Profile::Blocking)
        .await
        .unwrap();
    assert!(r.failure.is_some(), "harness failed to detect dropped writes");
}

#[tokio::test(flavor = "multi_thread")]
async fn blocking_profile_is_green_on_current_code() {
    let r = run_seeds(WalSut::new, 0, 20, 60, Profile::Blocking).await.unwrap();
    assert!(r.checks > 0, "vacuous run: nothing compared");
    assert!(r.failure.is_none(), "blocking profile failed: {:?}", r.failure);
}

/// STO-01 via the harness: tripwire until Phase 2d fixes checkpoint recovery.
#[tokio::test(flavor = "multi_thread")]
async fn sto01_discovery_profile_finds_checkpoint_loss_tripwire() {
    let r = run_seeds(WalSut::new, 0, 50, 80, Profile::Discovery).await.unwrap();
    let f = r.failure.expect("STO-01 appears fixed: invert this tripwire");
    assert!(f.ops.iter().any(|o| matches!(o, prkdb_verify::ops::Op::Checkpoint)), "minimized failure: {:?}", f.ops);
}
```

- [ ] **Step 2: Run**

Run: `cargo nextest run -p prkdb-verify --test harness`
Expected: 3 passed. **If `blocking_profile_is_green_on_current_code` fails**, apply the demotion rule (spec Phase 1): record the minimized failing sequence, identify the finding (likely STO-07 or STO-03), add that op combination to `Profile::Discovery` only, add a tripwire for the finding, and re-run. Report to the maintainer before continuing; it's a new or confirmed finding.

- [ ] **Step 3: Ledger** — this task fixes TST-03. Set TST-03 `status = "fixed"`, `regression_tests = ["test:crates/prkdb-verify/tests/harness.rs::meta_harness_catches_a_lossy_sut", "test:crates/prkdb-verify/tests/harness.rs::blocking_profile_is_green_on_current_code"]`. Add the harness STO-01 tripwire as a second evidence note in STO-01's `evidence` array (the `tripwire` field keeps the direct test from Task 0.8).

- [ ] **Step 4: Commit** — `test: prove the harness can fail and reproduces STO-01`.

---

### Task 1.9: Harness binary and `cargo xtask verify`

**Files:**
- Create: `crates/prkdb-verify/src/bin/verify.rs`
- Create: `xtask/src/verify.rs`
- Modify: `xtask/src/main.rs` (no new xtask dependencies — see Conventions)

- [ ] **Step 1: Harness binary** — `crates/prkdb-verify/src/bin/verify.rs`:

```rust
//! verify [--profile blocking|discovery] [--seed N] [--seed-offset N] [--seeds K] [--ops M] [--mode durable]

use anyhow::{bail, Result};
use prkdb_verify::ops::Profile;
use prkdb_verify::runner::run_seeds;
use prkdb_verify::sut::WalSut;

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut profile = Profile::Blocking;
    let (mut first, mut seeds, mut ops) = (0u64, 200u64, 80usize);
    let mut it = args.iter();
    while let Some(a) = it.next() {
        let mut val = || it.next().cloned().ok_or_else(|| anyhow::anyhow!("{a} needs a value"));
        match a.as_str() {
            "--profile" => profile = Profile::parse(&val()?).ok_or_else(|| anyhow::anyhow!("bad profile"))?,
            "--seed" => {
                first = val()?.parse()?;
                seeds = 1;
            }
            "--seed-offset" => first = val()?.parse()?,
            "--seeds" => seeds = val()?.parse()?,
            "--ops" => ops = val()?.parse()?,
            "--mode" => {
                if val()? != "durable" {
                    bail!("only durable mode exists until Phase 2a");
                }
            }
            other => bail!("unknown argument {other}"),
        }
    }
    let name = if profile == Profile::Blocking { "blocking" } else { "discovery" };
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    let report = rt.block_on(run_seeds(WalSut::new, first, seeds, ops, profile))?;
    println!("profile={name} seeds={} checks={}", report.seeds, report.checks);
    if report.checks == 0 {
        bail!("vacuous run: no checks compared");
    }
    if let Some(f) = report.failure {
        eprintln!("FAILED seed={} (replay: cargo xtask verify --profile {name} --seed {} --ops {ops})", f.seed, f.seed);
        eprintln!("minimized ops: {:#?}", f.ops);
        eprintln!("mismatch: {:?}", f.mismatch);
        bail!("harness failure");
    }
    Ok(())
}
```

- [ ] **Step 2: xtask shim** — `xtask/src/verify.rs` (xtask stays free of prkdb and protoc):

```rust
//! `cargo xtask verify ...` forwards to the prkdb-verify binary so xtask stays light.

use anyhow::{bail, Result};

pub fn run(args: &[&str]) -> Result<()> {
    let status = std::process::Command::new(env!("CARGO"))
        .args(["run", "--release", "-q", "-p", "prkdb-verify", "--bin", "verify", "--"])
        .args(args)
        .status()?;
    if !status.success() {
        bail!("verify failed ({status})");
    }
    Ok(())
}
```
In `main.rs`: `mod verify;`, arm `["verify", rest @ ..] => verify::run(rest),`, and a usage line.

- [ ] **Step 3: Run**

Run: `cargo xtask verify --seeds 50` → `profile=blocking seeds=50 checks=…`, exit 0.
Run: `cargo xtask verify --profile discovery --seeds 50` → non-zero exit with a minimized sequence containing `Checkpoint`.
Run: `cargo tree -p xtask -e normal --prefix none | grep -cE '^prkdb'` → `0`.

- [ ] **Step 4: Commit** — `feat: add harness binary and cargo xtask verify`.

---

### Task 1.10: Subprocess `SIGKILL` crash (nightly)

**Files:**
- Create: `crates/prkdb-verify/src/bin/crash_child.rs`
- Create: `crates/prkdb-verify/tests/sigkill.rs`

- [ ] **Step 1: Child binary** — writes `n` puts to `--dir`, printing `ACK <i>` to stdout after each acknowledged put, then sleeps forever:

```rust
use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use prkdb_types::storage::StorageAdapter;
use std::io::Write;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let dir = std::path::PathBuf::from(&args[1]);
    let n: u32 = args[2].parse()?;
    let db = WalStorageAdapter::new(WalConfig { log_dir: dir, ..WalConfig::test_config() })?;
    let mut out = std::io::stdout();
    for i in 0..n {
        db.put(format!("k{i}").as_bytes(), b"v").await?;
        writeln!(out, "ACK {i}")?;
        out.flush()?;
    }
    std::thread::sleep(std::time::Duration::from_secs(3600));
    Ok(())
}
```

- [ ] **Step 2: Test** — spawns the child (`env!("CARGO_BIN_EXE_crash_child")`), reads stdout until `ACK 199`, sends `SIGKILL` (`child.kill()` sends SIGKILL on Unix), reopens with `open_async`, asserts `k0..k199` all present. It takes a few seconds, so it runs on every PR (not `#[ignore]`d); add `| binary(sigkill)` to the nextest `serial-servers` filter. Gate the file with `#![cfg(unix)]`.

```rust
#[tokio::test(flavor = "multi_thread")]
async fn acknowledged_writes_survive_sigkill() {
    use std::io::{BufRead, BufReader};
    let dir = tempfile::tempdir().unwrap();
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_crash_child"))
        .args([dir.path().to_str().unwrap(), "200"])
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let reader = BufReader::new(child.stdout.take().unwrap());
    for line in reader.lines() {
        if line.unwrap() == "ACK 199" { break; }
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let db = prkdb::storage::WalStorageAdapter::open_async(prkdb_core::wal::WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..prkdb_core::wal::WalConfig::test_config()
    })
    .await
    .unwrap();
    use prkdb_types::storage::StorageAdapter;
    for i in 0..200 {
        assert!(db.get(format!("k{i}").as_bytes()).await.unwrap().is_some(), "k{i} lost after SIGKILL");
    }
}
```

- [ ] **Step 3: Run it five times** — `for i in 1 2 3 4 5; do cargo nextest run -p prkdb-verify --test sigkill || break; done`. If it fails **every** time: rename it `sto07_acknowledged_writes_lost_on_sigkill_tripwire`, invert the assertion (at least one key missing), set it as STO-07's `tripwire`, and tell the maintainer. If it fails **intermittently**: a tripwire would flake, so instead make the test return early unless `PRKDB_DISCOVERY=1` is set, run it in the nightly discovery step, record the observed loss rate in STO-07's `evidence`, and tell the maintainer. Never use `#[ignore]` for this (ledger invariant 4).
- [ ] **Step 4: Commit** — `test: add SIGKILL crash check for acknowledged writes`.

---

### Task 1.11: TST-01 moves to Phase 4 (spec amendment)

The chaos monkey loses a *random* amount of data, so before Raft is fixed it can be neither a deterministic tripwire nor a zero-loss test. It is also `#[ignore]`d ("needs a built prkdb-server binary"), which ledger invariant 4 forbids for tripwires, and `chaos-tests.yml:53` selects it by name.

The spec amendment is already applied (spec revision 5). If Task 0.5 transcribed TST-01 with `phase = 1`:

- [ ] **Step 1:** In `ledger.toml`, set TST-01 `phase = 4`; run `cargo xtask remediation check`.
- [ ] **Step 2:** Commit — `docs: record TST-01 under phase 4 in the ledger`.

(Phase 4 changes `raft_chaos_tests.rs:~1070` from `assert!(verification_rate >= 0.8, …)` to `assert_eq!(missing, 0, …)` — `missing` is a counter — and records `ci-job:chaos-tests.yml/raft-chaos-tests` as TST-01's regression evidence.)

---

### Task 1.12: TST-04 and the performance baseline

**Files:**
- Modify: `crates/prkdb/Cargo.toml` (`[[bench]]` entry)
- Create: `scripts/capture_baseline.sh`, `docs/benchmarks/baseline-2026-09.toml`

- [ ] **Step 1: TST-04**

```toml
[[bench]]
name = "e2e_throughput_bench"
harness = false
```
Run: `cargo bench -p prkdb --bench e2e_throughput_bench -- --warm-up-time 1 --measurement-time 3` → Criterion output appears (it did not before). Regression evidence: `ci-job:ci.yml/benchmark` if that job runs it; otherwise add a `script:scripts/check_bench_harness.sh` that greps for the entry.

- [ ] **Step 2: Baseline script**

`scripts/capture_baseline.sh` — checks out nothing (runs on the current tree), runs the §6.1 benches, and writes the TOML:
```bash
#!/usr/bin/env bash
# Captures §6.1 metrics for the current tree into $1 (TOML). Wall-clock numbers from the
# maintainer machine; record machine details so later comparisons are like-for-like.
set -euo pipefail
cd "$(dirname "$0")/.."
out=${1:?usage: capture_baseline.sh <out.toml>}
{
  echo "# Generated by scripts/capture_baseline.sh on $(date -u +%F)"
  echo "commit = \"$(git rev-parse --short HEAD)\""
  echo "machine = \"$(uname -srm) / $(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo | cut -d: -f2)\""
} > "$out"
for bench in e2e_throughput_bench; do
  # --output-format bencher prints one line per benchmark
  # ("test <name> ... bench: <ns> ns/iter (+/- <ns>)"), which survives long names that
  # Criterion's default output wraps onto two lines.
  cargo bench -p prkdb --bench "$bench" -- --output-format bencher 2>/dev/null > "/tmp/$bench.log"
  echo "[$bench]" >> "$out"
  sed -nE 's/^test (.+) \.\.\. bench: +([0-9,]+) ns\/iter.*/"\1" = \2/p' "/tmp/$bench.log" | tr -d ',' >> "$out"
done
echo "wrote $out"
```
Extend the `for bench in` list with every bench that covers a §6.1 row (`ls crates/prkdb/benches crates/prkdb-core/benches` and pick: put, batch put, get/index query, recovery, consumer poll). The 1 GiB recovery and 3-node rows need new benches. Add `crates/prkdb/benches/recovery_bench.rs` (write 1 GiB with `put_batch`, flush, time `open_async`) and `crates/prkdb/benches/cluster_write_bench.rs` (reuse `tests/helpers/in_process_cluster.rs` via `#[path]`), each with a `[[bench]] harness = false` entry.

- [ ] **Step 3: Capture** — `scripts/capture_baseline.sh docs/benchmarks/baseline-2026-09.toml`; confirm every bench in the log appears with a numeric ns/iter value; commit the file.
- [ ] **Step 4: Commit** — `perf: record phase 1 benchmark baseline and fix e2e bench harness`.

---

### Task 1.13: iai-callgrind deterministic gate

**Files:**
- Create: `crates/prkdb/benches/iai_hot_paths.rs`, `[[bench]] name = "iai_hot_paths" harness = false`
- Create: `.github/workflows/perf-gate.yml`

- [ ] **Step 1:** Check the current crate name: `cargo search iai-callgrind gungraun --limit 3`. Use whichever is maintained; pin its runner version to the library version.
- [ ] **Step 2:** Bench functions for: `WalStorageAdapter::put` (1 KiB, 100 iterations, in a tempdir), `get` hit, `put_batch` of 100, `IndexedStorage::insert` with one index, `LogRecord` encode/decode.
- [ ] **Step 3:** `perf-gate.yml` runs on `pull_request` to `main` (phase PRs only), on ubuntu: `sudo apt-get install -y valgrind`, install the runner, bench the **base SHA**, then the **head SHA** in the same job, and compute per-benchmark instruction deltas (use the CI preamble plus `sudo apt-get install -y valgrind`). **Override (spec §6.2):** a regression > 5 % fails the job unless the PR's diff to `docs/remediation/ledger.toml` adds or changes a non-empty `perf_note`:
```bash
if git diff "$BASE_SHA" "$HEAD_SHA" -- docs/remediation/ledger.toml | grep -qE '^\+perf_note = ".+"'; then
  echo "regression justified by a ledger perf_note"; exit 0
fi
echo "instruction-count regression > 5% without a ledger perf_note"; exit 1
```
The job summary lists every delta either way.
- [ ] **Step 4: Commit** — `ci: add deterministic instruction-count perf gate for phase PRs`.

---

### Task 1.14: Harness in CI and gate workflow

**Files:** `.github/workflows/ci.yml`, `.github/workflows/remediation-gate.yml`

- [ ] **Step 1: Per-PR job**

```yaml
  harness:
    name: Crash/Restart Harness
    runs-on: ubuntu-latest
    # A cold cache builds a separate release profile of prkdb for the harness binary.
    timeout-minutes: 30
    steps:
      # CI preamble (plan Conventions): disk cleanup, checkout, protoc, rust 1.98.1, cache
      - run: cargo xtask verify --profile blocking --seeds 200 --mode durable
```

- [ ] **Step 2: Nightly** — time it first: `time cargo xtask verify --seeds 200` locally, extrapolate to 20k, and create a dedicated `nightly-harness` job (schedule/dispatch only, CI preamble) with `timeout-minutes` = 1.5× the estimate; shard it like the gate job if that exceeds 60 minutes. Do not add it to the 45-minute `nightly-slow-tests` job. Add a `continue-on-error: true` step `cargo xtask verify --profile discovery --seeds 2000`. Add the nightly Criterion trend (spec §6.2): run the §6.1 benches with `--output-format bencher` and publish with `benchmark-action/github-action-benchmark@v1` (`tool: cargo`, `gh-pages-branch: bench-data`, `auto-push: true`, `fail-on-alert: false`). That job needs `permissions: contents: write` because `auto-push` writes to the `bench-data` branch; ci.yml sets no permissions today.

- [ ] **Step 3: Gate workflow** — add to `remediation-gate.yml`:
```yaml
  harness:
    if: ${{ inputs.phase != '0' && inputs.phase != '5' }}
    runs-on: ubuntu-latest
    timeout-minutes: 120
    strategy:
      fail-fast: false
      matrix:
        shard: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    steps:
      # CI preamble (plan Conventions), with checkout `ref: ${{ inputs.ref }}`
      # 10 shards x 1000 seeds = the 10k-seed gate (spec §5 step 3).
      - run: cargo xtask verify --profile blocking --seeds 1000 --seed-offset $(( ${{ matrix.shard }} * 1000 ))
```
(`--seed-offset` is implemented in Task 1.9.)

- [ ] **Step 4: Commit** — `ci: run crash/restart harness per PR, nightly, and at phase gates`.

---

### Task 1.15: Phase 1 gate

- [ ] `scripts/pre-push-check.sh` green (it now runs the harness).
- [ ] Ledger: TST-03, TST-04 `fixed` with evidence (TST-01 moved to Phase 4 by Task 1.11).
- [ ] Push, open `Phase 1: harness and baseline` PR, CI green, dispatch `remediation-gate` with `phase=1`, record run URLs as `ci_evidence` / `harness` / `gate_evidence`, set `verified` / `gate_passed`, render, commit, push, maintainer merges.

---

# Phase 2 — Format v2 and single-node root fixes

Expanded to code level on 2026-09-24, after Task 2.1 decided **PROCEED** (`docs/remediation/decisions/2026-09-24-single-log-spike.md`, "the decision record" below). The decision record's §7 design notes are binding for Tasks 2.5–2.9. Where this section and the spec disagree, the spec wins; fix this section.

**Branch.** Phase 2 work happens on `remediation/phase-2` (cut from `remediation/phase-1`; merge `remediation/phase-1` in again whenever it gains commits, as was done for TST-09). The Phase 2 PR is `remediation/phase-2 → main` after Phase 1 merges.

**Phase 2 additions to the Conventions (apply to every Phase 2 task):**

- **Harness in every storage task.** Run `cargo xtask verify --profile blocking --seeds 200` before committing any task that touches `crates/prkdb-core/src/wal/`, `crates/prkdb/src/storage/`, `indexed_storage.rs`, `outbox.rs`, `consumer.rs` or `crates/prkdb-verify/`. From Task 2.10 on, also run it with `--mode fast`. Expected: `profile=blocking seeds=200 checks=<n>`, exit 0.
- **Demotion rule (spec §7 Phase 1).** If a blocking-profile op trips an unknown bug, move that op (or op combination) to `Profile::Discovery`, add a ledger finding with a tripwire, and report to the maintainer before continuing.
- **Tests that need `FaultFs` live in `crates/prkdb-verify/tests/`.** `prkdb-core` cannot dev-depend on `prkdb-verify`: `prkdb-verify` depends on `prkdb-core`, so the test build would link two copies of `prkdb-core` and `FaultFs` would implement the wrong copy's `Vfs`. `prkdb-core` tests use `StdVfs` plus small test doubles defined in the test file.
- **Durability of test data.** `WalConfig::test_config()` stays `SyncMode::Durable` (the honest default). A test that becomes slow under Durable sets `sync_mode: SyncMode::Fast` explicitly and says why in a comment. Never flip the default to make the suite faster.
- **Linux probes.** Tasks 2.3, 2.6, 2.7 and 2.8 need numbers from Linux (Valgrind and `fdatasync` do not exist or behave differently on macOS). Task 2.2 adds the dispatch path. Every probe run's URL goes into the commit body of the task that used it.
- **Ledger follow-up commits.** When a task's "changes" SHA is only known after the fix commit, record it in a separate `docs:` commit (`docs: record <ID> as fixed`). Never amend a commit whose SHA the ledger already records.
- **`git commit` runs as its own command** (hook `block-no-verify`, see Conventions). Examples below write the message with `-m`; for multi-line bodies use `git commit -F <file>`.

## Phase 2 file structure

| Path | Task | Responsibility |
|---|---|---|
| `.github/workflows/remediation-gate.yml` (modify) | 2.2, 2.10 | `probe` input + Linux probe jobs; harness in both modes |
| `scripts/wal_fast_rule.py` (create) | 2.2 | parses the WAL bench table, applies the ≤ 15 % Fast rule |
| `crates/prkdb/benches/iai_hot_paths.rs` (modify) | 2.3, 2.8 | instruction counts that include the WAL work |
| `scripts/perf_gate_floors.toml`, `scripts/check_perf_gate_floors.sh` (create); `scripts/perf_gate_deltas.py` (modify) | 2.3 | floor ratios so a vacuous measurement fails |
| `crates/prkdb-core/src/format.rs` (create) | 2.5 | the one `FORMAT_VERSION` constant |
| `crates/prkdb-core/src/wal/frame.rs` (create) | 2.5 | frame codec (length, CRC, LSN, kind, payload) |
| `crates/prkdb-core/src/wal/segment.rs` (create) | 2.5 | segment header, file names, verified scan |
| `crates/prkdb-core/src/wal/batch.rs` (create) | 2.5, 2.19, 2.20 | versioned payload of one atomic write batch |
| `crates/prkdb-core/src/wal/log.rs` (create) | 2.6, 2.7, 2.14, 2.15 | `Wal`: writer thread, group commit, `SyncMode`, roll, recovery, reads |
| `crates/prkdb-core/src/wal/config.rs` (modify) | 2.6, 2.8 | `SyncMode` and the new knobs; drop `segment_count`/`shard_count` |
| `crates/prkdb-core/tests/wal_segment.rs`, `wal_log.rs` (create) | 2.5, 2.6 | StdVfs-level WAL tests |
| `crates/prkdb-verify/tests/wal_power_loss.rs`, `power_loss.rs` (create) | 2.6, 2.8 | FaultFs power-loss tests (WAL and adapter) |
| `crates/prkdb/src/storage/wal_adapter.rs` (rewrite of internals) | 2.8 | adapter on `Wal`; publish in LSN order |
| `crates/prkdb/src/storage/checkpoint.rs` (rewrite) | 2.14 | index snapshot file |
| `crates/prkdb/src/storage/recovery.rs` (rewrite) | 2.8, 2.14 | replay (+ snapshot load) into the index |
| `crates/prkdb/src/storage/compaction.rs` (create) | 2.15 | rewrite live records, remove dead segments |
| `crates/prkdb/src/storage/format.rs`, `migrations.rs` (create) | 2.11 | `FORMAT` marker, open rules, migration registry |
| `crates/prkdb-cli/src/commands/migrate.rs` (create) | 2.11 | `prkdb-cli migrate --data-dir` |
| `crates/prkdb/src/keys.rs`, `catalog.rs` (create) | 2.12 | key codec, persisted collection catalog |
| `crates/prkdb/src/index_maintenance.rs` (create) | 2.17 | index diffing, unique checks, rebuild on first access |
| `crates/prkdb-types/src/event.rs` (create) | 2.20 | opaque `EventSeq` |
| `crates/prkdb/src/batch_accumulator.rs` (rewrite) | 2.16 | flush barrier, first-error reporting, byte-bounded admission |
| `crates/prkdb-schema/src/storage.rs`, `registry.rs` (modify) | 2.22 | fail-closed load, atomic writes, serialized versions |
| `crates/prkdb-verify/src/{model,ops,sut,checker,runner}.rs` (modify) | 2.10, 2.18, 2.21 | acceptable-prefix model, `PowerLoss`, typed/event SUTs |
| `fuzz/` (create) + `crates/prkdb-verify/src/fuzz_entry.rs` | 2.23 | cargo-fuzz targets and a stable-toolchain corpus test |
| `crates/prkdb-verify/src/golden.rs`, `tests/fixtures/format-v2/` (generated), `tests/storage_compat.rs` | 2.24 | golden data directory and compat tests |
| `scripts/check_single_wal.sh` (create) | 2.9 | STO-06 regression: one WAL implementation |

## Task map (old task-level numbers → code-level tasks)

| Code-level task | Replaces task-level | Findings closed |
|---|---|---|
| 2.1 Spike | 2.1 | — (decision: PROCEED) |
| 2.2 Linux probe dispatch path | new | — |
| 2.3 Perf gate measures the WAL | new | TST-09 |
| 2.4 `cache_capacity` honoured | new | STO-09 |
| 2.5 Frame, segment, batch codecs | part of old 2.2 + 2.3 | — (building blocks) |
| 2.6 `Wal`: writer thread, group commit, recovery | old 2.2 (core) + old 2.3 | — (building block; Linux ≤ 15 % rule) |
| 2.7 Fast-mode sync placement | new (spike risk 2) | — |
| 2.8 `WalStorageAdapter` on `Wal` | old 2.2 (switch), old 2.12 | STO-01, STO-02, STO-03, STO-04, STO-05, STO-08 |
| 2.9 Delete the other WAL implementations | old 2.4 | STO-06 |
| 2.10 `PowerLoss` and Fast mode in the harness | old 2.5 | TST-05 |
| 2.11 Format v2 marker and migrations | old 2.6 | — (D3, D4) |
| 2.12 Key codec and catalog | old 2.7 | KEY-01 |
| 2.13 Stable partitioner | old 2.8 | KEY-03 |
| 2.14 Checkpoint = index snapshot | old 2.10 | — (STO-01 already fixed in 2.8; adds the fast path back correctly) |
| 2.15 Real compaction | old 2.11 | — (STO-01 follow-through) |
| 2.16 `BatchAccumulator` flush barrier | old 2.13 | STO-07 |
| 2.17 Index maintenance and unique enforcement | old 2.14 | KEY-02 (+ KEY-04, filed in step 1) |
| 2.18 Harness: checkpoint, compaction, typed collections | new (§7.1 row "after 2d") | — |
| 2.19 Outbox in the WAL | old 2.15 | EVT-02 |
| 2.20 Event identity from the WAL | old 2.9 | EVT-01 |
| 2.21 Harness: events | new (§7.1 row "after 2e") | — |
| 2.22 Schema persistence | old 2.16 | SCH-02 |
| 2.23 Fuzz targets | old 2.17 | TST-07 |
| 2.24 Golden v2 data directory | old 2.18 | — |
| 2.25 Phase 2 baseline and gate | old 2.19 | — |

Two deliberate moves against the task-level order: STO-03 (publish order) is fixed inside 2.8, because the new adapter's publish path is written once and correctly rather than written wrong and repaired later (old task 2.12); and STO-01 is fixed in 2.8 by deleting the broken incremental recovery (full replay is always correct), with 2.14 re-adding checkpoints as index snapshots under the `recover(checkpoint, wal) == recover(∅, wal)` property. The harness profile still grows exactly as §7.1 lists: `PowerLoss` after 2a (Task 2.10), `Checkpoint`/`Compact`/multi-collection keys after 2d (Task 2.18), events after 2e (Task 2.21).

---

### Task 2.1: Single-log group-commit spike — DONE

Commits `0f25174` (`perf: add single-log group-commit WAL spike`) and `475b2bb` (`docs: record single-log WAL spike decision`). Decision: **PROCEED** with one globally ordered log per data directory, written by a dedicated group-commit writer thread through `Vfs`. Record: `docs/remediation/decisions/2026-09-24-single-log-spike.md`. Bench: `crates/prkdb/benches/wal_write_path_spike.rs` (lives in `crates/prkdb`, not `prkdb-core`, because it compares against `WalStorageAdapter`).

Carried conditions (each is a step below, not a note): Linux 1-writer re-run before the new path replaces the old one (Task 2.6 step 9 and Task 2.8 step 12); in-writer vs syncer-thread Fast sync (Task 2.7); error poisoning, byte-bounded admission and writer supervision (Task 2.6); `put_batch`/`put_many` as one WAL record (Task 2.8); STO-09 added to the ledger (`bb06c0a`, `docs: add STO-09, cache_capacity ignored by new_with_config`).

---

### Task 2.2: Linux probe dispatch path

GitHub dispatches a workflow only if a file with that name exists on the default branch, and then runs the version of the file at the dispatched ref. `remediation-gate.yml` is already on `main` (Phase 0), so a `probe` input added on the phase branch is dispatchable against that branch without touching `main`. The branch must be on `origin` for the dispatch to see it.

**Files:**
- Modify: `.github/workflows/remediation-gate.yml`
- Create: `scripts/wal_fast_rule.py`

- [ ] **Step 1: STOP — maintainer approval to push the phase branch early.** Conventions allow a push to `origin` only in a phase-gate task. Ask the maintainer: "Tasks 2.3/2.6/2.7/2.8 need Linux numbers. May I push `remediation/phase-2` to `origin` (no PR) so `remediation-gate.yml` can be dispatched with `probe=…`? Phase 2 has no security findings; its correctness findings are already public in the ledger." Do not continue past step 4 without a yes. If the answer is no, the fallback for **iteration only** (never for a decision or ledger evidence) is Docker on the maintainer machine: `docker run --rm -v "$PWD":/w -w /w rust:1.98.1 bash -c 'apt-get update && apt-get install -y valgrind protobuf-compiler && cargo bench -p prkdb --bench <bench>'`, which runs Linux arm64 under a VM; its fsync numbers are not representative.

- [ ] **Step 2: Inputs.** In `remediation-gate.yml` under `workflow_dispatch.inputs` add:

```yaml
      probe:
        description: "Linux probe to run instead of the gate (none = normal gate run)"
        required: false
        default: none
        type: choice
        options: [none, wal-bench, iai]
      base_ref:
        description: "wal-bench only: also bench this ref first, in the same job"
        required: false
        default: ""
```

Change `run-name` to `Remediation gate — phase ${{ inputs.phase }} @ ${{ inputs.ref }}${{ inputs.probe != 'none' && format(' (probe {0})', inputs.probe) || '' }}` so a probe run can never be mistaken for gate evidence. Add `if: ${{ inputs.probe == 'none' }}` to `ledger-and-tests`, and fold it into the existing condition on `harness` (`if: ${{ inputs.probe == 'none' && inputs.phase != '0' && inputs.phase != '5' }}`). In `harness-result`, add `PROBE: ${{ inputs.probe }}` to `env` and, before the existing checks, `if [ "$PROBE" != "none" ]; then echo "probe run: harness not applicable" >> "$GITHUB_STEP_SUMMARY"; exit 0; fi`.

- [ ] **Step 3: Probe jobs.** Append (CI preamble from the Conventions, with checkout `ref: ${{ needs.resolve.outputs.sha }}`, `persist-credentials: false`, `fetch-depth: 0`):

```yaml
  probe-wal-bench:
    name: Probe — WAL write path bench (Linux)
    needs: resolve
    if: ${{ inputs.probe == 'wal-bench' }}
    runs-on: ubuntu-latest
    timeout-minutes: 90
    steps:
      # CI preamble (plan Conventions)
      - name: Bench base ref (optional)
        if: ${{ inputs.base_ref != '' }}
        env:
          BASE_REF: ${{ inputs.base_ref }}
        run: |
          set -o pipefail
          git checkout "$BASE_REF"
          SPIKE_REPS=3 cargo bench -p prkdb --bench wal_write_path_spike | tee base.md
          git checkout "${{ needs.resolve.outputs.sha }}"
      - name: Bench ref
        run: |
          set -o pipefail
          SPIKE_REPS=3 cargo bench -p prkdb --bench wal_write_path_spike | tee head.md
      - name: Apply the Fast rule and publish the tables
        run: |
          {
            echo "## Host"; echo; nproc; lscpu | grep 'Model name'; df -hT . | tail -1
            if [ -f base.md ]; then echo; echo "## Base (${{ inputs.base_ref }})"; cat base.md; fi
            echo; echo "## Head"; cat head.md
            echo; echo "## Fast rule"
          } >> "$GITHUB_STEP_SUMMARY"
          if [ -f base.md ]; then
            python3 scripts/wal_fast_rule.py --base base.md --head head.md | tee -a "$GITHUB_STEP_SUMMARY"
          else
            python3 scripts/wal_fast_rule.py --head head.md | tee -a "$GITHUB_STEP_SUMMARY"
          fi

  probe-iai:
    name: Probe — instruction counts (Linux)
    needs: resolve
    if: ${{ inputs.probe == 'iai' }}
    runs-on: ubuntu-latest
    timeout-minutes: 60
    steps:
      # CI preamble (plan Conventions)
      - run: sudo apt-get install -y valgrind
      - run: cargo install gungraun-runner --version 0.19.4 --locked
      - name: Instruction counts, one thread per summary
        env:
          GUNGRAUN_COLOR: never
        run: |
          set -o pipefail
          cargo bench -p prkdb --bench iai_hot_paths -- --save-summary=json \
            --callgrind-args='--separate-threads=yes' | tee iai.log
          python3 scripts/perf_gate_deltas.py list-names target/gungraun
          python3 scripts/perf_gate_deltas.py floors target/gungraun scripts/perf_gate_floors.toml \
            | tee -a "$GITHUB_STEP_SUMMARY"
```

(The `floors` subcommand arrives in Task 2.3; until then the last command fails, which is expected for the first TST-09 diagnosis run.)

- [ ] **Step 4: `scripts/wal_fast_rule.py`.**

```python
#!/usr/bin/env python3
"""Apply the spec's 2a rule to the WAL write-path bench table (Task 2.2).

Rule (spec §7 2a, decision record §6 risk 1): the new write path in Fast mode may lose at
most 15 % put throughput against the path it replaces, per (writers, value size) cell.

Head-only mode compares cells inside one table:   `wal_fast` vs `current_mmap_wal`
(raw WAL vs raw WAL, same run). Base/head mode compares `current_adapter_put` in the head
table (new adapter) against `current_adapter_put` in the base table (old adapter), both
benched in the same job on the same runner. Exit status 1 if any cell loses more than 15 %.
"""
from __future__ import annotations

import argparse
import re
import statistics
import sys

ROW = re.compile(r"^\| (?P<cell>[a-z_]+)/(?P<w>\d+)w/(?P<v>\d+)k \| \d+ \| \d+ KiB \| (?P<ops>\d+) \|")
LIMIT = 0.85


def parse(path: str) -> dict[tuple[str, int, int], float]:
    samples: dict[tuple[str, int, int], list[float]] = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            m = ROW.match(line)
            if m:
                key = (m["cell"], int(m["w"]), int(m["v"]))
                samples.setdefault(key, []).append(float(m["ops"]))
    if not samples:
        sys.exit(f"{path}: no bench rows found; the table format changed or the bench failed")
    return {k: statistics.median(v) for k, v in samples.items()}


def compare(pairs: list[tuple[str, float, float]]) -> int:
    print("| cell | new ops/s | old ops/s | ratio | verdict |")
    print("|---|--:|--:|--:|---|")
    failed = 0
    for name, new, old in pairs:
        ratio = new / old if old else float("inf")
        ok = ratio >= LIMIT
        failed += not ok
        print(f"| {name} | {new:.0f} | {old:.0f} | {ratio:.2f} | {'ok' if ok else 'LOSS > 15 %'} |")
    return failed


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--head", required=True)
    ap.add_argument("--base")
    args = ap.parse_args()
    head = parse(args.head)
    pairs = []
    if args.base:
        base = parse(args.base)
        for (cell, w, v), old in sorted(base.items()):
            if cell == "current_adapter_put" and (cell, w, v) in head:
                pairs.append((f"adapter_put/{w}w/{v}k", head[(cell, w, v)], old))
    else:
        for (cell, w, v), new in sorted(head.items()):
            if cell == "wal_fast" and ("current_mmap_wal", w, v) in head:
                pairs.append((f"wal_fast/{w}w/{v}k", new, head[("current_mmap_wal", w, v)]))
    if not pairs:
        sys.exit("no comparable cells: expected wal_fast + current_mmap_wal, or current_adapter_put in both tables")
    failed = compare(pairs)
    print(f"\n{failed} cell(s) lose more than 15 %" if failed else "\nall cells within the 15 % rule")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 5: Self-check the parser locally** against the decision record's table: `python3 scripts/wal_fast_rule.py --head docs/remediation/decisions/2026-09-24-single-log-spike.md` → exits with "no comparable cells" (the record has no `wal_fast` rows yet; the rows it has must parse, which proves the regex). Then `sed 's/single_log_fast/wal_fast/' docs/remediation/decisions/2026-09-24-single-log-spike.md > /tmp/t.md && python3 scripts/wal_fast_rule.py --head /tmp/t.md` → prints a table with every ratio ≥ 0.85 and exits 0.
- [ ] **Step 6: Lint the workflow** — `actionlint .github/workflows/remediation-gate.yml` (install with `brew install actionlint`) → no output.
- [ ] **Step 7: Commit** — `ci: add Linux probe runs to the remediation gate workflow`.
- [ ] **Step 8: After approval (step 1):** `git push -u origin remediation/phase-2`, then smoke-test: `gh workflow run remediation-gate.yml --ref remediation/phase-2 -f ref=remediation/phase-2 -f phase=2 -f probe=wal-bench`. Expected: only `resolve`, `probe-wal-bench` and `harness-result` run; the summary shows the head table; the Fast-rule step fails with "no comparable cells" (no `wal_fast` cells until Task 2.6). Record the run URL in the next task's commit body.

---

### Task 2.3: The perf gate measures the WAL (TST-09)

**Root-cause hypothesis to confirm first.** Callgrind's `--toggle-collect` *toggles* on entry to and exit from every function that matches the pattern, and gungraun's default pattern is `*::__gungraun_wrapper_mod::*`. The `async { .. }` block inside each WAL benchmark compiles to a closure whose symbol is also under `__gungraun_wrapper_mod`, so entering its `poll` flips collection **off** for exactly the code that does the WAL work. That explains ~500 instructions for 100 puts. The ledger's thread hypothesis is the second suspect, and becomes the main one after Task 2.8, when the write happens on the WAL writer `std::thread`: toggle state is per thread, and the writer thread never enters the benchmark function.

The fix below does not depend on which hypothesis is right: each WAL benchmark disables the entry-point toggle and brackets the measured call with callgrind client requests, which switch instrumentation for the whole process, so every thread's work inside the bracket is counted.

**Files:**
- Modify: `crates/prkdb/Cargo.toml` (gungraun `client_requests` feature), `crates/prkdb/benches/iai_hot_paths.rs`, `scripts/perf_gate_deltas.py`, `.github/workflows/perf-gate.yml`
- Create: `scripts/perf_gate_floors.toml`, `scripts/check_perf_gate_floors.sh`

- [ ] **Step 1: Diagnose on Linux.** Dispatch `probe=iai` at the current head (Task 2.2 step 8 pattern). With `--separate-threads=yes` the summary lists one entry per thread. Record in the commit body which thread carried the WAL instructions and whether the benchmark thread's count is ~500. If the benchmark thread shows ~500 and no other thread shows WAL-sized counts, the toggle-flip hypothesis is confirmed.

- [ ] **Step 2: Floors, the failing check first.** `scripts/perf_gate_floors.toml` states, per benchmark, a minimum instruction count as a multiple of a reference benchmark measured in the same run. Ratios, not absolute numbers, so they survive compiler changes:

```toml
# Minimum plausible instruction counts for the perf gate (TST-09). A benchmark whose Ir
# falls below `min_ratio * Ir(reference)` measured nothing and fails the gate, whatever
# its delta says. Reference benchmarks are pure single-threaded CPU work.
[floors.bench_wal_put_100]
reference = "bench_log_record_encode"
min_ratio = 100.0   # 100 puts must at least encode 100 records

[floors.bench_wal_get_hit]
reference = "bench_log_record_decode"
min_ratio = 1.0     # a hit must at least decode one record

[floors.bench_wal_put_batch_100]
reference = "bench_log_record_encode"
min_ratio = 50.0

[floors.bench_indexed_storage_insert]
reference = "bench_log_record_encode"
min_ratio = 1.0
```

(Benchmark ids are the function names gungraun reports; `bench_wal_put` is renamed `bench_wal_put_100` in step 4 so the name states its unit. Task 2.8 replaces the `LogRecord` references with `bench_batch_encode`/`bench_batch_decode`.)

Add to `scripts/perf_gate_deltas.py` a `floors` subcommand: `floors <gungraun-dir> <floors.toml>` loads every `summary.json` (reusing `find_summaries`, `load_benchmark`, `benchmark_name`, `find_ir_total`), sums `Ir` over all thread/part entries of a benchmark (with `--separate-threads=yes` a summary has one part per thread; without it, one), and for each `[floors.X]` fails if `Ir(X) < min_ratio * Ir(reference)` or if X or the reference is missing. Output: a markdown table `benchmark | Ir | reference Ir | ratio | floor | verdict`, exit 1 on any failure. Parse TOML with `tomllib` (Python ≥ 3.11 on ubuntu-latest). Also add `floors --self-test`, which builds two fake summary trees in a temp dir (one that passes, one where `bench_wal_put_100` is 500 Ir and must fail) and asserts both verdicts, so the logic is testable without Valgrind.

`scripts/check_perf_gate_floors.sh`:

```bash
#!/usr/bin/env bash
# TST-09 regression evidence: every WAL benchmark in iai_hot_paths.rs has a floor, and the
# floor logic rejects a vacuous measurement. Runs without Valgrind.
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/perf_gate_deltas.py floors --self-test
missing=0
for b in $(grep -oE '^fn (bench_wal_[a-z0-9_]+)' crates/prkdb/benches/iai_hot_paths.rs | awk '{print $2}'); do
  if ! grep -q "^\[floors\.$b\]" scripts/perf_gate_floors.toml; then
    echo "no floor for $b in scripts/perf_gate_floors.toml"; missing=1
  fi
done
exit "$missing"
```

`chmod +x scripts/check_perf_gate_floors.sh`. Run it now: it passes the self-test and reports `no floor for bench_wal_put` (the rename has not happened yet) → exit 1. That is the failing check.

Wire the floors into `perf-gate.yml`: after "Bench head SHA", add a step `python3 scripts/perf_gate_deltas.py floors target/gungraun scripts/perf_gate_floors.toml >> "$GITHUB_STEP_SUMMARY"` whose failure fails the job unconditionally (no `perf_note` override: a vacuous measurement is not a regression that can be justified).

- [ ] **Step 3: Enable client requests.** In `crates/prkdb/Cargo.toml`: `gungraun = { version = "=0.19.4", features = ["client_requests"] }`. Run `cargo bench -p prkdb --bench iai_hot_paths --no-run` on macOS → builds (client requests compile to no-ops off Valgrind).

- [ ] **Step 4: Measure the whole call, on every thread.** In `iai_hot_paths.rs`, define once:

```rust
use gungraun::client_requests::callgrind::{start_instrumentation, stop_instrumentation};
use gungraun::{Callgrind, EntryPoint, LibraryBenchmarkConfig};

/// Counts every thread's instructions between `start` and `stop` (TST-09).
///
/// The default entry point toggles collection on entry to anything under
/// `__gungraun_wrapper_mod`, including the `async` block's closure, which flips counting
/// *off* for the WAL work, and toggle state is per thread anyway. Instrumentation
/// switched by client requests is process-wide, so the WAL writer thread is counted too.
fn whole_process() -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    config.tool(
        // gungraun 0.19.4's documented pattern for client-request benchmarks
        // (`Callgrind::entry_point` rustdoc, src/common.rs).
        Callgrind::with_args(["--collect-atstart=no"]).entry_point(EntryPoint::None),
    );
    config
}
```

If step 6 shows this pattern counts nothing (a floor fails with near-zero Ir on every thread), switch the arguments to `["--instr-atstart=no", "--collect-atstart=yes"]` — instrumentation off until `start_instrumentation`, collection on whenever instrumented — and re-probe. Record which one worked in the module doc.

Move each WAL benchmark's measured body into a free function **outside** the benchmark function (so no closure symbol is under the wrapper module), and bracket it:

```rust
async fn put_100(adapter: &WalStorageAdapter, value: &[u8]) {
    for i in 0..PUT_ITERATIONS {
        let key = format!("bench-key-{i}").into_bytes();
        adapter.put(black_box(&key), black_box(value)).await.unwrap();
    }
}

#[library_benchmark(setup = setup_wal_put, config = whole_process())]
fn bench_wal_put_100(
    (rt, dir, adapter, value): (Runtime, TempDir, WalStorageAdapter, Vec<u8>),
) -> (Runtime, TempDir, WalStorageAdapter, Vec<u8>) {
    start_instrumentation();
    rt.block_on(put_100(&adapter, &value));
    stop_instrumentation();
    (rt, dir, adapter, value)
}
```

Do the same for `bench_wal_get_hit` (`get_one`), `bench_wal_put_batch_100` (`put_batch_100`) and `bench_indexed_storage_insert` (`insert_one`). Leave the two `LogRecord` benchmarks on the default entry point: they are the single-threaded references the floors divide by. Update the module doc comment: replace the paragraph on `setup` and the default `EntryPoint` with the explanation above, and state that background threads of the runtime are counted while instrumentation is on (so counts include idle tokio workers; Task 2.8 moves these benches to a current-thread runtime once the adapter no longer needs `block_in_place`).

If gungraun rejects `config = ..` together with `setup = ..` in one attribute, use its documented per-benchmark form `#[bench::put(setup = setup_wal_put, config = whole_process())]` inside `#[library_benchmark]`; either form must compile with `--no-run`.

- [ ] **Step 5: Local checks.** `cargo bench -p prkdb --bench iai_hot_paths --no-run` → builds. `scripts/check_perf_gate_floors.sh` → exit 0.
- [ ] **Step 6: Linux verification.** Dispatch `probe=iai`. Expected in the summary: `bench_wal_put_100` Ir ≥ 100 × `bench_log_record_encode` Ir (on the old mmap path, expect well above 10⁶), every floor row `ok`. If a floor still fails, the fix did not take: inspect per-thread parts, do not relax the ratio. Iterate with further probe runs; each run URL goes in the commit body.
- [ ] **Step 7: Ledger.** TST-09: `status = "fixed"`, `regression_tests = ["script:scripts/check_perf_gate_floors.sh", "ci-job:perf-gate.yml/instruction-count-gate"]`, `changes = ["<fix commit SHA>"]` (follow-up commit). `cargo xtask remediation check && cargo xtask remediation render`.
- [ ] **Step 8: Commit** — `fix: count WAL work in the instruction-count perf gate` (body: diagnosis from step 1, probe URLs, Ir before/after for the four WAL benches). Ledger follow-up: `docs: record TST-09 as fixed`.

---

### Task 2.4: `new_with_config` honours `cache_capacity` (STO-09)

Small and independent, so it lands before the rewrite and its test survives it.

**Files:**
- Modify: `crates/prkdb/src/storage/cache.rs`, `crates/prkdb/src/storage/wal_adapter.rs`

- [ ] **Step 1: Failing test.** Add to `ShardedLruCache` in `cache.rs`:

```rust
    /// Total entries this cache holds before evicting, across all shards.
    pub fn capacity(&self) -> usize {
        self.capacity_per_shard * self.shards.len()
    }
```

(Store `capacity_per_shard: usize` in the struct if it is not already a field; `with_shard_count` and `with_metrics` compute it.) Then in `wal_adapter.rs`'s `mod tests`:

```rust
    /// STO-09: `builder(..).with_cache_capacity(n)` reaches `new_with_config`, which used
    /// to hard-code 100,000 entries and ignore the knob.
    #[tokio::test(flavor = "multi_thread")]
    async fn new_with_config_honors_cache_capacity() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = WalStorageAdapter::builder(dir.path().to_path_buf())
            .with_cache_capacity(1_600)
            .build()
            .unwrap();
        assert_eq!(adapter.inner.cache.capacity(), 1_600);
    }
```

Run: `cargo nextest run -p prkdb --lib new_with_config_honors_cache_capacity` → FAIL (`left: 100000, right: 1600`; 1,600 divides evenly across the shard count, so rounding cannot mask the bug).

- [ ] **Step 2: Fix.** In `new_with_config`, replace `100_000, // Match default config cache capacity` with `config.cache_capacity,`. Re-run → PASS.
- [ ] **Step 3: Ledger** — STO-09 `fixed`, `regression_tests = ["test:crates/prkdb/src/storage/wal_adapter.rs::new_with_config_honors_cache_capacity"]`, `changes` in the follow-up. Check + render.
- [ ] **Step 4: Commit** — `fix: honour cache_capacity in WalStorageAdapter::new_with_config`; follow-up `docs: record STO-09 as fixed`.

---

### Task 2.5: Frame, segment and batch codecs

Pure, synchronous building blocks for the single log. Nothing in the product uses them until Task 2.8, so this task closes no finding; STO-04's CRC and torn-tail rules are written here and proven end to end in 2.6 and 2.8.

**Design (decision record §7, with two recorded deviations):**
- Frame: `len u32 | crc u32 | lsn u64 | kind u8 | payload`, little-endian, header 17 bytes. The CRC covers `lsn | kind | payload`. **Deviation 1:** CRC-32 via `crc32fast` (already a `prkdb-core` dependency, hardware-accelerated) instead of CRC-32C, to add no dependency. The frame header has no algorithm field, so this is fixed for format 2.
- `kind`: `1 = Batch` (one atomic write batch), `2 = Elided` (a record removed by compaction; header only, empty payload, keeps LSNs contiguous, Task 2.15). Unknown kinds are faults.
- Segment file `{first_lsn:020}.wal`, 24-byte header `b"PRKDBWAL" | format u32 | reserved u32 (0) | first_lsn u64`. `format` is `prkdb_core::format::FORMAT_VERSION` (= 2), the single version number also written to the data directory's `FORMAT` file in Task 2.11, so the program has exactly one format version.
- Scan: stop at the first frame that is short, all-zero header, oversized, bad CRC, unknown kind, or whose LSN is not the expected next one. The scan reports where and why it stopped; the caller (Task 2.6) decides between "torn tail, truncate" (last segment) and "corruption, refuse" (earlier segment).
- Batch payload: `version u8 (=1) | codec u8 | raw_len u32 | body`, where `codec` is the existing `CompressionType` discriminant (`0` none, `1` LZ4, `2` Snappy, `3` Zstd) and `body` is the op list, compressed when `codec != 0`. **Deviation 2 (from the decision record, which had no compression):** `WalConfig::compression` defaults to LZ4 today; dropping it silently would add another knob that does nothing (root cause 4). Ops: `u32 count`, then per op a tag byte: `1 Put: u32 klen | key | u32 vlen | value`, `2 Delete: u32 klen | key`. Tags 3–4 (events) are added in Tasks 2.19–2.20, before the format is frozen by the golden directory in Task 2.24.

**Files:**
- Create: `crates/prkdb-core/src/format.rs`, `crates/prkdb-core/src/wal/frame.rs`, `crates/prkdb-core/src/wal/segment.rs`, `crates/prkdb-core/src/wal/batch.rs`, `crates/prkdb-core/tests/wal_segment.rs`
- Modify: `crates/prkdb-core/src/lib.rs` (`pub mod format;`), `crates/prkdb-core/src/wal/mod.rs` (`pub mod frame; pub mod segment; pub mod batch;` and new `WalError` variants)

- [ ] **Step 1: Interfaces (write the signatures with `todo!()` bodies so the tests compile and fail).**

`format.rs`:
```rust
//! The on-disk format version (spec D3). Written into every WAL segment header and into
//! the data directory's `FORMAT` file. Bump only together with a registered migration.
pub const FORMAT_VERSION: u32 = 2;
```

New `WalError` variants in `wal/mod.rs` (no existing code matches on `WalError` exhaustively):
```rust
    #[error("unsupported WAL format {found} in {path}; this build reads format {supported}")]
    UnsupportedFormat { path: std::path::PathBuf, found: u32, supported: u32 },
    #[error("corrupt WAL: {path} at byte {offset}: {reason}")]
    CorruptSegment { path: std::path::PathBuf, offset: u64, reason: String },
    #[error("record of {len} bytes exceeds the {max}-byte limit")]
    RecordTooLarge { len: usize, max: usize },
    #[error("WAL is poisoned by an earlier I/O failure and accepts no more writes: {0}")]
    Poisoned(String),
    #[error("WAL is closed")]
    Closed,
```

`frame.rs`:
```rust
pub type Lsn = u64;
pub const FRAME_HEADER_LEN: usize = 17;
/// Largest payload accepted on write and trusted on read (checked before allocating).
pub const MAX_PAYLOAD_LEN: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind { Batch = 1, Elided = 2 }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameFault {
    /// Fewer bytes left than a header, or than the header's length claims.
    Truncated,
    /// A header of all zeros: preallocated or never-written space. End of log.
    ZeroHeader,
    BadLength(u32),
    BadCrc,
    UnknownKind(u8),
    LsnGap { expected: Lsn, found: Lsn },
}

#[derive(Debug, PartialEq, Eq)]
pub enum Decoded<'a> {
    Frame { lsn: Lsn, kind: FrameKind, payload: &'a [u8], frame_len: usize },
    Fault(FrameFault),
}

/// Appends one frame to `out`. Panics in debug if `payload.len() > MAX_PAYLOAD_LEN`;
/// callers check with `WalError::RecordTooLarge` first.
pub fn encode_frame(out: &mut Vec<u8>, lsn: Lsn, kind: FrameKind, payload: &[u8]);
/// Decodes the frame at the start of `buf`. Never allocates, never panics.
pub fn decode_frame(buf: &[u8]) -> Decoded<'_>;
```

`segment.rs`:
```rust
pub const SEGMENT_MAGIC: [u8; 8] = *b"PRKDBWAL";
pub const SEGMENT_HEADER_LEN: u64 = 24;
pub fn segment_file_name(first_lsn: Lsn) -> String;            // "{first_lsn:020}.wal"
pub fn parse_segment_file_name(name: &str) -> Option<Lsn>;      // inverse; None for anything else
/// Writes the header at offset 0. Does not sync.
pub fn write_segment_header(file: &dyn VfsFile, first_lsn: Lsn) -> io::Result<()>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentScan {
    pub first_lsn: Lsn,
    /// LSN the next frame appended to this segment must carry.
    pub next_lsn: Lsn,
    /// Offset just past the last good frame (>= SEGMENT_HEADER_LEN).
    pub valid_len: u64,
    pub file_len: u64,
    /// Where and why the scan stopped before `file_len`; `None` if every byte was a good frame.
    pub stopped: Option<(u64, FrameFault)>,
}

/// Where a frame lives, as handed to the visitor and stored in indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecordLoc {
    pub lsn: Lsn,
    /// First LSN of the segment holding the frame (its file-name key).
    pub segment: Lsn,
    /// Byte offset of the frame header within the segment file.
    pub offset: u64,
    pub payload_len: u32,
}

/// Verifies the header (magic, format == FORMAT_VERSION, first_lsn == `first_lsn`), then
/// every frame in order, reading in 1 MiB chunks (a frame spanning chunks is read whole
/// after checking its length against MAX_PAYLOAD_LEN and the file length). Calls `visit`
/// for each good frame. Header problems are errors: `UnsupportedFormat` for a different
/// format number, `CorruptSegment` for bad magic or first_lsn. Frame problems are not
/// errors: they end the scan and are reported in `SegmentScan::stopped`.
pub fn scan_segment(
    file: &dyn VfsFile,
    path: &Path,
    first_lsn: Lsn,
    visit: &mut dyn FnMut(RecordLoc, FrameKind, &[u8]) -> Result<(), WalError>,
) -> Result<SegmentScan, WalError>;

/// Reads one frame at `loc`, verifying CRC, kind and that its LSN is `loc.lsn`.
pub fn read_frame(file: &dyn VfsFile, path: &Path, loc: RecordLoc) -> Result<Vec<u8>, WalError>;
```

`batch.rs`:
```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Batch { pub ops: Vec<BatchOp> }

impl Batch {
    pub fn encode(&self, compression: &CompressionConfig) -> Result<Vec<u8>, WalError>;
    /// Rejects unknown versions, codecs and tags, lengths past the end, and trailing bytes.
    pub fn decode(bytes: &[u8]) -> Result<Batch, WalError>;
}
```

- [ ] **Step 2: Failing tests** — `crates/prkdb-core/tests/wal_segment.rs`:

```rust
//! Frame/segment/batch codecs (Task 2.5). StdVfs only; FaultFs tests live in prkdb-verify.

use prkdb_core::format::FORMAT_VERSION;
use prkdb_core::vfs::{OpenMode, StdVfs, Vfs, VfsFile};
use prkdb_core::wal::batch::{Batch, BatchOp};
use prkdb_core::wal::frame::{decode_frame, encode_frame, Decoded, FrameFault, FrameKind, FRAME_HEADER_LEN};
use prkdb_core::wal::segment::{
    parse_segment_file_name, read_frame, scan_segment, segment_file_name, write_segment_header,
    RecordLoc, SegmentScan, SEGMENT_HEADER_LEN,
};
use prkdb_core::wal::{CompressionConfig, WalError};
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn write_segment(dir: &Path, first: u64, payloads: &[Vec<u8>]) -> (PathBuf, Vec<u64>) {
    let path = dir.join(segment_file_name(first));
    let f = StdVfs.create(&path).unwrap();
    write_segment_header(f.as_ref(), first).unwrap();
    let (mut buf, mut starts) = (Vec::new(), Vec::new());
    for (i, p) in payloads.iter().enumerate() {
        starts.push(SEGMENT_HEADER_LEN + buf.len() as u64);
        encode_frame(&mut buf, first + i as u64, FrameKind::Batch, p);
    }
    f.write_at(SEGMENT_HEADER_LEN, &buf).unwrap();
    f.sync_data().unwrap();
    (path, starts)
}

fn scan(path: &Path, first: u64) -> Result<(Vec<(u64, Vec<u8>)>, SegmentScan), WalError> {
    let f: Arc<dyn VfsFile> = StdVfs.open(path, OpenMode::Read).unwrap();
    let mut seen = Vec::new();
    let s = scan_segment(f.as_ref(), path, first, &mut |loc, kind, payload| {
        assert_eq!(kind, FrameKind::Batch);
        seen.push((loc.lsn, payload.to_vec()));
        Ok(())
    })?;
    Ok((seen, s))
}

fn payloads(n: usize) -> Vec<Vec<u8>> {
    (0..n).map(|i| format!("payload-{i}").repeat(i + 1).into_bytes()).collect()
}

fn flip_byte(path: &Path, at: u64) {
    let f = StdVfs.open(path, OpenMode::ReadWrite).unwrap();
    let mut b = [0u8; 1];
    f.read_at(at, &mut b).unwrap();
    f.write_at(at, &[b[0] ^ 0xFF]).unwrap();
}

#[test]
fn frames_round_trip_in_lsn_order() {
    let dir = tempfile::tempdir().unwrap();
    let p = payloads(5);
    let (path, _) = write_segment(dir.path(), 41, &p);
    let (seen, s) = scan(&path, 41).unwrap();
    assert_eq!(seen, (41..46).zip(p).collect::<Vec<_>>());
    assert_eq!((s.first_lsn, s.next_lsn, s.stopped), (41, 46, None));
    assert_eq!(s.valid_len, s.file_len);
}

#[test]
fn a_flipped_payload_byte_stops_the_scan_at_that_frame() {
    let dir = tempfile::tempdir().unwrap();
    let (path, starts) = write_segment(dir.path(), 1, &payloads(3));
    flip_byte(&path, starts[1] + FRAME_HEADER_LEN as u64 + 2);
    let (seen, s) = scan(&path, 1).unwrap();
    assert_eq!(seen.len(), 1, "only the frame before the corruption is good");
    assert_eq!(s.stopped, Some((starts[1], FrameFault::BadCrc)));
    assert_eq!((s.valid_len, s.next_lsn), (starts[1], 2));
}

#[test]
fn a_short_tail_is_truncated_not_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let (path, starts) = write_segment(dir.path(), 1, &payloads(3));
    let f = StdVfs.open(&path, OpenMode::ReadWrite).unwrap();
    f.set_len(starts[2] + 5).unwrap();
    let (seen, s) = scan(&path, 1).unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(s.stopped, Some((starts[2], FrameFault::Truncated)));
}

#[test]
fn zeroed_space_after_the_last_frame_is_end_of_log() {
    let dir = tempfile::tempdir().unwrap();
    let (path, _) = write_segment(dir.path(), 1, &payloads(2));
    let f = StdVfs.open(&path, OpenMode::ReadWrite).unwrap();
    let len = f.len().unwrap();
    f.set_len(len + 4096).unwrap(); // what preallocation looks like
    let (seen, s) = scan(&path, 1).unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(s.stopped, Some((len, FrameFault::ZeroHeader)));
    assert_eq!(s.valid_len, len);
}

#[test]
fn an_lsn_gap_is_a_fault() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(segment_file_name(1));
    let f = StdVfs.create(&path).unwrap();
    write_segment_header(f.as_ref(), 1).unwrap();
    let mut buf = Vec::new();
    encode_frame(&mut buf, 1, FrameKind::Batch, b"a");
    let second = SEGMENT_HEADER_LEN + buf.len() as u64;
    encode_frame(&mut buf, 3, FrameKind::Batch, b"b");
    f.write_at(SEGMENT_HEADER_LEN, &buf).unwrap();
    let (_, s) = scan(&path, 1).unwrap();
    assert_eq!(s.stopped, Some((second, FrameFault::LsnGap { expected: 2, found: 3 })));
}

#[test]
fn a_huge_length_is_rejected_without_allocating() {
    let mut buf = Vec::new();
    encode_frame(&mut buf, 7, FrameKind::Batch, b"x");
    buf[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(decode_frame(&buf), Decoded::Fault(FrameFault::BadLength(u32::MAX)));
}

#[test]
fn a_newer_format_is_refused_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let (path, _) = write_segment(dir.path(), 1, &payloads(1));
    let f = StdVfs.open(&path, OpenMode::ReadWrite).unwrap();
    f.write_at(8, &(FORMAT_VERSION + 1).to_le_bytes()).unwrap();
    let err = scan(&path, 1).unwrap_err();
    assert!(
        matches!(err, WalError::UnsupportedFormat { found, supported, .. } if found == FORMAT_VERSION + 1 && supported == FORMAT_VERSION),
        "{err}"
    );
}

#[test]
fn read_frame_checks_the_lsn_it_was_asked_for() {
    let dir = tempfile::tempdir().unwrap();
    let (path, starts) = write_segment(dir.path(), 10, &payloads(2));
    let f = StdVfs.open(&path, OpenMode::Read).unwrap();
    let loc = RecordLoc { lsn: 11, segment: 10, offset: starts[1], payload_len: payloads(2)[1].len() as u32 };
    assert_eq!(read_frame(f.as_ref(), &path, loc).unwrap(), payloads(2)[1]);
    let wrong = RecordLoc { lsn: 12, ..loc };
    assert!(read_frame(f.as_ref(), &path, wrong).is_err());
}

#[test]
fn segment_names_sort_by_first_lsn() {
    assert_eq!(segment_file_name(42), "00000000000000000042.wal");
    assert_eq!(parse_segment_file_name("00000000000000000042.wal"), Some(42));
    assert_eq!(parse_segment_file_name("00000000000000000042.wal.tmp"), None);
    assert_eq!(parse_segment_file_name("checkpoint.json"), None);
}

#[test]
fn batches_round_trip_with_and_without_compression() {
    let batch = Batch {
        ops: vec![
            BatchOp::Put { key: b"k1".to_vec(), value: vec![7; 4096] },
            BatchOp::Delete { key: b"k0".to_vec() },
        ],
    };
    for cfg in [CompressionConfig::none(), CompressionConfig::default()] {
        assert_eq!(Batch::decode(&batch.encode(&cfg).unwrap()).unwrap(), batch);
    }
}

#[test]
fn a_batch_with_trailing_bytes_or_unknown_tag_is_corrupt() {
    let mut bytes = Batch { ops: vec![BatchOp::Delete { key: b"k".to_vec() }] }
        .encode(&CompressionConfig::none())
        .unwrap();
    bytes.push(0);
    assert!(Batch::decode(&bytes).is_err());
    let mut bad_tag = Batch { ops: vec![BatchOp::Delete { key: b"k".to_vec() }] }
        .encode(&CompressionConfig::none())
        .unwrap();
    let tag_at = 1 + 1 + 4 + 4; // version, codec, raw_len, count
    bad_tag[tag_at] = 99;
    assert!(Batch::decode(&bad_tag).is_err());
}
```

Run: `cargo nextest run -p prkdb-core --test wal_segment` → every test panics in `todo!()`.

- [ ] **Step 3: Implement** `frame.rs`, `segment.rs`, `batch.rs` to the contracts above. Notes: `decode_frame` returns `ZeroHeader` only when all 17 header bytes are zero; a zero `len` with non-zero CRC or LSN is `BadLength(0)` only if `kind` is `Batch` (an `Elided` frame legitimately has `len == 0`). `scan_segment` uses the frame's CRC over `lsn | kind | payload`; the expected next LSN starts at `first_lsn`. Batch `decode` checks every length against the remaining input before slicing. No `unwrap` outside tests (crate lint).
- [ ] **Step 4: Run** → 11 passed. `cargo clippy -p prkdb-core --all-targets -- -D warnings` → clean.
- [ ] **Step 5: Commit** — `feat: add frame, segment and batch codecs for the single WAL`.

---

### Task 2.6: `Wal` — writer thread, group commit, recovery (library only)

The real log, not yet wired into the adapter. Ported from the spike's `SingleLog` (`wal_write_path_spike.rs`, `writer_loop`) and completed with everything the spike omitted (decision record §6 risk 5).

**Files:**
- Create: `crates/prkdb-core/src/wal/log.rs`, `crates/prkdb-core/tests/wal_log.rs`, `crates/prkdb-verify/tests/wal_power_loss.rs`
- Modify: `crates/prkdb-core/src/wal/mod.rs` (`pub mod log; pub use log::{Wal, WalOptions, WalHealth, RecoveryReport, CommitHook}; pub use frame::Lsn; pub use segment::RecordLoc;` and `SyncMode` added to the existing `pub use config::{…}`), `crates/prkdb-core/src/wal/config.rs`, `crates/prkdb/benches/wal_write_path_spike.rs`

- [ ] **Step 1: Config.** In `wal/config.rs`:

```rust
/// When a write is acknowledged (spec §6.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// Ack after the group-commit batch containing the write is fsynced.
    #[default]
    Durable,
    /// Ack after the write reaches the OS; synced at least every `sync_interval_ms`.
    /// A power cut can lose up to `sync_interval_ms` of acknowledged writes.
    Fast,
}
```

and add to `WalConfig` (update `Default`, `test_config`, `benchmark_config`, `production_config`, `compression_optimized`, and every struct literal found by `rg -n 'WalConfig \{' crates --glob '*.rs'` — `builder.rs::default_wal_config` lists every field):
```rust
    /// Acknowledgement policy. Default `Durable` everywhere, including `test_config()`.
    pub sync_mode: SyncMode,
    /// Fast mode's sync bound (default 10).
    pub sync_interval_ms: u64,
    /// Largest group-commit write (default 16 MiB).
    pub max_batch_bytes: usize,
    /// Admission bound: bytes queued for the writer before appenders wait (default 64 MiB).
    pub max_queued_bytes: usize,
```
`segment_bytes` is now honoured (STO-08); `test_config()` keeps 1 MiB. `segment_count` and `shard_count` stay until Task 2.8 removes them with their last reader.

`log.rs` public surface:

```rust
pub struct WalOptions {
    pub sync_mode: SyncMode,
    pub sync_interval: Duration,
    pub segment_bytes: u64,
    pub max_batch_bytes: usize,
    pub max_queued_bytes: usize,
}
impl WalOptions { pub fn from_config(c: &WalConfig) -> Self; }

/// Runs on the writer thread, in LSN order, after the frame is durable (Durable) or
/// written (Fast), and before the appender's future resolves. Must not block or panic;
/// a panic poisons the log.
pub type CommitHook = Box<dyn FnOnce(RecordLoc) + Send>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalHealth {
    Healthy,
    /// Requests are queued and no batch completed for longer than the stall bound.
    Stalled { queued_bytes: usize, oldest_ms: u64 },
    Poisoned(String),
    Closed,
}

#[derive(Debug, Default)]
pub struct RecoveryReport {
    pub segments: usize,
    pub frames: u64,
    pub next_lsn: Lsn,
    /// Set when the last segment ended in a torn frame and was truncated.
    pub truncated: Option<(PathBuf, u64, FrameFault)>,
}

pub struct Wal { /* Arc<Shared>, request sender, writer JoinHandle */ }

impl Wal {
    /// Opens (creating `dir` if absent: create_dir_all + sync_dir of the parent) and
    /// recovers: lists `*.wal`, sorts by first LSN, checks each segment's first LSN equals
    /// the previous segment's `next_lsn`, scans every segment, and calls `replay` for every
    /// frame with lsn >= `replay_from` in LSN order. Last segment: a torn tail is logged,
    /// truncated (`set_len` + `sync_data`) and reported. Earlier segment: any fault is
    /// `CorruptSegment` and nothing is modified. A zero-length or header-only last segment
    /// is valid (a crash right after a roll). An empty directory starts at LSN 1.
    pub fn open(
        vfs: Arc<dyn Vfs>,
        dir: &Path,
        opts: WalOptions,
        replay_from: Lsn,
        replay: &mut dyn FnMut(RecordLoc, FrameKind, &[u8]) -> Result<(), WalError>,
    ) -> Result<(Wal, RecoveryReport), WalError>;

    pub async fn append(&self, payload: Vec<u8>, hook: Option<CommitHook>) -> Result<RecordLoc, WalError>;
    /// For callers outside a runtime (checkpoint, compaction, tests).
    pub fn append_blocking(&self, payload: Vec<u8>, hook: Option<CommitHook>) -> Result<RecordLoc, WalError>;
    /// Makes every write acknowledged so far durable; returns the durable watermark.
    pub async fn sync(&self) -> Result<Lsn, WalError>;
    pub fn sync_blocking(&self) -> Result<Lsn, WalError>;
    pub fn read(&self, loc: RecordLoc) -> Result<Vec<u8>, WalError>;
    /// Visits committed frames with lsn >= `from`, in order (reads through Vfs).
    pub fn scan_from(&self, from: Lsn, visit: &mut dyn FnMut(RecordLoc, FrameKind, &[u8]) -> Result<(), WalError>) -> Result<(), WalError>;
    pub fn next_lsn(&self) -> Lsn;
    pub fn durable_lsn(&self) -> Lsn;
    pub fn health(&self) -> WalHealth;
    /// First LSNs of the segments, oldest first (tests, compaction).
    pub fn segments(&self) -> Vec<Lsn>;
    /// Drains the queue, syncs, joins the writer. `Drop` does the same and logs errors.
    pub fn close(self) -> Result<(), WalError>;
}
```

**Invariants the implementation must hold (each has a test below):**
1. LSNs are assigned by the writer, contiguous from 1, one per `append`. Replay visits them in LSN order.
2. Durable: the frame is `sync_data`ed before its hook runs and before the caller is answered. Fast: the frame is written before; it is synced within `sync_interval` even if no further writes arrive (the writer waits with `recv_timeout(remaining interval)` while it holds unsynced data).
3. Hooks run on the writer thread in LSN order.
4. Any I/O error, or a panicking hook, **poisons** the log: the failing batch and every later append get `WalError::Poisoned`; a failed `fsync` is never retried (fsyncgate); `health()` reports `Poisoned`. The writer body runs under `catch_unwind` so a panic becomes poisoning, and remaining queued requests are answered, never dropped silently (each request's `oneshot` sender sits in a struct whose `Drop` sends `Err(Closed)`, mirroring today's `PendingWrite`).
5. A batch never spans segments. Roll when the next batch would push the active segment past `segment_bytes` (a batch larger than `segment_bytes` gets a segment of its own): `sync_data` the old segment, `create` `{next_lsn:020}.wal`, write and `sync_data` its header, `sync_dir(dir)`, switch.
6. Admission: an append acquires `min(payload.len(), max_queued_bytes)` permits from a `tokio::sync::Semaphore` of `max_queued_bytes` before queueing, released when the writer answers it. A payload over `MAX_PAYLOAD_LEN` is refused with `RecordTooLarge` before queueing. `append_blocking` acquires with `futures::executor::block_on`.
7. Batching: block for the first request, drain what is already queued up to `max_batch_bytes`, no linger timer (decision record §7).
8. `read` verifies CRC and LSN (`segment::read_frame`); it never trusts an offset blindly.

- [ ] **Step 2: Failing tests (StdVfs)** — `crates/prkdb-core/tests/wal_log.rs`:

```rust
//! `Wal` behaviour over StdVfs (Task 2.6). Power-loss behaviour: prkdb-verify/tests/wal_power_loss.rs.

use prkdb_core::vfs::{OpenMode, StdVfs, Vfs, VfsFile};
use prkdb_core::wal::frame::FrameKind;
use prkdb_core::wal::{Lsn, RecordLoc, SyncMode, Wal, WalError, WalHealth, WalOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn opts(mode: SyncMode, segment_bytes: u64) -> WalOptions {
    WalOptions {
        sync_mode: mode,
        sync_interval: Duration::from_millis(20),
        segment_bytes,
        max_batch_bytes: 1 << 20,
        max_queued_bytes: 8 << 20,
    }
}

fn open(vfs: Arc<dyn Vfs>, dir: &Path, o: WalOptions) -> (Wal, Vec<(Lsn, Vec<u8>)>) {
    let mut seen = Vec::new();
    let (wal, _) = Wal::open(vfs, dir, o, 1, &mut |loc, kind, p| {
        assert_eq!(kind, FrameKind::Batch);
        seen.push((loc.lsn, p.to_vec()));
        Ok(())
    })
    .unwrap();
    (wal, seen)
}

/// Wraps StdVfs, counting syncs and optionally failing them.
#[derive(Default)]
struct Probe {
    syncs: AtomicU64,
    fail_syncs: AtomicBool,
}
struct ProbeVfs(Arc<Probe>);
struct ProbeFile(Arc<dyn VfsFile>, Arc<Probe>);
impl VfsFile for ProbeFile {
    fn write_at(&self, o: u64, b: &[u8]) -> io::Result<()> { self.0.write_at(o, b) }
    fn read_at(&self, o: u64, b: &mut [u8]) -> io::Result<usize> { self.0.read_at(o, b) }
    fn set_len(&self, l: u64) -> io::Result<()> { self.0.set_len(l) }
    fn len(&self) -> io::Result<u64> { self.0.len() }
    fn sync_data(&self) -> io::Result<()> {
        if self.1.fail_syncs.load(Ordering::SeqCst) {
            return Err(io::Error::other("injected fsync failure"));
        }
        self.1.syncs.fetch_add(1, Ordering::SeqCst);
        self.0.sync_data()
    }
}
impl Vfs for ProbeVfs {
    fn open(&self, p: &Path, m: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
        Ok(Arc::new(ProbeFile(StdVfs.open(p, m)?, self.0.clone())))
    }
    fn create(&self, p: &Path) -> io::Result<Arc<dyn VfsFile>> {
        Ok(Arc::new(ProbeFile(StdVfs.create(p)?, self.0.clone())))
    }
    fn rename(&self, a: &Path, b: &Path) -> io::Result<()> { StdVfs.rename(a, b) }
    fn remove(&self, p: &Path) -> io::Result<()> { StdVfs.remove(p) }
    fn create_dir_all(&self, p: &Path) -> io::Result<()> { StdVfs.create_dir_all(p) }
    fn read_dir(&self, p: &Path) -> io::Result<Vec<PathBuf>> { StdVfs.read_dir(p) }
    fn exists(&self, p: &Path) -> io::Result<bool> { StdVfs.exists(p) }
    fn sync_dir(&self, d: &Path) -> io::Result<()> { StdVfs.sync_dir(d) }
}

/// STO-05: replay order is append order, by global LSN, across segment rolls.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replay_order_equals_append_order_across_segment_roll() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = open(Arc::new(StdVfs), dir.path(), opts(SyncMode::Fast, 4096));
    let wal = Arc::new(wal);
    let mut tasks = Vec::new();
    for t in 0..8 {
        let wal = wal.clone();
        tasks.push(tokio::spawn(async move {
            let mut acked = Vec::new();
            for i in 0..200 {
                let p = format!("t{t}-{i}").into_bytes();
                let loc = wal.append(p.clone(), None).await.unwrap();
                acked.push((loc.lsn, p));
            }
            acked
        }));
    }
    let mut acked: Vec<(Lsn, Vec<u8>)> = Vec::new();
    for t in tasks {
        acked.extend(t.await.unwrap());
    }
    assert!(wal.segments().len() >= 3, "4 KiB segments must roll: {:?}", wal.segments());
    Arc::try_unwrap(wal).ok().unwrap().close().unwrap();

    acked.sort();
    let lsns: Vec<Lsn> = acked.iter().map(|(l, _)| *l).collect();
    assert_eq!(lsns, (1..=1600).collect::<Vec<_>>(), "LSNs are contiguous from 1");
    let (_, replayed) = open(Arc::new(StdVfs), dir.path(), opts(SyncMode::Fast, 4096));
    assert_eq!(replayed, acked, "replay yields exactly the acknowledged records, in LSN order");
}

#[tokio::test(flavor = "multi_thread")]
async fn durable_appends_are_synced_before_the_ack() {
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let (wal, _) = open(Arc::new(ProbeVfs(probe.clone())), dir.path(), opts(SyncMode::Durable, 1 << 20));
    let before = probe.syncs.load(Ordering::SeqCst);
    let loc = wal.append(b"x".to_vec(), None).await.unwrap();
    assert!(probe.syncs.load(Ordering::SeqCst) > before, "ack came before any sync");
    assert!(wal.durable_lsn() >= loc.lsn);
}

#[tokio::test(flavor = "multi_thread")]
async fn fast_appends_are_synced_within_the_interval_without_more_writes() {
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let (wal, _) = open(Arc::new(ProbeVfs(probe.clone())), dir.path(), opts(SyncMode::Fast, 1 << 20));
    let loc = wal.append(b"x".to_vec(), None).await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while wal.durable_lsn() < loc.lsn {
        assert!(std::time::Instant::now() < deadline, "never synced");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_sync_poisons_the_log_and_is_never_retried() {
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let (wal, _) = open(Arc::new(ProbeVfs(probe.clone())), dir.path(), opts(SyncMode::Durable, 1 << 20));
    wal.append(b"ok".to_vec(), None).await.unwrap();
    probe.fail_syncs.store(true, Ordering::SeqCst);
    assert!(wal.append(b"lost".to_vec(), None).await.is_err());
    probe.fail_syncs.store(false, Ordering::SeqCst);
    let later = wal.append(b"later".to_vec(), None).await;
    assert!(matches!(later, Err(WalError::Poisoned(_))), "{later:?}");
    assert!(matches!(wal.health(), WalHealth::Poisoned(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_hooks_run_in_lsn_order() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = open(Arc::new(StdVfs), dir.path(), opts(SyncMode::Fast, 1 << 20));
    let wal = Arc::new(wal);
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut tasks = Vec::new();
    for t in 0..8 {
        let (wal, order) = (wal.clone(), order.clone());
        tasks.push(tokio::spawn(async move {
            for i in 0..100 {
                let o = order.clone();
                let hook: prkdb_core::wal::CommitHook = Box::new(move |loc: RecordLoc| o.lock().unwrap().push(loc.lsn));
                wal.append(format!("{t}-{i}").into_bytes(), Some(hook)).await.unwrap();
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let order = order.lock().unwrap().clone();
    assert_eq!(order, (1..=800).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_records_are_refused_before_queueing() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = open(Arc::new(StdVfs), dir.path(), opts(SyncMode::Fast, 1 << 20));
    let big = vec![0u8; prkdb_core::wal::frame::MAX_PAYLOAD_LEN + 1];
    assert!(matches!(wal.append(big, None).await, Err(WalError::RecordTooLarge { .. })));
    assert_eq!(wal.next_lsn(), 1, "a refused record consumes no LSN");
}

#[tokio::test(flavor = "multi_thread")]
async fn read_returns_the_payload_and_rejects_a_stale_location() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = open(Arc::new(StdVfs), dir.path(), opts(SyncMode::Durable, 1 << 20));
    let a = wal.append(b"alpha".to_vec(), None).await.unwrap();
    let b = wal.append(b"beta".to_vec(), None).await.unwrap();
    assert_eq!(wal.read(a).unwrap(), b"alpha");
    assert!(wal.read(RecordLoc { lsn: b.lsn, ..a }).is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_corrupt_sealed_segment_refuses_to_open() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (wal, _) = open(Arc::new(StdVfs), dir.path(), opts(SyncMode::Durable, 4096));
        for i in 0..200 {
            wal.append(format!("record-{i}").into_bytes(), None).await.unwrap();
        }
        assert!(wal.segments().len() >= 2);
        wal.close().unwrap();
    }
    let first = dir.path().join(prkdb_core::wal::segment::segment_file_name(1));
    let f = StdVfs.open(&first, OpenMode::ReadWrite).unwrap();
    f.write_at(prkdb_core::wal::segment::SEGMENT_HEADER_LEN + 20, &[0xAB; 4]).unwrap();
    let err = Wal::open(Arc::new(StdVfs), dir.path(), opts(SyncMode::Durable, 4096), 1, &mut |_, _, _| Ok(()))
        .err()
        .expect("corruption in a sealed segment must refuse to open");
    assert!(matches!(err, WalError::CorruptSegment { ref path, .. } if *path == first), "{err}");
}
```

(Admission is covered by `oversized_records_are_refused_before_queueing` plus a unit test inside `log.rs` that fills the semaphore with a `ProbeVfs`-style file whose `write_at` blocks on a `std::sync::Barrier`, asserts a further `append` stays pending for 100 ms, then releases the barrier and asserts it completes. Write it in `log.rs`'s `#[cfg(test)] mod tests`, where the blocking file type can be private.)

Run: `cargo nextest run -p prkdb-core --test wal_log` → all fail (`todo!()`).

- [ ] **Step 3: Failing power-loss tests (FaultFs)** — `crates/prkdb-verify/tests/wal_power_loss.rs`:

```rust
//! `Wal` under simulated power loss (Task 2.6, STO-04). FaultFs rules: faultfs.rs module docs.

use prkdb_core::vfs::{OpenMode, Vfs, VfsFile};
use prkdb_core::wal::frame::FrameKind;
use prkdb_core::wal::{Lsn, SyncMode, Wal, WalOptions};
use prkdb_verify::faultfs::{FaultFs, Tear};
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

const DIR: &str = "/db/wal";

fn opts(mode: SyncMode, segment_bytes: u64) -> WalOptions {
    WalOptions {
        sync_mode: mode,
        // Fast-mode tests must not depend on a timer firing: an hour means "only explicit
        // syncs", which makes the unsynced window deterministic.
        sync_interval: Duration::from_secs(3600),
        segment_bytes,
        max_batch_bytes: 1 << 20,
        max_queued_bytes: 8 << 20,
    }
}

fn fs() -> FaultFs {
    let fs = FaultFs::new();
    fs.mkdir_durable(Path::new("/db")).unwrap();
    fs
}

fn recover(fs: &FaultFs, o: WalOptions) -> (Wal, Vec<(Lsn, Vec<u8>)>) {
    let mut seen = Vec::new();
    let (wal, _) = Wal::open(Arc::new(fs.clone()), Path::new(DIR), o, 1, &mut |loc, _, p| {
        seen.push((loc.lsn, p.to_vec()));
        Ok(())
    })
    .unwrap();
    (wal, seen)
}

fn payload(i: u64) -> Vec<u8> {
    format!("rec-{i:04}").repeat(8).into_bytes()
}

const TEARS: [Tear; 4] = [Tear::None, Tear::Prefix, Tear::ZeroTail, Tear::Garbage];

/// STO-02 at the log level: Durable acks survive power loss, whatever happens to the tail.
#[tokio::test(flavor = "multi_thread")]
async fn durable_acks_survive_power_loss_with_any_tear() {
    for seed in 0..25u64 {
        for tear in TEARS {
            let fs = fs();
            let (wal, _) = recover(&fs, opts(SyncMode::Durable, 2048));
            let mut acked = Vec::new();
            for i in 0..60 {
                acked.push((wal.append(payload(i), None).await.unwrap().lsn, payload(i)));
            }
            fs.power_loss(&mut ChaCha8Rng::seed_from_u64(seed), tear);
            drop(wal); // after the loss: its handles are stale, so nothing more can be synced
            let (_, replayed) = recover(&fs, opts(SyncMode::Durable, 2048));
            assert_eq!(replayed, acked, "seed {seed} tear {tear:?}");
        }
    }
}

/// Fast mode loses at most the unsynced suffix, and what survives is a prefix: no holes.
#[tokio::test(flavor = "multi_thread")]
async fn fast_power_loss_keeps_a_prefix_no_shorter_than_the_last_sync() {
    for seed in 0..25u64 {
        for tear in TEARS {
            let fs = fs();
            let (wal, _) = recover(&fs, opts(SyncMode::Fast, 1 << 20));
            for i in 0..20 {
                wal.append(payload(i), None).await.unwrap();
            }
            let synced = wal.sync().await.unwrap();
            for i in 20..40 {
                wal.append(payload(i), None).await.unwrap();
            }
            fs.power_loss(&mut ChaCha8Rng::seed_from_u64(seed), tear);
            drop(wal);
            let (_, replayed) = recover(&fs, opts(SyncMode::Fast, 1 << 20));
            let n = replayed.len() as u64;
            assert!(n >= synced && n <= 40, "seed {seed} tear {tear:?}: kept {n}, synced {synced}");
            let expected: Vec<_> = (0..n).map(|i| (i + 1, payload(i))).collect();
            assert_eq!(replayed, expected, "seed {seed} tear {tear:?}: not a prefix");
        }
    }
}

/// STO-04: a torn tail is truncated at open, and writes after the truncation survive.
#[tokio::test(flavor = "multi_thread")]
async fn torn_tail_is_truncated_and_later_appends_survive() {
    let fs = fs();
    let (wal, _) = recover(&fs, opts(SyncMode::Fast, 1 << 20));
    for i in 0..10 {
        wal.append(payload(i), None).await.unwrap();
    }
    wal.sync().await.unwrap();
    for i in 10..15 {
        wal.append(payload(i), None).await.unwrap();
    }
    fs.power_loss(&mut ChaCha8Rng::seed_from_u64(3), Tear::Garbage);
    drop(wal);

    let (wal, first) = recover(&fs, opts(SyncMode::Durable, 1 << 20));
    let kept = first.len() as u64;
    assert!(kept >= 10);
    let mut later = Vec::new();
    for i in 100..103 {
        later.push((wal.append(payload(i), None).await.unwrap().lsn, payload(i)));
    }
    fs.power_loss(&mut ChaCha8Rng::seed_from_u64(4), Tear::None);
    drop(wal);

    let (_, second) = recover(&fs, opts(SyncMode::Durable, 1 << 20));
    assert_eq!(&second[..kept as usize], &first[..]);
    assert_eq!(&second[kept as usize..], &later[..], "appends after the truncation were lost");
}

/// STO-04: a segment created by a roll survives power loss (directory fsync on create).
#[tokio::test(flavor = "multi_thread")]
async fn a_new_segment_is_durable_after_roll() {
    let fs = fs();
    let (wal, _) = recover(&fs, opts(SyncMode::Durable, 1024));
    let mut acked = Vec::new();
    for i in 0..40 {
        acked.push((wal.append(payload(i), None).await.unwrap().lsn, payload(i)));
    }
    assert!(wal.segments().len() >= 3);
    fs.power_loss(&mut ChaCha8Rng::seed_from_u64(9), Tear::None);
    drop(wal);
    let (_, replayed) = recover(&fs, opts(SyncMode::Durable, 1024));
    assert_eq!(replayed, acked);
}

/// Corruption in a sealed segment is refused, never truncated silently.
#[tokio::test(flavor = "multi_thread")]
async fn corruption_in_a_sealed_segment_refuses_to_open() {
    let fs = fs();
    let (wal, _) = recover(&fs, opts(SyncMode::Durable, 1024));
    for i in 0..40 {
        wal.append(payload(i), None).await.unwrap();
    }
    wal.close().unwrap();
    let first = Path::new(DIR).join(prkdb_core::wal::segment::segment_file_name(1));
    let f = fs.open(&first, OpenMode::ReadWrite).unwrap();
    f.write_at(prkdb_core::wal::segment::SEGMENT_HEADER_LEN + 30, &[0x5A; 8]).unwrap();
    f.sync_data().unwrap();
    let r = Wal::open(Arc::new(fs.clone()), Path::new(DIR), opts(SyncMode::Durable, 1024), 1, &mut |_, kind, _| {
        assert_eq!(kind, FrameKind::Batch);
        Ok(())
    });
    assert!(r.is_err(), "sealed-segment corruption must refuse to open");
}
```

`rand` and `rand_chacha` are already `prkdb-verify` dependencies. Run: `cargo nextest run -p prkdb-verify --test wal_power_loss` → fail (`todo!()`).

- [ ] **Step 4: Implement `log.rs`.** Structure (port `writer_loop` from the spike and extend):
  - `Shared` (in an `Arc`, read by `Wal` and the writer): `next_lsn: AtomicU64`, `durable_lsn: AtomicU64`, `health: RwLock<WalHealth>` (read on the probe path only), `segments: RwLock<BTreeMap<Lsn, Arc<dyn VfsFile>>>` (read handles for `read`/`scan_from`), `queued_bytes: AtomicUsize`, `last_progress_ms: AtomicU64`, `oldest_enqueued_ms: AtomicU64`, `admission: Arc<Semaphore>`.
  - `Request::Append { payload, hook, reply: Reply, _permit: OwnedSemaphorePermit }`, `Request::Sync { reply }`, `Request::Close { reply }`. `Reply` wraps `oneshot::Sender<Result<RecordLoc, WalError>>` with a `Drop` that sends `Err(WalError::Closed)` if never answered.
  - Channel: `std::sync::mpsc::channel` (unbounded, bounded in bytes by the semaphore).
  - Writer thread `prkdb-wal-writer` owns the active segment `(first_lsn, Arc<dyn VfsFile>, write_pos)` and a reusable `Vec<u8>` batch buffer. Loop: `recv()` (or `recv_timeout` while unsynced Fast data is outstanding), drain with `try_recv` up to `max_batch_bytes`, frame, roll if needed (invariant 5), one `write_at`, `sync_data` if Durable (or Fast and the interval elapsed), run hooks in order, update `durable_lsn`/`last_progress_ms`, answer replies. On error: set `Poisoned(cause)`, answer the batch and everything queued with `Poisoned`, then keep answering new requests with `Poisoned` until `Close`.
  - `health()`: `Stalled` when `queued_bytes > 0` and `now - last_progress_ms > max(1s, 100 × sync_interval)`; computed on demand, so an idle log performs no wakeups (liveness spec acceptance 1).
  - `Drop for Wal`: send `Close`, join; log (`tracing::warn!`) any error; never panic.
- [ ] **Step 5: Run** both test files → all pass. Run each five times (`for i in 1 2 3 4 5; do cargo nextest run -p prkdb-verify --test wal_power_loss || break; done`): they are deterministic (explicit syncs only), so one failure is a bug.
- [ ] **Step 6: Add real-`Wal` cells to the spike bench** (`wal_write_path_spike.rs`): a `Target::Wal(prkdb_core::wal::Wal)` whose `put` encodes a one-op `Batch` on the caller (as the adapter will) and calls `append(payload, None)`; cells `wal_durable` and `wal_fast`, opened with `WalOptions { segment_bytes: 256 MiB, max_batch_bytes: 16 MiB, max_queued_bytes: 64 MiB, sync_interval: 10 ms, .. }`. Keep every existing cell.
- [ ] **Step 7: Local sanity run** — `SPIKE_FILTER=/1w/ cargo bench -p prkdb --bench wal_write_path_spike` → `wal_fast/1w/1k` within 20 % of `single_log_fast/1w/1k` on the maintainer machine (the real `Wal` adds admission and hooks, not I/O).
- [ ] **Step 8: Commit** — `feat: add single ordered WAL with group-commit writer thread` (body: local bench rows from step 7).
- [ ] **Step 9: Linux ≤ 15 % rule (decision record risk 1).** Dispatch `probe=wal-bench` (no `base_ref`) at this commit. The Fast-rule step compares `wal_fast` against `current_mmap_wal` per cell.
  - All cells ok → record the run URL and the rule table in the decision record under a new heading "## 8. Linux re-run (Task 2.6)", commit `docs: record Linux WAL bench for the Fast rule`, continue.
  - A 1-writer cell fails → apply the mitigation from risk 1 once: after draining, spin on `try_recv` for up to 50 µs (`std::hint::spin_loop`, check elapsed every 64 iterations) before parking, and measure again. Commit it (`perf: spin briefly before the WAL writer parks`) only if it fixes the cell.
  - **STOP.** If any cell still loses more than 15 %, do not start Task 2.8. Report to the maintainer with both probe tables, the mitigation tried, and the fallback the spec names (sharded logs with a global sequence assigned at commit). Continue only on the maintainer's decision, recorded in the decision record.

---

### Task 2.7: Fast-mode sync placement (decision record risk 2)

The spike measured Fast p99 of 3–42 ms at 64 KiB because `pwrite` stalls while a 10 ms `F_FULLFSYNC` runs on the same file. Task 2.6 syncs inside the writer thread at batch boundaries. This task measures the alternative, a separate syncer thread, and keeps only the better one.

**Files:** `crates/prkdb-core/src/wal/log.rs`, `crates/prkdb/benches/wal_write_path_spike.rs`, `docs/remediation/decisions/2026-09-24-single-log-spike.md`

- [ ] **Step 1: Alternative behind a temporary option.** Add `pub fast_sync: FastSync` to `WalOptions` with `pub enum FastSync { InWriter, SyncerThread }` (default `InWriter`). `SyncerThread`: a second `std::thread` (`prkdb-wal-syncer`) that every `sync_interval` reads `written_lsn` (an `AtomicU64` the writer sets after each `write_at`), calls `sync_data` on the active segment handle (shared via `Arc<RwLock<Arc<dyn VfsFile>>>`, swapped by the writer on roll after it has synced the old segment itself), then raises `durable_lsn` to the value read before the sync. Its errors poison the log exactly like the writer's.
- [ ] **Step 2: Test both.** Parametrize `fast_appends_are_synced_within_the_interval_without_more_writes` and `fast_power_loss_keeps_a_prefix_no_shorter_than_the_last_sync` over both variants (loop over `[FastSync::InWriter, FastSync::SyncerThread]` inside each test). Run → pass.
- [ ] **Step 3: Bench cells** `wal_fast_inwriter` and `wal_fast_syncer` (replacing `wal_fast` for this run only). Dispatch `probe=wal-bench`. Compare p99 and p99.9 at 8w and 64w, 1 KiB and 64 KiB.
- [ ] **Step 4: Decide by rule, not by feel:** keep `SyncerThread` if it lowers p99.9 at 64w/64k by ≥ 25 % without losing > 5 % throughput in any cell; otherwise keep `InWriter` (simpler: one thread owns the file). Delete the losing variant, the `FastSync` enum and its option (no dead knobs), rename the bench cell back to `wal_fast`.
- [ ] **Step 5: Record** the table, run URL and choice in the decision record under "## 9. Fast sync placement (Task 2.7)".
- [ ] **Step 6: Commit** — `perf: choose Fast-mode sync placement from Linux measurements` (body: the p99/p99.9 table and the rule outcome).

---

### Task 2.8: `WalStorageAdapter` on the single `Wal` (STO-01, STO-02, STO-03, STO-04, STO-05, STO-08)

The switch. `WalStorageAdapter` keeps its public API (constructors, `StorageAdapter`, `flush`, `save_checkpoint`, `take_snapshot`, `get_changes_since`, raft appends, `write_path_health`) and replaces its internals: `MmapParallelWal`, the accumulator + flush loop + supervisor task, the legacy checkpoint and the `Compactor` hook all go. The other WAL implementations are deleted in Task 2.9, not here, so this commit series stays reviewable.

**Design:**
- **One write = one frame.** `put`, `put_batch`, `put_many`, `delete`, `delete_many`, the raft appends and (Task 2.19) outbox writes each encode one `Batch` on the caller's task and call `wal.append(payload, Some(hook))`. `put_batch`/`put_many` are one frame and one sync (decision record risk 4).
- **Publish in LSN order, on the writer thread (STO-03).** The hook captures the batch's keys and applies them to the index (`papaya::HashMap<Vec<u8>, RecordLoc>`) when the writer runs it. Hooks run in LSN order after the frame is durable (Durable) or written (Fast), so the live index can never hold an older location than recovery would compute, and in Durable mode it never exposes a write that a power cut could remove. The multi-key visibility guarantee (spec S-03) becomes a `parking_lot::RwLock<()>` taken for write by each hook for the duration of its index updates, and for read by `snapshot_get_many` while it resolves every key to a `RecordLoc` (no `.await` under the guard; values are read afterwards from those fixed locations).
- **The cache is a validated memo, not a second source of truth.** `ShardedLruCache<Vec<u8>, (Lsn, Vec<u8>)>`. `get`: look up the index → `loc`; if the cache holds `(loc.lsn, v)` return `v`, else `wal.read(loc)` → `Batch::decode` → the last op for the key → cache `(loc.lsn, value)`. Hooks never touch the cache, so a racing reader can at worst cache a value under an LSN the index no longer points to, which the next `get` ignores.
- **Recovery = full replay** (`Wal::open` with `replay_from = 1`), rebuilding the index. The legacy `checkpoint.json` is never read, which removes STO-01's data loss; Task 2.14 adds a correct snapshot.
- **Errors.** `WalError::Poisoned` → `StorageError::Internal("WAL poisoned: …")`; `WalError::Closed` → `StorageError::WriteAbandoned`; `RecordTooLarge` → `StorageError::Validation`. An append that does not complete within `LivenessBounds::client_bound` returns `StorageError::WriteNotConfirmed` (the write is with the writer and may still land). Waiting for admission permits past the bound returns `StorageError::WriteBackpressure` (nothing was queued, so the answer is definite).
- **No runtime needed to open.** `Wal::open` is synchronous, so `new`/`open`/`new_with_config`/`open_with_vfs` no longer call `block_in_place`; `open_async` runs the same code in `spawn_blocking` so a long replay does not block a runtime worker.
- **Old data directories are refused, not silently shadowed.** Until Task 2.11 adds the `FORMAT` marker, `open_inner` refuses a directory that contains `mmap_segment_0` with `StorageError::Corruption("data directory {dir} was created by an older PrkDB (format 1); this version reads format 2. See docs/guide/upgrade")`. Without this, the new log would start empty next to the old one and the database would look wiped.

**Files:**
- Modify: `crates/prkdb/src/storage/wal_adapter.rs`, `storage/recovery.rs` (rewrite: replay into the index), `storage/checkpoint.rs` (strip to what `save_checkpoint` still needs), `storage/config.rs`, `storage/cache.rs`, `storage/writer_liveness.rs`, `storage/collection_partitioned_adapter.rs` (constructor call sites, fault-injection tests), `crates/prkdb-core/src/wal/config.rs` (remove `segment_count`, `shard_count`), `crates/prkdb/src/builder.rs`, `crates/prkdb/tests/{tripwires.rs,durability.rs,wal_adapter_surface.rs}`, `crates/prkdb-verify/{src/sut.rs,src/bin/crash_child.rs,tests/harness.rs,tests/sigkill.rs}`, `crates/prkdb/benches/iai_hot_paths.rs`, `scripts/perf_gate_floors.toml`
- Create: `crates/prkdb-verify/tests/power_loss.rs`

- [ ] **Step 1: Failing tests — adapter under power loss** (`crates/prkdb-verify/tests/power_loss.rs`). These do not compile until `open_with_vfs` exists; that is the failing state.

```rust
//! The storage adapter under simulated power loss (Task 2.8): STO-02 and STO-04 end to end.

use prkdb::storage::config::StorageConfig;
use prkdb::storage::WalStorageAdapter;
use prkdb_core::vfs::{OpenMode, Vfs, VfsFile};
use prkdb_core::wal::{SyncMode, WalConfig};
use prkdb_types::storage::StorageAdapter;
use prkdb_verify::faultfs::{FaultFs, Tear};
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn config(mode: SyncMode) -> StorageConfig {
    let dir = PathBuf::from("/db/wal");
    StorageConfig {
        wal: WalConfig {
            log_dir: dir.clone(),
            sync_mode: mode,
            sync_interval_ms: 3_600_000, // Fast syncs only when asked: deterministic window
            segment_bytes: 16 * 1024,
            ..WalConfig::test_config()
        },
        ..StorageConfig::new(dir)
    }
}

fn open(fs: &FaultFs, mode: SyncMode) -> WalStorageAdapter {
    WalStorageAdapter::open_with_vfs(config(mode), Arc::new(fs.clone())).expect("open")
}

fn fresh() -> FaultFs {
    let fs = FaultFs::new();
    fs.mkdir_durable(Path::new("/db")).unwrap();
    fs
}

/// STO-02: an acknowledged Durable put survives power loss, whatever the tail looks like.
#[tokio::test(flavor = "multi_thread")]
async fn durable_put_survives_power_loss() {
    for tear in [Tear::None, Tear::Prefix, Tear::ZeroTail, Tear::Garbage] {
        let fs = fresh();
        let db = open(&fs, SyncMode::Durable);
        for i in 0..50u32 {
            db.put(format!("k{i}").as_bytes(), &i.to_le_bytes()).await.unwrap();
        }
        db.delete(b"k7").await.unwrap();
        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(1), tear);
        drop(db);
        let db = open(&fs, SyncMode::Durable);
        for i in 0..50u32 {
            let want = (i != 7).then(|| i.to_le_bytes().to_vec());
            assert_eq!(db.get(format!("k{i}").as_bytes()).await.unwrap(), want, "k{i} after {tear:?}");
        }
    }
}

/// STO-04 end to end: garbage after the last synced record is truncated at open, earlier
/// keys stay readable, and writes after the reopen survive the next power loss.
#[tokio::test(flavor = "multi_thread")]
async fn a_torn_adapter_tail_keeps_earlier_keys_and_accepts_new_writes() {
    let fs = fresh();
    let db = open(&fs, SyncMode::Fast);
    for i in 0..20u32 {
        db.put(format!("k{i}").as_bytes(), b"early").await.unwrap();
    }
    db.flush().await.unwrap(); // flush == sync: everything above is durable
    for i in 20..30u32 {
        db.put(format!("k{i}").as_bytes(), b"late").await.unwrap();
    }
    fs.power_loss(&mut ChaCha8Rng::seed_from_u64(5), Tear::Garbage);
    drop(db);

    let db = open(&fs, SyncMode::Durable);
    for i in 0..20u32 {
        assert_eq!(db.get(format!("k{i}").as_bytes()).await.unwrap().as_deref(), Some(&b"early"[..]));
    }
    db.put(b"after", b"reopen").await.unwrap();
    fs.power_loss(&mut ChaCha8Rng::seed_from_u64(6), Tear::None);
    drop(db);

    let db = open(&fs, SyncMode::Durable);
    assert_eq!(db.get(b"after").await.unwrap().as_deref(), Some(&b"reopen"[..]));
    for i in 0..20u32 {
        assert!(db.get(format!("k{i}").as_bytes()).await.unwrap().is_some());
    }
}

/// A sealed segment corrupted on disk refuses to open instead of losing data silently.
#[tokio::test(flavor = "multi_thread")]
async fn a_corrupt_sealed_segment_fails_the_open_by_name() {
    let fs = fresh();
    let db = open(&fs, SyncMode::Durable);
    for i in 0..400u32 {
        db.put(format!("k{i}").as_bytes(), &[1u8; 100]).await.unwrap();
    }
    drop(db);
    let seg = Path::new("/db/wal").join(prkdb_core::wal::segment::segment_file_name(1));
    let f = fs.open(&seg, OpenMode::ReadWrite).unwrap();
    f.write_at(64, &[0xEE; 16]).unwrap();
    f.sync_data().unwrap();
    let err = WalStorageAdapter::open_with_vfs(config(SyncMode::Durable), Arc::new(fs.clone()))
        .err()
        .expect("must refuse");
    assert!(err.to_string().contains("00000000000000000001.wal"), "{err}");
}
```

Run: `cargo nextest run -p prkdb-verify --test power_loss` → compile error (`open_with_vfs` missing, `sync_mode` not a `WalConfig` field until Task 2.6 — it is by now, so only the constructor is missing).

- [ ] **Step 2: Failing tests — ordering, segment size, old directories** (append to `crates/prkdb/tests/durability.rs`):

```rust
use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::{SyncMode, WalConfig};
use prkdb_types::storage::StorageAdapter;
use std::sync::Arc;

fn wal_config(dir: &Path, segment_bytes: u64) -> WalConfig {
    WalConfig {
        log_dir: dir.to_path_buf(),
        segment_bytes,
        sync_mode: SyncMode::Fast, // concurrency test: sync cost is not what is measured
        ..WalConfig::test_config()
    }
}

/// STO-03 + STO-05: with many writers racing on the same keys, the value a reader sees
/// before a restart is the value recovery produces after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_same_key_live_equals_recovered() {
    let dir = tempfile::tempdir().unwrap();
    let live = {
        let db = Arc::new(WalStorageAdapter::new(wal_config(dir.path(), 8 * 1024)).unwrap());
        let mut tasks = Vec::new();
        for w in 0..8u32 {
            let db = db.clone();
            tasks.push(tokio::spawn(async move {
                for round in 0..300u32 {
                    let key = format!("hot-{}", round % 4);
                    if round % 17 == w {
                        db.delete(key.as_bytes()).await.unwrap();
                    } else {
                        db.put(key.as_bytes(), format!("w{w}-r{round}").as_bytes()).await.unwrap();
                    }
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let mut live = Vec::new();
        for k in 0..4 {
            live.push(db.get(format!("hot-{k}").as_bytes()).await.unwrap());
        }
        db.flush().await.unwrap();
        live
    };
    let db = WalStorageAdapter::open_async(wal_config(dir.path(), 8 * 1024)).await.unwrap();
    for (k, want) in live.iter().enumerate() {
        assert_eq!(&db.get(format!("hot-{k}").as_bytes()).await.unwrap(), want, "hot-{k}");
    }
}

/// STO-08: `WalConfig::segment_bytes` decides when a segment rolls.
#[tokio::test(flavor = "multi_thread")]
async fn segment_bytes_is_honored() {
    let dir = tempfile::tempdir().unwrap();
    let db = WalStorageAdapter::new(wal_config(dir.path(), 64 * 1024)).unwrap();
    for i in 0..256u32 {
        db.put(format!("k{i}").as_bytes(), &vec![b'v'; 4096]).await.unwrap();
    }
    db.flush().await.unwrap();
    let segments: Vec<u64> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "wal"))
        .map(|p| std::fs::metadata(p).unwrap().len())
        .collect();
    assert!(segments.len() >= 16, "1 MiB of writes in 64 KiB segments: {segments:?}");
    assert!(segments.iter().all(|&len| len <= 64 * 1024), "a segment overran: {segments:?}");
}

/// Until the FORMAT marker (Task 2.11), a format-1 directory is refused by name, never
/// opened as an empty database next to the old log.
#[tokio::test(flavor = "multi_thread")]
async fn a_format_1_directory_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("mmap_segment_0")).unwrap();
    let err = WalStorageAdapter::new(wal_config(dir.path(), 1 << 20)).err().expect("must refuse");
    assert!(err.to_string().contains("format 1"), "{err}");
}
```

Run: `cargo nextest run -p prkdb --test durability` → `segment_bytes_is_honored` and `a_format_1_directory_is_refused` fail on the mmap path; `concurrent_same_key_live_equals_recovered` may pass by luck (STO-03 was never reproduced; the test stays as the regression test and the ordering argument is in the design above).

- [ ] **Step 3: Invert the STO-01 tripwires.** In `crates/prkdb/tests/tripwires.rs` replace `sto01_checkpoint_drops_pre_checkpoint_keys_tripwire` with:

```rust
/// STO-01 regression (was the tripwire): keys written before a checkpoint survive reopen.
#[tokio::test(flavor = "multi_thread")]
async fn sto01_checkpoint_keeps_pre_checkpoint_keys() {
    let dir = tempfile::tempdir().unwrap();
    {
        let a = WalStorageAdapter::new(wal_config(dir.path())).unwrap();
        for i in 0..5u8 {
            a.put(&[b'k', i], b"v").await.unwrap();
        }
        a.flush().await.unwrap();
        a.save_checkpoint().unwrap();
        a.put(b"after", b"checkpoint").await.unwrap();
    }
    let b = WalStorageAdapter::open_async(wal_config(dir.path())).await.unwrap();
    for i in 0..5u8 {
        assert_eq!(b.get(&[b'k', i]).await.unwrap().as_deref(), Some(&b"v"[..]), "k{i}");
    }
    assert_eq!(b.get(b"after").await.unwrap().as_deref(), Some(&b"checkpoint"[..]));
}
```

(The module doc keeps saying what a tripwire is; this file now also holds inverted tripwires, so add one sentence: "Inverted tripwires stay here under their regression names, so the history of each finding is in one file.")

In `crates/prkdb-verify/tests/harness.rs` replace `sto01_discovery_profile_finds_checkpoint_loss_tripwire` and `find_sto01_finding` with:

```rust
/// STO-01 regression (was the discovery tripwire): the discovery profile, which includes
/// `Checkpoint`, finds no checkpoint-shaped loss.
#[tokio::test(flavor = "multi_thread")]
async fn discovery_profile_checkpoint_keeps_every_key() {
    let report = run_seeds(WalSut::new, 0, 100, 60, Profile::Discovery)
        .await
        .expect("harness error");
    if let Some(f) = &report.failure {
        assert!(!is_sto01(f), "STO-01 is back: seed {} ops {:?}", f.seed, f.ops);
        panic!("discovery found a different failure; record it in the ledger: {f:?}");
    }
    assert!(report.checks > 0, "vacuous run");
}
```

- [ ] **Step 4: Config surgery.**
  - `crates/prkdb-core/src/wal/config.rs`: delete `segment_count` and `shard_count` (their only reader, `MmapParallelWal` construction, goes in step 6); fix every literal and every `config.segment_count` use found by `rg -n 'segment_count|shard_count' crates --glob '*.rs'` outside `crates/prkdb-core/src/wal/{mmap_parallel_wal,parallel_wal,async_parallel_wal}.rs` and the streaming/sharded adapters (deleted in 2.9; until then they keep a local `const SEGMENTS: usize = 4;`). In `crates/prkdb-verify` the `--segments` flag of `crash_child` and its use in `tests/sigkill.rs` go away: the single log has no segment count, and the mid-stream tests' "force a resize" purpose becomes "force a roll" via a small `segment_bytes` (set `segment_bytes: 8 * 1024 * 1024` for the 100 MB mid-stream run and update the doc comment's arithmetic).
  - `crates/prkdb/src/storage/config.rs`: delete the local `SyncMode` enum and `StorageConfig::sync_mode` (two knobs for one setting is root cause 4); add `pub use prkdb_core::wal::SyncMode;`. For source compatibility with the old variant name, add in `crates/prkdb-core/src/wal/config.rs` (an inherent impl must live in the defining crate):
    ```rust
    impl SyncMode {
        /// The old name for [`SyncMode::Fast`].
        #[deprecated(note = "renamed to SyncMode::Fast")]
        #[allow(non_upper_case_globals)]
        pub const Performance: SyncMode = SyncMode::Fast;
    }
    ```
  - `WalStorageAdapterBuilder::with_sync_mode` sets `self.config.wal.sync_mode`.

- [ ] **Step 5: Rewrite the adapter internals.** `WalStorageInner` becomes:

```rust
struct WalStorageInner {
    config: StorageConfig,
    wal: prkdb_core::wal::Wal,
    /// key -> location of the frame holding its latest put. Written only by commit hooks,
    /// which the WAL writer runs in LSN order (STO-03).
    index: Arc<LockFreeHashMap<Vec<u8>, RecordLoc>>,
    /// Held for write by a commit hook while it publishes one batch; for read while
    /// `snapshot_get_many` resolves its keys. Never held across an await.
    publish: Arc<parking_lot::RwLock<()>>,
    /// Highest LSN whose hook has run.
    applied_lsn: Arc<AtomicU64>,
    cache: Arc<ShardedLruCache<Vec<u8>, (Lsn, Vec<u8>)>>,
    outbox: Arc<LockFreeHashMap<String, Vec<u8>>>, // memory-only until Task 2.19 (EVT-02)
    metrics: Arc<StorageMetrics>,
    transaction_barrier: Arc<RwLock<()>>,
    bounds: LivenessBounds,
    recovery: Arc<RecoveryManager>,
}
```

Constructors all delegate to one function:

```rust
impl WalStorageAdapter {
    pub fn new(config: WalConfig) -> Result<Self, StorageError>;                 // StdVfs, create if missing
    pub fn new_with_config(config: StorageConfig) -> Result<Self, StorageError>; // StdVfs, create if missing
    pub fn open(config: WalConfig) -> Result<Self, StorageError>;                // StdVfs, create if missing
    pub async fn open_async(config: WalConfig) -> Result<Self, StorageError>;    // same, in spawn_blocking
    /// Opens on any `Vfs`: the harness passes `FaultFs`.
    pub fn open_with_vfs(config: StorageConfig, vfs: Arc<dyn Vfs>) -> Result<Self, StorageError>;
    fn open_inner(config: StorageConfig, vfs: Arc<dyn Vfs>) -> Result<Self, StorageError>;
}
```

(`open` used to fail on a missing directory; nothing depends on that — `rg -n 'WalStorageAdapter::open\(' crates` shows only reopen call sites — so all constructors now create-if-missing. Say so in `open`'s doc comment.)

`open_inner`: refuse a format-1 directory (design above); wrap `vfs` with `fault_injection::wrap(vfs, &log_dir)` under `#[cfg(test)]` (step 8); `Wal::open(vfs, &log_dir, WalOptions::from_config(&config.wal), 1, &mut replay)` where `replay` decodes each `Batch` and applies it to a fresh index (put → insert `loc`, delete → remove); set `applied_lsn` to the last replayed LSN; build the cache with `config.cache_capacity` (keeps STO-09 fixed).

Write path, one helper used by every mutating method:

```rust
    /// Encodes `batch`, appends it as one frame, and publishes it into the index from
    /// the WAL writer thread (LSN order) before returning its location.
    async fn commit(&self, batch: Batch) -> Result<RecordLoc, StorageError> {
        let payload = batch.encode(&self.inner.config.wal.compression).map_err(wal_err)?;
        let keys: Vec<(Vec<u8>, bool)> = batch.ops.into_iter().map(|op| match op {
            BatchOp::Put { key, .. } => (key, true),
            BatchOp::Delete { key } => (key, false),
        }).collect();
        let (index, publish, applied) =
            (self.inner.index.clone(), self.inner.publish.clone(), self.inner.applied_lsn.clone());
        let hook: CommitHook = Box::new(move |loc| {
            let _visible = publish.write();
            let pinned = index.pin();
            for (key, is_put) in keys {
                if is_put { pinned.insert(key, loc); } else { pinned.remove(&key); }
            }
            applied.store(loc.lsn, Ordering::Release);
        });
        match tokio::time::timeout(self.inner.bounds.client_bound, self.inner.wal.append(payload, Some(hook))).await {
            Ok(r) => r.map_err(wal_err),
            Err(_) => Err(StorageError::WriteNotConfirmed(format!(
                "no result from the WAL writer within {}ms", self.inner.bounds.client_bound.as_millis()))),
        }
    }
```

(Admission waits happen inside `wal.append`; split them out with a `Wal::reserve(len) -> Permit` if the timeout must distinguish "never queued" (`WriteBackpressure`) from "queued, unanswered" (`WriteNotConfirmed`). Do split: add `pub async fn reserve(&self, len: usize) -> Result<Reservation, WalError>` and `pub async fn append_reserved(&self, r: Reservation, payload, hook)` to `Wal` in this task, with `append` = reserve + append_reserved. Time the two separately.)

Metrics: keep `record_write`/`record_write_batch`/`record_read` calls at the same points. `put`/`put_batch`/`put_many`/`delete`/`delete_many` take `transaction_barrier.read()` as today; `put_batch_unlocked`/`delete_many_unlocked` skip it (transactions hold the write side).

Read path: `get` as in the design; `get_many` = `get` per key (the ≥ 100-key "full WAL scan" branch is deleted: it existed because reads were expensive on the mmap path and it is the kind of second read path that hides bugs); `snapshot_get_many` resolves locations under `publish.read()` then reads; `scan_prefix` and `scan_range` iterate the index (`pin_owned`), filter, sort, read; `get_all_keys` from the index; `get_changes_since(offset)` uses `wal.scan_from(offset + 1)` and expands each batch into `Change::Put`/`Change::Delete` with `version = loc.lsn`; `max_offset()` returns `applied_lsn`; `append_raft_entry`/`append_raft_entries_batch` commit a batch of `Put { key: b"__raft_log/" ++ uuid, value }` and return LSNs (one frame for the whole raft batch, so every entry gets the same LSN — `append_raft_entries_batch` returns `vec![loc.lsn; n]`; `wal_adapter_surface.rs::a_raft_batch_returns_one_result_per_entry` asserts length only, check it still passes); `flush()` = `wal.sync()`; `save_checkpoint()` = `wal.sync_blocking()` and nothing else until Task 2.14 (doc comment says so); `take_snapshot` unchanged in shape (iterate index, `get`, stream to the writer task).

`write_path_health()` maps `wal.health()`: `Healthy` → `healthy: true`; `Stalled{queued_bytes, oldest_ms}` → `healthy: false`, reason "WAL writer stalled: …", `queue_depth` = queued requests, `oldest_unpublished_age_ms = oldest_ms`; `Poisoned(r)` → `healthy: false`, reason r; `Closed` → `healthy: false`. `publishes_total` counts completed frames; `direct_appends_total` stays 0 (there is one path now; keep the field for the probe schema and say so in `WritePathHealth`'s doc).

`Drop for WalStorageAdapter`: nothing special — dropping the last `Arc<WalStorageInner>` drops the `Wal`, which closes (drain, sync, join). Delete `flush_on_last_handle_drop` and its helper thread.

Delete: `PendingWrite`, `WriterTasks`, `run_flush_loop`, `run_writer_supervisor`, `observe_write_path`, `fail_write_path`, `discharge_pending`, `discharge_report`, `enqueue_write(s)`, `await_write`, `flush_accumulator_inner`, `publish_batch`, `rebuild_index_async`, the `Compactor` field and its trigger, `new_with_replication` and the `replication` field (the core `ReplicationManager` it fed is deleted in Task 2.9; it has no production caller: `rg -n new_with_replication crates` shows only its own tests), `checkpoint_path`/`max_offset` fields. In `writer_liveness.rs` keep `LivenessBounds` and `unix_millis`; delete `WritePathProgress`, `SharedProgress` and `WriterFailure` if the compiler reports them unused (it should).

- [ ] **Step 6: Recovery and checkpoint modules.** `storage/recovery.rs`: `RecoveryManager` now holds `log_dir` and the `Arc<dyn Vfs>`; `check_health()` re-scans every segment with `segment::scan_segment` and maps any fault to `StorageError::Corruption`; `recover()` returns `StorageError::Recovery("run the database open path; torn tails are truncated there and mid-log corruption is not repaired automatically")` — the old `repair_segments` silently truncated at the first bad record anywhere, which the new rules forbid. `create_backup` unchanged. `storage/checkpoint.rs`: delete the JSON `Checkpoint` type, `save_checkpoint`/`load_checkpoint` and their tests (Task 2.14 writes the new file format in this module).

- [ ] **Step 7: Existing adapter tests.** Keep every test in `wal_adapter.rs`'s `mod tests` that tests behaviour a user can observe; retarget or delete the ones that test deleted machinery. Required mapping (apply by name):

| Old test | New form |
|---|---|
| `replication_constructor_uses_the_supplied_wal_config`, `test_wal_adapter_replication` | deleted with `new_with_replication` |
| `test_wal_adapter_compaction` | deleted (Task 2.15 adds a real compaction test) |
| `test_wal_adapter_auto_recovery_on_startup`, `test_wal_adapter_runtime_corruption_detection` | rewrite against the new rules: torn tail → opens, key before the tear readable; corrupt sealed segment → open fails naming the file |
| `dropping_the_last_handle_publishes_what_is_still_queued` | `dropping_the_last_handle_closes_the_log_durably`: `put_many` 100 pairs, drop, reopen, all present |
| `a_batch_reports_its_bytes_and_flush_publishes_what_is_queued` | keep the metrics assertions; "flush publishes" becomes "flush syncs" (`wal.durable_lsn() == applied_lsn` after `flush`) |
| `a_mixed_batch_applies_its_puts_and_deletes_in_order` | keep (one `Batch`, ops applied in order by the hook) |
| `queued_puts_and_deletes_keep_their_collection_names` | deleted: `LogOperation::collection` no longer exists; key namespacing is Task 2.12 |
| `a_write_is_refused_when_no_writer_was_started` | `a_write_after_close_is_refused` (`WalError::Closed` → `WriteAbandoned`) |
| `a_direct_write_is_counted_without_disturbing_the_stall_detector`, `an_idle_watchdog_does_not_wake_at_all`, `the_watchdog_returns_to_waiting_once_the_queue_drains`, `a_write_into_an_empty_queue_wakes_the_watchdog`, `a_discharge_of_nothing_is_silent` | deleted: there is no watchdog task and no direct/queued split; health is computed on demand (liveness spec acceptance 1 now holds by construction — say so in `write_path_health`'s doc) |
| `a_stale_index_entry_does_not_return_another_keys_value` | keep |
| `dropping_a_queued_write_answers_its_caller`, `taking_a_write_apart_disarms_the_drop_guard` | move to `log.rs` unit tests against `Reply`'s drop guard |
| `a_panicking_writer_discharges_its_waiters_with_the_panic` | `a_panicking_write_poisons_and_answers_every_waiter` via the test Vfs's `panic_writer_at` |
| `a_writer_that_publishes_nothing_is_detected_and_reported_unhealthy` | `a_stalled_writer_is_reported_unhealthy` via `stall_writer_at` (blocks `write_at`) |
| `a_working_writer_is_never_reported_as_stalled`, `the_not_confirmed_variant_survives_the_storage_adapter_boundary`, `the_write_path_publishes_the_numbers_that_show_a_stall_forming` | keep, retargeted at `Wal` counters |
| `a_full_queue_refuses_new_writes_instead_of_growing` | `a_full_queue_makes_writers_wait_then_refuses` (small `max_queued_bytes`, stalled writer, the extra write returns `WriteBackpressure` after the client bound) |
| `a_failed_append_does_not_count_as_a_publish` | `a_failed_append_is_never_visible` (`fail_append_at` → put errors, `get` returns `None`, later writes `Poisoned`) |
| `dropping_an_adapter_stops_its_background_tasks_promptly` | `dropping_an_adapter_joins_the_writer_thread` (drop returns within 1 s; `prkdb-wal-writer` thread gone) |

Then re-read `docs/superpowers/specs/2026-08-11-wal-writer-liveness.md` "Acceptance" and confirm each item maps to a test above; list the mapping in the commit body.

- [ ] **Step 8: Test-only fault injection through `Vfs`.** Keep the `fault_injection` module's public test API and its directory-keyed registry (so `collection_partitioned_adapter.rs` tests keep working unchanged), but implement the writer faults as a `Vfs` wrapper instead of hooks in the deleted flush loop:
  - `fail_flush_at`/`clear_flush_failure`: unchanged (checked at the top of `WalStorageAdapter::flush`, before the WAL; it tests that wrappers forward errors, and must not poison the log).
  - `fail_append_at`: the wrapper's `write_at` returns `Err` for files under the directory (poisons the log, as a real disk error would).
  - `stall_writer_at`/`clear_writer_stall`: `write_at` blocks on a `Condvar` until cleared.
  - `panic_writer_at`: `write_at` panics.
  - `never_start_writer_at`: deleted with its test.
  `open_inner` applies `#[cfg(test)] let vfs: Arc<dyn Vfs> = Arc::new(fault_injection::FaultInjectingVfs::new(vfs, log_dir.clone()));`. The `queue_depths_sum_across_collections` test keeps passing because `queue_depth` is now the WAL's queued-request count.

- [ ] **Step 9: Other callers.**
  - `crates/prkdb/tests/wal_adapter_surface.rs`: the four tests that write compressed batches through `MmapParallelWal` directly (`compressed_batches_round_trip`, `compressed_batch_deletes_are_applied`, `scan_range_reads_compressed_batches`, `get_changes_since_expands_compressed_batches`, `scan_prefix_honours_a_compressed_batch_delete`, `get_many_reads_each_key_out_of_a_compressed_batch`) produce compressed frames through the adapter instead: `compressing_adapter` already sets LZ4; write with `put_batch` (≥ `min_compress_bytes`) and assert the same things. `the_adapter_accessors_report_real_state` asserts `max_offset()` advances and `save_checkpoint()` makes `durable_lsn == max_offset` instead of asserting a JSON file appears.
  - `crates/prkdb-verify/src/sut.rs`: unchanged API; `WalSut::crash` comment updated ("drop now closes the log, which syncs; this is a clean process exit, and power loss is `PowerLoss` from Task 2.10").
  - `crates/prkdb/benches/iai_hot_paths.rs`: WAL fixtures use a `current_thread` runtime (the adapter no longer needs `block_in_place`), so no idle worker threads are counted; replace `bench_log_record_encode`/`decode` with `bench_batch_encode`/`bench_batch_decode` (one 1 KiB put, `Batch::encode`/`decode`); update `scripts/perf_gate_floors.toml` references to the new names. The perf gate will show these as new benchmarks (no base comparison), which the deltas script already handles.
  - `crates/prkdb/src/storage/collection_partitioned_adapter.rs`: `WalStorageAdapter::new` no longer needs `spawn_blocking` (it does not call `block_in_place`); keep `spawn_blocking` anyway, because a replay can be long — only fix the comment that justifies it.
- [ ] **Step 10: Run everything.**
  - `cargo build --workspace --all-targets` → clean.
  - `cargo nextest run -p prkdb-verify --test power_loss --test wal_power_loss` → pass.
  - `cargo nextest run -p prkdb --test durability --test tripwires --test wal_adapter_surface` → pass.
  - `cargo nextest run --workspace` → pass. If wall time grew more than 2× on the maintainer machine, list the slowest tests (`cargo nextest run --workspace --final-status-level slow`) and switch only those that do not test durability to `SyncMode::Fast` with a comment (Phase 2 conventions).
  - `cargo xtask verify --profile blocking --seeds 200` and `--profile discovery --seeds 200` → both green (discovery now includes a working `Checkpoint`).
  - `cargo nextest run -p prkdb-verify --test sigkill` five times → green.
- [ ] **Step 11: Local bench delta** (Conventions): `cargo bench -p prkdb --bench storage_bench -- --baseline before` (baseline saved before step 5) → table in the commit body.
- [ ] **Step 12: Linux adapter rule.** Dispatch `probe=wal-bench` with `ref` = this task's head and `base_ref` = the commit before step 5. The rule compares `current_adapter_put` (new adapter, Fast is not the default — the bench builds it from `test_config()`, so set `sync_mode: SyncMode::Fast` for the adapter cells in the bench as part of this task) against the base run's `current_adapter_put` (old adapter). **STOP** if any cell loses more than 15 %: report both tables to the maintainer before the ledger step.
- [ ] **Step 13: Ledger.** Set `fixed` with these `regression_tests` (and `changes` in the follow-up commit):
  - STO-01: `test:crates/prkdb/tests/tripwires.rs::sto01_checkpoint_keeps_pre_checkpoint_keys`, `test:crates/prkdb-verify/tests/harness.rs::discovery_profile_checkpoint_keeps_every_key`; clear `tripwire`; remove the harness tripwire from `evidence`.
  - STO-02: `test:crates/prkdb-verify/tests/power_loss.rs::durable_put_survives_power_loss`, `test:crates/prkdb-verify/tests/wal_power_loss.rs::durable_acks_survive_power_loss_with_any_tear`, `test:crates/prkdb-core/tests/wal_log.rs::durable_appends_are_synced_before_the_ack`.
  - STO-03: `test:crates/prkdb/tests/durability.rs::concurrent_same_key_live_equals_recovered`, `test:crates/prkdb-core/tests/wal_log.rs::commit_hooks_run_in_lsn_order`.
  - STO-04: `test:crates/prkdb-verify/tests/wal_power_loss.rs::torn_tail_is_truncated_and_later_appends_survive`, `…::a_new_segment_is_durable_after_roll`, `…::corruption_in_a_sealed_segment_refuses_to_open`, `test:crates/prkdb-verify/tests/power_loss.rs::a_torn_adapter_tail_keeps_earlier_keys_and_accepts_new_writes`.
  - STO-05: `test:crates/prkdb-core/tests/wal_log.rs::replay_order_equals_append_order_across_segment_roll`, `test:crates/prkdb/tests/durability.rs::concurrent_same_key_live_equals_recovered`.
  - STO-08: `test:crates/prkdb/tests/durability.rs::segment_bytes_is_honored`.
  - STO-01 is critical: re-render `docs/status/repo-status.md` (`cargo xtask repo-status render`) if the Verification dimension changes (pre-push script note). STO-02 is critical too.
- [ ] **Step 14: Commits** (one series): `test: add power-loss, ordering and segment-size tests for the WAL adapter` (steps 1–3, failing), `refactor: remove WalConfig segment_count and shard_count` (step 4), `fix: run WalStorageAdapter on the single ordered WAL` (steps 5–10; body: bench delta, probe URL, liveness acceptance mapping), then `docs: record STO-01..STO-05 and STO-08 as fixed`.

---

### Task 2.9: Delete the other WAL implementations (STO-06)

**Files:** delete `crates/prkdb-core/src/wal/{mmap_parallel_wal,mmap_log_segment,parallel_wal,async_parallel_wal,async_log_segment,log_segment,write_ahead_log,offset_index,compaction}.rs` and whatever else the compiler then reports unused in `wal/` (candidates: `async_fsync.rs`, `adaptive.rs` if `WalConfig` no longer needs it, `buffer_pool.rs` in `wal/`, `metrics.rs` if only the deleted WALs used it); `crates/prkdb-core/src/replication/{follower_server,manager,replica_client,protocol}.rs` + `crates/prkdb-core/tests/replication_integration_tests.rs` + `crates/prkdb-core/benches/{replication_bench,parallel_wal_bench,async_parallel_wal_bench,mmap_parallel_wal_bench,wal_bench,wal_recovery_bench,wal_random_read_bench,wal_single_write_bench,batching_bench}.rs` (their `[[bench]]` entries too, keeping any that no longer reference deleted types); `crates/prkdb-core/tests/format_version.rs`; `crates/prkdb/src/storage/{sharded_wal_adapter,streaming_adapter,partitioned_streaming_adapter,write_queue}.rs`; `crates/prkdb/examples/{raw_wal_bench,streaming_bench,partitioned_bench}.rs` and the streaming/sharded sections of `comprehensive_bench.rs`/`ultra_performance.rs`.

- [ ] **Step 1: STOP — confirm the deletion list with the maintainer.** Public API disappears: `prkdb::storage::{ShardedWalAdapter, StreamingStorageAdapter, StreamingConfig, StreamingRecord, PartitionedStreamingAdapter, PartitionedStreamingConfig, PartitionStrategy}` and `prkdb_core::replication::{FollowerServer, ReplicationManager, ReplicaClient, …}` (the core leader/follower replication fed only by the `new_with_replication` constructor deleted in Task 2.8; `prkdb::replication` is a separate module and stays). Recommendation: delete all of it — each exists to showcase the parallel mmap WAL (spec 2a: "Delete the other WAL implementations after migrating their callers"; these adapters *are* the callers, and a second, unverified log path per data directory is root cause 4). If the maintainer wants the streaming adapters kept, port them instead: `StreamingStorageAdapter` becomes a thin wrapper over `Wal` (`append_batch(records)` → one `append` of the encoded records, returning the LSN; `read_from(offset)` → `wal.scan_from(offset)`), and `PartitionedStreamingAdapter` holds one such wrapper per partition directory. Record the decision in the commit body.
- [ ] **Step 2: Inventory.** `rg -lw 'ParallelWal|AsyncParallelWal|MmapParallelWal|WriteAheadLog|MmapLogSegment|LogSegment|AsyncLogSegment|OffsetIndex|Compactor' crates --glob '*.rs'` → expected before this task: only the files listed above plus `wal/mod.rs` and `wal_write_path_spike.rs`.
- [ ] **Step 3: The spike bench becomes the comparison bench.** Rename `crates/prkdb/benches/wal_write_path_spike.rs` → `wal_write_path.rs` (and its `[[bench]]`), delete the `SingleLog` prototype, `current_mmap_wal` and `two_shard_fast` cells; keep `wal_durable`, `wal_fast`, `current_adapter_put` (renamed `adapter_put`), `model_memcpy_only`, and the device ceilings. Update `scripts/wal_fast_rule.py`: base/head mode compares the cell named `adapter_put` or `current_adapter_put` (whichever each table has), and head-only mode compares `wal_fast` against the `wal_fast` rows recorded in the decision record §8 (`--reference docs/remediation/decisions/2026-09-24-single-log-spike.md`), since the old path it used to compare against no longer exists. Update `remediation-gate.yml`'s `probe-wal-bench` job to run `wal_write_path`. Update the decision record's "Bench" line with the new path.
- [ ] **Step 4: Delete, then build.** Remove the files, the `pub mod`/`pub use` lines in `wal/mod.rs`, `replication/mod.rs` (keep the re-exports of `prkdb_types::replication` types if anything still uses them; else delete the module), `storage/mod.rs`, and the `[[bench]]`/`[[example]]` entries. `LogRecord`/`LogOperation` go too if the compiler reports no users (after Task 2.8 only the deleted modules and `write_queue.rs` used them). `cargo build --workspace --all-targets` → clean.
- [ ] **Step 5: Regression check that nothing re-adds a second log.** Create `scripts/check_single_wal.sh`:

```bash
#!/usr/bin/env bash
# STO-06 regression: exactly one WAL implementation. Fails if a deleted type returns.
set -euo pipefail
cd "$(dirname "$0")/.."
if rg -nw 'ParallelWal|AsyncParallelWal|MmapParallelWal|WriteAheadLog|MmapLogSegment|AsyncLogSegment' crates --glob '*.rs'; then
  echo "a second WAL implementation is back (STO-06)"; exit 1
fi
test "$(rg -l 'impl Wal \{' crates/prkdb-core/src/wal | wc -l | tr -d ' ')" = "1"
```

`chmod +x`; run → exit 0. Add `step single-wal; bash scripts/check_single_wal.sh` to `scripts/pre-push-check.sh`.
- [ ] **Step 6: Tests** — `cargo nextest run --workspace` → pass; harness blocking 200 seeds → green.
- [ ] **Step 7: Ledger** — STO-06 `fixed`, `regression_tests = ["script:scripts/check_single_wal.sh", "test:crates/prkdb-core/tests/wal_log.rs::replay_order_equals_append_order_across_segment_roll"]`.
- [ ] **Step 8: Commit** — `refactor: delete the parallel, async, mmap and legacy WAL implementations` (body: the maintainer's decision from step 1, deleted public types). Follow-up `docs: record STO-06 as fixed`.

---

### Task 2.10: `PowerLoss` and Fast mode in the harness (TST-05)

§7.1 row "Phase 2 (after 2a)": the blocking profile gains `PowerLoss` (via `Vfs`) and runs in Durable and Fast. This task applies the Phase 1 harness review's constraints (see the note kept below) before adding the op.

> **Design constraints from the Phase 1 harness review (apply before adding ops):**
> - **Model answers "acceptable values", not one value.** Fast mode (acked-but-unsynced writes may or may not survive) and Phase 3 transactions break exact equality. Change `Model` to track, per key, the durable value plus pending values since the last sync/checkpoint/clean reopen, and `Mismatch.expected` to that acceptable set. Do this in 2.5, before the Fast profile lands, or the checker gets rewritten twice.
> - **`Sut` grows without breaking implementations.** Either a single `async fn apply(&mut self, op: &Op) -> anyhow::Result<OpResult>` plus `get`, or new methods with default bodies returning `Unsupported` that the runner treats as a profile mismatch. `PowerLoss` needs the `FaultFs` handle and a seeded rng: derive one `ChaCha8Rng` stream per concern (workload, faults) from the seed in the runner, never ad hoc.
> - **Op coverage is reported.** Count executed ops per kind in `Report`, so a green run can't hide a disabled op.
> - The Phase 1 FaultFs already models: per-directory durable entries (including subdirectories), inode-reusing truncating `create`, stale handles after power loss (epoch), and `Tear::{None, Prefix, ZeroTail, Garbage}` with sector-granular tearing of in-place overwrites.

How this task meets them: the model keeps a durable state plus an ordered list of pending mutations and exposes every **prefix** state as a candidate — stronger than per-key acceptable sets, because the WAL can only lose a suffix (spec §7 checker: "SUT state equals the model at some prefix no earlier than the last completed sync"). `Mismatch` keeps `expected` (the full-model value, so existing code and reports keep working) and gains `acceptable`. `Sut` grows by default-bodied methods returning a typed `Unsupported` error, which the runner reports as a harness error, never as a finding. Fault randomness is carried inside the op (`fault_seed`), so replay and minimization are deterministic.

**Files:**
- Modify: `crates/prkdb-verify/src/{model.rs,ops.rs,sut.rs,checker.rs,runner.rs,bin/verify.rs}`, `crates/prkdb-verify/tests/{harness.rs,self_test.rs}`, `xtask/src/verify.rs` (usage text only), `.github/workflows/{ci.yml,remediation-gate.yml}`, `scripts/pre-push-check.sh`

- [ ] **Step 1: Failing tests** (append to `crates/prkdb-verify/tests/harness.rs`; adjust imports to `use prkdb_verify::model::Mode; use prkdb_verify::sut::FaultSut; use prkdb_verify::faultfs::Tear; use prkdb_verify::runner::{run, RunConfig};`):

```rust
/// A Fast-mode SUT that loses data it had already synced must be caught: the checker
/// may only accept prefixes that start at the last durable point.
struct ForgetsSyncedKey {
    inner: FaultSut,
}

#[async_trait::async_trait]
impl Sut for ForgetsSyncedKey {
    async fn put(&mut self, k: &Key, v: &Value) -> anyhow::Result<()> { self.inner.put(k, v).await }
    async fn delete(&mut self, k: &Key) -> anyhow::Result<()> { self.inner.delete(k).await }
    async fn get(&mut self, k: &Key) -> anyhow::Result<Option<Value>> { self.inner.get(k).await }
    async fn reopen(&mut self) -> anyhow::Result<()> { self.inner.reopen().await }
    async fn crash(&mut self) -> anyhow::Result<()> { self.inner.crash().await }
    async fn checkpoint(&mut self) -> anyhow::Result<()> { self.inner.checkpoint().await }
    async fn power_loss(&mut self, tear: Tear, fault_seed: u64) -> anyhow::Result<()> {
        self.inner.power_loss(tear, fault_seed).await?;
        // Simulates a WAL that discarded synced data: key 0 vanishes whatever the model says.
        self.inner.delete(&prkdb_verify::ops::key(0)).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn fast_mode_checker_catches_lost_synced_data() {
    let cfg = RunConfig { first_seed: 0, seeds: 40, ops: 60, profile: Profile::Blocking, mode: Mode::Fast, repro_attempts: 1 };
    let report = run(|| async { Ok(ForgetsSyncedKey { inner: FaultSut::new(Mode::Fast).await? }) }, &cfg)
        .await
        .expect("harness error");
    let f = report.failure.expect("a SUT that loses synced data must be caught in Fast mode");
    assert!(f.ops.iter().any(|o| matches!(o, Op::PowerLoss { .. })), "{:?}", f.ops);
}

#[test]
fn blocking_profile_includes_power_loss() {
    assert!((0..20).any(|s| prkdb_verify::ops::generate(s, 200, Profile::Blocking)
        .iter()
        .any(|o| matches!(o, Op::PowerLoss { .. }))));
    assert!((0..50).all(|s| !prkdb_verify::ops::generate(s, 200, Profile::Core)
        .iter()
        .any(|o| matches!(o, Op::PowerLoss { .. }))), "Core stays the Phase 1 op set");
}

#[tokio::test(flavor = "multi_thread")]
async fn blocking_profile_is_green_in_fast_mode() {
    let cfg = RunConfig { first_seed: 0, seeds: 20, ops: 60, profile: Profile::Blocking, mode: Mode::Fast, repro_attempts: 1 };
    let report = run(|| FaultSut::new(Mode::Fast), &cfg).await.expect("harness error");
    assert!(report.failure.is_none(), "{:?}", report.failure);
    assert!(report.op_counts.get("PowerLoss").copied().unwrap_or(0) > 0, "PowerLoss never ran: {:?}", report.op_counts);
}
```

Change `blocking_profile_is_green_on_current_code` (keep the name: TST-03 cites it) to run `FaultSut::new(Mode::Durable)` with `Profile::Blocking` through `run(..)`, and additionally assert `op_counts["PowerLoss"] > 0`. Change `meta_harness_catches_a_lossy_sut` to wrap `FaultSut` (forward `power_loss`), and `discovery_profile_checkpoint_keeps_every_key` to use `FaultSut::new(Mode::Durable)` (Discovery now includes `PowerLoss`, which `WalSut` does not support). Run: `cargo nextest run -p prkdb-verify --test harness` → compile errors (`Mode`, `FaultSut`, `RunConfig`, `Op::PowerLoss`, `Profile::Core` do not exist).

- [ ] **Step 2: Model** (`model.rs`), replacing `kv`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode { Durable, Fast }

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mutation { Put(Key, Value), Delete(Key) }

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Model {
    /// State as of the last point the SUT certainly made durable.
    pub durable: BTreeMap<Key, Value>,
    /// Acknowledged mutations since then, oldest first.
    pub pending: Vec<Mutation>,
    pub touched: BTreeSet<Key>,
}

impl Model {
    pub fn put(&mut self, k: Key, v: Value);          // touched + pending.push
    pub fn delete(&mut self, k: &Key);                // touched + pending.push
    /// The state if every acknowledged mutation survived.
    pub fn state(&self) -> BTreeMap<Key, Value>;
    pub fn get(&self, k: &Key) -> Option<Value>;      // from state()
    /// `durable` with the first `n` pending mutations applied, for n in 0..=pending.len().
    pub fn prefix(&self, n: usize) -> BTreeMap<Key, Value>;
    /// A clean reopen, flush or checkpoint happened: everything pending is durable.
    pub fn mark_durable(&mut self);
    /// Power loss kept exactly the first `n` pending mutations.
    pub fn settle(&mut self, n: usize);
}
```

Update the two model unit tests to use `state()`.

- [ ] **Step 3: Ops and profiles** (`ops.rs`): add `Op::PowerLoss { tear: Tear, fault_seed: u64 }` and `Kind::PowerLoss`; add `Profile::Core` (the Phase 1 blocking table, unchanged, so self-test seeds reproduce exactly as before) and retune:

```rust
const CORE_WEIGHTS: &[(Kind, u32)] = &[(Kind::Put, 60), (Kind::Delete, 20), (Kind::Reopen, 10), (Kind::Crash, 10)];
const BLOCKING_WEIGHTS: &[(Kind, u32)] =
    &[(Kind::Put, 55), (Kind::Delete, 20), (Kind::Reopen, 8), (Kind::Crash, 8), (Kind::PowerLoss, 9)];
const DISCOVERY_WEIGHTS: &[(Kind, u32)] = &[
    (Kind::Put, 55), (Kind::Delete, 20), (Kind::Reopen, 8), (Kind::Checkpoint, 5), (Kind::Crash, 5), (Kind::PowerLoss, 7),
];
```

The generator draws `tear` (`Tear::random`) and `fault_seed` (`rng.gen()`) from a second `ChaCha8Rng` seeded with `seed ^ 0xFA17_FA17_FA17_FA17` (one stream per concern), only when it emits a `PowerLoss`, so workload draws for a given seed are unaffected by fault draws. `Profile::parse` accepts `core`. Update `blocking_never_checkpoints` to cover `Core` too.

- [ ] **Step 4: SUT** (`sut.rs`):

```rust
/// Returned by `Sut` methods a SUT does not implement. The runner treats it as a harness
/// error (the profile asked for an op this SUT cannot do), never as a finding.
#[derive(Debug)]
pub struct Unsupported(pub &'static str);
impl std::fmt::Display for Unsupported { /* "SUT does not support {0}" */ }
impl std::error::Error for Unsupported {}

// Added to trait Sut, with a default body:
    async fn power_loss(&mut self, _tear: Tear, _fault_seed: u64) -> anyhow::Result<()> {
        Err(Unsupported("power_loss").into())
    }

/// The WAL adapter on `FaultFs` (Durable or Fast), for power-loss testing.
pub struct FaultSut { fs: FaultFs, mode: Mode, db: Option<WalStorageAdapter> }

impl FaultSut {
    pub async fn new(mode: Mode) -> anyhow::Result<Self>;   // FaultFs::new(), mkdir_durable("/db"), open
    fn config(&self) -> StorageConfig;  // log_dir "/db/wal", segment_bytes 16 KiB (rolls often),
                                        // sync_mode from mode, sync_interval_ms 3_600_000 in Fast
                                        // (explicit syncs only: deterministic), rest test_config()
    fn open(&mut self) -> anyhow::Result<()>;  // WalStorageAdapter::open_with_vfs(config, Arc::new(fs.clone()))
}
```

`FaultSut`'s `put/delete/get` forward; `reopen` = `flush` (sync) + drop + open; `crash` = drop + open (process exit: written data stays in FaultFs's page cache); `power_loss(tear, seed)` = `fs.power_loss(&mut ChaCha8Rng::seed_from_u64(seed), tear)` **first**, then drop the adapter (its handles are stale, so the drop-time sync fails harmlessly and cannot make anything durable after the fact), then open; `checkpoint` = `save_checkpoint`. `WalSut` stays (StdVfs) for `Profile::Core`, the self-tests and `sigkill.rs`.

- [ ] **Step 5: Checker and runner.**
  - `Mismatch` gains `pub acceptable: Vec<Option<Value>>`.
  - `check_durable(model, sut)` compares against `model.state()` (`acceptable = vec![expected]`).
  - New `check_after_power_loss(model: &mut Model, sut, mode) -> CheckOutcome`: read the SUT value of every checked key once; Durable → compare with `state()`; Fast → find the largest `n` in `(0..=pending.len()).rev()` with `prefix(n) == sut_state` restricted to checked keys; on success `model.settle(n)`, on failure report the first key where the SUT differs from `state()`, with `acceptable` = that key's value in every prefix.
  - `RunConfig { first_seed, seeds, ops, profile, mode, repro_attempts }` and `pub async fn run(make, &RunConfig) -> anyhow::Result<Report>`. `run_seeds`/`run_seeds_with` stay as wrappers with `mode: Durable` (existing callers compile unchanged).
  - `run_ops` takes `mode`: after an acked op in Durable mode nothing changes (every ack is durable, so the full state is the only candidate); after `Reopen` or `Checkpoint` → `model.mark_durable()`; after `Crash` → `check_durable` (a process exit loses nothing that was written) and, in Durable mode, `mark_durable()`; after `PowerLoss` → `check_after_power_loss`. A `Sut` error that downcasts to `Unsupported` returns `Err` from `run_ops` (harness error).
  - `Report` gains `op_counts: BTreeMap<&'static str, u64>` (executed ops per kind, including the implicit trailing reopen as `"Reopen"`); `Outcome::same_kind` compares `acceptable` shape as it does `expected`.
- [ ] **Step 6: Self-tests** (`self_test.rs`): replace `Profile::Blocking` with `Profile::Core` (they wrap `WalSut`, which has no `power_loss`; `Core` reproduces the seeds they were tuned on). Add one self-test that a `Sut` without `power_loss` run under `Profile::Blocking` produces `Err` mentioning "does not support power_loss", not a finding.
- [ ] **Step 7: Binary and xtask.** `verify` accepts `--mode durable|fast` (default durable) and `--sut fault|std` (default `fault` for `blocking`/`discovery`, `std` for `core`), builds `RunConfig`, prints `profile=<p> mode=<m> seeds=<n> checks=<c> ops=<Put:…,Delete:…,…>`, and fails a run in which any op kind enabled by the profile has count 0 ("vacuous for <kind>"). Update the usage text in the binary and `xtask/src/verify.rs`'s doc.
- [ ] **Step 8: CI and gate.** `ci.yml` `harness` job: run both `cargo xtask verify --profile blocking --seeds 200 --mode durable` and `… --mode fast`. `nightly-harness`: both modes. `remediation-gate.yml`: add a job `harness-fast`, a copy of `harness` with `--mode fast` and `if: ${{ inputs.probe == 'none' && contains(fromJSON('["2","3","4"]'), inputs.phase) }}` (Fast mode exists from Phase 2 on), and make `harness-result` need both jobs, accepting `skipped` for `harness-fast` only when the phase is outside that list. `pre-push-check.sh`: add the Fast run after the Durable one.
- [ ] **Step 9: Run** — `cargo nextest run -p prkdb-verify` → all pass; `cargo xtask verify --profile blocking --seeds 200 --mode durable` and `--mode fast` → green, `PowerLoss` count > 0 in both. If a seed fails, it is a real finding in Tasks 2.5–2.8's code: minimize, fix, add a regression test at the WAL level, re-run (not the demotion rule: PowerLoss is the op this phase exists to pass).
- [ ] **Step 10: Ledger** — TST-05 `fixed`, `regression_tests = ["test:crates/prkdb-verify/tests/harness.rs::blocking_profile_is_green_on_current_code", "test:crates/prkdb-verify/tests/harness.rs::blocking_profile_is_green_in_fast_mode", "test:crates/prkdb-verify/tests/harness.rs::fast_mode_checker_catches_lost_synced_data"]`. Add the same two harness tests to STO-02's and STO-04's `regression_tests` (§7.1: this row proves STO-02, STO-04, TST-05).
- [ ] **Step 11: Commit** — `feat: add power loss and Fast mode to the crash/restart harness`; follow-up `docs: record TST-05 as fixed`.

---

### Task 2.11: Format v2 marker, open rules, migration registry, `prkdb-cli migrate` (D3, D4)

The format-version mechanism that existed before this phase (`LogSegment`'s `PRKDBWAL` header with `FORMAT_VERSION = 1`, tested by `crates/prkdb-core/tests/format_version.rs`) was deleted with `LogSegment` in Task 2.9. The new segment header (Task 2.5) and the `FORMAT` file below both use `prkdb_core::format::FORMAT_VERSION`, so the program ends this task with exactly one version number, checked at two levels.

**Files:**
- Create: `crates/prkdb/src/storage/format.rs`, `crates/prkdb/src/storage/migrations.rs`, `crates/prkdb/tests/format_v2.rs`, `crates/prkdb-cli/src/commands/migrate.rs`, `crates/prkdb-cli/tests/migrate.rs`
- Modify: `crates/prkdb/src/storage/mod.rs`, `crates/prkdb/src/storage/wal_adapter.rs` (`open_inner`), `crates/prkdb/src/storage/collection_partitioned_adapter.rs` (root marker), `crates/prkdb-types/src/error.rs` (variant), `crates/prkdb-cli/src/{main.rs,commands.rs}`

- [ ] **Step 1: Failing tests** — `crates/prkdb/tests/format_v2.rs`:

```rust
use prkdb::storage::format::{read_format, FORMAT_FILE};
use prkdb::storage::WalStorageAdapter;
use prkdb_core::format::FORMAT_VERSION;
use prkdb_core::wal::WalConfig;

fn cfg(dir: &std::path::Path) -> WalConfig {
    WalConfig { log_dir: dir.to_path_buf(), ..WalConfig::test_config() }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_directory_is_created_as_format_2() {
    let dir = tempfile::tempdir().unwrap();
    drop(WalStorageAdapter::new(cfg(dir.path())).unwrap());
    let text = std::fs::read_to_string(dir.path().join(FORMAT_FILE)).unwrap();
    assert!(text.contains("format = 2"), "{text}");
    assert!(text.contains(&format!("created_by = \"{}\"", env!("CARGO_PKG_VERSION"))), "{text}");
    assert_eq!(read_format(dir.path()).unwrap().unwrap().format, FORMAT_VERSION);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_format_2_directory_reopens() {
    let dir = tempfile::tempdir().unwrap();
    drop(WalStorageAdapter::new(cfg(dir.path())).unwrap());
    WalStorageAdapter::open_async(cfg(dir.path())).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_empty_directory_without_format_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("something.log"), b"old data").unwrap();
    let err = WalStorageAdapter::new(cfg(dir.path())).err().expect("must refuse");
    let msg = err.to_string();
    assert!(msg.contains("format") && msg.contains("docs/guide/upgrade"), "{msg}");
    assert!(!dir.path().join(FORMAT_FILE).exists(), "refusal must not write anything");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_newer_format_is_refused_by_number() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(FORMAT_FILE), "format = 3\ncreated_by = \"9.9.9\"\n").unwrap();
    let err = WalStorageAdapter::new(cfg(dir.path())).err().expect("must refuse");
    let msg = err.to_string();
    assert!(msg.contains("newer PrkDB (format 3)") && msg.contains("reads format 2"), "{msg}");
}
```

`crates/prkdb-cli/tests/migrate.rs`:

```rust
use std::process::Command;

fn migrate(dir: &std::path::Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_prkdb-cli"))
        .args(["migrate", "--data-dir", dir.to_str().unwrap()])
        .output()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn migrate_on_a_current_directory_says_there_is_nothing_to_do() {
    let dir = tempfile::tempdir().unwrap();
    drop(prkdb::storage::WalStorageAdapter::new(prkdb_core::wal::WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..prkdb_core::wal::WalConfig::test_config()
    }).unwrap());
    let out = migrate(dir.path());
    assert!(out.status.success(), "{out:?}");
    assert!(String::from_utf8_lossy(&out.stdout).contains("no migrations available for format 2"), "{out:?}");
}

#[test]
fn migrate_on_a_format_1_directory_fails_and_explains() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("mmap_segment_0")).unwrap();
    let out = migrate(dir.path());
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("format 1") && err.contains("docs/guide/upgrade"), "{err}");
}
```

(`prkdb-cli` needs `tempfile` and `tokio` as dev-dependencies; check `crates/prkdb-cli/Cargo.toml` and add what is missing.) Run both → fail.

- [ ] **Step 2: `format.rs`.**

```rust
//! The data-directory format marker (spec §7 2b, D3).

pub const FORMAT_FILE: &str = "FORMAT";
pub use prkdb_core::format::FORMAT_VERSION;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatMarker { pub format: u32, pub created_by: String }

/// Reads `dir/FORMAT` without creating anything. `Ok(None)` if the file does not exist.
pub fn read_format(dir: &Path) -> Result<Option<FormatMarker>, StorageError>;

/// The open rules: an absent or empty directory is created as format 2 (FORMAT written
/// atomically: `FORMAT.tmp` create → write → sync_data → rename → sync_dir); `FORMAT == 2`
/// opens; anything else is refused before a single byte is written.
pub fn ensure_format(vfs: &dyn Vfs, dir: &Path) -> Result<FormatMarker, StorageError>;
```

File syntax, parsed by hand (two `key = value` lines; unknown keys ignored so a later version can add fields): `format = 2` and `created_by = "<CARGO_PKG_VERSION>"`. Errors are a new `StorageError::UnsupportedFormat(String)` variant (`#[error("{0}")]`) whose text follows spec 2b exactly: `data directory {dir} was created by an older PrkDB (format {n}); this version reads format 2. See docs/guide/upgrade.` (older, including "no FORMAT file: format 1"), or `… by a newer PrkDB (format {n}) …`. "Empty" means `read_dir` returns nothing; a directory holding only `FORMAT.tmp` (a crash during creation) counts as empty and the stale temp file is removed first.

In `WalStorageAdapter::open_inner`: `ensure_format(vfs.as_ref(), &log_dir)?` before `Wal::open`, replacing Task 2.8's `mmap_segment_0` guard (its test keeps passing: that directory is non-empty without `FORMAT`). `CollectionPartitionedAdapter::new` calls `ensure_format` on its root before creating `collections/` (each per-collection directory gets its own `FORMAT` through its adapter).

- [ ] **Step 3: `migrations.rs`.**

```rust
//! Migration registry (spec D4). Empty in format 2; format 3 ships its migrator here.

pub trait Migration: Send + Sync {
    fn from(&self) -> u32;
    fn to(&self) -> u32;
    fn description(&self) -> &str;
    /// Must leave `dir` either fully at `to()` (FORMAT rewritten last, atomically) or untouched.
    fn run(&self, dir: &Path) -> Result<(), StorageError>;
}

pub fn registry() -> Vec<Box<dyn Migration>> { Vec::new() }

/// The chain of migrations from `found` to `FORMAT_VERSION`, or an error naming the gap.
pub fn plan(found: u32) -> Result<Vec<Box<dyn Migration>>, StorageError>;
```

- [ ] **Step 4: CLI.** `commands/migrate.rs`: `#[derive(Args, Clone, Debug)] pub struct MigrateArgs { #[arg(long)] pub data_dir: PathBuf, #[arg(long)] pub dry_run: bool }` and `pub fn handle_migrate(args: MigrateArgs) -> anyhow::Result<()>`: `read_format`; `Some(2)` → print `data directory {dir} is at format 2; no migrations available for format 2` and succeed; `None` on a non-empty directory → treat as format 1; any other `n` → `plan(n)`: empty plan → error `no migrations available for format {n} → 2; format {n} directories cannot be converted by this version. See docs/guide/upgrade.`; non-empty → list them (dry run) or run them in order. Register: `pub mod migrate;` in `commands.rs`, a `Migrate(migrate::MigrateArgs)` variant in `Commands` (doc comment `/// Upgrade a data directory to this version's format (offline)`) and its match arm (no `init_database_manager`, it is offline).
- [ ] **Step 5: Run** — `cargo nextest run -p prkdb --test format_v2` and `cargo nextest run -p prkdb-cli --test migrate` → pass; `cargo nextest run --workspace` → pass; harness 200 seeds both modes → green.
- [ ] **Step 6: Commit** — `feat: add format v2 marker, open rules and migrate command`.

---

### Task 2.12: Key codec and collection catalog (KEY-01)

Spec 2c: `[namespace_len u8][namespace][collection_id u32 BE][key bytes]`, where `collection_id` comes from a persisted catalog keyed by the collection's persisted name. `IndexedStorage` today stores `serde_json(id)` with no namespace at all (KEY-01); `CollectionHandle` prefixes `std::any::type_name::<C>()`, which changes with module paths and compiler versions. Both move to the codec. Extracting the codec from `indexed_storage.rs` follows spec §9.

**Persisted name.** Add to `prkdb_types::collection::Collection`:

```rust
    /// The name this collection is stored under. Recorded in the catalog at first use;
    /// changing it makes the data unreachable, so the derive pins it: `#[collection(name =
    /// "...")]`, or the struct name in snake_case. Manual impls get the snake_case of the
    /// last path segment of the type name.
    fn persisted_name() -> std::borrow::Cow<'static, str>
    where
        Self: Sized,
    {
        std::borrow::Cow::Owned(crate::collection::default_persisted_name(std::any::type_name::<Self>()))
    }
```

and `pub fn default_persisted_name(type_name: &str) -> String` (strip generics, take the segment after the last `::`, CamelCase → snake_case). The `Collection` derive (`prkdb-macros`) overrides it with `#[collection(name = "...")]` (the attribute is already declared and currently unused) or `snake_case(ident)`, emitted as `Cow::Borrowed("…")`. The name is `persisted_name`, not `collection_name`, because the derive also implements `ProtoSchema::collection_name`, and two trait methods with one name make `T::collection_name()` ambiguous. **Spec deviation to record:** spec 2c says renaming the type does not change the persisted name; with a derived default it does, unless `#[collection(name)]` pins it — the catalog cannot know two names are the same collection. Document on the trait and in the spec's revision history at this task.

**Files:**
- Create: `crates/prkdb/src/keys.rs`, `crates/prkdb/src/catalog.rs`, `crates/prkdb/tests/key_codec.rs`
- Modify: `crates/prkdb-types/src/collection.rs`, `crates/prkdb-macros/src/lib.rs`, `crates/prkdb/src/lib.rs`, `crates/prkdb/src/indexed_storage.rs`, `crates/prkdb/src/collection_handle.rs`, `crates/prkdb/src/db.rs` (catalog on `PrkDb`), `crates/prkdb/src/consumer.rs` (collection names), `crates/prkdb/tests/tripwires.rs`

- [ ] **Step 1: Invert the tripwire into the failing regression test.** In `tripwires.rs` replace `key01_collections_share_primary_keys_tripwire` with:

```rust
/// KEY-01 regression (was the tripwire): same id in two collections, through get,
/// query, delete and a restart.
#[tokio::test(flavor = "multi_thread")]
async fn key01_collections_with_same_id_are_independent() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = IndexedStorage::new(Arc::new(WalStorageAdapter::new(wal_config(dir.path())).unwrap()));
        db.insert(&TwUser { id: 1, name: "Alice".into() }).await.unwrap();
        db.insert(&TwProject { id: 1, name: "Project".into() }).await.unwrap();
        assert_eq!(db.get::<TwUser>(&1).await.unwrap().unwrap().name, "Alice");
        assert_eq!(db.get::<TwProject>(&1).await.unwrap().unwrap().name, "Project");
        assert_eq!(db.query_by::<TwUser>("name", &"Alice").await.unwrap().len(), 1);
        assert!(db.query_by::<TwUser>("name", &"Project").await.unwrap().is_empty());
        db.delete(&TwProject { id: 1, name: "Project".into() }).await.unwrap();
        assert!(db.get::<TwProject>(&1).await.unwrap().is_none());
        assert_eq!(db.get::<TwUser>(&1).await.unwrap().unwrap().name, "Alice");
        db.inner().flush().await.unwrap();
    }
    let db = IndexedStorage::new(Arc::new(WalStorageAdapter::open_async(wal_config(dir.path())).await.unwrap()));
    assert_eq!(db.get::<TwUser>(&1).await.unwrap().unwrap().name, "Alice");
    assert!(db.get::<TwProject>(&1).await.unwrap().is_none());
}
```

`crates/prkdb/tests/key_codec.rs`:

```rust
use prkdb::catalog::Catalog;
use prkdb::keys::{collection_prefix, decode_key, encode_key, CollectionId, SYSTEM_COLLECTION};
use prkdb::storage::{InMemoryAdapter, WalStorageAdapter};
use prkdb_core::wal::WalConfig;
use prkdb_types::collection::Collection;
use std::sync::Arc;

#[test]
fn the_codec_is_the_documented_layout() {
    let k = encode_key(b"ns", CollectionId(7), b"id").unwrap();
    assert_eq!(k, [&[2u8][..], b"ns", &[0, 0, 0, 7], b"id"].concat());
    assert_eq!(decode_key(&k).unwrap(), (&b"ns"[..], CollectionId(7), &b"id"[..]));
    assert!(k.starts_with(&collection_prefix(b"ns", CollectionId(7))));
    assert!(!encode_key(b"ns", CollectionId(70), b"").unwrap().starts_with(&collection_prefix(b"ns", CollectionId(7))));
    assert!(encode_key(&[0u8; 256], CollectionId(1), b"x").is_err(), "namespace longer than 255 bytes");
}

#[tokio::test]
async fn catalog_ids_are_stable_and_distinct() {
    let cat = Catalog::new(Arc::new(InMemoryAdapter::new()), Vec::new());
    let users = cat.id_for_name("users").await.unwrap();
    let orders = cat.id_for_name("orders").await.unwrap();
    assert_ne!(users, orders);
    assert_ne!(users, SYSTEM_COLLECTION);
    assert_eq!(cat.id_for_name("users").await.unwrap(), users);
}

#[tokio::test(flavor = "multi_thread")]
async fn catalog_ids_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = || WalConfig { log_dir: dir.path().to_path_buf(), ..WalConfig::test_config() };
    let (a, b) = {
        let cat = Catalog::new(Arc::new(WalStorageAdapter::new(cfg()).unwrap()), Vec::new());
        (cat.id_for_name("alpha").await.unwrap(), cat.id_for_name("beta").await.unwrap())
    };
    let cat = Catalog::new(Arc::new(WalStorageAdapter::open_async(cfg()).await.unwrap()), Vec::new());
    assert_eq!(cat.id_for_name("beta").await.unwrap(), b);
    assert_eq!(cat.id_for_name("alpha").await.unwrap(), a);
    let gamma = cat.id_for_name("gamma").await.unwrap();
    assert!(gamma != a && gamma != b, "an id was reused after restart");
}

#[test]
fn persisted_names_are_snake_case_and_pinnable() {
    #[derive(prkdb_macros::Collection, serde::Serialize, serde::Deserialize, Clone, Debug)]
    struct UserProfile { #[id] id: u64 }
    #[derive(prkdb_macros::Collection, serde::Serialize, serde::Deserialize, Clone, Debug)]
    #[collection(name = "people")]
    struct Person { #[id] id: u64 }
    assert_eq!(UserProfile::persisted_name(), "user_profile");
    assert_eq!(Person::persisted_name(), "people");
    assert!(Catalog::validate_name("Bad Name").is_err());
}
```

Run → the tripwire test fails on the `TwProject` overwrite; `key_codec.rs` does not compile.

- [ ] **Step 2: `keys.rs`.**

```rust
//! Key codec (spec §7 2c): [ns_len u8][ns][collection_id u32 BE][key bytes].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CollectionId(pub u32);
/// Reserved for the catalog itself.
pub const SYSTEM_COLLECTION: CollectionId = CollectionId(0);
pub fn encode_key(ns: &[u8], coll: CollectionId, key: &[u8]) -> Result<Vec<u8>, StorageError>;
pub fn collection_prefix(ns: &[u8], coll: CollectionId) -> Vec<u8>;
pub fn decode_key(bytes: &[u8]) -> Result<(&[u8], CollectionId, &[u8]), StorageError>;
/// Primary-key bytes for an id: bincode standard config (what CollectionHandle used).
pub fn encode_id<I: serde::Serialize>(id: &I) -> Result<Vec<u8>, StorageError>;
```

- [ ] **Step 3: `catalog.rs`.**

```rust
pub struct Catalog {
    storage: Arc<dyn StorageAdapter>,
    ns: Vec<u8>,
    cache: DashMap<String, CollectionId>,
    alloc: tokio::sync::Mutex<()>,
}
impl Catalog {
    pub fn new(storage: Arc<dyn StorageAdapter>, ns: Vec<u8>) -> Self;
    /// `^[a-z][a-z0-9_]{0,63}$`.
    pub fn validate_name(name: &str) -> Result<(), StorageError>;
    /// Returns the id for `name`, allocating and persisting one on first use.
    pub async fn id_for_name(&self, name: &str) -> Result<CollectionId, StorageError>;
    pub async fn id_for<C: Collection>(&self) -> Result<CollectionId, StorageError>;
    pub async fn list(&self) -> Result<Vec<(String, CollectionId)>, StorageError>;
}
```

Storage: entry `encode_key(ns, SYSTEM_COLLECTION, b"catalog/name/" ++ name)` → `id u32 BE`; counter `encode_key(ns, SYSTEM_COLLECTION, b"catalog/next")` → `u32 BE`, starting at 1. Allocation under `alloc`: read counter, **write the bumped counter first, then the entry** (two plain `put`s, so it is safe on adapters without atomic batches: a crash between them wastes an id, never reuses one). `IndexedStorage::new(storage)` builds a `Catalog` with an empty namespace; `PrkDb` builds one with its namespace in `builder.rs::finish` and exposes `pub(crate) fn catalog(&self)`.

- [ ] **Step 4: Use the codec everywhere a record key is built.** `rg -n 'serde_json::to_vec\((record\.)?id\(?\)?|serde_json::to_vec\(id\)|type_name::<T>\(\)' crates/prkdb/src/indexed_storage.rs` lists the sites (13 primary-key sites plus the collection-name sites for the in-memory index maps). Add to `IndexedStorage`:

```rust
    async fn primary_key<T: Collection>(&self, id: &T::Id) -> Result<Vec<u8>, StorageError> {
        let coll = self.catalog.id_for::<T>().await?;
        crate::keys::encode_key(&[], coll, &crate::keys::encode_id(id)?)
    }
```

and replace every site; key the in-memory index maps by `T::persisted_name()` instead of `type_name`. Functions that list a collection (`all`, `count`, `filter`, `query_active`, `snapshot`, …) that currently iterate the in-memory index or the whole store switch to `storage.scan_prefix(&collection_prefix(&[], coll))`. The transaction path (`Transaction::commit`, ~line 3309) builds keys with the same helper. In `collection_handle.rs`, `get_namespaced_key` becomes `encode_key(db.namespace or [], db.catalog().id_for::<C>(), encode_id(id))` — the partition is no longer part of the key (partitions are logical, spec 2c; the partition is still computed for the outbox stream and metrics), and `apply_namespace` is deleted (the namespace is inside the codec). `consumer.rs`'s `collection_name` values (`type_name::<C>()`) become `C::persisted_name()`, and so do the outbox/DLQ prefixes in `outbox.rs` (`make_outbox_id_for_type` keeps its counter until Task 2.20; only the name changes here).
- [ ] **Step 5: Run** — `cargo nextest run -p prkdb --test tripwires --test key_codec --test indexed_db_tests` → pass; `cargo nextest run --workspace` → pass (fix any test that constructed raw keys by hand to use the public API or the codec); harness 200 seeds both modes → green.
- [ ] **Step 6: Ledger** — KEY-01 `fixed`, `regression_tests = ["test:crates/prkdb/tests/tripwires.rs::key01_collections_with_same_id_are_independent", "test:crates/prkdb/tests/key_codec.rs::catalog_ids_survive_restart"]`, clear `tripwire`. Spec: add a revision-history row for the persisted-name deviation above.
- [ ] **Step 7: Commit** — `fix: namespace every record key by a persisted collection id` (body: perf delta for `indexed_storage` insert/get — the catalog lookup is a cached `DashMap` hit after first use). Follow-up `docs: record KEY-01 as fixed`.

---

### Task 2.13: Stable partitioner (KEY-03)

**Files:** `crates/prkdb/src/partitioning.rs`, `crates/prkdb/src/raft/partitioner.rs`, `crates/prkdb/tests/tripwires.rs`, `crates/prkdb/tests/partitioning_tests.rs`

- [ ] **Step 1: Invert the tripwire.** Replace `key03_partition_differs_across_processes_tripwire` with the same child-process harness asserting the opposite:

```rust
/// KEY-03 regression (was the tripwire): a key's partition is the same in every process.
#[test]
fn key03_partition_is_stable_across_processes() {
    use prkdb::partitioning::{DefaultPartitioner, Partitioner};
    if is_child("key03_partition_is_stable_across_processes") {
        let p = DefaultPartitioner::<String>::new().partition(&"user-42".to_string(), 1_000_000);
        println!("CHILD_RESULT={p}");
        return;
    }
    let results: Vec<String> = (0..3)
        .map(|_| child_result("key03_partition_is_stable_across_processes"))
        .collect();
    assert!(results.windows(2).all(|w| w[0] == w[1]), "partition changed across processes: {results:?}");
}
```

Run → fails (ahash is randomly seeded).

- [ ] **Step 2: Fix.** In `DefaultPartitioner::partition`, `let mut hasher = seahash::SeaHasher::new();` (seahash's fixed default seeds; `seahash` is already a dependency) instead of `AHasher::default()`; drop the `ahash` import. Update the struct doc: "stable across processes, machines and releases for the key types' `Hash` output; golden vectors in `partitioning_tests.rs` fail CI if the mapping changes." Same bug, same fix, in the legacy `raft/partitioner.rs::Partitioner::get_partition` (line ~176, `ahash::AHasher::default()` over `&[u8]`): use `seahash::hash(key)` like `ConsistentHashRing` in the same file. Remove `ahash` from `crates/prkdb/Cargo.toml` if nothing else uses it.
- [ ] **Step 3: Golden vectors.** Create `crates/prkdb/examples/print_partition_golden.rs` (a generator, not a test, so it needs no `#[ignore]`, which `scripts/check_ignore_reasons.sh` would reject):

```rust
//! Prints the KEY-03 golden vectors. Run once when the vectors are created:
//! `cargo run -p prkdb --example print_partition_golden`.
use prkdb::partitioning::{DefaultPartitioner, Partitioner};
fn main() {
    let p = DefaultPartitioner::<String>::new();
    for i in 0..10 {
        let k = format!("user-{i}");
        println!("(\"{k}\", {}),", p.partition(&k, 1024));
    }
    println!("const GOLDEN_U64_42: u32 = {};", DefaultPartitioner::<u64>::new().partition(&42u64, 1024));
}
```

Run it and paste its output into a new test in `partitioning_tests.rs`:

```rust
/// KEY-03: changing the key → partition mapping moves every existing key. If this fails,
/// the change needs a registered migration (spec D4), not new vectors.
#[test]
fn default_partitioner_golden_vectors() {
    use prkdb::partitioning::{DefaultPartitioner, Partitioner};
    // Output of `cargo run -p prkdb --example print_partition_golden`, pasted verbatim.
    const GOLDEN: [(&str, u32); 10] = [/* ten ("user-N", partition) pairs */];
    const GOLDEN_U64_42: u32 = 0; // replaced by the printed value
    let p = DefaultPartitioner::<String>::new();
    for (k, want) in GOLDEN {
        assert_eq!(p.partition(&k.to_string(), 1024), want, "{k}");
    }
    assert_eq!(DefaultPartitioner::<u64>::new().partition(&42u64, 1024), GOLDEN_U64_42);
}
```

(The placeholders are replaced by the generator's output before the test is committed; the committed test contains only literal numbers.) Run it a second time in a fresh process to confirm the values do not move (`key03_partition_is_stable_across_processes` also covers this).
- [ ] **Step 4: Run** — `cargo nextest run -p prkdb --test tripwires --test partitioning_tests` → pass; workspace → pass.
- [ ] **Step 5: Ledger** — KEY-03 `fixed`, `regression_tests = ["test:crates/prkdb/tests/tripwires.rs::key03_partition_is_stable_across_processes", "test:crates/prkdb/tests/partitioning_tests.rs::default_partitioner_golden_vectors"]`, clear `tripwire`. (KEY-03 is exempt from `harness`, spec §4.1.)
- [ ] **Step 6: Commit** — `fix: use a fixed-seed partitioner so keys keep their partition`; follow-up `docs: record KEY-03 as fixed`.

---

### Task 2.14: Checkpoint = index snapshot

Task 2.8 made recovery correct by always replaying the whole log. This task makes it fast again without giving up correctness: `save_checkpoint` writes a snapshot of the index at a known LSN, and recovery loads it and replays only what follows. The invariant (spec 2d) is `recover(checkpoint, wal) == recover(∅, wal)` for every log, proven by a property test.

**Design:**
- **Fuzzy snapshot.** Read `covered = applied_lsn` (every frame ≤ `covered` is published), then iterate the index while writes continue. Entries published after `covered` may or may not be in the snapshot; replaying every frame with LSN > `covered` on top makes the result exact, because replay applies puts and deletes in LSN order and the last one wins. No writer pause.
- **Only durable frames may be referenced.** After iterating, `wal.sync_blocking()`; the snapshot is written only after the log covering every entry in it is durable.
- **File:** `checkpoints/index-{covered:020}.ckpt`, written atomically through `Vfs` (`.tmp` → `sync_data` → `rename` → `sync_dir`), then older checkpoints removed (+ `sync_dir`). Layout: `b"PRKDBCKP" | format u32 (FORMAT_VERSION) | covered u64 | entries u64 | entries… | crc32 u32` where an entry is `klen u32 | key | lsn u64 | segment u64 | offset u64 | payload_len u32` and the CRC covers every preceding byte.
- **Load rules (spec §8 "refuse to open, rather than misread" does not apply here, because a full replay is always available):** newest checkpoint first; a bad CRC, wrong magic/format, a `covered` beyond the log's last LSN, or a referenced segment that does not exist → `warn!` naming the file, ignore it, try the next older one, else full replay.
- **Compaction (Task 2.15) invalidates locations.** It deletes checkpoints before moving any record and writes a fresh one afterwards; this task only has to make loading robust.

**Files:**
- Modify: `crates/prkdb/src/storage/checkpoint.rs` (new format), `crates/prkdb/src/storage/recovery.rs` (load + replay from `covered + 1`), `crates/prkdb/src/storage/wal_adapter.rs` (`save_checkpoint`, `open_inner`), `crates/prkdb-core/src/wal/log.rs` (nothing new if `Wal::open` already takes `replay_from`)
- Create: `crates/prkdb-verify/tests/checkpoint.rs`

- [ ] **Step 1: Failing tests** — `crates/prkdb-verify/tests/checkpoint.rs`:

```rust
//! Checkpoints are an optimisation, never a source of truth (spec §7 2d):
//! recover(checkpoint, wal) == recover(∅, wal).

use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::{SyncMode, WalConfig};
use prkdb_types::storage::StorageAdapter;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::collections::BTreeMap;
use std::path::Path;

fn cfg(dir: &Path) -> WalConfig {
    WalConfig { log_dir: dir.to_path_buf(), segment_bytes: 16 * 1024, sync_mode: SyncMode::Fast, ..WalConfig::test_config() }
}

async fn contents(db: &WalStorageAdapter) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut out = BTreeMap::new();
    for k in db.get_all_keys() {
        out.insert(k.clone(), db.get(&k).await.unwrap().expect("indexed key readable"));
    }
    out
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let p = e.unwrap().path();
        let dest = to.join(p.file_name().unwrap());
        if p.is_dir() { copy_dir(&p, &dest) } else { std::fs::copy(&p, &dest).unwrap(); }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn recovery_from_a_checkpoint_equals_full_replay() {
    for seed in 0..30u64 {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let dir = tempfile::tempdir().unwrap();
        {
            let db = WalStorageAdapter::new(cfg(dir.path())).unwrap();
            for i in 0..400u32 {
                let key = format!("k{}", rng.gen_range(0..40)).into_bytes();
                match rng.gen_range(0..10) {
                    0..=5 => db.put(&key, format!("s{seed}-{i}").as_bytes()).await.unwrap(),
                    6..=7 => db.delete(&key).await.unwrap(),
                    8 => db.save_checkpoint().unwrap(),
                    _ => db.put_batch(vec![(key.clone(), vec![i as u8; 64]), (b"batch".to_vec(), vec![1])]).await.unwrap(),
                }
            }
            db.flush().await.unwrap();
        }
        let replay_dir = tempfile::tempdir().unwrap();
        copy_dir(dir.path(), replay_dir.path());
        let _ = std::fs::remove_dir_all(replay_dir.path().join("checkpoints"));

        let with_ckpt = contents(&WalStorageAdapter::open_async(cfg(dir.path())).await.unwrap()).await;
        let full = contents(&WalStorageAdapter::open_async(cfg(replay_dir.path())).await.unwrap()).await;
        assert_eq!(with_ckpt, full, "seed {seed}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_checkpoint_is_actually_used() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = WalStorageAdapter::new(cfg(dir.path())).unwrap();
        for i in 0..1000u32 {
            db.put(format!("k{i}").as_bytes(), b"v").await.unwrap();
        }
        db.save_checkpoint().unwrap();
        db.put(b"tail", b"v").await.unwrap();
    }
    let db = WalStorageAdapter::open_async(cfg(dir.path())).await.unwrap();
    let r = db.last_recovery();
    assert!(r.checkpoint_lsn.is_some(), "{r:?}");
    assert!(r.frames_replayed <= 2, "replayed {} frames after a checkpoint at the tail", r.frames_replayed);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_corrupt_checkpoint_falls_back_to_full_replay() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = WalStorageAdapter::new(cfg(dir.path())).unwrap();
        for i in 0..100u32 {
            db.put(format!("k{i}").as_bytes(), b"v").await.unwrap();
        }
        db.save_checkpoint().unwrap();
    }
    let ckpt = std::fs::read_dir(dir.path().join("checkpoints")).unwrap().next().unwrap().unwrap().path();
    let mut bytes = std::fs::read(&ckpt).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    std::fs::write(&ckpt, bytes).unwrap();
    let db = WalStorageAdapter::open_async(cfg(dir.path())).await.unwrap();
    assert_eq!(db.last_recovery().checkpoint_lsn, None);
    assert_eq!(db.get_all_keys().len(), 100);
}
```

`last_recovery()` is new public API returning `RecoveryStats { checkpoint_lsn: Option<Lsn>, frames_replayed: u64, truncated: Option<String> }` (useful to operators too: log it at `info!` on open). Run → `a_checkpoint_is_actually_used` and `a_corrupt_checkpoint_falls_back_to_full_replay` fail to compile (`last_recovery`); the property test passes today (full replay) and must keep passing — it is the regression test for this task's risk.

- [ ] **Step 2: Implement** `checkpoint.rs` (`pub fn write_checkpoint(vfs, dir, covered, entries: impl Iterator<Item = (&[u8], RecordLoc)>) -> Result<PathBuf, StorageError>`, `pub fn load_newest(vfs, dir, log_last_lsn, segment_exists: &dyn Fn(Lsn) -> bool) -> Option<(Lsn, Vec<(Vec<u8>, RecordLoc)>)>`, `pub fn decode_checkpoint(bytes: &[u8]) -> Result<(Lsn, Vec<(Vec<u8>, RecordLoc)>), StorageError>` — the last one is also a fuzz target in Task 2.23), `save_checkpoint` per the design, and in `open_inner`: list segments first (cheap directory read) to answer `segment_exists`, load the newest valid checkpoint into the index, then `Wal::open(.., replay_from = covered + 1, ..)`. `Wal::open` still *scans* every segment to verify CRCs (STO-04 must hold with or without a checkpoint) but only calls `replay` for LSN ≥ `replay_from`; if scan cost matters later, that is a measured optimisation, not this task.
- [ ] **Step 3: Run** — `cargo nextest run -p prkdb-verify --test checkpoint` → pass; `cargo xtask verify --profile discovery --seeds 200` (includes `Checkpoint`) → green; workspace → pass.
- [ ] **Step 4: Recovery bench** — `cargo bench -p prkdb --bench recovery_bench` before/after (the Phase 1 bench times `open_async` over a large log): with a checkpoint at the tail, recovery time drops; put both numbers in the commit body.
- [ ] **Step 5: Ledger** — add `test:crates/prkdb-verify/tests/checkpoint.rs::recovery_from_a_checkpoint_equals_full_replay` to STO-01's `regression_tests`.
- [ ] **Step 6: Commit** — `feat: checkpoint the index so recovery replays only the tail`.

---

### Task 2.15: Real compaction

`MmapParallelWal::truncate_before` was a stub and the `Compactor` that called it is gone (Task 2.9). Spec 2d: "compaction rewrites live records into new segments, fsyncs, then removes old segments; `truncate_before` becomes real or is deleted" — it is deleted; this task adds compaction.

**Design:**
- Only **sealed** segments are compacted (never the active one), one run at a time (`Mutex<()>`).
- A frame is **live** if any of its ops is the current index entry for its key (`index[key].lsn == frame.lsn`), or (from Task 2.19) holds an outbox entry still present. Live frames are rewritten with only their live ops, **keeping their LSN**. Dead frames become `Elided` frames (17-byte header, empty payload) so every segment keeps contiguous LSNs and recovery's continuity check is unchanged. Trade-off to record: an elided record still costs 17 bytes until its segment becomes entirely elided and is removed from the front of the log.
- Per chosen segment: write `{first:020}.wal.compact` (header + frames) → `sync_data` → **delete every checkpoint + `sync_dir`** (their locations are about to change) → `rename` over `{first:020}.wal` → `sync_dir` → swap the `Wal`'s read handle for that segment → update the index with compare-and-set (only entries whose `lsn` still equals the rewritten frame's LSN get the new `offset`; a key overwritten meanwhile keeps its newer location) → after all segments, write a fresh checkpoint.
- A leading run of segments that are entirely `Elided` is removed (`remove` + `sync_dir`); the log then starts at a later LSN, which `Wal::open` accepts (the first segment's first LSN is the log start).
- A read racing a rename can pair an old location with the new file. `Wal::read` detects it (CRC/LSN mismatch → `WalError::CorruptSegment` is wrong here): add `WalError::Moved` for "the frame at this location is not the one asked for, and the segment was rewritten since" (the `Wal` keeps a per-segment generation counter bumped on swap), and the adapter's `get` retries up to 3 times by re-reading the index. Any other mismatch stays `CorruptSegment`.
- API: `pub async fn compact(&self) -> Result<CompactionReport, StorageError>` with `#[derive(Debug)] pub struct CompactionReport { pub segments_rewritten: usize, pub segments_removed: usize, pub bytes_before: u64, pub bytes_after: u64 }`.
- Trigger: `WalStorageAdapter::compact()` (public, blocking-safe: runs on `spawn_blocking` from async callers) plus, when a runtime exists at open, a background task that every `CompactionConfig::min_interval` runs `compact()` if total segment bytes ≥ `min_wal_size_bytes` and the sealed segments' dead ratio ≥ `min_dead_ratio` (new field, default 0.5). `CompactionConfig` moves from the deleted `prkdb_core::wal::compaction` to `crates/prkdb/src/storage/compaction.rs`; `keep_segments` is deleted (it meant "history to keep" for the stub and has no meaning now).

**Files:**
- Create: `crates/prkdb/src/storage/compaction.rs`
- Modify: `crates/prkdb-core/src/wal/log.rs` (`sealed_segments()`, `replace_segment(first, path_of_compacted_file)`, `remove_leading_segments(upto)`, `WalError::Moved`, per-segment generation), `crates/prkdb/src/storage/wal_adapter.rs`, `crates/prkdb/src/storage/config.rs`, `crates/prkdb/tests/compaction_test.rs`

- [ ] **Step 1: Failing test** (append to `crates/prkdb/tests/compaction_test.rs`, which today holds a Raft log-compaction test):

```rust
/// Compaction keeps exactly the live values, shrinks the log, and survives reopen.
#[tokio::test(flavor = "multi_thread")]
async fn compaction_keeps_only_live_values_and_removes_old_segments() {
    use prkdb_core::wal::{SyncMode, WalConfig};
    let dir = tempfile::tempdir().unwrap();
    let cfg = || WalConfig {
        log_dir: dir.path().to_path_buf(),
        segment_bytes: 32 * 1024,
        sync_mode: SyncMode::Fast,
        ..WalConfig::test_config()
    };
    let wal_bytes = || -> u64 {
        std::fs::read_dir(dir.path()).unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "wal"))
            .map(|p| std::fs::metadata(p).unwrap().len())
            .sum()
    };
    let db = WalStorageAdapter::new(cfg()).unwrap();
    for round in 0..50u32 {
        for k in 0..20u32 {
            db.put(format!("k{k}").as_bytes(), &vec![round as u8; 512]).await.unwrap();
        }
    }
    db.delete(b"k3").await.unwrap();
    db.flush().await.unwrap();
    let before = wal_bytes();
    let report = db.compact().await.unwrap();
    let after = wal_bytes();
    assert!(report.segments_rewritten > 0, "{report:?}");
    assert!(after * 4 < before, "compaction must reclaim most of 50 overwrites: {before} -> {after}");
    let check = |db: WalStorageAdapter| async move {
        for k in 0..20u32 {
            let want = (k != 3).then(|| vec![49u8; 512]);
            assert_eq!(db.get(format!("k{k}").as_bytes()).await.unwrap(), want, "k{k}");
        }
    };
    check(db).await;
    check(WalStorageAdapter::open_async(cfg()).await.unwrap()).await;
}

/// Writers keep writing while compaction runs; nothing they wrote is lost or reverted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_races_writers_safely() {
    use prkdb_core::wal::{SyncMode, WalConfig};
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let cfg = || WalConfig { log_dir: dir.path().to_path_buf(), segment_bytes: 16 * 1024, sync_mode: SyncMode::Fast, ..WalConfig::test_config() };
    let db = Arc::new(WalStorageAdapter::new(cfg()).unwrap());
    for i in 0..2000u32 {
        db.put(format!("k{}", i % 50).as_bytes(), &i.to_le_bytes()).await.unwrap();
    }
    let writer = {
        let db = db.clone();
        tokio::spawn(async move {
            for i in 2000..4000u32 {
                db.put(format!("k{}", i % 50).as_bytes(), &i.to_le_bytes()).await.unwrap();
            }
        })
    };
    for _ in 0..5 {
        db.compact().await.unwrap();
    }
    writer.await.unwrap();
    for k in 0..50u32 {
        let last = (2000..4000u32).rev().find(|i| i % 50 == k).unwrap();
        assert_eq!(db.get(format!("k{k}").as_bytes()).await.unwrap(), Some(last.to_le_bytes().to_vec()));
    }
    db.flush().await.unwrap();
    drop(db);
    let db = WalStorageAdapter::open_async(cfg()).await.unwrap();
    for k in 0..50u32 {
        let last = (2000..4000u32).rev().find(|i| i % 50 == k).unwrap();
        assert_eq!(db.get(format!("k{k}").as_bytes()).await.unwrap(), Some(last.to_le_bytes().to_vec()));
    }
}
```

Add a FaultFs test to `crates/prkdb-verify/tests/power_loss.rs`: `compaction_is_crash_safe_at_every_step` — for each of the five durable steps in the design, inject a power loss right after it (test hook: `WalStorageAdapter::compact_with_hook(|step: CompactionStep| …)` under `#[doc(hidden)]`, where the hook calls `fs.power_loss(.., Tear::Prefix)` at the chosen step and returns `Err` to abort), reopen, and assert the same contents as before compaction. Run → does not compile (`compact` missing).

- [ ] **Step 2: Implement** per the design.
- [ ] **Step 3: Run** — `cargo nextest run -p prkdb --test compaction_test`, `-p prkdb-verify --test power_loss --test checkpoint` → pass; workspace → pass; harness both modes → green.
- [ ] **Step 4: Commit** — `feat: compact sealed WAL segments and drop dead ones` (body: bytes before/after for the test workload; recovery_bench delta).

---

### Task 2.16: `BatchAccumulator::flush` is a barrier (STO-07)

Today `flush()` sleeps `linger_ms + 10` and returns `Ok`, executor errors are dropped with `let _ =`, the queue is an unbounded `crossbeam_channel`, and `CollectionHandle::with_batching`'s executor turns per-item failures into a `warn!` and `Ok(())`.

**Design:** one `tokio::sync::mpsc::UnboundedSender<Msg<C>>` whose memory is bounded by a byte `Semaphore` (`BatchConfig::max_buffer_bytes`), because an item's permit travels with it:

```rust
enum Msg<C> {
    Item(C, OwnedSemaphorePermit),
    /// Answered after every item sent before it has been executed; carries the first
    /// executor error since the previous flush (then clears it).
    Flush(oneshot::Sender<Result<(), StorageError>>),
}
```

The worker task batches items (linger timer or `max_batch_size`), executes, remembers the first error; on `Flush` it executes what it holds, then answers. Channel FIFO order makes that a sequence barrier: everything sent before the flush is in the batch that runs before the answer. `add_put` computes the item's size with a counting `std::io::Write` sink and `bincode::serde::encode_into_writer` (no allocation), acquires that many permits (`min(size, max_buffer_bytes)`), and waits when the buffer is full. On drop, the worker drains and executes what is left and logs any error with `tracing::error!` (nobody is left to return it to; this replaces `let _ =`, spec §8).

**Files:** `crates/prkdb/src/batch_accumulator.rs` (rewrite), `crates/prkdb/src/collection_handle.rs` (executor reports failures), `crates/prkdb/tests/batch_accumulator_flush.rs` (create); `batch_accumulator` is a private module, so the new test file drives it through `CollectionHandle::with_batching` + `flush`, and the unit-level barrier tests go in the module's `#[cfg(test)] mod tests`.

- [ ] **Step 1: Failing tests** (in `batch_accumulator.rs`'s test module; adapted from `docs/reviews/probes/core_review.rs::flush_returns_while_the_executor_is_blocked`):

```rust
    #[tokio::test]
    async fn flush_stays_pending_while_the_executor_is_blocked() {
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let (s, r) = (started.clone(), release.clone());
        let acc = BatchAccumulator::new(
            BatchConfig { linger_ms: 1, max_batch_size: 1, ..Default::default() },
            move |_: Vec<TestItem>| {
                let (s, r) = (s.clone(), r.clone());
                async move { s.notify_one(); r.notified().await; Ok(()) }
            },
        );
        acc.add_put(TestItem { id: "a".into(), value: 1 }).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), started.notified()).await.unwrap();
        let flush = acc.flush();
        tokio::pin!(flush);
        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut flush).await.is_err(),
            "flush returned while the executor was still running"
        );
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), flush).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn flush_returns_the_executor_error() {
        let acc = BatchAccumulator::new(
            BatchConfig { linger_ms: 1, max_batch_size: 10, ..Default::default() },
            |_: Vec<TestItem>| async { Err(StorageError::Internal("disk on fire".into())) },
        );
        acc.add_put(TestItem { id: "a".into(), value: 1 }).await.unwrap();
        let err = acc.flush().await.expect_err("an executor error must reach flush");
        assert!(err.to_string().contains("disk on fire"), "{err}");
        acc.flush().await.expect("the error is reported once, then cleared");
    }

    #[tokio::test]
    async fn admission_is_bounded_by_bytes() {
        let release = Arc::new(tokio::sync::Notify::new());
        let r = release.clone();
        let acc = Arc::new(BatchAccumulator::new(
            BatchConfig { linger_ms: 1, max_batch_size: 1, max_buffer_bytes: 256, ..Default::default() },
            move |_: Vec<TestItem>| { let r = r.clone(); async move { r.notified().await; Ok(()) } },
        ));
        let big = || TestItem { id: "x".repeat(100), value: 0 }; // ~100 bytes encoded
        acc.add_put(big()).await.unwrap(); // taken by the (blocked) executor
        acc.add_put(big()).await.unwrap();
        acc.add_put(big()).await.unwrap();
        let blocked = { let acc = acc.clone(); tokio::spawn(async move { acc.add_put(big()).await }) };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!blocked.is_finished(), "admission must wait when the byte budget is used up");
        release.notify_waiters();
        for _ in 0..8 { release.notify_one(); tokio::time::sleep(Duration::from_millis(10)).await; }
        blocked.await.unwrap().unwrap();
    }
```

and in `crates/prkdb/tests/batch_accumulator_flush.rs`:

```rust
//! STO-07 end to end: a batched CollectionHandle's flush reports storage failures.

use prkdb::prelude::*;
use prkdb_types::error::StorageError;
use prkdb_types::storage::StorageAdapter;

struct RefusesWrites;

#[async_trait::async_trait]
impl StorageAdapter for RefusesWrites {
    async fn get(&self, _: &[u8]) -> Result<Option<Vec<u8>>, StorageError> { Ok(None) }
    async fn put(&self, _: &[u8], _: &[u8]) -> Result<(), StorageError> { Err(StorageError::BackendError("refused".into())) }
    async fn delete(&self, _: &[u8]) -> Result<(), StorageError> { Ok(()) }
}

#[derive(Collection, serde::Serialize, serde::Deserialize, Clone, Debug)]
struct Item { #[id] id: u64 }

#[tokio::test]
async fn a_batched_handle_flush_reports_failed_writes() {
    let db = PrkDb::builder().with_storage(RefusesWrites).build().unwrap();
    let handle = db.collection::<Item>().with_batching(prkdb_core::batch_config::BatchConfig {
        linger_ms: 1, max_batch_size: 4, ..Default::default()
    });
    handle.put(Item { id: 1 }).await.unwrap(); // accepted into the buffer
    let err = handle.flush().await.expect_err("the write failed; flush must say so");
    assert!(err.to_string().contains("refused"), "{err}");
}
```

(Adjust the `prelude` import to whatever exports `Collection`, `PrkDb`; see `crates/prkdb/tests/prkdb_orm_macro.rs` for the working pattern.) Run → `flush_stays_pending…` fails (flush returns after the sleep), `flush_returns_the_executor_error` fails (`Ok`), the end-to-end test fails (`Ok`).

- [ ] **Step 2: Implement** per the design; `CollectionHandle::with_batching`'s executor returns `Err(StorageError::Internal(format!("{failed} of {n} batched writes failed; first: {first}")))` when any item failed, instead of `warn!` + `Ok`.
- [ ] **Step 3: Run** — `cargo nextest run -p prkdb --lib batch_accumulator` and `--test batch_accumulator_flush --test batching_window_perf_test` → pass; workspace → pass.
- [ ] **Step 4: Ledger** — STO-07 `fixed`, `regression_tests = ["test:crates/prkdb/src/batch_accumulator.rs::flush_stays_pending_while_the_executor_is_blocked", "test:crates/prkdb/src/batch_accumulator.rs::flush_returns_the_executor_error", "test:crates/prkdb/tests/batch_accumulator_flush.rs::a_batched_handle_flush_reports_failed_writes"]`.
- [ ] **Step 5: Commit** — `fix: make BatchAccumulator::flush wait for and report its writes` (body: `batching_bench` delta). Follow-up `docs: record STO-07 as fixed`.

---

### Task 2.17: Index maintenance and unique enforcement (KEY-02, KEY-04)

`upsert` calls `self.delete(record)` with the **new** record, so it removes the new record's index values, not the old ones; `insert` over an existing id leaves the old values indexed; `#[index(unique)]` is never checked (KEY-02). Reading the code for this plan found a second defect with the same root: `IndexedStorage`'s secondary indexes live only in memory (`lock_free_indexes`), are never rebuilt when a database is reopened, and are only restored if the caller saved and reloaded a side file (`save_indexes`/`load_from`) — so after a restart, `query_by` returns nothing for data that is still there, and unique enforcement would pass duplicates.

**Design:** extract index maintenance into `crates/prkdb/src/index_maintenance.rs` (spec §9). Every mutating `IndexedStorage` method for collection `T` takes that collection's write lock (`DashMap<CollectionId, Arc<tokio::sync::Mutex<()>>>`), ensures the collection's indexes are loaded, reads the **prior** record under the lock, computes the index delta between prior and new, checks unique fields (fail before mutation, spec §8), writes the record, then applies the delta. Indexes are rebuilt lazily on first access per collection by `scan_prefix(collection_prefix(coll))` (Task 2.12's codec) — they stay derived state, so there is no second on-disk index format to lock into format v2. `save_indexes`/`load_from` remain as an optional warm-start; `load_from` must not suppress the rebuild (it marks a collection loaded only if the file's record count matches a prefix scan count; otherwise it rebuilds).

```rust
pub(crate) struct IndexDelta { pub remove: Vec<(&'static str, Vec<u8>)>, pub add: Vec<(&'static str, Vec<u8>)> }
pub(crate) fn delta<T: Indexed>(prior: Option<&T>, next: Option<&T>) -> IndexDelta;
/// Unique fields of `next` whose value is indexed to a different primary key.
pub(crate) fn unique_conflicts<T: Indexed>(next: &T, pk: &[u8], lookup: impl Fn(&str, &[u8]) -> Vec<Vec<u8>>) -> Vec<&'static str>;
```

New error: `StorageError::UniqueViolation { collection: String, field: String }` (`#[error("unique index {collection}.{field} already holds this value")]`).

**Files:** `crates/prkdb/src/index_maintenance.rs` (create), `crates/prkdb/src/indexed_storage.rs`, `crates/prkdb-types/src/error.rs`, `crates/prkdb/tests/indexed_db_tests.rs`, `docs/remediation/ledger.toml` (+ spec §3.2 row for KEY-04)

- [ ] **Step 1: File KEY-04 first** (it is a new finding): spec §3.2 row `KEY-04 | HIGH | IndexedStorage secondary indexes are memory-only and not rebuilt on reopen; after a restart query_by misses existing records and unique checks cannot see them | indexed_storage.rs:3404-3435 (found while expanding the Phase 2 plan) | Verified | 2`, revision-history row, ledger entry (`status = "open"`, `tripwire` = the test below while it passes on the buggy code). Tripwire in `tripwires.rs`:

```rust
/// KEY-04: secondary indexes are not rebuilt after reopen.
#[tokio::test(flavor = "multi_thread")]
async fn key04_secondary_index_empty_after_reopen_tripwire() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = IndexedStorage::new(Arc::new(WalStorageAdapter::new(wal_config(dir.path())).unwrap()));
        db.insert(&TwUser { id: 1, name: "Alice".into() }).await.unwrap();
        db.inner().flush().await.unwrap();
    }
    let db = IndexedStorage::new(Arc::new(WalStorageAdapter::open_async(wal_config(dir.path())).await.unwrap()));
    assert!(db.get::<TwUser>(&1).await.unwrap().is_some(), "the record itself is there");
    assert!(
        db.query_by::<TwUser>("name", &"Alice").await.unwrap().is_empty(),
        "KEY-04 appears fixed: invert this tripwire"
    );
}
```

Commit: `docs: add KEY-04, secondary indexes lost on reopen` (spec, ledger, tripwire, render).

- [ ] **Step 2: Failing tests** (append to `crates/prkdb/tests/indexed_db_tests.rs`; reuse its record types, or add):

```rust
#[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Account {
    #[id]
    id: u64,
    #[index]
    role: String,
    #[index(unique)]
    email: String,
}

fn acct(id: u64, role: &str, email: &str) -> Account {
    Account { id, role: role.into(), email: email.into() }
}

/// KEY-02: an upsert moves the record to its new index value and out of the old one.
#[tokio::test]
async fn upsert_moves_the_record_to_its_new_index_value() {
    let db = IndexedStorage::new(Arc::new(InMemoryAdapter::new()));
    db.insert(&acct(1, "admin", "a@x")).await.unwrap();
    db.upsert(&acct(1, "user", "a@x")).await.unwrap();
    assert!(db.query_by::<Account>("role", &"admin").await.unwrap().is_empty());
    assert_eq!(db.query_by::<Account>("role", &"user").await.unwrap(), vec![acct(1, "user", "a@x")]);
}

#[tokio::test]
async fn insert_over_an_existing_id_replaces_its_index_entries() {
    let db = IndexedStorage::new(Arc::new(InMemoryAdapter::new()));
    db.insert(&acct(1, "admin", "a@x")).await.unwrap();
    db.insert(&acct(1, "user", "b@x")).await.unwrap();
    assert!(db.query_by::<Account>("role", &"admin").await.unwrap().is_empty());
    assert!(db.query_by::<Account>("email", &"a@x").await.unwrap().is_empty());
}

#[tokio::test]
async fn a_unique_index_rejects_a_duplicate_without_writing() {
    let db = IndexedStorage::new(Arc::new(InMemoryAdapter::new()));
    db.insert(&acct(1, "admin", "a@x")).await.unwrap();
    let err = db.insert(&acct(2, "user", "a@x")).await.expect_err("duplicate email");
    assert!(matches!(err, StorageError::UniqueViolation { ref field, .. } if field == "email"), "{err}");
    assert!(db.get::<Account>(&2).await.unwrap().is_none(), "no partial write");
    assert!(db.query_by::<Account>("role", &"user").await.unwrap().is_empty(), "no index residue");
}

#[tokio::test]
async fn a_unique_index_allows_rewriting_the_same_record() {
    let db = IndexedStorage::new(Arc::new(InMemoryAdapter::new()));
    db.insert(&acct(1, "admin", "a@x")).await.unwrap();
    db.upsert(&acct(1, "owner", "a@x")).await.unwrap();
}

#[tokio::test]
async fn concurrent_inserts_of_one_unique_value_admit_exactly_one() {
    let db = Arc::new(IndexedStorage::new(Arc::new(InMemoryAdapter::new())));
    let tasks: Vec<_> = (0..16u64)
        .map(|id| { let db = db.clone(); tokio::spawn(async move { db.insert(&acct(id, "r", "same@x")).await }) })
        .collect();
    let mut ok = 0;
    for t in tasks { if t.await.unwrap().is_ok() { ok += 1; } }
    assert_eq!(ok, 1);
}
```

Invert the KEY-04 tripwire into `key04_secondary_indexes_survive_reopen` (same setup; assert `query_by("name", "Alice")` returns the record, and that a unique insert of an existing value after reopen is rejected). Run → all fail on current code (the concurrency test fails because there is no per-collection lock).

- [ ] **Step 3: Implement** per the design: `insert`, `upsert`, `update`, `delete`, `insert_batch`, `upsert_batch`, `delete_batch`, `soft_delete`, `restore`, `update_where`, `delete_where` and the `Transaction::commit` path all go through one `write_record::<T>(prior_policy, next: Option<&T>)` helper in `index_maintenance.rs` (list them with `rg -n 'lock_free_indexes' crates/prkdb/src/indexed_storage.rs`). The legacy `indexes: Arc<RwLock<BTreeMap<String, MemoryIndex>>>` map: `rg` its readers; if `lock_free_indexes` supersedes it everywhere (the `save_indexes` comment says it does), delete it in this task.
- [ ] **Step 4: Run** — `cargo nextest run -p prkdb --test indexed_db_tests --test tripwires` → pass; workspace → pass; `bench_indexed_storage_insert` (iai, local compile only) and `query_bench` wall-clock delta in the commit body (inserts now read the prior record: expect a cost, record it; it is a correctness cost → `perf_note` if the gate flags it).
- [ ] **Step 5: Ledger** — KEY-02 `fixed` (`regression_tests`: the five tests above), KEY-04 `fixed` (`key04_secondary_indexes_survive_reopen`), clear KEY-04's `tripwire`.
- [ ] **Step 6: Commit** — `fix: maintain secondary indexes from the prior record and enforce unique indexes`; follow-up `docs: record KEY-02 and KEY-04 as fixed`.

---

### Task 2.18: Harness — checkpoint, compaction, typed collections (§7.1 "after 2d")

The blocking profile gains `Checkpoint`, `Compact` and multi-collection keys (with index queries and a batched handle), in Durable and Fast, proving STO-01, STO-03, STO-07, KEY-01 and KEY-02 under crash and power loss.

**Design:** a third SUT, `TypedSut`, drives the typed API over `WalStorageAdapter::open_with_vfs(FaultFs)`:
- Harness keys become `(collection, id)`: `key(i)` → `vec![b'a' + i / 8, b'k', i % 8]` (16 keys: two collections × 8 ids that collide across collections, which is what KEY-01 needs). `KEY_SPACE` stays 16.
- Collection `a` is `HarnessA { #[id] id: u64, v: Vec<u8>, #[index] tag: u8 }` written with `IndexedStorage::upsert` (KEY-02's path; `tag = v.last() % 4`). Collection `b` is `HarnessB` (same shape) written through `db.collection::<HarnessB>().with_batching(..)`: each `put` is followed by `flush()`, and only a successful flush acknowledges it — if flush returned early (STO-07), a following crash or power loss loses an acknowledged write.
- `Sut` gains `async fn index_query(&mut self, coll: u8, tag: u8) -> anyhow::Result<Option<BTreeSet<Key>>>` (default `Ok(None)` = not supported, skipped) and `async fn compact(&mut self) -> anyhow::Result<()>` (default `Unsupported`). The checker, after every restart, also compares each `(coll a, tag)` query against the model's keys whose value's last byte % 4 == tag.
- `Op::Compact`; blocking weights become `Put 50, Delete 18, Reopen 7, Crash 7, PowerLoss 8, Checkpoint 5, Compact 5`, and Discovery gains `Compact` too. `Profile::Blocking` and `Profile::Discovery` use `TypedSut` from now on (it exercises the adapter underneath; `--sut typed` becomes the default for both); `FaultSut` implements `compact` as well and stays available with `--sut fault`. `discovery_profile_checkpoint_keeps_every_key` switches to `TypedSut`.

**Files:** `crates/prkdb-verify/src/{ops.rs,sut.rs,checker.rs,runner.rs,bin/verify.rs}`, `crates/prkdb-verify/Cargo.toml` (`prkdb-macros`, `serde`), `crates/prkdb-verify/tests/harness.rs`

- [ ] **Step 1: Failing tests** (`harness.rs`):

```rust
#[test]
fn blocking_profile_checkpoints_and_compacts() {
    let ops: Vec<Op> = (0..20).flat_map(|s| prkdb_verify::ops::generate(s, 200, Profile::Blocking)).collect();
    assert!(ops.iter().any(|o| matches!(o, Op::Checkpoint)));
    assert!(ops.iter().any(|o| matches!(o, Op::Compact)));
    assert!(ops.iter().any(|o| matches!(o, Op::Put(k, _) if k[0] == b'a'))
        && ops.iter().any(|o| matches!(o, Op::Put(k, _) if k[0] == b'b')), "both collections");
}

/// KEY-01 through the harness: a SUT that ignores the collection must be caught.
struct IgnoresCollection { inner: TypedSut }
// Sut impl: forwards everything, but rewrites every key's first byte to b'a' before
// forwarding put/delete/get (so collections a and b collide).

#[tokio::test(flavor = "multi_thread")]
async fn typed_harness_catches_colliding_collections() {
    let cfg = RunConfig { first_seed: 0, seeds: 30, ops: 60, profile: Profile::Blocking, mode: Mode::Durable, repro_attempts: 1 };
    let r = run(|| async { Ok(IgnoresCollection { inner: TypedSut::new(Mode::Durable).await? }) }, &cfg).await.unwrap();
    assert!(r.failure.is_some());
}
```

`blocking_profile_is_green_on_current_code` and `blocking_profile_is_green_in_fast_mode` switch to `TypedSut` and additionally assert `op_counts["Checkpoint"] > 0 && op_counts["Compact"] > 0`. Run → fail (no `Compact`, no `TypedSut`).

- [ ] **Step 2: Implement** per the design.
- [ ] **Step 3: Run** — `cargo nextest run -p prkdb-verify` → pass; `cargo xtask verify --profile blocking --seeds 1000 --mode durable` and `--mode fast` → green. A failure here is either a real bug in Tasks 2.12–2.17 (fix it with a unit-level regression test first) or an unknown bug (demotion rule + ledger).
- [ ] **Step 4: Ledger** — append `test:crates/prkdb-verify/tests/harness.rs::blocking_profile_is_green_on_current_code` to the `regression_tests` of STO-01, STO-03, STO-07, KEY-01, KEY-02 (the row these ops prove). The `harness` field (`<commit> seeds=10000 mode=… profile=phase2 run=<URL>`) is filled at the gate (Task 2.25).
- [ ] **Step 5: Commit** — `feat: add checkpoint, compaction and typed collections to the blocking harness profile`.

---

### Task 2.19: Outbox persisted with its data (EVT-02)

`WalStorageAdapter` keeps the outbox in a `papaya` map that is never written to disk; `CollectionPartitionedAdapter` discards every outbox write and returns `Ok`.

**Design:**
- Two batch ops (tags 3 and 4, added before the format freeze in Task 2.24): `OutboxPut { id: String, payload: Vec<u8> }` and `OutboxRemove { id: String }`.
- `outbox_save(id, payload)` commits `[OutboxPut]`; `outbox_remove(id)` commits `[OutboxRemove]`; `put_with_outbox(key, value, id, payload)` commits `[Put, OutboxPut]` and `delete_with_outbox` commits `[Delete, OutboxPut]` — one frame each, so the data and its event are atomic (they were two independent in-memory steps).
- The outbox map becomes `papaya::HashMap<String, RecordLoc>`, published by the commit hook like the key index and rebuilt by replay; `outbox_list` reads payloads through `wal.read`. Compaction (Task 2.15) treats a frame holding a present outbox id as live. The checkpoint (Task 2.14) gains an outbox section after the key entries (`outbox_entries u64 | (idlen u32 | id | loc)…`) — the checkpoint format is not frozen until Task 2.24.
- `CollectionPartitionedAdapter`: the outbox is persisted in one dedicated inner `WalStorageAdapter` at `collections/__outbox/` (opened eagerly), so `outbox_save`/`outbox_list`/`outbox_remove` forward to it. `put_with_outbox`/`delete_with_outbox` cannot be atomic across two logs, so they return the new `StorageError::UnsupportedCapability(String)` **before mutating anything** (spec §8), which makes `CollectionHandle` take its existing fallback (write, then `outbox_save`). That fallback is not atomic — that is EVT-05, Phase 3 — but it no longer loses the event. Record this interaction in EVT-05's `evidence`.

**Files:** `crates/prkdb-core/src/wal/batch.rs`, `crates/prkdb/src/storage/{wal_adapter.rs,checkpoint.rs,compaction.rs,collection_partitioned_adapter.rs}`, `crates/prkdb-types/src/error.rs`, `crates/prkdb/tests/outbox_cdc_tests.rs`, `crates/prkdb-verify/tests/power_loss.rs`

- [ ] **Step 1: Failing tests** (append to `crates/prkdb/tests/outbox_cdc_tests.rs`):

```rust
use prkdb::storage::{CollectionPartitionedAdapter, WalStorageAdapter};
use prkdb_core::wal::WalConfig;
use prkdb_types::error::StorageError;
use prkdb_types::storage::StorageAdapter;

fn wal(dir: &std::path::Path) -> WalConfig {
    WalConfig { log_dir: dir.to_path_buf(), ..WalConfig::test_config() }
}

/// EVT-02: the WAL adapter's outbox survives a restart, and removals do too.
#[tokio::test(flavor = "multi_thread")]
async fn wal_outbox_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = WalStorageAdapter::new(wal(dir.path())).unwrap();
        db.put_with_outbox(b"k1", b"v1", "users:0:1", b"event-1").await.unwrap();
        db.outbox_save("users:0:2", b"event-2").await.unwrap();
        db.outbox_save("users:0:3", b"event-3").await.unwrap();
        db.outbox_remove("users:0:2").await.unwrap();
    }
    let db = WalStorageAdapter::open_async(wal(dir.path())).await.unwrap();
    let mut list = db.outbox_list().await.unwrap();
    list.sort();
    assert_eq!(
        list,
        vec![("users:0:1".to_string(), b"event-1".to_vec()), ("users:0:3".to_string(), b"event-3".to_vec())]
    );
    assert_eq!(db.get(b"k1").await.unwrap().as_deref(), Some(&b"v1"[..]));
}

/// EVT-02: the partitioned adapter persists outbox entries instead of dropping them, and
/// refuses the atomic variants it cannot honour, before writing anything.
#[tokio::test(flavor = "multi_thread")]
async fn partitioned_outbox_persists_or_refuses() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = CollectionPartitionedAdapter::new(wal(dir.path())).unwrap();
        db.outbox_save("users:0:1", b"event-1").await.unwrap();
        let err = db.put_with_outbox(b"users:k", b"v", "users:0:2", b"event-2").await.unwrap_err();
        assert!(matches!(err, StorageError::UnsupportedCapability(_)), "{err}");
        assert!(db.get(b"users:k").await.unwrap().is_none(), "refused before mutating");
        db.flush().await.unwrap();
    }
    let db = CollectionPartitionedAdapter::new(wal(dir.path())).unwrap();
    assert_eq!(db.outbox_list().await.unwrap(), vec![("users:0:1".to_string(), b"event-1".to_vec())]);
}
```

and in `power_loss.rs`, `put_with_outbox_is_atomic_under_power_loss`: Fast mode, 50 `put_with_outbox` calls with distinct keys/ids, `power_loss(Tear::Prefix)`, reopen → for every key present, its event is present, and vice versa. Run → fail (outbox empty after restart; partitioned adapter returns `Ok` and an empty list).

- [ ] **Step 2: Implement** per the design.
- [ ] **Step 3: Run** — `cargo nextest run -p prkdb --test outbox_cdc_tests --test atomic_outbox_tests --test consumer_tests` and `-p prkdb-verify --test power_loss --test checkpoint` → pass; workspace → pass; harness both modes → green.
- [ ] **Step 4: Ledger** — EVT-02 `fixed`, `regression_tests = ["test:crates/prkdb/tests/outbox_cdc_tests.rs::wal_outbox_survives_restart", "test:crates/prkdb/tests/outbox_cdc_tests.rs::partitioned_outbox_persists_or_refuses", "test:crates/prkdb-verify/tests/power_loss.rs::put_with_outbox_is_atomic_under_power_loss"]`.
- [ ] **Step 5: Commit** — `fix: persist the outbox in the WAL with the data it describes`; follow-up `docs: record EVT-02 as fixed`.

---

### Task 2.20: Event identity from the WAL (EVT-01)

`static OUTBOX_SEQ` restarts at 1 in every process, so persisted consumer offsets skip new events after a restart. Spec 2c: an event's sequence **is** the WAL position of its commit record; no counter exists that could reset. Consumers see an opaque, ordered `EventSeq` so the Raft index can replace the WAL position in Phase 4 without changing types.

**Design:**
- `prkdb_types::event::EventSeq(u64)`: `Ord`, `Copy`, `Display` as 20 zero-padded digits (the outbox id suffix, so string order = sequence order), `EventSeq::from_raw(u64)`/`raw()` for adapters, and `EventSeq::from_wal(lsn, index_in_frame: u16)` = `lsn << 16 | index` for the WAL adapter (a frame carries at most 65,536 events, enforced with `StorageError::Validation`). **Design decision to review:** packing keeps one `u64` offset type for consumers while letting one atomic frame carry several events (Phase 3 transactions will emit several). It limits LSNs to 2⁴⁸ (≈ 2.8 × 10¹⁴ frames).
- New `StorageAdapter` methods (default: `Err(StorageError::UnsupportedCapability("event_append"…))`):

```rust
    /// Appends an event to `stream`; the adapter assigns its sequence, which is strictly
    /// greater than every sequence this data directory has handed out before, across
    /// restarts. The event is listed by `outbox_list` as `"{stream}:{seq}"`.
    async fn event_append(&self, stream: &str, payload: &[u8]) -> Result<EventSeq, StorageError>;
    /// `put` and `event_append` as one atomic write.
    async fn put_with_event(&self, key: &[u8], value: &[u8], stream: &str, payload: &[u8]) -> Result<EventSeq, StorageError>;
    /// `delete` and `event_append` as one atomic write.
    async fn delete_with_event(&self, key: &[u8], stream: &str, payload: &[u8]) -> Result<EventSeq, StorageError>;
```

  `outbox_save`/`outbox_remove` stay: `prkdb::replication` mirrors a leader's ids verbatim on followers, and draining removes by id.
- WAL adapter: batch op tag 5 `Event { stream: String, payload: Vec<u8> }`; the commit hook computes each event's id from the frame's LSN and the event's index among the frame's events and publishes it into the outbox map; the method returns the `EventSeq`. Replay does the same, so ids are identical after restart.
- sled: `EventSeq::from_raw(db.generate_id()?)` (sled persists its id generator and jumps ahead on restart: monotonic across restarts, not contiguous). The two-tree write stays non-atomic — that is EVT-04 (Phase 3).
- SQL: table `outbox_seq (id INTEGER PRIMARY KEY CHECK (id = 1), next INTEGER NOT NULL)`, created with the other tables (`INSERT OR IGNORE INTO outbox_seq VALUES (1, 1)`), and `UPDATE outbox_seq SET next = next + 1 WHERE id = 1 RETURNING next - 1` inside the same transaction as the outbox row (and the kv row for `put_with_event`).
- In-memory adapter: an `AtomicU64` (its data does not survive a restart either). `prkdb-storage-segmented`'s wrapper forwards the three methods.
- `outbox.rs`: delete `OUTBOX_SEQ`, `make_outbox_id_for_type`, `make_dlq_id_for_type`; add `pub fn event_stream<C: Collection>(partition: Option<u32>) -> String` = `format!("{}:{}", C::persisted_name(), partition.unwrap_or(0))`; `save_outbox_event`/`outbox_save_batch` take a stream and return `EventSeq`; DLQ records use stream `dlq:{persisted_name}` through `event_append`. `collection_handle.rs` calls `put_with_event`/`delete_with_event` and keeps today's fallback shape (on any error: plain write, then `event_append`; narrowing that fallback is EVT-05, Phase 3). `replication.rs::replicate_change` uses `event_append`. `consumer.rs` already parses the last `:` segment as a `u64`; `get_latest_offset` returns `max + 1` as before — offsets are now `EventSeq` raw values.

**Files:** `crates/prkdb-types/src/{event.rs,lib.rs,storage.rs}`, `crates/prkdb-core/src/wal/batch.rs`, `crates/prkdb/src/{outbox.rs,collection_handle.rs,consumer.rs,replication.rs}`, `crates/prkdb/src/storage/{wal_adapter.rs,in_memory.rs,collection_partitioned_adapter.rs}`, `crates/prkdb-storage-sled/src/lib.rs`, `crates/prkdb-storage-sql/src/lib.rs`, `crates/prkdb-storage-segmented/src/uring.rs`, `crates/prkdb/tests/{tripwires.rs,consumer_tests.rs}`

- [ ] **Step 1: Invert the tripwire into the failing regression test.** The tripwire's own doc names its blind spot (the child opened no storage), so the regression test opens a real data directory in each child:

```rust
/// EVT-01 regression (was the tripwire): event ids never repeat across processes that
/// share a data directory, and later processes hand out larger ones.
#[test]
fn evt01_event_ids_do_not_repeat_across_processes() {
    const NAME: &str = "evt01_event_ids_do_not_repeat_across_processes";
    if is_child(NAME) {
        let dir = std::env::var("PRKDB_TRIPWIRE_DIR").unwrap();
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let seq = rt.block_on(async {
            let db = WalStorageAdapter::new(wal_config(std::path::Path::new(&dir))).unwrap();
            db.event_append("tw_event:0", b"e").await.unwrap()
        });
        println!("CHILD_RESULT={}", seq.raw());
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let run = || {
        std::env::set_var("PRKDB_TRIPWIRE_DIR", dir.path());
        child_result(NAME).parse::<u64>().unwrap()
    };
    let (first, second) = (run(), run());
    assert!(second > first, "event ids repeated or went backwards across restarts: {first} then {second}");
}
```

(`child_result` inherits the parent environment, which is how the directory reaches the child; setting a variable in a single-threaded test process before spawning is fine.) Add to `consumer_tests.rs`:

```rust
/// EVT-01 end to end: a consumer at its committed offset sees every event written after a
/// restart, and none twice.
#[tokio::test(flavor = "multi_thread")]
async fn a_committed_consumer_sees_new_events_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let open = || PrkDb::builder().with_data_dir(dir.path()).build().unwrap();
    let config = || ConsumerConfig { group_id: "g".into(), auto_commit: false, ..Default::default() };
    {
        let db = open();
        for id in 0..3u64 { db.collection::<Item>().put(Item { id }).await.unwrap(); }
        let mut c = db.consumer::<Item>(config()).await.unwrap();
        assert_eq!(c.poll().await.unwrap().len(), 3);
        c.commit().await.unwrap();
    }
    let db = open();
    for id in 3..5u64 { db.collection::<Item>().put(Item { id }).await.unwrap(); }
    let mut c = db.consumer::<Item>(config()).await.unwrap();
    let ids: Vec<u64> = c.poll().await.unwrap().into_iter().map(|r| *r.value.id()).collect();
    assert_eq!(ids, vec![3, 4]);
}
```

(Match the existing consumer-construction API used elsewhere in `consumer_tests.rs`; the names `consumer`, `ConsumerConfig`, `r.value` above follow that file — adjust to what it actually calls them.) Run → the tripwire replacement fails to compile (`event_append`), the consumer test sees `[]` after the restart (new ids restart below the committed offset).

- [ ] **Step 2: Implement** per the design.
- [ ] **Step 3: Run** — `cargo nextest run -p prkdb --test tripwires --test consumer_tests --test outbox_cdc_tests --test dlq_tests --test dlq_retry_tests --test replication_tests`, `-p prkdb-storage-sled`, `-p prkdb-storage-sql` → pass; workspace → pass; harness both modes → green.
- [ ] **Step 4: Ledger** — EVT-01 `fixed`, `regression_tests = ["test:crates/prkdb/tests/tripwires.rs::evt01_event_ids_do_not_repeat_across_processes", "test:crates/prkdb/tests/consumer_tests.rs::a_committed_consumer_sees_new_events_after_restart"]`, clear `tripwire`.
- [ ] **Step 5: Commit** — `fix: derive event sequence numbers from the WAL instead of a process counter`; follow-up `docs: record EVT-01 as fixed`.

---

### Task 2.21: Harness — events (§7.1 "after 2e")

**Design:** `Op::Emit(Key, Value)`: `TypedSut` writes collection `a`'s record and its event atomically (`put_with_event` through `CollectionHandle::put_sync`, stream `harness_a:0`); `Sut::events(&mut self) -> anyhow::Result<Option<Vec<(u64, Vec<u8>)>>>` (default `Ok(None)`) lists that stream's events. The model's pending list gains `Mutation::Emit(key, value)` (applies the put **and** appends the payload to the event list), so a prefix carries both state and events. After every restart the checker requires: event payloads equal the chosen prefix's event list (Durable: all; Fast: the same prefix chosen for the key state — data and event are one frame, so they can never diverge); sequences strictly increasing; each surviving event keeps the sequence it had before the restart; in Durable mode no sequence is ever reused. (In Fast mode an event lost to power loss may have its sequence reused by a later event, because the LSN is reused after truncation. Document this next to Fast mode's loss window in `docs/guide` in Phase 5: a consumer that read a lost event is ahead of the log.)

**Files:** `crates/prkdb-verify/src/{model.rs,ops.rs,sut.rs,checker.rs}`, `crates/prkdb-verify/tests/harness.rs`

- [ ] **Step 1: Failing tests** — `blocking_profile_emits_events` (generator), `event_harness_catches_a_sut_that_reuses_sequences` (wrapper that, after each restart, deletes the newest event and re-emits its payload, so the payload list matches but its sequence changes — must be caught in Durable mode), and `blocking_profile_is_green_*` gain `op_counts["Emit"] > 0`. Blocking weights: `Put 45, Delete 16, Emit 10, Reopen 7, Crash 7, PowerLoss 7, Checkpoint 4, Compact 4`. Run → fail.
- [ ] **Step 2: Implement** per the design.
- [ ] **Step 3: Run** — `cargo nextest run -p prkdb-verify`; `cargo xtask verify --profile blocking --seeds 1000` in both modes → green.
- [ ] **Step 4: Ledger** — append `test:crates/prkdb-verify/tests/harness.rs::blocking_profile_is_green_on_current_code` to EVT-01's and EVT-02's `regression_tests`.
- [ ] **Step 5: Commit** — `feat: check events across restarts in the blocking harness profile`.

---

### Task 2.22: Schema persistence (SCH-02)

**Files:** `crates/prkdb-schema/src/{storage.rs,registry.rs,types.rs,error.rs}`, `crates/prkdb-schema/tests/schema_persistence.rs` (create)

- [ ] **Step 1: Failing tests** — `crates/prkdb-schema/tests/schema_persistence.rs` (first one from `docs/reviews/probes/schema_review.rs`, inverted):

```rust
use prkdb_schema::{CompatibilityMode, FileSchemaStorage, SchemaRegistry, SchemaStorage};
use std::sync::Arc;

async fn registered(root: &std::path::Path) {
    let registry = SchemaRegistry::new(Arc::new(FileSchemaStorage::new(root.into())));
    registry.register("users", vec![10, 0], CompatibilityMode::Backward, None).await.unwrap();
}

#[tokio::test]
async fn a_missing_descriptor_fails_the_load() {
    let root = tempfile::tempdir().unwrap();
    registered(root.path()).await;
    std::fs::remove_file(root.path().join("descriptors/users/v1.binpb")).unwrap();
    let mut reopened = FileSchemaStorage::new(root.path().into());
    let err = reopened.load().await.expect_err("a missing descriptor must not load as empty");
    assert!(err.to_string().contains("v1.binpb"), "{err}");
}

#[tokio::test]
async fn a_corrupt_descriptor_fails_the_load() {
    let root = tempfile::tempdir().unwrap();
    registered(root.path()).await;
    let p = root.path().join("descriptors/users/v1.binpb");
    let mut bytes = std::fs::read(&p).unwrap();
    bytes[0] ^= 0xFF;
    std::fs::write(&p, bytes).unwrap();
    let mut reopened = FileSchemaStorage::new(root.path().into());
    assert!(reopened.load().await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_registrations_never_reuse_a_version() {
    let root = tempfile::tempdir().unwrap();
    let registry = Arc::new(SchemaRegistry::new(Arc::new(FileSchemaStorage::new(root.path().into()))));
    let tasks: Vec<_> = (0..16)
        .map(|_| {
            let r = registry.clone();
            tokio::spawn(async move { r.register("users", vec![10, 0], CompatibilityMode::Backward, None).await })
        })
        .collect();
    let mut versions = Vec::new();
    for t in tasks {
        versions.push(t.await.unwrap().unwrap().version);
    }
    versions.sort();
    assert_eq!(versions, (1..=16).collect::<Vec<u32>>());
    let mut reopened = FileSchemaStorage::new(root.path().into());
    reopened.load().await.unwrap();
    assert_eq!(reopened.get_latest("users").await.unwrap().unwrap().version, 16);
}

#[tokio::test]
async fn a_leftover_temp_index_is_ignored() {
    let root = tempfile::tempdir().unwrap();
    registered(root.path()).await;
    std::fs::write(root.path().join("schemas.json.tmp"), b"{ half written").unwrap();
    let mut reopened = FileSchemaStorage::new(root.path().into());
    reopened.load().await.unwrap();
    assert!(reopened.get("users", 1).await.unwrap().is_some());
}
```

Run → the first two load successfully (bug), the concurrency test sees duplicate versions (or a lost index write).

- [ ] **Step 2: Implement.**
  - Fail closed: `load()` returns `SchemaError::Storage("missing descriptor {path} for {collection} v{version}; restore it from backup or remove the entry from schemas.json")` when a descriptor is absent, and a checksum error when it does not match. The checksum: new `Schema` field `#[serde(default)] pub descriptor_crc32: Option<u32>`, written by `put`; `None` (an index written before this change) skips only the checksum, never the existence check.
  - Atomic writes: one helper `async fn write_atomic(path, bytes)` = write `path.tmp` → `File::sync_all` → `rename` → `sync` the parent directory (on Unix, `std::fs::File::open(parent)?.sync_all()`, inside `spawn_blocking`), used for descriptors and `schemas.json`. Descriptor first, index second, so a crash leaves at worst an unreferenced descriptor. `load()` removes stale `*.tmp` files.
  - Serialized allocation: `SchemaRegistry` holds `register_lock: tokio::sync::Mutex<()>` around the whole read-check-allocate-put sequence in `register`; `FileSchemaStorage::put` refuses to overwrite an existing `(collection, version)` with `SchemaError::VersionConflict { collection, version }` (new variant) as a second line of defence for other callers of the storage trait.
- [ ] **Step 3: Run** — `cargo nextest run -p prkdb-schema` → pass; `cargo nextest run -p prkdb-cli --test http_api_integration` (schema routes) → pass.
- [ ] **Step 4: Ledger** — SCH-02 `fixed`, `regression_tests` = the four tests above.
- [ ] **Step 5: Commit** — `fix: fail closed on missing schema descriptors and serialize version allocation`; follow-up `docs: record SCH-02 as fixed`.

---

### Task 2.23: Fuzz targets (TST-07)

**Files:** `fuzz/Cargo.toml`, `fuzz/fuzz_targets/{frame_decode,batch_decode,segment_scan,checkpoint_load,proto_decode}.rs`, `fuzz/corpus/<target>/…` (seeds), `crates/prkdb-verify/src/fuzz_entry.rs`, `crates/prkdb-verify/tests/fuzz_corpus.rs`, root `Cargo.toml` (`exclude = ["fuzz"]`), `.github/workflows/ci.yml` (`fuzz` job)

- [ ] **Step 1: Entry points live in a normal crate** so they also run on stable in every CI run. `crates/prkdb-verify/src/fuzz_entry.rs`:

```rust
//! Fuzz entry points (TST-07). Each must never panic, whatever the input; cargo-fuzz
//! targets call these, and `tests/fuzz_corpus.rs` runs them over the seed corpus on stable.

pub fn frame_decode(data: &[u8]) { let _ = prkdb_core::wal::frame::decode_frame(data); }

pub fn batch_decode(data: &[u8]) { let _ = prkdb_core::wal::batch::Batch::decode(data); }

/// Writes `data` after a valid segment header into an in-memory file and scans it.
pub fn segment_scan(data: &[u8]) {
    let fs = crate::faultfs::FaultFs::new();
    let dir = std::path::Path::new("/f");
    use prkdb_core::vfs::Vfs;
    fs.create_dir_all(dir).unwrap();
    let path = dir.join(prkdb_core::wal::segment::segment_file_name(1));
    let f = fs.create(&path).unwrap();
    prkdb_core::wal::segment::write_segment_header(f.as_ref(), 1).unwrap();
    f.write_at(prkdb_core::wal::segment::SEGMENT_HEADER_LEN, data).unwrap();
    let _ = prkdb_core::wal::segment::scan_segment(f.as_ref(), &path, 1, &mut |_, _, _| Ok(()));
}

pub fn checkpoint_load(data: &[u8]) { let _ = prkdb::storage::checkpoint::decode_checkpoint(data); }

pub fn proto_decode(data: &[u8]) {
    use prost::Message;
    let _ = prkdb_proto::raft::AppendEntriesRequest::decode(data);
    let _ = prkdb_proto::raft::InstallSnapshotRequest::decode(data);
    let _ = prkdb_proto::raft::PutRequest::decode(data);
}
```

(`prkdb-proto` and `prost` become `prkdb-verify` dependencies; `VfsFile` must be in scope for `write_at`.) `tests/fuzz_corpus.rs`:

```rust
/// TST-07: every fuzz entry point accepts its whole seed corpus without panicking.
#[test]
fn fuzz_entries_accept_their_seed_corpus() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus");
    let targets: [(&str, fn(&[u8])); 5] = [
        ("frame_decode", prkdb_verify::fuzz_entry::frame_decode),
        ("batch_decode", prkdb_verify::fuzz_entry::batch_decode),
        ("segment_scan", prkdb_verify::fuzz_entry::segment_scan),
        ("checkpoint_load", prkdb_verify::fuzz_entry::checkpoint_load),
        ("proto_decode", prkdb_verify::fuzz_entry::proto_decode),
    ];
    for (name, f) in targets {
        let dir = root.join(name);
        let files: Vec<_> = std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())).collect();
        assert!(!files.is_empty(), "{name} has no seed corpus");
        for entry in files {
            f(&std::fs::read(entry.unwrap().path()).unwrap());
        }
    }
}
```

Run → fails (no corpus, no module).

- [ ] **Step 2: Seed corpus.** Generators are binaries, not tests (a test that writes into the source tree would modify the repo on every CI run). Add `crates/prkdb-verify/src/bin/fuzz_seeds.rs` that writes 3–5 valid inputs per target (a real frame, a real encoded batch with and without LZ4, a two-frame segment body, a checkpoint from a small adapter, each proto message encoded) to `fuzz/corpus/<target>/seed-N`. Run it once (`cargo run -p prkdb-verify --bin fuzz_seeds`), commit the files.
- [ ] **Step 3: cargo-fuzz crate.** `cargo install cargo-fuzz --locked`; `fuzz/Cargo.toml`:

```toml
[package]
name = "prkdb-fuzz"
version = "0.0.0"
publish = false
edition = "2021"

[package.metadata]
cargo-fuzz = true

[dependencies]
libfuzzer-sys = "0.4"
prkdb-verify = { path = "../crates/prkdb-verify" }

[workspace]

[[bin]]
name = "frame_decode"
path = "fuzz_targets/frame_decode.rs"
test = false
doc = false
bench = false
# … one [[bin]] per target
```

Each target: `#![no_main] libfuzzer_sys::fuzz_target!(|data: &[u8]| prkdb_verify::fuzz_entry::frame_decode(data));`. Local smoke: `cargo +nightly fuzz run frame_decode -- -max_total_time=30` for each target → no crash. (cargo-fuzz needs nightly; this is the one place the pinned toolchain does not apply.)
- [ ] **Step 4: Nightly CI job** in `ci.yml` (schedule/dispatch only; CI preamble but `dtolnay/rust-toolchain@nightly` for this job — the job builds only the fuzz crate):

```yaml
  fuzz:
    name: Fuzz (nightly)
    if: github.event_name == 'schedule' || github.event_name == 'workflow_dispatch'
    runs-on: ubuntu-latest
    timeout-minutes: 45
    steps:
      # CI preamble with dtolnay/rust-toolchain@nightly
      - run: cargo install cargo-fuzz --locked
      - name: Run each target for 5 minutes
        run: |
          for t in frame_decode batch_decode segment_scan checkpoint_load proto_decode; do
            cargo +nightly fuzz run "$t" fuzz/corpus/"$t" -- -max_total_time=300 || exit 1
          done
      - if: failure()
        uses: actions/upload-artifact@v4
        with:
          name: fuzz-artifacts
          path: fuzz/artifacts
```

A crash found by the job becomes a new corpus seed plus a ledger finding.
- [ ] **Step 5: Run** — `cargo nextest run -p prkdb-verify --test fuzz_corpus` → pass.
- [ ] **Step 6: Ledger** — TST-07 `fixed`, `regression_tests = ["test:crates/prkdb-verify/tests/fuzz_corpus.rs::fuzz_entries_accept_their_seed_corpus", "ci-job:ci.yml/fuzz"]`.
- [ ] **Step 7: Commit** — `test: add fuzz targets for WAL, checkpoint and proto decoding`; follow-up `docs: record TST-07 as fixed`.

---

### Task 2.24: Golden v2 data directory and compat check

Freezes format 2 (D3): from this commit on, any change to the bytes a v2 build writes, or to what it reads back, fails CI unless it comes with `FORMAT_VERSION + 1` and a registered migration (D4). The test lives in `prkdb-verify` because the generator does, and `prkdb-verify` already depends on `prkdb` (a dev-dependency from `prkdb` back to `prkdb-verify` would link two copies of `prkdb`).

**Files:** `crates/prkdb-verify/src/golden.rs` (generator library), `crates/prkdb-verify/src/bin/golden_v2.rs` (thin `main`), `crates/prkdb-verify/tests/fixtures/format-v2/{data/…,expected.json}` (generated, committed), `crates/prkdb-verify/tests/storage_compat.rs`, `.gitattributes`, `scripts/pre-push-check.sh`

- [ ] **Step 1: Generator.** `pub async fn write_golden_v2(out: &Path) -> anyhow::Result<()>` writes `out/data/` — a data directory built with fixed keys and values and no timestamps, covering every record type: `FORMAT`; puts and deletes; a 50-item `put_batch` with LZ4; `outbox_save` + `outbox_remove`; `put_with_event` and `delete_with_event`; catalog entries for two collections (two record types through `IndexedStorage`, same ids in both); ≥ 3 segments (`segment_bytes` 4 KiB); one compaction (so an `Elided` frame and a rewritten segment exist); a checkpoint after the compaction; then 5 more puts (a replayed tail) — and `out/expected.json`:

```json
{ "kv": { "<hex key>": "<hex value>" }, "absent": ["<hex key>"],
  "outbox": [["<id>", "<hex payload>"]], "catalog": { "<name>": 1 } }
```

produced by reading the directory back through the public API right after writing it. `golden_v2.rs`: `fn main() { tokio runtime; write_golden_v2(&PathBuf::from(std::env::args().nth(1).expect("out dir"))) }`.
- [ ] **Step 2: Tests** — `crates/prkdb-verify/tests/storage_compat.rs`:

```rust
//! Format 2 is frozen (spec D3). If this fails, either the change is a bug, or it is a
//! format change that needs FORMAT_VERSION + 1 and a registered migration (spec D4).

use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use prkdb_types::storage::StorageAdapter;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/format-v2")
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let p = e.unwrap().path();
        let dest = to.join(p.file_name().unwrap());
        if p.is_dir() { copy_dir(&p, &dest) } else { std::fs::copy(&p, &dest).unwrap(); }
    }
}

fn files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut out = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() { stack.push(p) } else { out.insert(p.strip_prefix(root).unwrap().to_path_buf(), std::fs::read(&p).unwrap()); }
        }
    }
    out
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn golden_v2_directory_reads_back() {
    let tmp = tempfile::tempdir().unwrap();
    copy_dir(&fixture().join("data"), tmp.path()); // never open the committed copy
    let expected: serde_json::Value =
        serde_json::from_slice(&std::fs::read(fixture().join("expected.json")).unwrap()).unwrap();
    let db = WalStorageAdapter::open_async(WalConfig { log_dir: tmp.path().to_path_buf(), ..WalConfig::test_config() })
        .await
        .expect("a format-2 directory must open");
    for (k, v) in expected["kv"].as_object().unwrap() {
        assert_eq!(db.get(&unhex(k)).await.unwrap(), Some(unhex(v.as_str().unwrap())), "key {k}");
    }
    for k in expected["absent"].as_array().unwrap() {
        assert_eq!(db.get(&unhex(k.as_str().unwrap())).await.unwrap(), None, "deleted key {k}");
    }
    let mut outbox = db.outbox_list().await.unwrap();
    outbox.sort();
    let want: Vec<(String, Vec<u8>)> = expected["outbox"].as_array().unwrap().iter()
        .map(|e| (e[0].as_str().unwrap().to_string(), unhex(e[1].as_str().unwrap())))
        .collect();
    assert_eq!(outbox, want);
    let catalog = prkdb::catalog::Catalog::new(std::sync::Arc::new(db), Vec::new());
    for (name, id) in expected["catalog"].as_object().unwrap() {
        assert_eq!(catalog.id_for_name(name).await.unwrap().0 as u64, id.as_u64().unwrap(), "catalog {name}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn regenerating_golden_v2_is_byte_identical() {
    let tmp = tempfile::tempdir().unwrap();
    prkdb_verify::golden::write_golden_v2(tmp.path()).await.unwrap();
    assert_eq!(files(tmp.path()), files(&fixture()), "format 2 output changed");
}
```

(`serde_json` becomes a `prkdb-verify` dev-dependency.) Run → fail (no fixture).
- [ ] **Step 3: Generate and commit the fixture** — `cargo run -p prkdb-verify --bin golden_v2 crates/prkdb-verify/tests/fixtures/format-v2`; run the two tests → pass; add `crates/prkdb-verify/tests/fixtures/format-v2/data/** binary` to `.gitattributes`. If `regenerating_golden_v2_is_byte_identical` is flaky, something in the write path is nondeterministic (a timestamp, a hash-map iteration order in the checkpoint): fix the writer (sort checkpoint entries by key), do not weaken the test.
- [ ] **Step 4: Pre-push and CI** — `pre-push-check.sh`: `step storage-compat; cargo nextest run -p prkdb-verify --test storage_compat` (also covered by the workspace run; the named step makes a compat failure obvious). The nightly Iggy-style check (the `main` binary writes, HEAD reads) is added after the Phase 2 PR merges — spec 2b says not before, because `main` writes format 1 until then; it is the first task of the Phase 3 plan.
- [ ] **Step 5: Commit** — `test: freeze format v2 with a golden data directory`.

---

### Task 2.25: Phase 2 durable baseline and gate

- [ ] **Step 1: Baseline.** Extend `scripts/capture_baseline.sh` to accept `--mode durable|fast` (sets `PRKDB_BENCH_SYNC_MODE`, which the §6.1 benches read to build their `WalConfig`) and to include `wal_write_path` and `recovery_bench`. On a quiet, cooled machine (decision record risk 3; or Linux), run `scripts/capture_baseline.sh docs/benchmarks/baseline-format-v2.toml --mode durable` and again with `--mode fast` appending a `[fast]` table. Commit `perf: record the phase 2 durable and fast baseline`.
- [ ] **Step 2: Perf notes.** For every `iai_hot_paths` benchmark the Phase 2 PR regresses by more than 5 % (expected: put and index insert — fsync bookkeeping, prior-record reads), add a `perf_note` to the responsible finding naming the durability or correctness reason (STO-02 for put, KEY-02 for index insert).
- [ ] **Step 3: Local gate.** `scripts/pre-push-check.sh` green (fmt, clippy, nextest, doctests, harness 200 seeds both modes, ledger check + render check, repo-status, readme tests, doc claims, single-WAL, storage-compat). `cargo xtask remediation check` shows every phase-2 finding `fixed` (STO-01..09, KEY-01..04, EVT-01..02, SCH-02, TST-05, TST-07, TST-09).
- [ ] **Step 4: Phase PR sequence** (spec §5): push; open `Phase 2: format v2 and single-node root fixes` (`remediation/phase-2 → main`); CI green including the perf gate (with floors); dispatch `remediation-gate` with `phase=2` (10k seeds × durable and fast, sharded). Record run URLs: `ci_evidence` for every phase-2 finding, `harness = "<sha> seeds=10000 mode=durable+fast profile=phase2 run=<gate URL>"` for STO/KEY (except KEY-03)/EVT findings, `gate_evidence` for phase 2; set findings `verified`, phase 2 `gate_passed`; render; commit `docs: record phase 2 gate evidence` (not `[skip ci]`); push; the maintainer merges with a merge commit (keep the branch).

---

# Phase 3 — Semantics (outline; expand at phase start)

- 3.1 Transaction batch record with commit marker (TXN-01) — test: crash between put and delete of one txn never leaves half.
- 3.2 Versioned read sets, one commit lock; Serializable default (TXN-03, TXN-04) — invert TXN-04 tripwire; two-read history test from the Sep 7 review R04.
- 3.3 Indexed transactions on the batch path (TXN-02).
- 3.4 Consumer at-least-once, generation fencing, no partition-0 fallback, errors propagate (EVT-06).
- 3.5 Partition-aware batches (EVT-03), sled single-transaction outbox (EVT-04), no silent non-atomic fallback (EVT-05).
- 3.6 TTL single record, conditional expiry, auto-start (TTL-01).
- 3.7 Harness Phase 3 profile (`Txn`, concurrency, `Poll`/`Commit`, consumer restart, `AdvanceClock`); gate.

# Phase 4 — Raft (outline; expand after 4a)

- 4.1 openraft spike (pinned version, `testing::Suite`), decision record against Phase 2 durable baseline.
- 4.2 Adopt or repair (RFT-01..07); `raft/` store with `truncate_after` / `purge_before`.
- 4.3 madsim simulation (TST-06) with `faultfs` behind `Vfs`; turmoil fallback.
- 4.4 RFT-08 peer auth, RFT-09 flaky baseline root cause, TST-02 workloads; invert RFT tripwires.
- 4.5 Freeze `raft/` format; remove experimental labels only if the gate holds.

# Phase 5 — Docs and release (outline)

- 5.1 Compiled doc samples (DOC-03), credentialed clients and Python client (DOC-02), global `--credential` (DOC-04).
- 5.2 Compose + 3-node smoke in CI (DOC-12); CLI reference from clap (DOC-05, DOC-07); new pages (DOC-08); strict dead links (DOC-10).
- 5.3 Release packaging (REL-01).

# Phase 6 — AI

Separate plan written at phase start from `2026-09-07-embedded-ai-state-design.md` and `2026-09-07-ai-database-design.md`, with this program's constraints (spec §7 Phase 6).
