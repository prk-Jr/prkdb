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
| 0 Honesty and tracking | Full (code-level) | not started |
| 1 Harness and baseline | Full (code-level) | not started |
| 2 Format v2 + single-node | Task-level; code-level detail is written after Task 2.1 (the WAL spike), because the spike decides the write-path shape | not started |
| 3 Semantics | Outline — expand at phase start | — |
| 4 Raft | Outline — expand after the 4a spike | — |
| 5 Docs and release | Outline — expand at phase start | — |
| 6 AI | Separate plan per Sep 7 specs | — |

---

## Conventions (apply to every task)

- **Branch:** all work is on `remediation/root-cause`. Never push to `origin` except in a phase-gate task (§5.1).
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
step ledger;       cargo xtask remediation check && cargo xtask remediation render --check
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
retries = 2            # retries are reported in the summary, never silent
failure-output = "immediate-final"
fail-fast = false

[test-groups]
process-spawning = { max-threads = 1 }

[[profile.default.overrides]]
filter = "test(/_tripwire$/) | binary(raft_chaos_tests) | binary(in_process_cluster)"
test-group = "process-spawning"
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

- [ ] **Step 2: Test** — spawns the child (`env!("CARGO_BIN_EXE_crash_child")`), reads stdout until `ACK 199`, sends `SIGKILL` (`child.kill()` sends SIGKILL on Unix), reopens with `open_async`, asserts `k0..k199` all present. It takes a few seconds, so it runs on every PR (not `#[ignore]`d); add `| binary(sigkill)` to the nextest `process-spawning` filter. Gate the file with `#![cfg(unix)]`.

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

Code-level steps for Tasks 2.2–2.19 are written into this file **after Task 2.1**, because the spike decides the write-path shape. Every task below already names its findings, files, and the failing test that opens it.

### Task 2.1: Single-log group-commit spike (1–2 days)

**Files:** `crates/prkdb-core/benches/wal_write_path_spike.rs` (throwaway branch-local bench), `docs/remediation/decisions/2026-xx-single-log-spike.md`

- [ ] Prototype `SingleLog`: one active segment behind `Vfs`, a `std::thread` writer that drains a channel of `(record bytes, oneshot)`, `pwrite`s the batch, `sync_data` once (Durable) or not (Fast), then completes all oneshots.
- [ ] Bench vs current `MmapParallelWal` path: put throughput and p99 at 1, 8, 64 concurrent writers, 1 KiB and 64 KiB values, Durable and Fast.
- [ ] Decision rule (spec 2a): proceed if Fast loses ≤ 15 % vs the Phase 1 baseline and the single writer is not the bottleneck; otherwise **stop and report** to the maintainer with numbers and the sharded-with-global-sequence alternative.
- [ ] Write the decision record; then expand Tasks 2.2–2.19 to code level in this plan and commit (`docs: expand phase 2 plan after WAL spike`).

### Task 2.2: One globally ordered WAL through `Vfs` with a writer thread (STO-06, STO-02, STO-05)
Failing tests first: `crates/prkdb-verify/tests/harness.rs::durable_put_survives_power_loss` (fails today: no `Vfs`) and `crates/prkdb-core/tests/single_log.rs::replay_order_equals_append_order_across_segment_roll` (append three segments' worth of records from 8 tasks, reopen, replay yields exactly append order by global offset — STO-05). Files: `crates/prkdb-core/src/wal/single_log.rs`, `crates/prkdb-core/src/wal/segment.rs` (new), `crates/prkdb-core/src/wal/config.rs` (`SyncMode`; remove `segment_count`/`shard_count`), `crates/prkdb/src/storage/wal_adapter.rs` (switch to it).

### Task 2.3: CRC on open, torn-tail truncation, directory fsync (STO-04)
Files: `crates/prkdb-core/src/wal/segment.rs` (open/scan), `crates/prkdb-core/src/wal/single_log.rs` (dir fsync on roll/remove). Failing test `crates/prkdb-core/tests/single_log.rs::torn_tail_is_truncated_and_later_appends_survive`: write records, corrupt the middle of the last record via `FaultFs`, reopen → earlier records readable, tail truncated, later appends visible after next reopen.

### Task 2.4: Delete the other WAL implementations (STO-06)
Migrate every caller found by `grep -rlw "ParallelWal\|AsyncParallelWal\|MmapParallelWal\|WriteAheadLog" crates --include='*.rs'`; delete modules; `cargo build --workspace` and full tests green.

### Task 2.5: `PowerLoss` and Fast mode in the harness (TST-05)
Files: `crates/prkdb-verify/src/{ops.rs, sut.rs, checker.rs, bin/verify.rs}`. Failing tests in `crates/prkdb-verify/tests/harness.rs`: `fast_mode_checker_catches_lost_synced_data` (a Fast-mode SUT that discards already-synced data must be caught) and `blocking_profile_includes_power_loss` (generator). Extend `ops.rs` (`Op::PowerLoss`), `sut.rs` (`FaultFs`-backed SUT), `checker.rs` (Fast prefix check), `xtask verify --mode fast`. Blocking profile gains `PowerLoss` per §7.1.

### Task 2.6: Format v2 marker, open rules, migration registry, `prkdb-cli migrate` (D3, D4)
First read `crates/prkdb-core/tests/format_version.rs` and whatever it tests: if a format-version mechanism already exists, extend it (or delete it) rather than adding a second one; this task must end with exactly one. Files: `crates/prkdb/src/storage/format.rs` (new: `FORMAT` read/write, open rules), `crates/prkdb/src/storage/migrations.rs` (new: `Migration` trait, empty registry), `crates/prkdb/src/storage/wal_adapter.rs` (format check in `new`/`open_async`), `crates/prkdb-cli/src/commands/migrate.rs` (new) + registration in `crates/prkdb-cli/src/commands.rs`. Failing tests in `crates/prkdb/tests/format_v2.rs`: empty dir → `FORMAT` created with `format = 2`; non-empty dir without `FORMAT` → error text containing "format"; `prkdb-cli migrate --data-dir` prints "no migrations available for format 2".

### Task 2.7: Key codec and collection catalog (KEY-01)
Invert `key01_collections_share_primary_keys_tripwire` into `key01_collections_with_same_id_are_independent` (get/query/update/delete/restart). Files: new `crates/prkdb/src/keys.rs` (codec), `crates/prkdb/src/catalog.rs`, `indexed_storage.rs` call sites.

### Task 2.8: Stable partitioner (KEY-03)
Invert the KEY-03 tripwire into `key03_partition_is_stable_across_processes` plus golden vectors (`partition("user-42", 1024) == <value>` for 10 keys). Switch `partitioning.rs` to seahash with a fixed seed.

### Task 2.9: Event identity from the WAL offset (EVT-01)
Files: `crates/prkdb/src/outbox.rs` (remove `OUTBOX_SEQ`; add `EventSeq`), `crates/prkdb/src/consumer.rs` (offsets as `EventSeq`), `crates/prkdb-storage-sled/src/lib.rs` and `crates/prkdb-storage-sql/src/` (persisted sequence row). Invert the EVT-01 tripwire into `evt01_event_ids_do_not_repeat_across_processes`; add a restart test where a consumer at offset N sees every new event. Opaque `EventSeq` type. Sled/SQL: persisted sequence row in the same transaction.

### Task 2.10: Checkpoint = index snapshot (STO-01)
Invert both STO-01 tripwires; add property test `recover(checkpoint, wal) == recover(∅, wal)` in `prkdb-verify`. Discovery `Checkpoint` op moves to the blocking profile.

### Task 2.11: Real compaction (STO-01 follow-through)
Files: `crates/prkdb-core/src/wal/compaction.rs`, `crates/prkdb-core/src/wal/single_log.rs`, `crates/prkdb/src/storage/wal_adapter.rs:~1737` (compactor trigger). Failing test `crates/prkdb/tests/compaction_test.rs::compaction_keeps_only_live_values_and_removes_old_segments`: write > segment size with overwrites, compact, reopen → only live values, old segments removed.

### Task 2.12: Append/publish ordering (STO-03)
Files: `crates/prkdb/src/storage/wal_adapter.rs:~478-509` (publish under the writer's ordering point). Failing test `crates/prkdb/tests/durability.rs::concurrent_same_key_live_equals_recovered`: 8 concurrent writers to one key, many rounds, then reopen; live value must equal recovered value (the concurrency op in the harness, if not caught earlier).

### Task 2.13: `BatchAccumulator::flush` barrier (STO-07)
Failing test adapted from `docs/reviews/probes/core_review.rs::flush_returns_while_the_executor_is_blocked` (assert flush stays pending while the executor is blocked; executor error is returned by flush).

### Task 2.14: Upsert index cleanup and unique enforcement (KEY-02)
Files: `crates/prkdb/src/indexed_storage.rs:~4733` (upsert); extract index maintenance into `crates/prkdb/src/index_maintenance.rs` while rewriting it (spec §9). Failing tests in `crates/prkdb/tests/indexed_db_tests.rs`: update a record's indexed field → old value's query returns nothing; insert duplicate on `#[index(unique)]` → typed error, no partial write.

### Task 2.15: Outbox persisted with its data (EVT-02)
Failing test: put with outbox on `WalStorageAdapter`, reopen, `outbox_list` still contains the event; partitioned adapter returns `UnsupportedCapability` instead of Ok.

### Task 2.16: Schema persistence (SCH-02)
Failing tests from `docs/reviews/probes/schema_review.rs::missing_descriptor_is_loaded_as_empty` (must fail to load); concurrent registrations never reuse a version.

### Task 2.17: Fuzz targets (TST-07)
`fuzz/` with `cargo-fuzz` targets: `wal_record_decode`, `segment_scan`, `checkpoint_load`, `proto_decode`. Nightly job runs each for 5 minutes.

### Task 2.18: Golden v2 data directory and compat check
Generator writes `tests/fixtures/format-v2/` covering every record type; test reads it and asserts contents. Added to `pre-push-check.sh` and CI.

### Task 2.19: Phase 2 durable baseline and gate
`scripts/capture_baseline.sh docs/benchmarks/baseline-format-v2.toml` (Durable and Fast); ledger evidence; phase PR sequence with `remediation-gate phase=2`.

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
