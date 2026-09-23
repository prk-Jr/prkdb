//! Spec §4.2 invariants. Returns every violation, not just the first.

use super::evidence;
use super::model::{Ledger, PhaseStatus, Status};
use std::collections::{HashMap, HashSet};
use std::path::Path;

const PREFIXES: [&str; 10] = [
    "STO", "KEY", "EVT", "TXN", "TTL", "RFT", "SCH", "REL", "TST", "DOC",
];

pub fn check(ledger: &Ledger, root: &Path) -> Vec<String> {
    let mut errs = Vec::new();
    let mut seen = HashSet::new();
    let status_by_id: HashMap<&str, Status> = ledger
        .finding
        .iter()
        .map(|f| (f.id.as_str(), f.status))
        .collect();

    for f in &ledger.finding {
        let id = &f.id;
        // 1. unique, well-formed id
        if !seen.insert(id.as_str()) {
            errs.push(format!("{id}: duplicate id"));
        }
        let bytes = id.as_bytes();
        let well_formed = bytes.len() == 6
            && PREFIXES.contains(&f.prefix())
            && bytes[3] == b'-'
            && bytes[4..].iter().all(|b| b.is_ascii_digit());
        if !well_formed {
            errs.push(format!(
                "{id}: id must match ^(STO|KEY|EVT|TXN|TTL|RFT|SCH|REL|TST|DOC)-\\d{{2}}$"
            ));
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
                errs.push(format!(
                    "{id}: tripwire still exists; invert it into the regression test"
                ));
            }
        }
        // 3. open tripwires must exist
        if matches!(f.status, Status::Open | Status::InProgress) && !f.tripwire.is_empty() {
            if let Err(e) = evidence::resolve(root, &f.tripwire) {
                errs.push(format!("{id}: tripwire {e}"));
            }
        }
        // 4. nothing listed may be #[ignore]d
        for t in f
            .regression_tests
            .iter()
            .chain(std::iter::once(&f.tripwire))
        {
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
        if f.status == Status::Duplicate {
            if f.duplicate_of == *id {
                errs.push(format!("{id}: duplicate_of must not reference itself"));
            } else {
                match status_by_id.get(f.duplicate_of.as_str()) {
                    None => errs.push(format!("{id}: duplicate_of must name an existing id")),
                    Some(Status::Duplicate) => errs.push(format!(
                        "{id}: duplicate_of must not reference another duplicate"
                    )),
                    Some(_) => {}
                }
            }
        }
        // 7. phase is a single integer by construction (u8); range-check it
        if f.phase > 6 {
            errs.push(format!("{id}: phase must be 0..=6"));
        }
        // finding phases must reference a declared phase, but only once the ledger
        // actually declares phases (small fixtures without any [[phase]] still pass)
        if !ledger.phase.is_empty() && !ledger.phase.iter().any(|p| p.id == f.phase) {
            errs.push(format!(
                "{id}: phase {} has no matching [[phase]] entry",
                f.phase
            ));
        }
    }

    // duplicate [[phase]] ids
    let mut seen_phase_ids = HashSet::new();
    for p in &ledger.phase {
        if !seen_phase_ids.insert(p.id) {
            errs.push(format!("phase {}: duplicate phase id", p.id));
        }
    }

    // 8. gates
    for p in &ledger.phase {
        if p.status != PhaseStatus::GatePassed {
            continue;
        }
        if p.gate_evidence.is_empty() {
            errs.push(format!(
                "phase {}: gate_passed requires gate_evidence",
                p.id
            ));
        }
        for f in ledger.finding.iter().filter(|f| f.phase == p.id) {
            if !matches!(
                f.status,
                Status::Verified | Status::WontFix | Status::Duplicate
            ) {
                errs.push(format!(
                    "phase {}: {} is {:?}, not verified",
                    p.id, f.id, f.status
                ));
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
        fs::write(
            d.path().join("t/a.rs"),
            "#[test]\nfn trip() {}\n#[test]\nfn reg() {}\n#[test]\n#[ignore = \"slow: x\"]\nfn ign() {}\n",
        )
        .unwrap();
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
        assert!(check(&l, root().path())
            .iter()
            .any(|m| m.contains("tripwire still exists")));
    }

    #[test]
    fn ignored_regression_test_fails() {
        let l = one(
            "status = \"fixed\"\nregression_tests = [\"test:t/a.rs::ign\"]\nchanges = [\"abc\"]",
        );
        assert!(check(&l, root().path())
            .iter()
            .any(|m| m.contains("#[ignore]d")));
    }

    #[test]
    fn verified_harness_area_needs_harness() {
        let l = one("status = \"verified\"\nregression_tests = [\"test:t/a.rs::reg\"]\nchanges = [\"abc\"]\nci_evidence = \"https://x\"");
        assert!(check(&l, root().path())
            .iter()
            .any(|m| m.contains("requires harness")));
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
        assert!(check(&l, root().path())
            .iter()
            .any(|m| m.contains("id must match")));
    }

    #[test]
    fn self_duplicate_fails() {
        let l = Ledger::parse("[[finding]]\nid = \"STO-01\"\ntitle = \"t\"\narea = \"s\"\nseverity = \"low\"\nphase = 0\nstatus = \"duplicate\"\nduplicate_of = \"STO-01\"\n").unwrap();
        assert!(check(&l, root().path())
            .iter()
            .any(|m| m.contains("must not reference itself")));
    }

    #[test]
    fn duplicate_of_a_duplicate_fails() {
        let text = concat!(
            "[[finding]]\nid = \"STO-01\"\ntitle = \"a\"\narea = \"s\"\nseverity = \"low\"\nphase = 0\nstatus = \"duplicate\"\nduplicate_of = \"STO-02\"\n",
            "[[finding]]\nid = \"STO-02\"\ntitle = \"b\"\narea = \"s\"\nseverity = \"low\"\nphase = 0\nstatus = \"duplicate\"\nduplicate_of = \"STO-03\"\n",
            "[[finding]]\nid = \"STO-03\"\ntitle = \"c\"\narea = \"s\"\nseverity = \"low\"\nphase = 0\nstatus = \"open\"\n",
        );
        let e = check(&Ledger::parse(text).unwrap(), root().path());
        assert!(e
            .iter()
            .any(|m| m.contains("must not reference another duplicate")));
    }

    #[test]
    fn finding_phase_without_declared_phase_entry_fails_when_phases_declared() {
        let text = concat!(
            "[[finding]]\nid = \"STO-01\"\ntitle = \"a\"\narea = \"s\"\nseverity = \"low\"\nphase = 3\nstatus = \"open\"\n",
            "[[phase]]\nid = 0\ntitle = \"p\"\nstatus = \"not_started\"\n",
        );
        let e = check(&Ledger::parse(text).unwrap(), root().path());
        assert!(e.iter().any(|m| m.contains("no matching [[phase]] entry")));
    }

    #[test]
    fn finding_phase_without_declared_phase_entry_is_ok_when_no_phases_declared() {
        let l = one("status = \"open\"");
        assert!(!check(&l, root().path())
            .iter()
            .any(|m| m.contains("no matching [[phase]] entry")));
    }

    #[test]
    fn duplicate_phase_ids_fail() {
        let text = concat!(
            "[[phase]]\nid = 0\ntitle = \"a\"\nstatus = \"not_started\"\n",
            "[[phase]]\nid = 0\ntitle = \"b\"\nstatus = \"not_started\"\n",
        );
        let e = check(&Ledger::parse(text).unwrap(), root().path());
        assert!(e.iter().any(|m| m.contains("duplicate phase id")));
    }
}
