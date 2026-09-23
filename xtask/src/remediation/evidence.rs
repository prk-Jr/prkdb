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
    Err(format!(
        "`{target}`: unknown evidence kind (test:, script:, ci-job:, xtask:)"
    ))
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
            let meta =
                std::fs::metadata(&full).map_err(|_| format!("`{target}`: {path} missing"))?;
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
                return Err(format!(
                    "`{target}`: xtask subcommand `{sub}` not dispatched"
                ));
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
    let Ok(Target::Test { file, func }) = parse(target) else {
        return false;
    };
    let Ok(text) = read(root, file) else {
        return false;
    };
    let lines: Vec<&str> = text.lines().collect();
    let Some(idx) = find_fn(&text, func) else {
        return false;
    };
    lines[idx.saturating_sub(6)..idx]
        .iter()
        .any(|l| l.trim_start().starts_with("#[ignore"))
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
        fs::write(
            dir.path().join(".github/workflows/ci.yml"),
            "jobs:\n  remediation:\n    runs-on: x\n",
        )
        .unwrap();
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
