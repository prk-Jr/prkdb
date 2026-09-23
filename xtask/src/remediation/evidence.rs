//! Resolves evidence targets (spec §4.2 invariant 2). These checks prove evidence
//! *exists*; `ci_evidence` proves it ran.
//!
//! `test:`/tripwire targets are resolved by actually parsing the Rust source with
//! `syn` and walking every `fn` item (including inside nested `mod`s) rather than
//! substring-matching lines, so a comment or a same-named helper can't fool the
//! checker into reporting evidence that doesn't really exist.

use std::path::{Component, Path};
use syn::visit::{self, Visit};
use syn::{Attribute, ItemFn, Meta};

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
        if !valid_rel_path(file) {
            return Err(format!("`{target}`: `{file}` is not a valid relative path"));
        }
        if func.is_empty() {
            return Err(format!("`{target}`: function name is empty"));
        }
        return Ok(Target::Test { file, func });
    }
    if let Some(rest) = target.strip_prefix("script:") {
        if !valid_rel_path(rest) {
            return Err(format!("`{target}`: `{rest}` is not a valid relative path"));
        }
        return Ok(Target::Script(rest));
    }
    if let Some(rest) = target.strip_prefix("ci-job:") {
        let (workflow, job) = rest
            .split_once('/')
            .ok_or_else(|| format!("`{target}`: expected ci-job:<workflow file>/<job id>"))?;
        if !valid_rel_path(workflow) {
            return Err(format!(
                "`{target}`: `{workflow}` is not a valid relative path"
            ));
        }
        if job.is_empty() {
            return Err(format!("`{target}`: job id is empty"));
        }
        return Ok(Target::CiJob { workflow, job });
    }
    if let Some(rest) = target.strip_prefix("xtask:") {
        if rest.split_whitespace().next().is_none() {
            return Err(format!("`{target}`: expected xtask:<subcommand words>"));
        }
        return Ok(Target::Xtask(rest));
    }
    Err(format!(
        "`{target}`: unknown evidence kind (test:, script:, ci-job:, xtask:)"
    ))
}

/// Rejects empty, absolute, and `..`-containing relative paths.
fn valid_rel_path(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let path = Path::new(s);
    if path.is_absolute() {
        return false;
    }
    !path.components().any(|c| matches!(c, Component::ParentDir))
}

/// Returns Ok if the target exists under `root`.
pub fn resolve(root: &Path, target: &str) -> Result<(), String> {
    match parse(target)? {
        Target::Test { file, func } => {
            let item = find_fn_item(root, file, func)?
                .ok_or_else(|| format!("`{target}`: fn `{func}` not found in {file}"))?;
            if !has_test_attr(&item.attrs) {
                return Err(format!(
                    "`{target}`: fn `{func}` in {file} does not carry a test attribute"
                ));
            }
            Ok(())
        }
        Target::Script(path) => {
            let full = root.join(path);
            let meta = std::fs::metadata(&full)
                .map_err(|e| format!("`{target}`: {path} missing ({:?})", e.kind()))?;
            if !meta.is_file() {
                return Err(format!("`{target}`: {path} is not a regular file"));
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if meta.permissions().mode() & 0o111 == 0 {
                    return Err(format!("`{target}`: {path} is not executable"));
                }
            }
            Ok(())
        }
        Target::CiJob { workflow, job } => resolve_ci_job(root, target, workflow, job),
        Target::Xtask(sub) => resolve_xtask(root, target, sub),
    }
}

/// Only accepts job keys directly under the top-level `jobs:` mapping: a line at
/// exactly 2-space indent, ending in a colon (with an optional trailing comment).
/// Scanning stops at the next column-0, non-comment, non-blank line.
fn resolve_ci_job(root: &Path, target: &str, workflow: &str, job: &str) -> Result<(), String> {
    let text = read(root, &format!(".github/workflows/{workflow}"))?;
    let lines: Vec<&str> = text.lines().collect();
    let Some(jobs_idx) = lines.iter().position(|l| l.trim_end() == "jobs:") else {
        return Err(format!(
            "`{target}`: no top-level `jobs:` key in {workflow}"
        ));
    };
    for line in &lines[jobs_idx + 1..] {
        if !line.starts_with(' ') {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            break;
        }
        let Some(rest) = line.strip_prefix("  ") else {
            continue;
        };
        if rest.starts_with(' ') || rest.starts_with('\t') {
            // More than 2-space indent: nested under a job, not a job key.
            continue;
        }
        let Some(colon_idx) = rest.find(':') else {
            continue;
        };
        let key = &rest[..colon_idx];
        if key.is_empty()
            || !key
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            continue;
        }
        let after = rest[colon_idx + 1..].trim_start();
        if !(after.is_empty() || after.starts_with('#')) {
            continue;
        }
        if key == job {
            return Ok(());
        }
    }
    Err(format!(
        "`{target}`: job `{job}` not defined under top-level `jobs:` in {workflow}"
    ))
}

/// Splits the target into words and requires a line in `xtask/src/main.rs` whose
/// trimmed start is the quoted word list followed by `]` or `,`.
fn resolve_xtask(root: &Path, target: &str, sub: &str) -> Result<(), String> {
    let words: Vec<&str> = sub.split_whitespace().collect();
    if words.is_empty() {
        return Err(format!("`{target}`: empty xtask target"));
    }
    let text = read(root, "xtask/src/main.rs")?;
    let quoted: Vec<String> = words.iter().map(|w| format!("\"{w}\"")).collect();
    let needle = format!("[{}", quoted.join(", "));
    let found = text.lines().any(|l| {
        l.trim_start()
            .strip_prefix(needle.as_str())
            .map(|rest| {
                let rest = rest.trim_start();
                rest.starts_with(']') || rest.starts_with(',')
            })
            .unwrap_or(false)
    });
    if found {
        Ok(())
    } else {
        Err(format!(
            "`{target}`: xtask subcommand `{sub}` not dispatched"
        ))
    }
}

fn has_test_attr(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|a| match &a.meta {
        Meta::Path(p) => p
            .segments
            .last()
            .map(|s| s.ident == "test")
            .unwrap_or(false),
        Meta::List(list) => list
            .path
            .segments
            .last()
            .map(|s| s.ident == "test")
            .unwrap_or(false),
        Meta::NameValue(_) => false,
    })
}

fn has_ignore_attr(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|a| match &a.meta {
        Meta::Path(p) => p.is_ident("ignore"),
        Meta::NameValue(nv) => nv.path.is_ident("ignore"),
        Meta::List(list) => {
            list.path.is_ident("cfg_attr") && list.tokens.to_string().contains("ignore")
        }
    })
}

/// Parses `file` under `root` and returns the fn item named `name`, walking into
/// nested modules. `Ok(None)` means the file parsed but no such fn exists.
fn find_fn_item(root: &Path, file: &str, name: &str) -> Result<Option<ItemFn>, String> {
    let text = read(root, file)?;
    let parsed = syn::parse_file(&text).map_err(|e| format!("{file}: failed to parse: {e}"))?;

    struct Finder<'a> {
        name: &'a str,
        found: Option<ItemFn>,
    }
    impl<'a, 'ast> Visit<'ast> for Finder<'a> {
        fn visit_item_fn(&mut self, node: &'ast ItemFn) {
            if self.found.is_none() && node.sig.ident == self.name {
                self.found = Some(node.clone());
            }
            visit::visit_item_fn(self, node);
        }
    }

    let mut finder = Finder { name, found: None };
    finder.visit_file(&parsed);
    Ok(finder.found)
}

/// True if the fn named `func` in `file` carries an `#[ignore]` or
/// `#[cfg_attr(.., ignore)]` attribute, determined from its own parsed attributes
/// rather than a line window.
pub fn is_ignored(root: &Path, target: &str) -> bool {
    let Ok(Target::Test { file, func }) = parse(target) else {
        return false;
    };
    match find_fn_item(root, file, func) {
        Ok(Some(item)) => has_ignore_attr(&item.attrs),
        _ => false,
    }
}

fn read(root: &Path, rel: &str) -> Result<String, String> {
    std::fs::read_to_string(root.join(rel)).map_err(|e| format!("{rel} missing ({:?})", e.kind()))
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
            r#"
// good referenced above
#[test]
fn good() {}

fn helper_without_test_attr() {}

// fn commented_out() {}

#[test]
#[ignore = "slow: x"]
fn skipped() {}

#[test]
#[cfg_attr(not(miri), ignore)]
fn skipped_cfg() {}

#[test]
#[cfg_attr(
    not(miri),
    ignore
)]
fn skipped_multiline() {}

#[test]
fn short() {}

mod nested {
    #[test]
    fn nested_good() {}
}
"#,
        )
        .unwrap();
        fs::create_dir_all(dir.path().join(".github/workflows")).unwrap();
        fs::write(
            dir.path().join(".github/workflows/ci.yml"),
            "on:\n  push:\njobs:\n  remediation:  # c\n    runs-on: x\n",
        )
        .unwrap();
        fs::create_dir_all(dir.path().join("xtask/src")).unwrap();
        fs::write(
            dir.path().join("xtask/src/main.rs"),
            "        [\"repo-status\", \"render\"] => repo_status::render(),\n        [\"remediation\", \"check\"] => remediation::run_check(),\n",
        )
        .unwrap();
        dir
    }

    #[test]
    fn resolves_test_targets_requiring_real_test_attr() {
        let r = repo();
        assert!(resolve(r.path(), "test:t/a.rs::good").is_ok());
        assert!(resolve(r.path(), "test:t/a.rs::nested_good").is_ok());
        assert!(resolve(r.path(), "test:t/a.rs::helper_without_test_attr").is_err());
        assert!(resolve(r.path(), "test:t/a.rs::commented_out").is_err());
        assert!(resolve(r.path(), "test:t/a.rs::missing").is_err());
    }

    #[test]
    fn is_ignored_uses_the_fns_own_attrs_not_a_line_window() {
        let r = repo();
        assert!(is_ignored(r.path(), "test:t/a.rs::skipped"));
        assert!(is_ignored(r.path(), "test:t/a.rs::skipped_cfg"));
        assert!(is_ignored(r.path(), "test:t/a.rs::skipped_multiline"));
        assert!(!is_ignored(r.path(), "test:t/a.rs::good"));
        // `short` follows two ignored fns; it must not inherit their ignore status.
        assert!(!is_ignored(r.path(), "test:t/a.rs::short"));
    }

    #[test]
    fn ci_job_only_matches_the_top_level_jobs_mapping() {
        let r = repo();
        assert!(resolve(r.path(), "ci-job:ci.yml/push").is_err());
        assert!(resolve(r.path(), "ci-job:ci.yml/remediation").is_ok());
        assert!(resolve(r.path(), "ci-job:ci.yml/nope").is_err());
    }

    #[test]
    fn xtask_target_requires_the_full_word_sequence() {
        let r = repo();
        assert!(resolve(r.path(), "xtask:render").is_err());
        assert!(resolve(r.path(), "xtask:remediation check").is_ok());
    }

    #[test]
    fn parse_rejects_malformed_targets() {
        assert!(parse("script:").is_err());
        assert!(parse("script:/bin/sh").is_err());
        assert!(parse("test:../x.rs::f").is_err());
        assert!(parse("bogus:x").is_err());
    }

    #[test]
    fn script_must_be_an_executable_regular_file() {
        let r = repo();
        fs::create_dir_all(r.path().join("adir")).unwrap();
        assert!(resolve(r.path(), "script:adir").is_err());

        let script = r.path().join("run.sh");
        fs::write(&script, "#!/bin/sh\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
        }
        assert!(resolve(r.path(), "script:run.sh").is_ok());
    }
}
