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
        Target::Test { file, func } => match fn_status(root, file, func) {
            FnStatus::FileMissing => Err(format!("`{target}`: {file} missing (NotFound)")),
            FnStatus::ParseError(e) => Err(format!("`{target}`: {e}")),
            FnStatus::NotFound => Err(format!("`{target}`: fn `{func}` not found in {file}")),
            FnStatus::Ambiguous(n) => Err(format!("`{target}`: ambiguous: {n} fns named {func}")),
            FnStatus::Found(item) => {
                if has_test_attr(&item.attrs) {
                    Ok(())
                } else {
                    Err(format!(
                        "`{target}`: fn `{func}` in {file} does not carry a test attribute"
                    ))
                }
            }
        },
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

/// Outcome of looking up a named fn in a source file, distinguishing "the file
/// doesn't exist" from "the file exists but can't be parsed" from "it parsed
/// but the fn isn't there" from "more than one fn has that name" — each of
/// which a caller may need to treat differently (see [`tripwire_is_gone`]).
enum FnStatus {
    /// No file at that path.
    FileMissing,
    /// The file exists but isn't valid Rust (or couldn't be read for another
    /// reason); we can't tell whether the fn is there.
    ParseError(String),
    /// The file parsed; no fn with that name exists anywhere in it.
    NotFound,
    /// The file parsed; more than one fn with that name exists (e.g. in
    /// different `mod`s), so which one is "the" evidence is ambiguous.
    Ambiguous(usize),
    /// Exactly one fn with that name exists.
    Found(Box<ItemFn>),
}

/// Parses `file` under `root` and looks for every fn item named `name`,
/// walking into nested modules.
fn fn_status(root: &Path, file: &str, name: &str) -> FnStatus {
    let text = match std::fs::read_to_string(root.join(file)) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return FnStatus::FileMissing,
        Err(e) => return FnStatus::ParseError(format!("{file} unreadable ({:?})", e.kind())),
    };
    let parsed = match syn::parse_file(&text) {
        Ok(p) => p,
        Err(e) => return FnStatus::ParseError(format!("{file}: failed to parse: {e}")),
    };

    struct Finder<'a> {
        name: &'a str,
        matches: Vec<ItemFn>,
    }
    impl<'a, 'ast> Visit<'ast> for Finder<'a> {
        fn visit_item_fn(&mut self, node: &'ast ItemFn) {
            if node.sig.ident == self.name {
                self.matches.push(node.clone());
            }
            visit::visit_item_fn(self, node);
        }
    }

    let mut finder = Finder {
        name,
        matches: Vec::new(),
    };
    finder.visit_file(&parsed);
    match finder.matches.len() {
        0 => FnStatus::NotFound,
        1 => FnStatus::Found(Box::new(finder.matches.remove(0))),
        n => FnStatus::Ambiguous(n),
    }
}

/// True if the fn named `func` in `file` carries an `#[ignore]` or
/// `#[cfg_attr(.., ignore)]` attribute, determined from its own parsed attributes
/// rather than a line window. Returns `false` (can't tell) when the fn is
/// missing, ambiguous, or the file doesn't parse.
pub fn is_ignored(root: &Path, target: &str) -> bool {
    let Ok(Target::Test { file, func }) = parse(target) else {
        return false;
    };
    matches!(fn_status(root, file, func), FnStatus::Found(item) if has_ignore_attr(&item.attrs))
}

/// Spec §4.2 invariant 2: a tripwire counts as "gone" only if its file is
/// missing, or the file parses cleanly and contains no fn with that name.
/// A file that exists but fails to parse (or an ambiguous match) is reported
/// as an error instead of silently being treated as gone, since we genuinely
/// can't tell whether the tripwire was removed.
///
/// Only meaningful for `test:` targets (tripwires are always fn references);
/// other target kinds fall back to plain [`resolve`] semantics.
pub fn tripwire_is_gone(root: &Path, target: &str) -> Result<bool, String> {
    match parse(target)? {
        Target::Test { file, func } => match fn_status(root, file, func) {
            FnStatus::FileMissing | FnStatus::NotFound => Ok(true),
            FnStatus::Found(_) => Ok(false),
            FnStatus::ParseError(e) => Err(e),
            FnStatus::Ambiguous(n) => Err(format!("ambiguous: {n} fns named {func}")),
        },
        _ => Ok(resolve(root, target).is_err()),
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

    #[cfg(unix)]
    #[test]
    fn non_executable_script_is_rejected() {
        let r = repo();
        let script = r.path().join("run.sh");
        fs::write(&script, "#!/bin/sh\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&script, fs::Permissions::from_mode(0o644)).unwrap();
        let err = resolve(r.path(), "script:run.sh").unwrap_err();
        assert!(err.contains("not executable"), "{err}");
    }

    #[test]
    fn empty_fn_name_is_rejected() {
        let err = parse("test:x.rs::").unwrap_err();
        assert!(err.contains("function name is empty"), "{err}");
    }

    #[test]
    fn ambiguous_fn_name_is_reported_by_resolve_and_is_ignored() {
        let r = repo();
        fs::write(
            r.path().join("t/b.rs"),
            "mod one {\n    #[test]\n    fn foo() {}\n}\n\nmod two {\n    #[test]\n    fn foo() {}\n}\n",
        )
        .unwrap();
        let err = resolve(r.path(), "test:t/b.rs::foo").unwrap_err();
        assert!(err.contains("ambiguous: 2 fns named foo"), "{err}");
        // Can't reliably say an ambiguous fn is ignored; is_ignored stays false.
        assert!(!is_ignored(r.path(), "test:t/b.rs::foo"));
    }

    #[test]
    fn tripwire_parse_error_is_reported_not_treated_as_gone() {
        let r = repo();
        fs::write(r.path().join("t/broken.rs"), "fn ( { this is not rust").unwrap();
        let err = tripwire_is_gone(r.path(), "test:t/broken.rs::anything").unwrap_err();
        assert!(err.contains("parse"), "{err}");
    }

    #[test]
    fn tripwire_is_gone_when_file_missing_or_fn_absent() {
        let r = repo();
        assert!(tripwire_is_gone(r.path(), "test:t/does-not-exist.rs::x").unwrap());
        assert!(tripwire_is_gone(r.path(), "test:t/a.rs::not_a_real_fn").unwrap());
        assert!(!tripwire_is_gone(r.path(), "test:t/a.rs::good").unwrap());
    }
}
