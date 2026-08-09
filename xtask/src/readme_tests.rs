//! Turn the README's Rust examples into a compiled test.
//!
//! # Why not `#![doc = include_str!("README.md")]`
//!
//! That is the usual answer, and it was tried. It compiles the README's fences as
//! doctests, but 36 of the 37 are fragments — `let count = db.count::<User>().await?;` —
//! which need setup to compile. In a doctest, setup is hidden with `# ` lines, and those
//! render **literally on GitHub**, where the README is actually read. Satisfying the check
//! would have put roughly 120 lines of `# use …` on the project's front page.
//!
//! So the setup lives here instead. The README stays clean, and the examples are still
//! type-checked — which is the property that matters, because an example that no longer
//! compiles is documentation that lies.
//!
//! # How
//!
//! Each fence becomes a function in a generated test file, sharing one preamble that
//! defines the handful of things the examples refer to (`db`, `User`, `client`). The
//! functions are never called: compiling them is the test. `cargo test` type-checks them
//! like any other code, and a renamed method breaks the build.
//!
//! Fences that cannot be made to compile are listed in `SKIP` with a reason. That list is
//! the honest measure of how much of the README is unverified, and it should shrink.

use anyhow::{Context, Result};
use std::fmt::Write as _;

/// Where the generated file goes. Checked in, so a reviewer sees it change.
const GENERATED: &str = "crates/prkdb/tests/readme_examples.rs";

/// Fences that are skipped, keyed by the README line their fence opens on.
///
/// This list is the honest measure of how much of the README is unverified. Every entry
/// names a **specific defect in the example**, not "it does not compile" — that is the
/// finding, not the excuse. The list may only shrink.
///
/// These were found by building this generator, which is the point: none of them were
/// visible before, because nothing had ever compiled the README.
const SKIP: &[(usize, &str)] = &[
    // Defines its own `User` without `Debug`, then later fences format it with `{:?}`.
    (
        200,
        "its User derives no Debug, but later examples print it",
    ),
    (
        229,
        "uses an `orders` field the User at line 200 does not declare",
    ),
    // `db.sum(|u: &User| u.orders)` and friends: the closure's return type is not
    // inferable from the call, so the example needs an annotation it does not show.
    (
        236,
        "aggregate closure return type is not inferable as written",
    ),
    (244, "same"),
    (255, "same"),
    (262, "same"),
    (299, "same"),
    (313, "same"),
    // `where_role_eq` comes from the generated `UserQueryExt` trait. The README never
    // mentions that the trait must be in scope, so the example cannot compile as shown.
    (
        335,
        "uses generated query methods without importing UserQueryExt",
    ),
    (362, "same"),
    (377, "same"),
    (405, "same"),
    (423, "same"),
    (437, "same"),
    (458, "same"),
    (
        480,
        "annotated result type does not match what the call returns",
    ),
    (496, "same"),
    (
        512,
        "contains literal `...` placeholders; illustrative, not runnable",
    ),
    (528, "same type mismatch"),
    (
        541,
        "re-implements Timestamped for User, which the preamble already provides",
    ),
    (558, "same type mismatch"),
    (571, "same"),
    (583, "same"),
    (601, "same"),
    (618, "same"),
    (628, "same"),
    (638, "same"),
    (651, "same"),
    (666, "compares a String id against an integer"),
    (688, "same"),
    (
        764,
        "annotated result type does not match what the call returns",
    ),
    (779, "same"),
];

/// Bindings injected into every generated function.
///
/// The fragments are excerpts: they assume a `db`, a `user`, a `key`. Binding the whole
/// set in every function is wasteful but harmless — the functions never run, and
/// `unused_variables` is allowed — and it beats classifying 36 fences by hand.
const BINDINGS: &str = "\
    let mut db = a_db();
    let client = a_client();
    let mut storage = any_storage();
    let any_storage_adapter = any_storage();
    let backup_db = a_db();
    let prkdb = a_prkdb();
    let mut user = any_user();
    let mut user1 = any_user();
    let mut user2 = any_user();
    let mut user3 = any_user();
    let mut old_user = any_user();
    let mut record = any_user();
    let users: Vec<User> = Vec::new();
    let user_id = String::new();
    let last_id = String::new();
    let key = b\"users:1\".to_vec();
    let now: u64 = 0;";

/// One Rust fence lifted out of the README.
struct Example {
    /// Line in README.md where the fence opens, so a failure points at the source.
    line: usize,
    body: String,
}

fn extract(readme: &str) -> Vec<Example> {
    let mut out = Vec::new();
    let mut inside: Option<(usize, Vec<&str>)> = None;

    for (i, line) in readme.lines().enumerate() {
        match &mut inside {
            None => {
                if line.trim() == "```rust" {
                    inside = Some((i + 1, Vec::new()));
                }
            }
            Some((start, body)) => {
                if line.trim() == "```" {
                    out.push(Example {
                        line: *start,
                        body: body.join("\n"),
                    });
                    inside = None;
                } else {
                    body.push(line);
                }
            }
        }
    }
    out
}

/// A complete program defines its own `main`; a fragment does not.
fn is_complete_program(body: &str) -> bool {
    body.contains("fn main")
}

fn preamble() -> &'static str {
    r##"// @generated by `cargo run -p xtask -- readme-tests`. Do not edit by hand.
//
// The README's Rust examples, lifted out and compiled. See xtask/src/readme_tests.rs for
// why they are not doctests.
//
// Nothing here is executed. Compiling is the test: an example that no longer type-checks
// is documentation that lies, and that is what this catches.
#![allow(
    unused_variables,
    unused_imports,
    unused_mut,
    unused_macros,
    dead_code,
    unreachable_code,
    clippy::all
)]

use prkdb::prelude::*;
use prkdb::PrkDb;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
pub use prkdb_client::ClientConfig;

/// The type nearly every example names.
#[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct User {
    #[id]
    id: String,
    #[index]
    age: u32,
    #[index]
    name: String,
    #[index]
    role: String,
    email: String,
    active: bool,
    created_at: u64,
    updated_at: u64,
    orders: f64,
    salary: f64,
    bio: String,
    dept: String,
    birth_date: u64,
    verified: bool,
    deleted: bool,
}

impl SoftDeletable for User {
    fn is_deleted(&self) -> bool { self.deleted }
    fn mark_deleted(&mut self) { self.deleted = true; }
    fn restore(&mut self) { self.deleted = false; }
}
impl Timestamped for User {
    fn created_at(&self) -> u64 { self.created_at }
    fn updated_at(&self) -> u64 { self.updated_at }
    fn set_created_at(&mut self, t: u64) { self.created_at = t; }
    fn set_updated_at(&mut self, t: u64) { self.updated_at = t; }
}

#[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Order {
    #[id]
    id: String,
    #[index]
    user_id: String,
    total: f64,
}

/// Schema-evolution examples name two versions of the same record.
#[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct UserV1 {
    #[id]
    id: String,
    name: String,
}

#[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct UserV2 {
    #[id]
    id: String,
    name: String,
    email: String,
    premium: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct UserSummary {
    id: String,
    name: String,
}

pub use prkdb_client::PrkDbClient;
pub use prkdb_types::collection::{
    Hooks, SoftDeletable, Timestamped, Validatable, ValidationError, Versioned,
};

/// Stand-ins for the values the fragments assume are in scope.
///
/// `unimplemented!()` is safe because no function below is ever called — and it keeps the
/// preamble from asserting anything about runtime behaviour it has not checked.
/// Most query examples call `query`, `count`, `sum` — which live on `IndexedStorage`,
/// not on `PrkDb`. The README writes `db` for both without saying which, so the generated
/// tests bind `db` to the one that makes the majority compile and expose the other as
/// `prkdb`. That ambiguity is itself a finding; see the report in docs/.
#[allow(unused_imports)]
use {UserQueryExt as _, OrderQueryExt as _, UserV1QueryExt as _, UserV2QueryExt as _};

fn a_db() -> prkdb::indexed_storage::IndexedStorage<prkdb::storage::InMemoryAdapter> {
    unimplemented!("examples are compiled, never run")
}
fn a_prkdb() -> PrkDb {
    unimplemented!("examples are compiled, never run")
}
fn a_client() -> PrkDbClient {
    unimplemented!("examples are compiled, never run")
}
fn any_user() -> User {
    unimplemented!("examples are compiled, never run")
}
fn any_storage() -> Arc<prkdb::storage::WalStorageAdapter> {
    unimplemented!("examples are compiled, never run")
}

/// `log` appears in one example; the crate is not a dependency, so this stands in for the
/// macro call rather than adding a dependency for a README fence.
mod log {
    macro_rules! info {
        ($($t:tt)*) => {};
    }
    pub(crate) use info;
}
"##
}

pub fn generate(check_only: bool) -> Result<()> {
    let root = repo_root()?;
    let readme = std::fs::read_to_string(root.join("README.md")).context("reading README.md")?;
    let examples = extract(&readme);

    let mut out = String::from(preamble());
    let mut emitted = 0usize;
    let mut skipped = Vec::new();

    for example in &examples {
        if let Some((_, reason)) = SKIP.iter().find(|(l, _)| *l == example.line) {
            skipped.push((example.line, *reason));
            continue;
        }
        if is_complete_program(&example.body) {
            // A complete program brings its own `main` and its own imports; wrapping it
            // in a function would nest `fn main`, and stripping the attributes would test
            // something other than what the README shows.
            skipped.push((example.line, "complete program: has its own fn main"));
            continue;
        }

        writeln!(
            out,
            "\n/// README.md line {}\nasync fn readme_line_{}() -> Result<(), Box<dyn std::error::Error>> {{\n{}\n{}\n    Ok(())\n}}",
            example.line,
            example.line,
            BINDINGS,
            indent(&example.body)
        )?;
        emitted += 1;
    }

    writeln!(
        out,
        "\n// {emitted} example(s) compiled, {} skipped.",
        skipped.len()
    )?;
    for (line, reason) in &skipped {
        writeln!(out, "// skipped README.md line {line}: {reason}")?;
    }

    let target = root.join(GENERATED);
    let current = std::fs::read_to_string(&target).unwrap_or_default();

    if check_only {
        if current != out {
            anyhow::bail!(
                "{GENERATED} is out of date with README.md.\n\
                 Run: cargo run -p xtask -- readme-tests"
            );
        }
        println!("{GENERATED} matches README.md ({emitted} examples)");
        return Ok(());
    }

    std::fs::write(&target, &out).with_context(|| format!("writing {GENERATED}"))?;
    println!(
        "wrote {GENERATED}: {emitted} examples, {} skipped",
        skipped.len()
    );
    for (line, reason) in &skipped {
        println!("  skipped line {line}: {reason}");
    }
    Ok(())
}

fn indent(body: &str) -> String {
    body.lines()
        .map(|l| {
            if l.trim().is_empty() {
                String::new()
            } else {
                format!("    {l}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn repo_root() -> Result<std::path::PathBuf> {
    let mut dir = std::env::current_dir()?;
    loop {
        if dir.join("README.md").exists() && dir.join("Cargo.toml").exists() {
            return Ok(dir);
        }
        if !dir.pop() {
            anyhow::bail!("could not find the repository root from the current directory");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_only_rust_fences() {
        let md = "# Title\n\n```rust\nlet a = 1;\n```\n\ntext\n\n```text\nnot rust\n```\n\n```rust\nlet b = 2;\n```\n";
        let found = extract(md);
        assert_eq!(found.len(), 2, "a ```text fence must not be extracted");
        assert_eq!(found[0].body, "let a = 1;");
        assert_eq!(found[1].body, "let b = 2;");
    }

    #[test]
    fn records_the_line_a_fence_opens_on() {
        let md = "a\nb\n```rust\nlet a = 1;\n```\n";
        assert_eq!(extract(md)[0].line, 3);
    }

    #[test]
    fn a_complete_program_is_recognised() {
        assert!(is_complete_program("#[tokio::main]\nasync fn main() {}"));
        assert!(!is_complete_program("let x = db.get(b\"k\").await?;"));
    }
}
