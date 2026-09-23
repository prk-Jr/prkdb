pub mod check;
pub mod evidence;
pub mod model;
pub mod render;

use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};

const LEDGER: &str = "docs/remediation/ledger.toml";
const PAGE: &str = "docs/status/remediation.md";

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn load() -> Result<model::Ledger> {
    let path = root().join(LEDGER);
    let text =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
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
