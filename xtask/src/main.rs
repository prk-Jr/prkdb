mod repo_status;

use anyhow::Result;

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let args = args.iter().map(String::as_str).collect::<Vec<_>>();

    match args.as_slice() {
        ["repo-status", "snapshot"] => repo_status::snapshot(false),
        ["repo-status", "snapshot", "--fail-on-objective-drift"] => repo_status::snapshot(true),
        ["repo-status", "audit"] => repo_status::audit(false),
        ["repo-status", "audit", "--run-commands"] => repo_status::audit(true),
        ["repo-status", "render"] => repo_status::render(),
        _ => {
            print_usage_and_exit();
        }
    }
}

fn print_usage_and_exit() -> ! {
    eprintln!(
        "Usage: cargo run -p xtask -- repo-status <snapshot|audit|render> [--fail-on-objective-drift|--run-commands]"
    );
    std::process::exit(2);
}
