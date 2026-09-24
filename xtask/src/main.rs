mod readme_tests;
mod remediation;
mod repo_status;
mod verify;

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
        ["readme-tests"] => readme_tests::generate(false),
        ["readme-tests", "--check"] => readme_tests::generate(true),
        ["remediation", "check"] => remediation::run_check(),
        ["remediation", "render"] => remediation::run_render(false),
        ["remediation", "render", "--check"] => remediation::run_render(true),
        ["verify", rest @ ..] => verify::run(rest),
        _ => {
            print_usage_and_exit();
        }
    }
}

fn print_usage_and_exit() -> ! {
    eprintln!(
        "Usage:\n  \
         cargo run -p xtask -- repo-status <snapshot|audit|render> [--fail-on-objective-drift|--run-commands]\n  \
         cargo run -p xtask -- readme-tests [--check]\n  \
         cargo run -p xtask -- remediation <check|render> [--check]\n  \
         cargo run -p xtask -- verify [--profile blocking|discovery] [--seed N] [--seed-offset N] [--seeds K] [--ops M] [--mode durable]"
    );
    std::process::exit(2);
}
