use std::process::Command;
use time::{format_description::well_known::Rfc3339, OffsetDateTime, UtcOffset};

fn json_command_with_current_utc_timestamp(command: &[&str]) -> serde_json::Value {
    let directory = tempfile::tempdir().unwrap();
    let before = OffsetDateTime::now_utc();
    let output = Command::new(env!("CARGO_BIN_EXE_prkdb-cli"))
        .arg("--database")
        .arg(directory.path().join("database"))
        .args(["--format", "json"])
        .args(command)
        .output()
        .unwrap();
    let after = OffsetDateTime::now_utc();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    // Human progress lines precede the JSON object on the existing CLI surface.
    let start = stdout
        .find('{')
        .expect("the command must emit a JSON result");
    let value: serde_json::Value = serde_json::from_str(&stdout[start..]).unwrap();
    let timestamp = OffsetDateTime::parse(value["timestamp"].as_str().unwrap(), &Rfc3339).unwrap();
    assert_eq!(timestamp.offset(), UtcOffset::UTC);
    assert!((before..=after).contains(&timestamp));
    value
}

#[test]
fn system_metrics_emit_json_and_a_current_utc_timestamp() {
    let metrics = json_command_with_current_utc_timestamp(&["metrics", "show"]);
    assert!(metrics["events_produced"].is_u64());
    assert!(metrics["collections"].is_u64());
    assert!(metrics["uptime_seconds"].is_u64());
}

#[test]
fn database_health_emits_json_and_a_current_utc_timestamp() {
    let health = json_command_with_current_utc_timestamp(&["database", "health"]);
    assert!(health["status"].is_string());
    assert!(health["database"].is_string());
    assert!(health["storage"].is_string());
}
