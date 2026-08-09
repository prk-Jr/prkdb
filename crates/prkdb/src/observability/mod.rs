// Prometheus-style Metrics for PrkDB
// Tracks request latency and throughput across distributed layers

use lazy_static::lazy_static;
use prometheus::{
    register_counter_vec, register_histogram_vec, CounterVec, Encoder, HistogramVec, TextEncoder,
};

lazy_static! {
    // Operation counters
    pub static ref REQUESTS_TOTAL: CounterVec = register_counter_vec!(
        "prkdb_requests_total",
        "Total number of requests",
        &["operation", "status"]
    )
    .unwrap();

    // Request duration histograms (in seconds)
    pub static ref REQUEST_DURATION: HistogramVec = register_histogram_vec!(
        "prkdb_request_duration_seconds",
        "Request latency by layer",
        &["operation", "layer"],
        vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0] // Buckets
    )
    .unwrap();

    // Raft-specific metrics
    pub static ref RAFT_PROPOSALS: CounterVec = register_counter_vec!(
        "prkdb_raft_proposals_total",
        "Total Raft proposals",
        &["partition_id", "status"]
    )
    .unwrap();

    pub static ref RAFT_COMMIT_DURATION: HistogramVec = register_histogram_vec!(
        "prkdb_raft_commit_duration_seconds",
        "Time to commit a Raft entry",
        &["partition_id"],
        vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
    )
    .unwrap();
}

/// Record an operation with timing
pub fn record_request(operation: &str, layer: &str, duration_secs: f64) {
    REQUEST_DURATION
        .with_label_values(&[operation, layer])
        .observe(duration_secs);
}

/// Increment request counter
pub fn inc_request(operation: &str, status: &str) {
    REQUESTS_TOTAL.with_label_values(&[operation, status]).inc();
}

/// Record Raft proposal
pub fn record_raft_proposal(partition_id: u64, status: &str) {
    RAFT_PROPOSALS
        .with_label_values(&[&partition_id.to_string(), status])
        .inc();
}

/// Record Raft commit duration
pub fn record_raft_commit(partition_id: u64, duration_secs: f64) {
    RAFT_COMMIT_DURATION
        .with_label_values(&[&partition_id.to_string()])
        .observe(duration_secs);
}

/// Get metrics in Prometheus text format
pub fn metrics_text() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

/// Print a simple metrics summary to stdout
pub fn print_summary() {
    println!("\n=== PrkDB Metrics Summary ===");

    // Get metrics
    let metrics = prometheus::gather();

    for mf in metrics {
        println!("\n{}: {}", mf.name(), mf.help());
        for m in mf.get_metric() {
            // Print labels
            let labels: Vec<String> = m
                .get_label()
                .iter()
                .map(|l| format!("{}={}", l.name(), l.value()))
                .collect();

            if !labels.is_empty() {
                println!("  {{{}}}:", labels.join(", "));
            }

            // Print value.
            //
            // prometheus 0.14 moved to protobuf 3, where optional message fields are
            // `MessageField<T>` (an Option-like wrapper with public fields) rather than
            // has_*/get_* accessor pairs.
            // The scalar fields are `Option<T>` under proto2 optional semantics; an absent
            // value reads as zero here, which is what the old accessors returned.
            if let Some(counter) = m.get_counter().as_ref() {
                println!("    Count: {}", counter.value.unwrap_or_default());
            } else if let Some(h) = m.get_histogram().as_ref() {
                let count = h.sample_count.unwrap_or_default();
                let sum = h.sample_sum.unwrap_or_default();
                println!("    Count: {}", count);
                println!("    Sum: {:.6}s", sum);
                if count > 0 {
                    println!("    Avg: {:.6}ms", sum / count as f64 * 1000.0);
                }
            }
        }
    }
    println!("\n=============================\n");
}
