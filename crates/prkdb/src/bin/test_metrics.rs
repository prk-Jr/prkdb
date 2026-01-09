use prkdb::prometheus_metrics;

fn main() {
    println!("=== Manual Metrics Test ===\n");

    // Manually increment some metrics
    println!("1. Incrementing OPS_TOTAL...");
    prometheus_metrics::OPS_TOTAL
        .with_label_values(&["local"])
        .inc();

    println!("2. Recording operation duration...");
    prometheus_metrics::OPERATION_DURATION
        .with_label_values(&["local", "read"])
        .observe(0.001);

    println!("3. Incrementing cache hits...");
    prometheus_metrics::CACHE_HITS_TOTAL
        .with_label_values(&["local"])
        .inc();

    println!("4. Setting collections active...");
    prometheus_metrics::COLLECTIONS_ACTIVE
        .with_label_values(&["local"])
        .set(5.0);

    println!("\n=== Exporting Metrics ===\n");
    let metrics = prometheus_metrics::export_metrics();

    println!("Metrics length: {} bytes", metrics.len());
    println!(
        "\nFirst 1000 chars:\n{}",
        &metrics[..metrics.len().min(1000)]
    );

    if metrics.is_empty() {
        println!("\n❌ PROBLEM: Metrics are still empty even after manual increment!");
    } else {
        println!("\n✅ SUCCESS: Metrics export works when manually incremented!");
    }
}
