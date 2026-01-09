//! PrkDB Metrics - Prometheus metrics export and HTTP server
//!
//! This crate provides Prometheus metrics collection and export
//! for PrkDB, including partition-level metrics and consumer lag tracking.

pub mod exporter;
pub mod lag_tracker;
pub mod server;
pub mod storage;

pub use exporter::*;
pub use lag_tracker::ConsumerLagTracker;
pub use server::MetricsServer;
