//! # prkdb-client
//!
//! Lightweight client library for PrkDB with smart routing and automatic failover.
//!
//! This crate provides a Kafka-inspired client that:
//! - Automatically discovers cluster topology
//! - Routes requests to the correct partition leader
//! - Handles retries and failover transparently
//! - **NEW**: WebSocket consumer for real-time streaming
//!
//! ## Example
//!
//! ```rust,ignore
//! use prkdb_client::PrkDbClient;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = PrkDbClient::new(vec![
//!         "http://127.0.0.1:8081".to_string(),
//!         "http://127.0.0.1:8082".to_string(),
//!         "http://127.0.0.1:8083".to_string(),
//!     ]).await?;
//!     
//!     // Put a key-value pair
//!     client.put(b"my-key", b"my-value").await?;
//!     
//!     // Get a value
//!     if let Some(value) = client.get(b"my-key").await? {
//!         println!("Got: {:?}", value);
//!     }
//!     
//!     // Delete a key
//!     client.delete(b"my-key").await?;
//!     
//!     Ok(())
//! }
//! ```
//!
//! ## Features
//!
//! - **Automatic Routing**: Keys are hashed to partitions, requests go to the right leader
//! - **Metadata Caching**: Cluster topology is cached and refreshed on failure
//! - **Retry Logic**: Failed requests are retried with exponential backoff
//! - **WebSocket Streaming**: Real-time event consumption via WebSocket
//! - **Lightweight**: Only depends on `prkdb-proto` + minimal runtime dependencies

mod client;
pub mod ws;

pub use client::{ClientConfig, PrkDbClient, ReadConsistency};
pub use ws::{WsConfig, WsConsumer, WsEvent};

// Re-export commonly used types from prkdb-proto for convenience
pub use prkdb_proto::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, MetadataRequest, MetadataResponse,
    NodeInfo, PartitionInfo, PutRequest, PutResponse, ReadMode,
};
