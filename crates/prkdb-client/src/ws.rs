//! WebSocket Consumer for real-time streaming from PrkDB
//!
//! This module provides a WebSocket-based consumer that can connect to
//! a PrkDB server and receive real-time updates.
//!
//! # Example
//! ```rust,ignore
//! use prkdb_client::ws::WsConsumer;
//!
//! let mut consumer = WsConsumer::connect("ws://localhost:8080", "orders").await?;
//! while let Some(event) = consumer.next().await {
//!     match event {
//!         WsEvent::Update { data, .. } => println!("Got: {}", data),
//!         _ => {}
//!     }
//! }
//! ```

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;
use tokio::time::sleep;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

/// Configuration for WebSocket consumer
#[derive(Debug, Clone)]
pub struct WsConfig {
    /// WebSocket server URL (e.g., "ws://localhost:8080")
    pub server_url: String,
    /// Collection to subscribe to
    pub collection: String,
    /// Authentication token (optional)
    pub auth_token: Option<String>,
    /// Starting offset for replay (0 = from beginning, None = latest)
    pub from_offset: Option<u64>,
    /// Auto-reconnect on disconnect
    pub auto_reconnect: bool,
    /// Reconnect delay
    pub reconnect_delay: Duration,
    /// Maximum reconnect attempts (0 = infinite)
    pub max_reconnect_attempts: u32,
}

impl Default for WsConfig {
    fn default() -> Self {
        Self {
            server_url: "ws://127.0.0.1:8080".to_string(),
            collection: String::new(),
            auth_token: None,
            from_offset: None,
            auto_reconnect: true,
            reconnect_delay: Duration::from_secs(1),
            max_reconnect_attempts: 0, // Infinite
        }
    }
}

impl WsConfig {
    pub fn new(server_url: impl Into<String>, collection: impl Into<String>) -> Self {
        Self {
            server_url: server_url.into(),
            collection: collection.into(),
            ..Default::default()
        }
    }

    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    pub fn with_from_offset(mut self, offset: u64) -> Self {
        self.from_offset = Some(offset);
        self
    }

    pub fn with_auto_reconnect(mut self, enabled: bool) -> Self {
        self.auto_reconnect = enabled;
        self
    }

    /// Build the WebSocket URL with query parameters.
    ///
    /// Authentication is sent via the `Authorization` header, not the URL.
    fn build_url(&self) -> String {
        let base = format!(
            "{}/ws/collections/{}",
            self.server_url.trim_end_matches('/'),
            self.collection
        );

        let mut params = Vec::new();
        if let Some(offset) = self.from_offset {
            params.push(format!("from_offset={}", offset));
        }

        if params.is_empty() {
            base
        } else {
            format!("{}?{}", base, params.join("&"))
        }
    }
}

/// Events received from WebSocket connection
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsEvent {
    /// Initial data snapshot
    InitialData {
        collection: String,
        data: Value,
        timestamp: String,
    },
    /// Real-time update
    Update {
        collection: String,
        data: Value,
        timestamp: String,
    },
    /// Heartbeat ping
    Heartbeat { timestamp: String },
    /// Error from server
    Error { message: String },
    /// Connection established
    Connected { collection: String },
    /// Connection lost
    Disconnected { reason: String },
}

/// WebSocket consumer for real-time streaming
pub struct WsConsumer {
    config: WsConfig,
    reconnect_count: u32,
}

impl WsConsumer {
    /// Create a new WebSocket consumer with configuration
    pub fn new(config: WsConfig) -> Self {
        Self {
            config,
            reconnect_count: 0,
        }
    }

    /// Create a consumer with simple parameters
    pub fn simple(server_url: &str, collection: &str) -> Self {
        Self::new(WsConfig::new(server_url, collection))
    }

    /// Connect and start receiving events
    ///
    /// Returns a stream of WsEvent that can be iterated
    pub async fn subscribe(&mut self) -> impl futures::Stream<Item = WsEvent> + '_ {
        async_stream::stream! {
            loop {
                let url = self.config.build_url();
                let mut request = match url.clone().into_client_request() {
                    Ok(request) => request,
                    Err(e) => {
                        yield WsEvent::Error {
                            message: format!("Invalid WebSocket URL: {}", e),
                        };
                        break;
                    }
                };

                if let Some(token) = &self.config.auth_token {
                    if let Ok(header_value) = format!("Bearer {}", token).parse() {
                        request.headers_mut().insert(AUTHORIZATION, header_value);
                    }
                }

                info!(
                    url = %url,
                    has_auth_token = self.config.auth_token.is_some(),
                    "Connecting to WebSocket"
                );

                match connect_async(request).await {
                    Ok((ws_stream, _)) => {
                        self.reconnect_count = 0;
                        // The HTTP upgrade precedes subscription initialization. The server's
                        // `connected` message acknowledges its captured starting offset; emitting
                        // it here lets callers write before that offset has been chosen.
                        let (mut _write, mut read) = ws_stream.split();

                        while let Some(msg_result) = read.next().await {
                            match msg_result {
                                Ok(Message::Text(text)) => {
                                    match serde_json::from_str::<WsEvent>(&text) {
                                        Ok(event) => yield event,
                                        Err(e) => {
                                            warn!(error = %e, "Failed to parse WebSocket message");
                                            // Try to yield as raw update
                                            if let Ok(data) = serde_json::from_str::<Value>(&text) {
                                                yield WsEvent::Update {
                                                    collection: self.config.collection.clone(),
                                                    data,
                                                    timestamp: time::OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339).expect("current UTC time is RFC 3339"),
                                                };
                                            }
                                        }
                                    }
                                }
                                Ok(Message::Ping(_)) => {
                                    debug!("Received ping");
                                }
                                Ok(Message::Close(frame)) => {
                                    let reason = frame
                                        .map(|f| f.reason.to_string())
                                        .unwrap_or_else(|| "Unknown".to_string());
                                    yield WsEvent::Disconnected { reason };
                                    break;
                                }
                                Err(e) => {
                                    error!(error = %e, "WebSocket error");
                                    yield WsEvent::Disconnected {
                                        reason: e.to_string(),
                                    };
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to connect");
                        yield WsEvent::Error {
                            message: format!("Connection failed: {}", e),
                        };
                    }
                }

                // Handle reconnection
                if !self.config.auto_reconnect {
                    break;
                }

                self.reconnect_count += 1;
                if self.config.max_reconnect_attempts > 0
                    && self.reconnect_count >= self.config.max_reconnect_attempts
                {
                    yield WsEvent::Error {
                        message: format!(
                            "Max reconnect attempts ({}) reached",
                            self.config.max_reconnect_attempts
                        ),
                    };
                    break;
                }

                info!(
                    attempt = self.reconnect_count,
                    delay_ms = self.config.reconnect_delay.as_millis(),
                    "Reconnecting..."
                );
                sleep(self.config.reconnect_delay).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ws_config_build_url() {
        let config = WsConfig::new("ws://localhost:8080", "orders");
        assert_eq!(
            config.build_url(),
            "ws://localhost:8080/ws/collections/orders"
        );
    }

    #[test]
    fn test_ws_config_does_not_put_auth_token_in_query_string() {
        let config = WsConfig::new("ws://localhost:8080", "orders")
            .with_auth_token("secret123")
            .with_from_offset(42);

        let url = config.build_url();

        assert_eq!(
            url,
            "ws://localhost:8080/ws/collections/orders?from_offset=42"
        );
        assert!(!url.contains("secret123"));
    }

    #[test]
    fn test_ws_config_with_params() {
        let config = WsConfig::new("ws://localhost:8080", "orders")
            .with_auth_token("secret123")
            .with_from_offset(42);

        let url = config.build_url();
        assert_eq!(
            url,
            "ws://localhost:8080/ws/collections/orders?from_offset=42"
        );
    }

    #[test]
    fn test_ws_event_parsing() {
        let json = r#"{"type":"update","collection":"orders","data":{"id":1},"timestamp":"2024-01-01T00:00:00Z"}"#;
        let event: WsEvent = serde_json::from_str(json).unwrap();

        match event {
            WsEvent::Update { collection, .. } => assert_eq!(collection, "orders"),
            _ => panic!("Expected Update event"),
        }
    }

    /// The HTTP upgrade does not mean the collection's latest offset was captured.
    /// Only the server can announce that the subscription is ready for new writes.
    #[tokio::test]
    async fn connected_is_emitted_once_from_the_server_acknowledgement() {
        use futures::SinkExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
            for message in [
                r#"{"type":"connected","collection":"orders"}"#,
                r#"{"type":"update","collection":"orders","data":{"id":1},"timestamp":"2026-01-01T00:00:00Z"}"#,
            ] {
                websocket.send(Message::Text(message.into())).await.unwrap();
            }
        });

        let mut consumer = WsConsumer::new(
            WsConfig::new(format!("ws://{address}"), "orders").with_auto_reconnect(false),
        );
        let events = consumer.subscribe().await;
        tokio::pin!(events);
        assert!(
            matches!(events.next().await, Some(WsEvent::Connected { collection }) if collection == "orders")
        );
        assert!(
            matches!(events.next().await, Some(WsEvent::Update { data, .. }) if data == serde_json::json!({"id": 1}))
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn an_error_before_subscription_readiness_does_not_emit_connected() {
        use futures::SinkExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
            websocket
                .send(Message::Text(
                    r#"{"type":"error","message":"subscription initialization failed"}"#.into(),
                ))
                .await
                .unwrap();
        });

        let mut consumer = WsConsumer::new(
            WsConfig::new(format!("ws://{address}"), "orders").with_auto_reconnect(false),
        );
        let events = consumer.subscribe().await;
        tokio::pin!(events);
        assert!(
            matches!(events.next().await, Some(WsEvent::Error { message }) if message == "subscription initialization failed")
        );
        server.await.unwrap();
    }
}
