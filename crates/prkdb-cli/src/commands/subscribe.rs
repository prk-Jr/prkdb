//! Subscribe command for real-time collection streaming via WebSocket

use anyhow::Result;
use futures_util::StreamExt;
use prkdb_client::{WsConfig, WsConsumer, WsEvent};

/// Arguments for the subscribe command
#[derive(clap::Args, Clone)]
pub struct SubscribeArgs {
    /// Collection name to subscribe to
    #[arg(short, long)]
    pub collection: String,

    /// WebSocket server URL (e.g., ws://localhost:8080)
    #[arg(long, default_value = "ws://127.0.0.1:8080")]
    pub ws_server: String,

    /// Authentication token
    #[arg(long)]
    pub token: Option<String>,

    /// Start from specific offset (0 = beginning, omit for latest)
    #[arg(long)]
    pub from_offset: Option<u64>,

    /// Output format: json, compact, or raw
    #[arg(long, default_value = "json")]
    pub format: String,

    /// Disable auto-reconnect on disconnect
    #[arg(long)]
    pub no_reconnect: bool,

    /// Show only data (skip connected/disconnected messages)
    #[arg(long)]
    pub quiet: bool,
}

pub async fn handle_subscribe(args: SubscribeArgs) -> Result<()> {
    println!(
        "📡 Subscribing to collection '{}' at {}",
        args.collection, args.ws_server
    );

    let mut config =
        WsConfig::new(&args.ws_server, &args.collection).with_auto_reconnect(!args.no_reconnect);

    if let Some(token) = args.token {
        config = config.with_auth_token(token);
    }

    if let Some(offset) = args.from_offset {
        config = config.with_from_offset(offset);
        println!("📍 Starting from offset {}", offset);
    }

    let mut consumer = WsConsumer::new(config);
    let stream = consumer.subscribe().await;
    tokio::pin!(stream);

    println!("⏳ Connecting...\n");

    while let Some(event) = stream.next().await {
        match event {
            WsEvent::Connected { collection } => {
                if !args.quiet {
                    println!("✅ Connected to collection: {}", collection);
                }
            }
            WsEvent::InitialData {
                data, collection, ..
            } => {
                if !args.quiet {
                    println!("📦 Initial data from {}:", collection);
                }
                print_data(&data, &args.format)?;
            }
            WsEvent::Update {
                data, timestamp, ..
            } => {
                match args.format.as_str() {
                    "compact" => {
                        println!("{}", serde_json::to_string(&data)?);
                    }
                    "raw" => {
                        println!("{}", data);
                    }
                    _ => {
                        // json (pretty)
                        println!(
                            "📨 [{}] {}",
                            &timestamp[..19], // Trim to readable datetime
                            serde_json::to_string_pretty(&data)?
                        );
                    }
                }
            }
            WsEvent::Heartbeat { .. } => {
                // Silent heartbeat
            }
            WsEvent::Error { message } => {
                eprintln!("❌ Error: {}", message);
            }
            WsEvent::Disconnected { reason } => {
                if !args.quiet {
                    println!("🔌 Disconnected: {}", reason);
                }
            }
        }
    }

    println!("\n👋 Stream ended");
    Ok(())
}

fn print_data(data: &serde_json::Value, format: &str) -> Result<()> {
    match format {
        "compact" => println!("{}", serde_json::to_string(data)?),
        "raw" => println!("{}", data),
        _ => println!("{}", serde_json::to_string_pretty(data)?),
    }
    Ok(())
}
