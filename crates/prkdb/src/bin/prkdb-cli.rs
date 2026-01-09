use clap::{Parser, Subcommand};
use prkdb::client::PrkDbClient;
use std::str::from_utf8;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Bootstrap servers (comma separated)
    #[arg(long, default_value = "http://127.0.0.1:8081")]
    bootstrap: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Put a key-value pair
    Put { key: String, value: String },
    /// Get a value by key
    Get { key: String },
    /// Delete a key
    Delete { key: String },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    let bootstrap_servers: Vec<String> = cli.bootstrap.split(',').map(|s| s.to_string()).collect();

    let client = PrkDbClient::new(bootstrap_servers).await?;

    match cli.command {
        Commands::Put { key, value } => {
            client.put(key.as_bytes(), value.as_bytes()).await?;
            println!("Put successful: {} = {}", key, value);
        }
        Commands::Get { key } => match client.get(key.as_bytes()).await? {
            Some(value) => {
                if let Ok(s) = from_utf8(&value) {
                    println!("{}", s);
                } else {
                    println!("{:?}", value);
                }
            }
            None => println!("Key not found"),
        },
        Commands::Delete { key } => {
            client.delete(key.as_bytes()).await?;
            println!("Delete successful: {}", key);
        }
    }

    Ok(())
}
