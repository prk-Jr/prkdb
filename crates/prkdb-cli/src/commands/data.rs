use clap::{Args, ValueEnum};
use prkdb_client::{PrkDbClient, ReadConsistency};
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Args, Clone)]
pub struct GetArgs {
    /// Key to retrieve
    pub key: String,

    /// Read consistency level
    #[arg(short, long, default_value = "linearizable")]
    pub consistency: ReadConsistencyCli,

    /// Bootstrap servers (e.g. http://127.0.0.1:8080)
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    pub server: Vec<String>,

    /// Output raw value only (for scripting)
    #[arg(long)]
    pub raw: bool,
}

#[derive(Args, Clone)]
pub struct PutArgs {
    /// Key to write
    pub key: String,

    /// Value to write
    pub value: String,

    /// Bootstrap servers
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    pub server: Vec<String>,
}

#[derive(Args, Clone)]
pub struct DeleteArgs {
    /// Key to delete
    pub key: String,

    /// Bootstrap servers
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    pub server: Vec<String>,
}

#[derive(Args, Clone)]
pub struct BatchPutArgs {
    /// File containing KV pairs (CSV: key,value)
    pub file: PathBuf,

    /// Separator character
    #[arg(short, long, default_value = ",")]
    pub separator: char,

    /// Bootstrap servers
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    pub server: Vec<String>,
}

#[derive(ValueEnum, Clone, Debug, Copy)]
pub enum ReadConsistencyCli {
    Linearizable,
    Stale,
    Follower,
}

impl From<ReadConsistencyCli> for ReadConsistency {
    fn from(cli: ReadConsistencyCli) -> Self {
        match cli {
            ReadConsistencyCli::Linearizable => ReadConsistency::Linearizable,
            ReadConsistencyCli::Stale => ReadConsistency::Stale,
            ReadConsistencyCli::Follower => ReadConsistency::Follower,
        }
    }
}

pub async fn handle_get(args: GetArgs) -> anyhow::Result<()> {
    let client = PrkDbClient::new(args.server).await?;
    let val = client
        .get_with_consistency(args.key.as_bytes(), args.consistency.into())
        .await?;

    match val {
        Some(v) => {
            if args.raw {
                use std::io::Write;
                std::io::stdout().write_all(&v)?;
                // Ensure newline if tty? No, raw means exact bytes.
            } else {
                let s = String::from_utf8_lossy(&v);
                println!("{}", s);
            }
        }
        None => {
            if !args.raw {
                eprintln!("Key not found: {}", args.key);
            }
            std::process::exit(1);
        }
    }
    Ok(())
}

pub async fn handle_put(args: PutArgs) -> anyhow::Result<()> {
    let client = PrkDbClient::new(args.server).await?;
    client
        .put(args.key.as_bytes(), args.value.as_bytes())
        .await?;
    println!("OK");
    Ok(())
}

pub async fn handle_delete(args: DeleteArgs) -> anyhow::Result<()> {
    let client = PrkDbClient::new(args.server).await?;
    client.delete(args.key.as_bytes()).await?;
    println!("OK");
    Ok(())
}

pub async fn handle_batch_put(args: BatchPutArgs) -> anyhow::Result<()> {
    let file = File::open(&args.file).await?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let mut batch = Vec::new();
    let sep = args.separator.to_string(); // simple string split

    while let Some(line) = lines.next_line().await? {
        if let Some((k, v)) = line.split_once(&sep) {
            batch.push((k.as_bytes().to_vec(), v.as_bytes().to_vec()));
        }
    }

    if batch.is_empty() {
        println!("No valid entries found in file");
        return Ok(());
    }

    let client = PrkDbClient::new(args.server).await?;
    println!("Sending batch of {} items...", batch.len());
    client.batch_put(batch).await?;
    println!("Batch put successful");
    Ok(())
}
