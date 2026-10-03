//! Own a CLI child and its database from the moment the process is spawned.

use std::io::{BufRead, BufReader, Read};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use serde::Deserialize;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

const STARTUP_LIMIT: Duration = Duration::from_secs(30);

#[derive(Debug, Deserialize)]
pub struct Listening {
    pub event: String,
    pub http_address: std::net::SocketAddr,
    pub grpc_address: std::net::SocketAddr,
    pub http_url: String,
    pub grpc_url: String,
}

pub struct ServerProcess {
    child: Child,
    directory: tempfile::TempDir,
    stdout: Arc<Mutex<String>>,
    stderr: Arc<Mutex<String>>,
    lines: UnboundedReceiver<String>,
    readers: Vec<JoinHandle<()>>,
}

impl ServerProcess {
    pub fn spawn(arguments: &[&str]) -> Self {
        let directory = tempfile::tempdir().expect("unique server directory");
        let child = Command::new(env!("CARGO_BIN_EXE_prkdb-cli"))
            .arg("--database")
            .arg(directory.path().join("db"))
            .args(["--format", "json", "serve", "--host", "127.0.0.1"])
            .arg("--allow-anonymous")
            .args(arguments)
            .env_remove("PRKDB_BOOTSTRAP_TOKEN")
            .env_remove("PRKDB_ADMIN_TOKEN")
            .env_remove("PRKDB_WS_TOKEN")
            .env_remove("PRKDB_CLUSTER_SECRET")
            .env_remove("PRKDB_ADVERTISED_GRPC_ADDR")
            .env_remove("PRKDB_ADVERTISED_HTTP_ADDR")
            .env_remove("PRKDB_PEER_ADVERTISED_GRPC_ADDRS")
            .env_remove("PRKDB_PEER_HTTP_ADDRS")
            .env_remove("RUST_LOG")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn CLI server");
        let (sender, lines) = mpsc::unbounded_channel();
        // The guard exists before taking pipes or launching reader threads. Any startup
        // failure or cancellation after spawn therefore kills and reaps this child.
        let mut process = Self {
            child,
            directory,
            stdout: Arc::default(),
            stderr: Arc::default(),
            lines,
            readers: Vec::new(),
        };
        process.readers.push(drain(
            process.child.stdout.take().expect("piped stdout"),
            process.stdout.clone(),
            Some(sender),
        ));
        process.readers.push(drain(
            process.child.stderr.take().expect("piped stderr"),
            process.stderr.clone(),
            None,
        ));
        process
    }

    pub async fn listening(&mut self) -> Result<Listening, String> {
        let result = tokio::time::timeout(STARTUP_LIMIT, async {
            let line =
                self.lines.recv().await.ok_or_else(|| {
                    "child closed stdout before reporting its listeners".to_string()
                })?;
            let record: Listening = serde_json::from_str(&line)
                .map_err(|error| format!("invalid JSON startup record {line:?}: {error}"))?;
            if record.event != "server_listening" {
                return Err(format!("unexpected startup event: {}", record.event));
            }
            Ok(record)
        })
        .await;
        match result {
            Ok(result) => result.map_err(|error| format!("{error}\n{}", self.diagnostics())),
            Err(_) => Err(format!(
                "child did not report its listeners within {STARTUP_LIMIT:?}\n{}",
                self.diagnostics()
            )),
        }
    }

    pub async fn wait_for_exit(&mut self) -> Result<ExitStatus, String> {
        tokio::time::timeout(STARTUP_LIMIT, async {
            loop {
                if let Some(status) = self.child.try_wait().map_err(|error| error.to_string())? {
                    self.join_readers();
                    return Ok(status);
                }
                // Observe child completion; this is no retry of a failed startup.
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| {
            format!(
                "child kept running after startup failure\n{}",
                self.diagnostics()
            )
        })?
    }

    pub fn stdout(&self) -> String {
        self.stdout.lock().expect("stdout capture").clone()
    }

    pub fn stderr(&self) -> String {
        self.stderr.lock().expect("stderr capture").clone()
    }

    pub fn database_directory(&self) -> &std::path::Path {
        self.directory.path()
    }

    pub fn diagnostics(&self) -> String {
        format!("stdout:\n{}\nstderr:\n{}", self.stdout(), self.stderr())
    }

    fn join_readers(&mut self) {
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.join_readers();
        // TempDir removes the owned database only after the process is reaped.
    }
}

fn drain(
    pipe: impl Read + Send + 'static,
    output: Arc<Mutex<String>>,
    sender: Option<UnboundedSender<String>>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        for line in BufReader::new(pipe).lines() {
            let Ok(line) = line else { break };
            {
                let mut output = output.lock().expect("output capture");
                output.push_str(&line);
                output.push('\n');
            }
            if let Some(sender) = &sender {
                let _ = sender.send(line);
            }
        }
    })
}
