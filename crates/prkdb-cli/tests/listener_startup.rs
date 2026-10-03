#[path = "common/server.rs"]
pub mod server;

use server::ServerProcess;

fn assert_no_startup_success(process: &ServerProcess) {
    let stdout = process.stdout();
    assert!(
        !stdout.contains("Server started successfully") && !stdout.contains("server_listening"),
        "failed startup reported success: {}",
        process.diagnostics()
    );
}

#[tokio::test]
async fn occupied_http_port_fails_without_reporting_startup_success() {
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = occupied.local_addr().unwrap();
    let port = address.port().to_string();
    let mut process = ServerProcess::spawn(&["--port", &port, "--grpc-port", "0"]);
    let status = process.wait_for_exit().await.expect("HTTP bind must fail");
    assert!(!status.success(), "occupied HTTP port was accepted");
    assert_no_startup_success(&process);
    let error = process.stderr();
    assert!(
        error.contains("HTTP") && error.contains(&address.to_string()),
        "{error}"
    );
    assert!(
        error.contains("Address already in use") || error.contains("address in use"),
        "{error}"
    );
}

#[tokio::test]
async fn occupied_grpc_port_terminates_instead_of_serving_http() {
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = occupied.local_addr().unwrap();
    let port = address.port().to_string();
    let mut process = ServerProcess::spawn(&["--port", "0", "--grpc-port", &port]);
    let status = process.wait_for_exit().await.expect("gRPC bind must fail");
    assert!(!status.success(), "occupied gRPC port was accepted");
    assert_no_startup_success(&process);
    let error = process.stderr();
    assert!(
        error.contains("gRPC") && error.contains(&address.to_string()),
        "{error}"
    );
    assert!(
        error.contains("Address already in use") || error.contains("address in use"),
        "{error}"
    );
}

#[tokio::test]
async fn clustered_ephemeral_grpc_port_is_rejected_before_creating_the_database() {
    let mut process = ServerProcess::spawn(&[
        "--port",
        "0",
        "--grpc-port",
        "0",
        "--peers",
        "2=127.0.0.1:50052",
        "--allow-unauthenticated-peers",
    ]);
    let status = process
        .wait_for_exit()
        .await
        .expect("cluster gRPC port zero must fail");
    assert!(!status.success());
    assert_no_startup_success(&process);
    assert!(
        process.stderr().contains("port 0"),
        "{}",
        process.diagnostics()
    );
    assert!(!process.database_directory().join("db").exists());
}

#[tokio::test]
async fn invalid_tls_material_fails_without_reporting_startup_success() {
    let directory = tempfile::tempdir().unwrap();
    let certificate = directory.path().join("certificate.pem");
    let key = directory.path().join("key.pem");
    std::fs::write(&certificate, "readable, invalid certificate").unwrap();
    std::fs::write(&key, "readable, invalid key").unwrap();
    let mut process = ServerProcess::spawn(&[
        "--port",
        "0",
        "--grpc-port",
        "0",
        "--tls-cert",
        certificate.to_str().unwrap(),
        "--tls-key",
        key.to_str().unwrap(),
    ]);
    let status = process
        .wait_for_exit()
        .await
        .expect("invalid TLS must fail");
    assert!(!status.success());
    assert_no_startup_success(&process);
    let error = process.stderr();
    assert!(error.contains("TLS") || error.contains("HTTPS"), "{error}");
}

#[tokio::test]
async fn ephemeral_ports_report_serving_endpoints_and_drop_closes_them() {
    use prkdb::raft::rpc::{prk_db_service_client::PrkDbServiceClient, MetadataRequest};

    let mut process = ServerProcess::spawn(&["--port", "0", "--grpc-port", "0"]);
    let directory = process.database_directory().to_path_buf();
    let listening = process
        .listening()
        .await
        .expect("actual listening endpoints");
    assert_ne!(listening.http_address.port(), 0);
    assert_ne!(listening.grpc_address.port(), 0);
    assert_ne!(listening.http_address, listening.grpc_address);
    assert_eq!(
        listening.http_url,
        format!("http://{}", listening.http_address)
    );
    assert_eq!(
        listening.grpc_url,
        format!("http://{}", listening.grpc_address)
    );
    let response = reqwest::get(format!("{}/health", listening.http_url))
        .await
        .unwrap();
    assert!(response.status().is_success());
    let mut grpc = PrkDbServiceClient::connect(listening.grpc_url.clone())
        .await
        .unwrap();
    grpc.metadata(MetadataRequest { topics: vec![] })
        .await
        .unwrap();

    let stdout = process.stdout();
    let lines: Vec<_> = stdout.lines().collect();
    assert_eq!(
        lines.len(),
        1,
        "JSON startup mixed diagnostics into stdout: {stdout}"
    );
    let value: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(value["event"], "server_listening");

    drop(grpc);
    drop(process);
    assert!(!directory.exists(), "owned database directory leaked");
    assert!(tokio::net::TcpStream::connect(listening.http_address)
        .await
        .is_err());
    assert!(tokio::net::TcpStream::connect(listening.grpc_address)
        .await
        .is_err());
}

#[tokio::test]
async fn simultaneous_servers_own_distinct_endpoints_and_directories() {
    let mut first = ServerProcess::spawn(&["--port", "0", "--grpc-port", "0"]);
    let mut second = ServerProcess::spawn(&["--port", "0", "--grpc-port", "0"]);
    let (first_ready, second_ready) = tokio::join!(first.listening(), second.listening());
    let first_ready = first_ready.unwrap();
    let second_ready = second_ready.unwrap();
    assert_ne!(first.database_directory(), second.database_directory());
    let endpoints = [
        first_ready.http_address,
        first_ready.grpc_address,
        second_ready.http_address,
        second_ready.grpc_address,
    ];
    let unique: std::collections::HashSet<_> = endpoints.into_iter().collect();
    assert_eq!(unique.len(), 4);
}
