use prkdb::raft::rpc::prk_db_service_client::PrkDbServiceClient;
use prkdb::raft::rpc::{DeleteRequest, GetRequest, PutRequest};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    let data_addr = "http://127.0.0.1:8091";
    println!("Connecting to {}", data_addr);

    // Connect to Node 1's data service
    let mut client = PrkDbServiceClient::connect(data_addr.to_string()).await?;

    let key = b"test_key".to_vec();
    let value = b"Hello World".to_vec();

    // 1. PUT
    println!("1. Sending PUT...");
    let put_req = tonic::Request::new(PutRequest {
        key: key.clone(),
        value: value.clone(),
    });
    let put_resp = client.put(put_req).await?.into_inner();
    if put_resp.success {
        println!("✅ PUT successful");
    } else {
        println!("❌ PUT failed");
        return Ok(());
    }

    sleep(Duration::from_millis(500)).await;

    // 2. GET
    println!("2. Sending GET...");
    let get_req = tonic::Request::new(GetRequest {
        key: key.clone(),
        read_mode: prkdb::raft::rpc::ReadMode::Linearizable.into(),
    });
    let get_resp = client.get(get_req).await?.into_inner();
    if get_resp.success && get_resp.found && get_resp.value == value {
        println!("✅ GET successful: found correct value");
    } else {
        println!(
            "❌ GET failed: success={}, found={}, value={:?}",
            get_resp.success, get_resp.found, get_resp.value
        );
    }

    sleep(Duration::from_millis(500)).await;

    // 3. DELETE
    println!("3. Sending DELETE...");
    let del_req = tonic::Request::new(DeleteRequest { key: key.clone() });
    let del_resp = client.delete(del_req).await?.into_inner();
    if del_resp.success {
        println!("✅ DELETE successful");
    } else {
        println!("❌ DELETE failed");
    }

    sleep(Duration::from_millis(500)).await;

    // 4. GET (should be not found)
    println!("4. Sending GET (expecting Not Found)...");
    let get_req2 = tonic::Request::new(GetRequest {
        key: key.clone(),
        read_mode: prkdb::raft::rpc::ReadMode::Linearizable.into(),
    });
    let get_resp2 = client.get(get_req2).await?.into_inner();
    if get_resp2.success && !get_resp2.found {
        println!("✅ GET successful: key correctly not found");
    } else {
        println!(
            "❌ GET failed: expected not found, got success={}, found={}",
            get_resp2.success, get_resp2.found
        );
    }

    Ok(())
}
