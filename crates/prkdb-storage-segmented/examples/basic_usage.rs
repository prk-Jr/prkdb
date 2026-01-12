use prkdb_storage_segmented::SegmentedLogAdapter;
use prkdb_types::storage::StorageAdapter;
use tempfile::tempdir;

#[tokio::main]
async fn main() {
    let dir = tempdir().expect("tmpdir");
    let adapter = SegmentedLogAdapter::new(dir.path(), 128, None, 100)
        .await
        .expect("init");

    adapter.put(b"user:1", br#"{"name":"ada"}"#).await.unwrap();
    adapter
        .put(b"user:2", br#"{"name":"grace"}"#)
        .await
        .unwrap();

    let v = adapter.get(b"user:1").await.unwrap().unwrap();
    println!("user:1 = {}", String::from_utf8_lossy(&v));

    let all = adapter.scan_prefix(b"user:").await.unwrap();
    println!("total users: {}", all.len());
}
