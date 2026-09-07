//! Diagnostic probes: passing assertions confirm defects in the reviewed revision.
//! Copy to crates/prkdb/tests/core_review.rs before running that test target.
#[path = "../src/batch_accumulator.rs"]
mod batch_accumulator;

use prkdb::{indexed_storage::IndexedStorage, storage::InMemoryAdapter};
use prkdb_core::batch_config::BatchConfig;
use prkdb_macros::Collection;
use serde::{Deserialize, Serialize};
use std::sync::{atomic::{AtomicBool, Ordering}, Arc};
use tokio::sync::Notify;

#[derive(Collection, Serialize, Deserialize, Clone, Debug)]
struct User { #[id] id: u64, #[index] name: String }
#[derive(Collection, Serialize, Deserialize, Clone, Debug)]
struct Project { #[id] id: u64, #[index] name: String }

#[tokio::test]
async fn indexed_collections_with_the_same_id_collide() {
    let db = IndexedStorage::new(Arc::new(InMemoryAdapter::new()));
    db.insert(&User { id: 1, name: "Alice".into() }).await.unwrap();
    db.insert(&Project { id: 1, name: "Project".into() }).await.unwrap();
    assert_eq!(db.get::<User>(&1).await.unwrap().unwrap().name, "Project");
    println!("CONFIRMED: reading a User returned another collection's record");
}

#[tokio::test]
async fn flush_returns_while_the_executor_is_blocked() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let done = Arc::new(AtomicBool::new(false));
    let (s, r, d) = (started.clone(), release.clone(), done.clone());
    let accumulator = batch_accumulator::BatchAccumulator::new(
        BatchConfig { linger_ms: 1, max_batch_size: 1, ..Default::default() },
        move |_: Vec<User>| {
            let (s, r, d) = (s.clone(), r.clone(), d.clone());
            async move { s.notify_one(); r.notified().await; d.store(true, Ordering::SeqCst); Ok(()) }
        },
    );
    accumulator.add_put(User { id: 1, name: "Alice".into() }).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), started.notified()).await.unwrap();
    accumulator.flush().await.unwrap();
    assert!(!done.load(Ordering::SeqCst));
    release.notify_one();
    println!("CONFIRMED: flush succeeded before write executor completed");
}
