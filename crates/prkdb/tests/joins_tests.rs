//! Tests for stream joins.

use futures::{stream, StreamExt};
use prkdb::joins::{JoinConfig, JoinExt, JoinType};
use prkdb_types::collection::{ChangeEvent, Collection};
use prkdb_types::consumer::ConsumerRecord;
use prkdb_types::error::Error;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TestCollection {
    id: i32,
    name: String,
}

impl Collection for TestCollection {
    type Id = i32;

    fn id(&self) -> &Self::Id {
        &self.id
    }
}

fn create_test_record(
    id: i32,
    value: i32,
    collection_name: &str,
) -> Result<ConsumerRecord<TestCollection>, Error> {
    Ok(ConsumerRecord::new(
        collection_name.to_string(),
        0,
        id as u64,
        Vec::new(),
        TestCollection {
            id: value,
            name: collection_name.to_string(),
        },
        chrono::Utc::now().timestamp_millis(),
    ))
}

#[test]
fn test_join_config_creation() {
    let config = JoinConfig {
        join_type: JoinType::Inner,
        window: Duration::from_secs(5),
    };

    assert_eq!(config.join_type, JoinType::Inner);
    assert_eq!(config.window, Duration::from_secs(5));
}

#[tokio::test]
async fn test_inner_join_basic() {
    // Create test streams
    let left_stream = stream::iter(vec![
        create_test_record(1, 100, "left"),
        create_test_record(2, 200, "left"),
        create_test_record(3, 300, "left"),
    ]);

    let right_stream = stream::iter(vec![
        create_test_record(1, 1000, "right"),
        create_test_record(2, 2000, "right"),
        create_test_record(4, 4000, "right"), // This won't match
    ]);

    // Join on record id (offset)
    let joined = left_stream.inner_join(
        right_stream,
        |record| match record {
            Ok(r) => r.offset,
            Err(_) => 0,
        },
        |record| match record {
            Ok(r) => r.offset,
            Err(_) => 0,
        },
        Duration::from_secs(5),
    );

    let results: Vec<_> = joined.collect().await;

    // Should have 2 matches (id 1 and 2)
    assert_eq!(results.len(), 2);

    for result in results {
        assert!(result.is_ok());
        let (left, right_opt) = result.unwrap();
        assert!(left.is_ok());
        assert!(right_opt.is_some());

        let left_record = left.unwrap();
        let right_record = right_opt.unwrap().unwrap();

        // Check that the join keys match
        assert_eq!(left_record.offset, right_record.offset);
    }
}

#[tokio::test]
async fn test_left_join_basic() {
    // Create test streams
    let left_stream = stream::iter(vec![
        create_test_record(1, 100, "left"),
        create_test_record(2, 200, "left"),
        create_test_record(3, 300, "left"), // This won't have a match
    ]);

    let right_stream = stream::iter(vec![
        create_test_record(1, 1000, "right"),
        create_test_record(2, 2000, "right"),
    ]);

    // Join on record id (offset)
    let joined = left_stream.left_join(
        right_stream,
        |record| match record {
            Ok(r) => r.offset,
            Err(_) => 0,
        },
        |record| match record {
            Ok(r) => r.offset,
            Err(_) => 0,
        },
        Duration::from_secs(5),
    );

    let results: Vec<_> = joined.collect().await;

    // Should have 3 results (all left records)
    assert_eq!(results.len(), 3);

    let mut matched_count = 0;
    let mut unmatched_count = 0;

    for result in results {
        assert!(result.is_ok());
        let (left, right_opt) = result.unwrap();
        assert!(left.is_ok());

        if right_opt.is_some() {
            matched_count += 1;
            let right_record = right_opt.unwrap().unwrap();
            let left_record = left.unwrap();
            assert_eq!(left_record.offset, right_record.offset);
        } else {
            unmatched_count += 1;
        }
    }

    assert_eq!(matched_count, 2); // Records 1 and 2 matched
    assert_eq!(unmatched_count, 1); // Record 3 didn't match
}

#[tokio::test]
async fn test_empty_streams() {
    let left_stream = stream::iter(vec![]);
    let right_stream = stream::iter(vec![]);

    let joined = left_stream.inner_join(
        right_stream,
        |record: &Result<ConsumerRecord<TestCollection>, Error>| match record {
            Ok(r) => r.offset,
            Err(_) => 0,
        },
        |record: &Result<ConsumerRecord<TestCollection>, Error>| match record {
            Ok(r) => r.offset,
            Err(_) => 0,
        },
        Duration::from_secs(5),
    );

    let results: Vec<_> = joined.collect().await;
    assert_eq!(results.len(), 0);
}

#[tokio::test]
async fn test_single_record_streams() {
    let left_stream = stream::iter(vec![create_test_record(1, 100, "left")]);
    let right_stream = stream::iter(vec![create_test_record(1, 1000, "right")]);

    let joined = left_stream.inner_join(
        right_stream,
        |record| match record {
            Ok(r) => r.offset,
            Err(_) => 0,
        },
        |record| match record {
            Ok(r) => r.offset,
            Err(_) => 0,
        },
        Duration::from_secs(5),
    );

    let results: Vec<_> = joined.collect().await;
    assert_eq!(results.len(), 1);

    let result = &results[0];
    assert!(result.is_ok());
    let (left, right_opt) = result.as_ref().unwrap();
    assert!(left.is_ok());
    assert!(right_opt.is_some());
}

#[tokio::test]
async fn test_no_matches() {
    let left_stream = stream::iter(vec![
        create_test_record(1, 100, "left"),
        create_test_record(2, 200, "left"),
    ]);

    let right_stream = stream::iter(vec![
        create_test_record(3, 3000, "right"),
        create_test_record(4, 4000, "right"),
    ]);

    let joined = left_stream.inner_join(
        right_stream,
        |record| match record {
            Ok(r) => r.offset,
            Err(_) => 0,
        },
        |record| match record {
            Ok(r) => r.offset,
            Err(_) => 0,
        },
        Duration::from_secs(5),
    );

    let results: Vec<_> = joined.collect().await;
    assert_eq!(results.len(), 0);
}

#[tokio::test]
async fn test_multiple_matches_same_key() {
    // Test when multiple records have the same join key
    let left_stream = stream::iter(vec![
        create_test_record(1, 100, "left"),
        create_test_record(2, 101, "left"), // Different offset, same value % 100
    ]);

    let right_stream = stream::iter(vec![
        create_test_record(3, 1000, "right"),
        create_test_record(4, 1001, "right"), // Different offset, same value % 100
    ]);

    // Join on data value modulo 100
    let joined = left_stream.inner_join(
        right_stream,
        |record| match record {
            Ok(r) => r.value.id % 100,
            Err(_) => 0,
        },
        |record| match record {
            Ok(r) => r.value.id % 100,
            Err(_) => 0,
        },
        Duration::from_secs(5),
    );

    let results: Vec<_> = joined.collect().await;
    // Key 0: left(100) matches right(1000) -> 1 match
    // Key 1: left(101) matches right(1001) -> 1 match
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn test_error_handling() {
    let left_stream = stream::iter(vec![
        Ok(ConsumerRecord::new(
            "left".to_string(),
            0,
            1,
            Vec::new(),
            TestCollection {
                id: 100,
                name: "left".to_string(),
            },
            chrono::Utc::now().timestamp_millis(),
        )),
        Err(Error::Internal("test error".to_string())),
    ]);

    let right_stream = stream::iter(vec![Ok(ConsumerRecord::new(
        "right".to_string(),
        0,
        1,
        Vec::new(),
        TestCollection {
            id: 1000,
            name: "right".to_string(),
        },
        chrono::Utc::now().timestamp_millis(),
    ))]);

    let joined = left_stream.inner_join(
        right_stream,
        |record| match record {
            Ok(r) => r.offset,
            Err(_) => 0,
        },
        |record| match record {
            Ok(r) => r.offset,
            Err(_) => 0,
        },
        Duration::from_secs(5),
    );

    let results: Vec<_> = joined.collect().await;

    // Should get results: 1 matching record only for standard inner join
    assert_eq!(results.len(), 1);

    // First result should be a successful join
    assert!(results[0].is_ok());
    let (left, right) = &results[0].as_ref().unwrap();
    assert!(left.is_ok());
    assert!(right.is_some());
}
