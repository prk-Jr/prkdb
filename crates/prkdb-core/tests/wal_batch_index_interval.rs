//! Deterministic counterexample for sparse-index accounting in batch appends.
use prkdb_core::wal::offset_index::IndexEntry;
use prkdb_core::wal::{LogOperation, LogRecord, LogSegment};
use std::io;
use std::path::Path;

fn index_entries(dir: &Path) -> io::Result<Vec<(u32, u32)>> {
    let bytes = std::fs::read(dir.join("00000000000000000000.index"))?;
    let (entries, remainder) = bytes.as_chunks::<8>();
    assert!(remainder.is_empty(), "index must contain complete entries");
    Ok(entries
        .iter()
        .map(|bytes| {
            let entry = IndexEntry::from_bytes(bytes);
            (entry.relative_offset, entry.position)
        })
        .collect())
}

fn verify_records(segment: &LogSegment, count: u64) -> io::Result<()> {
    assert_eq!(segment.next_offset(), count);
    for offset in 0..count {
        let record = segment.read(offset)?;
        assert_eq!(record.offset, offset);
        record.verify_checksum().expect("record checksum");
        match record.operation {
            LogOperation::Put {
                collection,
                id,
                data,
            } => {
                assert_eq!(collection, "test");
                assert_eq!(id, format!("key{offset}").as_bytes());
                assert_eq!(data, format!("value{offset}").as_bytes());
            }
            operation => panic!("unexpected operation: {operation:?}"),
        }
    }
    Ok(())
}

#[test]
fn batch_index_interval_matches_single_appends_across_batches() -> io::Result<()> {
    let single_dir = tempfile::tempdir()?;
    let batch_dir = tempfile::tempdir()?;
    let single = LogSegment::create(single_dir.path(), 0, 1024)?;
    let batch = LogSegment::create(batch_dir.path(), 0, 1024)?;
    let mut snapshots = Vec::new();
    let mut count = 0;

    // The first batch exactly matches replication_integration_tests' 100 puts.
    // Subsequent small batches cross the carried interval remainder.
    for batch_size in [100, 1, 1, 20] {
        let records: Vec<_> = (count..count + batch_size)
            .map(|i| {
                LogRecord::new(LogOperation::Put {
                    collection: "test".to_string(),
                    id: format!("key{i}").into_bytes(),
                    data: format!("value{i}").into_bytes(),
                })
            })
            .collect();
        for record in &records {
            single.append(record.clone())?;
        }
        count += batch_size;
        assert_eq!(batch.append_batch(records)?, count - 1);
        let single_entries = index_entries(single_dir.path())?;
        let batch_entries = index_entries(batch_dir.path())?;
        eprintln!(
            "records={count}: single index entries={}, batch index entries={}",
            single_entries.len(),
            batch_entries.len()
        );
        snapshots.push((count, single_entries, batch_entries));
    }

    // Exercise reads only after writing, then verify the persisted index on reopen.
    verify_records(&single, count)?;
    verify_records(&batch, count)?;
    drop(single);
    drop(batch);
    let single = LogSegment::open(single_dir.path(), 0, 1024)?;
    let batch = LogSegment::open(batch_dir.path(), 0, 1024)?;
    verify_records(&single, count)?;
    verify_records(&batch, count)?;
    eprintln!("verified {count} records in both segments before and after reopen");

    let first_offsets: Vec<_> = snapshots[0].1.iter().map(|entry| entry.0).collect();
    assert_eq!(first_offsets, [20, 40, 60, 80]);
    for (count, single_entries, batch_entries) in snapshots {
        assert_eq!(
            batch_entries, single_entries,
            "batch index offsets and positions must match single appends after {count} records"
        );
    }
    Ok(())
}

#[test]
fn mixed_single_and_batch_appends_preserve_interval_remainder() -> io::Result<()> {
    let reference_dir = tempfile::tempdir()?;
    let mixed_dir = tempfile::tempdir()?;
    let reference = LogSegment::create(reference_dir.path(), 0, 512)?;
    let mixed = LogSegment::create(mixed_dir.path(), 0, 512)?;
    let mut snapshots = Vec::new();
    let mut count = 0;
    for (batched, size) in [
        (true, 13),
        (false, 1),
        (true, 8),
        (false, 7),
        (true, 11),
        (false, 3),
    ] {
        let records: Vec<_> = (count..count + size)
            .map(|offset| {
                LogRecord::new(LogOperation::Put {
                    collection: "test".to_string(),
                    id: format!("key{offset}").into_bytes(),
                    data: format!("value{offset}").into_bytes(),
                })
            })
            .collect();
        for record in &records {
            reference.append(record.clone())?;
        }
        if batched {
            assert_eq!(mixed.append_batch(records)?, count + size - 1);
        } else {
            for record in records {
                mixed.append(record)?;
            }
        }
        count += size;
        snapshots.push((
            count,
            index_entries(reference_dir.path())?,
            index_entries(mixed_dir.path())?,
        ));
    }
    verify_records(&reference, count)?;
    verify_records(&mixed, count)?;
    drop(reference);
    drop(mixed);
    verify_records(&LogSegment::open(reference_dir.path(), 0, 512)?, count)?;
    verify_records(&LogSegment::open(mixed_dir.path(), 0, 512)?, count)?;
    for (count, expected, actual) in snapshots {
        assert_eq!(
            actual, expected,
            "mixed interval accounting differs after {count} records"
        );
    }
    Ok(())
}
