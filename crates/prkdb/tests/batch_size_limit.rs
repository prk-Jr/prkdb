//! STO-12: a write batch whose uncompressed size the decoder refuses must be refused at
//! write, not acknowledged and then refused at recovery.
//!
//! `Batch::decode` refuses `raw_len > MAX_PAYLOAD_LEN` before decompressing (a
//! decompression-bomb bound). `Batch::encode` used to check only the encoded size (via
//! `Wal::reserve`), so a compressible value over 64 MiB compressed to a few hundred KiB,
//! was written and acknowledged, and the next open failed with `ReplayFailed`.

use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::compression::CompressionConfig;
use prkdb_core::wal::frame::MAX_PAYLOAD_LEN;
use prkdb_core::wal::WalConfig;
use prkdb_types::error::StorageError;
use prkdb_types::storage::StorageAdapter;

/// A compressing adapter (LZ4, the default), where the encoded frame of a huge but
/// repetitive value is small.
fn compressing_adapter(dir: &std::path::Path) -> WalStorageAdapter {
    let config = WalConfig {
        log_dir: dir.to_path_buf(),
        compression: CompressionConfig::default(),
        ..WalConfig::test_config()
    };
    WalStorageAdapter::new(config).expect("open a compressing WAL adapter")
}

/// The uncompressed size of a one-put batch: `count u32 | tag u8 | klen u32 | key |
/// vlen u32 | value`.
fn one_put_raw_len(key: &[u8], value_len: usize) -> usize {
    4 + 1 + 4 + key.len() + 4 + value_len
}

/// A compressible value whose batch is one byte over the limit is refused as a
/// validation error, and nothing reaches the log: the directory reopens without it.
#[tokio::test(flavor = "multi_thread")]
async fn sto12_a_batch_over_the_limit_is_refused_at_write_even_when_it_compresses_small() {
    let dir = tempfile::tempdir().unwrap();
    let key = b"big";
    let value_len = MAX_PAYLOAD_LEN + 1 - one_put_raw_len(key, 0);
    assert_eq!(one_put_raw_len(key, value_len), MAX_PAYLOAD_LEN + 1);
    {
        let db = compressing_adapter(dir.path());
        db.put(b"before", b"kept").await.unwrap();
        let err = db.put(key, &vec![b'x'; value_len]).await.unwrap_err();
        assert!(matches!(err, StorageError::Validation(_)), "{err:?}");
        assert!(err.to_string().contains("exceeds"), "{err}");
        // A 64 MiB + 1 value, the size the external review reported, likewise.
        let err = db
            .put(key, &vec![b'x'; MAX_PAYLOAD_LEN + 1])
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::Validation(_)), "{err:?}");
    }
    let db = compressing_adapter(dir.path());
    assert_eq!(db.get(key).await.unwrap(), None);
    assert_eq!(db.get(b"before").await.unwrap(), Some(b"kept".to_vec()));
}

/// A batch of exactly `MAX_PAYLOAD_LEN` uncompressed bytes is written, survives a
/// reopen (replay decodes it), and reads back whole.
#[tokio::test(flavor = "multi_thread")]
async fn sto12_a_batch_of_exactly_the_limit_round_trips_through_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let key = b"big";
    let value_len = MAX_PAYLOAD_LEN - one_put_raw_len(key, 0);
    let value = vec![b'x'; value_len];
    {
        let db = compressing_adapter(dir.path());
        db.put(key, &value).await.unwrap();
    }
    let db = compressing_adapter(dir.path());
    let got = db
        .get(key)
        .await
        .unwrap()
        .expect("the value survives reopen");
    assert_eq!(got.len(), value_len);
    assert!(got == value);
}
