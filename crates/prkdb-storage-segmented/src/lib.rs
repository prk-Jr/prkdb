//! Segmented log storage adapter implementing `StorageAdapter`.
//! Designed for append-only workloads with simple in-memory indexing rebuilt on startup.
//! This is a pragmatic, test-focused implementation (not production-hardened yet).

use async_trait::async_trait;
use bytes::BufMut;
use prkdb_types::error::StorageError;
use prkdb_types::storage::StorageAdapter;
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use tokio::fs::{self as async_fs, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;

#[cfg(feature = "uring")]
mod uring;

const SEGMENT_PREFIX: &str = "segment_";
const SEGMENT_EXT: &str = "log";

#[derive(Debug)]
struct SegmentHandle {
    file: BufWriter<tokio::fs::File>,
    size: u64,
}

/// Simple segmented log keyed KV store.
/// Maintains an in-memory index (key -> optional value), rebuilt on startup or restored from checkpoint.
pub struct SegmentedLogAdapter {
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    dir: PathBuf,
    segment_size: u64,
    max_segments: Option<usize>,
    ops_since_checkpoint: u64,
    checkpoint_every: u64,
    current: SegmentHandle,
    next_id: usize,
    index: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

impl SegmentedLogAdapter {
    /// Create adapter in the given directory (will be created if missing) with a max segment size.
    pub async fn new<P: AsRef<Path>>(
        dir: P,
        segment_size: u64,
        max_segments: Option<usize>,
        checkpoint_every: u64,
    ) -> Result<Self, StorageError> {
        let dir_path = dir.as_ref().to_path_buf();
        async_fs::create_dir_all(&dir_path)
            .await
            .map_err(|e| StorageError::BackendError(format!("create dir: {e}")))?;

        let (index, next_id) = Self::load_index(&dir_path).await?;
        let current_id = next_id;
        let current_path = dir_path.join(Self::segment_name(current_id));
        // If last segment exists, append; otherwise create new.
        let (file, size) = if current_path.exists() {
            let metadata = fs::metadata(&current_path)
                .map_err(|e| StorageError::BackendError(format!("metadata: {e}")))?;
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&current_path)
                .await
                .map_err(|e| StorageError::BackendError(format!("open segment: {e}")))?;
            (file, metadata.len())
        } else {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&current_path)
                .await
                .map_err(|e| StorageError::BackendError(format!("open new segment: {e}")))?;
            (file, 0)
        };

        let handle = SegmentHandle {
            file: BufWriter::new(file),
            size,
        };

        Ok(Self {
            inner: Mutex::new(Inner {
                dir: dir_path,
                segment_size,
                max_segments,
                ops_since_checkpoint: 0,
                checkpoint_every: checkpoint_every.max(1),
                current: handle,
                next_id: current_id + 1,
                index,
            }),
        })
    }

    /// Flush buffered data to disk and persist the current index snapshot.
    pub async fn flush(&self) -> Result<(), StorageError> {
        let mut guard = self.inner.lock().await;
        Self::flush_current(&mut guard).await?;
        Self::persist_index(&guard)?;
        guard.ops_since_checkpoint = 0;
        Ok(())
    }

    fn segment_name(id: usize) -> String {
        format!("{SEGMENT_PREFIX}{id}.{SEGMENT_EXT}")
    }

    async fn flush_current(inner: &mut Inner) -> Result<(), StorageError> {
        inner
            .current
            .file
            .flush()
            .await
            .map_err(|e| StorageError::BackendError(format!("flush: {e}")))?;
        inner
            .current
            .file
            .get_ref()
            .sync_data()
            .await
            .map_err(|e| StorageError::BackendError(format!("sync log: {e}")))?;
        Ok(())
    }

    async fn load_index(
        dir: &Path,
    ) -> Result<(HashMap<Vec<u8>, Option<Vec<u8>>>, usize), StorageError> {
        // prefer checkpoint
        let checkpoint_path = dir.join("index.snapshot");
        if checkpoint_path.exists() {
            if let Ok(bytes) = fs::read(&checkpoint_path) {
                if let Ok(map) = bincode::deserialize::<HashMap<Vec<u8>, Option<Vec<u8>>>>(&bytes) {
                    // determine next_id from segments present
                    let mut max_id = 0usize;
                    for ent in (fs::read_dir(dir)
                        .map_err(|e| StorageError::BackendError(format!("read_dir: {e}")))?)
                    .flatten()
                    {
                        let name = ent.file_name().to_string_lossy().to_string();
                        if let Some(id_str) = name
                            .strip_prefix(SEGMENT_PREFIX)
                            .and_then(|s| s.strip_suffix(&format!(".{SEGMENT_EXT}")))
                        {
                            if let Ok(id) = id_str.parse::<usize>() {
                                max_id = max_id.max(id);
                            }
                        }
                    }
                    return Ok((map, max_id + 1));
                }
            }
        }

        let mut files = fs::read_dir(dir)
            .map_err(|e| StorageError::BackendError(format!("read_dir: {e}")))?
            .filter_map(|res| res.ok())
            .filter_map(|entry| {
                let fname = entry.file_name().to_string_lossy().to_string();
                if fname.starts_with(SEGMENT_PREFIX) {
                    if let Some(id_str) = fname
                        .strip_prefix(SEGMENT_PREFIX)
                        .and_then(|s| s.strip_suffix(&format!(".{SEGMENT_EXT}")))
                    {
                        if let Ok(id) = id_str.parse::<usize>() {
                            return Some((id, entry.path()));
                        }
                    }
                }
                None
            })
            .collect::<Vec<_>>();
        files.sort_by_key(|(id, _)| *id);

        let mut index: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();
        let mut max_id = 0usize;
        for (id, path) in files {
            max_id = max_id.max(id);
            let mut f = fs::File::open(path)
                .map_err(|e| StorageError::BackendError(format!("open segment: {e}")))?;
            Self::replay_segment(&mut f, &mut index)?;
        }
        Ok((index, max_id + 1))
    }

    fn replay_segment(
        file: &mut fs::File,
        index: &mut HashMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> Result<(), StorageError> {
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)
            .map_err(|e| StorageError::BackendError(format!("read segment: {e}")))?;
        let mut cursor: &[u8] = &buf;
        while !cursor.is_empty() {
            if cursor.len() < 13 {
                break;
            }
            let op = cursor[0];
            let key_len = u32::from_le_bytes(cursor[1..5].try_into().unwrap()) as usize;
            let val_len = u32::from_le_bytes(cursor[5..9].try_into().unwrap()) as usize;
            if cursor.len() < 13 + key_len + val_len {
                break;
            }
            let key_start = 9;
            let val_start = key_start + key_len;
            let crc_start = val_start + val_len;
            let key = cursor[key_start..val_start].to_vec();
            let val_slice = &cursor[val_start..val_start + val_len];
            let expected_crc =
                u32::from_le_bytes(cursor[crc_start..crc_start + 4].try_into().unwrap());
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&cursor[..crc_start]);
            let actual_crc = hasher.finalize();
            if expected_crc != actual_crc {
                break;
            }
            match op {
                0 => {
                    index.insert(key, Some(val_slice.to_vec()));
                }
                1 => {
                    index.insert(key, None);
                }
                _ => {}
            }
            cursor = &cursor[crc_start + 4..];
        }
        Ok(())
    }

    async fn rotate_if_needed(inner: &mut Inner, record_len: u64) -> Result<(), StorageError> {
        if inner.current.size + record_len <= inner.segment_size {
            return Ok(());
        }
        // Flush buffered data before rotating to a new segment.
        Self::flush_current(inner).await?;
        // close current (drop) and open new
        let new_id = inner.next_id;
        inner.next_id += 1;
        let new_path = inner.dir.join(Self::segment_name(new_id));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_path)
            .await
            .map_err(|e| StorageError::BackendError(format!("open rotated segment: {e}")))?;
        inner.current = SegmentHandle {
            file: BufWriter::new(file),
            size: 0,
        };

        if let Some(max) = inner.max_segments {
            // Simple policy: if segment count exceeded max, compact to a single segment backed by current index.
            if new_id + 1 > max {
                Self::compact_in_place(inner)?;
            }
        }
        Ok(())
    }

    async fn append_record(
        inner: &mut Inner,
        op: u8,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), StorageError> {
        let record_len = 1 + 4 + 4 + key.len() as u64 + value.len() as u64 + 4;
        Self::rotate_if_needed(inner, record_len).await?;

        let mut buf = Vec::with_capacity(record_len as usize);
        buf.put_u8(op);
        buf.put_u32_le(key.len() as u32);
        buf.put_u32_le(value.len() as u32);
        buf.extend_from_slice(key);
        buf.extend_from_slice(value);
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();
        buf.put_u32_le(crc);

        inner
            .current
            .file
            .write_all(&buf)
            .await
            .map_err(|e| StorageError::BackendError(format!("write log: {e}")))?;
        inner.current.size += record_len;
        inner.ops_since_checkpoint += 1;
        Ok(())
    }

    /// Compact segments into a single segment reflecting current index.
    pub async fn compact(&self) -> Result<(), StorageError> {
        let mut guard = self.inner.lock().await;
        // Ensure buffered data is on disk before compaction.
        Self::flush_current(&mut guard).await?;
        Self::compact_in_place(&mut guard)?;
        Ok(())
    }

    fn compact_in_place(guard: &mut Inner) -> Result<(), StorageError> {
        let tmp_path = guard.dir.join("compact.tmp");
        {
            let mut f = fs::File::create(&tmp_path)
                .map_err(|e| StorageError::BackendError(format!("compact create: {e}")))?;
            for (k, v_opt) in guard.index.iter() {
                Self::write_record_sync(&mut f, k, v_opt.as_deref())?;
            }
        }
        // Remove old segments
        for ent in (fs::read_dir(&guard.dir)
            .map_err(|e| StorageError::BackendError(format!("read_dir: {e}")))?)
        .flatten()
        {
            let name = ent.file_name().to_string_lossy().to_string();
            if name.starts_with(SEGMENT_PREFIX) {
                let _ = fs::remove_file(ent.path());
            }
        }
        // Move compacted file to new segment 0
        let new_path = guard.dir.join(Self::segment_name(0));
        fs::rename(&tmp_path, &new_path)
            .map_err(|e| StorageError::BackendError(format!("rename compact: {e}")))?;

        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_path)
            .map_err(|e| StorageError::BackendError(format!("open compacted: {e}")))?;

        guard.current = SegmentHandle {
            file: BufWriter::new(tokio::fs::File::from_std(file)),
            size: fs::metadata(&new_path)
                .map_err(|e| StorageError::BackendError(format!("meta: {e}")))?
                .len(),
        };
        guard.next_id = 1;
        guard.ops_since_checkpoint = 0;
        Self::persist_index(&*guard)?;
        Ok(())
    }

    fn write_record_sync(
        f: &mut fs::File,
        key: &[u8],
        val: Option<&[u8]>,
    ) -> Result<(), StorageError> {
        let op = if val.is_some() { 0u8 } else { 1u8 };
        let value = val.unwrap_or(&[]);
        let record_len = 1 + 4 + 4 + key.len() + value.len() + 4;
        let mut buf = Vec::with_capacity(record_len);
        buf.put_u8(op);
        buf.put_u32_le(key.len() as u32);
        buf.put_u32_le(value.len() as u32);
        buf.extend_from_slice(key);
        buf.extend_from_slice(value);
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();
        buf.put_u32_le(crc);
        f.write_all(&buf)
            .map_err(|e| StorageError::BackendError(format!("compact write: {e}")))?;
        Ok(())
    }

    fn persist_index(inner: &Inner) -> Result<(), StorageError> {
        let path = inner.dir.join("index.snapshot");
        let bytes = bincode::serialize(&inner.index)
            .map_err(|e| StorageError::BackendError(format!("serialize index: {e}")))?;
        fs::write(&path, bytes)
            .map_err(|e| StorageError::BackendError(format!("write checkpoint: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl StorageAdapter for SegmentedLogAdapter {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let guard = self.inner.lock().await;
        Ok(guard.index.get(key).cloned().flatten())
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        let mut guard = self.inner.lock().await;
        Self::append_record(&mut guard, 0, key, value).await?;
        guard.index.insert(key.to_vec(), Some(value.to_vec()));
        if guard.ops_since_checkpoint >= guard.checkpoint_every {
            guard
                .current
                .file
                .flush()
                .await
                .map_err(|e| StorageError::BackendError(format!("flush: {e}")))?;
            guard
                .current
                .file
                .get_ref()
                .sync_data()
                .await
                .map_err(|e| StorageError::BackendError(format!("sync log: {e}")))?;
            Self::persist_index(&guard)?;
            guard.ops_since_checkpoint = 0;
        }
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        let mut guard = self.inner.lock().await;
        Self::append_record(&mut guard, 1, key, &[]).await?;
        guard.index.insert(key.to_vec(), None);
        if guard.ops_since_checkpoint >= guard.checkpoint_every {
            guard
                .current
                .file
                .flush()
                .await
                .map_err(|e| StorageError::BackendError(format!("flush: {e}")))?;
            guard
                .current
                .file
                .get_ref()
                .sync_data()
                .await
                .map_err(|e| StorageError::BackendError(format!("sync log: {e}")))?;
            Self::persist_index(&guard)?;
            guard.ops_since_checkpoint = 0;
        }
        Ok(())
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let guard = self.inner.lock().await;
        let mut items: Vec<_> = guard
            .index
            .iter()
            .filter_map(|(k, v)| {
                if k.starts_with(prefix) {
                    v.as_ref().map(|val| (k.clone(), val.clone()))
                } else {
                    None
                }
            })
            .collect();
        items.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(items)
    }

    async fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        let guard = self.inner.lock().await;
        let mut items: Vec<_> = guard
            .index
            .iter()
            .filter_map(|(k, v)| {
                if k.as_slice() >= start && k.as_slice() < end {
                    v.as_ref().map(|val| (k.clone(), val.clone()))
                } else {
                    None
                }
            })
            .collect();
        items.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(items)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use tempfile::tempdir;

    #[tokio::test]
    async fn put_get_delete_round_trip() {
        let dir = tempdir().unwrap();
        let adapter = SegmentedLogAdapter::new(dir.path(), 1024, None, 100)
            .await
            .unwrap();

        adapter.put(b"k1", b"v1").await.unwrap();
        adapter.put(b"k2", b"v2").await.unwrap();

        assert_eq!(adapter.get(b"k1").await.unwrap(), Some(b"v1".to_vec()));
        assert_eq!(adapter.get(b"k2").await.unwrap(), Some(b"v2".to_vec()));

        adapter.delete(b"k1").await.unwrap();
        assert_eq!(adapter.get(b"k1").await.unwrap(), None);
    }

    #[tokio::test]
    async fn rotation_and_reload_preserves_data() {
        let dir = tempdir().unwrap();
        {
            let adapter = SegmentedLogAdapter::new(dir.path(), 64, None, 100)
                .await
                .unwrap();
            for i in 0..20u8 {
                let k = format!("k{i}").into_bytes();
                let v = format!("v{i}").into_bytes();
                adapter.put(&k, &v).await.unwrap();
            }
            // Force some deletes
            adapter.delete(b"k3").await.unwrap();
            adapter.delete(b"k5").await.unwrap();
            adapter.flush().await.unwrap();
        }

        // Reloads from segments
        let adapter = SegmentedLogAdapter::new(dir.path(), 64, None, 100)
            .await
            .unwrap();
        assert_eq!(adapter.get(b"k0").await.unwrap(), Some(b"v0".to_vec()));
        assert_eq!(adapter.get(b"k3").await.unwrap(), None);
        assert_eq!(adapter.get(b"k5").await.unwrap(), None);

        let items = adapter.scan_prefix(b"k").await.unwrap();
        assert!(items.len() >= 18);
    }

    #[tokio::test]
    async fn compaction_reduces_segments() {
        let dir = tempdir().unwrap();
        let adapter = SegmentedLogAdapter::new(dir.path(), 64, None, 100)
            .await
            .unwrap();
        for i in 0..10u8 {
            let k = format!("a{i}").into_bytes();
            let v = format!("b{i}").into_bytes();
            adapter.put(&k, &v).await.unwrap();
        }
        adapter.delete(b"a1").await.unwrap();
        adapter.compact().await.unwrap();
        let items = adapter.scan_prefix(b"a").await.unwrap();
        assert_eq!(items.len(), 9);
        assert_eq!(
            adapter.get(b"a1").await.unwrap(),
            None,
            "delete survives compaction"
        );
    }

    #[tokio::test]
    async fn scan_range_filters_correctly() {
        let dir = tempdir().unwrap();
        let adapter = SegmentedLogAdapter::new(dir.path(), 128, None, 100)
            .await
            .unwrap();
        for i in 0..5u8 {
            let k = format!("key{}", i).into_bytes();
            let v = vec![i];
            adapter.put(&k, &v).await.unwrap();
        }
        let res = adapter.scan_range(b"key1", b"key4").await.unwrap();
        let keys: Vec<_> = res.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(
            keys,
            vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()]
        );
    }

    #[tokio::test]
    async fn handles_random_workload() {
        let dir = tempdir().unwrap();
        let adapter = SegmentedLogAdapter::new(dir.path(), 256, None, 100)
            .await
            .unwrap();
        let mut rng = rand::thread_rng();
        let mut mirror: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();
        for _ in 0..200 {
            let key = vec![rng.gen::<u8>()];
            if rng.gen_bool(0.2) {
                adapter.delete(&key).await.unwrap();
                mirror.insert(key, None);
            } else {
                let val = vec![rng.gen::<u8>(); 4];
                adapter.put(&key, &val).await.unwrap();
                mirror.insert(key, Some(val));
            }
        }
        // Reload and compare
        let adapter = SegmentedLogAdapter::new(dir.path(), 256, None, 100)
            .await
            .unwrap();
        for (k, v) in mirror {
            let got = adapter.get(&k).await.unwrap();
            assert_eq!(got, v);
        }
    }
}
