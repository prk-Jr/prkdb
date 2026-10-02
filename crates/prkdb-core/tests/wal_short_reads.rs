//! Short reads are valid VFS results, not evidence of a torn WAL frame.
use prkdb_core::vfs::{LockGuard, OpenMode, StdVfs, Vfs, VfsFile};
use prkdb_core::wal::frame::{encode_frame, FrameFault, FrameKind};
use prkdb_core::wal::segment::{
    scan_segment, segment_file_name, write_segment_header, SEGMENT_HEADER_LEN,
};
use prkdb_core::wal::{SyncMode, Wal, WalConfig, WalError, WalOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

struct ShortReadFile {
    inner: Arc<dyn VfsFile>,
    max_read: usize,
    eof_at: Option<u64>,
    fail_at: Option<u64>,
    short_header: bool,
}
impl VfsFile for ShortReadFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        assert!(
            !buf.is_empty(),
            "no read after the requested bytes are complete"
        );
        if self.fail_at.is_some_and(|at| offset >= at) {
            return Err(io::Error::other("injected short-read failure"));
        }
        let available = self
            .eof_at
            .map_or(buf.len(), |at| at.saturating_sub(offset) as usize);
        let max_read = if offset < SEGMENT_HEADER_LEN && !self.short_header {
            buf.len()
        } else {
            self.max_read
        };
        let len = buf.len().min(max_read).min(available);
        if len == 0 {
            return Ok(0);
        }
        self.inner.read_at(offset, &mut buf[..len])
    }
    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        self.inner.write_at(offset, buf)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()
    }
}
fn short_file(inner: Arc<dyn VfsFile>, max_read: usize) -> ShortReadFile {
    ShortReadFile {
        inner,
        max_read,
        eof_at: None,
        fail_at: None,
        short_header: true,
    }
}
struct ShortReadVfs;
impl Vfs for ShortReadVfs {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
        let mut file = short_file(StdVfs.open(path, mode)?, 7);
        // Isolate frame refills: a complete header must not hide acknowledged loss.
        file.short_header = false;
        Ok(Arc::new(file))
    }
    fn create(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>> {
        StdVfs.create(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        StdVfs.rename(from, to)
    }
    fn remove(&self, path: &Path) -> io::Result<()> {
        StdVfs.remove(path)
    }
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        StdVfs.create_dir_all(path)
    }
    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        StdVfs.read_dir(path)
    }
    fn exists(&self, path: &Path) -> io::Result<bool> {
        StdVfs.exists(path)
    }
    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        StdVfs.sync_dir(path)
    }
    fn lock_exclusive(&self, path: &Path) -> io::Result<Box<dyn LockGuard>> {
        StdVfs.lock_exclusive(path)
    }
}
fn seed(path: &Path, payloads: &[Vec<u8>]) -> (Arc<dyn VfsFile>, Vec<u64>) {
    let file = StdVfs.create(path).unwrap();
    write_segment_header(&*file, 1).unwrap();
    let mut bytes = Vec::new();
    let mut offsets = Vec::new();
    for (i, payload) in payloads.iter().enumerate() {
        offsets.push(SEGMENT_HEADER_LEN + bytes.len() as u64);
        encode_frame(&mut bytes, i as u64 + 1, FrameKind::Batch, payload);
    }
    file.write_at(SEGMENT_HEADER_LEN, &bytes).unwrap();
    file.sync_data().unwrap();
    (file, offsets)
}
#[test]
fn seven_byte_reads_scan_every_frame_without_a_false_torn_tail() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(segment_file_name(1));
    let payloads = vec![b"first".to_vec(), b"second payload".to_vec()];
    let (file, _) = seed(&path, &payloads);
    let reader = short_file(file, 7);
    let mut seen = Vec::new();
    let scan = scan_segment(&reader, &path, 1, &mut |loc, _, payload| {
        seen.push((loc.lsn, payload.to_vec()));
        Ok(())
    })
    .unwrap();
    assert_eq!(
        scan.stopped, None,
        "short reads must not manufacture corrupt bytes"
    );
    assert_eq!(seen, (1..=2).zip(payloads).collect::<Vec<_>>());
    assert_eq!(scan.next_lsn, 3);
    assert_eq!(scan.valid_len, scan.file_len);
}
#[test]
fn durable_recovery_through_short_reads_keeps_acknowledged_frames() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = WalConfig {
        sync_mode: SyncMode::Durable,
        ..WalConfig::default()
    };
    let opts = WalOptions::from_config(&cfg);
    let payloads = vec![
        b"first durable write".to_vec(),
        b"second durable write".to_vec(),
    ];
    let (wal, _) = Wal::open(
        Arc::new(StdVfs),
        dir.path(),
        opts.clone(),
        1,
        &mut |_, _, _| Ok(()),
    )
    .unwrap();
    for payload in &payloads {
        wal.append_blocking(payload.clone(), None).unwrap();
    }
    wal.close().unwrap();
    let path = dir.path().join(segment_file_name(1));
    let len_before = StdVfs.open(&path, OpenMode::Read).unwrap().len().unwrap();
    let mut replayed = Vec::new();
    let (wal, report) = Wal::open(
        Arc::new(ShortReadVfs),
        dir.path(),
        opts,
        1,
        &mut |_, _, payload| {
            replayed.push(payload.to_vec());
            Ok(())
        },
    )
    .unwrap();
    wal.close().unwrap();
    let len_after = StdVfs.open(&path, OpenMode::Read).unwrap().len().unwrap();
    assert_eq!(
        replayed, payloads,
        "recovery must preserve every acknowledged Durable write; file length {len_before} -> {len_after}; truncation {:?}", report.truncated
    );
    assert_eq!(report.truncated, None);
    assert_eq!(report.next_lsn, 3);
    assert_eq!(
        StdVfs.open(&path, OpenMode::Read).unwrap().len().unwrap(),
        len_before
    );
}
#[test]
fn short_reads_fill_a_frame_spanning_scan_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(segment_file_name(1));
    let payloads = vec![vec![17; (1 << 20) + 23], b"after large frame".to_vec()];
    let (file, _) = seed(&path, &payloads);
    let reader = short_file(file, 128 << 10);
    let mut seen = Vec::new();
    let scan = scan_segment(&reader, &path, 1, &mut |_, _, payload| {
        seen.push(payload.to_vec());
        Ok(())
    })
    .unwrap();
    assert_eq!(scan.stopped, None);
    assert_eq!(seen, payloads);
    assert_eq!(scan.valid_len, scan.file_len);
}
#[test]
fn short_reads_preserve_real_torn_tail_boundaries() {
    for early_eof in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(segment_file_name(1));
        let (file, offsets) = seed(&path, &[b"first".to_vec(), b"second payload".to_vec()]);
        let mut reader = short_file(file.clone(), 7);
        let tail = offsets[1] + 5;
        if early_eof {
            reader.eof_at = Some(tail);
        } else {
            file.set_len(tail).unwrap();
        }
        let mut seen = Vec::new();
        let scan = scan_segment(&reader, &path, 1, &mut |loc, _, _| {
            seen.push(loc.lsn);
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, vec![1], "early_eof={early_eof}");
        assert_eq!(scan.stopped, Some((offsets[1], FrameFault::Truncated)));
        assert_eq!(scan.valid_len, offsets[1]);
        assert_eq!(scan.next_lsn, 2);
    }
}
#[test]
fn a_failure_after_a_short_read_is_reported_as_io() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(segment_file_name(1));
    let (file, _) = seed(&path, &[b"first".to_vec()]);
    let mut reader = short_file(file, 7);
    reader.short_header = false;
    reader.fail_at = Some(SEGMENT_HEADER_LEN + 7);
    let err = scan_segment(&reader, &path, 1, &mut |_, _, _| Ok(())).unwrap_err();
    assert!(
        matches!(err, WalError::Io(ref e) if e.to_string() == "injected short-read failure"),
        "{err}"
    );
}
#[test]
fn completed_reads_do_not_issue_an_empty_follow_up_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(segment_file_name(1));
    let (file, _) = seed(&path, &[b"first".to_vec()]);
    let reader = short_file(file, usize::MAX);
    let scan = scan_segment(&reader, &path, 1, &mut |_, _, _| Ok(())).unwrap();
    assert_eq!(scan.stopped, None);
    assert_eq!(scan.next_lsn, 2);
}

#[test]
fn premature_eof_reports_the_actual_segment_header_length() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(segment_file_name(1));
    let (file, _) = seed(&path, &[b"first".to_vec()]);
    assert!(file.len().unwrap() >= SEGMENT_HEADER_LEN);
    let mut reader = short_file(file, 7);
    reader.eof_at = Some(7);
    let err = scan_segment(&reader, &path, 1, &mut |_, _, _| Ok(())).unwrap_err();
    assert!(
        matches!(err, WalError::CorruptSegment { path: ref found, offset: 0, ref reason }
            if found == &path && reason.contains("only 7 bytes of segment header")),
        "{err}"
    );
}
