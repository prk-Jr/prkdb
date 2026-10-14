//! Header-only lookup: count actual VFS reads, never payload or fallback I/O.
use prkdb_core::vfs::{LockGuard, OpenMode, StdVfs, Vfs, VfsFile};
use prkdb_core::wal::frame::{encode_frame, FrameKind, MAX_PAYLOAD_LEN};
use prkdb_core::wal::segment::{read_stream_frame_crc, RecordLoc, SEGMENT_HEADER_LEN};
use prkdb_core::wal::{FrontRelease, SyncMode, Wal, WalConfig, WalError, WalOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

struct Counted {
    bytes: Vec<u8>,
    reads: Mutex<Vec<(u64, usize, usize)>>,
    lengths: AtomicUsize,
    chunk: usize,
    eof: Option<u64>,
    reported_len: Option<u64>,
    fail: bool,
    fail_len: bool,
    header_at: Option<u64>,
}
impl VfsFile for Counted {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        if self.fail {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "injected header read",
            ));
        }
        if self
            .header_at
            .is_some_and(|at| offset >= at && offset <= at + 16)
        {
            let start = (offset - self.header_at.unwrap()) as usize;
            let n = buf.len().min(17 - start);
            buf[..n].copy_from_slice(&self.bytes[start..start + n]);
            self.reads.lock().unwrap().push((offset, buf.len(), n));
            return Ok(n);
        }
        let end = self
            .eof
            .unwrap_or(self.bytes.len() as u64)
            .min(self.bytes.len() as u64);
        let n = end
            .saturating_sub(offset)
            .min(buf.len() as u64)
            .min(self.chunk as u64) as usize;
        if n > 0 {
            buf[..n].copy_from_slice(&self.bytes[offset as usize..offset as usize + n]);
        }
        self.reads.lock().unwrap().push((offset, buf.len(), n));
        Ok(n)
    }
    fn len(&self) -> io::Result<u64> {
        self.lengths.fetch_add(1, Ordering::Relaxed);
        if self.fail_len {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "injected metadata failure",
            ));
        }
        Ok(self.reported_len.unwrap_or(self.bytes.len() as u64))
    }
    fn write_at(&self, _: u64, _: &[u8]) -> io::Result<()> {
        panic!("lookup never writes")
    }
    fn set_len(&self, _: u64) -> io::Result<()> {
        panic!("lookup never truncates")
    }
    fn sync_data(&self) -> io::Result<()> {
        panic!("lookup never syncs")
    }
}
fn fixture(lengths: &[usize]) -> (Counted, Vec<RecordLoc>, Vec<u32>) {
    let mut bytes = vec![0; SEGMENT_HEADER_LEN as usize];
    let mut locs = Vec::new();
    let mut crcs = Vec::new();
    for (i, len) in lengths.iter().enumerate() {
        let offset = bytes.len();
        let lsn = i as u64 + 1;
        locs.push(RecordLoc {
            lsn,
            segment: 1,
            offset: offset as u64,
            payload_len: *len as u32,
        });
        encode_frame(&mut bytes, lsn, FrameKind::Records, &vec![i as u8; *len]);
        crcs.push(u32::from_le_bytes(
            bytes[offset + 4..offset + 8].try_into().unwrap(),
        ));
    }
    (
        Counted {
            bytes,
            reads: Mutex::new(Vec::new()),
            lengths: AtomicUsize::new(0),
            chunk: usize::MAX,
            eof: None,
            reported_len: None,
            fail: false,
            fail_len: false,
            header_at: None,
        },
        locs,
        crcs,
    )
}
fn lookup(f: &Counted, loc: RecordLoc, target: u64) -> Result<u32, WalError> {
    read_stream_frame_crc(f, Path::new("00000000000000000001.wal"), loc, target)
}
fn assert_headers_only(f: &Counted, locs: &[RecordLoc], logical: usize, bytes: usize) {
    let reads = f.reads.lock().unwrap();
    assert_eq!(
        reads
            .iter()
            .filter(|(o, _, _)| locs.iter().any(|l| l.offset == *o))
            .count(),
        logical
    );
    assert_eq!(reads.iter().map(|(_, _, n)| n).sum::<usize>(), bytes);
    for (o, requested, _) in reads.iter() {
        assert!(
            locs.iter()
                .any(|l| *o >= l.offset && *o + *requested as u64 <= l.offset + 17),
            "payload read {o}/{requested}"
        );
    }
    assert_eq!(
        f.lengths.load(Ordering::Relaxed),
        1,
        "file length sampled once"
    );
}
#[test]
fn exact_hint_fetches_one_header_even_for_a_large_payload() {
    let (f, locs, crcs) = fixture(&[1 << 20]);
    assert_eq!(lookup(&f, locs[0], 1).unwrap(), crcs[0]);
    assert_headers_only(&f, &locs, 1, 17);
}
#[test]
fn minimum_records_reach_exact_1873_fetch_31841_byte_ceiling() {
    let (f, locs, crcs) = fixture(&vec![18; 1873]);
    assert_eq!(lookup(&f, locs[0], 1873).unwrap(), crcs[1872]);
    assert_headers_only(&f, &locs, 1873, 31841);
}
#[test]
fn refuses_a_stale_hint_before_fetch_1874() {
    let (f, locs, _) = fixture(&vec![18; 1874]);
    assert!(matches!(
        lookup(&f, locs[0], 1874),
        Err(WalError::CorruptSegment { .. })
    ));
    assert_headers_only(&f, &locs, 1873, 31841);
}
#[test]
fn refuses_a_65536_byte_span_but_accepts_its_exact_hint() {
    let (f, locs, crcs) = fixture(&[65519, 18]);
    assert!(matches!(
        lookup(&f, locs[0], 2),
        Err(WalError::CorruptSegment { .. })
    ));
    assert_headers_only(&f, &locs, 1, 17);
    f.reads.lock().unwrap().clear();
    f.lengths.store(0, Ordering::Relaxed);
    assert_eq!(lookup(&f, locs[1], 2).unwrap(), crcs[1]);
    assert_headers_only(&f, &locs, 1, 17);
}
#[test]
fn short_reads_split_calls_without_reading_payloads() {
    let (mut f, locs, crcs) = fixture(&[18, 18]);
    f.chunk = 3;
    assert_eq!(lookup(&f, locs[0], 2).unwrap(), crcs[1]);
    assert_headers_only(&f, &locs, 2, 34);
    assert_eq!(f.reads.lock().unwrap().len(), 12);
}
#[test]
fn refuses_bad_kind_short_payload_length_mismatch_and_lsn_gap_at_every_hop() {
    for hop in [0, 1] {
        for variant in 0..6 {
            let (mut f, locs, _) = fixture(&[18, 18]);
            let at = locs[hop].offset as usize;
            match variant {
                0 => f.bytes[at + 16] = FrameKind::Batch as u8,
                1 => f.bytes[at + 16] = FrameKind::Elided as u8,
                2 => f.bytes[at + 16] = 255,
                3 => f.bytes[at..at + 4].copy_from_slice(&17u32.to_le_bytes()),
                4 => f.bytes[at + 8..at + 16].copy_from_slice(&99u64.to_le_bytes()),
                5 => f.bytes[at..at + 4]
                    .copy_from_slice(&((MAX_PAYLOAD_LEN + 1) as u32).to_le_bytes()),
                _ => unreachable!(),
            }
            let result = lookup(&f, locs[0], 2);
            if variant == 5 {
                assert!(
                    matches!(result, Err(WalError::RecordTooLarge { .. })),
                    "oversized payload hop{hop}"
                );
            } else {
                assert!(
                    matches!(result, Err(WalError::CorruptSegment { .. })),
                    "hop{hop} variant{variant}"
                );
            }
            assert!(f.reads.lock().unwrap().len() <= hop + 1);
        }
    }
    let (f, locs, _) = fixture(&[18]);
    let mut wrong = locs[0];
    wrong.payload_len = 19;
    assert!(matches!(
        lookup(&f, wrong, 1),
        Err(WalError::CorruptSegment { .. })
    ));
}
#[test]
fn refuses_invalid_hint_and_checked_extent_without_fallback() {
    for which in 0..5 {
        let (f, locs, _) = fixture(&[18]);
        let mut loc = locs[0];
        match which {
            0 => loc.offset = 23,
            1 => loc.segment = 2,
            2 => loc.lsn = 0,
            3 => loc.lsn = 2,
            4 => loc.offset = u64::MAX - 8,
            _ => unreachable!(),
        }
        assert!(matches!(
            lookup(&f, loc, 1),
            Err(WalError::CorruptSegment { .. })
        ));
        assert!(f.reads.lock().unwrap().is_empty());
    }
    let (mut f, locs, _) = fixture(&[18]);
    f.bytes.pop();
    assert!(matches!(
        lookup(&f, locs[0], 1),
        Err(WalError::CorruptSegment { .. })
    ));
    let (mut f, locs, _) = fixture(&[18]);
    f.eof = Some(locs[0].offset + 16);
    assert!(matches!(
        lookup(&f, locs[0], 1),
        Err(WalError::CorruptSegment { .. })
    ));
}
#[test]
fn io_failure_preserves_kind_source_and_segment_context() {
    let (mut f, locs, _) = fixture(&[18]);
    f.fail = true;
    let Err(WalError::Io(e)) = lookup(&f, locs[0], 1) else {
        panic!("expected typed I/O")
    };
    assert_eq!(e.kind(), io::ErrorKind::PermissionDenied);
    assert!(e.to_string().contains("00000000000000000001.wal"));
    assert!(e.to_string().contains("24"));
    assert!(std::error::Error::source(&e).is_some());
}
#[derive(Clone, Default)]
struct WalProbeFs(Arc<AtomicUsize>);
struct WalProbeFile(Arc<dyn VfsFile>, Arc<AtomicUsize>);
impl VfsFile for WalProbeFile {
    fn read_at(&self, o: u64, b: &mut [u8]) -> io::Result<usize> {
        self.1.fetch_add(1, Ordering::Relaxed);
        self.0.read_at(o, b)
    }
    fn write_at(&self, o: u64, b: &[u8]) -> io::Result<()> {
        self.0.write_at(o, b)
    }
    fn len(&self) -> io::Result<u64> {
        self.0.len()
    }
    fn set_len(&self, n: u64) -> io::Result<()> {
        self.0.set_len(n)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.0.sync_data()
    }
}
impl Vfs for WalProbeFs {
    fn open(&self, p: &Path, m: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
        Ok(Arc::new(WalProbeFile(StdVfs.open(p, m)?, self.0.clone())))
    }
    fn create(&self, p: &Path) -> io::Result<Arc<dyn VfsFile>> {
        Ok(Arc::new(WalProbeFile(StdVfs.create(p)?, self.0.clone())))
    }
    fn rename(&self, p: &Path, q: &Path) -> io::Result<()> {
        StdVfs.rename(p, q)
    }
    fn remove(&self, p: &Path) -> io::Result<()> {
        StdVfs.remove(p)
    }
    fn create_dir_all(&self, p: &Path) -> io::Result<()> {
        StdVfs.create_dir_all(p)
    }
    fn read_dir(&self, p: &Path) -> io::Result<Vec<PathBuf>> {
        StdVfs.read_dir(p)
    }
    fn exists(&self, p: &Path) -> io::Result<bool> {
        StdVfs.exists(p)
    }
    fn sync_dir(&self, p: &Path) -> io::Result<()> {
        StdVfs.sync_dir(p)
    }
    fn lock_exclusive(&self, p: &Path) -> io::Result<Box<dyn LockGuard>> {
        StdVfs.lock_exclusive(p)
    }
}
fn wal_fixture(dir: &Path) -> (Wal, WalProbeFs) {
    let cfg = WalConfig {
        log_dir: dir.into(),
        sync_mode: SyncMode::Fast,
        sync_interval_ms: 3600000,
        ..WalConfig::default()
    };
    let mut opts = WalOptions::from_config(&cfg);
    opts.front_release = FrontRelease::Retention;
    opts.append_kind = FrameKind::Records;
    let probe = WalProbeFs::default();
    let wal = Wal::open(Arc::new(probe.clone()), dir, opts, 1, &mut |_, _, _| Ok(()))
        .unwrap()
        .0;
    probe.0.store(0, Ordering::Relaxed);
    (wal, probe)
}
#[tokio::test]
async fn wal_uses_owned_handle_and_distinguishes_absence_from_invalid_hints() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, probe) = wal_fixture(dir.path());
    wal.append(vec![0; 18], None).await.unwrap();
    let first = RecordLoc {
        lsn: 1,
        segment: 1,
        offset: 24,
        payload_len: 18,
    };
    assert!(wal.stream_frame_crc_from(1, 1, first).unwrap().is_some());
    assert_eq!(wal.stream_frame_crc_from(0, 1, first).unwrap(), None);
    assert_eq!(wal.stream_frame_crc_from(2, u64::MAX, first).unwrap(), None);
    assert_eq!(wal.stream_frame_crc_from(1, 0, first).unwrap(), None);
    assert_eq!(
        probe.0.swap(0, Ordering::Relaxed),
        1,
        "all three absence queries require zero reads"
    );
    wal.roll().await.unwrap();
    wal.append(vec![1; 18], None).await.unwrap();
    assert!(matches!(
        wal.stream_frame_crc_from(2, 2, first),
        Err(WalError::CorruptSegment { .. })
    ));
    assert_eq!(
        probe.0.load(Ordering::Relaxed),
        0,
        "wrong segment rejected before any read"
    );
    let second = RecordLoc {
        lsn: 2,
        segment: 2,
        offset: 24,
        payload_len: 18,
    };
    assert!(wal.stream_frame_crc_from(2, 2, second).unwrap().is_some());
    wal.sync().await.unwrap();
    wal.set_log_start(2).unwrap();
    wal.remove_leading_segments(2).unwrap();
    assert_eq!(wal.stream_frame_crc_from(1, 2, first).unwrap(), None);
    wal.close().unwrap();
}
#[tokio::test]
async fn wal_refuses_rewritten_generation_without_reading_previous_handle() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, probe) = wal_fixture(dir.path());
    wal.append(vec![0; 18], None).await.unwrap();
    wal.roll().await.unwrap();
    wal.append(vec![1; 18], None).await.unwrap();
    let source = wal.segment_path(1);
    let replacement = dir.path().join("replacement.tmp");
    std::fs::copy(&source, &replacement).unwrap();
    StdVfs
        .open(&replacement, prkdb_core::vfs::OpenMode::ReadWrite)
        .unwrap()
        .sync_data()
        .unwrap();
    wal.replace_segment(1, &replacement).unwrap();
    probe.0.store(0, Ordering::Relaxed);
    assert_eq!(wal.segment_generation(1), Some(1));
    assert!(matches!(
        wal.stream_frame_crc_from(
            1,
            2,
            RecordLoc {
                lsn: 1,
                segment: 1,
                offset: 24,
                payload_len: 18
            }
        ),
        Err(WalError::Moved { .. })
    ));
    assert_eq!(
        probe.0.load(Ordering::Relaxed),
        0,
        "generation refusal must not read either handle"
    );
    wal.close().unwrap();
}

#[test]
fn payload_mutation_does_not_turn_crc_metadata_lookup_into_validation() {
    let (mut f, locs, crcs) = fixture(&[18]);
    f.bytes[41] ^= 1;
    assert_eq!(lookup(&f, locs[0], 1).unwrap(), crcs[0]);
    assert_headers_only(&f, &locs, 1, 17);
}
#[test]
fn metadata_length_failure_preserves_typed_io_and_path_context() {
    let (mut f, locs, _) = fixture(&[18]);
    f.fail_len = true;
    let Err(WalError::Io(e)) = lookup(&f, locs[0], 1) else {
        panic!("expected typed metadata I/O")
    };
    assert_eq!(e.kind(), io::ErrorKind::PermissionDenied);
    assert!(e.to_string().contains("00000000000000000001.wal"));
    assert!(std::error::Error::source(&e).is_some());
    assert!(f.reads.lock().unwrap().is_empty());
}
#[test]
fn virtual_payload_extent_overflow_is_refused_after_one_header() {
    let (mut f, locs, _) = fixture(&[18]);
    f.bytes = f.bytes[24..41].to_vec();
    f.reported_len = Some(u64::MAX);
    f.header_at = Some(u64::MAX - 20);
    let mut hint = locs[0];
    hint.offset = u64::MAX - 20;
    assert!(matches!(
        lookup(&f, hint, 1),
        Err(WalError::CorruptSegment { .. })
    ));
    assert_eq!(f.reads.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn wal_refuses_keyed_mode_before_header_lookup() {
    for retention in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = WalConfig {
            log_dir: dir.path().into(),
            ..WalConfig::default()
        };
        let probe = WalProbeFs::default();
        let mut opts = WalOptions::from_config(&cfg);
        if retention {
            opts.front_release = FrontRelease::Retention;
        }
        let (wal, _) = Wal::open(
            Arc::new(probe.clone()),
            dir.path(),
            opts,
            1,
            &mut |_, _, _| Ok(()),
        )
        .unwrap();
        wal.append(vec![0; 18], None).await.unwrap();
        probe.0.store(0, Ordering::Relaxed);
        assert!(matches!(
            wal.stream_frame_crc_from(
                1,
                1,
                RecordLoc {
                    lsn: 1,
                    segment: 1,
                    offset: 24,
                    payload_len: 18
                }
            ),
            Err(WalError::Corruption(_))
        ));
        assert_eq!(probe.0.load(Ordering::Relaxed), 0);
        wal.close().unwrap();
    }
}
