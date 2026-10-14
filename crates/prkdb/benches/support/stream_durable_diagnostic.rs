//! Opt-in benchmark observer. Never used by production or acceptance timing.
//! Events are fixed 88-byte little-endian records; metadata describes their clock.
use parking_lot::Mutex;
use prkdb_core::vfs::{LockGuard, OpenMode, Vfs, VfsFile};
use serde_json::json;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

pub const CELL: &str = "stream_append/durable/1w/1k/b1";
pub const RECORD_BYTES: usize = 88;
pub const BUFFER_LIMIT_BYTES: usize = 256 * 1024 * 1024;
// Bounded paths, JSON metadata, exporter scratch and breach slot are accounted for.
const METADATA_RESERVE: usize = 64 * 1024;
const MAX_PATH_BYTES: usize = 16 * 1024;
const MAX_PATHS: usize = 64;

pub trait MonotonicClock: Send + Sync {
    fn now_ns(&self) -> io::Result<u64>;
    fn tid(&self) -> u64;
}

pub struct NativeClock;
impl MonotonicClock for NativeClock {
    fn now_ns(&self) -> io::Result<u64> {
        #[cfg(unix)]
        {
            let mut t = libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            // SAFETY: initialized writable timespec; CLOCK_MONOTONIC has no wall-clock epoch.
            if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut t) } != 0 {
                return Err(io::Error::last_os_error());
            }
            if t.tv_sec < 0 || !(0..1_000_000_000).contains(&t.tv_nsec) {
                return Err(io::Error::other("invalid monotonic timespec"));
            }
            (t.tv_sec as u64)
                .checked_mul(1_000_000_000)
                .and_then(|v| v.checked_add(t.tv_nsec as u64))
                .ok_or_else(|| io::Error::other("monotonic nanoseconds overflow"))
        }
        #[cfg(not(unix))]
        {
            Err(io::Error::other(
                "diagnostic requires native CLOCK_MONOTONIC",
            ))
        }
    }
    fn tid(&self) -> u64 {
        #[cfg(target_os = "linux")]
        {
            // SAFETY: gettid takes no pointers and has no side effects.
            unsafe { libc::syscall(libc::SYS_gettid) as u64 }
        }
        #[cfg(target_os = "macos")]
        {
            let mut tid = 0;
            // SAFETY: zero pthread_t selects current thread; tid points to writable u64.
            if unsafe { libc::pthread_threadid_np(0 as libc::pthread_t, &mut tid) } != 0 {
                0
            } else {
                tid
            }
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            0
        }
    }
}

pub struct ClockWitness {
    anchor: Instant,
    before_ns: u64,
    after_ns: u64,
    anchor_ns: u64,
}
impl ClockWitness {
    pub fn checked(anchor: Instant, before_ns: u64, after_ns: u64) -> io::Result<Self> {
        let span = after_ns
            .checked_sub(before_ns)
            .ok_or_else(|| io::Error::other("monotonic witness reversed"))?;
        Ok(Self {
            anchor,
            before_ns,
            after_ns,
            anchor_ns: before_ns + span / 2,
        })
    }
    pub fn map(&self, at: Instant) -> io::Result<u64> {
        let distance = if at >= self.anchor {
            at.duration_since(self.anchor)
        } else {
            self.anchor.duration_since(at)
        };
        let ns = u64::try_from(distance.as_nanos())
            .map_err(|_| io::Error::other("Instant conversion overflow"))?;
        if at >= self.anchor {
            self.anchor_ns.checked_add(ns)
        } else {
            self.anchor_ns.checked_sub(ns)
        }
        .ok_or_else(|| io::Error::other("Instant mapping overflow"))
    }
    pub fn uncertainty_ns(&self) -> u64 {
        self.after_ns - self.before_ns
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Event {
    pub kind: u64,
    pub phase: u64,
    pub operation_id: u64,
    pub start_ns: u64,
    pub append_start_ns: u64,
    pub end_ns: u64,
    pub tid: u64,
    pub arg: u64,
    pub amount: u64,
    pub outcome: i64,
    pub path_id: u64,
}
impl Event {
    pub fn encode(&self) -> [u8; RECORD_BYTES] {
        let fields = [
            self.kind,
            self.phase,
            self.operation_id,
            self.start_ns,
            self.append_start_ns,
            self.end_ns,
            self.tid,
            self.arg,
            self.amount,
            self.outcome as u64,
            self.path_id,
        ];
        let mut raw = [0; RECORD_BYTES];
        for (slot, value) in raw.as_chunks_mut::<8>().0.iter_mut().zip(fields) {
            slot.copy_from_slice(&value.to_le_bytes());
        }
        raw
    }
}

pub struct Operation {
    id: u64,
    phase: u64,
    start_ns: u64,
    tid: u64,
}
struct State {
    events: Vec<Event>,
    paths: Vec<String>,
    path_bytes: usize,
    active: Option<(u64, u64)>,
    next_id: u64,
    warmup: u64,
    measured: u64,
    begun: bool,
    begin_ns: u64,
    ended: bool,
    failure: Option<&'static str>,
    used_records: usize,
}
pub struct Diagnostic {
    clock: Arc<dyn MonotonicClock>,
    witness: ClockWitness,
    record_limit: usize,
    state: Mutex<State>,
}
impl Diagnostic {
    pub fn new(clock: Arc<dyn MonotonicClock>, cap: usize) -> io::Result<Arc<Self>> {
        let before = clock.now_ns()?;
        let anchor = Instant::now();
        let after = clock.now_ns()?;
        let witness = ClockWitness::checked(anchor, before, after)?;
        let record_limit = cap.min(BUFFER_LIMIT_BYTES - METADATA_RESERVE) / RECORD_BYTES;
        if record_limit < 2 {
            return Err(io::Error::other(
                "diagnostic buffer has no prefix/breach capacity",
            ));
        }
        let mut events = Vec::new();
        events
            .try_reserve_exact(record_limit)
            .map_err(io::Error::other)?;
        Ok(Arc::new(Self {
            clock,
            witness,
            record_limit,
            state: Mutex::new(State {
                events,
                paths: Vec::with_capacity(MAX_PATHS),
                path_bytes: 0,
                active: None,
                next_id: 1,
                warmup: 0,
                measured: 0,
                begun: false,
                begin_ns: 0,
                ended: false,
                failure: None,
                used_records: 0,
            }),
        }))
    }
    // Keep allocation and process-wide budget; reset only the per-repetition records.
    pub fn reset_cell(&self) {
        let mut s = self.state.lock();
        s.events.clear();
        s.paths.clear();
        s.path_bytes = 0;
        s.active = None;
        s.next_id = 1;
        s.warmup = 0;
        s.measured = 0;
        s.begun = false;
        s.begin_ns = 0;
        s.ended = false;
    }
    pub fn mapped_timestamp(&self, at: Instant) -> u64 {
        match self.witness.map(at) {
            Ok(v) => v,
            Err(_) => {
                self.fail("Instant mapping failed");
                0
            }
        }
    }
    pub fn timestamp(&self) -> u64 {
        match self.clock.now_ns() {
            Ok(v) => v,
            Err(_) => {
                self.fail("native monotonic clock failed");
                0
            }
        }
    }
    pub fn failed(&self) -> bool {
        self.state.lock().failure.is_some()
    }
    pub fn fail(&self, reason: &'static str) {
        let mut s = self.state.lock();
        self.fail_locked(&mut s, reason);
    }
    fn fail_locked(&self, s: &mut State, reason: &'static str) {
        if s.failure.is_none() {
            s.failure = Some(reason);
            // A breach slot is reserved in the preset byte ceiling.
            if s.used_records < self.record_limit {
                s.events.push(Event {
                    kind: 6,
                    tid: self.clock.tid(),
                    ..Event::default()
                });
                s.used_records += 1;
            }
        }
    }
    fn push(&self, event: Event) {
        let mut s = self.state.lock();
        if s.failure.is_some() {
            return;
        }
        if s.used_records >= self.record_limit - 1 {
            self.fail_locked(&mut s, "diagnostic observation buffer breached");
            return;
        }
        s.events.push(event);
        s.used_records += 1;
    }
    pub fn begin_measurement(&self, at_ns: u64) {
        {
            let mut s = self.state.lock();
            if s.begun {
                self.fail_locked(&mut s, "duplicate measurement begin");
                return;
            }
            s.begun = true;
            s.begin_ns = at_ns;
        }
        self.push(Event {
            kind: 1,
            phase: 1,
            start_ns: at_ns,
            end_ns: at_ns,
            tid: self.clock.tid(),
            ..Event::default()
        });
    }
    pub fn end_measurement(&self) {
        let at = self.timestamp();
        let measured = {
            let mut s = self.state.lock();
            if !s.begun || s.ended || s.active.is_some() || at < s.begin_ns {
                self.fail_locked(&mut s, "incomplete or reversed measurement end");
                return;
            }
            s.ended = true;
            s.measured
        };
        self.push(Event {
            kind: 2,
            phase: 1,
            start_ns: at,
            end_ns: at,
            tid: self.clock.tid(),
            amount: measured,
            ..Event::default()
        });
    }
    pub fn start_operation(&self, phase: u64, start_ns: u64) -> Option<Operation> {
        let mut s = self.state.lock();
        if s.failure.is_some() {
            return None;
        }
        if s.active.is_some() || phase > 1 {
            self.fail_locked(&mut s, "overlapping operation or invalid phase");
            return None;
        }
        if phase == 1 && (!s.begun || s.ended || start_ns < s.begin_ns) || phase == 0 && s.begun {
            self.fail_locked(
                &mut s,
                "operation does not correspond to measurement markers",
            );
            return None;
        }
        if s.used_records >= self.record_limit - 1 {
            self.fail_locked(&mut s, "diagnostic observation buffer breached");
            return None;
        }
        let id = s.next_id;
        s.next_id += 1;
        s.active = Some((id, phase));
        Some(Operation {
            id,
            phase,
            start_ns,
            tid: self.clock.tid(),
        })
    }
    pub fn finish_operation_at(
        &self,
        op: Operation,
        append_start_ns: u64,
        end_ns: u64,
        lsn: u64,
        count: u64,
        outcome: i64,
    ) {
        {
            let mut s = self.state.lock();
            s.active = None;
            if op.phase == 1 {
                s.measured += 1;
            } else {
                s.warmup += 1;
            }
        }
        if !(op.start_ns <= append_start_ns && append_start_ns <= end_ns) {
            self.fail("nonmonotonic append record");
            return;
        }
        self.push(Event {
            kind: 3,
            phase: op.phase,
            operation_id: op.id,
            start_ns: op.start_ns,
            append_start_ns,
            end_ns,
            tid: op.tid,
            arg: lsn,
            amount: count,
            outcome,
            path_id: 0,
        });
        // Tokio may resume the awaiting caller on another worker. The operation
        // record retains submission TID; this witness identifies its actual return
        // thread. No await occurs between return timestamp capture and this call.
        self.push(Event {
            kind: 7,
            phase: op.phase,
            operation_id: op.id,
            start_ns: end_ns,
            end_ns,
            tid: self.clock.tid(),
            ..Event::default()
        });
    }
    fn path(&self, path: &Path) -> u64 {
        let mut s = self.state.lock();
        let Some(path) = path.to_str() else {
            self.fail_locked(&mut s, "non-UTF8 observer path");
            return 0;
        };
        if let Some(i) = s.paths.iter().position(|p| p == path) {
            return i as u64 + 1;
        }
        if path.len() > 512
            || s.path_bytes + path.len() > MAX_PATH_BYTES
            || s.paths.len() >= MAX_PATHS
        {
            self.fail_locked(&mut s, "diagnostic path table breached");
            return 0;
        }
        s.path_bytes += path.len();
        s.paths.push(path.to_owned());
        s.paths.len() as u64
    }
    fn vfs_begin(&self, kind: u64, path_id: u64, arg: u64, amount: u64) -> Option<Event> {
        let active = {
            let s = self.state.lock();
            if s.failure.is_some() {
                return None;
            }
            s.active
        };
        let (id, phase) = active?;
        // Failure only affects the observer: the caller always delegates afterwards.
        {
            let mut s = self.state.lock();
            if s.used_records >= self.record_limit - 1 {
                self.fail_locked(&mut s, "diagnostic observation buffer breached");
                return None;
            }
        }
        Some(Event {
            kind,
            phase,
            operation_id: id,
            start_ns: self.timestamp(),
            tid: self.clock.tid(),
            path_id,
            arg,
            amount,
            ..Event::default()
        })
    }
    fn vfs_end(&self, event: Option<Event>, result: &io::Result<()>) {
        if let Some(mut e) = event {
            e.end_ns = self.timestamp();
            e.outcome = match result {
                Ok(()) => 0,
                Err(err) => err.raw_os_error().map_or(i64::MIN, |n| -(n as i64)),
            };
            if e.end_ns < e.start_ns {
                self.fail("nonmonotonic VFS record");
            } else {
                self.push(e);
            }
        }
    }
    pub fn export(
        &self,
        dir: &Path,
        source: &str,
        repetition: usize,
        warmup_ms: u64,
        measure_ms: u64,
    ) -> io::Result<()> {
        std::fs::create_dir_all(dir)?;
        let s = self.state.lock();
        let mut raw = io::BufWriter::new(std::fs::File::create(dir.join("events.bin"))?);
        for event in &s.events {
            raw.write_all(&event.encode())?;
        }
        raw.flush()?;
        let metadata = json!({"schema_version":1,"record_bytes":RECORD_BYTES,"byte_order":"little",
            "clock":"CLOCK_MONOTONIC","clock_witness":{"before_ns":self.witness.before_ns,
            "after_ns":self.witness.after_ns,"anchor_ns":self.witness.anchor_ns,"uncertainty_ns":self.witness.uncertainty_ns()},
            "source":source,"cell":CELL,"repetition":repetition,"pid":std::process::id(),
            "config":{"writers":1,"value_bytes":1024,"batch":1,"sync_mode":"Durable",
            "segment_bytes":268435456,"sync_interval_ms":10,"warmup_ms":warmup_ms,"measure_ms":measure_ms},
            "phase_totals":{"warmup":s.warmup,"measurement":s.measured},"paths":s.paths,
            "failed":s.failure.is_some(),"failure":s.failure,"complete":s.failure.is_none()&&s.begun&&s.ended&&s.measured>0,
            "record_count":s.events.len(),"buffer_limit_bytes":BUFFER_LIMIT_BYTES,
            "observer_cost":"preallocated bounded storage; clock/conversion/mutex/recording costs included; uncertainty bounds conversion only"});
        serde_json::to_writer(std::fs::File::create(dir.join("metadata.json"))?, &metadata)
            .map_err(io::Error::other)
    }
}

pub struct Session {
    pub diagnostic: Arc<Diagnostic>,
    pub directory: PathBuf,
    pub source: String,
}
impl Session {
    pub fn from_env() -> io::Result<Option<Self>> {
        let Some(dir) = std::env::var_os("PRKDB_STREAM_DURABLE_DIAGNOSTIC_DIR") else {
            return Ok(None);
        };
        if dir.is_empty() {
            return Err(io::Error::other("empty diagnostic directory"));
        }
        let source = std::env::var("PRKDB_STREAM_DURABLE_DIAGNOSTIC_SOURCE")
            .map_err(|_| io::Error::other("diagnostic source identity required"))?;
        if source.len() != 40 || !source.bytes().all(|v| v.is_ascii_hexdigit()) {
            return Err(io::Error::other("diagnostic source must be full Git SHA"));
        }
        Ok(Some(Self {
            diagnostic: Diagnostic::new(Arc::new(NativeClock), BUFFER_LIMIT_BYTES)?,
            directory: dir.into(),
            source,
        }))
    }
}

pub struct ObservedVfs {
    inner: Arc<dyn Vfs>,
    diagnostic: Arc<Diagnostic>,
}
impl ObservedVfs {
    pub fn new(inner: Arc<dyn Vfs>, diagnostic: Arc<Diagnostic>) -> Self {
        Self { inner, diagnostic }
    }
    fn file(&self, path: &Path, inner: Arc<dyn VfsFile>) -> Arc<dyn VfsFile> {
        Arc::new(ObservedFile::new(
            inner,
            self.diagnostic.clone(),
            self.diagnostic.path(path),
        ))
    }
}
impl Vfs for ObservedVfs {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
        self.inner.open(path, mode).map(|f| self.file(path, f))
    }
    fn create(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>> {
        self.inner.create(path).map(|f| self.file(path, f))
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.inner.rename(from, to)
    }
    fn remove(&self, path: &Path) -> io::Result<()> {
        self.inner.remove(path)
    }
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.create_dir_all(path)
    }
    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        self.inner.read_dir(path)
    }
    fn exists(&self, path: &Path) -> io::Result<bool> {
        self.inner.exists(path)
    }
    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        self.inner.sync_dir(path)
    }
    fn lock_exclusive(&self, path: &Path) -> io::Result<Box<dyn LockGuard>> {
        self.inner.lock_exclusive(path)
    }
}
pub struct ObservedFile {
    inner: Arc<dyn VfsFile>,
    diagnostic: Arc<Diagnostic>,
    path_id: u64,
}
impl ObservedFile {
    pub fn new(inner: Arc<dyn VfsFile>, diagnostic: Arc<Diagnostic>, path_id: u64) -> Self {
        Self {
            inner,
            diagnostic,
            path_id,
        }
    }
}
impl VfsFile for ObservedFile {
    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        let event = self
            .diagnostic
            .vfs_begin(4, self.path_id, offset, buf.len() as u64);
        let result = self.inner.write_at(offset, buf);
        self.diagnostic.vfs_end(event, &result);
        result
    }
    fn sync_data(&self) -> io::Result<()> {
        let event = self.diagnostic.vfs_begin(5, self.path_id, 0, 0);
        let result = self.inner.sync_data();
        self.diagnostic.vfs_end(event, &result);
        result
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read_at(offset, buf)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn is_empty(&self) -> io::Result<bool> {
        self.inner.is_empty()
    }
}
