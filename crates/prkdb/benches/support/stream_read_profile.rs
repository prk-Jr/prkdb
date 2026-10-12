//! Optional setup verification and measured-phase markers; never speed-gate output.
use parking_lot::Mutex;
use prkdb::stream_log::{AppendAck, Clock, EventSeq, ReadBatch, ReadLimits, StartAt, StreamLog};
use serde_json::json;
use std::io;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};

const MIB: usize = 1024 * 1024;
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

pub struct ExpectedFrame {
    pub ack: AppendAck,
    pub time_ms: i64,
}
pub struct Fixture {
    pub frames: Vec<ExpectedFrame>,
    pub batch: usize,
    pub value: Vec<u8>,
}
impl Fixture {
    pub async fn verify_log(&self, log: &StreamLog) -> Result<(), String> {
        let total = self.validate_shape()?;
        let end = self.watermark()?;
        if log.next_offset() != end || log.durable_end() != end {
            return Err("fixture acknowledged/synced end differs".into());
        }
        let mut from = self.frames[0].ack.first();
        let mut consumed = 0;
        let limits = ReadLimits {
            max_records: usize::MAX,
            max_bytes: MIB,
        };
        loop {
            let page = log
                .read_from(StartAt::Offset(from), limits)
                .await
                .map_err(|e| e.to_string())?;
            let count = self.verify_page(consumed, from, &page)?;
            if count == 0 {
                break;
            }
            consumed += count;
            from = page.next;
        }
        if consumed != total {
            return Err("fixture lost records".into());
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<usize, String> {
        if self.batch == 0 || self.batch > 65536 || self.frames.is_empty() || self.value.is_empty()
        {
            return Err("invalid fixture shape".into());
        }
        if self
            .frames
            .iter()
            .any(|f| f.ack.count as usize != self.batch)
        {
            return Err("append count differs from inputs".into());
        }
        self.frames
            .len()
            .checked_mul(self.batch)
            .ok_or_else(|| "fixture count overflow".into())
    }
    pub fn watermark(&self) -> Result<EventSeq, String> {
        let last = self.frames.last().ok_or("empty fixture")?;
        EventSeq::try_from_wal(last.ack.lsn.checked_add(1).ok_or("LSN overflow")?, 0)
            .ok_or_else(|| "unrepresentable watermark".into())
    }
    pub fn verify_page(
        &self,
        consumed: usize,
        from: EventSeq,
        page: &ReadBatch,
    ) -> Result<usize, String> {
        let total = self.validate_shape()?;
        if consumed > total {
            return Err("consumption exceeds fixture".into());
        }
        let size = self
            .value
            .len()
            .checked_add(7)
            .ok_or("record size overflow")?;
        let count = (MIB / size).max(1).min(total - consumed);
        if page.records.len() != count {
            return Err("page count differs from input-derived limits".into());
        }
        for (i, record) in page.records.iter().enumerate() {
            let ordinal = consumed + i;
            let frame = &self.frames[ordinal / self.batch];
            let index = ordinal % self.batch;
            if record.offset != frame.ack.offset(index as u32)
                || record.append_time_ms != frame.time_ms
                || record.key.as_deref() != Some(format!("k{index:06}").as_bytes())
                || record.value != self.value
                || !record.headers.is_empty()
            {
                return Err(format!("record {ordinal} differs from append inputs"));
            }
        }
        let next = if count == 0 {
            from
        } else {
            let last = consumed + count - 1;
            let frame = &self.frames[last / self.batch];
            EventSeq::from_raw(frame.ack.offset((last % self.batch) as u32).raw() + 1)
        };
        if page.next != next || page.high_watermark != self.watermark()? {
            return Err("resume or watermark differs".into());
        }
        Ok(count)
    }
}

#[derive(Default)]
pub struct RecordingClock {
    times: Mutex<Vec<i64>>,
}
impl Clock for RecordingClock {
    fn now_ms(&self) -> i64 {
        // Same signed and saturating conversion as stream_log::SystemClock.
        let time = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => d.as_millis().min(i64::MAX as u128) as i64,
            Err(e) => -(e.duration().as_millis().min(i64::MAX as u128) as i64),
        };
        self.times.lock().push(time);
        time
    }
}

pub struct ReadProfiler {
    pub clock: Arc<RecordingClock>,
    pub fixture: Fixture,
    id: u64,
}
impl ReadProfiler {
    pub fn from_env(value: &[u8], batch: usize, cold: bool) -> Result<Option<Self>, String> {
        match std::env::var("SPIKE_STREAM_PROFILE") {
            Err(std::env::VarError::NotPresent) => return Ok(None),
            Ok(v) if v == "1" => {}
            _ => return Err("SPIKE_STREAM_PROFILE must be absent or1".into()),
        }
        if option_env!("PRKDB_STREAM_PROFILE_SOURCE_SHA").is_none() {
            return Err("profile build must embed PRKDB_STREAM_PROFILE_SOURCE_SHA".into());
        }
        if cold || value.len() != 1024 || batch != 1024 {
            return Err("profile requires original warm1KiB/b1024 cell".into());
        }
        Ok(Some(Self::new(value, batch)))
    }
    pub fn new(value: &[u8], batch: usize) -> Self {
        Self {
            clock: Arc::new(RecordingClock::default()),
            fixture: Fixture {
                frames: vec![],
                batch,
                value: value.to_vec(),
            },
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
        }
    }
    pub fn record_append(&mut self, ack: AppendAck) -> Result<(), String> {
        let times = std::mem::take(&mut *self.clock.times.lock());
        if times.len() != 1 || ack.count as usize != self.fixture.batch {
            return Err(
                "each append must have exactly one recorded timestamp and input count".into(),
            );
        }
        self.fixture.frames.push(ExpectedFrame {
            ack,
            time_ms: times[0],
        });
        Ok(())
    }
    pub async fn verify(&self, log: &StreamLog) -> Result<(), String> {
        if self.fixture.frames.len() != 256
            || self.fixture.batch != 1024
            || self.fixture.value.len() != 1024
            || !self.clock.times.lock().is_empty()
        {
            return Err(
                "profile needs exactly256 acknowledged input batches, no unused clock entries"
                    .into(),
            );
        }
        self.fixture.verify_log(log).await?;
        self.emit("fixture", None);
        Ok(())
    }
    pub fn begin(&self) -> io::Result<()> {
        self.emit("begin", Some(monotonic_ns()?));
        Ok(())
    }
    pub fn end(&self) -> io::Result<()> {
        self.emit("end", Some(monotonic_ns()?));
        Ok(())
    }
    fn emit(&self, phase: &str, mono_ns: Option<u64>) {
        let mut marker = json!({"schema":1,"phase":phase,"cell":"stream_read/tail/1k",
            "id":self.id,"pid":std::process::id(),
            "source_sha":option_env!("PRKDB_STREAM_PROFILE_SOURCE_SHA").unwrap_or("unrecorded")});
        if let Some(ns) = mono_ns {
            marker["mono_ns"] = json!(ns);
        }
        if phase == "fixture" {
            let fields = json!({"verified":true,"frames":256,"batch":1024,"records":262144,
                "value_bytes":268435456,"value_size":1024,"key_size":7,"compression":"none",
                "mode":"Fast","synced":true,"retention":false,"segment_bytes":268435456});
            if let (Some(target), Some(fields)) = (marker.as_object_mut(), fields.as_object()) {
                target.extend(fields.iter().map(|(k, v)| (k.clone(), v.clone())));
            }
        }
        println!("# STREAM_PROFILE {marker}");
    }
}

#[cfg(target_os = "linux")]
fn monotonic_ns() -> io::Result<u64> {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: time is valid writable storage; CLOCK_MONOTONIC requires no pointers beyond it.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut time) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let seconds =
        u64::try_from(time.tv_sec).map_err(|_| io::Error::other("negative monotonic clock"))?;
    let nanos =
        u64::try_from(time.tv_nsec).map_err(|_| io::Error::other("negative nanoseconds"))?;
    if nanos >= 1_000_000_000 {
        return Err(io::Error::other("invalid monotonic nanoseconds"));
    }
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|s| s.checked_add(nanos))
        .ok_or_else(|| io::Error::other("monotonic clock overflow"))
}
#[cfg(not(target_os = "linux"))]
fn monotonic_ns() -> io::Result<u64> {
    Err(io::Error::other("CPU profiling markers require Linux"))
}
