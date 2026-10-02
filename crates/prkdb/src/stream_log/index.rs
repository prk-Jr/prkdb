//! The in-memory sparse frame index; rebuilt by replay and extended before each ack.

use prkdb_core::wal::{Lsn, RecordLoc};
use std::collections::BTreeMap;

const STRIDE: u64 = 64 * 1024;

#[derive(Default)]
struct SegmentIndex {
    entries: Vec<(RecordLoc, i64)>,
    max_time: i64,
    min_time: i64,
    previous_max_time: Option<i64>,
    last_lsn: Lsn,
}

#[derive(Default)]
pub(super) struct SparseIndex {
    segments: BTreeMap<Lsn, SegmentIndex>,
}

impl SparseIndex {
    pub fn insert(&mut self, loc: RecordLoc, time: i64) {
        let segment = self
            .segments
            .entry(loc.segment)
            .or_insert_with(|| SegmentIndex {
                entries: Vec::new(),
                max_time: time,
                min_time: time,
                previous_max_time: None,
                last_lsn: 0,
            });
        if segment.last_lsn != 0 {
            segment.previous_max_time = Some(segment.max_time);
        }
        segment.max_time = segment.max_time.max(time);
        segment.min_time = segment.min_time.min(time);
        segment.last_lsn = loc.lsn;
        if segment
            .entries
            .last()
            .is_none_or(|(previous, _)| loc.offset - previous.offset >= STRIDE)
        {
            segment.entries.push((loc, time));
        }
    }

    pub fn max_time(&self, segment: Lsn) -> Option<i64> {
        self.segments.get(&segment).map(|s| s.max_time)
    }

    pub fn min_time(&self, segment: Lsn) -> Option<i64> {
        self.segments.get(&segment).map(|s| s.min_time)
    }

    pub fn remove_before(&mut self, floor: Lsn) {
        self.segments = self.segments.split_off(&floor);
    }

    pub fn seek(&self, lsn: Lsn) -> Option<RecordLoc> {
        let (_, segment) = self.segments.range(..=lsn).next_back()?;
        let end = segment.entries.partition_point(|(loc, _)| loc.lsn <= lsn);
        end.checked_sub(1).map(|i| segment.entries[i].0)
    }

    // Callers hold the index read lock while sampling acked_lsn. The writer invokes
    // one hook immediately before each ack, so at most the latest frame is unacked.
    // Keep its prior maximum to avoid seeking on that frame's future timestamp.
    pub fn timestamp(&self, ts: i64, floor: Lsn, cap: Lsn) -> Option<Lsn> {
        if floor > cap {
            return None;
        }
        self.segments.range(floor..=cap).find_map(|(_, segment)| {
            let max_time = if segment.last_lsn <= cap {
                Some(segment.max_time)
            } else {
                segment.previous_max_time
            };
            max_time
                .filter(|time| *time >= ts)
                .map(|_| segment.entries[0].0.lsn)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_excludes_the_hook_frame_before_its_ack() {
        let mut index = SparseIndex::default();
        let loc = |lsn, offset| RecordLoc {
            lsn,
            segment: 1,
            offset,
            payload_len: 10,
        };
        index.insert(loc(1, 16), 1000);
        index.insert(loc(2, 43), 2000);
        assert_eq!(index.timestamp(1500, 1, 1), None);
        assert_eq!(index.timestamp(1000, 1, 1), Some(1));
        assert_eq!(index.timestamp(1500, 1, 2), Some(1));
    }
}
