//! Event and stream-record identity (Task 2.20's `EventSeq`, created early by Task
//! 2.15b.2 so events and stream records share one offset type).

use std::fmt;

/// The first WAL LSN that has no [`EventSeq`]: 2⁴⁸, since the low 16 bits hold the
/// item's index in its frame.
pub const WAL_LSN_LIMIT: u64 = 1 << 48;

/// An opaque, ordered sequence number: an event's id suffix in the outbox (Task 2.20) and
/// a stream record's offset (Task 2.15b).
///
/// On the single WAL it packs the frame's LSN and the item's index within the frame:
/// `lsn << 16 | index` ([`EventSeq::from_wal`]). A frame carries at most 65,536 items,
/// and LSNs must stay below 2⁴⁸ ([`WAL_LSN_LIMIT`]); callers refuse an append past either
/// limit before it is written, with [`EventSeq::try_from_wal`]. Other adapters assign
/// their own values ([`EventSeq::from_raw`]), so code
/// outside an adapter treats the value as opaque: ordered, unique and stable, not dense.
///
/// `Display` is the value as 20 zero-padded decimal digits (the width of `u64::MAX`), so
/// the string order of two sequences is their numeric order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventSeq(u64);

impl EventSeq {
    /// A sequence from an adapter's own counter.
    pub const fn from_raw(raw: u64) -> Self {
        EventSeq(raw)
    }

    /// The packed value, for adapters and for persisting a position.
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// The sequence of item `index_in_frame` of the WAL frame at `lsn`: `lsn << 16 |
    /// index_in_frame`. `lsn` must be below 2⁴⁸ (checked in debug builds; release builds
    /// rely on the caller's check, which refuses the append first).
    pub const fn from_wal(lsn: u64, index_in_frame: u16) -> Self {
        debug_assert!(
            lsn < 1 << 48,
            "LSN at or above 2^48 does not fit an EventSeq"
        );
        EventSeq(lsn << 16 | index_in_frame as u64)
    }

    /// [`EventSeq::from_wal`], or `None` when `lsn` is at or above [`WAL_LSN_LIMIT`] and
    /// so has no sequence. The append paths use it to refuse such an append before it is
    /// written (2.15b.2 review).
    pub const fn try_from_wal(lsn: u64, index_in_frame: u16) -> Option<Self> {
        if lsn >= WAL_LSN_LIMIT {
            None
        } else {
            Some(EventSeq(lsn << 16 | index_in_frame as u64))
        }
    }
}

impl fmt::Display for EventSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:020}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_wal_packs_lsn_above_the_index() {
        assert_eq!(EventSeq::from_wal(1, 0).raw(), 1 << 16);
        assert_eq!(EventSeq::from_wal(5, 2).raw(), 5 << 16 | 2);
        assert_eq!(
            EventSeq::from_wal((1 << 48) - 1, u16::MAX).raw(),
            u64::MAX,
            "the largest LSN and index fill the u64 exactly"
        );
    }

    /// The checked form refuses an LSN at or above 2⁴⁸ instead of shifting its top bits
    /// away (2.15b.2 review); below it, it agrees with `from_wal`.
    #[test]
    fn try_from_wal_refuses_an_lsn_at_or_above_2_pow_48() {
        assert_eq!(WAL_LSN_LIMIT, 1 << 48);
        for (lsn, idx) in [(0, 0), (1, 2), ((1 << 48) - 1, u16::MAX)] {
            assert_eq!(
                EventSeq::try_from_wal(lsn, idx),
                Some(EventSeq::from_wal(lsn, idx))
            );
        }
        for lsn in [1 << 48, (1 << 48) + 1, u64::MAX] {
            assert_eq!(EventSeq::try_from_wal(lsn, 0), None, "{lsn}");
        }
    }

    #[test]
    fn raw_round_trips() {
        for raw in [0, 1, 65_535, 65_536, u64::MAX] {
            assert_eq!(EventSeq::from_raw(raw).raw(), raw);
        }
    }

    /// Order is (lsn, index): every item of a frame sorts before the next frame's first.
    #[test]
    fn order_is_lsn_then_index() {
        let seqs = [
            EventSeq::from_wal(1, 0),
            EventSeq::from_wal(1, 1),
            EventSeq::from_wal(1, u16::MAX),
            EventSeq::from_wal(2, 0),
            EventSeq::from_wal(1 << 40, 7),
        ];
        assert!(seqs.windows(2).all(|w| w[0] < w[1]), "{seqs:?}");
        // "Next after the last item of a frame" carries into the next LSN.
        assert_eq!(
            EventSeq::from_raw(EventSeq::from_wal(1, u16::MAX).raw() + 1),
            EventSeq::from_wal(2, 0)
        );
    }

    #[test]
    fn display_is_20_zero_padded_digits_in_numeric_order() {
        assert_eq!(EventSeq::from_raw(0).to_string(), "00000000000000000000");
        assert_eq!(EventSeq::from_wal(1, 2).to_string(), "00000000000000065538");
        assert_eq!(
            EventSeq::from_raw(u64::MAX).to_string(),
            "18446744073709551615"
        );
        let values = [0u64, 9, 10, 65_535, 65_536, 1 << 40, u64::MAX];
        let strings: Vec<String> = values
            .iter()
            .map(|&v| EventSeq::from_raw(v).to_string())
            .collect();
        assert!(strings.iter().all(|s| s.len() == 20), "{strings:?}");
        assert!(strings.windows(2).all(|w| w[0] < w[1]), "{strings:?}");
    }
}
