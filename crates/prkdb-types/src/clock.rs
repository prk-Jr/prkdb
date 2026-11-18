//! Millisecond wall-clock timestamps shared by stored records and health reports.

/// Current Unix wall-clock time in milliseconds, for persisted timestamps and lag reports.
pub fn unix_millis() -> i64 {
    timestamp_millis(time::OffsetDateTime::now_utc())
}

fn timestamp_millis(timestamp: time::OffsetDateTime) -> i64 {
    // Floor negative instants as chrono did; integer division would truncate toward zero.
    timestamp.unix_timestamp_nanos().div_euclid(1_000_000) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn timestamps_use_milliseconds_and_floor_before_the_epoch() {
        for (nanos, millis) in [
            (0, 0),
            (1_234_567_890, 1234),
            (1_700_000_000_123_456_789, 1_700_000_000_123),
            (-1, -1),
            (-1_000_001, -2),
            (-1_234_567_890, -1235),
        ] {
            let timestamp = time::OffsetDateTime::from_unix_timestamp_nanos(nanos).unwrap();
            assert_eq!(timestamp_millis(timestamp), millis);
        }
    }

    #[test]
    fn current_timestamp_matches_the_system_clock_in_milliseconds() {
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let actual = unix_millis();
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        assert!(
            (before..=after).contains(&actual),
            "{actual} outside {before}..={after}"
        );
    }
}
