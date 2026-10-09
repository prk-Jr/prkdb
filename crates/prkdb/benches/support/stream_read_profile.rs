//! Expected input verification for optional Linux benchmark profiling.
use prkdb::stream_log::{AppendAck, EventSeq, ReadBatch};

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
    pub fn verify_page(
        &self,
        _consumed: usize,
        _from: EventSeq,
        _page: &ReadBatch,
    ) -> Result<usize, String> {
        Ok(0)
    }
}
