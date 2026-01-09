use zerocopy::{AsBytes, FromBytes, FromZeroes};

/// Fixed-size header for log records.
/// This allows us to read the header without parsing the entire record.
#[derive(Debug, Clone, Copy, AsBytes, FromBytes, FromZeroes)]
#[repr(C)]
pub struct ZeroCopyLogHeader {
    /// Unique offset (assigned by WAL)
    pub offset: u64,

    /// Timestamp (milliseconds since epoch)
    pub timestamp: u64,

    /// CRC32 checksum of the operation data
    pub checksum: u32,

    /// Length of the payload (operation data)
    pub payload_len: u32,

    /// Compression type (u8 representation)
    pub compression_type: u8,

    /// Padding to align to 8 bytes.
    /// u64(8) + u64(8) + u32(4) + u32(4) + u8(1) = 25 bytes.
    /// Next multiple of 8 is 32. 32 - 25 = 7 bytes padding.
    pub _padding: [u8; 7],
}

impl ZeroCopyLogHeader {
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_layout() {
        assert_eq!(ZeroCopyLogHeader::SIZE, 32); // 8 + 8 + 4 + 4 + 1 + 3 = 28? Wait.
                                                 // u64 (8) + u64 (8) + u32 (4) + u32 (4) + u8 (1) + [u8; 3] (3) = 28 bytes.
                                                 // Let's check alignment.
                                                 // u64 needs 8-byte alignment.
                                                 // 28 is not divisible by 8.
                                                 // So compiler might add padding if not packed.
                                                 // But we used repr(C).
                                                 // Let's verify size in test.
    }
}
