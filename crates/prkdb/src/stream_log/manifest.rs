//! Versioned, checksummed codec for a partitioned stream's `STREAM` manifest.

pub const STREAM_MANIFEST_VERSION: u32 = 1;

const MAGIC: &[u8; 8] = b"PRKSTRM\0";
const HEADER_LEN: usize = 20;
const CHECKSUM_LEN: usize = 4;
const MAX_MANIFEST_BYTES: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamManifest {
    pub partitions: u32,
    pub created_by: String,
}

impl StreamManifest {
    pub fn current(partitions: u32) -> Self {
        Self {
            partitions,
            created_by: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, String> {
        if self.partitions == 0 {
            return Err("STREAM manifest must contain at least one partition".into());
        }
        let name = self.created_by.as_bytes();
        if name.len() > MAX_MANIFEST_BYTES - HEADER_LEN - CHECKSUM_LEN {
            return Err("STREAM manifest exceeds 4096 bytes".into());
        }
        let mut bytes = Vec::with_capacity(HEADER_LEN + name.len() + CHECKSUM_LEN);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&STREAM_MANIFEST_VERSION.to_le_bytes());
        bytes.extend_from_slice(&self.partitions.to_le_bytes());
        bytes.extend_from_slice(&(name.len() as u32).to_le_bytes());
        bytes.extend_from_slice(name);
        let checksum = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if !(HEADER_LEN + CHECKSUM_LEN..=MAX_MANIFEST_BYTES).contains(&bytes.len()) {
            return Err("STREAM manifest length must be between 24 and 4096 bytes".into());
        }
        let crc_at = bytes.len() - CHECKSUM_LEN;
        let stored_crc = u32::from_le_bytes(bytes[crc_at..].try_into().unwrap());
        if crc32fast::hash(&bytes[..crc_at]) != stored_crc {
            return Err("STREAM manifest checksum mismatch".into());
        }
        if &bytes[..MAGIC.len()] != MAGIC {
            return Err("invalid STREAM manifest magic".into());
        }
        let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        if version != STREAM_MANIFEST_VERSION {
            return Err(format!("unsupported STREAM manifest version {version}"));
        }
        let partitions = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
        if partitions == 0 {
            return Err("STREAM manifest must contain at least one partition".into());
        }
        let name_len = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
        if name_len as u64 != (crc_at - HEADER_LEN) as u64 {
            return Err("STREAM manifest created_by length does not match file length".into());
        }
        let created_by = std::str::from_utf8(&bytes[HEADER_LEN..crc_at])
            .map_err(|_| "STREAM manifest created_by is not UTF-8")?
            .to_string();
        Ok(Self {
            partitions,
            created_by,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> StreamManifest {
        StreamManifest {
            partitions: 3,
            created_by: "0.6.0".into(),
        }
    }

    fn replace_u32(bytes: &mut [u8], offset: usize, value: u32) {
        bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
        let crc_at = bytes.len() - 4;
        let crc = crc32fast::hash(&bytes[..crc_at]);
        bytes[crc_at..].copy_from_slice(&crc.to_le_bytes());
    }

    #[test]
    fn round_trip_and_golden_bytes() {
        let manifest = fixture();
        let bytes = manifest.encode().unwrap();
        assert_eq!(bytes, GOLDEN);
        assert_eq!(StreamManifest::decode(&bytes).unwrap(), manifest);
    }

    #[test]
    fn every_single_bit_flip_is_refused() {
        for byte in 0..GOLDEN.len() {
            for bit in 0..8 {
                let mut bytes = GOLDEN.to_vec();
                bytes[byte] ^= 1 << bit;
                assert!(
                    StreamManifest::decode(&bytes).is_err(),
                    "byte {byte}, bit {bit}"
                );
            }
        }
    }

    #[test]
    fn every_truncation_and_trailing_bytes_are_refused() {
        for len in 0..GOLDEN.len() {
            assert!(
                StreamManifest::decode(&GOLDEN[..len]).is_err(),
                "length {len}"
            );
        }
        let mut bytes = GOLDEN.to_vec();
        bytes.push(0);
        assert!(StreamManifest::decode(&bytes).is_err());
        let crc = crc32fast::hash(&bytes[..bytes.len() - 4]);
        let crc_at = bytes.len() - 4;
        bytes[crc_at..].copy_from_slice(&crc.to_le_bytes());
        assert!(StreamManifest::decode(&bytes).is_err());
    }

    #[test]
    fn unknown_version_is_named_after_crc_validation() {
        let mut bytes = GOLDEN.to_vec();
        replace_u32(&mut bytes, 8, 2);
        let error = StreamManifest::decode(&bytes).unwrap_err();
        assert!(error.contains("version 2"), "{error}");
        bytes[8] ^= 1;
        let error = StreamManifest::decode(&bytes).unwrap_err();
        assert!(error.contains("checksum"), "{error}");
    }

    #[test]
    fn zero_partitions_are_refused_at_encode_and_decode() {
        let mut manifest = fixture();
        manifest.partitions = 0;
        assert!(manifest.encode().is_err());
        let mut bytes = GOLDEN.to_vec();
        replace_u32(&mut bytes, 12, 0);
        assert!(StreamManifest::decode(&bytes).is_err());
    }

    #[test]
    fn lengths_are_bounded_and_exact() {
        let manifest = StreamManifest {
            partitions: 1,
            created_by: "x".repeat(4096 - 24),
        };
        let encoded = manifest.encode().unwrap();
        assert_eq!(encoded.len(), 4096);
        assert_eq!(StreamManifest::decode(&encoded).unwrap(), manifest);
        let too_large = StreamManifest {
            created_by: "x".repeat(4096 - 23),
            ..manifest
        };
        assert!(too_large.encode().is_err());
        assert!(StreamManifest::decode(&vec![0; 4097]).is_err());
        let mut bytes = GOLDEN.to_vec();
        replace_u32(&mut bytes, 16, u32::MAX);
        assert!(StreamManifest::decode(&bytes).is_err());
    }

    #[test]
    fn crc_valid_invalid_magic_and_utf8_are_refused() {
        for offset in [0, 20] {
            let mut bytes = GOLDEN.to_vec();
            bytes[offset] = 0xff;
            replace_u32(&mut bytes, 12, 3);
            assert!(StreamManifest::decode(&bytes).is_err());
        }
    }

    #[test]
    fn current_names_the_creating_build() {
        let manifest = StreamManifest::current(2);
        assert_eq!(manifest.partitions, 2);
        assert_eq!(manifest.created_by, env!("CARGO_PKG_VERSION"));
    }

    const GOLDEN: &[u8] = &[
        80, 82, 75, 83, 84, 82, 77, 0, 1, 0, 0, 0, 3, 0, 0, 0, 5, 0, 0, 0, 48, 46, 54, 46, 48, 151,
        36, 14, 247,
    ];
}
