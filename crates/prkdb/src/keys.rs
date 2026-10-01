//! Key codec (spec §7 2c, KEY-01): every collection record's storage key is
//!
//! ```text
//! [ns_len: u8][ns: ns_len bytes][collection_id: u32 big-endian][key bytes]
//! ```
//!
//! `ns` is the namespace (`PrkDb::builder().with_namespace(..)`; empty for
//! `IndexedStorage` and `CollectionPartitionedAdapter`'s routing API). `collection_id` comes
//! from the persisted [`Catalog`](crate::catalog::Catalog), keyed by the collection's
//! persisted name, so two collections never share a key even when their ids are equal.
//! `key bytes` is the record's id as [`encode_id`] writes it (bincode, standard config), so
//! records of one collection sort by encoded id and a collection is one contiguous range,
//! found by [`collection_prefix`]. The id is big-endian for the same reason: id 7 and id
//! 70 must not share a prefix.
//!
//! [`SYSTEM_COLLECTION`] (id 0) is reserved for the catalog's own entries.
//!
//! # Raw system keyspaces (not collection records)
//!
//! These stay raw in Phase 2; each moves under [`SYSTEM_COLLECTION`] in the phase that next
//! changes it:
//!
//! - `__consumer_offset:{group}:{collection}:{partition}` (`consumer.rs`)
//! - `__ttl:` (`ttl.rs`)
//! - `meta:col:{name}` and `__replication:`, `__replication_nodes:`, `__replication_lag:`
//!   (`db.rs`)
//! - `__prkdb_metadata:authz:principal:` (`authz/store.rs`)
//! - `__raft_log/` (`storage/wal_adapter.rs`)
//! - the dead-letter-queue keys `{topic}:{partition}:{timestamp}` (`dlq.rs`); DLQ records
//!   are not collection records
//!
//! They cannot collide with a codec key in practice: a codec key starts with a namespace
//! length byte followed by that many namespace bytes and a 4-byte id, so a printable raw
//! key would have to be read as a namespace of 32 bytes or more.

use prkdb_types::error::StorageError;

/// A collection's id in the [`Catalog`](crate::catalog::Catalog).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CollectionId(pub u32);

/// Reserved for the catalog itself.
pub const SYSTEM_COLLECTION: CollectionId = CollectionId(0);

/// Longest namespace the codec can carry (its length is one byte).
pub const MAX_NAMESPACE_LEN: usize = u8::MAX as usize;

/// `[ns_len][ns][collection id BE][key]`. Fails only for a namespace longer than
/// [`MAX_NAMESPACE_LEN`] bytes.
pub fn encode_key(ns: &[u8], coll: CollectionId, key: &[u8]) -> Result<Vec<u8>, StorageError> {
    let ns_len = u8::try_from(ns.len()).map_err(|_| {
        StorageError::Validation(format!(
            "namespace of {} bytes exceeds the key codec's {MAX_NAMESPACE_LEN}-byte limit",
            ns.len()
        ))
    })?;
    let mut out = Vec::with_capacity(1 + ns.len() + 4 + key.len());
    out.push(ns_len);
    out.extend_from_slice(ns);
    out.extend_from_slice(&coll.0.to_be_bytes());
    out.extend_from_slice(key);
    Ok(out)
}

/// The prefix every key of `coll` in `ns` starts with, and no other key does. A namespace
/// longer than [`MAX_NAMESPACE_LEN`] bytes has no keys, so it gets a prefix that matches
/// none ([`encode_key`] refuses it).
pub fn collection_prefix(ns: &[u8], coll: CollectionId) -> Vec<u8> {
    match encode_key(ns, coll, &[]) {
        Ok(prefix) => prefix,
        // Length byte 255 followed by more than 255 bytes of namespace: never produced by
        // `encode_key`, so a scan over it is empty, which is the truth.
        Err(_) => {
            let mut prefix = vec![u8::MAX];
            prefix.extend_from_slice(ns);
            prefix
        }
    }
}

/// Splits a codec key into `(ns, collection id, key bytes)`. Fails if `bytes` is too short
/// to hold the namespace length it declares plus a collection id.
pub fn decode_key(bytes: &[u8]) -> Result<(&[u8], CollectionId, &[u8]), StorageError> {
    let not_a_key = || {
        StorageError::Validation(format!(
            "{} bytes are not a codec key ([ns_len][ns][collection id u32][key])",
            bytes.len()
        ))
    };
    let (&ns_len, rest) = bytes.split_first().ok_or_else(not_a_key)?;
    let ns_len = ns_len as usize;
    if rest.len() < ns_len + 4 {
        return Err(not_a_key());
    }
    let (ns, rest) = rest.split_at(ns_len);
    let (id, key) = rest.split_at(4);
    let id = u32::from_be_bytes([id[0], id[1], id[2], id[3]]);
    Ok((ns, CollectionId(id), key))
}

/// Primary-key bytes for an id: bincode, standard config (what `CollectionHandle` has
/// always used; `IndexedStorage` used JSON before Task 2.12, D12).
pub fn encode_id<I: serde::Serialize>(id: &I) -> Result<Vec<u8>, StorageError> {
    bincode::serde::encode_to_vec(id, bincode::config::standard())
        .map_err(|e| StorageError::Serialization(format!("Failed to serialize id: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_round_trips_with_and_without_a_namespace() {
        for ns in [&b""[..], b"tenant"] {
            let k = encode_key(ns, CollectionId(0x0102_0304), b"\x00:id").unwrap();
            assert_eq!(
                decode_key(&k).unwrap(),
                (ns, CollectionId(0x0102_0304), &b"\x00:id"[..])
            );
        }
    }

    #[test]
    fn short_bytes_are_not_a_key() {
        assert!(decode_key(b"").is_err());
        assert!(decode_key(&[0, 0, 0, 0]).is_err(), "three id bytes");
        assert!(
            decode_key(&[3, b'a', b'b', 0, 0, 0, 1]).is_err(),
            "ns cut short"
        );
        assert!(
            decode_key(&[0, 0, 0, 0, 1]).is_ok(),
            "empty key bytes are fine"
        );
    }

    #[test]
    fn prefixes_separate_namespaces_and_collections() {
        let k = encode_key(b"a", CollectionId(1), b"x").unwrap();
        assert!(k.starts_with(&collection_prefix(b"a", CollectionId(1))));
        assert!(!k.starts_with(&collection_prefix(b"", CollectionId(1))));
        assert!(!k.starts_with(&collection_prefix(b"ab", CollectionId(1))));
        assert!(!k.starts_with(&collection_prefix(b"a", CollectionId(256))));
        let long = [b'n'; 256];
        assert!(!k.starts_with(&collection_prefix(&long, CollectionId(1))));
    }

    #[test]
    fn ids_are_bincode() {
        assert_eq!(encode_id(&7u64).unwrap(), vec![7]);
        assert_eq!(encode_id(&"ab").unwrap(), vec![2, b'a', b'b']);
    }
}
