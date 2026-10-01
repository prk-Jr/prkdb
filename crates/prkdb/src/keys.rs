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
//! A collection is one contiguous key range, found by [`collection_prefix`]: the
//! collection id's **fixed width** makes the prefix unambiguous (no id's bytes are a
//! prefix of another's, so id 7 and id 70 never share one), and its **big-endian** order
//! sorts collections by id.
//!
//! `key bytes` is the record's id as [`encode_id`] writes it: the memcomparable format
//! (the `memcomparable` crate, the MyRocks key format), which is **order-preserving**,
//! so a collection's keys sort exactly as its ids do (`u64` 100 < 250 < 300, `i64` by
//! sign, strings and byte sequences lexicographically, tuples and structs field by
//! field) and a key range is an id range. The encoding is self-delimiting, so a composite
//! id's fields cannot run into each other. Maps are not supported as ids.
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

/// Primary-key bytes for an id: the order-preserving memcomparable encoding (module
/// docs). `IndexedStorage` used JSON and `CollectionHandle` bincode before Task 2.12
/// (D12); neither sorted by id.
pub fn encode_id<I: serde::Serialize>(id: &I) -> Result<Vec<u8>, StorageError> {
    write_id(Vec::with_capacity(ID_CAPACITY), id)
}

/// The stored key of record `id` in collection `coll` of namespace `ns`:
/// `encode_key(ns, coll, &encode_id(id)?)` in one allocation, which is the hot path for
/// every typed put and get.
pub fn encode_record_key<I: serde::Serialize>(
    ns: &[u8],
    coll: CollectionId,
    id: &I,
) -> Result<Vec<u8>, StorageError> {
    let ns_len = u8::try_from(ns.len()).map_err(|_| {
        StorageError::Validation(format!(
            "namespace of {} bytes exceeds the key codec's {MAX_NAMESPACE_LEN}-byte limit",
            ns.len()
        ))
    })?;
    let mut out = Vec::with_capacity(1 + ns.len() + 4 + ID_CAPACITY);
    out.push(ns_len);
    out.extend_from_slice(ns);
    out.extend_from_slice(&coll.0.to_be_bytes());
    write_id(out, id)
}

/// Room for the common ids (integers, short strings, UUIDs) without a reallocation.
const ID_CAPACITY: usize = 64;

fn write_id<I: serde::Serialize>(out: Vec<u8>, id: &I) -> Result<Vec<u8>, StorageError> {
    let mut ser = memcomparable::Serializer::new(out);
    id.serialize(&mut ser)
        .map_err(|e| StorageError::Serialization(format!("Failed to serialize id: {e}")))?;
    Ok(ser.into_inner())
}

/// A printable form of an encoded id, for tools that do not know the id's type (the CLI
/// and HTTP server): a string id as itself, an 8-byte id as an unsigned integer, anything
/// else `None`.
pub fn decode_id_hint(id: &[u8]) -> Option<String> {
    if let Ok(s) = memcomparable::from_slice::<String>(id) {
        return Some(s);
    }
    if id.len() == 8 {
        return memcomparable::from_slice::<u64>(id)
            .ok()
            .map(|n| n.to_string());
    }
    None
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
    fn ids_are_memcomparable() {
        assert_eq!(encode_id(&7u64).unwrap(), 7u64.to_be_bytes());
        assert_eq!(
            encode_id(&-1i64).unwrap(),
            0x7fff_ffff_ffff_ffffu64.to_be_bytes(),
            "the sign bit is flipped so negatives sort first"
        );
        assert_eq!(
            encode_id(&"ab").unwrap(),
            [1, b'a', b'b', 0, 0, 0, 0, 0, 0, 2],
            "8-byte groups, each followed by its significant length"
        );
    }

    #[test]
    fn a_record_key_is_the_key_of_the_encoded_id() {
        for ns in [&b""[..], b"tenant"] {
            assert_eq!(
                encode_record_key(ns, CollectionId(9), &"user-1").unwrap(),
                encode_key(ns, CollectionId(9), &encode_id(&"user-1").unwrap()).unwrap()
            );
        }
        assert!(encode_record_key(&[0u8; 256], CollectionId(1), &1u64).is_err());
    }

    #[test]
    fn id_hints_print_strings_and_integers() {
        assert_eq!(
            decode_id_hint(&encode_id(&"user-1").unwrap()).as_deref(),
            Some("user-1")
        );
        assert_eq!(
            decode_id_hint(&encode_id(&42u64).unwrap()).as_deref(),
            Some("42")
        );
        assert_eq!(decode_id_hint(&[1, 2, 3]), None);
    }

    #[test]
    fn maps_are_refused_as_ids() {
        let mut map = std::collections::BTreeMap::new();
        map.insert(1u8, 2u8);
        assert!(encode_id(&map).is_err());
    }
}
