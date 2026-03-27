use bincode::{config, serde::decode_from_slice};

pub const INTERNAL_METADATA_PREFIX: &str = "__prkdb_metadata:";
pub const COLLECTION_METADATA_PREFIX: &str = "meta:col:";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedStorageKey {
    InternalMetadata,
    CollectionMetadata {
        name: String,
    },
    Data {
        raw_collection: String,
        logical_collection: String,
        partition: Option<u32>,
        id_hint: Option<String>,
    },
    Unknown,
}

pub fn parse_storage_key(key: &[u8]) -> ParsedStorageKey {
    if key.starts_with(INTERNAL_METADATA_PREFIX.as_bytes()) {
        return ParsedStorageKey::InternalMetadata;
    }

    if let Ok(key_str) = std::str::from_utf8(key) {
        if let Some(name) = key_str.strip_prefix(COLLECTION_METADATA_PREFIX) {
            return ParsedStorageKey::CollectionMetadata {
                name: name.to_string(),
            };
        }
    }

    let Some(last_delimiter) = key.iter().rposition(|byte| *byte == b':') else {
        return ParsedStorageKey::Unknown;
    };

    let prefix_bytes = &key[..last_delimiter];
    let id_bytes = &key[last_delimiter + 1..];
    let prefix = String::from_utf8_lossy(prefix_bytes).to_string();
    if prefix.is_empty() {
        return ParsedStorageKey::Unknown;
    }

    let (raw_collection, partition) = split_partition_suffix(&prefix);
    let logical_collection = raw_collection
        .rsplit("::")
        .next()
        .unwrap_or(&raw_collection)
        .to_string();

    ParsedStorageKey::Data {
        raw_collection,
        logical_collection,
        partition,
        id_hint: decode_id_hint(id_bytes),
    }
}

pub fn logical_collection_name(key: &[u8]) -> Option<String> {
    match parse_storage_key(key) {
        ParsedStorageKey::CollectionMetadata { name } => Some(name),
        ParsedStorageKey::Data {
            logical_collection, ..
        } => Some(logical_collection),
        ParsedStorageKey::InternalMetadata | ParsedStorageKey::Unknown => None,
    }
}

pub fn is_internal_metadata_key(key: &[u8]) -> bool {
    matches!(parse_storage_key(key), ParsedStorageKey::InternalMetadata)
}

fn split_partition_suffix(prefix: &str) -> (String, Option<u32>) {
    let Some((raw_collection, suffix)) = prefix.rsplit_once(':') else {
        return (prefix.to_string(), None);
    };

    if raw_collection.ends_with(':') || !suffix.chars().all(|ch| ch.is_ascii_digit()) {
        return (prefix.to_string(), None);
    }

    match suffix.parse::<u32>() {
        Ok(partition) => (raw_collection.to_string(), Some(partition)),
        Err(_) => (prefix.to_string(), None),
    }
}

fn decode_id_hint(id_bytes: &[u8]) -> Option<String> {
    if let Ok(id) = std::str::from_utf8(id_bytes) {
        if !id.is_empty() && id.chars().all(|ch| !ch.is_control()) {
            return Some(id.to_string());
        }
    }

    try_decode_common_id::<String>(id_bytes)
        .or_else(|| try_decode_common_id::<u64>(id_bytes))
        .or_else(|| try_decode_common_id::<u32>(id_bytes))
        .or_else(|| try_decode_common_id::<i64>(id_bytes))
        .or_else(|| try_decode_common_id::<i32>(id_bytes))
}

fn try_decode_common_id<T>(id_bytes: &[u8]) -> Option<String>
where
    T: serde::de::DeserializeOwned + ToString,
{
    decode_from_slice::<T, _>(id_bytes, config::standard())
        .ok()
        .map(|(value, _)| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::{
        logical_collection_name, parse_storage_key, ParsedStorageKey, COLLECTION_METADATA_PREFIX,
        INTERNAL_METADATA_PREFIX,
    };

    #[test]
    fn parses_internal_metadata_keys() {
        assert!(matches!(
            parse_storage_key(format!("{INTERNAL_METADATA_PREFIX}startup").as_bytes()),
            ParsedStorageKey::InternalMetadata
        ));
    }

    #[test]
    fn parses_collection_metadata_keys() {
        assert_eq!(
            parse_storage_key(format!("{COLLECTION_METADATA_PREFIX}users").as_bytes()),
            ParsedStorageKey::CollectionMetadata {
                name: "users".to_string()
            }
        );
    }

    #[test]
    fn parses_typed_collection_keys() {
        let mut key = b"my_app::User:".to_vec();
        key.extend_from_slice(
            &bincode::serde::encode_to_vec("1".to_string(), bincode::config::standard()).unwrap(),
        );

        match parse_storage_key(&key) {
            ParsedStorageKey::Data {
                raw_collection,
                logical_collection,
                partition,
                id_hint,
            } => {
                assert_eq!(raw_collection, "my_app::User");
                assert_eq!(logical_collection, "User");
                assert_eq!(partition, None);
                assert_eq!(id_hint.as_deref(), Some("1"));
            }
            other => panic!("unexpected key parse: {other:?}"),
        }

        assert_eq!(logical_collection_name(&key).as_deref(), Some("User"));
    }
}
