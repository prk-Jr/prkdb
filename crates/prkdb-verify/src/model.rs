//! Pure reference model: what the database must contain after any sequence of
//! acknowledged operations. No I/O.

use std::collections::{BTreeMap, BTreeSet};

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Model {
    pub kv: BTreeMap<Key, Value>,
    /// Every key ever put or deleted, independent of the generator's key
    /// space. The checker verifies the union of this set and the generator's
    /// full key space, so widening the generator (or a bug that touches a key
    /// outside it) can never silently shrink what gets checked.
    pub touched: BTreeSet<Key>,
}

impl Model {
    pub fn put(&mut self, k: Key, v: Value) {
        self.touched.insert(k.clone());
        self.kv.insert(k, v);
    }
    pub fn delete(&mut self, k: &Key) {
        self.touched.insert(k.clone());
        self.kv.remove(k);
    }
    pub fn get(&self, k: &Key) -> Option<&Value> {
        self.kv.get(k)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_overwrite_delete() {
        let mut m = Model::default();
        m.put(b"a".to_vec(), b"1".to_vec());
        m.put(b"a".to_vec(), b"2".to_vec());
        assert_eq!(m.get(&b"a".to_vec()), Some(&b"2".to_vec()));
        m.delete(&b"a".to_vec());
        assert!(m.kv.is_empty());
    }

    #[test]
    fn touched_survives_delete() {
        let mut m = Model::default();
        m.put(b"a".to_vec(), b"1".to_vec());
        m.delete(&b"a".to_vec());
        assert!(m.touched.contains(b"a".as_slice()));
        assert!(!m.kv.contains_key(b"a".as_slice()));
    }
}
