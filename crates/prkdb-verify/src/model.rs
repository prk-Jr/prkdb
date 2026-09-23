//! Pure reference model: what the database must contain after any sequence of
//! acknowledged operations. No I/O.

use std::collections::BTreeMap;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Model {
    pub kv: BTreeMap<Key, Value>,
}

impl Model {
    pub fn put(&mut self, k: Key, v: Value) {
        self.kv.insert(k, v);
    }
    pub fn delete(&mut self, k: &Key) {
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
}
