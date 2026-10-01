//! Pure reference model: what the database may contain after any sequence of
//! acknowledged operations. No I/O.
//!
//! The model answers "which states are acceptable", not "which single state":
//! it keeps the state the SUT certainly made durable plus the ordered list of
//! acknowledged mutations since then. A WAL can only lose a suffix, so every
//! *prefix* of that list applied to the durable state is a candidate (spec §7
//! checker: "SUT state equals the model at some prefix no earlier than the
//! last completed sync").

use std::collections::{BTreeMap, BTreeSet};

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

/// Durability mode of a run. In `Durable` mode every acknowledged write is
/// durable, so the full state is the only candidate after any restart. In
/// `Fast` mode acknowledged-but-unsynced writes may or may not survive a power
/// loss (Task 2.10b).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Durable,
    Fast,
}

impl Mode {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "durable" => Some(Self::Durable),
            "fast" => Some(Self::Fast),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Durable => "durable",
            Self::Fast => "fast",
        }
    }
}

/// One acknowledged mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mutation {
    Put(Key, Value),
    Delete(Key),
}

impl Mutation {
    fn apply(&self, kv: &mut BTreeMap<Key, Value>) {
        match self {
            Mutation::Put(k, v) => {
                kv.insert(k.clone(), v.clone());
            }
            Mutation::Delete(k) => {
                kv.remove(k);
            }
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Model {
    /// State as of the last point the SUT certainly made durable.
    pub durable: BTreeMap<Key, Value>,
    /// Acknowledged mutations since then, oldest first.
    pub pending: Vec<Mutation>,
    /// Every key ever put or deleted, independent of the generator's key
    /// space. The checker verifies the union of this set and the generator's
    /// full key space, so widening the generator (or a bug that touches a key
    /// outside it) can never silently shrink what gets checked.
    pub touched: BTreeSet<Key>,
}

impl Model {
    pub fn put(&mut self, k: Key, v: Value) {
        self.touched.insert(k.clone());
        self.pending.push(Mutation::Put(k, v));
    }

    pub fn delete(&mut self, k: &Key) {
        self.touched.insert(k.clone());
        self.pending.push(Mutation::Delete(k.clone()));
    }

    /// The state if every acknowledged mutation survived.
    pub fn state(&self) -> BTreeMap<Key, Value> {
        self.prefix(self.pending.len())
    }

    /// The value of `k` if every acknowledged mutation survived.
    pub fn get(&self, k: &Key) -> Option<Value> {
        let mut value = self.durable.get(k).cloned();
        for m in &self.pending {
            match m {
                Mutation::Put(pk, v) if pk == k => value = Some(v.clone()),
                Mutation::Delete(dk) if dk == k => value = None,
                _ => {}
            }
        }
        value
    }

    /// `durable` with the first `n` pending mutations applied, for `n` in
    /// `0..=pending.len()`.
    ///
    /// # Panics
    /// If `n > pending.len()`: asking for a prefix that was never
    /// acknowledged is a harness bug.
    pub fn prefix(&self, n: usize) -> BTreeMap<Key, Value> {
        let mut kv = self.durable.clone();
        for m in &self.pending[..n] {
            m.apply(&mut kv);
        }
        kv
    }

    /// A clean reopen, flush or checkpoint happened: everything pending is
    /// durable.
    pub fn mark_durable(&mut self) {
        self.settle(self.pending.len());
    }

    /// The SUT reports that the first `n` pending mutations are durable (its
    /// own sync points: a segment roll, a close, an open). They move into
    /// `durable`; the rest stay pending. `n` is clamped to `pending.len()`.
    pub fn mark_durable_prefix(&mut self, n: usize) {
        let n = n.min(self.pending.len());
        for m in self.pending.drain(..n) {
            m.apply(&mut self.durable);
        }
    }

    /// Power loss kept exactly the first `n` pending mutations; the rest are
    /// gone and nothing is pending any more.
    ///
    /// # Panics
    /// If `n > pending.len()`.
    pub fn settle(&mut self, n: usize) {
        for m in &self.pending[..n] {
            m.apply(&mut self.durable);
        }
        self.pending.clear();
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
        assert_eq!(m.get(&b"a".to_vec()), Some(b"2".to_vec()));
        m.delete(&b"a".to_vec());
        assert!(m.state().is_empty());
        assert_eq!(m.get(&b"a".to_vec()), None);
    }

    #[test]
    fn touched_survives_delete() {
        let mut m = Model::default();
        m.put(b"a".to_vec(), b"1".to_vec());
        m.delete(&b"a".to_vec());
        assert!(m.touched.contains(b"a".as_slice()));
        assert!(!m.state().contains_key(b"a".as_slice()));
    }

    #[test]
    fn prefix_zero_is_the_durable_state() {
        let mut m = Model::default();
        m.put(b"a".to_vec(), b"1".to_vec());
        m.mark_durable();
        m.put(b"b".to_vec(), b"2".to_vec());
        let p0 = m.prefix(0);
        assert_eq!(p0.len(), 1);
        assert_eq!(p0.get(b"a".as_slice()), Some(&b"1".to_vec()));
        let full = m.state();
        assert_eq!(full.len(), 2);
        assert_eq!(full.get(b"b".as_slice()), Some(&b"2".to_vec()));
    }

    #[test]
    fn settle_keeps_exactly_n_pending() {
        let mut m = Model::default();
        m.put(b"a".to_vec(), b"1".to_vec());
        m.put(b"b".to_vec(), b"2".to_vec());
        m.put(b"c".to_vec(), b"3".to_vec());
        m.settle(1);
        assert!(m.pending.is_empty());
        let s = m.state();
        assert_eq!(s.len(), 1);
        assert_eq!(s.get(b"a".as_slice()), Some(&b"1".to_vec()));
    }

    #[test]
    fn mark_durable_prefix_keeps_the_rest_pending() {
        let mut m = Model::default();
        m.put(b"a".to_vec(), b"1".to_vec());
        m.put(b"b".to_vec(), b"2".to_vec());
        m.put(b"c".to_vec(), b"3".to_vec());
        let before = m.state();
        m.mark_durable_prefix(2);
        assert_eq!(m.pending.len(), 1);
        assert_eq!(m.state(), before);
        assert_eq!(m.prefix(0).len(), 2);
        m.mark_durable_prefix(10);
        assert!(m.pending.is_empty());
        assert_eq!(m.durable, before);
    }

    #[test]
    fn get_agrees_with_state() {
        let mut m = Model::default();
        m.put(b"a".to_vec(), b"1".to_vec());
        m.mark_durable();
        m.delete(&b"a".to_vec());
        m.put(b"b".to_vec(), b"2".to_vec());
        let s = m.state();
        for k in [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()] {
            assert_eq!(m.get(&k), s.get(&k).cloned());
        }
    }
}
