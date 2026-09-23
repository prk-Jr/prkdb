//! Operation vocabulary and profiles (spec §7.1). The generator draws only ops
//! enabled by the selected profile. Seeds are stable: ChaCha8 is portable.

use crate::model::{Key, Value};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Put(Key, Value),
    Delete(Key),
    /// Clean close (flush) and reopen.
    Reopen,
    /// Drop the adapter without flushing, then reopen.
    Crash,
    Checkpoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    /// Phase 1 blocking profile.
    Blocking,
    /// Everything implemented so far; failures are findings, not gates.
    Discovery,
}

impl Profile {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "blocking" => Some(Self::Blocking),
            "discovery" => Some(Self::Discovery),
            _ => None,
        }
    }
}

pub fn generate(seed: u64, len: usize, profile: Profile) -> Vec<Op> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let keys = 16u8; // small key space so overwrites and deletes collide
    (0..len)
        .map(|i| {
            let k = vec![b'k', rng.gen_range(0..keys)];
            let roll = rng.gen_range(0..100);
            match (profile, roll) {
                (_, 0..=59) => Op::Put(k, format!("v{seed}-{i}").into_bytes()),
                (_, 60..=79) => Op::Delete(k),
                (_, 80..=89) => Op::Reopen,
                (Profile::Discovery, 90..=94) => Op::Checkpoint,
                _ => Op::Crash,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_ops() {
        assert_eq!(
            generate(42, 100, Profile::Blocking),
            generate(42, 100, Profile::Blocking)
        );
    }

    #[test]
    fn blocking_never_checkpoints() {
        for seed in 0..50 {
            assert!(!generate(seed, 200, Profile::Blocking).contains(&Op::Checkpoint));
        }
    }

    #[test]
    fn discovery_does_checkpoint() {
        assert!((0..20).any(|s| generate(s, 200, Profile::Discovery).contains(&Op::Checkpoint)));
    }
}
