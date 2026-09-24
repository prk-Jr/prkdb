//! Operation vocabulary and profiles (spec §7.1). The generator draws only ops
//! enabled by the selected profile. Seeds are stable for `rand 0.8` /
//! `rand_chacha 0.3` (pinned in `Cargo.lock`): the same seed always produces the
//! same op sequence for a given profile, as long as those crate versions and
//! the shape of `generate`'s rng calls don't change.

use crate::model::{Key, Value};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// Number of distinct keys the generator draws from. Keep this small so
/// overwrites and deletes collide often enough to exercise interesting
/// interleavings.
pub const KEY_SPACE: u8 = 16;

/// The single source of truth for turning a key index into an actual `Key`.
/// Both the generator and the checker (via [`crate::model::Model`]) must go
/// through this so that widening the generator's key space can never
/// accidentally shrink what the checker verifies.
pub fn key(i: u8) -> Key {
    vec![b'k', i]
}

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

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Blocking => "blocking",
            Self::Discovery => "discovery",
        }
    }
}

/// The op kind a weight table entry selects; `generate` turns this plus the
/// per-op payload (key, value) into an [`Op`].
#[derive(Debug, Clone, Copy)]
enum Kind {
    Put,
    Delete,
    Reopen,
    Crash,
    Checkpoint,
}

/// Explicit per-profile weight tables, out of 100. These reproduce exactly the
/// cumulative ranges the generator used before this table existed
/// (Put 0..=59, Delete 60..=79, Reopen 80..=89, then Checkpoint/Crash), so
/// existing seeds still produce the same ops.
const BLOCKING_WEIGHTS: &[(Kind, u32)] = &[
    (Kind::Put, 60),
    (Kind::Delete, 20),
    (Kind::Reopen, 10),
    (Kind::Crash, 10),
];
const DISCOVERY_WEIGHTS: &[(Kind, u32)] = &[
    (Kind::Put, 60),
    (Kind::Delete, 20),
    (Kind::Reopen, 10),
    (Kind::Checkpoint, 5),
    (Kind::Crash, 5),
];

fn weights(profile: Profile) -> &'static [(Kind, u32)] {
    match profile {
        Profile::Blocking => BLOCKING_WEIGHTS,
        Profile::Discovery => DISCOVERY_WEIGHTS,
    }
}

pub fn generate(seed: u64, len: usize, profile: Profile) -> Vec<Op> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let table = weights(profile);
    let total: u32 = table.iter().map(|(_, w)| w).sum();
    (0..len)
        .map(|i| {
            let k = key(rng.gen_range(0..KEY_SPACE));
            let mut roll = rng.gen_range(0..total);
            let mut kind = table[0].0;
            for (candidate, weight) in table {
                if roll < *weight {
                    kind = *candidate;
                    break;
                }
                roll -= weight;
            }
            match kind {
                Kind::Put => Op::Put(k, format!("v{seed}-{i}").into_bytes()),
                Kind::Delete => Op::Delete(k),
                Kind::Reopen => Op::Reopen,
                Kind::Crash => Op::Crash,
                Kind::Checkpoint => Op::Checkpoint,
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

    #[test]
    fn key_and_key_space_agree() {
        assert_eq!(key(0), vec![b'k', 0]);
        assert_eq!(key(KEY_SPACE - 1), vec![b'k', KEY_SPACE - 1]);
    }
}
