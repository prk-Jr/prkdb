//! Operation vocabulary and profiles (spec §7.1). The generator draws only ops
//! enabled by the selected profile. Seeds are stable for `rand 0.8` /
//! `rand_chacha 0.3` (pinned in `Cargo.lock`): the same seed always produces the
//! same op sequence for a given profile, as long as those crate versions and
//! the shape of `generate`'s rng calls don't change. Fault parameters
//! (`PowerLoss`'s tear and fault seed) come from a second stream, so adding or
//! retuning faults never changes a seed's workload draws.

use crate::faultfs::Tear;
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
    /// Run a WAL compaction to completion (Task 2.15). Changes no logical state.
    Compact,
    /// Power cut: everything not yet synced may be lost or torn per `tear`
    /// (see [`crate::faultfs`]), with `fault_seed` driving FaultFs's choices.
    /// Then the SUT reopens.
    PowerLoss {
        tear: Tear,
        fault_seed: u64,
    },
}

impl Op {
    /// The op's kind name, as counted in [`crate::runner::Report::op_counts`].
    pub fn kind_name(&self) -> &'static str {
        match self {
            Op::Put(..) => Kind::Put.name(),
            Op::Delete(_) => Kind::Delete.name(),
            Op::Reopen => Kind::Reopen.name(),
            Op::Crash => Kind::Crash.name(),
            Op::Checkpoint => Kind::Checkpoint.name(),
            Op::Compact => Kind::Compact.name(),
            Op::PowerLoss { .. } => Kind::PowerLoss.name(),
        }
    }
}

/// Every op kind name, in canonical (declaration) order.
pub const OP_KIND_NAMES: &[&str] = &[
    "Put",
    "Delete",
    "Reopen",
    "Crash",
    "Checkpoint",
    "Compact",
    "PowerLoss",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    /// The Phase 1 blocking table, frozen: the self-tests were tuned on its
    /// seeds and keep reproducing exactly, whatever `Blocking` later becomes.
    Core,
    /// The gating profile: `Core` plus `PowerLoss` (Task 2.10b). Needs a SUT
    /// that can lose power (`FaultSut`).
    Blocking,
    /// Everything implemented so far; failures are findings, not gates.
    Discovery,
}

impl Profile {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "core" => Some(Self::Core),
            "blocking" => Some(Self::Blocking),
            "discovery" => Some(Self::Discovery),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Core => "core",
            Self::Blocking => "blocking",
            Self::Discovery => "discovery",
        }
    }

    /// Names of the op kinds this profile can generate, in canonical order.
    /// A run in which any of these never executed is vacuous for that kind.
    pub fn op_kinds(self) -> Vec<&'static str> {
        let table = weights(self);
        OP_KIND_NAMES
            .iter()
            .copied()
            .filter(|name| table.iter().any(|(k, w)| *w > 0 && k.name() == *name))
            .collect()
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
    Compact,
    PowerLoss,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Put => "Put",
            Kind::Delete => "Delete",
            Kind::Reopen => "Reopen",
            Kind::Crash => "Crash",
            Kind::Checkpoint => "Checkpoint",
            Kind::Compact => "Compact",
            Kind::PowerLoss => "PowerLoss",
        }
    }
}

/// Explicit per-profile weight tables, out of 100. `CORE_WEIGHTS` reproduces
/// exactly the cumulative ranges the generator used before these tables
/// existed (Put 0..=59, Delete 60..=79, Reopen 80..=89, then Crash), so Phase 1
/// seeds still produce the same ops under `Core`.
const CORE_WEIGHTS: &[(Kind, u32)] = &[
    (Kind::Put, 60),
    (Kind::Delete, 20),
    (Kind::Reopen, 10),
    (Kind::Crash, 10),
];
const BLOCKING_WEIGHTS: &[(Kind, u32)] = &[
    (Kind::Put, 55),
    (Kind::Delete, 20),
    (Kind::Reopen, 8),
    (Kind::Crash, 8),
    (Kind::PowerLoss, 9),
];
const DISCOVERY_WEIGHTS: &[(Kind, u32)] = &[
    (Kind::Put, 51),
    (Kind::Delete, 20),
    (Kind::Reopen, 8),
    (Kind::Checkpoint, 5),
    (Kind::Compact, 4),
    (Kind::Crash, 5),
    (Kind::PowerLoss, 7),
];

/// XORed into the seed for the fault stream: one rng per concern, so fault
/// draws never shift a seed's workload draws.
const FAULT_STREAM: u64 = 0xFA17_FA17_FA17_FA17;

fn weights(profile: Profile) -> &'static [(Kind, u32)] {
    match profile {
        Profile::Core => CORE_WEIGHTS,
        Profile::Blocking => BLOCKING_WEIGHTS,
        Profile::Discovery => DISCOVERY_WEIGHTS,
    }
}

pub fn generate(seed: u64, len: usize, profile: Profile) -> Vec<Op> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut fault_rng = ChaCha8Rng::seed_from_u64(seed ^ FAULT_STREAM);
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
                Kind::Compact => Op::Compact,
                // Drawn only when a PowerLoss is emitted, and from the fault stream.
                Kind::PowerLoss => Op::PowerLoss {
                    tear: Tear::random(&mut fault_rng),
                    fault_seed: fault_rng.gen(),
                },
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
        for profile in [Profile::Core, Profile::Blocking] {
            for seed in 0..50 {
                assert!(
                    !generate(seed, 200, profile).contains(&Op::Checkpoint),
                    "{profile:?} generated a Checkpoint for seed {seed}"
                );
            }
        }
    }

    #[test]
    fn power_loss_parameters_vary() {
        let losses: Vec<Op> = (0..20)
            .flat_map(|s| generate(s, 200, Profile::Blocking))
            .filter(|op| matches!(op, Op::PowerLoss { .. }))
            .collect();
        for tear in [Tear::None, Tear::Prefix, Tear::ZeroTail, Tear::Garbage] {
            assert!(
                losses
                    .iter()
                    .any(|op| matches!(op, Op::PowerLoss { tear: t, .. } if *t == tear)),
                "{tear:?} never drawn"
            );
        }
        let mut fault_seeds: Vec<u64> = losses
            .iter()
            .filter_map(|op| match op {
                Op::PowerLoss { fault_seed, .. } => Some(*fault_seed),
                _ => None,
            })
            .collect();
        fault_seeds.sort_unstable();
        fault_seeds.dedup();
        assert!(fault_seeds.len() > 1, "fault seeds never vary");
    }

    #[test]
    fn discovery_does_checkpoint() {
        assert!((0..20).any(|s| generate(s, 200, Profile::Discovery).contains(&Op::Checkpoint)));
    }

    #[test]
    fn discovery_compacts_and_blocking_does_not() {
        assert!((0..20).any(|s| generate(s, 200, Profile::Discovery).contains(&Op::Compact)));
        for profile in [Profile::Core, Profile::Blocking] {
            assert!((0..50).all(|s| !generate(s, 200, profile).contains(&Op::Compact)));
        }
    }

    #[test]
    fn profiles_round_trip_through_parse() {
        for p in [Profile::Core, Profile::Blocking, Profile::Discovery] {
            assert_eq!(Profile::parse(p.as_str()), Some(p));
        }
        assert_eq!(Profile::parse("nope"), None);
    }

    #[test]
    fn op_kinds_follow_the_weight_tables() {
        assert_eq!(
            Profile::Blocking.op_kinds(),
            vec!["Put", "Delete", "Reopen", "Crash", "PowerLoss"]
        );
        assert_eq!(
            Profile::Core.op_kinds(),
            vec!["Put", "Delete", "Reopen", "Crash"]
        );
        assert_eq!(Profile::Discovery.op_kinds(), OP_KIND_NAMES.to_vec());
        for op in generate(7, 200, Profile::Discovery) {
            assert!(OP_KIND_NAMES.contains(&op.kind_name()));
        }
    }

    #[test]
    fn key_and_key_space_agree() {
        assert_eq!(key(0), vec![b'k', 0]);
        assert_eq!(key(KEY_SPACE - 1), vec![b'k', KEY_SPACE - 1]);
    }
}
