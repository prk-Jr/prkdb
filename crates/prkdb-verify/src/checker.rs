//! Post-restart checks. After a restart (and after any power loss in Durable
//! mode) every key's value equals the model's full state. After a power loss
//! in Fast mode the SUT's whole snapshot must equal ONE prefix of the
//! acknowledged mutations no shorter than the last known sync (spec §7).
//!
//! A SUT error while checking (e.g. `get` failing after reopen) is itself a
//! finding, not a harness error: it means the implementation under test is
//! broken, not that the harness is.

use crate::model::{Key, Mode, Model, Value};
use crate::ops::{key, KEY_SPACE};
use crate::sut::Sut;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mismatch {
    pub key: Vec<u8>,
    /// The value if every acknowledged mutation survived (the full model
    /// state). Kept for reports and existing callers.
    pub expected: Option<Vec<u8>>,
    /// Every value the checker would have accepted for this key. In Durable
    /// mode this is exactly `[expected]`.
    pub acceptable: Vec<Option<Value>>,
    pub actual: Option<Vec<u8>>,
}

/// The result of comparing every checked key against the model.
#[derive(Debug)]
pub enum CheckOutcome {
    /// Every checked key matched. `compared` is the number of keys compared.
    Ok { compared: usize },
    /// The first key whose value diverged from the model.
    Mismatch { compared: usize, mismatch: Mismatch },
    /// The SUT itself errored while being queried.
    SutError { compared: usize, error: String },
}

impl Mismatch {
    /// True for a Fast-mode "no prefix fits" finding: this key's value on its
    /// own is one the checker would accept (some prefix gives it), yet no
    /// single prefix explains the whole snapshot — the SUT kept a later write
    /// while losing an earlier one. The reported key is then just the first
    /// one that differs from the full state, not the whole story.
    pub fn no_prefix_fits(&self) -> bool {
        self.acceptable.contains(&self.actual)
    }
}

/// The keys every check compares: the generator's full key space
/// ([`KEY_SPACE`] keys from [`key`]) plus every key the model has ever seen
/// touched (see [`Model::touched`]).
fn checked_keys(model: &Model) -> BTreeSet<Key> {
    let mut keys: BTreeSet<Key> = (0..KEY_SPACE).map(key).collect();
    keys.extend(model.touched.iter().cloned());
    keys
}

/// Compares the SUT against the model's full state (every acknowledged
/// mutation survived) over the union of the generator's full key space
/// ([`KEY_SPACE`] keys from [`key`]) and every key the model has ever seen
/// touched (see [`Model::touched`]), so a wider generator, or a bug that
/// reaches outside the nominal key space, can't shrink coverage.
pub async fn check_durable(model: &Model, sut: &mut dyn Sut) -> CheckOutcome {
    let state = model.state();

    let mut compared = 0;
    for k in checked_keys(model) {
        compared += 1;
        match sut.get(&k).await {
            Ok(actual) => {
                let expected = state.get(&k).cloned();
                if expected != actual {
                    return CheckOutcome::Mismatch {
                        compared,
                        mismatch: Mismatch {
                            key: k,
                            acceptable: vec![expected.clone()],
                            expected,
                            actual,
                        },
                    };
                }
            }
            Err(e) => {
                return CheckOutcome::SutError {
                    compared,
                    error: e.to_string(),
                }
            }
        }
    }
    CheckOutcome::Ok { compared }
}

/// The check after a power loss. On success the model learns what survived.
///
/// - Durable: every acknowledged mutation was durable, so this is
///   [`check_durable`]; afterwards everything is durable.
/// - Fast: reads every checked key once, then looks for the LONGEST `n` such
///   that `model.prefix(n)` equals the SUT's whole snapshot. This is one prefix
///   for the entire snapshot, never a per-key test: a per-key test would accept
///   key A from a long prefix together with key B from a short one, i.e. a
///   hole in the log. Prefixes start at `model.durable` (the last sync the
///   model knows of), so losing synced data never fits. On success
///   `model.settle(n)`. On failure the mismatch names the first key whose
///   value differs from the full state, with `acceptable` = that key's value
///   in every prefix; if its actual value is among them, no single prefix
///   explains the snapshot (see [`Mismatch::no_prefix_fits`]).
pub async fn check_after_power_loss(
    model: &mut Model,
    sut: &mut dyn Sut,
    mode: Mode,
) -> CheckOutcome {
    if mode == Mode::Durable {
        let outcome = check_durable(model, sut).await;
        if matches!(outcome, CheckOutcome::Ok { .. }) {
            model.mark_durable();
        }
        return outcome;
    }

    let keys = checked_keys(model);
    let mut snapshot: BTreeMap<Key, Value> = BTreeMap::new();
    let mut compared = 0;
    for k in &keys {
        compared += 1;
        match sut.get(k).await {
            Ok(Some(v)) => {
                snapshot.insert(k.clone(), v);
            }
            Ok(None) => {}
            Err(e) => {
                return CheckOutcome::SutError {
                    compared,
                    error: e.to_string(),
                }
            }
        }
    }

    // Every key a prefix holds was touched, so it is a checked key: comparing
    // whole maps compares exactly the checked keys.
    let prefixes: Vec<BTreeMap<Key, Value>> =
        (0..=model.pending.len()).map(|n| model.prefix(n)).collect();
    if let Some(n) = (0..prefixes.len()).rev().find(|&n| prefixes[n] == snapshot) {
        model.settle(n);
        return CheckOutcome::Ok { compared };
    }

    let state = &prefixes[prefixes.len() - 1];
    let k = keys
        .into_iter()
        .find(|k| snapshot.get(k) != state.get(k))
        .expect("the snapshot differs from every prefix, so from the full state too");
    let mut acceptable: Vec<Option<Value>> = prefixes.iter().map(|p| p.get(&k).cloned()).collect();
    acceptable.dedup();
    CheckOutcome::Mismatch {
        compared,
        mismatch: Mismatch {
            expected: state.get(&k).cloned(),
            actual: snapshot.get(&k).cloned(),
            key: k,
            acceptable,
        },
    }
}
