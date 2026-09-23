//! Durable-mode check: after a restart, every key's value equals the model's.
//!
//! A SUT error while checking (e.g. `get` failing after reopen) is itself a
//! finding, not a harness error: it means the implementation under test is
//! broken, not that the harness is.

use crate::model::Model;
use crate::ops::{key, KEY_SPACE};
use crate::sut::Sut;
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mismatch {
    pub key: Vec<u8>,
    pub expected: Option<Vec<u8>>,
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

/// Compares the SUT against the model over the union of the generator's full
/// key space ([`KEY_SPACE`] keys from [`key`]) and every key the model has
/// ever seen touched (see [`Model::touched`]), so a wider generator, or a bug
/// that reaches outside the nominal key space, can't shrink coverage.
pub async fn check_durable(model: &Model, sut: &mut dyn Sut) -> CheckOutcome {
    let mut keys: BTreeSet<Vec<u8>> = (0..KEY_SPACE).map(key).collect();
    keys.extend(model.touched.iter().cloned());

    let mut compared = 0;
    for k in keys {
        compared += 1;
        match sut.get(&k).await {
            Ok(actual) => {
                let expected = model.get(&k).cloned();
                if expected != actual {
                    return CheckOutcome::Mismatch {
                        compared,
                        mismatch: Mismatch {
                            key: k,
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
