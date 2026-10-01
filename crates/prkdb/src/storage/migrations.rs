//! Migration registry (spec D4). Empty in format 2; format 3 ships its migrator here.
//!
//! A format change is only allowed together with a registered migration from the
//! previous format, so `prkdb-cli migrate --data-dir` always has a path forward for a
//! directory the previous release wrote. Format 1 had no marker and has no migrator: it
//! is refused (D3).

use super::format::FORMAT_VERSION;
use prkdb_types::error::StorageError;
use std::path::Path;

/// One step from format `from()` to format `to()`, run offline on a closed directory.
pub trait Migration: Send + Sync {
    fn from(&self) -> u32;
    fn to(&self) -> u32;
    fn description(&self) -> &str;
    /// Must leave `dir` either fully at `to()` (FORMAT rewritten last, atomically) or untouched.
    fn run(&self, dir: &Path) -> Result<(), StorageError>;
}

/// Every migration this build knows, in no particular order.
pub fn registry() -> Vec<Box<dyn Migration>> {
    Vec::new()
}

/// The chain of migrations from `found` to `FORMAT_VERSION`, or an error naming the gap.
/// Empty when `found` is already current.
pub fn plan(found: u32) -> Result<Vec<Box<dyn Migration>>, StorageError> {
    plan_from(registry(), found)
}

fn plan_from(
    mut available: Vec<Box<dyn Migration>>,
    found: u32,
) -> Result<Vec<Box<dyn Migration>>, StorageError> {
    if found > FORMAT_VERSION {
        return Err(StorageError::UnsupportedFormat(format!(
            "a newer PrkDB (format {found}) wrote this directory; this version reads format \
             {FORMAT_VERSION} and cannot migrate down. See docs/guide/upgrade."
        )));
    }
    let mut chain = Vec::new();
    let mut at = found;
    while at != FORMAT_VERSION {
        // Only forward steps that do not overshoot, so the walk always terminates.
        let next = available
            .iter()
            .position(|m| m.from() == at && m.to() > at && m.to() <= FORMAT_VERSION);
        let Some(next) = next else {
            return Err(StorageError::UnsupportedFormat(if at == found {
                format!(
                    "no migrations available for format {found} → {FORMAT_VERSION}; format \
                     {found} directories cannot be converted by this version. See \
                     docs/guide/upgrade."
                )
            } else {
                format!(
                    "no migration from format {at} (needed to bring format {found} to \
                     {FORMAT_VERSION}). See docs/guide/upgrade."
                )
            }));
        };
        let step = available.swap_remove(next);
        at = step.to();
        chain.push(step);
    }
    Ok(chain)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Step(u32, u32);

    impl Migration for Step {
        fn from(&self) -> u32 {
            self.0
        }
        fn to(&self) -> u32 {
            self.1
        }
        fn description(&self) -> &str {
            "test step"
        }
        fn run(&self, _dir: &Path) -> Result<(), StorageError> {
            Ok(())
        }
    }

    fn steps(pairs: &[(u32, u32)]) -> Vec<Box<dyn Migration>> {
        pairs
            .iter()
            .map(|&(f, t)| Box::new(Step(f, t)) as Box<dyn Migration>)
            .collect()
    }

    #[test]
    fn the_format_2_registry_is_empty_and_the_current_format_needs_nothing() {
        assert!(registry().is_empty());
        assert!(plan(FORMAT_VERSION).unwrap().is_empty());
    }

    #[test]
    fn format_1_has_no_migration() {
        let err = plan(1).err().expect("format 1 is refused").to_string();
        assert!(
            err.contains("no migrations available for format 1"),
            "{err}"
        );
        assert!(err.contains("docs/guide/upgrade"), "{err}");
    }

    #[test]
    fn a_newer_format_is_refused() {
        let err = plan(FORMAT_VERSION + 1).err().expect("refused").to_string();
        assert!(err.contains("newer PrkDB"), "{err}");
    }

    #[test]
    fn a_chain_is_followed_in_order() {
        let chain = plan_from(steps(&[(1, 2), (0, 1)]), 0).unwrap();
        let hops: Vec<_> = chain.iter().map(|m| (m.from(), m.to())).collect();
        assert_eq!(hops, vec![(0, 1), (1, 2)]);
    }

    #[test]
    fn a_gap_in_the_chain_is_named() {
        let err = plan_from(steps(&[(0, 1)]), 0)
            .err()
            .expect("gap")
            .to_string();
        assert!(err.contains("no migration from format 1"), "{err}");
    }

    #[test]
    fn a_backward_step_is_never_taken() {
        let err = plan_from(steps(&[(1, 1), (1, 0)]), 1)
            .err()
            .expect("no forward step")
            .to_string();
        assert!(err.contains("format 1"), "{err}");
    }
}
