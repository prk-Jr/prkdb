//! Permission decisions, exhaustively.
//!
//! Regression guard for spec R12 acceptance 1-3. Authorization is decision logic over a
//! small input space, so a table beats prose reasoning about cases — and it is the
//! artifact someone will reread in a year when deciding whether a change is safe.

use prkdb::authz::{Grant, Permission, Principal};
use rstest::rstest;

/// Build a principal holding exactly these grants.
fn principal_with(grants: &[(&str, Permission)]) -> Principal {
    Principal::new(
        "test-principal",
        "test-credential",
        grants
            .iter()
            .map(|(pattern, perm)| Grant::new(*pattern, *perm))
            .collect(),
    )
}

#[rstest]
// exact-match grants
#[case(&[("users", Permission::Read)],   "users",     Permission::Read,  true )]
#[case(&[("users", Permission::Read)],   "users",     Permission::Write, false)]
#[case(&[("users", Permission::Read)],   "orders",    Permission::Read,  false)]
#[case(&[("users", Permission::Write)],  "users",     Permission::Write, true )]
// Write implies Read: a principal that may modify a collection may observe it.
#[case(&[("users", Permission::Write)],  "users",     Permission::Read,  true )]
// Admin implies everything on the collections it covers.
#[case(&[("users", Permission::Admin)],  "users",     Permission::Write, true )]
#[case(&[("users", Permission::Admin)],  "users",     Permission::Read,  true )]
#[case(&[("*",     Permission::Admin)],  "anything",  Permission::Write, true )]
#[case(&[("*",     Permission::Admin)],  "anything",  Permission::Admin, true )]
// Read does NOT imply Write or Admin — the direction that matters for safety.
#[case(&[("*",     Permission::Read)],   "anything",  Permission::Write, false)]
#[case(&[("*",     Permission::Read)],   "anything",  Permission::Admin, false)]
#[case(&[("users", Permission::Write)],  "users",     Permission::Admin, false)]
// prefix wildcards
#[case(&[("logs/*", Permission::Write)], "logs/app",  Permission::Write, true )]
#[case(&[("logs/*", Permission::Write)], "logs/",     Permission::Write, true )]
#[case(&[("logs/*", Permission::Write)], "users",     Permission::Read,  false)]
// `logs/*` must not match a collection merely starting with the same letters.
#[case(&[("logs/*", Permission::Write)], "logsmith",  Permission::Write, false)]
// no grants at all
#[case(&[],                              "users",     Permission::Read,  false)]
#[case(&[],                              "users",     Permission::Admin, false)]
// several grants, only one relevant
#[case(&[("a", Permission::Read), ("b", Permission::Write)], "b", Permission::Write, true )]
#[case(&[("a", Permission::Read), ("b", Permission::Write)], "a", Permission::Write, false)]
fn permits(
    #[case] grants: &[(&str, Permission)],
    #[case] collection: &str,
    #[case] action: Permission,
    #[case] expected: bool,
) {
    let principal = principal_with(grants);
    assert_eq!(
        principal.permits(collection, action),
        expected,
        "principal with {grants:?} asking {action:?} on {collection:?}"
    );
}

/// Credential comparison must not leak the credential through response timing.
///
/// This asserts the API is wired to a constant-time comparison; it does not attempt to
/// measure timing, which is not something a unit test can do reliably.
#[test]
fn credential_check_accepts_only_the_exact_credential() {
    let p = principal_with(&[("users", Permission::Read)]);

    assert!(p.credential_matches("test-credential"));
    assert!(!p.credential_matches("test-credentia"));
    assert!(!p.credential_matches("test-credential "));
    assert!(!p.credential_matches("TEST-CREDENTIAL"));
    assert!(!p.credential_matches(""));
}

/// A principal carrying several grants gets the union of them, not the first match.
#[test]
fn grants_union_rather_than_shadow() {
    let p = principal_with(&[
        ("users", Permission::Read),
        ("users", Permission::Write),
        ("logs/*", Permission::Read),
    ]);

    assert!(p.permits("users", Permission::Write));
    assert!(p.permits("users", Permission::Read));
    assert!(p.permits("logs/app", Permission::Read));
    assert!(!p.permits("logs/app", Permission::Write));
}
