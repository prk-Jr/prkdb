//! Principals, roles, grants and permissions.
//!
//! Deliberately three concepts and no more. Groups, permission inheritance, row- and
//! field-level rules, external identity providers, and credential expiry are all real
//! features and none of them is needed to close S-01. Add them when something demands
//! them, not in anticipation.

use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

/// What a principal may do with a collection.
///
/// Ordered by strength: `Admin` implies `Write` implies `Read`. The implication runs one
/// way only — a `Read` grant never confers `Write`, which is the direction that matters
/// when a mistake here is a security hole rather than an inconvenience.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Permission {
    Read,
    Write,
    Admin,
}

impl Permission {
    /// Whether holding `self` is sufficient authority to perform `required`.
    pub fn satisfies(self, required: Permission) -> bool {
        self >= required
    }
}

/// A permission over a set of collections named by a pattern.
///
/// The pattern is either an exact collection name, `*` for every collection, or a
/// `prefix/*` wildcard. Nothing more elaborate: glob semantics invite ambiguity in
/// exactly the place where ambiguity becomes a vulnerability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    pattern: String,
    permission: Permission,
}

impl Grant {
    pub fn new(pattern: impl Into<String>, permission: Permission) -> Self {
        Self {
            pattern: pattern.into(),
            permission,
        }
    }

    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    pub fn permission(&self) -> Permission {
        self.permission
    }

    /// Whether this grant's pattern covers `collection`.
    fn covers(&self, collection: &str) -> bool {
        if self.pattern == "*" {
            return true;
        }
        if let Some(prefix) = self.pattern.strip_suffix('*') {
            // `logs/*` covers `logs/` and `logs/app`, but must not cover `logsmith`.
            // Requiring the literal prefix — separator included — is what prevents that.
            return collection.starts_with(prefix);
        }
        self.pattern == collection
    }
}

/// A named set of grants, so several principals can share one policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Role {
    name: String,
    grants: Vec<Grant>,
}

impl Role {
    pub fn new(name: impl Into<String>, grants: Vec<Grant>) -> Self {
        Self {
            name: name.into(),
            grants,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn grants(&self) -> &[Grant] {
        &self.grants
    }
}

/// An identity holding one credential and a set of grants.
///
/// `Admin` on `*` reproduces the behaviour of the previous single `PRKDB_ADMIN_TOKEN`
/// exactly, which is the migration path: that token becomes a bootstrap principal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Principal {
    name: String,
    credential: String,
    grants: Vec<Grant>,
}

impl Principal {
    pub fn new(name: impl Into<String>, credential: impl Into<String>, grants: Vec<Grant>) -> Self {
        Self {
            name: name.into(),
            credential: credential.into(),
            grants,
        }
    }

    /// A principal with `Admin` on every collection.
    pub fn admin(name: impl Into<String>, credential: impl Into<String>) -> Self {
        Self::new(name, credential, vec![Grant::new("*", Permission::Admin)])
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn grants(&self) -> &[Grant] {
        &self.grants
    }

    /// Whether this principal may perform `required` on `collection`.
    ///
    /// Grants union rather than shadow: any one of them granting sufficient authority is
    /// enough. There is no deny rule, so ordering cannot matter.
    pub fn permits(&self, collection: &str, required: Permission) -> bool {
        self.grants
            .iter()
            .any(|g| g.covers(collection) && g.permission.satisfies(required))
    }

    /// Constant-time credential comparison.
    ///
    /// A byte-wise `==` leaks the credential prefix through response timing, one byte at
    /// a time. Length is compared first and is not secret — `ct_eq` requires equal-length
    /// inputs, and a length mismatch is already observable from the failure itself.
    pub fn credential_matches(&self, presented: &str) -> bool {
        let expected = self.credential.as_bytes();
        let presented = presented.as_bytes();
        expected.len() == presented.len() && bool::from(expected.ct_eq(presented))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_ordering_runs_one_way() {
        assert!(Permission::Admin.satisfies(Permission::Read));
        assert!(Permission::Admin.satisfies(Permission::Write));
        assert!(Permission::Write.satisfies(Permission::Read));

        assert!(!Permission::Read.satisfies(Permission::Write));
        assert!(!Permission::Read.satisfies(Permission::Admin));
        assert!(!Permission::Write.satisfies(Permission::Admin));
    }

    #[test]
    fn wildcard_requires_the_literal_prefix() {
        let g = Grant::new("logs/*", Permission::Read);
        assert!(g.covers("logs/"));
        assert!(g.covers("logs/app"));
        assert!(!g.covers("logsmith"));
        assert!(!g.covers("log"));
    }

    #[test]
    fn admin_helper_grants_everything() {
        let p = Principal::admin("root", "secret");
        assert!(p.permits("anything", Permission::Admin));
        assert!(p.permits("other", Permission::Write));
    }
}
