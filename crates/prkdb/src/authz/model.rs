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
    /// SHA-256 of the credential, hex-encoded — never the credential itself.
    ///
    /// Principals are persisted so they survive a restart and agree across a cluster,
    /// which means this value reaches disk and the replication log. Storing the bearer
    /// token there would turn any read of the storage layer, any backup archive, and any
    /// `fetch_segment` stream into a credential dump.
    ///
    /// A single SHA-256 is deliberate rather than a password KDF: these are
    /// machine-generated bearer tokens with full entropy, not human passwords, so the
    /// brute-force resistance a slow KDF buys does not apply. If PrkDB ever accepts
    /// user-chosen credentials, this must become argon2 or bcrypt.
    credential_hash: String,
    grants: Vec<Grant>,
}

/// Hex-encoded SHA-256 of a credential.
fn hash_credential(credential: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(credential.as_bytes());
    format!("{:x}", hasher.finalize())
}

impl Principal {
    pub fn new(name: impl Into<String>, credential: impl Into<String>, grants: Vec<Grant>) -> Self {
        Self {
            name: name.into(),
            credential_hash: hash_credential(&credential.into()),
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
        // Both sides are hex SHA-256, so the lengths always match and the comparison is
        // over digests rather than secrets. Still constant-time: a digest comparison that
        // short-circuits leaks which prefix was right, which is enough to walk a forgery
        // one nibble at a time.
        let expected = self.credential_hash.as_bytes();
        let presented = hash_credential(presented);
        let presented = presented.as_bytes();
        expected.len() == presented.len() && bool::from(expected.ct_eq(presented))
    }

    /// The stored digest. Exposed for persistence, never for comparison — use
    /// [`credential_matches`](Self::credential_matches), which is constant-time.
    pub fn credential_hash(&self) -> &str {
        &self.credential_hash
    }
}

#[cfg(test)]
mod tests {
    /// Pins the accessors that carry authorization data between crates.
    ///
    /// # Why these are worth asserting directly
    ///
    /// Mutation run 31329241574 replaced each of them with a constant — `Grant::pattern`
    /// with `""` and `"xyzzy"`, `Role::name` with `""`, `Role::grants` and
    /// `Principal::grants` with an empty slice, `Principal::credential_hash` with `""` —
    /// and every one survived the `prkdb` test suite.
    ///
    /// They survived for a scoping reason rather than a logic one: their only callers are
    /// in `prkdb-cli` (`authz_layer.rs` and `admin_principals.rs`), and the run was
    /// `--package prkdb`, so no test that could have killed them was in scope. The
    /// consequences are real anyway — `Role::grants` returning empty silently strips a
    /// role's authority, and `credential_hash` returning `""` writes a principal that no
    /// credential can ever match — so the fix is to pin them here rather than to widen the
    /// mutation scope and pay a full workspace test run per mutant.
    #[test]
    fn accessors_return_what_was_configured() {
        let grant = Grant::new("orders:*", Permission::Write);
        assert_eq!(grant.pattern(), "orders:*");
        assert_eq!(grant.permission(), Permission::Write);

        let role = Role::new("auditor", vec![grant.clone()]);
        assert_eq!(role.name(), "auditor");
        assert_eq!(role.grants().len(), 1);
        assert_eq!(role.grants()[0].pattern(), "orders:*");

        let principal = Principal::new("alice", "s3cret", vec![grant]);
        assert_eq!(principal.name(), "alice");
        assert_eq!(principal.grants().len(), 1);
        assert_eq!(principal.grants()[0].pattern(), "orders:*");
    }

    /// The stored digest is the SHA-256 of the credential, and never the credential.
    #[test]
    fn credential_hash_is_the_digest_of_the_credential() {
        let principal = Principal::new("alice", "s3cret", vec![]);
        let hash = principal.credential_hash();

        assert_eq!(hash, hash_credential("s3cret"));
        assert_eq!(hash.len(), 64, "hex SHA-256 is 64 characters");
        assert!(!hash.is_empty());
        assert_ne!(
            hash, "s3cret",
            "the credential must not be stored in the clear"
        );

        // An empty digest would match nothing, so this also guards the accessor being
        // replaced by a constant.
        assert!(principal.credential_matches("s3cret"));
        assert!(!principal.credential_matches("wrong"));
    }

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
