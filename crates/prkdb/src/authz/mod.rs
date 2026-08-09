//! Authorization: who may do what to which collection.
//!
//! Closes S-01, where both data planes were reachable without any credential —
//! `fetch_segment` in particular streamed raw WAL segments to any caller.
//!
//! # The model
//!
//! ```text
//! Principal  — a named identity holding one credential
//! Role       — a named set of grants, shareable between principals
//! Grant      — (collection-pattern, permission)
//! Permission — Read < Write < Admin
//! ```
//!
//! `Admin` on `*` reproduces the old single `PRKDB_ADMIN_TOKEN` exactly, which is the
//! migration path rather than a coincidence: that token becomes a bootstrap principal.
//!
//! # Two properties worth knowing before changing anything here
//!
//! - **Permission implication runs one way.** `Admin` implies `Write` implies `Read`; the
//!   reverse never holds. Getting that backwards turns a read-only credential into a
//!   write credential.
//! - **Grants union, they do not shadow.** Any grant conferring sufficient authority is
//!   enough, and there are no deny rules, so evaluation order cannot affect the outcome.
//!   Introducing a deny rule later would change that and needs its own design.

mod model;
mod store;

pub use model::{Grant, Permission, Principal, Role};
pub use store::{BootstrapError, PrincipalStore, PRINCIPAL_KEY_PREFIX};
