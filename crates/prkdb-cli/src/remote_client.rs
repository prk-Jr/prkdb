//! Building a [`PrkDbClient`] for a CLI subcommand.
//!
//! # Why every subcommand must come through here
//!
//! `PrkDbClient::new` fetches cluster metadata *before it returns*, and `Metadata` requires
//! `Read` on a server with authorization enabled. So the natural-looking
//!
//! ```rust,ignore
//! PrkDbClient::new(servers).await?.with_admin_token(token)
//! ```
//!
//! applies the credential one step too late: the bootstrap fetch has already been refused
//! and `new` returned `Err`. Every remote subcommand was written that way, which made the
//! whole CLI unusable against a secured cluster — `prkdb-cli schema list` against a server
//! started with `PRKDB_BOOTSTRAP_TOKEN` failed with
//!
//! ```text
//! Error: Failed to fetch metadata from any bootstrap server or cached node
//! ```
//!
//! a message that names the network and never mentions authorization, so it reads as an
//! unreachable server. It survived local testing because every script that exercised the
//! CLI ran it against an anonymous server; CI caught it only once the mixed-client
//! integration test was switched to an authorized one.
//!
//! Twelve call sites each doing this by hand is the same shape as spec S-01, where ten
//! server RPCs were individually responsible for their own authorization and ten of them
//! forgot. One constructor, so the next subcommand cannot get it wrong.

use anyhow::Result;
use prkdb_client::PrkDbClient;

/// Connect to a cluster, authenticating when the invocation carries a credential.
///
/// `credential` and `admin_token` are separate because they mean different things:
/// `--admin-token` additionally populates the deprecated `admin_token` message field that
/// older servers read, while `--credential` only sets the bearer header. An admin token is
/// still *also* a bearer credential — it is `Admin` on `*` under the authorization model —
/// so it is used as one when no explicit credential is given.
pub async fn connect(
    servers: Vec<String>,
    credential: Option<String>,
    admin_token: Option<String>,
) -> Result<PrkDbClient> {
    let bearer = credential.or_else(|| admin_token.clone());

    let client = match bearer {
        Some(bearer) => PrkDbClient::connect_with_credential(servers, bearer).await?,
        None => PrkDbClient::new(servers).await?,
    };

    // Applied after connecting: this only sets the legacy message field. `with_admin_token`
    // fills the credential via `get_or_insert_with`, so it cannot clobber the bearer that
    // was already used to connect.
    Ok(match admin_token {
        Some(token) => client.with_admin_token(token),
        None => client,
    })
}
