//! Raft peer authentication.
//!
//! `RaftService` carries the five inter-node RPCs — `RequestVote`, `PreVote`,
//! `AppendEntries`, `InstallSnapshot`, `ReadIndex`. It is a separate gRPC service from
//! `PrkDbService`, so tonic can apply a different policy to each, but it shares a server
//! and port with the client API.
//!
//! **Leaving it open because "only peers call it" is not safe.** Any client that can reach
//! the port could forge `AppendEntries` and rewrite the log. `ReadIndex` matters just as
//! much and is easier to overlook: it is the mechanism behind linearizable follower reads,
//! so forging it breaks the guarantee those reads advertise.
//!
//! D7 chose mTLS client certificates over a shared cluster secret: no secret to rotate,
//! encryption in transit comes along with it, and the machinery is what `--tls-client-ca`
//! already builds.

use tonic::{Request, Status};

/// How a caller proves it is a cluster peer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PeerIdentity {
    /// The TLS layer verified a client certificate against the cluster CA.
    ///
    /// rustls has already validated the chain by the time a request arrives, so this
    /// extracts identity from an already-trusted certificate rather than verifying trust.
    /// The corollary matters: **without `--tls-client-ca` configured, any self-signed
    /// certificate produces a `Some`**, so the CA must be set for this to mean anything.
    MutualTls,

    /// A shared secret, D7's rejected alternative. Kept because a cluster that cannot
    /// deploy certificates should degrade to something rather than to nothing — but it is
    /// weaker: one leak compromises every node and rotation means a rolling restart.
    ClusterSecret(String),

    /// Peer authentication disabled. Development only.
    Disabled,
}

impl PeerIdentity {
    /// Pick the strongest policy the node is configured for.
    ///
    /// mTLS wins when a cluster CA is configured, because it is the only option that also
    /// authenticates the *server* to the peer and needs no shared material on disk. The
    /// cluster secret is the fallback for deployments that cannot issue certificates.
    ///
    /// Returning `Disabled` is not a decision this function is entitled to make quietly —
    /// callers are expected to refuse to serve a multi-node cluster in that state, and
    /// [`Self::is_disabled`] exists so they can check without matching.
    pub fn from_config(client_ca_configured: bool, cluster_secret: Option<String>) -> Self {
        match (client_ca_configured, cluster_secret) {
            (true, _) => PeerIdentity::MutualTls,
            (false, Some(secret)) if !secret.is_empty() => PeerIdentity::ClusterSecret(secret),
            _ => PeerIdentity::Disabled,
        }
    }

    /// Whether this policy needs TLS on both ends to work at all.
    ///
    /// mTLS is not merely "TLS available": peers must present certificates *to each
    /// other*, so the dialling side needs its own cert and key. A node that enables the
    /// policy without them starts and then cannot form a cluster.
    pub fn requires_tls(&self) -> bool {
        matches!(self, PeerIdentity::MutualTls)
    }

    pub fn is_disabled(&self) -> bool {
        matches!(self, PeerIdentity::Disabled)
    }

    /// Short description for startup logs, so an operator can see which policy is active
    /// without inferring it from which flags they passed.
    pub fn describe(&self) -> &'static str {
        match self {
            PeerIdentity::MutualTls => "mutual TLS (client certificate signed by the cluster CA)",
            PeerIdentity::ClusterSecret(_) => "shared cluster secret",
            PeerIdentity::Disabled => "disabled",
        }
    }
}

/// Authenticates the five `RaftService` RPCs.
#[derive(Clone)]
pub struct PeerAuthInterceptor {
    identity: PeerIdentity,
}

impl PeerAuthInterceptor {
    pub fn new(identity: PeerIdentity) -> Self {
        Self { identity }
    }

    /// Whether TLS is required for this policy to be enforceable.
    pub fn requires_tls(&self) -> bool {
        matches!(self.identity, PeerIdentity::MutualTls)
    }

    /// Decide one peer request.
    ///
    /// `peer_certs_present` reports whether the transport saw a verified client
    /// certificate; `presented_secret` carries the cluster secret when that mode is used.
    #[allow(clippy::result_large_err)]
    pub fn check(
        &self,
        peer_certs_present: bool,
        presented_secret: Option<&str>,
    ) -> Result<(), Status> {
        match &self.identity {
            PeerIdentity::Disabled => Ok(()),

            PeerIdentity::MutualTls => {
                if peer_certs_present {
                    Ok(())
                } else {
                    Err(Status::unauthenticated(
                        "Raft RPCs require a client certificate signed by the cluster CA",
                    ))
                }
            }

            PeerIdentity::ClusterSecret(expected) => {
                let presented = presented_secret.ok_or_else(|| {
                    Status::unauthenticated("Raft RPCs require the cluster secret")
                })?;
                // Constant-time: a byte-wise compare leaks the secret through timing just
                // as it would for a user credential.
                use subtle::ConstantTimeEq;
                let ok = expected.len() == presented.len()
                    && bool::from(expected.as_bytes().ct_eq(presented.as_bytes()));
                if ok {
                    Ok(())
                } else {
                    Err(Status::unauthenticated("invalid cluster secret"))
                }
            }
        }
    }
}

impl tonic::service::Interceptor for PeerAuthInterceptor {
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        // `peer_certs` returns Some only on the server side of a TLS connection whose
        // client presented a certificate. It requires tonic's "server" and "tls" features,
        // both of which this workspace enables.
        let certs_present = req.peer_certs().is_some_and(|c| !c.is_empty());

        let secret = req
            .metadata()
            .get("x-prkdb-cluster-secret")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        self.check(certs_present, secret.as_deref())?;
        Ok(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mtls_requires_a_client_certificate() {
        let auth = PeerAuthInterceptor::new(PeerIdentity::MutualTls);

        auth.check(true, None).expect("a verified peer is accepted");

        let err = auth
            .check(false, None)
            .expect_err("a caller with no client certificate is not a peer");
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    /// The shortcut this module exists to prevent: a client forging AppendEntries.
    #[test]
    fn a_plain_client_cannot_pass_as_a_peer() {
        let auth = PeerAuthInterceptor::new(PeerIdentity::MutualTls);
        assert!(
            auth.check(false, Some("guessed-secret")).is_err(),
            "under mTLS a metadata secret must not substitute for a certificate"
        );
    }

    #[test]
    fn cluster_secret_mode_compares_exactly() {
        let auth = PeerAuthInterceptor::new(PeerIdentity::ClusterSecret("s3cret".into()));

        auth.check(false, Some("s3cret")).expect("exact match");
        assert!(auth.check(false, Some("s3cre")).is_err());
        assert!(auth.check(false, Some("s3cret ")).is_err());
        assert!(auth.check(false, None).is_err());
    }

    /// A wrong secret of the *right length* must still be refused.
    ///
    /// # The bypass this exists for
    ///
    /// The comparison is
    ///
    /// ```text
    /// expected.len() == presented.len() && ct_eq(expected, presented)
    /// ```
    ///
    /// Changing that `&&` to `||` makes a length match sufficient on its own, so **any**
    /// six-character string authenticates as the cluster secret — full peer authentication
    /// bypass, and with it the authority to forge AppendEntries and rewrite the log.
    ///
    /// Every case in the test above uses a secret of the wrong length (`"s3cre"` is 5,
    /// `"s3cret "` is 7), so all of them fail under the mutant for the wrong reason and it
    /// survived the suite (mutation run 31358158012, shard 4). Same length, different
    /// bytes is the only input that separates the two operators.
    #[test]
    fn a_same_length_wrong_secret_is_refused() {
        let auth = PeerAuthInterceptor::new(PeerIdentity::ClusterSecret("s3cret".into()));

        for wrong in ["s3creT", "S3cret", "xxxxxx", "s3crXt", "aaaaaa"] {
            assert_eq!(
                wrong.len(),
                "s3cret".len(),
                "test input must be the same length"
            );
            let err = auth
                .check(false, Some(wrong))
                .expect_err("a same-length wrong secret must not authenticate");
            assert_eq!(err.code(), tonic::Code::Unauthenticated);
        }

        // The empty secret against an empty expectation: lengths match and so do bytes.
        let empty = PeerAuthInterceptor::new(PeerIdentity::ClusterSecret(String::new()));
        assert!(
            empty.check(false, Some("x")).is_err(),
            "a non-empty secret must not match an empty expectation"
        );
    }

    /// `describe` names the active policy for the startup log.
    ///
    /// Replacing it with `""` or `"xyzzy"` survived: an operator reads this line to learn
    /// which policy is in force, and a blank or wrong answer there is how a cluster gets
    /// run with authentication the operator believes is stronger than it is.
    #[test]
    fn describe_names_the_active_policy() {
        assert!(PeerIdentity::MutualTls.describe().contains("mutual TLS"));
        assert!(PeerIdentity::ClusterSecret("s".into())
            .describe()
            .contains("secret"));
        assert_eq!(PeerIdentity::Disabled.describe(), "disabled");

        // All three must be distinguishable from one another.
        let all = [
            PeerIdentity::MutualTls.describe(),
            PeerIdentity::ClusterSecret("s".into()).describe(),
            PeerIdentity::Disabled.describe(),
        ];
        for d in all {
            assert!(!d.is_empty(), "a policy description must not be blank");
        }
        assert_ne!(all[0], all[1]);
        assert_ne!(all[1], all[2]);
    }

    /// `requires_tls` is true for mTLS and false for the other two.
    ///
    /// Both constant replacements survived. A caller uses this to refuse to start a
    /// multi-node cluster whose policy cannot be enforced, so a constant `false` starts a
    /// cluster that authenticates nothing and a constant `true` refuses to start one that
    /// is correctly configured for a shared secret.
    #[test]
    fn requires_tls_is_true_only_for_mutual_tls() {
        assert!(PeerIdentity::MutualTls.requires_tls());
        assert!(!PeerIdentity::ClusterSecret("s".into()).requires_tls());
        assert!(!PeerIdentity::Disabled.requires_tls());
    }

    #[test]
    fn disabled_accepts_everything_and_says_it_needs_no_tls() {
        let auth = PeerAuthInterceptor::new(PeerIdentity::Disabled);
        auth.check(false, None).expect("disabled permits all");
        assert!(!auth.requires_tls());
    }

    #[test]
    fn mtls_declares_its_tls_requirement() {
        assert!(PeerAuthInterceptor::new(PeerIdentity::MutualTls).requires_tls());
        assert!(!PeerAuthInterceptor::new(PeerIdentity::ClusterSecret("x".into())).requires_tls());
    }
}
