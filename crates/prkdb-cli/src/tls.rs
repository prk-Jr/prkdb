//! TLS configuration shared by the HTTP and gRPC surfaces.
//!
//! Closes spec S-02. `start_raft_server_tls` had implemented full mTLS since before this
//! session, but its only caller in the whole workspace was an example — no binary a user
//! actually runs could turn TLS on. A `certs/` directory in the repo implied otherwise.
//!
//! Supplying `--tls-client-ca` requires callers to present a certificate signed by that
//! CA. That is the mechanism D7 chose for Raft peer identity: a peer proves membership by
//! its client certificate rather than by a shared secret that has to be rotated.

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

/// Paths to the PEM material enabling TLS.
#[derive(Clone, Debug)]
pub struct TlsPaths {
    pub cert: PathBuf,
    pub key: PathBuf,
    /// When present, clients must present a certificate signed by this CA (mTLS).
    pub client_ca: Option<PathBuf>,
}

impl TlsPaths {
    /// Build from CLI arguments, returning `None` when TLS was not requested.
    ///
    /// Reads every file eagerly. Failing at startup with a clear message beats binding a
    /// port and discovering the key is unreadable on the first handshake — worse, a
    /// misconfigured server that silently serves plaintext is the failure this whole
    /// requirement exists to prevent.
    pub fn from_args(
        cert: Option<PathBuf>,
        key: Option<PathBuf>,
        client_ca: Option<PathBuf>,
    ) -> Result<Option<Self>> {
        let (cert, key) = match (cert, key) {
            (Some(c), Some(k)) => (c, k),
            (None, None) => return Ok(None),
            // clap's `requires` should make this unreachable; refuse rather than assume.
            _ => anyhow::bail!("--tls-cert and --tls-key must be supplied together"),
        };

        check_readable(&cert, "--tls-cert")?;
        check_readable(&key, "--tls-key")?;
        if let Some(ca) = &client_ca {
            check_readable(ca, "--tls-client-ca")?;
        }

        Ok(Some(Self {
            cert,
            key,
            client_ca,
        }))
    }

    pub fn read_cert(&self) -> Result<Vec<u8>> {
        std::fs::read(&self.cert).with_context(|| format!("reading {}", self.cert.display()))
    }

    pub fn read_key(&self) -> Result<Vec<u8>> {
        std::fs::read(&self.key).with_context(|| format!("reading {}", self.key.display()))
    }

    pub fn read_client_ca(&self) -> Result<Option<Vec<u8>>> {
        self.client_ca
            .as_ref()
            .map(|p| std::fs::read(p).with_context(|| format!("reading {}", p.display())))
            .transpose()
    }

    /// Whether peers must present a client certificate.
    pub fn requires_client_certs(&self) -> bool {
        self.client_ca.is_some()
    }

    /// Build the tonic server TLS configuration.
    pub fn tonic_config(&self) -> Result<tonic::transport::ServerTlsConfig> {
        use tonic::transport::{Certificate, Identity, ServerTlsConfig};

        let identity = Identity::from_pem(self.read_cert()?, self.read_key()?);
        let mut config = ServerTlsConfig::new().identity(identity);

        if let Some(ca) = self.read_client_ca()? {
            config = config.client_ca_root(Certificate::from_pem(ca));
        }
        Ok(config)
    }
}

fn check_readable(path: &Path, flag: &str) -> Result<()> {
    if !path.exists() {
        anyhow::bail!("{flag}: {} does not exist", path.display());
    }
    std::fs::File::open(path)
        .with_context(|| format!("{flag}: {} is not readable", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn temp_pem(dir: &Path, name: &str) -> PathBuf {
        let p = dir.join(name);
        let mut f = std::fs::File::create(&p).unwrap();
        writeln!(f, "-----BEGIN CERTIFICATE-----").unwrap();
        p
    }

    #[test]
    fn absent_flags_mean_no_tls() {
        assert!(TlsPaths::from_args(None, None, None).unwrap().is_none());
    }

    #[test]
    fn half_configured_tls_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let cert = temp_pem(dir.path(), "c.pem");
        assert!(TlsPaths::from_args(Some(cert), None, None).is_err());
    }

    #[test]
    fn missing_file_fails_at_startup_not_at_handshake() {
        let dir = tempfile::tempdir().unwrap();
        let cert = temp_pem(dir.path(), "c.pem");
        let err = TlsPaths::from_args(Some(cert), Some(dir.path().join("absent.pem")), None)
            .expect_err("an unreadable key must be refused before binding");
        assert!(err.to_string().contains("does not exist"), "{err}");
    }

    #[test]
    fn client_ca_switches_on_mtls() {
        let dir = tempfile::tempdir().unwrap();
        let cert = temp_pem(dir.path(), "c.pem");
        let key = temp_pem(dir.path(), "k.pem");
        let ca = temp_pem(dir.path(), "ca.pem");

        let without = TlsPaths::from_args(Some(cert.clone()), Some(key.clone()), None)
            .unwrap()
            .unwrap();
        assert!(!without.requires_client_certs());

        let with = TlsPaths::from_args(Some(cert), Some(key), Some(ca))
            .unwrap()
            .unwrap();
        assert!(with.requires_client_certs());
    }
}
