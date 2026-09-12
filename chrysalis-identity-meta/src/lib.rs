/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Meta mTLS identity and Rootcanal trust configuration for Chrysalis.
//!
//! [`load`] reuses the process's installed Meta identity. [`issue_host`] mints a
//! fresh leaf authenticated by that installed identity, while [`issue`] uses a
//! MAST delegated CAT. Since a Chrysalis PID is derived from the leaf
//! certificate, every successful issuance creates a fresh self-certifying PID.
//! The issued combined PEM is captured through a pipe and parsed in memory; the
//! private key is not written to the filesystem.

use std::fs;
use std::io::Cursor;
use std::net::IpAddr;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use chrysalis_transport::QuicIdentity;
use tokio::process::Command;
use x509_parser::extensions::GeneralName;
use x509_parser::prelude::FromDer;
use x509_parser::prelude::X509Certificate;

const THRIFT_TLS_SRV_CA_PATH_ENV: &str = "THRIFT_TLS_SRV_CA_PATH";
const DEFAULT_SRV_CA_PATH: &str = "/var/facebook/rootcanal/ca.pem";
const DEFAULT_SERVER_PEM_PATH: &str = "/var/facebook/x509_identities/server.pem";
const DEFAULT_CERTREQ_PATH: &str = "/bin/certreq";
const DEFAULT_CERTREQ_AUTHENTICATION_PATH: &str = "/var/facebook/tls/server.pem";
const DEFAULT_CERTREQ_CAT_LIST_PATH: &str =
    "/var/facebook/tupperware/tls/mast_signed_secure_group_serialized_cats";
const DEFAULT_CERTREQ_USE_CASE: &str = "ai_training_credentials";
const DEFAULT_CERTREQ_VALIDITY: Duration = Duration::from_secs(7_862_400);
const DEFAULT_HOST_CERTREQ_VALIDITY: Duration = Duration::from_secs(86_400);
const DEFAULT_CERTREQ_CA_TIER: &str = "ProdZeroCA";

/// Files that supply one Meta mTLS identity and its trust roots.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Config {
    ca_path: PathBuf,
    identity_path: PathBuf,
}

impl Config {
    /// Constructs a configuration from explicit CA and combined identity PEM paths.
    pub fn new(ca_path: impl Into<PathBuf>, identity_path: impl Into<PathBuf>) -> Self {
        Self {
            ca_path: ca_path.into(),
            identity_path: identity_path.into(),
        }
    }

    /// Uses the standard Meta paths and Hyperactor-compatible CA override.
    pub fn from_environment() -> Self {
        Self::new(ca_path_from_environment(), DEFAULT_SERVER_PEM_PATH)
    }

    /// Loads and validates the configured mutual TLS identity.
    pub fn load(&self) -> Result<QuicIdentity> {
        let ca_pem = fs::read(&self.ca_path)
            .with_context(|| format!("read CA file {}", self.ca_path.display()))?;
        let identity_pem = fs::read(&self.identity_path)
            .with_context(|| format!("read identity file {}", self.identity_path.display()))?;
        build_identity(
            &ca_pem,
            &identity_pem,
            &self.ca_path.display().to_string(),
            &self.identity_path.display().to_string(),
        )
    }
}

/// Configuration for minting one fresh delegated Meta identity with `certreq`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CertReqConfig {
    ca_path: PathBuf,
    authentication_path: PathBuf,
    cat_list_path: PathBuf,
    use_case: String,
    certreq_path: PathBuf,
    validity: Duration,
}

/// Configuration for minting a fresh leaf from an installed Meta host identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostCertReqConfig {
    ca_path: PathBuf,
    authentication_path: PathBuf,
    use_case: String,
    certreq_path: PathBuf,
    validity: Duration,
}

impl CertReqConfig {
    /// Constructs an issuance configuration from explicit trust and delegation inputs.
    pub fn new(
        ca_path: impl Into<PathBuf>,
        authentication_path: impl Into<PathBuf>,
        cat_list_path: impl Into<PathBuf>,
        use_case: impl Into<String>,
    ) -> Self {
        Self {
            ca_path: ca_path.into(),
            authentication_path: authentication_path.into(),
            cat_list_path: cat_list_path.into(),
            use_case: use_case.into(),
            certreq_path: PathBuf::from(DEFAULT_CERTREQ_PATH),
            validity: DEFAULT_CERTREQ_VALIDITY,
        }
    }

    /// Uses the standard MAST delegated-token inputs and Meta CA paths.
    pub fn from_environment() -> Self {
        Self::new(
            ca_path_from_environment(),
            DEFAULT_CERTREQ_AUTHENTICATION_PATH,
            DEFAULT_CERTREQ_CAT_LIST_PATH,
            DEFAULT_CERTREQ_USE_CASE,
        )
    }

    /// Overrides the `certreq` executable, primarily for packaged deployments and tests.
    pub fn with_certreq_path(mut self, certreq_path: impl Into<PathBuf>) -> Self {
        self.certreq_path = certreq_path.into();
        self
    }

    /// Overrides the requested certificate validity.
    pub fn with_validity(mut self, validity: Duration) -> Self {
        self.validity = validity;
        self
    }

    /// Mints and loads a fresh mutual TLS identity.
    ///
    /// This executes a networked `certreq tls --mode delegated` request. The
    /// caller must provide an active Tokio runtime. Dropping the future kills
    /// the child process, and no private-key material is persisted to disk.
    pub async fn issue(&self) -> Result<QuicIdentity> {
        issue_with_command(&self.ca_path, &self.certreq_path, self.command()).await
    }

    fn command(&self) -> Command {
        let mut command = Command::new(&self.certreq_path);
        command
            .kill_on_drop(true)
            .arg("tls")
            .args(["--mode", "delegated"])
            .arg("--cat-list")
            .arg(&self.cat_list_path)
            .arg("--cert")
            .arg(&self.authentication_path)
            .arg("--combined")
            .arg("--stdout")
            .args(["--key-type", "ecdsa"])
            .args(["--prod-zero-tier", DEFAULT_CERTREQ_CA_TIER])
            .arg("--validity_in_seconds")
            .arg(self.validity.as_secs().to_string())
            .arg("--use-case")
            .arg(&self.use_case);
        command
    }
}

impl HostCertReqConfig {
    /// Constructs a host-authenticated issuance configuration.
    pub fn new(
        ca_path: impl Into<PathBuf>,
        authentication_path: impl Into<PathBuf>,
        use_case: impl Into<String>,
    ) -> Self {
        Self {
            ca_path: ca_path.into(),
            authentication_path: authentication_path.into(),
            use_case: use_case.into(),
            certreq_path: PathBuf::from(DEFAULT_CERTREQ_PATH),
            validity: DEFAULT_HOST_CERTREQ_VALIDITY,
        }
    }

    /// Uses the installed Meta host identity and standard CA paths.
    pub fn from_environment() -> Self {
        Self::new(
            ca_path_from_environment(),
            DEFAULT_SERVER_PEM_PATH,
            DEFAULT_CERTREQ_USE_CASE,
        )
    }

    /// Overrides the `certreq` executable, primarily for tests.
    pub fn with_certreq_path(mut self, certreq_path: impl Into<PathBuf>) -> Self {
        self.certreq_path = certreq_path.into();
        self
    }

    /// Overrides the requested certificate validity.
    pub fn with_validity(mut self, validity: Duration) -> Self {
        self.validity = validity;
        self
    }

    /// Mints a fresh host-authenticated mutual TLS identity.
    pub async fn issue(&self) -> Result<QuicIdentity> {
        issue_with_command(&self.ca_path, &self.certreq_path, self.command()).await
    }

    fn command(&self) -> Command {
        let mut command = Command::new(&self.certreq_path);
        command
            .kill_on_drop(true)
            .arg("tls")
            .args(["--mode", "server"])
            .arg("--cert")
            .arg(&self.authentication_path)
            .arg("--combined")
            .arg("--stdout")
            .args(["--key-type", "ecdsa"])
            .args(["--prod-zero-tier", DEFAULT_CERTREQ_CA_TIER])
            .arg("--validity_in_seconds")
            .arg(self.validity.as_secs().to_string())
            .arg("--use-case")
            .arg(&self.use_case);
        command
    }
}

/// Loads a Chrysalis identity from the standard Meta environment.
pub fn load() -> Result<QuicIdentity> {
    Config::from_environment().load()
}

/// Mints a fresh host-authenticated Chrysalis identity.
pub async fn issue_host() -> Result<QuicIdentity> {
    HostCertReqConfig::from_environment().issue().await
}

/// Mints a fresh Chrysalis identity from the standard MAST environment.
pub async fn issue() -> Result<QuicIdentity> {
    CertReqConfig::from_environment().issue().await
}

async fn issue_with_command(
    ca_path: &Path,
    certreq_path: &Path,
    mut command: Command,
) -> Result<QuicIdentity> {
    let ca_pem = tokio::fs::read(ca_path)
        .await
        .with_context(|| format!("read CA file {}", ca_path.display()))?;
    let output = command
        .output()
        .await
        .with_context(|| format!("execute {}", certreq_path.display()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("certreq exited with {}: {}", output.status, stderr.trim());
    }
    if output.stdout.is_empty() {
        anyhow::bail!("certreq succeeded without producing an identity");
    }
    build_identity(
        &ca_pem,
        &output.stdout,
        &ca_path.display().to_string(),
        "certreq output",
    )
}

fn ca_path_from_environment() -> PathBuf {
    std::env::var_os(THRIFT_TLS_SRV_CA_PATH_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_SRV_CA_PATH))
}

fn build_identity(
    ca_pem: &[u8],
    identity_pem: &[u8],
    ca_source: &str,
    identity_source: &str,
) -> Result<QuicIdentity> {
    let certificates = parse_certificates(identity_pem, identity_source)?;
    let leaf = certificates
        .first()
        .context("metatls identity contains no certificate")?
        .clone();
    let certificate_chain = encode_certificates(&certificates);
    validate_private_key(identity_pem, identity_source)?;
    let roots = parse_certificates(ca_pem, ca_source)?;
    if roots.is_empty() {
        anyhow::bail!("no trusted certificates in {ca_source}");
    }
    let trust_roots = encode_certificates(&roots);
    let server_name = certificate_server_name(leaf.as_ref())?;
    Ok(QuicIdentity::new(
        leaf.as_ref(),
        certificate_chain,
        identity_pem.to_vec(),
        trust_roots,
        server_name,
    )
    .with_udp_destination_server_name())
}

fn certificate_server_name(leaf: &[u8]) -> Result<String> {
    let (_, certificate) =
        X509Certificate::from_der(leaf).context("parse metatls leaf certificate")?;
    let alternative_names = certificate
        .subject_alternative_name()
        .context("parse metatls leaf subject alternative names")?
        .context("metatls leaf certificate has no subject alternative names")?;

    let mut ip_address = None;
    for name in &alternative_names.value.general_names {
        match name {
            GeneralName::DNSName(name) => return Ok((*name).to_owned()),
            GeneralName::IPAddress(bytes) if ip_address.is_none() => {
                ip_address = match bytes.len() {
                    4 => <[u8; 4]>::try_from(*bytes).ok().map(IpAddr::from),
                    16 => <[u8; 16]>::try_from(*bytes).ok().map(IpAddr::from),
                    _ => None,
                };
            }
            _ => {}
        }
    }

    ip_address
        .map(|address| address.to_string())
        .context("metatls leaf certificate has no DNS or IP subject alternative name")
}

fn validate_private_key(pem: &[u8], source: &str) -> Result<()> {
    let mut reader = Cursor::new(pem);
    loop {
        match rustls_pemfile::read_one(&mut reader)
            .with_context(|| format!("parse private key from {source}"))?
        {
            Some(rustls_pemfile::Item::Pkcs1Key(_))
            | Some(rustls_pemfile::Item::Pkcs8Key(_))
            | Some(rustls_pemfile::Item::Sec1Key(_)) => return Ok(()),
            Some(_) => {}
            None => anyhow::bail!("no private key in {source}"),
        }
    }
}

fn parse_certificates(
    pem: &[u8],
    source: &str,
) -> Result<Vec<rustls_pki_types::CertificateDer<'static>>> {
    rustls_pemfile::certs(&mut Cursor::new(pem))
        .collect::<std::io::Result<Vec<_>>>()
        .with_context(|| format!("parse certificates from {source}"))
}

fn encode_certificates(certificates: &[rustls_pki_types::CertificateDer<'_>]) -> Vec<u8> {
    certificates
        .iter()
        .flat_map(|certificate| {
            pem::encode(&pem::Pem::new("CERTIFICATE", certificate.as_ref())).into_bytes()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::fs;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    use chrysalis_transport::certificate_pid;

    use super::*;

    static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

    struct Fixture {
        directory: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let sequence = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
            let directory = std::env::temp_dir().join(format!(
                "chrysalis-identity-meta-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir(&directory).expect("create identity fixture directory");
            Self { directory }
        }

        fn path(&self, name: &str) -> PathBuf {
            self.directory.join(name)
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.directory).expect("remove identity fixture directory");
        }
    }

    #[test]
    fn loads_configured_certificate_key_and_trust_root() {
        let fixture = Fixture::new();
        let credential = rcgen::generate_simple_self_signed(vec!["localhost".into()])
            .expect("generate identity fixture");
        let ca_path = fixture.path("ca.pem");
        let identity_path = fixture.path("identity.pem");
        fs::write(&ca_path, credential.cert.pem()).expect("write fixture CA");
        fs::write(
            &identity_path,
            format!(
                "{}{}",
                credential.cert.pem(),
                credential.signing_key.serialize_pem()
            ),
        )
        .expect("write fixture identity");

        let identity = Config::new(ca_path, identity_path)
            .load()
            .expect("load fixture identity");

        assert_eq!(
            identity.pid(),
            certificate_pid(credential.cert.der().as_ref())
        );
        assert_eq!(identity.certificate_server_name(), "localhost");
    }

    #[test]
    fn uses_ip_subject_alternative_name_for_ip_only_certificate() {
        let fixture = Fixture::new();
        let credential = rcgen::generate_simple_self_signed(vec!["2001:db8::42".into()])
            .expect("generate IP-only identity fixture");
        let ca_path = fixture.path("ca.pem");
        let identity_path = fixture.path("identity.pem");
        fs::write(&ca_path, credential.cert.pem()).expect("write fixture CA");
        fs::write(
            &identity_path,
            format!(
                "{}{}",
                credential.cert.pem(),
                credential.signing_key.serialize_pem()
            ),
        )
        .expect("write fixture identity");

        let identity = Config::new(ca_path, identity_path)
            .load()
            .expect("load fixture identity");

        assert_eq!(identity.certificate_server_name(), "2001:db8::42");
    }

    #[test]
    fn certreq_command_requests_delegated_ecdsa_identity_on_stdout() {
        let config = CertReqConfig::new("/ca.pem", "/auth.pem", "/cats", "test_use_case")
            .with_certreq_path("/certreq")
            .with_validity(Duration::from_secs(42));

        let command = config.command();
        assert_eq!(command.as_std().get_program(), OsStr::new("/certreq"));
        let arguments: Vec<_> = command
            .as_std()
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            arguments,
            vec![
                "tls",
                "--mode",
                "delegated",
                "--cat-list",
                "/cats",
                "--cert",
                "/auth.pem",
                "--combined",
                "--stdout",
                "--key-type",
                "ecdsa",
                "--prod-zero-tier",
                "ProdZeroCA",
                "--validity_in_seconds",
                "42",
                "--use-case",
                "test_use_case",
            ]
        );
    }

    #[test]
    fn certreq_command_requests_host_authenticated_ecdsa_identity_on_stdout() {
        let config = HostCertReqConfig::new("/ca.pem", "/host.pem", "test_use_case")
            .with_certreq_path("/certreq")
            .with_validity(Duration::from_secs(42));

        let command = config.command();
        assert_eq!(command.as_std().get_program(), OsStr::new("/certreq"));
        let arguments: Vec<_> = command
            .as_std()
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            arguments,
            vec![
                "tls",
                "--mode",
                "server",
                "--cert",
                "/host.pem",
                "--combined",
                "--stdout",
                "--key-type",
                "ecdsa",
                "--prod-zero-tier",
                "ProdZeroCA",
                "--validity_in_seconds",
                "42",
                "--use-case",
                "test_use_case",
            ]
        );
    }
}
