/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Workload fixture, ephemeral PKI, HTTP helpers, and generic topology
//! traversal.
//!
//! This module owns transport and process lifecycle. Scenario types
//! (`DiningScenario`, `PyspyScenario`) own workload-specific interpretation.
//!
//! See the crate root (`main.rs`) for the MIT invariant registry.

use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use hyperactor_mesh::introspect::NodePayload;
use reqwest::Client;
use reqwest::Response;
use serde::de::DeserializeOwned;
use tempfile::TempDir;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::process::Child;
use tokio::process::Command;

/// Classified service and worker proc references. See MIT-7
/// (proc-classification).
pub(crate) struct ClassifiedProcs {
    pub(crate) service: String,
    pub(crate) worker: String,
}

/// A running workload process with its HTTP client and ephemeral PKI.
///
/// Cleanup layers (see MIT-2):
/// 1. `shutdown()` — async kill + reap (primary path, called by
///    scenario `run`)
/// 2. `Drop` — synchronous `start_kill()` fallback (panic, early
///    return)
pub(crate) struct WorkloadFixture {
    child: Mutex<Option<Child>>,
    pub(crate) admin_url: String,
    pub(crate) client: Client,
    ca_pem: Vec<u8>,
    _cert_dir: TempDir,
    _pyspy_dir: Option<TempDir>,
}

impl WorkloadFixture {
    /// Explicitly kill and reap the child process. MIT-2
    /// (scoped-cleanup).
    pub(crate) async fn shutdown(&self) {
        let child = self.child.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(mut child) = child {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
    }

    /// Build a client that trusts the test CA but presents no client
    /// cert. For MIT-6 (mtls-rejection) testing: the failure should
    /// come from the server requiring a client cert, not from the
    /// client rejecting the server's certificate.
    pub(crate) fn build_unauthenticated_client(&self) -> Result<Client> {
        let builder = Client::builder().timeout(Duration::from_secs(30));
        let (builder, ok) =
            hyperactor_mesh::mesh_admin_client::add_tls(builder, &self.ca_pem, None, None);
        if !ok {
            bail!("MIT-6: failed to configure TLS for unauthenticated client");
        }
        Ok(builder.build()?)
    }

    /// The fixture's CA PEM, for building custom TLS clients.
    pub(crate) fn ca_pem(&self) -> &[u8] {
        &self.ca_pem
    }

    /// GET a path relative to the admin URL.
    pub(crate) async fn get(&self, path: &str) -> Result<Response> {
        let url = format!("{}{}", self.admin_url, path);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        Ok(resp)
    }

    /// GET a path and deserialize the JSON response. MIT-9
    /// (success-typing).
    ///
    /// Single-shot: no retries. Discovery paths (`classify_procs`,
    /// `discover_pyspy_workers`) have their own retry loops and
    /// tolerate transient failures. Assertion paths get one honest
    /// attempt — if the endpoint fails under load, that is a real
    /// product signal.
    pub(crate) async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = format!("{}{}", self.admin_url, path);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            bail!("GET {url}: HTTP {status}: {body}");
        }
        serde_json::from_str(&body)
            .with_context(|| format!("deserialize response from GET {url} (HTTP {status}): {body}"))
    }

    /// Walk root → hosts → procs → actors and classify service vs
    /// worker procs. MIT-7 (proc-classification).
    ///
    /// Retries with backoff. Fails if both proc types are not found
    /// within the retry budget (15 attempts, ~30s).
    pub(crate) async fn classify_procs(&self) -> Result<ClassifiedProcs> {
        let mut service = None;
        let mut worker = None;

        for _attempt in 1..=15 {
            let root: NodePayload = match self.get_json("/v1/root").await {
                Ok(r) => r,
                Err(_) => {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };

            for host_ref in &root.children {
                let encoded = urlencoding::encode(host_ref);
                let host: NodePayload = match self.get_json(&format!("/v1/{encoded}")).await {
                    Ok(h) => h,
                    Err(_) => continue,
                };

                for proc_ref in &host.children {
                    let encoded = urlencoding::encode(proc_ref);
                    let proc_node: NodePayload =
                        match self.get_json(&format!("/v1/{encoded}")).await {
                            Ok(p) => p,
                            Err(_) => continue,
                        };

                    let actor_names: Vec<String> = proc_node
                        .children
                        .iter()
                        .map(|r| {
                            let name = r.rsplit(',').next().unwrap_or(r);
                            name.split('[').next().unwrap_or(name).to_string()
                        })
                        .collect();

                    if actor_names.iter().any(|n| n == "host_agent") {
                        service = Some(proc_ref.clone());
                    } else if actor_names.iter().any(|n| n == "proc_agent") && worker.is_none() {
                        worker = Some(proc_ref.clone());
                    }
                }
            }

            if service.is_some() && worker.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }

        Ok(ClassifiedProcs {
            service: service.ok_or_else(|| {
                anyhow::anyhow!("MIT-7: service proc not found after 15 attempts")
            })?,
            worker: worker
                .ok_or_else(|| anyhow::anyhow!("MIT-7: worker proc not found after 15 attempts"))?,
        })
    }
}

/// MIT-2: Drop fallback — synchronous best-effort kill.
impl Drop for WorkloadFixture {
    fn drop(&mut self) {
        if let Some(mut child) = self
            .child
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            let _ = child.start_kill();
        }
    }
}

// Binary resolution

/// Resolve the Rust dining_philosophers binary via Buck resources.
pub(crate) fn dining_philosophers_rust_binary() -> PathBuf {
    buck_resources::get("monarch/hyperactor_mesh/dining_philosophers_rs")
        .expect("dining_philosophers_rust resource not found")
        .to_path_buf()
}

/// Resolve the Python dining_philosophers binary via Buck resources.
pub(crate) fn dining_philosophers_python_binary() -> PathBuf {
    buck_resources::get("monarch/hyperactor_mesh/dining_philosophers_py")
        .expect("dining_philosophers_python resource not found")
        .to_path_buf()
}

/// Resolve the pyspy_workload binary via Buck resources.
pub(crate) fn pyspy_workload_binary() -> PathBuf {
    buck_resources::get("monarch/hyperactor_mesh/pyspy_workload")
        .expect("pyspy_workload resource not found")
        .to_path_buf()
}

// Workload launch

/// Start a workload binary and wait for the mesh admin server to
/// become ready.
///
/// Enforces MIT-1 (fixture-readiness), MIT-4 (ephemeral-pki), MIT-5
/// (mtls-required). See MIT-3 for why we do NOT use PR_SET_PDEATHSIG.
pub(crate) async fn start_workload(
    binary: &Path,
    args: &[&str],
    timeout: Duration,
) -> Result<WorkloadFixture> {
    // MIT-4: Generate ephemeral PKI via rcgen.
    let cert_dir = TempDir::new()?;
    let pki = generate_pki(cert_dir.path())?;

    let combined_path = cert_dir.path().join("combined.pem");
    let ca_path = cert_dir.path().join("ca.crt");
    let (pyspy_bin, pyspy_dir) = install_pyspy()?;

    let mut cmd = Command::new(binary);
    cmd.args(args)
        .env("HYPERACTOR_TLS_CERT", &combined_path)
        .env("HYPERACTOR_TLS_KEY", &combined_path)
        .env("HYPERACTOR_TLS_CA", &ca_path)
        .env("HYPERACTOR_MESH_ADMIN_ADDR", "[::]:0")
        .stdout(std::process::Stdio::piped());

    // Match the old shell tests: prefer an fbpkg-fetched py-spy and
    // fall back to whatever is already on PATH if the fetch fails.
    if let Some(py_spy) = &pyspy_bin {
        cmd.env("PYSPY_BIN", py_spy);
    }

    // NOTE: We intentionally do NOT set PR_SET_PDEATHSIG here. See
    // MIT-3 (no-shared-fixtures) in this module doc.

    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to spawn {}", binary.display()))?;

    // MIT-1: Wait for the admin URL sentinel on stdout.
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("child stdout not captured"))?;
    let mut reader = BufReader::new(stdout).lines();

    let sentinel = "Mesh admin server listening on ";
    let sentinel_result = tokio::time::timeout(timeout, async {
        while let Some(line) = reader.next_line().await? {
            if let Some(url) = line.strip_prefix(sentinel) {
                return Ok::<String, anyhow::Error>(url.trim().to_string());
            }
        }
        bail!("MIT-1: workload exited without printing admin URL sentinel");
    })
    .await;

    let admin_url = match sentinel_result {
        Ok(Ok(url)) => url,
        Ok(Err(e)) => {
            let _ = child.start_kill();
            return Err(e);
        }
        Err(_) => {
            let _ = child.start_kill();
            bail!(
                "MIT-1: admin URL sentinel not observed within {}s",
                timeout.as_secs(),
            );
        }
    };

    // Drain remaining stdout in background to prevent pipe deadlock.
    tokio::spawn(async move { while let Ok(Some(_)) = reader.next_line().await {} });

    // MIT-5: Build reqwest client with test CA and client cert.
    let client = build_client(&pki.ca_pem, &pki.cert_pem, &pki.key_pem)?;

    Ok(WorkloadFixture {
        child: Mutex::new(Some(child)),
        admin_url,
        client,
        ca_pem: pki.ca_pem,
        _cert_dir: cert_dir,
        _pyspy_dir: pyspy_dir,
    })
}

fn install_pyspy() -> Result<(Option<PathBuf>, Option<TempDir>)> {
    let dir = TempDir::new()?;
    let status = std::process::Command::new("fbpkg")
        .arg("fetch")
        .arg("fb-py-spy:prod")
        .arg("-d")
        .arg(dir.path())
        .stderr(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .status();

    match status {
        Ok(status) if status.success() => {
            let pyspy = dir.path().join("py-spy");
            if pyspy.exists() {
                Ok((Some(pyspy), Some(dir)))
            } else {
                Ok((None, None))
            }
        }
        Ok(_) | Err(_) => Ok((None, None)),
    }
}

// PKI generation

/// Generated PEM material from ephemeral PKI. MIT-4 (ephemeral-pki).
pub(crate) struct PkiMaterial {
    pub(crate) ca_pem: Vec<u8>,
    pub(crate) cert_pem: Vec<u8>,
    pub(crate) key_pem: Vec<u8>,
}

/// Generate ephemeral CA + server cert with rcgen. MIT-4
/// (ephemeral-pki).
///
/// Returns separate CA, cert, and key PEM buffers. Also writes ca.crt
/// and combined.pem to the cert_dir (the combined file is what the
/// workload process reads via HYPERACTOR_TLS_CERT/KEY).
pub(crate) fn generate_pki(cert_dir: &Path) -> Result<PkiMaterial> {
    use rcgen::BasicConstraints;
    use rcgen::CertificateParams;
    use rcgen::DnType;
    use rcgen::IsCa;
    use rcgen::Issuer;
    use rcgen::KeyPair;
    use rcgen::SanType;

    // Generate CA key + self-signed cert.
    let ca_key = KeyPair::generate()?;
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "test-ca");
    let ca_cert = ca_params.self_signed(&ca_key)?;

    // Build an Issuer from the CA params + key for signing the server
    // cert.
    let issuer = Issuer::from_params(&ca_params, &ca_key);

    // Generate server/client key + cert signed by CA.
    let hostname = hostname::get()
        .unwrap_or_else(|_| "localhost".into())
        .to_string_lossy()
        .to_string();

    let server_key = KeyPair::generate()?;
    let mut server_params = CertificateParams::new(vec!["localhost".to_string(), hostname])?;
    server_params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    server_params
        .subject_alt_names
        .push(SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)));
    server_params
        .subject_alt_names
        .push(SanType::IpAddress(IpAddr::V6(Ipv6Addr::LOCALHOST)));
    let server_cert = server_params.signed_by(&server_key, &issuer)?;

    let ca_pem = ca_cert.pem();
    let server_pem = server_cert.pem();
    let server_key_pem = server_key.serialize_pem();

    // combined.pem = server_cert + ca_cert + server_key (matches
    // Meta's server.pem format). The workload process reads this via
    // HYPERACTOR_TLS_CERT and HYPERACTOR_TLS_KEY.
    let combined = format!("{server_pem}{ca_pem}{server_key_pem}");

    std::fs::write(cert_dir.join("ca.crt"), ca_pem.as_bytes())?;
    std::fs::write(cert_dir.join("combined.pem"), combined.as_bytes())?;

    let cert_chain_pem = format!("{server_pem}{ca_pem}").into_bytes();
    Ok(PkiMaterial {
        ca_pem: ca_pem.into_bytes(),
        cert_pem: cert_chain_pem,
        key_pem: server_key_pem.into_bytes(),
    })
}

/// Build a reqwest::Client with the test CA and client cert. MIT-5
/// (mtls-required).
///
/// Full TLS verification is on: the client validates the server cert
/// chain against the ephemeral test CA. Both the server URL and the
/// cert SANs derive from `hostname::get()`, so hostname verification
/// succeeds.
///
/// Delegates to [`hyperactor_mesh::mesh_admin_client::add_tls`] for
/// the fbcode/OSS native-tls vs rustls identity split.
fn build_client(ca_pem: &[u8], cert_pem: &[u8], key_pem: &[u8]) -> Result<Client> {
    let builder = Client::builder()
        // Per-request timeout prevents any single HTTP call from
        // hanging indefinitely if the server stops responding without
        // closing.
        .timeout(Duration::from_secs(30));

    let (builder, ok) = hyperactor_mesh::mesh_admin_client::add_tls(
        builder,
        ca_pem,
        Some(cert_pem.to_vec()),
        Some(key_pem.to_vec()),
    );
    if !ok {
        bail!("MIT-5: failed to configure TLS on reqwest client");
    }
    Ok(builder.build()?)
}

/// Build a client with explicit trust root and optional client
/// identity. Covers both "wrong client cert" (trust fixture CA,
/// present foreign cert) and "wrong trust root" (trust foreign CA,
/// present foreign cert).
pub(crate) fn build_tls_client(
    trusted_ca_pem: &[u8],
    cert_pem: Option<&[u8]>,
    key_pem: Option<&[u8]>,
) -> Result<Client> {
    let builder = Client::builder().timeout(Duration::from_secs(30));
    let (builder, ok) = hyperactor_mesh::mesh_admin_client::add_tls(
        builder,
        trusted_ca_pem,
        cert_pem.map(|b| b.to_vec()),
        key_pem.map(|b| b.to_vec()),
    );
    if !ok {
        bail!("failed to configure TLS on custom client");
    }
    Ok(builder.build()?)
}
