/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Shared TLS-aware `reqwest` client construction for mesh-admin
//! HTTP clients.
//!
//! The mesh admin server requires mutual TLS. Building a correct
//! `reqwest::Client` against it is subtle because fbcode Buck builds
//! compile both `native-tls` and `rustls` features into reqwest, and
//! the two backends use incompatible `Identity` constructors.
//!
//! This module centralizes that logic so that every mesh-admin client
//! (admin TUI, integration tests, future tooling) shares the same
//! correct code path.
//!
//! # Transport selection (highest priority first)
//!
//! 0. `--plaintext`: plain HTTP, ignoring any TLS material.
//! 1. Explicit `http://` scheme: plain HTTP. Combining it with
//!    `--tls-ca` / `--tls-cert` / `--tls-key` is a hard error.
//! 2. Explicit `--tls-ca` (with optional `--tls-cert` + `--tls-key`
//!    for mutual TLS): an unreadable or invalid CA, a cert/key that
//!    cannot be read, or only one of cert/key, is a hard error.
//!    `--tls-cert`/`--tls-key` without `--tls-ca` is also an error,
//!    since they are unusable without a CA.
//! 3. Auto-detection via [`hyperactor::channel::try_tls_pem_bundle`]
//!    (OSS config first, then Meta well-known paths).
//! 4. If no CA is found and TLS is expected (an `https://` scheme or,
//!    at Meta, the default), that is a hard error; otherwise (OSS with
//!    no certs) fall back to plain HTTP.
//!
//! Transport is **fail-closed**: when TLS is expected but cannot be
//! configured, [`build_client`] returns an error rather than silently
//! downgrading to plain HTTP.

use std::io;

/// Connection options for a mesh admin server, mirroring the
/// `--tls-ca`, `--tls-cert`, `--tls-key`, and `--plaintext` CLI flags.
#[derive(Debug, Clone, Default)]
pub struct AdminClientOptions {
    /// Admin server address: `host:port`, `https://host:port`, or
    /// `http://host:port`.
    pub addr: String,
    /// Path to a PEM CA certificate for TLS server verification.
    pub tls_ca: Option<String>,
    /// Path to a PEM client certificate for mutual TLS.
    pub tls_cert: Option<String>,
    /// Path to a PEM client private key for mutual TLS.
    pub tls_key: Option<String>,
    /// Disable TLS and use plain HTTP, overriding TLS auto-detection.
    pub plaintext: bool,
}

/// Configure TLS on a `reqwest::ClientBuilder` by adding a root CA,
/// and optionally a client identity (cert + key) for mutual TLS.
///
/// - `ca_bytes` must be a PEM-encoded CA certificate; if it cannot be
///   parsed, this returns `(builder, false)` and leaves the builder
///   unchanged.
/// - If both `cert_bytes` and `key_bytes` are provided, they are
///   concatenated and parsed as a PEM identity. Identity parse
///   failures are non-fatal: the root CA remains installed and the
///   function still returns `true`.
///
/// Returns `(updated_builder, ca_installed)`.
pub fn add_tls(
    builder: reqwest::ClientBuilder,
    ca_bytes: &[u8],
    cert_bytes: Option<Vec<u8>>,
    key_bytes: Option<Vec<u8>>,
) -> (reqwest::ClientBuilder, bool) {
    let root_cert = match reqwest::Certificate::from_pem(ca_bytes) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("TLS: invalid CA PEM: {}", e);
            return (builder, false);
        }
    };
    let mut builder = builder.add_root_certificate(root_cert);

    if let (Some(cert), Some(key)) = (cert_bytes, key_bytes) {
        // reqwest's Identity type is backend-specific: from_pkcs8_pem
        // creates a native-tls identity, from_pem creates a rustls
        // identity. When both features are compiled (fbcode Buck builds),
        // using the wrong variant silently fails at connect time with
        // "incompatible TLS identity type".
        //
        // Meta's server.pem bundles certs + key in one file.
        // from_pkcs8_pem requires the key as a separate buffer, so we
        // split it out by finding the private key marker.
        let combined = if cert == key {
            cert
        } else {
            let mut c = cert;
            c.extend_from_slice(&key);
            c
        };
        let identity_result = {
            // Split PEM into cert-only and key-only buffers for native-tls.
            // reqwest 0.11 with both native-tls and rustls features compiled
            // (fbcode Buck builds) defaults to the native-tls connector.
            // Identity::from_pem creates a rustls-flavored identity that is
            // silently rejected by native-tls at connect time. We must use
            // from_pkcs8_pem (native-tls) in fbcode, and from_pem (rustls)
            // in OSS where native-tls is excluded (D93626607).
            let combined_str = String::from_utf8_lossy(&combined);
            let key_markers = [
                // @lint-ignore PRIVATEKEY
                "-----BEGIN PRIVATE KEY-----",
                // @lint-ignore PRIVATEKEY
                "-----BEGIN RSA PRIVATE KEY-----",
                // @lint-ignore PRIVATEKEY
                "-----BEGIN EC PRIVATE KEY-----",
            ];
            let key_pos = key_markers
                .iter()
                .filter_map(|m| combined_str.find(m))
                .min();
            #[cfg(fbcode_build)]
            {
                if let Some(key_start) = key_pos {
                    let cert_pem = combined_str[..key_start].trim().as_bytes();
                    let key_pem = combined_str[key_start..].trim().as_bytes();
                    reqwest::Identity::from_pkcs8_pem(cert_pem, key_pem)
                } else {
                    reqwest::Identity::from_pem(&combined)
                }
            }
            #[cfg(not(fbcode_build))]
            {
                let _ = key_pos; // suppress unused warning
                reqwest::Identity::from_pem(&combined)
            }
        };
        match identity_result {
            Ok(identity) => {
                builder = builder.identity(identity);
            }
            Err(e) => eprintln!(
                "WARNING: TLS: failed to parse client identity PEM: {}. \
                 The mesh admin server requires mTLS — connection will fail \
                 without a valid client certificate.",
                e
            ),
        }
    }

    (builder, true)
}

/// Read all bytes from a [`Pem`](hyperactor::config::Pem), returning
/// `None` if it can't be opened/read or if the result is empty.
fn read_pem(pem: &hyperactor::config::Pem) -> Option<Vec<u8>> {
    use std::io::Read;
    let mut buf = Vec::new();
    pem.reader().ok()?.read_to_end(&mut buf).ok()?;
    if buf.is_empty() { None } else { Some(buf) }
}

/// Split an address into an optional `http`/`https` scheme and the
/// remaining host part.
///
/// If `addr` starts with `https://` or `http://`, returns
/// `(Some(scheme), rest)`. Otherwise returns `(None, addr)`. The
/// returned `&str` values are slices of `addr`.
fn parse_addr(addr: &str) -> (Option<&str>, &str) {
    if let Some(host) = addr.strip_prefix("https://") {
        (Some("https"), host)
    } else if let Some(host) = addr.strip_prefix("http://") {
        (Some("http"), host)
    } else {
        (None, addr)
    }
}

/// Configure TLS on a `reqwest::ClientBuilder` using PEM files
/// supplied via CLI (`--tls-ca`, and optionally `--tls-cert` +
/// `--tls-key`).
///
/// Fail-closed: an unreadable or invalid CA is an error, as is a
/// `--tls-cert`/`--tls-key` that cannot be read or that is supplied
/// without its counterpart. (A cert/key that reads but fails to parse
/// is handled by [`add_tls`], which keeps CA-only TLS; that still fails
/// the handshake against an mTLS server rather than downgrading to
/// plain HTTP.)
fn add_tls_from_paths(
    builder: reqwest::ClientBuilder,
    ca_path: &str,
    cert_path: Option<&str>,
    key_path: Option<&str>,
) -> io::Result<reqwest::ClientBuilder> {
    let ca_bytes = std::fs::read(ca_path)
        .map_err(|e| io::Error::other(format!("cannot read TLS CA file {ca_path}: {e}")))?;

    let (cert_bytes, key_bytes) = match (cert_path, key_path) {
        (Some(cert), Some(key)) => {
            let cert_bytes = std::fs::read(cert)
                .map_err(|e| io::Error::other(format!("cannot read TLS cert file {cert}: {e}")))?;
            let key_bytes = std::fs::read(key)
                .map_err(|e| io::Error::other(format!("cannot read TLS key file {key}: {e}")))?;
            (Some(cert_bytes), Some(key_bytes))
        }
        (None, None) => (None, None),
        _ => {
            return Err(io::Error::other(
                "--tls-cert and --tls-key must be provided together",
            ));
        }
    };

    let (builder, ca_installed) = add_tls(builder, &ca_bytes, cert_bytes, key_bytes);
    if !ca_installed {
        return Err(io::Error::other(format!(
            "TLS CA file {ca_path} is not a valid PEM certificate"
        )));
    }
    Ok(builder)
}

/// Configure TLS on a `reqwest::ClientBuilder` using an auto-detected
/// hyperactor [`PemBundle`](hyperactor::config::PemBundle).
///
/// Fail-closed: a bundle whose CA is missing or is not a valid PEM is
/// an error. A client cert/key are applied when both are readable.
fn add_tls_from_bundle(
    builder: reqwest::ClientBuilder,
    bundle: &hyperactor::config::PemBundle,
) -> io::Result<reqwest::ClientBuilder> {
    let ca_bytes = read_pem(&bundle.ca)
        .ok_or_else(|| io::Error::other("auto-detected TLS CA is missing or unreadable"))?;

    let (builder, ca_installed) = add_tls(
        builder,
        &ca_bytes,
        read_pem(&bundle.cert),
        read_pem(&bundle.key),
    );
    if !ca_installed {
        return Err(io::Error::other(
            "auto-detected TLS CA is not a valid PEM certificate",
        ));
    }
    Ok(builder)
}

/// Build a `reqwest` client and a base URL for a mesh admin server.
///
/// Returns `(base_url, client)` where `base_url` always includes the
/// selected scheme (`http://...` or `https://...`). See the module
/// docs for the transport-selection order.
///
/// Transport is **fail-closed**: when TLS is expected (an `https://`
/// scheme, an explicit `--tls-ca`, or the Meta default) but no usable
/// CA can be configured, this returns an error instead of silently
/// using plain HTTP.
pub fn build_client(config: &AdminClientOptions) -> io::Result<(String, reqwest::Client)> {
    let (explicit_scheme, host) = parse_addr(&config.addr);
    let has_tls_paths =
        config.tls_ca.is_some() || config.tls_cert.is_some() || config.tls_key.is_some();

    // TP-7: no client-level timeout. All timeout enforcement is
    // per-operation via tokio::time::timeout at the call boundary.

    // --plaintext is the explicit override: plain HTTP, ignore any TLS material.
    if config.plaintext {
        let client = reqwest::Client::builder()
            .build()
            .map_err(io::Error::other)?;
        return Ok((format!("http://{host}"), client));
    }

    // An explicit http:// scheme also selects plain HTTP; combining it with TLS
    // paths is contradictory.
    if explicit_scheme == Some("http") {
        if has_tls_paths {
            return Err(io::Error::other(
                "http:// scheme requested but TLS certificate paths were also given; use https:// or drop --tls-*",
            ));
        }
        let client = reqwest::Client::builder()
            .build()
            .map_err(io::Error::other)?;
        return Ok((format!("http://{host}"), client));
    }

    // A client cert/key is only consumed alongside a --tls-ca; without one the
    // paths would be silently ignored, so reject the combination outright.
    if config.tls_ca.is_none() && (config.tls_cert.is_some() || config.tls_key.is_some()) {
        return Err(io::Error::other("--tls-cert/--tls-key require --tls-ca"));
    }

    // TLS is expected for an https:// scheme, an explicit CA, or (at Meta) by
    // default. In OSS with no CA, plain HTTP is a legitimate fallback.
    let tls_expected =
        explicit_scheme == Some("https") || config.tls_ca.is_some() || cfg!(fbcode_build);

    let mut builder = reqwest::Client::builder();
    let use_tls = if let Some(ca_path) = &config.tls_ca {
        builder = add_tls_from_paths(
            builder,
            ca_path,
            config.tls_cert.as_deref(),
            config.tls_key.as_deref(),
        )?;
        true
    } else if let Some(bundle) = hyperactor::channel::try_tls_pem_bundle() {
        builder = add_tls_from_bundle(builder, &bundle)?;
        true
    } else {
        false
    };

    if !use_tls && tls_expected {
        return Err(io::Error::other(
            "TLS is required (an https:// scheme or the Meta default) but no CA certificate was found; pass --tls-ca or use --plaintext",
        ));
    }

    let scheme = if use_tls { "https" } else { "http" };
    let client = builder.build().map_err(io::Error::other)?;
    Ok((format!("{scheme}://{host}"), client))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    use super::*;

    /// A valid self-signed CA PEM, copied from hyperactor's test suite.
    /// `reqwest::Certificate::from_pem` parses it regardless of expiry, so
    /// it exercises the real "CA installs, TLS enabled" path.
    const TEST_CA_CERT: &str = "-----BEGIN CERTIFICATE-----
MIIDBTCCAe2gAwIBAgIUaGNmboiIosG+8Up0vgDr/+cg+2IwDQYJKoZIhvcNAQEL
BQAwEjEQMA4GA1UEAwwHVGVzdCBDQTAeFw0yNjAxMjgxNzA4MzlaFw0yNzAxMjgx
NzA4MzlaMBIxEDAOBgNVBAMMB1Rlc3QgQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IB
DwAwggEKAoIBAQC9RBoMYXCajklswt8Vi1JI1lEYzic0WNOmz45vG/7H6jTWkgL3
K5Ri+Seg3MobDNc48YHWXYm4hP9wCzkx8ih3ntT5XiY1My/G3jLUuoIEE9pF/BoJ
YQwZVoPNFhA9WhXNRsINf1cXFf8NzRfXpxBfKWtQJxYXU4JiDBQ6rLnQQABo8JmQ
vYFhJbBaYip5jTSiVNn7mB1zNr5jsVxuoSF53Pb7xQ76bwBdOq4zd6PSxL5/lr4G
cHSoxwZQdZMG7PL6hbxDQ2S2YI2lYVET1zwc2WPKCfjbEXBC/jzx828CInQtuksk
18gJt6xHkTFEA8CSA29GM3lejnwYWf51xyyBAgMBAAGjUzBRMB0GA1UdDgQWBBRX
cbxSZ70NsUkAS3Hhy6irugywJDAfBgNVHSMEGDAWgBRXcbxSZ70NsUkAS3Hhy6ir
ugywJDAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQA7aAFfyW67
Z+uGSVYhpsT/uH/3Z3nr7X1smTz5CGEfq2czEcTC7gbYI2l8GZ47GPfnAvHTBZVm
V/XncBCsj7/thOh2jYEHFyCbPckoaSCRyCOnK7LPUlr4HN5uP9EFe45qBLCJDEoY
GTTw7MtzwdovfjchNfKQCTtkBJCXQ95WLCf6UOh02Sn28UTlgfXzF0X0FrcWqWa3
uJZd4XOo4O6hKKlHaBaQPiEr++1xc3SWPV7jZHbckI/vKBnDdEZ9JQX5fFZuypUI
sgomYHxvxrU2hWx+7k53CRdjfaIvT9Ie44z9sSdsU/+blw2S8f/ZTmuECoIAAXYO
0qpzlxZMdr7T
-----END CERTIFICATE-----
";

    /// A temp PEM file removed on drop, so a panicking assertion never
    /// leaks it (RAII cleanup, no `tempfile` dep).
    struct TempPem(PathBuf);

    impl TempPem {
        fn new(contents: &str) -> Self {
            static N: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "mesh_admin_client_test_{}_{}.pem",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::write(&path, contents).expect("write temp file");
            Self(path)
        }

        fn path(&self) -> String {
            self.0.to_string_lossy().into_owned()
        }
    }

    impl Drop for TempPem {
        fn drop(&mut self) {
            std::fs::remove_file(&self.0).ok();
        }
    }

    fn config(addr: &str, plaintext: bool, tls_ca: Option<String>) -> AdminClientOptions {
        config_full(addr, plaintext, tls_ca, None, None)
    }

    fn config_full(
        addr: &str,
        plaintext: bool,
        tls_ca: Option<String>,
        tls_cert: Option<String>,
        tls_key: Option<String>,
    ) -> AdminClientOptions {
        AdminClientOptions {
            addr: addr.to_string(),
            tls_ca,
            tls_cert,
            tls_key,
            plaintext,
        }
    }

    #[test]
    fn plaintext_forces_http_over_https_scheme() {
        // --plaintext is the top-priority transport override.
        let (base_url, _) =
            build_client(&config("https://host:1729", true, None)).expect("plaintext");
        assert!(
            base_url.starts_with("http://"),
            "plaintext must force plain HTTP even for an https:// addr, got {base_url}"
        );
    }

    #[test]
    fn plaintext_ignores_a_valid_tls_ca() {
        // --plaintext wins over explicit TLS material (the CLI also rejects the
        // combination via conflicts_with; this covers direct callers).
        let ca = TempPem::new(TEST_CA_CERT);
        let (base_url, _) =
            build_client(&config("host:1729", true, Some(ca.path()))).expect("plaintext");
        assert!(
            base_url.starts_with("http://"),
            "plaintext must win over a --tls-ca, got {base_url}"
        );
    }

    #[test]
    fn https_with_valid_ca_uses_tls() {
        // A genuinely loadable CA enables TLS; a bogus path would fall back to
        // http, so the CA must actually parse to prove the TLS path.
        let ca = TempPem::new(TEST_CA_CERT);
        let (base_url, _) =
            build_client(&config("https://host:1729", false, Some(ca.path()))).expect("tls");
        assert!(
            base_url.starts_with("https://"),
            "a valid --tls-ca on an https:// addr must use TLS, got {base_url}"
        );
    }

    #[test]
    fn http_scheme_forces_plain_http() {
        // scheme is authoritative: explicit http:// is plain HTTP, no auto-upgrade.
        let (base_url, _) = build_client(&config("http://host:1729", false, None)).expect("http");
        assert!(
            base_url.starts_with("http://"),
            "http:// must stay plain HTTP, got {base_url}"
        );
    }

    #[test]
    fn http_scheme_with_tls_ca_is_error() {
        let result = build_client(&config(
            "http://host:1729",
            false,
            Some("/x/ca.pem".to_string()),
        ));
        assert!(
            result.is_err(),
            "http:// combined with --tls-ca must be a hard error"
        );
    }

    #[test]
    fn unreadable_tls_ca_is_error() {
        let result = build_client(&config(
            "host:1729",
            false,
            Some("/nonexistent/tui-admin-test/ca.pem".to_string()),
        ));
        assert!(
            result.is_err(),
            "an unreadable --tls-ca must be a hard error, not a plaintext downgrade"
        );
    }

    #[test]
    fn tls_cert_without_key_is_error() {
        let ca = TempPem::new(TEST_CA_CERT);
        let result = build_client(&config_full(
            "host:1729",
            false,
            Some(ca.path()),
            Some("/some/cert.pem".to_string()),
            None,
        ));
        assert!(
            result.is_err(),
            "--tls-cert without --tls-key must be a hard error"
        );
    }

    #[test]
    fn tls_cert_and_key_without_ca_is_error() {
        // cert/key are unusable without a CA; the combination must be rejected
        // rather than silently ignored (which would downgrade to plain HTTP).
        let result = build_client(&config_full(
            "host:1729",
            false,
            None,
            Some("/some/cert.pem".to_string()),
            Some("/some/key.pem".to_string()),
        ));
        assert!(
            result.is_err(),
            "--tls-cert/--tls-key without --tls-ca must be a hard error"
        );
    }
}
