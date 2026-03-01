/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! TLS-aware `reqwest` client construction for the admin TUI.
//!
//! This module builds `(base_url, reqwest::Client)` from CLI
//! arguments, choosing HTTP vs HTTPS and configuring certificate
//! verification when TLS material is available.
//!
//! Address handling:
//! - `--addr` may be `host:port` (no scheme) or an explicit
//!   `http://...` / `https://...`.
//! - If a scheme is provided, it is treated as authoritative.
//!
//! TLS configuration (highest priority first):
//! 1. Explicit CLI paths: `--tls-ca` (required to enable TLS), with
//!    optional `--tls-cert` + `--tls-key` for mutual TLS.
//! 2. Auto-detection via [`hyperactor::channel::try_tls_pem_bundle`],
//!    which probes configured paths (OSS) and Meta well-known
//!    locations.
//! 3. Fallback to plain HTTP when no usable CA is found.
//!
//! When TLS is enabled, the returned client verifies the server
//! certificate against the configured CA. When mutual TLS material is
//! present, a client identity is attached on a best-effort basis
//! (failure to parse the identity does not disable TLS).

use std::time::Duration;

use crate::theme::Args;

/// Resolve a `mast_conda:///<job-name>` handle to an
/// `https://fqdn:port` URL.
///
/// Thin wrapper around
/// [`hyperactor_meta_lib::mesh_admin::resolve_mast_handle`] that
/// converts errors into `eprintln!` + `exit(1)` for the TUI.
#[cfg(fbcode_build)]
pub(crate) async fn resolve_mast_addr(addr: &str, admin_port: Option<u16>) -> String {
    // SAFETY: This code path is gated by #[cfg(fbcode_build)] and
    // only reachable from main(), which is annotated #[fbinit::main]
    // — guaranteeing that FacebookInit has been performed.
    let fb = unsafe { fbinit::assume_init() };

    hyperactor_meta_lib::mesh_admin::resolve_mast_handle(fb, addr, admin_port)
        .await
        .unwrap_or_else(|e| {
            eprintln!("{:#}", e);
            std::process::exit(1);
        })
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
fn add_tls(
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
        let mut id_pem = cert;
        id_pem.extend_from_slice(&key);
        match reqwest::Identity::from_pem(&id_pem) {
            Ok(identity) => builder = builder.identity(identity),
            Err(e) => eprintln!("TLS: invalid client identity PEM: {}", e),
        }
    }

    (builder, true)
}

/// Configure TLS on a `reqwest::ClientBuilder` using PEM files
/// supplied via CLI (`--tls-ca`, and optionally `--tls-cert` +
/// `--tls-key`).
///
/// Reads `ca_path` and installs it as the root trust anchor. If the
/// CA file cannot be read, returns `(builder, false)` with no
/// changes.
///
/// If `cert_path`/`key_path` are provided, attempts to read them and
/// pass the bytes through to [`add_tls`] to configure an mTLS
/// identity; failures to read these optional files simply omit the
/// identity (the CA may still be applied).
///
/// Returns `(updated_builder, ca_installed)`.
fn add_tls_from_paths(
    builder: reqwest::ClientBuilder,
    ca_path: &str,
    cert_path: Option<&str>,
    key_path: Option<&str>,
) -> (reqwest::ClientBuilder, bool) {
    let ca_bytes = match std::fs::read(ca_path) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("TLS: cannot read CA file {}: {}", ca_path, e);
            return (builder, false);
        }
    };
    let cert_bytes = cert_path.and_then(|p| std::fs::read(p).ok());
    let key_bytes = key_path.and_then(|p| std::fs::read(p).ok());
    add_tls(builder, &ca_bytes, cert_bytes, key_bytes)
}

/// Configure TLS on a `reqwest::ClientBuilder` using a hyperactor
/// [`PemBundle`](hyperactor::config::PemBundle).
///
/// The bundle provides a CA (`bundle.ca`) and optionally a client
/// certificate and key (`bundle.cert`, `bundle.key`). This reads the
/// CA via [`read_pem`] and installs it as a root trust anchor. If the
/// CA is missing, unreadable, or empty, returns `(builder, false)`
/// with no changes.
///
/// If both client cert and key are readable, they are combined and
/// passed to [`add_tls`] to configure an mTLS identity; otherwise the
/// client identity is omitted (CA-only TLS verification still
/// applies).
///
/// Returns `(updated_builder, ca_installed)`.
fn add_tls_from_bundle(
    builder: reqwest::ClientBuilder,
    bundle: &hyperactor::config::PemBundle,
) -> (reqwest::ClientBuilder, bool) {
    let ca_bytes = match read_pem(&bundle.ca) {
        Some(b) => b,
        None => {
            eprintln!("TLS: CA not readable from PemBundle");
            return (builder, false);
        }
    };
    add_tls(
        builder,
        &ca_bytes,
        read_pem(&bundle.cert),
        read_pem(&bundle.key),
    )
}

/// Build a `reqwest` client and a base URL from CLI arguments.
///
/// `args.addr` may be either a bare `host:port` or an explicit URL
/// (`http://host:port` / `https://host:port`). If an explicit scheme
/// is provided, that scheme is honored.
///
/// TLS configuration is applied in priority order:
/// 1. If `--tls-ca` is provided, attempt to load the CA (and
///    optionally `--tls-cert` + `--tls-key` for a client identity)
///    from those paths.
/// 2. Otherwise, if no explicit scheme was given, try auto-detection
///    via [`hyperactor::channel::try_tls_pem_bundle`] (OSS config
///    first, then Meta well-known paths).
/// 3. If no CA can be loaded, fall back to plain HTTP.
///
/// Returns `(base_url, client)` where `base_url` always includes the
/// scheme selected (`http://...` or `https://...`).
pub(crate) fn build_client(args: &Args) -> (String, reqwest::Client) {
    let (explicit_scheme, host) = parse_addr(&args.addr);

    let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(5));
    let mut use_tls = explicit_scheme == Some("https");

    // 1. Explicit CLI cert paths.
    if let Some(ca_path) = &args.tls_ca {
        let (b, ok) = add_tls_from_paths(
            builder,
            ca_path,
            args.tls_cert.as_deref(),
            args.tls_key.as_deref(),
        );
        builder = b;
        use_tls = use_tls || ok;
    }

    // 2. Auto-detect (only when no explicit scheme or CLI certs).
    if explicit_scheme.is_none() && !use_tls {
        if let Some(bundle) = hyperactor::channel::try_tls_pem_bundle() {
            let (b, ok) = add_tls_from_bundle(builder, &bundle);
            builder = b;
            use_tls = ok;
        }
    }

    let scheme = if use_tls { "https" } else { "http" };
    let base_url = format!("{}://{}", scheme, host);
    let client = builder.build().unwrap_or_else(|_| reqwest::Client::new());

    (base_url, client)
}
