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
