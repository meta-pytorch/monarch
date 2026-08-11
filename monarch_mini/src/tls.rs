/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::Arc;

use rustls::RootCertStore;
use rustls::crypto::CryptoProvider;
use rustls::crypto::ring::cipher_suite;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use rustls::server::WebPkiClientVerifier;

pub(crate) const SERVER_NAME: &str = "monarch-mini";

pub(crate) struct Config {
    pub(crate) server: rustls::ServerConfig,
    pub(crate) client: rustls::ClientConfig,
}

impl Config {
    pub(crate) fn load() -> anyhow::Result<Self> {
        let provider = Arc::new(selected_provider());
        let certs = load_certs(&required_env("MM_QUIC_CERT")?)?;
        let key = load_key(&required_env("MM_QUIC_KEY")?)?;
        let mut roots = RootCertStore::empty();
        for ca in load_certs(&required_env("MM_QUIC_CA")?)? {
            roots.add(ca)?;
        }
        let roots = Arc::new(roots);

        let client_verifier =
            WebPkiClientVerifier::builder_with_provider(roots.clone(), provider.clone()).build()?;
        let server = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(certs.clone(), key.clone_key())?;
        let client = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_root_certificates(roots)
            .with_client_auth_cert(certs, key)?;
        Ok(Self { server, client })
    }
}

/// The ring crypto provider, optionally restricted to a single TLS 1.3 AEAD by
/// `MM_QUIC_CIPHER` (`aes128` | `aes256` | `chacha20`) for both network transports.
fn selected_provider() -> CryptoProvider {
    let base = rustls::crypto::ring::default_provider();
    let suite = match std::env::var("MM_QUIC_CIPHER").ok().as_deref() {
        Some("aes128") => cipher_suite::TLS13_AES_128_GCM_SHA256,
        Some("aes256") => cipher_suite::TLS13_AES_256_GCM_SHA384,
        Some("chacha20") => cipher_suite::TLS13_CHACHA20_POLY1305_SHA256,
        _ => return base,
    };
    eprintln!("MM_QUIC_CIPHER: restricting TLS to a single cipher suite");
    CryptoProvider {
        cipher_suites: vec![suite],
        ..base
    }
}

fn required_env(name: &str) -> anyhow::Result<String> {
    std::env::var(name).map_err(|_| anyhow::anyhow!("{name} not set"))
}

fn load_certs(path: &str) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let data = std::fs::read(path).map_err(|err| anyhow::anyhow!("reading {path}: {err}"))?;
    let certs = rustls_pemfile::certs(&mut &data[..]).collect::<Result<Vec<_>, _>>()?;
    anyhow::ensure!(!certs.is_empty(), "no certificates in {path}");
    Ok(certs)
}

fn load_key(path: &str) -> anyhow::Result<PrivateKeyDer<'static>> {
    let data = std::fs::read(path).map_err(|err| anyhow::anyhow!("reading {path}: {err}"))?;
    rustls_pemfile::private_key(&mut &data[..])?
        .ok_or_else(|| anyhow::anyhow!("no private key in {path}"))
}
