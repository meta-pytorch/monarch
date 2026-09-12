/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fs;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;

use anyhow::Context;
use anyhow::Result;
use chrysalis::QuicIdentity;
use rcgen::BasicConstraints;
use rcgen::CertificateParams;
use rcgen::ExtendedKeyUsagePurpose;
use rcgen::IsCa;
use rcgen::Issuer;
use rcgen::KeyPair;
use rcgen::KeyUsagePurpose;

const CA_SEPARATOR: &str = "\n-- CHRYSALIS DEVELOPMENT CA PRIVATE KEY --\n";

/// Generates one ephemeral, self-certifying node identity.
pub fn generate() -> Result<QuicIdentity> {
    let (ca_certificate, ca_key) = development_ca()?;
    let issuer = Issuer::from_ca_cert_pem(&ca_certificate, KeyPair::from_pem(&ca_key)?)?;
    let signing_key = KeyPair::generate()?;
    let mut params = CertificateParams::new(vec!["localhost".to_owned()])?;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ClientAuth,
        ExtendedKeyUsagePurpose::ServerAuth,
    ];
    let certificate = params.signed_by(&signing_key, &issuer)?;
    let chain = format!("{}{}", certificate.pem(), ca_certificate);
    Ok(QuicIdentity::new(
        certificate.der().as_ref(),
        chain.into_bytes(),
        signing_key.serialize_pem().into_bytes(),
        ca_certificate.into_bytes(),
        "localhost",
    ))
}

fn development_ca() -> Result<(String, String)> {
    // SAFETY: getuid has no preconditions and does not access memory.
    let uid = unsafe { libc::getuid() };
    let path = std::env::temp_dir().join(format!("chrysalis-development-ca-{uid}.pem"));
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&path)
        .with_context(|| format!("open development CA {}", path.display()))?;
    // SAFETY: file owns a valid descriptor and LOCK_EX is a supported flock operation.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
        return Err(std::io::Error::last_os_error()).context("lock development CA");
    }
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .context("read development CA")?;
    if contents.is_empty() {
        let key = KeyPair::generate()?;
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let certificate = params.self_signed(&key)?;
        contents = format!(
            "{}{}{}",
            certificate.pem(),
            CA_SEPARATOR,
            key.serialize_pem()
        );
        file.seek(SeekFrom::Start(0))?;
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
        fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
    }
    let (certificate, key) = contents
        .split_once(CA_SEPARATOR)
        .context("development CA file is malformed")?;
    Ok((certificate.to_owned(), key.to_owned()))
}
