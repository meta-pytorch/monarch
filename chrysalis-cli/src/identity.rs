/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::env;
use std::fs;
use std::fs::DirBuilder;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

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
use tracing::warn;

const CA_SEPARATOR: &str = "\n-- CHRYSALIS DEVELOPMENT CA PRIVATE KEY --\n";

/// Generates one ephemeral, self-certifying node identity.
pub(crate) async fn generate() -> Result<QuicIdentity> {
    tokio::task::spawn_blocking(generate_blocking)
        .await
        .context("join development identity task")?
}

fn generate_blocking() -> Result<QuicIdentity> {
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
    let directory = development_state_dir(uid)?;
    let path = directory.join("development-ca.pem");
    let lock_path = directory.join("development-ca.lock");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&lock_path)
        .with_context(|| format!("open development CA lock {}", lock_path.display()))?;
    validate_private_file(&lock, uid, "development CA lock")?;
    // SAFETY: lock owns a valid descriptor and LOCK_EX is a supported flock operation.
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } != 0 {
        return Err(io::Error::last_os_error()).context("lock development CA");
    }
    let contents = match read_development_ca(&path, uid)? {
        Some(contents) => contents,
        None => {
            let contents = generate_development_ca()?;
            write_atomic(&directory, &path, contents.as_bytes())?;
            contents
        }
    };
    let (certificate, key) = contents
        .split_once(CA_SEPARATOR)
        .context("development CA file is malformed")?;
    Ok((certificate.to_owned(), key.to_owned()))
}

fn read_development_ca(path: &Path, uid: u32) -> Result<Option<String>> {
    let mut file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("open development CA {}", path.display()));
        }
    };
    validate_private_file(&file, uid, "development CA")?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .context("read development CA")?;
    anyhow::ensure!(!contents.is_empty(), "development CA file is empty");
    Ok(Some(contents))
}

fn generate_development_ca() -> Result<String> {
    let key = KeyPair::generate()?;
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let certificate = params.self_signed(&key)?;
    Ok(format!(
        "{}{}{}",
        certificate.pem(),
        CA_SEPARATOR,
        key.serialize_pem()
    ))
}

fn write_atomic(directory: &Path, path: &Path, contents: &[u8]) -> Result<()> {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock precedes Unix epoch")?
        .as_nanos();
    let temporary = directory.join(format!(
        ".development-ca.pem.{}.{nonce}.tmp",
        std::process::id()
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&temporary)
        .with_context(|| format!("create temporary development CA {}", temporary.display()))?;
    let result = (|| -> Result<()> {
        file.write_all(contents)?;
        file.sync_all()?;
        fs::rename(&temporary, path).with_context(|| {
            format!(
                "install development CA {} as {}",
                temporary.display(),
                path.display()
            )
        })?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
        return result;
    }
    if let Err(error) = File::open(directory).and_then(|directory| directory.sync_all()) {
        warn!(
            path = %directory.display(),
            %error,
            "development CA was installed but its directory could not be synchronized"
        );
    }
    Ok(())
}

fn validate_private_file(file: &File, uid: u32, description: &str) -> Result<()> {
    let metadata = file
        .metadata()
        .with_context(|| format!("inspect {description}"))?;
    anyhow::ensure!(
        metadata.is_file()
            && metadata.uid() == uid
            && metadata.mode() & 0o777 == 0o600
            && metadata.nlink() == 1,
        "{description} must be a user-owned 0600 regular file with one link"
    );
    Ok(())
}

fn development_state_dir(uid: u32) -> Result<PathBuf> {
    let home = env::var_os("HOME").context("HOME is not set")?;
    let path = PathBuf::from(home).join(".chrysalis");
    match DirBuilder::new().mode(0o700).create(&path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("create development state {}", path.display()));
        }
    }
    let mut metadata = fs::symlink_metadata(&path)
        .with_context(|| format!("inspect development state {}", path.display()))?;
    anyhow::ensure!(
        metadata.is_dir() && metadata.uid() == uid,
        "development state directory {} must be a directory owned by uid {uid}; found uid {} and mode {:04o}",
        path.display(),
        metadata.uid(),
        metadata.mode() & 0o777
    );
    if metadata.mode() & 0o777 != 0o700 {
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).with_context(|| {
            format!(
                "tighten development state permissions on {}",
                path.display()
            )
        })?;
        metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("reinspect development state {}", path.display()))?;
        anyhow::ensure!(
            metadata.mode() & 0o777 == 0o700,
            "development state directory {} remains mode {:04o}; run `chmod 700 {}`",
            path.display(),
            metadata.mode() & 0o777,
            path.display()
        );
    }
    Ok(path)
}
