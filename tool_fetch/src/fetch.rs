//! Provider download and byte verification.
//!
//! This module owns the trust transition from "bytes from a provider"
//! to "verified blob that may be installed or extracted". Callers must
//! not extract or execute artifacts until this module has verified size
//! and digest.
//!
//! # Invariants
//!
//! - **TF-FETCH-1 (verify-before-use):** `fetch_verified_blob`
//!   persists the destination only after size and digest verification.
//! - **TF-FETCH-2 (downloaded-byte-hash):** SHA256 is computed over
//!   the exact downloaded bytes.
//! - **TF-FETCH-3 (provider-order):** Providers are tried in spec order
//!   until one verifies successfully.
//! - **TF-FETCH-4 (atomic-blob-placement):** Downloaded bytes are first
//!   written to a temp file in the destination directory, then persisted
//!   to the final blob path.
//! - **TF-FETCH-5 (download-observability):** HTTP provider attempts
//!   emit structured start/complete events, but never per-chunk events.

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::io::Write;
use std::path::Path;

use sha2::Digest;
use sha2::Sha256;

use crate::HashAlgorithm;
use crate::PlatformEntry;
use crate::Provider;
use crate::ProvisionError;

/// Fetch and verify an artifact into `destination`.
///
/// TF-FETCH-1: success means `destination` exists and its bytes match
/// the spec entry's size and digest.
pub(crate) async fn fetch_verified_blob(
    entry: &PlatformEntry,
    destination: &Path,
) -> Result<(), ProvisionError> {
    let mut last_error = None;
    for provider in &entry.providers {
        match provider {
            Provider::Http { url } => match fetch_http(entry, url, destination).await {
                Ok(()) => return Ok(()),
                Err(error) => last_error = Some(error),
            },
        }
    }

    Err(
        last_error.unwrap_or_else(|| ProvisionError::DownloadFailed {
            url: "<none>".to_string(),
            error: "no providers configured".to_string(),
        }),
    )
}

async fn fetch_http(
    entry: &PlatformEntry,
    url: &str,
    destination: &Path,
) -> Result<(), ProvisionError> {
    tracing::info!(
        name = "ToolFetchStatus",
        status = "Download::Start",
        message = %format!(
            "download start: {} -> {}",
            url,
            destination.display(),
        ),
        url = %url,
        expected_size = entry.size,
        artifact_digest = %entry.digest,
        destination = %destination.display(),
    );
    let response = reqwest::get(url)
        .await
        .map_err(|e| ProvisionError::DownloadFailed {
            url: url.to_string(),
            error: e.to_string(),
        })?
        .error_for_status()
        .map_err(|e| ProvisionError::DownloadFailed {
            url: url.to_string(),
            error: e.to_string(),
        })?;

    let parent = destination
        .parent()
        .ok_or_else(|| ProvisionError::DownloadFailed {
            url: url.to_string(),
            error: format!("destination has no parent: {}", destination.display()),
        })?;
    std::fs::create_dir_all(parent)?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    let mut hasher = Sha256::new();
    let mut size = 0_u64;
    let mut response = response;

    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| ProvisionError::DownloadFailed {
            url: url.to_string(),
            error: e.to_string(),
        })?
    {
        size += chunk.len() as u64;
        hasher.update(&chunk);
        temp.write_all(&chunk)?;
    }
    temp.flush()?;

    if size != entry.size {
        return Err(ProvisionError::DownloadFailed {
            url: url.to_string(),
            error: format!("size mismatch: expected {}, actual {}", entry.size, size),
        });
    }

    let actual = match entry.hash_algorithm {
        HashAlgorithm::Sha256 => hex::encode(hasher.finalize()),
    };
    if !actual.eq_ignore_ascii_case(&entry.digest) {
        return Err(ProvisionError::HashMismatch {
            expected: entry.digest.clone(),
            actual,
        });
    }

    temp.persist(destination)
        .map_err(|e| ProvisionError::IoError(e.error))?;
    tracing::info!(
        name = "ToolFetchStatus",
        status = "Download::Complete",
        message = %format!(
            "download complete: {} bytes={} -> {}",
            url,
            size,
            destination.display(),
        ),
        url = %url,
        actual_size = size,
        artifact_digest = %entry.digest,
        destination = %destination.display(),
    );
    Ok(())
}
