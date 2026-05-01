/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

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
//! - **TF-FETCH-5 (async-write):** The chunked download loop and the
//!   temp-file writes both run on the async runtime. No per-chunk
//!   `std::io::Write` call holds the tokio worker, so providers that
//!   stream slowly cannot block other actor tasks.
//! - **TF-FETCH-7 (injected-client):** [`fetch_verified_blob`] takes
//!   a caller-provided [`reqwest::Client`] rather than constructing
//!   one ad hoc. The client carries any HTTPS proxy configuration
//!   (see [`FetchConfig`]); fetch logic does not read environment
//!   policy, keeping `tool_fetch` config-agnostic.
//! - **TF-FETCH-6 (download-observability):** HTTP provider attempts
//!   emit structured start/complete events, but never per-chunk events.

use std::path::Path;

use sha2::Digest;
use sha2::Sha256;
use tokio::io::AsyncWriteExt;

use crate::HashAlgorithm;
use crate::PlatformEntry;
use crate::Provider;
use crate::ProvisionError;

/// HTTP fetch configuration injected into the cache by callers.
///
/// `tool_fetch` is the mechanism layer; the policy decision of whether
/// (and through which proxy) to fetch artifacts lives one layer up
/// (e.g., `hyperactor_mesh` config). [`FetchConfig::build_client`]
/// turns this declarative config into a [`reqwest::Client`] that
/// [`crate::ToolCache`] stores and reuses for every download.
#[derive(Debug, Clone, Default)]
pub struct FetchConfig {
    /// Optional HTTPS proxy URL. When `Some`, the resulting client
    /// routes HTTPS requests through this proxy and passes HTTP
    /// requests through directly. When `None`, the client uses no
    /// proxy at all (`.no_proxy()`), independent of the host's
    /// `https_proxy` env var.
    pub https_proxy: Option<String>,
}

impl FetchConfig {
    /// Build a [`reqwest::Client`] honoring this config.
    ///
    /// TF-FETCH-7: returns an error rather than panicking on
    /// misconfiguration, so a policy-layer caller can map the error
    /// into operator-facing inventory state.
    pub fn build_client(&self) -> Result<reqwest::Client, ProvisionError> {
        let mut builder = reqwest::Client::builder().no_proxy();
        if let Some(url) = &self.https_proxy {
            let proxy = reqwest::Proxy::https(url).map_err(|e| ProvisionError::InvalidConfig {
                error: format!("invalid HTTPS proxy URL {url:?}: {e}"),
            })?;
            builder = builder.proxy(proxy);
        }
        builder.build().map_err(|e| ProvisionError::InvalidConfig {
            error: format!("failed to build reqwest client: {e}"),
        })
    }
}

/// Fetch and verify an artifact into `destination`.
///
/// TF-FETCH-1: success means `destination` exists and its bytes match
/// the spec entry's size and digest. TF-FETCH-7: callers supply the
/// [`reqwest::Client`].
pub(crate) async fn fetch_verified_blob(
    client: &reqwest::Client,
    entry: &PlatformEntry,
    destination: &Path,
) -> Result<(), ProvisionError> {
    let mut last_error = None;
    for provider in &entry.providers {
        match provider {
            Provider::Http { url } => match fetch_http(client, entry, url, destination).await {
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
    client: &reqwest::Client,
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
    let response = client
        .get(url)
        .send()
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
    tokio::fs::create_dir_all(parent).await?;

    // Reserve a temp path with cleanup-on-drop semantics, then close
    // the sync handle so we can re-open it via tokio::fs for fully
    // async writes (TF-FETCH-5). On any early return below, the
    // `TempPath` drops and the temp file is removed; success path ends
    // with `keep()` and an async rename to `destination` (TF-FETCH-4).
    let temp_path = tempfile::Builder::new()
        .tempfile_in(parent)?
        .into_temp_path();

    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&temp_path)
        .await?;
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
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    drop(file);

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

    // Persist via async rename. `keep()` releases the auto-delete on
    // the `TempPath` and yields a `PathBuf` we can hand to
    // `tokio::fs::rename`.
    let temp_path = temp_path
        .keep()
        .map_err(|e| ProvisionError::IoError(e.error))?;
    tokio::fs::rename(&temp_path, destination).await?;
    // TF-FETCH-6: emit Download::Complete after the rename commits,
    // so an observer that sees this event can trust the artifact is
    // at `destination`.
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
