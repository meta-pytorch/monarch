//! Content-addressed cache for verified tool artifacts.
//!
//! The cache separates verified downloaded bytes (`blobs/`) from
//! executable install trees (`extracted/`). Higher layers should expose
//! paths from the install tree only.
//!
//! # Layout
//!
//! ```text
//! {cache_dir}/
//!   blobs/{digest[0..2]}/{digest}
//!   extracted/{digest[0..2]}/{digest}/
//!     .tool-fetch-metadata.json
//!   locks/{digest[0..2]}/{digest}.lock
//! ```
//!
//! # Invariants
//!
//! - **TF-CACHE-1 (content-addressed-blob):** Verified artifact bytes
//!   are stored under the artifact digest.
//! - **TF-CACHE-2 (executable-not-blob):** [`ToolCache::lookup`] and
//!   [`ToolCache::provision`] return paths from `extracted/`, never
//!   direct blob paths.
//! - **TF-CACHE-3 (cache-hit-no-download):** A complete install with
//!   metadata and executable is reused without contacting providers.
//! - **TF-CACHE-4 (metadata-gates-scan):** [`ToolCache::scan`] reports
//!   only entries with parseable metadata and an existing executable.
//! - **TF-CACHE-5 (blob-reverify):** Existing blobs are rechecked
//!   against size and digest before reuse.
//! - **TF-CACHE-6 (metadata-contained-path):** Metadata executable
//!   paths must be relative paths contained by their extracted artifact
//!   directory.
//! - **TF-SPEC-1 (spec-selects-platform):** [`ToolCache::provision`]
//!   accepts a full spec plus platform and performs entry selection
//!   internally.
//! - **TF-CACHE-7 (operator-observability):** Cache decisions emit
//!   structured tracing events so callers running inside actor
//!   recording spans can explain whether provisioning downloaded,
//!   reused a blob, reused an install, or extracted from cache.

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fs;
use std::io::Read;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;

use fs2::FileExt;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;

use crate::ArtifactFormat;
use crate::Platform;
use crate::PlatformEntry;
use crate::ProvisionError;
use crate::ToolSpec;
use crate::extract;
use crate::fetch;

const METADATA_FILE: &str = ".tool-fetch-metadata.json";

/// Content-addressed cache rooted at a caller-provided directory.
///
/// The cache is safe to share between independent handles in the same
/// process or host; per-digest advisory locks serialize provisioning.
#[derive(Debug, Clone)]
pub struct ToolCache {
    cache_dir: PathBuf,
}

/// Artifact discovered from cache metadata.
///
/// This is the durable inventory representation used by the future
/// mesh-facing actor after restart recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedArtifact {
    /// Tool name recorded at provision time.
    pub name: String,
    /// Tool version recorded at provision time.
    pub version: String,
    /// Platform entry used to provision the artifact.
    pub platform: Platform,
    /// Digest of the downloaded artifact bytes.
    pub digest: String,
    /// Resolved executable path under `extracted/`.
    pub executable: PathBuf,
    /// Provision timestamp as a simple string suitable for JSON metadata.
    pub provisioned_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheMetadata {
    name: String,
    version: String,
    platform: Platform,
    digest: String,
    executable_path: PathBuf,
    provisioned_at: String,
}

impl ToolCache {
    /// Create a cache rooted at `cache_dir`.
    ///
    /// Tests pass a tempdir here; production actors use
    /// [`ToolCache::default_dir`].
    pub fn new(cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            cache_dir: cache_dir.into(),
        }
    }

    /// Default OSS cache directory.
    ///
    /// Uses `${XDG_CACHE_HOME}/monarch/tools` when set, otherwise
    /// `$HOME/.cache/monarch/tools`, with a tempdir fallback only when
    /// no home directory is available.
    pub fn default_dir() -> PathBuf {
        if let Some(xdg) = std::env::var_os("XDG_CACHE_HOME") {
            return PathBuf::from(xdg).join("monarch").join("tools");
        }
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home)
                .join(".cache")
                .join("monarch")
                .join("tools");
        }
        std::env::temp_dir().join("monarch").join("tools")
    }

    /// Return the root cache directory.
    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }

    /// Return the cached executable path for `entry`, if fully installed.
    ///
    /// TF-CACHE-2 and TF-CACHE-4: this succeeds only when metadata is
    /// present and the executable exists under `extracted/`.
    pub fn lookup(&self, entry: &PlatformEntry) -> Option<PathBuf> {
        let extracted_dir = self.extracted_path(&entry.digest);
        let metadata = read_metadata(&extracted_dir).ok()?;
        if metadata.digest != entry.digest || !is_safe_relative_path(&metadata.executable_path) {
            return None;
        }
        let executable = extracted_dir.join(metadata.executable_path);
        executable.is_file().then_some(executable)
    }

    /// Fetch, verify, install/extract, write metadata, and resolve a tool.
    ///
    /// TF-SPEC-1: platform entry selection happens inside this method.
    /// TF-FETCH-1: downloaded bytes are verified before extraction.
    /// TF-CACHE-3: completed installs return without redownloading.
    pub async fn provision(
        &self,
        spec: &ToolSpec,
        platform: Platform,
    ) -> Result<PathBuf, ProvisionError> {
        let entry =
            spec.platforms
                .get(&platform)
                .ok_or_else(|| ProvisionError::UnsupportedPlatform {
                    os: format!("{platform:?}"),
                    arch: "not in spec".to_string(),
                })?;

        fs::create_dir_all(self.blobs_dir(&entry.digest))?;
        fs::create_dir_all(self.extracted_parent(&entry.digest))?;
        fs::create_dir_all(self.locks_dir(&entry.digest))?;

        let lock_path = self.lock_path(&entry.digest);
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(lock_path)?;
        lock.lock_exclusive()?;

        let result = self.provision_locked(spec, platform, entry).await;
        let unlock_result = lock.unlock();
        match (result, unlock_result) {
            (Ok(path), Ok(())) => Ok(path),
            (Err(err), _) => Err(err),
            (Ok(_), Err(err)) => Err(ProvisionError::IoError(err)),
        }
    }

    async fn provision_locked(
        &self,
        spec: &ToolSpec,
        platform: Platform,
        entry: &PlatformEntry,
    ) -> Result<PathBuf, ProvisionError> {
        if let Some(path) = self.lookup(entry) {
            tracing::info!(
                name = "ToolFetchStatus",
                status = "InstallCache::Hit",
                tool = %spec.name,
                version = %spec.version,
                platform = ?platform,
                artifact_digest = %entry.digest,
                executable = %path.display(),
            );
            return Ok(path);
        }

        let blob = self.blob_path(&entry.digest);
        if blob.is_file() && !verify_blob(&blob, entry)? {
            tracing::warn!(
                name = "ToolFetchStatus",
                status = "BlobCache::Invalid",
                tool = %spec.name,
                version = %spec.version,
                platform = ?platform,
                artifact_digest = %entry.digest,
                blob = %blob.display(),
            );
            fs::remove_file(&blob)?;
        }
        if !blob.is_file() {
            tracing::info!(
                name = "ToolFetchStatus",
                status = "BlobCache::Miss",
                tool = %spec.name,
                version = %spec.version,
                platform = ?platform,
                artifact_digest = %entry.digest,
                blob = %blob.display(),
            );
            fetch::fetch_verified_blob(entry, &blob).await?;
        } else {
            tracing::info!(
                name = "ToolFetchStatus",
                status = "BlobCache::Hit",
                tool = %spec.name,
                version = %spec.version,
                platform = ?platform,
                artifact_digest = %entry.digest,
                blob = %blob.display(),
            );
        }

        let extracted_dir = self.extracted_path(&entry.digest);
        let temp_dir = tempfile::Builder::new()
            .prefix("extract-")
            .tempdir_in(self.extracted_parent(&entry.digest))?;

        tracing::info!(
            name = "ToolFetchStatus",
            status = "Extract::Start",
            tool = %spec.name,
            version = %spec.version,
            platform = ?platform,
            artifact_format = ?entry.format,
            artifact_digest = %entry.digest,
            destination = %extracted_dir.display(),
        );
        let executable = match entry.format {
            ArtifactFormat::Plain => extract::install_plain(&blob, temp_dir.path(), &spec.name)?,
            ArtifactFormat::TarGz => {
                extract::extract_tar_gz(&blob, temp_dir.path())?;
                resolve_archive_executable(temp_dir.path(), entry)?
            }
            ArtifactFormat::Zip => {
                extract::extract_zip(&blob, temp_dir.path())?;
                resolve_archive_executable(temp_dir.path(), entry)?
            }
        };
        extract::make_executable(&executable)?;

        let executable_path = executable.strip_prefix(temp_dir.path()).map_err(|e| {
            ProvisionError::ExtractionFailed {
                error: e.to_string(),
            }
        })?;
        write_metadata(
            temp_dir.path(),
            &CacheMetadata {
                name: spec.name.clone(),
                version: spec.version.clone(),
                platform,
                digest: entry.digest.clone(),
                executable_path: executable_path.to_path_buf(),
                provisioned_at: current_timestamp(),
            },
        )?;

        if extracted_dir.exists() {
            fs::remove_dir_all(&extracted_dir)?;
        }
        let temp_path = temp_dir.keep();
        fs::rename(&temp_path, &extracted_dir)?;

        let executable = self
            .lookup(entry)
            .ok_or_else(|| ProvisionError::ExecutableMissing {
                expected_path: extracted_dir.join(executable_path),
            })?;
        tracing::info!(
            name = "ToolFetchStatus",
            status = "Extract::Complete",
            tool = %spec.name,
            version = %spec.version,
            platform = ?platform,
            artifact_digest = %entry.digest,
            executable = %executable.display(),
        );
        Ok(executable)
    }

    /// Scan installed cache entries.
    ///
    /// TF-CACHE-4: directories without valid metadata or executable are
    /// ignored, so callers never receive anonymous digest directories as
    /// inventory.
    pub fn scan(&self) -> Vec<CachedArtifact> {
        let mut artifacts = Vec::new();
        let extracted = self.cache_dir.join("extracted");
        let Ok(prefixes) = fs::read_dir(extracted) else {
            return artifacts;
        };

        for prefix in prefixes.flatten() {
            let Ok(entries) = fs::read_dir(prefix.path()) else {
                continue;
            };
            for entry in entries.flatten() {
                let Ok(metadata) = read_metadata(&entry.path()) else {
                    continue;
                };
                if !is_safe_relative_path(&metadata.executable_path) {
                    continue;
                }
                let executable = entry.path().join(&metadata.executable_path);
                if executable.is_file() {
                    artifacts.push(CachedArtifact {
                        name: metadata.name,
                        version: metadata.version,
                        platform: metadata.platform,
                        digest: metadata.digest,
                        executable,
                        provisioned_at: metadata.provisioned_at,
                    });
                }
            }
        }

        artifacts
    }

    fn blob_path(&self, digest: &str) -> PathBuf {
        self.blobs_dir(digest).join(digest)
    }

    fn blobs_dir(&self, digest: &str) -> PathBuf {
        self.cache_dir.join("blobs").join(prefix(digest))
    }

    fn extracted_parent(&self, digest: &str) -> PathBuf {
        self.cache_dir.join("extracted").join(prefix(digest))
    }

    fn extracted_path(&self, digest: &str) -> PathBuf {
        self.extracted_parent(digest).join(digest)
    }

    fn locks_dir(&self, digest: &str) -> PathBuf {
        self.cache_dir.join("locks").join(prefix(digest))
    }

    fn lock_path(&self, digest: &str) -> PathBuf {
        self.locks_dir(digest).join(format!("{digest}.lock"))
    }
}

impl Default for ToolCache {
    /// Construct a cache rooted at [`ToolCache::default_dir`].
    fn default() -> Self {
        Self::new(Self::default_dir())
    }
}

fn prefix(digest: &str) -> &str {
    digest.get(..2).unwrap_or(digest)
}

fn resolve_archive_executable(
    extracted_dir: &Path,
    entry: &PlatformEntry,
) -> Result<PathBuf, ProvisionError> {
    let executable_path =
        entry
            .executable_path
            .as_ref()
            .ok_or_else(|| ProvisionError::ExecutableMissing {
                expected_path: extracted_dir.join("<missing executable_path>"),
            })?;
    let executable = extracted_dir.join(executable_path);
    if executable.is_file() {
        Ok(executable)
    } else {
        Err(ProvisionError::ExecutableMissing {
            expected_path: executable,
        })
    }
}

fn write_metadata(path: &Path, metadata: &CacheMetadata) -> Result<(), ProvisionError> {
    let data = serde_json::to_vec_pretty(metadata)?;
    fs::write(path.join(METADATA_FILE), data)?;
    Ok(())
}

fn read_metadata(path: &Path) -> Result<CacheMetadata, ProvisionError> {
    let data = fs::read(path.join(METADATA_FILE))?;
    Ok(serde_json::from_slice(&data)?)
}

fn is_safe_relative_path(path: &Path) -> bool {
    !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_) | Component::CurDir))
}

fn verify_blob(path: &Path, entry: &PlatformEntry) -> Result<bool, ProvisionError> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut size = 0_u64;
    let mut buffer = [0_u8; 8192];

    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        size += n as u64;
        hasher.update(&buffer[..n]);
    }

    if size != entry.size {
        return Ok(false);
    }
    let actual = hex::encode(hasher.finalize());
    Ok(actual.eq_ignore_ascii_case(&entry.digest))
}

fn current_timestamp() -> String {
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    duration.as_secs().to_string()
}
