/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

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
//! - **TF-CACHE-8 (rfc3339-utc-timestamps):** [`CachedArtifact`] and
//!   the on-disk metadata record `provisioned_at` as RFC 3339 UTC.
//! - **TF-EXTRACT-6 (archive-executable-path-contained):** A spec's
//!   `executable_path` for archive formats is validated as a contained
//!   relative path before any join, `is_file` check, or `chmod`, so a
//!   malicious spec cannot redirect `make_executable` to a file
//!   outside the extraction tree.
//! - **TF-CACHE-9 (async-correctness):** The post-fetch sync work —
//!   blob verification, archive extraction, metadata write, swap-in
//!   `remove_dir_all`, and atomic rename — runs on
//!   `tokio::task::spawn_blocking`, not the tokio worker. Concurrent
//!   actor tasks remain schedulable while a provision is in flight.
//! - **TF-FETCH-7 (injected-client):** The cache owns the
//!   [`reqwest::Client`] used for downloads. Callers configure HTTPS
//!   proxying through [`ToolCache::with_fetch_config`]; the cache
//!   never reads `https_proxy` env vars on its own.
//! - **TF-CACHE-10 (operator-observability):** Cache decisions emit
//!   structured tracing events so callers running inside actor
//!   recording spans can explain whether provisioning downloaded,
//!   reused a blob, reused an install, or extracted from cache.

use std::fs;
use std::io::Read;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;

use chrono::DateTime;
use chrono::Utc;
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
use crate::fetch::FetchConfig;

const METADATA_FILE: &str = ".tool-fetch-metadata.json";

/// Content-addressed cache rooted at a caller-provided directory.
///
/// The cache is safe to share between independent handles in the same
/// process or host; per-digest advisory locks serialize provisioning.
///
/// TF-FETCH-7: the cache owns a [`reqwest::Client`] used for every
/// artifact download. Callers configure HTTPS proxy behavior via
/// [`Self::with_fetch_config`]; the cache never reads
/// `https_proxy`/`HTTPS_PROXY` env vars on its own.
#[derive(Debug, Clone)]
pub struct ToolCache {
    cache_dir: PathBuf,
    http_client: reqwest::Client,
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
    /// Provision wall-clock time, encoded as RFC 3339 in metadata.
    ///
    /// TF-CACHE-8: timestamps are stored as RFC 3339 UTC instants so
    /// operator-facing inventory and the on-disk metadata sidecar share
    /// one human-readable encoding.
    pub provisioned_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheMetadata {
    name: String,
    version: String,
    platform: Platform,
    digest: String,
    executable_path: PathBuf,
    provisioned_at: DateTime<Utc>,
}

impl ToolCache {
    /// Create a cache rooted at `cache_dir`.
    ///
    /// Tests pass a tempdir here; production actors use
    /// [`ToolCache::default_dir`]. The cache is constructed with a
    /// no-proxy HTTP client; callers that need HTTPS proxying chain
    /// [`Self::with_fetch_config`].
    pub fn new(cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            cache_dir: cache_dir.into(),
            http_client: FetchConfig::default()
                .build_client()
                .expect("default reqwest client must build"),
        }
    }

    /// Replace the cache's [`reqwest::Client`] with one built from
    /// `config`.
    ///
    /// TF-FETCH-7: this is the single point at which a policy-layer
    /// caller can inject HTTPS proxy configuration. Errors propagate
    /// from [`FetchConfig::build_client`] (e.g., unparseable proxy
    /// URL) so misconfiguration is caught at construction rather than
    /// surfacing as a download failure later.
    pub fn with_fetch_config(mut self, config: FetchConfig) -> Result<Self, ProvisionError> {
        self.http_client = config.build_client()?;
        Ok(self)
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
    ///
    /// This entry point is sync — it reads the metadata sidecar and
    /// `stat`s the executable, both via `std::fs`. Public callers can
    /// invoke it from sync contexts directly. Async callers on the
    /// provisioning path use [`Self::lookup_async`] (TF-CACHE-9) so
    /// they never run sync I/O on a tokio worker.
    pub fn lookup(&self, entry: &PlatformEntry) -> Option<PathBuf> {
        lookup_at(&self.extracted_path(&entry.digest), &entry.digest)
    }

    /// Async wrapper around [`Self::lookup`] that runs the sync I/O on
    /// the blocking pool. Used internally on the async provisioning
    /// path to honor TF-CACHE-9.
    async fn lookup_async(&self, entry: &PlatformEntry) -> Option<PathBuf> {
        let extracted_dir = self.extracted_path(&entry.digest);
        let digest = entry.digest.clone();
        tokio::task::spawn_blocking(move || lookup_at(&extracted_dir, &digest))
            .await
            .expect("tool_fetch lookup task panicked")
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

        tokio::fs::create_dir_all(self.blobs_dir(&entry.digest)).await?;
        tokio::fs::create_dir_all(self.extracted_parent(&entry.digest)).await?;
        tokio::fs::create_dir_all(self.locks_dir(&entry.digest)).await?;

        let lock_path = self.lock_path(&entry.digest);
        let lock = tokio::task::spawn_blocking(move || -> Result<fs::File, std::io::Error> {
            let lock = fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(lock_path)?;
            lock.lock_exclusive()?;
            Ok(lock)
        })
        .await
        .expect("tool_fetch lock task panicked")?;

        let result = self.provision_locked(spec, platform, entry).await;
        let unlock_result = tokio::task::spawn_blocking(move || lock.unlock())
            .await
            .expect("tool_fetch unlock task panicked");
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
        // TF-CACHE-9: even the cache-hit fast path goes through
        // spawn_blocking so the metadata `fs::read` and `is_file`
        // probe never run on a tokio worker.
        if let Some(path) = self.lookup_async(entry).await {
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

        // TF-CACHE-9: blob verification reads the entire artifact;
        // run it on the blocking pool rather than holding a tokio
        // worker. The current `tracing::Span` is captured before the
        // jump to the blocking pool and re-entered inside the closure
        // so any tracing events emitted by the sync section land in
        // the caller's recording span (e.g. an actor's flight
        // recorder) instead of being detached on the blocking thread.
        // TF-CACHE-10: emit BlobCache::Invalid from inside the closure
        // so the operator sees the rejection at the moment the cached
        // blob fails reverification.
        let blob = self.blob_path(&entry.digest);
        let blob_for_verify = blob.clone();
        let entry_for_verify = entry.clone();
        let verify_tool = spec.name.clone();
        let verify_version = spec.version.clone();
        let verify_blob_display = blob.display().to_string();
        let verify_span = tracing::Span::current();
        let needs_fetch = tokio::task::spawn_blocking(move || -> Result<bool, ProvisionError> {
            let _enter = verify_span.enter();
            if blob_for_verify.is_file() && !verify_blob(&blob_for_verify, &entry_for_verify)? {
                tracing::warn!(
                    name = "ToolFetchStatus",
                    status = "BlobCache::Invalid",
                    tool = %verify_tool,
                    version = %verify_version,
                    platform = ?platform,
                    artifact_digest = %entry_for_verify.digest,
                    blob = %verify_blob_display,
                );
                fs::remove_file(&blob_for_verify)?;
            }
            Ok(!blob_for_verify.is_file())
        })
        .await
        .expect("tool_fetch verify task panicked")?;

        if needs_fetch {
            tracing::info!(
                name = "ToolFetchStatus",
                status = "BlobCache::Miss",
                tool = %spec.name,
                version = %spec.version,
                platform = ?platform,
                artifact_digest = %entry.digest,
                blob = %blob.display(),
            );
            fetch::fetch_verified_blob(&self.http_client, entry, &blob).await?;
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

        // TF-CACHE-9: archive extraction (tar/zip) and the followup
        // metadata write, swap-in `remove_dir_all`, and atomic rename
        // are all sync and can run for seconds. Hand the whole
        // post-fetch sync section to the blocking pool so concurrent
        // actor tasks keep making progress. The `tracing::Span` is
        // captured here and re-entered inside the closure so events
        // emitted during extraction land in the caller's recording
        // span rather than on a detached blocking thread.
        // TF-CACHE-10: emit Extract::Start from inside the closure so
        // the event marks the moment extraction actually begins.
        let extracted_dir = self.extracted_path(&entry.digest);
        let extracted_parent = self.extracted_parent(&entry.digest);
        let extracted_dir_for_extract = extracted_dir.clone();
        let blob_for_extract = blob;
        let spec_for_extract = spec.clone();
        let entry_for_extract = entry.clone();
        let extract_destination_display = extracted_dir.display().to_string();
        let extract_span = tracing::Span::current();
        let executable = tokio::task::spawn_blocking(move || -> Result<PathBuf, ProvisionError> {
            let _enter = extract_span.enter();
            let temp_dir = tempfile::Builder::new()
                .prefix("extract-")
                .tempdir_in(&extracted_parent)?;

            tracing::info!(
                name = "ToolFetchStatus",
                status = "Extract::Start",
                tool = %spec_for_extract.name,
                version = %spec_for_extract.version,
                platform = ?platform,
                artifact_format = ?entry_for_extract.format,
                artifact_digest = %entry_for_extract.digest,
                destination = %extract_destination_display,
            );

            let executable = match entry_for_extract.format {
                ArtifactFormat::Plain => extract::install_plain(
                    &blob_for_extract,
                    temp_dir.path(),
                    &spec_for_extract.name,
                )?,
                ArtifactFormat::TarGz => {
                    extract::extract_tar_gz(&blob_for_extract, temp_dir.path())?;
                    resolve_archive_executable(temp_dir.path(), &entry_for_extract)?
                }
                ArtifactFormat::Zip => {
                    extract::extract_zip(&blob_for_extract, temp_dir.path())?;
                    resolve_archive_executable(temp_dir.path(), &entry_for_extract)?
                }
            };
            extract::make_executable(&executable)?;

            let executable_path = executable
                .strip_prefix(temp_dir.path())
                .map_err(|e| ProvisionError::ExtractionFailed {
                    error: e.to_string(),
                })?
                .to_path_buf();
            write_metadata(
                temp_dir.path(),
                &CacheMetadata {
                    name: spec_for_extract.name,
                    version: spec_for_extract.version,
                    platform,
                    digest: entry_for_extract.digest.clone(),
                    executable_path: executable_path.clone(),
                    provisioned_at: current_timestamp(),
                },
            )?;

            if extracted_dir_for_extract.exists() {
                fs::remove_dir_all(&extracted_dir_for_extract)?;
            }
            let temp_path = temp_dir.keep();
            fs::rename(&temp_path, &extracted_dir_for_extract)?;

            // TF-CACHE-9: the post-rename verification lookup runs
            // inside the same spawn_blocking section, not back on a
            // tokio worker.
            lookup_at(&extracted_dir_for_extract, &entry_for_extract.digest).ok_or_else(|| {
                ProvisionError::ExecutableMissing {
                    expected_path: extracted_dir_for_extract.join(executable_path),
                }
            })
        })
        .await
        .expect("tool_fetch extract task panicked")?;

        // TF-CACHE-10: Extract::Complete is the operator-visible
        // marker that the install tree is ready under
        // `extracted_dir`. It fires only after the spawn_blocking
        // closure committed the rename and the post-rename lookup
        // succeeded, so an observer can trust the event implies a
        // resolvable executable.
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
    // TF-EXTRACT-6: validate the spec's executable_path before any
    // filesystem op. Without this guard, a malicious spec like
    // `executable_path: "../../etc/some_file"` would resolve through
    // `extracted_dir.join(...)` to a path outside the extraction tree;
    // a subsequent `make_executable` in the caller would chmod that
    // external file before `lookup`'s containment check rejected the
    // path. Reject up front to keep the chmod confined.
    if !is_safe_relative_path(executable_path) {
        return Err(ProvisionError::UnsafeArchiveEntry {
            path: executable_path.display().to_string(),
            reason: "executable_path must be a relative path contained by the extraction root"
                .to_string(),
        });
    }
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

/// Static-style lookup over an explicit `extracted_dir` and `digest`,
/// used by both the sync public [`ToolCache::lookup`] and the
/// async-path [`ToolCache::lookup_async`] / extract closures so the
/// TF-CACHE-2 / TF-CACHE-4 / TF-CACHE-6 containment rules apply
/// uniformly.
fn lookup_at(extracted_dir: &Path, digest: &str) -> Option<PathBuf> {
    let metadata = read_metadata(extracted_dir).ok()?;
    if metadata.digest != digest || !is_safe_relative_path(&metadata.executable_path) {
        return None;
    }
    let executable = extracted_dir.join(metadata.executable_path);
    executable.is_file().then_some(executable)
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

fn current_timestamp() -> DateTime<Utc> {
    Utc::now()
}
