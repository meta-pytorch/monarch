/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Verified fetch, cache, and install support for diagnostic tool artifacts.
//!
//! `tool_fetch` is the standalone substrate for Monarch's diagnostic
//! capability plane: a declarative spec names an artifact, the cache
//! fetches and verifies downloaded bytes, and callers receive a stable
//! executable path that can be surfaced as mesh state by higher layers.
//!
//! # Invariant registry
//!
//! The module family uses `TF-*` labels for invariants that tests and
//! higher-level mesh integrations can reference.
//!
//! ## Spec and platform invariants
//!
//! - **TF-SPEC-1 (spec-selects-platform):** Provisioning selects exactly
//!   one [`PlatformEntry`] from a [`ToolSpec`] by [`Platform`]; callers do
//!   not pass decomposed spec fields that can drift apart.
//! - **TF-SPEC-2 (artifact-hash-contract):** `size`, `hash_algorithm`,
//!   and `digest` describe the downloaded artifact bytes, not extracted
//!   contents.
//! - **TF-SPEC-3 (plain-has-no-inner-path):** Plain artifacts use
//!   `executable_path = None` and are installed to `bin/{name}`.
//! - **TF-PLAT-1 (known-platforms-only):** Runtime platform detection
//!   returns one of the explicitly modeled [`Platform`] variants or a
//!   structured unsupported-platform error.
//!
//! ## Fetch/cache invariants
//!
//! - **TF-FETCH-1 (verify-before-use):** Downloaded bytes are size- and
//!   hash-verified before extraction or installation.
//! - **TF-FETCH-2 (downloaded-byte-hash):** SHA256 is computed over the
//!   downloaded bytes exactly as served by the provider.
//! - **TF-CACHE-1 (content-addressed-blob):** Verified artifacts live at
//!   `blobs/{digest[0..2]}/{digest}`.
//! - **TF-CACHE-2 (executable-not-blob):** Resolved executables come from
//!   the extracted/installed tree, never directly from `blobs/`.
//! - **TF-CACHE-3 (cache-hit-no-download):** A complete verified install
//!   is reused without contacting providers.
//! - **TF-CACHE-4 (metadata-gates-scan):** `scan()` only reports
//!   extracted directories with parseable `.tool-fetch-metadata.json` and
//!   an existing executable.
//! - **TF-CACHE-5 (blob-reverify):** Existing blobs are rechecked against
//!   size and digest before reuse.
//! - **TF-CACHE-6 (metadata-contained-path):** Cache metadata executable
//!   paths must stay inside their extracted artifact directory.
//! - **TF-CACHE-7 (managed-executable-runs):** The executable path
//!   returned by provisioning is directly runnable by callers.
//! - **TF-CACHE-8 (rfc3339-utc-timestamps):** Cache metadata records
//!   `provisioned_at` as an RFC 3339 UTC instant; one human-readable
//!   encoding flows from the on-disk sidecar to operator-facing
//!   inventory without per-layer reformatting.
//! - **TF-CACHE-9 (async-correctness):** All sync I/O on the
//!   provisioning path — blob verification, archive extraction,
//!   metadata write, `remove_dir_all`, atomic rename — runs on
//!   `tokio::task::spawn_blocking`, never on a tokio worker.
//! - **TF-FETCH-5 (async-write):** The HTTP download loop and the
//!   temp-file writes both run on the async runtime; per-chunk writes
//!   never hold the tokio worker.
//! - **TF-FETCH-7 (injected-client):** Fetch logic uses a caller-
//!   provided [`reqwest::Client`] built from a [`FetchConfig`]. HTTPS
//!   proxy policy is supplied by the caller; the substrate does not
//!   read `https_proxy`/`HTTPS_PROXY` env vars on its own.
//!
//! ## Extraction invariants
//!
//! - **TF-EXTRACT-1 (archive-contained):** Archive entries must stay
//!   within the extraction root.
//! - **TF-EXTRACT-2 (tar-links-rejected):** Tar symlinks and hardlinks
//!   are rejected rather than resolved.
//! - **TF-EXTRACT-3 (zip-regular-only):** Zip extraction writes only
//!   regular files and directories; symlink-like entries are rejected.
//! - **TF-EXTRACT-4 (unix-executable):** Installed/resolved executables
//!   are marked executable on Unix.
//! - **TF-EXTRACT-5 (plain-name-contained):** A `Plain` artifact's
//!   spec `name` is validated as a single normal path component before
//!   it is joined to `bin/`.
//! - **TF-EXTRACT-6 (archive-executable-path-contained):** An archive
//!   format spec's `executable_path` is validated as a contained
//!   relative path before any filesystem op, so a malicious spec
//!   cannot drive `make_executable` to chmod a file outside the
//!   extraction tree.
//!
//! ## Error invariants
//!
//! - **TF-ERR-1 (structured-failures):** Public failure modes are
//!   represented by [`ProvisionError`] variants, preserving enough detail
//!   for higher layers to map errors into operator-facing inventory.

pub mod cache;
pub mod error;
pub mod extract;
pub mod fetch;
pub mod platform;
pub mod spec;

#[cfg(test)]
mod tests;

pub use cache::CachedArtifact;
pub use cache::ToolCache;
pub use error::ProvisionError;
pub use fetch::FetchConfig;
pub use platform::current_platform;
pub use spec::ArtifactFormat;
pub use spec::HashAlgorithm;
pub use spec::Platform;
pub use spec::PlatformEntry;
pub use spec::Provider;
pub use spec::ToolSpec;

/// Bundled `py-spy 0.4.1` tool spec, embedded at compile time from
/// `tool_fetch/specs/py-spy.json`.
///
/// Exposed so consumer crates can use the canonical spec without
/// cross-crate `include_str!` against `tool_fetch`'s file layout.
/// `tool_fetch` resolves the path inside its own crate at compile
/// time, which both Cargo and Buck can satisfy from `tool_fetch`'s
/// declared sources.
pub const BUNDLED_PYSPY_SPEC_JSON: &str = include_str!("../specs/py-spy.json");

/// Decode [`BUNDLED_PYSPY_SPEC_JSON`] into a [`ToolSpec`].
///
/// Panics if the bundled JSON is malformed; that is a compile-time
/// invariant of this crate, not a runtime concern. The
/// `parses_bundled_pyspy_spec` test guards the invariant.
pub fn bundled_pyspy_spec() -> ToolSpec {
    serde_json::from_str(BUNDLED_PYSPY_SPEC_JSON)
        .expect("bundled py-spy spec is valid JSON (guarded by parses_bundled_pyspy_spec)")
}
