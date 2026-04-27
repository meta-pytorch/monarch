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

pub use cache::CachedArtifact;
pub use cache::ToolCache;
pub use error::ProvisionError;
pub use platform::current_platform;
pub use spec::ArtifactFormat;
pub use spec::HashAlgorithm;
pub use spec::Platform;
pub use spec::PlatformEntry;
pub use spec::Provider;
pub use spec::ToolSpec;
