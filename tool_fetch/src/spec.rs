//! Declarative tool specification types.
//!
//! Specs are data: they describe what artifact should exist for each
//! platform and how to verify it. They do not encode Monarch behavior.
//!
//! # Invariants
//!
//! - **TF-SPEC-1 (spec-selects-platform):** [`ToolSpec::platforms`]
//!   is the only place a platform artifact is selected from a spec.
//! - **TF-SPEC-2 (artifact-hash-contract):** [`PlatformEntry::size`],
//!   [`PlatformEntry::hash_algorithm`], and [`PlatformEntry::digest`]
//!   describe downloaded bytes.
//! - **TF-SPEC-3 (plain-has-no-inner-path):** [`ArtifactFormat::Plain`]
//!   entries use `None` for [`PlatformEntry::executable_path`].

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;

/// Declarative description of a single tool version.
///
/// A `ToolSpec` is intentionally independent of Monarch actor types so
/// the fetch/verify/cache layer can remain reusable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolSpec {
    /// Stable tool name used by inventory and resolution, e.g. `py-spy`.
    pub name: String,
    /// Tool version string shown to operators and stored in cache metadata.
    pub version: String,
    /// Per-platform artifact entries keyed by the normalized platform.
    ///
    /// TF-SPEC-1: provisioning selects from this map using the target
    /// [`Platform`].
    pub platforms: HashMap<Platform, PlatformEntry>,
}

/// Artifact metadata and providers for one platform.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlatformEntry {
    /// Size of the downloaded artifact in bytes.
    ///
    /// TF-SPEC-2: this is the compressed/downloaded size for archive
    /// formats, not the extracted size.
    pub size: u64,
    /// Hash algorithm used to verify the downloaded artifact bytes.
    pub hash_algorithm: HashAlgorithm,
    /// Hex digest of the downloaded artifact bytes.
    pub digest: String,
    /// Artifact container format.
    pub format: ArtifactFormat,
    /// Path to the executable inside the extracted tree.
    ///
    /// TF-SPEC-3: `None` is valid only for [`ArtifactFormat::Plain`],
    /// which installs to `bin/{ToolSpec::name}`.
    pub executable_path: Option<PathBuf>,
    /// Download providers tried in order.
    pub providers: Vec<Provider>,
}

/// Artifact source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Provider {
    /// HTTP(S) URL for the artifact bytes.
    Http { url: String },
}

/// Normalized platform keys supported by the first spike.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Platform {
    /// Linux on x86_64.
    #[serde(rename = "linux-x86_64")]
    LinuxX86_64,
    /// Linux on AArch64.
    #[serde(rename = "linux-aarch64")]
    LinuxAarch64,
    /// macOS on x86_64.
    #[serde(rename = "macos-x86_64")]
    MacosX86_64,
    /// macOS on Apple Silicon / AArch64.
    #[serde(rename = "macos-aarch64")]
    MacosAarch64,
}

/// Supported artifact hash algorithms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HashAlgorithm {
    /// SHA-256 over downloaded artifact bytes.
    Sha256,
}

/// Supported artifact container/install formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ArtifactFormat {
    /// Downloaded bytes are the executable artifact.
    Plain,
    /// Gzip-compressed tar archive.
    TarGz,
    /// Zip archive, including Python wheels.
    Zip,
}
