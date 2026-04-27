//! Structured provisioning errors.
//!
//! These errors form the boundary that future mesh/admin layers will
//! map into operator-facing inventory states such as "unsupported
//! platform", "hash mismatch", or "unsafe archive".
//!
//! # Invariants
//!
//! - **TF-ERR-1 (structured-failures):** Expected provisioning failure
//!   classes have explicit variants rather than opaque strings.
//! - **TF-ERR-2 (operator-context):** Variants carry the path, URL,
//!   digest, or platform detail needed to explain the failure.

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::path::PathBuf;

/// Error returned while fetching, verifying, extracting, or resolving a tool.
///
/// TF-ERR-1: each variant is a stable failure class that higher layers
/// can translate into mesh inventory state.
#[derive(Debug, thiserror::Error)]
pub enum ProvisionError {
    /// The host OS/architecture pair has no supported platform mapping.
    #[error("unsupported platform: {os}-{arch}")]
    UnsupportedPlatform {
        /// Runtime OS string from `std::env::consts::OS`, or a spec-selection
        /// context string when no platform entry exists.
        os: String,
        /// Runtime architecture string from `std::env::consts::ARCH`, or
        /// explanatory context when no platform entry exists.
        arch: String,
    },

    /// Provider download failed or returned bytes with the wrong size.
    #[error("download failed from {url}: {error}")]
    DownloadFailed {
        /// Provider URL that failed.
        url: String,
        /// Human-readable download or size error.
        error: String,
    },

    /// Downloaded bytes did not match the expected digest.
    #[error("hash mismatch: expected {expected}, actual {actual}")]
    HashMismatch {
        /// Expected hex digest from the spec.
        expected: String,
        /// Actual hex digest computed from downloaded bytes.
        actual: String,
    },

    /// Archive extraction failed for an I/O or format reason.
    #[error("extraction failed: {error}")]
    ExtractionFailed {
        /// Human-readable extraction error.
        error: String,
    },

    /// Archive entry failed safety validation.
    #[error("unsafe archive entry {path}: {reason}")]
    UnsafeArchiveEntry {
        /// Archive path that was rejected.
        path: String,
        /// Safety rule that rejected the path.
        reason: String,
    },

    /// The expected executable path was not present after install/extract.
    #[error("executable missing: {expected_path}")]
    ExecutableMissing {
        /// Fully qualified expected executable path.
        expected_path: PathBuf,
    },

    /// A per-digest advisory lock could not be acquired.
    #[error("lock contention for digest {digest}")]
    LockContention {
        /// Artifact digest whose lock could not be acquired.
        digest: String,
    },

    /// Filesystem error.
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    /// Cache metadata JSON error.
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
}
