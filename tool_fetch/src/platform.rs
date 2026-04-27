//! Runtime platform detection for tool specs.
//!
//! Platform detection is deliberately small and explicit. The first
//! spike supports the platform matrix needed for local macOS arm64
//! development and Linux pickup at Meta; unsupported OS/architecture
//! pairs fail as structured errors instead of guessing.
//!
//! # Invariants
//!
//! - **TF-PLAT-1 (known-platforms-only):** [`current_platform`]
//!   returns an explicitly modeled [`crate::Platform`] or
//!   [`crate::ProvisionError::UnsupportedPlatform`].
//! - **TF-PLAT-2 (spec-key-alignment):** Returned platform variants
//!   match the serde keys accepted by [`crate::ToolSpec`].

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::Platform;
use crate::ProvisionError;

/// Detect the current host platform.
///
/// TF-PLAT-1: only explicitly supported platforms are returned.
pub fn current_platform() -> Result<Platform, ProvisionError> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => Ok(Platform::LinuxX86_64),
        ("linux", "aarch64") => Ok(Platform::LinuxAarch64),
        ("macos", "x86_64") => Ok(Platform::MacosX86_64),
        ("macos", "aarch64") => Ok(Platform::MacosAarch64),
        (os, arch) => Err(ProvisionError::UnsupportedPlatform {
            os: os.to_string(),
            arch: arch.to_string(),
        }),
    }
}
