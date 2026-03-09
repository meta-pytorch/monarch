/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! ROCm installation detection and utilities
//!
//! This module provides functionality for detecting ROCm installations,
//! validating versions, and running hipify_torch for CUDA-to-HIP conversion.

use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

use which::which;

use crate::BuildError;
use crate::get_env_var_with_rerun;

/// Validate ROCm installation exists and return ROCm home path
///
/// Checks for ROCm installation through:
/// 1. ROCM_PATH environment variable
/// 2. Default location /opt/rocm
/// 3. Finding hipcc in PATH and resolving symlinks
pub fn validate_rocm_installation() -> Result<String, BuildError> {
    // Try ROCM_PATH environment variable first
    if let Ok(rocm_path) = get_env_var_with_rerun("ROCM_PATH") {
        if Path::new(&rocm_path).join("bin/hipcc").exists() {
            return Ok(rocm_path);
        }
    }

    // Try default location /opt/rocm (handles versioned installs like /opt/rocm-7.1.1 via symlink)
    let default_rocm = "/opt/rocm";
    if Path::new(default_rocm).join("bin/hipcc").exists() {
        // Resolve symlink to get actual versioned path if it exists
        if let Ok(canonical) = fs::canonicalize(default_rocm) {
            return Ok(canonical.to_string_lossy().to_string());
        }
        return Ok(default_rocm.to_string());
    }

    // Try finding hipcc in PATH and resolving symlinks
    if let Ok(hipcc_path) = which("hipcc") {
        // Resolve symlinks to get the real path
        if let Ok(real_hipcc) = fs::canonicalize(&hipcc_path) {
            if let Some(rocm_home) = real_hipcc.parent().and_then(|p| p.parent()) {
                return Ok(rocm_home.to_string_lossy().to_string());
            }
        }
    }

    Err(BuildError::PathNotFound("ROCm installation".to_string()))
}

/// Get ROCm version from installation
///
/// Returns (major, minor) version tuple. Requires ROCm 7.0+.
pub fn get_rocm_version(rocm_home: &str) -> Result<(u32, u32), BuildError> {
    let version_file = Path::new(rocm_home).join(".info/version");

    if let Ok(content) = fs::read_to_string(&version_file) {
        // Parse version like "6.0.2" or "7.0.0"
        let parts: Vec<&str> = content.trim().split('.').collect();
        if parts.len() >= 2 {
            if let (Ok(major), Ok(minor)) = (parts[0].parse::<u32>(), parts[1].parse::<u32>()) {
                // Enforce ROCm 7.0+ requirement
                if major < 7 {
                    return Err(BuildError::CommandFailed(format!(
                        "ROCm {}.{} detected, but ROCm 7.0+ is required",
                        major, minor
                    )));
                }
                return Ok((major, minor));
            }
        }
    }

    Err(BuildError::PathNotFound("ROCm version file".to_string()))
}

/// Get ROCm library directory
pub fn get_rocm_lib_dir() -> Result<String, BuildError> {
    let rocm_home = validate_rocm_installation()?;
    let lib_path = Path::new(&rocm_home).join("lib");

    if lib_path.exists() {
        Ok(lib_path.to_string_lossy().to_string())
    } else {
        Err(BuildError::PathNotFound("ROCm lib directory".to_string()))
    }
}

/// Run hipify_torch to convert CUDA sources to HIP
///
/// # Arguments
/// * `project_root` - Root directory containing deps/hipify_torch
/// * `source_files` - CUDA source files to hipify
/// * `output_dir` - Directory to write hipified files
pub fn run_hipify_torch(
    project_root: &Path,
    source_files: &[PathBuf],
    output_dir: &Path,
) -> Result<(), BuildError> {
    // Create output directory
    fs::create_dir_all(output_dir).map_err(|e| {
        BuildError::PathNotFound(format!("Failed to create output directory: {}", e))
    })?;

    // Copy source files to output directory (hipify runs in-place)
    for source_file in source_files {
        let filename = source_file.file_name().ok_or_else(|| {
            BuildError::PathNotFound(format!("Invalid source file: {:?}", source_file))
        })?;
        let dest = output_dir.join(filename);
        fs::copy(source_file, &dest).map_err(|e| {
            BuildError::CommandFailed(format!(
                "Failed to copy {:?} to {:?}: {}",
                source_file, dest, e
            ))
        })?;
    }

    // Find hipify script
    let hipify_script = project_root.join("deps/hipify_torch/hipify_cli.py");
    if !hipify_script.exists() {
        return Err(BuildError::PathNotFound(format!(
            "hipify_cli.py not found at {:?}. Did you initialize the submodule?",
            hipify_script
        )));
    }

    // Find Python interpreter
    let python = find_python_interpreter();

    // Run hipify_torch
    let mut cmd = Command::new(&python);
    cmd.arg(&hipify_script)
        .arg("--project-directory")
        .arg(output_dir)
        .arg("--v2")
        .arg("--output-directory")
        .arg(output_dir);

    let output = cmd
        .output()
        .map_err(|e| BuildError::CommandFailed(format!("Failed to run hipify_torch: {}", e)))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(BuildError::CommandFailed(format!(
            "hipify_torch failed:\nstdout: {}\nstderr: {}",
            stdout, stderr
        )));
    }

    Ok(())
}

/// Find Python interpreter (python3 or python)
fn find_python_interpreter() -> String {
    which("python3")
        .or_else(|_| which("python"))
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| "python3".to_string())
}
