/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Build utilities shared across monarch *-sys crates
//!
//! This module provides common functionality for Python environment discovery
//! and CUDA installation detection used by various build scripts.

use std::env;
use std::path::Path;
use std::path::PathBuf;

use glob::glob;
use which::which;

/// Python script to extract Python paths from sysconfig
pub const PYTHON_PRINT_DIRS: &str = r"
import sysconfig
print('PYTHON_INCLUDE_DIR:', sysconfig.get_config_var('INCLUDEDIR'))
print('PYTHON_LIB_DIR:', sysconfig.get_config_var('LIBDIR'))
";

/// Python script to extract PyTorch details from torch.utils.cpp_extension
pub const PYTHON_PRINT_PYTORCH_DETAILS: &str = r"
import torch
from torch.utils import cpp_extension
print('LIBTORCH_CXX11:', torch._C._GLIBCXX_USE_CXX11_ABI)
for include_path in cpp_extension.include_paths():
    print('LIBTORCH_INCLUDE:', include_path)
for library_path in cpp_extension.library_paths():
    print('LIBTORCH_LIB:', library_path)
";

/// Python script to extract PyTorch details including CUDA info
pub const PYTHON_PRINT_CUDA_DETAILS: &str = r"
import torch
from torch.utils import cpp_extension
print('CUDA_HOME:', cpp_extension.CUDA_HOME)
for include_path in cpp_extension.include_paths():
    print('LIBTORCH_INCLUDE:', include_path)
for library_path in cpp_extension.library_paths():
    print('LIBTORCH_LIB:', library_path)
print('LIBTORCH_CXX11:', torch._C._GLIBCXX_USE_CXX11_ABI)
";

/// Python script to extract Python include paths
pub const PYTHON_PRINT_INCLUDE_PATH: &str = r"
import sysconfig
print('PYTHON_INCLUDE:', sysconfig.get_path('include'))
print('PYTHON_INCLUDE_DIR:', sysconfig.get_config_var('INCLUDEDIR'))
print('PYTHON_LIB_DIR:', sysconfig.get_config_var('LIBDIR'))
";

/// Configuration structure for CUDA environment
#[derive(Debug, Clone, Default)]
pub struct CudaConfig {
    pub cuda_home: Option<PathBuf>,
    pub include_dirs: Vec<PathBuf>,
    pub lib_dirs: Vec<PathBuf>,
}

/// Result of Python environment discovery
#[derive(Debug, Clone)]
pub struct PythonConfig {
    pub include_dir: Option<String>,
    pub lib_dir: Option<String>,
}

/// Error type for build utilities
#[derive(Debug)]
pub enum BuildError {
    CudaNotFound,
    PythonNotFound,
    CommandFailed(String),
    PathNotFound(String),
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildError::CudaNotFound => write!(f, "CUDA installation not found"),
            BuildError::PythonNotFound => write!(f, "Python interpreter not found"),
            BuildError::CommandFailed(cmd) => write!(f, "Command failed: {}", cmd),
            BuildError::PathNotFound(path) => write!(f, "Path not found: {}", path),
        }
    }
}

impl std::error::Error for BuildError {}

/// Get environment variable with cargo rerun notification
pub fn get_env_var_with_rerun(name: &str) -> Result<String, std::env::VarError> {
    println!("cargo::rerun-if-env-changed={}", name);
    env::var(name)
}

/// Find CUDA home directory using various heuristics
///
/// This function attempts to locate CUDA installation through:
/// 1. CUDA_HOME environment variable
/// 2. CUDA_PATH environment variable
/// 3. Finding nvcc in PATH and deriving cuda home
/// 4. Platform-specific default locations
pub fn find_cuda_home() -> Option<String> {
    // Guess #1: Environment variables
    let mut cuda_home = get_env_var_with_rerun("CUDA_HOME")
        .ok()
        .or_else(|| get_env_var_with_rerun("CUDA_PATH").ok());

    if cuda_home.is_none() {
        // Guess #2: Find nvcc in PATH
        if let Ok(nvcc_path) = which("nvcc") {
            // Get parent directory twice (nvcc is in CUDA_HOME/bin)
            if let Some(cuda_dir) = nvcc_path.parent().and_then(|p| p.parent()) {
                cuda_home = Some(cuda_dir.to_string_lossy().into_owned());
            }
        } else {
            // Guess #3: Platform-specific defaults
            if cfg!(windows) {
                let pattern = r"C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v*.*";
                let cuda_homes: Vec<_> = glob(pattern).unwrap().filter_map(Result::ok).collect();
                if !cuda_homes.is_empty() {
                    cuda_home = Some(cuda_homes[0].to_string_lossy().into_owned());
                }
            } else {
                // Unix-like systems
                let cuda_candidate = "/usr/local/cuda";
                if Path::new(cuda_candidate).exists() {
                    cuda_home = Some(cuda_candidate.to_string());
                }
            }
        }
    }

    cuda_home
}

/// Discover CUDA configuration including home, include dirs, and lib dirs
pub fn discover_cuda_config() -> Result<CudaConfig, BuildError> {
    let cuda_home = find_cuda_home().ok_or(BuildError::CudaNotFound)?;
    let cuda_home_path = PathBuf::from(&cuda_home);

    let mut config = CudaConfig {
        cuda_home: Some(cuda_home_path.clone()),
        include_dirs: Vec::new(),
        lib_dirs: Vec::new(),
    };

    // Add standard include directories
    // Check both old-style (include) and new-style target-specific paths
    // Support both x86_64 and aarch64/ARM architectures
    for include_subdir in &[
        "include",
        "targets/x86_64-linux/include",
        "targets/aarch64-linux/include",
        "targets/sbsa-linux/include",
    ] {
        let include_dir = cuda_home_path.join(include_subdir);
        if include_dir.exists() {
            config.include_dirs.push(include_dir);
        }
    }

    // Add standard library directories
    // Check both old-style and new-style CUDA installations for both x86_64 and aarch64
    // Try architecture-specific paths first, then generic paths
    for lib_subdir in &[
        "lib64",                          // Common x86_64 location
        "lib",                             // Common aarch64 location
        "lib/x64",                         // Windows x64
        "targets/x86_64-linux/lib",        // CUDA toolkit x86_64
        "targets/aarch64-linux/lib",       // CUDA toolkit aarch64
        "targets/sbsa-linux/lib",          // CUDA toolkit ARM server
        "lib/aarch64-linux-gnu",           // Debian/Ubuntu aarch64
        "lib/x86_64-linux-gnu",            // Debian/Ubuntu x86_64
    ] {
        let lib_dir = cuda_home_path.join(lib_subdir);
        if lib_dir.exists() {
            config.lib_dirs.push(lib_dir);
            break; // Use first found
        }
    }

    Ok(config)
}

/// Validate CUDA installation exists and is complete
pub fn validate_cuda_installation() -> Result<String, BuildError> {
    let cuda_config = discover_cuda_config()?;
    let cuda_home = cuda_config.cuda_home.ok_or(BuildError::CudaNotFound)?;
    let cuda_home_str = cuda_home.to_string_lossy().to_string();

    // Verify CUDA include directory exists
    let cuda_include_path = cuda_home.join("include");
    if !cuda_include_path.exists() {
        return Err(BuildError::PathNotFound(format!(
            "CUDA include directory at {}",
            cuda_include_path.display()
        )));
    }

    Ok(cuda_home_str)
}

/// Get CUDA library directory
pub fn get_cuda_lib_dir() -> Result<String, BuildError> {
    // Check if user explicitly set CUDA_LIB_DIR
    if let Ok(cuda_lib_dir) = env::var("CUDA_LIB_DIR") {
        return Ok(cuda_lib_dir);
    }

    // Try to deduce from CUDA configuration
    let cuda_config = discover_cuda_config()?;
    if let Some(cuda_home) = cuda_config.cuda_home {
        // Check both x86_64 and aarch64 CUDA library paths
        for lib_subdir in &[
            "lib64",                          // Common x86_64 location
            "lib",                             // Common aarch64 location
            "targets/x86_64-linux/lib",        // CUDA toolkit x86_64
            "targets/aarch64-linux/lib",       // CUDA toolkit aarch64
            "targets/sbsa-linux/lib",          // CUDA toolkit ARM server
            "lib/aarch64-linux-gnu",           // Debian/Ubuntu aarch64
            "lib/x86_64-linux-gnu",            // Debian/Ubuntu x86_64
        ] {
            let lib_path = cuda_home.join(lib_subdir);
            if lib_path.exists() {
                return Ok(lib_path.to_string_lossy().to_string());
            }
        }
    }

    Err(BuildError::PathNotFound(
        "CUDA library directory".to_string(),
    ))
}

/// Discover Python environment directories using sysconfig
///
/// Returns tuple of (include_dir, lib_dir) as optional strings
pub fn python_env_dirs() -> Result<PythonConfig, BuildError> {
    python_env_dirs_with_interpreter("python")
}

/// Discover Python environment directories with specific interpreter
pub fn python_env_dirs_with_interpreter(interpreter: &str) -> Result<PythonConfig, BuildError> {
    let output = std::process::Command::new(interpreter)
        .arg("-c")
        .arg(PYTHON_PRINT_DIRS)
        .output()
        .map_err(|_| BuildError::CommandFailed(format!("running {}", interpreter)))?;

    if !output.status.success() {
        return Err(BuildError::CommandFailed(format!(
            "{} exited with error",
            interpreter
        )));
    }

    let mut include_dir = None;
    let mut lib_dir = None;

    for line in String::from_utf8_lossy(&output.stdout).lines() {
        if let Some(path) = line.strip_prefix("PYTHON_INCLUDE_DIR: ") {
            include_dir = Some(path.to_string());
        }
        if let Some(path) = line.strip_prefix("PYTHON_LIB_DIR: ") {
            lib_dir = Some(path.to_string());
        }
    }

    Ok(PythonConfig {
        include_dir,
        lib_dir,
    })
}

/// Print helpful error message for CUDA not found
pub fn print_cuda_error_help() {
    eprintln!("Error: CUDA installation not found!");
    eprintln!("Please ensure CUDA is installed and one of the following is true:");
    eprintln!("  1. Set CUDA_HOME environment variable to your CUDA installation directory");
    eprintln!("  2. Set CUDA_PATH environment variable to your CUDA installation directory");
    eprintln!("  3. Ensure 'nvcc' is in your PATH");
    eprintln!("  4. Install CUDA to the default location (/usr/local/cuda on Linux)");
    eprintln!();
    eprintln!("Example: export CUDA_HOME=/usr/local/cuda-12.0");
}

/// Print helpful error message for CUDA lib dir not found
pub fn print_cuda_lib_error_help() {
    eprintln!("Error: CUDA library directory not found!");
    eprintln!("Please set CUDA_LIB_DIR environment variable to your CUDA library directory.");
    eprintln!();
    eprintln!("Example: export CUDA_LIB_DIR=/usr/local/cuda-12.0/lib64");
    eprintln!("Or: export CUDA_LIB_DIR=/usr/lib64");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_cuda_home_env_var() {
        env::set_var("CUDA_HOME", "/test/cuda");
        let result = find_cuda_home();
        env::remove_var("CUDA_HOME");
        assert_eq!(result, Some("/test/cuda".to_string()));
    }

    #[test]
    fn test_python_scripts_constants() {
        assert!(PYTHON_PRINT_DIRS.contains("sysconfig"));
        assert!(PYTHON_PRINT_PYTORCH_DETAILS.contains("torch"));
        assert!(PYTHON_PRINT_CUDA_DETAILS.contains("CUDA_HOME"));
    }
}
