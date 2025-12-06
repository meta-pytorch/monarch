/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Build utilities shared across monarch *-sys crates
//!
//! This module provides common functionality for Python environment discovery,
//! CUDA installation detection, and ROCm installation detection used by various
//! build scripts.

use std::env;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

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

/// Configuration structure for ROCm environment
#[derive(Debug, Clone, Default)]
pub struct RocmConfig {
    pub rocm_home: Option<PathBuf>,
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
    RocmNotFound,
    PythonNotFound,
    CommandFailed(String),
    PathNotFound(String),
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildError::CudaNotFound => write!(f, "CUDA installation not found"),
            BuildError::RocmNotFound => write!(f, "ROCm installation not found"),
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

/// Finds the python interpreter, preferring `python3` if available.
///
/// This function checks in order:
/// 1. PYO3_PYTHON environment variable
/// 2. `python3` command availability
/// 3. Falls back to `python`
pub fn find_python_interpreter() -> PathBuf {
    get_env_var_with_rerun("PYO3_PYTHON")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            if Command::new("python3").arg("--version").output().is_ok() {
                PathBuf::from("python3")
            } else {
                PathBuf::from("python")
            }
        })
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
    // Check both old-style (include) and new-style (targets/x86_64-linux/include) CUDA installations
    for include_subdir in &["include", "targets/x86_64-linux/include"] {
        let include_dir = cuda_home_path.join(include_subdir);
        if include_dir.exists() {
            config.include_dirs.push(include_dir);
        }
    }

    // Add standard library directories
    // Check both old-style (lib64, lib) and new-style (targets/x86_64-linux/lib) CUDA installations
    for lib_subdir in &["lib64", "lib", "lib/x64", "targets/x86_64-linux/lib"] {
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
        // Check both old-style and new-style CUDA library paths
        for lib_subdir in &["lib64", "lib", "targets/x86_64-linux/lib"] {
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

/// Find ROCm home directory using various heuristics
///
/// This function attempts to locate ROCm installation through:
/// 1. ROCM_HOME environment variable
/// 2. ROCM_PATH environment variable
/// 3. Platform-specific default locations (/opt/rocm-* or /opt/rocm)
/// 4. Finding hipcc in PATH and deriving ROCm home
pub fn find_rocm_home() -> Option<String> {
    // Guess #1: Environment variables
    let mut rocm_home = get_env_var_with_rerun("ROCM_HOME")
        .ok()
        .or_else(|| get_env_var_with_rerun("ROCM_PATH").ok());

    if rocm_home.is_none() {
        // Guess #2: Platform-specific defaults (check these before PATH to avoid /usr)
        // Check for versioned ROCm installations
        let pattern = "/opt/rocm-*";
        if let Ok(entries) = glob(pattern) {
            let mut rocm_homes: Vec<_> = entries.filter_map(Result::ok).collect();
            if !rocm_homes.is_empty() {
                // Sort to get the most recent version
                rocm_homes.sort();
                rocm_homes.reverse();
                rocm_home = Some(rocm_homes[0].to_string_lossy().into_owned());
            }
        }

        // Fallback to /opt/rocm symlink
        if rocm_home.is_none() {
            let rocm_candidate = "/opt/rocm";
            if Path::new(rocm_candidate).exists() {
                rocm_home = Some(rocm_candidate.to_string());
            }
        }

        // Guess #3: Find hipcc in PATH (only if nothing else found)
        if rocm_home.is_none() {
            if let Ok(hipcc_path) = which("hipcc") {
                // Get parent directory twice (hipcc is in ROCM_HOME/bin)
                // But avoid using /usr as ROCm home
                if let Some(rocm_dir) = hipcc_path.parent().and_then(|p| p.parent()) {
                    let rocm_str = rocm_dir.to_string_lossy();
                    if rocm_str != "/usr" {
                        rocm_home = Some(rocm_str.into_owned());
                    }
                }
            }
        }
    }

    rocm_home
}

/// Detects ROCm version and returns (major, minor) or None if not found
///
/// This function attempts to detect ROCm version through:
/// 1. Reading .info/version file in ROCm home
/// 2. Parsing hipcc --version output
///
/// Returns `None` if version cannot be detected. Callers should provide
/// their own default (e.g., `.unwrap_or((6, 0))`).
pub fn get_rocm_version(rocm_home: &str) -> Option<(u32, u32)> {
    // Try to read ROCm version from .info/version file
    let version_file = PathBuf::from(rocm_home).join(".info").join("version");
    if let Ok(content) = fs::read_to_string(&version_file) {
        let trimmed = content.trim();
        if let Some((major_str, rest)) = trimmed.split_once('.') {
            if let Some((minor_str, _)) = rest.split_once('.') {
                if let (Ok(major), Ok(minor)) = (major_str.parse::<u32>(), minor_str.parse::<u32>())
                {
                    println!(
                        "cargo:warning=Detected ROCm version {}.{} from {}",
                        major,
                        minor,
                        version_file.display()
                    );
                    return Some((major, minor));
                }
            }
        }
    }

    // Fallback: try hipcc --version
    let hipcc_path = format!("{}/bin/hipcc", rocm_home);
    if let Ok(output) = Command::new(&hipcc_path).arg("--version").output() {
        let version_output = String::from_utf8_lossy(&output.stdout);
        // Look for version pattern like "HIP version: 6.2.41134"
        for line in version_output.lines() {
            if line.contains("HIP version:") {
                if let Some(version_part) = line.split("HIP version:").nth(1) {
                    let version_str = version_part.trim();
                    if let Some((major_str, rest)) = version_str.split_once('.') {
                        if let Some((minor_str, _)) = rest.split_once('.') {
                            if let (Ok(major), Ok(minor)) =
                                (major_str.parse::<u32>(), minor_str.parse::<u32>())
                            {
                                println!(
                                    "cargo:warning=Detected ROCm version {}.{} from hipcc",
                                    major, minor
                                );
                                return Some((major, minor));
                            }
                        }
                    }
                }
            }
        }
    }

    println!("cargo:warning=Could not detect ROCm version");
    None
}

/// Discover ROCm configuration including home, include dirs, and lib dirs
pub fn discover_rocm_config() -> Result<RocmConfig, BuildError> {
    let rocm_home = find_rocm_home().ok_or(BuildError::RocmNotFound)?;
    let rocm_home_path = PathBuf::from(&rocm_home);

    let mut config = RocmConfig {
        rocm_home: Some(rocm_home_path.clone()),
        include_dirs: Vec::new(),
        lib_dirs: Vec::new(),
    };

    // Add standard include directories
    for include_subdir in &["include", "include/hip"] {
        let include_dir = rocm_home_path.join(include_subdir);
        if include_dir.exists() {
            config.include_dirs.push(include_dir);
        }
    }

    // Add standard library directories
    for lib_subdir in &["lib", "lib64"] {
        let lib_dir = rocm_home_path.join(lib_subdir);
        if lib_dir.exists() {
            config.lib_dirs.push(lib_dir);
            break; // Use first found
        }
    }

    Ok(config)
}

/// Validate ROCm installation exists and is complete
pub fn validate_rocm_installation() -> Result<String, BuildError> {
    let rocm_config = discover_rocm_config()?;
    let rocm_home = rocm_config.rocm_home.ok_or(BuildError::RocmNotFound)?;
    let rocm_home_str = rocm_home.to_string_lossy().to_string();

    // Verify ROCm include directory exists
    let rocm_include_path = rocm_home.join("include");
    if !rocm_include_path.exists() {
        return Err(BuildError::PathNotFound(format!(
            "ROCm include directory at {}",
            rocm_include_path.display()
        )));
    }

    Ok(rocm_home_str)
}

/// Get ROCm library directory
pub fn get_rocm_lib_dir() -> Result<String, BuildError> {
    // Check if user explicitly set ROCM_LIB_DIR
    if let Ok(rocm_lib_dir) = env::var("ROCM_LIB_DIR") {
        return Ok(rocm_lib_dir);
    }

    // Try to deduce from ROCm configuration
    let rocm_config = discover_rocm_config()?;
    if let Some(rocm_home) = rocm_config.rocm_home {
        // Check both lib and lib64
        for lib_subdir in &["lib", "lib64"] {
            let lib_path = rocm_home.join(lib_subdir);
            if lib_path.exists() {
                return Ok(lib_path.to_string_lossy().to_string());
            }
        }
    }

    Err(BuildError::PathNotFound(
        "ROCm library directory".to_string(),
    ))
}

/// Print helpful error message for ROCm not found
pub fn print_rocm_error_help() {
    eprintln!("Error: ROCm installation not found!");
    eprintln!("Please ensure ROCm is installed and one of the following is true:");
    eprintln!("  1. Set ROCM_HOME environment variable to your ROCm installation directory");
    eprintln!("  2. Set ROCM_PATH environment variable to your ROCm installation directory");
    eprintln!("  3. Ensure 'hipcc' is in your PATH");
    eprintln!("  4. Install ROCm to the default location (/opt/rocm on Linux)");
    eprintln!();
    eprintln!("Example: export ROCM_HOME=/opt/rocm-6.4.2");
}

/// Print helpful error message for ROCm lib dir not found
pub fn print_rocm_lib_error_help() {
    eprintln!("Error: ROCm library directory not found!");
    eprintln!("Please set ROCM_LIB_DIR environment variable to your ROCm library directory.");
    eprintln!();
    eprintln!("Example: export ROCM_LIB_DIR=/opt/rocm/lib");
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
    fn test_find_rocm_home_env_var() {
        env::set_var("ROCM_HOME", "/test/rocm");
        let result = find_rocm_home();
        env::remove_var("ROCM_HOME");
        assert_eq!(result, Some("/test/rocm".to_string()));
    }

    #[test]
    fn test_python_scripts_constants() {
        assert!(PYTHON_PRINT_DIRS.contains("sysconfig"));
        assert!(PYTHON_PRINT_PYTORCH_DETAILS.contains("torch"));
        assert!(PYTHON_PRINT_CUDA_DETAILS.contains("CUDA_HOME"));
    }
}
