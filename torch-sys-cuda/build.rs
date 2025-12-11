/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This build script locates CUDA/ROCm libraries and headers for torch-sys-cuda,
//! which provides CUDA-specific PyTorch functionality. It depends on the base
//! torch-sys crate for core PyTorch integration.

#![feature(exit_status_error)]

use std::env;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use cxx_build::CFG;

#[cfg(target_os = "macos")]
fn main() {}

/// Hipify the bridge sources for ROCm compatibility using build_utils,
/// then apply torch-sys-cuda specific patches.
/// Returns the path to the hipified bridge.rs file.
fn hipify_sources(
    src_dir: &Path,
    hip_src_dir: &Path,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    println!(
        "cargo:warning=torch-sys-cuda: Hipifying sources from {} to {}...",
        src_dir.display(),
        hip_src_dir.display()
    );

    // Find project root
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let project_root = manifest_dir
        .parent()
        .ok_or("Failed to find project root")?;

    // Collect source files to hipify
    let source_files: Vec<PathBuf> = ["bridge.h", "bridge.cpp", "bridge.rs"]
        .iter()
        .map(|f| src_dir.join(f))
        .filter(|p| p.exists())
        .collect();

    // Use centralized hipify function from build_utils
    build_utils::run_hipify_torch(project_root, &source_files, hip_src_dir)
        .map_err(|e| format!("hipify_torch failed: {}", e))?;

    // Apply torch-sys-cuda specific patches:
    // Modify bridge.cpp to include bridge_hip.h instead of the original header
    let bridge_cpp_path = hip_src_dir.join("bridge.cpp");
    let bridge_cpp_content = fs::read_to_string(&bridge_cpp_path)?;
    let modified_cpp = bridge_cpp_content.replace(
        "#include \"monarch/torch-sys-cuda/src/bridge.h\"",
        "#include \"bridge_hip.h\"",
    );
    fs::write(&bridge_cpp_path, modified_cpp)?;

    // Modify bridge.rs to include bridge_hip.h instead of the original header
    let bridge_rs_path = hip_src_dir.join("bridge.rs");
    let bridge_rs_content = fs::read_to_string(&bridge_rs_path)?;
    let modified_rs = bridge_rs_content.replace(
        "include!(\"monarch/torch-sys-cuda/src/bridge.h\")",
        "include!(\"bridge_hip.h\")",
    );
    fs::write(&bridge_rs_path, modified_rs)?;

    println!("cargo:warning=torch-sys-cuda: hipify complete");

    // Return the path to the hipified bridge.rs
    Ok(bridge_rs_path)
}

#[cfg(not(target_os = "macos"))]
fn main() {
    // Declare custom cfg options to avoid warnings
    println!("cargo::rustc-check-cfg=cfg(rocm)");
    println!("cargo::rustc-check-cfg=cfg(rocm_6_x)");
    println!("cargo::rustc-check-cfg=cfg(rocm_7_plus)");

    // Auto-detect ROCm vs CUDA using build_utils
    let (is_rocm, compute_home) =
        if let Ok(rocm_home) = build_utils::validate_rocm_installation() {
            let version = build_utils::get_rocm_version(&rocm_home).unwrap_or((6, 0));
            println!(
                "cargo:warning=torch-sys-cuda: Using ROCm {}.{} at {}",
                version.0, version.1, rocm_home
            );
            println!("cargo:rustc-cfg=rocm");
            if version.0 >= 7 {
                println!("cargo:rustc-cfg=rocm_7_plus");
            } else {
                println!("cargo:rustc-cfg=rocm_6_x");
            }
            (true, rocm_home)
        } else if let Ok(cuda_home) = build_utils::validate_cuda_installation() {
            println!(
                "cargo:warning=torch-sys-cuda: Using CUDA at {}",
                cuda_home
            );
            (false, cuda_home)
        } else {
            panic!("Neither CUDA nor ROCm installation found!");
        };

    // Use PyO3's Python discovery to find the correct Python library paths
    let mut python_lib_dir: Option<String> = None;
    let python_config = pyo3_build_config::get();

    // Add Python library directory to search path
    if let Some(lib_dir) = &python_config.lib_dir {
        println!("cargo::rustc-link-search=native={}", lib_dir);
        python_lib_dir = Some(lib_dir.clone());
    }

    // On some platforms, we may need to explicitly link against Python
    if let Some(lib_name) = &python_config.lib_name {
        println!("cargo::rustc-link-lib={}", lib_name);
    }

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let src_dir = manifest_dir.join("src");
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());

    // Determine source files to compile
    let (bridge_rs_path, bridge_cpp_path, include_dir) = if is_rocm {
        let hip_src_dir = out_path.join("hipified_src");
        let hipified_bridge_rs = hipify_sources(&src_dir, &hip_src_dir)
            .expect("Failed to hipify torch-sys-cuda sources");

        (hipified_bridge_rs, hip_src_dir.join("bridge.cpp"), hip_src_dir)
    } else {
        (src_dir.join("bridge.rs"), src_dir.join("bridge.cpp"), src_dir.clone())
    };

    // Prefix includes with `monarch` to maintain consistency with fbcode folder structure
    CFG.include_prefix = "monarch/torch-sys-cuda";

    let mut builder = cxx_build::bridge(&bridge_rs_path);
    builder
        .file(&bridge_cpp_path)
        .flag("-std=c++14")
        .include(format!("{}/include", compute_home))
        .include(&include_dir)
        // Suppress warnings, otherwise we get massive spew from libtorch
        .flag_if_supported("-w");

    // Add platform-specific defines
    if is_rocm {
        builder
            .define("__HIP_PLATFORM_AMD__", "1")
            .define("USE_ROCM", "1");
    }

    builder.compile("torch-sys-cuda");

    // Configure platform-specific linking
    if is_rocm {
        // ROCm uses amdhip64 and rccl
        println!("cargo::rustc-link-lib=amdhip64");
        println!("cargo::rustc-link-lib=rccl");
        println!("cargo::rustc-link-search=native={}/lib", compute_home);
    } else {
        // CUDA uses cudart
        println!("cargo::rustc-link-lib=cudart");
        println!("cargo::rustc-link-search=native={}/lib64", compute_home);
    }

    // Add Python library directory to rpath for runtime linking
    if let Some(python_lib_dir) = &python_lib_dir {
        println!("cargo::rustc-link-arg=-Wl,-rpath,{}", python_lib_dir);
    }

    println!("cargo::rerun-if-changed=src/bridge.rs");
    println!("cargo::rerun-if-changed=src/bridge.cpp");
    println!("cargo::rerun-if-changed=src/bridge.h");
}
