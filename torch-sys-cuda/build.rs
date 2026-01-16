/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
//! This build script configures platform detection for torch-sys-cuda.
//! The crate is now pure Rust, using nccl-sys for CUDA/HIP type bindings.
//! No C++ compilation is needed.
#[cfg(target_os = "macos")]
fn main() {}
#[cfg(not(target_os = "macos"))]
fn main() {
    // Set up Python rpath for runtime linking
    build_utils::set_python_rpath();
    // Statically link libstdc++ to avoid runtime dependency on system libstdc++
    build_utils::link_libstdcpp_static();

    // Declare custom cfg options to avoid warnings
    println!("cargo::rustc-check-cfg=cfg(rocm)");
    println!("cargo::rustc-check-cfg=cfg(rocm_6_x)");
    println!("cargo::rustc-check-cfg=cfg(rocm_7_plus)");

    // Auto-detect ROCm vs CUDA using build_utils
    let (is_rocm, compute_home) = if let Ok(rocm_home) = build_utils::validate_rocm_installation() {
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
        println!("cargo:warning=torch-sys-cuda: Using CUDA at {}", cuda_home);
        (false, cuda_home)
    } else {
        panic!("Neither CUDA nor ROCm installation found!");
    };

    // Configure platform-specific library search paths
    // Actual library linking is handled by nccl-sys dependency
    if is_rocm {
        println!("cargo::rustc-link-search=native={}/lib", compute_home);
    } else {
        println!("cargo::rustc-link-search=native={}/lib64", compute_home);
    }
}
