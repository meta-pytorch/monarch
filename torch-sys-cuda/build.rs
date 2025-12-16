/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This build script locates CUDA libraries and headers for torch-sys-cuda,
//! which provides CUDA-specific PyTorch functionality. It depends on the base
//! torch-sys crate for core PyTorch integration.

#![feature(exit_status_error)]

#[cfg(target_os = "macos")]
fn main() {}

#[cfg(not(target_os = "macos"))]
fn main() {
    // Use PyO3's Python discovery to find the correct Python library paths
    // This is more robust than hardcoding platform-specific paths
    let mut python_lib_dir: Option<String> = None;
    let python_config = pyo3_build_config::get();

    // Add Python library directory to search path
    // This allows the linker to find libpython when needed by test binaries
    if let Some(lib_dir) = &python_config.lib_dir {
        println!("cargo::rustc-link-search=native={}", lib_dir);
        python_lib_dir = Some(lib_dir.clone());
    }

    // Do NOT link against libpython in the build script!
    // Library crates for Python extensions should not have DT_NEEDED for libpython.
    // Test binaries that need libpython use #[cfg(test)] #[link] in lib.rs instead.

    // Statically link libstdc++ to avoid runtime dependency on system libstdc++
    build_utils::link_libstdcpp_static();

    // Add Python library directory to rpath for runtime linking
    if let Some(python_lib_dir) = &python_lib_dir {
        println!("cargo::rustc-link-arg=-Wl,-rpath,{}", python_lib_dir);
    }
}
