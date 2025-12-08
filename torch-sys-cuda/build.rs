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

use std::path::PathBuf;

use build_utils::find_cuda_home;
use cxx_build::CFG;

#[cfg(target_os = "macos")]
fn main() {}

#[cfg(not(target_os = "macos"))]
fn main() {
    // Use PyO3's Python discovery to find the correct Python library paths
    // This is more robust than hardcoding platform-specific paths
    let mut python_lib_dir: Option<String> = None;
    let python_config = pyo3_build_config::get();

    // Add Python library directory to search path
    if let Some(lib_dir) = &python_config.lib_dir {
        println!("cargo::rustc-link-search=native={}", lib_dir);
        python_lib_dir = Some(lib_dir.clone());
    }

    // On some platforms, we may need to explicitly link against Python
    // PyO3 handles the complexity of determining when this is needed
    if let Some(lib_name) = &python_config.lib_name {
        println!("cargo::rustc-link-lib={}", lib_name);
    }

    let cuda_home = PathBuf::from(find_cuda_home().expect("CUDA installation not found"));

    // Prefix includes with `monarch` to maintain consistency with fbcode
    // folder structure
    CFG.include_prefix = "monarch/torch-sys-cuda";
    let _builder = cxx_build::bridge("src/bridge.rs")
        .file("src/bridge.cpp")
        .flag("-std=c++14")
        .include(format!("{}/include", cuda_home.display()))
        // Suppress warnings, otherwise we get massive spew from libtorch
        .flag_if_supported("-w")
        .compile("torch-sys-cuda");

    // Statically link libstdc++ to avoid runtime dependency on system libstdc++
    build_utils::link_libstdcpp_static();

    // Configure CUDA-specific linking - use static cudart
    println!("cargo::rustc-link-lib=static=cudart_static");
    // cudart_static requires linking against librt and libpthread
    println!("cargo::rustc-link-lib=rt");
    println!("cargo::rustc-link-lib=pthread");
    println!("cargo::rustc-link-lib=dl");
    println!(
        "cargo::rustc-link-search=native={}/lib64",
        cuda_home.display()
    );

    // Add Python library directory to rpath for runtime linking
    if let Some(python_lib_dir) = &python_lib_dir {
        println!("cargo::rustc-link-arg=-Wl,-rpath,{}", python_lib_dir);
    }

    println!("cargo::rerun-if-changed=src/bridge.rs");
    println!("cargo::rerun-if-changed=src/bridge.cpp");
    println!("cargo::rerun-if-changed=src/bridge.h");
}
