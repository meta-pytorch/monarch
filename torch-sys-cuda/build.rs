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
fn main() {
    build_utils::set_python_rpath();
}

#[cfg(not(target_os = "macos"))]
fn main() {
    // CPU-only tests still link libpython through pyo3.
    build_utils::set_python_rpath();

    // Skip CUDA-specific setup when building without the cuda feature.
    if std::env::var("CARGO_FEATURE_CUDA").is_err() {
        return;
    }

    // Statically link libstdc++ to avoid runtime dependency on system libstdc++
    build_utils::link_libstdcpp_static();
}
