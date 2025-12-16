/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// When building tests, we need to link against libpython because test binaries
// are standalone executables. Regular library builds for Python extensions should
// NOT link libpython (they get symbols from the interpreter that loads them).
#[cfg(test)]
#[link(name = "python3.11")]
extern "C" {}

/// A companion to the `torch-sys` crate that provides bindings for
/// CUDA-specific functionality from libtorch. This crate is separated out to
/// make it easier for clients who want to avoid compiling CUDA code.
///
/// The same safety logic described in the `torch-sys` crate applies here.
pub mod cuda;
pub mod nccl;
