/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/// A companion to the `torch-sys` crate that provides bindings for
/// CUDA-specific functionality from libtorch. This crate is separated out to
/// make it easier for clients who want to avoid compiling CUDA code.
///
/// The same safety logic described in the `torch-sys` crate applies here.
///
/// When the `cuda` feature is enabled, the `cuda` and `nccl` modules provide
/// full CUDA/NCCL bindings. When disabled, `cuda` provides Python-based CUDA
/// stream/event wrappers (which panic if no GPU is available at runtime), and
/// `nccl` provides stub types for compilation without NCCL headers.
pub mod cuda;
#[cfg(feature = "cuda")]
pub mod nccl;
pub mod nccl_common;
#[cfg(not(feature = "cuda"))]
pub mod nccl_stubs;
#[cfg(not(feature = "cuda"))]
pub use nccl_stubs as nccl;
