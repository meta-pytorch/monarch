/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Static C++ libraries for Monarch
//!
//! This crate builds NCCL and rdma-core (libibverbs, libmlx5) from source
//! as static libraries. Depend on this crate to link against them statically,
//! eliminating runtime dependencies on libnccl.so, libibverbs.so, and libmlx5.so.
//!
//! This crate does not provide Rust bindings - use `nccl-sys` and `rdmaxcel-sys`
//! for the bindings, and add this crate as a dependency to get static linking.
