/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![deny(clippy::disallowed_methods)]

// torch-sys-cuda is a link-only dependency on Linux to consolidate NCCL
// linking for downstream consumers; reference it so the Rust crate does
// not trip the `unused_crate_dependencies` lint.
#[cfg(feature = "cuda")]
use torch_sys_cuda as _;

pub mod client;
pub mod controller;
pub mod debugger;
pub mod wire_value;
pub mod worker;
