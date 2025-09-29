/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! CUDA RDMA Ping-Pong Example Library
//!
//! This library provides the main functionality for the CUDA RDMA ping-pong example.

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    cuda_ping_pong::run().await
}
