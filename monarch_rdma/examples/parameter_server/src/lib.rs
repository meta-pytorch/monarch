/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Parameter Server Example Library
//!
//! This library provides the main functionality for the parameter server example.

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    parameter_server::run(4, 5).await
}
