/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Thin binary for the Monarch mesh admin TUI.
//!
//! Delegates argument parsing and execution to the reusable TUI library.

use std::io;

// tokio is the async runtime on the OSS path (#[tokio::main]);
// fbcode uses fbinit::main. Explicit use suppresses the unused-deps
// linter while keeping tokio in BUCK deps for autocargo.
use tokio as _;

#[cfg(fbcode_build)]
#[fbinit::main]
async fn main(_fb: fbinit::FacebookInit) -> io::Result<()> {
    hyperactor_mesh_admin_tui_lib::run_cli().await
}

#[cfg(not(fbcode_build))]
#[tokio::main]
async fn main() -> io::Result<()> {
    hyperactor_mesh_admin_tui_lib::run_cli().await
}
