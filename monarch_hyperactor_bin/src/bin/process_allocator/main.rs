/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

mod common;

use std::str::FromStr;

use clap::Parser;
use common::Args;
use common::main_impl;
use hyperactor::channel::ChannelAddr;
use tokio::process::Command;
use tokio::time::Duration;

#[tokio::main]
async fn main() {
    let args = Args::parse();
    hyperactor::initialize_with_current_runtime();

    let bind = args
        .addr
        .unwrap_or_else(|| format!("tcp![::]:{}", args.port));

    let serve_address = ChannelAddr::from_str(&bind).unwrap();
    let program = Command::new(args.program);
    let timeout = args.timeout_sec.map(Duration::from_secs);

    let _ = main_impl(serve_address, program, timeout).await.unwrap();
}
