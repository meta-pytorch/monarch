/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![feature(exit_status_error)]

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use futures::try_join;
use monarch_conda::sync::receiver;
use monarch_conda::sync::sender;

#[derive(Parser)]
#[command(name = "conda-sync")]
#[command(about = "A tool to diff conda environments")]
struct Args {
    /// Path to the source conda environment
    src: PathBuf,
    /// Path to the dest conda environment
    dst: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Receiver -> Sender
    let (recv, send) = tokio::io::duplex(5 * 1024 * 1024);
    let (from_receiver, to_receiver) = tokio::io::split(recv);
    let (from_sender, to_sender) = tokio::io::split(send);
    try_join!(
        receiver(&args.dst, from_sender, to_sender, HashMap::new()),
        sender(&args.src, from_receiver, to_receiver),
    )?;

    Ok(())
}
