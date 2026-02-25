/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// used in alloc/remoteprocess.rs test_send_signal_to_remote_process_allocator

use std::str::FromStr;

use clap::Parser;
use hyperactor::channel::ChannelAddr;
use hyperactor_mesh::alloc::remoteprocess::RemoteProcessAllocator;
use tokio::process::Command;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    about = "Serves a `RemoteProcessAllocator` for alloc/remoteprocess.rs test_remote_process_alloc_signal_handler"
)]
pub struct Args {
    #[arg(
        long,
        help = "The address to bind to in the form: \
                `{transport}!{address}:{port}` (e.g. `tcp!127.0.0.1:26600`). \
                If specified, `--port` argument is ignored"
    )]
    pub addr: Option<String>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    hyperactor::initialize_with_current_runtime();

    let bind = args.addr.unwrap();

    let serve_address = ChannelAddr::from_str(&bind).unwrap();

    tracing::info!("bind address is: {}", serve_address);

    let _ = tokio::spawn(async {
        RemoteProcessAllocator::new()
            .start(
                Command::new(buck_resources::get("monarch/hyperactor_mesh/bootstrap").unwrap()),
                serve_address,
                None,
            )
            .await
    })
    .await
    .unwrap();
}
