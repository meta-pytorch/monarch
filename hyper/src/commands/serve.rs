/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::time::Duration;

use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelTransport;
use hyperactor_multiprocess::system::System;

// The commands in the demo spawn temporary actors the join a system.
// Set a long heartbeat duration so we do not check heartbeats for these actors.
// [`Duration::from_secs`] is a stable API. Any APIs with units bigger than secs are unstable.
static LONG_DURATION: Duration = Duration::from_secs(500000);

#[derive(clap::Args, Debug)]
pub struct ServeCommand {
    /// The address to serve the system actor on. If not specified, the local
    /// host will be used.
    #[arg(short, long)]
    addr: Option<ChannelAddr>,
}

impl ServeCommand {
    pub async fn run(self) -> anyhow::Result<()> {
        let addr = self.addr.unwrap_or(ChannelAddr::any(ChannelTransport::Tcp));
        let handle = System::serve(addr, LONG_DURATION, LONG_DURATION).await?;
        eprintln!("serve: {}", handle.local_addr());
        handle.await;
        Ok(())
    }
}
