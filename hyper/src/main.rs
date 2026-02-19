/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

mod commands;

use clap::Parser;
use clap::Subcommand;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;

use crate::commands::list::ListCommand;
use crate::commands::show::ShowCommand;

#[derive(Parser)]
#[command()]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[clap(about = r#"Show the state of a resource"#)]
    Show(ShowCommand),

    #[clap(about = r#"List available resources"#)]
    List(ListCommand),
}

#[cfg(fbcode_build)]
#[fbinit::main]
async fn main(_: fbinit::FacebookInit) -> Result<(), anyhow::Error> {
    run().await
}

#[cfg(not(fbcode_build))]
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    run().await
}

async fn run() -> Result<(), anyhow::Error> {
    let args = Cli::parse();
    hyperactor::initialize_with_current_runtime();

    let result = match args.command {
        Command::Show(command) => command.run().await,
        Command::List(command) => command.run().await,
    };

    // Allow the channel layer to flush pending acks before exit.
    // Without this, the remote host's MailboxClient observes a
    // broken link (30 s ack timeout) and the resulting undeliverable
    // message crashes the HostMeshAgent, tearing down the entire
    // mesh.  The ack interval is 500 ms, so 1 s is sufficient.
    RealClock.sleep(std::time::Duration::from_secs(1)).await;

    result
}
