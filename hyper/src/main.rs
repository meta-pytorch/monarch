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
// tokio is used by #[tokio::main] in OSS builds
// (cfg(not(fbcode_build))). In fbcode builds fbinit-tokio provides
// the runtime, so the compiler doesn't see a direct reference. This
// import suppresses the unused_extern warning while keeping tokio
// explicit in the generated Cargo.toml.
use tokio as _;

use crate::commands::list::ListCommand;
use crate::commands::resolve::ResolveCommand;
use crate::commands::show::ShowCommand;

#[derive(Parser)]
#[command()]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[clap(about = "Show a host, proc, or actor via the mesh admin server")]
    Show(ShowCommand),

    #[clap(about = "List the children of a mesh admin node")]
    List(ListCommand),

    #[clap(about = "Resolve a MAST job handle to a mesh admin URL")]
    Resolve(ResolveCommand),
}

#[cfg(fbcode_build)]
#[fbinit::main]
async fn main(_fb: fbinit::FacebookInit) -> Result<(), anyhow::Error> {
    run().await
}

#[cfg(not(fbcode_build))]
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    run().await
}

async fn run() -> Result<(), anyhow::Error> {
    match Cli::parse().command {
        Command::Show(command) => command.run().await,
        Command::List(command) => command.run().await,
        Command::Resolve(command) => command.run().await,
    }
}
