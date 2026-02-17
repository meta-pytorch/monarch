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

use crate::commands::admin::AdminCommand;
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

    #[clap(about = r#"Admin commands for the hyperactor admin HTTP API"#)]
    Admin(AdminCommand),
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

    match args.command {
        Command::Show(command) => Ok(command.run().await?),
        Command::List(command) => Ok(command.run().await?),
        Command::Admin(command) => Ok(command.run().await?),
    }
}
