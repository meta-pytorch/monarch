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
use crate::commands::resolve::ResolveCommand;
use crate::commands::show::ShowCommand;

#[derive(Parser)]
#[command()]
struct Cli {
    /// MAST resolution strategy: "thrift" (default at Meta) or "cli".
    #[arg(long)]
    mast_resolver: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[clap(about = r#"Show the state of a resource"#)]
    Show(ShowCommand),

    #[clap(about = r#"List available resources"#)]
    List(ListCommand),

    #[clap(about = "Resolve a MAST job handle to a mesh admin URL")]
    Resolve(ResolveCommand),
}

// -- MAST resolution dispatch (INV-DISPATCH) --
//
// `MastResolver` is defined locally in each binary (here and in the
// admin TUI) rather than in `hyperactor_mesh`, to avoid pulling
// `fbinit` into the shared library's dependency graph. The CLI
// implementation lives in `hyperactor_mesh::mesh_admin`; the thrift
// implementation lives in `hyperactor_meta::mesh_admin`. Each binary
// owns the dispatch.
//
// TODO: a dedicated `hyperactor_mast` bridge crate could unify the
// enum and dispatch if more binaries need this pattern.

/// Resolution strategy for `mast_conda:///` handles.
pub(crate) enum MastResolver {
    /// Shell out to the `mast` CLI. Works in both Meta and OSS.
    Cli,
    /// Use the MAST Thrift API. Only available in Meta builds where
    /// `fbinit` and `hyperactor_meta_lib` are present.
    #[cfg(fbcode_build)]
    Thrift(fbinit::FacebookInit),
}

impl MastResolver {
    /// Construct from an optional `FacebookInit` and `--mast-resolver`
    /// CLI arg. In fbcode builds, defaults to `Thrift` when `fb` is
    /// available and `choice` is not `"cli"`. Otherwise `Cli`.
    fn new(
        #[allow(unused_variables)] fb: Option<fbinit::FacebookInit>,
        choice: Option<&str>,
    ) -> Self {
        #[cfg(fbcode_build)]
        if choice != Some("cli") {
            if let Some(fb) = fb {
                return MastResolver::Thrift(fb);
            }
        }
        MastResolver::Cli
    }
}

#[cfg(fbcode_build)]
#[fbinit::main]
async fn main(fb: fbinit::FacebookInit) -> Result<(), anyhow::Error> {
    run(Some(fb)).await
}

#[cfg(not(fbcode_build))]
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    run(None).await
}

async fn run(fb: Option<fbinit::FacebookInit>) -> Result<(), anyhow::Error> {
    let args = Cli::parse();
    hyperactor::initialize_with_current_runtime();

    let resolver = MastResolver::new(fb, args.mast_resolver.as_deref());

    let result = match args.command {
        Command::Show(command) => command.run().await,
        Command::List(command) => command.run().await,
        Command::Resolve(command) => command.run(&resolver).await,
    };

    // Allow the channel layer to flush pending acks before exit.
    // Without this, the remote host's MailboxClient observes a
    // broken link (30 s ack timeout) and the resulting undeliverable
    // message crashes the HostMeshAgent, tearing down the entire
    // mesh.  The ack interval is 500 ms, so 1 s is sufficient.
    RealClock.sleep(std::time::Duration::from_secs(1)).await;

    result
}
