/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use anyhow::Result;
use clap::Parser;
use hyperactor::channel::ChannelAddr;
use hyperactor_state::spawn_actor;
use hyperactor_state::state_actor::StateActor;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

/// A simple state actor binary
/// ```
///   buck run //monarch/hyperactor_state:state_actor_example -- -a 'tcp![::]:3000'
/// ```
#[derive(Parser, Debug)]
#[command()]
struct Args {
    /// The system address
    #[arg(short, long)]
    address: ChannelAddr,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the tracing subscriber
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    let args = Args::parse();

    println!("\x1b[33m======= STATE ACTOR STARTING ========\x1b[0m");

    // Create a state actor
    let proc_id = hyperactor::reference::ProcId(hyperactor::WorldId("state_server".to_string()), 0);
    let addr = args.address.clone();

    // Spawn the state actor
    let (local_addr, _state_actor_ref) =
        spawn_actor::<StateActor>(addr, proc_id, "state_actor", ()).await?;

    println!("State actor spawned at address: {:?}", local_addr);

    // Keep the application running until terminated
    println!("State actor system running. Press Ctrl+C to exit.");
    tokio::signal::ctrl_c().await?;
    println!("Shutting down state actor system");

    Ok(())
}
