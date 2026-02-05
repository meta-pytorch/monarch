/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use hyperactor::ActorRef;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelTransport;
use hyperactor::id;
use hyperactor_state::client::ClientActor;
use hyperactor_state::client::ClientActorParams;
use hyperactor_state::create_remote_client;
use hyperactor_state::object::GenericStateObject;
use hyperactor_state::spawn_actor;
use hyperactor_state::state_actor::StateActor;
use hyperactor_state::state_actor::StateMessageClient;
use tokio::sync::mpsc;

/// A state client binary that subscribes to logs from a state actor
/// ```
///   buck run //monarch/hyperactor_state:state_client_example -- -a 'tcp![::]:3000'
/// ```
#[derive(Parser, Debug)]
#[command()]
struct Args {
    /// The state actor address to connect to
    #[arg(short, long)]
    address: ChannelAddr,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the tracing subscriber for better logging
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    let args = Args::parse();

    println!("\x1b[36m======= STATE CLIENT STARTING ========\x1b[0m");
    println!("Connecting to state actor at: {:?}", args.address);

    // Connect to the state actor
    let (_proc, remote_client) = create_remote_client(args.address.clone()).await?;

    // Get a reference to the state actor
    let state_actor_ref = ActorRef::<StateActor>::attest(id!(state[0].state_actor[0]));

    // Create a channel to receive logs
    let (sender, mut receiver) = mpsc::channel::<GenericStateObject>(100);

    // Create a client actor to receive logs
    let client_actor_addr = ChannelAddr::any(ChannelTransport::Unix);
    let params = ClientActorParams { sender };
    let (client_actor_addr, client_actor_ref) = spawn_actor::<ClientActor>(
        client_actor_addr.clone(),
        id![state_client[0].log_client],
        params,
    )
    .await?;

    // Subscribe to logs from the state actor
    println!("Subscribing to logs from state actor...");
    state_actor_ref
        .subscribe_logs(&remote_client, client_actor_addr, client_actor_ref)
        .await?;

    println!("\x1b[32mSubscribed successfully! Waiting for logs...\x1b[0m");

    // Process received logs
    let mut log_count = 0;
    loop {
        match tokio::time::timeout(Duration::from_secs(60), receiver.recv()).await {
            Ok(Some(log)) => {
                log_count += 1;
                println!("\x1b[34m--- Log #{} Received ---\x1b[0m", log_count);
                println!("Metadata: {:?}", log.metadata());

                // Try to parse the data as JSON for better display
                match serde_json::from_str::<serde_json::Value>(log.data()) {
                    Ok(json_data) => {
                        if let Some(status) = json_data.get("status") {
                            if let Some(message) = status.get("message") {
                                println!("Message: {}", message);
                            }
                            if let Some(seq) = status.get("seq") {
                                println!("Sequence: {}", seq);
                            }
                        } else {
                            println!("Data: {}", serde_json::to_string_pretty(&json_data)?);
                        }
                    }
                    Err(_) => {
                        // If not valid JSON, just print the raw data
                        println!("Raw data: {}", log.data());
                    }
                }
                println!();
            }
            Ok(None) => {
                println!("\x1b[31mChannel closed, exiting\x1b[0m");
                break;
            }
            Err(_) => {
                println!("\x1b[33mNo logs received in the last 60 seconds\x1b[0m");
            }
        }
    }

    Ok(())
}
