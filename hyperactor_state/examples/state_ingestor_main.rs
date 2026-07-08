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
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::id;
use hyperactor_state::create_remote_client;
use hyperactor_state::state_actor::StateActor;
use hyperactor_state::state_actor::StateMessage;
use hyperactor_state::test_utils::log_items;

/// A state ingestor binary that sends logs to a state actor at a customizable rate
/// ```
///   buck run //monarch/hyperactor_state:state_ingestor_example -- -a 'tcp![::]:3000' -r 2
/// ```
#[derive(Parser, Debug)]
#[command()]
struct Args {
    /// The state actor address to connect to
    #[arg(short, long)]
    address: ChannelAddr,

    /// Rate of log ingestion in logs per second
    #[arg(short, long, default_value = "1")]
    rate: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Print a yellow banner
    println!("\x1b[33m======= STATE INGESTOR STARTING ========\x1b[0m");
    println!("Connecting to state actor at: {:?}", args.address);
    println!("Ingestion rate: {} logs per second", args.rate);

    // Calculate the delay between logs based on the rate
    let delay = Duration::from_millis(1000 / args.rate);

    // Connect to the state actor
    let (_proc, remote_client) = create_remote_client(args.address.clone()).await?;

    // Get a reference to the state actor
    let state_actor_ref = ActorRef::<StateActor>::attest(id!(state[0].state_actor[0]));

    println!("\x1b[32mStarting log ingestion...\x1b[0m");

    // Send logs at the specified rate
    let mut seq = 0;
    loop {
        seq += 1;

        // Create a simple text log message
        let log_message = format!(
            "Log message #{:04} at {}",
            seq,
            chrono::Utc::now().to_rfc3339()
        );
        println!("Sending log #{:04}: {}", seq, log_message);

        // Send the log message to the state actor
        // Note: In a real implementation, you would create proper GenericStateObject instances
        // but for this example, we're just sending an empty message
        let logs = log_items(seq, seq + 1);
        state_actor_ref.send(
            &remote_client,
            StateMessage::SetLogs {
                logs, // Empty logs for now, as we can't create GenericStateObject directly
            },
        )?;

        // Wait for the specified delay
        RealClock.sleep(delay).await;
    }
}
