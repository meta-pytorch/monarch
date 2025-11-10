/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use hyperactor::channel::ChannelTransport;
/// Test binary for ProcessAllocator child process cleanup behavior.
/// This binary creates a ProcessAllocator and spawns several child processes,
/// then keeps running until killed. It's designed to test whether child
/// processes are properly cleaned up when the parent process is killed.
use hyperactor_mesh::alloc::Alloc;
use hyperactor_mesh::alloc::AllocConstraints;
use hyperactor_mesh::alloc::AllocSpec;
use hyperactor_mesh::alloc::Allocator;
use hyperactor_mesh::alloc::ProcState;
use hyperactor_mesh::alloc::ProcessAllocator;
use ndslice::extent;
use tokio::process::Command;

fn emit_proc_state(state: &ProcState) {
    if let Ok(json) = serde_json::to_string(state) {
        println!("{}", json);
        // Flush immediately to ensure parent can read events in real-time
        use std::io::Write;
        use std::io::{self};
        io::stdout().flush().unwrap();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing to stderr to avoid interfering with JSON output
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    let bootstrap_path = buck_resources::get("monarch/hyperactor_mesh/bootstrap").unwrap();
    eprintln!("Bootstrap cmd: {:?}", bootstrap_path);
    let cmd = Command::new(&bootstrap_path);
    let mut allocator = ProcessAllocator::new(cmd);

    // Create an allocation with 4 child processes
    let mut alloc = allocator
        .allocate(AllocSpec {
            extent: extent! { replica = 4 },
            constraints: AllocConstraints::default(),
            proc_name: None,
            transport: ChannelTransport::Unix,
            proc_allocation_mode: Default::default(),
        })
        .await?;

    while let Some(state) = alloc.next().await {
        emit_proc_state(&state);
    }
    Ok(())
}
