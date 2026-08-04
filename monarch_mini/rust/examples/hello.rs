/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! minimonarch hello world in Rust, mirroring `examples/hello.c`.
//!
//! Creates a parent and child actor over an `inproc://` connection and passes a
//! message each way. Run with `cargo run -p monarch_mini_rs --example hello`.

use monarch_mini_rs::Context;
use monarch_mini_rs::Part;
use monarch_mini_rs::Role;

// A single-threaded runtime suffices — minimonarch runs its own runtime inside
// its native library, so nothing here requires a multi-threaded executor.
#[tokio::main(flavor = "current_thread")]
async fn main() -> monarch_mini_rs::Result<()> {
    let ctx = Context::new()?;

    let parent = ctx.actor(Some(b"hello-actor"), /*gateway=*/ true)?;
    let child = ctx.actor(Some(b"child-actor"), /*gateway=*/ false)?;

    let mut parent_poller = ctx.poller()?;
    parent_poller.subscribe(0, &parent)?;
    let mut child_poller = ctx.poller()?;
    child_poller.subscribe(0, &child)?;

    // Send a message to self before anyone is listening; the actor buffers it.
    parent.send(b"hello-actor", vec![Part::copy_from(b"hello, self")])?;
    let (_, parts) = parent_poller.recv().await?;
    println!("received: {}", String::from_utf8_lossy(parts[0].as_bytes()));

    // Establish the parent/child link over inproc.
    let url = "inproc://hello-child";
    parent.serve(url, Role::Parent, None, &[], &[])?;
    child.join(url, Role::Child, None, &[], &[])?;

    let (_, parts) = parent_poller.recv().await?;
    println!(
        "parent joined: {} -> {}",
        String::from_utf8_lossy(parts[0].as_bytes()),
        String::from_utf8_lossy(parts[1].as_bytes()),
    );
    let (_, parts) = child_poller.recv().await?;
    println!(
        "child joined: {} -> {}",
        String::from_utf8_lossy(parts[0].as_bytes()),
        String::from_utf8_lossy(parts[1].as_bytes()),
    );

    // Parent -> child, then child -> parent.
    parent.send(b"child-actor", vec![Part::copy_from(b"hello, child")])?;
    let (_, parts) = child_poller.recv().await?;
    println!(
        "child received: {}",
        String::from_utf8_lossy(parts[0].as_bytes())
    );

    child.send(b"hello-actor", vec![Part::copy_from(b"hello, parent")])?;
    let (_, parts) = parent_poller.recv().await?;
    println!(
        "parent received: {}",
        String::from_utf8_lossy(parts[0].as_bytes())
    );

    Ok(())
}
