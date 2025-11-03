/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use clap::Parser;
use clap::Subcommand;
use enum_as_inner::EnumAsInner;
use hyperactor::Named;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelRx;
use hyperactor::channel::ChannelTransport;
use hyperactor::channel::Rx;
use hyperactor::channel::Tx;
use serde::Deserialize;
use serde::Serialize;
use tokio::time::Duration;
use tokio::time::Instant;

#[derive(Clone, Debug, Named, Serialize, Deserialize, EnumAsInner)]
enum Message {
    Hello(ChannelAddr),
    Echo(serde_multipart::Part),
}

impl Message {
    fn len(&self) -> usize {
        match self {
            Message::Hello(_) => 0,
            Message::Echo(part) => part.len(),
        }
    }

    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        match self {
            Message::Hello(_) => true,
            Message::Echo(part) => part.is_empty(),
        }
    }
}

async fn server(mut server_rx: ChannelRx<Message>) -> Result<(), anyhow::Error> {
    let client_addr: ChannelAddr = server_rx
        .recv()
        .await?
        .into_hello()
        .map_err(|_| anyhow::anyhow!("expected hello message"))?;
    let client_tx = channel::dial(client_addr)?;
    loop {
        let message = server_rx.recv().await?;
        client_tx.post(message);
    }
}

async fn client(
    server_addr: ChannelAddr,
    message_size: usize,
    num_iter: Option<usize>,
) -> anyhow::Result<()> {
    let server_tx = channel::dial(server_addr)?;

    let (client_addr, mut client_rx) = channel::serve::<Message>(
        ChannelAddr::any(server_tx.addr().transport().clone()),
        "example",
    )
    .unwrap();

    server_tx.post(Message::Hello(client_addr));

    let message = Message::Echo(serde_multipart::Part::from(vec![0u8; message_size]));

    for _ in 0..10 {
        // Warmup
        #[allow(clippy::disallowed_methods)]
        let _t = Instant::now();
        server_tx.post(message.clone() /*cheap */);
        client_rx.recv().await?;
    }

    let mut latencies = vec![];
    let mut total_bytes_sent = 0usize;
    let mut total_bytes_received = 0usize;

    #[allow(clippy::disallowed_methods)]
    let start = Instant::now();
    for i in 0usize.. {
        if num_iter.is_some_and(|n| i >= n) {
            break;
        }

        total_bytes_sent += message.len();
        #[allow(clippy::disallowed_methods)]
        let start = Instant::now();
        server_tx.post(message.clone() /*cheap */);
        total_bytes_received += client_rx.recv().await?.len();
        latencies.push(start.elapsed());

        if i % 1000 == 0 {
            println!("sent: {} messages, {} MiB", i, total_bytes_sent >> 20);
        }
    }
    let elapsed = start.elapsed();

    let avg_latency = ((latencies.iter().sum::<Duration>().as_micros() as f64) / 1000f64)
        / (latencies.len() as f64);
    let min_latency = (latencies.iter().min().unwrap().as_micros() as f64) / 1000f64;
    let max_latency = (latencies.iter().max().unwrap().as_micros() as f64) / 1000f64;

    let total_bytes_transferred = total_bytes_sent + total_bytes_received;
    let bandwidth_bytes_per_sec =
        (total_bytes_transferred as f64) / ((elapsed.as_millis() as f64) / 1000f64);
    let bandwidth_mbps = (bandwidth_bytes_per_sec * 8f64) / (1024f64 * 1024f64);

    println!("Results:");
    println!("Average latency: {} ms", avg_latency);
    println!("Min latency: {} ms", min_latency);
    println!("Max latency: {} ms", max_latency);
    println!("Total iterations: {}", latencies.len());
    println!("Total time: {} seconds", elapsed.as_secs());
    println!("Bytes sent: {} bytes", total_bytes_sent);
    println!("Bytes received: {} bytes", total_bytes_received);
    println!("Total bytes transferred: {} bytes", total_bytes_transferred);
    println!(
        "Bandwidth: {} bytes/sec ({} Mbps)",
        bandwidth_bytes_per_sec, bandwidth_mbps
    );

    Ok(())
}

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// The transport to use
    #[arg(long, default_value = "tcp")]
    transport: ChannelTransport,

    /// Message size in bytes
    #[arg(long, default_value_t = 1_000_000)]
    message_size: usize,

    /// Number of iterations
    #[arg(long)]
    num_iter: Option<usize>,
}

#[derive(Subcommand)]
enum Commands {
    Client { server_addr: ChannelAddr },
    Server,
}

// Analog of https://www.internalfb.com/phabricator/paste/view/P1903314366, using Channel APIs.
// Possibly we should create separate threads for the client and server to also make the OS-level
// setup equivalent.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), anyhow::Error> {
    let args = Cli::parse();

    match args.command {
        Some(Commands::Server) => {
            let (server_addr, server_rx) =
                channel::serve::<Message>(ChannelAddr::any(args.transport.clone()), "example")
                    .unwrap();
            eprintln!("server listening on {}", server_addr);
            server(server_rx).await?;
        }

        Some(Commands::Client { server_addr }) => {
            client(server_addr, args.message_size, args.num_iter).await?;
        }

        // No command: run a self-contained benchmark.
        None => {
            let (server_addr, server_rx) =
                channel::serve::<Message>(ChannelAddr::any(args.transport.clone()), "example")
                    .unwrap();
            let _server_handle = tokio::spawn(server(server_rx));
            let client_handle = tokio::spawn(client(server_addr, args.message_size, args.num_iter));

            client_handle.await??;
        }
    }

    Ok(())
}
