/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::str::FromStr;

use clap::Parser;
use clap::Subcommand;
use hyperactor::Endpoint;
use hyperactor::PortAddr;
use hyperactor::PortRef;
use hyperactor::Proc;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelTransport;
use hyperactor::mailbox::PortReceiver;
use hyperactor_telemetry::DefaultTelemetryClock;
use hyperactor_telemetry::initialize_logging;
use serde::Deserialize;
use serde::Serialize;
use tokio::time::Duration;
use tokio::time::Instant;
use typeuri::Named;

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Named, Serialize, Deserialize)]
enum Message {
    Hello(PortRef<Message>),
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

async fn server(
    mut server_rx: PortReceiver<Message>,
    server_proc: Proc,
) -> Result<(), anyhow::Error> {
    let server_client = server_proc.client("server");
    let mut client_port: Option<PortRef<Message>> = None;

    loop {
        match server_rx.recv().await? {
            Message::Hello(port) => {
                client_port = Some(port);
            }
            Message::Echo(part) => {
                let port = client_port
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("expected hello message"))?;
                port.post(&server_client, Message::Echo(part));
            }
        }
    }
}

async fn client(
    server_port: PortRef<Message>,
    message_size: usize,
    num_iter: Option<usize>,
) -> anyhow::Result<()> {
    let transport = server_port.port_addr().location().addr().transport();
    let client_proc = Proc::direct(ChannelAddr::any(transport), "pingpong_client".to_owned())?;
    let client = client_proc.client("client");
    let (client_port, mut client_rx) = client.open_port::<Message>();
    server_port.post(&client, Message::Hello(client_port.bind()));

    let message = Message::Echo(serde_multipart::Part::from(vec![0u8; message_size]));

    for _ in 0..10 {
        server_port.post(&client, message.clone());
        client_rx.recv().await?;
    }

    let mut latencies = vec![];
    let mut total_bytes_sent = 0usize;
    let mut total_bytes_received = 0usize;

    let start = Instant::now();
    for i in 0usize.. {
        if num_iter.is_some_and(|n| i >= n) {
            break;
        }

        total_bytes_sent += message.len();
        let start = Instant::now();
        server_port.post(&client, message.clone());
        total_bytes_received += client_rx.recv().await?.len();
        latencies.push(start.elapsed());

        if i % 1000 == 0 {
            println!("sent: {} messages, {} MiB", i, total_bytes_sent >> 20);
        }
    }
    let elapsed = start.elapsed();

    client_proc.flush().await?;
    client_proc.join_mailbox_server().await;

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

#[derive(Clone, Copy, Debug)]
enum RuntimeMode {
    MultiThread,
    SingleThread,
    CurrentThread,
}

impl RuntimeMode {
    fn build(self) -> anyhow::Result<tokio::runtime::Runtime> {
        let mut builder = match self {
            Self::MultiThread => tokio::runtime::Builder::new_multi_thread(),
            Self::SingleThread => {
                let mut builder = tokio::runtime::Builder::new_multi_thread();
                builder.worker_threads(1);
                builder
            }
            Self::CurrentThread => tokio::runtime::Builder::new_current_thread(),
        };

        Ok(builder.enable_all().build()?)
    }
}

impl FromStr for RuntimeMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "multi_thread" | "multi-thread" => Ok(Self::MultiThread),
            "single_thread" | "single-thread" => Ok(Self::SingleThread),
            "current_thread" | "current-thread" => Ok(Self::CurrentThread),
            unknown => Err(anyhow::anyhow!(
                "unknown runtime mode: {}; expected one of multi_thread, single_thread, current_thread",
                unknown
            )),
        }
    }
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

    /// Tokio runtime mode: multi_thread, single_thread, or current_thread
    #[arg(long, default_value = "current_thread")]
    runtime: RuntimeMode,

    /// Enable telemetry/logging initialization
    #[arg(long)]
    enable_telemetry: bool,
}

#[derive(Subcommand)]
enum Commands {
    Client { server_port: PortAddr },
    Server,
}

fn main() -> Result<(), anyhow::Error> {
    let args = Cli::parse();
    if args.enable_telemetry {
        initialize_logging(DefaultTelemetryClock {});
    }

    let runtime = args.runtime.build()?;
    runtime.block_on(run(args))
}

async fn run(args: Cli) -> Result<(), anyhow::Error> {
    match args.command {
        Some(Commands::Server) => {
            let server_proc = Proc::direct(
                ChannelAddr::any(args.transport.clone()),
                "pingpong_server".to_owned(),
            )?;
            let server_client = server_proc.client("server");
            let (server_port, server_rx) = server_client.open_port::<Message>();
            eprintln!("server listening on {}", server_port.bind().port_addr());
            server(server_rx, server_proc).await?;
        }

        Some(Commands::Client { server_port }) => {
            client(
                PortRef::attest(server_port),
                args.message_size,
                args.num_iter,
            )
            .await?;
        }

        None => {
            let server_proc = Proc::direct(
                ChannelAddr::any(args.transport.clone()),
                "pingpong_server".to_owned(),
            )?;
            let server_client = server_proc.client("server");
            let (server_port, server_rx) = server_client.open_port::<Message>();
            let server_ref = server_port.bind();
            let server_handle = tokio::spawn(server(server_rx, server_proc.clone()));

            client(server_ref, args.message_size, args.num_iter).await?;

            server_handle.abort();
            server_proc.flush().await?;
            server_proc.join_mailbox_server().await;
        }
    }

    Ok(())
}
