/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Ping-pong latency benchmark using hyperactor channels.
//!
//! Equivalent to the ZMQ-based Python ping-pong benchmark, but uses hyperactor
//! channels directly. Supports TCP, Unix, and Local transports, as well as
//! duplex mode (single connection, both directions).
//!
//! Usage:
//!   (no args)                        run locally (subprocesses for TCP, in-process otherwise)
//!   --duplex                         run using duplex channels (in-process)
//!   --server [--transport tcp]       run as echo server
//!   --client <ADDR>                  run as client connecting to ADDR (e.g. tcp:[::1]:5555)

use std::io::Write;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;
use std::time::Instant;

use clap::Parser;
use enum_as_inner::EnumAsInner;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelRx;
use hyperactor::channel::ChannelTransport;
use hyperactor::channel::Rx;
use hyperactor::channel::TcpMode;
use hyperactor::channel::Tx;
use hyperactor::channel::duplex;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

#[derive(Clone, Debug, Named, Serialize, Deserialize, EnumAsInner)]
enum Message {
    /// Initial handshake carrying the client's reply address.
    Hello(ChannelAddr),
    /// Payload echoed back and forth.
    Echo(serde_multipart::Part),
}

impl Message {
    fn payload_len(&self) -> usize {
        match self {
            Message::Hello(_) => 0,
            Message::Echo(part) => part.len(),
        }
    }
}

#[derive(Parser)]
#[command(about = "Hyperactor channel ping-pong benchmark")]
struct Cli {
    /// Run as server.
    #[arg(long)]
    server: bool,

    /// Run as client connecting to the given channel address (e.g. "tcp:[::1]:5555").
    #[arg(long)]
    client: Option<ChannelAddr>,

    /// Channel transport.
    #[arg(long, default_value = "tcp")]
    transport: ChannelTransport,

    /// Number of ping-pong iterations.
    #[arg(long, default_value_t = 1000)]
    iterations: usize,

    /// Port for TCP transport (server and local mode).
    #[arg(long, default_value_t = 5555)]
    port: u16,

    /// Payload size in bytes.
    #[arg(long, default_value_t = 100)]
    message_size: usize,

    /// Use duplex channels (single connection, both directions).
    #[arg(long)]
    duplex: bool,

    /// Run a full benchmark suite; write CSV to the given path.
    #[arg(long)]
    suite: Option<PathBuf>,

    /// Compare two suite CSV files (baseline, then current).
    #[arg(long, num_args = 2, value_names = ["BASELINE", "CURRENT"])]
    diff: Option<Vec<PathBuf>>,
}

async fn run_server(addr: ChannelAddr) -> anyhow::Result<()> {
    let (listen_addr, mut rx) = channel::serve::<Message>(addr)?;
    println!("Server listening on {listen_addr}");

    // First message is a Hello carrying the client's reply address.
    let client_addr = rx
        .recv()
        .await?
        .into_hello()
        .map_err(|_| anyhow::anyhow!("expected Hello"))?;
    let client_tx = channel::dial(client_addr)?;

    // Echo loop.
    loop {
        let msg = rx.recv().await?;
        client_tx.post(msg);
    }
}

async fn run_client(
    server_addr: ChannelAddr,
    num_iterations: usize,
    message_size: usize,
) -> anyhow::Result<()> {
    let server_tx = channel::dial::<Message>(server_addr.clone())?;

    // Open our own channel for receiving replies.
    let (client_addr, mut client_rx) =
        channel::serve::<Message>(ChannelAddr::any(server_tx.addr().transport().clone()))?;

    println!("Client connected to {server_addr}");

    // Tell the server where to send replies.
    server_tx.post(Message::Hello(client_addr));

    let message = Message::Echo(serde_multipart::Part::from(vec![0u8; message_size]));
    let message_bytes = message.payload_len();

    // Warmup.
    for _ in 0..10 {
        server_tx.post(message.clone());
        client_rx.recv().await?;
    }

    println!("Payload size: {message_size} bytes");
    println!("Starting {num_iterations} ping-pong iterations...");

    let mut latencies = Vec::with_capacity(num_iterations);
    let mut total_bytes_sent = 0usize;
    let mut total_bytes_received = 0usize;

    let total_start = Instant::now();

    for i in 0..num_iterations {
        let start = Instant::now();
        server_tx.post(message.clone()); // cheap: Part is Arc-backed
        total_bytes_sent += message_bytes;

        let response = client_rx.recv().await?;
        total_bytes_received += response.payload_len();

        latencies.push(start.elapsed());

        if (i + 1) % 100 == 0 {
            println!("Completed {}/{num_iterations} iterations", i + 1);
        }
    }

    let total_elapsed = total_start.elapsed();

    let avg_ms = latencies.iter().sum::<Duration>().as_secs_f64() * 1000.0 / latencies.len() as f64;
    let min_ms = latencies.iter().min().unwrap().as_secs_f64() * 1000.0;
    let max_ms = latencies.iter().max().unwrap().as_secs_f64() * 1000.0;

    let total_bytes = total_bytes_sent + total_bytes_received;
    let total_secs = total_elapsed.as_secs_f64();
    let bw_bps = total_bytes as f64 / total_secs;
    let bw_mbps = bw_bps * 8.0 / (1024.0 * 1024.0);

    println!();
    println!("Results:");
    println!("Average latency: {avg_ms:.3} ms");
    println!("Min latency: {min_ms:.3} ms");
    println!("Max latency: {max_ms:.3} ms");
    println!("Total iterations: {}", latencies.len());
    println!("Total time: {total_secs:.3} seconds");
    println!("Bytes sent: {total_bytes_sent} bytes");
    println!("Bytes received: {total_bytes_received} bytes");
    println!("Total bytes transferred: {total_bytes} bytes");
    println!("Bandwidth: {bw_bps:.0} bytes/sec ({bw_mbps:.2} Mbps)");

    Ok(())
}

/// TCP local mode: spawn server and client as separate OS processes.
fn run_local_subprocess(
    transport: &str,
    port: u16,
    iterations: usize,
    message_size: usize,
) -> anyhow::Result<()> {
    println!("Running local benchmark (subprocesses)...");
    println!("Using port {port}");

    let exe = std::env::current_exe()?;

    // Start server subprocess.
    let mut server = Command::new(&exe)
        .args([
            "--transport",
            transport,
            "--server",
            "--port",
            &port.to_string(),
        ])
        .spawn()?;

    // Give the server time to bind.
    std::thread::sleep(Duration::from_millis(200));

    // Start client subprocess.
    let addr = format!("tcp:[::1]:{port}");
    let mut client = Command::new(&exe)
        .args([
            "--transport",
            transport,
            "--client",
            &addr,
            "--iterations",
            &iterations.to_string(),
            "--message-size",
            &message_size.to_string(),
        ])
        .spawn()?;

    // Wait for client to finish.
    client.wait()?;

    // Terminate server.
    let _ = server.kill();
    let _ = server.wait();

    Ok(())
}

/// Non-TCP local mode: run server and client as tokio tasks in-process.
async fn run_local_inprocess(
    transport: ChannelTransport,
    iterations: usize,
    message_size: usize,
) -> anyhow::Result<()> {
    println!("Running local benchmark (in-process, {transport})...");

    let (server_addr, server_rx) = channel::serve::<Message>(ChannelAddr::any(transport))?;
    let _server = tokio::spawn(async move {
        let _ = run_server_loop(server_rx).await;
    });

    run_client(server_addr, iterations, message_size).await
}

/// Echo loop used by the in-process server task.
async fn run_server_loop(mut rx: ChannelRx<Message>) -> anyhow::Result<()> {
    let client_addr = rx
        .recv()
        .await?
        .into_hello()
        .map_err(|_| anyhow::anyhow!("expected Hello"))?;
    let client_tx = channel::dial(client_addr)?;
    loop {
        let msg = rx.recv().await?;
        client_tx.post(msg);
    }
}

/// Run a duplex benchmark in-process.
async fn run_local_duplex(
    transport: ChannelTransport,
    iterations: usize,
    message_size: usize,
) -> anyhow::Result<()> {
    println!("Running duplex benchmark (in-process, {transport})...");

    let mut server = duplex::serve::<Message, Message>(ChannelAddr::any(transport))?;
    let server_addr = server.addr().clone();

    // Server task: accept one link, echo back.
    let server_handle = tokio::spawn(async move {
        let (mut rx, tx) = server.accept().await.unwrap();
        while let Ok(msg) = rx.recv().await {
            tx.post(msg);
        }
    });

    let (client_tx, mut client_rx) = duplex::dial::<Message, Message>(server_addr.clone()).await?;

    println!("Client connected to {server_addr} (duplex)");

    let message = Message::Echo(serde_multipart::Part::from(vec![0u8; message_size]));
    let message_bytes = message.payload_len();

    // Warmup.
    for _ in 0..10 {
        client_tx.post(message.clone());
        client_rx.recv().await?;
    }

    println!("Payload size: {message_size} bytes");
    println!("Starting {iterations} ping-pong iterations...");

    let mut latencies = Vec::with_capacity(iterations);
    let mut total_bytes_sent = 0usize;
    let mut total_bytes_received = 0usize;

    let total_start = Instant::now();

    for i in 0..iterations {
        let start = Instant::now();
        client_tx.post(message.clone());
        total_bytes_sent += message_bytes;

        let response = client_rx.recv().await?;
        total_bytes_received += response.payload_len();

        latencies.push(start.elapsed());

        if (i + 1) % 100 == 0 {
            println!("Completed {}/{iterations} iterations", i + 1);
        }
    }

    let total_elapsed = total_start.elapsed();

    let avg_ms = latencies.iter().sum::<Duration>().as_secs_f64() * 1000.0 / latencies.len() as f64;
    let min_ms = latencies.iter().min().unwrap().as_secs_f64() * 1000.0;
    let max_ms = latencies.iter().max().unwrap().as_secs_f64() * 1000.0;

    let total_bytes = total_bytes_sent + total_bytes_received;
    let total_secs = total_elapsed.as_secs_f64();
    let bw_bps = total_bytes as f64 / total_secs;
    let bw_mbps = bw_bps * 8.0 / (1024.0 * 1024.0);

    println!();
    println!("Results:");
    println!("Average latency: {avg_ms:.3} ms");
    println!("Min latency: {min_ms:.3} ms");
    println!("Max latency: {max_ms:.3} ms");
    println!("Total iterations: {}", latencies.len());
    println!("Total time: {total_secs:.3} seconds");
    println!("Bytes sent: {total_bytes_sent} bytes");
    println!("Bytes received: {total_bytes_received} bytes");
    println!("Total bytes transferred: {total_bytes} bytes");
    println!("Bandwidth: {bw_bps:.0} bytes/sec ({bw_mbps:.2} Mbps)");

    server_handle.abort();
    Ok(())
}

/// Quiet duplex ping-pong benchmark for suite mode, returning total elapsed time.
async fn bench_ping_pong_duplex(
    transport: ChannelTransport,
    num_iterations: usize,
    message_size: usize,
) -> anyhow::Result<Duration> {
    let mut server = duplex::serve::<Message, Message>(ChannelAddr::any(transport))?;
    let server_addr = server.addr().clone();

    let server_handle = tokio::spawn(async move {
        let (mut rx, tx) = server.accept().await.unwrap();
        while let Ok(msg) = rx.recv().await {
            tx.post(msg);
        }
    });

    let (client_tx, mut client_rx) = duplex::dial::<Message, Message>(server_addr).await?;

    let message = Message::Echo(serde_multipart::Part::from(vec![0u8; message_size]));

    // Warmup.
    for _ in 0..10 {
        client_tx.post(message.clone());
        client_rx.recv().await?;
    }

    let start = Instant::now();
    for _ in 0..num_iterations {
        client_tx.post(message.clone());
        client_rx.recv().await?;
    }
    let elapsed = start.elapsed();

    server_handle.abort();
    Ok(elapsed)
}

const SUITE_SIZES: &[usize] = &[100, 1_000, 10_000, 100_000, 1_000_000];

/// Run a single in-process ping-pong benchmark, returning total elapsed time.
async fn bench_ping_pong(
    transport: ChannelTransport,
    num_iterations: usize,
    message_size: usize,
) -> anyhow::Result<Duration> {
    let (server_addr, server_rx) = channel::serve::<Message>(ChannelAddr::any(transport))?;
    let server_handle = tokio::spawn(async move {
        let _ = run_server_loop(server_rx).await;
    });

    let server_tx = channel::dial::<Message>(server_addr)?;
    let (client_addr, mut client_rx) =
        channel::serve::<Message>(ChannelAddr::any(server_tx.addr().transport().clone()))?;
    server_tx.post(Message::Hello(client_addr));

    let message = Message::Echo(serde_multipart::Part::from(vec![0u8; message_size]));

    // Warmup.
    for _ in 0..10 {
        server_tx.post(message.clone());
        client_rx.recv().await?;
    }

    let start = Instant::now();
    for _ in 0..num_iterations {
        server_tx.post(message.clone());
        client_rx.recv().await?;
    }
    let elapsed = start.elapsed();

    server_handle.abort();
    Ok(elapsed)
}

/// Benchmark entry: name, transport, and whether to use duplex.
struct BenchEntry {
    name: &'static str,
    transport: ChannelTransport,
    use_duplex: bool,
}

/// Run a benchmark suite across transports and message sizes, writing CSV output.
async fn run_suite(output: &std::path::Path, iterations: usize) -> anyhow::Result<()> {
    let entries = vec![
        BenchEntry {
            name: "local",
            transport: ChannelTransport::Local,
            use_duplex: false,
        },
        BenchEntry {
            name: "unix",
            transport: ChannelTransport::Unix,
            use_duplex: false,
        },
        BenchEntry {
            name: "tcp",
            transport: ChannelTransport::Tcp(TcpMode::Hostname),
            use_duplex: false,
        },
        BenchEntry {
            name: "duplex-unix",
            transport: ChannelTransport::Unix,
            use_duplex: true,
        },
        BenchEntry {
            name: "duplex-tcp",
            transport: ChannelTransport::Tcp(TcpMode::Hostname),
            use_duplex: true,
        },
    ];

    let mut file = std::fs::File::create(output)?;

    // CSV header.
    let headers: Vec<String> = SUITE_SIZES.iter().map(|s| s.to_string()).collect();
    writeln!(file, "transport,{}", headers.join(","))?;

    // Table header to stdout.
    print!("{:<14}", "transport");
    for size in SUITE_SIZES {
        print!("{:>14}", size);
    }
    println!();

    for entry in &entries {
        print!("{:<14}", entry.name);
        let mut times_ms = Vec::new();
        for &size in SUITE_SIZES {
            let dur = if entry.use_duplex {
                bench_ping_pong_duplex(entry.transport.clone(), iterations, size).await?
            } else {
                bench_ping_pong(entry.transport.clone(), iterations, size).await?
            };
            let ms = dur.as_secs_f64() * 1000.0;
            times_ms.push(ms);
            print!("{:>12.3}ms", ms);
        }
        println!();

        let values: Vec<String> = times_ms.iter().map(|t| format!("{t:.3}")).collect();
        writeln!(file, "{},{}", entry.name, values.join(","))?;
    }

    eprintln!("\nResults written to {}", output.display());
    Ok(())
}

/// Compare two suite CSV files and print a delta table.
fn run_diff(baseline: &std::path::Path, current: &std::path::Path) -> anyhow::Result<()> {
    let parse_csv =
        |path: &std::path::Path| -> anyhow::Result<(Vec<String>, Vec<(String, Vec<f64>)>)> {
            let content = std::fs::read_to_string(path)?;
            let mut lines = content.lines();
            let header = lines.next().ok_or_else(|| anyhow::anyhow!("empty CSV"))?;
            let sizes: Vec<String> = header
                .split(',')
                .skip(1)
                .map(|s| s.trim().to_string())
                .collect();
            let mut rows = Vec::new();
            for line in lines {
                if line.is_empty() {
                    continue;
                }
                let parts: Vec<&str> = line.split(',').collect();
                let name = parts[0].trim().to_string();
                let values: Vec<f64> = parts[1..]
                    .iter()
                    .map(|s| s.trim().parse().unwrap_or(f64::NAN))
                    .collect();
                rows.push((name, values));
            }
            Ok((sizes, rows))
        };

    let (sizes_a, rows_a) = parse_csv(baseline)?;
    let (sizes_b, rows_b) = parse_csv(current)?;

    if sizes_a != sizes_b {
        anyhow::bail!("CSV files have different message size columns");
    }

    println!("baseline: {}", baseline.display());
    println!("current:  {}", current.display());
    println!();

    // Header.
    print!("{:<16}", "");
    for size in &sizes_a {
        print!("{:>14}", size);
    }
    println!();

    for (name, vals_base) in &rows_a {
        let Some((_, vals_curr)) = rows_b.iter().find(|(n, _)| n == name) else {
            continue;
        };

        print!("{:<16}", format!("{name} (base)"));
        for v in vals_base {
            print!("{:>12.3}ms", v);
        }
        println!();

        print!("{:<16}", format!("{name} (curr)"));
        for v in vals_curr {
            print!("{:>12.3}ms", v);
        }
        println!();

        print!("{:<16}", format!("{name} (diff)"));
        for (a, b) in vals_base.iter().zip(vals_curr.iter()) {
            if *a > 0.0 {
                let pct = (b - a) / a * 100.0;
                let sign = if pct >= 0.0 { "+" } else { "" };
                print!("{:>14}", format!("{sign}{pct:.1}%"));
            } else {
                print!("{:>14}", "N/A");
            }
        }
        println!();
        println!();
    }

    Ok(())
}

/// Build a server listen address from the transport and port.
fn server_listen_addr(transport: &ChannelTransport, port: u16) -> ChannelAddr {
    match transport {
        ChannelTransport::Tcp(_) => {
            ChannelAddr::Tcp(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port))
        }
        _ => ChannelAddr::any(transport.clone()),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    if let Some(path) = args.suite {
        run_suite(&path, args.iterations).await
    } else if let Some(paths) = args.diff {
        run_diff(&paths[0], &paths[1])
    } else if args.server {
        let addr = server_listen_addr(&args.transport, args.port);
        run_server(addr).await
    } else if let Some(addr) = args.client {
        run_client(addr, args.iterations, args.message_size).await
    } else if args.duplex {
        run_local_duplex(args.transport, args.iterations, args.message_size).await
    } else {
        match &args.transport {
            ChannelTransport::Tcp(_) => {
                run_local_subprocess("tcp", args.port, args.iterations, args.message_size)
            }
            _ => run_local_inprocess(args.transport, args.iterations, args.message_size).await,
        }
    }
}
