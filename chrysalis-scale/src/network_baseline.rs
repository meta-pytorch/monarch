/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::net::IpAddr;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use bytes::Bytes;
use chrysalis::DatagramSocket;
use chrysalis::DatagramSwitch;
use chrysalis::Pid;
use chrysalis::QuicConnectionStats;
use chrysalis::QuicIoStats;
use chrysalis::QuicTransport;
use chrysalis::Route;
use chrysalis::Router;
use chrysalis::UdpSocket;
use clap::Args;
use clap::ValueEnum;
use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use serde_json::json;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::process::Child;
use tokio::process::Command;
use tokio::task::JoinSet;

use crate::benchmark::ProcessCpuTime;
use crate::benchmark::resolve_ipv6;
use crate::benchmark::same_hostname;
use crate::benchmark::scale_quic_config;
use crate::benchmark::scale_quic_config_with_limits;
use crate::benchmark::task_hosts;

const CHUNK_SIZE: usize = 64 * 1024;
const QUIC_CHUNK_SIZE: usize = 1024 * 1024;
const MAX_IN_FLIGHT_SENDS: usize = 128;
const LARGE_SEND_WINDOW: u64 = 64 * 1024 * 1024;
const MIN_UDP_PAYLOAD: u16 = 1_200;
const BASELINE_TRANSMIT_SEGMENTS: usize = 10;
const EXPANDED_TRANSMIT_SEGMENTS: usize = 20;
const QUIC_WARMUP_BYTES: u64 = 64 * 1024 * 1024;
const PHASE_READY: u8 = 1;
const PHASE_COMPLETE: u8 = 2;
const DEFAULT_BYTES: u64 = 2 * 1024 * 1024 * 1024;
static PAYLOAD_CHUNK: LazyLock<Bytes> = LazyLock::new(|| Bytes::from(vec![0x2a; QUIC_CHUNK_SIZE]));

fn followup_address(address: SocketAddr) -> Result<SocketAddr> {
    let port = address
        .port()
        .checked_add(1)
        .context("QUIC follow-up port exceeds u16")?;
    Ok(SocketAddr::new(address.ip(), port))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum NetworkBaselineProtocol {
    /// Kernel TCP, bypassing Chrysalis and QUIC.
    Tcp,
    /// Chrysalis QUIC directly over UDP, bypassing Node and packet switching.
    DirectQuic,
    /// Direct QUIC followed by switched QUIC on the same pair of hosts.
    QuicAblation,
    /// Copied and owned-chunk QUIC I/O in an ABBA sequence on the same hosts.
    QuicIoAblation,
    /// Serial and pipelined owned-chunk writes in an ABBA sequence on the same hosts.
    QuicWriteChunksAblation,
    /// Default and enlarged QUIC send windows in an ABBA sequence on the same hosts.
    QuicSendWindowAblation,
    /// Path and minimum QUIC MTUs in an ABBA sequence on the same hosts.
    QuicMtuAblation,
    /// Default and disabled QUIC pacing in an ABBA sequence on the same hosts.
    QuicPacingAblation,
    /// Ten- and 20-segment QUIC GSO batches in an ABBA sequence on the same hosts.
    QuicGsoAblation,
}

impl NetworkBaselineProtocol {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::DirectQuic => "direct-quic",
            Self::QuicAblation => "quic-ablation",
            Self::QuicIoAblation => "quic-io-ablation",
            Self::QuicWriteChunksAblation => "quic-write-chunks-ablation",
            Self::QuicSendWindowAblation => "quic-send-window-ablation",
            Self::QuicMtuAblation => "quic-mtu-ablation",
            Self::QuicPacingAblation => "quic-pacing-ablation",
            Self::QuicGsoAblation => "quic-gso-ablation",
        }
    }

    const fn quic_followup(self) -> Option<QuicFollowup> {
        match self {
            Self::Tcp => None,
            Self::DirectQuic => Some(QuicFollowup::None),
            Self::QuicAblation => Some(QuicFollowup::Switch),
            Self::QuicIoAblation => Some(QuicFollowup::Io),
            Self::QuicWriteChunksAblation => Some(QuicFollowup::WriteChunks),
            Self::QuicSendWindowAblation => Some(QuicFollowup::SendWindow),
            Self::QuicMtuAblation => Some(QuicFollowup::Mtu),
            Self::QuicPacingAblation => Some(QuicFollowup::Pacing),
            Self::QuicGsoAblation => Some(QuicFollowup::Gso),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QuicFollowup {
    None,
    Switch,
    Io,
    WriteChunks,
    SendWindow,
    Mtu,
    Pacing,
    Gso,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QuicIoMode {
    Copy,
    Chunks,
    BatchedChunks,
}

#[derive(Clone, Copy, Debug)]
struct QuicPhase {
    protocol: &'static str,
    mode: QuicIoMode,
    send_window: Option<u64>,
    max_udp_payload: Option<u16>,
    disable_pacing: bool,
    max_transmit_batch_segments: Option<usize>,
}

const IO_PHASES: &[QuicPhase] = &[
    QuicPhase::new("direct-quic-chunks-1", QuicIoMode::Chunks),
    QuicPhase::new("direct-quic-chunks-2", QuicIoMode::Chunks),
    QuicPhase::new("direct-quic-copy-2", QuicIoMode::Copy),
];
const WRITE_CHUNK_PHASES: &[QuicPhase] = &[
    QuicPhase::new("direct-quic-batched-chunks-1", QuicIoMode::BatchedChunks),
    QuicPhase::new("direct-quic-batched-chunks-2", QuicIoMode::BatchedChunks),
    QuicPhase::new("direct-quic-chunks-2", QuicIoMode::Chunks),
];
const SEND_WINDOW_PHASES: &[QuicPhase] = &[
    QuicPhase::new(
        "direct-quic-batched-64m-window-1",
        QuicIoMode::BatchedChunks,
    )
    .with_send_window(LARGE_SEND_WINDOW),
    QuicPhase::new(
        "direct-quic-batched-64m-window-2",
        QuicIoMode::BatchedChunks,
    )
    .with_send_window(LARGE_SEND_WINDOW),
    QuicPhase::new(
        "direct-quic-batched-default-window-2",
        QuicIoMode::BatchedChunks,
    ),
];
const MTU_PHASES: &[QuicPhase] = &[
    QuicPhase::new("direct-quic-batched-1200-mtu-1", QuicIoMode::BatchedChunks)
        .with_max_udp_payload(MIN_UDP_PAYLOAD),
    QuicPhase::new("direct-quic-batched-1200-mtu-2", QuicIoMode::BatchedChunks)
        .with_max_udp_payload(MIN_UDP_PAYLOAD),
    QuicPhase::new("direct-quic-batched-path-mtu-2", QuicIoMode::BatchedChunks),
];
const PACING_PHASES: &[QuicPhase] = &[
    QuicPhase::new(
        "direct-quic-batched-pacing-disabled-1",
        QuicIoMode::BatchedChunks,
    )
    .without_pacing(),
    QuicPhase::new(
        "direct-quic-batched-pacing-disabled-2",
        QuicIoMode::BatchedChunks,
    )
    .without_pacing(),
    QuicPhase::new(
        "direct-quic-batched-default-pacing-2",
        QuicIoMode::BatchedChunks,
    ),
];
const GSO_PHASES: &[QuicPhase] = &[
    QuicPhase::new("direct-quic-batched-20-gso-1", QuicIoMode::BatchedChunks)
        .with_max_transmit_segments(EXPANDED_TRANSMIT_SEGMENTS),
    QuicPhase::new("direct-quic-batched-20-gso-2", QuicIoMode::BatchedChunks)
        .with_max_transmit_segments(EXPANDED_TRANSMIT_SEGMENTS),
    QuicPhase::new("direct-quic-batched-10-gso-2", QuicIoMode::BatchedChunks)
        .with_max_transmit_segments(BASELINE_TRANSMIT_SEGMENTS),
];

impl QuicPhase {
    const fn new(protocol: &'static str, mode: QuicIoMode) -> Self {
        Self {
            protocol,
            mode,
            send_window: None,
            max_udp_payload: None,
            disable_pacing: false,
            max_transmit_batch_segments: None,
        }
    }

    const fn with_send_window(mut self, send_window: u64) -> Self {
        self.send_window = Some(send_window);
        self
    }

    const fn with_max_udp_payload(mut self, max_udp_payload: u16) -> Self {
        self.max_udp_payload = Some(max_udp_payload);
        self
    }

    const fn without_pacing(mut self) -> Self {
        self.disable_pacing = true;
        self
    }

    const fn with_max_transmit_segments(mut self, max_transmit_batch_segments: usize) -> Self {
        self.max_transmit_batch_segments = Some(max_transmit_batch_segments);
        self
    }
}

impl QuicFollowup {
    const fn initial_protocol(self) -> &'static str {
        match self {
            Self::Io => "direct-quic-copy-1",
            Self::WriteChunks => "direct-quic-chunks-1",
            Self::SendWindow => "direct-quic-batched-default-window-1",
            Self::Mtu => "direct-quic-batched-path-mtu-1",
            Self::Pacing => "direct-quic-batched-default-pacing-1",
            Self::Gso => "direct-quic-batched-10-gso-1",
            Self::None | Self::Switch => "direct-quic",
        }
    }

    const fn phases(self) -> &'static [QuicPhase] {
        match self {
            Self::None | Self::Switch => &[],
            Self::Io => IO_PHASES,
            Self::WriteChunks => WRITE_CHUNK_PHASES,
            Self::SendWindow => SEND_WINDOW_PHASES,
            Self::Mtu => MTU_PHASES,
            Self::Pacing => PACING_PHASES,
            Self::Gso => GSO_PHASES,
        }
    }
}

const fn initial_quic_io_mode(followup: QuicFollowup) -> QuicIoMode {
    match followup {
        QuicFollowup::Io => QuicIoMode::Copy,
        QuicFollowup::WriteChunks => QuicIoMode::Chunks,
        QuicFollowup::None
        | QuicFollowup::Switch
        | QuicFollowup::SendWindow
        | QuicFollowup::Mtu
        | QuicFollowup::Pacing
        | QuicFollowup::Gso => QuicIoMode::BatchedChunks,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum NetworkBaselineProfile {
    /// Do not attach a system profiler.
    None,
    /// Count hardware and scheduler events with `perf stat`.
    Stat,
    /// Sample CPU-clock stacks and print the hottest symbols with `perf record`.
    Record,
}

impl NetworkBaselineProfile {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Stat => "stat",
            Self::Record => "record",
        }
    }
}

#[derive(Clone, Debug, Args)]
pub(crate) struct NetworkBaselineArgs {
    /// Protocol layer measured by this control.
    #[arg(
        long,
        env = "CHRYSALIS_SCALE_BASELINE_PROTOCOL",
        value_enum,
        default_value = "tcp"
    )]
    protocol: NetworkBaselineProtocol,

    /// Optional system profiler attached during the measured transfer.
    #[arg(
        long,
        env = "CHRYSALIS_SCALE_BASELINE_PROFILE",
        value_enum,
        default_value = "none"
    )]
    profile: NetworkBaselineProfile,

    /// Total bytes distributed across the measured connections.
    #[arg(long, env = "CHRYSALIS_SCALE_BASELINE_BYTES", default_value_t = DEFAULT_BYTES)]
    bytes: u64,

    /// Number of parallel kernel TCP connections sharing the payload.
    #[arg(
        long,
        env = "CHRYSALIS_SCALE_BASELINE_CONNECTIONS",
        default_value_t = 1
    )]
    connections: usize,

    /// Fixed TCP port used by the receiving task.
    #[arg(long, env = "CHRYSALIS_SCALE_PORT", default_value_t = 26600)]
    port: u16,

    /// Maximum time for placement, connection, and transfer.
    #[arg(
        long,
        env = "CHRYSALIS_SCALE_ECHO_TIMEOUT_SECS",
        default_value_t = 1800
    )]
    timeout_secs: u64,
}

pub(crate) async fn run(args: NetworkBaselineArgs) -> Result<()> {
    anyhow::ensure!(args.bytes > 0, "baseline byte count must be nonzero");
    anyhow::ensure!(
        args.connections > 0,
        "baseline connection count must be nonzero"
    );
    let hosts = task_hosts().await?;
    anyhow::ensure!(
        hosts.len() == 2,
        "network baseline requires exactly two MAST tasks, found {}",
        hosts.len()
    );
    let local_hostname = hostname::get()
        .context("read local hostname")?
        .into_string()
        .map_err(|_| anyhow::anyhow!("local hostname is not UTF-8"))?;
    anyhow::ensure!(
        hosts
            .iter()
            .any(|host| same_hostname(&local_hostname, host)),
        "local host {local_hostname} is absent from the MAST hostname vector"
    );
    let receiver = resolve_ipv6(&hosts[0], args.port).await?;
    let sender = resolve_ipv6(&hosts[1], args.port).await?;
    let timeout = Duration::from_secs(args.timeout_secs);
    let is_receiver = same_hostname(&local_hostname, &hosts[0]);
    match args.protocol {
        NetworkBaselineProtocol::Tcp if is_receiver => {
            receive_tcp(
                receiver,
                args.bytes,
                args.connections,
                timeout,
                args.profile,
                sender.ip(),
            )
            .await
        }
        NetworkBaselineProtocol::Tcp => {
            send_tcp(
                receiver,
                args.bytes,
                args.connections,
                timeout,
                args.profile,
            )
            .await
        }
        protocol if is_receiver => {
            anyhow::ensure!(
                args.connections == 1,
                "direct QUIC baseline requires exactly one connection"
            );
            receive_direct_quic(
                receiver,
                args.bytes,
                timeout,
                protocol
                    .quic_followup()
                    .expect("non-TCP protocol has a QUIC follow-up"),
                args.profile,
                sender.ip(),
            )
            .await
        }
        protocol => {
            anyhow::ensure!(
                args.connections == 1,
                "direct QUIC baseline requires exactly one connection"
            );
            let local = resolve_ipv6(&local_hostname, args.port).await?;
            send_direct_quic(
                receiver,
                local,
                args.bytes,
                timeout,
                protocol
                    .quic_followup()
                    .expect("non-TCP protocol has a QUIC follow-up"),
                args.profile,
            )
            .await
        }
    }
}

async fn receive_tcp(
    address: std::net::SocketAddr,
    expected: u64,
    connections: usize,
    timeout: Duration,
    profile: NetworkBaselineProfile,
    expected_peer: IpAddr,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("bind TCP baseline receiver {address}"))?;
    println!("[network-baseline] receiver listening on tcp://{address}");
    let mut streams = Vec::with_capacity(connections);
    for _ in 0..connections {
        let (stream, peer) = tokio::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            listener.accept(),
        )
        .await
        .context("timed out accepting TCP baseline connection")??;
        anyhow::ensure!(
            peer.ip() == expected_peer,
            "TCP baseline connection came from unexpected host {}",
            peer.ip()
        );
        stream.set_nodelay(true)?;
        streams.push(stream);
    }
    for stream in &mut streams {
        stream.write_all(&[1]).await?;
    }
    let profiler = PerfSession::start(profile, "tcp", "receiver").await?;
    let cpu_before = ProcessCpuTime::now();
    let started = Instant::now();
    let transfer = tokio::time::timeout(
        deadline.saturating_duration_since(Instant::now()),
        async move {
            let mut readers = JoinSet::new();
            for stream in streams {
                readers.spawn(receive_stream(stream));
            }
            let mut received = 0u64;
            while let Some(result) = readers.join_next().await {
                received = received
                    .checked_add(result.context("TCP baseline receiver task failed")??)
                    .context("TCP baseline byte count overflow")?;
            }
            Result::<u64>::Ok(received)
        },
    )
    .await;
    let elapsed = started.elapsed();
    let stop_profile = PerfSession::stop(profiler).await;
    let received = transfer.context("timed out receiving TCP baseline payload")??;
    anyhow::ensure!(
        received == expected,
        "TCP baseline received {received} bytes, expected {expected}"
    );
    let cpu = ProcessCpuTime::now().since(cpu_before);
    stop_profile?;
    print_result(
        "tcp",
        "receiver",
        address,
        received,
        connections,
        elapsed,
        cpu,
        None,
        None,
    );
    Ok(())
}

async fn receive_stream(mut stream: TcpStream) -> Result<u64> {
    let mut buffer = vec![0; CHUNK_SIZE];
    let mut received = 0u64;
    loop {
        let count = stream.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        received = received
            .checked_add(u64::try_from(count).expect("read size fits u64"))
            .context("TCP baseline stream byte count overflow")?;
    }
    stream.write_all(&received.to_be_bytes()).await?;
    stream.shutdown().await?;
    Ok(received)
}

async fn send_tcp(
    address: std::net::SocketAddr,
    bytes: u64,
    connections: usize,
    timeout: Duration,
    profile: NetworkBaselineProfile,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let mut streams = Vec::with_capacity(connections);
    for _ in 0..connections {
        let stream = loop {
            match TcpStream::connect(address).await {
                Ok(stream) => break stream,
                Err(error) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    if Instant::now() >= deadline {
                        return Err(error).context("timed out connecting TCP baseline sender");
                    }
                }
                Err(error) => return Err(error).context("connect TCP baseline sender"),
            }
        };
        stream.set_nodelay(true)?;
        streams.push(stream);
    }
    for stream in &mut streams {
        let mut ready = [0];
        stream.read_exact(&mut ready).await?;
        anyhow::ensure!(ready == [1], "invalid TCP baseline start marker");
    }
    let profiler = PerfSession::start(profile, "tcp", "sender").await?;
    let cpu_before = ProcessCpuTime::now();
    let started = Instant::now();
    let connections_u64 = u64::try_from(connections).context("connection count exceeds u64")?;
    let transfer = tokio::time::timeout(
        deadline.saturating_duration_since(Instant::now()),
        async move {
            let mut writers = JoinSet::new();
            for (index, stream) in streams.into_iter().enumerate() {
                let index = u64::try_from(index).expect("connection index fits u64");
                let stream_bytes =
                    bytes / connections_u64 + u64::from(index < bytes % connections_u64);
                writers.spawn(send_stream(stream, stream_bytes));
            }
            let mut received = 0u64;
            while let Some(result) = writers.join_next().await {
                received = received
                    .checked_add(result.context("TCP baseline sender task failed")??)
                    .context("TCP baseline receipt overflow")?;
            }
            Result::<u64>::Ok(received)
        },
    )
    .await;
    let elapsed = started.elapsed();
    let stop_profile = PerfSession::stop(profiler).await;
    let received = transfer.context("timed out sending TCP baseline payload")??;
    anyhow::ensure!(
        received == bytes,
        "TCP baseline receipt reports {received} bytes, expected {bytes}"
    );
    let cpu = ProcessCpuTime::now().since(cpu_before);
    stop_profile?;
    print_result(
        "tcp",
        "sender",
        address,
        bytes,
        connections,
        elapsed,
        cpu,
        None,
        None,
    );
    Ok(())
}

async fn send_stream(mut stream: TcpStream, bytes: u64) -> Result<u64> {
    let chunk = vec![0x2a; CHUNK_SIZE];
    let mut remaining = bytes;
    while remaining > 0 {
        let count = usize::try_from(remaining.min(CHUNK_SIZE as u64)).expect("bounded chunk size");
        stream.write_all(&chunk[..count]).await?;
        remaining -= count as u64;
    }
    stream.shutdown().await?;
    let mut receipt = [0; size_of::<u64>()];
    stream.read_exact(&mut receipt).await?;
    Ok(u64::from_be_bytes(receipt))
}

async fn receive_direct_quic(
    address: SocketAddr,
    expected: u64,
    timeout: Duration,
    followup: QuicFollowup,
    profile: NetworkBaselineProfile,
    expected_control_peer: IpAddr,
) -> Result<()> {
    let socket = UdpSocket::bind(address)
        .await
        .with_context(|| format!("bind direct QUIC receiver {address}"))?
        .into_std()
        .context("transfer direct QUIC receiver socket")?;
    let identity = chrysalis_identity_meta::issue()
        .await
        .context("issue direct QUIC receiver identity")?;
    let pid = identity.pid();
    let quic_config = if followup == QuicFollowup::Gso {
        scale_quic_config_with_limits(None, None, false, Some(BASELINE_TRANSMIT_SEGMENTS))?
    } else {
        scale_quic_config()?
    };
    let transport =
        QuicTransport::spawn_direct_udp_with_config(socket, identity.clone(), quic_config)
            .context("start direct QUIC receiver")?;
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("bind direct QUIC control receiver {address}"))?;
    println!("[network-baseline] receiver listening on direct-quic://{address}");
    let (mut control, control_peer) = tokio::time::timeout(timeout, listener.accept())
        .await
        .context("timed out accepting direct QUIC control connection")??;
    anyhow::ensure!(
        control_peer.ip() == expected_control_peer,
        "direct QUIC control connection came from unexpected host {}",
        control_peer.ip()
    );
    control.write_all(pid.as_bytes()).await?;
    let mut encoded_peer = [0; chrysalis::core::PID_LEN];
    control.read_exact(&mut encoded_peer).await?;
    let peer = Pid::from_bytes(encoded_peer);

    let initial_mode = initial_quic_io_mode(followup);
    let (warmup_source, connection_before) =
        receive_quic_stream(&transport, QUIC_WARMUP_BYTES, timeout, initial_mode).await?;
    anyhow::ensure!(warmup_source == peer, "direct QUIC control PID mismatch");
    let io_before = transport.io_stats();
    let protocol = followup.initial_protocol();
    let profiler = PerfSession::start(profile, protocol, "receiver").await?;
    let cpu_before = ProcessCpuTime::now();
    let started = Instant::now();
    let (source, connection_after) =
        receive_quic_stream(&transport, expected, timeout, initial_mode).await?;
    let elapsed = started.elapsed();
    let cpu = ProcessCpuTime::now().since(cpu_before);
    anyhow::ensure!(source == warmup_source, "direct QUIC source PID changed");
    let io = transport.io_stats().since(io_before);
    let connection = connection_after.since(connection_before);
    PerfSession::stop(profiler).await?;
    print_result(
        protocol,
        "receiver",
        address,
        expected,
        1,
        elapsed,
        cpu,
        Some(io),
        Some(connection),
    );
    signal_phase_complete(&mut control).await?;
    transport.shutdown();
    transport.join().await;
    drop(transport);
    match followup {
        QuicFollowup::None => {}
        QuicFollowup::Switch => {
            let local = followup_address(address)?;
            let peer_address =
                followup_address(SocketAddr::new(control.peer_addr()?.ip(), address.port()))?;
            receive_switched_quic(
                &mut control,
                local,
                peer_address,
                identity,
                peer,
                expected,
                timeout,
                profile,
            )
            .await?;
        }
        QuicFollowup::Io
        | QuicFollowup::WriteChunks
        | QuicFollowup::SendWindow
        | QuicFollowup::Mtu
        | QuicFollowup::Pacing
        | QuicFollowup::Gso => {
            for phase in followup.phases() {
                receive_direct_io_phase(
                    &mut control,
                    address,
                    identity.clone(),
                    peer,
                    expected,
                    timeout,
                    profile,
                    *phase,
                )
                .await?;
            }
        }
    }
    Ok(())
}

async fn send_direct_quic(
    address: SocketAddr,
    local: SocketAddr,
    bytes: u64,
    timeout: Duration,
    followup: QuicFollowup,
    profile: NetworkBaselineProfile,
) -> Result<()> {
    let mut control = connect_with_timeout(address, timeout).await?;
    let mut encoded_pid = [0; chrysalis::core::PID_LEN];
    control.read_exact(&mut encoded_pid).await?;
    let target = Pid::from_bytes(encoded_pid);
    let socket = UdpSocket::bind(local)
        .await
        .with_context(|| format!("bind direct QUIC sender {local}"))?
        .into_std()
        .context("transfer direct QUIC sender socket")?;
    let identity = chrysalis_identity_meta::issue()
        .await
        .context("issue direct QUIC sender identity")?;
    let quic_config = if followup == QuicFollowup::Gso {
        scale_quic_config_with_limits(None, None, false, Some(BASELINE_TRANSMIT_SEGMENTS))?
    } else {
        scale_quic_config()?
    };
    let transport =
        QuicTransport::spawn_direct_udp_with_config(socket, identity.clone(), quic_config)
            .context("start direct QUIC sender")?;
    control.write_all(identity.pid().as_bytes()).await?;
    let destination = UdpSocket::datagram_addr(address);

    let initial_mode = initial_quic_io_mode(followup);
    let connection_before = send_quic_stream(
        &transport,
        target,
        &destination,
        QUIC_WARMUP_BYTES,
        timeout,
        initial_mode,
    )
    .await?;
    let io_before = transport.io_stats();
    let protocol = followup.initial_protocol();
    let profiler = PerfSession::start(profile, protocol, "sender").await?;
    let cpu_before = ProcessCpuTime::now();
    let started = Instant::now();
    let connection_after = send_quic_stream(
        &transport,
        target,
        &destination,
        bytes,
        timeout,
        initial_mode,
    )
    .await?;
    let elapsed = started.elapsed();
    let cpu = ProcessCpuTime::now().since(cpu_before);
    let io = transport.io_stats().since(io_before);
    let connection = connection_after.since(connection_before);
    PerfSession::stop(profiler).await?;
    print_result(
        protocol,
        "sender",
        address,
        bytes,
        1,
        elapsed,
        cpu,
        Some(io),
        Some(connection),
    );
    wait_for_phase_complete(&mut control).await?;
    transport.shutdown();
    transport.join().await;
    drop(transport);
    match followup {
        QuicFollowup::None => {}
        QuicFollowup::Switch => {
            send_switched_quic(
                &mut control,
                followup_address(address)?,
                followup_address(local)?,
                identity,
                target,
                bytes,
                timeout,
                profile,
            )
            .await?;
        }
        QuicFollowup::Io
        | QuicFollowup::WriteChunks
        | QuicFollowup::SendWindow
        | QuicFollowup::Mtu
        | QuicFollowup::Pacing
        | QuicFollowup::Gso => {
            for phase in followup.phases() {
                send_direct_io_phase(
                    &mut control,
                    address,
                    local,
                    identity.clone(),
                    target,
                    bytes,
                    timeout,
                    profile,
                    *phase,
                )
                .await?;
            }
        }
    }
    Ok(())
}

async fn receive_direct_io_phase(
    control: &mut TcpStream,
    local: SocketAddr,
    identity: chrysalis::QuicIdentity,
    peer: Pid,
    expected: u64,
    timeout: Duration,
    profile: NetworkBaselineProfile,
    phase: QuicPhase,
) -> Result<()> {
    let protocol = phase.protocol;
    let socket = UdpSocket::bind(local)
        .await
        .with_context(|| format!("bind {protocol} receiver {local}"))?
        .into_std()
        .with_context(|| format!("transfer {protocol} receiver socket"))?;
    let transport = QuicTransport::spawn_direct_udp_with_config(
        socket,
        identity,
        scale_quic_config_with_limits(
            phase.send_window,
            phase.max_udp_payload,
            phase.disable_pacing,
            phase.max_transmit_batch_segments,
        )?,
    )
    .with_context(|| format!("start {protocol} receiver"))?;
    control.write_all(&[PHASE_READY]).await?;

    let (warmup_source, connection_before) =
        receive_quic_stream(&transport, QUIC_WARMUP_BYTES, timeout, phase.mode).await?;
    anyhow::ensure!(warmup_source == peer, "QUIC I/O source PID changed");
    let io_before = transport.io_stats();
    let profiler = PerfSession::start(profile, protocol, "receiver").await?;
    let cpu_before = ProcessCpuTime::now();
    let started = Instant::now();
    let (source, connection_after) =
        receive_quic_stream(&transport, expected, timeout, phase.mode).await?;
    let elapsed = started.elapsed();
    let cpu = ProcessCpuTime::now().since(cpu_before);
    anyhow::ensure!(source == peer, "QUIC I/O source PID changed");
    let io = transport.io_stats().since(io_before);
    let connection = connection_after.since(connection_before);
    PerfSession::stop(profiler).await?;
    print_result(
        protocol,
        "receiver",
        local,
        expected,
        1,
        elapsed,
        cpu,
        Some(io),
        Some(connection),
    );
    signal_phase_complete(control).await?;
    transport.shutdown();
    transport.join().await;
    Ok(())
}

async fn send_direct_io_phase(
    control: &mut TcpStream,
    peer_address: SocketAddr,
    local: SocketAddr,
    identity: chrysalis::QuicIdentity,
    target: Pid,
    bytes: u64,
    timeout: Duration,
    profile: NetworkBaselineProfile,
    phase: QuicPhase,
) -> Result<()> {
    let protocol = phase.protocol;
    let mut ready = [0];
    control.read_exact(&mut ready).await?;
    anyhow::ensure!(ready == [PHASE_READY], "invalid QUIC I/O start marker");
    let socket = UdpSocket::bind(local)
        .await
        .with_context(|| format!("bind {protocol} sender {local}"))?
        .into_std()
        .with_context(|| format!("transfer {protocol} sender socket"))?;
    let transport = QuicTransport::spawn_direct_udp_with_config(
        socket,
        identity,
        scale_quic_config_with_limits(
            phase.send_window,
            phase.max_udp_payload,
            phase.disable_pacing,
            phase.max_transmit_batch_segments,
        )?,
    )
    .with_context(|| format!("start {protocol} sender"))?;
    let destination = UdpSocket::datagram_addr(peer_address);

    let connection_before = send_quic_stream(
        &transport,
        target,
        &destination,
        QUIC_WARMUP_BYTES,
        timeout,
        phase.mode,
    )
    .await?;
    let io_before = transport.io_stats();
    let profiler = PerfSession::start(profile, protocol, "sender").await?;
    let cpu_before = ProcessCpuTime::now();
    let started = Instant::now();
    let connection_after =
        send_quic_stream(&transport, target, &destination, bytes, timeout, phase.mode).await?;
    let elapsed = started.elapsed();
    let cpu = ProcessCpuTime::now().since(cpu_before);
    let io = transport.io_stats().since(io_before);
    let connection = connection_after.since(connection_before);
    PerfSession::stop(profiler).await?;
    print_result(
        protocol,
        "sender",
        peer_address,
        bytes,
        1,
        elapsed,
        cpu,
        Some(io),
        Some(connection),
    );
    wait_for_phase_complete(control).await?;
    transport.shutdown();
    transport.join().await;
    Ok(())
}

async fn receive_switched_quic(
    control: &mut TcpStream,
    local: SocketAddr,
    peer_address: SocketAddr,
    identity: chrysalis::QuicIdentity,
    peer: Pid,
    expected: u64,
    timeout: Duration,
    profile: NetworkBaselineProfile,
) -> Result<()> {
    let physical = UdpSocket::bind(local)
        .await
        .with_context(|| format!("bind switched QUIC receiver {local}"))?;
    let router = Arc::new(Router::new());
    router.insert(
        peer,
        Route::permanent(UdpSocket::datagram_addr(peer_address)),
    );
    let datagram_switch = DatagramSwitch::spawn(physical, router);
    let binding = Arc::new(
        datagram_switch
            .bind_routed(identity.pid())
            .context("bind switched QUIC receiver PID")?,
    );
    let transport = QuicTransport::spawn_with_config(binding, identity, scale_quic_config()?)
        .context("start switched QUIC receiver")?;
    control.write_all(&[PHASE_READY]).await?;

    let (warmup_source, connection_before) = receive_quic_stream(
        &transport,
        QUIC_WARMUP_BYTES,
        timeout,
        QuicIoMode::BatchedChunks,
    )
    .await?;
    anyhow::ensure!(warmup_source == peer, "switched QUIC source PID changed");
    let io_before = transport.io_stats();
    let profiler = PerfSession::start(profile, "switched-quic", "receiver").await?;
    let cpu_before = ProcessCpuTime::now();
    let started = Instant::now();
    let (source, connection_after) =
        receive_quic_stream(&transport, expected, timeout, QuicIoMode::BatchedChunks).await?;
    let elapsed = started.elapsed();
    let cpu = ProcessCpuTime::now().since(cpu_before);
    anyhow::ensure!(source == peer, "switched QUIC source PID changed");
    let io = transport.io_stats().since(io_before);
    let connection = connection_after.since(connection_before);
    PerfSession::stop(profiler).await?;
    print_result(
        "switched-quic",
        "receiver",
        local,
        expected,
        1,
        elapsed,
        cpu,
        Some(io),
        Some(connection),
    );
    signal_phase_complete(control).await?;
    transport.shutdown();
    transport.join().await;
    drop(transport);
    datagram_switch.shutdown();
    datagram_switch.join().await;
    Ok(())
}

async fn send_switched_quic(
    control: &mut TcpStream,
    peer_address: SocketAddr,
    local: SocketAddr,
    identity: chrysalis::QuicIdentity,
    target: Pid,
    bytes: u64,
    timeout: Duration,
    profile: NetworkBaselineProfile,
) -> Result<()> {
    let mut ready = [0];
    control.read_exact(&mut ready).await?;
    anyhow::ensure!(ready == [PHASE_READY], "invalid switched QUIC start marker");
    let physical = UdpSocket::bind(local)
        .await
        .with_context(|| format!("bind switched QUIC sender {local}"))?;
    let router = Arc::new(Router::new());
    router.insert(
        target,
        Route::permanent(UdpSocket::datagram_addr(peer_address)),
    );
    let datagram_switch = DatagramSwitch::spawn(physical, router);
    let binding = Arc::new(
        datagram_switch
            .bind_routed(identity.pid())
            .context("bind switched QUIC sender PID")?,
    );
    let transport = QuicTransport::spawn_with_config(binding, identity, scale_quic_config()?)
        .context("start switched QUIC sender")?;
    let destination = UdpSocket::datagram_addr(peer_address);

    let connection_before = send_quic_stream(
        &transport,
        target,
        &destination,
        QUIC_WARMUP_BYTES,
        timeout,
        QuicIoMode::BatchedChunks,
    )
    .await?;
    let io_before = transport.io_stats();
    let profiler = PerfSession::start(profile, "switched-quic", "sender").await?;
    let cpu_before = ProcessCpuTime::now();
    let started = Instant::now();
    let connection_after = send_quic_stream(
        &transport,
        target,
        &destination,
        bytes,
        timeout,
        QuicIoMode::BatchedChunks,
    )
    .await?;
    let elapsed = started.elapsed();
    let cpu = ProcessCpuTime::now().since(cpu_before);
    let io = transport.io_stats().since(io_before);
    let connection = connection_after.since(connection_before);
    PerfSession::stop(profiler).await?;
    print_result(
        "switched-quic",
        "sender",
        peer_address,
        bytes,
        1,
        elapsed,
        cpu,
        Some(io),
        Some(connection),
    );
    wait_for_phase_complete(control).await?;
    transport.shutdown();
    transport.join().await;
    drop(transport);
    datagram_switch.shutdown();
    datagram_switch.join().await;
    Ok(())
}

async fn signal_phase_complete(control: &mut TcpStream) -> Result<()> {
    control.write_all(&[PHASE_COMPLETE]).await?;
    Ok(())
}

async fn wait_for_phase_complete(control: &mut TcpStream) -> Result<()> {
    let mut complete = [0];
    control.read_exact(&mut complete).await?;
    anyhow::ensure!(
        complete == [PHASE_COMPLETE],
        "invalid QUIC phase completion marker"
    );
    Ok(())
}

async fn connect_with_timeout(address: SocketAddr, timeout: Duration) -> Result<TcpStream> {
    let deadline = Instant::now() + timeout;
    loop {
        match TcpStream::connect(address).await {
            Ok(stream) => return Ok(stream),
            Err(error) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(25)).await;
                if Instant::now() >= deadline {
                    return Err(error).context("timed out connecting baseline control channel");
                }
            }
            Err(error) => return Err(error).context("connect baseline control channel"),
        }
    }
}

async fn receive_quic_stream<T: DatagramSocket>(
    transport: &QuicTransport<T>,
    expected: u64,
    timeout: Duration,
    mode: QuicIoMode,
) -> Result<(Pid, QuicConnectionStats)> {
    let incoming = tokio::time::timeout(timeout, transport.accept())
        .await
        .context("timed out accepting direct QUIC stream")??;
    let source = incoming.source();
    let (_, stream) = incoming.into_parts();
    let (mut send, mut recv) = stream.into_parts();
    let received = match mode {
        QuicIoMode::Copy => receive_quic_copied(&mut recv).await?,
        QuicIoMode::Chunks | QuicIoMode::BatchedChunks => receive_quic_chunks(&mut recv).await?,
    };
    anyhow::ensure!(
        received == expected,
        "direct QUIC received {received} bytes, expected {expected}"
    );
    send.write_all(&received.to_be_bytes()).await?;
    send.finish().await.context("finish direct QUIC receipt")?;
    let connection = transport
        .connection_stats(source)
        .context("direct QUIC connection disappeared while receiving")?;
    Ok((source, connection))
}

async fn send_quic_stream<T: DatagramSocket>(
    transport: &QuicTransport<T>,
    target: Pid,
    destination: &chrysalis::DatagramAddr,
    bytes: u64,
    timeout: Duration,
    mode: QuicIoMode,
) -> Result<QuicConnectionStats> {
    let connection = tokio::time::timeout(timeout, async {
        let stream = transport.dial(target, destination.clone()).await?;
        let (mut send, mut recv) = stream.into_parts();
        match mode {
            QuicIoMode::Copy => send_quic_copied(&mut send, bytes).await?,
            QuicIoMode::Chunks => send_quic_chunks(&mut send, bytes).await?,
            QuicIoMode::BatchedChunks => send_quic_chunk_batches(&mut send, bytes).await?,
        }
        send.finish().await.context("finish direct QUIC payload")?;
        let mut receipt = [0; size_of::<u64>()];
        recv.read_exact(&mut receipt).await?;
        let received = u64::from_be_bytes(receipt);
        anyhow::ensure!(
            received == bytes,
            "direct QUIC receipt reports {received} bytes, expected {bytes}"
        );
        transport
            .connection_stats(target)
            .context("direct QUIC connection disappeared while sending")
    })
    .await
    .context("direct QUIC transfer timed out")??;
    Ok(connection)
}

async fn receive_quic_copied(recv: &mut chrysalis::RecvStream) -> Result<u64> {
    let mut buffer = vec![0; CHUNK_SIZE];
    let mut received = 0u64;
    loop {
        let count = recv.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        received = received
            .checked_add(u64::try_from(count).expect("read size fits u64"))
            .context("direct QUIC byte count overflow")?;
    }
    Ok(received)
}

async fn receive_quic_chunks(recv: &mut chrysalis::RecvStream) -> Result<u64> {
    let mut received = 0u64;
    let max_bytes =
        std::num::NonZeroUsize::new(QUIC_CHUNK_SIZE).expect("QUIC chunk size is nonzero");
    loop {
        let (count, status) = recv.discard(max_bytes).await?;
        received = received
            .checked_add(u64::try_from(count).expect("discard size fits u64"))
            .context("direct QUIC byte count overflow")?;
        match status {
            chrysalis::ReceiveStatus::Data => {}
            chrysalis::ReceiveStatus::Fin => return Ok(received),
            chrysalis::ReceiveStatus::Reset(code) => {
                anyhow::bail!("direct QUIC stream reset: {code}")
            }
            chrysalis::ReceiveStatus::Closed => {
                anyhow::bail!("direct QUIC stream closed before FIN")
            }
            chrysalis::ReceiveStatus::Cancelled => {
                anyhow::bail!("direct QUIC receive was cancelled")
            }
            chrysalis::ReceiveStatus::Stopped(code) => {
                anyhow::bail!("direct QUIC receive stopped locally: {code}")
            }
        }
    }
}

async fn send_quic_copied(send: &mut chrysalis::SendStream, bytes: u64) -> Result<()> {
    let mut remaining = bytes;
    while remaining > 0 {
        let count = usize::try_from(remaining.min(CHUNK_SIZE as u64)).expect("bounded chunk size");
        send.write_all(&PAYLOAD_CHUNK[..count]).await?;
        remaining -= count as u64;
    }
    Ok(())
}

async fn send_quic_chunks(send: &mut chrysalis::SendStream, bytes: u64) -> Result<()> {
    let chunk = Bytes::clone(&PAYLOAD_CHUNK);
    let mut remaining = bytes;
    while remaining > 0 {
        let count =
            usize::try_from(remaining.min(QUIC_CHUNK_SIZE as u64)).expect("bounded chunk size");
        send.send(chunk.slice(..count)).await?;
        remaining -= count as u64;
    }
    Ok(())
}

async fn send_quic_chunk_batches(send: &mut chrysalis::SendStream, bytes: u64) -> Result<()> {
    let chunk = Bytes::clone(&PAYLOAD_CHUNK);
    let mut remaining = bytes;
    let mut sends = FuturesUnordered::new();
    while remaining > 0 {
        let count =
            usize::try_from(remaining.min(QUIC_CHUNK_SIZE as u64)).expect("bounded chunk size");
        sends.push(send.send(chunk.slice(..count)));
        remaining -= count as u64;
        if sends.len() == MAX_IN_FLIGHT_SENDS {
            sends
                .next()
                .await
                .expect("full send window contains one future")?;
        }
    }
    while let Some(result) = sends.next().await {
        result?;
    }
    Ok(())
}

struct PerfSession {
    mode: NetworkBaselineProfile,
    protocol: &'static str,
    side: &'static str,
    child: Child,
    data: Option<PathBuf>,
}

impl PerfSession {
    async fn start(
        mode: NetworkBaselineProfile,
        protocol: &'static str,
        side: &'static str,
    ) -> Result<Option<Self>> {
        if mode == NetworkBaselineProfile::None {
            return Ok(None);
        }
        let pid = std::process::id().to_string();
        let mut command = Command::new("perf");
        let data = match mode {
            NetworkBaselineProfile::None => unreachable!("none returned above"),
            NetworkBaselineProfile::Stat => {
                command.args([
                    "stat",
                    "--field-separator",
                    ",",
                    "--event",
                    "task-clock,context-switches,cpu-migrations,page-faults,cycles,instructions,branches,branch-misses,cache-references,cache-misses",
                    "--pid",
                    &pid,
                ]);
                None
            }
            NetworkBaselineProfile::Record => {
                let path = std::env::temp_dir().join(format!(
                    "chrysalis-{protocol}-{side}-{}.perf.data",
                    std::process::id()
                ));
                command
                    .args([
                        "record",
                        "--quiet",
                        "--event",
                        "cpu-clock",
                        "--freq",
                        "499",
                        "--call-graph",
                        "fp",
                        "--pid",
                        &pid,
                        "--output",
                    ])
                    .arg(&path);
                Some(path)
            }
        };
        let mut child = command
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("start perf {mode:?} for {protocol} {side}"))?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(status) = child.try_wait().context("inspect perf startup")? {
            let output = child.wait_with_output().await?;
            anyhow::bail!(
                "perf {mode:?} for {protocol} {side} exited during startup with {status}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(Some(Self {
            mode,
            protocol,
            side,
            child,
            data,
        }))
    }

    async fn stop(session: Option<Self>) -> Result<()> {
        let Some(session) = session else {
            return Ok(());
        };
        let child_pid = session.child.id().context("perf process has no PID")?;
        // SAFETY: `child_pid` identifies the live perf child owned by this session.
        let signal_result = unsafe { libc::kill(child_pid as libc::pid_t, libc::SIGINT) };
        if signal_result != 0 {
            return Err(std::io::Error::last_os_error()).context("stop perf process");
        }
        let output = session.child.wait_with_output().await?;
        match session.mode {
            NetworkBaselineProfile::None => unreachable!("none has no session"),
            NetworkBaselineProfile::Stat => {
                println!(
                    "[network-baseline] PERF protocol={} side={}\n{}",
                    session.protocol,
                    session.side,
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            NetworkBaselineProfile::Record => {
                let data = session.data.expect("record mode has an output path");
                let report = Command::new("perf")
                    .args([
                        "report",
                        "--stdio",
                        "--no-children",
                        "--percent-limit",
                        "1.0",
                        "--input",
                    ])
                    .arg(&data)
                    .output()
                    .await
                    .context("generate perf report")?;
                anyhow::ensure!(
                    report.status.success(),
                    "perf report failed: {}",
                    String::from_utf8_lossy(&report.stderr)
                );
                println!(
                    "[network-baseline] PERF protocol={} side={}\n{}",
                    session.protocol,
                    session.side,
                    String::from_utf8_lossy(&report.stdout)
                );
                tokio::fs::remove_file(&data)
                    .await
                    .with_context(|| format!("remove perf data {}", data.display()))?;
            }
        }
        Ok(())
    }
}

fn print_result(
    protocol: &str,
    side: &str,
    peer: SocketAddr,
    bytes: u64,
    connections: usize,
    elapsed: Duration,
    cpu: ProcessCpuTime,
    io: Option<QuicIoStats>,
    connection: Option<QuicConnectionStats>,
) {
    let mib_per_second = if elapsed.is_zero() {
        0.0
    } else {
        bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64()
    };
    println!(
        "[network-baseline] RESULT {}",
        json!({
            "protocol": protocol,
            "side": side,
            "peer": peer.to_string(),
            "connections": connections,
            "bytes": bytes,
            "elapsed_seconds": elapsed.as_secs_f64(),
            "mib_per_second": mib_per_second,
            "gibits_per_second": mib_per_second * 8.0 / 1024.0,
            "user_cpu_seconds": cpu.user.as_secs_f64(),
            "system_cpu_seconds": cpu.system.as_secs_f64(),
            "cpu_cores": cpu.utilization(elapsed),
            "transmit_calls": io.map(|stats| stats.transmit_calls),
            "transmit_datagrams": io.map(|stats| stats.transmit_datagrams),
            "transmit_bytes": io.map(|stats| stats.transmit_bytes),
            "receive_calls": io.map(|stats| stats.receive_calls),
            "receive_datagrams": io.map(|stats| stats.receive_datagrams),
            "receive_bytes": io.map(|stats| stats.receive_bytes),
            "connection_transmit_datagrams": connection.map(|stats| stats.transmit_datagrams),
            "connection_transmit_bytes": connection.map(|stats| stats.transmit_bytes),
            "connection_transmit_ios": connection.map(|stats| stats.transmit_ios),
            "connection_receive_datagrams": connection.map(|stats| stats.receive_datagrams),
            "connection_receive_bytes": connection.map(|stats| stats.receive_bytes),
            "connection_receive_ios": connection.map(|stats| stats.receive_ios),
            "connection_rtt_micros": connection.map(|stats| stats.rtt.as_micros()),
            "connection_congestion_window": connection.map(|stats| stats.congestion_window),
            "connection_congestion_events": connection.map(|stats| stats.congestion_events),
            "connection_mtu": connection.map(|stats| stats.current_mtu),
            "connection_lost_packets": connection.map(|stats| stats.lost_packets),
            "connection_lost_bytes": connection.map(|stats| stats.lost_bytes),
            "connection_sent_packets": connection.map(|stats| stats.sent_packets),
        })
    );
}

#[cfg(test)]
mod tests {
    use super::QuicFollowup;
    use super::QuicIoMode;
    use super::initial_quic_io_mode;

    #[test]
    fn roofline_modes_use_pipelined_owned_chunks() {
        for followup in [QuicFollowup::None, QuicFollowup::Switch] {
            assert_eq!(initial_quic_io_mode(followup), QuicIoMode::BatchedChunks);
        }
    }

    #[test]
    fn ablation_controls_retain_their_io_modes() {
        assert_eq!(initial_quic_io_mode(QuicFollowup::Io), QuicIoMode::Copy);
        assert_eq!(
            initial_quic_io_mode(QuicFollowup::WriteChunks),
            QuicIoMode::Chunks
        );
    }
}
