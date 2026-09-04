/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs;
use std::io;
use std::io::ErrorKind;
use std::io::Write as _;
use std::mem::MaybeUninit;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::path::Path;
use std::path::PathBuf;
use std::process::Stdio;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::Context;
use anyhow::Result;
use bytes::Bytes;
use bytes::BytesMut;
use chrysalis::DatagramAddr;
use chrysalis::DatagramSocket;
use chrysalis::DatagramSocketSet;
use chrysalis::Labels;
use chrysalis::NamespaceConfig;
use chrysalis::Node;
use chrysalis::NodeConfig;
use chrysalis::ParentEndpoint;
use chrysalis::ParentManagerStatus;
use chrysalis::Pid;
use chrysalis::PidPrefix;
use chrysalis::ProcEntry;
use chrysalis::ReceiveOptions;
use chrysalis::ReceiveStatus;
use chrysalis::ResolveConsistency;
use chrysalis::TransportConfig;
use chrysalis::UdpSocket;
use chrysalis::UnixDatagramSocket;
use clap::Args;
use clap::ValueEnum;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use serde_json::Value;
use serde_json::json;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt as _;
use tokio::process::Child;
use tokio::process::Command;
use tokio::task::JoinSet;

use crate::persist::Experiment;
use crate::persist::ExperimentKind;
use crate::persist::ExperimentResult;
use crate::persist::ExperimentStore;
use crate::persist::ExperimentTargets;
use crate::persist::NodeRecord;

const ECHO_OPERATION: u8 = 1;
const DELIVERY_OPERATION: u8 = 2;
const ECHO_HEADER_LEN: usize = size_of::<u32>();
const PAYLOAD_CHUNK_LEN: usize = 64 * 1024;
const OWNED_PAYLOAD_CHUNK_LEN: usize = 1024 * 1024;
static PAYLOAD_CHUNK: [u8; PAYLOAD_CHUNK_LEN] = [PAYLOAD_BYTE; PAYLOAD_CHUNK_LEN];
static OWNED_PAYLOAD_CHUNK: [u8; OWNED_PAYLOAD_CHUNK_LEN] = [PAYLOAD_BYTE; OWNED_PAYLOAD_CHUNK_LEN];
const RECEIVE_CHUNK_LEN: usize = 256 * 1024;
const MAX_IN_FLIGHT_SENDS: usize = 128;
const SCALE_FLOW_WINDOW: u64 = 256 * 1024 * 1024;
const SCALE_GSO_SEGMENTS: usize = 12;
const PAYLOAD_BYTE: u8 = 0x2a;
const META_NETWORK_MAX_UDP_PAYLOAD: u16 = 1_450;
const EXPERIMENT_POLL_INTERVAL: Duration = Duration::from_millis(250);
const EXPERIMENT_LEASE_GRACE: Duration = Duration::from_secs(60);
const WORKER_POLL_INTERVAL: Duration = Duration::from_millis(25);
const STREAM_FIN_TIMEOUT: Duration = Duration::from_secs(5);
const PERSISTENCE_SCHEMA_VERSION: u32 = 8;
pub(crate) const DEFAULT_NODES_PER_TASK: usize = 100;
pub(crate) const DEFAULT_IDENTITY_CONCURRENCY: usize = 8;

#[derive(Clone, Copy, Debug)]
pub(crate) struct ProcessCpuTime {
    pub(crate) user: Duration,
    pub(crate) system: Duration,
}

impl ProcessCpuTime {
    pub(crate) fn now() -> Self {
        let mut usage = MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: `usage` points to writable storage for one `rusage` value.
        let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        assert_eq!(result, 0, "getrusage failed");
        // SAFETY: successful `getrusage` initialized the complete output value.
        let usage = unsafe { usage.assume_init() };
        Self {
            user: timeval_duration(usage.ru_utime),
            system: timeval_duration(usage.ru_stime),
        }
    }

    pub(crate) fn since(self, earlier: Self) -> Self {
        Self {
            user: self.user.saturating_sub(earlier.user),
            system: self.system.saturating_sub(earlier.system),
        }
    }

    pub(crate) fn utilization(self, elapsed: Duration) -> f64 {
        if elapsed.is_zero() {
            return 0.0;
        }
        (self.user + self.system).as_secs_f64() / elapsed.as_secs_f64()
    }
}

fn timeval_duration(value: libc::timeval) -> Duration {
    assert!(value.tv_sec >= 0 && value.tv_usec >= 0);
    Duration::from_secs(value.tv_sec as u64) + Duration::from_micros(value.tv_usec as u64)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum Topology {
    Flat,
    TaskHead,
}

impl Topology {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Flat => "flat",
            Self::TaskHead => "task-head",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NodeRole {
    Root,
    Head,
    Leaf,
}

impl NodeRole {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Root => "root",
            Self::Head => "head",
            Self::Leaf => "leaf",
        }
    }
}

#[derive(Clone, Debug, Args)]
pub(crate) struct RunArgs {
    /// Number of logical Chrysalis nodes expected in this job.
    #[arg(long, env = "CHRYSALIS_SCALE_NODES")]
    nodes: usize,

    /// Maximum number of logical nodes hosted by each MAST task.
    #[arg(
        long,
        env = "CHRYSALIS_SCALE_NODES_PER_TASK",
        default_value_t = DEFAULT_NODES_PER_TASK
    )]
    nodes_per_task: usize,

    /// Process-mesh topology used by logical nodes.
    #[arg(
        long,
        env = "CHRYSALIS_SCALE_TOPOLOGY",
        value_enum,
        default_value = "task-head"
    )]
    topology: Topology,

    /// Maximum concurrent Meta identity requests in each task.
    #[arg(
        long,
        env = "CHRYSALIS_SCALE_IDENTITY_CONCURRENCY",
        default_value_t = DEFAULT_IDENTITY_CONCURRENCY
    )]
    identity_concurrency: usize,

    /// Fixed UDP port used to locate the root node.
    #[arg(long, env = "CHRYSALIS_SCALE_PORT", default_value_t = 26600)]
    port: u16,

    /// Maximum number of simultaneous root echo operations.
    #[arg(long, env = "CHRYSALIS_SCALE_CONCURRENCY", default_value_t = 1024)]
    concurrency: usize,

    /// Maximum time to wait for full namespace convergence.
    #[arg(
        long,
        env = "CHRYSALIS_SCALE_JOIN_TIMEOUT_SECS",
        default_value_t = 1800
    )]
    join_timeout_secs: u64,

    /// Maximum time for one echo sweep or one-shot child wait.
    #[arg(
        long,
        env = "CHRYSALIS_SCALE_ECHO_TIMEOUT_SECS",
        default_value_t = 1800
    )]
    echo_timeout_secs: u64,
}

#[cfg(test)]
impl RunArgs {
    pub(crate) const fn topology(&self) -> Topology {
        self.topology
    }
}

#[derive(Clone, Debug, Args)]
pub(crate) struct PersistArgs {
    #[command(flatten)]
    run: RunArgs,

    /// Directory containing one replicated SQLite database per logical node.
    #[arg(
        long,
        env = "CHRYSALIS_SCALE_DATABASE_DIR",
        default_value = "/tmp/chrysalis-scale"
    )]
    database_dir: PathBuf,
}

#[derive(Clone, Debug, Args)]
pub(crate) struct WorkerArgs {
    #[command(flatten)]
    run: RunArgs,

    /// Logical rank hosted by this worker process.
    #[arg(long)]
    rank: usize,

    /// Topology level recorded for this node.
    #[arg(long)]
    level: u8,

    /// Carrier address to bind.
    #[arg(long)]
    bind: CarrierAddr,

    /// Parent PID and carrier address.
    #[arg(long)]
    parent: ParentTarget,

    /// File created after this worker has joined its parent.
    #[arg(long)]
    ready_file: PathBuf,

    /// Optional directory containing one database per logical node.
    #[arg(long)]
    database_dir: Option<PathBuf>,
}

struct LocalNode {
    rank: usize,
    is_root: bool,
    node: Arc<Node>,
    store: Option<ExperimentStore>,
}

#[derive(Clone)]
struct TaskMetadata {
    id: usize,
    handle: String,
    hostname: String,
    expected_nodes: usize,
    nodes_per_task: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ParentTarget {
    Discover(CarrierAddr),
    Pinned { pid: Pid, address: CarrierAddr },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CarrierAddr {
    Udp(SocketAddr),
    Unix(PathBuf),
}

#[derive(Debug)]
struct WorkerSpec {
    rank: usize,
    level: u8,
    bind: CarrierAddr,
    parent: ParentTarget,
    ready_file: PathBuf,
}

#[derive(Debug)]
struct WorkerProcess {
    rank: usize,
    child: Child,
    ready_file: PathBuf,
    started: Instant,
    status: Option<std::process::ExitStatus>,
}

#[derive(Debug, Default)]
struct WorkerGroup {
    processes: Vec<WorkerProcess>,
}

impl ParentTarget {
    fn config(&self) -> Result<NamespaceConfig> {
        let endpoint = ParentEndpoint::new(self.address().datagram_addr());
        match self {
            Self::Discover(_) => Ok(NamespaceConfig::try_discover(vec![endpoint])?),
            Self::Pinned { pid, .. } => Ok(NamespaceConfig::try_new(*pid, vec![endpoint])?),
        }
    }

    const fn address(&self) -> &CarrierAddr {
        match self {
            Self::Discover(address) | Self::Pinned { address, .. } => address,
        }
    }
}

impl CarrierAddr {
    fn datagram_addr(&self) -> DatagramAddr {
        match self {
            Self::Udp(address) => UdpSocket::datagram_addr(*address),
            Self::Unix(path) => UnixDatagramSocket::datagram_addr(path),
        }
    }
}

impl std::fmt::Display for CarrierAddr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Udp(address) => write!(formatter, "udp://{address}"),
            Self::Unix(path) => write!(formatter, "unix://{}", path.display()),
        }
    }
}

impl FromStr for CarrierAddr {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if let Some(address) = value.strip_prefix("udp://") {
            return Ok(Self::Udp(address.parse().context("invalid UDP address")?));
        }
        if let Some(path) = value.strip_prefix("unix://") {
            if path.is_empty() {
                anyhow::bail!("Unix address requires a path");
            }
            return Ok(Self::Unix(path.into()));
        }
        anyhow::bail!("carrier address must use udp:// or unix://")
    }
}

impl std::fmt::Display for ParentTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Discover(address) => write!(formatter, "{address}"),
            Self::Pinned { pid, address } => {
                write!(formatter, "{address}?authority={}", format_pid(*pid))
            }
        }
    }
}

impl FromStr for ParentTarget {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (locator, query) = match value.split_once('?') {
            Some((locator, query)) => (locator, Some(query)),
            None => (value, None),
        };
        let address = locator.parse()?;
        match query {
            Some(query) => {
                let pid = query
                    .strip_prefix("authority=")
                    .filter(|pid| !pid.contains('&'))
                    .context("parent locator only supports the authority query")?;
                Ok(Self::Pinned {
                    pid: parse_pid(pid)?,
                    address,
                })
            }
            None => Ok(Self::Discover(address)),
        }
    }
}

pub(crate) async fn run(args: RunArgs) -> Result<()> {
    run_mode(args, None).await
}

pub(crate) async fn persist(args: PersistArgs) -> Result<()> {
    fs::create_dir_all(&args.database_dir).with_context(|| {
        format!(
            "create experiment database directory {}",
            args.database_dir.display()
        )
    })?;
    run_mode(args.run, Some(args.database_dir)).await
}

async fn run_mode(args: RunArgs, database_dir: Option<PathBuf>) -> Result<()> {
    anyhow::ensure!(
        args.nodes > 1,
        "scale benchmark requires at least two nodes"
    );
    anyhow::ensure!(args.nodes_per_task > 0, "nodes per task must be nonzero");
    anyhow::ensure!(
        args.identity_concurrency > 0,
        "identity concurrency must be nonzero"
    );
    anyhow::ensure!(args.concurrency > 0, "echo concurrency must be nonzero");
    let started = Instant::now();
    let task_id = task_rank()?;
    let task_count = task_count(args.nodes, args.nodes_per_task);
    anyhow::ensure!(
        task_id < task_count,
        "task ID {task_id} exceeds task count {task_count}"
    );
    let hosts = task_hosts()?;
    anyhow::ensure!(
        hosts.len() == task_count,
        "scale benchmark requires one distinct host per task: found {} hosts for {task_count} tasks",
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
    let root_address = resolve_ipv6(&hosts[0], args.port).await?;
    let local_bind_address = resolve_ipv6(&local_hostname, 0).await?;
    let task = Arc::new(TaskMetadata {
        id: task_id,
        handle: mast_task_handle(task_id),
        hostname: local_hostname.clone(),
        expected_nodes: args.nodes,
        nodes_per_task: args.nodes_per_task,
    });
    let ranks = node_range(args.nodes, args.nodes_per_task, task_id);
    println!(
        "[task {task_id}/{task_count}] hosting nodes {}..{} on {local_hostname} with {} topology",
        ranks.start,
        ranks.end,
        args.topology.as_str(),
    );

    let root_socket = if same_hostname(&local_hostname, &hosts[0]) {
        match UdpSocket::bind(root_address).await {
            Ok(socket) => Some(socket),
            Err(error) if error.kind() == ErrorKind::AddrInUse => {
                eprintln!(
                    "warning: root UDP address {root_address} is already in use; \
                     proceeding as a non-root task head"
                );
                None
            }
            Err(error) => return Err(error).context("bind root UDP carrier"),
        }
    } else {
        None
    };
    let runtime_dir =
        std::env::temp_dir().join(format!("chrysalis-scale-{task_id}-{}", std::process::id()));
    tokio::fs::create_dir(&runtime_dir)
        .await
        .with_context(|| format!("create task runtime directory {}", runtime_dir.display()))?;
    let root_parent = ParentTarget::Discover(CarrierAddr::Udp(root_address));
    let head_path = runtime_dir.join("head.sock");
    let local_node = async {
        match (args.topology, root_socket) {
            (Topology::TaskHead, Some(socket)) => Ok(Some(
                create_local_node(
                    ranks.start,
                    0,
                    true,
                    args.topology,
                    socket,
                    Some(&head_path),
                    None,
                    database_dir.as_deref(),
                    &task,
                    Duration::from_secs(args.join_timeout_secs),
                )
                .await?,
            )),
            (Topology::TaskHead, None) => {
                let socket = UdpSocket::bind(local_bind_address)
                    .await
                    .context("bind task-head UDP carrier")?;
                Ok(Some(
                    create_local_node(
                        ranks.start,
                        1,
                        false,
                        args.topology,
                        socket,
                        Some(&head_path),
                        Some(&root_parent),
                        database_dir.as_deref(),
                        &task,
                        Duration::from_secs(args.join_timeout_secs),
                    )
                    .await?,
                ))
            }
            (Topology::Flat, Some(socket)) => Ok(Some(
                create_local_node(
                    ranks.start,
                    0,
                    true,
                    args.topology,
                    socket,
                    None,
                    None,
                    database_dir.as_deref(),
                    &task,
                    Duration::from_secs(args.join_timeout_secs),
                )
                .await?,
            )),
            (Topology::Flat, None) => Ok(None),
        }
    }
    .await;
    let mut local_node = match local_node {
        Ok(local_node) => local_node,
        Err(error) => {
            if let Err(cleanup_error) = remove_runtime_dir(&runtime_dir).await {
                eprintln!("warning: cleanup after node startup failure: {cleanup_error:#}");
            }
            return Err(error);
        }
    };
    let worker_specs = match args.topology {
        Topology::TaskHead => {
            let head = local_node
                .as_ref()
                .expect("task-head topology always creates a local head");
            let parent = ParentTarget::Pinned {
                pid: head.node.pid(),
                address: CarrierAddr::Unix(head_path.clone()),
            };
            let level = if head.is_root { 1 } else { 2 };
            make_worker_specs(
                ranks.start + 1..ranks.end,
                level,
                &runtime_dir,
                |rank| CarrierAddr::Unix(runtime_dir.join(format!("node-{rank}.sock"))),
                parent,
            )
        }
        Topology::Flat => {
            let start = ranks.start + usize::from(local_node.is_some());
            make_worker_specs(
                start..ranks.end,
                1,
                &runtime_dir,
                |_| CarrierAddr::Udp(local_bind_address),
                root_parent,
            )
        }
    };
    let mut workers = match spawn_workers(
        worker_specs,
        &args,
        database_dir.as_deref(),
        Duration::from_secs(args.join_timeout_secs),
    )
    .await
    {
        Ok(workers) => workers,
        Err(error) => {
            if let Some(local) = local_node.as_ref() {
                shutdown_node(local).await;
            }
            drop(local_node);
            if let Err(cleanup_error) = remove_runtime_dir(&runtime_dir).await {
                eprintln!("warning: cleanup after worker startup failure: {cleanup_error:#}");
            }
            return Err(error);
        }
    };
    println!(
        "[task {task_id}/{task_count}] started {} local nodes",
        workers.len() + usize::from(local_node.is_some())
    );

    let result = match local_node.as_ref() {
        Some(local) => {
            tokio::try_join!(run_local_node(local, &args, started), workers.join(),).map(|_| ())
        }
        None => workers.join().await,
    };
    let worker_shutdown = workers.shutdown().await;
    if let Some(local) = local_node.as_ref() {
        shutdown_node(local).await;
    }
    drop(local_node.take());
    let cleanup = remove_runtime_dir(&runtime_dir).await;
    result.and(worker_shutdown).and(cleanup)
}

async fn create_local_node(
    rank: usize,
    level: u8,
    is_root: bool,
    topology: Topology,
    socket: UdpSocket,
    unix_path: Option<&Path>,
    parent: Option<&ParentTarget>,
    database_dir: Option<&Path>,
    task: &TaskMetadata,
    join_timeout: Duration,
) -> Result<LocalNode> {
    let udp_address = socket.address();
    let identity = issue_scale_identity(rank).await?;
    let quic_config = scale_quic_config()?;
    let transport = match unix_path {
        Some(path) => {
            let fallback: Arc<dyn DatagramSocket> = Arc::new(
                UnixDatagramSocket::bind(path)
                    .with_context(|| format!("bind task-head Unix carrier {}", path.display()))?,
            );
            TransportConfig::routed_udp(socket.into_std()?, Some(fallback), identity)?
        }
        None => TransportConfig::direct_udp(socket.into_std()?, identity)?,
    }
    .with_quic_config(quic_config);
    let mut config = NodeConfig::new(transport);
    let role = if is_root {
        NodeRole::Root
    } else {
        NodeRole::Head
    };
    config = config.with_labels(scale_labels(rank, level, role, task, topology));
    if let Some(parent) = parent {
        config = config.with_parent(parent.config()?);
    }
    let (config, store) = configure_store(rank, config, database_dir).await?;
    let node =
        Arc::new(Node::create(config).with_context(|| format!("create Chrysalis node {rank}"))?);
    let parent_pid = if is_root {
        None
    } else {
        match wait_for_parent(&node, join_timeout).await {
            Ok(parent) => Some(parent),
            Err(error) => {
                node.shutdown();
                node.join().await;
                return Err(error).with_context(|| format!("join parent for node {rank}"));
            }
        }
    };
    if let Some(store) = &store {
        store
            .register_node(NodeRecord {
                rank,
                pid: node.pid(),
                task_id: task.id,
                task_handle: task.handle.clone(),
                hostname: task.hostname.clone(),
                address: format!("udp://{udp_address}"),
                is_root,
                parent_pid,
                level,
                expected_nodes: task.expected_nodes,
                nodes_per_task: task.nodes_per_task,
                started_at_ms: unix_millis()?,
            })
            .await?;
    }
    if is_root {
        let join_token = root_join_token(node.pid(), udp_address);
        println!("{join_token}");
        std::io::stdout().flush().context("flush root join token")?;
    } else {
        println!(
            "[node {rank} task head] joined {} via udp://{udp_address}",
            format_pid(parent_pid.expect("non-root node has a parent"))
        );
    }
    Ok(LocalNode {
        rank,
        is_root,
        node,
        store,
    })
}

fn make_worker_specs<F>(
    ranks: Range<usize>,
    level: u8,
    runtime_dir: &Path,
    mut bind: F,
    parent: ParentTarget,
) -> Vec<WorkerSpec>
where
    F: FnMut(usize) -> CarrierAddr,
{
    ranks
        .map(|rank| WorkerSpec {
            rank,
            level,
            bind: bind(rank),
            parent: parent.clone(),
            ready_file: runtime_dir.join(format!("ready-{rank}")),
        })
        .collect()
}

pub(crate) async fn worker(args: WorkerArgs) -> Result<()> {
    let task_id = task_rank()?;
    let hostname = hostname::get()
        .context("read local hostname")?
        .into_string()
        .map_err(|_| anyhow::anyhow!("local hostname is not UTF-8"))?;
    let task = TaskMetadata {
        id: task_id,
        handle: mast_task_handle(task_id),
        hostname,
        expected_nodes: args.run.nodes,
        nodes_per_task: args.run.nodes_per_task,
    };
    let (binding, advertised_address, unix_path) = bind_worker_socket(&args.bind).await?;
    let identity = issue_scale_identity(args.rank).await?;
    let transport = match binding {
        WorkerBinding::Carrier(socket) => TransportConfig::new(socket, identity),
        WorkerBinding::DirectUdp(socket) => TransportConfig::direct_udp(socket, identity)?,
    }
    .with_quic_config(scale_quic_config()?);
    let config = NodeConfig::new(transport)
        .with_labels(scale_labels(
            args.rank,
            args.level,
            NodeRole::Leaf,
            &task,
            args.run.topology,
        ))
        .with_parent(args.parent.config()?);
    let (config, store) = configure_store(args.rank, config, args.database_dir.as_deref()).await?;
    let node = Arc::new(
        Node::create(config).with_context(|| format!("create Chrysalis node {}", args.rank))?,
    );
    let parent_pid = match wait_for_parent(&node, Duration::from_secs(args.run.join_timeout_secs))
        .await
    {
        Ok(parent) => parent,
        Err(error) => {
            node.shutdown();
            node.join().await;
            if let Err(cleanup_error) = remove_socket(unix_path.as_deref()).await {
                eprintln!(
                    "warning: failed to clean up node {} socket after parent join failure: {cleanup_error:#}",
                    args.rank
                );
            }
            return Err(error).with_context(|| format!("join parent for node {}", args.rank));
        }
    };
    let setup = async {
        if let Some(store) = &store {
            store
                .register_node(NodeRecord {
                    rank: args.rank,
                    pid: node.pid(),
                    task_id: task.id,
                    task_handle: task.handle.clone(),
                    hostname: task.hostname.clone(),
                    address: advertised_address.to_string(),
                    is_root: false,
                    parent_pid: Some(parent_pid),
                    level: args.level,
                    expected_nodes: task.expected_nodes,
                    nodes_per_task: task.nodes_per_task,
                    started_at_ms: unix_millis()?,
                })
                .await?;
        }
        tokio::fs::write(&args.ready_file, format_pid(node.pid()))
            .await
            .with_context(|| format!("publish worker readiness {}", args.ready_file.display()))?;
        Result::<()>::Ok(())
    }
    .await;
    if let Err(error) = setup {
        node.shutdown();
        node.join().await;
        if let Err(cleanup_error) = remove_socket(unix_path.as_deref()).await {
            eprintln!(
                "warning: failed to clean up node {} socket after startup failure: {cleanup_error:#}",
                args.rank
            );
        }
        return Err(error);
    }
    println!(
        "[node {} worker process {}] joined {} via {}",
        args.rank,
        std::process::id(),
        format_pid(parent_pid),
        advertised_address,
    );
    let local = LocalNode {
        rank: args.rank,
        is_root: false,
        node,
        store,
    };
    let result = run_local_node(&local, &args.run, Instant::now()).await;
    shutdown_node(&local).await;
    drop(local);
    let cleanup = remove_socket(unix_path.as_deref()).await;
    match (result, cleanup) {
        (Err(error), Err(cleanup_error)) => {
            eprintln!(
                "warning: failed to clean up node {} socket after benchmark failure: {cleanup_error:#}",
                args.rank
            );
            Err(error)
        }
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

async fn issue_scale_identity(rank: usize) -> Result<chrysalis::QuicIdentity> {
    chrysalis_identity_meta::issue()
        .await
        .with_context(|| format!("issue Meta identity for node {rank}"))
}

fn scale_labels(
    rank: usize,
    level: u8,
    role: NodeRole,
    task: &TaskMetadata,
    topology: Topology,
) -> Labels {
    Labels::try_from_iter([
        ("client".to_owned(), "chrysalis-scale".to_owned()),
        ("rank".to_owned(), rank.to_string()),
        ("task".to_owned(), task.id.to_string()),
        ("role".to_owned(), role.as_str().to_owned()),
        ("level".to_owned(), level.to_string()),
        ("topology".to_owned(), topology.as_str().to_owned()),
    ])
    .expect("generated scale labels are valid")
}

pub(crate) fn scale_quic_config() -> Result<chrysalis::QuicConfig> {
    scale_quic_config_with_limits(None, None, false, None)
}

pub(crate) fn scale_quic_config_with_limits(
    send_window: Option<u64>,
    max_udp_payload: Option<u16>,
    disable_pacing: bool,
    max_transmit_batch_segments: Option<usize>,
) -> Result<chrysalis::QuicConfig> {
    let max_udp_payload = max_udp_payload.unwrap_or(META_NETWORK_MAX_UDP_PAYLOAD);
    let max_transmit_batch_segments = max_transmit_batch_segments.unwrap_or(SCALE_GSO_SEGMENTS);
    let mut config = chrysalis::QuicConfig::default()
        .with_pacing(!disable_pacing)
        .with_flow_window(send_window.unwrap_or(SCALE_FLOW_WINDOW))
        .with_max_transmit_batch_segments(
            NonZeroUsize::new(max_transmit_batch_segments)
                .context("maximum transmit segments must be nonzero")?,
        )
        .try_with_max_udp_payload_size(max_udp_payload)?;
    if disable_pacing {
        config = config.with_initial_congestion_window_packets(u32::MAX as usize);
    }
    Ok(config)
}

enum WorkerBinding {
    Carrier(Arc<DatagramSocketSet>),
    DirectUdp(std::net::UdpSocket),
}

async fn bind_worker_socket(
    address: &CarrierAddr,
) -> Result<(WorkerBinding, CarrierAddr, Option<PathBuf>)> {
    match address {
        CarrierAddr::Udp(address) => {
            let socket = UdpSocket::bind(*address)
                .await
                .with_context(|| format!("bind worker UDP carrier {address}"))?;
            let advertised = CarrierAddr::Udp(socket.address());
            Ok((
                WorkerBinding::DirectUdp(socket.into_std()?),
                advertised,
                None,
            ))
        }
        CarrierAddr::Unix(path) => {
            let socket: Arc<dyn DatagramSocket> = Arc::new(
                UnixDatagramSocket::bind(path)
                    .with_context(|| format!("bind worker Unix carrier {}", path.display()))?,
            );
            let sockets = Arc::new(
                DatagramSocketSet::new(socket, Vec::new()).context("create worker socket set")?,
            );
            Ok((
                WorkerBinding::Carrier(sockets),
                CarrierAddr::Unix(path.clone()),
                Some(path.clone()),
            ))
        }
    }
}

async fn spawn_workers(
    specs: Vec<WorkerSpec>,
    args: &RunArgs,
    database_dir: Option<&Path>,
    startup_timeout: Duration,
) -> Result<WorkerGroup> {
    let total = specs.len();
    let mut specs = specs.into_iter();
    let mut launched = 0;
    let mut starting = Vec::new();
    let mut group = WorkerGroup::default();
    let mut startup_error = None;

    'startup: while group.len() < total {
        while starting.len() < args.identity_concurrency && launched < total {
            let spec = specs.next().expect("worker specification count is exact");
            match launch_worker(spec, args, database_dir).await {
                Ok(process) => {
                    starting.push(process);
                    launched += 1;
                }
                Err(error) => {
                    startup_error = Some(error);
                    break 'startup;
                }
            }
        }

        let mut index = 0;
        while index < starting.len() {
            let process = &mut starting[index];
            match tokio::fs::try_exists(&process.ready_file).await {
                Ok(true) => {
                    let process = starting.swap_remove(index);
                    if let Err(error) = remove_file(&process.ready_file).await {
                        startup_error = Some(error);
                        group.processes.push(process);
                        break 'startup;
                    }
                    println!(
                        "[task] worker node {} started as process {}",
                        process.rank,
                        process.child.id().unwrap_or_default()
                    );
                    group.processes.push(process);
                    continue;
                }
                Ok(false) => {}
                Err(error) => {
                    startup_error = Some(anyhow::Error::new(error).context(format!(
                        "inspect worker readiness {}",
                        process.ready_file.display()
                    )));
                    break 'startup;
                }
            }
            match process
                .child
                .try_wait()
                .with_context(|| format!("inspect worker node {}", process.rank))?
            {
                Some(status) => {
                    startup_error = Some(anyhow::anyhow!(
                        "worker node {} exited before joining: {status}",
                        process.rank
                    ));
                    break 'startup;
                }
                None if process.started.elapsed() >= startup_timeout => {
                    startup_error = Some(anyhow::anyhow!(
                        "worker node {} timed out joining its parent",
                        process.rank
                    ));
                    break 'startup;
                }
                None => {}
            }
            index += 1;
        }
        if group.len() < total {
            tokio::time::sleep(WORKER_POLL_INTERVAL).await;
        }
    }

    if let Some(error) = startup_error {
        group.processes.extend(starting);
        if let Err(cleanup_error) = group.shutdown().await {
            eprintln!(
                "warning: failed to shut down workers after startup failure: {cleanup_error:#}"
            );
        }
        return Err(error);
    }
    Ok(group)
}

async fn launch_worker(
    spec: WorkerSpec,
    args: &RunArgs,
    database_dir: Option<&Path>,
) -> Result<WorkerProcess> {
    remove_file(&spec.ready_file).await?;
    let executable = std::env::current_exe().context("locate scale benchmark executable")?;
    let mut command = Command::new(executable);
    command
        .arg("worker")
        .arg("--nodes")
        .arg(args.nodes.to_string())
        .arg("--nodes-per-task")
        .arg(args.nodes_per_task.to_string())
        .arg("--topology")
        .arg(args.topology.as_str())
        .arg("--identity-concurrency")
        .arg(args.identity_concurrency.to_string())
        .arg("--port")
        .arg(args.port.to_string())
        .arg("--concurrency")
        .arg(args.concurrency.to_string())
        .arg("--join-timeout-secs")
        .arg(args.join_timeout_secs.to_string())
        .arg("--echo-timeout-secs")
        .arg(args.echo_timeout_secs.to_string())
        .arg("--rank")
        .arg(spec.rank.to_string())
        .arg("--level")
        .arg(spec.level.to_string())
        .arg("--bind")
        .arg(spec.bind.to_string())
        .arg("--parent")
        .arg(spec.parent.to_string())
        .arg("--ready-file")
        .arg(&spec.ready_file)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    if let Some(database_dir) = database_dir {
        command.arg("--database-dir").arg(database_dir);
    }
    let child = command
        .spawn()
        .with_context(|| format!("spawn worker process for node {}", spec.rank))?;
    Ok(WorkerProcess {
        rank: spec.rank,
        child,
        ready_file: spec.ready_file,
        started: Instant::now(),
        status: None,
    })
}

impl WorkerGroup {
    fn len(&self) -> usize {
        self.processes.len()
    }

    async fn join(&mut self) -> Result<()> {
        loop {
            let mut running = 0;
            for process in &mut self.processes {
                if process.status.is_some() {
                    continue;
                }
                match process
                    .child
                    .try_wait()
                    .with_context(|| format!("inspect worker node {}", process.rank))?
                {
                    Some(status) => {
                        process.status = Some(status);
                        anyhow::ensure!(
                            status.success(),
                            "worker node {} exited with {status}",
                            process.rank
                        );
                    }
                    None => running += 1,
                }
            }
            if running == 0 {
                return Ok(());
            }
            tokio::time::sleep(WORKER_POLL_INTERVAL).await;
        }
    }

    async fn shutdown(&mut self) -> Result<()> {
        let mut first_error = None;
        for process in &mut self.processes {
            if process.status.is_some() {
                continue;
            }
            match process.child.try_wait() {
                Ok(Some(status)) => process.status = Some(status),
                Ok(None) => {
                    if let Err(error) = process.child.start_kill()
                        && first_error.is_none()
                    {
                        first_error = Some(
                            anyhow::Error::new(error)
                                .context(format!("terminate worker node {}", process.rank)),
                        );
                    }
                }
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(
                            anyhow::Error::new(error)
                                .context(format!("inspect worker node {}", process.rank)),
                        );
                    }
                    let _ = process.child.start_kill();
                }
            }
        }
        for process in &mut self.processes {
            if process.status.is_none() {
                match process.child.wait().await {
                    Ok(status) => process.status = Some(status),
                    Err(error) if first_error.is_none() => {
                        first_error = Some(
                            anyhow::Error::new(error)
                                .context(format!("join worker node {}", process.rank)),
                        );
                    }
                    Err(_) => {}
                }
            }
            if let Err(error) = remove_file(&process.ready_file).await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

async fn run_local_node(local: &LocalNode, args: &RunArgs, started: Instant) -> Result<()> {
    match &local.store {
        Some(store) => {
            run_persistent_node(
                local.node.clone(),
                store.clone(),
                args,
                local.rank,
                local.is_root,
                started,
            )
            .await
        }
        None if local.is_root => run_root(local.node.clone(), args, started).await,
        None => run_child(local.node.clone(), args, local.rank).await,
    }
}

async fn shutdown_node(local: &LocalNode) {
    local.node.shutdown();
    local.node.join().await;
}

async fn remove_runtime_dir(path: &Path) -> Result<()> {
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("remove runtime directory {}", path.display()))
        }
    }
}

async fn remove_socket(path: Option<&Path>) -> Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    remove_file(path).await
}

async fn remove_file(path: &Path) -> Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove {}", path.display())),
    }
}

async fn wait_for_parent(node: &Node, timeout: Duration) -> Result<Pid> {
    let mut parent = node
        .subscribe_parent()
        .expect("child node is configured with a parent");
    tokio::time::timeout(timeout, async {
        loop {
            match parent.borrow().clone() {
                ParentManagerStatus::Connected { peer, .. } => return Ok(peer),
                ParentManagerStatus::Connecting => {}
                ParentManagerStatus::Failed { error } => return Err(error.into()),
                ParentManagerStatus::Stopped => anyhow::bail!("parent manager stopped"),
            }
            parent.changed().await.context("parent manager stopped")?;
        }
    })
    .await
    .context("timed out joining parent")?
}

async fn configure_store(
    rank: usize,
    config: NodeConfig,
    database_dir: Option<&Path>,
) -> Result<(NodeConfig, Option<ExperimentStore>)> {
    let Some(database_dir) = database_dir else {
        return Ok((config, None));
    };
    let path = database_dir.join(format!("node-v{PERSISTENCE_SCHEMA_VERSION}-{rank}.db"));
    let store = ExperimentStore::open(&path)
        .await
        .with_context(|| format!("initialize experiment database for node {rank}"))?;
    let config = store
        .configure(config)
        .await
        .with_context(|| format!("configure SQLite replication for node {rank}"))?;
    Ok((config, Some(store)))
}

async fn run_root(node: Arc<Node>, args: &RunArgs, started: Instant) -> Result<()> {
    println!("[root] waiting for {} namespace entries", args.nodes);
    let entries = wait_for_entries(
        &node,
        args.nodes,
        Duration::from_secs(args.join_timeout_secs),
    )
    .await?;
    let join_elapsed = started.elapsed();
    let children: Vec<_> = entries
        .into_iter()
        .filter_map(|entry| (entry.pid != node.pid()).then_some(entry.pid))
        .collect();
    anyhow::ensure!(
        children.len() == args.nodes - 1,
        "expected {} children, found {}",
        args.nodes - 1,
        children.len()
    );
    println!(
        "[root] all {} children joined in {:.3}s; starting echoes with concurrency {}",
        children.len(),
        join_elapsed.as_secs_f64(),
        args.concurrency
    );
    let echo_started = Instant::now();
    let latencies = run_all(node, children, args.concurrency, 1, ExperimentKind::Echo).await?;
    let echo_elapsed = echo_started.elapsed();
    let max_latency = latencies.iter().copied().max().unwrap_or_default();
    let mean_latency =
        latencies.iter().map(Duration::as_secs_f64).sum::<f64>() / latencies.len() as f64;
    let echoes_per_second = if echo_elapsed.is_zero() {
        0.0
    } else {
        latencies.len() as f64 / echo_elapsed.as_secs_f64()
    };
    let result = json!({
        "event": "chrysalis_scale_result",
        "nodes": args.nodes,
        "tasks": task_count(args.nodes, args.nodes_per_task),
        "nodes_per_task": args.nodes_per_task,
        "topology": args.topology.as_str(),
        "identity_concurrency": args.identity_concurrency,
        "children": latencies.len(),
        "join_seconds": join_elapsed.as_secs_f64(),
        "echo_seconds": echo_elapsed.as_secs_f64(),
        "echoes_per_second": echoes_per_second,
        "mean_echo_millis": mean_latency * 1000.0,
        "max_echo_millis": max_latency.as_secs_f64() * 1000.0,
        "concurrency": args.concurrency,
    });
    println!("[root] RESULT {result}");
    Ok(())
}

async fn run_child(node: Arc<Node>, args: &RunArgs, rank: usize) -> Result<()> {
    let incoming = tokio::time::timeout(Duration::from_secs(args.echo_timeout_secs), node.accept())
        .await
        .with_context(|| format!("node {rank} timed out waiting for echo stream"))??;
    relay_incoming(incoming, rank).await
}

async fn run_persistent_node(
    node: Arc<Node>,
    store: ExperimentStore,
    args: &RunArgs,
    rank: usize,
    is_root: bool,
    started: Instant,
) -> Result<()> {
    let benchmark_server = run_persistent_benchmark_server(node.clone(), rank);
    let experiment_runner = async {
        if is_root {
            println!("[root] waiting for {} registered scale nodes", args.nodes);
        }
        let nodes = wait_for_nodes(
            &store,
            args.nodes,
            Duration::from_secs(args.join_timeout_secs),
            is_root,
        )
        .await?;
        let targets: Vec<_> = nodes.into_iter().filter(|pid| *pid != node.pid()).collect();
        anyhow::ensure!(
            targets.len() == args.nodes - 1,
            "expected {} peer nodes, found {}",
            args.nodes - 1,
            targets.len()
        );
        if is_root {
            println!(
                "[root] persistent experiment mesh ready with {} nodes after {:.3}s",
                args.nodes,
                started.elapsed().as_secs_f64()
            );
        }
        run_experiment_loop(node, store, args, rank).await
    };
    tokio::try_join!(benchmark_server, experiment_runner)?;
    Ok(())
}

async fn run_experiment_loop(
    node: Arc<Node>,
    store: ExperimentStore,
    args: &RunArgs,
    rank: usize,
) -> Result<()> {
    loop {
        let timeout = Duration::from_secs(args.echo_timeout_secs);
        let lease = timeout
            .saturating_mul(2)
            .saturating_add(EXPERIMENT_LEASE_GRACE);
        let Some(claim) = store
            .claim_experiment(node.pid(), unix_millis()?, lease)
            .await?
        else {
            tokio::time::sleep(EXPERIMENT_POLL_INTERVAL).await;
            continue;
        };
        let experiment = claim.experiment;
        println!(
            "[node {rank}] starting {} {} experiment {:?}: count={} size={}",
            experiment.targets.selection(),
            experiment.kind.as_str(),
            experiment.name,
            experiment.targets.count(),
            experiment.size
        );
        let targets: Vec<_> = store
            .nodes_for_run(args.nodes)
            .await?
            .into_iter()
            .filter(|pid| *pid != node.pid())
            .collect();
        let result = execute_experiment(
            node.clone(),
            &targets,
            args.concurrency,
            timeout,
            experiment,
            claim.attempt,
        )
        .await?;
        let status = if result.error.is_some() {
            "failed"
        } else {
            "complete"
        };
        let payload_bytes = result
            .completed
            .saturating_mul(usize::try_from(result.experiment.size).unwrap_or_default())
            .saturating_mul(result.experiment.kind.payload_multiplier());
        let payload_mib_per_second = if result.elapsed.is_zero() {
            0.0
        } else {
            payload_bytes as f64 / (1024.0 * 1024.0) / result.elapsed.as_secs_f64()
        };
        let explicit_targets = match &result.experiment.targets {
            ExperimentTargets::Count(_) => None,
            ExperimentTargets::Explicit(targets) => {
                Some(targets.iter().copied().map(format_pid).collect::<Vec<_>>())
            }
        };
        let output = json!({
            "event": "chrysalis_persist_result",
            "pid": format_pid(result.experiment.pid),
            "experiment": result.experiment.name.as_str(),
            "kind": result.experiment.kind.as_str(),
            "selection": result.experiment.targets.selection(),
            "targets": explicit_targets,
            "status": status,
            "count": result.experiment.targets.count(),
            "size": result.experiment.size,
            "completed": result.completed,
            "warmup_seconds": result.warmup_elapsed.as_secs_f64(),
            "operation_seconds": result.elapsed.as_secs_f64(),
            "operations_per_second": if result.elapsed.is_zero() {
                0.0
            } else {
                result.completed as f64 / result.elapsed.as_secs_f64()
            },
            "payload_bytes": payload_bytes,
            "payload_mib_per_second": payload_mib_per_second,
            "transmit_calls": result.io_stats.transmit_calls,
            "transmit_datagrams": result.io_stats.transmit_datagrams,
            "transmit_bytes": result.io_stats.transmit_bytes,
            "transmit_blocked": result.io_stats.transmit_blocked,
            "receive_calls": result.io_stats.receive_calls,
            "receive_datagrams": result.io_stats.receive_datagrams,
            "receive_bytes": result.io_stats.receive_bytes,
            "connection_rtt_micros": result.connection_stats.rtt.as_micros(),
            "connection_congestion_window": result.connection_stats.congestion_window,
            "connection_congestion_events": result.connection_stats.congestion_events,
            "connection_lost_packets": result.connection_stats.lost_packets,
            "connection_lost_bytes": result.connection_stats.lost_bytes,
            "connection_sent_packets": result.connection_stats.sent_packets,
            "connection_mtu": result.connection_stats.current_mtu,
            "mean_operation_millis": result.mean_latency.as_secs_f64() * 1000.0,
            "max_operation_millis": result.max_latency.as_secs_f64() * 1000.0,
            "error": result.error.as_deref(),
        });
        println!("[node {rank}] RESULT {output}");
        store.record_result(result).await?;
    }
}

async fn wait_for_nodes(
    store: &ExperimentStore,
    expected: usize,
    timeout: Duration,
    report_progress: bool,
) -> Result<Vec<Pid>> {
    let deadline = Instant::now() + timeout;
    let progress_interval = (expected / 100).max(1);
    let mut last_reported: usize = 0;
    loop {
        let node_count = store.node_count_for_run(expected).await?;
        if report_progress
            && (node_count == expected
                || node_count >= last_reported.saturating_add(progress_interval))
        {
            last_reported = node_count;
            println!("[root] registered {last_reported}/{expected} scale nodes");
        }
        if node_count == expected {
            return store.nodes_for_run(expected).await;
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "timed out waiting for {expected} scale nodes"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn execute_experiment(
    node: Arc<Node>,
    targets: &[Pid],
    concurrency: usize,
    timeout: Duration,
    experiment: Experiment,
    claim_attempt: i64,
) -> Result<ExperimentResult> {
    let mut started_at_ms = unix_millis()?;
    let mut warmup_elapsed = Duration::ZERO;
    let mut elapsed = Duration::ZERO;
    let mut io_stats = Default::default();
    let mut connection_stats = Default::default();
    let mut latencies = Vec::new();
    let execution = async {
        let selected = match &experiment.targets {
            ExperimentTargets::Count(count) => {
                let count =
                    usize::try_from(*count).context("experiment count must be nonnegative")?;
                anyhow::ensure!(count > 0, "experiment count must be positive");
                anyhow::ensure!(
                    count <= targets.len(),
                    "experiment requests {count} nodes, but only {} peers are available",
                    targets.len()
                );
                targets[..count].to_vec()
            }
            ExperimentTargets::Explicit(selected) => {
                anyhow::ensure!(!selected.is_empty(), "targeted experiment has no targets");
                let peers = targets.iter().copied().collect::<BTreeSet<_>>();
                let mut unique = BTreeSet::new();
                for target in selected {
                    anyhow::ensure!(
                        *target != experiment.pid,
                        "targeted experiment includes its source PID"
                    );
                    anyhow::ensure!(peers.contains(target), "target PID is not a live peer");
                    anyhow::ensure!(unique.insert(*target), "target PID is repeated");
                }
                selected.clone()
            }
        };
        let count = selected.len();
        let size =
            usize::try_from(experiment.size).context("experiment size must be nonnegative")?;
        if experiment.kind == ExperimentKind::Echo {
            u32::try_from(size).context("echo size exceeds u32")?;
        }

        let warmup_started = Instant::now();
        let mut warmup_latencies = Vec::new();
        let warmup = tokio::time::timeout(
            timeout,
            run_all_into(
                node.clone(),
                selected.clone(),
                concurrency.min(count),
                1,
                experiment.kind,
                &mut warmup_latencies,
            ),
        )
        .await;
        warmup_elapsed = warmup_started.elapsed();
        warmup
            .context("experiment warm-up timed out")?
            .context("experiment warm-up failed")?;

        started_at_ms = unix_millis()?;
        let connection_before = if selected.len() == 1 {
            let target = selected[0];
            let stats = node
                .transport()
                .connection_stats(target)
                .context("measured connection disappeared after warm-up")?;
            Some((target, stats))
        } else {
            None
        };
        let io_before = node.transport().io_stats();
        let cpu_before = ProcessCpuTime::now();
        let started = Instant::now();
        let measured = tokio::time::timeout(
            timeout,
            run_all_into(
                node.clone(),
                selected,
                concurrency.min(count),
                size,
                experiment.kind,
                &mut latencies,
            ),
        )
        .await;
        elapsed = started.elapsed();
        let cpu = ProcessCpuTime::now().since(cpu_before);
        println!(
            "[experiment-profile] {}",
            json!({
                "experiment": experiment.name.as_str(),
                "pid": format_pid(experiment.pid),
                "kind": experiment.kind.as_str(),
                "elapsed_seconds": elapsed.as_secs_f64(),
                "user_cpu_seconds": cpu.user.as_secs_f64(),
                "system_cpu_seconds": cpu.system.as_secs_f64(),
                "cpu_cores": cpu.utilization(elapsed),
            })
        );
        io_stats = node.transport().io_stats().since(io_before);
        if let Some((target, before)) = connection_before {
            connection_stats = node
                .transport()
                .connection_stats(target)
                .map(|after| after.since(before))
                .unwrap_or_else(|| before.since(before));
        }
        measured.context("experiment operation sweep timed out")??;
        Result::<()>::Ok(())
    }
    .await;
    let finished_at_ms = unix_millis()?;
    let completed = latencies.len();
    let mean_latency = if latencies.is_empty() {
        Duration::ZERO
    } else {
        Duration::from_secs_f64(
            latencies.iter().map(Duration::as_secs_f64).sum::<f64>() / latencies.len() as f64,
        )
    };
    let max_latency = latencies.iter().copied().max().unwrap_or_default();
    let error = execution.err().map(|error| format!("{error:#}"));
    Ok(ExperimentResult {
        experiment,
        claim_attempt,
        completed,
        started_at_ms,
        finished_at_ms,
        warmup_elapsed,
        elapsed,
        mean_latency,
        max_latency,
        io_stats,
        connection_stats,
        error,
    })
}

async fn run_persistent_benchmark_server(node: Arc<Node>, rank: usize) -> Result<()> {
    let mut relays = JoinSet::new();
    loop {
        tokio::select! {
            incoming = node.accept() => {
                let incoming = incoming
                    .with_context(|| format!("node {rank} failed to accept benchmark stream"))?;
                relays.spawn(async move { relay_incoming(incoming, rank).await });
            }
            completed = relays.join_next(), if !relays.is_empty() => {
                completed.expect("relay set is not empty")
                    .context("benchmark relay task failed")??;
            }
        }
    }
}

async fn relay_incoming(incoming: chrysalis::IncomingStream, rank: usize) -> Result<()> {
    let (_, stream) = incoming.into_parts();
    let (mut send, mut recv) = stream.into_parts();
    let mut operation = [0];
    recv.read_exact(&mut operation)
        .await
        .with_context(|| format!("node {rank} failed to read benchmark operation"))?;
    match operation[0] {
        ECHO_OPERATION => relay_echo(&mut recv, &mut send)
            .await
            .with_context(|| format!("node {rank} failed to relay echo"))?,
        DELIVERY_OPERATION => {
            let cpu_before = ProcessCpuTime::now();
            let started = Instant::now();
            let received = receive_delivery(recv, &mut send)
                .await
                .with_context(|| format!("node {rank} failed to receive delivery"))?;
            let elapsed = started.elapsed();
            let cpu = ProcessCpuTime::now().since(cpu_before);
            println!(
                "[node {rank}] delivery-profile {}",
                json!({
                    "received": received,
                    "elapsed_seconds": elapsed.as_secs_f64(),
                    "user_cpu_seconds": cpu.user.as_secs_f64(),
                    "system_cpu_seconds": cpu.system.as_secs_f64(),
                    "cpu_cores": cpu.utilization(elapsed),
                })
            );
            received
        }
        first => relay_raw(first, &mut recv, &mut send)
            .await
            .with_context(|| format!("node {rank} failed to relay raw stream"))?,
    };
    send.finish().await.context("finish benchmark response")?;
    Ok(())
}

fn unix_millis() -> Result<i64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock precedes Unix epoch")?
        .as_millis();
    i64::try_from(millis).context("Unix timestamp exceeds i64")
}

async fn wait_for_entries(
    node: &Node,
    expected: usize,
    timeout: Duration,
) -> Result<Vec<ProcEntry>> {
    let deadline = Instant::now() + timeout;
    let progress_interval = (expected / 100).max(1);
    let mut last_reported: usize = 0;
    loop {
        let entries = node
            .enumerate(ResolveConsistency::Cached)
            .await
            .context("enumerate namespace while waiting for entries")?;
        if entries.len() == expected
            || entries.len() >= last_reported.saturating_add(progress_interval)
        {
            last_reported = entries.len();
            println!("[root] joined {last_reported}/{expected}");
        }
        if entries.len() == expected {
            return Ok(entries);
        }
        anyhow::ensure!(
            entries.len() < expected,
            "namespace contains {} entries, expected exactly {expected}",
            entries.len()
        );
        anyhow::ensure!(
            Instant::now() < deadline,
            "timed out waiting for {expected} entries"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn run_all(
    node: Arc<Node>,
    children: Vec<Pid>,
    concurrency: usize,
    size: usize,
    kind: ExperimentKind,
) -> Result<Vec<Duration>> {
    let mut latencies = Vec::with_capacity(children.len());
    run_all_into(node, children, concurrency, size, kind, &mut latencies).await?;
    Ok(latencies)
}

async fn run_all_into(
    node: Arc<Node>,
    children: Vec<Pid>,
    concurrency: usize,
    size: usize,
    kind: ExperimentKind,
    latencies: &mut Vec<Duration>,
) -> Result<()> {
    let total = children.len();
    let mut pending = children.into_iter();
    let mut tasks = JoinSet::new();
    for pid in pending.by_ref().take(concurrency) {
        spawn_operation(&mut tasks, node.clone(), pid, size, kind);
    }
    latencies.reserve(total);
    let progress_interval = (total / 10).max(1);
    while let Some(result) = tasks.join_next().await {
        let latency = result.context("benchmark task failed")??;
        latencies.push(latency);
        if latencies.len().is_multiple_of(progress_interval) || latencies.len() == total {
            println!(
                "[runner] completed {} {}/{}",
                kind.as_str(),
                latencies.len(),
                total
            );
        }
        if let Some(pid) = pending.next() {
            spawn_operation(&mut tasks, node.clone(), pid, size, kind);
        }
    }
    Ok(())
}

fn spawn_operation(
    tasks: &mut JoinSet<Result<Duration>>,
    node: Arc<Node>,
    pid: Pid,
    size: usize,
    kind: ExperimentKind,
) {
    tasks.spawn(async move {
        let started = Instant::now();
        let stream = node
            .dial(pid, ResolveConsistency::Cached)
            .await
            .with_context(|| format!("dial benchmark target {pid:?}"))?;
        let (mut send, mut recv) = stream.into_parts();
        match kind {
            ExperimentKind::Echo => {
                let send_request = async {
                    send.write_all(&[ECHO_OPERATION]).await?;
                    write_echo(&mut send, size).await?;
                    send.finish().await.context("finish echo request")?;
                    Result::<()>::Ok(())
                };
                let receive_response = read_echo(&mut recv, size);
                tokio::try_join!(send_request, receive_response)?;
            }
            ExperimentKind::Delivery => {
                send.write_all(&[DELIVERY_OPERATION]).await?;
                let send = send_payload(send, size).await?;
                send.finish().await.context("finish delivery request")?;
                read_delivery_receipt(&mut recv, size).await?;
            }
        }
        let elapsed = started.elapsed();
        let mut trailing = [0];
        anyhow::ensure!(
            tokio::time::timeout(STREAM_FIN_TIMEOUT, recv.read(&mut trailing))
                .await
                .context("timed out waiting for benchmark response FIN")??
                == 0,
            "benchmark response contains trailing data"
        );
        Ok(elapsed)
    });
}

async fn send_payload(send: chrysalis::SendStream, size: usize) -> Result<chrysalis::SendStream> {
    let chunk = Bytes::from_static(&OWNED_PAYLOAD_CHUNK);
    let mut remaining = size;
    let mut sends = FuturesUnordered::new();
    while remaining > 0 {
        let length = remaining.min(chunk.len());
        sends.push(send.send(chunk.slice(..length)));
        remaining -= length;
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
    Ok(send)
}

async fn write_echo<W>(writer: &mut W, size: usize) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let size = u32::try_from(size).context("echo size exceeds u32")?;
    writer.write_all(&size.to_be_bytes()).await?;
    write_payload(writer, size as usize).await
}

async fn write_payload<W>(writer: &mut W, size: usize) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut remaining = size;
    while remaining > 0 {
        let length = remaining.min(PAYLOAD_CHUNK.len());
        writer.write_all(&PAYLOAD_CHUNK[..length]).await?;
        remaining -= length;
    }
    Ok(())
}

async fn read_echo<R>(reader: &mut R, expected_size: usize) -> Result<()>
where
    R: AsyncRead + Unpin,
{
    let size = read_echo_size(reader).await?;
    anyhow::ensure!(
        size == expected_size,
        "echo response has size {size}, expected {expected_size}"
    );
    let mut chunk = vec![0; PAYLOAD_CHUNK_LEN];
    let mut remaining = size;
    while remaining > 0 {
        let length = remaining.min(chunk.len());
        reader.read_exact(&mut chunk[..length]).await?;
        anyhow::ensure!(
            chunk[..length].iter().all(|byte| *byte == PAYLOAD_BYTE),
            "echo response payload is corrupt"
        );
        remaining -= length;
    }
    Ok(())
}

async fn relay_echo<R, W>(reader: &mut R, writer: &mut W) -> Result<usize>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let size = read_echo_size(reader).await?;
    let encoded_size = u32::try_from(size).expect("echo frame size originated as u32");
    writer.write_all(&encoded_size.to_be_bytes()).await?;
    let mut chunk = vec![0; PAYLOAD_CHUNK_LEN];
    let mut remaining = size;
    while remaining > 0 {
        let length = remaining.min(chunk.len());
        reader.read_exact(&mut chunk[..length]).await?;
        writer.write_all(&chunk[..length]).await?;
        remaining -= length;
    }
    Ok(size)
}

async fn relay_raw<R, W>(first: u8, reader: &mut R, writer: &mut W) -> Result<usize>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    writer.write_all(&[first]).await?;
    let copied = tokio::io::copy(reader, writer).await?;
    usize::try_from(copied)
        .context("raw stream byte count exceeds usize")?
        .checked_add(1)
        .context("raw stream byte count overflow")
}

async fn receive_delivery(
    reader: chrysalis::RecvStream,
    writer: &mut chrysalis::SendStream,
) -> Result<usize> {
    let options = ReceiveOptions::new(
        NonZeroUsize::new(RECEIVE_CHUNK_LEN).expect("receive chunk length is nonzero"),
    );
    let mut buffer = BytesMut::with_capacity(RECEIVE_CHUNK_LEN);
    let mut received = 0usize;
    loop {
        let completion = reader.receive(buffer, options).await?;
        let length = completion.data().len();
        received = received
            .checked_add(length)
            .context("delivery byte count overflow")?;
        let status = completion.status();
        buffer = completion.into_buffer();
        buffer.clear();
        match status {
            ReceiveStatus::Data => {}
            ReceiveStatus::Fin => break,
            ReceiveStatus::Reset(code) => anyhow::bail!("delivery stream reset: {code}"),
            ReceiveStatus::Closed => anyhow::bail!("delivery stream closed before FIN"),
            ReceiveStatus::Cancelled => anyhow::bail!("delivery receive was cancelled"),
            ReceiveStatus::Stopped(code) => {
                anyhow::bail!("delivery receive stopped locally: {code}")
            }
        }
    }
    let receipt = u64::try_from(received).context("delivery byte count exceeds u64")?;
    writer.write_all(&receipt.to_be_bytes()).await?;
    Ok(received)
}

async fn read_delivery_receipt<R>(reader: &mut R, expected_size: usize) -> Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut receipt = [0; size_of::<u64>()];
    reader.read_exact(&mut receipt).await?;
    let received = u64::from_be_bytes(receipt);
    anyhow::ensure!(
        received == u64::try_from(expected_size).context("delivery size exceeds u64")?,
        "delivery receipt reports {received} bytes, expected {expected_size}"
    );
    Ok(())
}

async fn read_echo_size<R>(reader: &mut R) -> Result<usize>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0; ECHO_HEADER_LEN];
    reader.read_exact(&mut header).await?;
    Ok(u32::from_be_bytes(header) as usize)
}

fn task_count(nodes: usize, nodes_per_task: usize) -> usize {
    nodes.div_ceil(nodes_per_task)
}

fn node_range(nodes: usize, nodes_per_task: usize, task_id: usize) -> Range<usize> {
    let start = task_id.saturating_mul(nodes_per_task);
    start..start.saturating_add(nodes_per_task).min(nodes)
}

fn root_join_token(pid: Pid, address: SocketAddr) -> String {
    format!("udp://{address}?authority={}", format_pid(pid))
}

fn format_pid(pid: Pid) -> String {
    let mut output = String::with_capacity(32);
    for byte in pid.as_bytes() {
        write!(&mut output, "{byte:02x}").expect("writing to a string cannot fail");
    }
    output
}

pub(crate) fn parse_pid(value: &str) -> Result<Pid> {
    let pid = value
        .parse::<PidPrefix>()
        .ok()
        .and_then(PidPrefix::as_pid)
        .context("PID must contain 32 hexadecimal digits")?;
    if pid.is_link_local() {
        anyhow::bail!("link-local PID is reserved");
    }
    Ok(pid)
}

fn mast_task_handle(task_id: usize) -> String {
    let Ok(cluster) = std::env::var("TW_JOB_CLUSTER") else {
        return format!("task/{task_id}");
    };
    let Ok(user) = std::env::var("TW_JOB_USER") else {
        return format!("task/{task_id}");
    };
    let Ok(job) = std::env::var("TW_JOB_NAME") else {
        return format!("task/{task_id}");
    };
    format!("{cluster}/{user}/{job}/{task_id}")
}

fn task_rank() -> Result<usize> {
    std::env::var("TW_TASK_ID")
        .context("TW_TASK_ID is not set")?
        .parse()
        .context("TW_TASK_ID is not a rank")
}

pub(crate) fn task_hosts() -> Result<Vec<String>> {
    let raw = match (
        std::env::var("TW_USER_METADATA_FILE_PATH"),
        std::env::var("TW_USER_METADATA_HOSTNAMES_LIST_KEY"),
    ) {
        (Ok(path), Ok(key)) => {
            let contents = fs::read_to_string(&path)
                .with_context(|| format!("read Tupperware user metadata {path}"))?;
            metadata_hostnames(&contents, &key)
                .with_context(|| format!("parse Tupperware user metadata {path}"))?
        }
        _ => std::env::var("MAST_HPC_TASK_GROUP_HOSTNAMES")
            .context("MAST_HPC_TASK_GROUP_HOSTNAMES is not set")?,
    };
    let mut hosts: Vec<_> = raw
        .split(',')
        .filter(|host| !host.is_empty())
        .map(str::to_owned)
        .collect();
    anyhow::ensure!(!hosts.is_empty(), "MAST supplied no task hostnames");
    hosts.sort_unstable();
    hosts.dedup();
    Ok(hosts)
}

fn metadata_hostnames(contents: &str, key: &str) -> Result<String> {
    let metadata: Value = serde_json::from_str(contents)?;
    metadata
        .get("userAttributes")
        .and_then(|attributes| attributes.get(key))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .with_context(|| format!("Tupperware metadata has no string userAttributes.{key}"))
}

pub(crate) async fn resolve_ipv6(host: &str, port: u16) -> Result<SocketAddr> {
    tokio::net::lookup_host((host, port))
        .await
        .with_context(|| format!("resolve host {host}"))?
        .find(|address| matches!(address.ip(), IpAddr::V6(_)))
        .with_context(|| format!("host {host} has no IPv6 address"))
}

pub(crate) fn same_hostname(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
        || left
            .split_once('.')
            .map(|(short, _)| short)
            .unwrap_or(left)
            .eq_ignore_ascii_case(
                right
                    .split_once('.')
                    .map(|(short, _)| short)
                    .unwrap_or(right),
            )
}

#[cfg(test)]
mod tests {
    use chrysalis::DatagramSocket;
    use chrysalis::QuicIdentity;
    use rcgen::BasicConstraints;
    use rcgen::CertificateParams;
    use rcgen::CertifiedIssuer;
    use rcgen::ExtendedKeyUsagePurpose;
    use rcgen::IsCa;
    use rcgen::KeyPair;
    use rcgen::KeyUsagePurpose;
    use tokio::time::timeout;

    use super::*;

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    fn test_identities(count: usize) -> Vec<QuicIdentity> {
        let mut issuer_params = CertificateParams::default();
        issuer_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        issuer_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let issuer = CertifiedIssuer::self_signed(
            issuer_params,
            KeyPair::generate().expect("generate test issuer key"),
        )
        .expect("generate test issuer");
        let trust_roots = issuer.pem();
        (0..count)
            .map(|_| {
                let key = KeyPair::generate().expect("generate test key");
                let mut params = CertificateParams::new(vec!["localhost".to_owned()])
                    .expect("construct test certificate parameters");
                params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
                params.extended_key_usages = vec![
                    ExtendedKeyUsagePurpose::ClientAuth,
                    ExtendedKeyUsagePurpose::ServerAuth,
                ];
                let certificate = params
                    .signed_by(&key, &issuer)
                    .expect("sign test certificate");
                QuicIdentity::new(
                    certificate.der().as_ref(),
                    format!("{}{}", certificate.pem(), trust_roots).into_bytes(),
                    key.serialize_pem().into_bytes(),
                    trust_roots.as_bytes().to_vec(),
                    "localhost",
                )
            })
            .collect()
    }

    #[test]
    fn parses_complete_host_list_from_tupperware_metadata() {
        let raw = metadata_hostnames(
            r#"{"userAttributes":{"hostnames_list":"host0,host1,host2"}}"#,
            "hostnames_list",
        )
        .expect("parse hostname list");
        assert_eq!(
            raw.split(',').collect::<Vec<_>>(),
            ["host0", "host1", "host2"]
        );
    }

    #[test]
    fn hostnames_match_across_fqdn_and_short_forms() {
        assert!(same_hostname("host123", "host123.example.com"));
        assert!(!same_hostname("host123", "host456.example.com"));
    }

    #[test]
    fn logical_nodes_are_partitioned_across_tasks() {
        assert_eq!(task_count(100_000, 100), 1_000);
        assert_eq!(task_count(1_001, 100), 11);
        assert_eq!(node_range(1_001, 100, 0), 0..100);
        assert_eq!(node_range(1_001, 100, 9), 900..1_000);
        assert_eq!(node_range(1_001, 100, 10), 1_000..1_001);
    }

    #[test]
    fn scale_labels_describe_node_placement() {
        let task = TaskMetadata {
            id: 7,
            handle: "task-handle".into(),
            hostname: "host.test".into(),
            expected_nodes: 100,
            nodes_per_task: 10,
        };
        let labels = scale_labels(73, 2, NodeRole::Leaf, &task, Topology::TaskHead);
        assert_eq!(
            labels
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>(),
            [
                "client=chrysalis-scale",
                "level=2",
                "rank=73",
                "role=leaf",
                "task=7",
                "topology=task-head",
            ]
        );
        assert_eq!(NodeRole::Root.as_str(), "root");
        assert_eq!(NodeRole::Head.as_str(), "head");
    }

    #[test]
    fn task_head_workers_use_distinct_unix_carriers() {
        let runtime_dir = Path::new("/tmp/chrysalis-scale-test");
        let parent = ParentTarget::Pinned {
            pid: Pid::from_bytes([0x42; 16]),
            address: CarrierAddr::Unix(runtime_dir.join("head.sock")),
        };
        let specs = make_worker_specs(
            1..4,
            1,
            runtime_dir,
            |rank| CarrierAddr::Unix(runtime_dir.join(format!("node-{rank}.sock"))),
            parent.clone(),
        );

        assert_eq!(specs.len(), 3);
        for (rank, spec) in (1..4).zip(specs) {
            assert_eq!(spec.rank, rank);
            assert_eq!(spec.level, 1);
            assert_eq!(
                spec.bind,
                CarrierAddr::Unix(runtime_dir.join(format!("node-{rank}.sock")))
            );
            assert_eq!(spec.parent, parent);
            assert_eq!(spec.ready_file, runtime_dir.join(format!("ready-{rank}")));
        }
    }

    #[test]
    fn worker_parent_tokens_round_trip_udp_and_unix_addresses() {
        let discovered = ParentTarget::Discover(CarrierAddr::Udp(
            "[::1]:26600".parse().expect("parse UDP address"),
        ));
        assert_eq!(
            discovered
                .to_string()
                .parse::<ParentTarget>()
                .expect("parse discovered parent target"),
            discovered
        );

        let pinned = ParentTarget::Pinned {
            pid: Pid::from_bytes([0x42; 16]),
            address: CarrierAddr::Unix("/tmp/chrysalis-head.sock".into()),
        };
        assert_eq!(
            pinned
                .to_string()
                .parse::<ParentTarget>()
                .expect("parse pinned parent target"),
            pinned
        );
    }

    #[test]
    fn pid_parser_rejects_non_ascii_input_without_panicking() {
        let value = format!("aé{}", "a".repeat(29));

        assert!(parse_pid(&value).is_err());
    }

    #[test]
    fn root_join_token_matches_chrysalis_cli_format() {
        let pid = Pid::from_bytes([0x42; 16]);
        let address = "[::1]:26600".parse().expect("parse root address");

        assert_eq!(
            root_join_token(pid, address),
            "udp://[::1]:26600?authority=42424242424242424242424242424242"
        );
    }

    #[tokio::test]
    async fn raw_streams_echo_without_benchmark_framing() {
        let mut reader = &b"oo\n"[..];
        let mut writer = Vec::new();

        assert_eq!(
            relay_raw(b'f', &mut reader, &mut writer)
                .await
                .expect("relay raw stream"),
            4
        );
        assert_eq!(writer, b"foo\n");
    }

    #[tokio::test]
    async fn unix_leaves_exchange_streams_across_udp_heads() {
        let directory = tempfile::tempdir().expect("create carrier directory");
        let head_a_path = directory.path().join("head-a.sock");
        let leaf_a_path = directory.path().join("leaf-a.sock");
        let head_b_path = directory.path().join("head-b.sock");
        let leaf_b_path = directory.path().join("leaf-b.sock");
        let root_socket = Arc::new(
            UdpSocket::bind("127.0.0.1:0".parse().expect("parse root address"))
                .await
                .expect("bind root UDP carrier"),
        );
        let root_address = root_socket.local_addr().clone();
        let head_a_udp = Arc::new(
            UdpSocket::bind("127.0.0.1:0".parse().expect("parse head A address"))
                .await
                .expect("bind head A UDP carrier"),
        );
        let head_a_unix =
            Arc::new(UnixDatagramSocket::bind(&head_a_path).expect("bind head A Unix carrier"));
        let head_a_socket = Arc::new(
            DatagramSocketSet::new(head_a_udp, vec![head_a_unix])
                .expect("create head A socket set"),
        );
        let leaf_a_socket =
            Arc::new(UnixDatagramSocket::bind(&leaf_a_path).expect("bind leaf A Unix carrier"));
        let head_b_udp = Arc::new(
            UdpSocket::bind("127.0.0.1:0".parse().expect("parse head B address"))
                .await
                .expect("bind head B UDP carrier"),
        );
        let head_b_unix =
            Arc::new(UnixDatagramSocket::bind(&head_b_path).expect("bind head B Unix carrier"));
        let head_b_socket = Arc::new(
            DatagramSocketSet::new(head_b_udp, vec![head_b_unix])
                .expect("create head B socket set"),
        );
        let leaf_b_socket =
            Arc::new(UnixDatagramSocket::bind(&leaf_b_path).expect("bind leaf B Unix carrier"));
        let mut identities = test_identities(5).into_iter();
        let root = Arc::new(
            Node::create(NodeConfig::new(TransportConfig::new(
                root_socket,
                identities.next().expect("root identity"),
            )))
            .expect("create root"),
        );
        let head_a = Arc::new(
            Node::create(
                NodeConfig::new(TransportConfig::new(
                    head_a_socket,
                    identities.next().expect("head A identity"),
                ))
                .with_parent(
                    NamespaceConfig::try_discover(vec![ParentEndpoint::new(root_address.clone())])
                        .expect("create root parent config"),
                ),
            )
            .expect("create head A"),
        );
        let leaf_a = Arc::new(
            Node::create(
                NodeConfig::new(TransportConfig::new(
                    leaf_a_socket,
                    identities.next().expect("leaf A identity"),
                ))
                .with_parent(
                    NamespaceConfig::try_new(
                        head_a.pid(),
                        vec![ParentEndpoint::new(UnixDatagramSocket::datagram_addr(
                            &head_a_path,
                        ))],
                    )
                    .expect("create head A parent config"),
                ),
            )
            .expect("create leaf A"),
        );
        let head_b = Arc::new(
            Node::create(
                NodeConfig::new(TransportConfig::new(
                    head_b_socket,
                    identities.next().expect("head B identity"),
                ))
                .with_parent(
                    NamespaceConfig::try_discover(vec![ParentEndpoint::new(root_address)])
                        .expect("create root parent config"),
                ),
            )
            .expect("create head B"),
        );
        let leaf_b = Arc::new(
            Node::create(
                NodeConfig::new(TransportConfig::new(
                    leaf_b_socket,
                    identities.next().expect("leaf B identity"),
                ))
                .with_parent(
                    NamespaceConfig::try_new(
                        head_b.pid(),
                        vec![ParentEndpoint::new(UnixDatagramSocket::datagram_addr(
                            &head_b_path,
                        ))],
                    )
                    .expect("create head B parent config"),
                ),
            )
            .expect("create leaf B"),
        );
        let children = [head_a, leaf_a, head_b, leaf_b];
        let mut servers = JoinSet::new();
        for (rank, node) in std::iter::once(root.clone())
            .chain(children.iter().cloned())
            .enumerate()
        {
            servers.spawn(async move { run_persistent_benchmark_server(node, rank).await });
        }

        let entries = timeout(TEST_TIMEOUT, wait_for_entries(&root, 5, TEST_TIMEOUT))
            .await
            .expect("join timed out")
            .expect("wait for entries");
        let pids = entries
            .into_iter()
            .filter_map(|entry| (entry.pid != root.pid()).then_some(entry.pid))
            .collect();
        let latencies = timeout(
            TEST_TIMEOUT,
            run_all(
                root.clone(),
                pids,
                4,
                PAYLOAD_CHUNK_LEN * 2 + 17,
                ExperimentKind::Echo,
            ),
        )
        .await
        .expect("echo sweep timed out")
        .expect("echo sweep");
        assert_eq!(latencies.len(), 4);

        let latencies = timeout(
            TEST_TIMEOUT,
            run_all(
                children[1].clone(),
                vec![children[3].pid()],
                1,
                PAYLOAD_CHUNK_LEN + 9,
                ExperimentKind::Echo,
            ),
        )
        .await
        .expect("cross-subtree leaf echo timed out")
        .expect("cross-subtree leaf echo");
        assert_eq!(latencies.len(), 1);

        let latencies = timeout(
            TEST_TIMEOUT,
            run_all(
                children[1].clone(),
                vec![children[3].pid()],
                1,
                PAYLOAD_CHUNK_LEN * 2 + 11,
                ExperimentKind::Delivery,
            ),
        )
        .await
        .expect("cross-subtree leaf delivery timed out")
        .expect("cross-subtree leaf delivery");
        assert_eq!(latencies.len(), 1);

        servers.abort_all();
        while let Some(result) = servers.join_next().await {
            assert!(
                result
                    .expect_err("echo servers run until aborted")
                    .is_cancelled()
            );
        }
        for node in std::iter::once(&root).chain(children.iter()) {
            node.shutdown();
        }
        for node in std::iter::once(&root).chain(children.iter()) {
            timeout(TEST_TIMEOUT, node.join())
                .await
                .expect("node shutdown timed out");
        }
    }
}
