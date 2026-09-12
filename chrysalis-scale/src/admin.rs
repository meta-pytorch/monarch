/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use chrysalis::NamespaceConfig;
use chrysalis::Node;
use chrysalis::NodeConfig;
use chrysalis::ParentEndpoint;
use chrysalis::ParentManagerStatus;
use chrysalis::Pid;
use chrysalis::TransportConfig;
use chrysalis::UdpSocket;
use chrysalis_resolver::IdentityProvider;
use chrysalis_resolver::ResolverSpec;
use clap::Args;
use clap::Subcommand;
use clap::ValueEnum;
use libsql::Connection;
use libsql::Value;

use crate::persist::Experiment;
use crate::persist::ExperimentKind;
use crate::persist::ExperimentStatus;
use crate::persist::ExperimentStore;
use crate::persist::ExperimentTargets;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(1800);
const POLL_INTERVAL: Duration = Duration::from_millis(25);

#[derive(Debug, Args)]
pub(crate) struct ExperimentsArgs {
    /// Root token, UDP address, or deployment resolver URL.
    join: AdminTarget,

    #[command(subcommand)]
    command: ExperimentCommand,
}

#[derive(Debug, Subcommand)]
enum ExperimentCommand {
    /// Lists every experiment and its current status.
    List,
    /// Adds an experiment and waits until its target node claims it.
    Add {
        name: String,
        pid: PidArg,
        count: usize,
        size: usize,
        #[arg(long, value_enum, default_value_t = ExperimentKindArg::Echo)]
        kind: ExperimentKindArg,
    },
    /// Adds an experiment that targets the listed PIDs exactly.
    AddTargeted {
        name: String,
        pid: PidArg,
        size: usize,
        #[arg(required = true)]
        targets: Vec<PidArg>,
        #[arg(long, value_enum, default_value_t = ExperimentKindArg::Echo)]
        kind: ExperimentKindArg,
    },
    /// Shows an experiment and its result, when available.
    Show { name: String },
}

#[derive(Debug, Args)]
pub(crate) struct NodeArgs {
    /// Root token, UDP address, or deployment resolver URL.
    join: AdminTarget,

    #[command(subcommand)]
    command: NodeCommand,
}

#[derive(Debug, Subcommand)]
enum NodeCommand {
    /// Lists every scale node.
    List,
    /// Shows all metadata for one PID.
    Show { pid: PidArg },
}

#[derive(Clone, Debug)]
struct JoinTarget {
    pid: Option<Pid>,
    address: SocketAddr,
}

#[derive(Clone, Debug)]
enum AdminTarget {
    Direct(JoinTarget),
    Resolver(ResolverSpec),
}

struct ConnectionTarget {
    join: JoinTarget,
    carrier: SocketAddr,
    identity: IdentityProvider,
}

#[derive(Clone, Copy, Debug)]
struct PidArg(Pid);

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum ExperimentKindArg {
    Echo,
    Delivery,
}

impl From<ExperimentKindArg> for ExperimentKind {
    fn from(value: ExperimentKindArg) -> Self {
        match value {
            ExperimentKindArg::Echo => Self::Echo,
            ExperimentKindArg::Delivery => Self::Delivery,
        }
    }
}

struct Admin {
    node: Arc<Node>,
    store: ExperimentStore,
}

pub(crate) async fn experiments(args: ExperimentsArgs) -> Result<()> {
    let admin = Admin::connect(args.join.resolve().await?).await?;
    let result = match args.command {
        ExperimentCommand::List => list_experiments(&admin.store).await,
        ExperimentCommand::Add {
            name,
            pid,
            count,
            size,
            kind,
        } => add_experiment(&admin.store, name, pid.0, count, size, kind.into()).await,
        ExperimentCommand::AddTargeted {
            name,
            pid,
            size,
            targets,
            kind,
        } => {
            add_targeted_experiment(
                &admin.store,
                name,
                pid.0,
                size,
                targets.into_iter().map(|target| target.0).collect(),
                kind.into(),
            )
            .await
        }
        ExperimentCommand::Show { name } => show_experiment(&admin.store, &name).await,
    };
    admin.shutdown().await;
    result
}

pub(crate) async fn nodes(args: NodeArgs) -> Result<()> {
    let admin = Admin::connect(args.join.resolve().await?).await?;
    let result = match args.command {
        NodeCommand::List => list_nodes(&admin.store).await,
        NodeCommand::Show { pid } => show_node(&admin.store, pid.0).await,
    };
    admin.shutdown().await;
    result
}

impl Admin {
    async fn connect(target: ConnectionTarget) -> Result<Self> {
        let identity = match target.identity {
            IdentityProvider::Meta => chrysalis_identity_meta::issue_host()
                .await
                .context("issue Meta identity")?,
        };
        let socket = Arc::new(
            UdpSocket::bind(target.carrier)
                .await
                .context("bind admin UDP socket")?,
        );
        let endpoint = ParentEndpoint::new(UdpSocket::datagram_addr(target.join.address));
        let parent = match target.join.pid {
            Some(pid) => NamespaceConfig::try_new(pid, vec![endpoint])?,
            None => NamespaceConfig::try_discover(vec![endpoint])?,
        };
        let store = ExperimentStore::open(Path::new(":memory:")).await?;
        let config = NodeConfig::new(TransportConfig::new(socket, identity)).with_parent(parent);
        let config = store.configure(config).await?;
        let node = Arc::new(Node::create(config).context("create admin node")?);
        let ready = async {
            let peer = wait_for_parent(&node, DEFAULT_TIMEOUT).await?;
            eprintln!(
                "joined scale root {}; synchronizing node registry",
                format_pid(peer)
            );
            wait_for_registry(&store, DEFAULT_TIMEOUT).await
        }
        .await;
        if let Err(error) = ready {
            node.shutdown();
            node.join().await;
            return Err(error);
        }
        Ok(Self { node, store })
    }

    async fn shutdown(self) {
        self.node.shutdown();
        self.node.join().await;
    }
}

impl AdminTarget {
    async fn resolve(self) -> Result<ConnectionTarget> {
        match self {
            Self::Direct(join) => Ok(ConnectionTarget {
                carrier: wildcard_address(join.address),
                join,
                identity: IdentityProvider::Meta,
            }),
            Self::Resolver(resolver) => {
                let resolved = resolver.resolve().await?;
                let join = resolved
                    .join()
                    .parse()
                    .map_err(anyhow::Error::msg)
                    .context("resolver returned an invalid join token")?;
                let carrier = parse_udp_address(resolved.carrier())
                    .map_err(anyhow::Error::msg)
                    .context("resolver returned an invalid carrier")?;
                Ok(ConnectionTarget {
                    join,
                    carrier,
                    identity: resolved.identity(),
                })
            }
        }
    }
}

fn wildcard_address(address: SocketAddr) -> SocketAddr {
    SocketAddr::new(
        match address.ip() {
            IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
        },
        0,
    )
}

fn parse_udp_address(value: &str) -> Result<SocketAddr, String> {
    value
        .strip_prefix("udp://")
        .ok_or_else(|| "carrier must use udp://".to_owned())?
        .parse()
        .map_err(|error| format!("invalid UDP address: {error}"))
}

async fn wait_for_parent(node: &Node, timeout: Duration) -> Result<Pid> {
    let mut parent = node
        .subscribe_parent()
        .expect("admin node is configured with a parent");
    tokio::time::timeout(timeout, async {
        loop {
            match &*parent.borrow() {
                ParentManagerStatus::Connected { peer, .. } => return Ok(*peer),
                ParentManagerStatus::Connecting => {}
                ParentManagerStatus::Failed { error } => anyhow::bail!(error.clone()),
                ParentManagerStatus::Stopped => anyhow::bail!("parent manager stopped"),
            }
            parent.changed().await.context("parent manager stopped")?;
        }
    })
    .await
    .context("timed out joining scale root")?
}

async fn wait_for_registry(store: &ExperimentStore, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let mut last_reported: usize = 0;
    loop {
        let nodes = store.nodes().await?;
        if let Some(expected) = store.expected_nodes().await? {
            anyhow::ensure!(
                nodes.len() <= expected,
                "node registry contains {} entries, expected {expected}",
                nodes.len()
            );
            let report_interval = (expected / 100).max(1);
            if nodes.len() == expected
                || nodes.len() >= last_reported.saturating_add(report_interval)
            {
                last_reported = nodes.len();
                eprintln!("synchronized {last_reported}/{expected} scale nodes");
            }
            if nodes.len() == expected {
                return Ok(());
            }
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "timed out synchronizing node registry"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn list_experiments(store: &ExperimentStore) -> Result<()> {
    print_query(
        store.connection(),
        "SELECT name, lower(hex(pid)) AS pid, status, kind, selection, count, size \
         FROM experiments ORDER BY name",
        Vec::new(),
    )
    .await?;
    Ok(())
}

async fn add_experiment(
    store: &ExperimentStore,
    name: String,
    pid: Pid,
    count: usize,
    size: usize,
    kind: ExperimentKind,
) -> Result<()> {
    anyhow::ensure!(!name.is_empty(), "experiment name must not be empty");
    anyhow::ensure!(store.has_node(pid).await?, "target PID is not a scale node");
    let nodes = store.nodes().await?;
    anyhow::ensure!(count > 0, "experiment count must be positive");
    anyhow::ensure!(
        count < nodes.len(),
        "experiment count must be smaller than the {}-node mesh",
        nodes.len()
    );
    let size = validate_size(kind, size)?;
    let experiment = Experiment {
        pid,
        name: name.clone(),
        kind,
        targets: ExperimentTargets::Count(
            i64::try_from(count).context("experiment count exceeds i64")?,
        ),
        size,
    };
    store.add_experiment(&experiment).await?;
    let status = wait_until_claimed(store, &name, DEFAULT_TIMEOUT).await?;
    println!("name\tpid\tstatus\tkind\tselection\tcount\tsize\ttargets");
    println!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t",
        escape_text(&name),
        format_pid(pid),
        status.as_str(),
        experiment.kind.as_str(),
        experiment.targets.selection(),
        count,
        size
    );
    Ok(())
}

async fn add_targeted_experiment(
    store: &ExperimentStore,
    name: String,
    pid: Pid,
    size: usize,
    targets: Vec<Pid>,
    kind: ExperimentKind,
) -> Result<()> {
    anyhow::ensure!(!name.is_empty(), "experiment name must not be empty");
    anyhow::ensure!(!targets.is_empty(), "targeted experiment requires a target");
    let size = validate_size(kind, size)?;
    let nodes = store.nodes().await?.into_iter().collect::<BTreeSet<_>>();
    anyhow::ensure!(nodes.contains(&pid), "source PID is not a scale node");
    let mut unique = BTreeSet::new();
    for target in &targets {
        anyhow::ensure!(*target != pid, "source PID cannot also be a target");
        anyhow::ensure!(nodes.contains(target), "target PID is not a scale node");
        anyhow::ensure!(unique.insert(*target), "target PID is repeated");
    }
    let target_list = targets
        .iter()
        .copied()
        .map(format_pid)
        .collect::<Vec<_>>()
        .join(",");
    let experiment = Experiment {
        pid,
        name: name.clone(),
        kind,
        targets: ExperimentTargets::Explicit(targets),
        size,
    };
    store.add_experiment(&experiment).await?;
    let status = wait_until_claimed(store, &name, DEFAULT_TIMEOUT).await?;
    println!("name\tpid\tstatus\tkind\tselection\tcount\tsize\ttargets");
    println!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        escape_text(&name),
        format_pid(pid),
        status.as_str(),
        experiment.kind.as_str(),
        experiment.targets.selection(),
        experiment.targets.count(),
        size,
        target_list,
    );
    Ok(())
}

fn validate_size(kind: ExperimentKind, size: usize) -> Result<i64> {
    anyhow::ensure!(size > 0, "experiment size must be positive");
    if kind == ExperimentKind::Echo {
        u32::try_from(size).context("echo size exceeds u32")?;
    }
    i64::try_from(size).context("experiment size exceeds i64")
}

async fn wait_until_claimed(
    store: &ExperimentStore,
    name: &str,
    timeout: Duration,
) -> Result<ExperimentStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        let status = store
            .experiment_status(name)
            .await?
            .with_context(|| format!("experiment {name:?} disappeared before it was claimed"))?;
        if status != ExperimentStatus::Pending {
            return Ok(status);
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "timed out waiting for experiment to be claimed"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn show_experiment(store: &ExperimentStore, name: &str) -> Result<()> {
    let rows = print_query(
        store.connection(),
        "SELECT e.name, lower(hex(e.pid)) AS pid, e.status, e.kind, e.selection, \
                e.count, e.size, \
                COALESCE((SELECT group_concat(lower(hex(t.pid)), ',') \
                          FROM (SELECT pid FROM experiment_targets \
                                WHERE experiment_name = e.name \
                                ORDER BY position) AS t), '') AS targets, \
                r.status AS result_status, r.completed, r.started_at_ms, \
                r.finished_at_ms, r.warmup_seconds, \
                r.echo_seconds AS operation_seconds, \
                r.echoes_per_second AS operations_per_second, \
                CASE WHEN r.echo_seconds > 0 \
                     THEN ((CASE e.kind WHEN 'echo' THEN 2.0 ELSE 1.0 END) * \
                           r.completed * r.size) / (1048576.0 * r.echo_seconds) \
                     ELSE 0 END AS payload_mib_per_second, \
                r.transmit_calls, r.transmit_datagrams, r.transmit_bytes, \
                r.transmit_blocked, \
                CASE WHEN r.transmit_datagrams > 0 \
                     THEN (1.0 * r.transmit_bytes) / r.transmit_datagrams \
                     ELSE 0 END AS mean_transmit_bytes, \
                CASE WHEN r.echo_seconds > 0 \
                     THEN r.transmit_datagrams / r.echo_seconds \
                     ELSE 0 END AS transmit_datagrams_per_second, \
                r.receive_calls, r.receive_datagrams, r.receive_bytes, \
                CASE WHEN r.receive_datagrams > 0 \
                     THEN (1.0 * r.receive_bytes) / r.receive_datagrams \
                     ELSE 0 END AS mean_receive_bytes, \
                r.connection_rtt_micros, r.connection_congestion_window, \
                r.connection_congestion_events, r.connection_lost_packets, \
                r.connection_lost_bytes, r.connection_sent_packets, r.connection_mtu, \
                r.mean_echo_millis AS mean_operation_millis, \
                r.max_echo_millis AS max_operation_millis, r.error \
         FROM experiments AS e \
         LEFT JOIN results AS r ON r.pid = e.pid AND r.experiment_name = e.name \
         WHERE e.name = ?1",
        vec![Value::Text(name.into())],
    )
    .await?;
    anyhow::ensure!(rows == 1, "experiment {name:?} was not found");
    Ok(())
}

async fn list_nodes(store: &ExperimentStore) -> Result<()> {
    print_query(
        store.connection(),
        "SELECT rank, lower(hex(pid)) AS pid, \
                CASE WHEN parent_pid = X'00000000000000000000000000000000' \
                     THEN '' ELSE lower(hex(parent_pid)) END AS parent_pid, \
                level, task_handle, hostname, address, is_root \
         FROM nodes ORDER BY rank",
        Vec::new(),
    )
    .await?;
    Ok(())
}

async fn show_node(store: &ExperimentStore, pid: Pid) -> Result<()> {
    let rows = print_query(
        store.connection(),
        "SELECT rank, lower(hex(pid)) AS pid, \
                CASE WHEN parent_pid = X'00000000000000000000000000000000' \
                     THEN '' ELSE lower(hex(parent_pid)) END AS parent_pid, \
                level, task_id, task_handle, hostname, address, is_root, expected_nodes, \
                nodes_per_task, started_at_ms \
         FROM nodes WHERE pid = ?1",
        vec![Value::Blob(pid.as_bytes().to_vec())],
    )
    .await?;
    match rows {
        0 => anyhow::bail!("node {} was not found", format_pid(pid)),
        1 => Ok(()),
        _ => anyhow::bail!("node {} matched {rows} rows", format_pid(pid)),
    }
}

async fn print_query(connection: &Connection, sql: &str, parameters: Vec<Value>) -> Result<usize> {
    let mut rows = connection
        .query(sql, parameters)
        .await
        .context("query database")?;
    let columns = rows.column_count();
    for column in 0..columns {
        if column > 0 {
            print!("\t");
        }
        print!("{}", rows.column_name(column).unwrap_or("?"));
    }
    println!();
    let mut count = 0;
    while let Some(row) = rows.next().await.context("read query result")? {
        count += 1;
        for column in 0..columns {
            if column > 0 {
                print!("\t");
            }
            print!("{}", format_value(row.get_value(column)?));
        }
        println!();
    }
    Ok(count)
}

fn format_value(value: Value) -> String {
    match value {
        Value::Null => "NULL".into(),
        Value::Integer(value) => value.to_string(),
        Value::Real(value) => value.to_string(),
        Value::Text(value) => escape_text(&value),
        Value::Blob(value) => format!("x'{}'", format_bytes(&value)),
    }
}

fn escape_text(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
}

fn format_pid(pid: Pid) -> String {
    format_bytes(pid.as_bytes())
}

fn format_bytes(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to a string cannot fail");
    }
    output
}

impl FromStr for JoinTarget {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (locator, query) = match value.split_once('?') {
            Some((locator, query)) => (locator, Some(query)),
            None => (value, None),
        };
        let pid = query
            .map(|query| {
                query
                    .strip_prefix("authority=")
                    .filter(|pid| !pid.contains('&'))
                    .ok_or_else(|| "join locator only supports the authority query".to_owned())
                    .and_then(parse_pid)
            })
            .transpose()?;
        let address = locator
            .strip_prefix("udp://")
            .ok_or("join locator must use udp://")?
            .parse()
            .map_err(|error| format!("invalid UDP address: {error}"))?;
        Ok(Self { pid, address })
    }
}

impl FromStr for AdminTarget {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if ResolverSpec::recognizes(value) {
            value
                .parse()
                .map(Self::Resolver)
                .map_err(|error| error.to_string())
        } else {
            value.parse().map(Self::Direct)
        }
    }
}

impl FromStr for PidArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_pid(value).map(Self)
    }
}

fn parse_pid(value: &str) -> Result<Pid, String> {
    if value.len() != 32 {
        return Err("PID must contain 32 hexadecimal digits".into());
    }
    let mut bytes = [0; 16];
    for (byte, digits) in bytes.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        let digits =
            std::str::from_utf8(digits).map_err(|_| "PID must contain 32 hexadecimal digits")?;
        *byte =
            u8::from_str_radix(digits, 16).map_err(|_| "PID must contain 32 hexadecimal digits")?;
    }
    let pid = Pid::from_bytes(bytes);
    if pid.is_link_local() {
        return Err("PID zero is reserved".into());
    }
    Ok(pid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_target_accepts_pinned_and_discovered_udp_addresses() {
        let discovered: JoinTarget = "udp://127.0.0.1:1234".parse().unwrap();
        assert_eq!(discovered.pid, None);
        assert_eq!(discovered.address, "127.0.0.1:1234".parse().unwrap());

        let pinned: JoinTarget = "udp://[::1]:4321?authority=42424242424242424242424242424242"
            .parse()
            .unwrap();
        assert_eq!(pinned.pid, Some(Pid::from_bytes([0x42; 16])));
        assert_eq!(pinned.address, "[::1]:4321".parse().unwrap());
    }

    #[test]
    fn admin_target_accepts_mast_resolver() {
        assert!(matches!(
            "mast://scale_job".parse::<AdminTarget>(),
            Ok(AdminTarget::Resolver(ResolverSpec::Mast { job })) if job == "scale_job"
        ));
    }

    #[test]
    fn direct_target_selects_matching_wildcard_carrier() {
        assert_eq!(
            wildcard_address("127.0.0.1:26600".parse().expect("parse IPv4 address")),
            "0.0.0.0:0".parse().expect("parse IPv4 wildcard")
        );
        assert_eq!(
            wildcard_address("[::1]:26600".parse().expect("parse IPv6 address")),
            "[::]:0".parse().expect("parse IPv6 wildcard")
        );
    }

    #[test]
    fn pid_rejects_multibyte_utf8_without_panicking() {
        assert!(parse_pid(&"é".repeat(16)).is_err());
    }

    #[test]
    fn experiment_size_must_be_positive() {
        assert!(validate_size(ExperimentKind::Echo, 0).is_err());
        assert!(validate_size(ExperimentKind::Delivery, 0).is_err());
    }
}
