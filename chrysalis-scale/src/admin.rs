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
use chrysalis_resolver::ParseResolverError;
use chrysalis_resolver::ResolverSpec;
use clap::Args;
use clap::Subcommand;
use clap::ValueEnum;
use libsql::Connection;
use libsql::Value;

use crate::benchmark::format_bytes;
use crate::benchmark::format_pid;
use crate::benchmark::parse_pid;
use crate::persist::Experiment;
use crate::persist::ExperimentKind;
use crate::persist::ExperimentStatus;
use crate::persist::ExperimentStore;
use crate::persist::ExperimentTargets;

const DEFAULT_TIMEOUT_SECS: u64 = 1800;
const POLL_INTERVAL: Duration = Duration::from_millis(25);

#[derive(Debug, Args)]
pub(crate) struct ExperimentsArgs {
    /// Root token, UDP address, or deployment resolver URL.
    join: AdminTarget,

    /// Maximum duration for each connection or synchronization phase.
    #[arg(long = "timeout", value_name = "SECONDS", default_value_t = DEFAULT_TIMEOUT_SECS)]
    timeout_secs: u64,

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

    /// Maximum duration for each connection or synchronization phase.
    #[arg(long = "timeout", value_name = "SECONDS", default_value_t = DEFAULT_TIMEOUT_SECS)]
    timeout_secs: u64,

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
    timeout: Duration,
}

pub(crate) async fn experiments(args: ExperimentsArgs) -> Result<()> {
    let ExperimentsArgs {
        join,
        timeout_secs,
        command,
    } = args;
    anyhow::ensure!(timeout_secs > 0, "timeout must be nonzero");
    let admin = Admin::connect(join.resolve().await?, Duration::from_secs(timeout_secs)).await?;
    let result = match command {
        ExperimentCommand::List => list_experiments(&admin.store).await,
        ExperimentCommand::Add {
            name,
            pid,
            count,
            size,
            kind,
        } => {
            let kind = kind.into();
            add_experiment(
                &admin.store,
                Experiment {
                    pid: pid.0,
                    name,
                    kind,
                    targets: ExperimentTargets::Count(
                        i64::try_from(count).context("experiment count exceeds i64")?,
                    ),
                    size: validate_size(kind, size)?,
                },
                admin.timeout,
            )
            .await
        }
        ExperimentCommand::AddTargeted {
            name,
            pid,
            size,
            targets,
            kind,
        } => {
            let kind = kind.into();
            add_experiment(
                &admin.store,
                Experiment {
                    pid: pid.0,
                    name,
                    kind,
                    targets: ExperimentTargets::Explicit(
                        targets.into_iter().map(|target| target.0).collect(),
                    ),
                    size: validate_size(kind, size)?,
                },
                admin.timeout,
            )
            .await
        }
        ExperimentCommand::Show { name } => show_experiment(&admin.store, &name).await,
    };
    admin.shutdown().await;
    result
}

pub(crate) async fn nodes(args: NodeArgs) -> Result<()> {
    let NodeArgs {
        join,
        timeout_secs,
        command,
    } = args;
    anyhow::ensure!(timeout_secs > 0, "timeout must be nonzero");
    let admin = Admin::connect(join.resolve().await?, Duration::from_secs(timeout_secs)).await?;
    let result = match command {
        NodeCommand::List => list_nodes(&admin.store).await,
        NodeCommand::Show { pid } => show_node(&admin.store, pid.0).await,
    };
    admin.shutdown().await;
    result
}

impl Admin {
    async fn connect(target: ConnectionTarget, timeout: Duration) -> Result<Self> {
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
            let peer = wait_for_parent(&node, timeout).await?;
            eprintln!(
                "joined scale root {}; synchronizing experiment store",
                format_pid(peer)
            );
            wait_for_registry(&store, timeout).await
        }
        .await;
        if let Err(error) = ready {
            node.shutdown();
            node.join().await;
            return Err(error);
        }
        Ok(Self {
            node,
            store,
            timeout,
        })
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
                Ok(ConnectionTarget {
                    join: JoinTarget {
                        pid: None,
                        address: resolved.join(),
                    },
                    carrier: resolved.carrier(),
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
    let mut waiting_for_root_reported = false;
    loop {
        if let Some(expected) = store.expected_nodes().await? {
            let nodes = store.node_count_for_run(expected).await?;
            let report_interval = (expected / 100).max(1);
            if nodes == expected || nodes >= last_reported.saturating_add(report_interval) {
                last_reported = nodes;
                eprintln!("synchronized {last_reported}/{expected} scale nodes");
            }
            if nodes == expected {
                return Ok(());
            }
        } else if !waiting_for_root_reported {
            waiting_for_root_reported = true;
            eprintln!("waiting for scale root registration");
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
    experiment: Experiment,
    timeout: Duration,
) -> Result<()> {
    anyhow::ensure!(
        !experiment.name.is_empty(),
        "experiment name must not be empty"
    );
    let expected = store
        .expected_nodes()
        .await?
        .context("scale root registration disappeared")?;
    let nodes = store
        .nodes_for_run(expected)
        .await?
        .into_iter()
        .collect::<BTreeSet<_>>();
    anyhow::ensure!(
        nodes.contains(&experiment.pid),
        "experiment PID is not a scale node"
    );
    match &experiment.targets {
        ExperimentTargets::Count(count) => {
            let count = usize::try_from(*count).context("experiment count is invalid")?;
            anyhow::ensure!(count > 0, "experiment count must be positive");
            anyhow::ensure!(
                count < nodes.len(),
                "experiment count must be smaller than the {}-node mesh",
                nodes.len()
            );
        }
        ExperimentTargets::Explicit(targets) => {
            anyhow::ensure!(!targets.is_empty(), "targeted experiment requires a target");
            let mut unique = BTreeSet::new();
            for target in targets {
                anyhow::ensure!(
                    *target != experiment.pid,
                    "experiment PID cannot also be a target"
                );
                anyhow::ensure!(nodes.contains(target), "target PID is not a scale node");
                anyhow::ensure!(unique.insert(*target), "target PID is repeated");
            }
        }
    }
    store.add_experiment(&experiment).await?;
    let status = wait_until_claimed(store, &experiment.name, timeout).await?;
    let target_list = match &experiment.targets {
        ExperimentTargets::Count(_) => String::new(),
        ExperimentTargets::Explicit(targets) => targets
            .iter()
            .copied()
            .map(format_pid)
            .collect::<Vec<_>>()
            .join(","),
    };
    println!("name\tpid\tstatus\tkind\tselection\tcount\tsize\ttargets");
    println!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        escape_text(&experiment.name),
        format_pid(experiment.pid),
        status.as_str(),
        experiment.kind.as_str(),
        experiment.targets.selection(),
        experiment.targets.count(),
        experiment.size,
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
    let multiplier = store
        .experiment_kind(name)
        .await?
        .with_context(|| format!("experiment {name:?} was not found"))?
        .payload_multiplier() as f64;
    let (rows, output) = query_output(
        store.connection(),
        "SELECT e.name, lower(hex(e.pid)) AS pid, e.status, e.kind, e.selection, \
                e.count, e.size, \
                COALESCE((SELECT group_concat(lower(hex(t.pid)), ',' ORDER BY t.position) \
                          FROM experiment_targets AS t \
                          WHERE t.experiment_name = e.name), '') AS targets, \
                r.status AS result_status, r.completed, r.started_at_ms, \
                r.finished_at_ms, r.warmup_seconds, \
                r.operation_seconds, r.operations_per_second, \
                CASE WHEN r.experiment_name IS NULL THEN NULL \
                     WHEN r.operation_seconds > 0 \
                     THEN (?2 * \
                           r.completed * r.size) / (1048576.0 * r.operation_seconds) \
                     ELSE 0 END AS payload_mib_per_second, \
                r.transmit_calls, r.transmit_datagrams, r.transmit_bytes, \
                r.transmit_blocked, \
                CASE WHEN r.experiment_name IS NULL THEN NULL \
                     WHEN r.transmit_datagrams > 0 \
                     THEN (1.0 * r.transmit_bytes) / r.transmit_datagrams \
                     ELSE 0 END AS mean_transmit_bytes, \
                CASE WHEN r.experiment_name IS NULL THEN NULL \
                     WHEN r.operation_seconds > 0 \
                     THEN r.transmit_datagrams / r.operation_seconds \
                     ELSE 0 END AS transmit_datagrams_per_second, \
                r.receive_calls, r.receive_datagrams, r.receive_bytes, \
                CASE WHEN r.experiment_name IS NULL THEN NULL \
                     WHEN r.receive_datagrams > 0 \
                     THEN (1.0 * r.receive_bytes) / r.receive_datagrams \
                     ELSE 0 END AS mean_receive_bytes, \
                r.connection_rtt_micros, r.connection_congestion_window, \
                r.connection_congestion_events, r.connection_lost_packets, \
                r.connection_lost_bytes, r.connection_sent_packets, r.connection_mtu, \
                r.mean_latency_millis AS mean_operation_millis, \
                r.max_latency_millis AS max_operation_millis, r.error \
         FROM experiments AS e \
         LEFT JOIN results AS r ON r.experiment_name = e.name \
         WHERE e.name = ?1",
        vec![Value::Text(name.into()), Value::Real(multiplier)],
    )
    .await?;
    anyhow::ensure!(rows == 1, "experiment {name:?} was not found");
    print!("{output}");
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
    let (rows, output) = query_output(
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
        1 => {
            print!("{output}");
            Ok(())
        }
        _ => anyhow::bail!(
            "PID {} is registered for {rows} node ranks",
            format_pid(pid)
        ),
    }
}

async fn print_query(connection: &Connection, sql: &str, parameters: Vec<Value>) -> Result<usize> {
    let (count, output) = query_output(connection, sql, parameters).await?;
    print!("{output}");
    Ok(count)
}

async fn query_output(
    connection: &Connection,
    sql: &str,
    parameters: Vec<Value>,
) -> Result<(usize, String)> {
    let mut rows = connection
        .query(sql, parameters)
        .await
        .context("query database")?;
    let columns = rows.column_count();
    let mut output = String::new();
    for column in 0..columns {
        if column > 0 {
            output.push('\t');
        }
        output.push_str(rows.column_name(column).unwrap_or("?"));
    }
    output.push('\n');
    let mut count = 0;
    while let Some(row) = rows.next().await.context("read query result")? {
        count += 1;
        for column in 0..columns {
            if column > 0 {
                output.push('\t');
            }
            write!(&mut output, "{}", format_value(row.get_value(column)?))
                .expect("writing to a string cannot fail");
        }
        output.push('\n');
    }
    Ok((count, output))
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
        .replace('\r', "\\r")
        .replace('\n', "\\n")
}

impl FromStr for JoinTarget {
    type Err = anyhow::Error;

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
                    .context("join locator only supports the authority query")
                    .and_then(parse_pid)
            })
            .transpose()?;
        let address = locator
            .strip_prefix("udp://")
            .context("join locator must use udp://")?
            .parse()
            .context("invalid UDP address")?;
        Ok(Self { pid, address })
    }
}

impl FromStr for AdminTarget {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.parse() {
            Ok(resolver) => Ok(Self::Resolver(resolver)),
            Err(
                ParseResolverError::MissingScheme | ParseResolverError::UnsupportedScheme { .. },
            ) => value.parse().map(Self::Direct),
            Err(error) => Err(error.into()),
        }
    }
}

impl FromStr for PidArg {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_pid(value).map(Self)
    }
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
        assert!("mast://".parse::<AdminTarget>().is_err());
        assert!(matches!(
            "udp://127.0.0.1:26600".parse::<AdminTarget>(),
            Ok(AdminTarget::Direct(_))
        ));
    }

    #[test]
    fn pid_parser_rejects_non_ascii_without_panicking() {
        let value = format!("aé{}", "0".repeat(29));
        assert!(parse_pid(&value).is_err());
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
    fn experiment_size_must_be_positive() {
        assert!(validate_size(ExperimentKind::Echo, 0).is_err());
        assert!(validate_size(ExperimentKind::Delivery, 0).is_err());
    }

    #[tokio::test]
    async fn ordered_group_concat_follows_position() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let store = ExperimentStore::open(&directory.path().join("admin.db"))
            .await
            .expect("open experiment store");
        let (_, output) = query_output(
            store.connection(),
            "WITH items(position, value) AS (VALUES (2, 'b'), (1, 'a')) \
             SELECT group_concat(value, ',' ORDER BY position) AS joined FROM items",
            Vec::new(),
        )
        .await
        .expect("query ordered concatenation");
        assert_eq!(output, "joined\na,b\n");
    }
}
