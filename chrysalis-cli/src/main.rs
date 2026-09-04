/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

mod address;
mod identity;
mod socket;
mod sqlite_shell;

use std::io::Write as _;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use chrysalis::IncomingStream;
use chrysalis::LabelKey;
use chrysalis::LabelValue;
use chrysalis::Labels;
use chrysalis::Locator;
use chrysalis::NamespaceConfig;
use chrysalis::Node;
use chrysalis::NodeConfig;
use chrysalis::ParentEndpoint;
use chrysalis::ParentManagerStatus;
use chrysalis::Pid;
use chrysalis::ProcEntry;
use chrysalis::Resolution;
use chrysalis::ResolveConsistency;
use chrysalis::TransportConfig;
use chrysalis_resolver::IdentityProvider;
use clap::Parser;
use clap::Subcommand;
use clap::ValueEnum;
use libsql::Builder;
use libsql::Connection;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::address::CarrierSpec;
use crate::address::ClusterLocator;
use crate::address::JoinToken;
use crate::address::NodeAddr;
use crate::address::Reference;
use crate::address::format_datagram_addr;
use crate::address::format_pid;
use crate::socket::BoundSockets;

const DEFAULT_SQLITE_DATABASE: &str = ":memory:";
const ECHO_QUEUE_CAPACITY: usize = 8;

#[derive(Debug, Parser)]
#[command(
    name = "chrysalis",
    bin_name = "chrysalis",
    about = "Inspect and interact with Chrysalis process namespaces"
)]
struct Cli {
    /// Diagnostic verbosity
    #[arg(long, global = true, default_value = "info")]
    log_level: String,

    /// Cluster locator used by commands without a qualified reference
    #[arg(long, global = true)]
    cluster: Option<ClusterLocator>,

    /// Deprecated alias for `--cluster`
    #[arg(long, global = true, hide = true, conflicts_with = "cluster")]
    join: Option<ClusterLocator>,

    /// Public datagram carrier to advertise
    #[arg(long, global = true)]
    carrier: Option<CarrierSpec>,

    /// Identity and trust configuration
    #[arg(long, global = true, value_enum)]
    identity: Option<IdentityKind>,

    /// Kubernetes-style process label (`KEY=VALUE`); may be repeated
    #[arg(long = "label", global = true, value_name = "KEY=VALUE")]
    labels: Vec<LabelArg>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LabelArg {
    key: LabelKey,
    value: LabelValue,
}

impl FromStr for LabelArg {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (key, value) = value
            .split_once('=')
            .context("label must have the form KEY=VALUE")?;
        Ok(Self {
            key: key.parse()?,
            value: value.parse()?,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum IdentityKind {
    Ephemeral,
    Meta,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve streams, print received bytes, and echo them to each sender
    Serve,
    /// Copy stdin to a process stream and copy its response to stdout
    Cat {
        /// Process reference (`PID-PREFIX[@LOCATOR]`)
        target: Reference,
    },
    /// List processes visible in the namespace
    Ps {
        /// Cluster locator; overrides `--cluster`
        locator: Option<ClusterLocator>,
        /// Display complete 32-digit PIDs
        #[arg(long)]
        full: bool,
    },
    /// Show one complete nameserver entry
    Show {
        /// Process reference (`PID-PREFIX[@LOCATOR]`)
        reference: Reference,
    },
    /// Inspect a local SQLite database
    Sqlite {
        #[command(subcommand)]
        command: Option<SqliteCommand>,
    },
}

impl Command {
    const fn label(&self) -> &'static str {
        match self {
            Self::Serve => "serve",
            Self::Cat { .. } => "cat",
            Self::Ps { .. } => "ps",
            Self::Show { .. } => "show",
            Self::Sqlite { .. } => "sqlite",
        }
    }

    fn cluster(&self) -> Option<&ClusterLocator> {
        match self {
            Self::Cat { target } => target.cluster.as_ref(),
            Self::Ps { locator, .. } => locator.as_ref(),
            Self::Show { reference } => reference.cluster.as_ref(),
            Self::Serve | Self::Sqlite { .. } => None,
        }
    }
}

#[derive(Debug, Subcommand)]
enum SqliteCommand {
    /// Open an interactive SQLite shell
    Repl {
        #[arg(default_value = DEFAULT_SQLITE_DATABASE)]
        database: PathBuf,
    },
    /// Execute SQL against a local database file
    Query { database: PathBuf, sql: String },
}

struct ConnectionArgs {
    join: Option<JoinToken>,
    carrier: CarrierSpec,
    identity: IdentityKind,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let log_level = cli
        .log_level
        .parse::<tracing::level_filters::LevelFilter>()
        .context("parse --log-level")?;
    tracing_subscriber::fmt()
        .with_max_level(log_level)
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|error| anyhow!("initialize diagnostics: {error}"))?;
    run(cli).await
}

async fn run(cli: Cli) -> Result<()> {
    let Cli {
        log_level: _,
        cluster,
        join,
        carrier,
        identity,
        labels,
        command,
    } = cli;
    if matches!(&command, Command::Sqlite { .. })
        && (cluster.is_some()
            || join.is_some()
            || carrier.is_some()
            || identity.is_some()
            || !labels.is_empty())
    {
        bail!("SQLite commands do not accept mesh connection options");
    }
    let cluster = match (cluster, join) {
        (Some(cluster), None) => Some(cluster),
        (None, Some(cluster)) => {
            warn!("--join is deprecated; use --cluster");
            Some(cluster)
        }
        (None, None) => None,
        (Some(_), Some(_)) => unreachable!("clap rejects --cluster with --join"),
    };
    let command = match command {
        Command::Sqlite { command: None } => Command::Sqlite {
            command: Some(SqliteCommand::Repl {
                database: PathBuf::from(DEFAULT_SQLITE_DATABASE),
            }),
        },
        command => command,
    };
    let command = match command {
        Command::Sqlite {
            command: Some(SqliteCommand::Query { database, sql }),
        } => return sqlite_query(&database, &sql).await,
        Command::Sqlite {
            command: Some(SqliteCommand::Repl { database }),
        } => return sqlite_repl(&database).await,
        command => command,
    };
    let selected_cluster = command.cluster().cloned().or(cluster);
    let connection = resolve_connection(selected_cluster, carrier, identity).await?;
    let labels = cli_labels(labels, connection.join.is_some(), &command)?;
    let ConnectionArgs {
        join,
        carrier,
        identity: identity_kind,
    } = connection;
    let identity = match identity_kind {
        IdentityKind::Ephemeral => identity::generate().await?,
        IdentityKind::Meta => chrysalis_identity_meta::issue_host().await?,
    };
    let pid = identity.pid();
    let advertise_to = join.as_ref().map(|parent| match parent {
        JoinToken::Pinned(parent) => &parent.address,
        JoinToken::Discover(address) => address,
    });
    let bindings = BoundSockets::bind(&carrier, pid, advertise_to).await?;
    let address = NodeAddr {
        pid,
        address: bindings.address.clone(),
    };
    let mut config = NodeConfig::new(TransportConfig::new(bindings.socket.clone(), identity))
        .with_labels(labels)
        .with_locators(vec![Locator {
            address: bindings.address.datagram_addr(),
            priority: 0,
        }]);
    if let Some(parent) = &join {
        let parent = match parent {
            JoinToken::Pinned(parent) => NamespaceConfig::try_new(
                parent.pid,
                vec![ParentEndpoint::new(parent.address.datagram_addr())],
            )?,
            JoinToken::Discover(address) => {
                NamespaceConfig::try_discover(vec![ParentEndpoint::new(address.datagram_addr())])?
            }
        };
        config = config.with_parent(parent);
    }
    let node = Arc::new(Node::create(config)?);
    wait_until_ready(&node).await?;
    let result = match command {
        Command::Serve => serve(node.clone(), address).await,
        Command::Cat { target } => cat(&node, target).await,
        Command::Ps { full, .. } => ps(&node, full).await,
        Command::Show { reference } => show(&node, reference).await,
        Command::Sqlite { .. } => unreachable!("SQLite commands returned before node construction"),
    };
    node.shutdown();
    node.join().await;
    drop(bindings);
    result
}

fn cli_labels(labels: Vec<LabelArg>, joined: bool, command: &Command) -> Result<Labels> {
    let automatic = [
        ("client".to_owned(), "chrysalis".to_owned()),
        ("command".to_owned(), command.label().to_owned()),
        (
            "role".to_owned(),
            if joined { "leaf" } else { "root" }.to_owned(),
        ),
    ];
    Ok(Labels::try_from_iter(
        automatic.into_iter().chain(
            labels
                .into_iter()
                .map(|label| (label.key.to_string(), label.value.to_string())),
        ),
    )?)
}

async fn resolve_connection(
    cluster: Option<ClusterLocator>,
    carrier: Option<CarrierSpec>,
    identity: Option<IdentityKind>,
) -> Result<ConnectionArgs> {
    let (join, resolved) = match cluster {
        Some(ClusterLocator::Direct(join)) => (Some(join), None),
        Some(ClusterLocator::Resolver(resolver)) => {
            let resolved = resolver.resolve().await?;
            let join = JoinToken::Discover(crate::address::CarrierAddr::Udp(resolved.join()));
            (Some(join), Some(resolved))
        }
        None => (None, None),
    };
    let carrier = match carrier {
        Some(carrier) => carrier,
        None => resolved
            .as_ref()
            .map(|resolved| CarrierSpec::Udp(resolved.carrier().to_string()))
            .unwrap_or_else(|| CarrierSpec::Udp("127.0.0.1:0".into())),
    };
    let identity = identity
        .or_else(|| resolved.as_ref().map(|resolved| resolved.identity().into()))
        .unwrap_or(IdentityKind::Ephemeral);
    Ok(ConnectionArgs {
        join,
        carrier,
        identity,
    })
}

impl From<IdentityProvider> for IdentityKind {
    fn from(provider: IdentityProvider) -> Self {
        match provider {
            IdentityProvider::Meta => Self::Meta,
        }
    }
}

async fn open_sqlite(path: &Path) -> Result<Connection> {
    let path = path
        .to_str()
        .with_context(|| format!("database path is not UTF-8: {}", path.display()))?;
    let database = Builder::new_local(path)
        .build()
        .await
        .with_context(|| format!("open SQLite database {path}"))?;
    database
        .connect()
        .with_context(|| format!("connect to SQLite database {path}"))
}

async fn sqlite_query(database: &Path, sql: &str) -> Result<()> {
    let connection = open_sqlite(database).await?;
    let output = sqlite_shell::execute(&connection, sql)
        .await
        .with_context(|| format!("execute SQL against {}", database.display()))?;
    print!("{output}");
    std::io::stdout()
        .flush()
        .context("flush SQLite query output")?;
    Ok(())
}

async fn sqlite_repl(database: &Path) -> Result<()> {
    let connection = open_sqlite(database).await?;
    sqlite_shell::run(&connection).await
}

async fn wait_until_ready(node: &Node) -> Result<Option<Pid>> {
    let Some(mut parent) = node.subscribe_parent() else {
        info!(pid = %format_pid(node.pid()), "started root");
        return Ok(None);
    };
    loop {
        let status = parent.borrow().clone();
        match status {
            ParentManagerStatus::Connected { peer, address, .. } => {
                info!(
                    peer = %format_pid(peer),
                    address = %format_datagram_addr(&address),
                    "joined parent"
                );
                return Ok(Some(peer));
            }
            ParentManagerStatus::Stopped => anyhow::bail!("parent manager stopped while joining"),
            ParentManagerStatus::Failed { error } => anyhow::bail!(error),
            ParentManagerStatus::Connecting => {}
        }
        tokio::select! {
            result = parent.changed() => {
                result.context("parent manager stopped while joining")?;
            }
            result = tokio::signal::ctrl_c() => {
                result.context("wait for interrupt")?;
                anyhow::bail!("interrupted while joining parent");
            }
        }
    }
}

async fn serve(node: Arc<Node>, address: NodeAddr) -> Result<()> {
    println!("{address}");
    std::io::stdout().flush().context("flush join token")?;
    info!(pid = %format_pid(node.pid()), address = %address.address, "serving streams");
    let mut streams = JoinSet::new();
    loop {
        tokio::select! {
            incoming = node.accept() => {
                let incoming = incoming.context("accept application stream")?;
                info!(source = %format_pid(incoming.source()), "accepted stream");
                streams.spawn(handle_stream(incoming));
            }
            completed = streams.join_next(), if !streams.is_empty() => {
                match completed.expect("stream set is not empty") {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => error!(%error, "stream failed"),
                    Err(error) => error!(%error, "stream task failed"),
                }
            }
            result = tokio::signal::ctrl_c() => {
                result.context("wait for interrupt")?;
                info!(pid = %format_pid(node.pid()), "shutting down");
                node.shutdown();
                break;
            }
        }
    }
    while let Some(result) = streams.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => error!(%error, "stream failed during shutdown"),
            Err(error) => error!(%error, "stream task failed during shutdown"),
        }
    }
    Ok(())
}

async fn handle_stream(incoming: IncomingStream) -> Result<()> {
    let source = incoming.source();
    let (_, stream) = incoming.into_parts();
    let (mut send, mut recv) = stream.into_parts();
    let (echo_sender, mut echo_receiver) = mpsc::channel::<Vec<u8>>(ECHO_QUEUE_CAPACITY);
    let mut echo_task = tokio::spawn(async move {
        while let Some(bytes) = echo_receiver.recv().await {
            send.write_all(&bytes)
                .await
                .with_context(|| format!("echo stream to {}", format_pid(source)))?;
        }
        send.finish().await?;
        Result::<()>::Ok(())
    });
    let mut stdout = tokio::io::stdout();
    let mut buffer = [0; 16 * 1024];
    loop {
        let length = tokio::select! {
            result = AsyncReadExt::read(&mut recv, &mut buffer) => {
                result.with_context(|| format!("read stream from {}", format_pid(source)))?
            }
            result = &mut echo_task => {
                return result.context("join echo writer task")?;
            }
        };
        if length == 0 {
            break;
        }
        stdout.write_all(&buffer[..length]).await?;
        stdout.flush().await?;
        match echo_sender.try_send(buffer[..length].to_vec()) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                echo_task.abort();
                let _ = echo_task.await;
                bail!("peer {} stopped draining echoed data", format_pid(source));
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return echo_task.await.context("join echo writer task")?;
            }
        }
    }
    drop(echo_sender);
    echo_task.await.context("join echo writer task")??;
    info!(source = %format_pid(source), "closed stream");
    Ok(())
}

async fn cat(node: &Node, target: Reference) -> Result<()> {
    let target = node
        .expand_pid(target.pid, ResolveConsistency::Refresh)
        .await
        .with_context(|| format!("resolve PID prefix {}", target.pid))?;
    let stream = node
        .dial(target, ResolveConsistency::Refresh)
        .await
        .with_context(|| format!("dial {}", format_pid(target)))?;
    info!(target = %format_pid(target), "connected stream");
    let (mut send, mut recv) = stream.into_parts();
    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let upload = async {
        tokio::io::copy(&mut stdin, &mut send).await?;
        send.finish().await?;
        Result::<()>::Ok(())
    };
    let download = async {
        tokio::io::copy(&mut recv, &mut stdout).await?;
        stdout.flush().await?;
        Result::<()>::Ok(())
    };
    tokio::try_join!(upload, download)?;
    Ok(())
}

async fn show(node: &Node, reference: Reference) -> Result<()> {
    let prefix = reference.pid;
    let pid = node
        .expand_pid(prefix, ResolveConsistency::Refresh)
        .await
        .with_context(|| format!("resolve PID prefix {prefix}"))?;
    let Resolution::Found { entry, .. } = node
        .resolve(pid, ResolveConsistency::Refresh)
        .await
        .with_context(|| format!("resolve process {}", format_pid(pid)))?
    else {
        anyhow::bail!("process disappeared while resolving {}", format_pid(pid));
    };
    print!("{}", format_process_entry(entry));
    std::io::stdout().flush().context("flush process entry")?;
    Ok(())
}

async fn ps(node: &Node, full: bool) -> Result<()> {
    let entries = node
        .enumerate(ResolveConsistency::Refresh)
        .await
        .context("enumerate namespace")?;
    print!("{}", format_process_table(entries, full));
    std::io::stdout().flush().context("flush process table")?;
    Ok(())
}

fn format_process_table(mut entries: Vec<ProcEntry>, full: bool) -> String {
    entries.sort_by_key(|entry| entry.pid);
    let mut rows = Vec::new();
    for entry in entries {
        let pid = format_pid(entry.pid);
        let pid = if full { pid } else { pid[..8].to_owned() };
        let labels = if entry.labels.is_empty() {
            "-".to_owned()
        } else {
            entry
                .labels
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(",")
        };
        if entry.locators.is_empty() {
            rows.push((pid, "-".to_owned(), labels, "-".to_owned()));
            continue;
        }
        let mut locators = entry.locators;
        locators.sort_by_key(|locator| locator.priority);
        for locator in locators {
            rows.push((
                pid.clone(),
                locator.priority.to_string(),
                labels.clone(),
                format_datagram_addr(&locator.address),
            ));
        }
    }

    let pid_width = rows
        .iter()
        .map(|(pid, _, _, _)| pid.len())
        .max()
        .unwrap_or(0)
        .max("PID".len());
    let priority_width = rows
        .iter()
        .map(|(_, priority, _, _)| priority.len())
        .max()
        .unwrap_or(0)
        .max("PRIORITY".len());
    let labels_width = rows
        .iter()
        .map(|(_, _, labels, _)| labels.len())
        .max()
        .unwrap_or(0)
        .max("LABELS".len());
    let mut output = String::new();
    use std::fmt::Write as _;
    writeln!(
        &mut output,
        "{:<pid_width$}  {:>priority_width$}  {:<labels_width$}  ADDRESS",
        "PID", "PRIORITY", "LABELS"
    )
    .expect("writing to a string cannot fail");
    for (pid, priority, labels, address) in rows {
        writeln!(
            &mut output,
            "{pid:<pid_width$}  {priority:>priority_width$}  {labels:<labels_width$}  {address}"
        )
        .expect("writing to a string cannot fail");
    }
    output
}

fn format_process_entry(mut entry: ProcEntry) -> String {
    use std::fmt::Write as _;

    let mut output = String::new();
    writeln!(&mut output, "pid: {}", format_pid(entry.pid))
        .expect("writing to a string cannot fail");
    writeln!(&mut output, "tls_server_name: {}", entry.tls_server_name)
        .expect("writing to a string cannot fail");
    if entry.labels.is_empty() {
        writeln!(&mut output, "labels: []").expect("writing to a string cannot fail");
    } else {
        writeln!(&mut output, "labels:").expect("writing to a string cannot fail");
        for (key, value) in entry.labels.iter() {
            writeln!(&mut output, "  {key}={value}").expect("writing to a string cannot fail");
        }
    }
    if entry.locators.is_empty() {
        writeln!(&mut output, "locators: []").expect("writing to a string cannot fail");
    } else {
        writeln!(&mut output, "locators:").expect("writing to a string cannot fail");
        entry.locators.sort_by_key(|locator| locator.priority);
        for locator in entry.locators {
            writeln!(&mut output, "  - priority: {}", locator.priority)
                .expect("writing to a string cannot fail");
            writeln!(
                &mut output,
                "    address: {}",
                format_datagram_addr(&locator.address)
            )
            .expect("writing to a string cannot fail");
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use chrysalis::DatagramAddr;
    use chrysalis::Labels;
    use chrysalis::Locator;
    use chrysalis::Pid;
    use chrysalis_resolver::ResolverSpec;

    use super::*;

    #[test]
    fn bare_sqlite_command_uses_no_explicit_nested_command() {
        let cli = Cli::try_parse_from(["chrysalis", "sqlite"]).unwrap();
        assert!(matches!(cli.command, Command::Sqlite { command: None }));
    }

    #[test]
    fn top_level_command_is_required() {
        assert!(Cli::try_parse_from(["chrysalis"]).is_err());
    }

    #[test]
    fn cli_nodes_include_command_and_topology_labels() {
        let labels = cli_labels(
            vec!["team=runtime".parse().expect("parse user label")],
            true,
            &Command::Ps {
                locator: None,
                full: false,
            },
        )
        .expect("construct CLI labels");
        assert_eq!(
            labels
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>(),
            [
                "client=chrysalis",
                "command=ps",
                "role=leaf",
                "team=runtime",
            ]
        );
        assert!(cli_labels(Vec::new(), false, &Command::Serve).is_ok());
        assert!(
            cli_labels(
                vec!["role=custom".parse().expect("parse conflicting label")],
                true,
                &Command::Ps {
                    locator: None,
                    full: false,
                },
            )
            .is_err()
        );
    }

    #[test]
    fn deprecated_join_alias_still_selects_a_cluster() {
        let cli = Cli::try_parse_from(["chrysalis", "--join", "mast://scale_job", "ps"])
            .expect("parse MAST resolver");
        assert_eq!(
            cli.join,
            Some(ClusterLocator::Resolver(ResolverSpec::Mast {
                job: "scale_job".into()
            }))
        );
        assert_eq!(cli.cluster, None);
        assert_eq!(cli.carrier, None);
        assert_eq!(cli.identity, None);
    }

    #[test]
    fn command_locators_override_the_global_cluster() {
        let cli = Cli::try_parse_from([
            "chrysalis",
            "--cluster",
            "mast://global",
            "cat",
            "4242@mast://override",
        ])
        .expect("parse qualified reference");
        let Command::Cat { target } = &cli.command else {
            panic!("cat command must parse");
        };
        assert_eq!(
            target.cluster,
            Some(ClusterLocator::Resolver(ResolverSpec::Mast {
                job: "override".into()
            }))
        );
        assert_eq!(
            cli.command.cluster(),
            target.cluster.as_ref(),
            "qualified reference should select its locator"
        );

        let ps = Cli::try_parse_from(["chrysalis", "ps", "mast://scale_job", "--full"])
            .expect("parse qualified ps");
        assert!(matches!(
            ps.command,
            Command::Ps {
                locator: Some(ClusterLocator::Resolver(ResolverSpec::Mast { .. })),
                full: true,
            }
        ));
        Cli::try_parse_from(["chrysalis", "show", "4242@mast://scale_job"])
            .expect("parse qualified show");
    }

    #[tokio::test]
    async fn direct_connections_preserve_local_defaults_and_overrides() {
        let defaults = resolve_connection(None, None, None)
            .await
            .expect("resolve local defaults");
        assert_eq!(defaults.join, None);
        assert_eq!(defaults.carrier, CarrierSpec::Udp("127.0.0.1:0".into()));
        assert_eq!(defaults.identity, IdentityKind::Ephemeral);

        let direct = "udp://127.0.0.1:26600"
            .parse()
            .expect("parse direct cluster");
        let explicit_carrier = CarrierSpec::Udp("0.0.0.0:0".into());
        let resolved = resolve_connection(
            Some(ClusterLocator::Direct(direct)),
            Some(explicit_carrier.clone()),
            Some(IdentityKind::Meta),
        )
        .await
        .expect("resolve direct connection");
        assert_eq!(resolved.carrier, explicit_carrier);
        assert_eq!(resolved.identity, IdentityKind::Meta);
    }

    #[test]
    fn process_table_sorts_processes_and_locators() {
        let first = Pid::from_bytes([1; 16]);
        let second = Pid::from_bytes([2; 16]);
        let entries = vec![
            ProcEntry {
                pid: second,
                tls_server_name: "second.test".into(),
                labels: Labels::new(),
                locators: Vec::new(),
            },
            ProcEntry {
                pid: first,
                tls_server_name: "first.test".into(),
                labels: Labels::try_from_iter([
                    ("app.kubernetes.io/name", "api"),
                    ("tier", "frontend"),
                ])
                .expect("valid labels"),
                locators: vec![
                    Locator {
                        address: DatagramAddr::new("test", [2]),
                        priority: 20,
                    },
                    Locator {
                        address: DatagramAddr::new("test", [1]),
                        priority: 10,
                    },
                ],
            },
        ];
        let table = format_process_table(entries.clone(), false);

        assert_eq!(
            table,
            concat!(
                "PID       PRIORITY  LABELS                                    ADDRESS\n",
                "01010101        10  app.kubernetes.io/name=api,tier=frontend  test://01\n",
                "01010101        20  app.kubernetes.io/name=api,tier=frontend  test://02\n",
                "02020202         -  -                                         -\n",
            )
        );
        assert!(format_process_table(entries, true).contains("01010101010101010101010101010101"));
    }

    #[test]
    fn process_entry_renders_every_nameserver_field() {
        let entry = ProcEntry {
            pid: Pid::from_bytes([1; 16]),
            tls_server_name: "first.test".into(),
            labels: Labels::try_from_iter([("role", "leaf"), ("rank", "7")]).expect("valid labels"),
            locators: vec![
                Locator {
                    address: DatagramAddr::new("test", [2]),
                    priority: 20,
                },
                Locator {
                    address: DatagramAddr::new("test", [1]),
                    priority: 10,
                },
            ],
        };

        assert_eq!(
            format_process_entry(entry),
            concat!(
                "pid: 01010101010101010101010101010101\n",
                "tls_server_name: first.test\n",
                "labels:\n",
                "  rank=7\n",
                "  role=leaf\n",
                "locators:\n",
                "  - priority: 10\n",
                "    address: test://01\n",
                "  - priority: 20\n",
                "    address: test://02\n",
            )
        );
    }
}
