/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use chrysalis::NodeConfig;
use chrysalis::Pid;
use chrysalis::QuicConnectionStats;
use chrysalis::QuicIoStats;
use chrysalis_sqlite::Replica;
use chrysalis_sqlite::ReplicationTopology;
use chrysalis_sqlite::TableSchema;
use libsql::Builder;
use libsql::Connection;
use libsql::TransactionBehavior;
use libsql::Value;

const NODES_SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS nodes (
        rank INTEGER PRIMARY KEY NOT NULL,
        pid BLOB NOT NULL DEFAULT X'',
        task_id INTEGER NOT NULL DEFAULT 0,
        task_handle TEXT NOT NULL DEFAULT '',
        hostname TEXT NOT NULL DEFAULT '',
        address TEXT NOT NULL DEFAULT '',
        is_root INTEGER NOT NULL DEFAULT 0,
        parent_pid BLOB NOT NULL DEFAULT X'00000000000000000000000000000000',
        level INTEGER NOT NULL DEFAULT 0,
        expected_nodes INTEGER NOT NULL DEFAULT 0,
        nodes_per_task INTEGER NOT NULL DEFAULT 0,
        started_at_ms INTEGER NOT NULL DEFAULT 0
    )";
const EXPERIMENTS_SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS experiments (
        name TEXT PRIMARY KEY NOT NULL DEFAULT '',
        pid BLOB NOT NULL DEFAULT X'',
        kind TEXT NOT NULL DEFAULT 'echo',
        selection TEXT NOT NULL DEFAULT 'count',
        count INTEGER NOT NULL DEFAULT 0,
        size INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'pending',
        claim_attempt INTEGER NOT NULL DEFAULT 0,
        lease_until_ms INTEGER NOT NULL DEFAULT 0
    )";
const EXPERIMENT_TARGETS_SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS experiment_targets (
        experiment_name TEXT NOT NULL DEFAULT '',
        position INTEGER NOT NULL DEFAULT 0,
        pid BLOB NOT NULL DEFAULT X'',
        PRIMARY KEY (experiment_name, position)
    )";
const RESULTS_SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS results (
        experiment_name TEXT PRIMARY KEY NOT NULL DEFAULT '',
        pid BLOB NOT NULL DEFAULT X'',
        status TEXT NOT NULL DEFAULT '',
        count INTEGER NOT NULL DEFAULT 0,
        size INTEGER NOT NULL DEFAULT 0,
        completed INTEGER NOT NULL DEFAULT 0,
        started_at_ms INTEGER NOT NULL DEFAULT 0,
        finished_at_ms INTEGER NOT NULL DEFAULT 0,
        warmup_seconds REAL NOT NULL DEFAULT 0,
        operation_seconds REAL NOT NULL DEFAULT 0,
        operations_per_second REAL NOT NULL DEFAULT 0,
        mean_latency_millis REAL NOT NULL DEFAULT 0,
        max_latency_millis REAL NOT NULL DEFAULT 0,
        transmit_calls INTEGER NOT NULL DEFAULT 0,
        transmit_datagrams INTEGER NOT NULL DEFAULT 0,
        transmit_bytes INTEGER NOT NULL DEFAULT 0,
        transmit_blocked INTEGER NOT NULL DEFAULT 0,
        receive_calls INTEGER NOT NULL DEFAULT 0,
        receive_datagrams INTEGER NOT NULL DEFAULT 0,
        receive_bytes INTEGER NOT NULL DEFAULT 0,
        connection_rtt_micros INTEGER NOT NULL DEFAULT 0,
        connection_congestion_window INTEGER NOT NULL DEFAULT 0,
        connection_congestion_events INTEGER NOT NULL DEFAULT 0,
        connection_lost_packets INTEGER NOT NULL DEFAULT 0,
        connection_lost_bytes INTEGER NOT NULL DEFAULT 0,
        connection_sent_packets INTEGER NOT NULL DEFAULT 0,
        connection_mtu INTEGER NOT NULL DEFAULT 0,
        error TEXT NOT NULL DEFAULT ''
    )";

const NODE_COLUMNS: &[&str] = &[
    "rank",
    "pid",
    "task_id",
    "task_handle",
    "hostname",
    "address",
    "is_root",
    "parent_pid",
    "level",
    "expected_nodes",
    "nodes_per_task",
    "started_at_ms",
];
const EXPERIMENT_COLUMNS: &[&str] = &[
    "name",
    "pid",
    "kind",
    "selection",
    "count",
    "size",
    "status",
    "claim_attempt",
    "lease_until_ms",
];
const TARGET_COLUMNS: &[&str] = &["experiment_name", "position", "pid"];
const RESULT_COLUMNS: &[&str] = &[
    "experiment_name",
    "pid",
    "status",
    "count",
    "size",
    "completed",
    "started_at_ms",
    "finished_at_ms",
    "warmup_seconds",
    "operation_seconds",
    "operations_per_second",
    "mean_latency_millis",
    "max_latency_millis",
    "transmit_calls",
    "transmit_datagrams",
    "transmit_bytes",
    "transmit_blocked",
    "receive_calls",
    "receive_datagrams",
    "receive_bytes",
    "connection_rtt_micros",
    "connection_congestion_window",
    "connection_congestion_events",
    "connection_lost_packets",
    "connection_lost_bytes",
    "connection_sent_packets",
    "connection_mtu",
    "error",
];

#[derive(Clone)]
pub(crate) struct ExperimentStore {
    connection: Connection,
    replica: Replica,
}

pub(crate) struct NodeRecord {
    pub(crate) rank: usize,
    pub(crate) pid: Pid,
    pub(crate) task_id: usize,
    pub(crate) task_handle: String,
    pub(crate) hostname: String,
    pub(crate) address: String,
    pub(crate) is_root: bool,
    pub(crate) parent_pid: Option<Pid>,
    pub(crate) level: u8,
    pub(crate) expected_nodes: usize,
    pub(crate) nodes_per_task: usize,
    pub(crate) started_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Experiment {
    pub(crate) pid: Pid,
    pub(crate) name: String,
    pub(crate) kind: ExperimentKind,
    pub(crate) targets: ExperimentTargets,
    pub(crate) size: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExperimentKind {
    Echo,
    Delivery,
}

impl ExperimentKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Echo => "echo",
            Self::Delivery => "delivery",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "echo" => Ok(Self::Echo),
            "delivery" => Ok(Self::Delivery),
            _ => anyhow::bail!("invalid experiment kind {value:?}"),
        }
    }

    pub(crate) const fn payload_multiplier(self) -> usize {
        match self {
            Self::Echo => 2,
            Self::Delivery => 1,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ExperimentTargets {
    Count(i64),
    Explicit(Vec<Pid>),
}

impl ExperimentTargets {
    pub(crate) const fn selection(&self) -> &'static str {
        match self {
            Self::Count(_) => "count",
            Self::Explicit(_) => "targeted",
        }
    }

    pub(crate) fn count(&self) -> i64 {
        match self {
            Self::Count(count) => *count,
            Self::Explicit(targets) => {
                i64::try_from(targets.len()).expect("target count exceeds i64")
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "used by persistent administration in the next stack change"
    )
)]
pub(crate) enum ExperimentStatus {
    Pending,
    Processing,
    Done,
}

#[expect(
    dead_code,
    reason = "used by persistent administration in the next stack change"
)]
impl ExperimentStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Processing => "processing",
            Self::Done => "done",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "processing" => Ok(Self::Processing),
            "done" => Ok(Self::Done),
            _ => anyhow::bail!("invalid experiment status {value:?}"),
        }
    }
}

pub(crate) struct ExperimentResult {
    pub(crate) experiment: Experiment,
    pub(crate) claim_attempt: i64,
    pub(crate) completed: usize,
    pub(crate) started_at_ms: i64,
    pub(crate) finished_at_ms: i64,
    pub(crate) warmup_elapsed: Duration,
    pub(crate) elapsed: Duration,
    pub(crate) mean_latency: Duration,
    pub(crate) max_latency: Duration,
    pub(crate) io_stats: QuicIoStats,
    pub(crate) connection_stats: QuicConnectionStats,
    pub(crate) error: Option<String>,
}

pub(crate) struct ExperimentClaim {
    pub(crate) experiment: Experiment,
    pub(crate) attempt: i64,
}

fn table_schemas() -> Result<Vec<TableSchema>> {
    Ok(vec![
        TableSchema::try_new("nodes", NODES_SCHEMA, NODE_COLUMNS, &["rank"])?,
        TableSchema::try_new(
            "experiments",
            EXPERIMENTS_SCHEMA,
            EXPERIMENT_COLUMNS,
            &["name"],
        )?,
        TableSchema::try_new(
            "experiment_targets",
            EXPERIMENT_TARGETS_SCHEMA,
            TARGET_COLUMNS,
            &["experiment_name", "position"],
        )?,
        TableSchema::try_new(
            "results",
            RESULTS_SCHEMA,
            RESULT_COLUMNS,
            &["experiment_name"],
        )?,
    ])
}

#[expect(
    dead_code,
    reason = "used by persistent administration in the next stack change"
)]
impl ExperimentStore {
    pub(crate) async fn open(path: &Path) -> Result<Self> {
        let path_str = path
            .to_str()
            .with_context(|| format!("database path is not UTF-8: {}", path.display()))?;
        let database = Builder::new_local(path_str)
            .build()
            .await
            .with_context(|| format!("open SQLite database {path_str}"))?;
        let connection = database
            .connect()
            .with_context(|| format!("connect to SQLite database {path_str}"))?;
        let replica = Replica::new(connection.clone(), table_schemas()?)
            .await
            .with_context(|| format!("initialize replicated SQLite at {path_str}"))?;
        Ok(Self {
            connection,
            replica,
        })
    }

    pub(crate) async fn configure(&self, config: NodeConfig) -> Result<NodeConfig> {
        Ok(ReplicationTopology::new(self.replica.clone()).configure(config))
    }

    pub(crate) fn connection(&self) -> &Connection {
        &self.connection
    }

    pub(crate) async fn register_node(&self, node: NodeRecord) -> Result<()> {
        let mut transaction = self
            .replica
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .context("begin node registration")?;
        let rank = i64::try_from(node.rank).context("node rank overflow")?;
        let mut rows = transaction
            .query(
                "SELECT pid FROM nodes WHERE rank = ?1",
                vec![Value::Integer(rank)],
            )
            .await
            .context("query prior scale node")?;
        let old_pid = rows
            .next()
            .await
            .context("read prior scale node")?
            .map(|row| row.get::<Vec<u8>>(0))
            .transpose()?;
        drop(rows);
        let mut migrated_experiments = Vec::new();
        let mut migrated_targets = Vec::new();
        if let Some(old_pid) = old_pid.filter(|old| old != node.pid.as_bytes()) {
            (migrated_experiments, migrated_targets) =
                migration_keys(&transaction, &old_pid).await?;
            let new_pid = node.pid.as_bytes().to_vec();
            transaction
                .execute(
                    "UPDATE experiments SET pid = ?1, lease_until_ms = 0 \
                     WHERE pid = ?2 AND status != 'done'",
                    vec![Value::Blob(new_pid.clone()), Value::Blob(old_pid.clone())],
                )
                .await
                .context("migrate experiment source PID")?;
            transaction
                .execute(
                    "UPDATE experiment_targets SET pid = ?1 \
                     WHERE pid = ?2 AND EXISTS (\
                         SELECT 1 FROM experiments \
                         WHERE experiments.name = experiment_targets.experiment_name \
                           AND experiments.status != 'done'\
                     )",
                    vec![Value::Blob(new_pid), Value::Blob(old_pid)],
                )
                .await
                .context("migrate experiment target PID")?;
        }
        transaction
            .execute(
                "INSERT INTO nodes (\
                    rank, pid, task_id, task_handle, hostname, address, is_root, \
                    parent_pid, level, expected_nodes, nodes_per_task, started_at_ms\
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12) \
                 ON CONFLICT (rank) DO UPDATE SET \
                    pid = excluded.pid, \
                    task_id = excluded.task_id, \
                    task_handle = excluded.task_handle, \
                    hostname = excluded.hostname, \
                    address = excluded.address, \
                    is_root = excluded.is_root, \
                    parent_pid = excluded.parent_pid, \
                    level = excluded.level, \
                    expected_nodes = excluded.expected_nodes, \
                    nodes_per_task = excluded.nodes_per_task, \
                    started_at_ms = excluded.started_at_ms",
                vec![
                    Value::Integer(rank),
                    Value::Blob(node.pid.as_bytes().to_vec()),
                    Value::Integer(i64::try_from(node.task_id).context("task ID overflow")?),
                    Value::Text(node.task_handle),
                    Value::Text(node.hostname),
                    Value::Text(node.address),
                    Value::Integer(i64::from(node.is_root)),
                    Value::Blob(
                        node.parent_pid
                            .unwrap_or(Pid::LINK_LOCAL)
                            .as_bytes()
                            .to_vec(),
                    ),
                    Value::Integer(i64::from(node.level)),
                    Value::Integer(
                        i64::try_from(node.expected_nodes)
                            .context("expected node count overflow")?,
                    ),
                    Value::Integer(
                        i64::try_from(node.nodes_per_task).context("nodes per task overflow")?,
                    ),
                    Value::Integer(node.started_at_ms),
                ],
            )
            .await
            .context("register scale node")?;
        transaction
            .capture_upsert("nodes", vec![Value::Integer(rank)])
            .await
            .context("capture node registration")?;
        for key in migrated_experiments {
            transaction
                .capture_upsert("experiments", key)
                .await
                .context("capture migrated experiment")?;
        }
        for key in migrated_targets {
            transaction
                .capture_upsert("experiment_targets", key)
                .await
                .context("capture migrated experiment target")?;
        }
        transaction
            .commit()
            .await
            .context("commit node registration")?;
        Ok(())
    }

    pub(crate) async fn nodes(&self) -> Result<Vec<Pid>> {
        let capacity = self.node_count().await?;
        let mut rows = self
            .connection
            .query("SELECT pid FROM nodes ORDER BY rank", ())
            .await
            .context("query scale nodes")?;
        let mut nodes = Vec::with_capacity(capacity);
        while let Some(row) = rows.next().await.context("read scale node")? {
            let bytes: Vec<u8> = row.get(0)?;
            nodes.push(decode_pid(bytes)?);
        }
        Ok(nodes)
    }

    pub(crate) async fn node_count_for_run(&self, expected: usize) -> Result<usize> {
        let expected = i64::try_from(expected).context("expected node count exceeds i64")?;
        let mut rows = self
            .connection
            .query(
                "SELECT COUNT(*) FROM nodes \
                 WHERE rank >= 0 AND rank < ?1 AND expected_nodes = ?1",
                vec![Value::Integer(expected)],
            )
            .await
            .context("count scale nodes for current run")?;
        let count: i64 = rows
            .next()
            .await
            .context("read scale node count")?
            .expect("aggregate query returns one row")
            .get(0)?;
        usize::try_from(count).context("scale node count is invalid")
    }

    pub(crate) async fn nodes_for_run(&self, expected: usize) -> Result<Vec<Pid>> {
        let capacity = expected;
        let expected = i64::try_from(expected).context("expected node count exceeds i64")?;
        let mut rows = self
            .connection
            .query(
                "SELECT pid FROM nodes \
                 WHERE rank >= 0 AND rank < ?1 AND expected_nodes = ?1 \
                 ORDER BY rank",
                vec![Value::Integer(expected)],
            )
            .await
            .context("query scale nodes for current run")?;
        let mut nodes = Vec::with_capacity(capacity);
        while let Some(row) = rows.next().await.context("read scale node")? {
            let bytes: Vec<u8> = row.get(0)?;
            nodes.push(decode_pid(bytes)?);
        }
        Ok(nodes)
    }

    async fn node_count(&self) -> Result<usize> {
        let mut rows = self
            .connection
            .query("SELECT COUNT(*) FROM nodes", ())
            .await
            .context("count scale nodes")?;
        let count: i64 = rows
            .next()
            .await
            .context("read scale node count")?
            .expect("aggregate query returns one row")
            .get(0)?;
        usize::try_from(count).context("scale node count is invalid")
    }

    pub(crate) async fn expected_nodes(&self) -> Result<Option<usize>> {
        let mut rows = self
            .connection
            .query("SELECT expected_nodes FROM nodes WHERE rank = 0", ())
            .await
            .context("query expected node count")?;
        let Some(row) = rows.next().await.context("read expected node count")? else {
            return Ok(None);
        };
        let expected: i64 = row.get(0)?;
        anyhow::ensure!(expected > 0, "root expected node count is invalid");
        Ok(Some(
            usize::try_from(expected).context("expected node count is invalid")?,
        ))
    }

    pub(crate) async fn has_node(&self, pid: Pid) -> Result<bool> {
        let mut rows = self
            .connection
            .query(
                "SELECT EXISTS(SELECT 1 FROM nodes WHERE pid = ?1)",
                vec![Value::Blob(pid.as_bytes().to_vec())],
            )
            .await
            .context("query scale node")?;
        let exists: i64 = rows
            .next()
            .await
            .context("read scale node")?
            .expect("scalar query returns one row")
            .get(0)?;
        Ok(exists != 0)
    }

    pub(crate) async fn add_experiment(&self, experiment: &Experiment) -> Result<()> {
        let mut transaction = self
            .replica
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .context("begin experiment transaction")?;
        transaction
            .execute(
                "INSERT INTO experiments (name, pid, kind, selection, count, size, status) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending')",
                vec![
                    Value::Text(experiment.name.clone()),
                    Value::Blob(experiment.pid.as_bytes().to_vec()),
                    Value::Text(experiment.kind.as_str().into()),
                    Value::Text(experiment.targets.selection().into()),
                    Value::Integer(experiment.targets.count()),
                    Value::Integer(experiment.size),
                ],
            )
            .await
            .context("add experiment")?;
        let mut target_keys = Vec::new();
        if let ExperimentTargets::Explicit(targets) = &experiment.targets {
            for (position, pid) in targets.iter().enumerate() {
                let position = i64::try_from(position).context("target position exceeds i64")?;
                transaction
                    .execute(
                        "INSERT INTO experiment_targets (experiment_name, position, pid) \
                         VALUES (?1, ?2, ?3)",
                        vec![
                            Value::Text(experiment.name.clone()),
                            Value::Integer(position),
                            Value::Blob(pid.as_bytes().to_vec()),
                        ],
                    )
                    .await
                    .context("add experiment target")?;
                target_keys.push(vec![
                    Value::Text(experiment.name.clone()),
                    Value::Integer(position),
                ]);
            }
        }
        transaction
            .capture_upsert("experiments", vec![Value::Text(experiment.name.clone())])
            .await
            .context("capture experiment")?;
        for key in target_keys {
            transaction
                .capture_upsert("experiment_targets", key)
                .await
                .context("capture experiment target")?;
        }
        transaction
            .commit()
            .await
            .context("commit experiment transaction")?;
        Ok(())
    }

    pub(crate) async fn experiment_status(&self, name: &str) -> Result<Option<ExperimentStatus>> {
        let mut rows = self
            .connection
            .query(
                "SELECT status FROM experiments WHERE name = ?1",
                vec![Value::Text(name.into())],
            )
            .await
            .context("query experiment status")?;
        let Some(row) = rows.next().await.context("read experiment status")? else {
            return Ok(None);
        };
        let status: String = row.get(0)?;
        Ok(Some(ExperimentStatus::parse(&status)?))
    }

    pub(crate) async fn claim_experiment(
        &self,
        pid: Pid,
        now_ms: i64,
        lease: Duration,
    ) -> Result<Option<ExperimentClaim>> {
        let lease_ms = i64::try_from(lease.as_millis()).context("experiment lease overflow")?;
        let lease_until_ms = now_ms
            .checked_add(lease_ms)
            .context("experiment lease deadline overflow")?;
        let mut transaction = self
            .replica
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .context("begin experiment claim")?;
        let mut rows = transaction
            .query(
                "SELECT name, kind, selection, count, size, claim_attempt \
                 FROM experiments \
                 WHERE pid = ?1 \
                   AND (status = 'pending' \
                        OR (status = 'processing' AND lease_until_ms <= ?2)) \
                 ORDER BY name \
                 LIMIT 1",
                vec![Value::Blob(pid.as_bytes().to_vec()), Value::Integer(now_ms)],
            )
            .await
            .context("query pending experiments")?;
        let Some(row) = rows.next().await.context("read pending experiment")? else {
            transaction.commit().await.context("commit empty claim")?;
            return Ok(None);
        };
        let name: String = row.get(0)?;
        let kind: String = row.get(1)?;
        let selection: String = row.get(2)?;
        let count: i64 = row.get(3)?;
        let size: i64 = row.get(4)?;
        let attempt: i64 = row.get(5)?;
        drop(rows);
        let kind = ExperimentKind::parse(&kind)?;
        let targets = match selection.as_str() {
            "count" => ExperimentTargets::Count(count),
            "targeted" => {
                let mut rows = transaction
                    .query(
                        "SELECT pid FROM experiment_targets \
                         WHERE experiment_name = ?1 ORDER BY position",
                        vec![Value::Text(name.clone())],
                    )
                    .await
                    .context("query experiment targets")?;
                let mut targets = Vec::new();
                while let Some(row) = rows.next().await.context("read experiment target")? {
                    targets.push(decode_pid(row.get(0)?)?);
                }
                anyhow::ensure!(
                    i64::try_from(targets.len()).context("target count exceeds i64")? == count,
                    "targeted experiment {name:?} has an inconsistent target count"
                );
                ExperimentTargets::Explicit(targets)
            }
            _ => anyhow::bail!("invalid experiment selection {selection:?}"),
        };
        let experiment = Experiment {
            pid,
            name,
            kind,
            targets,
            size,
        };
        let attempt = attempt
            .checked_add(1)
            .context("experiment claim attempt overflow")?;
        let updated = transaction
            .execute(
                "UPDATE experiments \
                 SET status = 'processing', claim_attempt = ?3, lease_until_ms = ?4 \
                 WHERE name = ?1 AND pid = ?2 \
                   AND (status = 'pending' \
                        OR (status = 'processing' AND lease_until_ms <= ?5))",
                vec![
                    Value::Text(experiment.name.clone()),
                    Value::Blob(pid.as_bytes().to_vec()),
                    Value::Integer(attempt),
                    Value::Integer(lease_until_ms),
                    Value::Integer(now_ms),
                ],
            )
            .await
            .context("mark experiment processing")?;
        anyhow::ensure!(updated == 1, "selected pending experiment disappeared");
        transaction
            .capture_upsert("experiments", vec![Value::Text(experiment.name.clone())])
            .await
            .context("capture experiment claim")?;
        transaction
            .commit()
            .await
            .context("commit experiment claim")?;
        Ok(Some(ExperimentClaim {
            experiment,
            attempt,
        }))
    }

    pub(crate) async fn record_result(&self, result: ExperimentResult) -> Result<()> {
        let status = if result.error.is_some() {
            "failed"
        } else {
            "complete"
        };
        let operations_per_second = if result.elapsed.is_zero() {
            0.0
        } else {
            result.completed as f64 / result.elapsed.as_secs_f64()
        };
        let mut transaction = self
            .replica
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .context("begin result transaction")?;
        let updated = transaction
            .execute(
                "UPDATE experiments \
                 SET status = 'done', lease_until_ms = 0 \
                 WHERE name = ?1 AND pid = ?2 AND status = 'processing' \
                   AND claim_attempt = ?3",
                vec![
                    Value::Text(result.experiment.name.clone()),
                    Value::Blob(result.experiment.pid.as_bytes().to_vec()),
                    Value::Integer(result.claim_attempt),
                ],
            )
            .await
            .context("mark experiment done")?;
        anyhow::ensure!(updated == 1, "experiment claim is no longer current");
        transaction
            .execute(
                "INSERT INTO results (\
                    experiment_name, pid, status, count, size, completed, \
                    started_at_ms, finished_at_ms, warmup_seconds, operation_seconds, \
                    operations_per_second, mean_latency_millis, max_latency_millis, \
                    transmit_calls, transmit_datagrams, transmit_bytes, transmit_blocked, \
                    receive_calls, receive_datagrams, receive_bytes, \
                    connection_rtt_micros, connection_congestion_window, \
                    connection_congestion_events, connection_lost_packets, \
                    connection_lost_bytes, connection_sent_packets, connection_mtu, error\
                 ) VALUES (\
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, \
                    ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, \
                    ?28\
                 )",
                vec![
                    Value::Text(result.experiment.name.clone()),
                    Value::Blob(result.experiment.pid.as_bytes().to_vec()),
                    Value::Text(status.into()),
                    Value::Integer(result.experiment.targets.count()),
                    Value::Integer(result.experiment.size),
                    Value::Integer(i64::try_from(result.completed).context("completed overflow")?),
                    Value::Integer(result.started_at_ms),
                    Value::Integer(result.finished_at_ms),
                    Value::Real(result.warmup_elapsed.as_secs_f64()),
                    Value::Real(result.elapsed.as_secs_f64()),
                    Value::Real(operations_per_second),
                    Value::Real(result.mean_latency.as_secs_f64() * 1000.0),
                    Value::Real(result.max_latency.as_secs_f64() * 1000.0),
                    Value::Integer(
                        i64::try_from(result.io_stats.transmit_calls)
                            .context("transmit call count overflow")?,
                    ),
                    Value::Integer(
                        i64::try_from(result.io_stats.transmit_datagrams)
                            .context("transmit datagram count overflow")?,
                    ),
                    Value::Integer(
                        i64::try_from(result.io_stats.transmit_bytes)
                            .context("transmit byte count overflow")?,
                    ),
                    Value::Integer(
                        i64::try_from(result.io_stats.transmit_blocked)
                            .context("blocked transmit count overflow")?,
                    ),
                    Value::Integer(
                        i64::try_from(result.io_stats.receive_calls)
                            .context("receive call count overflow")?,
                    ),
                    Value::Integer(
                        i64::try_from(result.io_stats.receive_datagrams)
                            .context("receive datagram count overflow")?,
                    ),
                    Value::Integer(
                        i64::try_from(result.io_stats.receive_bytes)
                            .context("receive byte count overflow")?,
                    ),
                    Value::Integer(
                        i64::try_from(result.connection_stats.rtt.as_micros())
                            .context("connection RTT overflow")?,
                    ),
                    Value::Integer(
                        i64::try_from(result.connection_stats.congestion_window)
                            .context("connection congestion window overflow")?,
                    ),
                    Value::Integer(
                        i64::try_from(result.connection_stats.congestion_events)
                            .context("connection congestion event count overflow")?,
                    ),
                    Value::Integer(
                        i64::try_from(result.connection_stats.lost_packets)
                            .context("connection lost packet count overflow")?,
                    ),
                    Value::Integer(
                        i64::try_from(result.connection_stats.lost_bytes)
                            .context("connection lost byte count overflow")?,
                    ),
                    Value::Integer(
                        i64::try_from(result.connection_stats.sent_packets)
                            .context("connection sent packet count overflow")?,
                    ),
                    Value::Integer(i64::from(result.connection_stats.current_mtu)),
                    Value::Text(result.error.unwrap_or_default()),
                ],
            )
            .await
            .context("record experiment result")?;
        let key = vec![Value::Text(result.experiment.name.clone())];
        transaction
            .capture_upsert("experiments", key.clone())
            .await
            .context("capture completed experiment")?;
        transaction
            .capture_upsert("results", key)
            .await
            .context("capture experiment result")?;
        transaction
            .commit()
            .await
            .context("commit experiment result")?;
        Ok(())
    }
}

async fn migration_keys(
    transaction: &libsql::Transaction,
    old_pid: &[u8],
) -> Result<(Vec<Vec<Value>>, Vec<Vec<Value>>)> {
    let mut rows = transaction
        .query(
            "SELECT name FROM experiments WHERE pid = ?1 AND status != 'done'",
            vec![Value::Blob(old_pid.to_vec())],
        )
        .await
        .context("query migrated experiment keys")?;
    let mut experiments = Vec::new();
    while let Some(row) = rows.next().await.context("read migrated experiment key")? {
        experiments.push(vec![row.get_value(0)?]);
    }
    drop(rows);

    let mut rows = transaction
        .query(
            "SELECT experiment_name, position FROM experiment_targets \
             WHERE pid = ?1 AND EXISTS (\
                 SELECT 1 FROM experiments \
                 WHERE experiments.name = experiment_targets.experiment_name \
                   AND experiments.status != 'done'\
             )",
            vec![Value::Blob(old_pid.to_vec())],
        )
        .await
        .context("query migrated experiment target keys")?;
    let mut targets = Vec::new();
    while let Some(row) = rows
        .next()
        .await
        .context("read migrated experiment target key")?
    {
        targets.push(vec![row.get_value(0)?, row.get_value(1)?]);
    }
    Ok((experiments, targets))
}

fn decode_pid(bytes: Vec<u8>) -> Result<Pid> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("scale node has invalid PID"))?;
    Ok(Pid::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(rank: usize, pid: Pid) -> NodeRecord {
        NodeRecord {
            rank,
            pid,
            task_id: rank / 2,
            task_handle: format!("cluster/user/job/{}", rank / 2),
            hostname: format!("host-{}", rank / 2),
            address: format!("udp://127.0.0.1:{}", 1000 + rank),
            is_root: rank == 0,
            parent_pid: (rank != 0).then_some(Pid::from_bytes([9; 16])),
            level: u8::from(rank != 0),
            expected_nodes: 2,
            nodes_per_task: 2,
            started_at_ms: 42,
        }
    }

    fn result(claim: ExperimentClaim) -> ExperimentResult {
        ExperimentResult {
            experiment: claim.experiment,
            claim_attempt: claim.attempt,
            completed: 1,
            started_at_ms: 1,
            finished_at_ms: 2,
            warmup_elapsed: Duration::ZERO,
            elapsed: Duration::from_secs(1),
            mean_latency: Duration::ZERO,
            max_latency: Duration::ZERO,
            io_stats: QuicIoStats::default(),
            connection_stats: QuicConnectionStats::default(),
            error: None,
        }
    }

    #[tokio::test]
    async fn node_registration_replaces_a_restarted_rank() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let store = ExperimentStore::open(&directory.path().join("nodes.db"))
            .await
            .expect("open experiment store");
        let old_pid = Pid::from_bytes([1; 16]);
        let new_pid = Pid::from_bytes([2; 16]);
        store
            .register_node(node(0, old_pid))
            .await
            .expect("register old node");
        store
            .register_node(node(0, new_pid))
            .await
            .expect("register replacement node");

        assert_eq!(store.nodes().await.expect("query nodes"), [new_pid]);
        assert_eq!(
            store.expected_nodes().await.expect("query expected nodes"),
            Some(2)
        );
        assert!(!store.has_node(old_pid).await.expect("query old node"));
        assert!(store.has_node(new_pid).await.expect("query new node"));
    }

    #[tokio::test]
    async fn node_restart_migrates_queued_sources_and_targets() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let store = ExperimentStore::open(&directory.path().join("restart.db"))
            .await
            .expect("open experiment store");
        let old_source = Pid::from_bytes([1; 16]);
        let new_source = Pid::from_bytes([2; 16]);
        let old_target = Pid::from_bytes([3; 16]);
        let new_target = Pid::from_bytes([4; 16]);
        store
            .register_node(node(0, old_source))
            .await
            .expect("register source");
        store
            .register_node(node(1, old_target))
            .await
            .expect("register target");
        store
            .add_experiment(&Experiment {
                pid: old_source,
                name: "restart".into(),
                kind: ExperimentKind::Echo,
                targets: ExperimentTargets::Explicit(vec![old_target]),
                size: 1,
            })
            .await
            .expect("add queued experiment");

        store
            .register_node(node(0, new_source))
            .await
            .expect("replace source");
        store
            .register_node(node(1, new_target))
            .await
            .expect("replace target");

        assert!(
            store
                .claim_experiment(old_source, 1_000, Duration::from_secs(10))
                .await
                .expect("query old source")
                .is_none()
        );
        let claim = store
            .claim_experiment(new_source, 1_000, Duration::from_secs(10))
            .await
            .expect("claim migrated experiment")
            .expect("migrated experiment exists");
        assert_eq!(
            claim.experiment.targets,
            ExperimentTargets::Explicit(vec![new_target])
        );
    }

    #[tokio::test]
    async fn expired_claim_is_recovered_and_fenced() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let store = ExperimentStore::open(&directory.path().join("lease.db"))
            .await
            .expect("open experiment store");
        let pid = Pid::from_bytes([7; 16]);
        store
            .add_experiment(&Experiment {
                pid,
                name: "leased".into(),
                kind: ExperimentKind::Echo,
                targets: ExperimentTargets::Count(1),
                size: 1,
            })
            .await
            .expect("add leased experiment");
        let first = store
            .claim_experiment(pid, 1_000, Duration::from_secs(10))
            .await
            .expect("claim experiment")
            .expect("experiment exists");
        assert!(
            store
                .claim_experiment(pid, 10_999, Duration::from_secs(10))
                .await
                .expect("query active lease")
                .is_none()
        );
        let second = store
            .claim_experiment(pid, 11_000, Duration::from_secs(10))
            .await
            .expect("reclaim experiment")
            .expect("expired experiment exists");
        assert_eq!(second.attempt, first.attempt + 1);
        assert!(store.record_result(result(first)).await.is_err());
        store
            .record_result(result(second))
            .await
            .expect("record current claimant result");
    }

    #[tokio::test]
    async fn node_registration_records_parent_and_level() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let store = ExperimentStore::open(&directory.path().join("lineage.db"))
            .await
            .expect("open experiment store");
        let child_pid = Pid::from_bytes([2; 16]);
        store
            .register_node(node(1, child_pid))
            .await
            .expect("register child node");

        let mut rows = store
            .connection
            .query(
                "SELECT parent_pid, level FROM nodes WHERE pid = ?1",
                vec![Value::Blob(child_pid.as_bytes().to_vec())],
            )
            .await
            .expect("query child lineage");
        let row = rows
            .next()
            .await
            .expect("read child lineage")
            .expect("child lineage exists");
        assert_eq!(row.get::<Vec<u8>>(0).expect("read parent PID"), [9; 16]);
        assert_eq!(row.get::<i64>(1).expect("read level"), 1);
    }

    #[tokio::test]
    async fn root_expected_count_is_authoritative() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let store = ExperimentStore::open(&directory.path().join("convergence.db"))
            .await
            .expect("open experiment store");
        let first = node(0, Pid::from_bytes([1; 16]));
        let mut second = node(1, Pid::from_bytes([2; 16]));
        second.expected_nodes = 3;
        store
            .register_node(first)
            .await
            .expect("register first node");
        store
            .register_node(second)
            .await
            .expect("register second node");

        assert_eq!(
            store.expected_nodes().await.expect("query expected nodes"),
            Some(2)
        );
    }

    #[tokio::test]
    async fn claims_and_completes_experiments_for_pid() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let store = ExperimentStore::open(&directory.path().join("test.db"))
            .await
            .expect("open experiment store");
        let pid = Pid::from_bytes([7; 16]);
        store
            .register_node(node(0, pid))
            .await
            .expect("register node");
        assert_eq!(store.nodes().await.expect("query nodes"), [pid]);
        store
            .add_experiment(&Experiment {
                pid,
                name: "second".into(),
                kind: ExperimentKind::Echo,
                targets: ExperimentTargets::Count(2),
                size: 20,
            })
            .await
            .expect("insert second experiment");
        store
            .add_experiment(&Experiment {
                pid,
                name: "first".into(),
                kind: ExperimentKind::Delivery,
                targets: ExperimentTargets::Count(1),
                size: 10,
            })
            .await
            .expect("insert first experiment");
        let other_pid = Pid::from_bytes([8; 16]);
        store
            .add_experiment(&Experiment {
                pid: other_pid,
                name: "other".into(),
                kind: ExperimentKind::Echo,
                targets: ExperimentTargets::Count(1),
                size: 10,
            })
            .await
            .expect("insert other process experiment");
        assert!(
            store
                .add_experiment(&Experiment {
                    pid: other_pid,
                    name: "first".into(),
                    kind: ExperimentKind::Echo,
                    targets: ExperimentTargets::Count(1),
                    size: 10,
                })
                .await
                .is_err()
        );
        assert_eq!(
            store
                .experiment_status("first")
                .await
                .expect("query pending status"),
            Some(ExperimentStatus::Pending)
        );

        let first = store
            .claim_experiment(pid, 1_000, Duration::from_secs(10))
            .await
            .expect("query first experiment")
            .expect("first experiment exists");
        assert_eq!(first.experiment.name, "first");
        assert_eq!(first.experiment.kind, ExperimentKind::Delivery);
        assert_eq!(
            store
                .experiment_status("first")
                .await
                .expect("query processing status"),
            Some(ExperimentStatus::Processing)
        );
        store
            .record_result(ExperimentResult {
                experiment: first.experiment,
                claim_attempt: first.attempt,
                completed: 1,
                started_at_ms: 1,
                finished_at_ms: 2,
                warmup_elapsed: Duration::from_millis(2),
                elapsed: Duration::from_secs(1),
                mean_latency: Duration::from_millis(3),
                max_latency: Duration::from_millis(4),
                io_stats: QuicIoStats::default(),
                connection_stats: QuicConnectionStats::default(),
                error: None,
            })
            .await
            .expect("record first result");
        assert_eq!(
            store
                .experiment_status("first")
                .await
                .expect("query done status"),
            Some(ExperimentStatus::Done)
        );
        assert_eq!(
            store
                .claim_experiment(pid, 1_000, Duration::from_secs(10))
                .await
                .expect("query second experiment")
                .expect("second experiment exists")
                .experiment
                .name,
            "second"
        );
        assert_eq!(
            store
                .claim_experiment(other_pid, 1_000, Duration::from_secs(10))
                .await
                .expect("query other process experiment")
                .expect("other process experiment exists")
                .experiment
                .name,
            "other"
        );
    }

    #[tokio::test]
    async fn claims_explicit_targets_in_position_order() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let store = ExperimentStore::open(&directory.path().join("targets.db"))
            .await
            .expect("open experiment store");
        let source = Pid::from_bytes([7; 16]);
        let targets = vec![Pid::from_bytes([9; 16]), Pid::from_bytes([8; 16])];
        store
            .add_experiment(&Experiment {
                pid: source,
                name: "targeted".into(),
                kind: ExperimentKind::Delivery,
                targets: ExperimentTargets::Explicit(targets.clone()),
                size: 1024,
            })
            .await
            .expect("insert targeted experiment");

        let experiment = store
            .claim_experiment(source, 1_000, Duration::from_secs(10))
            .await
            .expect("claim targeted experiment")
            .expect("targeted experiment exists");

        assert_eq!(experiment.experiment.kind, ExperimentKind::Delivery);
        assert_eq!(
            experiment.experiment.targets,
            ExperimentTargets::Explicit(targets)
        );
    }
}
