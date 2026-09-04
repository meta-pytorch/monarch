/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fs;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::Context;
use anyhow::Result;
use clap::Args;
use clap::ValueEnum;
use serde_json::Value;
use serde_json::json;

use crate::benchmark::DEFAULT_IDENTITY_CONCURRENCY;
use crate::benchmark::DEFAULT_NODES_PER_TASK;
use crate::benchmark::Topology;
use crate::network_baseline::NetworkBaselineProfile;
use crate::network_baseline::NetworkBaselineProtocol;

const TARGET: &str = "fbcode//monarch/chrysalis-scale:chrysalis-scale";
const FBPKG_NAME: &str = "monarch_additional_packages";
const RESOURCE_LOCATOR_TIER: &str = "mast.lookup.prod";
const PACKAGE_BINARY: &str = "chrysalis-scale";
const MAST_READ_TIER: &str = "mast.api.read";
const PLACEMENT_POLL_INTERVAL: Duration = Duration::from_secs(5);
const MONARCH_CICD_TENANT: &str = "root/gen_ai/msl/msl_infra/cicd/monarch_cicd";
const MONARCH_TRAINING_TENANT: &str =
    "root/cfp/ai_rnd/ai_systems_rnd/ai_infra_training_rnd_tc/monarch_training";

#[derive(Clone, Copy, Debug, ValueEnum)]
enum OpecTag {
    DedicatedOnly,
    OpecOnly,
    OpecFloating,
}

impl OpecTag {
    const fn value(self) -> u8 {
        match self {
            Self::DedicatedOnly => 0,
            Self::OpecOnly => 2,
            Self::OpecFloating => 3,
        }
    }
}

#[derive(Clone, Debug, Args)]
pub(crate) struct MastArgs {
    /// Comma-separated logical node counts to schedule.
    #[arg(long, value_delimiter = ',', default_value = "1000,10000,100000")]
    nodes: Vec<usize>,

    /// Maximum number of logical nodes hosted by each MAST task.
    #[arg(long, default_value_t = DEFAULT_NODES_PER_TASK)]
    nodes_per_task: usize,

    /// Process-mesh topology used by logical nodes.
    #[arg(long, value_enum, default_value = "task-head")]
    topology: Topology,

    /// Maximum concurrent Meta identity requests in each task.
    #[arg(long, default_value_t = DEFAULT_IDENTITY_CONCURRENCY)]
    identity_concurrency: usize,

    /// Reuse an existing package as NAME:EPHEMERAL_ID.
    #[arg(long)]
    package: Option<String>,

    /// Submit generated jobs to MAST.
    #[arg(long)]
    launch: bool,

    /// Run long-lived SQLite-driven experiments instead of one fixed sweep.
    #[arg(long, conflicts_with = "network_baseline")]
    persist: bool,

    /// Run a two-task kernel TCP network control instead of Chrysalis.
    #[arg(long, conflicts_with = "persist")]
    network_baseline: bool,

    /// Protocol layer measured by the two-task network control.
    #[arg(long, value_enum, default_value = "tcp")]
    baseline_protocol: NetworkBaselineProtocol,

    /// Optional profiler attached during the measured network control.
    #[arg(long, value_enum, default_value = "none")]
    baseline_profile: NetworkBaselineProfile,

    /// Payload size for the kernel TCP network control.
    #[arg(long, default_value_t = 2 * 1024 * 1024 * 1024_u64)]
    baseline_bytes: u64,

    /// Parallel connections used by the kernel TCP network control.
    #[arg(long, default_value_t = 1)]
    baseline_connections: usize,

    /// Print each generated job specification.
    #[arg(long)]
    print_spec: bool,

    /// Expiration passed to the ephemeral fbpkg build.
    #[arg(long, default_value = "1w")]
    expire: String,

    /// Prefix for generated MAST job names.
    #[arg(long, default_value = "chrysalis_scale")]
    name_prefix: String,

    /// MAST cluster used for the run.
    #[arg(long, default_value = "MastGenAICluster")]
    cluster: String,

    /// MAST entitlement and attribution identity.
    #[arg(long, default_value = "monarch_cicd")]
    entitlement: String,

    /// Capacity class; defaults by cluster.
    #[arg(long, value_enum)]
    opec_tag: Option<OpecTag>,

    /// Optional region that must contain every task in one job.
    #[arg(long)]
    region: Option<String>,

    /// Optional logical server subtype, such as 10018 for T1 Bergamo.
    #[arg(long)]
    server_subtype: Option<u64>,

    /// Fixed UDP port used to locate the root node.
    #[arg(long, default_value_t = 26600)]
    port: u16,

    /// Memory reserved for each MAST task.
    #[arg(long, default_value_t = 2048)]
    ram_mb: usize,

    /// CPUs reserved for each MAST task.
    #[arg(long, default_value_t = 4)]
    cpus: usize,

    /// Maximum simultaneous root echo operations.
    #[arg(long, default_value_t = 1024)]
    concurrency: usize,

    /// Full-join timeout passed to every task.
    #[arg(long, default_value_t = 1800)]
    join_timeout_secs: u64,

    /// Echo timeout passed to every task.
    #[arg(long, default_value_t = 1800)]
    echo_timeout_secs: u64,
}

pub(crate) fn run(args: MastArgs) -> Result<()> {
    anyhow::ensure!(
        !args.nodes.is_empty(),
        "at least one node count is required"
    );
    anyhow::ensure!(args.nodes_per_task > 0, "nodes per task must be nonzero");
    anyhow::ensure!(
        args.identity_concurrency > 0,
        "identity concurrency must be nonzero"
    );
    anyhow::ensure!(args.concurrency > 0, "echo concurrency must be nonzero");
    anyhow::ensure!(args.ram_mb > 0, "per-task memory must be nonzero");
    anyhow::ensure!(args.cpus > 0, "per-task CPU count must be nonzero");
    anyhow::ensure!(
        args.baseline_bytes > 0,
        "baseline byte count must be nonzero"
    );
    anyhow::ensure!(
        args.baseline_connections > 0,
        "baseline connection count must be nonzero"
    );
    for nodes in &args.nodes {
        anyhow::ensure!(*nodes > 1, "node count must exceed one: {nodes}");
        if args.network_baseline {
            anyhow::ensure!(
                *nodes == 2 && args.nodes_per_task == 1,
                "network baseline requires --nodes 2 --nodes-per-task 1"
            );
        }
    }
    let package = match &args.package {
        Some(package) => package.clone(),
        None => build_package(&args.expire)?,
    };
    let (package_name, package_version) = package
        .split_once(':')
        .context("package must use NAME:EPHEMERAL_ID")?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock precedes Unix epoch")?
        .as_secs();
    let mut launched = Vec::new();
    for nodes in &args.nodes {
        let tasks = nodes.div_ceil(args.nodes_per_task);
        let job_name = format!(
            "{}_{}_{}n_{}t_{}",
            args.name_prefix,
            std::env::var("USER").context("USER is not set")?,
            nodes,
            tasks,
            timestamp
        );
        let spec = make_job_spec(&args, &job_name, package_name, package_version, *nodes)?;
        let spec_path = std::env::temp_dir().join(format!("{job_name}.jobspec.json"));
        fs::write(&spec_path, serde_json::to_vec_pretty(&spec)?)
            .with_context(|| format!("write job spec {}", spec_path.display()))?;
        println!(
            "[mast] nodes={nodes} tasks={tasks} nodes_per_task={} topology={} package={package} job={job_name} spec={}",
            args.nodes_per_task,
            args.topology.as_str(),
            spec_path.display()
        );
        if args.print_spec {
            println!("{}", serde_json::to_string_pretty(&spec)?);
        }
        if args.launch {
            submit(&args.cluster, &spec)?;
            println!("[mast] scheduled {job_name}");
            println!("[mast] status: meta ai.mast-job get-status --name={job_name}");
            println!(
                "[mast] result: lg mast:{job_name} --pattern 'joined|RESULT|ERROR' --stream stdout --print-all"
            );
            launched.push((job_name, tasks, args.network_baseline));
        }
    }
    for (job_name, tasks, network_baseline) in launched {
        let address = wait_for_root_address(
            &job_name,
            tasks,
            args.port,
            Duration::from_secs(args.join_timeout_secs),
        )?;
        if network_baseline {
            println!(
                "[mast] baseline: {}://{address}",
                args.baseline_protocol.as_str()
            );
        } else {
            println!("[mast] connect: udp://{address}");
            println!("[mast] resolver: mast://{job_name}");
        }
    }
    Ok(())
}

fn wait_for_root_address(
    job_name: &str,
    expected_tasks: usize,
    port: u16,
    timeout: Duration,
) -> Result<SocketAddr> {
    let started = Instant::now();
    loop {
        let query_error = match query_job_status(job_name) {
            Ok(status) => {
                if let Some(address) = root_address(&status, expected_tasks, port)? {
                    return Ok(address);
                }
                if matches!(status["state"].as_str(), Some("DEAD" | "FAILED")) {
                    anyhow::bail!(
                        "MAST job {job_name} entered terminal state {} before full placement",
                        status["state"]
                    );
                }
                None
            }
            Err(error) => Some(error),
        };
        if started.elapsed() >= timeout {
            let detail = query_error
                .map(|error| format!(": {error:#}"))
                .unwrap_or_default();
            anyhow::bail!("timed out waiting for root placement for {job_name}{detail}");
        }
        thread::sleep(PLACEMENT_POLL_INTERVAL);
    }
}

fn query_job_status(job_name: &str) -> Result<Value> {
    let request = json!({"request": {"hpcJobName": job_name}}).to_string();
    let output = command_output(Command::new("thriftdbg").args([
        "sendRequest",
        "getHpcJobStatus",
        &request,
        "--tier",
        MAST_READ_TIER,
    ]))?;
    output
        .lines()
        .rev()
        .find_map(|line| serde_json::from_str(line).ok())
        .context("MAST status returned no JSON response")
}

fn root_address(status: &Value, expected_tasks: usize, port: u16) -> Result<Option<SocketAddr>> {
    let Some(tasks) = status["taskGroups"]
        .as_array()
        .and_then(|groups| groups.iter().find(|group| group["name"] == "nodes"))
        .and_then(|group| group["tasks"].as_array())
    else {
        return Ok(None);
    };
    if tasks.len() < expected_tasks {
        return Ok(None);
    }
    let mut placements = tasks
        .iter()
        .filter_map(|task| Some((task["hostname"].as_str()?, task["taskIp"].as_str()?)))
        .collect::<Vec<_>>();
    if placements.len() < expected_tasks {
        return Ok(None);
    }
    placements.sort_unstable_by_key(|(hostname, _)| *hostname);
    anyhow::ensure!(
        placements.windows(2).all(|pair| pair[0].0 != pair[1].0),
        "MAST placed multiple benchmark tasks on one host"
    );
    let (_, root_ip) = placements
        .first()
        .context("MAST supplied no task placements")?;
    let ip: IpAddr = root_ip
        .parse()
        .with_context(|| format!("MAST supplied invalid task IP {root_ip}"))?;
    Ok(Some(SocketAddr::new(ip, port)))
}

fn build_package(expire: &str) -> Result<String> {
    let output = command_output(Command::new("buck").args([
        "build",
        "@fbcode//mode/opt",
        "--show-output",
        TARGET,
    ]))?;
    let output_path = |target: &str| {
        output.lines().rev().find_map(|line| {
            let (line_target, path) = line.split_once(' ')?;
            (line_target == target).then_some(PathBuf::from(path))
        })
    };
    let binary = output_path(TARGET).context("buck output did not contain the scale binary")?;
    let root = PathBuf::from(
        command_output(Command::new("buck").args(["root", "--kind", "project"]))?.trim(),
    );
    let binary = resolve_build_output(&root, binary);
    let staging = tempfile::tempdir().context("create fbpkg staging directory")?;
    let staged_binary = staging.path().join(PACKAGE_BINARY);
    fs::copy(&binary, &staged_binary)
        .with_context(|| format!("stage scale binary from {}", binary.display()))?;
    fs::set_permissions(&staged_binary, fs::Permissions::from_mode(0o755))?;
    let config = tempfile::tempdir().context("create fbpkg config directory")?;
    let materialized = config.path().join("materialized_configs");
    fs::create_dir(&materialized)?;
    let package_config = json!({
        "paths": [PACKAGE_BINARY],
        "build_command": "",
    });
    fs::write(
        materialized.join(format!("{FBPKG_NAME}.fbpkg.materialized_JSON")),
        serde_json::to_vec(&package_config)?,
    )?;
    let output = command_output(
        Command::new("fbpkg")
            .current_dir(staging.path())
            .args(["build", "--yes", "--ephemeral", "--configerator-path"])
            .arg(config.path())
            .args([FBPKG_NAME, "--expire", expire]),
    )?;
    output
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .map(|line| line.trim().to_owned())
        .context("fbpkg output did not contain a package identifier")
}

fn resolve_build_output(project_root: &Path, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        project_root.join(path)
    }
}

fn make_job_spec(
    args: &MastArgs,
    job_name: &str,
    package_name: &str,
    package_version: &str,
    nodes: usize,
) -> Result<Value> {
    let tasks = nodes.div_ceil(args.nodes_per_task);
    let command = format!("/packages/{package_name}/{PACKAGE_BINARY}");
    let mode = if args.network_baseline {
        "network-baseline"
    } else if args.persist {
        "persist"
    } else {
        "run"
    };
    let tenant_path = match args.entitlement.as_str() {
        "monarch_cicd" => MONARCH_CICD_TENANT,
        "monarch_training" => MONARCH_TRAINING_TENANT,
        entitlement => anyhow::bail!("unsupported MAST entitlement: {entitlement}"),
    };
    let application_cluster = match args.cluster.as_str() {
        "MastProdCluster" | "CPUTrainingWorkloads" => "MastProdCluster",
        cluster => cluster,
    };
    let opec_tag = args.opec_tag.unwrap_or_else(|| {
        if args.cluster == "MastGenAICluster" {
            OpecTag::DedicatedOnly
        } else {
            OpecTag::OpecOnly
        }
    });
    let machine_constraints = match args.server_subtype {
        Some(server_subtype) => json!({"types": {"serverSubTypes": [server_subtype]}}),
        None => json!({"types": {"serverTypes": [100]}}),
    };
    // Raw TCP is the explicit unencrypted transport control; every Chrysalis and
    // QUIC measurement retains the platform's encryption-in-transit protection.
    let enable_ttls =
        !args.network_baseline || args.baseline_protocol != NetworkBaselineProtocol::Tcp;
    let mut spec = json!({
        "name": job_name,
        "hpcClusterUuid": args.cluster,
        "hpcTaskGroups": [{
            "name": "nodes",
            "taskCount": tasks,
            "taskCountPerHost": 1,
            "hardwareSpecificTaskGroupOverride": {},
            "spec": {
                "command": command,
                "arguments": [mode],
                "applicationPackages": [{
                    "name": package_name,
                    "version": {"ephemeralId": package_version},
                    "fbpkgIdentifier": format!("{package_name}:{package_version}"),
                }],
                "packages": [],
                "ports": {
                    "chrysalis_root": args.port,
                },
                "env": {
                    "CHRYSALIS_SCALE_NODES": nodes.to_string(),
                    "CHRYSALIS_SCALE_NODES_PER_TASK": args.nodes_per_task.to_string(),
                    "CHRYSALIS_SCALE_TOPOLOGY": args.topology.as_str(),
                    "CHRYSALIS_SCALE_IDENTITY_CONCURRENCY": args.identity_concurrency.to_string(),
                    "CHRYSALIS_SCALE_PORT": args.port.to_string(),
                    "CHRYSALIS_SCALE_CONCURRENCY": args.concurrency.to_string(),
                    "CHRYSALIS_SCALE_JOIN_TIMEOUT_SECS": args.join_timeout_secs.to_string(),
                    "CHRYSALIS_SCALE_ECHO_TIMEOUT_SECS": args.echo_timeout_secs.to_string(),
                    "CHRYSALIS_SCALE_BASELINE_BYTES": args.baseline_bytes.to_string(),
                    "CHRYSALIS_SCALE_BASELINE_CONNECTIONS": args.baseline_connections.to_string(),
                    "CHRYSALIS_SCALE_BASELINE_PROTOCOL": args.baseline_protocol.as_str(),
                    "CHRYSALIS_SCALE_BASELINE_PROFILE": args.baseline_profile.as_str(),
                    "RUST_BACKTRACE": "1",
                },
                "resourceLimit": {
                    "ramMB": args.ram_mb,
                    "compute": {"cpu": args.cpus, "gpu": 0},
                    "enableSwapAndSenpai": false,
                    "limitType": 0,
                    "wholeHost": true,
                },
                "machineConstraints": machine_constraints,
                "networkAffinity": {"preferredScope": 2, "fallbackScope": 1},
                "oncallShortname": "monarch",
                "bindMounts": [],
                "runningTimeoutSec": if args.persist { 604800 } else { 3600 },
                "unixUser": "root",
                "restartPolicy": {
                    "scope": 0,
                    "maxTotalFailures": 0,
                    "failoverOnHostFailures": false,
                    "failJobOnFinalFailure": true,
                },
                "ttlsConfig": {"enable": enable_ttls},
                "opecTag": opec_tag.value(),
            },
        }],
        "networkAffinity": {"preferredScope": 2, "fallbackScope": 1},
        "applicationMetadata": {
            "model_type_name": "gen_ai_default",
            "rm_attribution": args.entitlement,
            "hpcClusterUuid": application_cluster,
        },
        "identity": {"name": "hyper_monarch"},
        "owner": {
            "oncallShortname": "monarch",
            "unixname": std::env::var("USER").unwrap_or_else(|_| "unknown".into()),
        },
        "enableGracefulPreemption": true,
        "maxJobFailures": 0,
        "jobType": 0,
        "aiTrainingMetadata": {
            "jobType": 0,
            "modelTypeName": "gen_ai_default",
            "entitlement": args.entitlement,
            "tenantPath": tenant_path,
            "productGroup": "gen_ai",
            "mastJobID": job_name,
            "model_lifecycle_status": {},
        },
    });
    if let Some(region) = &args.region {
        spec["localityConstraints"] = json!({"locality": 1, "options": [region]});
    }
    Ok(spec)
}

fn submit(cluster: &str, spec: &Value) -> Result<()> {
    let tier = locate_write_tier(cluster)?;
    let request = tempfile::NamedTempFile::new().context("create schedule request file")?;
    fs::write(
        request.path(),
        serde_json::to_vec(&json!({"request": {"hpcJob": spec}}))?,
    )?;
    command_output(
        Command::new("thriftdbg")
            .args(["sendRequest", "scheduleHpcJob", "", "--request_json"])
            .arg(request.path())
            .args(["--tier", &tier, "--request_timeout_ms", "90000"]),
    )?;
    Ok(())
}

fn locate_write_tier(cluster: &str) -> Result<String> {
    let request = json!({"request": {"hpcClusterUuid": cluster}}).to_string();
    let output = command_output(Command::new("thriftdbg").args([
        "sendRequest",
        "locateHpcCluster",
        &request,
        "--tier",
        RESOURCE_LOCATOR_TIER,
    ]))?;
    let response: Value = output
        .lines()
        .rev()
        .find_map(|line| serde_json::from_str(line).ok())
        .context("resource locator returned no JSON response")?;
    response["smcTiers"]["writeTier"]
        .as_str()
        .map(str::to_owned)
        .context("resource locator response has no write tier")
}

fn command_output(command: &mut Command) -> Result<String> {
    let description = format!("{command:?}");
    let output = command
        .output()
        .with_context(|| format!("run {description}"))?;
    if !output.status.success() {
        anyhow::bail!(
            "command failed: {description}\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    String::from_utf8(output.stdout).context("command output is not UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> MastArgs {
        MastArgs {
            nodes: vec![1000, 10000, 100000],
            nodes_per_task: 100,
            topology: Topology::TaskHead,
            identity_concurrency: 8,
            package: Some("pkg:version".into()),
            launch: false,
            persist: false,
            network_baseline: false,
            baseline_protocol: NetworkBaselineProtocol::Tcp,
            baseline_profile: NetworkBaselineProfile::None,
            baseline_bytes: 2 * 1024 * 1024 * 1024,
            baseline_connections: 1,
            print_spec: false,
            expire: "1w".into(),
            name_prefix: "test".into(),
            cluster: "CPUTrainingWorkloads".into(),
            entitlement: "monarch_training".into(),
            opec_tag: None,
            region: Some("atn".into()),
            server_subtype: None,
            port: 26600,
            ram_mb: 2048,
            cpus: 4,
            concurrency: 1024,
            join_timeout_secs: 1800,
            echo_timeout_secs: 1800,
        }
    }

    #[test]
    fn job_spec_reserves_one_host_per_task_and_preserves_benchmark_parameters() {
        let spec =
            make_job_spec(&args(), "job", "pkg", "version", 100000).expect("create job spec");
        let group = &spec["hpcTaskGroups"][0];
        assert_eq!(group["taskCount"], 1000);
        assert_eq!(group["taskCountPerHost"], 1);
        assert_eq!(group["spec"]["resourceLimit"]["wholeHost"], true);
        assert_eq!(group["spec"]["resourceLimit"]["ramMB"], 2048);
        assert_eq!(group["spec"]["resourceLimit"]["compute"]["cpu"], 4);
        assert_eq!(group["spec"]["opecTag"], 2);
        assert_eq!(group["spec"]["arguments"], json!(["run"]));
        assert_eq!(group["spec"]["ports"]["chrysalis_root"], 26600);
        assert_eq!(group["spec"]["runningTimeoutSec"], 3600);
        assert_eq!(group["spec"]["env"]["CHRYSALIS_SCALE_NODES"], "100000");
        assert_eq!(
            group["spec"]["env"]["CHRYSALIS_SCALE_NODES_PER_TASK"],
            "100"
        );
        assert_eq!(
            group["spec"]["env"]["CHRYSALIS_SCALE_TOPOLOGY"],
            "task-head"
        );
        assert_eq!(
            group["spec"]["env"]["CHRYSALIS_SCALE_IDENTITY_CONCURRENCY"],
            "8"
        );
        assert_eq!(spec["localityConstraints"]["options"], json!(["atn"]));
    }

    #[test]
    fn job_spec_rounds_up_partial_final_task() {
        let spec =
            make_job_spec(&args(), "job", "pkg", "version", 1001).expect("create uneven job spec");
        assert_eq!(spec["hpcTaskGroups"][0]["taskCount"], 11);
    }

    #[test]
    fn task_head_topology_is_forwarded_to_every_task() {
        let mut args = args();
        args.topology = Topology::TaskHead;
        let spec =
            make_job_spec(&args, "job", "pkg", "version", 1000).expect("create task-head job spec");
        assert_eq!(
            spec["hpcTaskGroups"][0]["spec"]["env"]["CHRYSALIS_SCALE_TOPOLOGY"],
            "task-head"
        );
    }

    #[test]
    fn ablation_defaults_are_ordered() {
        let args = args();
        assert_eq!(args.nodes, [1000, 10000, 100000]);
    }

    #[test]
    fn genai_cluster_defaults_to_dedicated_capacity() {
        let mut args = args();
        args.cluster = "MastGenAICluster".into();
        let spec =
            make_job_spec(&args, "job", "pkg", "version", 1000).expect("create GenAI job spec");
        assert_eq!(spec["hpcTaskGroups"][0]["spec"]["opecTag"], 0);
    }

    #[test]
    fn server_subtype_replaces_generic_server_type() {
        let mut args = args();
        args.server_subtype = Some(10018);
        let spec = make_job_spec(&args, "job", "pkg", "version", 1000)
            .expect("create subtype-constrained job spec");
        let constraints = &spec["hpcTaskGroups"][0]["spec"]["machineConstraints"]["types"];
        assert_eq!(constraints["serverSubTypes"], json!([10018]));
        assert!(constraints.get("serverTypes").is_none());
    }

    #[test]
    fn network_baseline_job_forwards_connection_count() {
        let mut args = args();
        args.nodes = vec![2];
        args.nodes_per_task = 1;
        args.network_baseline = true;
        args.baseline_connections = 4;
        let spec = make_job_spec(&args, "job", "pkg", "version", 2)
            .expect("create network baseline job spec");
        let task = &spec["hpcTaskGroups"][0]["spec"];
        assert_eq!(task["arguments"], json!(["network-baseline"]));
        assert_eq!(task["env"]["CHRYSALIS_SCALE_BASELINE_CONNECTIONS"], "4");
        assert_eq!(task["env"]["CHRYSALIS_SCALE_BASELINE_PROTOCOL"], "tcp");
        assert_eq!(task["env"]["CHRYSALIS_SCALE_BASELINE_PROFILE"], "none");
        assert_eq!(task["ttlsConfig"]["enable"], false);
    }

    #[test]
    fn ttls_remains_enabled_for_chrysalis_network_baselines() {
        let mut args = args();
        args.nodes = vec![2];
        args.nodes_per_task = 1;
        args.network_baseline = true;
        args.baseline_protocol = NetworkBaselineProtocol::DirectQuic;
        let spec = make_job_spec(&args, "job", "pkg", "version", 2)
            .expect("create QUIC network baseline job spec");
        assert_eq!(
            spec["hpcTaskGroups"][0]["spec"]["ttlsConfig"]["enable"],
            true
        );
    }

    #[test]
    fn persistent_job_uses_sqlite_driven_mode() {
        let mut args = args();
        args.persist = true;
        let spec = make_job_spec(&args, "job", "pkg", "version", 1000)
            .expect("create persistent job spec");
        let task = &spec["hpcTaskGroups"][0]["spec"];
        assert_eq!(task["arguments"], json!(["persist"]));
        assert_eq!(task["runningTimeoutSec"], 604800);
    }

    #[test]
    fn root_address_uses_first_hostname_after_complete_placement() {
        let status = json!({
            "taskGroups": [{
                "name": "nodes",
                "tasks": [
                    {"hostname": "host-z", "taskIp": "2401:db00::2"},
                    {"hostname": "host-a", "taskIp": "2401:db00::1"},
                ],
            }],
        });

        assert_eq!(
            root_address(&status, 2, 26600).expect("read root placement"),
            Some("[2401:db00::1]:26600".parse().expect("parse address"))
        );
        assert_eq!(
            root_address(&status, 3, 26600).expect("wait for full placement"),
            None
        );
    }

    #[test]
    fn root_address_rejects_multiple_tasks_on_one_host() {
        let status = json!({
            "taskGroups": [{
                "name": "nodes",
                "tasks": [
                    {"hostname": "host-a", "taskIp": "2401:db00::1"},
                    {"hostname": "host-a", "taskIp": "2401:db00::1"},
                ],
            }],
        });

        assert!(root_address(&status, 2, 26600).is_err());
    }

    #[test]
    fn relative_build_outputs_are_resolved_from_the_project_root() {
        let root = Path::new("/fbsource");
        assert_eq!(
            resolve_build_output(root, "relative/artifact".into()),
            Path::new("/fbsource/relative/artifact")
        );
        assert_eq!(
            resolve_build_output(root, "/tmp/artifact".into()),
            Path::new("/tmp/artifact")
        );
    }
}
