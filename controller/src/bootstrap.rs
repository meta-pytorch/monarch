/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;

use anyhow::Result;
use anyhow::anyhow;
use clap::Args;
use clap::Subcommand;
use const_format::concatcp;
use hyperactor::GangRef;
use hyperactor::actor::ActorHandle;
use hyperactor::channel::ChannelAddr;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::context;
use hyperactor::mailbox::open_port;
use hyperactor::reference::ActorId;
use hyperactor::reference::ActorRef;
use hyperactor::reference::GangId;
use hyperactor::reference::Index;
use hyperactor::reference::WorldId;
use hyperactor_mesh::comm::CommActor;
use hyperactor_multiprocess::System;
use hyperactor_multiprocess::proc_actor::Environment;
use hyperactor_multiprocess::proc_actor::ProcActor;
use hyperactor_multiprocess::proc_actor::ProcMessageClient;
use hyperactor_multiprocess::system_actor;
use hyperactor_multiprocess::system_actor::ProcLifecycleMode;
use hyperactor_multiprocess::system_actor::Shape;
use hyperactor_multiprocess::system_actor::SystemMessageClient;
use monarch_messages::worker::WorkerParams;
use pyo3::prelude::*;
use pyo3::types::PyType;
use serde::Deserialize;
use serde::Serialize;
use tokio::task::JoinHandle;

use crate::ControllerActor;
use crate::ControllerParams;

/// Domain name for all monarch reserved labels.
pub static MONARCH_LABEL_PREFIX: &str = "monarch.meta.com/";
/// Prefix for all monarch reserved labels for procs.
static WORKER_LABEL_PREFIX: &str = concatcp!("proc.", MONARCH_LABEL_PREFIX);
/// Labels suffix indicating the role of a proc.
static LABEL_NAME_ROLE: &str = concatcp!(WORKER_LABEL_PREFIX, "role");
/// Label value indicating proc role is controller.
static LABEL_VALUE_ROLE_CONTROLLER: &str = "controller";
/// Label value indicating proc role is host.
static LABEL_VALUE_ROLE_HOST: &str = "host";
/// Label indicating the worker world for a given controller to allow
/// for backreferencing.
static LABEL_NAME_WORKER_WORLD: &str = concatcp!(WORKER_LABEL_PREFIX, "workerWorld");
/// The global name used for comm actors.
static COMM_ACTOR_NAME: &str = "comm";

/// Prefix for all monarch reserved labels for worlds.
pub static WORLD_LABEL_PREFIX: &str = concatcp!("world.", MONARCH_LABEL_PREFIX);
/// Label indicating if a given world is a worker world. A value of "1" indicates
/// a worker world. This allows us to query all worker worlds in the system.
static LABEL_NAME_WORKER: &str = concatcp!(WORLD_LABEL_PREFIX, "worker");
/// Label indicating the controller actor id for a given worker world. This allows
/// to query all worker worlds and communcicate with their controllers.
static LABEL_NAME_CONTROLLER_ACTOR_ID: &str = concatcp!(WORLD_LABEL_PREFIX, "controllerActorId");

#[derive(Clone, Debug, Serialize, Deserialize, Args)]
#[pyclass(module = "monarch._rust_bindings.controller.bootstrap")]
pub struct ControllerCommand {
    /// The worker world to create
    #[arg(long)]
    pub worker_world: String,

    /// The system address to bootstrap with.
    #[arg(long)]
    pub system_addr: String,

    /// The controller actor id to give to.
    #[arg(long, default_value_t = String::from("controller[0].root"))]
    pub controller_actor_id: String,

    // Global world size for this job
    #[arg(long)]
    pub world_size: usize,

    /// The number of processes per host.
    #[arg(long, default_value_t = 8)]
    pub num_procs_per_host: usize,

    /// The worker name.
    #[arg(long, default_value_t = String::from("worker"))]
    pub worker_name: String,

    /// The worker program to execute for each process. It is not needed if worker procs
    /// are directly launched without management from host actors.
    #[arg(long)]
    pub program: Option<String>,

    /// The supervision check interval in seconds. It indicates how often the controller
    /// will poll system actor to check the status of all procs/actors in a world. This
    /// decides how fast the client could observe a failure in the system.
    #[arg(long, default_value_t = 2)]
    pub supervision_query_interval_in_sec: u64,

    /// The supervision update interval in seconds, it indiciates how often the controller
    /// proc should report its supervision status to the system.
    #[arg(long, default_value_t = 2)]
    pub supervision_update_interval_in_sec: u64,

    /// The worker progress check interval in seconds, it indicates how often the controller
    /// will check that progress is being made.
    #[arg(long, default_value_t = 10)]
    pub worker_progress_check_interval_in_sec: u64,

    /// The operation timeout duration interval in seconds, it indicates how long we will allow
    /// progress to stall for before letting the client know that worker(s) may be stuck.
    #[arg(long, default_value_t = 120)]
    pub operation_timeout_in_sec: u64,

    /// The number of operations invoked before we proactively check worker progress. If a large number
    /// of operations are invoked all at once, it is expected that it will take a while for all operations
    /// to complete so we want to inject progress requests at a higher frequency to check if we are making progress
    #[arg(long, default_value_t = 100)]
    pub operations_per_worker_progress_request: u64,

    /// If the controller should propagate a failure to the client if the workers become stuck.
    #[arg(long, default_value_t = false)]
    pub fail_on_worker_timeout: bool,

    /// If to launch the workers for CPU-only devices.
    #[arg(long, default_value_t = false)]
    pub is_cpu_worker: bool,

    /// Proc metadata which will be available through system.
    #[arg(long, value_parser=parse_key_val)]
    pub extra_proc_labels: Option<Vec<(String, String)>>,
}

#[pymethods]
impl ControllerCommand {
    #[new]
    #[pyo3(signature = (*, worker_world, system_addr, controller_actor_id, world_size, num_procs_per_host, worker_name, program, supervision_query_interval_in_sec, supervision_update_interval_in_sec, worker_progress_check_interval_in_sec, operation_timeout_in_sec, operations_per_worker_progress_request, fail_on_worker_timeout, is_cpu_worker, extra_proc_labels))]
    fn new(
        worker_world: String,
        system_addr: String,
        controller_actor_id: String,
        world_size: usize,
        num_procs_per_host: usize,
        worker_name: String,
        program: Option<String>,
        supervision_query_interval_in_sec: u64,
        supervision_update_interval_in_sec: u64,
        worker_progress_check_interval_in_sec: u64,
        operation_timeout_in_sec: u64,
        operations_per_worker_progress_request: u64,
        fail_on_worker_timeout: bool,
        is_cpu_worker: bool,
        extra_proc_labels: Option<Vec<(String, String)>>,
    ) -> Self {
        Self {
            worker_world,
            system_addr,
            controller_actor_id,
            world_size,
            num_procs_per_host,
            worker_name,
            program,
            supervision_query_interval_in_sec,
            supervision_update_interval_in_sec,
            worker_progress_check_interval_in_sec,
            operation_timeout_in_sec,
            operations_per_worker_progress_request,
            fail_on_worker_timeout,
            is_cpu_worker,
            extra_proc_labels,
        }
    }
}

/// The different types of hyperactor to launch based on the subcommands.
/// The ones for System / Host should probably be moved to the hyperactor
/// multiprocess crate.
#[derive(Clone, Debug, Serialize, Deserialize, Subcommand)]
#[pyclass(module = "monarch._rust_bindings.controller.bootstrap")]
pub enum RunCommand {
    System {
        /// The system address to bootstrap with.
        #[arg(long)]
        system_addr: String,

        /// The supervision update timeout in seconds. A proc is considered dead if system
        /// doesn't get any supervision update from it within this timeout.
        #[arg(long, default_value_t = 20)]
        supervision_update_timeout_in_sec: u64,

        /// Evict a world if it has been unhealthy for this many seconds.
        #[arg(long, default_value_t = 10)]
        world_eviction_timeout_in_sec: u64,
    },

    Host {
        /// The system address to bootstrap with.
        #[arg(long)]
        system_addr: String,

        /// The host world to create.
        #[arg(long)]
        host_world: String,

        /// The host rank; i.e., the index of the host in the world.
        #[arg(long)]
        host_rank: Index,

        /// The supervision update interval in seconds, it indiciates how often a proc should
        /// report its supervision status to the system.
        #[arg(long, default_value_t = 2)]
        supervision_update_interval_in_sec: u64,
    },

    Controller(ControllerCommand),
}

#[pyclass(frozen, module = "monarch._rust_bindings.controller.bootstrap")]
#[derive(Debug, Serialize, Deserialize)]
pub enum ControllerServerRequest {
    Run(RunCommand),
    Exit(),
}

#[pymethods]
impl ControllerServerRequest {
    fn to_json(&self) -> PyResult<String> {
        Ok(serde_json::to_string(self).map_err(|e| anyhow!(e))?)
    }

    fn __str__(&self) -> String {
        format!("{:?}", self)
    }
}

#[pyclass(frozen, module = "monarch._rust_bindings.controller.bootstrap")]
#[derive(Debug, Serialize, Deserialize)]
pub enum ControllerServerResponse {
    Finished { error: Option<String> },
}

#[pymethods]
impl ControllerServerResponse {
    #[classmethod]
    fn from_json(_: &Bound<'_, PyType>, json: &str) -> PyResult<Self> {
        Ok(serde_json::from_str(json).map_err(|e| anyhow!(e))?)
    }

    fn __str__(&self) -> String {
        format!("{:?}", self)
    }
}

/// A helper function to launch the system, host, or controller actors.
/// Returns the handle to be waited on.
pub fn run(command: RunCommand) -> Result<JoinHandle<Result<(), anyhow::Error>>> {
    Ok(match command {
        RunCommand::System {
            system_addr,
            supervision_update_timeout_in_sec,
            world_eviction_timeout_in_sec,
        } => tokio::spawn(spawn_system(
            system_addr.parse()?,
            Duration::from_secs(supervision_update_timeout_in_sec),
            Duration::from_secs(world_eviction_timeout_in_sec),
        )),
        RunCommand::Host {
            system_addr,
            host_world,
            host_rank,
            supervision_update_interval_in_sec,
        } => tokio::spawn(spawn_host(
            system_addr.parse()?,
            host_world.parse()?,
            host_rank,
            Duration::from_secs(supervision_update_interval_in_sec),
        )),
        RunCommand::Controller(ControllerCommand {
            worker_world,
            system_addr,
            controller_actor_id,
            world_size,
            num_procs_per_host,
            worker_name,
            program,
            supervision_query_interval_in_sec,
            supervision_update_interval_in_sec,
            worker_progress_check_interval_in_sec,
            operation_timeout_in_sec,
            operations_per_worker_progress_request,
            is_cpu_worker,
            extra_proc_labels,
            fail_on_worker_timeout,
        }) => tokio::spawn(spawn_controller(
            system_addr.parse()?,
            controller_actor_id.parse()?,
            world_size,
            num_procs_per_host,
            worker_world.parse()?,
            worker_name,
            program,
            Duration::from_secs(supervision_query_interval_in_sec),
            Duration::from_secs(supervision_update_interval_in_sec),
            Duration::from_secs(worker_progress_check_interval_in_sec),
            Duration::from_secs(operation_timeout_in_sec),
            operations_per_worker_progress_request,
            is_cpu_worker,
            extra_proc_labels,
            fail_on_worker_timeout,
        )),
    })
}

/// Spawn the system actor
async fn spawn_system(
    system_addr: ChannelAddr,
    supervision_update_timeout: Duration,
    world_eviction_timeout: Duration,
) -> anyhow::Result<()> {
    tracing::info!("spawning system");

    let handle = System::serve(
        system_addr.clone(),
        supervision_update_timeout,
        world_eviction_timeout,
    )
    .await?;
    tracing::info!("system serve: {}", handle.local_addr());

    // This will not end until the system actor is stopped.
    handle.system_actor_handle().clone().await;

    tracing::info!("system actor exited");

    Ok(())
}

/// Spawn the host actor
#[tracing::instrument(skip_all)]
async fn spawn_host(
    system_addr: ChannelAddr,
    host_world_id: WorldId,
    host_rank: Index,
    supervision_update_interval: Duration,
) -> anyhow::Result<()> {
    tracing::info!("spawning host actor");

    let proc_id = host_world_id.proc_id(host_rank);
    let host_addr = ChannelAddr::any(system_addr.transport());

    let bootstrap = ProcActor::bootstrap(
        proc_id.clone(),
        host_world_id.clone(),
        host_addr,
        system_addr,
        supervision_update_interval,
        HashMap::from([(
            LABEL_NAME_ROLE.to_string(),
            LABEL_VALUE_ROLE_HOST.to_string(),
        )]),
        ProcLifecycleMode::ManagedBySystem,
    )
    .await?;
    tracing::info!(
        "{}: joined; host actor: {}",
        proc_id,
        bootstrap.proc_actor.actor_id()
    );

    // This will not end until the proc actor is stopped.
    bootstrap.proc_actor.await;

    Ok(())
}

/// Spawn the controller actor. The order of bootstrap is:
/// 1. Create the new worker world.
/// 2. Check if the worker world is alive
/// 3. Spawn the controller proc and actor.
/// 4. Spawn all the worker actors and wait for them to be ready.
/// 5. Create the new controller world. The client is able to send traffic
///    only after both the controller and worker worlds are alive.
#[tracing::instrument(skip_all)]
async fn spawn_controller(
    system_addr: ChannelAddr,
    controller_actor_id: ActorId,
    num_procs: usize,
    num_procs_per_host: usize,
    worker_world_id: WorldId,
    worker_name: String,
    program: Option<String>,
    supervision_query_interval: Duration,
    supervision_update_interval: Duration,
    worker_progress_check_interval: Duration,
    operation_timeout: Duration,
    operations_per_worker_progress_request: u64,
    is_cpu_worker: bool,
    extra_proc_labels: Option<Vec<(String, String)>>,
    fail_on_worker_timeout: bool,
) -> anyhow::Result<()> {
    tracing::info!("spawning controller");

    let mut system = hyperactor_multiprocess::System::new(system_addr.clone());
    let instance = system.attach().await.unwrap();

    self::create_world(
        &instance,
        controller_actor_id.clone(),
        num_procs,
        num_procs_per_host,
        worker_world_id.clone(),
        program,
    )
    .await?;
    let handle = self::bootstrap_controller(
        system_addr,
        None, // listen_addr
        controller_actor_id.clone(),
        num_procs,
        worker_world_id.clone(),
        worker_name.clone(),
        supervision_query_interval,
        supervision_update_interval,
        worker_progress_check_interval,
        operation_timeout,
        operations_per_worker_progress_request,
        extra_proc_labels,
        fail_on_worker_timeout,
    )
    .await?;

    self::spawn_worker_actors(
        &instance,
        controller_actor_id.clone(),
        num_procs,
        worker_world_id,
        worker_name,
        is_cpu_worker,
    )
    .await?;

    // Controller will join its own world.
    // This will announce itself as live so the client can observe it.
    system_actor::SYSTEM_ACTOR_REF
        .upsert_world(
            &instance,
            WorldId(controller_actor_id.world_name().to_string()),
            Shape::Definite(vec![1]),
            1,
            Environment::Local,
            HashMap::new(),
        )
        .await?;
    tracing::info!(
        "created new controller world {}",
        controller_actor_id.world_name()
    );

    // This will not end until the system actor is stopped.
    handle.await;

    tracing::info!("controller actor exited");

    Ok(())
}

/// Bootstraps the controller actor.
/// Listen address is optional. If not provided, it will be assigned with a random available
/// address that has the same transport as the system address.
pub async fn bootstrap_controller(
    system_addr: ChannelAddr,
    listen_addr: Option<ChannelAddr>,
    controller_actor_id: ActorId,
    num_procs: usize,
    worker_world_id: WorldId,
    worker_name: String,
    supervision_query_interval: Duration,
    supervision_update_interval: Duration,
    worker_progress_check_interval: Duration,
    operation_timeout: Duration,
    operations_per_worker_progress_request: u64,
    extra_controller_labels: Option<Vec<(String, String)>>,
    fail_on_worker_timeout: bool,
) -> anyhow::Result<ActorHandle<ProcActor>> {
    let listen_addr = listen_addr.unwrap_or(ChannelAddr::any(system_addr.transport()));
    let mut controller_labels = HashMap::from([
        (
            LABEL_NAME_ROLE.to_string(),
            LABEL_VALUE_ROLE_CONTROLLER.to_string(),
        ),
        (
            LABEL_NAME_WORKER_WORLD.to_string(),
            worker_world_id.to_string(),
        ),
    ]);
    tracing::info!("controller labels: {:?}", extra_controller_labels);
    if let Some(extra_controller_labels) = extra_controller_labels {
        controller_labels.extend(extra_controller_labels);
    }
    let (handle, actor_ref) = ControllerActor::bootstrap(
        controller_actor_id.clone(),
        listen_addr,
        system_addr,
        ControllerParams {
            world_size: num_procs,
            comm_actor_ref: ActorRef::<CommActor>::attest(
                controller_actor_id.proc_id().actor_id(COMM_ACTOR_NAME, 0),
            ),
            worker_gang_ref: GangRef::attest(GangId(worker_world_id.clone(), worker_name.clone())),
            supervision_query_interval,
            worker_progress_check_interval,
            operation_timeout,
            operations_per_worker_progress_request,
            fail_on_worker_timeout,
        },
        supervision_update_interval,
        controller_labels,
    )
    .await?;
    tracing::info!("controller starts with id: {}", actor_ref.actor_id());

    Ok(handle)
}

async fn create_world(
    cx: &impl context::Actor,
    controller_actor_id: ActorId,
    num_procs: usize,
    num_procs_per_host: usize,
    worker_world_id: WorldId,
    program: Option<String>,
) -> anyhow::Result<()> {
    system_actor::SYSTEM_ACTOR_REF
        .upsert_world(
            cx,
            worker_world_id.clone(),
            Shape::Definite(vec![num_procs]),
            num_procs_per_host,
            match program {
                Some(program) => Environment::Exec { program },
                None => Environment::Local,
            },
            HashMap::from([
                (LABEL_NAME_WORKER.to_string(), "1".to_string()),
                (
                    LABEL_NAME_CONTROLLER_ACTOR_ID.to_string(),
                    controller_actor_id.to_string(),
                ),
            ]),
        )
        .await?;
    tracing::info!("created new worker world {}", worker_world_id);

    // Wait for all the worker procs to join the worker world.
    let timeout = hyperactor::config::global::get(hyperactor::config::MESSAGE_DELIVERY_TIMEOUT);
    tracing::info!("waiting for worker world {} to be alive", worker_world_id);
    loop {
        let snapshot = RealClock
            .timeout(timeout, async {
                system_actor::SYSTEM_ACTOR_REF
                    .snapshot(
                        cx,
                        system_actor::SystemSnapshotFilter {
                            worlds: vec![worker_world_id.clone()],
                            world_labels: HashMap::new(),
                            proc_labels: HashMap::new(),
                        },
                    )
                    .await
            })
            .await?;
        let snapshot = snapshot?;
        if let Some(world) = snapshot.worlds.get(&worker_world_id) {
            if world.status.is_live() {
                break;
            }
        }
        RealClock.sleep(Duration::from_millis(10)).await;
    }
    tracing::info!(
        "worker world {} is alive; spawning {} worker actors",
        worker_world_id,
        num_procs
    );
    Ok(())
}

async fn spawn_worker_actors(
    cx: &impl context::Actor,
    controller_actor_id: ActorId,
    num_procs: usize,
    worker_world_id: WorldId,
    worker_name: String,
    is_cpu_worker: bool,
) -> anyhow::Result<()> {
    // Bootstrap worker actors and wait for them to be ready.
    let (spawned_port, mut spawned_receiver) = open_port(cx);
    for rank in 0..num_procs {
        let param = WorkerParams {
            world_size: num_procs,
            // Rank assignment is consistent with proc indices.
            rank,
            // TODO: We never use device index during Monarch bootstrap.
            // Instead, CUDA_VISIBLE_DEVICES is used for workers to access CUDA devices.
            device_index: if is_cpu_worker { None } else { Some(0) },
            controller_actor: ActorRef::attest(controller_actor_id.clone()),
        };
        let worker_proc =
            ActorRef::<ProcActor>::attest(worker_world_id.proc_id(rank).actor_id("proc", 0));

        worker_proc
            .spawn(
                cx,
                // Use explicit actor type to avoid the WorkActor dependency.
                "monarch_tensor_worker::WorkerActor".to_owned(),
                worker_name.clone(),
                bincode::serialize(&param)?,
                spawned_port.bind(),
            )
            .await?;
    }
    let mut spawned = HashSet::new();
    while spawned.len() < num_procs {
        spawned.insert(spawned_receiver.recv().await?);
    }
    tracing::info!("spawned {} worker actors", num_procs);

    Ok(())
}

pub fn parse_key_val(s: &str) -> anyhow::Result<(String, String)> {
    match s.split_once('=') {
        None => Err(anyhow::anyhow!("invalid KEY=value: no `=` found in `{s}`")),
        Some((a, b)) => Ok((a.to_owned(), b.to_owned())),
    }
}

pub fn register_python_bindings(controller_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    controller_mod.add_class::<ControllerServerRequest>()?;
    controller_mod.add_class::<ControllerServerResponse>()?;
    controller_mod.add_class::<RunCommand>()?;
    controller_mod.add_class::<ControllerCommand>()?;
    Ok(())
}
