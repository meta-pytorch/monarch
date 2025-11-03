/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Proc actor manages a proc. It works in conjunction with a
//! [`super::system_actor::SystemActor`]. Proc actors are usually spawned
//! as the "agent" to manage a proc directly.

use core::fmt;
use std::collections::HashMap;
use std::process::Stdio;
use std::time::Duration;
use std::time::SystemTime;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::Context;
use hyperactor::Data;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Named;
use hyperactor::OncePortRef;
use hyperactor::PortRef;
use hyperactor::RefClient;
use hyperactor::RemoteMessage;
use hyperactor::WorldId;
use hyperactor::actor::ActorHandle;
use hyperactor::actor::Referable;
use hyperactor::actor::remote::Remote;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::clock::Clock;
use hyperactor::clock::ClockKind;
use hyperactor::context;
use hyperactor::mailbox::BoxedMailboxSender;
use hyperactor::mailbox::DialMailboxRouter;
use hyperactor::mailbox::MailboxAdminMessage;
use hyperactor::mailbox::MailboxAdminMessageHandler;
use hyperactor::mailbox::MailboxClient;
use hyperactor::mailbox::MailboxServer;
use hyperactor::mailbox::MailboxServerHandle;
use hyperactor::mailbox::open_port;
use hyperactor::proc::ActorLedgerSnapshot;
use hyperactor::proc::Proc;
use hyperactor::reference::ActorId;
use hyperactor::reference::ActorRef;
use hyperactor::reference::Index;
use hyperactor::reference::ProcId;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_mesh::comm::CommActor;
use serde::Deserialize;
use serde::Serialize;
use tokio::process::Command;
use tokio::sync::watch;
use tokio_retry::strategy::jitter;

use crate::pyspy::PySpyTrace;
use crate::pyspy::py_spy;
use crate::supervision::ProcStatus;
use crate::supervision::ProcSupervisionMessageClient;
use crate::supervision::ProcSupervisionState;
use crate::supervision::ProcSupervisor;
use crate::system_actor::ProcLifecycleMode;
use crate::system_actor::SYSTEM_ACTOR_REF;
use crate::system_actor::SystemActor;
use crate::system_actor::SystemMessageClient;

static HYPERACTOR_WORLD_ID: &str = "HYPERACTOR_WORLD_ID";
static HYPERACTOR_PROC_ID: &str = "HYPERACTOR_PROC_ID";
static HYPERACTOR_BOOTSTRAP_ADDR: &str = "HYPERACTOR_BOOTSTRAP_ADDR";
static HYPERACTOR_WORLD_SIZE: &str = "HYPERACTOR_WORLD_SIZE";
static HYPERACTOR_RANK: &str = "HYPERACTOR_RANK";
static HYPERACTOR_LOCAL_RANK: &str = "HYPERACTOR_LOCAL_RANK";

/// All setup parameters for an actor within a proc actor.
#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum Environment {
    /// The actor is spawned within the proc actor.
    Local,

    /// Spawn the actor in a separate proc.
    /// The program is the executable that will be spawned
    /// by the host actor. The program will operate under 3
    /// environment variables: ${HYPERACTOR_PROC_ID}: the proc id
    /// of the proc actor being spawned,
    /// ${HYPERACTOR_BOOTSTRAP_ADDR}: the address of the system actor and
    /// ${HYPERACTOR_WORLD_SIZE}: the world size the proc is a part of.
    /// This is often useful where a actor needs to know the side of the
    /// world like worker using nccl comms.
    Exec {
        /// The program to run in order to spawn the actor.
        program: String,
    },
}

/// The state of the proc.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Named)]
pub enum ProcState {
    /// The proc is waiting to the join the system.
    AwaitingJoin,

    /// The proc has joined the system.
    Joined,
}

impl fmt::Display for ProcState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AwaitingJoin => write!(f, "AwaitingJoin"),
            Self::Joined => write!(f, "Joined"),
        }
    }
}

/// The result after stopping the proc.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Named)]
pub struct ProcStopResult {
    /// The proc being stopped.
    pub proc_id: ProcId,
    /// The number of actors observed to stop.
    pub actors_stopped: usize,
    /// The number of proc actors that were aborted.
    pub actors_aborted: usize,
}

/// A snapshot of the proc.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Named)]
pub struct ProcSnapshot {
    /// The state of the proc.
    pub state: ProcState,
    /// The snapshot of the actors in the proc.
    pub actors: ActorLedgerSnapshot,
}

/// Remote py-spy dump configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PySpyConfig {
    /// Nonblocking dump for only python frames.
    NonBlocking,
    /// Blocking dump. Specifies whether native threads are included,
    /// and if so, whether those threads should also include native stack frames.
    Blocking {
        /// Dump native stack frames.
        native: Option<bool>,
        /// Dump stack frames for native threads. Implies native.
        native_all: Option<bool>,
    },
}

/// A stack trace of the proc.
/// Wrapper to dervice Named.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Named)]
pub struct StackTrace {
    /// The stack trace.
    pub trace: PySpyTrace,
}

/// Proc management messages.
#[derive(
    Handler,
    HandleClient,
    RefClient,
    Debug,
    Serialize,
    Deserialize,
    Clone,
    PartialEq,
    Named
)]
pub enum ProcMessage {
    /// Indicate that the proc has joined the system. This is sent by
    /// the system actor when the proc has been registered and is ready
    /// to receive instructions.
    #[log_level(debug)]
    Joined(),

    /// Retrieve the state of the proc.
    #[log_level(debug)]
    State {
        /// Used to return the result of the caller.
        #[reply]
        ret: OncePortRef<ProcState>,
    },

    /// Spawn an actor on the proc to the provided name.
    Spawn {
        /// registered actor type
        actor_type: String,
        /// spawned actor name
        actor_name: String,
        /// serialized parameters
        params_data: Data,
        /// reply port; the proc should send its rank to indicate a spawned actor
        status_port: PortRef<Index>,
    },

    /// Spawn a set of proc actors in the specified world (never its own)
    SpawnProc {
        /// Spawn the proc locally or in a separate program.
        env: Environment,
        /// The world into which to spawn the procs.
        world_id: WorldId,
        /// The proc ids of the procs to spawn.
        proc_ids: Vec<ProcId>,
        /// The total number of procs in the specified world.
        world_size: usize,
    },

    /// Self message to trigger a supervision update to the system actor.
    #[log_level(debug)]
    UpdateSupervision(),

    /// Stop the proc. Returns a pair of counts:
    /// - the number of actors observed to stop;
    /// - the number of proc actors not observed to stop.
    ///
    /// There will be at least one actor (the proc-actor itself) that
    /// will be not observed to stop. If there is more than one
    /// timeouts are indicated.
    Stop {
        /// The duration to wait for an actor to report status `Stopped`.
        timeout: Duration,
        /// Used to return the result to the caller.
        #[reply]
        reply_to: OncePortRef<ProcStopResult>,
    },

    /// Return a snapshot view of this proc. Used for debugging.
    #[log_level(debug)]
    Snapshot {
        /// Used to return the result of the caller.
        #[reply]
        reply_to: OncePortRef<ProcSnapshot>,
    },

    /// Get the proc local addr.
    LocalAddr {
        /// Used to return the result of the caller.
        #[reply]
        reply_to: OncePortRef<ChannelAddr>,
    },

    /// Run pyspy on the current proc and return the stack trace.
    #[log_level(debug)]
    PySpyDump {
        /// Dump config.
        config: PySpyConfig,
        /// Used to return the result of the caller.
        #[reply]
        reply_to: OncePortRef<StackTrace>,
    },
}

/// Parameters for managing the proc.
#[derive(Debug, Clone)]
pub struct ProcActorParams {
    /// The proc that is managed by this actor.
    pub proc: Proc,

    /// The system world to which this proc belongs.
    pub world_id: WorldId,

    /// Reference to the system actor that is managing this proc.
    pub system_actor_ref: ActorRef<SystemActor>,

    /// The channel address used to communicate with the system actor
    /// that manages this proc. This is passed through so that the proc
    /// can spawn sibling procs.
    pub bootstrap_channel_addr: ChannelAddr,

    /// The local address of this proc actor. This is used by
    /// the proc actor to register the proc with the system actor.
    pub local_addr: ChannelAddr,

    /// Watch into which the proc's state is published.
    pub state_watch: watch::Sender<ProcState>,

    /// Reference to supervisor.
    pub supervisor_actor_ref: ActorRef<ProcSupervisor>,

    /// Interval of reporting supervision status to the system actor.
    pub supervision_update_interval: Duration,

    /// Arbitrary labels for the proc. They can be used later to query
    /// proc(s) using system snapshot api.
    pub labels: HashMap<String, String>,

    /// Proc lifecycle management mode.
    ///
    /// If a proc is not managed
    ///   * it will not be stopped when system shutdowns.
    ///   * it will not be captured by system snapshot.
    ///
    /// Not being managed is useful for procs that runs on the client side,
    /// which might need to stay around for a while after the system is gone.
    pub lifecycle_mode: ProcLifecycleMode,
}

/// Outputs from bootstrapping proc.
#[derive(Debug)]
pub struct BootstrappedProc {
    /// Handle to proc actor.
    pub proc_actor: ActorHandle<ProcActor>,
    /// Handle to comm actor.
    pub comm_actor: ActorHandle<CommActor>,
    /// Mailbox for address served by proc actor.
    pub mailbox: MailboxServerHandle,
}

/// ProcActor manages a single proc. It is responsible for managing
/// the lifecycle of all of the proc's actors, and to route messages
/// accordingly.
#[derive(Debug)]
#[hyperactor::export(
    handlers = [
        ProcMessage,
        MailboxAdminMessage,
    ],
)]
pub struct ProcActor {
    params: ProcActorParams,
    state: ProcState,
    remote: Remote,
    last_successful_supervision_update: SystemTime,
}

impl ProcActor {
    /// Bootstrap a proc actor with the provided proc id. The bootstrapped proc
    /// actor will use the provided listen address to serve its mailbox, while
    /// the bootstrap address is used to register with the system actor.
    #[hyperactor::instrument]
    pub async fn bootstrap(
        proc_id: ProcId,
        world_id: WorldId,
        listen_addr: ChannelAddr,
        bootstrap_addr: ChannelAddr,
        supervision_update_interval: Duration,
        labels: HashMap<String, String>,
        lifecycle_mode: ProcLifecycleMode,
    ) -> Result<BootstrappedProc, anyhow::Error> {
        let system_supervision_ref: ActorRef<ProcSupervisor> =
            ActorRef::attest(SYSTEM_ACTOR_REF.actor_id().clone());

        Self::try_bootstrap(
            proc_id.clone(),
            world_id.clone(),
            listen_addr.clone(),
            bootstrap_addr.clone(),
            system_supervision_ref.clone(),
            supervision_update_interval,
            labels,
            lifecycle_mode,
        )
        .await
        .inspect_err(|err| {
            tracing::error!(
                "bootstrap {} {} {} {}: {}",
                proc_id,
                world_id,
                listen_addr,
                bootstrap_addr,
                err
            );
        })
    }

    /// Attempt to bootstrap a proc actor with the provided proc id. The bootstrapped proc
    /// actor will use the provided listen address to serve its mailbox, while
    /// the bootstrap address is used to register with the system actor.
    #[hyperactor::instrument]
    pub async fn try_bootstrap(
        proc_id: ProcId,
        world_id: WorldId,
        listen_addr: ChannelAddr,
        bootstrap_addr: ChannelAddr,
        supervisor_actor_ref: ActorRef<ProcSupervisor>,
        supervision_update_interval: Duration,
        labels: HashMap<String, String>,
        lifecycle_mode: ProcLifecycleMode,
    ) -> Result<BootstrappedProc, anyhow::Error> {
        let system_sender =
            BoxedMailboxSender::new(MailboxClient::new(channel::dial(bootstrap_addr.clone())?));
        let clock = ClockKind::for_channel_addr(&listen_addr);

        let proc_forwarder =
            BoxedMailboxSender::new(DialMailboxRouter::new_with_default(system_sender));
        let proc = Proc::new_with_clock(proc_id.clone(), proc_forwarder, clock);
        Self::bootstrap_for_proc(
            proc,
            world_id,
            listen_addr,
            bootstrap_addr,
            supervisor_actor_ref,
            supervision_update_interval,
            labels,
            lifecycle_mode,
        )
        .await
    }

    /// Bootstrap a proc actor with the provided proc. The bootstrapped proc actor
    /// will use the provided listen address to serve its mailbox, while the bootstrap
    /// address is used to register with the system actor.
    #[hyperactor::instrument]
    pub async fn bootstrap_for_proc(
        proc: Proc,
        world_id: WorldId,
        listen_addr: ChannelAddr,
        bootstrap_addr: ChannelAddr,
        supervisor_actor_ref: ActorRef<ProcSupervisor>,
        supervision_update_interval: Duration,
        labels: HashMap<String, String>,
        lifecycle_mode: ProcLifecycleMode,
    ) -> Result<BootstrappedProc, anyhow::Error> {
        let (local_addr, rx) = channel::serve(listen_addr, "bootstrap_for_proc")?;
        let mailbox_handle = proc.clone().serve(rx);
        let (state_tx, mut state_rx) = watch::channel(ProcState::AwaitingJoin);

        let handle = match proc
            .clone()
            .spawn::<Self>(
                "proc",
                ProcActorParams {
                    proc: proc.clone(),
                    world_id: world_id.clone(),
                    system_actor_ref: SYSTEM_ACTOR_REF.clone(),
                    bootstrap_channel_addr: bootstrap_addr,
                    local_addr,
                    state_watch: state_tx,
                    supervisor_actor_ref,
                    supervision_update_interval,
                    labels,
                    lifecycle_mode,
                },
            )
            .await
        {
            Ok(handle) => handle,
            Err(e) => {
                Self::failed_proc_bootstrap_cleanup(mailbox_handle).await;
                return Err(e);
            }
        };

        let comm_actor = match proc
            .clone()
            .spawn::<CommActor>("comm", Default::default())
            .await
        {
            Ok(handle) => handle,
            Err(e) => {
                Self::failed_proc_bootstrap_cleanup(mailbox_handle).await;
                return Err(e);
            }
        };
        comm_actor.bind::<CommActor>();

        loop {
            let proc_state = state_rx.borrow_and_update().clone();
            tracing::info!("{}: state: {:?}", &proc.proc_id(), proc_state);
            if matches!(proc_state, ProcState::Joined) {
                break;
            }
            match state_rx.changed().await {
                Ok(_) => {}
                Err(e) => {
                    Self::failed_proc_bootstrap_cleanup(mailbox_handle).await;
                    return Err(e.into());
                }
            }
        }

        proc.set_supervision_coordinator(handle.port::<ActorSupervisionEvent>())?;

        Ok(BootstrappedProc {
            proc_actor: handle,
            mailbox: mailbox_handle,
            comm_actor,
        })
    }

    /// Shutdown the mailbox server to free up rx and its cooresponding listen address.
    /// Because in the next bootstrap attempt, the same listen address will be used.
    async fn failed_proc_bootstrap_cleanup(mailbox_handle: MailboxServerHandle) {
        mailbox_handle.stop("failed proc bootstrap cleanup");
        if let Err(shutdown_err) = mailbox_handle.await {
            // Ignore the shutdown error and populate the original error.
            tracing::error!(
                "error shutting down during a failed bootstrap attempt: {}",
                shutdown_err
            );
        }
    }
}

#[async_trait]
impl Actor for ProcActor {
    type Params = ProcActorParams;

    async fn new(params: ProcActorParams) -> Result<Self, anyhow::Error> {
        let last_successful_supervision_update = params.proc.clock().system_time_now();
        Ok(Self {
            params,
            state: ProcState::AwaitingJoin,
            remote: Remote::collect(),
            last_successful_supervision_update,
        })
    }

    async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
        // Bind ports early so that when the proc actor joins, it can serve.
        this.bind::<Self>();

        // Join the system.
        self.params
            .system_actor_ref
            .join(
                this,
                /*world_id=*/ self.params.world_id.clone(),
                /*proc_id=*/ self.params.proc.proc_id().clone(),
                /*proc_message_port=*/ this.port().bind(),
                /*proc_addr=*/ self.params.local_addr.clone(),
                self.params.labels.clone(),
                self.params.lifecycle_mode.clone(),
            )
            .await?;

        // Trigger supervision status update
        // TODO: let the system actor determine/update the supervision interval.
        // Maybe by returning it from the join call, or defining some other proc
        // message to adjust it.
        if self.params.supervision_update_interval > Duration::from_secs(0)
            && self.params.lifecycle_mode == ProcLifecycleMode::ManagedBySystem
        {
            this.self_message_with_delay(
                ProcMessage::UpdateSupervision(),
                self.params.supervision_update_interval,
            )?;
        }

        Ok(())
    }
}

impl ProcActor {
    /// This proc's rank in the world.
    fn rank(&self) -> Index {
        self.params
            .proc
            .proc_id()
            .rank()
            .expect("proc must be ranked")
    }
}

#[hyperactor::forward(MailboxAdminMessage)]
#[async_trait]
impl MailboxAdminMessageHandler for ProcActor {
    async fn update_address(
        &mut self,
        cx: &Context<Self>,
        proc_id: ProcId,
        addr: ChannelAddr,
    ) -> Result<(), anyhow::Error> {
        tracing::trace!(
            "received address update:\n{:#?}",
            MailboxAdminMessage::UpdateAddress {
                proc_id: proc_id.clone(),
                addr: addr.clone()
            }
        );
        let forwarder = cx.proc().forwarder();
        if let Some(router) = forwarder.downcast_ref::<DialMailboxRouter>() {
            router.bind(proc_id.into(), addr);
        } else {
            tracing::warn!(
                "proc {} received update_address but does not use a DialMailboxRouter",
                cx.proc().proc_id()
            );
        }

        Ok(())
    }
}

#[async_trait]
#[hyperactor::forward(ProcMessage)]
impl ProcMessageHandler for ProcActor {
    async fn joined(&mut self, _cx: &Context<Self>) -> Result<(), anyhow::Error> {
        self.state = ProcState::Joined;
        let _ = self.params.state_watch.send(self.state.clone());
        Ok(())
    }

    async fn state(&mut self, _cx: &Context<Self>) -> Result<ProcState, anyhow::Error> {
        Ok(self.state.clone())
    }

    async fn spawn(
        &mut self,
        cx: &Context<Self>,
        actor_type: String,
        actor_name: String,
        params_data: Data,
        status_port: PortRef<Index>,
    ) -> Result<(), anyhow::Error> {
        let _actor_id = self
            .remote
            .gspawn(&self.params.proc, &actor_type, &actor_name, params_data)
            .await?;

        // Signal that the actor has joined:
        status_port.send(cx, self.rank())?;
        Ok(())
    }

    async fn spawn_proc(
        &mut self,
        _cx: &Context<Self>,
        env: Environment,
        world_id: WorldId,
        proc_ids: Vec<ProcId>,
        world_size: usize,
    ) -> Result<(), anyhow::Error> {
        for (index, proc_id) in proc_ids.into_iter().enumerate() {
            let proc_world_id = proc_id
                .world_id()
                .expect("proc must be ranked for world_id access")
                .clone();
            // Check world id isn't the same as this proc's world id.
            if &proc_world_id
                == self
                    .params
                    .proc
                    .proc_id()
                    .world_id()
                    .expect("proc must be ranked for world_id access")
                || &world_id
                    == self
                        .params
                        .proc
                        .proc_id()
                        .world_id()
                        .expect("proc must be ranked for world_id access")
            {
                return Err(anyhow::anyhow!(
                    "cannot spawn proc in same world {}",
                    proc_world_id
                ));
            }
            match env {
                Environment::Local => {
                    ProcActor::bootstrap(
                        proc_id,
                        world_id.clone(),
                        ChannelAddr::any(self.params.bootstrap_channel_addr.transport()),
                        self.params.bootstrap_channel_addr.clone(),
                        self.params.supervision_update_interval,
                        HashMap::new(),
                        ProcLifecycleMode::ManagedBySystem,
                    )
                    .await?;
                }
                Environment::Exec { ref program } => {
                    tracing::info!("spawning proc {} with program {}", proc_id, program);
                    let mut child = Command::new(program);
                    let _ = child
                        .env(HYPERACTOR_WORLD_ID, world_id.to_string())
                        .env(HYPERACTOR_PROC_ID, proc_id.to_string())
                        .env(
                            HYPERACTOR_BOOTSTRAP_ADDR,
                            self.params.bootstrap_channel_addr.to_string(),
                        )
                        .env(HYPERACTOR_WORLD_SIZE, world_size.to_string())
                        .env(
                            HYPERACTOR_RANK,
                            proc_id
                                .rank()
                                .expect("proc must be ranked for rank env var")
                                .to_string(),
                        )
                        .env(HYPERACTOR_LOCAL_RANK, index.to_string())
                        .stdin(Stdio::null())
                        .stdout(Stdio::inherit())
                        .stderr(Stdio::inherit())
                        .spawn()?;
                }
            }
        }
        Ok(())
    }

    async fn update_supervision(&mut self, cx: &Context<Self>) -> Result<(), anyhow::Error> {
        // Delay for next supervision update with some jitter.
        let delay = jitter(self.params.supervision_update_interval);

        // Only start updating supervision after the proc is joined.
        if self.state != ProcState::Joined {
            cx.self_message_with_delay(ProcMessage::UpdateSupervision(), delay)?;
            return Ok(());
        }

        let msg = ProcSupervisionState {
            world_id: self.params.world_id.clone(),
            proc_id: self.params.proc.proc_id().clone(),
            proc_addr: self.params.local_addr.clone(),
            proc_health: ProcStatus::Alive,
            failed_actors: Vec::new(),
        };

        match cx
            .clock()
            .timeout(
                // TODO: make the timeout configurable
                Duration::from_secs(10),
                self.params.supervisor_actor_ref.update(cx, msg),
            )
            .await
        {
            Ok(_) => {
                self.last_successful_supervision_update = cx.clock().system_time_now();
            }
            Err(_) => {}
        }

        let supervision_staleness = self
            .last_successful_supervision_update
            .elapsed()
            .unwrap_or_default();
        // Timeout when there are 3 consecutive supervision updates that fail.
        // TODO: make number of failed updates configurable.
        if supervision_staleness > 5 * self.params.supervision_update_interval {
            tracing::error!(
                "system actor isn't responsive to supervision update, stopping the proc"
            );
            // System actor is not responsive to supervision update, it is likely dead. Stop this proc.
            // TODO: make the timeout configurable
            self.stop(cx, Duration::from_secs(5)).await?;
        } else {
            // Schedule the next supervision update with some jitter.
            let delay = jitter(self.params.supervision_update_interval);
            cx.self_message_with_delay(ProcMessage::UpdateSupervision(), delay)?;
        }

        Ok(())
    }

    async fn stop(
        &mut self,
        cx: &Context<Self>,
        timeout: Duration,
    ) -> Result<ProcStopResult, anyhow::Error> {
        tracing::info!("stopping proc {}", self.params.proc.proc_id());
        self.params
            .proc
            .destroy_and_wait(timeout, Some(cx))
            .await
            .map(|(stopped, aborted)| {
                tracing::info!("stopped proc {}", self.params.proc.proc_id());
                ProcStopResult {
                    proc_id: self.params.proc.proc_id().clone(),
                    actors_stopped: stopped.len(),
                    actors_aborted: aborted.len(),
                }
            })
    }

    async fn snapshot(&mut self, _cx: &Context<Self>) -> Result<ProcSnapshot, anyhow::Error> {
        let state = self.state.clone();
        let actors = self.params.proc.ledger_snapshot();
        Ok(ProcSnapshot { state, actors })
    }

    async fn local_addr(&mut self, _cx: &Context<Self>) -> Result<ChannelAddr, anyhow::Error> {
        Ok(self.params.local_addr.clone())
    }

    async fn py_spy_dump(
        &mut self,
        _cx: &Context<Self>,
        config: PySpyConfig,
    ) -> Result<StackTrace, anyhow::Error> {
        let pid = std::process::id() as i32;
        tracing::info!(
            "running py-spy on proc {}, process id: {}",
            self.params.proc.proc_id(),
            pid
        );
        let trace = match config {
            PySpyConfig::Blocking { native, native_all } => {
                py_spy(
                    pid,
                    native.unwrap_or_default(),
                    native_all.unwrap_or_default(),
                    true,
                )
                .await?
            }
            PySpyConfig::NonBlocking => py_spy(pid, false, false, false).await?,
        };
        Ok(StackTrace { trace })
    }
}

#[async_trait]
impl Handler<ActorSupervisionEvent> for ProcActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        event: ActorSupervisionEvent,
    ) -> anyhow::Result<()> {
        let status = event.status();
        let message = ProcSupervisionState {
            world_id: self.params.world_id.clone(),
            proc_id: self.params.proc.proc_id().clone(),
            proc_addr: self.params.local_addr.clone(),
            proc_health: ProcStatus::Alive,
            failed_actors: Vec::from([(event.actor_id, status)]),
        };
        self.params.supervisor_actor_ref.update(cx, message).await?;
        Ok(())
    }
}

/// Convenience utility to spawn an actor on a proc. Spawn returns
/// with the new ActorRef on success.
pub async fn spawn<A: Actor + Referable>(
    cx: &impl context::Actor,
    proc_actor: &ActorRef<ProcActor>,
    actor_name: &str,
    params: &A::Params,
) -> Result<ActorRef<A>, anyhow::Error>
where
    A::Params: RemoteMessage,
{
    let remote = Remote::collect();
    let (spawned_port, mut spawned_receiver) = open_port(cx);
    let ActorId(proc_id, _, _) = (*proc_actor).clone().into();

    proc_actor
        .spawn(
            cx,
            remote
                .name_of::<A>()
                .ok_or(anyhow::anyhow!("actor not registered"))?
                .into(),
            actor_name.into(),
            bincode::serialize(params)?,
            spawned_port.bind(),
        )
        .await?;

    // Wait for the spawned actor to join.
    while spawned_receiver.recv().await?
        != proc_id
            .rank()
            .expect("proc must be ranked for rank comparison")
    {}

    // Gspawned actors are always exported.
    Ok(ActorRef::attest(proc_id.actor_id(actor_name, 0)))
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashSet;
    use std::time::Duration;

    use hyperactor::actor::ActorStatus;
    use hyperactor::channel;
    use hyperactor::channel::ChannelAddr;
    use hyperactor::channel::ChannelTransport;
    use hyperactor::channel::TcpMode;
    use hyperactor::clock::Clock;
    use hyperactor::clock::RealClock;
    use hyperactor::forward;
    use hyperactor::id;
    use hyperactor::reference::ActorRef;
    use hyperactor::test_utils::pingpong::PingPongActor;
    use hyperactor::test_utils::pingpong::PingPongActorParams;
    use hyperactor::test_utils::pingpong::PingPongMessage;
    use maplit::hashset;
    use rand::Rng;
    use rand::distributions::Alphanumeric;
    use regex::Regex;

    use super::*;
    use crate::supervision::ProcSupervisionMessage;
    use crate::system::ServerHandle;
    use crate::system::System;

    const MAX_WAIT_TIME: Duration = Duration::new(10, 0);

    struct Bootstrapped {
        server_handle: ServerHandle,
        proc_actor_ref: ActorRef<ProcActor>,
        comm_actor_ref: ActorRef<CommActor>,
        client: Instance<()>,
    }

    async fn bootstrap() -> Bootstrapped {
        let server_handle = System::serve(
            ChannelAddr::any(ChannelTransport::Local),
            Duration::from_secs(10),
            Duration::from_secs(10),
        )
        .await
        .unwrap();

        let world_id = id!(world);
        let proc_id = world_id.proc_id(0);
        let bootstrap = ProcActor::bootstrap(
            proc_id,
            world_id,
            ChannelAddr::any(ChannelTransport::Local),
            server_handle.local_addr().clone(),
            Duration::from_secs(1),
            HashMap::new(),
            ProcLifecycleMode::ManagedBySystem,
        )
        .await
        .unwrap();

        // Now join the system and talk to the proc actor.

        let mut system = System::new(server_handle.local_addr().clone());
        let client = system.attach().await.unwrap();

        // This is really not cool. We should manage state subscriptions instead.
        let start = RealClock.now();
        let mut proc_state;
        loop {
            proc_state = bootstrap.proc_actor.state(&client).await.unwrap();

            if matches!(proc_state, ProcState::Joined) || start.elapsed() >= MAX_WAIT_TIME {
                break;
            }
        }
        assert_matches!(proc_state, ProcState::Joined);

        Bootstrapped {
            server_handle,
            proc_actor_ref: bootstrap.proc_actor.bind(),
            comm_actor_ref: bootstrap.comm_actor.bind(),
            client,
        }
    }

    #[tokio::test]
    async fn test_bootstrap() {
        let Bootstrapped { server_handle, .. } = bootstrap().await;

        println!("bootrapped, now waiting");

        server_handle.stop().await.unwrap();
        server_handle.await;
    }

    #[derive(Debug, Default, Actor)]
    #[hyperactor::export(
        spawn = true,
        handlers = [
            TestActorMessage,
        ],
    )]
    struct TestActor;

    #[derive(Handler, HandleClient, RefClient, Serialize, Deserialize, Debug, Named)]
    enum TestActorMessage {
        Increment(u64, #[reply] OncePortRef<u64>),
        Fail(String),
    }

    #[async_trait]
    #[forward(TestActorMessage)]
    impl TestActorMessageHandler for TestActor {
        async fn increment(&mut self, _cx: &Context<Self>, num: u64) -> Result<u64, anyhow::Error> {
            Ok(num + 1)
        }

        async fn fail(&mut self, _cx: &Context<Self>, err: String) -> Result<(), anyhow::Error> {
            Err(anyhow::anyhow!(err))
        }
    }

    #[tokio::test]
    async fn test_stop() {
        // Show here that the proc actors are stopped when the proc
        // actor receives a `Stop()` message.
        let Bootstrapped {
            server_handle,
            proc_actor_ref,
            client,
            ..
        } = bootstrap().await;

        const NUM_ACTORS: usize = 4usize;
        for i in 0..NUM_ACTORS {
            spawn::<TestActor>(&client, &proc_actor_ref, format!("test{i}").as_str(), &())
                .await
                .unwrap();
        }

        let ProcStopResult {
            proc_id: _,
            actors_stopped,
            actors_aborted,
        } = proc_actor_ref
            .stop(&client, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(NUM_ACTORS + 1, actors_stopped);
        assert_eq!(1, actors_aborted);

        server_handle.stop().await.unwrap();
        server_handle.await;
    }

    // Sleep
    #[derive(Debug, Default, Actor)]
    #[hyperactor::export(
        spawn = true,
        handlers = [
            u64,
        ],
    )]
    struct SleepActor {}

    #[async_trait]
    impl Handler<u64> for SleepActor {
        async fn handle(&mut self, _cx: &Context<Self>, message: u64) -> anyhow::Result<()> {
            let duration = message;
            RealClock.sleep(Duration::from_secs(duration)).await;
            Ok(())
        }
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_stop_timeout() {
        let Bootstrapped {
            server_handle,
            proc_actor_ref,
            client,
            ..
        } = bootstrap().await;

        const NUM_ACTORS: usize = 4usize;
        for i in 0..NUM_ACTORS {
            let sleep_secs = 5u64;
            let sleeper = spawn::<SleepActor>(
                &client,
                &proc_actor_ref,
                format!("sleeper{i}").as_str(),
                &(),
            )
            .await
            .unwrap();
            if i > 0 {
                sleeper.send(&client, sleep_secs).unwrap();
            }
        }

        let ProcStopResult {
            proc_id: _,
            actors_stopped,
            actors_aborted,
        } = proc_actor_ref
            .stop(&client, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(2, actors_stopped);
        assert_eq!((NUM_ACTORS - 1) + 1, actors_aborted);

        assert!(tracing_test::internal::logs_with_scope_contain(
            "hyperactor::proc",
            "world[0].proc[0]: aborting (delayed) JoinHandle"
        ));
        for i in 1..3 {
            assert!(tracing_test::internal::logs_with_scope_contain(
                "hyperactor::proc",
                format!("world[0].sleeper{}[0]: aborting JoinHandle", i).as_str()
            ));
        }
        logs_assert(|logs| {
            let count = logs
                .iter()
                .filter(|log| {
                    log.contains("aborting JoinHandle")
                        || log.contains("aborting (delayed) JoinHandle")
                })
                .count();
            if count == actors_aborted {
                Ok(())
            } else {
                Err("task abort counting error".to_string())
            }
        });

        server_handle.stop().await.unwrap();
        server_handle.await;
    }

    #[tokio::test]
    async fn test_spawn() {
        let Bootstrapped {
            server_handle,
            proc_actor_ref,
            client,
            ..
        } = bootstrap().await;

        let test_actor_ref = spawn::<TestActor>(&client, &proc_actor_ref, "test", &())
            .await
            .unwrap();

        let result = test_actor_ref.increment(&client, 1).await.unwrap();
        assert_eq!(result, 2);

        server_handle.stop().await.unwrap();
        server_handle.await;
    }

    #[cfg(target_os = "linux")]
    fn random_abstract_addr() -> ChannelAddr {
        let random_string = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(24)
            .map(char::from)
            .collect::<String>();
        format!("unix!@{random_string}").parse().unwrap()
    }

    #[cfg(target_os = "linux")] // remove after making abstract unix sockets store-and-forward
    #[tokio::test]
    async fn test_bootstrap_retry() {
        if std::env::var("CARGO_TEST").is_ok() {
            eprintln!("test skipped under cargo as it causes other tests to fail when run");
            return;
        }

        // Spawn the proc before the server is up. This is imperfect
        // as we rely on sleeping. Ideally we'd make sure the proc performs
        // at least one try before we start the server.
        let bootstrap_addr = random_abstract_addr();

        let bootstrap_addr_clone = bootstrap_addr.clone();
        let handle = tokio::spawn(async move {
            let world_id = id!(world);
            let proc_id = world_id.proc_id(0);
            let bootstrap = ProcActor::bootstrap(
                proc_id,
                world_id,
                random_abstract_addr(),
                bootstrap_addr_clone,
                Duration::from_secs(1),
                HashMap::new(),
                ProcLifecycleMode::ManagedBySystem,
            )
            .await
            .unwrap();

            // Proc actor should still be running.
            let mut status = bootstrap.proc_actor.status();
            assert_eq!(*status.borrow_and_update(), ActorStatus::Idle);
        });

        // Sleep for enough time, the ProcActor supervision shouldn't timed out causing ProcActor to stop.
        // When System actor is brought up later, it should finish properly.
        RealClock.sleep(Duration::from_secs(5)).await;

        let _server_handle = System::serve(
            bootstrap_addr,
            Duration::from_secs(10),
            Duration::from_secs(10),
        )
        .await
        .unwrap();

        // Task completed successfully, so it connected correctly.
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_supervision_message_handling() {
        if std::env::var("CARGO_TEST").is_ok() {
            eprintln!("test skipped under cargo as it fails when run with others");
            return;
        }

        let server_handle = System::serve(
            ChannelAddr::any(ChannelTransport::Local),
            Duration::from_secs(3600),
            Duration::from_secs(3600),
        )
        .await
        .unwrap();

        // A test supervisor.
        let mut system = System::new(server_handle.local_addr().clone());
        let supervisor = system.attach().await.unwrap();
        let (_supervisor_supervision_tx, mut supervisor_supervision_receiver) =
            supervisor.bind_actor_port::<ProcSupervisionMessage>();
        let supervisor_actor_ref: ActorRef<ProcSupervisor> =
            ActorRef::attest(supervisor.self_id().clone());

        // Start the proc actor
        let local_world_id = hyperactor::id!(test_proc);
        let local_proc_id = local_world_id.proc_id(0);
        let bootstrap = ProcActor::try_bootstrap(
            local_proc_id.clone(),
            local_world_id.clone(),
            ChannelAddr::any(ChannelTransport::Local),
            server_handle.local_addr().clone(),
            supervisor_actor_ref.clone(),
            Duration::from_secs(1),
            HashMap::new(),
            ProcLifecycleMode::ManagedBySystem,
        )
        .await
        .unwrap();

        // Should receive supervision message sent from the periodic task
        // indicating the proc is alive.
        let msg = supervisor_supervision_receiver.recv().await;
        match msg.unwrap() {
            ProcSupervisionMessage::Update(state, port) => {
                assert_eq!(
                    state,
                    ProcSupervisionState {
                        world_id: local_world_id.clone(),
                        proc_addr: ChannelAddr::Local(3),
                        proc_id: local_proc_id.clone(),
                        proc_health: ProcStatus::Alive,
                        failed_actors: Vec::new(),
                    }
                );
                let _ = port.send(&supervisor, ());
            }
        }

        // Spawn a root actor on the proc.
        let proc_actor_ref = bootstrap.proc_actor.bind();
        let test_actor_ref = spawn::<TestActor>(&supervisor, &proc_actor_ref, "test", &())
            .await
            .unwrap();

        test_actor_ref
            .fail(&supervisor, "test actor is erroring out".to_string())
            .await
            .unwrap();
        // Since we could get messages from both the periodic task and the
        // report from the failed actor, we need to poll for a while to make
        // sure we get the right message.
        let result = RealClock
            .timeout(Duration::from_secs(5), async {
                loop {
                    match supervisor_supervision_receiver.recv().await {
                        Ok(ProcSupervisionMessage::Update(state, _port)) => {
                            match state.failed_actors.iter().find(|(failed_id, _)| {
                                failed_id == test_actor_ref.clone().actor_id()
                            }) {
                                Some((_, actor_status)) => return Ok(actor_status.clone()),
                                None => {}
                            }
                        }
                        _ => anyhow::bail!("unexpected message type"),
                    }
                }
            })
            .await;
        assert_matches!(
            result.unwrap().unwrap(),
            ActorStatus::Failed(msg) if msg.contains("test actor is erroring out")
        );

        server_handle.stop().await.unwrap();
        server_handle.await;
    }

    // Verify that the proc actor's ProcMessage port is bound properly so
    // that we can send messages to it through the system actor.
    #[tokio::test]
    async fn test_bind_proc_actor_in_bootstrap() {
        let server_handle = System::serve(
            ChannelAddr::any(ChannelTransport::Local),
            Duration::from_secs(10),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        let mut system = System::new(server_handle.local_addr().clone());
        let client = system.attach().await.unwrap();

        let world_id = id!(world);
        let proc_id = world_id.proc_id(0);
        let bootstrap = ProcActor::bootstrap(
            proc_id,
            world_id,
            ChannelAddr::any(ChannelTransport::Local),
            server_handle.local_addr().clone(),
            Duration::from_secs(1),
            HashMap::new(),
            ProcLifecycleMode::ManagedBySystem,
        )
        .await
        .unwrap();
        let proc_actor_id = bootstrap.proc_actor.actor_id().clone();
        let proc_actor_ref = ActorRef::<ProcActor>::attest(proc_actor_id);

        let res = RealClock
            .timeout(Duration::from_secs(5), proc_actor_ref.state(&client))
            .await;
        // If ProcMessage's static Named port is not bound, this test will fail
        // due to timeout.
        assert!(res.is_ok());
        assert_matches!(res.unwrap().unwrap(), ProcState::Joined);
        server_handle.stop().await.unwrap();
        server_handle.await;
    }

    #[tokio::test]
    async fn test_proc_snapshot() {
        let Bootstrapped {
            server_handle,
            proc_actor_ref,
            comm_actor_ref,
            client,
            ..
        } = bootstrap().await;

        // Spawn some actors on this proc.
        let root: ActorRef<TestActor> = spawn::<TestActor>(&client, &proc_actor_ref, "root", &())
            .await
            .unwrap();
        let another_root = spawn::<TestActor>(&client, &proc_actor_ref, "another_root", &())
            .await
            .unwrap();
        {
            let snapshot = proc_actor_ref.snapshot(&client).await.unwrap();
            assert_eq!(snapshot.state, ProcState::Joined);
            assert_eq!(
                snapshot.actors.roots.keys().collect::<HashSet<_>>(),
                hashset! {
                    proc_actor_ref.actor_id(),
                    comm_actor_ref.actor_id(),
                    root.actor_id(),
                    another_root.actor_id(),
                }
            );
        }

        server_handle.stop().await.unwrap();
        server_handle.await;
    }

    #[tokio::test]
    async fn test_undeliverable_message_return() {
        // Proc can't send a message to a remote actor because the
        // system connection is lost.
        use hyperactor::mailbox::Undeliverable;
        use hyperactor::test_utils::pingpong::PingPongActor;
        use hyperactor::test_utils::pingpong::PingPongMessage;

        // Use temporary config for this test
        let config = hyperactor::config::global::lock();
        let _guard = config.override_key(
            hyperactor::config::MESSAGE_DELIVERY_TIMEOUT,
            Duration::from_secs(1),
        );

        // Serve a system.
        let server_handle = System::serve(
            ChannelAddr::any(ChannelTransport::Tcp(TcpMode::Hostname)),
            Duration::from_secs(120),
            Duration::from_secs(120),
        )
        .await
        .unwrap();
        let mut system = System::new(server_handle.local_addr().clone());

        // Build a supervisor.
        let supervisor = system.attach().await.unwrap();
        let (_sup_tx, _sup_rx) = supervisor.bind_actor_port::<ProcSupervisionMessage>();
        let sup_ref = ActorRef::<ProcSupervisor>::attest(supervisor.self_id().clone());

        // Construct a system sender.
        let system_sender = BoxedMailboxSender::new(MailboxClient::new(
            channel::dial(server_handle.local_addr().clone()).unwrap(),
        ));

        // Construct a proc forwarder in terms of the system sender.
        let listen_addr = ChannelAddr::any(ChannelTransport::Tcp(TcpMode::Hostname));
        let proc_forwarder =
            BoxedMailboxSender::new(DialMailboxRouter::new_with_default(system_sender));

        // Bootstrap proc 'world[0]', join the system.
        let world_id = id!(world);
        let proc_0 = Proc::new(world_id.proc_id(0), proc_forwarder.clone());
        let _proc_actor_0 = ProcActor::bootstrap_for_proc(
            proc_0.clone(),
            world_id.clone(),
            listen_addr,
            server_handle.local_addr().clone(),
            sup_ref.clone(),
            Duration::from_secs(120),
            HashMap::new(),
            ProcLifecycleMode::ManagedBySystem,
        )
        .await
        .unwrap();
        let proc_0_client = proc_0.attach("client").unwrap();
        let (proc_0_undeliverable_tx, mut proc_0_undeliverable_rx) = proc_0_client.open_port();

        // Bootstrap a second proc 'world[1]', join the system.
        let proc_1 = Proc::new(world_id.proc_id(1), proc_forwarder.clone());
        let _proc_actor_1 = ProcActor::bootstrap_for_proc(
            proc_1.clone(),
            world_id.clone(),
            ChannelAddr::any(ChannelTransport::Tcp(TcpMode::Hostname)),
            server_handle.local_addr().clone(),
            sup_ref.clone(),
            Duration::from_secs(120),
            HashMap::new(),
            ProcLifecycleMode::ManagedBySystem,
        )
        .await
        .unwrap();
        let proc_1_client = proc_1.attach("client").unwrap();
        let (proc_1_undeliverable_tx, mut _proc_1_undeliverable_rx) = proc_1_client.open_port();

        let ping_params = PingPongActorParams::new(Some(proc_0_undeliverable_tx.bind()), None);
        // Spawn two actors 'ping' and 'pong' where 'ping' runs on
        // 'world[0]' and 'pong' on 'world[1]' (that is, not on the
        // same proc).
        let ping_handle = proc_0
            .spawn::<PingPongActor>("ping", ping_params)
            .await
            .unwrap();
        let pong_params = PingPongActorParams::new(Some(proc_1_undeliverable_tx.bind()), None);
        let pong_handle = proc_1
            .spawn::<PingPongActor>("pong", pong_params)
            .await
            .unwrap();

        // Now kill the system server making message delivery between
        // procs impossible.
        server_handle.stop().await.unwrap();
        server_handle.await;

        let n = 100usize;
        for i in 1..(n + 1) {
            // Have 'ping' send 'pong' a message.
            let ttl = 66 + i as u64; // Avoid ttl = 66!
            let (once_handle, _) = proc_0_client.open_once_port::<bool>();
            ping_handle
                .send(PingPongMessage(ttl, pong_handle.bind(), once_handle.bind()))
                .unwrap();
        }

        // `PingPongActor`s do not exit their message loop (a
        // non-default actor behavior) when they have an undelivered
        // message sent back to them (the reason being this very
        // test).
        assert!(matches!(*ping_handle.status().borrow(), ActorStatus::Idle));

        // We expect n undelivered messages.
        let Ok(Undeliverable(envelope)) = proc_0_undeliverable_rx.recv().await else {
            unreachable!()
        };
        let PingPongMessage(_, _, _) = envelope.deserialized().unwrap();
        let mut count = 1;
        while let Ok(Some(Undeliverable(envelope))) = proc_0_undeliverable_rx.try_recv() {
            // We care that every undeliverable message was accounted
            // for. We can't assume anything about their arrival
            // order.
            count += 1;
            let PingPongMessage(_, _, _) = envelope.deserialized().unwrap();
        }
        assert!(count == n);
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_proc_actor_mailbox_admin_message() {
        // Verify that proc actors update their address books on first
        // contact, and that no additional updates are triggered for
        // known procs.

        use hyperactor::test_utils::pingpong::PingPongActor;
        use hyperactor::test_utils::pingpong::PingPongMessage;

        // Serve a system.
        let server_handle = System::serve(
            ChannelAddr::any(ChannelTransport::Tcp(TcpMode::Hostname)),
            Duration::from_secs(120),
            Duration::from_secs(120),
        )
        .await
        .unwrap();
        let mut system = System::new(server_handle.local_addr().clone());
        let system_actor = server_handle.system_actor_handle();
        let system_client = system.attach().await.unwrap(); // world id: user

        // Build a supervisor.
        let supervisor = system.attach().await.unwrap();
        let (_sup_tx, _sup_rx) = supervisor.bind_actor_port::<ProcSupervisionMessage>();
        let sup_ref = ActorRef::<ProcSupervisor>::attest(supervisor.self_id().clone());

        // Construct a system sender.
        let system_sender = BoxedMailboxSender::new(MailboxClient::new(
            channel::dial(server_handle.local_addr().clone()).unwrap(),
        ));

        // Construct a proc forwarder in terms of the system sender.
        let listen_addr = ChannelAddr::any(ChannelTransport::Tcp(TcpMode::Hostname));
        let proc_forwarder =
            BoxedMailboxSender::new(DialMailboxRouter::new_with_default(system_sender));

        // Bootstrap proc 'world[0]', join the system.
        let world_id = id!(world);
        let proc_0 = Proc::new(world_id.proc_id(0), proc_forwarder.clone());
        let _proc_actor_0 = ProcActor::bootstrap_for_proc(
            proc_0.clone(),
            world_id.clone(),
            listen_addr,
            server_handle.local_addr().clone(),
            sup_ref.clone(),
            Duration::from_secs(120),
            HashMap::new(),
            ProcLifecycleMode::ManagedBySystem,
        )
        .await
        .unwrap();
        let proc_0_client = proc_0.attach("client").unwrap();
        let (proc_0_undeliverable_tx, _proc_0_undeliverable_rx) = proc_0_client.open_port();

        // Bootstrap a second proc 'world[1]', join the system.
        let proc_1 = Proc::new(world_id.proc_id(1), proc_forwarder.clone());
        let _proc_actor_1 = ProcActor::bootstrap_for_proc(
            proc_1.clone(),
            world_id.clone(),
            ChannelAddr::any(ChannelTransport::Tcp(TcpMode::Hostname)),
            server_handle.local_addr().clone(),
            sup_ref.clone(),
            Duration::from_secs(120),
            HashMap::new(),
            ProcLifecycleMode::ManagedBySystem,
        )
        .await
        .unwrap();
        let proc_1_client = proc_1.attach("client").unwrap();
        let (proc_1_undeliverable_tx, _proc_1_undeliverable_rx) = proc_1_client.open_port();

        // Spawn two actors 'ping' and 'pong' where 'ping' runs on
        // 'world[0]' and 'pong' on 'world[1]' (that is, not on the
        // same proc).
        let ping_params = PingPongActorParams::new(Some(proc_0_undeliverable_tx.bind()), None);
        let ping_handle = proc_0
            .spawn::<PingPongActor>("ping", ping_params)
            .await
            .unwrap();
        let pong_params = PingPongActorParams::new(Some(proc_1_undeliverable_tx.bind()), None);
        let pong_handle = proc_1
            .spawn::<PingPongActor>("pong", pong_params)
            .await
            .unwrap();

        // Have 'ping' send 'pong' a message.
        let ttl = 10u64; // Avoid ttl = 66!
        let (once_tx, once_rx) = system_client.open_once_port::<bool>();
        ping_handle
            .send(PingPongMessage(ttl, pong_handle.bind(), once_tx.bind()))
            .unwrap();

        assert!(once_rx.recv().await.unwrap());

        // Ping gets Pong's address
        let expected_1 = r#"UpdateAddress {
    proc_id: Ranked(
        WorldId(
            "world",
        ),
        1,
    ),
    addr: Tcp("#;

        // Pong gets Ping's address
        let expected_2 = r#"UpdateAddress {
    proc_id: Ranked(
        WorldId(
            "world",
        ),
        0,
    ),
    addr: Tcp("#;

        // Ping gets "user"'s address
        let expected_3 = r#"UpdateAddress {
    proc_id: Ranked(
        WorldId(
            "user",
        ),"#;

        logs_assert(|logs| {
            let log_body = logs.join("\n");

            let pattern = Regex::new(r"(?m)^UpdateAddress \{\n(?:.*\n)*?^\}").unwrap();
            let count = pattern.find_iter(&log_body).count();

            if count != 3 {
                return Err(format!(
                    "expected 3 UpdateAddress messages, found {}",
                    count
                ));
            }

            if !log_body.contains(expected_1) {
                return Err("missing expected update for proc_id 1".into());
            }
            if !log_body.contains(expected_2) {
                return Err("missing expected update for proc_id 0".into());
            }
            if !log_body.contains(expected_3) {
                return Err("missing expected update for proc_id user".into());
            }

            Ok(())
        });

        let (once_tx, once_rx) = system_client.open_once_port::<()>();
        assert_matches!(
            system_actor
                .stop(
                    /*unused*/ &system_client,
                    None,
                    Duration::from_secs(1),
                    once_tx.bind()
                )
                .await,
            Ok(())
        );
        assert_matches!(once_rx.recv().await.unwrap(), ());
    }

    #[tokio::test]
    async fn test_update_address_book_cache() {
        let server_handle = System::serve(
            ChannelAddr::any(ChannelTransport::Tcp(TcpMode::Hostname)),
            Duration::from_secs(2), // supervision update timeout
            Duration::from_secs(2), // duration to evict an unhealthy world
        )
        .await
        .unwrap();
        let system_addr = server_handle.local_addr().clone();
        let mut system = System::new(system_addr.clone());

        let system_client = system.attach().await.unwrap();

        // Spawn ping and pong actors to play a ping pong game.
        let ping_actor_id = id!(world[0].ping[0]);
        let (ping_actor_ref, _ping_proc_ref) =
            spawn_actor(&system_client, &ping_actor_id, &system_addr).await;

        let pong_actor_id = id!(world[1].pong[0]);
        let (pong_actor_ref, pong_proc_ref) =
            spawn_actor(&system_client, &pong_actor_id, &system_addr).await;

        // After playing the first round game, ping and pong actors has each other's
        // ChannelAddr cached in their procs' mailboxes, respectively.
        let (done_tx, done_rx) = system_client.open_once_port();
        let ping_pong_message = PingPongMessage(4, pong_actor_ref.clone(), done_tx.bind());
        ping_actor_ref
            .send(&system_client, ping_pong_message)
            .unwrap();
        assert!(done_rx.recv().await.unwrap());

        // Now we kill and respawn the pong actor so it's ChannelAddr is changed.
        let ProcStopResult { actors_aborted, .. } = pong_proc_ref
            .stop(&system_client, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(1, actors_aborted);
        let (pong_actor_ref, _pong_proc_ref) =
            spawn_actor(&system_client, &pong_actor_id, &system_addr).await;

        // Now we expect to play the game between ping and new pong. The new pong has the same
        // proc ID as the old pong but different ChannelAddr. The game should still be playable
        // with system actor updating the cached address of Pong inside Ping's mailbox.
        let (done_tx, done_rx) = system_client.open_once_port();
        let ping_pong_message = PingPongMessage(4, pong_actor_ref.clone(), done_tx.bind());
        ping_actor_ref
            .send(&system_client, ping_pong_message)
            .unwrap();
        assert!(done_rx.recv().await.unwrap());
    }

    async fn spawn_actor(
        cx: &impl context::Actor,
        actor_id: &ActorId,
        system_addr: &ChannelAddr,
    ) -> (ActorRef<PingPongActor>, ActorRef<ProcActor>) {
        let listen_addr = ChannelAddr::any(ChannelTransport::Tcp(TcpMode::Hostname));
        let bootstrap = ProcActor::bootstrap(
            actor_id.proc_id().clone(),
            actor_id
                .proc_id()
                .world_id()
                .expect("proc must be ranked for bootstrap world_id")
                .clone(),
            listen_addr.clone(),
            system_addr.clone(),
            Duration::from_secs(3),
            HashMap::new(),
            ProcLifecycleMode::ManagedBySystem,
        )
        .await
        .unwrap();
        let (undeliverable_msg_tx, _) = cx.mailbox().open_port();
        let params = PingPongActorParams::new(Some(undeliverable_msg_tx.bind()), None);
        let actor_ref = spawn::<PingPongActor>(
            cx,
            &bootstrap.proc_actor.bind(),
            &actor_id.to_string(),
            &params,
        )
        .await
        .unwrap();
        let proc_actor_ref = bootstrap.proc_actor.bind();
        (actor_ref, proc_actor_ref)
    }
}
