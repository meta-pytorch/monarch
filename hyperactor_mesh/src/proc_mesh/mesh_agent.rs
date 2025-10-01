/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The mesh agent actor manages procs in ProcMeshes.

use std::collections::HashMap;
use std::mem::replace;
use std::mem::take;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::RwLockWriteGuard;

use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorId;
use hyperactor::Bind;
use hyperactor::Context;
use hyperactor::Data;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Named;
use hyperactor::OncePortRef;
use hyperactor::PortHandle;
use hyperactor::PortRef;
use hyperactor::ProcId;
use hyperactor::RefClient;
use hyperactor::Unbind;
use hyperactor::actor::ActorStatus;
use hyperactor::actor::remote::Remote;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::mailbox::BoxedMailboxSender;
use hyperactor::mailbox::DialMailboxRouter;
use hyperactor::mailbox::IntoBoxedMailboxSender;
use hyperactor::mailbox::MailboxClient;
use hyperactor::mailbox::MailboxSender;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::Undeliverable;
use hyperactor::proc::Proc;
use hyperactor::supervision::ActorSupervisionEvent;
use serde::Deserialize;
use serde::Serialize;

use crate::proc_mesh::SupervisionEventState;
use crate::resource;
use crate::resource::RankStatus;
use crate::v1::Name;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Named)]
pub enum GspawnResult {
    Success { rank: usize, actor_id: ActorId },
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub enum StopActorResult {
    Success,
    Timeout,
    NotFound,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    Handler,
    HandleClient,
    RefClient,
    Named
)]
pub(crate) enum MeshAgentMessage {
    /// Configure the proc in the mesh.
    Configure {
        /// The rank of this proc in the mesh.
        rank: usize,
        /// The forwarder to send messages to unknown destinations.
        forwarder: ChannelAddr,
        /// The supervisor port to which the agent should report supervision events.
        supervisor: Option<PortRef<ActorSupervisionEvent>>,
        /// An address book to use for direct dialing.
        address_book: HashMap<ProcId, ChannelAddr>,
        /// The agent should write its rank to this port when it successfully
        /// configured.
        configured: PortRef<usize>,
        /// If true, and supervisor is None, record supervision events to be reported
        record_supervision_events: bool,
    },

    Status {
        /// The status of the proc.
        /// To be replaced with fine-grained lifecycle status,
        /// and to use aggregation.
        status: PortRef<(usize, bool)>,
    },

    /// Spawn an actor on the proc to the provided name.
    Gspawn {
        /// registered actor type
        actor_type: String,
        /// spawned actor name
        actor_name: String,
        /// serialized parameters
        params_data: Data,
        /// reply port; the proc should send its rank to indicated a spawned actor
        status_port: PortRef<GspawnResult>,
    },

    /// Stop actors of a specific mesh name
    StopActor {
        /// The actor to stop
        actor_id: ActorId,
        /// The timeout for waiting for the actor to stop
        timeout_ms: u64,
        /// The result when trying to stop the actor
        #[reply]
        stopped: OncePortRef<StopActorResult>,
    },
}

/// Internal configuration state of the mesh agent.
#[derive(Debug, EnumAsInner, Default)]
enum State {
    UnconfiguredV0 {
        sender: ReconfigurableMailboxSender,
    },

    ConfiguredV0 {
        sender: ReconfigurableMailboxSender,
        rank: usize,
        supervisor: Option<PortRef<ActorSupervisionEvent>>,
    },

    V1,

    #[default]
    Invalid,
}

impl State {
    fn rank(&self) -> Option<usize> {
        match self {
            State::ConfiguredV0 { rank, .. } => Some(*rank),
            _ => None,
        }
    }

    fn supervisor(&self) -> Option<PortRef<ActorSupervisionEvent>> {
        match self {
            State::ConfiguredV0 { supervisor, .. } => supervisor.clone(),
            _ => None,
        }
    }
}

/// Actor state used for v1 API.
#[derive(Debug)]
struct ActorInstanceState {
    create_rank: usize,
    spawn: Result<ActorId, anyhow::Error>,
}

/// A mesh agent is responsible for managing procs in a [`ProcMesh`].
#[derive(Debug)]
#[hyperactor::export(
    handlers=[
        MeshAgentMessage,
        resource::CreateOrUpdate<ActorSpec> { cast = true },
        resource::GetState<ActorState> { cast = true },
        resource::GetRankStatus { cast = true },
    ]
)]
pub struct ProcMeshAgent {
    proc: Proc,
    remote: Remote,
    state: State,
    /// Actors created and tracked through the resource behavior.
    actor_states: HashMap<Name, ActorInstanceState>,
    /// If true, and supervisor is None, record supervision events to be reported
    /// to owning actors later.
    record_supervision_events: bool,
    /// If record_supervision_events is true, then this will contain the list
    /// of all events that were received.
    supervision_events: HashMap<ActorId, Vec<ActorSupervisionEvent>>,
}

impl ProcMeshAgent {
    #[hyperactor::observe_result("MeshAgent")]
    pub(crate) async fn bootstrap(
        proc_id: ProcId,
    ) -> Result<(Proc, ActorHandle<Self>), anyhow::Error> {
        let sender = ReconfigurableMailboxSender::new();
        let proc = Proc::new(proc_id.clone(), BoxedMailboxSender::new(sender.clone()));

        // Wire up this proc to the global router so that any meshes managed by
        // this process can reach actors in this proc.
        super::router::global().bind(proc_id.into(), proc.clone());

        let agent = ProcMeshAgent {
            proc: proc.clone(),
            remote: Remote::collect(),
            state: State::UnconfiguredV0 { sender },
            actor_states: HashMap::new(),
            record_supervision_events: false,
            supervision_events: HashMap::new(),
        };
        let handle = proc.spawn::<Self>("mesh", agent).await?;
        Ok((proc, handle))
    }

    pub(crate) async fn boot_v1(proc: Proc) -> Result<ActorHandle<Self>, anyhow::Error> {
        let agent = ProcMeshAgent {
            proc: proc.clone(),
            remote: Remote::collect(),
            state: State::V1,
            actor_states: HashMap::new(),
            record_supervision_events: true,
            supervision_events: HashMap::new(),
        };
        proc.spawn::<Self>("agent", agent).await
    }
}

#[async_trait]
impl Actor for ProcMeshAgent {
    type Params = Self;

    async fn new(params: Self::Params) -> Result<Self, anyhow::Error> {
        Ok(params)
    }

    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        self.proc.set_supervision_coordinator(this.port())?;
        Ok(())
    }
}

#[async_trait]
#[hyperactor::forward(MeshAgentMessage)]
impl MeshAgentMessageHandler for ProcMeshAgent {
    async fn configure(
        &mut self,
        cx: &Context<Self>,
        rank: usize,
        forwarder: ChannelAddr,
        supervisor: Option<PortRef<ActorSupervisionEvent>>,
        address_book: HashMap<ProcId, ChannelAddr>,
        configured: PortRef<usize>,
        record_supervision_events: bool,
    ) -> Result<(), anyhow::Error> {
        anyhow::ensure!(
            self.state.is_unconfigured_v0(),
            "mesh agent cannot be (re-)configured"
        );
        self.record_supervision_events = record_supervision_events;

        // Wire up the local proc to the global (process) router. This ensures that child
        // meshes are reachable from any actor created by this mesh.
        let client = MailboxClient::new(channel::dial(forwarder)?);

        // `HYPERACTOR_MESH_ROUTER_CONFIG_NO_GLOBAL_FALLBACK` may be
        // set as a means of failure injection in the testing of
        // supervision codepaths.
        let router = if std::env::var("HYPERACTOR_MESH_ROUTER_NO_GLOBAL_FALLBACK").is_err() {
            let default = super::router::global().fallback(client.into_boxed());
            DialMailboxRouter::new_with_default_direct_addressed_remote_only(default.into_boxed())
        } else {
            DialMailboxRouter::new_with_default_direct_addressed_remote_only(client.into_boxed())
        };

        for (proc_id, addr) in address_book {
            router.bind(proc_id.into(), addr);
        }

        let sender = take(&mut self.state).into_unconfigured_v0().unwrap();
        assert!(sender.configure(router.into_boxed()));

        // This is a bit suboptimal: ideally we'd set the supervisor first, to correctly report
        // any errors that occur during configuration. However, these should anyway be correctly
        // caught on process exit.
        self.state = State::ConfiguredV0 {
            sender,
            rank,
            supervisor,
        };
        configured.send(cx, rank)?;

        Ok(())
    }

    async fn gspawn(
        &mut self,
        cx: &Context<Self>,
        actor_type: String,
        actor_name: String,
        params_data: Data,
        status_port: PortRef<GspawnResult>,
    ) -> Result<(), anyhow::Error> {
        anyhow::ensure!(
            self.state.is_configured_v0(),
            "mesh agent is not v0 configured"
        );
        let actor_id = match self
            .remote
            .gspawn(&self.proc, &actor_type, &actor_name, params_data)
            .await
        {
            Ok(id) => id,
            Err(err) => {
                status_port.send(cx, GspawnResult::Error(format!("gspawn failed: {}", err)))?;
                return Err(anyhow::anyhow!("gspawn failed"));
            }
        };
        status_port.send(
            cx,
            GspawnResult::Success {
                rank: self.state.rank().unwrap(),
                actor_id,
            },
        )?;
        Ok(())
    }

    async fn stop_actor(
        &mut self,
        _cx: &Context<Self>,
        actor_id: ActorId,
        timeout_ms: u64,
    ) -> Result<StopActorResult, anyhow::Error> {
        tracing::info!(
            name = "StopActor",
            actor_id = %actor_id,
            actor_name = actor_id.name(),
        );

        if let Some(mut status) = self.proc.stop_actor(&actor_id) {
            match RealClock
                .timeout(
                    tokio::time::Duration::from_millis(timeout_ms),
                    status.wait_for(|state: &ActorStatus| matches!(*state, ActorStatus::Stopped)),
                )
                .await
            {
                Ok(_) => Ok(StopActorResult::Success),
                Err(_) => Ok(StopActorResult::Timeout),
            }
        } else {
            Ok(StopActorResult::NotFound)
        }
    }

    async fn status(
        &mut self,
        cx: &Context<Self>,
        status_port: PortRef<(usize, bool)>,
    ) -> Result<(), anyhow::Error> {
        match &self.state {
            State::ConfiguredV0 { rank, .. } => {
                // v0 path: configured with a concrete rank
                status_port.send(cx, (*rank, true))?;
                Ok(())
            }
            State::UnconfiguredV0 { .. } => {
                // v0 path but not configured yet
                Err(anyhow::anyhow!(
                    "status unavailable: v0 agent not configured (waiting for Configure)"
                ))
            }
            State::V1 => {
                // v1/owned path does not support status (no rank semantics)
                Err(anyhow::anyhow!(
                    "status unsupported in v1/owned path (no rank)"
                ))
            }
            State::Invalid => Err(anyhow::anyhow!(
                "status unavailable: agent in invalid state"
            )),
        }
    }
}

#[async_trait]
impl Handler<ActorSupervisionEvent> for ProcMeshAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        event: ActorSupervisionEvent,
    ) -> anyhow::Result<()> {
        if self.record_supervision_events {
            tracing::info!("Received supervision event: {:?}, recording", event);
            self.supervision_events
                .entry(event.actor_id.clone())
                .or_insert_with(Vec::new)
                .push(event.clone());
        }
        if let Some(supervisor) = self.state.supervisor() {
            supervisor.send(cx, event)?;
        } else if !self.record_supervision_events {
            // If there is no supervisor, and nothing is recording these, crash
            // the whole process.
            tracing::error!(
                name = SupervisionEventState::SupervisionEventTransmitFailed.as_ref(),
                "proc {}: could not propagate supervision event {:?}: crashing",
                cx.self_id().proc_id(),
                event
            );

            // We should have a custom "crash" function here, so that this works
            // in testing of the LocalAllocator, etc.
            std::process::exit(1);
        }
        Ok(())
    }
}

// Implement the resource behavior for managing actors:

/// Actor spec.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named)]
pub struct ActorSpec {
    /// registered actor type
    pub actor_type: String,
    /// serialized parameters
    pub params_data: Data,
}

/// Actor state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Named, Bind, Unbind)]
pub struct ActorState {
    /// The actor's ID.
    pub actor_id: ActorId,
    /// The rank of the proc that created the actor. This is before any slicing.
    pub create_rank: usize,
    // TODO status: ActorStatus,
    pub supervision_events: Vec<ActorSupervisionEvent>,
}

#[async_trait]
impl Handler<resource::CreateOrUpdate<ActorSpec>> for ProcMeshAgent {
    async fn handle(
        &mut self,
        _cx: &Context<Self>,
        create_or_update: resource::CreateOrUpdate<ActorSpec>,
    ) -> anyhow::Result<()> {
        if self.actor_states.contains_key(&create_or_update.name) {
            // There is no update.
            return Ok(());
        }

        let ActorSpec {
            actor_type,
            params_data,
        } = create_or_update.spec;
        self.actor_states.insert(
            create_or_update.name.clone(),
            ActorInstanceState {
                create_rank: create_or_update.rank.unwrap(),
                spawn: self
                    .remote
                    .gspawn(
                        &self.proc,
                        &actor_type,
                        &create_or_update.name.to_string(),
                        params_data,
                    )
                    .await,
            },
        );

        Ok(())
    }
}

#[async_trait]
impl Handler<resource::GetRankStatus> for ProcMeshAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        get_rank_status: resource::GetRankStatus,
    ) -> anyhow::Result<()> {
        let (rank, status) = match self.actor_states.get(&get_rank_status.name) {
            Some(ActorInstanceState {
                spawn: Ok(actor_id),
                create_rank,
            }) => {
                let supervision_events = self
                    .supervision_events
                    .get(actor_id)
                    .map_or_else(Vec::new, |a| a.clone());
                (
                    *create_rank,
                    if supervision_events.is_empty() {
                        resource::Status::Running
                    } else {
                        resource::Status::Failed(format!(
                            "because of supervision events: {:?}",
                            supervision_events
                        ))
                    },
                )
            }
            Some(ActorInstanceState {
                spawn: Err(e),
                create_rank,
            }) => (*create_rank, resource::Status::Failed(e.to_string())),
            // TODO: represent unknown rank
            None => (usize::MAX, resource::Status::NotExist),
        };

        get_rank_status.reply.send(cx, (rank, status).into())?;
        Ok(())
    }
}

#[async_trait]
impl Handler<resource::GetState<ActorState>> for ProcMeshAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        get_state: resource::GetState<ActorState>,
    ) -> anyhow::Result<()> {
        let state = match self.actor_states.get(&get_state.name) {
            Some(ActorInstanceState {
                create_rank,
                spawn: Ok(actor_id),
            }) => {
                let supervision_events = self
                    .supervision_events
                    .get(actor_id)
                    .map_or_else(Vec::new, |a| a.clone());
                let status = if supervision_events.is_empty() {
                    resource::Status::Running
                } else {
                    resource::Status::Failed(format!(
                        "because of supervision events: {:?}",
                        supervision_events
                    ))
                };
                resource::State {
                    name: get_state.name.clone(),
                    status,
                    state: Some(ActorState {
                        actor_id: actor_id.clone(),
                        create_rank: *create_rank,
                        supervision_events,
                    }),
                }
            }
            Some(ActorInstanceState { spawn: Err(e), .. }) => resource::State {
                name: get_state.name.clone(),
                status: resource::Status::Failed(e.to_string()),
                state: None,
            },
            None => resource::State {
                name: get_state.name.clone(),
                status: resource::Status::NotExist,
                state: None,
            },
        };

        get_state.reply.send(cx, state)?;
        Ok(())
    }
}

/// A mailbox sender that initially queues messages, and then relays them to
/// an underlying sender once configured.
#[derive(Clone)]
pub(crate) struct ReconfigurableMailboxSender {
    state: Arc<RwLock<ReconfigurableMailboxSenderState>>,
}

impl std::fmt::Debug for ReconfigurableMailboxSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Not super helpful, but we definitely don't wan to acquire any locks
        // in a Debug formatter.
        f.debug_struct("ReconfigurableMailboxSender").finish()
    }
}

pub(crate) struct ReconfigurableMailboxSenderInner<'a> {
    guard: RwLockWriteGuard<'a, ReconfigurableMailboxSenderState>,
}

impl<'a> ReconfigurableMailboxSenderInner<'a> {
    pub(crate) fn as_configured(&self) -> Option<&BoxedMailboxSender> {
        self.guard.as_configured()
    }
}

type Post = (MessageEnvelope, PortHandle<Undeliverable<MessageEnvelope>>);

#[derive(EnumAsInner, Debug)]
enum ReconfigurableMailboxSenderState {
    Queueing(Mutex<Vec<Post>>),
    Configured(BoxedMailboxSender),
}

impl ReconfigurableMailboxSender {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(ReconfigurableMailboxSenderState::Queueing(
                Mutex::new(Vec::new()),
            ))),
        }
    }

    /// Configure this mailbox with the provided sender. This will first
    /// enqueue any pending messages onto the sender; future messages are
    /// posted directly to the configured sender.
    pub(crate) fn configure(&self, sender: BoxedMailboxSender) -> bool {
        let mut state = self.state.write().unwrap();
        if state.is_configured() {
            return false;
        }

        let queued = replace(
            &mut *state,
            ReconfigurableMailboxSenderState::Configured(sender.clone()),
        );

        for (envelope, return_handle) in queued.into_queueing().unwrap().into_inner().unwrap() {
            sender.post(envelope, return_handle);
        }
        *state = ReconfigurableMailboxSenderState::Configured(sender);
        true
    }

    pub(crate) fn as_inner<'a>(
        &'a self,
    ) -> Result<ReconfigurableMailboxSenderInner<'a>, anyhow::Error> {
        let state = self.state.write().unwrap();
        if state.is_configured() {
            Ok(ReconfigurableMailboxSenderInner { guard: state })
        } else {
            Err(anyhow::anyhow!("cannot get inner sender: not configured"))
        }
    }
}

impl MailboxSender for ReconfigurableMailboxSender {
    fn post(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        match *self.state.read().unwrap() {
            ReconfigurableMailboxSenderState::Queueing(ref queue) => {
                queue.lock().unwrap().push((envelope, return_handle));
            }
            ReconfigurableMailboxSenderState::Configured(ref sender) => {
                sender.post(envelope, return_handle);
            }
        }
    }

    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        match *self.state.read().unwrap() {
            ReconfigurableMailboxSenderState::Queueing(ref queue) => {
                queue.lock().unwrap().push((envelope, return_handle));
            }
            ReconfigurableMailboxSenderState::Configured(ref sender) => {
                sender.post_unchecked(envelope, return_handle);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use hyperactor::attrs::Attrs;
    use hyperactor::id;
    use hyperactor::mailbox::BoxedMailboxSender;
    use hyperactor::mailbox::Mailbox;
    use hyperactor::mailbox::MailboxSender;
    use hyperactor::mailbox::MessageEnvelope;
    use hyperactor::mailbox::PortHandle;
    use hyperactor::mailbox::Undeliverable;

    use super::*;

    #[derive(Debug, Clone)]
    struct QueueingMailboxSender {
        messages: Arc<Mutex<Vec<MessageEnvelope>>>,
    }

    impl QueueingMailboxSender {
        fn new() -> Self {
            Self {
                messages: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn get_messages(&self) -> Vec<MessageEnvelope> {
            self.messages.lock().unwrap().clone()
        }
    }

    impl MailboxSender for QueueingMailboxSender {
        fn post_unchecked(
            &self,
            envelope: MessageEnvelope,
            _return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
        ) {
            self.messages.lock().unwrap().push(envelope);
        }
    }

    // Helper function to create a test message envelope
    fn envelope(data: u64) -> MessageEnvelope {
        MessageEnvelope::serialize(
            id!(world[0].sender),
            id!(world[0].receiver[0][1]),
            &data,
            Attrs::new(),
        )
        .unwrap()
    }

    fn return_handle() -> PortHandle<Undeliverable<MessageEnvelope>> {
        let mbox = Mailbox::new_detached(id!(test[0].test));
        let (port, _receiver) = mbox.open_port::<Undeliverable<MessageEnvelope>>();
        port
    }

    #[test]
    fn test_queueing_before_configure() {
        let sender = ReconfigurableMailboxSender::new();

        let test_sender = QueueingMailboxSender::new();
        let boxed_sender = BoxedMailboxSender::new(test_sender.clone());

        let return_handle = return_handle();
        sender.post(envelope(1), return_handle.clone());
        sender.post(envelope(2), return_handle.clone());

        assert_eq!(test_sender.get_messages().len(), 0);

        sender.configure(boxed_sender);

        let messages = test_sender.get_messages();
        assert_eq!(messages.len(), 2);

        assert_eq!(messages[0].deserialized::<u64>().unwrap(), 1);
        assert_eq!(messages[1].deserialized::<u64>().unwrap(), 2);
    }

    #[test]
    fn test_direct_delivery_after_configure() {
        // Create a ReconfigurableMailboxSender
        let sender = ReconfigurableMailboxSender::new();

        let test_sender = QueueingMailboxSender::new();
        let boxed_sender = BoxedMailboxSender::new(test_sender.clone());
        sender.configure(boxed_sender);

        let return_handle = return_handle();
        sender.post(envelope(3), return_handle.clone());
        sender.post(envelope(4), return_handle.clone());

        let messages = test_sender.get_messages();
        assert_eq!(messages.len(), 2);

        assert_eq!(messages[0].deserialized::<u64>().unwrap(), 3);
        assert_eq!(messages[1].deserialized::<u64>().unwrap(), 4);
    }

    #[test]
    fn test_multiple_configurations() {
        let sender = ReconfigurableMailboxSender::new();
        let boxed_sender = BoxedMailboxSender::new(QueueingMailboxSender::new());

        assert!(sender.configure(boxed_sender.clone()));
        assert!(!sender.configure(boxed_sender));
    }

    #[test]
    fn test_mixed_queueing_and_direct_delivery() {
        let sender = ReconfigurableMailboxSender::new();

        let test_sender = QueueingMailboxSender::new();
        let boxed_sender = BoxedMailboxSender::new(test_sender.clone());

        let return_handle = return_handle();
        sender.post(envelope(5), return_handle.clone());
        sender.post(envelope(6), return_handle.clone());

        sender.configure(boxed_sender);

        sender.post(envelope(7), return_handle.clone());
        sender.post(envelope(8), return_handle.clone());

        let messages = test_sender.get_messages();
        assert_eq!(messages.len(), 4);

        assert_eq!(messages[0].deserialized::<u64>().unwrap(), 5);
        assert_eq!(messages[1].deserialized::<u64>().unwrap(), 6);
        assert_eq!(messages[2].deserialized::<u64>().unwrap(), 7);
        assert_eq!(messages[3].deserialized::<u64>().unwrap(), 8);
    }
}
