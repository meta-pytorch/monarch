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
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorId;
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
        supervisor: PortRef<ActorSupervisionEvent>,
        /// An address book to use for direct dialing.
        address_book: HashMap<ProcId, ChannelAddr>,
        /// The agent should write its rank to this port when it successfully
        /// configured.
        configured: PortRef<usize>,
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

/// A mesh agent is responsible for managing procs in a [`ProcMesh`].
#[derive(Debug)]
#[hyperactor::export(handlers=[MeshAgentMessage])]
pub struct ProcMeshAgent {
    proc: Proc,
    remote: Remote,
    sender: ReconfigurableMailboxSender,
    rank: Option<usize>,
    supervisor: Option<PortRef<ActorSupervisionEvent>>,
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
            sender,
            rank: None,       // not yet assigned
            supervisor: None, // not yet assigned
        };
        let handle = proc.spawn::<Self>("mesh", agent).await?;
        Ok((proc, handle))
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
        supervisor: PortRef<ActorSupervisionEvent>,
        address_book: HashMap<ProcId, ChannelAddr>,
        configured: PortRef<usize>,
    ) -> Result<(), anyhow::Error> {
        // Set the supervisor first so that we can handle supervison events that might
        // occur from configuration failures. Though we should instead report these directly
        // for better ergonomics in the allocator.
        self.supervisor = Some(supervisor);

        // Wire up the local proc to the global (process) router. This ensures that child
        // meshes are reachable from any actor created by this mesh.
        let client = MailboxClient::new(channel::dial(forwarder)?);

        // `HYPERACTOR_MESH_ROUTER_CONFIG_NO_GLOBAL_FALLBACK` may be
        // set as a means of failure injection in the testing of
        // supervision codepaths.
        let router = if std::env::var("HYPERACTOR_MESH_ROUTER_NO_GLOBAL_FALLBACK").is_err() {
            let default = super::router::global().fallback(client.into_boxed());
            DialMailboxRouter::new_with_default(default.into_boxed())
        } else {
            DialMailboxRouter::new_with_default(client.into_boxed())
        };

        for (proc_id, addr) in address_book {
            router.bind(proc_id.into(), addr);
        }

        if self.sender.configure(router.into_boxed()) {
            self.rank = Some(rank);
            configured.send(cx, rank)?;
        } else {
            tracing::error!("tried to reconfigure mesh agent");
        }
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
        let rank = match self.rank {
            Some(rank) => rank,
            None => {
                let err = "tried to spawn on unconfigured proc";
                status_port.send(cx, GspawnResult::Error(err.to_string()))?;
                return Err(anyhow::anyhow!(err));
            }
        };
        status_port.send(cx, GspawnResult::Success { rank, actor_id })?;
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
}

#[async_trait]
impl Handler<ActorSupervisionEvent> for ProcMeshAgent {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        event: ActorSupervisionEvent,
    ) -> anyhow::Result<()> {
        if let Some(supervisor) = &self.supervisor {
            supervisor.send(cx, event)?;
        } else {
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
        fn post(
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
