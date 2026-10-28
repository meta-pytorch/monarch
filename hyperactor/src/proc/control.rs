/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Runtime control for the proc owning a directly contacted endpoint.
//!
//! Ownership resolution is channel-local, not a routable singleton proc alias.
//! Only listener ingress may consume it; ordinary Gateway routing rejects it.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use futures::FutureExt as _;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::Proc;
use super::WeakProc;
use crate::ActorAddr;
use crate::ControlPort;
use crate::Gateway;
use crate::Label;
use crate::OncePortRef;
use crate::PortAddr;
use crate::ProcAddr;
use crate::ProcId;
use crate::channel::ChannelAddr;
use crate::channel::ChannelTransport;
use crate::mailbox::MailboxSender;
use crate::mailbox::MailboxServer;
use crate::mailbox::MailboxServerHandle;
use crate::mailbox::MessageEnvelope;
use crate::mailbox::PortHandle;
use crate::mailbox::PortSender;
use crate::mailbox::Undeliverable;
use crate::mailbox::monitored_return_handle;
use crate::port::Port;

/// Unsequenced runtime request for the directly contacted endpoint's owner.
/// Submit through [`crate::gateway::GatewayServeHandle::resolve_proc`].
#[derive(Debug, Serialize, Deserialize, typeuri::Named)]
pub(crate) struct ResolveProc {
    /// Resolve this listener or ask it to contact one explicit target endpoint.
    pub target: ResolveProcTarget,
    /// Each request owns its normal, independently routed reply port.
    pub reply: OncePortRef<ResolveProcResult>,
}
wirevalue::register_type!(ResolveProc);

/// Via still names known peer UIDs; the relay leg needs an explicit endpoint
/// address because the target need not already be an attached peer.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ResolveProcTarget {
    OwnEndpoint,
    Endpoint(ChannelAddr),
}

/// Snapshot of a listener's live concrete owner.
#[derive(Debug, Serialize, Deserialize, typeuri::Named)]
pub(crate) enum ResolveProcResult {
    /// Concrete incarnation of the registered live proc.
    Resolved(ProcId),
    /// No live proc owns the listener.
    Unavailable,
    /// Relay admission or target resolution failed.
    Failed(String),
}
wirevalue::register_type!(ResolveProcResult);

impl ResolveProcResult {
    pub(crate) fn into_result(self) -> anyhow::Result<ProcId> {
        match self {
            Self::Resolved(id) => {
                anyhow::ensure!(
                    id.uid().is_instance(),
                    "resolved proc must have an instance id"
                );
                Ok(id)
            }
            Self::Unavailable => anyhow::bail!("endpoint owner unavailable"),
            Self::Failed(reason) => anyhow::bail!("endpoint discovery failed: {reason}"),
        }
    }
}

// A fixed protocol destination, never a ProcId inserted in Gateway.procs.
// There is intentionally no remote location here: choosing the channel chooses
// the endpoint. This prevents wildcard bind/DNS spelling from affecting scope.
fn destination() -> PortAddr {
    ActorAddr::root(
        ProcAddr::new(
            ProcId::singleton(Label::strip("endpoint_owner")),
            ChannelAddr::any(ChannelTransport::Local).into(),
        ),
        Label::strip("runtime"),
    )
    .port_addr(Port::control(ControlPort::ResolveProc))
}

pub(crate) fn resolve_envelope(
    reply: OncePortRef<ResolveProcResult>,
) -> anyhow::Result<MessageEnvelope> {
    resolve_target_envelope(ResolveProcTarget::OwnEndpoint, reply)
}

pub(crate) fn resolve_target_envelope(
    target: ResolveProcTarget,
    reply: OncePortRef<ResolveProcResult>,
) -> anyhow::Result<MessageEnvelope> {
    let mut envelope = MessageEnvelope::new_unknown(
        destination(),
        wirevalue::Any::serialize(&ResolveProc { target, reply })?,
    );
    envelope.set_header(crate::ordering::SEQ_INFO, crate::ordering::SeqInfo::Direct);
    envelope.set_return_undeliverable(false);
    Ok(envelope)
}

pub(crate) fn is_resolve(envelope: &MessageEnvelope) -> bool {
    envelope
        .dest()
        .is_control_port_kind(ControlPort::ResolveProc)
        || envelope
            .next_hop()
            .is_control_port_kind(ControlPort::ResolveProc)
}

/// Bounded independently for every listener. Admission never waits on a permit.
const MAX_ENDPOINT_RELAYS: usize = 128;

struct OwnerState {
    proc: Option<WeakProc>,
    stopped: bool,
}

struct EndpointState {
    owner: RwLock<OwnerState>,
    permits: Arc<Semaphore>,
    cancelled: CancellationToken,
    tasks: TaskTracker,
}

/// One registration and relay lifetime per listener. Weak proc ownership avoids
/// retaining the proc or its Gateway/server.
#[derive(Clone)]
pub(crate) struct EndpointOwner(Arc<EndpointState>);

impl Default for EndpointOwner {
    fn default() -> Self {
        Self::with_relay_limit(MAX_ENDPOINT_RELAYS)
    }
}

impl EndpointOwner {
    pub(crate) fn with_relay_limit(limit: usize) -> Self {
        Self(Arc::new(EndpointState {
            owner: RwLock::new(OwnerState {
                proc: None,
                stopped: false,
            }),
            permits: Arc::new(Semaphore::new(limit)),
            cancelled: CancellationToken::new(),
            tasks: TaskTracker::new(),
        }))
    }

    pub(crate) fn register(&self, proc: &Proc) -> anyhow::Result<()> {
        anyhow::ensure!(
            proc.proc_id().uid().is_instance(),
            "endpoint owner must have an instance id"
        );
        let mut owner = self.0.owner.write().unwrap();
        anyhow::ensure!(!owner.stopped, "endpoint is stopped");
        if let Some(current) = owner.proc.as_ref().and_then(WeakProc::upgrade) {
            anyhow::ensure!(
                current.downgrade().ptr_eq(&proc.downgrade()),
                "endpoint already has a live owner"
            );
        }
        owner.proc = Some(proc.downgrade());
        Ok(())
    }

    pub(crate) fn clear(&self) {
        // Serialize stopping with relay admission/spawn. TaskTracker::close
        // alone would allow a racing spawn after wait() observed zero tasks.
        let mut owner = self.0.owner.write().unwrap();
        owner.proc = None;
        owner.stopped = true;
        self.0.cancelled.cancel();
        self.0.tasks.close();
    }

    pub(crate) async fn shutdown(&self) {
        self.clear();
        self.0.tasks.wait().await;
    }

    fn resolve(&self) -> ResolveProcResult {
        match self
            .0
            .owner
            .read()
            .unwrap()
            .proc
            .as_ref()
            .and_then(WeakProc::upgrade)
        {
            Some(proc) => ResolveProcResult::Resolved(proc.proc_id().clone()),
            None => ResolveProcResult::Unavailable,
        }
    }

    fn relay(
        &self,
        gateway: Gateway,
        connection: CancellationToken,
        endpoint: ChannelAddr,
        reply: OncePortRef<ResolveProcResult>,
    ) {
        // Keep this guard only through synchronous admission + tracker spawn.
        let owner = self.0.owner.read().unwrap();
        if owner.stopped {
            send_reply(
                &gateway,
                reply,
                ResolveProcResult::Failed("endpoint is stopped".to_owned()),
            );
            return;
        }
        let Ok(permit) = self.0.permits.clone().try_acquire_owned() else {
            send_reply(
                &gateway,
                reply,
                ResolveProcResult::Failed("endpoint discovery relay overloaded".to_owned()),
            );
            return;
        };
        let cancelled = self.0.cancelled.clone();
        self.0.tasks.spawn(
            AssertUnwindSafe(async move {
                let _permit = permit;
                let result = tokio::select! {
                    result = Gateway::query_endpoint_proc(endpoint) => result,
                    () = cancelled.cancelled() => return,
                    () = connection.cancelled() => return,
                };
                let result = match result {
                    Ok(id) => ResolveProcResult::Resolved(id),
                    Err(error) => ResolveProcResult::Failed(error.to_string()),
                };
                send_reply(&gateway, reply, result);
            })
            .catch_unwind()
            .map(|result| {
                if let Err(error) = result {
                    tracing::error!(?error, "endpoint discovery relay task panicked");
                }
            }),
        );
    }
}

fn send_reply(gateway: &Gateway, reply: OncePortRef<ResolveProcResult>, result: ResolveProcResult) {
    if let Err(error) = gateway.serialize_and_send_once(reply, result, monitored_return_handle()) {
        tracing::warn!(%error, "endpoint ResolveProc reply failed");
    }
}

/// Listener ingress is the only runtime allowed to consume ResolveProc.
/// Ordinary envelopes retain Gateway routing, including Via hop semantics.
#[derive(Clone)]
pub(crate) struct EndpointIngress {
    gateway: Gateway,
    owner: EndpointOwner,
    connection: CancellationToken,
}

impl EndpointIngress {
    pub(crate) fn new(gateway: Gateway, owner: EndpointOwner) -> Self {
        Self {
            gateway,
            owner,
            connection: CancellationToken::new(),
        }
    }

    pub(crate) fn serve(
        self,
        rx: impl crate::channel::Rx<MessageEnvelope> + Send + 'static,
    ) -> MailboxServerHandle {
        let cleanup = self.connection.clone().drop_guard();
        MailboxServer::serve(self, ConnectionRx { inner: rx, cleanup })
    }
}

#[async_trait]
impl MailboxSender for EndpointIngress {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        if !is_resolve(&envelope) {
            self.gateway.post_unchecked(envelope, return_handle);
            return;
        }
        // Reject routed/retargeted queries and malformed control messages
        // without actor lookup, queueing, route mutation, or panic.
        if envelope.has_next_hop() || envelope.dest() != &destination() {
            Gateway::return_no_route(envelope, return_handle);
            return;
        }
        let request = match envelope.deserialized::<ResolveProc>() {
            Ok(request) => request,
            Err(error) => {
                tracing::warn!(%error, "malformed endpoint ResolveProc request");
                Gateway::return_no_route(envelope, return_handle);
                return;
            }
        };
        match request.target {
            ResolveProcTarget::OwnEndpoint => {
                send_reply(&self.gateway, request.reply, self.owner.resolve())
            }
            ResolveProcTarget::Endpoint(endpoint) => self.owner.relay(
                self.gateway.clone(),
                self.connection.clone(),
                endpoint.into_dial_addr(),
                request.reply,
            ),
        }
    }
}

/// Cancel relay work when this receiver stops serving, including source close.
/// Duplex attachments have one receiver per source connection.
struct ConnectionRx<R> {
    inner: R,
    cleanup: tokio_util::sync::DropGuard,
}

#[async_trait]
impl<R: crate::channel::Rx<MessageEnvelope> + Send> crate::channel::Rx<MessageEnvelope>
    for ConnectionRx<R>
{
    async fn recv(&mut self) -> Result<MessageEnvelope, crate::channel::ChannelError> {
        self.inner.recv().await
    }

    fn addr(&self) -> ChannelAddr {
        self.inner.addr()
    }

    async fn join(self) {
        drop(self.cleanup);
        self.inner.join().await;
    }
}
