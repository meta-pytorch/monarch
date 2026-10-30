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
//! Only terminal listener ingress consumes it; transit uses ordinary routing.

use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use serde::Deserialize;
use serde::Serialize;

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
/// Submit through [`crate::Gateway::resolve_endpoint_proc`].
#[derive(Debug, Serialize, Deserialize, typeuri::Named)]
pub(crate) struct ResolveProc {
    /// Each request owns its normal, independently routed reply port.
    pub reply: OncePortRef<ResolveProcResult>,
}
wirevalue::register_type!(ResolveProc);

/// Snapshot of a listener's live concrete owner.
#[derive(Debug, Serialize, Deserialize, typeuri::Named)]
pub(crate) enum ResolveProcResult {
    /// Concrete incarnation of the registered live proc.
    Resolved(ProcId),
    /// No live proc owns the listener.
    Unavailable,
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
        }
    }
}

// Fixed selector identity, never a ProcId inserted in Gateway.procs. The
// request supplies its own routable location independently. Terminal ingress
// chooses the endpoint owner without comparing bind and dial address spellings.
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
    location: crate::Location,
    reply: OncePortRef<ResolveProcResult>,
) -> anyhow::Result<MessageEnvelope> {
    let dest = PortAddr::new(destination().id().clone(), location.clone());
    let mut envelope =
        MessageEnvelope::new_unknown(dest, wirevalue::Any::serialize(&ResolveProc { reply })?)
            .with_explicit_route(location);
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

struct OwnerState {
    proc: Option<WeakProc>,
    stopped: bool,
}

/// Weak owner registration belongs to one listener, never the whole Gateway.
#[derive(Clone)]
pub(crate) struct EndpointOwner(Arc<RwLock<OwnerState>>);

impl Default for EndpointOwner {
    fn default() -> Self {
        Self(Arc::new(RwLock::new(OwnerState {
            proc: None,
            stopped: false,
        })))
    }
}

impl EndpointOwner {
    pub(crate) fn register(&self, proc: &Proc) -> anyhow::Result<()> {
        anyhow::ensure!(
            proc.proc_id().uid().is_instance(),
            "endpoint owner must have an instance id"
        );
        let mut owner = self.0.write().unwrap();
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
        let mut owner = self.0.write().unwrap();
        owner.proc = None;
        owner.stopped = true;
    }

    fn resolve(&self) -> ResolveProcResult {
        match self
            .0
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
}

impl EndpointIngress {
    pub(crate) fn new(gateway: Gateway, owner: EndpointOwner) -> Self {
        Self { gateway, owner }
    }

    pub(crate) fn serve(
        self,
        rx: impl crate::channel::Rx<MessageEnvelope> + Send + 'static,
    ) -> MailboxServerHandle {
        MailboxServer::serve(self, rx)
    }
}

#[async_trait]
impl MailboxSender for EndpointIngress {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        if !is_resolve(&envelope) || !envelope.is_terminal() {
            self.gateway.post_unchecked(envelope, return_handle);
            return;
        }
        // Reject wrong selectors and malformed terminal controls without actor
        // lookup, handler queueing, route mutation, or panic.
        if envelope.dest().id() != destination().id() {
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
        send_reply(&self.gateway, request.reply, self.owner.resolve());
    }
}
