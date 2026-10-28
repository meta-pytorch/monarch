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
    let mut envelope = MessageEnvelope::new_unknown(
        destination(),
        wirevalue::Any::serialize(&ResolveProc { reply })?,
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

/// One registration slot per listener; clones share only that listener's slot.
/// Weak ownership avoids retaining either the proc or its Gateway/server.
#[derive(Clone, Default)]
pub(crate) struct EndpointOwner(Arc<RwLock<Option<WeakProc>>>);

impl EndpointOwner {
    pub(crate) fn register(&self, proc: &Proc) -> anyhow::Result<()> {
        anyhow::ensure!(
            proc.proc_id().uid().is_instance(),
            "endpoint owner must have an instance id"
        );
        let mut owner = self.0.write().unwrap();
        if let Some(current) = owner.as_ref().and_then(WeakProc::upgrade) {
            anyhow::ensure!(
                current.downgrade().ptr_eq(&proc.downgrade()),
                "endpoint already has a live owner"
            );
        }
        *owner = Some(proc.downgrade());
        Ok(())
    }

    pub(crate) fn clear(&self) {
        *self.0.write().unwrap() = None;
    }

    fn resolve(&self) -> ResolveProcResult {
        match self.0.read().unwrap().as_ref().and_then(WeakProc::upgrade) {
            Some(proc) => ResolveProcResult::Resolved(proc.proc_id().clone()),
            None => ResolveProcResult::Unavailable,
        }
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
        if let Err(error) = self.gateway.serialize_and_send_once(
            request.reply,
            self.owner.resolve(),
            monitored_return_handle(),
        ) {
            tracing::warn!(%error, "endpoint ResolveProc reply failed");
        }
    }
}
