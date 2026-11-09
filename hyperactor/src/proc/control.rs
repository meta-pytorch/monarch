/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Runtime control for the proc owning a contacted endpoint.
//!
//! Ownership is endpoint-scoped, not a singleton proc alias. Only terminal
//! endpoint ingress consumes resolution; transit uses ordinary routing.

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
use crate::Location;
use crate::OncePortRef;
use crate::PortAddr;
use crate::ProcAddr;
use crate::ProcId;
use crate::channel::ChannelAddr;
use crate::channel::ChannelTransport;
use crate::mailbox::DeliveryFailure;
use crate::mailbox::MailboxSender;
use crate::mailbox::MailboxServer;
use crate::mailbox::MailboxServerHandle;
use crate::mailbox::MessageEnvelope;
use crate::mailbox::PortHandle;
use crate::mailbox::PortSender;
use crate::mailbox::Undeliverable;
use crate::mailbox::monitored_return_handle;
use crate::port::Port;

/// Unsequenced request for the destination endpoint's owner.
#[derive(Debug, Serialize, Deserialize, typeuri::Named)]
pub(crate) struct ResolveProc {
    /// Each request owns an independently routed one-shot reply port.
    pub reply: OncePortRef<ResolveProcResult>,
}
wirevalue::register_type!(ResolveProc);

/// Snapshot of an endpoint's live concrete owner.
#[derive(Debug, Serialize, Deserialize, typeuri::Named)]
pub(crate) enum ResolveProcResult {
    /// Concrete incarnation of the registered live proc.
    Resolved(ProcId),
    /// No live proc owns the endpoint.
    Unavailable,
    /// The request failed before it reached the endpoint.
    Failed(Box<DeliveryFailure>),
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
            Self::Failed(failure) => {
                anyhow::bail!("endpoint proc query failed: {}", failure.render_bounded())
            }
        }
    }
}

// This selector is never inserted in Gateway.procs. Endpoint ingress selects
// the owner, including when bind and dial address spellings differ.
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
    location: Location,
    reply: OncePortRef<ResolveProcResult>,
) -> anyhow::Result<MessageEnvelope> {
    let dest = PortAddr::new(destination().id().clone(), location);
    let mut envelope =
        MessageEnvelope::new_unknown(dest, wirevalue::Any::serialize(&ResolveProc { reply })?);
    envelope.set_header(crate::ordering::SEQ_INFO, crate::ordering::SeqInfo::Direct);
    envelope.set_return_undeliverable(true);
    Ok(envelope)
}

/// Called by the existing mailbox-server failure loop, not a per-query relay.
/// Returns false for controls that are not valid endpoint queries.
pub(crate) fn return_resolution_failure(
    sender: &impl MailboxSender,
    envelope: &MessageEnvelope,
) -> bool {
    if !is_resolve(envelope) || envelope.dest().id() != destination().id() {
        return false;
    }
    let Ok(request) = envelope.deserialized::<ResolveProc>() else {
        return false;
    };
    let Some(failure) = envelope.root_delivery_failure() else {
        return false;
    };
    if let Err(error) = sender.serialize_and_send_once(
        request.reply,
        ResolveProcResult::Failed(Box::new(failure.clone())),
        monitored_return_handle(),
    ) {
        tracing::warn!(%error, "endpoint failure reply could not be sent");
    }
    true
}

pub(crate) fn is_resolve(envelope: &MessageEnvelope) -> bool {
    envelope
        .dest()
        .is_control_port_kind(ControlPort::ResolveProc)
}

enum OwnerState {
    Open(Option<WeakProc>),
    Stopped,
}

/// Weak registration belongs to one endpoint, never the whole gateway.
#[derive(Clone)]
pub(crate) struct EndpointOwner(Arc<RwLock<OwnerState>>);

impl Default for EndpointOwner {
    fn default() -> Self {
        Self(Arc::new(RwLock::new(OwnerState::Open(None))))
    }
}

impl EndpointOwner {
    pub(crate) fn register(&self, proc: &Proc) -> anyhow::Result<()> {
        anyhow::ensure!(
            proc.proc_id().uid().is_instance(),
            "endpoint owner must have an instance id"
        );
        let mut state = self.0.write().unwrap();
        let OwnerState::Open(owner) = &mut *state else {
            anyhow::bail!("endpoint is stopped");
        };
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
        *self.0.write().unwrap() = OwnerState::Stopped;
    }

    fn resolve(&self) -> ResolveProcResult {
        let state = self.0.read().unwrap();
        let proc = match &*state {
            OwnerState::Open(owner) => owner.as_ref().and_then(WeakProc::upgrade),
            OwnerState::Stopped => None,
        };
        match proc {
            Some(proc) => ResolveProcResult::Resolved(proc.proc_id().clone()),
            None => ResolveProcResult::Unavailable,
        }
    }
}

/// Only the endpoint where the path ends may answer resolution.
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
        if !is_resolve(&envelope) || !envelope.route_complete() {
            self.gateway.post_unchecked(envelope, return_handle);
            return;
        }
        if envelope.dest().id() != destination().id() {
            Gateway::return_no_route(envelope, return_handle);
            return;
        }
        let request = match envelope.deserialized::<ResolveProc>() {
            Ok(request) => request,
            Err(error) => {
                tracing::warn!(%error, "malformed endpoint resolve proc request");
                Gateway::return_no_route(envelope, return_handle);
                return;
            }
        };
        if let Err(error) = self.gateway.serialize_and_send_once(
            request.reply,
            self.owner.resolve(),
            monitored_return_handle(),
        ) {
            tracing::warn!(%error, "endpoint resolve proc reply failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ownership_is_weak_and_stop_cannot_be_undone() {
        let gateway = Gateway::isolated();
        let proc = Proc::builder()
            .shared_gateway(gateway.clone())
            .build()
            .unwrap();
        let owner = EndpointOwner::default();
        owner.register(&proc).unwrap();
        assert_eq!(owner.resolve().into_result().unwrap(), *proc.proc_id());
        let old = proc.proc_id().clone();
        drop(proc);
        assert!(matches!(owner.resolve(), ResolveProcResult::Unavailable));
        let next = Proc::builder().shared_gateway(gateway).build().unwrap();
        owner.register(&next).unwrap();
        assert_ne!(owner.resolve().into_result().unwrap(), old);
        owner.clear();
        assert!(owner.register(&next).is_err());
        assert!(matches!(owner.resolve(), ResolveProcResult::Unavailable));
        assert!(
            ResolveProcResult::Resolved(ProcId::singleton(Label::strip("invalid")))
                .into_result()
                .is_err()
        );
    }

    #[tokio::test]
    async fn malformed_and_wrong_selector_controls_are_not_answered() {
        let gateway = Gateway::isolated();
        let proc = Proc::builder()
            .shared_gateway(gateway.clone())
            .build()
            .unwrap();
        let owner = EndpointOwner::default();
        owner.register(&proc).unwrap();
        let ingress = EndpointIngress::new(gateway, owner);
        let client = proc.client("query");
        let (returns, mut failures) = client.open_port::<Undeliverable<MessageEnvelope>>();
        let (reply, receiver) = client.open_once_port::<ResolveProcResult>();
        let mut wrong = resolve_envelope(proc.default_location(), reply.bind())
            .unwrap()
            .with_dest(
                proc.proc_addr()
                    .actor_addr("runtime")
                    .port_addr(Port::control(ControlPort::ResolveProc)),
            );
        wrong.set_route_cursor(0);
        wrong.set_return_undeliverable(true);
        ingress.post(wrong, returns.clone());
        assert!(failures.recv().await.is_ok());
        let mut malformed = MessageEnvelope::new_unknown(
            PortAddr::new(destination().id().clone(), proc.default_location()),
            wirevalue::Any::serialize(&()).unwrap(),
        );
        malformed.set_route_cursor(0);
        ingress.post(malformed, returns);
        assert!(failures.recv().await.is_ok());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), receiver.recv())
                .await
                .is_err()
        );
    }
}
