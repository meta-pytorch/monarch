/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::OnceLock;

use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate as hyperactor; // for macros
use crate::Message;
use crate::Named;
use crate::actor::ActorStatus;
use crate::id;
use crate::mailbox::DeliveryError;
use crate::mailbox::MailboxSender;
use crate::mailbox::MessageEnvelope;
use crate::mailbox::PortHandle;
use crate::mailbox::PortReceiver;
use crate::mailbox::UndeliverableMailboxSender;
use crate::supervision::ActorSupervisionEvent;

/// An undeliverable `M`-typed message (in practice `M` is
/// [MessageEnvelope]).
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Named)]
pub struct Undeliverable<M: Message>(pub M);

impl<M: Message> Undeliverable<M> {
    /// Return the inner M-typed message.
    pub fn into_inner(self) -> M {
        self.0
    }
}

// Port handle and receiver for undeliverable messages.
pub(crate) fn new_undeliverable_port() -> (
    PortHandle<Undeliverable<MessageEnvelope>>,
    PortReceiver<Undeliverable<MessageEnvelope>>,
) {
    crate::mailbox::Mailbox::new_detached(id!(world[0].proc))
        .open_port::<Undeliverable<MessageEnvelope>>()
}

// An undeliverable message port handle to be shared amongst multiple
// producers. Messages sent here are forwarded to the undeliverable
// mailbox sender.
static MONITORED_RETURN_HANDLE: OnceLock<PortHandle<Undeliverable<MessageEnvelope>>> =
    OnceLock::new();
/// Accessor to the shared monitored undeliverable message port
/// handle. Initialization spawns the undeliverable message port
/// monitor that forwards incoming messages to the undeliverable
/// mailbox sender.
pub fn monitored_return_handle() -> PortHandle<Undeliverable<MessageEnvelope>> {
    let return_handle = MONITORED_RETURN_HANDLE.get_or_init(|| {
        let (return_handle, mut rx) = new_undeliverable_port();
        // Don't reuse `return_handle` for `h`: else it will never get
        // dropped and the task will never return.
        let (h, _) = new_undeliverable_port();
        crate::init::get_runtime().spawn(async move {
            while let Ok(Undeliverable(mut envelope)) = rx.recv().await {
                envelope.set_error(DeliveryError::BrokenLink(
                    "message returned to undeliverable port".to_string(),
                ));
                super::UndeliverableMailboxSender.post(envelope, /*unused */ h.clone());
            }
        });
        return_handle
    });

    return_handle.clone()
}

/// Now that monitored return handles are rare, it's becoming helpful
/// to get insights into where they are getting used (so that they can
/// be eliminated and replaced with something better).
#[track_caller]
pub fn custom_monitored_return_handle(caller: &str) -> PortHandle<Undeliverable<MessageEnvelope>> {
    let caller = caller.to_owned();
    let (return_handle, mut rx) = new_undeliverable_port();
    tokio::task::spawn(async move {
        while let Ok(Undeliverable(mut envelope)) = rx.recv().await {
            envelope.set_error(DeliveryError::BrokenLink(
                "message returned to undeliverable port".to_string(),
            ));
            tracing::error!("{caller} took back an undeliverable message: {}", envelope);
        }
    });
    return_handle
}

/// Returns a message envelope to its original sender.
pub(crate) fn return_undeliverable(
    return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    envelope: MessageEnvelope,
) {
    if envelope.return_undeliverable() {
        let envelope_copy = envelope.clone();
        if (return_handle.send(Undeliverable(envelope))).is_err() {
            UndeliverableMailboxSender.post(envelope_copy, /*unused*/ return_handle)
        }
    }
}

#[derive(Debug, Error)]
/// Errors that occur during message delivery and return.
pub enum UndeliverableMessageError {
    /// Delivery of a message to its destination failed.
    #[error(
        "a message from {} to {} was undeliverable and returned: {:?}: {envelope}",
        .envelope.sender(),
        .envelope.dest(),
        .envelope.error_msg()
    )]
    DeliveryFailure {
        /// The undelivered message.
        envelope: MessageEnvelope,
    },

    /// Delivery of an undeliverable message back to its sender
    /// failed.
    #[error(
        "returning an undeliverable message to sender {} failed: {:?}: {envelope}",
        .envelope.sender(),
        .envelope.error_msg()
    )]
    ReturnFailure {
        /// The undelivered message.
        envelope: MessageEnvelope,
    },
}

/// Drain undeliverables and convert them into
/// `ActorSupervisionEvent`, using a caller-provided resolver to
/// obtain the (possibly late) sink. If the resolver returns `None`,
/// we **log and drop** the undeliverable.
pub fn supervise_undeliverable_messages_with<R, F>(
    mut rx: PortReceiver<Undeliverable<MessageEnvelope>>,
    mut resolve_sink: R,
    on_undeliverable: F,
) where
    R: FnMut() -> Option<PortHandle<ActorSupervisionEvent>> + Send + 'static,
    F: Fn(&MessageEnvelope) + Send + Sync + 'static,
{
    crate::init::get_runtime().spawn(async move {
        while let Ok(Undeliverable(mut env)) = rx.recv().await {
            // Let caller log/trace before we mutate.
            on_undeliverable(&env);

            // `resolve_sink` provides the current supervision sink,
            // which may appear later (e.g., after a ProcMesh finishes
            // allocation). We call it on each message to ensure we
            // always target the latest sink.
            match resolve_sink() {
                Some(sink) => {
                    env.set_error(DeliveryError::BrokenLink(
                        "message returned to supervised undeliverable port".to_string(),
                    ));
                    let actor_id = env.dest().actor_id().clone();
                    let headers = env.headers().clone();

                    if let Err(e) = sink.send(ActorSupervisionEvent::new(
                        actor_id,
                        None,
                        ActorStatus::generic_failure(format!("message not delivered: {}", env)),
                        Some(headers),
                    )) {
                        tracing::warn!(
                            %e,
                            actor=%env.dest().actor_id(),
                            headers=?env.headers(),
                            "failed to forward supervision event; logging undeliverable"
                        );
                        UndeliverableMailboxSender.post(env, monitored_return_handle());
                    }
                }
                None => {
                    tracing::warn!(
                        actor=%env.dest().actor_id(),
                        headers=?env.headers(),
                        "no supervision sink yet; logging undeliverable"
                    );
                    UndeliverableMailboxSender.post(env, monitored_return_handle());
                }
            }
        }
    });
}

/// Spawns a task that listens for undeliverable messages and posts a
/// corresponding `ActorSupervisionEvent` to the given supervision
/// port.
pub fn supervise_undeliverable_messages<F>(
    supervision_port: PortHandle<ActorSupervisionEvent>,
    rx: PortReceiver<Undeliverable<MessageEnvelope>>,
    on_deliverable: F,
) where
    F: Fn(&MessageEnvelope) + Send + Sync + 'static,
{
    supervise_undeliverable_messages_with(
        rx,
        move || Some(supervision_port.clone()),
        on_deliverable,
    );
}
