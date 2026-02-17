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

use crate::ActorHandle;
use crate::Instance;
// for macros
use crate::Message;
use crate::Proc;
use crate::id;
use crate::mailbox::DeliveryError;
use crate::mailbox::MailboxSender;
use crate::mailbox::MessageEnvelope;
use crate::mailbox::PortHandle;
use crate::mailbox::PortReceiver;
use crate::mailbox::UndeliverableMailboxSender;

/// An undeliverable `M`-typed message (in practice `M` is
/// [MessageEnvelope]).
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, typeuri::Named)]
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
        // A global client for returning undeliverable messages.
        static CLIENT: OnceLock<(Instance<()>, ActorHandle<()>)> = OnceLock::new();
        let client = &CLIENT
            .get_or_init(|| Proc::runtime().instance("global_return_client").unwrap())
            .0;
        let envelope_copy = envelope.clone();
        if (return_handle.send(client, Undeliverable(envelope))).is_err() {
            UndeliverableMailboxSender.post(envelope_copy, /*unused*/ return_handle)
        }
    }
}

#[derive(Debug, Error)]
/// Errors that occur during message delivery and return.
pub enum UndeliverableMessageError {
    /// Delivery of a message to its destination failed.
    DeliveryFailure {
        /// The undelivered message.
        envelope: MessageEnvelope,
    },

    /// Delivery of an undeliverable message back to its sender
    /// failed.
    ReturnFailure {
        /// The undelivered message.
        envelope: MessageEnvelope,
    },
}

impl std::fmt::Display for UndeliverableMessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UndeliverableMessageError::DeliveryFailure { envelope } => {
                writeln!(f, "undeliverable message error:")?;
                writeln!(
                    f,
                    "\tdescription: delivery of message from sender to dest failed"
                )?;
                writeln!(f, "\tsender: {}", envelope.sender())?;
                writeln!(f, "\tdest: {}", envelope.dest())?;
                writeln!(f, "\theaders: {}", envelope.headers())?;
                writeln!(f, "\tdata: {}", envelope.data())?;
                writeln!(
                    f,
                    "\terror: {}",
                    envelope.error_msg().unwrap_or("<none>".to_string())
                )
            }
            UndeliverableMessageError::ReturnFailure { envelope } => {
                writeln!(f, "undeliverable message error:")?;
                writeln!(
                    f,
                    "\tdescription: returning undeliverable message to original sender failed"
                )?;
                writeln!(f, "\toriginal sender: {}", envelope.sender())?;
                writeln!(f, "\toriginal dest: {}", envelope.dest())?;
                writeln!(f, "\theaders: {}", envelope.headers())?;
                writeln!(f, "\tdata: {}", envelope.data())?;
                writeln!(
                    f,
                    "\terror: {}",
                    envelope.error_msg().unwrap_or("<none>".to_string())
                )
            }
        }
    }
}
