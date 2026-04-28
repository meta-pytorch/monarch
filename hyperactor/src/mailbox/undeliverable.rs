/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Undeliverable-message port helpers and the user-visible
//! `UndeliverableMessageError` type.
//!
//! ## Undeliverable-error text invariants (UE-*)
//!
//! - **UE-1 (bounded rendering).** `UndeliverableMessageError`
//!   `Display` must not render `envelope.headers()` or
//!   `envelope.data()` via their `Display` impls.
//!
//! - **UE-2 (core diagnostics preserved).** The rendered text keeps
//!   `sender`, `dest`, `message type`, `data_len`, and `error`.
//!
//! - **UE-3 (operation context names the top line).** When the
//!   envelope carries operation-context headers, the top line names
//!   that operation instead of falling back to the neutral
//!   `"undeliverable message error"` prefix.
//!
//! - **UE-4 (neutral wording).** Top-line wording remains neutral
//!   (`"undeliverable message for ..."`); presence of operation
//!   context does not classify the envelope as request or reply.

use std::sync::OnceLock;

use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::ActorHandle;
use crate::Instance;
// for macros
use crate::Message;
use crate::Proc;
use crate::mailbox::DeliveryError;
use crate::mailbox::MailboxSender;
use crate::mailbox::MessageEnvelope;
use crate::mailbox::PortHandle;
use crate::mailbox::PortReceiver;
use crate::mailbox::UndeliverableMailboxSender;
use crate::mailbox::headers::OPERATION_ADVERB;
use crate::mailbox::headers::OPERATION_ENDPOINT;

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
    let proc = Proc::local();
    crate::mailbox::Mailbox::new_detached(proc.proc_id().actor_ref("undeliverable"))
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

/// Compute the top-line prefix for a bounced envelope (UE-3, UE-4).
///
/// When `OPERATION_ENDPOINT` is present, name the operation;
/// otherwise fall back to a neutral prefix.
fn undeliverable_prefix(envelope: &MessageEnvelope) -> String {
    if let Some(endpoint) = envelope.headers().get(OPERATION_ENDPOINT) {
        let adverb = envelope
            .headers()
            .get(OPERATION_ADVERB)
            .unwrap_or_else(|| "?".to_string());
        return format!("undeliverable message for {} ({})", endpoint, adverb);
    }
    "undeliverable message error".to_string()
}

impl std::fmt::Display for UndeliverableMessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // For `DeliveryFailure`, the sender/dest fields describe the
        // failing hop. For `ReturnFailure`, they describe the
        // *original* envelope — the return hop failed, but the
        // identity fields still refer to the original delivery. Keep
        // the labels distinct so readers know which one they're
        // looking at.
        let (envelope, description, sender_label, dest_label) = match self {
            UndeliverableMessageError::DeliveryFailure { envelope } => (
                envelope,
                "delivery of message from sender to dest failed",
                "sender",
                "dest",
            ),
            UndeliverableMessageError::ReturnFailure { envelope } => (
                envelope,
                "returning undeliverable message to original sender failed",
                "original sender",
                "original dest",
            ),
        };

        writeln!(f, "{}:", undeliverable_prefix(envelope))?;
        writeln!(f, "\tdescription: {}", description)?;
        writeln!(f, "\t{}: {}", sender_label, envelope.sender())?;
        writeln!(f, "\t{}: {}", dest_label, envelope.dest())?;
        writeln!(
            f,
            "\tmessage type: {}",
            envelope.data().typename().unwrap_or("unknown")
        )?;
        writeln!(f, "\tdata_len: {}", envelope.data().len())?;
        writeln!(
            f,
            "\terror: {}",
            envelope.error_msg().unwrap_or("<none>".to_string())
        )
    }
}

#[cfg(test)]
mod tests {
    use hyperactor_config::Flattrs;

    use super::*;
    use crate::mailbox::MessageEnvelope;
    use crate::testing::ids::test_actor_id;
    use crate::testing::ids::test_port_id;

    fn make_envelope(payload: &str, headers: Flattrs) -> MessageEnvelope {
        let sender = test_actor_id("ue_proc", "ue_sender");
        let dest = test_port_id("ue_dest_proc", "ue_dest", 42);
        let data = wirevalue::Any::serialize(&payload.to_string()).unwrap();
        MessageEnvelope::new(sender, dest, data, headers)
    }

    /// UE-1: `DeliveryFailure` Display is bounded — no unbounded
    /// `headers: ...` or `data: ...` dumps. `data_len` replaces
    /// the payload body.
    #[test]
    fn test_ue1_delivery_failure_bounded() {
        let payload: String = std::iter::repeat_n('x', 10_000).collect();
        let mut headers = Flattrs::new();
        headers.set(OPERATION_ENDPOINT, "training.buffer.sample()".to_string());
        let envelope = make_envelope(&payload, headers);
        let rendered = format!(
            "{}",
            UndeliverableMessageError::DeliveryFailure { envelope }
        );

        assert!(
            rendered.contains("message type:"),
            "UE-1: message type field must be present, got:\n{rendered}"
        );
        assert!(
            rendered.contains("data_len:"),
            "UE-1: data_len field must be present, got:\n{rendered}"
        );
        assert!(
            rendered.contains("sender:"),
            "UE-2: sender field must be preserved, got:\n{rendered}"
        );
        assert!(
            rendered.contains("dest:"),
            "UE-2: dest field must be preserved, got:\n{rendered}"
        );
        assert!(
            rendered.contains("error:"),
            "UE-2: error field must be preserved, got:\n{rendered}"
        );
        // UE-1: the unbounded raw dumps must not appear.
        assert!(
            !rendered.contains("\theaders: "),
            "UE-1: raw headers dump leaked, got:\n{rendered}"
        );
        assert!(
            !rendered.contains("\tdata: "),
            "UE-1: raw data dump leaked, got:\n{rendered}"
        );
        // The 10_000-byte payload body must not appear verbatim.
        assert!(
            !rendered.contains(&payload),
            "UE-1: payload body leaked into rendered text"
        );
    }

    /// UE-1: `ReturnFailure` Display is bounded — same rule as
    /// `DeliveryFailure`. Covers the other match arm.
    #[test]
    fn test_ue1_return_failure_bounded() {
        let payload: String = std::iter::repeat_n('y', 10_000).collect();
        let envelope = make_envelope(&payload, Flattrs::new());
        let rendered = format!("{}", UndeliverableMessageError::ReturnFailure { envelope });

        assert!(
            rendered.contains("data_len:"),
            "UE-1: data_len field must be present, got:\n{rendered}"
        );
        assert!(
            !rendered.contains("\theaders: "),
            "UE-1: raw headers dump leaked, got:\n{rendered}"
        );
        assert!(
            !rendered.contains("\tdata: "),
            "UE-1: raw data dump leaked, got:\n{rendered}"
        );
        assert!(
            !rendered.contains(&payload),
            "UE-1: payload body leaked into rendered text"
        );
    }

    /// UE-3 / UE-4: when the envelope carries an operation endpoint,
    /// the top line is `"undeliverable message for <endpoint>
    /// (<adverb>)"`. Neutral wording — no claim about send vs reply
    /// kind.
    #[test]
    fn test_ue3_operation_endpoint_names_top_line() {
        let mut headers = Flattrs::new();
        headers.set(OPERATION_ENDPOINT, "training.buffer.sample()".to_string());
        headers.set(OPERATION_ADVERB, "call_one".to_string());
        let envelope = make_envelope("payload", headers);
        let rendered = format!(
            "{}",
            UndeliverableMessageError::DeliveryFailure { envelope }
        );

        let expected_line = "undeliverable message for training.buffer.sample() (call_one):";
        assert!(
            rendered.starts_with(expected_line),
            "UE-3/UE-4: expected top line `{expected_line}`, got:\n{rendered}"
        );
        // UE-4 specifically: the wording must be neutral — it must
        // not claim "reply" or "send" when we only know that
        // operation context is present.
        assert!(
            !rendered.contains("undeliverable reply"),
            "UE-4: must not claim reply-kind from header presence alone, got:\n{rendered}"
        );
        assert!(
            !rendered.contains("undeliverable send"),
            "UE-4: must not claim send-kind from header presence alone, got:\n{rendered}"
        );
    }

    /// UE-3 / UE-4: envelope with no operation context falls back to
    /// the neutral prefix.
    #[test]
    fn test_ue3_no_context_neutral_prefix() {
        let envelope = make_envelope("payload", Flattrs::new());
        let rendered = format!(
            "{}",
            UndeliverableMessageError::DeliveryFailure { envelope }
        );

        assert!(
            rendered.starts_with("undeliverable message error:"),
            "UE-3: no context → neutral prefix, got:\n{rendered}"
        );
    }
}
