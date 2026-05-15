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
//! - **UE-3 (top-line shape).** The top line names the operation when
//!   the envelope carries `OPERATION_ENDPOINT`. Otherwise it names the
//!   actual failing hop, derived from the variant of
//!   `UndeliverableMessageError`:
//!   - `DeliveryFailure` → `"undeliverable message to {dest}"`,
//!     mirroring the abandonment-log surface in `mailbox.rs`.
//!   - `ReturnFailure` → `"undeliverable return to original sender
//!     {sender}"`. Sender/dest in this variant refer to the *original*
//!     envelope, so headlining `dest` would misstate the failing hop;
//!     the return-to-sender hop is the one that actually failed.
//!
//!   Both shapes only relocate UE-2 stable rendered fields into the
//!   headline; no unbounded surface is introduced.
//!
//! - **UE-4 (neutral wording).** Top-line wording is neutral re.
//!   request/reply classification. The three shapes — `"undeliverable
//!   message for {operation} ({adverb})"`, `"undeliverable message to
//!   {dest}"`, and `"undeliverable return to original sender
//!   {sender}"` — describe a bounce without claiming send-kind.
//!
//! - **UE-5 (message-type fallback).** When wirevalue type resolution
//!   is unavailable (`envelope.data().typename()` returns `None`), the
//!   `message type:` field falls back to the stamped
//!   `RUST_MESSAGE_TYPE` header (planted at every send by the
//!   `PortHandle`/`PortRef` paths in `mailbox.rs` / `ref_.rs`) before
//!   rendering the literal `"unknown"`. `"unknown"` is reserved for
//!   envelopes lacking both.

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
use crate::mailbox::headers::RUST_MESSAGE_TYPE;

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
    let proc = Proc::isolated();
    crate::mailbox::Mailbox::new_detached(proc.proc_addr().actor_addr("undeliverable"))
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
        if crate::Endpoint::send(&return_handle, client, Undeliverable(envelope)).is_err() {
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
/// When `OPERATION_ENDPOINT` is present, name the operation. Otherwise
/// name the actual failing hop, which differs between the two variants:
/// `DeliveryFailure` failed at `sender → dest`, while `ReturnFailure`
/// failed at the return hop `system → original sender` (sender/dest
/// in that variant still describe the original envelope, not the
/// failing return).
fn undeliverable_prefix(error: &UndeliverableMessageError) -> String {
    let envelope = match error {
        UndeliverableMessageError::DeliveryFailure { envelope }
        | UndeliverableMessageError::ReturnFailure { envelope } => envelope,
    };
    if let Some(endpoint) = envelope.headers().get(OPERATION_ENDPOINT) {
        let adverb = envelope
            .headers()
            .get(OPERATION_ADVERB)
            .unwrap_or_else(|| "?".to_string());
        return format!("undeliverable message for {} ({})", endpoint, adverb);
    }
    match error {
        UndeliverableMessageError::DeliveryFailure { .. } => {
            format!("undeliverable message to {}", envelope.dest())
        }
        UndeliverableMessageError::ReturnFailure { .. } => {
            format!(
                "undeliverable return to original sender {}",
                envelope.sender()
            )
        }
    }
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

        writeln!(f, "{}:", undeliverable_prefix(self))?;
        writeln!(f, "\tdescription: {}", description)?;
        writeln!(f, "\t{}: {}", sender_label, envelope.sender())?;
        writeln!(f, "\t{}: {}", dest_label, envelope.dest())?;
        // UE-5: prefer the wirevalue-resolved typename; fall back to
        // the static `RUST_MESSAGE_TYPE` stamped at send time before
        // resorting to the literal "unknown".
        let message_type = envelope
            .data()
            .typename()
            .map(|s| s.to_string())
            .or_else(|| envelope.headers().get(RUST_MESSAGE_TYPE))
            .unwrap_or_else(|| "unknown".to_string());
        writeln!(f, "\tmessage type: {}", message_type)?;
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

    /// UE-3: `DeliveryFailure` with no operation context falls back to
    /// naming the destination (the actual failing hop), mirroring the
    /// abandonment-log surface in `mailbox.rs`.
    #[test]
    fn test_ue3_delivery_failure_no_context_names_destination() {
        let envelope = make_envelope("payload", Flattrs::new());
        let dest_str = envelope.dest().to_string();
        let rendered = format!(
            "{}",
            UndeliverableMessageError::DeliveryFailure { envelope }
        );

        let expected_prefix = format!("undeliverable message to {}", dest_str);
        assert!(
            rendered.starts_with(&expected_prefix),
            "UE-3: delivery failure no context → destination prefix `{expected_prefix}`, got:\n{rendered}"
        );
        // The retired neutral wording must not return.
        assert!(
            !rendered.contains("undeliverable message error"),
            "UE-3: neutral fallback must not be re-introduced, got:\n{rendered}"
        );
    }

    /// UE-3: `ReturnFailure` with no operation context names the
    /// original sender, because in this variant `sender`/`dest` refer
    /// to the original envelope and the actual failing hop is
    /// `system → original sender`. Headlining `dest` here would
    /// misstate the failure.
    #[test]
    fn test_ue3_return_failure_no_context_names_original_sender() {
        let envelope = make_envelope("payload", Flattrs::new());
        let sender_str = envelope.sender().to_string();
        let dest_str = envelope.dest().to_string();
        let rendered = format!("{}", UndeliverableMessageError::ReturnFailure { envelope });

        let expected_prefix = format!("undeliverable return to original sender {}", sender_str);
        assert!(
            rendered.starts_with(&expected_prefix),
            "UE-3: return failure no context → original-sender prefix `{expected_prefix}`, got:\n{rendered}"
        );
        // Must not headline the original destination — the failing hop
        // is the return to the original sender, not the original
        // delivery.
        assert!(
            !rendered.starts_with(&format!("undeliverable message to {}", dest_str)),
            "UE-3: return failure must not headline the original destination, got:\n{rendered}"
        );
        // The retired neutral wording must not return.
        assert!(
            !rendered.contains("undeliverable message error"),
            "UE-3: neutral fallback must not be re-introduced, got:\n{rendered}"
        );
    }

    /// UE-5: when wirevalue type resolution is unavailable
    /// (`typename()` is `None`), the formatter falls back to the
    /// static `RUST_MESSAGE_TYPE` stamped at send time.
    #[test]
    fn test_ue5_message_type_falls_back_to_rust_message_type() {
        // `Any::new_broken()` carries `BROKEN_TYPEHASH` (0), which is
        // not in the wirevalue type registry, so `typename()` is None.
        // Mirrors the `test_broken_any` pattern in wirevalue itself.
        let sender = test_actor_id("ue_proc", "ue_sender");
        let dest = test_port_id("ue_dest_proc", "ue_dest", 42);
        let mut headers = Flattrs::new();
        headers.set(RUST_MESSAGE_TYPE, "my::Foo".to_string());
        let envelope = MessageEnvelope::new(sender, dest, wirevalue::Any::new_broken(), headers);
        assert!(
            envelope.data().typename().is_none(),
            "test fixture invariant: broken Any must have no typename()"
        );

        let rendered = format!(
            "{}",
            UndeliverableMessageError::DeliveryFailure { envelope }
        );

        assert!(
            rendered.contains("\tmessage type: my::Foo\n"),
            "UE-5: must surface RUST_MESSAGE_TYPE when typename() is absent, got:\n{rendered}"
        );
        assert!(
            !rendered.contains("\tmessage type: unknown"),
            "UE-5: must not render \"unknown\" when RUST_MESSAGE_TYPE is present, got:\n{rendered}"
        );
    }

    /// UE-5 (negative): when both `typename()` and `RUST_MESSAGE_TYPE`
    /// are absent, the formatter falls all the way through to the
    /// literal `"unknown"`.
    #[test]
    fn test_ue5_unknown_when_typename_and_rust_message_type_both_absent() {
        let sender = test_actor_id("ue_proc", "ue_sender");
        let dest = test_port_id("ue_dest_proc", "ue_dest", 42);
        let envelope =
            MessageEnvelope::new(sender, dest, wirevalue::Any::new_broken(), Flattrs::new());
        assert!(
            envelope.data().typename().is_none(),
            "test fixture invariant: broken Any must have no typename()"
        );

        let rendered = format!(
            "{}",
            UndeliverableMessageError::DeliveryFailure { envelope }
        );

        assert!(
            rendered.contains("\tmessage type: unknown\n"),
            "UE-5: with no typename and no RUST_MESSAGE_TYPE, must render \"unknown\", got:\n{rendered}"
        );
    }
}
