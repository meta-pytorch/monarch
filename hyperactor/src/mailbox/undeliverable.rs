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
//!   - `Report` → `"undeliverable message report to {dest}"`. The
//!     payload is unavailable, so the report carries structured
//!     delivery failures rather than the original envelope.
//!
//!   Both shapes only relocate UE-2 stable rendered fields into the
//!   headline; no unbounded surface is introduced.
//!
//! - **UE-4 (neutral wording).** Top-line wording is neutral re.
//!   request/reply classification. The top-line shapes describe a
//!   bounce without claiming send-kind.
//!
//! - **UE-5 (message-type fallback).** When wirevalue type resolution
//!   is unavailable (`envelope.data().typename()` returns `None`), the
//!   `message type:` field falls back to the stamped
//!   `RUST_MESSAGE_TYPE` header (planted at every send by the
//!   `PortHandle`/`PortRef` paths in `mailbox.rs` / `ref_.rs`) before
//!   rendering the literal `"unknown"`. `"unknown"` is reserved for
//!   envelopes lacking both.

use std::sync::OnceLock;

use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::ActorAddr;
use crate::ActorHandle;
use crate::Addr;
use crate::EndpointLocation;
use crate::Instance;
// for macros
use crate::Message;
use crate::Proc;
use crate::mailbox::DeliveryFailure;
use crate::mailbox::MailboxSender;
use crate::mailbox::MailboxSenderError;
use crate::mailbox::MessageEnvelope;
use crate::mailbox::PortHandle;
use crate::mailbox::PortReceiver;
use crate::mailbox::TransportFailure;
use crate::mailbox::TransportFailureReason;
use crate::mailbox::UndeliverableMailboxSender;
use crate::mailbox::UndeliverableReason;
use crate::mailbox::headers::OPERATION_ADVERB;
use crate::mailbox::headers::OPERATION_ENDPOINT;
use crate::mailbox::headers::RUST_MESSAGE_TYPE;

/// Metadata for a delivery failure whose original payload is unavailable.
#[derive(Debug, Serialize, Deserialize, Clone, typeuri::Named)]
pub struct DeliveryFailureReport {
    /// The actor that attempted the send.
    pub sender: ActorAddr,
    /// The destination that rejected the message.
    pub dest: EndpointLocation,
    /// The message type, if known.
    pub message_type: Option<String>,
    /// The delivery failures. The first entry is the root failure; later
    /// entries are failures encountered while returning or forwarding the
    /// failed message.
    pub delivery_failures: Vec<DeliveryFailure>,
}

impl DeliveryFailureReport {
    /// Construct delivery-failure metadata.
    pub fn new(
        sender: ActorAddr,
        dest: EndpointLocation,
        message_type: Option<String>,
        failure: DeliveryFailure,
    ) -> Self {
        Self {
            sender,
            dest,
            message_type,
            delivery_failures: vec![failure],
        }
    }

    /// Construct delivery-failure metadata from a local send error.
    pub(crate) fn from_send_error<M: Message>(
        sender: ActorAddr,
        dest: EndpointLocation,
        error: &MailboxSenderError,
    ) -> Self {
        let failure = match &dest {
            EndpointLocation::Port(port) => {
                super::serialized_send_error_delivery_failure(port, error)
            }
            EndpointLocation::Actor(actor) => {
                DeliveryFailure::new(UndeliverableReason::Transport(TransportFailure::new(
                    actor.clone(),
                    TransportFailureReason::LinkUnavailable(error.to_string()),
                )))
            }
            EndpointLocation::Local { actor, .. } => {
                DeliveryFailure::new(UndeliverableReason::Transport(TransportFailure::new(
                    actor.clone(),
                    TransportFailureReason::LinkUnavailable(error.to_string()),
                )))
            }
        };
        Self {
            sender,
            dest,
            message_type: Some(std::any::type_name::<M>().to_string()),
            delivery_failures: vec![failure],
        }
    }

    /// Construct delivery-failure metadata from a link-unavailable reason.
    pub(crate) fn link_unavailable<M: Message>(
        sender: ActorAddr,
        dest: EndpointLocation,
        error: impl Into<String>,
    ) -> Self {
        let failure = DeliveryFailure::new(UndeliverableReason::Transport(TransportFailure::new(
            delivery_failure_target(&dest),
            TransportFailureReason::LinkUnavailable(error.into()),
        )));
        Self::new(
            sender,
            dest,
            Some(std::any::type_name::<M>().to_string()),
            failure,
        )
    }

    /// Get the root structured delivery failure for this report.
    pub fn root_delivery_failure(&self) -> Option<&DeliveryFailure> {
        self.delivery_failures.first()
    }

    /// Get the string representation of the errors in this report.
    pub fn error_msg(&self) -> Option<String> {
        if self.delivery_failures.is_empty() {
            return None;
        }

        Some(
            self.delivery_failures
                .iter()
                .map(DeliveryFailure::render_bounded)
                .collect::<Vec<_>>()
                .join("; "),
        )
    }
}

fn delivery_failure_target(dest: &EndpointLocation) -> Addr {
    match dest {
        EndpointLocation::Actor(actor) => actor.clone().into(),
        EndpointLocation::Port(port) => port.clone().into(),
        EndpointLocation::Local { actor, .. } => actor.clone().into(),
    }
}

/// An undeliverable `M`-typed message.
#[expect(
    clippy::large_enum_variant,
    reason = "returned messages stay inline so callers can recover the original payload without extra allocation"
)]
#[derive(Debug, EnumAsInner, Serialize, Deserialize, Clone, typeuri::Named)]
pub enum Undeliverable<M: Message> {
    /// The message was returned intact.
    Returned(M),
    /// Delivery failed, but the original payload is unavailable.
    Report(DeliveryFailureReport),
}

impl<M: Message> Undeliverable<M> {
    /// Construct an undeliverable message that preserves the original payload.
    pub fn message(message: M) -> Self {
        Self::Returned(message)
    }

    /// Borrow the returned payload, if the payload was returned.
    pub fn as_message(&self) -> Option<&M> {
        match self {
            Self::Returned(message) => Some(message),
            Self::Report(_) => None,
        }
    }

    /// Mutably borrow the returned payload, if the payload was returned.
    pub fn as_message_mut(&mut self) -> Option<&mut M> {
        match self {
            Self::Returned(message) => Some(message),
            Self::Report(_) => None,
        }
    }

    /// Consume this undeliverable notification and return its payload, if the
    /// payload was returned.
    #[expect(
        clippy::result_large_err,
        reason = "preserve the old helper shape while callers migrate to explicit variants"
    )]
    pub fn into_message(self) -> Result<M, Self> {
        match self {
            Self::Returned(message) => Ok(message),
            report @ Self::Report(_) => Err(report),
        }
    }

    /// Construct an undeliverable message that carries only delivery-failure
    /// metadata.
    pub fn report(report: DeliveryFailureReport) -> Self {
        Self::Report(report)
    }
}

impl Undeliverable<MessageEnvelope> {
    /// Get the root structured delivery failure for this undeliverable
    /// notification.
    pub fn root_delivery_failure(&self) -> Option<&DeliveryFailure> {
        match self {
            Self::Returned(envelope) => envelope.root_delivery_failure(),
            Self::Report(report) => report.root_delivery_failure(),
        }
    }

    /// Convert this undeliverable notification into the corresponding error.
    pub fn into_error(self) -> UndeliverableMessageError {
        match self {
            Self::Returned(envelope) => UndeliverableMessageError::DeliveryFailure { envelope },
            Self::Report(report) => UndeliverableMessageError::Report { report },
        }
    }
}

// Port handle and receiver for undeliverable messages.
pub(crate) fn new_undeliverable_port() -> (
    PortHandle<Undeliverable<MessageEnvelope>>,
    PortReceiver<Undeliverable<MessageEnvelope>>,
) {
    let proc = Proc::isolated();
    crate::mailbox::Mailbox::new(proc.proc_addr().actor_addr("undeliverable"))
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
            while let Ok(undeliverable) = rx.recv().await {
                match undeliverable {
                    Undeliverable::Returned(mut envelope) => {
                        envelope.push_delivery_failure(DeliveryFailure::new(
                            UndeliverableReason::Transport(TransportFailure::new(
                                envelope.dest().clone(),
                                TransportFailureReason::LinkUnavailable(
                                    "message returned to undeliverable port".to_string(),
                                ),
                            )),
                        ));
                        super::UndeliverableMailboxSender
                            .post(envelope, /*unused */ h.clone());
                    }
                    Undeliverable::Report(report) => {
                        tracing::error!(
                            sender = %report.sender,
                            dest = %report.dest,
                            message_type = report.message_type.as_deref().unwrap_or("unknown"),
                            error = %report.error_msg().unwrap_or_default(),
                            "undeliverable message report returned to undeliverable port"
                        );
                    }
                }
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
        while let Ok(undeliverable) = rx.recv().await {
            match undeliverable {
                Undeliverable::Returned(mut envelope) => {
                    envelope.push_delivery_failure(DeliveryFailure::new(
                        UndeliverableReason::Transport(TransportFailure::new(
                            envelope.dest().clone(),
                            TransportFailureReason::LinkUnavailable(
                                "message returned to undeliverable port".to_string(),
                            ),
                        )),
                    ));
                    tracing::error!("{caller} took back an undeliverable message: {}", envelope);
                }
                Undeliverable::Report(report) => {
                    tracing::error!(
                        sender = %report.sender,
                        dest = %report.dest,
                        message_type = report.message_type.as_deref().unwrap_or("unknown"),
                        error = %report.error_msg().unwrap_or_default(),
                        "{caller} took back an undeliverable message report"
                    );
                }
            }
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
            .get_or_init(|| Proc::runtime().client("global_return_client").unwrap())
            .0;
        let envelope_copy = envelope.clone();
        if return_handle
            .try_send(client, Undeliverable::message(envelope))
            .is_err()
        {
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

    /// Delivery failed, but the original payload is unavailable.
    Report {
        /// The delivery-failure report.
        report: DeliveryFailureReport,
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
        UndeliverableMessageError::Report { report } => {
            return format!("undeliverable message report to {}", report.dest);
        }
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
        UndeliverableMessageError::Report { report } => {
            format!("undeliverable message report to {}", report.dest)
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
            UndeliverableMessageError::Report { report } => {
                writeln!(f, "{}:", undeliverable_prefix(self))?;
                writeln!(
                    f,
                    "\tdescription: delivery failed and the original payload is unavailable"
                )?;
                writeln!(f, "\tsender: {}", report.sender)?;
                writeln!(f, "\tdest: {}", report.dest)?;
                writeln!(
                    f,
                    "\tmessage type: {}",
                    report.message_type.as_deref().unwrap_or("unknown")
                )?;
                writeln!(
                    f,
                    "\terror: {}",
                    report.error_msg().unwrap_or("<none>".to_string())
                )?;
                return Ok(());
            }
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
    use crate::mailbox::InvalidReference;
    use crate::mailbox::InvalidReferenceReason;
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

    #[test]
    fn test_delivery_failure_display_uses_structured_failure() {
        let mut envelope = make_envelope("payload", Flattrs::new());
        let dest = envelope.dest().clone();
        envelope.push_delivery_failure(DeliveryFailure::new(InvalidReference::new(
            dest,
            InvalidReferenceReason::PortNeverAllocated,
        )));

        let rendered = format!(
            "{}",
            UndeliverableMessageError::DeliveryFailure { envelope }
        );

        assert!(
            rendered.contains("\terror: delivery failure: invalid reference"),
            "structured delivery failure should render in error field, got:\n{rendered}"
        );
        assert!(
            rendered.contains("port never allocated"),
            "structured reason should render in error field, got:\n{rendered}"
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
