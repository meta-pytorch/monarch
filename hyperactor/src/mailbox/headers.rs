/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Message headers and latency tracking functionality for the mailbox system.
//!
//! This module provides header attributes and utilities for message metadata,
//! including latency tracking timestamps used to measure message processing times.

use std::any::type_name;
use std::time::SystemTime;

use hyperactor_config::Flattrs;
use hyperactor_config::attrs::OPERATION_CONTEXT_HEADER;
use hyperactor_config::attrs::declare_attrs;
use hyperactor_config::global;

use crate::ActorAddr;
use crate::PortAddr;
use crate::metrics::MESSAGE_LATENCY_MICROS;
use crate::ordering::SeqInfo;

declare_attrs! {
    /// Send timestamp for message latency tracking
    pub attr SEND_TIMESTAMP: SystemTime;

    /// The rust type of the message.
    pub attr RUST_MESSAGE_TYPE: String;

    /// Hashed ActorId of the message sender, injected in post_unchecked().
    pub attr SENDER_ACTOR_ID_HASH: u64;

    /// Full ActorAddr of the session owner — the actor whose Sequencer
    /// assigned this message's SEQ_INFO. Stamped at SEQ_INFO
    /// assignment/install sites: MailboxExt::post, PortHandle::try_post,
    /// and CommActor::deliver_to_dest (after V1 installs SEQ_INFO).
    /// Paired with the SEQ_INFO value.
    ///
    /// Framework-owned: stamping sites OVERWRITE caller-supplied values
    /// (do not trust callers to know who owns the session). This attr
    /// must never be propagated by handlers via verbatim header
    /// forwarding; the framework will overwrite stale forwards on the
    /// next ordered send through a trusted site.
    ///
    /// Larger than SENDER_ACTOR_ID_HASH (~50-100 bytes vs 8); both kept
    /// so the hash remains available for high-cardinality OTel labels.
    pub attr SENDER_ACTOR_ID: ActorAddr;

    /// Telemetry message ID for correlating lifecycle events, injected in post_unchecked().
    pub attr TELEMETRY_MESSAGE_ID: u64;

    /// Port index the message was delivered to, injected in post_unchecked().
    pub attr TELEMETRY_PORT_INDEX: u64;

    // Operation-context headers (see `OPERATION_CONTEXT_HEADER` in
    // `hyperactor_config::attrs`). Carried from the caller's outgoing
    // request onto the reply envelope by a consumer-side helper that
    // filters on `OPERATION_CONTEXT_HEADER`. Read at the
    // undeliverable-abandonment log site in
    // `hyperactor/src/mailbox.rs` to name the user operation a
    // dropped reply belonged to (UM-3b).
    //
    // Layering note: these keys belong semantically to a higher layer
    // (Monarch endpoint / adverb / method). They live in `hyperactor`
    // as a tactical compromise because the log reader lives here and
    // cannot depend upward on `monarch_hyperactor`. Scope narrowly;
    // do not grow this vocabulary without revisiting whether the
    // reader should move up a layer or a generic substrate-owned
    // operation-context abstraction should replace these keys.

    /// Qualified endpoint name of the caller's operation, e.g.
    /// "<mesh>.<method>()". Stamped by the request-send site.
    @meta(OPERATION_CONTEXT_HEADER = true)
    pub attr OPERATION_ENDPOINT: String;

    /// Endpoint adverb describing the call shape. Typical values from
    /// current Monarch producers: "call", "call_one", "choose",
    /// "stream".
    @meta(OPERATION_CONTEXT_HEADER = true)
    pub attr OPERATION_ADVERB: String;
}

/// Set the send timestamp for latency tracking if timestamp not already set.
pub fn set_send_timestamp(headers: &mut Flattrs) {
    if !headers.contains_key(SEND_TIMESTAMP) {
        let time = std::time::SystemTime::now();
        headers.set(SEND_TIMESTAMP, time);
    }
}

/// Set the send timestamp for latency tracking if timestamp not already set.
pub fn set_rust_message_type<M>(headers: &mut Flattrs) {
    headers.set(RUST_MESSAGE_TYPE, type_name::<M>().to_string());
}

/// Stamp `SENDER_ACTOR_ID` into `headers` if the gate conditions are met.
/// Framework-owned: overwrites existing values, never "sets if absent".
///
/// Gate: stamp when (early-session OR caller-set-stale) AND ordered handler
/// traffic. `caller_set_stale` defends against handlers that forward inbound
/// headers verbatim.
///
/// Callable from cross-crate sites (CommActor::deliver_to_dest in
/// hyperactor_mesh), so visibility is `pub` with `#[doc(hidden)]` to keep
/// it out of the public API surface.
#[doc(hidden)]
pub fn stamp_sender_actor_id(
    headers: &mut Flattrs,
    seq_info: &SeqInfo,
    dest: &PortAddr,
    owner: &ActorAddr,
) {
    if let SeqInfo::Session { seq, .. } = seq_info {
        let early_session = *seq <= 4;
        let caller_set_stale = headers.contains_key(SENDER_ACTOR_ID);
        if (early_session || caller_set_stale) && dest.is_handler_port() {
            headers.set(SENDER_ACTOR_ID, owner.clone());
        }
    }
}

/// Simpler stamping for paths where headers start fresh (no caller-supplied
/// stale value to defend against). Used only within the hyperactor crate by
/// PortHandle::try_post.
pub(crate) fn stamp_sender_actor_id_fresh(
    headers: &mut Flattrs,
    seq: u64,
    dest: &PortAddr,
    owner: &ActorAddr,
) {
    if seq <= 4 && dest.is_handler_port() {
        headers.set(SENDER_ACTOR_ID, owner.clone());
    }
}

/// This function checks the configured sampling rate and, if the random sample passes,
/// calculates the latency between the send timestamp and the current time, then records
/// the latency metric with the associated actor ID.
pub fn log_message_latency_if_sampling(headers: &Flattrs, actor_id: String) {
    if fastrand::f32() > global::get(crate::config::MESSAGE_LATENCY_SAMPLING_RATE) {
        return;
    }

    if !headers.contains_key(SEND_TIMESTAMP) {
        tracing::debug!(
            actor_id = actor_id,
            "SEND_TIMESTAMP missing from message headers, cannot measure latency"
        );
        return;
    }

    let metric_pairs = hyperactor_telemetry::kv_pairs!(
        "actor_id" => actor_id
    );
    let Some(send_timestamp) = headers.get(SEND_TIMESTAMP) else {
        return;
    };
    let now = std::time::SystemTime::now();
    let latency = now.duration_since(send_timestamp).unwrap_or_default();
    MESSAGE_LATENCY_MICROS.record(latency.as_micros() as f64, metric_pairs);
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::port::ControlPort;
    use crate::port::Port;
    use crate::testing::ids::test_actor_id;

    fn session(seq: u64) -> SeqInfo {
        SeqInfo::Session {
            session_id: Uuid::now_v7(),
            seq,
        }
    }

    fn handler_port(actor_name: &str) -> (ActorAddr, PortAddr) {
        let addr: ActorAddr = test_actor_id(actor_name, "worker");
        let port = addr.port_addr(Port::handler::<TestHandlerMsg>());
        (addr, port)
    }

    fn non_handler_port(actor_name: &str) -> (ActorAddr, PortAddr) {
        let addr: ActorAddr = test_actor_id(actor_name, "worker");
        // Non-handler port: a plain Port::from(integer), without the handler
        // bit. Per the ordering tests (test_sequencer_non_handler_ports_*),
        // Port::from(N) for small N is a non-handler port.
        let port = addr.port_addr(Port::from(1));
        (addr, port)
    }

    fn control_port(actor_name: &str) -> (ActorAddr, PortAddr) {
        let addr: ActorAddr = test_actor_id(actor_name, "worker");
        let port = addr.port_addr(Port::control(ControlPort::Introspect));
        (addr, port)
    }

    // A test handler-port message type. Named with handler-port semantics
    // so its port is a handler port distinct from bypass ports.
    #[derive(typeuri::Named)]
    struct TestHandlerMsg;

    #[test]
    fn test_stamp_helper_sets_sender_on_seq_1() {
        let (owner, dest) = handler_port("test_0");
        let mut headers = Flattrs::new();
        stamp_sender_actor_id(&mut headers, &session(1), &dest, &owner);
        assert_eq!(headers.get(SENDER_ACTOR_ID), Some(owner));
    }

    #[test]
    fn test_stamp_helper_sets_sender_on_seq_4() {
        let (owner, dest) = handler_port("test_0");
        let mut headers = Flattrs::new();
        stamp_sender_actor_id(&mut headers, &session(4), &dest, &owner);
        assert_eq!(headers.get(SENDER_ACTOR_ID), Some(owner));
    }

    #[test]
    fn test_stamp_helper_skips_seq_5_no_stale() {
        let (owner, dest) = handler_port("test_0");
        let mut headers = Flattrs::new();
        stamp_sender_actor_id(&mut headers, &session(5), &dest, &owner);
        assert_eq!(headers.get(SENDER_ACTOR_ID), None);
    }

    #[test]
    fn test_stamp_helper_overwrites_stale_at_seq_5() {
        let (owner, dest) = handler_port("test_0");
        let fake_owner: ActorAddr = test_actor_id("fake_0", "imposter");
        let mut headers = Flattrs::new();
        headers.set(SENDER_ACTOR_ID, fake_owner.clone());
        stamp_sender_actor_id(&mut headers, &session(5), &dest, &owner);
        assert_eq!(headers.get(SENDER_ACTOR_ID), Some(owner));
    }

    #[test]
    fn test_stamp_helper_skips_non_handler_port() {
        let (owner, dest) = non_handler_port("test_0");
        let mut headers = Flattrs::new();
        stamp_sender_actor_id(&mut headers, &session(1), &dest, &owner);
        assert_eq!(headers.get(SENDER_ACTOR_ID), None);
    }

    #[test]
    fn test_stamp_helper_skips_control_port() {
        let (owner, dest) = control_port("test_0");
        let mut headers = Flattrs::new();
        stamp_sender_actor_id(&mut headers, &session(1), &dest, &owner);
        assert_eq!(headers.get(SENDER_ACTOR_ID), None);
    }

    #[test]
    fn test_stamp_helper_skips_seq_info_direct() {
        let (owner, dest) = handler_port("test_0");
        let mut headers = Flattrs::new();
        stamp_sender_actor_id(&mut headers, &SeqInfo::Direct, &dest, &owner);
        assert_eq!(headers.get(SENDER_ACTOR_ID), None);
    }

    #[test]
    fn test_stamp_fresh_helper_sets_on_seq_4() {
        let (owner, dest) = handler_port("test_0");
        let mut headers = Flattrs::new();
        stamp_sender_actor_id_fresh(&mut headers, 4, &dest, &owner);
        assert_eq!(headers.get(SENDER_ACTOR_ID), Some(owner));
    }

    #[test]
    fn test_stamp_fresh_helper_skips_on_seq_5() {
        let (owner, dest) = handler_port("test_0");
        let mut headers = Flattrs::new();
        stamp_sender_actor_id_fresh(&mut headers, 5, &dest, &owner);
        assert_eq!(headers.get(SENDER_ACTOR_ID), None);
    }
}
