/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module contains utilities used to help messages are delivered in order
//! for any given sender and receiver actor pair.

use std::any::TypeId;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;

use dashmap::DashMap;
use hyperactor_config::AttrValue;
use hyperactor_config::attrs::declare_attrs;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use typeuri::Named;
use uuid::Uuid;

use crate::ActorAddr;
use crate::PortAddr;
use crate::actor::Signal;
use crate::introspect::IntrospectMessage;

// Bypass-actor-workq registry: message types whose receivers are delivered via
// dedicated channels rather than the actor's work queue.
//
// Types in this registry share two invariants that the framework must honor:
//   1. Their actor port is pre-registered via `Ports::open_message_port` in
//      `Instance::new`; `Ports::get` rejects them via `is_bypass_workq_type_id` so
//      they can't accidentally be wired to the work queue.
//   2. Their sender-side sequence numbers must NOT share the per-actor seq
//      counter (`SeqKey::Actor`), because the work queue never observes them
//      and would otherwise see seq gaps that buffer subsequent workq messages
//      indefinitely. `Sequencer::assign_seq` uses `SeqKey::Port` for these.
//
// When adding a new bypass-channel actor-port message type, update both lists.
// A future move to `inventory`-driven registration can collapse them.

static BYPASS_TYPE_IDS: LazyLock<HashSet<TypeId>> =
    LazyLock::new(|| HashSet::from([TypeId::of::<Signal>(), TypeId::of::<IntrospectMessage>()]));

static BYPASS_ACTOR_PORTS: LazyLock<HashSet<u64>> =
    LazyLock::new(|| HashSet::from([Signal::port(), IntrospectMessage::port()]));

/// Returns true if `id` is the `TypeId` of a bypass-channel message type
/// (i.e. one that must not be wired through `Ports::get`).
pub(crate) fn is_bypass_workq_type_id(id: TypeId) -> bool {
    BYPASS_TYPE_IDS.contains(&id)
}

/// Returns true if `port` is the actor-port index of a bypass-channel
/// message type (i.e. the sequencer must use `SeqKey::Port` for it).
pub(crate) fn is_bypass_workq_actor_port(port: u64) -> bool {
    BYPASS_ACTOR_PORTS.contains(&port)
}

/// A client's re-ordering buffer state.
struct BufferState<T> {
    /// the last sequence number sent to receiver for this client. seq starts
    /// with 1 and 0 mean no message has been sent.
    last_seq: u64,
    /// Buffer out-of-order messages in order to ensures messages are delivered
    /// strictly in per-client sequence order.
    ///
    /// Map's key is seq_no, value is msg.
    buffer: HashMap<u64, T>,
    /// Sender ActorAddr, populated by **first non-None wins**: any
    /// arrival carrying a sender claims the slot if it's still None.
    /// Captured before the seq-ordering match, so even messages that are
    /// buffered (not delivered) or rejected as duplicate still record the
    /// sender. None when the originating send bypassed the normal
    /// `MailboxExt::post` / `PortHandle::try_post` / cast-leaf path (rare).
    sender: Option<ActorAddr>,
}

impl<T> Default for BufferState<T> {
    fn default() -> Self {
        Self {
            last_seq: 0,
            buffer: HashMap::new(),
            sender: None,
        }
    }
}

/// A sender that ensures messages are delivered in per-client sequence order.
pub(crate) struct OrderedSender<T> {
    tx: mpsc::UnboundedSender<T>,
    /// Map's key is session ID, and value is the buffer state of that session.
    states: Arc<DashMap<Uuid, Arc<Mutex<BufferState<T>>>>>,
    pub(crate) enable_buffering: bool,
    /// The identify of this object, which is used to distiguish it in debugging.
    log_id: String,
}

/// A receiver that receives messages in per-client sequence order.
pub(crate) fn ordered_channel<T>(
    log_id: String,
    enable_buffering: bool,
) -> (OrderedSender<T>, mpsc::UnboundedReceiver<T>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (
        OrderedSender {
            tx,
            states: Arc::new(DashMap::new()),
            enable_buffering,
            log_id,
        },
        rx,
    )
}

#[derive(Debug)]
pub(crate) enum OrderedSenderError<T> {
    InvalidZeroSeq(T),
    SendError(SendError<T>),
    FlushError(anyhow::Error),
}

impl<T> Clone for OrderedSender<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            states: self.states.clone(),
            enable_buffering: self.enable_buffering,
            log_id: self.log_id.clone(),
        }
    }
}

impl<T> OrderedSender<T> {
    /// Buffer msgs if necessary, and deliver them to receiver based on their
    /// seqs in monotonically increasing order. Note seq is scoped by `sender`
    /// so the ordering is also scoped by it.
    ///
    /// Locking behavior:
    ///
    /// For the same channel,
    /// * Calls from the same client will be serialized with a lock.
    /// * calls from different clients will be executed concurrently.
    pub(crate) fn send(
        &self,
        session_id: Uuid,
        seq_no: u64,
        sender: Option<ActorAddr>,
        msg: T,
    ) -> Result<(), OrderedSenderError<T>> {
        use std::cmp::Ordering;

        assert!(self.enable_buffering);
        if seq_no == 0 {
            return Err(OrderedSenderError::InvalidZeroSeq(msg));
        }

        // Make sure only this session's state is locked, not all states.
        let state = self.states.entry(session_id).or_default().value().clone();
        let mut state_guard = state.lock().unwrap();

        // Capture sender BEFORE the seq match: first non-None wins.
        // Ensures sender is recorded even when the message is buffered
        // (not delivered) or rejected as duplicate.
        if state_guard.sender.is_none()
            && let Some(addr) = sender
        {
            state_guard.sender = Some(addr);
        }

        let BufferState {
            last_seq,
            buffer,
            sender: _,
        } = state_guard.deref_mut();

        match seq_no.cmp(&(*last_seq + 1)) {
            Ordering::Less => {
                tracing::warn!(
                    "{} duplicate message from session {} with seq no: {}",
                    self.log_id,
                    session_id,
                    seq_no,
                );
            }
            Ordering::Greater => {
                // Future message: buffer until the gap is filled.
                let old = buffer.insert(seq_no, msg);
                assert!(
                    old.is_none(),
                    "{}: same seq is insert to buffer twice: {}",
                    self.log_id,
                    seq_no
                );
            }
            Ordering::Equal => {
                // In-order: deliver, then flush consecutives from buffer until
                // it reaches a gap.
                self.tx.send(msg).map_err(OrderedSenderError::SendError)?;
                *last_seq += 1;

                while let Some(m) = buffer.remove(&(*last_seq + 1)) {
                    match self.tx.send(m) {
                        Ok(()) => *last_seq += 1,
                        Err(err) => {
                            let flush_err = OrderedSenderError::FlushError(anyhow::anyhow!(
                                "failed to flush buffered message: {}",
                                err
                            ));
                            buffer.insert(*last_seq + 1, err.0);
                            return Err(flush_err);
                        }
                    }
                }
                // We do not remove a client's state even if its buffer becomes
                // empty. This is because a duplicate message might arrive after
                // the buffer became empty. Removing the state would cause the
                // duplicate message to be delivered.
            }
        }

        Ok(())
    }

    pub(crate) fn direct_send(&self, msg: T) -> Result<(), SendError<T>> {
        self.tx.send(msg)
    }
}

/// Key for sequence assignment.
/// Handler ports share a sequence per actor; non-handler ports get individual sequences.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum SeqKey {
    /// Shared sequence for all handler ports of an actor
    Actor(ActorAddr),
    /// Individual sequence for a specific non-handler port
    Port(PortAddr),
}

/// A message's sequencer number infomation.
#[derive(Debug, Serialize, Deserialize, Clone, Named, AttrValue, PartialEq)]
pub enum SeqInfo {
    /// Messages with the same session ID should be delivered in order.
    Session {
        /// Message's session ID
        session_id: Uuid,
        /// Message's sequence number in the given session.
        seq: u64,
    },
    /// This message does not have a seq number and should be delivered
    /// immediately.
    Direct,
}

impl fmt::Display for SeqInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Direct => write!(f, "direct"),
            Self::Session { session_id, seq } => write!(f, "{}:{}", session_id, seq),
        }
    }
}

impl std::str::FromStr for SeqInfo {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "direct" {
            return Ok(SeqInfo::Direct);
        }

        let parts: Vec<_> = s.split(':').collect();
        if parts.len() != 2 {
            return Err(anyhow::anyhow!("invalid SeqInfo: {}", s));
        }
        let session_id: Uuid = parts[0].parse()?;
        let seq: u64 = parts[1].parse()?;
        Ok(SeqInfo::Session { session_id, seq })
    }
}

declare_attrs! {
    /// The sender of this message, the session ID, and the message's sequence
    /// number assigned by this session.
    pub attr SEQ_INFO: SeqInfo;
}

/// Used by sender to track the message sequence numbers it sends to each destination.
/// Each [Sequencer] object has a session id, sequence numbers are scoped by
/// the (session_id, SeqKey) pair.
#[derive(Clone, Debug)]
pub struct Sequencer {
    session_id: Uuid,
    // Map's key is the sequence key (actor or port), value is the last seq number.
    last_seqs: Arc<Mutex<HashMap<SeqKey, u64>>>,
}

impl Sequencer {
    pub(crate) fn new(session_id: Uuid) -> Self {
        Self {
            session_id,
            last_seqs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Assign the next seq for a port, mutate the sequencer with the new seq,
    /// and return the new seq.
    ///
    /// - Handler ports: share the same sequence scheme per actor (keyed by ActorAddr)
    /// - Non-handler ports: get individual sequence schemes (keyed by PortAddr)
    /// - Bypass-workq actor ports: messages sent to these ports are delivered
    ///   to their dedicated channels rather than the actor's work queue. As a
    ///   result, they use the per-port counter instead.
    pub fn assign_seq(&self, port_id: &PortAddr) -> SeqInfo {
        let key = if port_id.is_handler_port() && !is_bypass_workq_actor_port(port_id.index()) {
            SeqKey::Actor(port_id.actor_addr().clone())
        } else {
            SeqKey::Port(port_id.clone())
        };

        let mut guard = self.last_seqs.lock().unwrap();
        let entry = guard.entry(key).or_default();
        *entry += 1;
        SeqInfo::Session {
            session_id: self.session_id,
            seq: *entry,
        }
    }

    /// Id of the session this sequencer belongs to.
    pub fn session_id(&self) -> Uuid {
        self.session_id
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::port::Port;
    use crate::testing::ids::test_actor_id;

    /// Test message type 1 for handler port sequencing tests.
    #[derive(Named)]
    struct TestMsg1;

    /// Test message type 2 for handler port sequencing tests.
    #[derive(Named)]
    struct TestMsg2;

    fn drain_try_recv<T: std::fmt::Debug + Clone>(rx: &mut mpsc::UnboundedReceiver<T>) -> Vec<T> {
        let mut out = Vec::new();
        while let Ok(m) = rx.try_recv() {
            out.push(m);
        }
        out
    }

    /// Helper to extract seq from SeqInfo::Session variant (for tests only)
    fn get_seq(seq_info: SeqInfo) -> u64 {
        match seq_info {
            SeqInfo::Session { seq, .. } => seq,
            SeqInfo::Direct => panic!("expected Session variant, got Direct"),
        }
    }

    #[test]
    fn test_ordered_channel_single_client_send_in_order() {
        let session_id_a = Uuid::now_v7();
        let (tx, mut rx) = ordered_channel::<u64>("test".to_string(), true);
        for s in 1..=10 {
            tx.send(session_id_a, s, None, s).unwrap();
            let got = drain_try_recv(&mut rx);
            assert_eq!(got, vec![s]);
        }
    }

    #[test]
    fn test_ordered_channel_single_client_send_out_of_order() {
        let session_id_a = Uuid::now_v7();
        let (tx, mut rx) = ordered_channel::<u64>("test".to_string(), true);

        // Send 2 to 4 in descending order: all should buffer until 1 arrives.
        for s in (2..=4).rev() {
            tx.send(session_id_a, s, None, s).unwrap();
        }

        // Send 7 to 9 in descending order: all should buffer until 1 - 6 arrives.
        for s in (7..=9).rev() {
            tx.send(session_id_a, s, None, s).unwrap();
        }

        assert!(
            drain_try_recv(&mut rx).is_empty(),
            "nothing should be delivered yet"
        );

        // Now send 1: should deliver 1 then flush 2 - 4.
        tx.send(session_id_a, 1, None, 1).unwrap();
        assert_eq!(drain_try_recv(&mut rx), vec![1, 2, 3, 4]);

        // Now send 5: should deliver immediately but not flush 7 - 9.
        tx.send(session_id_a, 5, None, 5).unwrap();
        assert_eq!(drain_try_recv(&mut rx), vec![5]);

        // Now send 6: should deliver 6 then flush 7 - 9.
        tx.send(session_id_a, 6, None, 6).unwrap();
        assert_eq!(drain_try_recv(&mut rx), vec![6, 7, 8, 9]);

        // Send 10: should deliver immediately.
        tx.send(session_id_a, 10, None, 10).unwrap();
        let got = drain_try_recv(&mut rx);
        assert_eq!(got, vec![10]);
    }

    #[test]
    fn test_ordered_channel_multi_clients() {
        let session_id_a = Uuid::now_v7();
        let session_id_b = Uuid::now_v7();
        let (tx, mut rx) = ordered_channel::<(Uuid, u64)>("test".to_string(), true);

        // A1 -> deliver
        tx.send(session_id_a, 1, None, (session_id_a, 1)).unwrap();
        assert_eq!(drain_try_recv(&mut rx), vec![(session_id_a, 1)]);
        // B1 -> deliver
        tx.send(session_id_b, 1, None, (session_id_b, 1)).unwrap();
        assert_eq!(drain_try_recv(&mut rx), vec![(session_id_b, 1)]);
        for s in (3..=5).rev() {
            // A3-5 -> buffer (waiting for A2)
            tx.send(session_id_a, s, None, (session_id_a, s)).unwrap();
            // B3-5 -> buffer (waiting for B2)
            tx.send(session_id_b, s, None, (session_id_b, s)).unwrap();
        }
        for s in (7..=9).rev() {
            // A7-9 -> buffer (waiting for A1-6)
            tx.send(session_id_a, s, None, (session_id_a, s)).unwrap();
            // B7-9 -> buffer (waiting for B1-6)
            tx.send(session_id_b, s, None, (session_id_b, s)).unwrap();
        }
        assert!(
            drain_try_recv(&mut rx).is_empty(),
            "nothing should be delivered yet"
        );

        // A2 -> deliver A2 then flush A3
        tx.send(session_id_a, 2, None, (session_id_a, 2)).unwrap();
        assert_eq!(
            drain_try_recv(&mut rx),
            vec![
                (session_id_a, 2),
                (session_id_a, 3),
                (session_id_a, 4),
                (session_id_a, 5),
            ]
        );
        // B2 -> deliver B2 then flush B3
        tx.send(session_id_b, 2, None, (session_id_b, 2)).unwrap();
        assert_eq!(
            drain_try_recv(&mut rx),
            vec![
                (session_id_b, 2),
                (session_id_b, 3),
                (session_id_b, 4),
                (session_id_b, 5),
            ]
        );

        // A6 -> should deliver immediately and flush A7-9
        tx.send(session_id_a, 6, None, (session_id_a, 6)).unwrap();
        assert_eq!(
            drain_try_recv(&mut rx),
            vec![
                (session_id_a, 6),
                (session_id_a, 7),
                (session_id_a, 8),
                (session_id_a, 9)
            ]
        );
        // B6 -> should deliver immediately and flush B7-9
        tx.send(session_id_b, 6, None, (session_id_b, 6)).unwrap();
        assert_eq!(
            drain_try_recv(&mut rx),
            vec![
                (session_id_b, 6),
                (session_id_b, 7),
                (session_id_b, 8),
                (session_id_b, 9)
            ]
        );
    }

    #[test]
    fn test_ordered_channel_duplicates() {
        let session_id_a = Uuid::now_v7();
        fn verify_empty_buffers<T>(states: &DashMap<Uuid, Arc<Mutex<BufferState<T>>>>) {
            for entry in states.iter() {
                assert!(entry.value().lock().unwrap().buffer.is_empty());
            }
        }

        let (tx, mut rx) = ordered_channel::<(Uuid, u64)>("test".to_string(), true);
        // A1 -> deliver
        tx.send(session_id_a, 1, None, (session_id_a, 1)).unwrap();
        assert_eq!(drain_try_recv(&mut rx), vec![(session_id_a, 1)]);
        verify_empty_buffers(&tx.states);
        // duplicate A1 -> drop even if the message is different.
        tx.send(session_id_a, 1, None, (session_id_a, 1_000))
            .unwrap();
        assert!(
            drain_try_recv(&mut rx).is_empty(),
            "nothing should be delivered yet"
        );
        verify_empty_buffers(&tx.states);
        // A2 -> deliver
        tx.send(session_id_a, 2, None, (session_id_a, 2)).unwrap();
        assert_eq!(drain_try_recv(&mut rx), vec![(session_id_a, 2)]);
        verify_empty_buffers(&tx.states);
        // late A1 duplicate -> drop
        tx.send(session_id_a, 1, None, (session_id_a, 1_001))
            .unwrap();
        assert!(
            drain_try_recv(&mut rx).is_empty(),
            "nothing should be delivered yet"
        );
        verify_empty_buffers(&tx.states);
    }

    #[test]
    fn test_sequencer_clone() {
        let sequencer = Sequencer {
            session_id: Uuid::now_v7(),
            last_seqs: Arc::new(Mutex::new(HashMap::new())),
        };

        let actor_ref: ActorAddr = test_actor_id("test_0", "test");
        let port_ref = actor_ref.port_addr(Port::from(1));

        // Modify original sequencer
        sequencer.assign_seq(&port_ref);
        sequencer.assign_seq(&port_ref);

        // Clone should share the same state
        let cloned_sequencer = sequencer.clone();
        assert_eq!(sequencer.session_id(), cloned_sequencer.session_id(),);
        assert_eq!(get_seq(cloned_sequencer.assign_seq(&port_ref)), 3);
    }

    #[test]
    fn test_sequencer_handler_ports_share_sequence() {
        let sequencer = Sequencer {
            session_id: Uuid::now_v7(),
            last_seqs: Arc::new(Mutex::new(HashMap::new())),
        };

        let actor_ref: ActorAddr = test_actor_id("worker_0", "worker");
        // Two different handler ports for the same actor (using Named::port())
        let handler_port_1 = actor_ref.port_addr(Port::from(TestMsg1::port()));
        let handler_port_2 = actor_ref.port_addr(Port::from(TestMsg2::port()));

        // Handler ports should share a sequence (keyed by ActorAddr)
        assert_eq!(get_seq(sequencer.assign_seq(&handler_port_1)), 1);
        assert_eq!(get_seq(sequencer.assign_seq(&handler_port_2)), 2); // continues from 1
        assert_eq!(get_seq(sequencer.assign_seq(&handler_port_1)), 3);

        // Handler ports from a different actor get their own shared sequence
        let actor_ref_2: ActorAddr = test_actor_id("worker_1", "worker");
        let handler_port_3 = actor_ref_2.port_addr(Port::from(TestMsg1::port()));
        assert_eq!(get_seq(sequencer.assign_seq(&handler_port_3)), 1); // independent from actor_ref
    }

    #[test]
    fn test_sequencer_non_handler_ports_have_independent_sequences() {
        let sequencer = Sequencer {
            session_id: Uuid::now_v7(),
            last_seqs: Arc::new(Mutex::new(HashMap::new())),
        };

        let actor_ref_0: ActorAddr = test_actor_id("worker_0", "worker");
        let actor_ref_1: ActorAddr = test_actor_id("worker_1", "worker");

        // Non-handler ports from the same actor (without ACTOR_PORT_BIT)
        let port_1 = actor_ref_0.port_addr(Port::from(1));
        let port_2 = actor_ref_0.port_addr(Port::from(2));

        // Non-handler ports should have independent sequences (keyed by PortAddr)
        assert_eq!(get_seq(sequencer.assign_seq(&port_1)), 1);
        assert_eq!(get_seq(sequencer.assign_seq(&port_2)), 1); // independent, starts at 1
        assert_eq!(get_seq(sequencer.assign_seq(&port_1)), 2);
        assert_eq!(get_seq(sequencer.assign_seq(&port_2)), 2);

        // Non-handler ports from different actors are also independent
        let port_3 = actor_ref_1.port_addr(Port::from(1));
        assert_eq!(get_seq(sequencer.assign_seq(&port_3)), 1); // independent from port_1
        assert_eq!(get_seq(sequencer.assign_seq(&port_1)), 3);
        assert_eq!(get_seq(sequencer.assign_seq(&port_3)), 2);
    }

    #[test]
    fn test_sequencer_mixed_handler_and_non_handler_ports() {
        let sequencer = Sequencer {
            session_id: Uuid::now_v7(),
            last_seqs: Arc::new(Mutex::new(HashMap::new())),
        };

        let actor_ref: ActorAddr = test_actor_id("worker_0", "worker");

        // Handler ports (share sequence per actor)
        let handler_port_1 = actor_ref.port_addr(Port::from(TestMsg1::port()));
        let handler_port_2 = actor_ref.port_addr(Port::from(TestMsg2::port()));

        // Non-handler ports (independent sequences per port)
        let non_handler_port_1 = actor_ref.port_addr(Port::from(1));
        let non_handler_port_2 = actor_ref.port_addr(Port::from(2));

        // Interleave sends to all port types
        assert_eq!(get_seq(sequencer.assign_seq(&handler_port_1)), 1);
        assert_eq!(get_seq(sequencer.assign_seq(&non_handler_port_1)), 1); // independent
        assert_eq!(get_seq(sequencer.assign_seq(&handler_port_2)), 2); // continues handler sequence
        assert_eq!(get_seq(sequencer.assign_seq(&non_handler_port_2)), 1); // independent
        assert_eq!(get_seq(sequencer.assign_seq(&non_handler_port_1)), 2); // continues its own
        assert_eq!(get_seq(sequencer.assign_seq(&handler_port_1)), 3); // continues handler sequence
        assert_eq!(get_seq(sequencer.assign_seq(&non_handler_port_2)), 2); // continues its own
    }

    #[test]
    fn bypass_registry_introspect_message() {
        assert!(is_bypass_workq_type_id(TypeId::of::<IntrospectMessage>()));
        assert!(is_bypass_workq_actor_port(IntrospectMessage::port()));
    }

    #[test]
    fn bypass_registry_signal() {
        assert!(is_bypass_workq_type_id(TypeId::of::<Signal>()));
        assert!(is_bypass_workq_actor_port(Signal::port()));
    }

    #[test]
    fn bypass_registry_lists_have_matching_lengths() {
        // If this fails, BYPASS_TYPE_IDS and BYPASS_ACTOR_PORTS have drifted.
        assert_eq!(BYPASS_TYPE_IDS.len(), BYPASS_ACTOR_PORTS.len());
    }

    #[test]
    fn bypass_actor_port_uses_per_port_seq_counter() {
        // Regression test for the seq-gap bug that caused pyspy 504s when
        // ENABLE_DEST_ACTOR_REORDERING_BUFFER=true. A bypass-channel actor port
        // (IntrospectMessage) must use SeqKey::Port so it doesn't share a counter
        // with workq-bound messages to the same actor.
        let sequencer = Sequencer::new(Uuid::now_v7());
        let actor_ref: ActorAddr = test_actor_id("agent_0", "proc_agent");

        let introspect_port = actor_ref.port_addr(Port::from(IntrospectMessage::port()));
        let regular_actor_port = actor_ref.port_addr(Port::from(TestMsg1::port()));

        // Both should start at seq=1 because their counters are independent.
        assert_eq!(get_seq(sequencer.assign_seq(&introspect_port)), 1);
        assert_eq!(get_seq(sequencer.assign_seq(&regular_actor_port)), 1);
        // And continue independently.
        assert_eq!(get_seq(sequencer.assign_seq(&introspect_port)), 2);
        assert_eq!(get_seq(sequencer.assign_seq(&regular_actor_port)), 2);
    }

    // --- SENDER_ACTOR_ID capture tests ---

    /// First send carrying a sender populates BufferState.sender.
    #[test]
    fn test_ordered_send_records_sender_on_first_with_addr() {
        let session_id = Uuid::now_v7();
        let addr: ActorAddr = test_actor_id("sender_0", "client");
        let (tx, _rx) = ordered_channel::<u64>("test".to_string(), true);

        tx.send(session_id, 1, Some(addr.clone()), 1).unwrap();

        let state = tx.states.get(&session_id).unwrap().value().clone();
        assert_eq!(state.lock().unwrap().sender, Some(addr));
    }

    /// First-non-None wins: a None arrival doesn't claim the slot; a
    /// subsequent Some arrival does.
    #[test]
    fn test_ordered_send_first_non_none_wins() {
        let session_id = Uuid::now_v7();
        let addr: ActorAddr = test_actor_id("sender_0", "client");
        let (tx, _rx) = ordered_channel::<u64>("test".to_string(), true);

        tx.send(session_id, 1, None, 1).unwrap();
        tx.send(session_id, 2, Some(addr.clone()), 2).unwrap();

        let state = tx.states.get(&session_id).unwrap().value().clone();
        assert_eq!(state.lock().unwrap().sender, Some(addr));
    }

    /// Second non-None arrival does NOT overwrite the first. Pathological
    /// (shouldn't happen in production); documents the invariant.
    #[test]
    fn test_ordered_send_second_addr_does_not_overwrite() {
        let session_id = Uuid::now_v7();
        let addr1: ActorAddr = test_actor_id("first_0", "client");
        let addr2: ActorAddr = test_actor_id("second_0", "client");
        let (tx, _rx) = ordered_channel::<u64>("test".to_string(), true);

        tx.send(session_id, 1, Some(addr1.clone()), 1).unwrap();
        tx.send(session_id, 2, Some(addr2), 2).unwrap();

        let state = tx.states.get(&session_id).unwrap().value().clone();
        assert_eq!(state.lock().unwrap().sender, Some(addr1));
    }

    /// All sends with None sender produce BufferState.sender = None; no panic.
    #[test]
    fn test_ordered_send_sender_absent() {
        let session_id = Uuid::now_v7();
        let (tx, _rx) = ordered_channel::<u64>("test".to_string(), true);

        tx.send(session_id, 1, None, 1).unwrap();
        tx.send(session_id, 2, None, 2).unwrap();

        let state = tx.states.get(&session_id).unwrap().value().clone();
        assert_eq!(state.lock().unwrap().sender, None);
    }

    /// Sender is captured even for messages that are buffered (not yet
    /// delivered) — capture runs before the seq match.
    #[test]
    fn test_ordered_send_sender_capture_runs_before_seq_match() {
        let session_id = Uuid::now_v7();
        let addr: ActorAddr = test_actor_id("sender_0", "client");
        let (tx, _rx) = ordered_channel::<u64>("test".to_string(), true);

        // Send seq 5 first: seq 1 is missing, so this gets buffered.
        // Sender capture must still happen before the buffer insert.
        tx.send(session_id, 5, Some(addr.clone()), 5).unwrap();

        let state = tx.states.get(&session_id).unwrap().value().clone();
        let guard = state.lock().unwrap();
        assert_eq!(guard.sender, Some(addr));
        // Confirm the message was buffered (not delivered): last_seq stays 0
        // and the buffer holds the message.
        assert_eq!(guard.last_seq, 0);
        assert!(guard.buffer.contains_key(&5));
    }

    /// Sequencer-level test for the debug-skip helper's underlying behavior:
    /// `assign_seq` called `count` times advances the per-(actor,dest)
    /// counter, so a subsequent `assign_seq` returns `count + 1` not 1.
    /// The `Instance<A>::debug_skip_next_ordering_seq` method is a tiny
    /// for-loop of `assign_seq` calls; its correctness reduces to this.
    #[test]
    fn test_sequencer_skip_advances_counter() {
        let sequencer = Sequencer::new(Uuid::now_v7());
        let actor_ref: ActorAddr = test_actor_id("test_0", "test");
        let dest = actor_ref.port_addr(Port::from(TestMsg1::port()));

        // Assign once: seq 1.
        assert_eq!(get_seq(sequencer.assign_seq(&dest)), 1);

        // Skip 2 (the helper would loop assign_seq).
        for _ in 0..2 {
            let _ = sequencer.assign_seq(&dest);
        }

        // Next assignment is seq 4, not 2.
        assert_eq!(get_seq(sequencer.assign_seq(&dest)), 4);
    }
}
