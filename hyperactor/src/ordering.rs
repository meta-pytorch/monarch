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
use std::fmt;
use std::ops::DerefMut;
use std::sync::Arc;
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
use crate::introspect::IntrospectMessage;

// Bypass-actor-workq: message types whose receivers are delivered via
// dedicated channels rather than the actor's work queue.
//
// The bypass-workq message type shares two invariants that the framework
// must honor:
//   1. Its actor port is pre-registered via `Ports::open_message_port` in
//      `Instance::new`; `Ports::get` rejects it via `is_bypass_workq_type_id`
//      so it can't accidentally be wired to the work queue.
//   2. Its sender-side sequence numbers must NOT share the per-actor seq
//      counter (`SeqKey::Actor`), because the work queue never observes them
//      and would otherwise see seq gaps that buffer subsequent workq messages
//      indefinitely. `Sequencer::assign_seq` uses `SeqKey::Port` for them.

/// Returns true if `id` is the `TypeId` of a bypass-channel message type
/// (i.e. one that must not be wired through `Ports::get`).
pub(crate) fn is_bypass_workq_type_id(id: TypeId) -> bool {
    id == TypeId::of::<IntrospectMessage>()
}

/// Returns true if `port` is the actor-port index of a bypass-channel
/// message type (i.e. the sequencer must use `SeqKey::Port` for it).
pub(crate) fn is_bypass_workq_actor_port(port: u64) -> bool {
    port == IntrospectMessage::port()
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

/// The sending half of an ordered channel; internally tracks one ordering
/// stream per remote sender session.
pub(crate) struct OrderedSender<T> {
    tx: mpsc::UnboundedSender<T>,
    /// Per-session reorder state keyed by `SeqInfo::Session.session_id`; one
    /// entry corresponds to one logical sender stream.
    states: Arc<DashMap<Uuid, Arc<Mutex<BufferState<T>>>>>,
    pub(crate) enable_buffering: bool,
    /// The identity of this object, used to distinguish it in debugging.
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

    /// Out-of-band snapshot of per-session ordering state. Reads each
    /// session's state via `try_lock`; sessions held by a concurrent
    /// `send` are counted in `skipped_session_count`, not silently
    /// dropped. Returned `sessions` are sorted by `session_id` so output
    /// is stable across calls and across DashMap iteration order.
    #[allow(dead_code)]
    pub(crate) fn snapshot(&self) -> OrderingSnapshot {
        let mut sessions = Vec::new();
        let mut skipped = 0usize;
        for entry in self.states.iter() {
            let session_id = *entry.key();
            match entry.value().try_lock() {
                Ok(state) => {
                    let oldest = state.buffer.keys().min().copied();
                    let newest = state.buffer.keys().max().copied();
                    sessions.push(OrderingSessionSnapshot {
                        session_id,
                        sender: state.sender.clone(),
                        last_released_seq: state.last_seq,
                        expected_next_seq: state.last_seq.saturating_add(1),
                        buffered_count: state.buffer.len(),
                        oldest_buffered_seq: oldest,
                        newest_buffered_seq: newest,
                    });
                }
                Err(_) => skipped += 1,
            }
        }
        // DashMap iter is nondeterministic; sort by session_id for
        // stable output.
        sessions.sort_by_key(|s| s.session_id);
        OrderingSnapshot {
            enabled: self.enable_buffering,
            sessions,
            skipped_session_count: skipped,
        }
    }
}

/// Per-session ordering snapshot. Read out-of-band from `OrderedSender`;
/// does not lock the work queue or hold per-session mutexes across awaits.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Named, AttrValue)]
pub struct OrderingSessionSnapshot {
    /// Identifier of the session whose ordering state this snapshot
    /// describes; matches `SeqInfo::Session { session_id, .. }` on the
    /// wire and the DashMap key in `OrderedSender::states`.
    pub session_id: Uuid,

    /// Sender `ActorAddr` captured by `BufferState`'s first-non-None-wins
    /// rule. `None` when every message in this session bypassed the
    /// normal stamping sites (rare: test fixtures, non-handler routes).
    pub sender: Option<ActorAddr>,

    /// Highest seq released from the reorder buffer into the actor work
    /// queue. NOT "handler processed" -- that's downstream of the queue.
    pub last_released_seq: u64,

    /// `last_released_seq + 1` -- the seq the next contiguous send must
    /// carry for delivery without further buffering.
    pub expected_next_seq: u64,

    /// Number of messages currently sitting in the reorder buffer waiting
    /// for a seq gap to be filled. Zero on healthy in-order sessions.
    pub buffered_count: usize,

    /// Lowest seq currently buffered. `None` when `buffered_count == 0`.
    pub oldest_buffered_seq: Option<u64>,

    /// Highest seq currently buffered. `None` when `buffered_count == 0`.
    pub newest_buffered_seq: Option<u64>,
}

/// Diagnostic projection of an `OrderedSender`.
///
/// `OrderedSender` is the sending half of an ordered channel for one actor
/// work queue, but it multiplexes many remote sender sessions internally.
/// Each `OrderingSessionSnapshot` corresponds to one `BufferState` entry,
/// keyed by `SeqInfo::Session.session_id`. Sequence progress here means
/// "released into the actor work queue", not "processed by the actor handler".
///
/// Carries completeness metadata so callers can distinguish "no stalled
/// sessions" from "snapshot was partial" from "buffering is disabled".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Named, AttrValue)]
pub struct OrderingSnapshot {
    /// Whether reorder buffering is enabled for this sender. When
    /// `false`, messages flow via `direct_send` and `sessions` is empty
    /// even under load. Sourced from `OrderedSender::enable_buffering`.
    pub enabled: bool,

    /// Per-session entries, sorted by `session_id` for stable output.
    /// Includes idle (drained) sessions with `buffered_count == 0`.
    pub sessions: Vec<OrderingSessionSnapshot>,

    /// Sessions whose mutex was held by a concurrent send when we tried
    /// to snapshot. NOT in `sessions`. Zero means the snapshot is
    /// complete.
    pub skipped_session_count: usize,
}

impl OrderingSnapshot {
    /// True when no session was skipped due to a concurrent send -- i.e.,
    /// every live session's state was successfully captured in
    /// `sessions`.
    pub fn is_complete(&self) -> bool {
        self.skipped_session_count == 0
    }
}

impl fmt::Display for OrderingSessionSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Infallible for this struct (all fields serialize cleanly).
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}

impl std::str::FromStr for OrderingSessionSnapshot {
    type Err = serde_json::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s)
    }
}

impl fmt::Display for OrderingSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}

impl std::str::FromStr for OrderingSnapshot {
    type Err = serde_json::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s)
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
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering as AtomicOrdering;

    use async_trait::async_trait;
    use hyperactor_config::Flattrs;
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use timed_test::async_timed_test;
    use tokio::sync::Barrier;
    use tokio::sync::oneshot;

    use super::*;
    use crate as hyperactor;
    use crate::Actor;
    use crate::ActorHandle;
    use crate::ActorRef;
    use crate::Context;
    use crate::Endpoint as _;
    use crate::Handler;
    use crate::Proc;
    use crate::config;
    use crate::mailbox::headers::SENDER_ACTOR_ID;
    use crate::mailbox::headers::stamp_sender_actor_id;
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
    /// delivered) -- capture runs before the seq match.
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

    // --------------------------------------------------------------------
    // OrderedSender::snapshot accessor tests.

    #[test]
    fn test_snapshot_empty() {
        let (tx, _rx) = ordered_channel::<u64>("test".to_string(), true);
        let snap = tx.snapshot();
        assert!(snap.enabled);
        assert!(snap.sessions.is_empty());
        assert_eq!(snap.skipped_session_count, 0);
        assert!(snap.is_complete());
    }

    #[test]
    fn test_snapshot_in_order() {
        let addr: ActorAddr = test_actor_id("sender_actor", "client");
        let session_id = Uuid::from_u128(1);
        let (tx, _rx) = ordered_channel::<u64>("test".to_string(), true);
        for s in 1..=3u64 {
            tx.send(session_id, s, Some(addr.clone()), s).unwrap();
        }

        let snap = tx.snapshot();
        assert_eq!(snap.sessions.len(), 1);
        let session = &snap.sessions[0];
        assert_eq!(session.session_id, session_id);
        assert_eq!(session.sender, Some(addr));
        assert_eq!(session.last_released_seq, 3);
        assert_eq!(session.expected_next_seq, 4);
        assert_eq!(session.buffered_count, 0);
        assert_eq!(session.oldest_buffered_seq, None);
        assert_eq!(session.newest_buffered_seq, None);
    }

    #[test]
    fn test_snapshot_out_of_order() {
        let addr: ActorAddr = test_actor_id("sender_actor", "client");
        let session_id = Uuid::from_u128(1);
        let (tx, _rx) = ordered_channel::<u64>("test".to_string(), true);

        tx.send(session_id, 1, Some(addr.clone()), 1).unwrap();
        tx.send(session_id, 3, None, 3).unwrap();
        tx.send(session_id, 5, None, 5).unwrap();

        let snap = tx.snapshot();
        assert_eq!(snap.sessions.len(), 1);
        let session = &snap.sessions[0];
        assert_eq!(session.last_released_seq, 1);
        assert_eq!(session.expected_next_seq, 2);
        assert_eq!(session.buffered_count, 2);
        assert_eq!(session.oldest_buffered_seq, Some(3));
        assert_eq!(session.newest_buffered_seq, Some(5));
        assert_eq!(session.sender, Some(addr));
    }

    /// The sender captured in `BufferState` during `OrderedSender::send`
    /// flows out through the snapshot accessor's `sender` field.
    #[test]
    fn test_snapshot_includes_sender() {
        let addr: ActorAddr = test_actor_id("captured", "client");
        let session_id = Uuid::from_u128(1);
        let (tx, _rx) = ordered_channel::<u64>("test".to_string(), true);
        tx.send(session_id, 1, Some(addr.clone()), 1).unwrap();

        let snap = tx.snapshot();
        assert_eq!(snap.sessions[0].sender, Some(addr));
    }

    #[test]
    fn test_snapshot_completeness_under_concurrent_send() {
        let session_a = Uuid::from_u128(1);
        let session_b = Uuid::from_u128(2);
        let (tx, _rx) = ordered_channel::<u64>("test".to_string(), true);
        tx.send(session_a, 1, None, 1).unwrap();
        tx.send(session_b, 1, None, 1).unwrap();

        // Hold session_a's mutex; snapshot's try_lock on this session
        // should fail and be counted as skipped.
        let state_a = tx.states.get(&session_a).unwrap().value().clone();
        let _guard = state_a.lock().unwrap();

        let snap = tx.snapshot();
        assert_eq!(snap.skipped_session_count, 1);
        assert!(!snap.is_complete());
        // session_b's mutex is free; it should still appear in `sessions`.
        assert_eq!(snap.sessions.len(), 1);
        assert_eq!(snap.sessions[0].session_id, session_b);
    }

    #[test]
    fn test_snapshot_disabled_buffering() {
        let (tx, _rx) = ordered_channel::<u64>("test".to_string(), false);
        let snap = tx.snapshot();
        assert!(!snap.enabled);
        assert!(snap.sessions.is_empty());
    }

    /// Pins the `sort_by_key(|s| s.session_id)` guarantee. Uses fixed
    /// UUIDs (not `Uuid::now_v7()`) so the test is independent of
    /// timestamp ordering. Sends `session_hi` first, then `session_lo`,
    /// then asserts the snapshot orders them low -> high.
    #[test]
    fn test_snapshot_sorted_by_session_id() {
        let session_lo = Uuid::from_u128(1);
        let session_hi = Uuid::from_u128(2);
        let (tx, _rx) = ordered_channel::<u64>("test".to_string(), true);
        tx.send(session_hi, 1, None, 1).unwrap();
        tx.send(session_lo, 1, None, 1).unwrap();

        let snap = tx.snapshot();
        assert_eq!(snap.sessions.len(), 2);
        assert_eq!(snap.sessions[0].session_id, session_lo);
        assert_eq!(snap.sessions[1].session_id, session_hi);
    }

    /// Pins the exact `AttrValue` contract used by `declare_attrs!`
    /// storage: `AttrValue::display(&self) -> String` then
    /// `AttrValue::parse(&str) -> Result<Self, _>`.
    #[test]
    fn test_snapshot_attr_roundtrip() {
        let addr: ActorAddr = test_actor_id("a", "client");
        let snap = OrderingSnapshot {
            enabled: true,
            sessions: vec![OrderingSessionSnapshot {
                session_id: Uuid::from_u128(42),
                sender: Some(addr),
                last_released_seq: 7,
                expected_next_seq: 8,
                buffered_count: 2,
                oldest_buffered_seq: Some(9),
                newest_buffered_seq: Some(11),
            }],
            skipped_session_count: 3,
        };
        let s = AttrValue::display(&snap);
        let parsed = <OrderingSnapshot as AttrValue>::parse(&s).unwrap();
        assert_eq!(snap, parsed);
    }

    // End-to-end ordering tests with real actor mailboxes. SenderActors
    // send normally to a ChaosActor. ChaosActor forwards the same messages
    // to ReceiverActor, but in shuffled batches and with optional duplicate
    // replay. ReceiverActor only records what its handler observes; its
    // mailbox OrderedSender is responsible for restoring per-session order.

    #[derive(Clone, Debug, Serialize, Deserialize, Named)]
    struct Frame {
        sender_idx: u32,
        payload_idx: u64,
    }

    #[derive(Clone, Debug, Serialize, Deserialize, Named)]
    struct SenderDone {
        sender_idx: u32,
    }

    // What ReceiverActor observed after its mailbox OrderedSender released a
    // frame: the preserved ordering header, the sender-owner header, and the
    // test payload.
    type ReceivedFrame = (SeqInfo, Option<ActorAddr>, Frame);

    // Start is only sent to a local ActorHandle, so it can carry local-only
    // test handles like ActorHandle and Arc<Barrier>. Do not export it.
    #[derive(Clone, Debug)]
    struct Start {
        count: u64,
        target: ActorHandle<ChaosActor>,
        start_barrier: Option<Arc<Barrier>>,
        yield_between_frames: bool,
    }

    #[derive(Debug)]
    struct SenderActor {
        sender_idx: u32,
    }

    #[async_trait]
    impl Actor for SenderActor {}

    #[async_trait]
    impl Handler<Start> for SenderActor {
        async fn handle(&mut self, cx: &Context<Self>, msg: Start) -> Result<(), anyhow::Error> {
            let Start {
                count,
                target,
                start_barrier,
                yield_between_frames,
            } = msg;
            if let Some(b) = start_barrier {
                b.wait().await;
            }
            for payload_idx in 0..count {
                target.post(
                    cx,
                    Frame {
                        sender_idx: self.sender_idx,
                        payload_idx,
                    },
                );
                if yield_between_frames {
                    tokio::task::yield_now().await;
                }
            }
            target.post(
                cx,
                SenderDone {
                    sender_idx: self.sender_idx,
                },
            );
            Ok(())
        }
    }

    // Which original frames should be replayed after their batch has been
    // forwarded once.
    #[derive(Debug, Clone, Copy)]
    enum DuplicatePolicy {
        None,
        EveryNth { stride: usize },
    }

    #[derive(Debug, Default)]
    struct ChaosStats {
        // Number of original frames that reveal a per-session inversion.
        // The counter is bumped on the lower sequence number, after a higher
        // sequence number from the same SEQ_INFO session was already
        // forwarded.
        out_of_order_original_forwards: AtomicUsize,

        // Number of original frames selected for replay.
        duplicates_selected: AtomicUsize,

        // Number of selected replays actually forwarded.
        late_duplicate_forwards: AtomicUsize,

        // Sessions that saw at least one inversion.
        out_of_order_sessions: Mutex<HashSet<Uuid>>,

        // Every frame ChaosActor forwarded, in forward order. Printed on
        // failures so the panic includes the chaos-side trace.
        forwarded_trace: Mutex<Vec<ForwardedEntry>>,

        // Frames observed by ReceiverActor after it already sent the result
        // snapshot. Non-zero is a failure. Zero is diagnostic only because
        // the test does not wait for a final drain.
        receiver_overflow: AtomicUsize,
    }

    // Used only in panic output.
    #[allow(dead_code)]
    #[derive(Debug, Clone)]
    struct ForwardedEntry {
        session_id: Uuid,
        seq: u64,
        sender_idx: u32,
        is_duplicate: bool,
    }

    #[derive(Debug)]
    #[hyperactor::export(handlers = [Frame, SenderDone])]
    struct ChaosActor {
        window: Vec<(SeqInfo, Flattrs, Frame)>,
        window_size: usize,
        target_port: PortAddr,
        rng: StdRng,
        expected_dones: u32,
        done_count: u32,
        expected_count_per_sender: u64,
        duplicate_policy: DuplicatePolicy,
        duplicate_cursor: usize,
        stats: Arc<ChaosStats>,
        session_owners: HashMap<Uuid, ActorAddr>,
        max_forwarded_seq_by_session: HashMap<Uuid, u64>,
    }

    #[async_trait]
    impl Actor for ChaosActor {}

    impl ChaosActor {
        // Forward the current batch to the receiver. The batch is shuffled,
        // then adjusted if needed so at least one session is out of order
        // whenever the batch has enough frames to make that possible.
        fn flush(&mut self, cx: &Context<Self>) -> Result<(), anyhow::Error> {
            self.window.shuffle(&mut self.rng);

            // A shuffled batch can accidentally still be ordered. If so,
            // pick the first session in window order that has at least two
            // frames and swap its first two frames.
            let mut first_seen_order: Vec<Uuid> = Vec::new();
            let mut session_indices: HashMap<Uuid, Vec<usize>> = HashMap::new();
            for (i, (seq_info, _, _)) in self.window.iter().enumerate() {
                if let SeqInfo::Session { session_id, .. } = seq_info {
                    if !session_indices.contains_key(session_id) {
                        first_seen_order.push(*session_id);
                    }
                    session_indices.entry(*session_id).or_default().push(i);
                }
            }
            for sid in &first_seen_order {
                let indices = &session_indices[sid];
                if indices.len() < 2 {
                    continue;
                }
                let mut already_inverted = false;
                for w in indices.windows(2) {
                    if seq_of(&self.window[w[0]].0) > seq_of(&self.window[w[1]].0) {
                        already_inverted = true;
                        break;
                    }
                }
                if !already_inverted {
                    self.window.swap(indices[0], indices[1]);
                    break;
                }
            }

            // Send each shuffled frame once. Some frames are cloned for a
            // later replay.
            let mut duplicates: Vec<(SeqInfo, Flattrs, Frame)> = Vec::new();
            let entries: Vec<(SeqInfo, Flattrs, Frame)> = self.window.drain(..).collect();
            for (seq_info, headers, frame) in entries {
                let (session_id, seq) = match &seq_info {
                    SeqInfo::Session { session_id, seq } => (*session_id, *seq),
                    SeqInfo::Direct => panic!("Direct SeqInfo at flush"),
                };
                // This frame proves an inversion if a higher sequence number
                // from the same session was already forwarded.
                let prev_max = self
                    .max_forwarded_seq_by_session
                    .get(&session_id)
                    .copied()
                    .unwrap_or(0);
                if seq < prev_max {
                    self.stats
                        .out_of_order_original_forwards
                        .fetch_add(1, AtomicOrdering::Relaxed);
                    self.stats
                        .out_of_order_sessions
                        .lock()
                        .unwrap()
                        .insert(session_id);
                }
                self.max_forwarded_seq_by_session
                    .insert(session_id, prev_max.max(seq));
                // Select every Nth original frame for later replay.
                let should_duplicate = match self.duplicate_policy {
                    DuplicatePolicy::None => false,
                    DuplicatePolicy::EveryNth { stride } => {
                        assert!(stride > 0, "DuplicatePolicy::EveryNth requires stride > 0");
                        self.duplicate_cursor = self.duplicate_cursor.wrapping_add(1);
                        self.duplicate_cursor.is_multiple_of(stride)
                    }
                };
                if should_duplicate {
                    duplicates.push((seq_info.clone(), headers.clone(), frame.clone()));
                    self.stats
                        .duplicates_selected
                        .fetch_add(1, AtomicOrdering::Relaxed);
                }
                // Forward with the original sequence header and original
                // sender owner, like a routing hop would.
                let owner = self
                    .session_owners
                    .get(&session_id)
                    .expect("session owner missing at original forward")
                    .clone();
                let dest = self.target_port.clone();
                let mut outbound = headers;
                stamp_sender_actor_id(&mut outbound, &seq_info, &dest, &owner);
                cx.post_with_external_seq_info(dest, outbound, wirevalue::Any::serialize(&frame)?);
                self.stats
                    .forwarded_trace
                    .lock()
                    .unwrap()
                    .push(ForwardedEntry {
                        session_id,
                        seq,
                        sender_idx: frame.sender_idx,
                        is_duplicate: false,
                    });
            }

            // Replay selected frames after the original batch. A duplicate
            // that arrives while its original is still buffered trips
            // OrderedSender's duplicate-buffer assert; these replays are
            // intended to hit the late-duplicate drop path instead.
            for (seq_info, headers, frame) in duplicates {
                let (session_id, seq) = match &seq_info {
                    SeqInfo::Session { session_id, seq } => (*session_id, *seq),
                    SeqInfo::Direct => unreachable!(),
                };
                let owner = self
                    .session_owners
                    .get(&session_id)
                    .expect("session owner missing at duplicate forward")
                    .clone();
                let dest = self.target_port.clone();
                let mut outbound = headers;
                stamp_sender_actor_id(&mut outbound, &seq_info, &dest, &owner);
                cx.post_with_external_seq_info(dest, outbound, wirevalue::Any::serialize(&frame)?);
                self.stats
                    .late_duplicate_forwards
                    .fetch_add(1, AtomicOrdering::Relaxed);
                self.stats
                    .forwarded_trace
                    .lock()
                    .unwrap()
                    .push(ForwardedEntry {
                        session_id,
                        seq,
                        sender_idx: frame.sender_idx,
                        is_duplicate: true,
                    });
            }

            Ok(())
        }
    }

    fn seq_of(s: &SeqInfo) -> u64 {
        match s {
            SeqInfo::Session { seq, .. } => *seq,
            SeqInfo::Direct => panic!("seq_of called on SeqInfo::Direct"),
        }
    }

    #[async_trait]
    impl Handler<Frame> for ChaosActor {
        async fn handle(&mut self, cx: &Context<Self>, frame: Frame) -> Result<(), anyhow::Error> {
            let seq_info = cx.headers().get(SEQ_INFO);
            let (session_id, seq) = match &seq_info {
                Some(SeqInfo::Session { session_id, seq }) => (*session_id, *seq),
                Some(SeqInfo::Direct) => {
                    panic!("chaos inbound has SeqInfo::Direct; bind misconfigured")
                }
                None => panic!("chaos inbound missing SEQ_INFO"),
            };
            let sender_addr = cx.headers().get(SENDER_ACTOR_ID);
            if seq <= 4 {
                assert!(
                    sender_addr.is_some(),
                    "missing SENDER_ACTOR_ID on early-session chaos inbound (seq={seq})",
                );
            }
            if let Some(addr) = &sender_addr {
                match self.session_owners.entry(session_id) {
                    std::collections::hash_map::Entry::Vacant(v) => {
                        v.insert(addr.clone());
                    }
                    std::collections::hash_map::Entry::Occupied(o) => {
                        assert_eq!(
                            o.get(),
                            addr,
                            "session owner changed mid-stream for session_id={session_id}",
                        );
                    }
                }
            }
            let inbound_headers = cx.headers().clone();
            self.window
                .push((seq_info.unwrap(), inbound_headers, frame));
            if self.window.len() >= self.window_size {
                self.flush(cx)?;
            }
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<SenderDone> for ChaosActor {
        async fn handle(
            &mut self,
            cx: &Context<Self>,
            done: SenderDone,
        ) -> Result<(), anyhow::Error> {
            let seq_info = cx.headers().get(SEQ_INFO);
            let (session_id, seq) = match &seq_info {
                Some(SeqInfo::Session { session_id, seq }) => (*session_id, *seq),
                Some(SeqInfo::Direct) => {
                    panic!("SenderDone arrived as SeqInfo::Direct; bind misconfigured")
                }
                None => panic!("SenderDone missing SEQ_INFO"),
            };
            // The sender posts all Frames and then SenderDone to the same
            // actor. They should share one sequence stream.
            assert_eq!(
                seq,
                self.expected_count_per_sender + 1,
                "SenderDone(sender_idx={}) seq={seq}; expected {} (count+1)",
                done.sender_idx,
                self.expected_count_per_sender + 1,
            );
            assert!(
                self.session_owners.contains_key(&session_id),
                "SenderDone(sender_idx={}) for session_id={session_id} \
                 before any Frame from that session",
                done.sender_idx,
            );
            // SenderDone is usually past the early messages that carry the
            // sender-owner header. If the header is present, it must still
            // match the session owner learned from the Frames.
            if let Some(addr) = cx.headers().get(SENDER_ACTOR_ID) {
                let owner = self.session_owners.get(&session_id).unwrap();
                assert_eq!(
                    &addr, owner,
                    "SenderDone SENDER_ACTOR_ID inconsistent with session_owner \
                     for session_id={session_id}",
                );
            }
            assert!(
                self.done_count < self.expected_dones,
                "extra SenderDone(sender_idx={}); done_count already at expected_dones={}",
                done.sender_idx,
                self.expected_dones,
            );
            self.done_count += 1;
            if self.done_count == self.expected_dones && !self.window.is_empty() {
                self.flush(cx)?;
            }
            Ok(())
        }
    }

    #[derive(Debug)]
    #[hyperactor::export(handlers = [Frame])]
    struct ReceiverActor {
        received: Vec<ReceivedFrame>,
        expected_total: usize,
        done: Option<oneshot::Sender<Vec<ReceivedFrame>>>,
        stats: Arc<ChaosStats>,
    }

    #[async_trait]
    impl Actor for ReceiverActor {}

    #[async_trait]
    impl Handler<Frame> for ReceiverActor {
        async fn handle(&mut self, cx: &Context<Self>, frame: Frame) -> Result<(), anyhow::Error> {
            let seq_info = match cx.headers().get(SEQ_INFO) {
                Some(SeqInfo::Session { session_id, seq }) => SeqInfo::Session { session_id, seq },
                Some(SeqInfo::Direct) => {
                    panic!("receiver inbound has SeqInfo::Direct; chaos forward bypassed SEQ_INFO")
                }
                None => panic!("receiver inbound missing SEQ_INFO"),
            };
            let sender_addr = cx.headers().get(SENDER_ACTOR_ID);
            // Anything after the snapshot is an extra delivery. Count it, but
            // keep the original snapshot unchanged for assertions.
            if self.done.is_none() {
                self.stats
                    .receiver_overflow
                    .fetch_add(1, AtomicOrdering::Relaxed);
                return Ok(());
            }
            self.received.push((seq_info, sender_addr, frame));
            if self.received.len() == self.expected_total {
                let tx = self.done.take().expect("done sender already consumed");
                let snapshot = std::mem::take(&mut self.received);
                let _ = tx.send(snapshot);
            }
            Ok(())
        }
    }

    // Checks the receiver-side invariants shared by all chaos tests. Panics
    // include the ChaosActor forward trace.
    fn assert_received_protocol_correct(
        received: &[ReceivedFrame],
        expected_senders: usize,
        expected_count_per_sender: u64,
        sender_addrs: &[ActorAddr],
        stats: &ChaosStats,
    ) {
        let expected_total = expected_senders * (expected_count_per_sender as usize);
        let dump_trace = || -> String {
            let trace = stats.forwarded_trace.lock().unwrap();
            format!("forwarded_trace ({} entries) = {:#?}", trace.len(), *trace,)
        };

        assert_eq!(
            received.len(),
            expected_total,
            "expected {} frames at receiver, got {}; {}",
            expected_total,
            received.len(),
            dump_trace(),
        );

        // This catches extra deliveries after the receiver snapshot.
        let overflow = stats.receiver_overflow.load(AtomicOrdering::Acquire);
        assert_eq!(
            overflow,
            0,
            "stats.receiver_overflow = {overflow}; duplicate leaked past \
             OrderedSender's drop branch after snapshot; {}",
            dump_trace(),
        );

        // Each logical sender should map to exactly one wire session.
        let mut sender_session: HashMap<u32, Uuid> = HashMap::new();
        for (seq_info, _, frame) in received {
            let session_id = match seq_info {
                SeqInfo::Session { session_id, .. } => *session_id,
                SeqInfo::Direct => {
                    panic!("receiver captured Direct SEQ_INFO; {}", dump_trace())
                }
            };
            match sender_session.entry(frame.sender_idx) {
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(session_id);
                }
                std::collections::hash_map::Entry::Occupied(o) => {
                    assert_eq!(
                        o.get(),
                        &session_id,
                        "sender_idx={} mapped to two distinct sessions ({} and {}); {}",
                        frame.sender_idx,
                        o.get(),
                        session_id,
                        dump_trace(),
                    );
                }
            }
        }

        // And each wire session should belong to exactly one logical sender.
        assert_eq!(
            sender_session.len(),
            expected_senders,
            "expected {} distinct sender_idx, got {}; {}",
            expected_senders,
            sender_session.len(),
            dump_trace(),
        );
        let distinct_sessions: HashSet<Uuid> = sender_session.values().copied().collect();
        assert_eq!(
            distinct_sessions.len(),
            expected_senders,
            "sender_idx -> session_id is not a bijection (got {} distinct sessions for {} senders); {}",
            distinct_sessions.len(),
            expected_senders,
            dump_trace(),
        );

        // Each sender's delivered subsequence should be exactly 0..K payloads
        // and 1..=K sequence numbers.
        for sender_idx in 0..(expected_senders as u32) {
            let subseq: Vec<&ReceivedFrame> = received
                .iter()
                .filter(|(_, _, f)| f.sender_idx == sender_idx)
                .collect();
            assert_eq!(
                subseq.len() as u64,
                expected_count_per_sender,
                "sender_idx={sender_idx}: expected {} frames, got {}; {}",
                expected_count_per_sender,
                subseq.len(),
                dump_trace(),
            );
            for (i, (seq_info, _, frame)) in subseq.iter().enumerate() {
                let expected_payload = i as u64;
                assert_eq!(
                    frame.payload_idx,
                    expected_payload,
                    "sender_idx={sender_idx}, position {i}: payload_idx={} (expected {}); {}",
                    frame.payload_idx,
                    expected_payload,
                    dump_trace(),
                );
                let expected_seq = (i as u64) + 1;
                let actual_seq = seq_of(seq_info);
                assert_eq!(
                    actual_seq,
                    expected_seq,
                    "sender_idx={sender_idx}, position {i}: SEQ_INFO.seq={} (expected {}); {}",
                    actual_seq,
                    expected_seq,
                    dump_trace(),
                );
            }
        }

        // Early messages should identify the actor that owns the sequence.
        for (seq_info, sender_addr_opt, frame) in received {
            let seq = seq_of(seq_info);
            if seq > 4 {
                continue;
            }
            let expected_addr = &sender_addrs[frame.sender_idx as usize];
            let captured = sender_addr_opt.as_ref().unwrap_or_else(|| {
                panic!(
                    "sender_idx={}, seq={seq}: SENDER_ACTOR_ID is None at early-session; {}",
                    frame.sender_idx,
                    dump_trace(),
                )
            });
            assert_eq!(
                captured,
                expected_addr,
                "sender_idx={}, seq={seq}: SENDER_ACTOR_ID {captured:?} != expected {expected_addr:?}; {}",
                frame.sender_idx,
                dump_trace(),
            );
        }
    }

    // One sender, shuffled batches, no duplicates.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_chaos_single_sender_preserves_order() {
        let config = hyperactor_config::global::lock();
        let _g = config.override_key(config::ENABLE_DEST_ACTOR_REORDERING_BUFFER, true);

        let proc = Proc::isolated();
        let client = proc.client("client");

        let stats = Arc::new(ChaosStats::default());
        let n: usize = 1;
        let k: u64 = 50;

        let (done_tx, done_rx) = oneshot::channel();
        let receiver_handle = proc.spawn_with_label(
            "receiver",
            ReceiverActor {
                received: Vec::new(),
                expected_total: n * (k as usize),
                done: Some(done_tx),
                stats: stats.clone(),
            },
        );
        let receiver_ref: ActorRef<ReceiverActor> = receiver_handle.bind();
        let target_port = receiver_ref.port::<Frame>().port_addr().clone();

        let chaos_handle = proc.spawn_with_label(
            "chaos",
            ChaosActor {
                window: Vec::new(),
                window_size: 10,
                target_port,
                rng: StdRng::seed_from_u64(0xC4A0_5EED),
                expected_dones: n as u32,
                done_count: 0,
                expected_count_per_sender: k,
                duplicate_policy: DuplicatePolicy::None,
                duplicate_cursor: 0,
                stats: stats.clone(),
                session_owners: HashMap::new(),
                max_forwarded_seq_by_session: HashMap::new(),
            },
        );
        // Bind exported handler ports so local handle posts carry
        // SEQ_INFO::Session instead of SeqInfo::Direct.
        let _bound_chaos_ref: ActorRef<ChaosActor> = chaos_handle.bind();

        let sender_handle = proc.spawn_with_label("sender0", SenderActor { sender_idx: 0 });
        let sender_addr = sender_handle.actor_addr().clone();

        sender_handle.post(
            &client,
            Start {
                count: k,
                target: chaos_handle.clone(),
                start_barrier: None,
                yield_between_frames: false,
            },
        );

        let received = match tokio::time::timeout(std::time::Duration::from_secs(5), done_rx).await
        {
            Ok(Ok(v)) => v,
            Ok(Err(_)) => panic!("done_rx sender dropped before sending; receiver crashed?"),
            Err(_) => {
                let trace = stats.forwarded_trace.lock().unwrap();
                panic!(
                    "timed out waiting for receiver; forwarded_trace ({} entries) = {:#?}",
                    trace.len(),
                    *trace,
                );
            }
        };

        assert_received_protocol_correct(&received, n, k, &[sender_addr], &stats);

        let oo = stats
            .out_of_order_original_forwards
            .load(AtomicOrdering::Acquire);
        assert!(
            oo > 0,
            "expected out_of_order_original_forwards > 0, got {oo}: \
             chaos shuffle never inverted a real frame (shuffle was identity?)",
        );
    }

    // Multiple senders share one chaos actor. Each sender must be delivered
    // in order; cross-sender interleaving is unconstrained.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_chaos_multi_sender_preserves_per_session_order() {
        let config = hyperactor_config::global::lock();
        let _g = config.override_key(config::ENABLE_DEST_ACTOR_REORDERING_BUFFER, true);

        let proc = Proc::isolated();
        let client = proc.client("client");

        let stats = Arc::new(ChaosStats::default());
        let n: usize = 4;
        let k: u64 = 25;

        let (done_tx, done_rx) = oneshot::channel();
        let receiver_handle = proc.spawn_with_label(
            "receiver",
            ReceiverActor {
                received: Vec::new(),
                expected_total: n * (k as usize),
                done: Some(done_tx),
                stats: stats.clone(),
            },
        );
        let receiver_ref: ActorRef<ReceiverActor> = receiver_handle.bind();
        let target_port = receiver_ref.port::<Frame>().port_addr().clone();

        let chaos_handle = proc.spawn_with_label(
            "chaos",
            ChaosActor {
                window: Vec::new(),
                window_size: 10,
                target_port,
                rng: StdRng::seed_from_u64(0xC4A0_5EED),
                expected_dones: n as u32,
                done_count: 0,
                expected_count_per_sender: k,
                duplicate_policy: DuplicatePolicy::None,
                duplicate_cursor: 0,
                stats: stats.clone(),
                session_owners: HashMap::new(),
                max_forwarded_seq_by_session: HashMap::new(),
            },
        );
        // Bind exported handler ports so local handle posts carry
        // SEQ_INFO::Session instead of SeqInfo::Direct.
        let _bound_chaos_ref: ActorRef<ChaosActor> = chaos_handle.bind();

        let barrier = Arc::new(Barrier::new(n));
        let mut sender_addrs: Vec<ActorAddr> = Vec::with_capacity(n);
        let mut sender_handles: Vec<ActorHandle<SenderActor>> = Vec::with_capacity(n);
        for sender_idx in 0..n {
            let h = proc.spawn_with_label(
                &format!("sender{sender_idx}"),
                SenderActor {
                    sender_idx: sender_idx as u32,
                },
            );
            sender_addrs.push(h.actor_addr().clone());
            sender_handles.push(h);
        }
        for h in &sender_handles {
            h.post(
                &client,
                Start {
                    count: k,
                    target: chaos_handle.clone(),
                    start_barrier: Some(barrier.clone()),
                    yield_between_frames: true,
                },
            );
        }

        let received = match tokio::time::timeout(std::time::Duration::from_secs(5), done_rx).await
        {
            Ok(Ok(v)) => v,
            Ok(Err(_)) => panic!("done_rx sender dropped before sending; receiver crashed?"),
            Err(_) => {
                let trace = stats.forwarded_trace.lock().unwrap();
                panic!(
                    "timed out waiting for receiver; forwarded_trace ({} entries) = {:#?}",
                    trace.len(),
                    *trace,
                );
            }
        };

        assert_received_protocol_correct(&received, n, k, &sender_addrs, &stats);

        let oo = stats
            .out_of_order_original_forwards
            .load(AtomicOrdering::Acquire);
        assert!(
            oo > 0,
            "expected out_of_order_original_forwards > 0, got {oo}",
        );
        // Keep the diagnostic set consistent with the global counter.
        let sessions_with_inversion = stats.out_of_order_sessions.lock().unwrap().len();
        assert!(
            sessions_with_inversion >= 1,
            "expected out_of_order_sessions.len() >= 1, got {sessions_with_inversion}",
        );
    }

    // Same as the multi-sender case, but replay every 10th original frame.
    // The receiver should still see each original exactly once.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_chaos_drops_duplicates() {
        let config = hyperactor_config::global::lock();
        let _g = config.override_key(config::ENABLE_DEST_ACTOR_REORDERING_BUFFER, true);

        let proc = Proc::isolated();
        let client = proc.client("client");

        let stats = Arc::new(ChaosStats::default());
        let n: usize = 4;
        let k: u64 = 25;

        let (done_tx, done_rx) = oneshot::channel();
        let receiver_handle = proc.spawn_with_label(
            "receiver",
            ReceiverActor {
                received: Vec::new(),
                expected_total: n * (k as usize),
                done: Some(done_tx),
                stats: stats.clone(),
            },
        );
        let receiver_ref: ActorRef<ReceiverActor> = receiver_handle.bind();
        let target_port = receiver_ref.port::<Frame>().port_addr().clone();

        let chaos_handle = proc.spawn_with_label(
            "chaos",
            ChaosActor {
                window: Vec::new(),
                window_size: 10,
                target_port,
                rng: StdRng::seed_from_u64(0xC4A0_5EED),
                expected_dones: n as u32,
                done_count: 0,
                expected_count_per_sender: k,
                duplicate_policy: DuplicatePolicy::EveryNth { stride: 10 },
                duplicate_cursor: 0,
                stats: stats.clone(),
                session_owners: HashMap::new(),
                max_forwarded_seq_by_session: HashMap::new(),
            },
        );
        // Bind exported handler ports so local handle posts carry
        // SEQ_INFO::Session instead of SeqInfo::Direct.
        let _bound_chaos_ref: ActorRef<ChaosActor> = chaos_handle.bind();

        let barrier = Arc::new(Barrier::new(n));
        let mut sender_addrs: Vec<ActorAddr> = Vec::with_capacity(n);
        let mut sender_handles: Vec<ActorHandle<SenderActor>> = Vec::with_capacity(n);
        for sender_idx in 0..n {
            let h = proc.spawn_with_label(
                &format!("sender{sender_idx}"),
                SenderActor {
                    sender_idx: sender_idx as u32,
                },
            );
            sender_addrs.push(h.actor_addr().clone());
            sender_handles.push(h);
        }
        for h in &sender_handles {
            h.post(
                &client,
                Start {
                    count: k,
                    target: chaos_handle.clone(),
                    start_barrier: Some(barrier.clone()),
                    yield_between_frames: true,
                },
            );
        }

        let received = match tokio::time::timeout(std::time::Duration::from_secs(5), done_rx).await
        {
            Ok(Ok(v)) => v,
            Ok(Err(_)) => panic!("done_rx sender dropped before sending; receiver crashed?"),
            Err(_) => {
                let trace = stats.forwarded_trace.lock().unwrap();
                panic!(
                    "timed out waiting for receiver; forwarded_trace ({} entries) = {:#?}",
                    trace.len(),
                    *trace,
                );
            }
        };

        assert_received_protocol_correct(&received, n, k, &sender_addrs, &stats);

        let selected = stats.duplicates_selected.load(AtomicOrdering::Acquire);
        let emitted = stats.late_duplicate_forwards.load(AtomicOrdering::Acquire);
        assert!(
            selected > 0,
            "expected duplicates_selected > 0, got {selected}: chaos policy never fired",
        );
        assert_eq!(
            emitted, selected,
            "duplicate selections did not match duplicate forwards: duplicates_selected={selected}, \
             late_duplicate_forwards={emitted}",
        );
        let oo = stats
            .out_of_order_original_forwards
            .load(AtomicOrdering::Acquire);
        assert!(
            oo > 0,
            "expected out_of_order_original_forwards > 0, got {oo}",
        );
    }
}
