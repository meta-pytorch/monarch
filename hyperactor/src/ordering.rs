/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module contains utilities used to help messages are delivered in order
//! for any given sender and receiver actor pair.

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
use uuid::Uuid;

use crate::ActorId;
use crate::PortId;

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
}

impl<T> Default for BufferState<T> {
    fn default() -> Self {
        Self {
            last_seq: 0,
            buffer: HashMap::new(),
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
        let BufferState { last_seq, buffer } = state_guard.deref_mut();

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
        assert!(!self.enable_buffering);
        self.tx.send(msg)
    }
}

/// Key for sequence assignment.
/// Actor ports share a sequence per actor; non-actor ports get individual sequences.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum SeqKey {
    /// Shared sequence for all actor ports of an actor
    Actor(ActorId),
    /// Individual sequence for a specific non-actor port
    Port(PortId),
}

/// A message's sequencer number infomation.
#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    typeuri::Named,
    AttrValue,
    PartialEq
)]
pub struct SeqInfo {
    /// Message's session ID
    pub session_id: Uuid,
    /// Message's sequence number in the given session.
    pub seq: u64,
}
wirevalue::register_type!(SeqInfo);

impl fmt::Display for SeqInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.session_id, self.seq)
    }
}

impl std::str::FromStr for SeqInfo {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<_> = s.split(':').collect();
        if parts.len() != 2 {
            return Err(anyhow::anyhow!("invalid SeqInfo: {}", s));
        }
        let session_id: Uuid = parts[0].parse()?;
        let seq: u64 = parts[1].parse()?;
        Ok(SeqInfo { session_id, seq })
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
    /// - Actor ports: share the same sequence scheme per actor (keyed by ActorId)
    /// - Non-actor ports: get individual sequence schemes (keyed by PortId)
    pub fn assign_seq(&self, port_id: &PortId) -> SeqInfo {
        let key = if port_id.is_actor_port() {
            SeqKey::Actor(port_id.actor_id().clone())
        } else {
            SeqKey::Port(port_id.clone())
        };

        let mut guard = self.last_seqs.lock().unwrap();
        let entry = guard.entry(key).or_default();
        *entry += 1;
        SeqInfo {
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

    use typeuri::Named;

    use super::*;
    use crate::channel::ChannelAddr;
    use crate::channel::ChannelTransport;
    use crate::reference::ActorId;
    use crate::reference::ProcId;

    // Helper to create test ActorId
    fn test_actor_id(proc_name: &str, actor_name: &str) -> ActorId {
        let proc_id = ProcId(
            ChannelAddr::any(ChannelTransport::Local),
            proc_name.to_string(),
        );
        proc_id.actor_id(actor_name, 0)
    }

    /// Test message type 1 for actor port sequencing tests.
    #[derive(Named)]
    struct TestMsg1;

    /// Test message type 2 for actor port sequencing tests.
    #[derive(Named)]
    struct TestMsg2;

    fn drain_try_recv<T: std::fmt::Debug + Clone>(rx: &mut mpsc::UnboundedReceiver<T>) -> Vec<T> {
        let mut out = Vec::new();
        while let Ok(m) = rx.try_recv() {
            out.push(m);
        }
        out
    }

    #[test]
    fn test_ordered_channel_single_client_send_in_order() {
        let session_id_a = Uuid::now_v7();
        let (tx, mut rx) = ordered_channel::<u64>("test".to_string(), true);
        for s in 1..=10 {
            tx.send(session_id_a, s, s).unwrap();
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
            tx.send(session_id_a, s, s).unwrap();
        }

        // Send 7 to 9 in descending order: all should buffer until 1 - 6 arrives.
        for s in (7..=9).rev() {
            tx.send(session_id_a, s, s).unwrap();
        }

        assert!(
            drain_try_recv(&mut rx).is_empty(),
            "nothing should be delivered yet"
        );

        // Now send 1: should deliver 1 then flush 2 - 4.
        tx.send(session_id_a, 1, 1).unwrap();
        assert_eq!(drain_try_recv(&mut rx), vec![1, 2, 3, 4]);

        // Now send 5: should deliver immediately but not flush 7 - 9.
        tx.send(session_id_a, 5, 5).unwrap();
        assert_eq!(drain_try_recv(&mut rx), vec![5]);

        // Now send 6: should deliver 6 then flush 7 - 9.
        tx.send(session_id_a, 6, 6).unwrap();
        assert_eq!(drain_try_recv(&mut rx), vec![6, 7, 8, 9]);

        // Send 10: should deliver immediately.
        tx.send(session_id_a, 10, 10).unwrap();
        let got = drain_try_recv(&mut rx);
        assert_eq!(got, vec![10]);
    }

    #[test]
    fn test_ordered_channel_multi_clients() {
        let session_id_a = Uuid::now_v7();
        let session_id_b = Uuid::now_v7();
        let (tx, mut rx) = ordered_channel::<(Uuid, u64)>("test".to_string(), true);

        // A1 -> deliver
        tx.send(session_id_a, 1, (session_id_a, 1)).unwrap();
        assert_eq!(drain_try_recv(&mut rx), vec![(session_id_a, 1)]);
        // B1 -> deliver
        tx.send(session_id_b, 1, (session_id_b, 1)).unwrap();
        assert_eq!(drain_try_recv(&mut rx), vec![(session_id_b, 1)]);
        for s in (3..=5).rev() {
            // A3-5 -> buffer (waiting for A2)
            tx.send(session_id_a, s, (session_id_a, s)).unwrap();
            // B3-5 -> buffer (waiting for B2)
            tx.send(session_id_b, s, (session_id_b, s)).unwrap();
        }
        for s in (7..=9).rev() {
            // A7-9 -> buffer (waiting for A1-6)
            tx.send(session_id_a, s, (session_id_a, s)).unwrap();
            // B7-9 -> buffer (waiting for B1-6)
            tx.send(session_id_b, s, (session_id_b, s)).unwrap();
        }
        assert!(
            drain_try_recv(&mut rx).is_empty(),
            "nothing should be delivered yet"
        );

        // A2 -> deliver A2 then flush A3
        tx.send(session_id_a, 2, (session_id_a, 2)).unwrap();
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
        tx.send(session_id_b, 2, (session_id_b, 2)).unwrap();
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
        tx.send(session_id_a, 6, (session_id_a, 6)).unwrap();
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
        tx.send(session_id_b, 6, (session_id_b, 6)).unwrap();
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
        tx.send(session_id_a, 1, (session_id_a, 1)).unwrap();
        assert_eq!(drain_try_recv(&mut rx), vec![(session_id_a, 1)]);
        verify_empty_buffers(&tx.states);
        // duplicate A1 -> drop even if the message is different.
        tx.send(session_id_a, 1, (session_id_a, 1_000)).unwrap();
        assert!(
            drain_try_recv(&mut rx).is_empty(),
            "nothing should be delivered yet"
        );
        verify_empty_buffers(&tx.states);
        // A2 -> deliver
        tx.send(session_id_a, 2, (session_id_a, 2)).unwrap();
        assert_eq!(drain_try_recv(&mut rx), vec![(session_id_a, 2)]);
        verify_empty_buffers(&tx.states);
        // late A1 duplicate -> drop
        tx.send(session_id_a, 1, (session_id_a, 1_001)).unwrap();
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

        let actor_id = test_actor_id("test_0", "test");
        let port_id = actor_id.port_id(1);

        // Modify original sequencer
        sequencer.assign_seq(&port_id);
        sequencer.assign_seq(&port_id);

        // Clone should share the same state
        let cloned_sequencer = sequencer.clone();
        assert_eq!(sequencer.session_id(), cloned_sequencer.session_id(),);
        assert_eq!(cloned_sequencer.assign_seq(&port_id).seq, 3);
    }

    #[test]
    fn test_sequencer_actor_ports_share_sequence() {
        let sequencer = Sequencer {
            session_id: Uuid::now_v7(),
            last_seqs: Arc::new(Mutex::new(HashMap::new())),
        };

        let actor_id = test_actor_id("worker_0", "worker");
        // Two different actor ports for the same actor (using Named::port())
        let actor_port_1 = actor_id.port_id(TestMsg1::port());
        let actor_port_2 = actor_id.port_id(TestMsg2::port());

        // Actor ports should share a sequence (keyed by ActorId)
        assert_eq!(sequencer.assign_seq(&actor_port_1).seq, 1);
        assert_eq!(sequencer.assign_seq(&actor_port_2).seq, 2); // continues from 1
        assert_eq!(sequencer.assign_seq(&actor_port_1).seq, 3);

        // Actor ports from a different actor get their own shared sequence
        let actor_id_2 = test_actor_id("worker_1", "worker");
        let actor_port_3 = actor_id_2.port_id(TestMsg1::port());
        assert_eq!(sequencer.assign_seq(&actor_port_3).seq, 1); // independent from actor_id
    }

    #[test]
    fn test_sequencer_non_actor_ports_have_independent_sequences() {
        let sequencer = Sequencer {
            session_id: Uuid::now_v7(),
            last_seqs: Arc::new(Mutex::new(HashMap::new())),
        };

        let actor_id_0 = test_actor_id("worker_0", "worker");
        let actor_id_1 = test_actor_id("worker_1", "worker");

        // Non-actor ports from the same actor (without ACTOR_PORT_BIT)
        let port_1 = actor_id_0.port_id(1);
        let port_2 = actor_id_0.port_id(2);

        // Non-actor ports should have independent sequences (keyed by PortId)
        assert_eq!(sequencer.assign_seq(&port_1).seq, 1);
        assert_eq!(sequencer.assign_seq(&port_2).seq, 1); // independent, starts at 1
        assert_eq!(sequencer.assign_seq(&port_1).seq, 2);
        assert_eq!(sequencer.assign_seq(&port_2).seq, 2);

        // Non-actor ports from different actors are also independent
        let port_3 = actor_id_1.port_id(1);
        assert_eq!(sequencer.assign_seq(&port_3).seq, 1); // independent from port_1
        assert_eq!(sequencer.assign_seq(&port_1).seq, 3);
        assert_eq!(sequencer.assign_seq(&port_3).seq, 2);
    }

    #[test]
    fn test_sequencer_mixed_actor_and_non_actor_ports() {
        let sequencer = Sequencer {
            session_id: Uuid::now_v7(),
            last_seqs: Arc::new(Mutex::new(HashMap::new())),
        };

        let actor_id = test_actor_id("worker_0", "worker");

        // Actor ports (share sequence per actor)
        let actor_port_1 = actor_id.port_id(TestMsg1::port());
        let actor_port_2 = actor_id.port_id(TestMsg2::port());

        // Non-actor ports (independent sequences per port)
        let non_actor_port_1 = actor_id.port_id(1);
        let non_actor_port_2 = actor_id.port_id(2);

        // Interleave sends to all port types
        assert_eq!(sequencer.assign_seq(&actor_port_1).seq, 1);
        assert_eq!(sequencer.assign_seq(&non_actor_port_1).seq, 1); // independent
        assert_eq!(sequencer.assign_seq(&actor_port_2).seq, 2); // continues actor sequence
        assert_eq!(sequencer.assign_seq(&non_actor_port_2).seq, 1); // independent
        assert_eq!(sequencer.assign_seq(&non_actor_port_1).seq, 2); // continues its own
        assert_eq!(sequencer.assign_seq(&actor_port_1).seq, 3); // continues actor sequence
        assert_eq!(sequencer.assign_seq(&non_actor_port_2).seq, 2); // continues its own
    }
}
