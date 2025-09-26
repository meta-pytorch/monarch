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
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use uuid::Uuid;

use crate::ActorId;

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

/// Used by sender to track the message sequence numbers it sends to each actor.
/// Each [Sequencers] object has a session id, sequencer numbers are scoped by
/// the (session_id, destination_actor) pair.
#[derive(Clone, Debug)]
pub(crate) struct Sequencer {
    session_id: Uuid,
    // map's key is the destination actor's name, value is the last seq number
    // sent to that actor.
    last_seqs: Arc<Mutex<HashMap<ActorId, u64>>>,
}

impl Sequencer {
    pub(crate) fn new(session_id: Uuid) -> Self {
        Self {
            session_id,
            last_seqs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Lock this sequencer, so the seq assignment can be serialized.
    ///
    /// Lock behavior:
    /// Only one caller can hold the lock. If the lock is already hold by a
    /// different caller, this function will block the local thread until that
    /// caller releases its lock.
    ///
    /// Note:
    /// Since sequencer is shared by the root mesh, its sliced meshes, and its
    /// actors, this lock's granularity is coarse grained because, e.g., two
    /// sliced meshes might not have overlapping ranks, and we technically can
    /// assign seqs for them concurrently. But we assume a client typically do
    /// not cast to non-overlapping meshes/actors concurrently, so in practice
    /// the mesh-level granularity is acceptable.
    pub fn lock<'a>(&'a self) -> SequencerLock<'a> {
        SequencerLock {
            session_id: self.session_id,
            guard: self.last_seqs.lock().unwrap(),
        }
    }
}

/// Lock of a sequencer. Only holder of this lock can read or mutate the
/// sequencer.
pub struct SequencerLock<'a> {
    session_id: Uuid,
    guard: MutexGuard<'a, HashMap<ActorId, u64>>,
}

impl<'a> SequencerLock<'a> {
    /// Return the next seq for the given actor ID. This method alone does not
    /// mutate sequencer. The caller needs to explicitly call [SequencerLock::incr]
    /// for that.
    pub fn next_seq(&self, actor_id: &ActorId) -> u64 {
        *self.guard.get(actor_id).unwrap_or(&0) + 1
    }

    /// Mutate sequencer by incrementing the given actor ID's last-sent seq by 1.
    pub fn incr(&mut self, actor_id: &ActorId) {
        let mut_ref = match self.guard.get_mut(actor_id) {
            Some(m) => m,
            None => self.guard.entry(actor_id.clone()).or_default(),
        };
        *mut_ref += 1;
    }

    /// Id of the session this sequencer belongs to.
    pub fn session_id(&self) -> Uuid {
        self.session_id
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::thread;
    use std::time::Duration;

    use super::*;
    use crate::id;

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

        let actor_id = id!(test[0].test);

        // Modify original sequencer
        {
            let mut lock = sequencer.lock();
            lock.incr(&actor_id);
            lock.incr(&actor_id);
        }

        // Clone should share the same state
        let cloned_sequencer = sequencer.clone();

        let lock = cloned_sequencer.lock();
        assert_eq!(lock.next_seq(&actor_id), 3);
    }

    #[test]
    fn test_sequencer_lock_seq_single_actor() {
        let sequencer = Sequencer {
            session_id: Uuid::now_v7(),
            last_seqs: Arc::new(Mutex::new(HashMap::new())),
        };

        let actor_id = id!(worker[0].worker);
        let mut lock = sequencer.lock();

        // First call should return 1 (0 + 1)
        assert_eq!(lock.next_seq(&actor_id), 1);

        // Should still return 1 since we haven't incremented
        assert_eq!(lock.next_seq(&actor_id), 1);

        // Increment and check next_seq is now 2
        lock.incr(&actor_id);
        assert_eq!(lock.next_seq(&actor_id), 2);

        // Increment again and check next_seq is now 3
        lock.incr(&actor_id);
        assert_eq!(lock.next_seq(&actor_id), 3);
    }

    #[test]
    fn test_sequencer_lock_multiple_actors() {
        let sequencer = Sequencer {
            session_id: Uuid::now_v7(),
            last_seqs: Arc::new(Mutex::new(HashMap::new())),
        };

        let actor_id_0 = id!(worker[0].worker);
        let actor_id_1 = id!(worker[1].worker);
        let mut lock = sequencer.lock();

        // Both actors should start with next_seq = 1
        assert_eq!(lock.next_seq(&actor_id_0), 1);
        assert_eq!(lock.next_seq(&actor_id_1), 1);

        // Increment actor_0 twice
        lock.incr(&actor_id_0);
        lock.incr(&actor_id_0);

        // Increment actor_1 once
        lock.incr(&actor_id_1);

        // Check independent sequences
        assert_eq!(lock.next_seq(&actor_id_0), 3);
        assert_eq!(lock.next_seq(&actor_id_1), 2);
    }

    #[test]
    fn test_sequencer_exclusive_lock_concurrency() {
        let sequencer = Arc::new(Sequencer {
            session_id: Uuid::now_v7(),
            last_seqs: Arc::new(Mutex::new(HashMap::new())),
        });
        let sequencer_clone = sequencer.clone();

        // lock it in the current thread
        let lock = sequencer.lock();

        let before_lock = Arc::new(AtomicBool::new(false));
        let after_lock = Arc::new(AtomicBool::new(false));
        // Try to lock it in a different thread
        let before_lock_clone = before_lock.clone();
        let after_lock_clone = after_lock.clone();
        let waiter_thread = thread::spawn(move || {
            before_lock_clone.store(true, Ordering::SeqCst);
            // This should block until holder releases the lock
            let _lock = sequencer_clone.lock();
            after_lock_clone.store(true, Ordering::SeqCst);
        });

        // Wait for the other thread to attempt to lock
        #[allow(clippy::disallowed_methods)]
        std::thread::sleep(Duration::from_secs(5));
        assert!(before_lock.load(Ordering::SeqCst));
        assert!(!after_lock.load(Ordering::SeqCst));

        // Release the lock
        drop(lock);

        // Wait for thread to finish
        waiter_thread.join().unwrap();

        assert!(after_lock.load(Ordering::SeqCst));
    }

    #[test]
    fn test_sequencer_lock_sequence_multiple_times() {
        let sequencer = Sequencer {
            session_id: Uuid::now_v7(),
            last_seqs: Arc::new(Mutex::new(HashMap::new())),
        };

        let actor_id = id!(compute[0].compute);

        // Test sequence: get next, increment, get next again
        {
            let mut lock = sequencer.lock();
            assert_eq!(lock.next_seq(&actor_id), 1);
            lock.incr(&actor_id);
        }

        // Lock should be released, acquire again
        {
            let mut lock = sequencer.lock();
            assert_eq!(lock.next_seq(&actor_id), 2);
            lock.incr(&actor_id);
            assert_eq!(lock.next_seq(&actor_id), 3);
        }
    }
}
