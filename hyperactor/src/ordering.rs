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

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;

use crate::dashmap::DashMap;

/// A client's re-ordering buffer state.
struct BufferState<T> {
    /// the last sequence number sent to receiver for this client. seq starts
    /// with 1 and 0 mean no message has been sent.
    last_seq: usize,
    /// Buffer out-of-order messages in order to ensures messages are delivered
    /// strictly in per-client sequence order.
    ///
    /// Map's key is seq_no, value is msg.
    buffer: HashMap<usize, T>,
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
    // map's key is name client which sens messages through this channel. Map's
    // value is the buffer state of that client.
    states: Arc<DashMap<String, Arc<Mutex<BufferState<T>>>>>,
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
        client: String,
        seq_no: usize,
        msg: T,
    ) -> Result<(), OrderedSenderError<T>> {
        use std::cmp::Ordering;

        assert!(self.enable_buffering);
        if seq_no == 0 {
            return Err(OrderedSenderError::InvalidZeroSeq(msg));
        }

        // Make sure only this client's state is locked, not all states.
        let state = match self.states.get(&client) {
            Some(state) => state.value().clone(),
            None => self
                .states
                .entry(client.clone())
                .or_default()
                .value()
                .clone(),
        };
        let mut state_guard = state.lock().unwrap();
        let BufferState { last_seq, buffer } = state_guard.deref_mut();

        match seq_no.cmp(&(*last_seq + 1)) {
            Ordering::Less => {
                tracing::warn!(
                    "{} duplicate message from {} with seq no: {}",
                    self.log_id,
                    client,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn drain_try_recv<T: std::fmt::Debug + Clone>(rx: &mut mpsc::UnboundedReceiver<T>) -> Vec<T> {
        let mut out = Vec::new();
        while let Ok(m) = rx.try_recv() {
            out.push(m);
        }
        out
    }

    #[test]
    fn test_ordered_channel_single_client_send_in_order() {
        let (tx, mut rx) = ordered_channel::<usize>("test".to_string(), true);
        for s in 1..=10 {
            tx.send("A".into(), s, s).unwrap();
            let got = drain_try_recv(&mut rx);
            assert_eq!(got, vec![s]);
        }
    }

    #[test]
    fn test_ordered_channel_single_client_send_out_of_order() {
        let (tx, mut rx) = ordered_channel::<usize>("test".to_string(), true);

        // Send 2 to 4 in descending order: all should buffer until 1 arrives.
        for s in (2..=4).rev() {
            tx.send("A".into(), s, s).unwrap();
        }

        // Send 7 to 9 in descending order: all should buffer until 1 - 6 arrives.
        for s in (7..=9).rev() {
            tx.send("A".into(), s, s).unwrap();
        }

        assert!(
            drain_try_recv(&mut rx).is_empty(),
            "nothing should be delivered yet"
        );

        // Now send 1: should deliver 1 then flush 2 - 4.
        tx.send("A".into(), 1, 1).unwrap();
        assert_eq!(drain_try_recv(&mut rx), vec![1, 2, 3, 4]);

        // Now send 5: should deliver immediately but not flush 7 - 9.
        tx.send("A".into(), 5, 5).unwrap();
        assert_eq!(drain_try_recv(&mut rx), vec![5]);

        // Now send 6: should deliver 6 then flush 7 - 9.
        tx.send("A".into(), 6, 6).unwrap();
        assert_eq!(drain_try_recv(&mut rx), vec![6, 7, 8, 9]);

        // Send 10: should deliver immediately.
        tx.send("A".into(), 10, 10).unwrap();
        let got = drain_try_recv(&mut rx);
        assert_eq!(got, vec![10]);
    }

    #[test]
    fn test_ordered_channel_multi_clients() {
        let (tx, mut rx) = ordered_channel::<(String, usize)>("test".to_string(), true);

        // A1 -> deliver
        tx.send("A".into(), 1, ("A".into(), 1)).unwrap();
        assert_eq!(drain_try_recv(&mut rx), vec![("A".into(), 1)]);
        // B1 -> deliver
        tx.send("B".into(), 1, ("B".into(), 1)).unwrap();
        assert_eq!(drain_try_recv(&mut rx), vec![("B".into(), 1)]);
        for s in (3..=5).rev() {
            // A3-5 -> buffer (waiting for A2)
            tx.send("A".into(), s, ("A".into(), s)).unwrap();
            // B3-5 -> buffer (waiting for B2)
            tx.send("B".into(), s, ("B".into(), s)).unwrap();
        }
        for s in (7..=9).rev() {
            // A7-9 -> buffer (waiting for A1-6)
            tx.send("A".into(), s, ("A".into(), s)).unwrap();
            // B7-9 -> buffer (waiting for B1-6)
            tx.send("B".into(), s, ("B".into(), s)).unwrap();
        }
        assert!(
            drain_try_recv(&mut rx).is_empty(),
            "nothing should be delivered yet"
        );

        // A2 -> deliver A2 then flush A3
        tx.send("A".into(), 2, ("A".into(), 2)).unwrap();
        assert_eq!(
            drain_try_recv(&mut rx),
            vec![
                ("A".into(), 2),
                ("A".into(), 3),
                ("A".into(), 4),
                ("A".into(), 5),
            ]
        );
        // B2 -> deliver B2 then flush B3
        tx.send("B".into(), 2, ("B".into(), 2)).unwrap();
        assert_eq!(
            drain_try_recv(&mut rx),
            vec![
                ("B".into(), 2),
                ("B".into(), 3),
                ("B".into(), 4),
                ("B".into(), 5),
            ]
        );

        // A6 -> should deliver immediately and flush A7-9
        tx.send("A".into(), 6, ("A".into(), 6)).unwrap();
        assert_eq!(
            drain_try_recv(&mut rx),
            vec![
                ("A".into(), 6),
                ("A".into(), 7),
                ("A".into(), 8),
                ("A".into(), 9)
            ]
        );
        // B6 -> should deliver immediately and flush B7-9
        tx.send("B".into(), 6, ("B".into(), 6)).unwrap();
        assert_eq!(
            drain_try_recv(&mut rx),
            vec![
                ("B".into(), 6),
                ("B".into(), 7),
                ("B".into(), 8),
                ("B".into(), 9)
            ]
        );
    }

    #[test]
    fn test_ordered_channel_duplicates() {
        fn verify_empty_buffers<T>(states: &DashMap<String, Arc<Mutex<BufferState<T>>>>) {
            for entry in states.iter() {
                assert!(entry.value().lock().unwrap().buffer.is_empty());
            }
        }

        let (tx, mut rx) = ordered_channel::<(String, usize)>("test".to_string(), true);
        // A1 -> deliver
        tx.send("A".into(), 1, ("A".into(), 1)).unwrap();
        assert_eq!(drain_try_recv(&mut rx), vec![("A".into(), 1)]);
        verify_empty_buffers(&tx.states);
        // duplicate A1 -> drop even if the message is different.
        tx.send("A".into(), 1, ("A".into(), 1_000)).unwrap();
        assert!(
            drain_try_recv(&mut rx).is_empty(),
            "nothing should be delivered yet"
        );
        verify_empty_buffers(&tx.states);
        // A2 -> deliver
        tx.send("A".into(), 2, ("A".into(), 2)).unwrap();
        assert_eq!(drain_try_recv(&mut rx), vec![("A".into(), 2)]);
        verify_empty_buffers(&tx.states);
        // late A1 duplicate -> drop
        tx.send("A".into(), 1, ("A".into(), 1_001)).unwrap();
        assert!(
            drain_try_recv(&mut rx).is_empty(),
            "nothing should be delivered yet"
        );
        verify_empty_buffers(&tx.states);
    }
}
