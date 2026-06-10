/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Receiver-local sequencing for mailbox delivery.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::collections::btree_map;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::TryLockError;

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use uuid::Uuid;

use crate::ActorAddr;
use crate::ordering::OrderingSessionSnapshot;
use crate::ordering::OrderingSnapshot;
use crate::ordering::SeqInfo;

const RING_BUFFER_LIMIT: usize = 32;

/// A message that carries sequencing metadata.
pub(crate) trait Sequenced {
    /// The message delivered after sequencing.
    type Message;

    /// The sequencing metadata for this message.
    fn seq_info(&self) -> SeqInfo;

    /// The sender actor, if known.
    fn sender(&self) -> Option<ActorAddr> {
        None
    }

    /// Convert this item into the delivered message.
    fn into_message(self) -> Self::Message;
}

/// A message paired with the sequencing metadata extracted from its mailbox
/// headers.
#[derive(Debug)]
pub(crate) struct SequencedEnvelope<M> {
    seq_info: SeqInfo,
    sender: Option<ActorAddr>,
    message: M,
}

impl<M> SequencedEnvelope<M> {
    /// Create a sequenced envelope.
    pub(crate) fn new(seq_info: SeqInfo, sender: Option<ActorAddr>, message: M) -> Self {
        Self {
            seq_info,
            sender,
            message,
        }
    }
}

impl<M> Sequenced for SequencedEnvelope<M> {
    type Message = M;

    fn seq_info(&self) -> SeqInfo {
        self.seq_info.clone()
    }

    fn sender(&self) -> Option<ActorAddr> {
        self.sender.clone()
    }

    fn into_message(self) -> Self::Message {
        self.message
    }
}

/// A message that bypasses sequencing.
#[derive(Debug)]
pub(crate) struct DirectEnvelope<M> {
    message: M,
}

impl<M> DirectEnvelope<M> {
    /// Create a direct envelope.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "direct envelopes are staged for follow-up control-port migrations"
        )
    )]
    pub(crate) fn new(message: M) -> Self {
        Self { message }
    }
}

impl<M> Sequenced for DirectEnvelope<M> {
    type Message = M;

    fn seq_info(&self) -> SeqInfo {
        SeqInfo::Direct
    }

    fn into_message(self) -> Self::Message {
        self.message
    }
}

/// Open an unbounded channel whose receiver reorders session-sequenced
/// messages.
pub(crate) fn sequenced_unbounded<M: Sequenced>() -> (mpsc::UnboundedSender<M>, SequencedReceiver<M>)
{
    sequenced_unbounded_with_buffering(true)
}

/// Open an unbounded channel with explicitly configured sequencing.
pub(crate) fn sequenced_unbounded_with_buffering<M: Sequenced>(
    enable_buffering: bool,
) -> (mpsc::UnboundedSender<M>, SequencedReceiver<M>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (tx, SequencedReceiver::new(rx, enable_buffering))
}

/// Out-of-band snapshot handle for a receiver-local sequencing domain.
#[derive(Debug)]
pub(crate) struct SequencedSnapshot<M> {
    enable_buffering: bool,
    state: Arc<Mutex<SequencedState<M>>>,
}

impl<M> Clone for SequencedSnapshot<M> {
    fn clone(&self) -> Self {
        Self {
            enable_buffering: self.enable_buffering,
            state: self.state.clone(),
        }
    }
}

impl<M> SequencedSnapshot<M> {
    /// Snapshot the currently buffered sequencing state.
    pub(crate) fn snapshot(&self) -> OrderingSnapshot {
        if !self.enable_buffering {
            return OrderingSnapshot {
                enabled: false,
                sessions: Vec::new(),
                skipped_session_count: 0,
            };
        }

        let state = match self.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::WouldBlock) => {
                return OrderingSnapshot {
                    enabled: true,
                    sessions: Vec::new(),
                    skipped_session_count: 1,
                };
            }
            Err(TryLockError::Poisoned(err)) => err.into_inner(),
        };

        let mut sessions: Vec<_> = state
            .senders
            .iter()
            .map(|(session_id, sequencer)| {
                let (buffered_count, oldest_buffered_seq, newest_buffered_seq) =
                    sequencer.buffer.snapshot(sequencer.seq);
                OrderingSessionSnapshot {
                    session_id: *session_id,
                    sender: sequencer.sender.clone(),
                    last_released_seq: sequencer.seq.saturating_sub(1),
                    expected_next_seq: sequencer.seq,
                    buffered_count,
                    oldest_buffered_seq,
                    newest_buffered_seq,
                }
            })
            .collect();
        sessions.sort_by_key(|session| session.session_id);

        OrderingSnapshot {
            enabled: true,
            sessions,
            skipped_session_count: 0,
        }
    }
}

/// A receiver that owns all reorder state for one sequencing domain.
#[derive(Debug)]
pub(crate) struct SequencedReceiver<M: Sequenced> {
    rx: mpsc::UnboundedReceiver<M>,
    enable_buffering: bool,
    state: Arc<Mutex<SequencedState<M::Message>>>,
    ready: VecDeque<M::Message>,
}

impl<M: Sequenced> SequencedReceiver<M> {
    fn new(rx: mpsc::UnboundedReceiver<M>, enable_buffering: bool) -> Self {
        Self {
            rx,
            enable_buffering,
            state: Arc::new(Mutex::new(SequencedState::default())),
            ready: VecDeque::new(),
        }
    }

    /// Return a cloneable diagnostic handle for this receiver.
    pub(crate) fn snapshot_handle(&self) -> SequencedSnapshot<M::Message> {
        SequencedSnapshot {
            enable_buffering: self.enable_buffering,
            state: self.state.clone(),
        }
    }

    /// Receive the next deliverable message.
    pub(crate) async fn recv(&mut self) -> Option<M::Message> {
        loop {
            if let Some(message) = self.ready.pop_front() {
                return Some(message);
            }

            let item = self.rx.recv().await?;
            if let Some(message) = self.admit(item) {
                return Some(message);
            }
        }
    }

    /// Try to receive the next deliverable message without waiting.
    pub(crate) fn try_recv(&mut self) -> Result<M::Message, TryRecvError> {
        loop {
            if let Some(message) = self.ready.pop_front() {
                return Ok(message);
            }

            let item = self.rx.try_recv()?;
            if let Some(message) = self.admit(item) {
                return Ok(message);
            }
        }
    }

    fn admit(&mut self, item: M) -> Option<M::Message> {
        if !self.enable_buffering {
            return Some(item.into_message());
        }

        match item.seq_info() {
            SeqInfo::Direct => Some(item.into_message()),
            SeqInfo::Session { session_id, seq } => {
                // TODO: allow sequence numbers to start at 0
                assert!(seq > 0, "sequence number must be nonzero");
                let mut state = self.state.lock().unwrap();
                let sequencer = state.senders.entry(session_id).or_default();
                if sequencer.sender.is_none()
                    && let Some(sender) = item.sender()
                {
                    sequencer.sender = Some(sender);
                }
                sequencer.admit(seq, item.into_message(), &mut self.ready)
            }
        }
    }
}

#[derive(Debug)]
struct SequencedState<M> {
    senders: HashMap<Uuid, Sequencer<M>>,
}

impl<M> Default for SequencedState<M> {
    fn default() -> Self {
        Self {
            senders: HashMap::new(),
        }
    }
}

#[derive(Debug)]
struct Sequencer<M> {
    seq: u64,
    sender: Option<ActorAddr>,
    buffer: Buffer<M>,
}

impl<M> Default for Sequencer<M> {
    fn default() -> Self {
        Self {
            seq: 1,
            sender: None,
            buffer: Buffer::default(),
        }
    }
}

impl<M> Sequencer<M> {
    fn admit(&mut self, seq: u64, message: M, ready: &mut VecDeque<M>) -> Option<M> {
        if seq < self.seq {
            tracing::warn!(
                expected_seq = self.seq,
                actual_seq = seq,
                "dropping stale sequenced message"
            );
            return None;
        }

        if seq > self.seq {
            self.buffer.insert(self.seq, seq, message);
            return None;
        }

        self.seq += 1;
        self.buffer.advance(self.seq);
        self.drain_ready(ready);
        Some(message)
    }

    fn drain_ready(&mut self, ready: &mut VecDeque<M>) {
        while let Some(message) = self.buffer.take_current(self.seq) {
            ready.push_back(message);
            self.seq += 1;
            self.buffer.advance(self.seq);
        }
    }
}

#[derive(Debug)]
struct Buffer<M> {
    /// Dense storage for near-future sequence numbers. An empty ring means the
    /// sequencer is in the steady state and has no buffered out-of-order
    /// messages.
    ring: Vec<Option<M>>,
    /// Index in `ring` that corresponds to the sequencer's current expected
    /// sequence number.
    head: usize,
    /// Sparse storage for sequence numbers that are outside the current ring
    /// window.
    spillover: Option<BTreeMap<u64, M>>,
}

impl<M> Default for Buffer<M> {
    fn default() -> Self {
        Self {
            ring: Vec::new(),
            head: 0,
            spillover: None,
        }
    }
}

impl<M> Buffer<M> {
    fn insert(&mut self, next_seq: u64, seq: u64, message: M) {
        assert!(seq > next_seq, "cannot buffer non-future sequence number");

        self.ensure_ring(next_seq, seq);
        if self.in_ring_window(next_seq, seq) {
            self.insert_ring(next_seq, seq, message);
        } else {
            match self.spillover.get_or_insert_with(BTreeMap::new).entry(seq) {
                btree_map::Entry::Vacant(entry) => {
                    entry.insert(message);
                }
                btree_map::Entry::Occupied(_) => {
                    tracing::warn!(seq, "dropping duplicate buffered sequenced message");
                }
            }
        }
    }

    fn advance(&mut self, next_seq: u64) {
        if self.ring.is_empty() {
            return;
        }

        self.ring[self.head] = None;
        self.head = (self.head + 1) % self.ring.len();
        self.refill_from_spillover(next_seq);
        self.reset_if_empty();
    }

    fn take_current(&mut self, next_seq: u64) -> Option<M> {
        if self.ring.is_empty() {
            return None;
        }
        self.refill_from_spillover(next_seq);
        self.ring[self.head].take()
    }

    fn ensure_ring(&mut self, next_seq: u64, seq: u64) {
        let gap = seq - next_seq;
        let wanted_len = if gap < RING_BUFFER_LIMIT as u64 {
            gap as usize + 1
        } else {
            RING_BUFFER_LIMIT
        };

        if self.ring.is_empty() {
            self.ring.resize_with(wanted_len, || None);
            self.head = 0;
        } else if self.ring.len() < wanted_len {
            self.grow_ring(wanted_len);
        }
    }

    fn grow_ring(&mut self, new_len: usize) {
        assert!(
            new_len <= RING_BUFFER_LIMIT,
            "ring buffer cannot exceed limit"
        );
        assert!(new_len > self.ring.len(), "ring buffer cannot shrink");

        let mut new_ring = Vec::new();
        new_ring.resize_with(new_len, || None);
        for (offset, slot) in new_ring.iter_mut().enumerate().take(self.ring.len()) {
            let old_index = (self.head + offset) % self.ring.len();
            *slot = self.ring[old_index].take();
        }
        self.ring = new_ring;
        self.head = 0;
    }

    fn in_ring_window(&self, next_seq: u64, seq: u64) -> bool {
        !self.ring.is_empty() && seq < next_seq + self.ring.len() as u64
    }

    fn insert_ring(&mut self, next_seq: u64, seq: u64, message: M) {
        assert!(
            self.in_ring_window(next_seq, seq),
            "sequence number outside ring window"
        );
        let offset = (seq - next_seq) as usize;
        let index = (self.head + offset) % self.ring.len();
        if self.ring[index].is_some() {
            tracing::warn!(seq, "dropping duplicate buffered sequenced message");
            return;
        }
        self.ring[index] = Some(message);
    }

    fn refill_from_spillover(&mut self, next_seq: u64) {
        if self.ring.is_empty() {
            return;
        }

        while let Some((&seq, _)) = self
            .spillover
            .as_ref()
            .and_then(|spillover| spillover.first_key_value())
        {
            if !self.in_ring_window(next_seq, seq) {
                break;
            }

            let message = self
                .spillover
                .as_mut()
                .expect("spillover should be present while first_key_value returned a sequence")
                .pop_first()
                .expect("spillover should be non-empty while first_key_value returned a sequence")
                .1;
            self.insert_ring(next_seq, seq, message);
        }

        if self.spillover.as_ref().is_some_and(BTreeMap::is_empty) {
            self.spillover = None;
        }
    }

    fn reset_if_empty(&mut self) {
        if self.spillover.is_none() && self.ring.iter().all(Option::is_none) {
            self.ring.clear();
            self.ring.shrink_to(0);
            self.head = 0;
        }
    }

    fn snapshot(&self, next_seq: u64) -> (usize, Option<u64>, Option<u64>) {
        let mut count = 0;
        let mut oldest = None;
        let mut newest = None;

        for offset in 0..self.ring.len() {
            let index = (self.head + offset) % self.ring.len();
            if self.ring[index].is_some() {
                let seq = next_seq + offset as u64;
                count += 1;
                oldest = Some(oldest.map_or(seq, |oldest: u64| oldest.min(seq)));
                newest = Some(newest.map_or(seq, |newest: u64| newest.max(seq)));
            }
        }

        if let Some(spillover) = &self.spillover {
            count += spillover.len();
            if let Some((&seq, _)) = spillover.first_key_value() {
                oldest = Some(oldest.map_or(seq, |oldest| oldest.min(seq)));
            }
            if let Some((&seq, _)) = spillover.last_key_value() {
                newest = Some(newest.map_or(seq, |newest| newest.max(seq)));
            }
        }

        (count, oldest, newest)
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc::error::TryRecvError;
    use uuid::Uuid;

    use super::*;
    use crate::testing::ids::test_actor_id;

    fn session(seq: u64) -> SeqInfo {
        SeqInfo::Session {
            session_id: Uuid::from_u128(1),
            seq,
        }
    }

    fn session_for(session_id: u128, seq: u64) -> SeqInfo {
        SeqInfo::Session {
            session_id: Uuid::from_u128(session_id),
            seq,
        }
    }

    fn envelope(seq_info: SeqInfo, message: u64) -> SequencedEnvelope<u64> {
        SequencedEnvelope::new(seq_info, None, message)
    }

    fn envelope_from(
        seq_info: SeqInfo,
        sender: Option<ActorAddr>,
        message: u64,
    ) -> SequencedEnvelope<u64> {
        SequencedEnvelope::new(seq_info, sender, message)
    }

    fn with_sequencer<T, R>(
        rx: &SequencedReceiver<SequencedEnvelope<T>>,
        session_id: u128,
        f: impl FnOnce(&Sequencer<T>) -> R,
    ) -> R {
        let state = rx.state.lock().unwrap();
        f(state.senders.get(&Uuid::from_u128(session_id)).unwrap())
    }

    #[tokio::test]
    async fn delivers_in_order_messages() {
        let (tx, mut rx) = sequenced_unbounded();

        tx.send(envelope(session(1), 10)).unwrap();
        tx.send(envelope(session(2), 20)).unwrap();

        assert_eq!(rx.recv().await, Some(10));
        assert_eq!(rx.recv().await, Some(20));

        with_sequencer(&rx, 1, |sequencer| {
            assert!(sequencer.buffer.ring.is_empty());
            assert_eq!(sequencer.buffer.ring.capacity(), 0);
            assert!(sequencer.buffer.spillover.is_none());
        });
    }

    #[test]
    fn buffers_small_gap_in_ring() {
        let (tx, mut rx) = sequenced_unbounded();

        tx.send(envelope(session(3), 30)).unwrap();
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        with_sequencer(&rx, 1, |sequencer| {
            assert_eq!(sequencer.buffer.ring.len(), 3);
            assert!(sequencer.buffer.spillover.is_none());
        });

        tx.send(envelope(session(1), 10)).unwrap();
        tx.send(envelope(session(2), 20)).unwrap();

        assert_eq!(rx.try_recv().unwrap(), 10);
        assert_eq!(rx.try_recv().unwrap(), 20);
        assert_eq!(rx.try_recv().unwrap(), 30);
    }

    #[test]
    fn stores_large_gap_in_spillover() {
        let (tx, mut rx) = sequenced_unbounded();

        tx.send(envelope(session(100), 1000)).unwrap();
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        with_sequencer(&rx, 1, |sequencer| {
            assert_eq!(sequencer.buffer.ring.len(), RING_BUFFER_LIMIT);
            assert!(
                sequencer
                    .buffer
                    .spillover
                    .as_ref()
                    .is_some_and(|spillover| spillover.contains_key(&100))
            );
        });
    }

    #[test]
    fn drains_spillover_when_hole_is_filled() {
        let (tx, mut rx) = sequenced_unbounded();

        tx.send(envelope(session(100), 1000)).unwrap();
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        for seq in 1..100 {
            tx.send(envelope(session(seq), seq)).unwrap();
        }

        for seq in 1..=100 {
            let expected = if seq == 100 { 1000 } else { seq };
            assert_eq!(rx.try_recv().unwrap(), expected);
        }
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        with_sequencer(&rx, 1, |sequencer| {
            assert!(sequencer.buffer.ring.is_empty());
            assert_eq!(sequencer.buffer.ring.capacity(), 0);
            assert!(sequencer.buffer.spillover.is_none());
        });
    }

    #[test]
    fn drops_stale_duplicates() {
        let (tx, mut rx) = sequenced_unbounded();

        tx.send(envelope(session(1), 10)).unwrap();
        tx.send(envelope(session(1), 11)).unwrap();
        tx.send(envelope(session(2), 20)).unwrap();

        assert_eq!(rx.try_recv().unwrap(), 10);
        assert_eq!(rx.try_recv().unwrap(), 20);
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn drops_duplicate_buffered_ring_message() {
        let (tx, mut rx) = sequenced_unbounded();

        tx.send(envelope(session(2), 20)).unwrap();
        tx.send(envelope(session(2), 21)).unwrap();
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        tx.send(envelope(session(1), 10)).unwrap();

        assert_eq!(rx.try_recv().unwrap(), 10);
        assert_eq!(rx.try_recv().unwrap(), 20);
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn drops_duplicate_buffered_spillover_message() {
        let (tx, mut rx) = sequenced_unbounded();

        tx.send(envelope(session(100), 1000)).unwrap();
        tx.send(envelope(session(100), 1001)).unwrap();
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        for seq in 1..100 {
            tx.send(envelope(session(seq), seq)).unwrap();
        }

        for seq in 1..=100 {
            let expected = if seq == 100 { 1000 } else { seq };
            assert_eq!(rx.try_recv().unwrap(), expected);
        }
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn sender_sessions_do_not_block_each_other() {
        let (tx, mut rx) = sequenced_unbounded();

        tx.send(envelope(session_for(1, 2), 12)).unwrap();
        tx.send(envelope(session_for(2, 1), 21)).unwrap();
        tx.send(envelope(session_for(1, 1), 11)).unwrap();

        assert_eq!(rx.try_recv().unwrap(), 21);
        assert_eq!(rx.try_recv().unwrap(), 11);
        assert_eq!(rx.try_recv().unwrap(), 12);
    }

    #[test]
    fn direct_messages_bypass_ordering() {
        let (tx, mut rx) = sequenced_unbounded();

        tx.send(envelope(session(2), 20)).unwrap();
        tx.send(envelope(SeqInfo::Direct, 99)).unwrap();

        assert_eq!(rx.try_recv().unwrap(), 99);
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        tx.send(envelope(session(1), 10)).unwrap();

        assert_eq!(rx.try_recv().unwrap(), 10);
        assert_eq!(rx.try_recv().unwrap(), 20);
    }

    #[test]
    fn direct_envelope_uses_same_channel_primitive() {
        let (tx, mut rx) = sequenced_unbounded();

        tx.send(DirectEnvelope::new(10)).unwrap();
        tx.send(DirectEnvelope::new(20)).unwrap();

        assert_eq!(rx.try_recv().unwrap(), 10);
        assert_eq!(rx.try_recv().unwrap(), 20);
    }

    #[test]
    fn snapshot_empty() {
        let (_tx, rx) = sequenced_unbounded::<SequencedEnvelope<u64>>();
        let snapshot = rx.snapshot_handle().snapshot();

        assert!(snapshot.enabled);
        assert!(snapshot.sessions.is_empty());
        assert_eq!(snapshot.skipped_session_count, 0);
        assert!(snapshot.is_complete());
    }

    #[test]
    fn snapshot_out_of_order_includes_sender() {
        let (tx, mut rx) = sequenced_unbounded();
        let sender = test_actor_id("sender", "client");

        tx.send(envelope_from(session(3), Some(sender.clone()), 30))
            .unwrap();
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        let snapshot = rx.snapshot_handle().snapshot();
        assert_eq!(snapshot.sessions.len(), 1);
        let session = &snapshot.sessions[0];
        assert_eq!(session.sender, Some(sender));
        assert_eq!(session.last_released_seq, 0);
        assert_eq!(session.expected_next_seq, 1);
        assert_eq!(session.buffered_count, 1);
        assert_eq!(session.oldest_buffered_seq, Some(3));
        assert_eq!(session.newest_buffered_seq, Some(3));
    }

    #[test]
    fn snapshot_sorts_sessions() {
        let (tx, mut rx) = sequenced_unbounded();
        let session_lo = Uuid::from_u128(1);
        let session_hi = Uuid::from_u128(2);

        tx.send(envelope(
            SeqInfo::Session {
                session_id: session_hi,
                seq: 2,
            },
            20,
        ))
        .unwrap();
        tx.send(envelope(
            SeqInfo::Session {
                session_id: session_lo,
                seq: 2,
            },
            10,
        ))
        .unwrap();
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        let snapshot = rx.snapshot_handle().snapshot();
        assert_eq!(snapshot.sessions.len(), 2);
        assert_eq!(snapshot.sessions[0].session_id, session_lo);
        assert_eq!(snapshot.sessions[1].session_id, session_hi);
    }

    #[test]
    fn disabled_buffering_delivers_without_snapshot_state() {
        let (tx, mut rx) = sequenced_unbounded_with_buffering(false);

        tx.send(envelope(session(2), 20)).unwrap();
        assert_eq!(rx.try_recv().unwrap(), 20);

        let snapshot = rx.snapshot_handle().snapshot();
        assert!(!snapshot.enabled);
        assert!(snapshot.sessions.is_empty());
    }
}
