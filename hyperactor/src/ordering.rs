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
use std::sync::Arc;
use std::sync::Mutex;

use hyperactor_config::AttrValue;
use hyperactor_config::attrs::declare_attrs;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;
use uuid::Uuid;

use crate::ActorAddr;
use crate::PortAddr;

// Control ports are delivered through runtime-owned channels rather than the
// actor's work queue, so they must not share the per-actor handler-port
// sequence counter.

/// Returns true if `id` is the `TypeId` of a bypass-channel message type
/// (i.e. one that must not be wired through `Ports::get`).
pub(crate) fn is_bypass_workq_type_id(id: TypeId) -> bool {
    id == TypeId::of::<crate::introspect::IntrospectMessage>()
        || id == TypeId::of::<crate::proc::StatusMessage>()
}

/// Per-session receiver-local ordering snapshot. This does not lock the
/// work queue or hold sequencing state across awaits.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Named, AttrValue)]
pub struct OrderingSessionSnapshot {
    /// Identifier of the session whose ordering state this snapshot
    /// describes; matches `SeqInfo::Session { session_id, .. }` on the
    /// wire.
    pub session_id: Uuid,

    /// Sender `ActorAddr` captured by the first-non-None-wins
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

/// Diagnostic projection of receiver-local ordering state.
///
/// Each `OrderingSessionSnapshot` corresponds to one sender session keyed
/// by `SeqInfo::Session.session_id`. Sequence progress here means
/// "released into the actor work queue", not "processed by the actor handler".
///
/// Carries completeness metadata so callers can distinguish "no stalled
/// sessions" from "snapshot was partial" from "buffering is disabled".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Named, AttrValue)]
pub struct OrderingSnapshot {
    /// Whether reorder buffering is enabled. When `false`, messages
    /// bypass sequencing, and `sessions` is empty even under load.
    pub enabled: bool,

    /// Per-session entries, sorted by `session_id` for stable output.
    /// Includes idle (drained) sessions with `buffered_count == 0`.
    pub sessions: Vec<OrderingSessionSnapshot>,

    /// Sessions that could not be observed because sequencing state was
    /// busy when we tried to snapshot. NOT in `sessions`. Zero means
    /// the snapshot is complete.
    pub skipped_session_count: usize,
}

impl OrderingSnapshot {
    /// True when no session was skipped, i.e., every live session's
    /// state was successfully captured in `sessions`.
    pub fn is_complete(&self) -> bool {
        self.skipped_session_count == 0
    }

    /// Delivery progress for messages sent by `sender`.
    pub fn delivery_progress_from(&self, sender: &ActorAddr) -> Option<DeliveryProgress> {
        if !self.is_complete() {
            return None;
        }

        let largest_dequeueable_sequence = self
            .sessions
            .iter()
            .filter(|session| session.sender.as_ref() == Some(sender))
            .map(|session| session.last_released_seq)
            .max()
            .unwrap_or_default();

        Some(DeliveryProgress {
            largest_dequeueable_sequence,
        })
    }
}

/// Receiver-side delivery progress for a sender.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Named)]
pub struct DeliveryProgress {
    /// Largest sequence released from receiver-side ordering into the
    /// actor's work queue.
    pub largest_dequeueable_sequence: u64,
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
/// Handler ports share a sequence per actor; ephemeral ports get individual
/// sequences. Control ports are direct.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum SeqKey {
    /// Shared sequence for all handler ports of an actor
    Actor(ActorAddr),
    /// Individual sequence for a specific ephemeral port.
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

impl SeqInfo {
    /// Whether this sequencing metadata is valid for receive-side ordering.
    pub fn is_valid(&self) -> bool {
        !matches!(self, Self::Session { seq: 0, .. })
    }
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
    /// - Handler ports: share the same sequence scheme per actor (keyed by ActorAddr).
    /// - Ephemeral ports: get individual sequence schemes (keyed by PortAddr).
    /// - Control ports: bypass receive-side reordering and use [`SeqInfo::Direct`].
    pub fn assign_seq(&self, port_id: &PortAddr) -> SeqInfo {
        let Some(key) = Self::seq_key(port_id) else {
            return SeqInfo::Direct;
        };

        let mut guard = self.last_seqs.lock().unwrap();
        let entry = guard.entry(key).or_default();
        *entry += 1;
        SeqInfo::Session {
            session_id: self.session_id,
            seq: *entry,
        }
    }

    /// Last sequence sent to `port_id`.
    pub fn last_sent(&self, port_id: &PortAddr) -> Option<u64> {
        let key = Self::seq_key(port_id)?;
        self.last_seqs.lock().unwrap().get(&key).copied()
    }

    fn seq_key(port_id: &PortAddr) -> Option<SeqKey> {
        if port_id.port().is_control() {
            return None;
        }

        if port_id.is_handler_port() {
            Some(SeqKey::Actor(port_id.actor_addr().clone()))
        } else {
            Some(SeqKey::Port(port_id.clone()))
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
    use crate::introspect::IntrospectMessage;
    use crate::mailbox::headers::SENDER_ACTOR_ID;
    use crate::mailbox::headers::stamp_sender_actor_id;
    use crate::port::ControlPort;
    use crate::port::Port;
    use crate::proc::StatusMessage;
    use crate::testing::ids::test_actor_id;

    /// Test message type 1 for handler port sequencing tests.
    #[derive(Named)]
    struct TestMsg1;

    /// Test message type 2 for handler port sequencing tests.
    #[derive(Named)]
    struct TestMsg2;

    /// Helper to extract seq from SeqInfo::Session variant (for tests only)
    fn get_seq(seq_info: SeqInfo) -> u64 {
        match seq_info {
            SeqInfo::Session { seq, .. } => seq,
            SeqInfo::Direct => panic!("expected Session variant, got Direct"),
        }
    }

    #[test]
    fn seq_info_validity_rejects_zero_session_seq() {
        let session_id = Uuid::now_v7();

        assert!(SeqInfo::Direct.is_valid());
        assert!(SeqInfo::Session { session_id, seq: 1 }.is_valid());
        assert!(!SeqInfo::Session { session_id, seq: 0 }.is_valid());
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
        // Two different handler ports for the same actor.
        let handler_port_1 = actor_ref.port_addr(Port::handler::<TestMsg1>());
        let handler_port_2 = actor_ref.port_addr(Port::handler::<TestMsg2>());

        // Handler ports should share a sequence (keyed by ActorAddr)
        assert_eq!(get_seq(sequencer.assign_seq(&handler_port_1)), 1);
        assert_eq!(get_seq(sequencer.assign_seq(&handler_port_2)), 2); // continues from 1
        assert_eq!(get_seq(sequencer.assign_seq(&handler_port_1)), 3);

        // Handler ports from a different actor get their own shared sequence
        let actor_ref_2: ActorAddr = test_actor_id("worker_1", "worker");
        let handler_port_3 = actor_ref_2.port_addr(Port::handler::<TestMsg1>());
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

        // Ephemeral ports from the same actor.
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
        let handler_port_1 = actor_ref.port_addr(Port::handler::<TestMsg1>());
        let handler_port_2 = actor_ref.port_addr(Port::handler::<TestMsg2>());

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
    }

    #[test]
    fn bypass_registry_status_message() {
        assert!(is_bypass_workq_type_id(TypeId::of::<StatusMessage>()));
    }

    #[test]
    fn control_port_uses_direct_seq_info() {
        // Control messages bypass the actor work queue and therefore must not
        // allocate session sequence numbers that a receive-side reorder buffer
        // would interpret.
        let sequencer = Sequencer::new(Uuid::now_v7());
        let actor_ref: ActorAddr = test_actor_id("agent_0", "proc_agent");

        let introspect_port = actor_ref.port_addr(Port::control(ControlPort::Introspect));
        let regular_actor_port = actor_ref.port_addr(Port::handler::<TestMsg1>());

        assert_eq!(sequencer.assign_seq(&introspect_port), SeqInfo::Direct);
        assert_eq!(get_seq(sequencer.assign_seq(&regular_actor_port)), 1);
        assert_eq!(sequencer.assign_seq(&introspect_port), SeqInfo::Direct);
        assert_eq!(get_seq(sequencer.assign_seq(&regular_actor_port)), 2);
    }

    #[test]
    fn test_sequencer_reports_last_sent() {
        let sequencer = Sequencer::new(Uuid::now_v7());
        let actor_ref: ActorAddr = test_actor_id("test_0", "test");
        let handler_port = actor_ref.port_addr(Port::handler::<TestMsg1>());
        let other_handler_port = actor_ref.port_addr(Port::handler::<TestMsg2>());
        let control_port = actor_ref.port_addr(Port::control(ControlPort::Status));

        assert_eq!(sequencer.last_sent(&handler_port), None);
        assert_eq!(get_seq(sequencer.assign_seq(&handler_port)), 1);
        assert_eq!(get_seq(sequencer.assign_seq(&other_handler_port)), 2);
        assert_eq!(sequencer.last_sent(&handler_port), Some(2));
        assert_eq!(sequencer.last_sent(&control_port), None);
    }

    #[test]
    fn test_ordering_snapshot_reports_delivery_progress_from_sender() {
        let sender = test_actor_id("sender", "test");
        let other = test_actor_id("other", "test");
        let snapshot = OrderingSnapshot {
            enabled: true,
            sessions: vec![
                OrderingSessionSnapshot {
                    session_id: Uuid::from_u128(1),
                    sender: Some(sender.clone()),
                    last_released_seq: 3,
                    expected_next_seq: 4,
                    buffered_count: 0,
                    oldest_buffered_seq: None,
                    newest_buffered_seq: None,
                },
                OrderingSessionSnapshot {
                    session_id: Uuid::from_u128(2),
                    sender: Some(sender.clone()),
                    last_released_seq: 5,
                    expected_next_seq: 6,
                    buffered_count: 1,
                    oldest_buffered_seq: Some(7),
                    newest_buffered_seq: Some(7),
                },
                OrderingSessionSnapshot {
                    session_id: Uuid::from_u128(3),
                    sender: Some(other.clone()),
                    last_released_seq: 8,
                    expected_next_seq: 9,
                    buffered_count: 0,
                    oldest_buffered_seq: None,
                    newest_buffered_seq: None,
                },
            ],
            skipped_session_count: 1,
        };

        assert_eq!(snapshot.delivery_progress_from(&sender), None,);

        let snapshot = OrderingSnapshot {
            skipped_session_count: 0,
            ..snapshot
        };
        assert_eq!(
            snapshot.delivery_progress_from(&sender),
            Some(DeliveryProgress {
                largest_dequeueable_sequence: 5,
            })
        );
        assert_eq!(
            snapshot.delivery_progress_from(&test_actor_id("missing", "test")),
            Some(DeliveryProgress {
                largest_dequeueable_sequence: 0,
            })
        );
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
        let dest = actor_ref.port_addr(Port::handler::<TestMsg1>());

        // Assign once: seq 1.
        assert_eq!(get_seq(sequencer.assign_seq(&dest)), 1);

        // Skip 2 (the helper would loop assign_seq).
        for _ in 0..2 {
            let _ = sequencer.assign_seq(&dest);
        }

        // Next assignment is seq 4, not 2.
        assert_eq!(get_seq(sequencer.assign_seq(&dest)), 4);
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
    // sequenced handler receiver restores per-session order.

    #[derive(Clone, Debug, Serialize, Deserialize, Named)]
    struct Frame {
        sender_idx: u32,
        payload_idx: u64,
    }

    #[derive(Clone, Debug, Serialize, Deserialize, Named)]
    struct SenderDone {
        sender_idx: u32,
    }

    // What ReceiverActor observed after its sequenced handler receiver released a
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
            // that arrives while its original is still buffered is dropped
            // as a duplicate; these replays are intended to hit the
            // late-duplicate drop path instead.
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
             sequenced receiver drop branch after snapshot; {}",
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
