/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashSet;
use std::collections::VecDeque;

use serde::Deserialize;
use serde::Serialize;

use crate::Role;
use crate::ctx::ChildConnectionKey;
use crate::ctx::Key;
use crate::msg::MsgPart;
use crate::shm::ShmClient;

pub(crate) struct ConnectRequest {
    pub(crate) role: Role,
    pub(crate) name_for_other: Option<MsgPart>,
    pub(crate) hello_prefix: Vec<MsgPart>,
    pub(crate) failure_prefix: Vec<MsgPart>,
}

#[derive(Clone, Copy)]
pub(crate) enum ConnectionRef {
    ParentConnection {
        ofactor: Key,
    },
    ChildConnection {
        ofactor: Key,
        slot: ChildConnectionKey,
    },
}

impl ConnectionRef {
    pub(crate) fn owning_actor(self) -> Key {
        match self {
            Self::ParentConnection { ofactor } | Self::ChildConnection { ofactor, .. } => ofactor,
        }
    }

    pub(crate) fn role(self) -> Role {
        match self {
            Self::ParentConnection { .. } => Role::Child,
            Self::ChildConnection { .. } => Role::Parent,
        }
    }
}

/// A parent/child link. State only moves forward:
///
/// ```text
/// Unestablished → Connecting → Established → Failed
/// ```
///
/// - `Unestablished`: attached, but the transport has not come up yet. Outgoing
///   commands are buffered.
/// - `Connecting`: the transport is up (we can send), our own `Establish` has
///   gone out over it, and we are waiting for the peer's `Establish` to learn the
///   peer's identity. Outgoing commands are still buffered for hello-ordering.
/// - `Established`: the peer's identity is known; buffered commands are flushed
///   and routing/hello proceed.
pub(crate) enum Connection {
    Unestablished {
        name_for_other: Option<Vec<u8>>,
        hello_prefix: Vec<MsgPart>,
        failure_prefix: Vec<MsgPart>,
        queued_commands: VecDeque<ConnectionCommand>,
    },
    Connecting {
        transport: Box<dyn ConnectionTransport>,
        // The name we assigned the peer (if any). Kept — even though we already
        // sent it in our own Establish — so we can resolve the peer's ident from
        // it should the peer still have been unnamed when it sent its Establish.
        name_for_other: Option<Vec<u8>>,
        hello_prefix: Vec<MsgPart>,
        failure_prefix: Vec<MsgPart>,
        queued_commands: VecDeque<ConnectionCommand>,
    },
    Established {
        transport: Box<dyn ConnectionTransport>,
        failure_prefix: Vec<MsgPart>,
        peer_ident: Vec<u8>,
        /// Every ident that has been *live* through this connection (the peer and,
        /// for a child connection, the live routes published up through it). On
        /// failure these are exactly the idents to mark dead — no route-table scan
        /// needed. Idents that arrived already dead are not tracked: they are dead
        /// regardless of this connection's fate.
        routed_idents: HashSet<Vec<u8>>,
    },
    Failed,
}

/// The send half of a connection's pipe. Transports are pure plumbing: `send`
/// ferries a [`ConnectionCommand`] to the peer, and **dropping** the transport is
/// the universal "pipe closed" signal — the peer observes it and is severed.
/// (For UNIX this falls out of closing the socket; the inproc transport pushes a
/// `Severed` from its `Drop`.)
pub(crate) trait ConnectionTransport: Send {
    /// Send a command to the peer. Each transport decides what to do with each
    /// command — in particular, quic silently drops
    /// [`ConnectionCommand::GatewayState`] (shared memory is machine-local), while
    /// unix and inproc forward it.
    fn send(&self, action: ConnectionCommand) -> bool;
}

/// What a [`ConnectionCommand::SendMessage`] delivers once it reaches the actor
/// it is addressed to. Both kinds route identically (destination-driven, the same
/// table walk); they differ only in what the destination does on arrival, so a
/// single routing pathway carries them.
pub(crate) enum SendPayload {
    /// An ordinary actor message: opaque part bytes handed to the destination's
    /// poller.
    ActorMessage(Vec<MsgPart>),
    /// A monitor firing: the dead target ident, from which the destination (the
    /// monitoring actor) reconstructs each local monitor's failure message. A fire
    /// is only ever armed at an ancestor that already has a route to the
    /// monitoring actor, so it routes strictly *downward* and never buffers at a
    /// gateway.
    FireMonitor(Vec<u8>),
}

/// What a [`ConnectionCommand::ToAncestor`] does once it reaches the common
/// ancestor of `to_monitor` (the first actor up the tree that holds it in its
/// routing table, or the root). Both variants route up that same path, differing
/// only in the action at the ancestor; `dest` is the monitoring actor a fire
/// returns to.
#[derive(Serialize, Deserialize)]
pub(crate) enum AncestorPayload {
    /// Register the subscription on the target's route (firing at once if it is
    /// already dead).
    Subscribe { dest: Vec<u8> },
    /// Remove the matching subscription from the target's route.
    Unsubscribe { dest: Vec<u8> },
}

pub(crate) enum ConnectionCommand {
    SendMessage {
        destination_ident: Vec<u8>,
        payload: SendPayload,
    },
    /// The sender announcing itself to the peer: its role, ident, the name it
    /// assigns the peer, and whether it is still alive. Flows over the transport
    /// like any other command. A live sender drives the receiver to `Established`;
    /// a sender that announces `alive: false` (its actor died before the pipe came
    /// up) drives the receiver to sever instead — but still carries the ident, so
    /// the failure names the connection that died.
    Establish {
        role: Role,
        ident: Option<Vec<u8>>,
        name_for_other: Option<Vec<u8>>,
        alive: bool,
    },
    Severed {
        reason: Vec<u8>,
    },
    /// Carry route state up the ancestry. `live` idents become
    /// [`Route::Connection`](crate::actor::Route::Connection) toward this child;
    /// `dead` idents become [`Route::Dead`](crate::actor::Route::Dead) and fire any
    /// monitors watching them. Death travels in the *same* command as life so a
    /// route's state is never lost on the way up (a dead actor is never mistaken
    /// for one that does not exist yet). This is the only way dead routes propagate.
    PublishRoutes {
        live: Vec<Vec<u8>>,
        dead: Vec<Vec<u8>>,
    },
    /// Route a monitor operation up toward the common ancestor that has
    /// `to_monitor` in its routing table (or the root): one per monitoring actor
    /// per target — no per-monitor id, since a fire is identified by the dead
    /// ident. The [`AncestorPayload`] selects register vs. cancel.
    ToAncestor {
        to_monitor: Vec<u8>,
        payload: AncestorPayload,
    },
    /// The parent handing the child its gateway's [`ShmClient`] (the slab + dgram
    /// request socket fds), so the child can move large parts through the same
    /// slab and re-propagate the state to its own machine-local children. Flows
    /// down unix and inproc edges only (never quic).
    GatewayState {
        client: ShmClient,
    },
}

impl Connection {
    pub(crate) fn new_unestablished(request: ConnectRequest) -> Self {
        let name_for_other = request
            .name_for_other
            .as_ref()
            .map(|name| name.as_bytes().to_vec());
        Self::Unestablished {
            name_for_other,
            hello_prefix: request.hello_prefix,
            failure_prefix: request.failure_prefix,
            queued_commands: VecDeque::new(),
        }
    }

    pub(crate) fn send(&mut self, command: ConnectionCommand) -> bool {
        match self {
            Self::Established { transport, .. } => transport.send(command),
            // Before establishment outgoing commands are buffered (even once the
            // transport is up, so the peer's hello precedes any message).
            Self::Unestablished {
                queued_commands, ..
            }
            | Self::Connecting {
                queued_commands, ..
            } => {
                queued_commands.push_back(command);
                true
            }
            Self::Failed => false,
        }
    }

    /// Consume a connection that is being torn down, yielding the failure-message
    /// prefix, the peer ident to report, and every ident that was reachable
    /// through it (to mark dead). `None` if it had already failed.
    pub(crate) fn into_failure_report(self) -> Option<FailureReport> {
        match self {
            Self::Established {
                failure_prefix,
                peer_ident,
                routed_idents,
                ..
            } => Some((failure_prefix, peer_ident, routed_idents)),
            Self::Unestablished {
                name_for_other,
                failure_prefix,
                ..
            } => Some((
                failure_prefix,
                name_for_other.unwrap_or_default(),
                HashSet::new(),
            )),
            // Connecting has not yet learned the peer's ident; fall back to the
            // name we assigned it, if any.
            Self::Connecting {
                name_for_other,
                failure_prefix,
                ..
            } => Some((
                failure_prefix,
                name_for_other.unwrap_or_default(),
                HashSet::new(),
            )),
            Self::Failed => None,
        }
    }
}

/// What [`Connection::into_failure_report`] yields: the failure-message prefix,
/// the peer ident to report, and every ident that routed through the connection.
pub(crate) type FailureReport = (Vec<MsgPart>, Vec<u8>, HashSet<Vec<u8>>);
