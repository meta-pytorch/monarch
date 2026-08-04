/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::VecDeque;

use crate::Role;
use crate::ctx::ChildConnectionKey;
use crate::ctx::Key;
use crate::msg::MsgPart;

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
    },
    Failed,
}

/// The send half of a connection's pipe. Transports are pure plumbing: `send`
/// ferries a [`ConnectionCommand`] to the peer, and **dropping** the transport is
/// the universal "pipe closed" signal — the peer observes it and is severed.
/// (For UNIX this falls out of closing the socket; the inproc transport pushes a
/// `Severed` from its `Drop`.)
pub(crate) trait ConnectionTransport: Send {
    fn send(&self, action: ConnectionCommand) -> bool;
}

pub(crate) enum ConnectionCommand {
    SendMessage {
        destination_ident: Vec<u8>,
        parts: Vec<MsgPart>,
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
    PublishRoutes {
        actor_idents: Vec<Vec<u8>>,
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
    /// prefix and the peer ident to report. `None` if it had already failed.
    pub(crate) fn into_failure_report(self) -> Option<(Vec<MsgPart>, Vec<u8>)> {
        match self {
            Self::Established {
                failure_prefix,
                peer_ident,
                ..
            } => Some((failure_prefix, peer_ident)),
            Self::Unestablished {
                name_for_other,
                failure_prefix,
                ..
            } => Some((failure_prefix, name_for_other.unwrap_or_default())),
            // Connecting has not yet learned the peer's ident; fall back to the
            // name we assigned it, if any.
            Self::Connecting {
                name_for_other,
                failure_prefix,
                ..
            } => Some((failure_prefix, name_for_other.unwrap_or_default())),
            Self::Failed => None,
        }
    }
}

/// Why a connection failed to establish, carrying everything needed to notify
/// the owning actor: the failure-message prefix, the peer ident, and the reason.
pub(crate) struct EstablishFailure {
    pub(crate) failure_prefix: Vec<MsgPart>,
    pub(crate) peer_ident: Vec<u8>,
    pub(crate) reason: Vec<u8>,
}
