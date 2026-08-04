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
use tokio::sync::mpsc;

use crate::Role;
use crate::ctx::ChildConnectionKey;
use crate::ctx::Command;
use crate::ctx::Key;
use crate::msg::MsgPart;
use crate::shm::ShmClient;

/// Tear `connection` down by emitting a `Severed` to the command loop. Shared by
/// every transport (the QUIC reader, its heartbeat coroutines, …) so the
/// connection-failure signal is constructed in exactly one place.
pub(crate) fn sever(
    loop_tx: &mpsc::UnboundedSender<Command>,
    connection: ConnectionRef,
    reason: Vec<u8>,
) {
    let _ = loop_tx.send(Command::ConnectionAction {
        connection,
        action: ConnectionCommand::Severed { reason },
    });
}

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
    /// A monitor firing: the dead-or-absent target ident, from which the
    /// destination (the monitoring actor) reconstructs each local monitor's
    /// failure message. `is_timeout` distinguishes a non-existence timeout (reason
    /// `"actor does not exist"`) from an actual death (`"actor died"`); both route
    /// identically. A fire is only ever armed at an ancestor that already has a
    /// route to the monitoring actor, so it routes strictly *downward* and never
    /// buffers at a gateway.
    FireMonitor {
        to_monitor: Vec<u8>,
        is_timeout: bool,
    },
}

/// A monitor subscription change, carried both up the local hierarchy (in
/// [`ConnectionCommand::UpdateMonitorSubscription`]) and across a gateway
/// side-channel (in [`SideChannelAction::UpdateRemoteMonitorState`]). It crosses
/// both wire formats, so it is `Serialize`/`Deserialize`.
#[derive(Clone, Copy, Serialize, Deserialize)]
pub(crate) enum MonitorOp {
    /// Register a monitor. `timeout_ms` (0 = none) is the "must exist" timeout:
    /// for a local target it arms a non-existence timer; for a remote one it also
    /// bounds how long the owning gateway has to acknowledge the registration.
    Subscribe { timeout_ms: u64 },
    /// Cancel a previously registered monitor.
    Unsubscribe,
}

/// A self-addressing message crossing a gateway-to-gateway side-channel.
/// `gateway_for_actor` names the actor whose gateway should receive it, and the
/// side-channel is dialed at `gateway_tag(gateway_for_actor)` — there is no
/// separate destination tag. Routing never inspects the action; the receiving
/// gateway resolves the gateway once and dispatches on [`SideChannelAction`].
pub(crate) struct SideChannelMessage {
    /// Ident of the actor whose gateway gets this message. For a `Send` it is also
    /// the destination actor; for an ack it is the `listener`. The side-channel is
    /// dialed at `gateway_tag(this)`.
    pub(crate) gateway_for_actor: Vec<u8>,
    pub(crate) action: SideChannelAction,
}

/// What a [`SideChannelMessage`] does at the owning gateway.
pub(crate) enum SideChannelAction {
    /// Deliver this payload to `gateway_for_actor` (an ordinary cross-gateway
    /// message, or a `FireMonitor` headed back to a listener).
    Send(SendPayload),
    /// Register/cancel a monitor on `gateway_for_actor` (the target) firing to
    /// `listener`. The owning gateway applies it and, for a subscribe, replies with
    /// [`SideChannelAction::AckRemoteMonitor`].
    UpdateRemoteMonitorState { listener: Vec<u8>, op: MonitorOp },
    /// Confirm a registration. The carrying message's `gateway_for_actor` is the
    /// `listener`; `monitoring` identifies the target (hence the owning gateway, by
    /// its `@tag`).
    AckRemoteMonitor { monitoring: Vec<u8> },
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
    /// Climb a monitor subscription change up the *local* hierarchy toward the
    /// actor that handles `target` (the common ancestor that routes it, or the
    /// gateway at the top of the domain). One per monitoring actor per target — no
    /// per-monitor id, since a fire is identified by the dead ident. The
    /// [`MonitorOp`] selects register vs. cancel. Acks never travel this link —
    /// they only ever cross side-channels as
    /// [`SideChannelAction::AckRemoteMonitor`].
    UpdateMonitorSubscription {
        listener: Vec<u8>,
        target: Vec<u8>,
        op: MonitorOp,
    },
    /// The parent handing the child its gateway's [`ShmClient`] (the slab + dgram
    /// request socket fds), so the child can move large parts through the same
    /// slab and re-propagate the state to its own machine-local children. Flows
    /// down unix and inproc edges only (never quic).
    GatewayState {
        client: ShmClient,
    },
    /// Carry newly-reachable gateway tags up the ancestry. Each hop records them
    /// against the child connection they arrived on (in
    /// [`gateway_routes`](crate::actor::ActorEntry::gateway_routes)) and forwards
    /// them up — *without* stopping at gateway boundaries, so the whole ancestry up
    /// to the root learns where each gateway lives. This builds the tree a gateway
    /// death is later broadcast over. Crosses every transport (gateway routing is
    /// inherently cross-machine).
    PublishGatewayRoutes {
        live: Vec<Vec<u8>>,
    },
    /// Announce that one or more gateways have died. The direction is inferred from
    /// the connection it arrives on: arriving from a child it travels *up* toward
    /// the root; the root turns it around and it travels *down* every gateway-route
    /// child connection, reaching every gateway so each records the death in its
    /// gateway state (firing any monitors it held for the now-dead gateway).
    GatewayDied {
        dead: Vec<Vec<u8>>,
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
