/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::VecDeque;

use tokio::sync::mpsc;

use crate::Role;
use crate::ctx::ChildConnectionKey;
use crate::ctx::Command;
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

pub(crate) enum Connection {
    Unestablished {
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

pub(crate) trait ConnectionTransport: Send {
    fn send(&self, action: ConnectionCommand) -> bool;
}

pub(crate) struct InprocConnectionTransport {
    pub(crate) tx: mpsc::UnboundedSender<Command>,
    pub(crate) peer: ConnectionRef,
}

impl ConnectionTransport for InprocConnectionTransport {
    fn send(&self, action: ConnectionCommand) -> bool {
        self.tx
            .send(Command::ConnectionSentCommand {
                connection: self.peer,
                action,
            })
            .is_ok()
    }
}

pub(crate) enum ConnectionCommand {
    SendMessage {
        destination_ident: Vec<u8>,
        parts: Vec<MsgPart>,
    },
    Establish {
        peer_alive: bool,
        peer_role: Role,
        peer_name: Option<Vec<u8>>,
        requested_name: Option<Vec<u8>>,
        transport: Box<dyn ConnectionTransport>,
    },
    Severed {
        reason: Vec<u8>,
    },
    PublishRoutes {
        actor_idents: Vec<Vec<u8>>,
    },
}

impl Connection {
    pub(crate) fn new_inproc(request: ConnectRequest) -> Self {
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
            Self::Unestablished {
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
