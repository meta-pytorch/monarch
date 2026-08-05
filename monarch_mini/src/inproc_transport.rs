/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! In-process transport: connects two actors living in the same context.
//!
//! The "pipe" is the context's own command channel. A serve and a join on the
//! same url are paired by a [`Matcher`]; on a match each side gets an
//! [`InprocConnectionTransport`] pointed at the other, announced to the command
//! loop via `Command::TransportConnected`. From there establishment is the same
//! generic flow the command loop runs for every transport — the command loop
//! sends each side's `Establish` along its transport, and the peer receives it.
//!
//! There are no coroutines and no extra hops: `send` forwards a command straight
//! to the peer connection through the command loop, and dropping the transport
//! pushes a `Severed` to the peer (the in-process analogue of a socket closing).

use std::collections::HashMap;

use tokio::sync::mpsc;

use crate::connection::ConnectionCommand;
use crate::connection::ConnectionRef;
use crate::connection::ConnectionTransport;
use crate::ctx::Command;
use crate::matcher::Matcher;
use crate::shm::ShmClientSlot;
use crate::transport::Transport;

pub(crate) struct InprocTransport {
    loop_tx: mpsc::UnboundedSender<Command>,
    matchers: HashMap<String, Matcher<ConnectionRef, ConnectionRef>>,
}

impl InprocTransport {
    pub(crate) fn new(loop_tx: mpsc::UnboundedSender<Command>) -> Self {
        Self {
            loop_tx,
            matchers: HashMap::new(),
        }
    }
}

impl Transport for InprocTransport {
    fn serve(&mut self, url: String, connection: ConnectionRef, _shm_client: ShmClientSlot) {
        let loop_tx = self.loop_tx.clone();
        let mut matcher = self.matchers.remove(&url).unwrap_or_else(Matcher::new);
        let _ = matcher.push_left(connection, |serve, join| pair(&loop_tx, serve, join));
        self.matchers.insert(url, matcher);
    }

    fn join(&mut self, url: String, connection: ConnectionRef, _shm_client: ShmClientSlot) {
        let loop_tx = self.loop_tx.clone();
        let mut matcher = self.matchers.remove(&url).unwrap_or_else(Matcher::new);
        let _ = matcher.push_right(connection, |serve, join| pair(&loop_tx, serve, join));
        self.matchers.insert(url, matcher);
    }
}

/// A serve and a join matched: give each side a transport pointed at the other
/// and announce both to the command loop.
fn pair(loop_tx: &mpsc::UnboundedSender<Command>, serve: ConnectionRef, join: ConnectionRef) {
    connect(loop_tx, serve, join);
    connect(loop_tx, join, serve);
}

fn connect(loop_tx: &mpsc::UnboundedSender<Command>, local: ConnectionRef, peer: ConnectionRef) {
    let transport = Box::new(InprocConnectionTransport {
        tx: loop_tx.clone(),
        peer,
    });
    let _ = loop_tx.send(Command::TransportConnected {
        connection: local,
        transport,
    });
}

/// Transport for one end of an inproc connection. `send` delivers a command to
/// the peer connection by re-entering the command loop.
struct InprocConnectionTransport {
    tx: mpsc::UnboundedSender<Command>,
    peer: ConnectionRef,
}

impl ConnectionTransport for InprocConnectionTransport {
    fn send(&self, action: ConnectionCommand) -> bool {
        // Same process: every command — gateway state (raw fds and all) included —
        // is delivered straight to the peer connection through the command loop.
        self.tx
            .send(Command::ConnectionAction {
                connection: self.peer,
                action,
            })
            .is_ok()
    }
}

impl Drop for InprocConnectionTransport {
    fn drop(&mut self) {
        // Dropping the send half is the in-process analogue of a socket closing:
        // tell the peer its side of the pipe is gone. (A no-op if the peer is
        // already failed.)
        let _ = self.tx.send(Command::ConnectionAction {
            connection: self.peer,
            action: ConnectionCommand::Severed {
                reason: b"peer connection closed".to_vec(),
            },
        });
    }
}
