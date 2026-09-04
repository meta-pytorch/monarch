/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use chrysalis_core::Pid;
use chrysalis_transport_core::CommandReceipt;
use chrysalis_transport_core::CommandSender;
use chrysalis_transport_core::ConnectionId;
use chrysalis_transport_core::TryCommandError;

/// One runtime-neutral endpoint control command.
#[derive(Debug)]
pub enum EndpointCommand {
    /// Creates a client connection state machine.
    Connect {
        /// PID encoded into locally issued source connection IDs.
        source: Option<Pid>,
        /// PID encoded into the Initial destination connection ID.
        route: Pid,
        /// Certificate-derived PID expected from the remote process.
        expected: Option<Pid>,
        /// Remote packet address.
        peer: SocketAddr,
        /// TLS server name sent during the handshake.
        server_name: Box<str>,
    },
    /// Allocates the next locally initiated bidirectional stream.
    OpenBidi {
        /// Connection that will own the stream.
        connection: ConnectionId,
    },
    /// Starts an application-level connection close.
    Close {
        /// Connection to close.
        connection: ConnectionId,
        /// QUIC application error code.
        error_code: u64,
        /// Diagnostic close reason.
        reason: Bytes,
    },
    /// Stops command and stream admission and drains the endpoint.
    Shutdown {
        /// Maximum time allowed for graceful QUIC close progress.
        grace_period: Duration,
    },
    /// Immediately abandons accepted endpoint work.
    Abort,
}

/// Thread-safe nonblocking control handle for one endpoint driver.
#[derive(Clone)]
pub struct EndpointCommands {
    sender: CommandSender<EndpointCommand>,
}

impl EndpointCommands {
    pub(crate) const fn new(sender: CommandSender<EndpointCommand>) -> Self {
        Self { sender }
    }

    /// Requests a client connection without waiting for driver or network progress.
    pub fn try_connect(
        &self,
        target: Pid,
        peer: SocketAddr,
        server_name: impl Into<Box<str>>,
    ) -> Result<CommandReceipt, TryCommandError<EndpointCommand>> {
        self.try_connect_routed(target, Some(target), peer, server_name)
    }

    /// Requests a client connection routed to `route`.
    ///
    /// `route` is encoded in the Initial destination CID for stateless forwarding. It identifies
    /// where packets are delivered, not who authenticates: the peer PID is derived from its
    /// certificate during the handshake and reported by the connection-established completion.
    pub fn try_connect_unpinned(
        &self,
        route: Pid,
        peer: SocketAddr,
        server_name: impl Into<Box<str>>,
    ) -> Result<CommandReceipt, TryCommandError<EndpointCommand>> {
        self.try_connect_routed(route, None, peer, server_name)
    }

    /// Requests a client connection with separate routing and authenticated identities.
    pub fn try_connect_routed(
        &self,
        route: Pid,
        expected: Option<Pid>,
        peer: SocketAddr,
        server_name: impl Into<Box<str>>,
    ) -> Result<CommandReceipt, TryCommandError<EndpointCommand>> {
        self.try_connect_from(None, route, expected, peer, server_name)
    }

    /// Requests a connection with explicit local and remote routing identities.
    pub fn try_connect_from(
        &self,
        source: Option<Pid>,
        route: Pid,
        expected: Option<Pid>,
        peer: SocketAddr,
        server_name: impl Into<Box<str>>,
    ) -> Result<CommandReceipt, TryCommandError<EndpointCommand>> {
        self.sender.try_submit(EndpointCommand::Connect {
            source,
            route,
            expected,
            peer,
            server_name: server_name.into(),
        })
    }

    /// Requests a locally initiated bidirectional stream.
    pub fn try_open_bidi(
        &self,
        connection: ConnectionId,
    ) -> Result<CommandReceipt, TryCommandError<EndpointCommand>> {
        self.sender
            .try_submit(EndpointCommand::OpenBidi { connection })
    }

    /// Requests an application-level connection close.
    pub fn try_close(
        &self,
        connection: ConnectionId,
        error_code: u64,
        reason: Bytes,
    ) -> Result<CommandReceipt, TryCommandError<EndpointCommand>> {
        self.sender.try_submit(EndpointCommand::Close {
            connection,
            error_code,
            reason,
        })
    }

    /// Requests structured endpoint shutdown.
    pub fn try_shutdown(
        &self,
        grace_period: Duration,
    ) -> Result<CommandReceipt, TryCommandError<EndpointCommand>> {
        self.sender
            .try_submit_and_close(EndpointCommand::Shutdown { grace_period })
    }

    /// Requests ordered immediate endpoint abort and atomically closes admission.
    pub fn try_abort(&self) -> Result<CommandReceipt, TryCommandError<EndpointCommand>> {
        self.sender.try_submit_and_close(EndpointCommand::Abort)
    }

    /// Returns whether the driver stopped accepting commands.
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}
