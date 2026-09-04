/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::hash_map::Entry;
use std::io;
use std::mem::size_of;

use ring::rand::SecureRandom;
use ring::rand::SystemRandom;

use super::connection::ConnectionState;
use super::*;
use crate::io::PacketSendSlot;

pub(super) struct Network {
    driver: DriverId,
    routing_pid: Pid,
    configs: EndpointConfigs,
    next_connection: u64,
    next_connection_key: Option<u32>,
    next_initial_key: Option<u32>,
    pub(super) connections: HashMap<ConnectionId, ConnectionState>,
    routes: HashMap<[u8; CID_LEN], ConnectionId>,
    initial_routes: HashMap<InitialRoute, ConnectionId>,
    discard_buffer: Box<[u8]>,
    closed_connections: Vec<ConnectionId>,
    send_connections: VecDeque<ConnectionId>,
    pub(super) limits: EndpointLimits,
    peer_addresses_validated: bool,
    retry_tokens: RetryTokens,
    pub(super) pending_server_handshakes: usize,
    pub(super) connections_per_source: HashMap<std::net::IpAddr, usize>,
    pub(super) counters: EndpointStats,
    failed_connections: HashSet<ConnectionId>,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub(super) struct InitialRoute {
    pub(super) destination: [u8; CID_LEN],
    source: [u8; CID_LEN],
    peer: std::net::SocketAddr,
}

pub(super) struct RetryDatagram {
    pub(super) bytes: Vec<u8>,
    pub(super) destination: std::net::SocketAddr,
}

fn cid_target(cid: &[u8]) -> Result<Pid, Error> {
    let bytes = cid
        .get(..chrysalis_core::PID_LEN)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(Error::UnroutablePacket)?;
    Ok(Pid::from_bytes(bytes))
}

impl Network {
    pub(super) fn new(
        driver: DriverId,
        routing_pid: Pid,
        configs: EndpointConfigs,
        limits: EndpointLimits,
        peer_addresses_validated: bool,
    ) -> Self {
        Self {
            driver,
            routing_pid,
            configs,
            next_connection: 1,
            next_connection_key: Some(1),
            next_initial_key: Some(1),
            connections: HashMap::new(),
            routes: HashMap::new(),
            initial_routes: HashMap::new(),
            discard_buffer: vec![0; u16::MAX as usize].into_boxed_slice(),
            closed_connections: Vec::new(),
            send_connections: VecDeque::new(),
            limits,
            peer_addresses_validated,
            retry_tokens: RetryTokens::new(),
            pending_server_handshakes: 0,
            connections_per_source: HashMap::new(),
            counters: EndpointStats::default(),
            failed_connections: HashSet::new(),
        }
    }

    fn allocate_connection(&mut self) -> ConnectionId {
        let value = self.next_connection;
        self.next_connection = self
            .next_connection
            .checked_add(1)
            .expect("connection ID space should not overflow");
        ConnectionId::new(self.driver, value)
    }

    fn issue_cid(&mut self, pid: Pid) -> Result<RoutedCid, Error> {
        let key = self
            .next_connection_key
            .ok_or(Error::ConnectionKeyExhausted)?;
        self.next_connection_key = key.checked_add(1).filter(|next| *next < (1_u32 << 31));
        Ok(RoutedCid::issued(pid, ConnectionKey::from_u32(key)))
    }

    fn issue_initial_cid(&mut self, pid: Pid) -> Result<RoutedCid, Error> {
        let key = self.next_initial_key.ok_or(Error::ConnectionKeyExhausted)?;
        self.next_initial_key = key.checked_add(1).filter(|next| *next < (1_u32 << 31));
        Ok(RoutedCid::issued(
            pid,
            ConnectionKey::from_u32(key | (1_u32 << 31)),
        ))
    }

    fn issue_retry_cid(&self, pid: Pid) -> RoutedCid {
        loop {
            let mut bytes = [0; size_of::<u32>()];
            SystemRandom::new()
                .fill(&mut bytes)
                .expect("operating-system randomness should be available for QUIC Retry");
            let key = u32::from_be_bytes(bytes) & !(1_u32 << 31);
            if key == 0 {
                continue;
            }
            let cid = RoutedCid::issued(pid, ConnectionKey::from_u32(key));
            if !self.routes.contains_key(cid.as_bytes()) {
                return cid;
            }
        }
    }

    pub(super) fn connect(
        &mut self,
        local: std::net::SocketAddr,
        peer: std::net::SocketAddr,
        source_route: Option<Pid>,
        route: Pid,
        expected: Option<Pid>,
        server_name: &str,
    ) -> Result<ConnectionId, Error> {
        if self.connections.len() == self.limits.max_connections.get() {
            self.counters.admission_rejections += 1;
            return Err(Error::AdmissionLimited);
        }
        let id = self.allocate_connection();
        let local_route = source_route.unwrap_or(self.routing_pid);
        let source_cid = self.issue_cid(local_route)?;
        let destination_cid = self.issue_initial_cid(route)?;
        let config = self.configs.client.as_mut().ok_or(Error::WrongRole)?;
        let source = quiche::ConnectionId::from_ref(source_cid.as_bytes());
        let destination = quiche::ConnectionId::from_ref(destination_cid.as_bytes());
        let connection = quiche::connect_with_buffer_factory_and_dcid::<BufferFactory>(
            Some(server_name),
            &source,
            &destination,
            local,
            peer,
            config,
        )?;
        self.insert_route(*source_cid.as_bytes(), id)?;
        self.connections.insert(
            id,
            ConnectionState::new(
                connection,
                local_route,
                *source_cid.as_bytes(),
                None,
                false,
                expected,
                peer,
                self.limits.max_streams_per_connection,
            ),
        );
        Ok(id)
    }

    pub(super) fn insert_route(
        &mut self,
        cid: [u8; CID_LEN],
        connection: ConnectionId,
    ) -> Result<(), Error> {
        match self.routes.entry(cid) {
            Entry::Vacant(route) => {
                route.insert(connection);
                Ok(())
            }
            Entry::Occupied(route) if *route.get() == connection => Ok(()),
            Entry::Occupied(_) => {
                self.counters.cid_collisions += 1;
                Err(Error::CidCollision)
            }
        }
    }

    pub(super) fn receive(
        &mut self,
        packet: &mut [u8],
        from: std::net::SocketAddr,
        to: std::net::SocketAddr,
    ) -> Result<Option<RetryDatagram>, Error> {
        if packet.is_empty() {
            return Err(Error::UnroutablePacket);
        }
        let (destination, source, packet_type) = match Self::parse_routing_header(packet) {
            Ok(fields) => fields,
            Err(Error::Quiche(error)) => {
                self.counters.parse_errors += 1;
                return Err(Error::Quiche(error));
            }
            Err(error) => {
                self.counters.routing_errors += 1;
                return Err(error);
            }
        };
        let connection_id = if let Some(connection) = self.routes.get(&destination) {
            *connection
        } else {
            if packet_type != quiche::Type::Initial {
                return Err(Error::UnroutablePacket);
            }
            let initial_route = InitialRoute {
                destination,
                source: source.expect("Initial packet has an explicit source CID"),
                peer: from,
            };
            if let Some(connection) = self.initial_routes.get(&initial_route) {
                *connection
            } else {
                let header = quiche::Header::from_slice(packet, quiche::MAX_CONN_ID_LEN)?;
                let local_route = cid_target(&initial_route.destination)?;
                let retry_token = header.token.as_deref().unwrap_or_default();
                let original_dcid = if self.peer_addresses_validated {
                    None
                } else if retry_token.is_empty() {
                    let retry_cid = self.issue_retry_cid(local_route);
                    let token =
                        self.retry_tokens
                            .mint(from, header.dcid.as_ref(), retry_cid.as_bytes());
                    let mut output = vec![0; 1_200];
                    let length = quiche::retry(
                        &header.scid,
                        &header.dcid,
                        &quiche::ConnectionId::from_ref(retry_cid.as_bytes()),
                        &token,
                        header.version,
                        &mut output,
                    )?;
                    output.truncate(length);
                    return Ok(Some(RetryDatagram {
                        bytes: output,
                        destination: from,
                    }));
                } else {
                    let Some(original) =
                        self.retry_tokens
                            .validate(from, retry_token, &initial_route.destination)
                    else {
                        self.counters.invalid_retry_tokens += 1;
                        return Err(Error::InvalidRetryToken);
                    };
                    Some(original)
                };
                if !self.can_accept(from) {
                    self.counters.admission_rejections += 1;
                    return Err(Error::AdmissionLimited);
                }
                let id = self.allocate_connection();
                let source_cid = if original_dcid.is_some() {
                    initial_route.destination
                } else {
                    *self.issue_cid(local_route)?.as_bytes()
                };
                let config = self
                    .configs
                    .server
                    .as_mut()
                    .ok_or(Error::UnroutablePacket)?;
                let source = quiche::ConnectionId::from_ref(&source_cid);
                let original = original_dcid.as_deref().map(quiche::ConnectionId::from_ref);
                let connection = quiche::accept_with_buf_factory::<BufferFactory>(
                    &source,
                    original.as_ref(),
                    to,
                    from,
                    config,
                )?;
                self.insert_route(source_cid, id)?;
                self.initial_routes.insert(initial_route, id);
                self.connections.insert(
                    id,
                    ConnectionState::new(
                        connection,
                        local_route,
                        source_cid,
                        Some(initial_route),
                        true,
                        None,
                        from,
                        self.limits.max_streams_per_connection,
                    ),
                );
                self.pending_server_handshakes += 1;
                *self.connections_per_source.entry(from.ip()).or_default() += 1;
                id
            }
        };
        let Some(state) = self.connections.get_mut(&connection_id) else {
            self.routes.remove(&destination);
            self.counters.routing_errors += 1;
            return Err(Error::UnroutablePacket);
        };
        match state.connection.recv(packet, quiche::RecvInfo { from, to }) {
            Ok(_) | Err(quiche::Error::Done) => {}
            Err(_) => {
                self.counters.quiche_receive_errors += 1;
                self.failed_connections.insert(connection_id);
            }
        }
        Ok(None)
    }

    pub(super) fn can_accept(&self, source: std::net::SocketAddr) -> bool {
        self.connections.len() < self.limits.max_connections.get()
            && self.pending_server_handshakes < self.limits.max_pending_handshakes.get()
            && self
                .connections_per_source
                .get(&source.ip())
                .copied()
                .unwrap_or(0)
                < self.limits.max_connections_per_source.get()
    }

    /// Extracts the connection IDs and packet type needed for route lookup.
    pub(super) fn parse_routing_header(
        packet: &mut [u8],
    ) -> Result<([u8; CID_LEN], Option<[u8; CID_LEN]>, quiche::Type), Error> {
        if packet[0] & 0x80 == 0 {
            let destination = packet
                .get(1..1 + CID_LEN)
                .and_then(|cid| cid.try_into().ok())
                .ok_or(Error::UnroutablePacket)?;
            return Ok((destination, None, quiche::Type::Short));
        }
        let header = quiche::Header::from_slice(packet, quiche::MAX_CONN_ID_LEN)?;
        let destination = header
            .dcid
            .as_ref()
            .try_into()
            .map_err(|_| Error::UnroutablePacket)?;
        let source = header
            .scid
            .as_ref()
            .try_into()
            .map_err(|_| Error::UnroutablePacket)?;
        Ok((destination, Some(source), header.ty))
    }

    /// Allocates a bidirectional stream on an existing connection.
    pub(super) fn allocate_bidi(&mut self, connection: ConnectionId) -> Result<StreamId, Error> {
        self.connections
            .get_mut(&connection)
            .ok_or(Error::UnknownConnection)?
            .allocate_bidi(connection)
    }

    pub(super) fn close(
        &mut self,
        connection: ConnectionId,
        error_code: u64,
        reason: &[u8],
    ) -> Result<(), Error> {
        self.connections
            .get_mut(&connection)
            .ok_or(Error::UnknownConnection)?
            .connection
            .close(true, error_code, reason)?;
        Ok(())
    }

    pub(super) fn enqueue(&mut self, submission: Submission, completions: &mut Vec<Completion>) {
        let connection_id = submission.stream().connection();
        let Some(connection) = self.connections.get_mut(&connection_id) else {
            complete_unknown(submission, completions);
            return;
        };
        connection.enqueue(submission, completions);
    }

    pub(super) fn wake_cancelled_receive(&mut self, stream: StreamId) {
        if let Some(connection) = self.connections.get_mut(&stream.connection()) {
            connection.mark_runnable(stream.stream());
        }
    }

    pub(super) fn progress(&mut self, completions: &mut Vec<Completion>) {
        let mut established_servers = 0;
        for (connection_id, connection) in &mut self.connections {
            let completion_start = completions.len();
            established_servers += usize::from(connection.progress(
                *connection_id,
                &mut self.discard_buffer,
                completions,
            ));
            self.counters.authentication_failures += completions[completion_start..]
                .iter()
                .filter(|completion| matches!(completion, Completion::AuthenticationFailed(_)))
                .count() as u64;
        }
        self.pending_server_handshakes = self
            .pending_server_handshakes
            .saturating_sub(established_servers);
    }

    pub(super) fn queue_packets<I: PacketIo>(
        &mut self,
        packet_io: &mut I,
        _completions: &mut Vec<Completion>,
    ) -> Result<(), Error> {
        let segment_size = packet_io.segment_size();
        let max_segments = packet_io.max_gso_segments();
        let required_slot_bytes = segment_size.checked_mul(max_segments).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "packet send slot size overflow",
            )
        })?;
        if required_slot_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "packet send geometry must be nonzero",
            )
            .into());
        }
        let queued = self
            .send_connections
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        self.send_connections.extend(
            self.connections
                .keys()
                .filter(|connection| {
                    !self.failed_connections.contains(connection) && !queued.contains(connection)
                })
                .copied(),
        );
        while let Some(connection_id) = self.send_connections.pop_front() {
            let Some(mut slot) = packet_io.try_send_slot() else {
                self.send_connections.push_front(connection_id);
                return Ok(());
            };
            if slot.buffer_mut().len() < required_slot_bytes {
                self.counters.packet_io_errors += 1;
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "packet send slot is smaller than the advertised GSO geometry",
                )
                .into());
            }
            let Some(state) = self.connections.get_mut(&connection_id) else {
                continue;
            };
            let mut length = 0;
            let mut segments = 0;
            let mut destination = None;
            let mut send_at = None;
            let mut failed = false;
            while segments < max_segments {
                let start = segments * segment_size;
                match state
                    .connection
                    .send(&mut slot.buffer_mut()[start..start + segment_size])
                {
                    Ok((written, info)) => {
                        if let Some(expected) = destination {
                            if expected != info.to {
                                self.counters.quiche_send_errors += 1;
                                failed = true;
                                break;
                            }
                        } else {
                            destination = Some(info.to);
                        }
                        send_at = Some(send_at.map_or(info.at, |at: Instant| at.max(info.at)));
                        length += written;
                        segments += 1;
                        if written < segment_size {
                            break;
                        }
                    }
                    Err(quiche::Error::Done) => break,
                    Err(_) => {
                        self.counters.quiche_send_errors += 1;
                        failed = true;
                        break;
                    }
                }
            }
            if failed {
                drop(slot);
                self.failed_connections.insert(connection_id);
                continue;
            }
            if length == 0 {
                drop(slot);
                continue;
            }
            if let Err(error) = slot.submit(
                length,
                destination.expect("QUIC send should have a destination"),
                send_at.expect("QUIC send should have a pacing deadline"),
            ) {
                self.counters.packet_io_errors += 1;
                return Err(error.into());
            }
            self.send_connections.push_back(connection_id);
        }
        Ok(())
    }

    pub(super) fn next_timeout(&self, maximum: Duration) -> Duration {
        self.connections
            .values()
            .filter_map(|state| state.connection.timeout())
            .fold(maximum, Duration::min)
    }

    pub(super) fn process_timeouts(&mut self) {
        for state in self.connections.values_mut() {
            if state
                .connection
                .timeout()
                .is_some_and(|timeout| timeout.is_zero())
            {
                state.connection.on_timeout();
            }
        }
    }

    pub(super) fn reap_closed(&mut self, completions: &mut Vec<Completion>) {
        self.closed_connections.clear();
        self.closed_connections
            .extend(self.connections.iter().filter_map(|(id, state)| {
                (state.connection.is_closed() || self.failed_connections.contains(id))
                    .then_some(*id)
            }));
        while let Some(connection_id) = self.closed_connections.pop() {
            self.abandon_connection(connection_id, completions);
        }
    }

    pub(super) fn close_all(&mut self, error_code: u64, reason: &[u8]) {
        for state in self.connections.values_mut() {
            let _ = state.connection.close(true, error_code, reason);
        }
    }

    pub(super) fn abandon_all(&mut self, completions: &mut Vec<Completion>) {
        self.closed_connections.clear();
        self.closed_connections
            .extend(self.connections.keys().copied());
        while let Some(connection_id) = self.closed_connections.pop() {
            self.abandon_connection(connection_id, completions);
        }
    }

    fn abandon_connection(
        &mut self,
        connection_id: ConnectionId,
        completions: &mut Vec<Completion>,
    ) {
        let state = self
            .connections
            .remove(&connection_id)
            .expect("connection selected for removal should exist");
        let error_code = state
            .connection
            .peer_error()
            .or_else(|| state.connection.local_error())
            .map(|error| error.error_code);
        self.failed_connections.remove(&connection_id);
        if state.is_server && state.authentication_pending() {
            self.pending_server_handshakes = self.pending_server_handshakes.saturating_sub(1);
        }
        if state.is_server {
            let source = state.remote_address.ip();
            let count = self
                .connections_per_source
                .get_mut(&source)
                .expect("accepted connection should retain source accounting");
            *count -= 1;
            if *count == 0 {
                self.connections_per_source.remove(&source);
            }
        }
        self.routes.remove(&state.route);
        if let Some(initial_route) = state.initial_route {
            self.initial_routes.remove(&initial_route);
        }
        state.abandon(connection_id, completions);
        completions.push(Completion::ConnectionClosed {
            connection: connection_id,
            error_code,
        });
    }

    pub(super) fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }

    pub(super) fn has_pending_operations(&self) -> bool {
        self.connections
            .values()
            .any(ConnectionState::has_pending_operations)
    }

    pub(super) fn connection_stats(&self) -> HashMap<ConnectionId, ConnectionStats> {
        self.connections
            .iter()
            .filter_map(|(connection_id, state)| {
                let peer = state.peer()?;
                let stats = state.connection.stats();
                let path = state
                    .connection
                    .path_stats()
                    .find(|path| path.active)
                    .or_else(|| state.connection.path_stats().next());
                let (transmit_datagrams, rtt, congestion_window, congestion_events, current_mtu) =
                    path.map(|path| {
                        (
                            path.sent as u64,
                            path.rtt,
                            path.cwnd as u64,
                            path.total_pto_count as u64,
                            u16::try_from(path.pmtu).unwrap_or(u16::MAX),
                        )
                    })
                    .unwrap_or_default();
                Some((
                    *connection_id,
                    ConnectionStats {
                        peer: Some(peer),
                        transmit_datagrams,
                        transmit_bytes: stats.sent_bytes,
                        receive_datagrams: stats.recv as u64,
                        receive_bytes: stats.recv_bytes,
                        rtt,
                        congestion_window,
                        congestion_events,
                        lost_packets: stats.lost as u64,
                        lost_bytes: stats.lost_bytes,
                        sent_packets: stats.sent as u64,
                        current_mtu,
                        active_streams: state.streams.len() as u64,
                        runnable_streams: state.runnable_streams.len() as u64,
                        reclaimed_streams: state.reclaimed_streams,
                    },
                ))
            })
            .collect()
    }

    pub(super) fn endpoint_stats(&self, completion_backlog: usize) -> EndpointStats {
        let mut stats = self.counters;
        stats.active_connections = self.connections.len() as u64;
        stats.pending_handshakes = self.pending_server_handshakes as u64;
        stats.active_streams = self
            .connections
            .values()
            .map(|state| state.streams.len() as u64)
            .sum();
        stats.runnable_streams = self
            .connections
            .values()
            .map(|state| state.runnable_streams.len() as u64)
            .sum();
        stats.reclaimed_streams = self
            .connections
            .values()
            .map(|state| state.reclaimed_streams)
            .sum();
        stats.completion_backlog = completion_backlog as u64;
        stats
    }
}
