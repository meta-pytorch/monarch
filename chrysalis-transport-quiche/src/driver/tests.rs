/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashSet;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::net::UdpSocket;
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use bytes::BytesMut;
use chrysalis_transport_core::Completion;
use chrysalis_transport_core::DriverId;
use chrysalis_transport_core::NoopNotifier;
use chrysalis_transport_core::ReceiveOptions;
use chrysalis_transport_core::SendOutcome;
use chrysalis_transport_core::StreamId;
use chrysalis_transport_core::SubmissionLimits;
use chrysalis_transport_core::TryControlError;
use chrysalis_transport_uring::DriverConfig;
use chrysalis_transport_uring::UdpDriver;
use rcgen::CertifiedKey;
use tempfile::TempDir;

use super::connection::StreamState;
use super::*;
use crate::io::PacketSendSlot;

const APPLICATION_PROTOCOL: &[u8] = b"chrysalis-transport-quiche-test/1";

fn transport_config() -> DriverConfig {
    DriverConfig::default()
        .with_ring_depth(NonZeroU32::new(32).unwrap())
        .with_receive_depth(NonZeroUsize::new(8).unwrap())
        .with_segment_size(NonZeroUsize::new(1200).unwrap())
        .with_max_gso_segments(NonZeroUsize::new(4).unwrap())
        .with_socket_buffer_bytes(NonZeroUsize::new(1024 * 1024).unwrap())
        .with_gro(true)
}

fn submission_limits() -> SubmissionLimits {
    SubmissionLimits::new(
        NonZeroUsize::new(32).unwrap(),
        NonZeroUsize::new(1024 * 1024).unwrap(),
        NonZeroUsize::new(1024 * 1024).unwrap(),
    )
}

fn configure_quic(config: &mut quiche::Config) {
    config
        .set_application_protos(&[APPLICATION_PROTOCOL])
        .unwrap();
    config.set_max_idle_timeout(5_000);
    config.set_max_recv_udp_payload_size(1200);
    config.set_max_send_udp_payload_size(1200);
    config.set_initial_max_data(1024 * 1024);
    config.set_initial_max_stream_data_bidi_local(1024 * 1024);
    config.set_initial_max_stream_data_bidi_remote(1024 * 1024);
    config.set_initial_max_streams_bidi(16);
    config.set_initial_max_streams_uni(0);
    config.set_disable_active_migration(true);
    config.enable_pacing(true);
    config.set_cc_algorithm(quiche::CongestionControlAlgorithm::CUBIC);
}

fn write_certificate(directory: &Path) -> (String, String, EndpointIdentity) {
    let CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let identity = EndpointIdentity::from_leaf_certificate(cert.der().as_ref());
    let certificate = directory.join("certificate.pem");
    let key = directory.join("key.pem");
    fs::write(&certificate, cert.pem()).unwrap();
    fs::write(&key, signing_key.serialize_pem()).unwrap();
    (
        certificate.to_str().unwrap().to_owned(),
        key.to_str().unwrap().to_owned(),
        identity,
    )
}

fn endpoint_io() -> UdpDriver {
    UdpDriver::new(UdpSocket::bind("[::1]:0").unwrap(), transport_config()).unwrap()
}

struct ShortSendSlot {
    buffer: Vec<u8>,
}

impl PacketSendSlot for ShortSendSlot {
    fn buffer_mut(&mut self) -> &mut [u8] {
        &mut self.buffer
    }

    fn submit(
        self: Box<Self>,
        _length: usize,
        _destination: SocketAddr,
        _send_at: Instant,
    ) -> io::Result<()> {
        panic!("undersized send slot should be rejected before submission")
    }
}

struct ShortSlotIo {
    slot: Option<Vec<u8>>,
}

impl PacketIo for ShortSlotIo {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok("[::1]:0".parse().unwrap())
    }

    fn segment_size(&self) -> usize {
        1_200
    }

    fn max_gso_segments(&self) -> usize {
        4
    }

    fn notifier(&self) -> Arc<dyn Notifier> {
        Arc::new(NoopNotifier)
    }

    fn try_send_slot(&mut self) -> Option<Box<dyn PacketSendSlot + '_>> {
        self.slot
            .take()
            .map(|buffer| Box::new(ShortSendSlot { buffer }) as Box<dyn PacketSendSlot>)
    }

    fn poll(&mut self, _timeout: Duration) -> io::Result<()> {
        Ok(())
    }

    fn drain_received(
        &mut self,
        _receive: &mut dyn FnMut(&mut [u8], SocketAddr, SocketAddr) -> Result<(), Error>,
    ) -> Result<(), Error> {
        Ok(())
    }
}

fn drain(handle: &EndpointHandle, events: &mut Vec<Completion>) {
    while let Some(completion) = handle.completions().try_pop() {
        events.push(completion);
    }
}

fn drive_until(
    client: &mut Endpoint,
    server: &mut Endpoint,
    client_handle: &EndpointHandle,
    server_handle: &EndpointHandle,
    client_events: &mut Vec<Completion>,
    server_events: &mut Vec<Completion>,
    done: impl Fn(&[Completion], &[Completion]) -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !done(client_events, server_events) {
        assert!(Instant::now() < deadline, "QUIC endpoint test timed out");
        client.poll(Duration::from_millis(1)).unwrap();
        server.poll(Duration::from_millis(1)).unwrap();
        drain(client_handle, client_events);
        drain(server_handle, server_events);
    }
}

fn endpoint_pair() -> (
    Endpoint,
    EndpointHandle,
    Endpoint,
    EndpointHandle,
    std::net::SocketAddr,
    Pid,
) {
    endpoint_pair_with_completion_capacity(64)
}

fn endpoint_pair_with_completion_capacity(
    completion_capacity: usize,
) -> (
    Endpoint,
    EndpointHandle,
    Endpoint,
    EndpointHandle,
    std::net::SocketAddr,
    Pid,
) {
    let directory = TempDir::new().unwrap();
    let (certificate, key, identity) = write_certificate(directory.path());

    let mut server_config = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();
    configure_quic(&mut server_config);
    server_config
        .load_cert_chain_from_pem_file(&certificate)
        .unwrap();
    server_config.load_priv_key_from_pem_file(&key).unwrap();
    server_config
        .load_verify_locations_from_file(&certificate)
        .unwrap();
    server_config.verify_peer(true);

    let mut client_config = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();
    configure_quic(&mut client_config);
    client_config
        .load_cert_chain_from_pem_file(&certificate)
        .unwrap();
    client_config.load_priv_key_from_pem_file(&key).unwrap();
    client_config.verify_peer(false);

    let server_io = endpoint_io();
    let server_address = server_io.local_addr().unwrap();
    let (server, server_handle) = Endpoint::server(
        DriverId::from_u16(1),
        server_io,
        identity,
        server_config,
        submission_limits(),
        NonZeroUsize::new(completion_capacity).unwrap(),
        Arc::new(NoopNotifier),
    );
    let (client, client_handle) = Endpoint::client(
        DriverId::from_u16(2),
        endpoint_io(),
        identity,
        client_config,
        submission_limits(),
        NonZeroUsize::new(completion_capacity).unwrap(),
        Arc::new(NoopNotifier),
    );
    (
        client,
        client_handle,
        server,
        server_handle,
        server_address,
        identity.pid(),
    )
}

#[test]
fn short_header_extracts_the_fixed_destination_cid() {
    let cid = RoutedCid::issued(
        Pid::from_bytes([0x42; chrysalis_core::PID_LEN]),
        ConnectionKey::from_u32(7),
    );
    let mut packet = vec![0x40];
    packet.extend_from_slice(cid.as_bytes());
    packet.extend_from_slice(b"payload");

    let (destination, source, packet_type) = Network::parse_routing_header(&mut packet).unwrap();

    assert_eq!(destination, *cid.as_bytes());
    assert_eq!(source, None);
    assert_eq!(packet_type, quiche::Type::Short);
}

#[test]
fn short_header_rejects_a_truncated_destination_cid() {
    let mut packet = vec![0x40; CID_LEN];

    assert!(matches!(
        Network::parse_routing_header(&mut packet),
        Err(Error::UnroutablePacket)
    ));
}

#[test]
fn untrusted_udp_uses_retry_before_allocating_server_state() {
    let (mut client, _client_handle, mut server, _server_handle, server_address, server_pid) =
        endpoint_pair();
    client
        .connect(server_address, server_pid, Some(server_pid), "localhost")
        .unwrap();

    client.poll(Duration::ZERO).unwrap();
    server.poll(Duration::from_millis(20)).unwrap();

    assert!(server.network.connections.is_empty());
    assert_eq!(server.network.pending_server_handshakes, 0);
}

#[test]
fn admission_limits_pending_handshakes_and_sources() {
    let (_client, _client_handle, mut server, _server_handle, _address, _pid) = endpoint_pair();
    server.network.limits = EndpointLimits::new(
        NonZeroUsize::new(4).unwrap(),
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(8).unwrap(),
    );
    let first: std::net::SocketAddr = "[::1]:1000".parse().unwrap();
    let second: std::net::SocketAddr = "[::2]:1000".parse().unwrap();
    assert!(server.network.can_accept(first));

    server.network.pending_server_handshakes = 1;
    assert!(!server.network.can_accept(second));
    server.network.pending_server_handshakes = 0;
    server.network.connections_per_source.insert(first.ip(), 1);
    assert!(!server.network.can_accept(first));
    assert!(server.network.can_accept(second));
}

#[test]
fn cid_collision_preserves_the_existing_route() {
    let (mut client, _client_handle, _server, _server_handle, _address, _pid) = endpoint_pair();
    let cid = *RoutedCid::issued(
        Pid::from_bytes([0x31; chrysalis_core::PID_LEN]),
        ConnectionKey::from_u32(7),
    )
    .as_bytes();
    let first = ConnectionId::new(DriverId::from_u16(1), 1);
    let second = ConnectionId::new(DriverId::from_u16(1), 2);

    assert!(matches!(client.network.insert_route(cid, first), Ok(())));
    assert!(matches!(
        client.network.insert_route(cid, second),
        Err(Error::CidCollision)
    ));
    assert!(matches!(client.network.insert_route(cid, first), Ok(())));
}

#[test]
fn malformed_datagram_does_not_interrupt_a_later_connection() {
    let (mut client, client_handle, mut server, server_handle, server_address, server_pid) =
        endpoint_pair();
    let injector = UdpSocket::bind("[::1]:0").unwrap();
    injector.send_to(&[0x40, 1, 2, 3], server_address).unwrap();
    server.poll(Duration::from_millis(20)).unwrap();
    assert!(server.network.counters.routing_errors > 0);

    client_handle
        .commands()
        .try_connect(server_pid, server_address, "localhost")
        .unwrap();
    let mut client_events = Vec::new();
    let mut server_events = Vec::new();
    drive_until(
        &mut client,
        &mut server,
        &client_handle,
        &server_handle,
        &mut client_events,
        &mut server_events,
        |client, server| {
            client
                .iter()
                .any(|event| matches!(event, Completion::ConnectionEstablished(_)))
                && server
                    .iter()
                    .any(|event| matches!(event, Completion::ConnectionEstablished(_)))
        },
    );
}

#[test]
fn packet_output_rejects_an_undersized_send_slot() {
    let (mut client, _client_handle, _server, _server_handle, server_address, server_pid) =
        endpoint_pair();
    client
        .connect(server_address, server_pid, Some(server_pid), "localhost")
        .unwrap();
    let mut io = ShortSlotIo {
        slot: Some(vec![0; 1_200]),
    };

    let error = client
        .network
        .queue_packets(&mut io, &mut Vec::new())
        .expect_err("undersized GSO slot should fail");
    assert!(matches!(error, Error::Io(error) if error.kind() == io::ErrorKind::InvalidInput));
}

#[test]
fn terminal_stream_churn_reclaims_state_immediately() {
    let (mut client, _client_handle, _server, _server_handle, server_address, server_pid) =
        endpoint_pair();
    let connection_id = client
        .connect(server_address, server_pid, Some(server_pid), "localhost")
        .unwrap();
    let connection = client.network.connections.get_mut(&connection_id).unwrap();
    let mut completions = Vec::new();
    for stream_id in 0..100_000_u64 {
        connection
            .streams
            .entry(stream_id)
            .or_insert_with(StreamState::terminal);
        assert!(connection.reap_terminal_stream(connection_id, stream_id, &mut completions));
        assert!(connection.streams.is_empty());
    }
    assert_eq!(connection.reclaimed_streams, 100_000);
    assert_eq!(completions.len(), 100_000);
}

#[test]
fn connection_statistics_keep_same_peer_connections_distinct() {
    let peer = Pid::from_bytes([7; chrysalis_core::PID_LEN]);
    let first = ConnectionId::new(DriverId::from_u16(1), 1);
    let second = ConnectionId::new(DriverId::from_u16(1), 2);
    let handle = ConnectionStatsHandle::default();
    handle.replace(HashMap::from([
        (
            first,
            ConnectionStats {
                peer: Some(peer),
                transmit_bytes: 10,
                ..ConnectionStats::default()
            },
        ),
        (
            second,
            ConnectionStats {
                peer: Some(peer),
                transmit_bytes: 20,
                ..ConnectionStats::default()
            },
        ),
    ]));

    assert_eq!(handle.get_connection(first).unwrap().transmit_bytes, 10);
    assert_eq!(handle.get_connection(second).unwrap().transmit_bytes, 20);
    assert_eq!(handle.aggregate_peer(peer).unwrap().transmit_bytes, 30);
}

#[test]
fn peer_application_close_code_reaches_terminal_event() {
    let (mut client, client_handle, mut server, server_handle, server_address, server_pid) =
        endpoint_pair();
    let connect = client_handle
        .commands()
        .try_connect(server_pid, server_address, "localhost")
        .unwrap();
    let mut client_events = Vec::new();
    let mut server_events = Vec::new();
    drive_until(
        &mut client,
        &mut server,
        &client_handle,
        &server_handle,
        &mut client_events,
        &mut server_events,
        |client, server| {
            client
                .iter()
                .any(|event| matches!(event, Completion::ConnectionEstablished(_)))
                && server
                    .iter()
                    .any(|event| matches!(event, Completion::ConnectionEstablished(_)))
        },
    );
    let connection = client_events
        .iter()
        .find_map(|event| match event {
            Completion::Command(completion) if completion.request() == connect.request() => {
                match completion.result() {
                    CommandResult::ConnectionCreated(connection) => Some(connection),
                    _ => None,
                }
            }
            _ => None,
        })
        .unwrap();
    client_handle
        .commands()
        .try_close(connection, 42, Bytes::from_static(b"test close"))
        .unwrap();
    drive_until(
        &mut client,
        &mut server,
        &client_handle,
        &server_handle,
        &mut client_events,
        &mut server_events,
        |_client, server| {
            server.iter().any(|event| {
                matches!(
                    event,
                    Completion::ConnectionClosed {
                        error_code: Some(42),
                        ..
                    }
                )
            })
        },
    );
}

#[test]
fn handshake_send_receive_and_ack_use_core_queues() {
    let (mut client, client_handle, mut server, server_handle, server_address, server_pid) =
        endpoint_pair();
    let connect = client_handle
        .commands()
        .try_connect(server_pid, server_address, "localhost")
        .unwrap();

    let mut client_events = Vec::new();
    let mut server_events = Vec::new();
    drive_until(
        &mut client,
        &mut server,
        &client_handle,
        &server_handle,
        &mut client_events,
        &mut server_events,
        |client, server| {
            client.iter().any(|event| {
                matches!(
                    event,
                    Completion::Command(completion)
                        if completion.request() == connect.request()
                            && matches!(completion.result(), CommandResult::ConnectionCreated(_))
                )
            }) && client
                .iter()
                .any(|event| matches!(event, Completion::ConnectionEstablished(_)))
                && server
                    .iter()
                    .any(|event| matches!(event, Completion::ConnectionEstablished(_)))
        },
    );
    let client_connection = client_events
        .iter()
        .find_map(|event| match event {
            Completion::Command(completion) if completion.request() == connect.request() => {
                match completion.result() {
                    CommandResult::ConnectionCreated(connection) => Some(connection),
                    _ => None,
                }
            }
            _ => None,
        })
        .unwrap();
    let server_connection = server_events
        .iter()
        .find_map(|event| match event {
            Completion::ConnectionEstablished(established) => {
                assert_eq!(established.peer(), server_pid);
                Some(established.connection())
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(
        &client.network.connections[&client_connection]
            .connection
            .source_id()[..chrysalis_core::PID_LEN],
        server_pid.as_bytes()
    );
    assert_eq!(
        &server.network.connections[&server_connection]
            .connection
            .source_id()[..chrysalis_core::PID_LEN],
        server_pid.as_bytes()
    );
    client_events.clear();
    server_events.clear();

    let open = client_handle
        .commands()
        .try_open_bidi(client_connection)
        .unwrap();
    client.poll(Duration::ZERO).unwrap();
    drain(&client_handle, &mut client_events);
    let client_stream = client_events
        .iter()
        .find_map(|event| match event {
            Completion::Command(completion) if completion.request() == open.request() => {
                match completion.result() {
                    CommandResult::StreamOpened(stream) => Some(stream),
                    _ => None,
                }
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(client_stream.stream(), 0);
    let server_stream = StreamId::new(server_connection, 0);
    server
        .network
        .connections
        .get_mut(&server_connection)
        .unwrap()
        .streams
        .entry(server_stream.stream())
        .or_default();
    let discard = server_handle
        .submissions()
        .try_discard(server_stream, NonZeroUsize::new(2).unwrap())
        .unwrap();
    server_handle
        .submissions()
        .try_receive(
            server_stream,
            BytesMut::with_capacity(64),
            ReceiveOptions::default(),
        )
        .unwrap();
    client_handle
        .submissions()
        .try_send(client_stream, Bytes::from_static(b"hello"))
        .unwrap();
    client_handle
        .submissions()
        .try_finish(client_stream)
        .unwrap();
    let rejected_send = client_handle
        .submissions()
        .try_send(client_stream, Bytes::from_static(b"late"))
        .unwrap();
    let rejected_finish = client_handle
        .submissions()
        .try_finish(client_stream)
        .unwrap();

    drive_until(
        &mut client,
        &mut server,
        &client_handle,
        &server_handle,
        &mut client_events,
        &mut server_events,
        |client, server| {
            client.iter().any(|event| {
                matches!(
                    event,
                    Completion::Send {
                        outcome: SendOutcome::Acknowledged { .. },
                        ..
                    }
                )
            }) && client.iter().any(|event| {
                matches!(
                    event,
                    Completion::Send {
                        operation,
                        outcome: SendOutcome::Rejected,
                        ..
                    } if *operation == rejected_send.operation()
                )
            }) && client.iter().any(|event| {
                matches!(
                    event,
                    Completion::Finish {
                        operation,
                        outcome: ControlOutcome::Rejected,
                        ..
                    } if *operation == rejected_finish.operation()
                )
            }) && server
                .iter()
                .any(|event| matches!(event, Completion::Receive(_)))
        },
    );

    let receive = server_events
        .iter()
        .find_map(|event| match event {
            Completion::Receive(receive) => Some(receive),
            _ => None,
        })
        .unwrap();
    assert_eq!(receive.data(), b"llo");
    assert!(server_events.iter().any(|event| matches!(
        event,
        Completion::Discard {
            operation,
            bytes: 2,
            status: ReceiveStatus::Data,
            ..
        } if *operation == discard.operation()
    )));
    assert_eq!(client_handle.submissions().retained_send_bytes(), 0);
    assert!(client_events.iter().any(|event| {
        matches!(
            event,
            Completion::Send {
                bytes: 5,
                outcome: SendOutcome::Acknowledged {
                    acknowledged_through: 5
                },
                ..
            }
        )
    }));

    client_handle
        .submissions()
        .try_receive(
            client_stream,
            BytesMut::with_capacity(1),
            ReceiveOptions::default(),
        )
        .unwrap();
    server_handle
        .submissions()
        .try_finish(server_stream)
        .unwrap();
    drive_until(
        &mut client,
        &mut server,
        &client_handle,
        &server_handle,
        &mut client_events,
        &mut server_events,
        |client, server| {
            client.iter().any(|event| {
                matches!(
                    event,
                    Completion::Closed { stream, .. } if *stream == client_stream
                )
            }) && server.iter().any(|event| {
                matches!(
                    event,
                    Completion::Closed { stream, .. } if *stream == server_stream
                )
            })
        },
    );
    assert!(
        client.network.connections[&client_connection]
            .streams
            .is_empty()
    );
    assert!(
        server.network.connections[&server_connection]
            .streams
            .is_empty()
    );

    client_events.clear();
    let stale_receive = client_handle
        .submissions()
        .try_receive(
            client_stream,
            BytesMut::with_capacity(8),
            ReceiveOptions::default(),
        )
        .unwrap();
    client.poll(Duration::ZERO).unwrap();
    drain(&client_handle, &mut client_events);
    assert!(client_events.iter().any(|event| matches!(
        event,
        Completion::Receive(receive)
            if receive.operation() == stale_receive.operation()
                && receive.status() == ReceiveStatus::Closed
    )));
    assert!(
        client.network.connections[&client_connection]
            .streams
            .is_empty()
    );

    client_events.clear();
    let close = client_handle
        .commands()
        .try_close(client_connection, 17, Bytes::from_static(b"test complete"))
        .unwrap();
    client.poll(Duration::ZERO).unwrap();
    drain(&client_handle, &mut client_events);
    assert!(client_events.iter().any(|event| matches!(
        event,
        Completion::Command(completion)
            if completion.request() == close.request()
                && completion.result() == CommandResult::CloseQueued(client_connection)
    )));
}

#[test]
fn multiplexes_connections_and_streams_on_one_udp_driver() {
    let (mut client, client_handle, mut server, server_handle, server_address, server_pid) =
        endpoint_pair();
    let first = client
        .connect(server_address, server_pid, Some(server_pid), "localhost")
        .unwrap();
    let second = client
        .connect(server_address, server_pid, Some(server_pid), "localhost")
        .unwrap();
    let mut client_events = Vec::new();
    let mut server_events = Vec::new();
    drive_until(
        &mut client,
        &mut server,
        &client_handle,
        &server_handle,
        &mut client_events,
        &mut server_events,
        |client, server| {
            client
                .iter()
                .filter(|event| matches!(event, Completion::ConnectionEstablished(_)))
                .count()
                == 2
                && server
                    .iter()
                    .filter(|event| matches!(event, Completion::ConnectionEstablished(_)))
                    .count()
                    == 2
        },
    );
    client_events.clear();
    server_events.clear();

    for (connection, payload) in [
        (first, Bytes::from_static(b"first")),
        (second, Bytes::from_static(b"second")),
    ] {
        let stream = client.network.allocate_bidi(connection).unwrap();
        client_handle
            .submissions()
            .try_send(stream, payload)
            .unwrap();
        client_handle.submissions().try_finish(stream).unwrap();
    }
    drive_until(
        &mut client,
        &mut server,
        &client_handle,
        &server_handle,
        &mut client_events,
        &mut server_events,
        |_, server| {
            server
                .iter()
                .filter(|event| matches!(event, Completion::IncomingStream(_)))
                .count()
                == 2
        },
    );
    let incoming: Vec<StreamId> = server_events
        .iter()
        .filter_map(|event| match event {
            Completion::IncomingStream(stream) => Some(*stream),
            _ => None,
        })
        .collect();
    assert_eq!(incoming.len(), 2);
    for stream in incoming {
        server_handle
            .submissions()
            .try_receive(
                stream,
                BytesMut::with_capacity(64),
                ReceiveOptions::default(),
            )
            .unwrap();
    }
    client_events.clear();
    server_events.clear();

    drive_until(
        &mut client,
        &mut server,
        &client_handle,
        &server_handle,
        &mut client_events,
        &mut server_events,
        |client, server| {
            client
                .iter()
                .filter(|event| {
                    matches!(
                        event,
                        Completion::Send {
                            outcome: SendOutcome::Acknowledged { .. },
                            ..
                        }
                    )
                })
                .count()
                == 2
                && server
                    .iter()
                    .filter(|event| matches!(event, Completion::Receive(_)))
                    .count()
                    == 2
        },
    );
    let payloads: HashSet<Vec<u8>> = server_events
        .into_iter()
        .filter_map(|event| match event {
            Completion::Receive(receive) => Some(receive.data().to_vec()),
            _ => None,
        })
        .collect();
    assert_eq!(
        payloads,
        HashSet::from([b"first".to_vec(), b"second".to_vec()])
    );
    assert_eq!(client_handle.submissions().retained_send_bytes(), 0);
}

#[test]
fn authenticated_peer_must_match_the_requested_pid() {
    let (mut client, client_handle, mut server, server_handle, server_address, server_pid) =
        endpoint_pair();
    let expected = Pid::from_bytes([0xa5; chrysalis_core::PID_LEN]);
    assert_ne!(expected, server_pid);
    client_handle
        .commands()
        .try_connect(expected, server_address, "localhost")
        .unwrap();
    let mut client_events = Vec::new();
    let mut server_events = Vec::new();

    drive_until(
        &mut client,
        &mut server,
        &client_handle,
        &server_handle,
        &mut client_events,
        &mut server_events,
        |client, _| {
            client
                .iter()
                .any(|event| matches!(event, Completion::AuthenticationFailed(_)))
        },
    );

    let failure = client_events
        .iter()
        .find_map(|event| match event {
            Completion::AuthenticationFailed(failure) => Some(*failure),
            _ => None,
        })
        .unwrap();
    assert_eq!(failure.expected(), Some(expected));
    assert_eq!(failure.actual(), Some(server_pid));
    assert!(
        !client_events
            .iter()
            .any(|event| matches!(event, Completion::ConnectionEstablished(_)))
    );
    if let Some(connection) = client.network.connections.get_mut(&failure.connection()) {
        let mut later_events = Vec::new();
        assert!(!connection.progress(
            failure.connection(),
            &mut vec![0; u16::MAX as usize],
            &mut later_events,
        ));
        assert!(later_events.is_empty());
    }
}

#[test]
fn abort_rejects_new_work_and_returns_all_accepted_ownership() {
    let (mut client, client_handle, _server, _server_handle, server_address, server_pid) =
        endpoint_pair();
    let connection = client
        .connect(server_address, server_pid, Some(server_pid), "localhost")
        .unwrap();
    let stream = client.network.allocate_bidi(connection).unwrap();
    let receive = BytesMut::with_capacity(64);
    let receive_pointer = receive.as_ptr();
    client_handle
        .submissions()
        .try_send(stream, Bytes::from_static(b"unacknowledged"))
        .unwrap();
    client_handle
        .submissions()
        .try_receive(stream, receive, ReceiveOptions::default())
        .unwrap();
    client_handle.submissions().try_finish(stream).unwrap();
    client_handle
        .submissions()
        .try_discard(stream, NonZeroUsize::new(32).unwrap())
        .unwrap();

    client.abort();

    assert_eq!(client.shutdown_state(), ShutdownState::Stopped);
    assert!(client_handle.submissions().is_closed());
    assert_eq!(client_handle.submissions().retained_send_bytes(), 0);
    assert_eq!(client_handle.submissions().posted_receive_bytes(), 64);
    let rejected = client_handle
        .submissions()
        .try_send(stream, Bytes::from_static(b"rejected"))
        .expect_err("shutdown rejects new sends");
    assert_eq!(rejected.into_bytes(), Bytes::from_static(b"rejected"));

    let mut events = Vec::new();
    drain(&client_handle, &mut events);
    let returned = events
        .into_iter()
        .find_map(|event| match event {
            Completion::Receive(receive) => Some(receive.into_buffer()),
            _ => None,
        })
        .expect("abort returns posted receive buffer");
    assert_eq!(returned.as_ptr(), receive_pointer);
    assert_eq!(client_handle.submissions().posted_receive_bytes(), 0);
}

#[test]
fn graceful_shutdown_is_bounded_and_emits_terminal_completion() {
    let (mut client, client_handle, mut server, server_handle, server_address, server_pid) =
        endpoint_pair();
    let connection = client
        .connect(server_address, server_pid, Some(server_pid), "localhost")
        .unwrap();
    let mut client_events = Vec::new();
    let mut server_events = Vec::new();
    drive_until(
        &mut client,
        &mut server,
        &client_handle,
        &server_handle,
        &mut client_events,
        &mut server_events,
        |client, _| {
            client.iter().any(|event| {
                matches!(
                    event,
                    Completion::ConnectionEstablished(established)
                        if established.connection() == connection
                )
            })
        },
    );
    client_events.clear();
    server_events.clear();

    let shutdown = client_handle
        .commands()
        .try_shutdown(Duration::from_millis(100))
        .unwrap();
    assert_eq!(client.shutdown_state(), ShutdownState::Running);
    let deadline = Instant::now() + Duration::from_secs(2);
    while client.shutdown_state() != ShutdownState::Stopped {
        assert!(Instant::now() < deadline, "graceful shutdown timed out");
        client.poll(Duration::from_millis(1)).unwrap();
        server.poll(Duration::from_millis(1)).unwrap();
        drain(&client_handle, &mut client_events);
        drain(&server_handle, &mut server_events);
    }
    drain(&client_handle, &mut client_events);

    assert!(matches!(
        client_events.last(),
        Some(Completion::DriverStopped(driver)) if *driver == DriverId::from_u16(2)
    ));
    assert!(client_events.iter().any(|event| matches!(
        event,
        Completion::Command(completion)
            if completion.request() == shutdown.request()
                && completion.result() == CommandResult::ShutdownStarted
    )));
    assert!(matches!(
        client_handle
            .commands()
            .try_connect(server_pid, server_address, "localhost"),
        Err(chrysalis_transport_core::TryCommandError::Closed(_))
    ));
}

#[test]
fn shutdown_waits_for_completion_backpressure_before_stopping() {
    let (mut client, client_handle, _server, _server_handle, server_address, server_pid) =
        endpoint_pair_with_completion_capacity(1);
    let connection = client
        .connect(server_address, server_pid, Some(server_pid), "localhost")
        .unwrap();
    let stream = client.network.allocate_bidi(connection).unwrap();
    client_handle
        .submissions()
        .try_send(stream, Bytes::from_static(b"pending"))
        .unwrap();
    client_handle
        .submissions()
        .try_receive(
            stream,
            BytesMut::with_capacity(64),
            ReceiveOptions::default(),
        )
        .unwrap();
    client_handle.submissions().try_finish(stream).unwrap();

    client.abort();

    assert_eq!(client.shutdown_state(), ShutdownState::Draining);
    let mut events = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(2);
    while client.shutdown_state() != ShutdownState::Stopped {
        assert!(
            Instant::now() < deadline,
            "shutdown completion drain timed out"
        );
        drain(&client_handle, &mut events);
        client.poll(Duration::ZERO).unwrap();
    }
    drain(&client_handle, &mut events);

    assert!(events.iter().any(|event| matches!(
        event,
        Completion::Send {
            outcome: SendOutcome::Abandoned,
            ..
        }
    )));
    assert!(
        events
            .iter()
            .any(|event| matches!(event, Completion::Receive(_)))
    );
    assert!(events.iter().any(|event| matches!(
        event,
        Completion::Finish {
            outcome: ControlOutcome::Abandoned,
            ..
        }
    )));
    assert!(matches!(
        events.last(),
        Some(Completion::DriverStopped(driver)) if *driver == DriverId::from_u16(2)
    ));
    assert_eq!(client_handle.submissions().retained_send_bytes(), 0);
    assert_eq!(client_handle.submissions().posted_receive_bytes(), 64);
    drop(events);
    assert_eq!(client_handle.submissions().posted_receive_bytes(), 0);
}

#[test]
fn ordered_abort_abandons_earlier_work_and_closes_admission() {
    let (mut client, client_handle, _server, _server_handle, server_address, server_pid) =
        endpoint_pair();
    let connection = client
        .connect(server_address, server_pid, Some(server_pid), "localhost")
        .unwrap();
    let stream = StreamId::new(connection, 0);
    let send = client_handle
        .submissions()
        .try_send(stream, Bytes::from_static(b"accepted before abort"))
        .unwrap();
    let abort = client_handle.commands().try_abort().unwrap();
    assert!(matches!(
        client_handle.submissions().try_finish(stream),
        Err(TryControlError::Closed)
    ));

    client.poll(Duration::ZERO).unwrap();
    let mut events = Vec::new();
    drain(&client_handle, &mut events);

    let send_position = events
        .iter()
        .position(|event| {
            matches!(
                event,
                Completion::Send {
                    operation,
                    outcome: SendOutcome::Abandoned,
                    ..
                } if *operation == send.operation()
            )
        })
        .expect("accepted send should be abandoned");
    let abort_position = events
        .iter()
        .position(|event| {
            matches!(
                event,
                Completion::Command(completion)
                    if completion.request() == abort.request()
                        && completion.result() == CommandResult::AbortStarted
            )
        })
        .expect("abort command should complete");
    assert!(send_position < abort_position);
}

#[test]
fn completion_backpressure_bounds_internal_staging_until_reader_resumes() {
    let (mut client, client_handle, _server, _server_handle, server_address, server_pid) =
        endpoint_pair_with_completion_capacity(1);
    let connection = client
        .connect(server_address, server_pid, Some(server_pid), "localhost")
        .unwrap();
    for index in 0..submission_limits().queue_capacity().get() {
        client_handle
            .submissions()
            .try_finish(StreamId::new(connection, (index as u64) * 4))
            .unwrap();
    }
    assert!(matches!(
        client_handle
            .submissions()
            .try_finish(StreamId::new(connection, 1_000_000)),
        Err(TryControlError::WouldBlock(
            chrysalis_transport_core::ControlBlockReason::CompletionFull
        ))
    ));

    client.abort();
    let bound = submission_limits().queue_capacity().get() + EVENT_COMPLETION_CAPACITY;
    for _ in 0..4 {
        client.poll(Duration::from_millis(1)).unwrap();
        assert!(client.completion_backlog() <= bound);
    }

    let mut events = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(2);
    while client.shutdown_state() != ShutdownState::Stopped {
        assert!(Instant::now() < deadline, "completion recovery timed out");
        drain(&client_handle, &mut events);
        client.poll(Duration::from_millis(1)).unwrap();
    }
    drain(&client_handle, &mut events);
    assert!(events.iter().any(|event| matches!(
        event,
        Completion::DriverStopped(driver) if *driver == DriverId::from_u16(2)
    )));
}

#[test]
fn cancelling_posted_receive_returns_its_original_buffer() {
    let (mut client, client_handle, _server, _server_handle, server_address, server_pid) =
        endpoint_pair();
    let connection = client
        .connect(server_address, server_pid, Some(server_pid), "localhost")
        .unwrap();
    let stream = client.network.allocate_bidi(connection).unwrap();
    let buffer = BytesMut::with_capacity(64);
    let pointer = buffer.as_ptr();
    let receive = client_handle
        .submissions()
        .try_receive(stream, buffer, ReceiveOptions::default())
        .unwrap();
    let receive_operation = receive.operation();
    receive.cancellation().unwrap().cancel();

    client.poll(Duration::ZERO).unwrap();
    let mut events = Vec::new();
    drain(&client_handle, &mut events);
    let completion = events
        .into_iter()
        .find_map(|event| match event {
            Completion::Receive(receive)
                if receive.operation() == receive_operation
                    && receive.status() == ReceiveStatus::Cancelled =>
            {
                Some(receive)
            }
            _ => None,
        })
        .expect("cancelled receive should complete");
    assert_eq!(completion.into_buffer().as_ptr(), pointer);
}

#[test]
fn reset_fences_later_sends_and_stop_is_a_terminal_receive_control() {
    let (mut client, client_handle, mut server, server_handle, server_address, server_pid) =
        endpoint_pair();
    let connection = client
        .connect(server_address, server_pid, Some(server_pid), "localhost")
        .unwrap();
    let stream = client.network.allocate_bidi(connection).unwrap();
    let reset = client_handle.submissions().try_reset(stream, 11).unwrap();
    let late_send = client_handle
        .submissions()
        .try_send(stream, Bytes::from_static(b"late"))
        .unwrap();
    let stop = client_handle.submissions().try_stop(stream, 12).unwrap();

    let mut client_events = Vec::new();
    let mut server_events = Vec::new();
    drive_until(
        &mut client,
        &mut server,
        &client_handle,
        &server_handle,
        &mut client_events,
        &mut server_events,
        |client, _server| {
            client.iter().any(|event| {
                matches!(
                    event,
                    Completion::Reset { operation, .. } if *operation == reset.operation()
                )
            }) && client.iter().any(|event| {
                matches!(
                    event,
                    Completion::Send {
                        operation,
                        outcome: SendOutcome::Rejected,
                        ..
                    } if *operation == late_send.operation()
                )
            }) && client.iter().any(|event| {
                matches!(
                    event,
                    Completion::Stop { operation, .. } if *operation == stop.operation()
                )
            })
        },
    );
}
