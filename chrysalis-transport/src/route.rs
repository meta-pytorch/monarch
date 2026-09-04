/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::RwLock;
use std::task::Context;
use std::task::Poll;

use chrysalis_core::Pid;
use chrysalis_core::target_pid;

use crate::DatagramAddr;
use crate::DatagramSocket;
use crate::DatagramTransmit;

/// A terminal forwarding gate shared by every route derived from one link.
#[derive(Clone, Debug)]
pub struct RouteGate {
    inner: Arc<RouteGateInner>,
}

#[derive(Debug)]
struct RouteGateInner {
    state: Mutex<RouteGateState>,
    drained: Condvar,
}

#[derive(Debug)]
struct RouteGateState {
    active: bool,
    in_flight: usize,
}

impl Default for RouteGate {
    fn default() -> Self {
        Self::new()
    }
}

impl RouteGate {
    /// Constructs an active gate.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RouteGateInner {
                state: Mutex::new(RouteGateState {
                    active: true,
                    in_flight: 0,
                }),
                drained: Condvar::new(),
            }),
        }
    }

    /// Permanently closes the gate.
    ///
    /// This waits for admitted nonblocking sends to finish. After it returns,
    /// no route guarded by this gate can begin another send.
    pub fn close(&self) -> bool {
        let mut state = self.inner.state.lock().expect("route gate lock poisoned");
        let changed = std::mem::replace(&mut state.active, false);
        while state.in_flight != 0 {
            state = self
                .inner
                .drained
                .wait(state)
                .expect("route gate lock poisoned");
        }
        changed
    }

    /// Returns whether the gate has not been closed.
    pub fn is_active(&self) -> bool {
        self.inner
            .state
            .lock()
            .expect("route gate lock poisoned")
            .active
    }

    fn try_send(
        &self,
        socket: &dyn DatagramSocket,
        datagram: &[u8],
        destination: &DatagramAddr,
    ) -> io::Result<bool> {
        let Some(_permit) = self.acquire() else {
            return Ok(false);
        };
        socket.try_send_to(datagram, destination)?;
        Ok(true)
    }

    fn try_send_transmit(
        &self,
        socket: &dyn DatagramSocket,
        transmit: &DatagramTransmit<'_>,
    ) -> io::Result<Option<usize>> {
        let Some(_permit) = self.acquire() else {
            return Ok(None);
        };
        socket.try_send_transmit(transmit).map(Some)
    }

    fn acquire(&self) -> Option<RouteGatePermit<'_>> {
        let mut state = self.inner.state.lock().expect("route gate lock poisoned");
        if !state.active {
            return None;
        }
        state.in_flight += 1;
        Some(RouteGatePermit { gate: self })
    }
}

struct RouteGatePermit<'a> {
    gate: &'a RouteGate,
}

impl Drop for RouteGatePermit<'_> {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .inner
            .state
            .lock()
            .expect("route gate lock poisoned");
        state.in_flight -= 1;
        if state.in_flight == 0 {
            self.gate.inner.drained.notify_all();
        }
    }
}

/// A permanent or link-gated route to one destination address.
#[derive(Clone, Debug)]
pub struct Route {
    destination: DatagramAddr,
    gate: Option<RouteGate>,
}

impl Route {
    /// Constructs a route that remains active until removed from the table.
    pub fn permanent(destination: DatagramAddr) -> Self {
        Self {
            destination,
            gate: None,
        }
    }

    /// Constructs a route guarded by a terminal gate.
    pub fn gated(destination: DatagramAddr, gate: RouteGate) -> Self {
        Self {
            destination,
            gate: Some(gate),
        }
    }

    pub(crate) fn destination(&self) -> &DatagramAddr {
        &self.destination
    }

    pub(crate) fn try_with_destination<T>(
        &self,
        use_destination: impl FnOnce(&DatagramAddr) -> io::Result<T>,
    ) -> io::Result<Option<T>> {
        match &self.gate {
            Some(gate) => {
                let Some(_permit) = gate.acquire() else {
                    return Ok(None);
                };
                use_destination(&self.destination).map(Some)
            }
            None => use_destination(&self.destination).map(Some),
        }
    }

    fn try_send(&self, socket: &dyn DatagramSocket, datagram: &[u8]) -> io::Result<bool> {
        match &self.gate {
            Some(gate) => gate.try_send(socket, datagram, &self.destination),
            None => {
                socket.try_send_to(datagram, &self.destination)?;
                Ok(true)
            }
        }
    }

    fn try_send_transmit(
        &self,
        socket: &dyn DatagramSocket,
        transmit: &DatagramTransmit<'_>,
    ) -> io::Result<Option<usize>> {
        let transmit = DatagramTransmit {
            destination: &self.destination,
            ..*transmit
        };
        match &self.gate {
            Some(gate) => gate.try_send_transmit(socket, &transmit),
            None => socket.try_send_transmit(&transmit).map(Some),
        }
    }

    fn poll_send_ready(
        &self,
        cx: &mut Context<'_>,
        socket: &dyn DatagramSocket,
        transmit: &DatagramTransmit<'_>,
    ) -> Poll<io::Result<()>> {
        if self.gate.as_ref().is_some_and(|gate| !gate.is_active()) {
            return Poll::Ready(Ok(()));
        }
        let routed = DatagramTransmit {
            destination: &self.destination,
            ..*transmit
        };
        socket.poll_send_ready(cx, &routed)
    }
}

/// The result of routing one datagram.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ForwardDisposition {
    /// A prefix of the transmission was accepted by the selected next hop.
    Forwarded { target: Pid, count: usize },

    /// The datagram was deliberately dropped.
    Dropped(DropReason),
}

/// Why a router deliberately dropped a datagram.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DropReason {
    /// The datagram does not contain a routable Chrysalis QUIC DCID.
    Malformed,

    /// No route is installed for the destination PID.
    NoRoute { target: Pid },

    /// PID 0 reached the router instead of a link-local binding.
    UnboundLinkLocal,

    /// The route's terminal gate has closed.
    Inactive { target: Pid },

    /// A local PID binding could not accept another datagram.
    LocalQueueFull { target: Pid },

    /// The egress queue accepted no datagram.
    ///
    /// The resulting drop provides backpressure through QUIC loss recovery.
    /// A future transport may instead signal local congestion with ECN.
    EgressQueueFull { target: Pid },
}

/// A concurrent PID router backed by a mutable route table.
#[derive(Debug, Default)]
pub struct Router {
    routes: RwLock<HashMap<Pid, Route>>,
    default: RwLock<Option<Route>>,
}

impl Router {
    /// Constructs an empty router.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts `route`, returning the route that it replaced, if any.
    pub fn insert(&self, target: Pid, route: Route) -> Option<Route> {
        self.routes
            .write()
            .expect("routing table lock poisoned")
            .insert(target, route)
    }

    /// Removes and returns the route for `target`, if any.
    ///
    /// Close a route's gate before removal when removal must fence concurrent
    /// forwarding. Removal itself is only index cleanup.
    pub fn remove(&self, target: Pid) -> Option<Route> {
        self.routes
            .write()
            .expect("routing table lock poisoned")
            .remove(&target)
    }

    /// Looks up the current route for `target`.
    pub fn get(&self, target: Pid) -> Option<Route> {
        self.routes
            .read()
            .expect("routing table lock poisoned")
            .get(&target)
            .cloned()
    }

    /// Replaces the route used when no destination-specific route exists.
    pub fn set_default(&self, route: Route) -> Option<Route> {
        self.default
            .write()
            .expect("default route lock poisoned")
            .replace(route)
    }

    /// Removes and returns the current default route.
    pub fn remove_default(&self) -> Option<Route> {
        self.default
            .write()
            .expect("default route lock poisoned")
            .take()
    }

    /// Returns the current default route.
    pub fn default_route(&self) -> Option<Route> {
        self.default
            .read()
            .expect("default route lock poisoned")
            .clone()
    }

    pub(crate) fn poll_send_ready(
        &self,
        cx: &mut Context<'_>,
        socket: &dyn DatagramSocket,
        target: Pid,
        transmit: &DatagramTransmit<'_>,
    ) -> Poll<io::Result<()>> {
        if target.is_link_local() {
            return Poll::Ready(Ok(()));
        }
        let route = self.get(target).or_else(|| self.default_route());
        match route {
            Some(route) => route.poll_send_ready(cx, socket, transmit),
            None => Poll::Ready(Ok(())),
        }
    }

    /// Routes one complete QUIC datagram by its destination PID.
    pub fn forward(
        &self,
        socket: &dyn DatagramSocket,
        datagram: &[u8],
    ) -> io::Result<ForwardDisposition> {
        let Some(target) = target_pid(datagram) else {
            return Ok(ForwardDisposition::Dropped(DropReason::Malformed));
        };
        if target.is_link_local() {
            return Ok(ForwardDisposition::Dropped(DropReason::UnboundLinkLocal));
        }
        let route = self.get(target).or_else(|| self.default_route());
        let Some(route) = route else {
            return Ok(ForwardDisposition::Dropped(DropReason::NoRoute { target }));
        };
        match route.try_send(socket, datagram) {
            Ok(true) => Ok(ForwardDisposition::Forwarded { target, count: 1 }),
            Ok(false) => Ok(ForwardDisposition::Dropped(DropReason::Inactive { target })),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                Ok(ForwardDisposition::Dropped(DropReason::EgressQueueFull {
                    target,
                }))
            }
            Err(error) => Err(error),
        }
    }

    /// Routes one or more segmented QUIC datagrams.
    pub fn forward_transmit(
        &self,
        socket: &dyn DatagramSocket,
        transmit: &DatagramTransmit<'_>,
    ) -> io::Result<ForwardDisposition> {
        let segment_size = transmit.segment_size.unwrap_or(transmit.contents.len());
        if segment_size == 0 {
            return Ok(ForwardDisposition::Dropped(DropReason::Malformed));
        }
        let mut segments = transmit.contents.chunks(segment_size);
        let Some(first) = segments.next() else {
            return Ok(ForwardDisposition::Dropped(DropReason::Malformed));
        };
        let Some(target) = target_pid(first) else {
            return Ok(ForwardDisposition::Dropped(DropReason::Malformed));
        };
        if segments.any(|segment| target_pid(segment) != Some(target)) {
            let mut accepted = 0;
            let mut forwarded_target = target;
            for segment in transmit.contents.chunks(segment_size) {
                match self.forward(socket, segment) {
                    Ok(ForwardDisposition::Dropped(
                        reason @ DropReason::EgressQueueFull { .. },
                    )) => {
                        return if accepted == 0 {
                            Ok(ForwardDisposition::Dropped(reason))
                        } else {
                            Ok(ForwardDisposition::Forwarded {
                                target: forwarded_target,
                                count: accepted,
                            })
                        };
                    }
                    Ok(ForwardDisposition::Forwarded { target, .. }) => {
                        forwarded_target = target;
                        accepted += 1;
                    }
                    Ok(ForwardDisposition::Dropped(reason)) => {
                        return Ok(ForwardDisposition::Dropped(reason));
                    }
                    Err(_) if accepted > 0 => {
                        return Ok(ForwardDisposition::Forwarded {
                            target: forwarded_target,
                            count: accepted,
                        });
                    }
                    Err(error) => return Err(error),
                }
            }
            return Ok(ForwardDisposition::Forwarded {
                target: forwarded_target,
                count: accepted,
            });
        }
        if target.is_link_local() {
            return Ok(ForwardDisposition::Dropped(DropReason::UnboundLinkLocal));
        }
        let route = self.get(target).or_else(|| self.default_route());
        let Some(route) = route else {
            return Ok(ForwardDisposition::Dropped(DropReason::NoRoute { target }));
        };
        match route.try_send_transmit(socket, transmit) {
            Ok(Some(0)) => Ok(ForwardDisposition::Dropped(DropReason::EgressQueueFull {
                target,
            })),
            Ok(Some(count)) => Ok(ForwardDisposition::Forwarded { target, count }),
            Ok(None) => Ok(ForwardDisposition::Dropped(DropReason::Inactive { target })),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Barrier;
    use std::sync::Mutex;
    use std::sync::mpsc;
    use std::task::Context;
    use std::task::Poll;
    use std::thread;

    use chrysalis_core::ConnectionKey;
    use chrysalis_core::RoutedCid;
    use tokio::io::ReadBuf;

    use super::*;

    const PID: Pid = Pid::from_bytes([0x42; chrysalis_core::PID_LEN]);

    #[derive(Debug)]
    struct RecordingSocket {
        local: DatagramAddr,
        datagrams: Arc<Mutex<Vec<(DatagramAddr, Vec<u8>)>>>,
        send_limit: usize,
    }

    impl RecordingSocket {
        fn new() -> Self {
            Self {
                local: address(0),
                datagrams: Arc::new(Mutex::new(Vec::new())),
                send_limit: usize::MAX,
            }
        }

        fn backpressured() -> Self {
            Self {
                send_limit: 0,
                ..Self::new()
            }
        }

        fn accepting(send_limit: usize) -> Self {
            Self {
                send_limit,
                ..Self::new()
            }
        }
    }

    impl DatagramSocket for RecordingSocket {
        fn shutdown(&self) {}

        fn join(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async {})
        }

        fn local_addr(&self) -> &DatagramAddr {
            &self.local
        }

        fn try_send_to(&self, datagram: &[u8], destination: &DatagramAddr) -> io::Result<()> {
            if self.send_limit == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "test socket is backpressured",
                ));
            }
            self.datagrams
                .lock()
                .expect("recording socket lock poisoned")
                .push((destination.clone(), datagram.to_vec()));
            Ok(())
        }

        fn try_send_transmit(&self, transmit: &DatagramTransmit<'_>) -> io::Result<usize> {
            let segment_size = transmit.segment_size.unwrap_or(transmit.contents.len());
            if segment_size == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "test datagram segment size must be nonzero",
                ));
            }
            let mut sent = 0;
            for datagram in transmit.contents.chunks(segment_size).take(self.send_limit) {
                self.datagrams
                    .lock()
                    .expect("recording socket lock poisoned")
                    .push((transmit.destination.clone(), datagram.to_vec()));
                sent += 1;
            }
            Ok(sent)
        }

        fn poll_send_ready(
            &self,
            _cx: &mut Context<'_>,
            _transmit: &DatagramTransmit<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_recv_from(
            &self,
            _cx: &mut Context<'_>,
            _buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<DatagramAddr>> {
            Poll::Pending
        }
    }

    #[derive(Debug)]
    struct BlockingSocket {
        local: DatagramAddr,
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
    }

    impl DatagramSocket for BlockingSocket {
        fn shutdown(&self) {}

        fn join(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async {})
        }

        fn local_addr(&self) -> &DatagramAddr {
            &self.local
        }

        fn try_send_to(&self, _datagram: &[u8], _destination: &DatagramAddr) -> io::Result<()> {
            self.entered.wait();
            self.release.wait();
            Ok(())
        }

        fn poll_send_ready(
            &self,
            _cx: &mut Context<'_>,
            _transmit: &DatagramTransmit<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_recv_from(
            &self,
            _cx: &mut Context<'_>,
            _buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<DatagramAddr>> {
            Poll::Pending
        }
    }

    fn address(id: u8) -> DatagramAddr {
        DatagramAddr::new("test", [id])
    }

    fn datagram(target: Pid) -> Vec<u8> {
        let cid = RoutedCid::issued(target, ConnectionKey::from_u32(7));
        let mut datagram = vec![0x40];
        datagram.extend_from_slice(cid.as_bytes());
        datagram
    }

    #[test]
    fn permanent_route_forwards() {
        let router = Router::new();
        let socket = RecordingSocket::new();
        let recorded = socket.datagrams.clone();
        let destination = address(1);
        router.insert(PID, Route::permanent(destination.clone()));
        let datagram = datagram(PID);

        assert_eq!(
            router
                .forward(&socket, &datagram)
                .expect("forward datagram"),
            ForwardDisposition::Forwarded {
                target: PID,
                count: 1,
            }
        );
        assert_eq!(
            recorded
                .lock()
                .expect("recording socket lock poisoned")
                .as_slice(),
            &[(destination, datagram)]
        );
    }

    #[test]
    fn closed_gate_fences_every_derived_route() {
        let router = Router::new();
        let gate = RouteGate::new();
        let socket = RecordingSocket::new();
        let other_pid = Pid::from_bytes([0x24; chrysalis_core::PID_LEN]);
        router.insert(PID, Route::gated(address(1), gate.clone()));
        router.insert(other_pid, Route::gated(address(2), gate.clone()));

        assert!(gate.close());
        assert!(!gate.close());
        assert!(!gate.is_active());
        assert_eq!(
            router
                .forward(&socket, &datagram(PID))
                .expect("route closed gate"),
            ForwardDisposition::Dropped(DropReason::Inactive { target: PID })
        );
        assert_eq!(
            router
                .forward(&socket, &datagram(other_pid))
                .expect("route second closed gate"),
            ForwardDisposition::Dropped(DropReason::Inactive { target: other_pid })
        );
    }

    #[test]
    fn close_waits_for_admitted_send_and_rejects_new_sends() {
        let router = Arc::new(Router::new());
        let gate = RouteGate::new();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let socket = Arc::new(BlockingSocket {
            local: address(0),
            entered: entered.clone(),
            release: release.clone(),
        });
        router.insert(PID, Route::gated(address(1), gate.clone()));

        let forwarding_router = router.clone();
        let forwarding_socket = socket.clone();
        let forwarding = thread::spawn(move || {
            forwarding_router.forward(forwarding_socket.as_ref(), &datagram(PID))
        });
        entered.wait();

        let (closed_tx, closed_rx) = mpsc::channel();
        let closing_gate = gate.clone();
        let closing = thread::spawn(move || {
            closed_tx
                .send(closing_gate.close())
                .expect("report gate closure");
        });
        while gate.is_active() {
            thread::yield_now();
        }

        assert_eq!(closed_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        assert_eq!(
            router
                .forward(socket.as_ref(), &datagram(PID))
                .expect("route closed gate"),
            ForwardDisposition::Dropped(DropReason::Inactive { target: PID })
        );

        release.wait();
        assert_eq!(
            forwarding
                .join()
                .expect("forwarding thread")
                .expect("forward admitted datagram"),
            ForwardDisposition::Forwarded {
                target: PID,
                count: 1,
            }
        );
        assert!(closed_rx.recv().expect("gate closure result"));
        closing.join().expect("closing thread");
    }

    #[test]
    fn malformed_and_unknown_datagrams_are_dropped() {
        let router = Router::new();
        let socket = RecordingSocket::new();
        let datagram = datagram(PID);

        assert_eq!(
            router
                .forward(&socket, &[])
                .expect("drop malformed datagram"),
            ForwardDisposition::Dropped(DropReason::Malformed)
        );
        assert_eq!(
            router
                .forward(&socket, &datagram)
                .expect("drop unknown target"),
            ForwardDisposition::Dropped(DropReason::NoRoute { target: PID })
        );
    }

    #[test]
    fn default_route_handles_unknown_targets_and_exact_route_wins() {
        let router = Router::new();
        let socket = RecordingSocket::new();
        let other = Pid::from_bytes([0x24; chrysalis_core::PID_LEN]);
        router.set_default(Route::permanent(address(1)));
        router.insert(PID, Route::permanent(address(2)));

        assert_eq!(
            router.forward(&socket, &datagram(other)).unwrap(),
            ForwardDisposition::Forwarded {
                target: other,
                count: 1,
            }
        );
        assert_eq!(
            router.forward(&socket, &datagram(PID)).unwrap(),
            ForwardDisposition::Forwarded {
                target: PID,
                count: 1,
            }
        );
        assert_eq!(
            router.forward(&socket, &datagram(Pid::LINK_LOCAL)).unwrap(),
            ForwardDisposition::Dropped(DropReason::UnboundLinkLocal)
        );
        let datagrams = socket
            .datagrams
            .lock()
            .expect("recording socket lock poisoned");
        assert_eq!(datagrams[0].0, address(1));
        assert_eq!(datagrams[1].0, address(2));
        drop(datagrams);
        assert!(router.remove_default().is_some());
        assert!(router.default_route().is_none());
    }

    #[test]
    fn full_egress_queue_drops_for_quic_retransmission() {
        let router = Router::new();
        let socket = RecordingSocket::backpressured();
        router.insert(PID, Route::permanent(address(1)));

        assert_eq!(
            router
                .forward(&socket, &datagram(PID))
                .expect("route backpressured datagram"),
            ForwardDisposition::Dropped(DropReason::EgressQueueFull { target: PID })
        );
    }

    #[test]
    fn segmented_route_reports_accepted_prefix() {
        let router = Router::new();
        let socket = RecordingSocket::accepting(2);
        let destination = address(1);
        router.insert(PID, Route::permanent(destination.clone()));
        let mut segment = datagram(PID);
        segment.resize(64, 0);
        let contents = segment.repeat(4);
        let transmit = DatagramTransmit {
            destination: &address(0),
            contents: &contents,
            segment_size: Some(segment.len()),
            ecn: None,
            source_ip: None,
        };

        assert_eq!(
            router
                .forward_transmit(&socket, &transmit)
                .expect("forward segmented datagrams"),
            ForwardDisposition::Forwarded {
                target: PID,
                count: 2,
            }
        );
        assert_eq!(
            socket
                .datagrams
                .lock()
                .expect("recording socket lock poisoned")
                .as_slice(),
            &[
                (destination.clone(), segment.clone()),
                (destination, segment),
            ]
        );
    }
}
