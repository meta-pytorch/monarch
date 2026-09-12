/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::future::poll_fn;
use std::io;
use std::io::IoSliceMut;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use chrysalis_core::Pid;
use chrysalis_core::target_pid;
use tokio::io::ReadBuf;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::task::JoinHandle;

use crate::DatagramAddr;
use crate::DatagramRecvMeta;
use crate::DatagramSocket;
use crate::DatagramTransmit;
use crate::DropReason;
use crate::ForwardDisposition;
use crate::Router;
use crate::datagram::DATAGRAM_BATCH_SIZE;
use crate::shutdown::CompletionGuard;
use crate::shutdown::ShutdownState;

const DEFAULT_LOCAL_QUEUE_CAPACITY: usize = 1024;
const MAX_DATAGRAM_SIZE: usize = 65_535;
const FIRST_BINDING_ID: u64 = 1;

static NEXT_SWITCH_ID: AtomicU64 = AtomicU64::new(1);

/// A PID-aware switch over one physical datagram carrier.
///
/// Local PID bindings take precedence over routes. Non-local datagrams are delegated to the router
/// supplied at construction; route mutation remains the router owner's responsibility.
#[derive(Debug)]
pub struct DatagramSwitch<T: DatagramSocket> {
    carrier: Arc<T>,
    state: Arc<SwitchState>,
    shutdown_state: Arc<ShutdownState>,
    supervisor_task: JoinHandle<()>,
}

impl<T: DatagramSocket> DatagramSwitch<T> {
    /// Starts a switch over `carrier` using `router` for non-local destinations.
    ///
    /// The switch owns the lifecycle of the supplied carrier.
    pub fn spawn(carrier: Arc<T>, router: Arc<Router>) -> Self {
        let egress: Arc<dyn DatagramSocket> = carrier.clone();
        let state = Arc::new(SwitchState {
            id: NEXT_SWITCH_ID.fetch_add(1, Ordering::Relaxed),
            router,
            egress,
            bindings: RwLock::new(HashMap::new()),
            next_binding_id: AtomicU64::new(FIRST_BINDING_ID),
        });
        let shutdown_state = Arc::new(ShutdownState::default());
        let supervisor_task = tokio::spawn(run_switch(
            carrier.clone(),
            state.clone(),
            shutdown_state.clone(),
        ));
        Self {
            carrier,
            state,
            shutdown_state,
            supervisor_task,
        }
    }

    /// Creates a local datagram socket for `pid`.
    pub fn bind(&self, pid: Pid) -> io::Result<SwitchSocket> {
        self.bind_with_options(
            &[pid],
            NonZeroUsize::new(DEFAULT_LOCAL_QUEUE_CAPACITY)
                .expect("default local queue capacity is nonzero"),
            false,
        )
    }

    /// Creates a local socket that routes every outbound datagram by its destination CID.
    pub fn bind_routed(&self, pid: Pid) -> io::Result<SwitchSocket> {
        self.bind_with_options(
            &[pid],
            NonZeroUsize::new(DEFAULT_LOCAL_QUEUE_CAPACITY)
                .expect("default local queue capacity is nonzero"),
            true,
        )
    }

    /// Creates one routed local socket for several destination PIDs.
    pub fn bind_routed_many(&self, pids: &[Pid]) -> io::Result<SwitchSocket> {
        self.bind_with_options(
            pids,
            NonZeroUsize::new(DEFAULT_LOCAL_QUEUE_CAPACITY)
                .expect("default local queue capacity is nonzero"),
            true,
        )
    }

    /// Creates a local datagram binding with an explicit receive queue capacity.
    pub fn bind_with_queue_capacity(
        &self,
        pid: Pid,
        queue_capacity: NonZeroUsize,
    ) -> io::Result<SwitchSocket> {
        self.bind_with_options(&[pid], queue_capacity, false)
    }

    fn bind_with_options(
        &self,
        pids: &[Pid],
        queue_capacity: NonZeroUsize,
        route_outbound: bool,
    ) -> io::Result<SwitchSocket> {
        let Some(&pid) = pids.first() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "datagram switch binding requires at least one PID",
            ));
        };
        if !self.shutdown_state.is_running() {
            return Err(shutdown_error("datagram switch is shut down"));
        }
        let binding_id = self.state.next_binding_id.fetch_add(1, Ordering::Relaxed);
        let shutdown_state = Arc::new(ShutdownState::default());
        let (sender, receiver) = mpsc::channel(queue_capacity.get());
        self.state.register_many(
            pids,
            BindingRegistration {
                id: binding_id,
                sender,
                shutdown_state: shutdown_state.clone(),
            },
        )?;
        if !self.shutdown_state.is_running() {
            self.state.unbind_many(pids, binding_id);
            shutdown_state.shutdown();
            shutdown_state.terminate();
            return Err(shutdown_error("datagram switch is shut down"));
        }
        Ok(SwitchSocket {
            switch: self.state.clone(),
            pids: pids.into(),
            binding_id,
            datagram_addr: switched_addr(self.state.id, pid),
            route_outbound,
            receiver: Mutex::new(BindingReceiver {
                incoming: receiver,
                pending: VecDeque::new(),
            }),
            shutdown_state,
        })
    }

    /// Returns the physical carrier used by this switch.
    pub fn carrier(&self) -> &Arc<T> {
        &self.carrier
    }

    /// Idempotently requests immediate switch shutdown.
    pub fn shutdown(&self) {
        if self.shutdown_state.shutdown() {
            self.carrier.shutdown();
        }
    }

    /// Waits for local bindings, the receive task, and the physical carrier to terminate.
    pub async fn join(&self) {
        self.shutdown_state.join().await;
    }
}

impl<T: DatagramSocket> Drop for DatagramSwitch<T> {
    fn drop(&mut self) {
        self.shutdown();
        self.state.shutdown_bindings();
        self.supervisor_task.abort();
    }
}

/// A virtual datagram socket receiving packets for one local PID.
#[derive(Debug)]
pub struct SwitchSocket {
    switch: Arc<SwitchState>,
    pids: Box<[Pid]>,
    binding_id: u64,
    datagram_addr: DatagramAddr,
    route_outbound: bool,
    receiver: Mutex<BindingReceiver>,
    shutdown_state: Arc<ShutdownState>,
}

impl SwitchSocket {
    /// Returns the PID served by this binding.
    pub const fn pid(&self) -> Pid {
        self.pids[0]
    }

    /// Idempotently unregisters this PID from the switch.
    pub fn shutdown(&self) {
        if self.shutdown_state.shutdown() {
            self.switch.unbind_many(&self.pids, self.binding_id);
            self.shutdown_state.terminate();
        }
    }

    /// Waits until this binding has been unregistered.
    pub async fn join(&self) {
        self.shutdown_state.join().await;
    }
}

impl DatagramSocket for SwitchSocket {
    fn shutdown(&self) {
        SwitchSocket::shutdown(self);
    }

    fn join(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(SwitchSocket::join(self))
    }

    fn local_addr(&self) -> &DatagramAddr {
        &self.datagram_addr
    }

    fn try_send_to(&self, datagram: &[u8], destination: &DatagramAddr) -> io::Result<()> {
        match self.try_send_transmit(&DatagramTransmit {
            destination,
            contents: datagram,
            segment_size: None,
            ecn: None,
            source_ip: None,
        })? {
            0 => Err(io::ErrorKind::WouldBlock.into()),
            1 => Ok(()),
            _ => unreachable!("one datagram cannot accept multiple segments"),
        }
    }

    fn try_send_transmit(&self, transmit: &DatagramTransmit<'_>) -> io::Result<usize> {
        if !self.shutdown_state.is_running() {
            return Err(shutdown_error("switch binding is shut down"));
        }
        if self.route_outbound {
            if transmit
                .contents
                .get(..transmit.segment_size.unwrap_or(transmit.contents.len()))
                .and_then(target_pid)
                .is_some_and(Pid::is_link_local)
            {
                return self.switch.egress.try_send_transmit(transmit);
            }
            return self
                .switch
                .dispatch_transmit(transmit, self.datagram_addr.clone())
                .map(|(_, accepted)| accepted);
        }
        let Some((switch, destination_pid)) = parse_switched_addr(transmit.destination)? else {
            return self.switch.egress.try_send_transmit(transmit);
        };
        if switch != self.switch.id {
            return Err(io::Error::new(
                io::ErrorKind::HostUnreachable,
                "destination belongs to another datagram switch",
            ));
        }
        let segment_size = transmit.segment_size.unwrap_or(transmit.contents.len());
        if segment_size == 0
            || transmit
                .contents
                .chunks(segment_size)
                .any(|segment| target_pid(segment) != Some(destination_pid))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "destination PID does not match datagram CID",
            ));
        }
        self.switch
            .dispatch_transmit(transmit, self.datagram_addr.clone())
            .map(|(_, accepted)| accepted)
    }

    fn poll_send_ready(
        &self,
        cx: &mut Context<'_>,
        transmit: &DatagramTransmit<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.shutdown_state.is_running() {
            return Poll::Ready(Err(shutdown_error("switch binding is shut down")));
        }
        self.shutdown_state.register_waker(cx.waker());
        if !self.shutdown_state.is_running() {
            return Poll::Ready(Err(shutdown_error("switch binding is shut down")));
        }
        self.switch.egress.poll_send_ready(cx, transmit)
    }

    fn poll_recv_from(
        &self,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<DatagramAddr>> {
        if !self.shutdown_state.is_running() {
            return Poll::Ready(Err(shutdown_error("switch binding is shut down")));
        }
        let mut receiver = self
            .receiver
            .lock()
            .expect("switch binding receiver lock poisoned");
        let mut datagram = match receiver.poll_recv(cx) {
            Poll::Ready(Some(datagram)) => datagram,
            Poll::Ready(None) => {
                self.shutdown();
                return Poll::Ready(Err(shutdown_error("datagram switch stopped")));
            }
            Poll::Pending => return Poll::Pending,
        };
        if datagram.bytes.len() > datagram.stride {
            let remainder = datagram.bytes.split_off(datagram.stride);
            receiver.pending.push_front(SwitchedDatagram {
                bytes: remainder,
                source: datagram.source.clone(),
                stride: datagram.stride,
                ecn: datagram.ecn,
            });
        }
        if datagram.bytes.len() > buffer.remaining() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "datagram exceeds receive buffer",
            )));
        }
        buffer.put_slice(&datagram.bytes);
        Poll::Ready(Ok(datagram.source))
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        buffers: &mut [IoSliceMut<'_>],
        meta: &mut [DatagramRecvMeta],
    ) -> Poll<io::Result<usize>> {
        if !self.shutdown_state.is_running() {
            return Poll::Ready(Err(shutdown_error("switch binding is shut down")));
        }
        let capacity = buffers.len().min(meta.len());
        if capacity == 0 {
            return Poll::Ready(Ok(0));
        }
        let mut receiver = self
            .receiver
            .lock()
            .expect("switch binding receiver lock poisoned");
        let first = match receiver.poll_recv(cx) {
            Poll::Ready(Some(datagram)) => datagram,
            Poll::Ready(None) => {
                self.shutdown();
                return Poll::Ready(Err(shutdown_error("datagram switch stopped")));
            }
            Poll::Pending => return Poll::Pending,
        };
        let mut next = Some(first);
        let mut count = 0;
        while count < capacity {
            let Some(datagram) = next.take() else {
                break;
            };
            if datagram.bytes.len() > buffers[count].len() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "datagram batch exceeds receive buffer",
                )));
            }
            buffers[count][..datagram.bytes.len()].copy_from_slice(&datagram.bytes);
            meta[count] = DatagramRecvMeta {
                source: datagram.source,
                len: datagram.bytes.len(),
                stride: datagram.stride,
                ecn: datagram.ecn,
                destination_ip: None,
            };
            count += 1;
            next = receiver.try_recv();
        }
        Poll::Ready(Ok(count))
    }

    fn max_transmit_segments(&self) -> usize {
        if self.route_outbound {
            self.switch.egress.max_transmit_segments()
        } else {
            1
        }
    }

    fn max_receive_segments(&self) -> usize {
        self.switch.egress.max_receive_segments()
    }

    fn may_fragment(&self) -> bool {
        self.switch.egress.may_fragment()
    }
}

impl Drop for SwitchSocket {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[derive(Clone, Debug)]
struct BindingRegistration {
    id: u64,
    sender: mpsc::Sender<SwitchedDatagram>,
    shutdown_state: Arc<ShutdownState>,
}

#[derive(Debug)]
struct SwitchedDatagram {
    bytes: Vec<u8>,
    source: DatagramAddr,
    stride: usize,
    ecn: Option<u8>,
}

#[derive(Debug)]
struct BindingReceiver {
    incoming: mpsc::Receiver<SwitchedDatagram>,
    pending: VecDeque<SwitchedDatagram>,
}

impl BindingReceiver {
    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<SwitchedDatagram>> {
        match self.pending.pop_front() {
            Some(datagram) => Poll::Ready(Some(datagram)),
            None => self.incoming.poll_recv(cx),
        }
    }

    fn try_recv(&mut self) -> Option<SwitchedDatagram> {
        self.pending
            .pop_front()
            .or_else(|| self.incoming.try_recv().ok())
    }
}

#[derive(Debug)]
struct SwitchState {
    id: u64,
    router: Arc<Router>,
    egress: Arc<dyn DatagramSocket>,
    bindings: RwLock<HashMap<Pid, BindingRegistration>>,
    next_binding_id: AtomicU64,
}

impl SwitchState {
    fn register_many(&self, pids: &[Pid], registration: BindingRegistration) -> io::Result<()> {
        let mut bindings = self
            .bindings
            .write()
            .expect("switch binding table lock poisoned");
        if pids.iter().any(|pid| bindings.contains_key(pid))
            || pids
                .iter()
                .enumerate()
                .any(|(index, pid)| pids[..index].contains(pid))
        {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                "PID is already bound to this switch binding",
            ));
        }
        for &pid in pids {
            let Entry::Vacant(entry) = bindings.entry(pid) else {
                unreachable!("binding conflicts were checked while holding the write lock");
            };
            entry.insert(registration.clone());
        }
        Ok(())
    }

    fn unbind(&self, pid: Pid, binding_id: u64) {
        let mut bindings = self
            .bindings
            .write()
            .expect("switch binding table lock poisoned");
        if bindings
            .get(&pid)
            .is_some_and(|binding| binding.id == binding_id)
        {
            bindings.remove(&pid);
        }
    }

    fn unbind_many(&self, pids: &[Pid], binding_id: u64) {
        for &pid in pids {
            self.unbind(pid, binding_id);
        }
    }

    fn dispatch(&self, datagram: &[u8], source: DatagramAddr) -> io::Result<ForwardDisposition> {
        let Some(target) = target_pid(datagram) else {
            return Ok(ForwardDisposition::Dropped(DropReason::Malformed));
        };
        if let Some(disposition) =
            self.dispatch_local(target, datagram, source, datagram.len(), None)
        {
            return Ok(disposition);
        }
        self.router.forward(self.egress.as_ref(), datagram)
    }

    fn dispatch_transmit(
        &self,
        transmit: &DatagramTransmit<'_>,
        source: DatagramAddr,
    ) -> io::Result<(ForwardDisposition, usize)> {
        let segment_count = transmit.segment_count();
        if transmit.segment_count() == 1 {
            let disposition = self.dispatch(transmit.contents, source)?;
            let accepted = match disposition {
                ForwardDisposition::Forwarded { count, .. } => count,
                ForwardDisposition::Dropped(
                    DropReason::EgressQueueFull { .. } | DropReason::LocalQueueFull { .. },
                ) => 0,
                _ => 1,
            };
            return Ok((disposition, accepted));
        }
        let segment_size = transmit
            .segment_size
            .expect("multiple segments have a stride");
        let mut segments = transmit.contents.chunks(segment_size);
        let Some(first) = segments.next() else {
            return Ok((ForwardDisposition::Dropped(DropReason::Malformed), 0));
        };
        let Some(target) = target_pid(first) else {
            return Ok((
                ForwardDisposition::Dropped(DropReason::Malformed),
                segment_count,
            ));
        };
        if segments.any(|segment| target_pid(segment) != Some(target)) {
            let mut disposition = ForwardDisposition::Dropped(DropReason::Malformed);
            let mut accepted = 0;
            for segment in transmit.contents.chunks(segment_size) {
                match self.dispatch(segment, source.clone()) {
                    Ok(ForwardDisposition::Dropped(
                        DropReason::EgressQueueFull { .. } | DropReason::LocalQueueFull { .. },
                    )) => break,
                    Ok(next) => {
                        disposition = next;
                        accepted += 1;
                    }
                    Err(_) if accepted > 0 => break,
                    Err(error) => return Err(error),
                }
            }
            return Ok((disposition, accepted));
        }
        if let Some(disposition) = self.dispatch_local(
            target,
            transmit.contents,
            source,
            segment_size,
            transmit.ecn,
        ) {
            let accepted = match disposition {
                ForwardDisposition::Forwarded { count, .. } => count,
                ForwardDisposition::Dropped(DropReason::LocalQueueFull { .. }) => 0,
                _ => segment_count,
            };
            return Ok((disposition, accepted));
        }
        let disposition = self
            .router
            .forward_transmit(self.egress.as_ref(), transmit)?;
        let accepted = match disposition {
            ForwardDisposition::Forwarded { count, .. } => count,
            ForwardDisposition::Dropped(DropReason::EgressQueueFull { .. }) => 0,
            _ => segment_count,
        };
        Ok((disposition, accepted))
    }

    fn dispatch_local(
        &self,
        target: Pid,
        bytes: &[u8],
        source: DatagramAddr,
        stride: usize,
        ecn: Option<u8>,
    ) -> Option<ForwardDisposition> {
        let local = self
            .bindings
            .read()
            .expect("switch binding table lock poisoned")
            .get(&target)
            .cloned()?;
        if !local.shutdown_state.is_running() {
            self.unbind(target, local.id);
            return None;
        }
        match local.sender.try_send(SwitchedDatagram {
            bytes: bytes.to_vec(),
            source,
            stride,
            ecn,
        }) {
            Ok(()) => Some(ForwardDisposition::Forwarded {
                target,
                count: bytes.len().div_ceil(stride),
            }),
            Err(TrySendError::Full(_)) => {
                Some(ForwardDisposition::Dropped(DropReason::LocalQueueFull {
                    target,
                }))
            }
            Err(TrySendError::Closed(_)) => {
                self.unbind(target, local.id);
                None
            }
        }
    }

    fn is_local(&self, target: Pid) -> bool {
        self.bindings
            .read()
            .expect("switch binding table lock poisoned")
            .get(&target)
            .is_some_and(|binding| binding.shutdown_state.is_running())
    }

    fn shutdown_bindings(&self) {
        let bindings = std::mem::take(
            &mut *self
                .bindings
                .write()
                .expect("switch binding table lock poisoned"),
        );
        for binding in bindings.into_values() {
            binding.shutdown_state.shutdown();
            binding.shutdown_state.terminate();
        }
    }
}

async fn run_switch<T: DatagramSocket>(
    carrier: Arc<T>,
    state: Arc<SwitchState>,
    shutdown_state: Arc<ShutdownState>,
) {
    let _completion = CompletionGuard::new(&shutdown_state);
    let mut storage = vec![0; MAX_DATAGRAM_SIZE * DATAGRAM_BATCH_SIZE];
    let mut buffers = storage
        .chunks_mut(MAX_DATAGRAM_SIZE)
        .map(IoSliceMut::new)
        .collect::<Vec<_>>();
    let mut meta = vec![DatagramRecvMeta::default(); DATAGRAM_BATCH_SIZE];
    let mut regrouped = Vec::with_capacity(MAX_DATAGRAM_SIZE);
    loop {
        let count = tokio::select! {
            _ = shutdown_state.cancelled() => break,
            received = poll_fn(|cx| carrier.poll_recv(cx, &mut buffers, &mut meta)) => match received {
                Ok(received) => received,
                Err(_) => {
                    shutdown_state.shutdown();
                    break;
                }
            },
        };
        let mut index = 0;
        while index < count {
            let received = &meta[index];
            let datagram = &buffers[index][..received.len];
            if received.stride < received.len {
                let _ = state.dispatch_transmit(
                    &DatagramTransmit {
                        destination: carrier.local_addr(),
                        contents: datagram,
                        segment_size: Some(received.stride),
                        ecn: received.ecn,
                        source_ip: None,
                    },
                    received.source.clone(),
                );
                index += 1;
                continue;
            }
            let Some(target) = target_pid(datagram) else {
                let _ = state.dispatch(datagram, received.source.clone());
                index += 1;
                continue;
            };
            if state.is_local(target) {
                let _ = state.dispatch(datagram, received.source.clone());
                index += 1;
                continue;
            }
            regrouped.clear();
            regrouped.extend_from_slice(datagram);
            let segment_size = datagram.len();
            let mut end = index + 1;
            while end < count
                && end - index < DATAGRAM_BATCH_SIZE
                && meta[end].len == segment_size
                && meta[end].stride == segment_size
                && meta[end].ecn == received.ecn
                && meta[end].source == received.source
                && target_pid(&buffers[end][..meta[end].len]) == Some(target)
            {
                regrouped.extend_from_slice(&buffers[end][..meta[end].len]);
                end += 1;
            }
            let _ = state.dispatch_transmit(
                &DatagramTransmit {
                    destination: carrier.local_addr(),
                    contents: &regrouped,
                    segment_size: (end - index > 1).then_some(segment_size),
                    ecn: received.ecn,
                    source_ip: None,
                },
                received.source.clone(),
            );
            index = end;
        }
    }
    state.shutdown_bindings();
    carrier.shutdown();
    carrier.join().await;
}

fn switched_addr(switch: u64, pid: Pid) -> DatagramAddr {
    let mut bytes = Vec::with_capacity(8 + chrysalis_core::PID_LEN);
    bytes.extend_from_slice(&switch.to_be_bytes());
    bytes.extend_from_slice(pid.as_bytes());
    DatagramAddr::new("switch", bytes)
}

fn parse_switched_addr(address: &DatagramAddr) -> io::Result<Option<(u64, Pid)>> {
    if address.scheme() != "switch" {
        return Ok(None);
    }
    let bytes = address.opaque();
    if bytes.len() != 8 + chrysalis_core::PID_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid datagram switch address",
        ));
    }
    let switch = u64::from_be_bytes(bytes[..8].try_into().expect("checked length"));
    let pid = Pid::from_bytes(bytes[8..].try_into().expect("checked length"));
    Ok(Some((switch, pid)))
}

fn shutdown_error(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, message)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrysalis_core::ConnectionKey;
    use chrysalis_core::RoutedCid;
    use tokio::time::timeout;

    use super::*;
    use crate::InprocNetwork;
    use crate::Route;

    const TARGET: Pid = Pid::from_bytes([0x42; chrysalis_core::PID_LEN]);
    const SOURCE: Pid = Pid::from_bytes([0x24; chrysalis_core::PID_LEN]);

    fn datagram(target: Pid) -> Vec<u8> {
        let cid = RoutedCid::issued(target, ConnectionKey::from_u32(7));
        let mut datagram = vec![0x40];
        datagram.extend_from_slice(cid.as_bytes());
        datagram
    }

    #[tokio::test]
    async fn forwards_nonlocal_datagrams_through_router() {
        let network = InprocNetwork::new(NonZeroUsize::new(32).expect("nonzero capacity"));
        let source = network.bind(1).expect("bind source");
        let switch_carrier = Arc::new(network.bind(2).expect("bind switch"));
        let destination = network.bind(3).expect("bind destination");
        let switch_address = switch_carrier.local_addr().clone();
        let router = Arc::new(Router::new());
        router.insert(
            TARGET,
            crate::Route::permanent(destination.local_addr().clone()),
        );
        let datagram_switch = DatagramSwitch::spawn(switch_carrier, router);
        let expected = datagram(TARGET);

        source
            .try_send_to(&expected, &switch_address)
            .expect("send to switch");
        let mut buffer = [0; 64];
        let (len, _) = timeout(Duration::from_secs(1), destination.recv_from(&mut buffer))
            .await
            .expect("forward timed out")
            .expect("receive forwarded datagram");
        assert_eq!(&buffer[..len], expected);

        datagram_switch.shutdown();
        timeout(Duration::from_secs(1), datagram_switch.join())
            .await
            .expect("switch join timed out");
        let _replacement = network.bind(2).expect("rebind switch carrier");
    }

    #[tokio::test]
    async fn local_switch_sockets_exchange_datagrams_without_egress() {
        let network = InprocNetwork::new(NonZeroUsize::new(32).expect("nonzero capacity"));
        let carrier = Arc::new(network.bind(1).expect("bind switch carrier"));
        let datagram_switch = DatagramSwitch::spawn(carrier, Arc::new(Router::new()));
        let source = datagram_switch.bind(SOURCE).expect("bind source PID");
        let destination = datagram_switch.bind(TARGET).expect("bind destination PID");
        let expected = datagram(TARGET);

        assert_eq!(
            source
                .try_send_to(&expected, source.local_addr())
                .expect_err("reject destination and CID mismatch")
                .kind(),
            io::ErrorKind::InvalidInput
        );
        source
            .try_send_to(&expected, destination.local_addr())
            .expect("send local datagram");
        let mut buffer = [0; 64];
        let (len, source_addr) =
            timeout(Duration::from_secs(1), destination.recv_from(&mut buffer))
                .await
                .expect("local delivery timed out")
                .expect("receive local datagram");

        assert_eq!(&buffer[..len], expected);
        assert_eq!(&source_addr, source.local_addr());
        datagram_switch.shutdown();
        timeout(Duration::from_secs(1), datagram_switch.join())
            .await
            .expect("switch join timed out");
    }

    #[tokio::test]
    async fn local_switch_preserves_segmented_batches() {
        let network = InprocNetwork::new(NonZeroUsize::new(32).expect("nonzero capacity"));
        let carrier = Arc::new(network.bind(1).expect("bind switch carrier"));
        let datagram_switch = DatagramSwitch::spawn(carrier, Arc::new(Router::new()));
        let source = datagram_switch.bind(SOURCE).expect("bind source PID");
        let destination = datagram_switch.bind(TARGET).expect("bind destination PID");
        let mut segment = datagram(TARGET);
        segment.resize(64, 0);
        let contents = segment.repeat(4);

        assert_eq!(
            source
                .try_send_transmit(&DatagramTransmit {
                    destination: destination.local_addr(),
                    contents: &contents,
                    segment_size: Some(segment.len()),
                    ecn: Some(2),
                    source_ip: None,
                })
                .expect("send local batch"),
            4
        );
        let mut storage = [0; 512];
        let mut buffers = [IoSliceMut::new(&mut storage)];
        let mut meta = [DatagramRecvMeta::default()];
        let count = poll_fn(|cx| destination.poll_recv(cx, &mut buffers, &mut meta))
            .await
            .expect("receive local batch");

        assert_eq!(count, 1);
        assert_eq!(&buffers[0][..meta[0].len], contents);
        assert_eq!(meta[0].stride, segment.len());
        assert_eq!(meta[0].ecn, Some(2));
        assert_eq!(meta[0].source, *source.local_addr());

        datagram_switch.shutdown();
        timeout(Duration::from_secs(1), datagram_switch.join())
            .await
            .expect("switch join timed out");
    }

    #[tokio::test]
    async fn single_receive_splits_local_segmented_batches() {
        let network = InprocNetwork::new(NonZeroUsize::new(32).expect("nonzero capacity"));
        let carrier = Arc::new(network.bind(1).expect("bind switch carrier"));
        let datagram_switch = DatagramSwitch::spawn(carrier, Arc::new(Router::new()));
        let source = datagram_switch.bind(SOURCE).expect("bind source PID");
        let destination = datagram_switch.bind(TARGET).expect("bind destination PID");
        let mut segment = datagram(TARGET);
        segment.resize(64, 0);
        let contents = segment.repeat(4);

        assert_eq!(
            source
                .try_send_transmit(&DatagramTransmit {
                    destination: destination.local_addr(),
                    contents: &contents,
                    segment_size: Some(segment.len()),
                    ecn: None,
                    source_ip: None,
                })
                .expect("send local batch"),
            4
        );
        let mut buffer = [0; 64];
        for _ in 0..4 {
            let (len, source_addr) = destination
                .recv_from(&mut buffer)
                .await
                .expect("receive local datagram");
            assert_eq!(&buffer[..len], segment);
            assert_eq!(source_addr, *source.local_addr());
        }

        datagram_switch.shutdown();
        timeout(Duration::from_secs(1), datagram_switch.join())
            .await
            .expect("switch join timed out");
    }

    #[tokio::test]
    async fn routed_binding_uses_cid_instead_of_supplied_address() {
        let network = InprocNetwork::new(NonZeroUsize::new(32).expect("nonzero capacity"));
        let switch_carrier = Arc::new(network.bind(1).expect("bind switch carrier"));
        let destination = network.bind(2).expect("bind destination");
        let advertised = network.bind(3).expect("bind advertised destination");
        let router = Arc::new(Router::new());
        router.insert(TARGET, Route::permanent(destination.local_addr().clone()));
        let datagram_switch = DatagramSwitch::spawn(switch_carrier, router);
        let source = datagram_switch
            .bind_routed(SOURCE)
            .expect("bind routed source PID");
        let expected = datagram(TARGET);

        source
            .try_send_to(&expected, advertised.local_addr())
            .expect("route outbound datagram");
        let mut buffer = [0; 64];
        let (len, _) = timeout(Duration::from_secs(1), destination.recv_from(&mut buffer))
            .await
            .expect("route delivery timed out")
            .expect("receive routed datagram");

        assert_eq!(&buffer[..len], expected);
        datagram_switch.shutdown();
        timeout(Duration::from_secs(1), datagram_switch.join())
            .await
            .expect("switch join timed out");
    }

    #[tokio::test]
    async fn local_binding_precedes_route_and_can_be_rebound() {
        let network = InprocNetwork::new(NonZeroUsize::new(32).expect("nonzero capacity"));
        let source = network.bind(1).expect("bind source");
        let switch_carrier = Arc::new(network.bind(2).expect("bind switch"));
        let switch_address = switch_carrier.local_addr().clone();
        let routed_destination = network.bind(3).expect("bind routed destination");
        let router = Arc::new(Router::new());
        router.insert(
            TARGET,
            Route::permanent(routed_destination.local_addr().clone()),
        );
        let datagram_switch = DatagramSwitch::spawn(switch_carrier, router);
        let binding = datagram_switch.bind(TARGET).expect("bind local PID");
        assert_eq!(binding.pid(), TARGET);
        assert_eq!(
            datagram_switch
                .bind(TARGET)
                .expect_err("reject duplicate local PID")
                .kind(),
            io::ErrorKind::AddrInUse
        );
        let mut expected = datagram(TARGET);
        expected.push(1);

        source
            .try_send_to(&expected, &switch_address)
            .expect("send to switch");
        let mut buffer = [0; 64];
        let (len, _) = timeout(Duration::from_secs(1), binding.recv_from(&mut buffer))
            .await
            .expect("local delivery timed out")
            .expect("receive local datagram");
        assert_eq!(&buffer[..len], expected);

        binding.shutdown();
        binding.join().await;
        let mut routed = datagram(TARGET);
        routed.push(2);
        source
            .try_send_to(&routed, &switch_address)
            .expect("send routed datagram to switch");
        let (len, _) = timeout(
            Duration::from_secs(1),
            routed_destination.recv_from(&mut buffer),
        )
        .await
        .expect("route delivery timed out")
        .expect("receive routed datagram");
        assert_eq!(&buffer[..len], routed);

        let rebound = datagram_switch.bind(TARGET).expect("rebind local PID");
        rebound.shutdown();
        rebound.join().await;
        datagram_switch.shutdown();
        timeout(Duration::from_secs(1), datagram_switch.join())
            .await
            .expect("switch join timed out");
    }
}
