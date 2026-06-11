/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Connectivity layer for Hyperactor procs.
//!
//! A proc by itself is an isolated actor runtime. It owns local actor
//! lifecycle and mailboxes, but it communicates with other procs by
//! attaching to a gateway. The gateway encapsulates the proc's connectivity
//! layer: it gives attached procs an advertised location, accepts inbound
//! traffic for that location, and forwards outbound traffic to destinations
//! outside the proc.
//!
//! This separation lets us compose different topologies without changing
//! proc identity. A host can attach all of its procs to one gateway, so the
//! gateway multiplexes ingress to those procs and routes egress on their
//! behalf. A proc from a foreign host can also attach through another host's
//! gateway, inheriting that host's advertised location while still retaining
//! its own proc id. Gateways can also act as pure proxies when they do not
//! own any local procs.
//!
//! From the channel/connectivity perspective, each location has one gateway.
//! Operationally, a gateway is both a proc multiplexer for ingress and a
//! router for egress.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::sync::Weak;

use async_trait::async_trait;
use futures::StreamExt as _;

use crate::Location;
use crate::PortAddr;
use crate::ProcId;
use crate::channel;
use crate::channel::ChannelAddr;
use crate::channel::ChannelError;
use crate::channel::ChannelTransport;
use crate::id::Uid;
use crate::mailbox::BoxedMailboxSender;
use crate::mailbox::DeliveryFailure;
use crate::mailbox::DialMailboxRouter;
use crate::mailbox::IntoBoxedMailboxSender as _;
use crate::mailbox::MailboxSender as _;
use crate::mailbox::MailboxServer as _;
use crate::mailbox::MailboxServerError;
use crate::mailbox::MailboxServerHandle;
use crate::mailbox::MessageEnvelope;
use crate::mailbox::PortHandle;
use crate::mailbox::TransportFailure;
use crate::mailbox::TransportFailureReason;
use crate::mailbox::Undeliverable;
use crate::mailbox::UndeliverableReason;
use crate::mailbox::UnroutableMailboxSender;
use crate::proc::Proc;
use crate::proc::WeakProc;

/// Connectivity boundary for one or more procs.
#[derive(Clone)]
pub struct Gateway {
    inner: Arc<GatewayState>,
}

/// Handle returned by [`Gateway::attach_proc`] that detaches the proc
/// (removing its local-delivery registration) when dropped. This is the
/// sole remover of the entry; it runs from `ProcState::drop`, so by the
/// time it fires the proc's [`WeakProc`] no longer upgrades. Drop is a
/// no-op if the gateway has already been dropped.
///
/// Removal is identity-guarded: it removes the entry only if the slot
/// still holds *this* proc's registration. If a same-id proc was rebuilt
/// after ours died, [`Gateway::attach_proc`] replaced our dead entry
/// with the new proc's, so the slot is no longer ours — we leave it
/// untouched rather than evict the successor's live registration.
#[must_use = "dropping the AttachedProcGuard immediately detaches the proc"]
pub struct AttachedProcGuard {
    gateway: Weak<GatewayState>,
    proc_id: ProcId,
    weak: WeakProc,
}

impl fmt::Debug for AttachedProcGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AttachedProcGuard")
            .field("proc_id", &self.proc_id)
            .finish()
    }
}

impl Drop for AttachedProcGuard {
    fn drop(&mut self) {
        let Some(state) = self.gateway.upgrade() else {
            return;
        };
        let mut locals = state.locals.write().unwrap();
        if locals
            .get(&self.proc_id)
            .is_some_and(|weak| weak.ptr_eq(&self.weak))
        {
            locals.remove(&self.proc_id);
        }
    }
}

/// Handle returned by [`Gateway::attach_peer`] that removes the peer
/// entry from the gateway's `peers` map when dropped.
#[must_use = "dropping the PeerAttachGuard immediately removes the peer entry"]
pub struct PeerAttachGuard {
    gateway: Weak<GatewayState>,
    uid: Uid,
}

impl fmt::Debug for PeerAttachGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerAttachGuard")
            .field("uid", &self.uid)
            .finish()
    }
}

impl Drop for PeerAttachGuard {
    fn drop(&mut self) {
        let Some(state) = self.gateway.upgrade() else {
            return;
        };
        state.peers.write().unwrap().remove(&self.uid);
    }
}

/// Error returned by [`Gateway::attach_peer`] when a peer is already
/// attached under the given uid.
#[derive(Debug, thiserror::Error)]
#[error("gateway already has a via peer with uid {uid}")]
pub struct PeerAttachError {
    /// The uid that was already registered.
    pub uid: Uid,
}

struct GatewayState {
    /// A random, stable identifier for this gateway. It is just a
    /// routing key in peers' tables: peers route messages
    /// back through this gateway by referencing its uid in a
    /// [`Location::Via`] hop. We mint a uid (rather than reuse an
    /// existing key) mainly so the entry can carry a meaningful label.
    uid: Uid,

    /// The location to use when no server is active.
    fallback_location: Location,

    /// The location used when constructing routeable addresses for
    /// newly bound refs. Attaching this gateway to a peer may replace
    /// it with `Via(self.uid, peer_default_location)` so addresses
    /// handed out by this gateway carry the via prefix and route back
    /// through the peer.
    default_location: RwLock<Location>,

    /// Sender used to forward messages whose destination is neither an
    /// attached proc nor matched by [`peers`].
    forwarder: RwLock<BoxedMailboxSender>,

    /// Local delivery targets registered with this gateway, keyed by
    /// id. Each value is a [`WeakProc`] so the gateway does not
    /// extend the proc's lifetime; on hit, the gateway upgrades and
    /// delivers directly to the muxer when the destination is a
    /// local-delivery target. Internal: never exposed; populated
    /// only by [`Gateway::attach_proc`], which is itself internal.
    locals: RwLock<HashMap<ProcId, WeakProc>>,

    /// Locations currently served by this gateway. The last location
    /// is the default advertised location.
    active_servers: RwLock<Vec<Location>>,

    /// Senders to gateways that have attached *to* this one. Each key
    /// is the attaching gateway's uid; values are senders that put
    /// envelopes back onto the duplex toward that gateway. Source
    /// routes (`Location::Via(uid, ...)`) consult this table to peel
    /// the outermost hop and forward.
    peers: RwLock<HashMap<Uid, BoxedMailboxSender>>,
}

impl Gateway {
    /// Create a fresh unserved gateway with dial-based forwarding.
    pub fn new() -> Self {
        Self::configured(
            channel::reserve_local_addr().into(),
            DialMailboxRouter::new().into_boxed(),
        )
    }

    /// Create a fresh unserved local-only gateway.
    pub fn isolated() -> Self {
        Self::configured(
            channel::reserve_local_addr().into(),
            BoxedMailboxSender::new(UnroutableMailboxSender),
        )
    }

    /// Return the process-wide global gateway.
    pub fn global() -> &'static Self {
        static GLOBAL_GATEWAY: OnceLock<Gateway> = OnceLock::new();
        GLOBAL_GATEWAY.get_or_init(Self::new)
    }

    /// Return the gateway for the current execution context.
    ///
    /// This is the gateway attached to [`Proc::current()`].
    pub fn current() -> Self {
        Proc::current().gateway()
    }

    /// Create a gateway with an explicit default advertised location
    /// and outbound forwarder. Inbound traffic for destinations that
    /// don't match a bound proc, route, or via peer is handed off to
    /// `forwarder`.
    pub(crate) fn configured(default_location: Location, forwarder: BoxedMailboxSender) -> Self {
        Self {
            inner: Arc::new(GatewayState {
                uid: Uid::anonymous(),
                fallback_location: default_location.clone(),
                default_location: RwLock::new(default_location),
                forwarder: RwLock::new(forwarder),
                locals: RwLock::new(HashMap::new()),
                active_servers: RwLock::new(Vec::new()),
                peers: RwLock::new(HashMap::new()),
            }),
        }
    }

    /// This gateway's stable uid — the routing key peers use to reach
    /// procs attached here, by addressing them as
    /// `Location::Via(this_uid, inner)`.
    pub fn uid(&self) -> &Uid {
        &self.inner.uid
    }

    /// The gateway's default advertised location.
    pub fn default_location(&self) -> Location {
        self.inner.default_location.read().unwrap().clone()
    }

    /// The outbound forwarder. Inbound traffic for destinations that
    /// don't match a bound proc, route, or via peer is handed off to
    /// this sender.
    pub fn forwarder(&self) -> BoxedMailboxSender {
        self.inner.forwarder.read().unwrap().clone()
    }

    /// Set the gateway's default advertised location.
    pub fn set_default_location(&self, location: Location) {
        *self.inner.default_location.write().unwrap() = location;
    }

    /// Attach a proc to this gateway, establishing the two-way
    /// relationship between them: the gateway can deliver inbound
    /// traffic directly to the proc's muxer, and the proc routes its
    /// egress through the gateway.
    ///
    /// The gateway delivers messages addressed to this id directly to
    /// the muxer when [`Proc::is_local_delivery_target`] holds;
    /// otherwise it falls through to peers and the forwarder.
    ///
    /// Internal-only: only [`Proc`] construction calls this, and the
    /// resulting [`AttachedProcGuard`] is held inside the proc itself
    /// so the proc's lifetime drives detachment. The public Gateway
    /// API exposes peer connectivity via [`Gateway::attach_peer`] (a
    /// sender-based via entry for a peer gateway uid), which does not
    /// take a [`Proc`].
    ///
    /// Panics if a live proc with the same id is already attached. A
    /// dead entry whose [`WeakProc`] has been dropped is replaced
    /// silently; this can occur in the brief window between the last
    /// strong proc reference dropping and its [`AttachedProcGuard`]
    /// field drop removing the table entry.
    pub(crate) fn attach_proc(&self, proc: &Proc) -> AttachedProcGuard {
        let proc_id = proc.proc_id().clone();
        let weak = proc.downgrade();
        let existing = self
            .inner
            .locals
            .write()
            .unwrap()
            .insert(proc_id.clone(), weak.clone());
        match existing {
            // No prior entry, or the prior entry was a dead handle
            // whose slot is now ours.
            None => {}
            Some(weak) if weak.upgrade().is_none() => {}
            Some(_) => {
                panic!("gateway already has a proc attached with id {}", proc_id)
            }
        }
        AttachedProcGuard {
            gateway: Arc::downgrade(&self.inner),
            proc_id,
            weak,
        }
    }

    /// Register a gateway peer that is reachable through `sender`.
    /// Messages whose destination location has an outermost
    /// [`Location::Via`] hop carrying `uid` are forwarded through
    /// `sender` after peeling the hop. The returned guard removes the
    /// entry on drop.
    ///
    /// Used by hosts that have spawned child procs reachable through a
    /// dial-based sender: the host publishes the child's address with
    /// a `Via(child_uid, ...)` prefix and registers the sender here
    /// under the same `uid`.
    ///
    /// Returns [`PeerAttachError`] if a peer with the same uid is
    /// already attached.
    pub fn attach_peer(
        &self,
        uid: Uid,
        sender: BoxedMailboxSender,
    ) -> Result<PeerAttachGuard, PeerAttachError> {
        let mut via = self.inner.peers.write().unwrap();
        if via.contains_key(&uid) {
            return Err(PeerAttachError { uid });
        }
        via.insert(uid.clone(), sender);
        Ok(PeerAttachGuard {
            gateway: Arc::downgrade(&self.inner),
            uid,
        })
    }

    pub(crate) fn serve_rx(
        &self,
        rx: impl channel::Rx<MessageEnvelope> + Send + 'static,
    ) -> MailboxServerHandle {
        WeakGateway::new(self).serve(rx)
    }

    /// Serve this gateway on the provided channel address.
    ///
    /// When serving the first local [`ChannelAddr::any`] address, the gateway
    /// binds the local address that was reserved when the gateway was created.
    /// Local reservation is separate from local binding so a gateway can have a
    /// stable location before it has a runtime available to run a server.
    /// Later local `any` serves allocate fresh local ports, so the gateway can
    /// have multiple active local servers.
    ///
    /// Serving updates the gateway's default location to the newly served
    /// address. When that server stops, the default location falls back to the
    /// previous active server, or to the reserved fallback location when no
    /// server remains.
    pub fn serve(&self, addr: ChannelAddr) -> Result<GatewayServeHandle, ChannelError> {
        let (location, handle) = self.serve_inner(addr)?;
        Ok(GatewayServeHandle {
            gateway: self.clone(),
            handle,
            stopped: false,
            location: Some(location),
        })
    }

    fn serve_inner(
        &self,
        addr: ChannelAddr,
    ) -> Result<(Location, MailboxServerHandle), ChannelError> {
        let addr = self.resolve_serve_addr(addr);
        let (addr, rx) = channel::serve(addr)?;
        let location = Location::from(addr);
        self.add_server(location.clone());
        Ok((location, self.serve_rx(rx)))
    }

    fn resolve_serve_addr(&self, addr: ChannelAddr) -> ChannelAddr {
        // The first local-any serve activates the address that was reserved at
        // construction time. Subsequent local-any serves should allocate new
        // ports, so multiple local servers can coexist for the same gateway.
        if addr == ChannelAddr::any(ChannelTransport::Local)
            && self.inner.active_servers.read().unwrap().is_empty()
            && matches!(self.inner.fallback_location.addr(), ChannelAddr::Local(_))
        {
            return self.inner.fallback_location.addr().clone();
        }
        addr
    }

    fn add_server(&self, location: Location) {
        let mut active_servers = self.inner.active_servers.write().unwrap();
        active_servers.push(location.clone());
        *self.inner.default_location.write().unwrap() = location;
    }

    fn remove_server(&self, location: &Location) {
        let mut active_servers = self.inner.active_servers.write().unwrap();
        if let Some(index) = active_servers.iter().rposition(|active| active == location) {
            active_servers.remove(index);
        }
        let default_location = active_servers
            .last()
            .cloned()
            .unwrap_or_else(|| self.inner.fallback_location.clone());
        *self.inner.default_location.write().unwrap() = default_location;
    }

    /// Flush pending gateway traffic.
    ///
    /// Flushes the muxers for all live attached procs and then the
    /// gateway's forwarder. Flushing the proc muxers drains local
    /// delivery and any return paths rooted in attached procs;
    /// flushing the forwarder drains outbound traffic that the gateway
    /// routed away from those targets.
    ///
    /// Flushing is best-effort: every muxer and the forwarder are
    /// flushed even if some fail, and the first error (if any) is
    /// returned afterward. The live proc set is snapshotted before
    /// awaiting, so we do not hold its map while flushing. Procs that
    /// have already been dropped are ignored. Concurrent posts may
    /// race with this operation; `flush` only guarantees that each
    /// flushed sender observes its usual sender-level flush semantics
    /// at the time it is flushed.
    pub(crate) async fn flush(&self) -> Result<(), anyhow::Error> {
        // Flush attached procs and the forwarder. We intentionally do
        // *not* iterate `peers` here: in-process gateway
        // attaches install each peer in the other's peers, so a
        // naive iteration recurses through the peer's `flush` and
        // overflows the stack.
        let local_procs: Vec<_> = self
            .inner
            .locals
            .read()
            .unwrap()
            .values()
            .filter_map(|weak| weak.upgrade())
            .collect();
        // Bound concurrency by the proc count so every muxer flush is
        // still launched at once (best-effort, order-independent). Each
        // future owns its `Proc` so the borrow stays self-contained.
        let concurrency = local_procs.len().max(1);
        let proc_results: Vec<_> = futures::stream::iter(
            local_procs
                .into_iter()
                .map(|proc| async move { proc.muxer().flush().await }),
        )
        .buffer_unordered(concurrency)
        .collect()
        .await;
        let forwarder = self.inner.forwarder.read().unwrap().clone();
        let forwarder_result = forwarder.flush().await;

        // Best-effort: all flushes have run; surface the first error.
        proc_results
            .into_iter()
            .chain(std::iter::once(forwarder_result))
            .collect()
    }
}

impl fmt::Debug for Gateway {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Gateway")
            .field("default_location", &self.default_location())
            .finish()
    }
}

/// A running gateway server. Returned by [`Gateway::serve`] and
/// [`Gateway::from_simplex`].
///
/// Shutdown is two steps: [`stop`](Self::stop) signals the server and
/// performs the active-server cleanup, and [`join`](Self::join) awaits
/// teardown. They are independent — `join` does not stop, so a caller
/// that wants both must call `stop` first.
pub struct GatewayServeHandle {
    gateway: Gateway,
    handle: MailboxServerHandle,
    stopped: bool,
    /// Location added to the gateway's `active_servers` list when this
    /// handle was created. `None` for handles built from a pre-bound
    /// receiver via [`from_simplex`] that the caller is tracking
    /// separately. Taken by `stop` so cleanup runs at most once.
    location: Option<Location>,
}

impl fmt::Debug for GatewayServeHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayServeHandle")
            .field("location", &self.location)
            .finish()
    }
}

impl GatewayServeHandle {
    /// Wrap a raw [`MailboxServerHandle`] from a simplex serve as a
    /// [`GatewayServeHandle`] with no active-server bookkeeping. Used
    /// when the caller passes a pre-bound receiver to the gateway
    /// (e.g. a local-transport Host).
    pub fn from_simplex(gateway: Gateway, handle: MailboxServerHandle) -> Self {
        Self {
            gateway,
            handle,
            stopped: false,
            location: None,
        }
    }

    /// Signal the underlying server to stop and unwind the
    /// active-server bookkeeping. Idempotent: later calls are no-ops.
    /// Call [`join`](Self::join) afterward to await teardown.
    pub fn stop(&mut self, reason: &str) {
        if self.stopped {
            return;
        }
        self.stopped = true;
        self.handle.stop(reason);
        if let Some(loc) = self.location.take() {
            self.gateway.remove_server(&loc);
        }
    }

    /// Await teardown of the underlying server, returning its join
    /// result. This does not signal the server to stop; call
    /// [`stop`](Self::stop) first if the server is still running, or
    /// `join` will block until the server terminates on its own.
    pub async fn join(self) -> Result<(), MailboxServerError> {
        match self.handle.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(err),
            Err(join_err) => Err(MailboxServerError::Channel(ChannelError::Other(
                anyhow::anyhow!("gateway serve task join error: {join_err}"),
            ))),
        }
    }
}

#[derive(Clone, Debug)]
struct WeakGateway(Weak<GatewayState>);

impl WeakGateway {
    fn new(gateway: &Gateway) -> Self {
        Self(Arc::downgrade(&gateway.inner))
    }

    fn upgrade(&self) -> Option<Gateway> {
        self.0.upgrade().map(|inner| Gateway { inner })
    }
}

#[async_trait]
impl crate::mailbox::MailboxSender for WeakGateway {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        match self.upgrade() {
            Some(gateway) => gateway.post(envelope, return_handle),
            None => {
                let target = envelope.dest().clone();
                let failure =
                    DeliveryFailure::new(UndeliverableReason::Transport(TransportFailure::new(
                        target,
                        TransportFailureReason::LinkUnavailable("gateway is gone".to_string()),
                    )));
                envelope.undeliverable(failure, return_handle)
            }
        }
    }

    async fn flush(&self) -> Result<(), anyhow::Error> {
        match self.upgrade() {
            Some(gateway) => Gateway::flush(&gateway).await,
            None => Ok(()),
        }
    }
}

#[async_trait]
impl crate::mailbox::MailboxSender for Gateway {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        // A message that reaches a gateway resolves to exactly one of
        // three outcomes, decided by the destination's outermost
        // `Via(uid, ...)` hop (if any):
        //
        //   * a *peer* hop (`uid` in `peers`): peel the hop and forward
        //     to that peer;
        //   * *our own* hop (`uid == self.uid`), or a via-less local
        //     destination: we are the leaf — deliver to the local proc;
        //   * anything else: not ours. Hand it to the forwarder, which
        //     is either a route onward (a dial router, or an attached
        //     duplex) or a terminal `UnroutableMailboxSender` that
        //     returns it as undeliverable.
        //
        // The via hop is resolved before any per-proc lookup, so a
        // foreign via is never silently delivered locally just because
        // its inner proc id happens to match a local proc.
        let dest_location = envelope.dest().location().clone();
        if let Ok((via_uid, inner_location)) = dest_location.pop_via() {
            if let Some(sender) = self.inner.peers.read().unwrap().get(&via_uid).cloned() {
                // Peer hop: rewrite the destination to the inner
                // location so the peer routes by it, then forward.
                let dest_id = envelope.dest().id().clone();
                let new_dest = PortAddr::new(dest_id, inner_location);
                sender.post(envelope.with_dest(new_dest), return_handle);
                return;
            }
            if via_uid != self.inner.uid {
                // A hop naming neither a peer nor this gateway: we are a
                // waypoint, not the destination. Forward toward the
                // default route, which itself returns the message as
                // undeliverable if it is terminal.
                let forwarder = self.inner.forwarder.read().unwrap().clone();
                forwarder.post(envelope, return_handle);
                return;
            }
            // Our own hop: this gateway is the named leaf. Deliver to
            // the local proc; if it is gone, the message is
            // undeliverable — a hop addressed to us is never forwarded
            // back out.
            let dest_proc = envelope.dest().actor_addr().proc_addr();
            let local = self
                .inner
                .locals
                .read()
                .unwrap()
                .get(dest_proc.id())
                .and_then(|weak| weak.upgrade());
            match local {
                Some(proc) if proc.is_local_delivery_target(&dest_proc) => {
                    proc.muxer().post(envelope, return_handle);
                }
                _ => {
                    let target = envelope.dest().clone();
                    let failure = DeliveryFailure::new(UndeliverableReason::Transport(
                        TransportFailure::new(target, TransportFailureReason::NoRoute),
                    ));
                    envelope.undeliverable(failure, return_handle);
                }
            }
            return;
        }

        // Via-less destination: deliver to the local proc if it is a
        // delivery target, otherwise hand to the forwarder (outbound
        // egress for plain remote addresses). A dead entry is left in
        // place — `AttachedProcGuard::drop` is the sole remover.
        let dest_proc = envelope.dest().actor_addr().proc_addr();
        let local = self
            .inner
            .locals
            .read()
            .unwrap()
            .get(dest_proc.id())
            .and_then(|weak| weak.upgrade());
        if let Some(proc) = local
            && proc.is_local_delivery_target(&dest_proc)
        {
            proc.muxer().post(envelope, return_handle);
            return;
        }
        let forwarder = self.inner.forwarder.read().unwrap().clone();
        forwarder.post(envelope, return_handle)
    }

    async fn flush(&self) -> Result<(), anyhow::Error> {
        Gateway::flush(self).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use async_trait::async_trait;
    use hyperactor_config::Flattrs;
    use timed_test::async_timed_test;
    use tokio::sync::mpsc;
    use tokio::time;

    use super::*;
    use crate::Endpoint as _;
    use crate::Label;
    use crate::ProcAddr;
    use crate::mailbox::DeliveryFailureKind;
    use crate::mailbox::MailboxClient;
    use crate::mailbox::MailboxSender;
    use crate::mailbox::PortLocation;
    use crate::mailbox::monitored_return_handle;
    use crate::port::Port;
    use crate::proc::Proc;
    use crate::testing::ids::test_actor_id;
    use crate::testing::pingpong::PingPongActor;
    use crate::testing::pingpong::PingPongMessage;

    #[derive(Clone)]
    struct RecordingSender(mpsc::UnboundedSender<MessageEnvelope>);

    #[async_trait]
    impl MailboxSender for RecordingSender {
        fn post_unchecked(
            &self,
            envelope: MessageEnvelope,
            _return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
        ) {
            self.0
                .send(envelope)
                .expect("recording sender should be open");
        }
    }

    /// Test-only helper that connects two gateways over real
    /// `Local`-transport channels, so the via-routing tests exercise a
    /// genuine cross-gateway hop: each gateway serves on a local channel
    /// and the peer reaches it by dialing. After attach, each side's
    /// `default_location` advertises its destinations through the peer's
    /// uid (`Via(self_uid, peer_default)`), so procs bound afterward
    /// inherit the via prefix and route across the link.
    ///
    /// This is just enough wiring to test the gateway's via routing
    /// locally; cross-process attach over a duplex transport is a
    /// follow-up.
    trait GatewayAttachExt {
        fn attach(&self, peer: &Gateway) -> AttachGuard;
    }

    impl GatewayAttachExt for Gateway {
        fn attach(&self, peer: &Gateway) -> AttachGuard {
            // Genuine pre-attach defaults, captured before serving
            // (serving overwrites `default_location` with the served
            // address).
            let pre_self_default = self.default_location();
            let pre_peer_default = peer.default_location();

            // Serve each gateway on a local channel so the other can
            // reach it by dialing.
            let self_serve = self
                .serve(ChannelAddr::any(ChannelTransport::Local))
                .expect("serve self on local channel");
            let peer_serve = peer
                .serve(ChannelAddr::any(ChannelTransport::Local))
                .expect("serve peer on local channel");
            let self_addr = self.default_location().addr().clone();
            let peer_addr = peer.default_location().addr().clone();

            // Cross-register dialed senders keyed by uid, so each side
            // peels the other's uid and forwards over the local channel.
            let self_via_guard = peer
                .attach_peer(
                    self.inner.uid.clone(),
                    MailboxClient::dial(self_addr)
                        .expect("dial self")
                        .into_boxed(),
                )
                .expect("peer has no via entry for this gateway's uid");
            let peer_via_guard = self
                .attach_peer(
                    peer.inner.uid.clone(),
                    MailboxClient::dial(peer_addr)
                        .expect("dial peer")
                        .into_boxed(),
                )
                .expect("self has no via entry for the peer gateway's uid");

            // Advertise each side's destinations through the peer's uid.
            *self.inner.default_location.write().unwrap() =
                Location::Via(self.inner.uid.clone(), Box::new(pre_peer_default.clone()));
            *peer.inner.default_location.write().unwrap() =
                Location::Via(peer.inner.uid.clone(), Box::new(pre_self_default.clone()));

            AttachGuard {
                self_gateway: Arc::downgrade(&self.inner),
                peer_gateway: Arc::downgrade(&peer.inner),
                prev_self_default: Some(pre_self_default),
                prev_peer_default: Some(pre_peer_default),
                _self_via_guard: self_via_guard,
                _peer_via_guard: peer_via_guard,
                _self_serve: self_serve,
                _peer_serve: peer_serve,
            }
        }
    }

    /// Guard for the test-only [`GatewayAttachExt::attach`]: on drop it
    /// restores both gateways' previous default locations and removes
    /// the cross-registered peers and local serve loops (via the
    /// held guards and serve handles).
    struct AttachGuard {
        self_gateway: Weak<GatewayState>,
        peer_gateway: Weak<GatewayState>,
        prev_self_default: Option<Location>,
        prev_peer_default: Option<Location>,
        _self_via_guard: PeerAttachGuard,
        _peer_via_guard: PeerAttachGuard,
        _self_serve: GatewayServeHandle,
        _peer_serve: GatewayServeHandle,
    }

    impl Drop for AttachGuard {
        fn drop(&mut self) {
            if let Some(state) = self.self_gateway.upgrade()
                && let Some(loc) = self.prev_self_default.take()
            {
                *state.default_location.write().unwrap() = loc;
            }
            if let Some(state) = self.peer_gateway.upgrade()
                && let Some(loc) = self.prev_peer_default.take()
            {
                *state.default_location.write().unwrap() = loc;
            }
            // via guards and serve handles drop themselves.
        }
    }

    /// `Gateway::post_unchecked` demuxes inbound envelopes by
    /// destination `ProcId` to the matching attached proc's muxer,
    /// and falls through to the configured forwarder for unknown
    /// destinations. Attached procs only receive envelopes addressed
    /// to them — a stranger-addressed envelope does not leak to local
    /// receivers.
    #[tokio::test]
    async fn test_gateway_post_demuxes_by_proc_id() {
        let (tx, mut forwarded_rx) = mpsc::unbounded_channel();
        let gateway = Gateway::configured(
            channel::reserve_local_addr().into(),
            BoxedMailboxSender::new(RecordingSender(tx)),
        );

        let alpha = Proc::builder()
            .proc_id(ProcId::instance(Label::strip("alpha")))
            .shared_gateway(gateway.clone())
            .build()
            .unwrap();
        let beta = Proc::builder()
            .proc_id(ProcId::instance(Label::strip("beta")))
            .shared_gateway(gateway.clone())
            .build()
            .unwrap();

        let alpha_client = alpha.client("client");
        let (alpha_port, mut alpha_rx) = alpha_client.bind_handler_port::<u64>();
        let PortLocation::Bound(alpha_dest) = alpha_port.location() else {
            panic!("alpha handler port must be bound");
        };

        let beta_client = beta.client("client");
        let (beta_port, mut beta_rx) = beta_client.bind_handler_port::<u64>();
        let PortLocation::Bound(beta_dest) = beta_port.location() else {
            panic!("beta handler port must be bound");
        };

        let sender = test_actor_id("test", "sender");

        gateway.post(
            MessageEnvelope::serialize(sender.clone(), alpha_dest.clone(), &111u64, Flattrs::new())
                .unwrap(),
            monitored_return_handle(),
        );
        let received = time::timeout(Duration::from_secs(5), alpha_rx.recv())
            .await
            .expect("alpha_rx timed out")
            .expect("alpha_rx closed");
        assert_eq!(received, 111);
        assert!(matches!(
            forwarded_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        gateway.post(
            MessageEnvelope::serialize(sender.clone(), beta_dest.clone(), &222u64, Flattrs::new())
                .unwrap(),
            monitored_return_handle(),
        );
        let received = time::timeout(Duration::from_secs(5), beta_rx.recv())
            .await
            .expect("beta_rx timed out")
            .expect("beta_rx closed");
        assert_eq!(received, 222);
        assert!(matches!(
            forwarded_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        let stranger_proc = ProcAddr::instance(ChannelAddr::Local(9999), "stranger");
        let stranger_dest = stranger_proc
            .actor_addr("ghost")
            .port_addr(Port::from(0u64));
        gateway.post(
            MessageEnvelope::serialize(sender, stranger_dest.clone(), &333u64, Flattrs::new())
                .unwrap()
                .set_ttl(3),
            monitored_return_handle(),
        );
        let forwarded = time::timeout(Duration::from_secs(5), forwarded_rx.recv())
            .await
            .expect("forwarded_rx timed out")
            .expect("forwarded_rx closed");
        assert_eq!(forwarded.dest(), &stranger_dest);
        // The fallback route is another `MailboxSender` hop: `gateway.post`
        // decrements once, and then the forwarder's `post` decrements again.
        assert_eq!(forwarded.ttl(), 1);
        assert!(
            time::timeout(Duration::from_millis(50), alpha_rx.recv())
                .await
                .is_err(),
            "alpha_rx received a message after stranger post",
        );
        assert!(
            time::timeout(Duration::from_millis(50), beta_rx.recv())
                .await
                .is_err(),
            "beta_rx received a message after stranger post",
        );
    }

    /// A via hop naming neither a peer nor this gateway is *forwarded*,
    /// never delivered locally — even when the inner proc id matches a
    /// live local proc. The source route wins over an incidental id
    /// match.
    #[tokio::test]
    async fn test_gateway_foreign_via_forwards_not_local() {
        #[derive(Clone)]
        struct CountingSender(Arc<AtomicUsize>);

        #[async_trait]
        impl MailboxSender for CountingSender {
            fn post_unchecked(
                &self,
                _envelope: MessageEnvelope,
                _return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
            ) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let forwarded = Arc::new(AtomicUsize::new(0));
        let gateway = Gateway::configured(
            channel::reserve_local_addr().into(),
            BoxedMailboxSender::new(CountingSender(forwarded.clone())),
        );

        // A live local proc whose id we will reuse behind a foreign via.
        let alpha = Proc::builder()
            .proc_id(ProcId::instance(Label::strip("alpha")))
            .shared_gateway(gateway.clone())
            .build()
            .unwrap();

        // Address alpha's proc id, but behind a via hop for a uid that
        // is neither a peer nor this gateway's own uid.
        let foreign_uid = Uid::Instance(0xfeed, Some(Label::strip("foreign")));
        assert_ne!(&foreign_uid, gateway.uid());
        let dest = ProcAddr::new(
            alpha.proc_id().clone(),
            Location::from(ChannelAddr::Local(7777)).with_via(foreign_uid),
        )
        .actor_addr("ghost")
        .port_addr(Port::from(0u64));

        gateway.post(
            MessageEnvelope::serialize(
                test_actor_id("test", "sender"),
                dest,
                &7u64,
                Flattrs::new(),
            )
            .unwrap(),
            monitored_return_handle(),
        );

        // Forwarded, not delivered locally by the matching inner id.
        assert_eq!(
            forwarded.load(Ordering::SeqCst),
            1,
            "a foreign via must be forwarded, not delivered locally",
        );
    }

    /// A via hop naming *this* gateway (the leaf) whose inner proc id is
    /// not a live local proc is undeliverable — a hop addressed to us is
    /// never forwarded back out.
    #[tokio::test]
    async fn test_gateway_self_via_unknown_proc_is_undeliverable() {
        let gateway = Gateway::isolated();

        // Scratch proc just to host the return port.
        let scratch = Proc::isolated();
        let scratch_client = scratch.client("return");
        let (return_handle, mut return_rx) =
            scratch_client.open_port::<Undeliverable<MessageEnvelope>>();

        // Address an id with no live local proc, behind this gateway's
        // own uid (so we are the named leaf).
        let dest = ProcAddr::new(
            ProcId::instance(Label::strip("stranger")),
            Location::from(ChannelAddr::Local(4321)).with_via(gateway.uid().clone()),
        )
        .actor_addr("ghost")
        .port_addr(Port::from(0u64));
        let envelope = MessageEnvelope::serialize(
            test_actor_id("test", "sender"),
            dest.clone(),
            &9u64,
            Flattrs::new(),
        )
        .unwrap()
        .set_ttl(3);

        gateway.post(envelope, return_handle);

        let Undeliverable::Returned(returned) =
            time::timeout(Duration::from_secs(5), return_rx.recv())
                .await
                .expect("return_rx timed out")
                .expect("return_rx closed")
        else {
            panic!("expected returned envelope");
        };
        assert_eq!(returned.dest(), &dest);
        assert!(
            returned
                .root_delivery_failure()
                .is_some_and(|failure| matches!(
                    &failure.kind,
                    DeliveryFailureKind::Undeliverable(UndeliverableReason::Transport(_))
                )),
            "expected NoRoute transport bounce, got {:?}",
            returned.delivery_failures(),
        );
        // This self-via miss is returned directly from `Gateway::post_unchecked`
        // without forwarding through another `MailboxSender`, so only
        // `gateway.post` decrements the TTL.
        assert_eq!(returned.ttl(), 2);
    }

    /// Ping-pong between two `PingPongActor`s on two procs that share
    /// one gateway. Each cross-proc hop goes `Proc::post_unchecked` →
    /// `Gateway::post_unchecked` demux → destination proc's muxer
    /// directly, without touching the gateway's forwarder.
    #[tokio::test]
    async fn test_ping_pong_across_shared_gateway() {
        let gateway = Gateway::isolated();

        let alpha = Proc::builder()
            .proc_id(ProcId::instance(Label::strip("alpha")))
            .shared_gateway(gateway.clone())
            .build()
            .unwrap();
        let beta = Proc::builder()
            .proc_id(ProcId::instance(Label::strip("beta")))
            .shared_gateway(gateway.clone())
            .build()
            .unwrap();

        let client = alpha.client("client");
        let (undeliverable_msg_tx, mut undeliverable_rx) =
            client.open_port::<Undeliverable<MessageEnvelope>>();

        let ping_actor = PingPongActor::new(Some(undeliverable_msg_tx.bind()), None, None);
        let pong_actor = PingPongActor::new(Some(undeliverable_msg_tx.bind()), None, None);
        let ping_handle = alpha.spawn_with_label::<PingPongActor>("ping", ping_actor);
        let pong_handle = beta.spawn_with_label::<PingPongActor>("pong", pong_actor);

        let (local_port, local_receiver) = client.open_once_port();

        ping_handle.post(
            &client,
            PingPongMessage(10, pong_handle.bind(), local_port.bind()),
        );

        let received = time::timeout(Duration::from_secs(5), local_receiver.recv())
            .await
            .expect("local_receiver timed out")
            .expect("local_receiver closed");
        assert!(received);

        assert!(
            time::timeout(Duration::from_millis(50), undeliverable_rx.recv())
                .await
                .is_err(),
            "unexpected undeliverable during cross-proc ping-pong",
        );
    }

    /// `Gateway::attach_proc` panics when a second proc with the
    /// same `ProcId` is built against the same gateway while the
    /// first is still alive. The check is in
    /// `Gateway::attach_proc`, invoked from `Proc::builder().build()`
    /// via `Proc::from_parts_unchecked`.
    #[test]
    #[should_panic(expected = "gateway already has a proc attached with id")]
    fn test_gateway_attach_proc_panics_on_duplicate_live_proc() {
        let gateway = Gateway::isolated();
        let proc_id = ProcId::instance(Label::strip("alpha"));

        // Hold the first proc in a binding so it stays alive across
        // the second build; if the first were dropped, the gateway's
        // stale-entry path would silently replace it instead of
        // panicking.
        let _first = Proc::builder()
            .proc_id(proc_id.clone())
            .shared_gateway(gateway.clone())
            .build()
            .unwrap();

        let _second = Proc::builder()
            .proc_id(proc_id)
            .shared_gateway(gateway.clone())
            .build()
            .unwrap();
    }

    /// `Gateway::flush()` propagates the flush to each attached
    /// proc's muxer (which in turn flushes its bound senders) and
    /// then to the gateway's forwarder. Verified by binding a
    /// `FlushCountingSender` into each proc's muxer and asserting all
    /// three counters (alpha's, beta's, the forwarder's) increment
    /// exactly once.
    #[tokio::test]
    async fn test_gateway_flush_propagates_to_attached_procs() {
        #[derive(Clone)]
        struct FlushCountingSender(Arc<AtomicUsize>);

        #[async_trait]
        impl MailboxSender for FlushCountingSender {
            fn post_unchecked(
                &self,
                _envelope: MessageEnvelope,
                _return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
            ) {
                // Not exercised by this test.
            }

            async fn flush(&self) -> Result<(), anyhow::Error> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let alpha_flushed = Arc::new(AtomicUsize::new(0));
        let beta_flushed = Arc::new(AtomicUsize::new(0));
        let forwarder_flushed = Arc::new(AtomicUsize::new(0));

        let gateway = Gateway::configured(
            channel::reserve_local_addr().into(),
            BoxedMailboxSender::new(FlushCountingSender(forwarder_flushed.clone())),
        );

        let alpha = Proc::builder()
            .proc_id(ProcId::instance(Label::strip("alpha")))
            .shared_gateway(gateway.clone())
            .build()
            .unwrap();
        let beta = Proc::builder()
            .proc_id(ProcId::instance(Label::strip("beta")))
            .shared_gateway(gateway.clone())
            .build()
            .unwrap();

        // Bind a flush-counting probe into each proc's muxer. Use a
        // fabricated actor id under the proc — no actor is spawned
        // there; the muxer just routes flushes to whatever's bound.
        let alpha_probe = alpha.proc_addr().actor_addr("alpha_probe").id().clone();
        let beta_probe = beta.proc_addr().actor_addr("beta_probe").id().clone();
        assert!(
            alpha
                .muxer()
                .bind(alpha_probe, FlushCountingSender(alpha_flushed.clone()))
        );
        assert!(
            beta.muxer()
                .bind(beta_probe, FlushCountingSender(beta_flushed.clone()))
        );

        // Sanity: two procs registered, both live.
        assert_eq!(gateway.inner.locals.read().unwrap().len(), 2);

        gateway.flush().await.unwrap();

        assert_eq!(alpha_flushed.load(Ordering::SeqCst), 1);
        assert_eq!(beta_flushed.load(Ordering::SeqCst), 1);
        assert_eq!(forwarder_flushed.load(Ordering::SeqCst), 1);
    }

    /// Driving `Gateway::flush` concurrently with proc attach + drop must
    /// not panic, deadlock, or leave the gateway in a torn state. The
    /// flush impl snapshots the live proc set before awaiting, so
    /// attaches/drops during flush should be invisible to the flush in
    /// flight.
    #[async_timed_test(timeout_secs = 10)]
    async fn test_gateway_flush_concurrent_with_attach_and_drop() {
        #[derive(Clone)]
        struct NoopSender;

        #[async_trait]
        impl MailboxSender for NoopSender {
            fn post_unchecked(
                &self,
                _envelope: MessageEnvelope,
                _return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
            ) {
                // Defensive no-op: this test shouldn't route messages.
            }

            async fn flush(&self) -> Result<(), anyhow::Error> {
                Ok(())
            }
        }

        let gateway = Gateway::configured(
            channel::reserve_local_addr().into(),
            BoxedMailboxSender::new(NoopSender),
        );

        let barrier = Arc::new(tokio::sync::Barrier::new(2));

        let flushes = {
            let gateway = gateway.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                for _ in 0..100 {
                    gateway.flush().await.unwrap();
                    tokio::task::yield_now().await;
                }
            })
        };

        let attach_drop = {
            let gateway = gateway.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                for i in 0..100 {
                    let proc = Proc::builder()
                        .proc_id(ProcId::instance(Label::strip(&format!("p{i}"))))
                        .shared_gateway(gateway.clone())
                        .build()
                        .unwrap();
                    // Hold the proc across at least one yield so it's
                    // attached for an observable window before drop.
                    tokio::task::yield_now().await;
                    drop(proc);
                }
            })
        };

        flushes.await.unwrap();
        attach_drop.await.unwrap();

        // No torn state: a final flush succeeds.
        gateway.flush().await.unwrap();

        // All procs dropped — no weak entries should still upgrade. Stale
        // weak entries may remain in the map (replaced on next attach with
        // same id), so we don't assert on `len()`; we assert on live
        // entries only.
        assert_eq!(
            gateway
                .inner
                .locals
                .read()
                .unwrap()
                .values()
                .filter_map(WeakProc::upgrade)
                .count(),
            0,
            "no procs should still be live after attach_drop task completes",
        );
    }

    /// After the gateway is dropped, `WeakGateway::post_unchecked`
    /// (the sender used by gateway-served mailbox tasks) bounces
    /// envelopes as a structured transport failure rather than panicking or
    /// hanging.
    /// Tested directly against `WeakGateway` — no channel server, no
    /// task lifecycle — because the bounce is in-process and
    /// observable at the caller's return port without going through
    /// any serialize/dispatch path.
    #[tokio::test]
    async fn test_weak_gateway_bounces_broken_link_after_drop() {
        let gateway = Gateway::isolated();
        let weak = WeakGateway::new(&gateway);
        drop(gateway);

        // Scratch proc just to host the return port.
        let scratch = Proc::isolated();
        let scratch_client = scratch.client("return");
        let (return_handle, mut return_rx) =
            scratch_client.open_port::<Undeliverable<MessageEnvelope>>();

        // Fabricate a destination — its contents don't matter; the
        // bounce happens at WeakGateway::upgrade before any demux
        // would run.
        let dest_proc = ProcAddr::instance(ChannelAddr::Local(1234), "stranger");
        let dest = dest_proc.actor_addr("ghost").port_addr(Port::from(0u64));
        let envelope = MessageEnvelope::serialize(
            test_actor_id("test", "sender"),
            dest.clone(),
            &42u64,
            Flattrs::new(),
        )
        .unwrap();

        // Post directly through the WeakGateway. Upgrade fails and sends the
        // bounce synchronously to our return port.
        weak.post(envelope, return_handle);

        let Undeliverable::Returned(envelope) =
            time::timeout(Duration::from_secs(5), return_rx.recv())
                .await
                .expect("return_rx timed out")
                .expect("return_rx closed")
        else {
            panic!("expected returned envelope");
        };
        assert_eq!(envelope.dest(), &dest);
        assert!(
            envelope
                .root_delivery_failure()
                .is_some_and(|failure| matches!(
                    &failure.kind,
                    DeliveryFailureKind::Undeliverable(UndeliverableReason::Transport(_))
                )),
            "expected structured transport bounce, got {:?}",
            envelope.delivery_failures(),
        );
    }

    /// Dropping the `Proc` drops its `AttachedProcGuard`, which
    /// eagerly removes the entry from the gateway's locals map. A
    /// subsequent attach with the same `ProcId` is therefore a fresh
    /// insert — no panic, no stale entry to replace.
    #[tokio::test]
    async fn test_gateway_attach_proc_after_proc_drop() {
        let gateway = Gateway::isolated();
        let proc_id = ProcId::instance(Label::strip("alpha"));

        let first = Proc::builder()
            .proc_id(proc_id.clone())
            .shared_gateway(gateway.clone())
            .build()
            .unwrap();
        drop(first);

        // AttachedProcGuard::drop removed the entry from the map.
        assert_eq!(gateway.inner.locals.read().unwrap().len(), 0);

        // The slot is free, so registering a new proc with the same id
        // is a fresh insert — no panic.
        let second = Proc::builder()
            .proc_id(proc_id.clone())
            .shared_gateway(gateway.clone())
            .build()
            .unwrap();
        assert_eq!(gateway.inner.locals.read().unwrap().len(), 1);

        // Verify the new proc is reachable via the gateway.
        let client = second.client("client");
        let (port, mut rx) = client.bind_handler_port::<u64>();
        let dest = port.bind().port_addr().clone();

        gateway.post(
            MessageEnvelope::serialize(
                test_actor_id("test", "sender"),
                dest,
                &42u64,
                Flattrs::new(),
            )
            .unwrap(),
            monitored_return_handle(),
        );

        let received = time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("rx timed out")
            .expect("rx closed");
        assert_eq!(received, 42);
    }

    /// `Gateway::remove_server` correctly unwinds `active_servers`
    /// when handles stop out of order. Three concurrent servers; stop
    /// the middle one, then the last, then the first, asserting the
    /// gateway's `default_location` at each step. Final empty state
    /// reverts to the construction-time fallback.
    #[tokio::test]
    async fn test_gateway_serve_stop_unwinds_in_any_order() {
        let gateway = Gateway::isolated();
        let fallback = gateway.default_location();

        let mut s1 = Gateway::serve(&gateway, ChannelAddr::any(ChannelTransport::Local)).unwrap();
        let loc1 = gateway.default_location();
        let mut s2 = Gateway::serve(&gateway, ChannelAddr::any(ChannelTransport::Local)).unwrap();
        let loc2 = gateway.default_location();
        let mut s3 = Gateway::serve(&gateway, ChannelAddr::any(ChannelTransport::Local)).unwrap();
        let loc3 = gateway.default_location();

        // First serve(any) reuses the gateway's reserved fallback
        // address (see resolve_serve_addr); subsequent serves
        // allocate fresh ports.
        assert_eq!(loc1, fallback);
        assert_ne!(loc1, loc2);
        assert_ne!(loc2, loc3);
        assert_ne!(loc1, loc3);

        // Middle handle stops first: default stays at loc3 (still the
        // last entry in active_servers). `stop` performs the cleanup;
        // `join` just awaits teardown.
        s2.stop("test");
        s2.join().await.unwrap();
        assert_eq!(gateway.default_location(), loc3);

        // Last handle stops: default falls back to loc1.
        s3.stop("test");
        s3.join().await.unwrap();
        assert_eq!(gateway.default_location(), loc1);

        // Final handle stops: default reverts to the
        // construction-time fallback.
        s1.stop("test");
        s1.join().await.unwrap();
        assert_eq!(gateway.default_location(), fallback);
    }

    /// `Gateway::attach(&Gateway)` is purely via-based:
    /// 1. Each gateway's `default_location` becomes a `Via` form
    ///    that advertises destinations through the peer.
    /// 2. Procs bound *after* attach inherit the via prefix and
    ///    route across the link in both directions without any
    ///    per-id registration.
    /// 3. On guard drop: restore both `default_location`s and
    ///    remove the peer entries.
    ///
    /// No snapshot cross-bind: pre-attach destinations are not
    /// reachable across the link because their addresses lack the
    /// via prefix. (Motivating use case: a client attached to a
    /// host inside a kubernetes cluster cannot dial the cluster's
    /// internal addresses directly, so any "shortcut" route would
    /// be unreachable.)
    #[tokio::test]
    async fn test_gateway_attach_bidirectional() {
        use crate::testing::ids::test_actor_id;

        let gw_a = Gateway::isolated();
        let pre_a_default = gw_a.default_location();
        let gw_b = Gateway::isolated();
        let pre_b_default = gw_b.default_location();

        // Connect the two gateways before binding any procs, so we
        // exercise the via-only path. (Pre-attach procs are not
        // reachable across the link by design.)
        let attach = gw_a.attach(&gw_b);

        // default_location on each side is now Via(self_uid, peer_default).
        let post_a_default = gw_a.default_location();
        let post_b_default = gw_b.default_location();
        let (uid_a, inner_a) = post_a_default.as_via().expect("gw_a default is via");
        assert_eq!(uid_a, gw_a.uid());
        assert_eq!(inner_a.as_ref(), &pre_b_default);
        let (uid_b, inner_b) = post_b_default.as_via().expect("gw_b default is via");
        assert_eq!(uid_b, gw_b.uid());
        assert_eq!(inner_b.as_ref(), &pre_a_default);

        // Bind procs on each side *after* attach so their port
        // addresses inherit the via prefix.
        let proc_a = Proc::builder()
            .proc_id(ProcId::instance(Label::strip("alpha")))
            .shared_gateway(gw_a.clone())
            .build()
            .unwrap();
        let alpha_client = proc_a.client("client");
        let (alpha_port, mut alpha_rx) = alpha_client.bind_handler_port::<u64>();
        let PortLocation::Bound(alpha_dest) = alpha_port.location() else {
            panic!("alpha port must be bound");
        };
        assert!(
            alpha_dest.location().as_via().is_some(),
            "alpha carries via prefix"
        );

        let proc_b = Proc::builder()
            .proc_id(ProcId::instance(Label::strip("beta")))
            .shared_gateway(gw_b.clone())
            .build()
            .unwrap();
        let beta_client = proc_b.client("client");
        let (beta_port, mut beta_rx) = beta_client.bind_handler_port::<u64>();
        let PortLocation::Bound(beta_dest) = beta_port.location() else {
            panic!("beta port must be bound");
        };
        assert!(
            beta_dest.location().as_via().is_some(),
            "beta carries via prefix"
        );

        // gw_a → alpha: gw_a's post sees outermost Via(gw_a.uid, ..),
        // which is *not* in gw_a.peers (those entries are keyed by
        // the peer's uid). Falls through to locals and delivers.
        let sender = test_actor_id("client", "sender");
        gw_a.post(
            MessageEnvelope::serialize(sender.clone(), alpha_dest.clone(), &11u64, Flattrs::new())
                .unwrap(),
            monitored_return_handle(),
        );
        assert_eq!(
            time::timeout(Duration::from_secs(2), alpha_rx.recv())
                .await
                .expect("alpha_rx timed out")
                .expect("alpha_rx closed"),
            11
        );

        // gw_b → alpha: gw_b's post sees Via(gw_a.uid, ..), finds
        // gw_a in gw_b.peers, peels, forwards to gw_a.
        gw_b.post(
            MessageEnvelope::serialize(sender.clone(), alpha_dest.clone(), &22u64, Flattrs::new())
                .unwrap(),
            monitored_return_handle(),
        );
        assert_eq!(
            time::timeout(Duration::from_secs(2), alpha_rx.recv())
                .await
                .expect("alpha_rx timed out")
                .expect("alpha_rx closed"),
            22
        );

        // gw_a → beta: symmetric direction.
        gw_a.post(
            MessageEnvelope::serialize(sender.clone(), beta_dest.clone(), &33u64, Flattrs::new())
                .unwrap(),
            monitored_return_handle(),
        );
        assert_eq!(
            time::timeout(Duration::from_secs(2), beta_rx.recv())
                .await
                .expect("beta_rx timed out")
                .expect("beta_rx closed"),
            33
        );

        // Dropping the AttachGuard restores default_location and
        // removes the peer entries.
        drop(attach);
        assert_eq!(gw_a.default_location(), pre_a_default);
        assert_eq!(gw_b.default_location(), pre_b_default);
        assert!(gw_a.inner.peers.read().unwrap().is_empty());
        assert!(gw_b.inner.peers.read().unwrap().is_empty());
    }
}
