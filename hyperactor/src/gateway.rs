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
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;

use crate::Location;
use crate::ProcAddr;
use crate::ProcId;
use crate::channel;
use crate::channel::ChannelAddr;
use crate::channel::ChannelError;
use crate::channel::ChannelTransport;
use crate::mailbox::BoxedMailboxSender;
use crate::mailbox::DeliveryError;
use crate::mailbox::DialMailboxRouter;
use crate::mailbox::IntoBoxedMailboxSender as _;
use crate::mailbox::MailboxClient;
use crate::mailbox::MailboxSender as _;
use crate::mailbox::MailboxServer as _;
use crate::mailbox::MailboxServerHandle;
use crate::mailbox::MessageEnvelope;
use crate::mailbox::PortHandle;
use crate::mailbox::Undeliverable;
use crate::mailbox::UnroutableMailboxSender;
use crate::proc::AttachRequest;
use crate::proc::AttachRx;
use crate::proc::Host2Client;
use crate::proc::Proc;
use crate::proc::RegisterProc;
use crate::proc::WeakProc;
use crate::proc::attach_handshake;
use crate::proc::build_sigil_envelope;

/// Connectivity boundary for one or more procs.
#[derive(Clone)]
pub struct Gateway {
    inner: Arc<GatewayState>,
}

/// A proc attached to a [`Gateway`]. An entry is either:
///
/// * `Local`: in-process, identified by a [`WeakProc`] so the gateway
///   does not extend the proc's lifetime.
/// * `Remote`: out-of-process (or otherwise not addressable through a
///   local [`Proc`] handle), reached via a caller-supplied
///   [`BoxedMailboxSender`].
///
/// Use [`Gateway::attach`] with anything that converts into a `ProcEntry`.
/// Local procs convert via [`From<&Proc>`]; remote attachments
/// convert via [`From<(ProcId, BoxedMailboxSender)>`] (or are
/// constructed directly with [`ProcEntry::Remote`]).
#[derive(Clone)]
pub enum ProcEntry {
    /// An in-process proc, held as a weak handle.
    Local(ProcId, WeakProc),
    /// A proc reachable through a caller-supplied sender.
    Remote(ProcId, BoxedMailboxSender),
}

impl ProcEntry {
    /// The id of the proc represented by this entry.
    pub fn proc_id(&self) -> &ProcId {
        match self {
            ProcEntry::Local(id, _) | ProcEntry::Remote(id, _) => id,
        }
    }
}

impl From<&Proc> for ProcEntry {
    fn from(proc: &Proc) -> Self {
        ProcEntry::Local(proc.proc_id().clone(), proc.downgrade())
    }
}

impl From<(ProcId, BoxedMailboxSender)> for ProcEntry {
    fn from((proc_id, sender): (ProcId, BoxedMailboxSender)) -> Self {
        ProcEntry::Remote(proc_id, sender)
    }
}

/// Handle returned by [`Gateway::attach`] that detaches the proc from
/// the gateway when dropped. Drop is a no-op if the gateway has already
/// been dropped, or if the entry has been replaced under the same proc
/// id since this guard was issued (so we never evict another caller's
/// attachment).
pub struct AttachGuard {
    gateway: Weak<GatewayState>,
    proc_id: ProcId,
}

impl fmt::Debug for AttachGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AttachGuard")
            .field("proc_id", &self.proc_id)
            .finish()
    }
}

impl AttachGuard {
    /// The id of the proc whose attachment this guard owns.
    pub fn proc_id(&self) -> &ProcId {
        &self.proc_id
    }
}

impl Drop for AttachGuard {
    fn drop(&mut self) {
        let Some(state) = self.gateway.upgrade() else {
            return;
        };
        state.procs.write().unwrap().remove(&self.proc_id);
        // Notify the remote gateway so the matching return route is
        // dropped. Without this, the remote keeps an AttachSender for
        // a now-defunct proc id; messages addressed there bounce over
        // the duplex and fall through the client's forwarder (which
        // is also the via duplex), forming a loop.
        Gateway { inner: state }.broadcast_unregister(&self.proc_id);
    }
}

struct GatewayState {
    /// The location to use when no server is active.
    fallback_location: Location,

    /// The location used when constructing routeable addresses for
    /// newly bound refs.
    default_location: RwLock<Location>,

    /// Sender used to forward messages whose destination is not an
    /// attached proc. Replaceable via [`Gateway::set_forwarder`] and
    /// [`Gateway::serve_via`] so the gateway's outbound path can be
    /// rewired after construction.
    forwarder: RwLock<BoxedMailboxSender>,

    /// Procs attached to this gateway, keyed by runtime identity.
    /// See [`ProcEntry`] for the distinction between local and remote
    /// attachments.
    procs: RwLock<HashMap<ProcId, ProcEntry>>,

    /// Locations currently served by this gateway. The last location
    /// is the default advertised location.
    active_servers: RwLock<Vec<Location>>,

    /// Set by [`Gateway::serve_via`] when a via duplex is connected.
    /// While true, [`Gateway::attach`] and [`AttachGuard::drop`]
    /// notify the remote gateway of route changes by posting
    /// [`RegisterProc`] / [`UnregisterProc`] sigil envelopes through
    /// the forwarder, which (by `serve_via`) is the same duplex tx,
    /// so control messages stay FIFO with regular client→host
    /// envelopes posted via the same forwarder.
    via_active: AtomicBool,
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

    /// Create a gateway with an explicit default advertised location
    /// and outbound forwarder. Inbound traffic for procs not registered
    /// via [`Gateway::attach`] is handed off to `forwarder`.
    pub fn configured(default_location: Location, forwarder: BoxedMailboxSender) -> Self {
        Self {
            inner: Arc::new(GatewayState {
                fallback_location: default_location.clone(),
                default_location: RwLock::new(default_location),
                forwarder: RwLock::new(forwarder),
                procs: RwLock::new(HashMap::new()),
                active_servers: RwLock::new(Vec::new()),
                via_active: AtomicBool::new(false),
            }),
        }
    }

    /// The gateway's default advertised location.
    pub fn default_location(&self) -> Location {
        self.inner.default_location.read().unwrap().clone()
    }

    /// Set the gateway's default advertised location.
    pub fn set_default_location(&self, location: Location) {
        *self.inner.default_location.write().unwrap() = location;
    }

    /// Construct a routeable proc address using this gateway's default location.
    pub fn proc_addr(&self, proc_id: &ProcId) -> ProcAddr {
        ProcAddr::new(proc_id.clone(), self.default_location())
    }

    /// Replace the gateway's outbound forwarder.
    ///
    /// In-flight `post` calls already holding the previous sender are
    /// unaffected; only subsequent demuxes route to the new sender.
    pub fn set_forwarder(&self, forwarder: BoxedMailboxSender) {
        *self.inner.forwarder.write().unwrap() = forwarder;
    }

    /// Attach a proc to this gateway.
    ///
    /// Accepts anything that converts into a [`ProcEntry`]:
    ///
    /// * `&Proc` — attaches an in-process proc by weak handle. The
    ///   gateway will deliver messages addressed to the proc's id
    ///   directly to `proc.muxer()` when
    ///   [`Proc::is_local_delivery_target`] holds.
    /// * `(ProcId, BoxedMailboxSender)` — attaches a remote proc. The
    ///   gateway routes envelopes whose destination matches `proc_id`
    ///   through the supplied sender instead of the gateway's
    ///   forwarder.
    ///
    /// Panics if a live entry with the same id is already attached. A
    /// dead local entry whose [`WeakProc`] has been dropped is replaced
    /// silently.
    ///
    /// Returns an [`AttachGuard`] that detaches the proc when dropped.
    /// Callers must keep the guard alive for as long as the registration
    /// should remain in place; for in-process procs, the guard is held
    /// inside the [`Proc`] itself so the proc's lifetime drives detach.
    #[must_use = "the returned AttachGuard detaches the proc when dropped"]
    pub fn attach(&self, entry: impl Into<ProcEntry>) -> AttachGuard {
        let entry = entry.into();
        let proc_id = entry.proc_id().clone();
        let existing = self
            .inner
            .procs
            .write()
            .unwrap()
            .insert(proc_id.clone(), entry);
        match existing {
            // No prior entry, or the prior entry was a dead local handle
            // whose slot is now ours.
            None => {}
            Some(ProcEntry::Local(_, weak)) if weak.upgrade().is_none() => {}
            Some(ProcEntry::Local(_, _)) | Some(ProcEntry::Remote(_, _)) => {
                panic!("gateway already has a proc with id {}", proc_id)
            }
        }
        self.broadcast_register(&proc_id);
        AttachGuard {
            gateway: Arc::downgrade(&self.inner),
            proc_id,
        }
    }

    /// Notify any active via duplex that a new proc has been
    /// registered locally, so the remote gateway can install a return
    /// route to it. Posts through the gateway's forwarder so
    /// control messages stay FIFO with regular client→host envelopes.
    /// Best-effort: a failed serialize is logged and dropped.
    fn broadcast_register(&self, proc_id: &ProcId) {
        self.broadcast_via_control("RegisterProc", proc_id, |id| RegisterProc {
            proc_id: id.clone(),
        });
    }

    /// Notify any active via duplex that a proc has been unregistered
    /// locally, so the remote gateway can drop its return route to it.
    /// See [`Gateway::broadcast_register`] for ordering semantics.
    fn broadcast_unregister(&self, proc_id: &ProcId) {
        self.broadcast_via_control("UnregisterProc", proc_id, |id| {
            crate::proc::UnregisterProc {
                proc_id: id.clone(),
            }
        });
    }

    fn broadcast_via_control<T, F>(&self, kind: &'static str, proc_id: &ProcId, build: F)
    where
        T: serde::Serialize + typeuri::Named,
        F: FnOnce(&ProcId) -> T,
    {
        if !self.inner.via_active.load(Ordering::Acquire) {
            return;
        }
        let envelope = match build_sigil_envelope(&build(proc_id)) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(
                    proc_id = %proc_id,
                    kind,
                    error = %e,
                    "failed to serialize control envelope; remote gateway will not learn this route change"
                );
                return;
            }
        };
        // Use the same path as regular envelopes so the control
        // message is FIFO with traffic the caller posted before us.
        let forwarder = {
            let forwarder = self.inner.forwarder.read().unwrap();
            if !self.inner.via_active.load(Ordering::Acquire) {
                return;
            }
            forwarder.clone()
        };
        forwarder.post(envelope, crate::mailbox::monitored_return_handle());
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
            location,
            handle,
            stopped: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Connect this gateway to a remote gateway via the host duplex
    /// attach protocol.
    ///
    /// Dials `addr`, sends the currently-attached proc ids in an
    /// [`AttachRequest`] so the remote gateway registers return
    /// routes for each, installs the duplex sender as this gateway's
    /// outbound forwarder, and serves inbound traffic from the duplex
    /// locally. The returned [`ServeViaHandle`] owns the duplex
    /// session and the inbound serve handle; dropping it disables
    /// dynamic via registration, restores the previous forwarder, and
    /// tears down the duplex.
    ///
    /// While the via duplex is active, subsequent [`Gateway::attach`]
    /// calls on this gateway notify the remote gateway via a
    /// [`RegisterProc`] sigil envelope so newly-attached procs become
    /// reachable from the remote without a new handshake.
    pub async fn serve_via(&self, addr: ChannelAddr) -> anyhow::Result<ServeViaHandle> {
        if self.inner.via_active.load(Ordering::Acquire) {
            anyhow::bail!("gateway already has an active serve_via connection");
        }
        let proc_ids: Vec<ProcId> = self.inner.procs.read().unwrap().keys().cloned().collect();
        let handshake = attach_handshake(addr, AttachRequest::Gateway { proc_ids }).await?;
        let previous_forwarder = {
            let mut forwarder = self.inner.forwarder.write().unwrap();
            if self
                .inner
                .via_active
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                anyhow::bail!("gateway already has an active serve_via connection");
            }
            std::mem::replace(
                &mut *forwarder,
                MailboxClient::new(handshake.duplex_tx).into_boxed(),
            )
        };
        let serve_handle = self.serve_rx(AttachRx(handshake.duplex_rx));
        Ok(ServeViaHandle {
            _serve_handle: serve_handle,
            _duplex_client: handshake.duplex_client,
            gateway: self.clone(),
            previous_forwarder,
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
    /// This first flushes the muxers for all live procs attached to the
    /// gateway, then flushes the gateway's forwarder. Flushing the proc muxers
    /// drains local delivery and any return paths rooted in attached procs;
    /// flushing the forwarder drains outbound traffic that the gateway has
    /// routed away from those procs.
    ///
    /// The live proc set is snapshotted before awaiting, so we do not hold the
    /// proc map while flushing. Procs that have already been dropped are
    /// ignored. Concurrent posts may race with this operation; `flush` only
    /// guarantees that each flushed sender observes its usual sender-level
    /// flush semantics at the time it is flushed.
    pub(crate) async fn flush(&self) -> Result<(), anyhow::Error> {
        let (local_procs, remote_senders) = {
            let procs = self.inner.procs.read().unwrap();
            let mut local = Vec::with_capacity(procs.len());
            let mut remote = Vec::with_capacity(procs.len());
            for entry in procs.values() {
                match entry {
                    ProcEntry::Local(_, weak) => {
                        if let Some(proc) = weak.upgrade() {
                            local.push(proc);
                        }
                    }
                    ProcEntry::Remote(_, sender) => remote.push(sender.clone()),
                }
            }
            (local, remote)
        };
        let proc_flushes = local_procs.iter().map(|proc| proc.muxer().flush());
        let remote_flushes = remote_senders.iter().map(|sender| sender.flush());
        futures::future::try_join_all(proc_flushes.chain(remote_flushes)).await?;
        let forwarder = self.inner.forwarder.read().unwrap().clone();
        forwarder.flush().await
    }
}

impl fmt::Debug for Gateway {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Gateway")
            .field("default_location", &self.default_location())
            .finish()
    }
}

/// A running gateway server.
#[derive(Debug)]
pub struct GatewayServeHandle {
    gateway: Gateway,
    location: Location,
    handle: MailboxServerHandle,
    stopped: Arc<AtomicBool>,
}

impl GatewayServeHandle {
    /// Signal the gateway server to stop.
    pub fn stop(&self, reason: &str) {
        if !self.stopped.swap(true, Ordering::AcqRel) {
            self.handle.stop(reason);
            self.gateway.remove_server(&self.location);
        }
    }
}

impl Future for GatewayServeHandle {
    type Output = <MailboxServerHandle as Future>::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: `handle` is structurally pinned with `GatewayServeHandle`:
        // this type is `!Unpin` because `MailboxServerHandle` is `!Unpin`, it
        // has no `Drop` impl that moves `handle`, and no method moves `handle`
        // out of a pinned `GatewayServeHandle`.
        let handle = unsafe {
            self.as_mut()
                .map_unchecked_mut(|container| &mut container.handle)
        };
        let result = handle.poll(cx);
        if result.is_ready() {
            // SAFETY: We only mutate unpinned bookkeeping fields after polling
            // the pinned `handle`; this does not move `handle` or any other
            // pinned field out of `self`.
            let this = unsafe { self.get_unchecked_mut() };
            if !this.stopped.swap(true, Ordering::AcqRel) {
                this.gateway.remove_server(&this.location);
            }
        }
        result
    }
}

/// Owned state for a [`Gateway::serve_via`] connection.
pub struct ServeViaHandle {
    // Drop order matters: tear down the inbound serve before the
    // duplex session that backs it.
    _serve_handle: MailboxServerHandle,
    _duplex_client: channel::duplex::DuplexClient<MessageEnvelope, Host2Client>,
    gateway: Gateway,
    previous_forwarder: BoxedMailboxSender,
}

impl fmt::Debug for ServeViaHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServeViaHandle").finish()
    }
}

impl Drop for ServeViaHandle {
    fn drop(&mut self) {
        let mut forwarder = self.gateway.inner.forwarder.write().unwrap();
        *forwarder = self.previous_forwarder.clone();
        self.gateway
            .inner
            .via_active
            .store(false, Ordering::Release);
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
            None => envelope.undeliverable(
                DeliveryError::BrokenLink("failed to upgrade WeakGateway".to_string()),
                return_handle,
            ),
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
        let dest_proc = envelope.dest().actor_addr().proc_addr();
        let entry = self
            .inner
            .procs
            .read()
            .unwrap()
            .get(dest_proc.id())
            .cloned();
        match entry {
            Some(ProcEntry::Local(_, weak)) => match weak.upgrade() {
                Some(proc) if proc.is_local_delivery_target(&dest_proc) => {
                    proc.muxer().post(envelope, return_handle);
                    return;
                }
                Some(_) => {
                    // Proc is alive but not a local-delivery target for
                    // this destination; fall through to the forwarder.
                }
                None => {
                    // Dead local entry. Best-effort cleanup; double-check
                    // under the write lock so we do not evict a slot that
                    // another thread has just re-attached.
                    let mut procs = self.inner.procs.write().unwrap();
                    if let Some(ProcEntry::Local(_, weak)) = procs.get(dest_proc.id())
                        && weak.upgrade().is_none()
                    {
                        procs.remove(dest_proc.id());
                    }
                }
            },
            Some(ProcEntry::Remote(_, sender)) => {
                sender.post(envelope, return_handle);
                return;
            }
            None => {}
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
    use tokio::time;

    use super::*;
    use crate::Endpoint as _;
    use crate::Label;
    use crate::mailbox::MailboxSender;
    use crate::mailbox::PortLocation;
    use crate::mailbox::monitored_return_handle;
    use crate::port::Port;
    use crate::proc::Proc;
    use crate::testing::ids::test_actor_id;
    use crate::testing::pingpong::PingPongActor;
    use crate::testing::pingpong::PingPongMessage;

    /// `Gateway::post_unchecked` demuxes inbound envelopes by
    /// destination `ProcId` to the matching attached proc's muxer,
    /// and falls through to the configured forwarder for unknown
    /// destinations. Attached procs only receive envelopes addressed
    /// to them — a stranger-addressed envelope does not leak to local
    /// receivers.
    #[tokio::test]
    async fn test_gateway_post_demuxes_by_proc_id() {
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

        let (alpha_client, _) = alpha.client("client").unwrap();
        let (alpha_port, mut alpha_rx) = alpha_client.bind_handler_port::<u64>();
        let PortLocation::Bound(alpha_dest) = alpha_port.location() else {
            panic!("alpha handler port must be bound");
        };

        let (beta_client, _) = beta.client("client").unwrap();
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
        assert_eq!(forwarded.load(Ordering::SeqCst), 0);

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
        assert_eq!(forwarded.load(Ordering::SeqCst), 0);

        let stranger_proc = ProcAddr::instance(ChannelAddr::Local(9999), "stranger");
        let stranger_dest = stranger_proc
            .actor_addr("ghost")
            .port_addr(Port::from(0u64));
        gateway.post(
            MessageEnvelope::serialize(sender, stranger_dest, &333u64, Flattrs::new()).unwrap(),
            monitored_return_handle(),
        );
        assert_eq!(forwarded.load(Ordering::SeqCst), 1);
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

        let (client, _) = alpha.client("client").unwrap();
        let (undeliverable_msg_tx, mut undeliverable_rx) =
            client.open_port::<Undeliverable<MessageEnvelope>>();

        let ping_actor = PingPongActor::new(Some(undeliverable_msg_tx.bind()), None, None);
        let pong_actor = PingPongActor::new(Some(undeliverable_msg_tx.bind()), None, None);
        let ping_handle = alpha.spawn::<PingPongActor>("ping", ping_actor).unwrap();
        let pong_handle = beta.spawn::<PingPongActor>("pong", pong_actor).unwrap();

        let (local_port, local_receiver) = client.open_once_port();

        ping_handle.post(
            &client,
            PingPongMessage(10, pong_handle.bind(), local_port.bind()),
        );

        let received = time::timeout(Duration::from_secs(5), local_receiver.recv())
            .await
            .expect("local_receiver timed out")
            .expect("ping pong did not complete");
        assert!(received);

        assert!(
            time::timeout(Duration::from_millis(50), undeliverable_rx.recv())
                .await
                .is_err(),
            "unexpected undeliverable during cross-proc ping-pong",
        );
    }

    /// `Gateway::attach` panics when a second proc with the same
    /// `ProcId` is built against the same gateway while the first is
    /// still alive. The check is in `Gateway::attach`, invoked from
    /// `Proc::builder().build()` via `Proc::from_parts_unchecked`.
    #[test]
    #[should_panic(expected = "gateway already has a proc with id")]
    fn test_gateway_attach_panics_on_duplicate_live_proc() {
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

        // Sanity: two procs attached, both live.
        assert_eq!(gateway.inner.procs.read().unwrap().len(), 2);

        gateway.flush().await.unwrap();

        assert_eq!(alpha_flushed.load(Ordering::SeqCst), 1);
        assert_eq!(beta_flushed.load(Ordering::SeqCst), 1);
        assert_eq!(forwarder_flushed.load(Ordering::SeqCst), 1);
    }

    /// After the gateway is dropped, `WeakGateway::post_unchecked`
    /// (the sender used by gateway-served mailbox tasks) bounces
    /// envelopes as `BrokenLink` rather than panicking or hanging.
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
        let (scratch_client, _) = scratch.client("return").unwrap();
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

        // Post directly through the WeakGateway. Upgrade fails →
        // envelope.undeliverable(BrokenLink, return_handle) sends
        // synchronously to our return port.
        weak.post(envelope, return_handle);

        let Undeliverable::Message(envelope) =
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
                .errors()
                .iter()
                .any(|e| matches!(e, DeliveryError::BrokenLink(_))),
            "expected BrokenLink bounce, got {:?}",
            envelope.errors(),
        );
    }

    /// Dropping the `Proc` drops its `AttachGuard`, which eagerly
    /// removes the entry from the gateway's `procs` map. A subsequent
    /// attach with the same `ProcId` is therefore a fresh insert — no
    /// panic, no stale entry to replace.
    #[tokio::test]
    async fn test_gateway_attach_after_proc_drop() {
        let gateway = Gateway::isolated();
        let proc_id = ProcId::instance(Label::strip("alpha"));

        let first = Proc::builder()
            .proc_id(proc_id.clone())
            .shared_gateway(gateway.clone())
            .build()
            .unwrap();
        drop(first);

        // AttachGuard::drop removed the entry from the map.
        assert_eq!(gateway.inner.procs.read().unwrap().len(), 0);

        // The slot is free, so attaching a new proc with the same id
        // is a fresh insert — no panic.
        let second = Proc::builder()
            .proc_id(proc_id.clone())
            .shared_gateway(gateway.clone())
            .build()
            .unwrap();
        assert_eq!(gateway.inner.procs.read().unwrap().len(), 1);

        // Verify the new proc is reachable via the gateway.
        let (client, _) = second.client("client").unwrap();
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

        let s1 = Gateway::serve(&gateway, ChannelAddr::any(ChannelTransport::Local)).unwrap();
        let loc1 = gateway.default_location();
        let s2 = Gateway::serve(&gateway, ChannelAddr::any(ChannelTransport::Local)).unwrap();
        let loc2 = gateway.default_location();
        let s3 = Gateway::serve(&gateway, ChannelAddr::any(ChannelTransport::Local)).unwrap();
        let loc3 = gateway.default_location();

        // First serve(any) reuses the gateway's reserved fallback
        // address (see resolve_serve_addr); subsequent serves
        // allocate fresh ports.
        assert_eq!(loc1, fallback);
        assert_ne!(loc1, loc2);
        assert_ne!(loc2, loc3);
        assert_ne!(loc1, loc3);

        // Middle handle stops first: default stays at loc3 (still the
        // last entry in active_servers).
        s2.stop("test");
        s2.await.unwrap().unwrap();
        assert_eq!(gateway.default_location(), loc3);

        // Last handle stops: default falls back to loc1.
        s3.stop("test");
        s3.await.unwrap().unwrap();
        assert_eq!(gateway.default_location(), loc1);

        // Final handle stops: default reverts to the
        // construction-time fallback.
        s1.stop("test");
        s1.await.unwrap().unwrap();
        assert_eq!(gateway.default_location(), fallback);
    }
}
