/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Connectivity layer for Hyperactor procs.
//!
//! A proc owns actor lifecycle and mailboxes; a [`Gateway`] owns how that proc
//! is reached. Attached procs derive advertised addresses from the gateway's
//! default location, receive inbound traffic through the gateway, and send
//! outbound traffic through its routing decision.
//!
//! A gateway routes by first checking for a `Via(uid, ...)` source-route hop,
//! then by trying local proc delivery, and finally by forwarding to its
//! configured outbound sender. Use [`Gateway::serve`] or [`Gateway::bind`] plus
//! [`Gateway::serve_bound`] to accept inbound traffic, and use
//! [`Gateway::serve_via`] to attach this gateway to a remote duplex gateway.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::sync::Weak;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt as _;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::Location;
use crate::PortAddr;
use crate::ProcAddr;
use crate::ProcId;
use crate::channel;
use crate::channel::ChannelAddr;
use crate::channel::ChannelError;
use crate::channel::ChannelTransport;
use crate::channel::Rx;
use crate::channel::Tx;
use crate::id::Uid;
use crate::mailbox::BoxedMailboxSender;
use crate::mailbox::DeliveryFailure;
use crate::mailbox::DialMailboxRouter;
use crate::mailbox::IntoBoxedMailboxSender as _;
use crate::mailbox::MailboxClient;
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

// ---------------------------------------------------------------------------
// Gateway attach protocol
// ---------------------------------------------------------------------------
//
// Gateways are the connectivity layer; all on-the-wire attach is
// gateway-to-gateway. A client gateway dials a peer's accept endpoint
// and sends [`AttachRequest`] carrying its own uid; the peer replies
// with [`AttachAck`] carrying the via location through which the client
// is reachable, then both ends serve regular [`MessageEnvelope`]
// traffic on the same duplex.

/// Label used for attach-control sigil envelopes.
const ATTACH_SIGIL_LABEL: &str = "attach";

/// Upper bound on how long [`Gateway::serve_via`] waits for the peer's
/// [`AttachAck`]. A peer that accepts the connection but never replies
/// (hung, misconfigured, or running an older protocol) would otherwise
/// block the caller — typically Python `bootstrap_host` — indefinitely.
const ATTACH_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

/// First message a gateway sends when attaching to a peer. The peer
/// records `uid` in its [`peers`] table; afterwards, any
/// destination whose outermost location hop is `Via(uid, ...)` is
/// peeled by the peer and forwarded back over the duplex.
#[derive(Debug, Clone, Serialize, Deserialize, typeuri::Named)]
pub(crate) struct AttachRequest {
    /// The attaching gateway's uid.
    pub(crate) uid: Uid,
}
wirevalue::register_type!(AttachRequest);

/// Acknowledgement returned by a peer gateway during attach. Carries
/// the via location the client should advertise as its
/// [`default_location`] — `Via(client_uid, peer_default_location)`.
#[derive(Debug, Clone, Serialize, Deserialize, typeuri::Named)]
pub(crate) struct AttachAck {
    /// The location through which the client is now reachable.
    pub(crate) location: Location,
}
wirevalue::register_type!(AttachAck);

/// Wire protocol for the peer → client direction on a duplex attach
/// connection.
#[derive(Debug, Serialize, Deserialize, typeuri::Named)]
#[expect(
    clippy::large_enum_variant,
    reason = "wire-protocol enum; boxing Envelope would ripple through channel/networking destructure sites"
)]
pub(crate) enum AttachWire {
    /// First message: the peer acknowledges and assigns a via location.
    Ack(AttachAck),
    /// Subsequent messages: routed envelopes.
    Envelope(MessageEnvelope),
}
wirevalue::register_type!(AttachWire);

/// [`Rx<MessageEnvelope>`](channel::Rx) adapter that unwraps
/// [`AttachWire::Envelope`] from a duplex receiver. Errors if the
/// peer sends another [`AttachWire::Ack`] after the handshake.
pub(crate) struct AttachRx(pub(crate) channel::duplex::DuplexRx<AttachWire>);

#[async_trait]
impl channel::Rx<MessageEnvelope> for AttachRx {
    async fn recv(&mut self) -> Result<MessageEnvelope, ChannelError> {
        match self.0.recv().await? {
            AttachWire::Envelope(envelope) => Ok(envelope),
            AttachWire::Ack(_) => Err(ChannelError::Other(anyhow::anyhow!(
                "unexpected attach ack after handshake"
            ))),
        }
    }

    fn addr(&self) -> ChannelAddr {
        self.0.addr()
    }

    async fn join(self) {
        self.0.join().await
    }
}

/// [`MailboxSender`] adapter that wraps outbound [`MessageEnvelope`]s
/// in [`AttachWire::Envelope`] before posting to a peer's
/// [`DuplexTx<AttachWire>`]. Used on the accept side to send messages
/// to an attached client gateway.
#[derive(Clone)]
pub(crate) struct AttachSender(pub(crate) channel::duplex::DuplexTx<AttachWire>);

#[async_trait]
impl crate::mailbox::MailboxSender for AttachSender {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        _return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        self.0.post(AttachWire::Envelope(envelope));
    }
}

struct PreboundAcceptServer {
    inner: channel::duplex::DuplexServer<MessageEnvelope, AttachWire>,
}

impl PreboundAcceptServer {
    fn duplex(
        addr: ChannelAddr,
        listener: Option<std::net::TcpListener>,
    ) -> Result<Self, channel::ServerError> {
        let inner = channel::duplex::serve::<MessageEnvelope, AttachWire>(addr, listener)?;
        Ok(Self { inner })
    }

    fn addr(&self) -> &ChannelAddr {
        self.inner.addr()
    }
}

/// A server endpoint bound by [`Gateway::bind`] and served by
/// [`Gateway::serve_bound`]. Duplex-capable transports yield a
/// gateway-attach-capable accept loop; simplex transports yield a
/// straight mailbox serve. Callers (e.g. `Host`) hold this opaquely
/// between bind and serve and never inspect the transport themselves.
pub struct BoundServer {
    inner: BoundServerKind,
}

enum BoundServerKind {
    Duplex(PreboundAcceptServer),
    Simplex {
        addr: ChannelAddr,
        rx: channel::ChannelRx<MessageEnvelope>,
    },
}

impl BoundServer {
    /// The bound address.
    pub fn addr(&self) -> &ChannelAddr {
        match &self.inner {
            BoundServerKind::Duplex(server) => server.addr(),
            BoundServerKind::Simplex { addr, .. } => addr,
        }
    }
}

/// Serialize a control payload into a placeholder [`MessageEnvelope`]
/// suitable for posting on a duplex client→peer channel.
///
/// Sender/dest ids are placeholders the peer consumes without routing;
/// `return_undeliverable` is cleared so an envelope that ever escapes
/// into the forwarder is dropped rather than bounced to the fake
/// sender.
fn build_sigil_envelope<T>(payload: &T) -> anyhow::Result<MessageEnvelope>
where
    T: serde::Serialize + typeuri::Named,
{
    let signal_actor_id = crate::ActorAddr::root(
        ProcAddr::singleton(
            ChannelAddr::any(channel::ChannelTransport::Local),
            ATTACH_SIGIL_LABEL,
        ),
        crate::id::Label::strip(ATTACH_SIGIL_LABEL),
    );
    let signal_port = signal_actor_id.port_addr(crate::port::Port::from(0u64));
    let mut envelope =
        MessageEnvelope::serialize(signal_actor_id, signal_port, payload, Default::default())?;
    envelope.set_return_undeliverable(false);
    Ok(envelope)
}

/// Connectivity boundary for one or more procs.
#[derive(Clone)]
pub struct Gateway {
    inner: Arc<GatewayState>,
}

enum ProcRoute {
    Local(WeakProc),
    Remote {
        via_uid: Uid,
        sender: BoxedMailboxSender,
    },
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
        let mut procs = state.procs.write().unwrap();
        if procs
            .get(&self.proc_id)
            .is_some_and(|route| matches!(route, ProcRoute::Local(weak) if weak.ptr_eq(&self.weak)))
        {
            procs.remove(&self.proc_id);
        }
    }
}

/// Handle returned by [`Gateway::attach_proc_sender`] that removes the
/// proc route when dropped.
#[must_use = "dropping the ProcRouteGuard immediately removes the proc route"]
pub struct ProcRouteGuard {
    gateway: Weak<GatewayState>,
    proc_id: ProcId,
    via_uid: Uid,
}

impl fmt::Debug for ProcRouteGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcRouteGuard")
            .field("proc_id", &self.proc_id)
            .field("via_uid", &self.via_uid)
            .finish()
    }
}

impl Drop for ProcRouteGuard {
    fn drop(&mut self) {
        let Some(state) = self.gateway.upgrade() else {
            return;
        };
        let mut procs = state.procs.write().unwrap();
        if procs.get(&self.proc_id).is_some_and(
            |route| matches!(route, ProcRoute::Remote { via_uid, .. } if via_uid == &self.via_uid),
        ) {
            procs.remove(&self.proc_id);
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

/// Error returned by [`Gateway::attach_proc_sender`] when a proc route
/// is already registered for the given proc id.
#[derive(Debug, thiserror::Error)]
#[error("gateway already has a proc route for id {proc_id}")]
pub struct ProcRouteError {
    /// The proc id that was already registered.
    pub proc_id: ProcId,
}

/// Outbound routing state for a gateway, kept behind a single lock so
/// the two fields stay mutually consistent across [`Gateway::serve_via`]
/// swaps and restores.
struct Routing {
    /// The advertised location for newly bound refs. After
    /// [`Gateway::serve_via`], this is replaced with the peer-supplied
    /// `Via(self.uid, peer_default_location)` location, so any address
    /// handed out by this gateway carries the via prefix and is
    /// source-routable back through the duplex.
    default_location: Location,

    /// Sender used to forward messages whose destination is neither an
    /// attached proc nor matched by [`peers`]. Swapped in by
    /// [`Gateway::serve_via`] and restored when the resulting handle
    /// is dropped or joined.
    forwarder: BoxedMailboxSender,
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

    /// Outbound routing state, held under one lock so the advertised
    /// location and the forwarder transition together. [`Gateway::serve_via`]
    /// swaps both in a single critical section and restores them likewise,
    /// so concurrent readers never observe a half-swapped pair (a new
    /// via-prefixed location with the old forwarder, or vice versa).
    routing: RwLock<Routing>,

    /// Proc routes registered with this gateway, keyed by proc id.
    /// Local routes hold weak proc references for in-process delivery;
    /// remote routes hold senders for child procs spawned behind this
    /// gateway.
    procs: RwLock<HashMap<ProcId, ProcRoute>>,

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
                routing: RwLock::new(Routing {
                    default_location,
                    forwarder,
                }),
                procs: RwLock::new(HashMap::new()),
                active_servers: RwLock::new(Vec::new()),
                peers: RwLock::new(HashMap::new()),
            }),
        }
    }

    /// This gateway's stable uid. Peers route messages back to procs
    /// attached here by addressing them as
    /// `Location::Via(this_uid, inner)`.
    pub fn uid(&self) -> &Uid {
        &self.inner.uid
    }

    /// The gateway's default advertised location.
    pub fn default_location(&self) -> Location {
        self.inner.routing.read().unwrap().default_location.clone()
    }

    /// The outbound forwarder. Inbound traffic for destinations that
    /// don't match a bound proc, route, or via peer is handed off to
    /// this sender.
    pub fn forwarder(&self) -> BoxedMailboxSender {
        self.inner.routing.read().unwrap().forwarder.clone()
    }

    /// Set the gateway's default advertised location.
    pub fn set_default_location(&self, location: Location) {
        self.inner.routing.write().unwrap().default_location = location;
    }

    /// Attach a proc to this gateway, establishing the two-way
    /// relationship between them: the gateway can deliver inbound
    /// traffic directly to the proc's muxer, and the proc routes its
    /// egress through the gateway.
    ///
    /// The gateway delivers messages addressed to this id directly to
    /// the muxer when [`Proc::is_local_delivery_target`] holds;
    /// otherwise routing continues to the appropriate peer or
    /// forwarder.
    ///
    /// Internal-only: only [`Proc`] construction calls this, and the
    /// resulting [`AttachedProcGuard`] is held inside the proc itself
    /// so the proc's lifetime drives detachment. The public Gateway
    /// API exposes gateway connectivity via [`Gateway::attach`] (an
    /// in-process bidirectional bind), [`Gateway::attach_peer`] (a
    /// sender-based via entry for a peer gateway uid), and
    /// [`Gateway::serve_via`] (a duplex-attach connection to a remote
    /// gateway). Hosts register spawned child procs with
    /// [`Gateway::attach_proc_sender`].
    ///
    /// Panics if a live proc with the same id is already attached. A
    /// dead entry whose [`WeakProc`] has been dropped is replaced
    /// silently.
    pub(crate) fn attach_proc(&self, proc: &Proc) -> AttachedProcGuard {
        let proc_id = proc.proc_id().clone();
        let weak = proc.downgrade();
        let mut procs = self.inner.procs.write().unwrap();
        match procs.get(&proc_id) {
            // No prior entry, or the prior entry was a dead handle
            // whose slot is now ours.
            None => {}
            Some(ProcRoute::Local(weak)) if weak.upgrade().is_none() => {}
            Some(_) => {
                panic!("gateway already has a proc attached with id {}", proc_id)
            }
        }
        procs.insert(proc_id.clone(), ProcRoute::Local(weak.clone()));
        AttachedProcGuard {
            gateway: Arc::downgrade(&self.inner),
            proc_id,
            weak,
        }
    }

    /// Register a spawned child proc that is reachable through
    /// `sender`.
    ///
    /// Messages addressed to `proc_id` with an outermost
    /// [`Location::Via`] hop carrying `via_uid` are forwarded through
    /// `sender` after peeling the hop. The returned guard removes the
    /// route on drop.
    ///
    /// Returns [`ProcRouteError`] if a live local proc or another
    /// remote route is already registered under `proc_id`. A dead
    /// local entry is replaced.
    pub fn attach_proc_sender(
        &self,
        proc_id: ProcId,
        via_uid: Uid,
        sender: BoxedMailboxSender,
    ) -> Result<ProcRouteGuard, ProcRouteError> {
        let mut procs = self.inner.procs.write().unwrap();
        match procs.get(&proc_id) {
            None => {}
            Some(ProcRoute::Local(weak)) if weak.upgrade().is_none() => {}
            Some(_) => {
                return Err(ProcRouteError { proc_id });
            }
        }
        procs.insert(
            proc_id.clone(),
            ProcRoute::Remote {
                via_uid: via_uid.clone(),
                sender,
            },
        );
        Ok(ProcRouteGuard {
            gateway: Arc::downgrade(&self.inner),
            proc_id,
            via_uid,
        })
    }

    /// Register a gateway peer that is reachable through `sender`.
    /// Messages whose destination location has an outermost
    /// [`Location::Via`] hop carrying `uid` are forwarded through
    /// `sender` after peeling the hop. The returned guard removes the
    /// entry on drop.
    ///
    /// Used by [`Gateway::attach`] and the duplex accept loop in
    /// [`Gateway::serve_duplex`]. Use [`Gateway::attach_proc_sender`]
    /// for a spawned child proc behind this gateway.
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
        WeakGateway::new(self, false).serve(rx)
    }

    pub(crate) fn serve_rx_after_peeled_via(
        &self,
        rx: impl channel::Rx<MessageEnvelope> + Send + 'static,
    ) -> MailboxServerHandle {
        WeakGateway::new(self, true).serve(rx)
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
            kind: HandleKind::Serve {
                location: Some(location),
            },
        })
    }

    /// Open a duplex endpoint that accepts both regular inbound
    /// envelope traffic and gateway-attach handshakes from peers.
    ///
    /// On each connection the accept loop reads the first message:
    /// * an [`AttachRequest`] sigil enters the attach branch — this
    ///   gateway records the peer's uid in `peers` (so source
    ///   routes addressed to `Via(peer_uid, ...)` flow back through
    ///   the duplex), replies with [`AttachAck`] carrying
    ///   `Via(peer_uid, default_location)`, and serves remaining
    ///   traffic from the duplex into this gateway.
    /// * a regular [`MessageEnvelope`] enters the inbound branch and
    ///   is served straight through.
    ///
    /// Returns a [`GatewayServeHandle`] of kind `ServeDuplex`. The
    /// accept loop respects `.stop("reason")`.
    ///
    /// Errors if `addr`'s transport cannot carry the duplex protocol
    /// (e.g. local transport). Callers that may be handed a non-duplex
    /// address should branch on
    /// [`ChannelTransport::supports_duplex`] and use [`serve`] instead.
    pub fn serve_duplex(&self, addr: ChannelAddr) -> Result<GatewayServeHandle, ChannelError> {
        if !addr.transport().supports_duplex() {
            return Err(ChannelError::Other(anyhow::anyhow!(
                "serve_duplex requires a duplex-capable transport, but {addr} does not support duplex"
            )));
        }
        let server = PreboundAcceptServer::duplex(addr, None)
            .map_err(|e| ChannelError::Other(anyhow::anyhow!("{e}")))?;
        Ok(self.serve_duplex_with_server(server))
    }

    /// [`serve_duplex`] variant that takes a pre-bound
    /// [`PreboundAcceptServer`]. Use when the caller needs to know the
    /// bound address before starting the accept loop (e.g. to construct
    /// addresses that reference it).
    fn serve_duplex_with_server(&self, server: PreboundAcceptServer) -> GatewayServeHandle {
        let bound_addr = server.addr().clone();
        let location = Location::from(bound_addr.clone());
        self.add_server(location.clone());

        // The accept loop and its per-connection tasks are driven by a
        // single cancellation token. `stop()` cancels it; dropping the
        // handle without stopping leaves the token uncancelled, so the
        // loop keeps running — matching the [`MailboxServerHandle`]
        // detach-on-drop convention.
        let cancel_token = CancellationToken::new();
        let loop_token = cancel_token.clone();
        let gateway = self.clone();
        let inner = server.inner;
        let join_handle = tokio::spawn(async move {
            duplex_accept_loop(inner, bound_addr, gateway, loop_token).await;
            Ok::<(), MailboxServerError>(())
        });
        // The inner handle exists only so `join()`/`await` can await the
        // accept-loop task; its stop watch is never signaled, because
        // stop flows through `cancel_token` instead.
        let (idle_stop_tx, _idle_stop_rx) = watch::channel(false);
        let handle = MailboxServerHandle::from_parts(join_handle, idle_stop_tx);
        GatewayServeHandle {
            gateway: self.clone(),
            handle,
            stopped: false,
            kind: HandleKind::ServeDuplex {
                location: Some(location),
                cancel_token,
            },
        }
    }

    /// Bind this gateway's server endpoint on `addr`, choosing a duplex
    /// accept server or a simplex receiver based on the transport, and
    /// adopt the bound address as the gateway's advertised location.
    ///
    /// Splitting bind from [`serve_bound`] lets a caller construct procs
    /// whose addresses derive from this gateway's (now bound-address-aware)
    /// default location *before* the accept loop runs. This owns all the
    /// connectivity policy a host used to carry: the transport choice and
    /// the location adoption live here, so callers operate on the gateway
    /// without inspecting transports or rewriting its location.
    ///
    /// The bound address is adopted only when no via session is active. A
    /// via session's `default_location` is `Via(self_uid, peer_addr)`;
    /// overwriting it with the bare bound address would drop the prefix and
    /// leave every proc on this gateway advertised at an address the peer
    /// cannot reach.
    pub fn bind(
        &self,
        addr: ChannelAddr,
        listener: Option<std::net::TcpListener>,
    ) -> Result<BoundServer, ChannelError> {
        let bound = if addr.transport().supports_duplex() {
            BoundServerKind::Duplex(
                PreboundAcceptServer::duplex(addr, listener)
                    .map_err(|e| ChannelError::Other(anyhow::anyhow!("{e}")))?,
            )
        } else {
            // Local transports can't carry the duplex protocol; bind a
            // simplex receiver directly.
            let (addr, rx) = channel::serve_with_listener(addr, listener)?;
            BoundServerKind::Simplex { addr, rx }
        };
        let bound = BoundServer { inner: bound };
        if self.default_location().as_via().is_none() {
            self.set_default_location(Location::from(bound.addr().clone()));
        }
        Ok(bound)
    }

    /// Serve an endpoint previously bound by [`bind`]. Duplex endpoints
    /// run the gateway-attach accept loop; simplex endpoints run a
    /// straight mailbox serve.
    pub fn serve_bound(&self, bound: BoundServer) -> GatewayServeHandle {
        match bound.inner {
            BoundServerKind::Duplex(server) => self.serve_duplex_with_server(server),
            BoundServerKind::Simplex { rx, .. } => {
                use crate::mailbox::MailboxServer as _;
                let raw = self.clone().serve(rx);
                GatewayServeHandle::from_simplex(self.clone(), raw)
            }
        }
    }

    /// Connect this gateway to a peer gateway's [`serve_duplex`] endpoint
    /// using the attach handshake.
    ///
    /// Dials `addr`, sends this gateway's uid as [`AttachRequest`],
    /// receives [`AttachAck`] with the via location the peer assigned,
    /// sets that location as this gateway's [`default_location`] (so
    /// every address handed out by this gateway carries the via
    /// prefix), installs the duplex sender as this gateway's outbound
    /// forwarder, and serves inbound traffic from the duplex locally.
    ///
    /// The returned [`GatewayServeHandle`] of kind `ServeVia` owns the
    /// duplex session and the inbound serve handle. Dropping it (or
    /// awaiting `.join()`) restores the previous forwarder and tears
    /// down the duplex.
    pub async fn serve_via(&self, addr: ChannelAddr) -> anyhow::Result<GatewayServeHandle> {
        let my_uid = self.inner.uid.clone();
        let mut duplex_client = channel::duplex::dial::<MessageEnvelope, AttachWire>(addr)?;
        let duplex_tx = duplex_client.tx();
        let mut duplex_rx = duplex_client
            .take_rx()
            .expect("dial returns a fresh DuplexClient with rx present");

        duplex_tx.post(build_sigil_envelope(&AttachRequest { uid: my_uid })?);

        let ack = match tokio::time::timeout(ATTACH_HANDSHAKE_TIMEOUT, duplex_rx.recv())
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "attach handshake timed out after {:?} waiting for AttachAck",
                    ATTACH_HANDSHAKE_TIMEOUT
                )
            })?? {
            AttachWire::Ack(a) => a,
            AttachWire::Envelope(_) => anyhow::bail!("expected attach ack as first message"),
        };

        // Swap both fields under one lock so no concurrent reader sees the
        // new via-prefixed location paired with the old forwarder.
        let (previous_default_location, previous_forwarder) = {
            let mut routing = self.inner.routing.write().unwrap();
            let prev_location = std::mem::replace(&mut routing.default_location, ack.location);
            let prev_forwarder = std::mem::replace(
                &mut routing.forwarder,
                MailboxClient::new(duplex_tx).into_boxed(),
            );
            (prev_location, prev_forwarder)
        };
        let serve_handle = self.serve_rx_after_peeled_via(AttachRx(duplex_rx));
        Ok(GatewayServeHandle {
            gateway: self.clone(),
            handle: serve_handle,
            stopped: false,
            kind: HandleKind::ServeVia {
                duplex_client: Some(duplex_client),
                previous_forwarder: Some(previous_forwarder),
                previous_default_location: Some(previous_default_location),
            },
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
        let mut routing = self.inner.routing.write().unwrap();
        if routing.default_location.as_via().is_none() {
            routing.default_location = location;
        }
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
        let mut routing = self.inner.routing.write().unwrap();
        if routing.default_location.as_via().is_none() {
            routing.default_location = default_location;
        }
    }

    fn restore_after_via(
        &self,
        previous_forwarder: &mut Option<BoxedMailboxSender>,
        previous_default_location: &mut Option<Location>,
    ) {
        let default_location = previous_default_location.take().map(|previous| {
            self.inner
                .active_servers
                .read()
                .unwrap()
                .last()
                .cloned()
                .unwrap_or(previous)
        });
        let mut routing = self.inner.routing.write().unwrap();
        if let Some(prev) = previous_forwarder.take() {
            routing.forwarder = prev;
        }
        if let Some(default_location) = default_location {
            routing.default_location = default_location;
        }
    }

    fn with_dest_location(envelope: MessageEnvelope, location: Location) -> MessageEnvelope {
        let dest_id = envelope.dest().id().clone();
        envelope.with_dest(PortAddr::new(dest_id, location))
    }

    fn post_local_proc_or_else(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
        allow_peeled_via_delivery: bool,
        on_miss: impl FnOnce(MessageEnvelope, PortHandle<Undeliverable<MessageEnvelope>>),
    ) {
        let dest_proc = envelope.dest().actor_addr().proc_addr();
        let local = self
            .inner
            .procs
            .read()
            .unwrap()
            .get(dest_proc.id())
            .and_then(|route| match route {
                ProcRoute::Local(weak) => weak.upgrade(),
                ProcRoute::Remote { .. } => None,
            });
        let is_local = local.as_ref().is_some_and(|proc| {
            if allow_peeled_via_delivery {
                proc.is_local_delivery_target_after_peeled_via(&dest_proc)
            } else {
                proc.is_local_delivery_target(&dest_proc)
            }
        });
        if let Some(proc) = local
            && is_local
        {
            proc.muxer().post(envelope, return_handle);
        } else {
            on_miss(envelope, return_handle);
        }
    }

    fn remote_proc_sender_for_via(
        &self,
        proc_id: &ProcId,
        via_uid: &Uid,
    ) -> Option<BoxedMailboxSender> {
        self.inner
            .procs
            .read()
            .unwrap()
            .get(proc_id)
            .and_then(|route| match route {
                ProcRoute::Remote {
                    via_uid: route_uid,
                    sender,
                } if route_uid == via_uid => Some(sender.clone()),
                ProcRoute::Local(_) | ProcRoute::Remote { .. } => None,
            })
    }

    fn return_no_route(
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        let target = envelope.dest().clone();
        let failure = DeliveryFailure::new(UndeliverableReason::Transport(TransportFailure::new(
            target,
            TransportFailureReason::NoRoute,
        )));
        envelope.undeliverable(failure, return_handle);
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
        // Flush local procs and the forwarder. We intentionally do
        // *not* iterate `peers` or remote proc routes here:
        // in-process gateway attaches install each peer in the other's
        // peers, so a naive iteration recurses through the peer's
        // `flush` and overflows the stack.
        let local_procs: Vec<_> = self
            .inner
            .procs
            .read()
            .unwrap()
            .values()
            .filter_map(|route| match route {
                ProcRoute::Local(weak) => weak.upgrade(),
                ProcRoute::Remote { .. } => None,
            })
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
        let forwarder = self.inner.routing.read().unwrap().forwarder.clone();
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

/// A running gateway server. Returned by [`Gateway::serve`],
/// [`Gateway::serve_duplex`], and [`Gateway::serve_via`].
///
/// The same type covers all three flavors; the internal [`HandleKind`]
/// carries flavor-specific teardown state. Shutdown is two steps:
/// [`stop`](Self::stop) signals the server and runs the flavor-specific
/// cleanup, and [`join`](Self::join) awaits teardown. They are
/// independent — `join` does not stop, so a caller that wants both must
/// call `stop` first.
pub struct GatewayServeHandle {
    gateway: Gateway,
    handle: MailboxServerHandle,
    stopped: bool,
    kind: HandleKind,
}

/// Per-flavor teardown state for a [`GatewayServeHandle`].
enum HandleKind {
    /// A simple serve on a [`ChannelAddr`]. `location` is added to the
    /// gateway's active-server list and removed on stop. `None` for
    /// handles built from a pre-bound receiver via [`from_simplex`].
    Serve { location: Option<Location> },
    /// A duplex accept loop. Same active-server bookkeeping as
    /// [`Serve`]; `cancel_token` stops the loop and its per-connection
    /// tasks when [`stop`](GatewayServeHandle::stop) cancels it.
    ServeDuplex {
        location: Option<Location>,
        cancel_token: CancellationToken,
    },
    /// An outbound attach session. Owns the duplex client and the
    /// previous gateway state so stop/drop can restore them.
    ServeVia {
        duplex_client: Option<channel::duplex::DuplexClient<MessageEnvelope, AttachWire>>,
        previous_forwarder: Option<BoxedMailboxSender>,
        previous_default_location: Option<Location>,
    },
}

impl fmt::Debug for GatewayServeHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let variant = match &self.kind {
            HandleKind::Serve { .. } => "Serve",
            HandleKind::ServeDuplex { .. } => "ServeDuplex",
            HandleKind::ServeVia { .. } => "ServeVia",
        };
        f.debug_struct("GatewayServeHandle")
            .field("kind", &variant)
            .finish()
    }
}

impl GatewayServeHandle {
    /// [`GatewayServeHandle`] of kind `Serve` with no active-server
    /// bookkeeping. Used when the caller passes a pre-bound receiver
    /// to the gateway (e.g. a local-transport Host).
    fn from_simplex(gateway: Gateway, handle: MailboxServerHandle) -> Self {
        Self {
            gateway,
            handle,
            stopped: false,
            kind: HandleKind::Serve { location: None },
        }
    }

    /// Signal the underlying server to stop and run the flavor-specific
    /// cleanup: remove the active-server location (`Serve`/`ServeDuplex`)
    /// or restore the swapped forwarder and default location
    /// (`ServeVia`). Idempotent: later calls are no-ops. Call
    /// [`join`](Self::join) afterward to await teardown.
    pub fn stop(&mut self, reason: &str) {
        if self.stopped {
            return;
        }
        self.stopped = true;
        // `ServeDuplex` is driven by its cancellation token, not the
        // inner mailbox-server watch (whose receiver is unused), so
        // signal the token here and leave the inner handle alone.
        match &self.kind {
            HandleKind::ServeDuplex { cancel_token, .. } => {
                tracing::info!("stopping gateway duplex accept loop; reason: {reason}");
                cancel_token.cancel();
            }
            HandleKind::Serve { .. } | HandleKind::ServeVia { .. } => {
                self.handle.stop(reason);
            }
        }
        self.run_cleanup();
    }

    /// Await teardown of the underlying server (and any owned duplex
    /// session), returning its join result. This does not signal the
    /// server to stop; call [`stop`](Self::stop) first if it is still
    /// running, or `join` will block until it terminates on its own.
    pub async fn join(mut self) -> Result<(), MailboxServerError> {
        let inner_result = (&mut self.handle).await;

        // Drain any owned duplex session as part of join.
        if let HandleKind::ServeVia { duplex_client, .. } = &mut self.kind
            && let Some(client) = duplex_client.take()
        {
            client.join().await;
        }
        match inner_result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(err),
            Err(join_err) => Err(MailboxServerError::Channel(ChannelError::Other(
                anyhow::anyhow!("gateway serve task join error: {join_err}"),
            ))),
        }
    }

    fn run_cleanup(&mut self) {
        match &mut self.kind {
            HandleKind::Serve { location } | HandleKind::ServeDuplex { location, .. } => {
                if let Some(loc) = location.take() {
                    self.gateway.remove_server(&loc);
                }
            }
            HandleKind::ServeVia {
                previous_forwarder,
                previous_default_location,
                ..
            } => self
                .gateway
                .restore_after_via(previous_forwarder, previous_default_location),
        }
    }
}

impl Drop for GatewayServeHandle {
    fn drop(&mut self) {
        // Graceful teardown is `stop()` + `join().await`. As a safety
        // net, the `ServeVia` variant must restore the swapped
        // forwarder/default_location on drop: it installed a forwarder
        // backed by the duplex client this handle owns, so dropping
        // without restoring would leave the gateway holding a dead
        // sender. The `Serve`/`Accept` active-server bookkeeping is
        // intentionally left to an explicit `stop()`.
        if self.stopped {
            return;
        }
        if let HandleKind::ServeVia {
            previous_forwarder,
            previous_default_location,
            ..
        } = &mut self.kind
        {
            self.gateway
                .restore_after_via(previous_forwarder, previous_default_location);
        }
    }
}

/// Accept loop body shared by all duplex servers attached to a
/// gateway. Each accepted connection is dispatched based on its first
/// message:
///
/// * [`AttachWire`] sigil (`AttachRequest`) — register the peer in
///   `peers`, reply with [`AttachAck`] carrying the via
///   location, then serve remaining envelope traffic from the duplex.
/// * regular [`MessageEnvelope`] — serve straight through.
async fn duplex_accept_loop(
    mut duplex_server: channel::duplex::DuplexServer<MessageEnvelope, AttachWire>,
    bound_addr: ChannelAddr,
    gateway: Gateway,
    cancel_token: CancellationToken,
) {
    let mut tasks: JoinSet<()> = JoinSet::new();
    loop {
        let accept = tokio::select! {
            result = duplex_server.accept() => result,
            () = cancel_token.cancelled() => break,
        };
        let (mut duplex_rx, duplex_tx) = match accept {
            Ok(pair) => pair,
            Err(e) => {
                tracing::info!(
                    bound_addr = bound_addr.to_string(),
                    error = %e,
                    "duplex accept loop ended"
                );
                break;
            }
        };

        let first_msg = match duplex_rx.recv().await {
            Ok(msg) => msg,
            Err(e) => {
                tracing::info!(error = %e, "duplex connection closed before first message");
                continue;
            }
        };

        if let Ok(attach_request) = first_msg.deserialized::<AttachRequest>() {
            let peer_uid = attach_request.uid;
            tracing::info!(
                uid = %peer_uid,
                "duplex accepted gateway-attach connection",
            );
            // Register the peer *before* acknowledging, so the handshake
            // reflects the actual server-side state. If registration
            // fails (e.g. a duplicate uid), drop the connection without
            // sending an `AttachAck`: the client then observes the closed
            // channel instead of adopting a via location and forwarder
            // for a session the peer discarded.
            let sender = AttachSender(duplex_tx.clone()).into_boxed();
            let attach_guard = match gateway.attach_peer(peer_uid.clone(), sender) {
                Ok(guard) => guard,
                Err(err) => {
                    tracing::warn!(
                        uid = %err.uid,
                        "ignoring duplicate gateway-attach connection"
                    );
                    continue;
                }
            };

            // Reply with the via location the peer should advertise.
            let via_location = gateway.default_location().with_via(peer_uid);
            duplex_tx.post(AttachWire::Ack(AttachAck {
                location: via_location,
            }));

            let mut handle = gateway.serve_rx_after_peeled_via(duplex_rx);
            let conn_token = cancel_token.clone();
            tasks.spawn(async move {
                tokio::select! {
                    _ = &mut handle => {}
                    () = conn_token.cancelled() => {
                        handle.stop("gateway accept loop stopping");
                        let _ = handle.await;
                    }
                }
                drop(attach_guard);
                tracing::info!("gateway-attach connection closed");
            });
        } else {
            // Regular inbound connection: route messages, no
            // outbound tag-0x01 traffic. The DuplexTx is held for
            // the lifetime of the connection: dropping it closes
            // the session's outbound channel, which causes the
            // session task to exit and the inbound receiver to
            // close after a single message.
            let gw = gateway.clone();
            let conn_token = cancel_token.clone();
            tasks.spawn(async move {
                let _keep_alive = duplex_tx;
                let rx = PrependRx {
                    first: Some(first_msg),
                    inner: duplex_rx,
                };
                let mut handle = gw.serve_rx(rx);
                tokio::select! {
                    _ = &mut handle => {}
                    () = conn_token.cancelled() => {
                        handle.stop("gateway accept loop stopping");
                        let _ = handle.await;
                    }
                }
            });
        }
    }

    while tasks.join_next().await.is_some() {}
    duplex_server.join().await;
}

/// [`Rx<MessageEnvelope>`] adapter that yields a single pre-read
/// envelope before delegating to an inner receiver. Used by the
/// duplex accept loop to re-inject the first message it consumed for
/// connection-type dispatch.
struct PrependRx<R> {
    first: Option<MessageEnvelope>,
    inner: R,
}

#[async_trait]
impl<R: channel::Rx<MessageEnvelope> + Send> channel::Rx<MessageEnvelope> for PrependRx<R> {
    async fn recv(&mut self) -> Result<MessageEnvelope, ChannelError> {
        if let Some(msg) = self.first.take() {
            return Ok(msg);
        }
        self.inner.recv().await
    }

    fn addr(&self) -> ChannelAddr {
        self.inner.addr()
    }

    async fn join(self) {
        self.inner.join().await
    }
}

#[derive(Clone, Debug)]
struct WeakGateway {
    inner: Weak<GatewayState>,
    allow_peeled_via_delivery: bool,
}

impl WeakGateway {
    fn new(gateway: &Gateway, allow_peeled_via_delivery: bool) -> Self {
        Self {
            inner: Arc::downgrade(&gateway.inner),
            allow_peeled_via_delivery,
        }
    }

    fn upgrade(&self) -> Option<Gateway> {
        self.inner.upgrade().map(|inner| Gateway { inner })
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
            Some(gateway) => {
                gateway.post_unchecked_with_options(
                    envelope,
                    return_handle,
                    self.allow_peeled_via_delivery,
                );
            }
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
        self.post_unchecked_with_options(envelope, return_handle, false);
    }

    async fn flush(&self) -> Result<(), anyhow::Error> {
        Gateway::flush(self).await
    }
}

impl Gateway {
    fn post_unchecked_with_options(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
        allow_peeled_via_delivery: bool,
    ) {
        // A message that reaches a gateway resolves to exactly one of
        // three outcomes, decided by the destination's outermost
        // `Via(uid, ...)` hop (if any):
        //
        //   * a spawned child proc hop (`proc_id` has a matching
        //     remote proc route): peel the hop and forward to that
        //     proc's sender;
        //   * a *peer* hop (`uid` in `peers`): peel the hop and
        //     forward to that peer;
        //   * *our own* hop (`uid == self.uid`), or a via-less local
        //     destination: we are the leaf — consume any owned hop and
        //     deliver locally;
        //   * anything else: not ours. Hand it to the forwarder, which
        //     is either a route onward (a dial router, or an attached
        //     duplex) or a terminal `UnroutableMailboxSender` that
        //     returns it as undeliverable.
        //
        // A foreign via is never silently delivered locally just
        // because its inner proc id happens to match a local proc.
        let dest_location = envelope.dest().location().clone();
        if let Ok((via_uid, inner_location)) = dest_location.pop_via() {
            let dest_proc_id = envelope.dest().actor_addr().proc_addr().id().clone();
            if let Some(sender) = self.remote_proc_sender_for_via(&dest_proc_id, &via_uid) {
                let envelope = Gateway::with_dest_location(envelope, inner_location);
                sender.post(envelope, return_handle);
                return;
            }
            if let Some(sender) = self.inner.peers.read().unwrap().get(&via_uid).cloned() {
                let envelope = Gateway::with_dest_location(envelope, inner_location);
                sender.post(envelope, return_handle);
                return;
            }
            if via_uid != self.inner.uid {
                // A hop naming neither a peer nor this gateway: we are a
                // waypoint, not the destination. Forward toward the
                // default route, which itself returns the message as
                // undeliverable if it is terminal.
                let forwarder = self.inner.routing.read().unwrap().forwarder.clone();
                forwarder.post(envelope, return_handle);
                return;
            }
            let envelope = Gateway::with_dest_location(envelope, inner_location);
            self.post_local_proc_or_else(envelope, return_handle, true, Gateway::return_no_route);
            return;
        }

        // Via-less destination: deliver to the local proc if it is a
        // delivery target, otherwise hand to the forwarder (outbound
        // egress for plain remote addresses). A dead entry is left in
        // place — `AttachedProcGuard::drop` is the sole remover.
        self.post_local_proc_or_else(
            envelope,
            return_handle,
            allow_peeled_via_delivery,
            |envelope, return_handle| {
                let forwarder = self.inner.routing.read().unwrap().forwarder.clone();
                forwarder.post(envelope, return_handle)
            },
        );
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

    /// Test-only helper that connects two gateways over real
    /// `Local`-transport channels, so the via-routing tests exercise a
    /// genuine cross-gateway hop: each gateway serves on a local channel
    /// and the peer reaches it by dialing. After attach, each side's
    /// `default_location` advertises its destinations through the peer's
    /// uid (`Via(self_uid, peer_default)`), so procs bound afterward
    /// inherit the via prefix and route across the link.
    ///
    /// Production cross-process attach is [`Gateway::serve_via`] /
    /// [`Gateway::serve_duplex`]; this is just enough wiring to test the
    /// gateway's via routing locally.
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
            self.inner.routing.write().unwrap().default_location =
                Location::Via(self.inner.uid.clone(), Box::new(pre_peer_default.clone()));
            peer.inner.routing.write().unwrap().default_location =
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
                state.routing.write().unwrap().default_location = loc;
            }
            if let Some(state) = self.peer_gateway.upgrade()
                && let Some(loc) = self.prev_peer_default.take()
            {
                state.routing.write().unwrap().default_location = loc;
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

    /// A via hop naming this gateway is consumed before local
    /// delivery, so the proc muxer sees the peeled destination.
    #[tokio::test]
    async fn test_gateway_self_via_peels_before_local_delivery() {
        #[derive(Clone)]
        struct CapturingSender(Arc<std::sync::Mutex<Option<PortAddr>>>);

        #[async_trait]
        impl MailboxSender for CapturingSender {
            fn post_unchecked(
                &self,
                envelope: MessageEnvelope,
                _return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
            ) {
                *self.0.lock().unwrap() = Some(envelope.dest().clone());
            }
        }

        let gateway = Gateway::isolated();
        let proc = Proc::builder()
            .proc_id(ProcId::instance(Label::strip("alpha")))
            .shared_gateway(gateway.clone())
            .build()
            .unwrap();

        let captured = Arc::new(std::sync::Mutex::new(None));
        let actor_addr = proc.proc_addr().actor_addr("capture");
        assert!(
            proc.muxer()
                .bind(actor_addr.id().clone(), CapturingSender(captured.clone()))
        );

        let inner_dest = actor_addr.port_addr(Port::from(7u64));
        let via_dest = PortAddr::new(
            inner_dest.id().clone(),
            inner_dest
                .location()
                .clone()
                .with_via(gateway.uid().clone()),
        );
        gateway.post(
            MessageEnvelope::serialize(
                test_actor_id("test", "sender"),
                via_dest,
                &7u64,
                Flattrs::new(),
            )
            .unwrap(),
            monitored_return_handle(),
        );

        assert_eq!(*captured.lock().unwrap(), Some(inner_dest));
    }

    /// A via hop naming *this* gateway (the leaf) whose inner proc id is
    /// not a live local proc is undeliverable — a hop addressed to us is
    /// never forwarded back out. The returned envelope carries the
    /// peeled destination because this gateway consumed the hop.
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
        let peeled_dest = ProcAddr::new(
            ProcId::instance(Label::strip("stranger")),
            Location::from(ChannelAddr::Local(4321)),
        )
        .actor_addr("ghost")
        .port_addr(Port::from(0u64));
        let dest = PortAddr::new(
            peeled_dest.id().clone(),
            peeled_dest
                .location()
                .clone()
                .with_via(gateway.uid().clone()),
        );
        let envelope = MessageEnvelope::serialize(
            test_actor_id("test", "sender"),
            dest.clone(),
            &9u64,
            Flattrs::new(),
        )
        .unwrap();

        gateway.post(envelope, return_handle);

        let Undeliverable::Returned(returned) =
            time::timeout(Duration::from_secs(5), return_rx.recv())
                .await
                .expect("return_rx timed out")
                .expect("return_rx closed")
        else {
            panic!("expected returned envelope");
        };
        assert_eq!(returned.dest(), &peeled_dest);
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
        assert_eq!(gateway.inner.procs.read().unwrap().len(), 2);

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
                .procs
                .read()
                .unwrap()
                .values()
                .filter_map(|route| match route {
                    ProcRoute::Local(weak) => weak.upgrade(),
                    ProcRoute::Remote { .. } => None,
                })
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
        let weak = WeakGateway::new(&gateway, false);
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
    /// eagerly removes the entry from the gateway's proc map. A
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
        assert_eq!(gateway.inner.procs.read().unwrap().len(), 0);

        // The slot is free, so registering a new proc with the same id
        // is a fresh insert — no panic.
        let second = Proc::builder()
            .proc_id(proc_id.clone())
            .shared_gateway(gateway.clone())
            .build()
            .unwrap();
        assert_eq!(gateway.inner.procs.read().unwrap().len(), 1);

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
        // last entry in active_servers). `stop` runs the cleanup;
        // `join` awaits teardown.
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

    /// End-to-end gateway-to-gateway attach via the new protocol:
    /// the client gateway calls `serve_via` against a peer that
    /// called `serve_duplex`; the handshake assigns the client a via
    /// location and installs the duplex sender; outbound traffic from
    /// the client's gateway falls through to the duplex; inbound
    /// envelopes addressed to procs on the server gateway are peeled
    /// at the via boundary and routed locally.
    #[tokio::test]
    async fn test_gateway_serve_via_peer() {
        // The server gateway accepts duplex attaches on a unix
        // address. Spawn a proc on it so the client has a destination
        // to reach.
        let server_gw = Gateway::new();
        let server_addr = ChannelAddr::any(ChannelTransport::Unix);
        let mut accept_handle = server_gw.serve_duplex(server_addr).unwrap();
        let server_addr = server_gw.default_location().addr().clone();

        let server_proc = Proc::builder()
            .proc_id(ProcId::instance(Label::strip("echo")))
            .shared_gateway(server_gw.clone())
            .build()
            .unwrap();
        let server_inst = server_proc.client("recv");
        let (server_port, mut server_rx) = server_inst.bind_handler_port::<u64>();
        let PortLocation::Bound(server_dest) = server_port.location() else {
            panic!("server port must be bound");
        };

        // The client gateway dials the server's accept endpoint.
        // After handshake, the client's default_location is wrapped in
        // Via(client_uid, server_addr).
        let client_gw = Gateway::new();
        let pre_default = client_gw.default_location();
        let serve_via = client_gw.serve_via(server_addr.clone()).await.unwrap();
        let post_default = client_gw.default_location();
        assert_ne!(
            pre_default, post_default,
            "serve_via must update default_location"
        );
        let (via_uid, inner) = post_default.as_via().expect("default must be via");
        assert_eq!(via_uid, client_gw.uid());
        assert_eq!(inner.addr(), &server_addr);

        // Post directly to the server proc through the client gateway —
        // since the server proc is on the server gateway, the
        // envelope flows out via the duplex and the server peels at
        // the post_unchecked via-first path.
        let sender = test_actor_id("client", "sender");
        client_gw.post(
            MessageEnvelope::serialize(sender, server_dest.clone(), &7u64, Flattrs::new()).unwrap(),
            monitored_return_handle(),
        );
        let received = time::timeout(Duration::from_secs(5), server_rx.recv())
            .await
            .expect("server_rx timed out")
            .expect("server_rx closed");
        assert_eq!(received, 7);

        // Drop the via handle: client's default_location is restored.
        drop(serve_via);
        assert_eq!(client_gw.default_location(), pre_default);

        // Clean up the accept loop.
        accept_handle.stop("test cleanup");
    }

    #[tokio::test]
    async fn test_gateway_serve_bound_preserves_via_default_location() {
        let server_gw = Gateway::new();
        let mut accept_handle = server_gw
            .serve_duplex(ChannelAddr::any(ChannelTransport::Unix))
            .unwrap();
        let server_addr = server_gw.default_location().addr().clone();
        let _server_proc = Proc::legacy_service_pseudo_singleton_on_gateway(server_gw.clone());

        let client_gw = Gateway::new();
        let pre_default = client_gw.default_location();
        let mut serve_via = client_gw.serve_via(server_addr).await.unwrap();
        let via_default = client_gw.default_location();
        assert!(via_default.as_via().is_some());

        let bound = client_gw
            .bind(ChannelAddr::any(ChannelTransport::Unix), None)
            .unwrap();
        assert_eq!(client_gw.default_location(), via_default);

        let mut frontend = client_gw.serve_bound(bound);
        assert_eq!(client_gw.default_location(), via_default);

        frontend.stop("test cleanup");
        frontend.join().await.unwrap();
        assert_eq!(client_gw.default_location(), via_default);

        serve_via.stop("test cleanup");
        serve_via.join().await.unwrap();
        assert_eq!(client_gw.default_location(), pre_default);

        accept_handle.stop("test cleanup");
        accept_handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_gateway_serve_via_delivers_peeled_legacy_local_proc_destination() {
        let server_gw = Gateway::new();
        let mut accept_handle = server_gw
            .serve_duplex(ChannelAddr::any(ChannelTransport::Unix))
            .unwrap();
        let server_addr = server_gw.default_location().addr().clone();

        let client_gw = Gateway::new();
        let mut serve_via = client_gw.serve_via(server_addr).await.unwrap();
        let client_proc = Proc::legacy_local_pseudo_singleton_on_gateway(client_gw.clone());
        let client = client_proc.client("recv");
        let (port, mut rx) = client.bind_handler_port::<u64>();
        let PortLocation::Bound(via_dest) = port.location() else {
            panic!("client port must be bound");
        };

        server_gw.post(
            MessageEnvelope::serialize(
                test_actor_id("server", "sender"),
                via_dest,
                &7u64,
                Flattrs::new(),
            )
            .unwrap(),
            monitored_return_handle(),
        );

        let received = time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("rx timed out")
            .expect("rx closed");
        assert_eq!(received, 7);

        serve_via.stop("test cleanup");
        serve_via.join().await.unwrap();
        accept_handle.stop("test cleanup");
        accept_handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_gateway_serve_via_does_not_shadow_peer_legacy_proc() {
        let server_gw = Gateway::new();
        let mut accept_handle = server_gw
            .serve_duplex(ChannelAddr::any(ChannelTransport::Unix))
            .unwrap();
        let server_addr = server_gw.default_location().addr().clone();

        let client_gw = Gateway::new();
        let mut serve_via = client_gw.serve_via(server_addr).await.unwrap();
        let client_proc = Proc::legacy_service_pseudo_singleton_on_gateway(client_gw.clone());
        let client = client_proc.client("recv");
        let (port, mut rx) = client.bind_handler_port::<u64>();
        let PortLocation::Bound(via_dest) = port.location() else {
            panic!("client port must be bound");
        };
        let (_, peeled_location) = via_dest
            .location()
            .as_via()
            .expect("client port must advertise via location");
        let peer_dest = PortAddr::new(via_dest.id().clone(), peeled_location.as_ref().clone());

        client_gw.post(
            MessageEnvelope::serialize(
                test_actor_id("client", "sender"),
                peer_dest,
                &7u64,
                Flattrs::new(),
            )
            .unwrap(),
            monitored_return_handle(),
        );

        time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .expect_err("vialess peer location must not deliver to the attached client proc");

        serve_via.stop("test cleanup");
        serve_via.join().await.unwrap();
        accept_handle.stop("test cleanup");
        accept_handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_gateway_serve_via_relay_delivers_to_client_legacy_local_proc() {
        let relay_gw = Gateway::new();
        let mut accept_handle = relay_gw
            .serve_duplex(ChannelAddr::any(ChannelTransport::Unix))
            .unwrap();
        let relay_addr = relay_gw.default_location().addr().clone();

        let client_gw = Gateway::new();
        let mut serve_via = client_gw.serve_via(relay_addr.clone()).await.unwrap();
        let client_proc = Proc::legacy_local_pseudo_singleton_on_gateway(client_gw.clone());
        let client = client_proc.client("recv");
        let (port, mut rx) = client.bind_handler_port::<u64>();
        let PortLocation::Bound(via_dest) = port.location() else {
            panic!("client port must be bound");
        };

        // A third gateway with no direct peer relationship to the
        // client can still reach client-local refs by dialing the
        // relay's raw address while preserving the via envelope. The
        // relay peels `Via(client_uid, relay_addr)` and forwards over
        // the attached duplex; the client then receives the peeled
        // legacy `local@relay_addr` destination.
        let relay_dialer = DialMailboxRouter::new();
        relay_dialer.post(
            MessageEnvelope::serialize(
                test_actor_id("third_party", "sender"),
                via_dest,
                &7u64,
                Flattrs::new(),
            )
            .unwrap(),
            monitored_return_handle(),
        );

        let received = time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("rx timed out")
            .expect("rx closed");
        assert_eq!(received, 7);

        serve_via.stop("test cleanup");
        serve_via.join().await.unwrap();
        accept_handle.stop("test cleanup");
        accept_handle.join().await.unwrap();
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

        // gw_a → alpha: gw_a consumes its own outermost Via and
        // delivers the peeled destination locally.
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
