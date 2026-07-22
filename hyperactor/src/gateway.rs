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
//! default location, receive inbound traffic through the gateway, and forward
//! outbound traffic directly through the gateway.
//!
//! Gateways route [`Location`]s: if the inbound message is source routed,
//! the gateway looks up the next hop via its peer table and forwards
//! accordingly; otherwise the message is delivered if the proc is attached
//! to the gateway.
//!
//! Messages with no local proc or peer route are forwarded through the
//! default forwarder.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::sync::Weak;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering as AtomicOrdering;
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
// with [`AttachAck`], either accepting with the via location through
// which the client is reachable or rejecting with a handshake error.
// After an accepted handshake, both ends serve regular
// [`MessageEnvelope`] traffic on the same duplex.

/// Label used for attach-control envelopes.
const ATTACH_CONTROL_LABEL: &str = "attach";

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

/// Acknowledgement returned by a peer gateway during attach.
#[derive(Debug, Clone, Serialize, Deserialize, typeuri::Named)]
pub(crate) enum AttachAck {
    /// Attach succeeded. Carries the via location the client should advertise
    /// as its [`default_location`] — `Via(client_uid, peer_default_location)`.
    Accepted {
        /// The location through which the client is now reachable.
        location: Location,
    },
    /// Attach failed before the peer registered this connection.
    Rejected {
        /// Human-readable rejection reason.
        reason: String,
    },
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
    /// First message: the peer accepts or rejects the attach.
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

/// [`Tx<MessageEnvelope>`](channel::Tx) adapter that wraps outbound
/// [`MessageEnvelope`]s in [`AttachWire::Envelope`] before posting to
/// a peer's [`DuplexTx<AttachWire>`]. Used through [`MailboxClient`]
/// on the accept side so normal sender flushing semantics apply.
#[derive(Clone)]
pub(crate) struct AttachTx(pub(crate) channel::duplex::DuplexTx<AttachWire>);

#[async_trait]
impl channel::Tx<MessageEnvelope> for AttachTx {
    fn do_post(
        &self,
        envelope: MessageEnvelope,
        completion: channel::CompletionSink<MessageEnvelope>,
    ) {
        let completion = completion.contramap_rejected(
            |channel::SendError {
                 error,
                 message,
                 reason,
             }| {
                let AttachWire::Envelope(envelope) = message else {
                    return None;
                };
                Some(channel::SendError {
                    error,
                    message: envelope,
                    reason,
                })
            },
        );
        self.0.do_post(AttachWire::Envelope(envelope), completion);
    }

    fn addr(&self) -> ChannelAddr {
        self.0.addr()
    }

    fn status(&self) -> &watch::Receiver<channel::TxStatus> {
        self.0.status()
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

/// Serialize a control payload into a placeholder [`MessageEnvelope`]
/// suitable for posting on a duplex client→peer channel.
///
/// Sender/dest ids are placeholders the peer consumes without routing;
/// `return_undeliverable` is cleared so an envelope that ever escapes
/// into the forwarder is dropped rather than bounced to the fake
/// sender.
fn build_control_envelope<T>(payload: &T) -> anyhow::Result<MessageEnvelope>
where
    T: serde::Serialize + typeuri::Named,
{
    let signal_actor_id = crate::ActorAddr::root(
        ProcAddr::singleton(
            ChannelAddr::any(channel::ChannelTransport::Local),
            ATTACH_CONTROL_LABEL,
        ),
        crate::id::Label::strip(ATTACH_CONTROL_LABEL),
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

// This explicit impl is only a compiler performance optimization. It is safe
// to delete this module and cfg and let Rust derive the auto traits; see proc.rs.
#[cfg(not(hyperactor_verify_auto_traits))]
mod _send_sync_shortcut {
    use super::*;

    // SAFETY: the verification build structurally checks this bound.
    unsafe impl Send for GatewayState {}
    // SAFETY: the verification build structurally checks this bound.
    unsafe impl Sync for GatewayState {}
}

const _: fn() = || {
    fn assert<T: Send + Sync + ?Sized>() {}
    assert::<GatewayState>();
};

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
            .is_some_and(|weak| weak.ptr_eq(&self.weak))
        {
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

/// Original routing inputs for a gateway. Mutate through [`Routing::mutate`]
/// so [`Routing`] can recompute its derived fields afterward.
struct RoutingState {
    /// The location to use when no server is active.
    fallback_location: Location,

    /// The forwarder configured when the gateway was created.
    base_forwarder: BoxedMailboxSender,

    /// Active normal serves and `serve_via` sessions in start order.
    active_serves: Vec<ActiveServe>,
}

/// Routing state for a gateway. Holds original inputs plus derived fields
/// (`default_location`, `local_delivery_locations`, and `forwarder`) behind one
/// lock so advertised reachability and outbound routing stay synchronized.
struct Routing {
    state: RoutingState,

    /// The advertised location for newly bound refs. The newest active
    /// normal serve or [`Gateway::serve_via`] session takes precedence.
    default_location: Location,

    /// Cached locations that may receive local delivery for attached procs.
    local_delivery_locations: Arc<[Location]>,

    /// Sender used to forward messages whose destination is neither
    /// an attached proc nor matched by [`peers`]. The newest active
    /// [`Gateway::serve_via`] session supplies this; otherwise this is
    /// `base_forwarder`.
    forwarder: BoxedMailboxSender,
}

impl Routing {
    fn new(fallback_location: Location, base_forwarder: BoxedMailboxSender) -> Self {
        Self {
            state: RoutingState {
                fallback_location: fallback_location.clone(),
                base_forwarder: base_forwarder.clone(),
                active_serves: Vec::new(),
            },
            default_location: fallback_location.clone(),
            local_delivery_locations: vec![fallback_location].into(),
            forwarder: base_forwarder,
        }
    }

    fn mutate(&mut self, update: impl FnOnce(&mut RoutingState)) {
        update(&mut self.state);
        self.recompute();
    }

    fn recompute(&mut self) {
        self.default_location = self
            .state
            .active_serves
            .last()
            .map(|serve| serve.location.clone())
            .unwrap_or_else(|| self.state.fallback_location.clone());
        self.local_delivery_locations = if self.state.active_serves.is_empty() {
            vec![self.default_location.clone()].into()
        } else {
            self.state
                .active_serves
                .iter()
                .map(|serve| serve.location.clone())
                .collect::<Vec<_>>()
                .into()
        };
        self.forwarder = self
            .state
            .active_serves
            .iter()
            .rev()
            .find_map(|serve| match &serve.kind {
                ActiveServeKind::Server => None,
                ActiveServeKind::Via { forwarder } => Some(forwarder.clone()),
            })
            .unwrap_or_else(|| self.state.base_forwarder.clone());
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ServeId(u64);

struct ActiveServe {
    id: ServeId,
    location: Location,
    kind: ActiveServeKind,
}

enum ActiveServeKind {
    Server,
    Via { forwarder: BoxedMailboxSender },
}

struct GatewayState {
    /// A random, stable identifier for this gateway. It is just a
    /// routing key in peers' tables: peers route messages
    /// back through this gateway by referencing its uid in a
    /// [`Location::Via`] hop. We mint a uid (rather than reuse an
    /// existing key) mainly so the entry can carry a meaningful label.
    uid: Uid,

    /// Outbound routing state, held under one lock so the advertised
    /// location and the forwarder transition together. Starting or
    /// stopping a serve updates both in one critical section, so
    /// concurrent readers never observe a half-updated pair.
    routing: RwLock<Routing>,

    /// Local procs registered with this gateway, keyed by proc id.
    /// Values hold weak proc references for in-process delivery.
    procs: RwLock<HashMap<ProcId, WeakProc>>,

    /// Monotonic id source for active serve handles.
    next_serve_id: AtomicU64,

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
                routing: RwLock::new(Routing::new(default_location, forwarder.clone())),
                procs: RwLock::new(HashMap::new()),
                next_serve_id: AtomicU64::new(1),
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

    /// Set the gateway's fallback advertised location.
    ///
    /// If any serves are active, the newest active normal serve or
    /// [`Gateway::serve_via`] session remains the advertised default. This
    /// location is used once all active serves stop.
    pub fn set_default_location(&self, location: Location) {
        let mut routing = self.inner.routing.write().unwrap();
        routing.mutate(|routing| routing.fallback_location = location);
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
    /// gateway). Hosts register spawned child proc gateways with
    /// [`Gateway::attach_peer`].
    ///
    /// Panics if a live proc with the same id is already attached. A
    /// dead entry whose [`WeakProc`] has been dropped is replaced
    /// silently.
    pub(crate) fn attach_proc(&self, proc: &Proc) -> AttachedProcGuard {
        let proc_id = proc.proc_id().clone();
        let weak = proc.downgrade();
        let mut procs = self.inner.procs.write().unwrap();
        let duplicate_live_proc = procs.get(&proc_id).and_then(WeakProc::upgrade).is_some();
        if duplicate_live_proc {
            drop(procs);
            panic!("gateway already has a proc attached with id {}", proc_id)
        }
        procs.insert(proc_id.clone(), weak.clone());
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
    /// Used by [`Gateway::attach`] and the duplex accept loop in
    /// [`Gateway::serve_duplex`]. Hosts also use this for spawned
    /// child proc gateways.
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
        Arc::downgrade(&self.inner).serve(rx)
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
    /// previous active normal serve or `serve_via` session, or to the reserved
    /// fallback location when no serve remains.
    pub fn serve(&self, addr: ChannelAddr) -> Result<GatewayServeHandle, ChannelError> {
        let (serve_id, handle) = self.serve_inner(addr)?;
        Ok(GatewayServeHandle {
            gateway: self.clone(),
            handle,
            stopped: false,
            kind: HandleKind::Serve {
                serve_id: Some(serve_id),
            },
        })
    }

    /// Open a duplex endpoint that accepts both regular inbound
    /// envelope traffic and gateway-attach handshakes from peers.
    ///
    /// On each connection the first message determines the branch:
    /// * an [`AttachRequest`] control envelope enters the attach
    ///   branch — this gateway records the peer's uid in `peers` (so
    ///   source routes addressed to `Via(peer_uid, ...)` flow back
    ///   through the duplex), replies with [`AttachAck::Accepted`]
    ///   carrying `Via(peer_uid, default_location)`, and serves
    ///   remaining traffic from the duplex into this gateway. If the
    ///   peer cannot be registered, it replies with
    ///   [`AttachAck::Rejected`] and closes the connection.
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
        self.serve_duplex_with_listener(addr, None)
    }

    fn serve_duplex_with_listener(
        &self,
        addr: ChannelAddr,
        listener: Option<std::net::TcpListener>,
    ) -> Result<GatewayServeHandle, ChannelError> {
        if !addr.transport().supports_duplex() {
            return Err(ChannelError::Other(anyhow::anyhow!(
                "serve_duplex requires a duplex-capable transport, but {addr} does not support duplex"
            )));
        }
        // A gateway duplex endpoint doubles as a relay: peers attach over
        // duplex (see `serve_via`), while a third party with no peer
        // relationship reaches via-addressed refs by dialing the raw
        // address with a plain *simplex* channel. Now that the link layer
        // distinguishes the two protocols on the wire, a strict duplex
        // server would reject those simplex dials. Serve net endpoints
        // through the mux instead, so one address accepts both protocols:
        // simplex posts are dispatched into the gateway, duplex attaches
        // run the shared `AttachWire` accept loop. The in-process `Local`
        // transport is not a kernel socket and cannot be muxed, so it
        // keeps the plain duplex accept path.
        if addr.transport().is_net() {
            return self.serve_mux_with_listener(addr, listener);
        }
        let server = PreboundAcceptServer::duplex(addr, listener)
            .map_err(|e| ChannelError::Other(anyhow::anyhow!("{e}")))?;
        Ok(self.serve_duplex_with_server(server))
    }

    fn serve_duplex_with_server(&self, server: PreboundAcceptServer) -> GatewayServeHandle {
        let bound_addr = server.addr().clone();
        let location = Location::from(bound_addr.clone());
        // The accept loop and its per-connection tasks are driven by a
        // single cancellation token. `stop()` cancels it; dropping the
        // handle without stopping leaves the token uncancelled, so the
        // loop keeps running — matching the [`MailboxServerHandle`]
        // detach-on-drop convention.
        let cancel_token = CancellationToken::new();
        let loop_token = cancel_token.clone();
        let gateway = self.clone();
        let inner = server.inner;
        let serve_id = self.add_server(location);
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
                serve_id: Some(serve_id),
                cancel_token,
            },
        }
    }

    /// Serve this gateway on `addr` (optionally with a pre-bound TCP
    /// listener) using a *muxed* listener: simplex clients (dialed via
    /// [`channel::dial`]) and duplex attach clients (dialed via
    /// [`channel::duplex::dial`]) share one address, demultiplexed at
    /// the link layer by [`channel::serve_mux`].
    ///
    /// Simplex traffic is served straight into this gateway; duplex
    /// connections run the *same* [`AttachWire`] accept path as
    /// [`serve_duplex`](Self::serve_duplex) — i.e. there is a single
    /// attach protocol regardless of whether a frontend is muxed or a
    /// plain duplex endpoint. Requires a net transport (`serve_mux`
    /// rejects non-net addresses).
    ///
    /// Like [`serve_with_listener`](Self::serve_with_listener), this
    /// registers the bound address as an active serve location (so the
    /// gateway delivers frontend-addressed traffic in-process and adopts
    /// it as the default location).
    pub fn serve_mux_with_listener(
        &self,
        addr: ChannelAddr,
        listener: Option<std::net::TcpListener>,
    ) -> Result<GatewayServeHandle, ChannelError> {
        let mux =
            channel::serve_mux::<MessageEnvelope, MessageEnvelope, AttachWire>(addr, listener)?;
        let bound_addr = mux.addr().clone();
        let simplex_gateway = self.clone();
        let duplex_gateway = self.clone();
        let duplex_addr = bound_addr.clone();
        let raw = mux.serve(
            move |rx| simplex_gateway.serve(rx),
            move |duplex_server, mut stop_rx| async move {
                // The mux signals shutdown through a watch channel, but
                // `duplex_accept_loop` drains on a `CancellationToken`.
                // Bridge the two: cancel only on an explicit stop, and
                // pend (leaving the token uncancelled) if the watch
                // sender is dropped without stopping, matching the
                // detach-on-drop convention of the serve handles.
                let cancel_token = CancellationToken::new();
                let loop_token = cancel_token.clone();
                tokio::spawn(async move {
                    if stop_rx.wait_for(|stopped| *stopped).await.is_ok() {
                        cancel_token.cancel();
                    }
                });
                duplex_accept_loop(duplex_server, duplex_addr, duplex_gateway, loop_token).await;
            },
        );
        Ok(GatewayServeHandle::from_mailbox_handle(
            self.clone(),
            bound_addr,
            raw,
        ))
    }

    /// Serve this gateway on `addr`, optionally using an already-bound
    /// listener.
    ///
    /// This chooses a duplex accept loop for duplex-capable transports and a
    /// simplex mailbox receiver otherwise. Serving updates the gateway's
    /// advertised default location to the concrete bound address. Callers that
    /// need to construct procs using that address can call
    /// [`Gateway::default_location`] after this returns.
    pub fn serve_with_listener(
        &self,
        addr: ChannelAddr,
        listener: Option<std::net::TcpListener>,
    ) -> Result<GatewayServeHandle, ChannelError> {
        if addr.transport().supports_duplex() {
            self.serve_duplex_with_listener(addr, listener)
        } else {
            let addr = self.resolve_serve_addr(addr);
            let (addr, rx) = channel::serve_with_listener(addr, listener)?;
            let serve_id = self.add_server(Location::from(addr));
            let raw = self.clone().serve(rx);
            Ok(GatewayServeHandle::from_simplex(
                self.clone(),
                raw,
                Some(serve_id),
            ))
        }
    }

    /// Connect this gateway to a peer gateway's [`serve_duplex`] endpoint
    /// using the attach handshake.
    ///
    /// Dials `addr`, sends this gateway's uid as [`AttachRequest`],
    /// receives [`AttachAck::Accepted`] with the via location the peer
    /// assigned, sets that location as this gateway's
    /// [`default_location`] (so every address handed out by this
    /// gateway carries the via prefix), installs the duplex sender as
    /// this gateway's outbound forwarder, and serves inbound traffic
    /// from the duplex locally. If the peer returns
    /// [`AttachAck::Rejected`], the rejection reason is returned as an
    /// error.
    ///
    /// Multiple `serve_via` sessions may be active. The newest active
    /// normal serve or `serve_via` session is this gateway's advertised
    /// default location, while older active locations remain valid. The
    /// newest active `serve_via` supplies the outbound forwarder.
    pub async fn serve_via(&self, addr: ChannelAddr) -> anyhow::Result<GatewayServeHandle> {
        let my_uid = self.inner.uid.clone();
        let mut duplex_client = channel::duplex::dial::<MessageEnvelope, AttachWire>(addr)?;
        let duplex_tx = duplex_client.tx();
        let mut duplex_rx = duplex_client
            .take_rx()
            .expect("dial returns a fresh DuplexClient with rx present");

        duplex_tx.post(build_control_envelope(&AttachRequest { uid: my_uid })?);

        let location = match tokio::time::timeout(ATTACH_HANDSHAKE_TIMEOUT, duplex_rx.recv())
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "attach handshake timed out after {:?} waiting for AttachAck",
                    ATTACH_HANDSHAKE_TIMEOUT
                )
            })?? {
            AttachWire::Ack(AttachAck::Accepted { location }) => location,
            AttachWire::Ack(AttachAck::Rejected { reason }) => {
                anyhow::bail!("attach rejected by peer: {reason}")
            }
            AttachWire::Envelope(_) => anyhow::bail!("expected attach ack as first message"),
        };

        let serve_id = self.add_via_server(location, MailboxClient::new(duplex_tx).into_boxed());
        let serve_handle = self.serve_rx(AttachRx(duplex_rx));
        Ok(GatewayServeHandle {
            gateway: self.clone(),
            handle: serve_handle,
            stopped: false,
            kind: HandleKind::ServeVia {
                serve_id: Some(serve_id),
                duplex_client: Some(duplex_client),
            },
        })
    }

    fn serve_inner(
        &self,
        addr: ChannelAddr,
    ) -> Result<(ServeId, MailboxServerHandle), ChannelError> {
        let addr = self.resolve_serve_addr(addr);
        let (addr, rx) = channel::serve(addr)?;
        let serve_id = self.add_server(Location::from(addr));
        Ok((serve_id, self.serve_rx(rx)))
    }

    fn resolve_serve_addr(&self, addr: ChannelAddr) -> ChannelAddr {
        if addr != ChannelAddr::any(ChannelTransport::Local) {
            return addr;
        }

        // The first local-any serve for the reserved fallback address activates
        // that address. Later local-any serves allocate fresh ports, so
        // multiple local servers can coexist for the same gateway.
        let routing = self.inner.routing.read().unwrap();
        let fallback_addr = routing.state.fallback_location.addr();
        let fallback_is_active = routing.state.active_serves.iter().any(|serve| {
            matches!(&serve.kind, ActiveServeKind::Server) && serve.location.addr() == fallback_addr
        });
        if matches!(fallback_addr, ChannelAddr::Local(_)) && !fallback_is_active {
            return fallback_addr.clone();
        }
        addr
    }

    fn add_server(&self, location: Location) -> ServeId {
        self.add_active_serve(location, ActiveServeKind::Server)
    }

    fn add_via_server(&self, location: Location, forwarder: BoxedMailboxSender) -> ServeId {
        self.add_active_serve(location, ActiveServeKind::Via { forwarder })
    }

    fn add_active_serve(&self, location: Location, kind: ActiveServeKind) -> ServeId {
        let id = ServeId(
            self.inner
                .next_serve_id
                .fetch_add(1, AtomicOrdering::Relaxed),
        );
        let mut routing = self.inner.routing.write().unwrap();
        routing.mutate(|routing| {
            routing
                .active_serves
                .push(ActiveServe { id, location, kind });
        });
        id
    }

    fn remove_active_serve(&self, serve_id: ServeId) {
        let mut routing = self.inner.routing.write().unwrap();
        routing.mutate(|routing| {
            if let Some(index) = routing
                .active_serves
                .iter()
                .rposition(|active| active.id == serve_id)
            {
                routing.active_serves.remove(index);
            }
        });
    }

    fn local_delivery_locations(&self) -> Arc<[Location]> {
        Arc::clone(&self.inner.routing.read().unwrap().local_delivery_locations)
    }

    fn envelope_with_next_hop_location(
        envelope: MessageEnvelope,
        location: Location,
    ) -> MessageEnvelope {
        let dest_id = envelope.next_hop().id().clone();
        envelope.with_next_hop(PortAddr::new(dest_id, location))
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
        // *not* iterate `peers` here:
        // in-process gateway attaches install each peer in the other's
        // peers, so a naive iteration recurses through the peer's
        // `flush` and overflows the stack.
        let local_procs: Vec<_> = self
            .inner
            .procs
            .read()
            .unwrap()
            .values()
            .filter_map(WeakProc::upgrade)
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
/// [`Gateway::serve_duplex`], [`Gateway::serve_with_listener`], and
/// [`Gateway::serve_via`].
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
    /// A simple serve on a [`ChannelAddr`]. `serve_id` is removed from
    /// the gateway's active-serve list on stop. `None` for callers that
    /// have already handled active-serve bookkeeping.
    Serve { serve_id: Option<ServeId> },
    /// A duplex accept loop. Same active-serve bookkeeping as
    /// [`Serve`]; `cancel_token` stops the loop and its per-connection
    /// tasks when [`stop`](GatewayServeHandle::stop) cancels it.
    ServeDuplex {
        serve_id: Option<ServeId>,
        cancel_token: CancellationToken,
    },
    /// An outbound attach session. Owns the duplex client and removes its
    /// active-serve entry on cleanup.
    ServeVia {
        serve_id: Option<ServeId>,
        duplex_client: Option<channel::duplex::DuplexClient<MessageEnvelope, AttachWire>>,
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
    /// [`GatewayServeHandle`] of kind `Serve` for an already-open receiver.
    fn from_simplex(
        gateway: Gateway,
        handle: MailboxServerHandle,
        serve_id: Option<ServeId>,
    ) -> Self {
        Self {
            gateway,
            handle,
            stopped: false,
            kind: HandleKind::Serve { serve_id },
        }
    }

    /// Wrap an externally-produced [`MailboxServerHandle`] — e.g. a
    /// host's muxed frontend accept loop driven outside the gateway —
    /// as a gateway serve handle. Registers `addr` as an active serve
    /// location (via [`add_server`](Gateway::add_server)) so the gateway
    /// delivers traffic addressed to the frontend in-process instead of
    /// dialing it, and advertises it as the default location — mirroring
    /// [`Gateway::serve_with_listener`]. The active-serve entry is removed
    /// when the handle is stopped.
    pub fn from_mailbox_handle(
        gateway: Gateway,
        addr: ChannelAddr,
        handle: MailboxServerHandle,
    ) -> Self {
        let serve_id = gateway.add_server(Location::from(addr));
        Self::from_simplex(gateway, handle, Some(serve_id))
    }

    /// Signal the underlying server to stop and run the flavor-specific
    /// cleanup: remove the active-serve entry for `Serve`, `ServeDuplex`,
    /// or `ServeVia`. Idempotent: later calls are no-ops. Call
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
        self.run_cleanup();
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
            HandleKind::Serve { serve_id }
            | HandleKind::ServeDuplex { serve_id, .. }
            | HandleKind::ServeVia { serve_id, .. } => {
                if let Some(serve_id) = serve_id.take() {
                    self.gateway.remove_active_serve(serve_id);
                }
            }
        }
    }
}

impl Drop for GatewayServeHandle {
    fn drop(&mut self) {
        // Graceful teardown is `stop()` + `join().await`. As a safety
        // net, the `ServeVia` variant must remove its active-serve
        // entry on drop: it installed a forwarder backed by the duplex
        // client this handle owns, so dropping without cleanup would
        // leave the gateway holding a dead sender. The `Serve` and
        // `ServeDuplex` active-serve bookkeeping is intentionally left
        // to an explicit `stop()`.
        if self.stopped {
            return;
        }
        if matches!(&self.kind, HandleKind::ServeVia { .. }) {
            self.run_cleanup();
        }
    }
}

/// Accept loop body shared by all duplex servers attached to a
/// gateway. Each accepted connection is dispatched based on its first
/// message:
///
/// * [`AttachRequest`] control envelope — register the peer in
///   `peers`, reply with [`AttachAck::Accepted`] carrying the via
///   location, then serve remaining envelope traffic from the duplex.
///   If registration fails, reply with [`AttachAck::Rejected`] and
///   close the connection.
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
        let (duplex_rx, duplex_tx) = match accept {
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

        tasks.spawn(serve_duplex_connection(
            gateway.clone(),
            duplex_rx,
            duplex_tx,
            cancel_token.clone(),
        ));
    }

    while tasks.join_next().await.is_some() {}
    // Tear down the server now that the accept loop has exited. The loop
    // broke on its own `cancel_token`, which is distinct from the server's
    // listener cancel, so we must signal the listener explicitly: `stop`
    // cancels it (for a muxed frontend that is the shared listener, so it
    // also closes the simplex half and lets simplex peers observe a clean
    // `Closed`), then `join` awaits the teardown. Stopping before joining
    // — rather than relying on `join` to cancel — keeps the teardown
    // correct regardless of how the underlying handle implements `join`.
    duplex_server.stop("duplex accept loop draining");
    duplex_server.join().await;
}

async fn serve_duplex_connection(
    gateway: Gateway,
    mut duplex_rx: channel::duplex::DuplexRx<MessageEnvelope>,
    duplex_tx: channel::duplex::DuplexTx<AttachWire>,
    cancel_token: CancellationToken,
) {
    let first_msg = tokio::select! {
        result = duplex_rx.recv() => match result {
            Ok(msg) => msg,
            Err(e) => {
                tracing::info!(error = %e, "duplex connection closed before first message");
                return;
            }
        },
        () = cancel_token.cancelled() => return,
    };

    if let Ok(attach_request) = first_msg.deserialized::<AttachRequest>() {
        let peer_uid = attach_request.uid;
        tracing::info!(
            uid = %peer_uid,
            "duplex accepted gateway-attach connection",
        );
        // Register the peer *before* accepting, so the handshake
        // reflects the actual server-side state. If registration
        // fails (e.g. a duplicate uid), reject the attach instead of
        // letting the client infer failure from a closed channel.
        let sender = MailboxClient::new(AttachTx(duplex_tx.clone())).into_boxed();
        let attach_guard = match gateway.attach_peer(peer_uid.clone(), sender) {
            Ok(guard) => guard,
            Err(err) => {
                let reason = err.to_string();
                tracing::warn!(
                    uid = %err.uid,
                    error = %reason,
                    "rejecting gateway-attach connection"
                );
                if let Err(send_error) = duplex_tx
                    .send(AttachWire::Ack(AttachAck::Rejected { reason }))
                    .await
                {
                    tracing::warn!(
                        uid = %err.uid,
                        error = %send_error,
                        "failed to send gateway-attach rejection"
                    );
                }
                return;
            }
        };

        // Reply with the via location the peer should advertise.
        let via_location = gateway.default_location().with_via(peer_uid);
        duplex_tx.post(AttachWire::Ack(AttachAck::Accepted {
            location: via_location,
        }));

        let mut handle = gateway.serve_rx(duplex_rx);
        tokio::select! {
            _ = &mut handle => {}
            () = cancel_token.cancelled() => {
                handle.stop("gateway accept loop stopping");
                let _ = handle.await;
            }
        }
        drop(attach_guard);
        tracing::info!("gateway-attach connection closed");
    } else {
        // Regular inbound connection: route messages, no outbound
        // tag-0x01 traffic. The DuplexTx is held for the lifetime of
        // the connection: dropping it closes the session's outbound
        // channel, which causes the session task to exit and the
        // inbound receiver to close after a single message.
        let _keep_alive = duplex_tx;
        let rx = PrependRx {
            first: Some(first_msg),
            inner: duplex_rx,
        };
        let mut handle = gateway.serve_rx(rx);
        tokio::select! {
            _ = &mut handle => {}
            () = cancel_token.cancelled() => {
                handle.stop("gateway accept loop stopping");
                let _ = handle.await;
            }
        }
    }
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

#[async_trait]
impl crate::mailbox::MailboxSender for Weak<GatewayState> {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        match Weak::upgrade(self).map(|inner| Gateway { inner }) {
            Some(gateway) => {
                gateway.route_envelope(envelope, return_handle);
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
        match Weak::upgrade(self).map(|inner| Gateway { inner }) {
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
        self.route_envelope(envelope, return_handle);
    }

    async fn flush(&self) -> Result<(), anyhow::Error> {
        Gateway::flush(self).await
    }
}

impl Gateway {
    fn route_envelope(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        // A message that reaches a gateway resolves by the
        // next hop's outermost `Via(uid, ...)` hop (if any):
        //
        //   * a peer hop (`uid` in `peers`): peel the hop and forward
        //     to that peer;
        //   * *our own* hop (`uid == self.uid`): consume the hop, then
        //     keep routing the peeled next hop;
        //   * a via-less local destination: deliver locally;
        //   * anything else: not ours. Hand it to the forwarder, which
        //     is either a route onward (a dial router, or an attached
        //     duplex) or a terminal `UnroutableMailboxSender` that
        //     returns it as undeliverable.
        //
        // A foreign via is never silently delivered locally just
        // because its inner proc id happens to match a local proc.
        let mut envelope = envelope;
        loop {
            let dest_location = envelope.next_hop().location().clone();
            let Ok((via_uid, inner_location)) = dest_location.pop_via() else {
                break;
            };

            if let Some(sender) = self.inner.peers.read().unwrap().get(&via_uid).cloned() {
                let envelope = Gateway::envelope_with_next_hop_location(envelope, inner_location);
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
            envelope = Gateway::envelope_with_next_hop_location(envelope, inner_location);
        }

        // Via-less destination: deliver to the local proc if it is a
        // delivery target, otherwise hand to the forwarder (outbound egress
        // for plain remote addresses). When a gateway has already consumed a
        // routing hop, it is the named leaf and a miss is undeliverable rather
        // than a fallback forward. A dead entry is left in place —
        // `AttachedProcGuard::drop` is the sole remover.
        let dest_proc = envelope.dest().actor_addr().proc_addr();
        let local = self
            .inner
            .procs
            .read()
            .unwrap()
            .get(dest_proc.id())
            .and_then(WeakProc::upgrade);
        if let Some(proc) = local {
            let local_locations = self.local_delivery_locations();
            if proc.is_local_delivery_target_at(&dest_proc, &local_locations) {
                proc.muxer().post(envelope, return_handle);
                return;
            }
        }

        if envelope.has_next_hop() {
            Gateway::return_no_route(envelope, return_handle);
        } else {
            let forwarder = self.inner.routing.read().unwrap().forwarder.clone();
            forwarder.post(envelope, return_handle)
        }
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

    /// A self via is only one routing hop. After it is consumed, any
    /// remaining via must be routed before local delivery is considered.
    #[tokio::test]
    async fn test_gateway_self_via_routes_remaining_via_before_local_delivery() {
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
        let proc = Proc::builder()
            .proc_id(ProcId::instance(Label::strip("alpha")))
            .shared_gateway(gateway.clone())
            .build()
            .unwrap();

        let foreign_uid = Uid::Instance(0xfeed, Some(Label::strip("foreign")));
        let dest = ProcAddr::new(
            proc.proc_id().clone(),
            proc.default_location()
                .with_via(foreign_uid)
                .with_via(gateway.uid().clone()),
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

        assert_eq!(
            forwarded.load(Ordering::SeqCst),
            1,
            "remaining via routes must not deliver locally after peeling this gateway's hop",
        );
    }

    /// A via hop naming this gateway is consumed for routing before local
    /// delivery, but the proc muxer still sees the canonical destination.
    #[tokio::test]
    async fn test_gateway_self_via_peels_before_local_delivery() {
        #[derive(Clone)]
        struct CapturingSender(Arc<std::sync::Mutex<Option<(PortAddr, PortAddr)>>>);

        #[async_trait]
        impl MailboxSender for CapturingSender {
            fn post_unchecked(
                &self,
                envelope: MessageEnvelope,
                _return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
            ) {
                *self.0.lock().unwrap() =
                    Some((envelope.dest().clone(), envelope.next_hop().clone()));
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
                via_dest.clone(),
                &7u64,
                Flattrs::new(),
            )
            .unwrap(),
            monitored_return_handle(),
        );

        assert_eq!(
            *captured.lock().unwrap(),
            Some((via_dest, inner_dest)),
            "delivery preserves the canonical destination and peels only the next hop",
        );
    }

    /// A via hop naming *this* gateway (the leaf) whose inner proc id is
    /// not a live local proc is undeliverable — a hop addressed to us is
    /// never forwarded back out. The returned envelope preserves the
    /// canonical destination and carries the peeled next hop.
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
        assert_eq!(returned.next_hop(), &peeled_dest);
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
                .filter_map(WeakProc::upgrade)
                .count(),
            0,
            "no procs should still be live after attach_drop task completes",
        );
    }

    /// After the gateway is dropped, its weak server sender
    /// (the sender used by gateway-served mailbox tasks) bounces
    /// envelopes as a structured transport failure rather than panicking or
    /// hanging.
    /// Tested directly against the weak sender: no channel server, no
    /// task lifecycle. The bounce is in-process and
    /// observable at the caller's return port without going through
    /// any serialize/dispatch path.
    #[tokio::test]
    async fn test_weak_gateway_bounces_broken_link_after_drop() {
        let gateway = Gateway::isolated();
        let weak = Arc::downgrade(&gateway.inner);
        drop(gateway);

        // Scratch proc just to host the return port.
        let scratch = Proc::isolated();
        let scratch_client = scratch.client("return");
        let (return_handle, mut return_rx) =
            scratch_client.open_port::<Undeliverable<MessageEnvelope>>();

        // Fabricate a destination. Its contents don't matter; the
        // bounce happens when the weak sender fails to upgrade before any demux
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

        // Post directly through the weak sender. Upgrade fails and sends the
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

    /// Active serves unwind by handle id when handles stop out of
    /// order. Three concurrent servers; stop the middle one, then the
    /// last, then the first, asserting the gateway's
    /// `default_location` at each step. Final empty state reverts to
    /// the construction-time fallback.
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
        // last active serve). `stop` runs the cleanup; `join` awaits
        // teardown.
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

    #[tokio::test]
    async fn test_gateway_first_local_serve_uses_fallback_after_nonlocal_serve() {
        let gateway = Gateway::isolated();
        let fallback = gateway.default_location();

        let mut unix = Gateway::serve(&gateway, ChannelAddr::any(ChannelTransport::Unix)).unwrap();
        let unix_location = gateway.default_location();
        assert_ne!(unix_location, fallback);

        let mut local =
            Gateway::serve(&gateway, ChannelAddr::any(ChannelTransport::Local)).unwrap();
        assert_eq!(gateway.default_location(), fallback);

        local.stop("test");
        local.join().await.unwrap();
        assert_eq!(gateway.default_location(), unix_location);

        unix.stop("test");
        unix.join().await.unwrap();
        assert_eq!(gateway.default_location(), fallback);
    }

    /// End-to-end gateway-to-gateway attach via the new protocol:
    /// the client gateway calls `serve_via` against a peer that
    /// called `serve_duplex`; the handshake assigns the client a via
    /// location and installs the duplex sender; outbound traffic from
    /// the client's gateway falls through to the duplex; inbound
    /// envelope next hops addressed to procs on the server gateway
    /// are peeled at the via boundary and routed locally.
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
    async fn test_gateway_serve_via_sessions_are_additive() {
        let server1_gw = Gateway::new();
        let mut accept1_handle = server1_gw
            .serve_duplex(ChannelAddr::any(ChannelTransport::Unix))
            .unwrap();
        let server1_addr = server1_gw.default_location().addr().clone();
        let server2_gw = Gateway::new();
        let mut accept2_handle = server2_gw
            .serve_duplex(ChannelAddr::any(ChannelTransport::Unix))
            .unwrap();
        let server2_addr = server2_gw.default_location().addr().clone();

        let client_gw = Gateway::new();
        let pre_default = client_gw.default_location();
        let mut serve_via1 = client_gw.serve_via(server1_addr.clone()).await.unwrap();
        let via1_default = client_gw.default_location();
        assert_eq!(via1_default.addr(), &server1_addr);

        let client_proc = Proc::legacy_service_pseudo_singleton_on_gateway(client_gw.clone());
        let client = client_proc.client("recv");
        let (client_port, mut client_rx) = client.bind_handler_port::<u64>();
        let PortLocation::Bound(old_client_dest) = client_port.location() else {
            panic!("client port must be bound");
        };

        let mut serve_via2 = client_gw.serve_via(server2_addr.clone()).await.unwrap();
        let via2_default = client_gw.default_location();
        assert_eq!(via2_default.addr(), &server2_addr);
        assert_ne!(via1_default, via2_default);

        server1_gw.post(
            MessageEnvelope::serialize(
                test_actor_id("server1", "sender"),
                old_client_dest,
                &11u64,
                Flattrs::new(),
            )
            .unwrap(),
            monitored_return_handle(),
        );
        let received = time::timeout(Duration::from_secs(5), client_rx.recv())
            .await
            .expect("client_rx timed out")
            .expect("client_rx closed");
        assert_eq!(received, 11);

        serve_via2.stop("test cleanup");
        serve_via2.join().await.unwrap();
        assert_eq!(client_gw.default_location(), via1_default);

        serve_via1.stop("test cleanup");
        serve_via1.join().await.unwrap();
        assert_eq!(client_gw.default_location(), pre_default);

        accept1_handle.stop("test cleanup");
        accept1_handle.join().await.unwrap();
        accept2_handle.stop("test cleanup");
        accept2_handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_gateway_serve_via_duplicate_peer_reports_rejection() {
        let server_gw = Gateway::new();
        let mut accept_handle = server_gw
            .serve_duplex(ChannelAddr::any(ChannelTransport::Unix))
            .unwrap();
        let server_addr = server_gw.default_location().addr().clone();

        let client_gw = Gateway::new();
        let mut serve_via = client_gw.serve_via(server_addr.clone()).await.unwrap();
        let via_default = client_gw.default_location();

        let err = client_gw.serve_via(server_addr).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("gateway already has a via peer with uid"),
            "unexpected error: {err:#}"
        );
        assert_eq!(client_gw.default_location(), via_default);

        serve_via.stop("test cleanup");
        serve_via.join().await.unwrap();
        accept_handle.stop("test cleanup");
        accept_handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_gateway_serve_with_listener_takes_precedence_after_via() {
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

        let mut frontend = client_gw
            .serve_with_listener(ChannelAddr::any(ChannelTransport::Unix), None)
            .unwrap();
        let frontend_location = client_gw.default_location();
        assert!(matches!(frontend_location.addr(), ChannelAddr::Unix(_)));
        assert_ne!(frontend_location, via_default);

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
    async fn test_set_default_location_updates_fallback_while_serving() {
        let gateway = Gateway::new();
        let mut serve =
            Gateway::serve(&gateway, ChannelAddr::any(ChannelTransport::Local)).unwrap();
        let served_location = gateway.default_location();
        let fallback_location = Location::from(ChannelAddr::Local(9876));

        gateway.set_default_location(fallback_location.clone());
        assert_eq!(gateway.default_location(), served_location);

        serve.stop("test cleanup");
        serve.join().await.unwrap();
        assert_eq!(gateway.default_location(), fallback_location);
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
        let server_proc = Proc::legacy_service_pseudo_singleton_on_gateway(server_gw.clone());
        let server = server_proc.client("recv");
        let (server_port, mut server_rx) = server.bind_handler_port::<u64>();
        let PortLocation::Bound(server_dest) = server_port.location() else {
            panic!("server port must be bound");
        };

        let client_gw = Gateway::new();
        let mut serve_via = client_gw.serve_via(server_addr).await.unwrap();
        let client_proc = Proc::legacy_service_pseudo_singleton_on_gateway(client_gw.clone());
        let (client_shadow_tx, mut client_shadow_rx) = mpsc::unbounded_channel();
        assert!(
            client_proc.muxer().bind(
                server_dest.actor_addr().id().clone(),
                RecordingSender(client_shadow_tx),
            ),
            "client shadow handler should bind"
        );

        client_gw.post(
            MessageEnvelope::serialize(
                test_actor_id("client", "sender"),
                server_dest.clone(),
                &7u64,
                Flattrs::new(),
            )
            .unwrap(),
            monitored_return_handle(),
        );

        let received = time::timeout(Duration::from_secs(5), server_rx.recv())
            .await
            .expect("server_rx timed out")
            .expect("server_rx closed");
        assert_eq!(received, 7);
        time::timeout(Duration::from_millis(200), client_shadow_rx.recv())
            .await
            .expect_err("peer legacy proc location must not deliver to the attached client proc");

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
        // relay's raw address while preserving the canonical destination. The
        // relay peels `Via(client_uid, relay_addr)` from the routing
        // destination and forwards over the attached duplex; the client then
        // delivers against canonical `local@Via(client_uid, relay_addr)`.
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

        // gw_a → alpha: gw_a consumes its own outermost Via from the routing
        // destination and delivers locally.
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
