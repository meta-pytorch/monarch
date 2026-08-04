/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::os::fd::OwnedFd;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use slotmap::SlotMap;
use slotmap::new_key_type;
use tokio::runtime;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::Role;
use crate::actor::ActorEntry;
use crate::actor::ActorName;
use crate::actor::Delivery;
use crate::actor::GatewayMonitors;
use crate::actor::GatewayState;
use crate::actor::MonitorSub;
use crate::actor::MonitorToFire;
use crate::actor::Route;
use crate::actor::TargetMonitors;
use crate::connection::ConnectRequest;
use crate::connection::Connection;
use crate::connection::ConnectionCommand;
use crate::connection::ConnectionRef;
use crate::connection::ConnectionTransport;
use crate::connection::MonitorOp;
use crate::connection::SendPayload;
use crate::connection::SideChannelAction;
use crate::connection::SideChannelMessage;
use crate::inproc_transport::InprocTransport;
use crate::msg::MsgPart;
use crate::poller::Delivered;
use crate::poller::PollerEntry;
use crate::quic_transport::QuicTransport;
use crate::shm::MapperHandle;
use crate::shm::ShmClient;
use crate::shm::ShmClientSlot;
use crate::shm::ShmMapper;
use crate::shm::ShmServer;
use crate::transport::Transport;
use crate::unix_transport::UnixTransport;

/// Whether verbose per-connection debug logging is enabled (`MM_QUIC_DEBUG` set).
/// Gates the command-loop `MM_CTX`/`MM_UDP` lines and scheduler-lag probe here, and
/// the per-connection reader diagnostics and heartbeat-send (`MM_HB`) logging in the
/// quic transport — all off by default so they cost nothing in normal operation.
pub(crate) fn connection_debug() -> bool {
    std::env::var_os("MM_QUIC_DEBUG").is_some_and(|v| !v.is_empty())
}

/// Cumulative UDP-over-IPv6 datagram counters from `/proc/net/snmp6`, as
/// `(InDatagrams, InErrors, RcvbufErrors)`. `RcvbufErrors` counts datagrams the
/// kernel dropped because a receiving socket's buffer was full — the direct signal
/// for root-side ingress overflow (packets arriving faster than the single-threaded
/// driver can drain them). System-wide, but on the root host our traffic dominates.
/// Reading this proc pseudo-file is an instant, non-blocking memory read, so
/// `std::fs` is fine here (debug instrumentation on the once-per-second log path).
fn read_udp6_stats() -> Option<(u64, u64, u64)> {
    let text = std::fs::read_to_string("/proc/net/snmp6").ok()?;
    let (mut indg, mut inerr, mut rcv) = (None, None, None);
    for line in text.lines() {
        let mut it = line.split_whitespace();
        let (Some(name), Some(val)) = (it.next(), it.next()) else {
            continue;
        };
        match name {
            "Udp6InDatagrams" => indg = val.parse().ok(),
            "Udp6InErrors" => inerr = val.parse().ok(),
            "Udp6RcvbufErrors" => rcv = val.parse().ok(),
            _ => {}
        }
    }
    Some((indg?, inerr?, rcv?))
}

/// Current wall-clock time as `HH:MM:SS.mmm` (UTC), for debug log lines. Kept
/// dependency-free (no chrono): derived straight from the UNIX epoch, so it lines
/// up with the smoke test's Python `log()` timestamps when hosts run in UTC.
pub(crate) fn wall_clock_hms() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        (secs / 3600) % 24,
        (secs / 60) % 60,
        secs % 60,
        now.subsec_millis(),
    )
}

/// Whether `url`'s scheme is one we have a transport for. Kept separate from
/// [`Ctx::transport_for`] so the scheme can be validated — before the connection
/// is attached, while the request can still report failure — without a `&mut self`
/// borrow.
fn valid_scheme(url: &str) -> bool {
    url.starts_with("inproc://") || url.starts_with("unix://") || url.starts_with("quic://")
}

/// Whether `url`'s scheme connects across machines (a network transport). A
/// gateway may only gain a parent over such a link — a gateway joined to a
/// unix/inproc parent is rejected, since it would no longer be the entry point
/// for its process group. (Today only quic; tcp joins this set later.)
fn is_network_scheme(url: &str) -> bool {
    url.starts_with("quic://")
}

/// The gateway specifier (the `@endpoint` suffix) of an ident, or an empty slice
/// if it has none. Actor idents are formatted `name@gateway_specifier`; an ident
/// with no `@` belongs to the root gateway (specifier-less). The specifier is the
/// address a gateway dials to reach the ident's owning gateway directly, so it is
/// also a gateway's own domain tag (parsed from its own ident).
fn gateway_tag(ident: &[u8]) -> &[u8] {
    match ident.iter().rposition(|&b| b == b'@') {
        Some(pos) => &ident[pos + 1..],
        None => &[],
    }
}

new_key_type! {
    pub(crate) struct Key;
    pub(crate) struct PollerKey;
    pub(crate) struct ChildConnectionKey;
}

/// Grace period before a target's debounced `Unsubscribe` actually goes out. A
/// monitor recreated on the same target within this window reuses the still-live
/// upstream subscription, so churn (e.g. per-message monitor/cancel) sends no
/// subscribe/unsubscribe traffic.
const MONITOR_DEBOUNCE: Duration = Duration::from_millis(25);

pub(crate) enum Command {
    Init {
        thread: JoinHandle<()>,
    },
    CreateActor {
        ident: Option<MsgPart>,
        gateway: bool,
        done: oneshot::Sender<Key>,
    },
    DestroyActor {
        key: Key,
    },
    CreatePoller {
        tx: mpsc::UnboundedSender<Delivered>,
        event_fd: OwnedFd,
        done: oneshot::Sender<PollerKey>,
    },
    DestroyPoller {
        poller: PollerKey,
    },
    Subscribe {
        poller: PollerKey,
        index: usize,
        actor: Key,
        done: oneshot::Sender<anyhow::Result<()>>,
    },
    Unsubscribe {
        poller: PollerKey,
        index: usize,
    },
    ArmPoller {
        poller: PollerKey,
        wake_after: u64,
    },
    Send {
        sender: Key,
        destination_ident: MsgPart,
        parts: Vec<MsgPart>,
    },
    Serve {
        actor: Key,
        url: String,
        request: ConnectRequest,
    },
    Join {
        actor: Key,
        url: String,
        request: ConnectRequest,
    },
    ConnectionAction {
        connection: ConnectionRef,
        action: ConnectionCommand,
    },
    // A message arrived over a gateway side-channel (a direct gateway-to-gateway
    // QUIC connection, not a parent/child link). The self-addressing message names
    // the actor whose gateway should handle it; the receiving gateway is resolved
    // from that (several gateways may share one endpoint url).
    SideChannelDeliver(SideChannelMessage),
    // A transport's pipe is up; install it on the connection. Transport-agnostic:
    // both inproc and unix emit it, and the command loop drives establishment
    // from here (sends our Establish along the transport).
    TransportConnected {
        connection: ConnectionRef,
        transport: Box<dyn ConnectionTransport>,
    },
    Die {
        actor: Key,
        reason: MsgPart,
    },
    Monitor {
        actor: Key,
        id: u64,
        to_monitor: MsgPart,
        failure_prefix: Vec<MsgPart>,
        timeout_ms: u64,
    },
    CancelMonitor {
        actor: Key,
        id: u64,
    },
    // Fired by a "must exist" timer armed at `at` by a local or remote subscribe.
    // Remote (a `gateway_state` entry for `target`'s tag whose `MonitorToFire` is
    // still unacked): the owning gateway never answered, so declare it dead. Local
    // (a still-`Unknown` route to `target` at `at`): the target never appeared, so
    // fire the non-existence timeout. Anything else is a no-op.
    CheckMonitorTimeout {
        at: Key,
        listener: Vec<u8>,
        target: Vec<u8>,
    },
    // Fired by a debounce timer: if `to_monitor` is still `PendingUnsubscribe`
    // (i.e. it was not re-monitored in the grace window), send the upstream
    // `Unsubscribe` now and drop the entry. Otherwise it is `Active` again (or
    // gone) and we keep the subscription.
    UnsubscribeMonitor {
        actor: Key,
        to_monitor: Vec<u8>,
    },
    Shutdown {
        done: oneshot::Sender<JoinHandle<()>>,
    },
}

struct Ctx {
    actors: SlotMap<Key, ActorEntry>,
    pollers: SlotMap<PollerKey, PollerEntry>,
    // The transports. Each owns its own pairing/socket state and coroutines plus
    // a clone of the loop sender; the command loop only forwards serves/joins to
    // them and handles the generic TransportConnected/ConnectionAction they
    // emit.
    inproc: InprocTransport,
    unix: UnixTransport,
    quic: QuicTransport,
    // The context-global shared-memory address-space manager. Always present
    // (idle if shm is unused); handed to the unix and quic transports, and unmaps
    // everything when the context — and thus this last reference — is dropped.
    #[expect(
        dead_code,
        reason = "held so the mapper outlives the context; used via the unix and quic transports"
    )]
    mapper: MapperHandle,
    // The context-global shared-memory server, shared by every gateway in this
    // context (created lazily when the first gateway appears). One slab + client
    // per serving process — not per gateway actor — so multiple independent actor
    // trees in the same process share it. Dropping it (with the context) releases
    // the slab.
    shm_server: Option<ShmServer>,
    // The single shm client for this context: the slab handed to gateways and to
    // the quic listener coroutines. Empty until the first gateway creates the
    // server. A clone of this slot lives in the quic transport.
    shm_client: ShmClientSlot,
    // Side-channel messages whose `gateway_for_actor` no gateway in this context can
    // resolve yet, keyed by that ident. The owning gateway is not known up front
    // (several gateways may serve the same endpoint url, and the actor may not exist
    // yet), so the *whole* self-addressing message is held context-wide — regardless
    // of its action — and replayed through `deliver_side_channel` once a route to the
    // ident is learned (see `populate_routes`).
    pending_side_channel: HashMap<Vec<u8>, Vec<SideChannelMessage>>,
    // Every gateway actor ever created in this context, so resolving a side-channel
    // destination scans only gateways (a handful) rather than every actor. A
    // *dead* gateway is kept: its routing table still tells us whether a destination
    // was reachable through it, so we can drop a side-channel message for a dead
    // subtree rather than hold it forever.
    gateways: Vec<Key>,
    // A clone of the loop sender, used to schedule debounced monitor unsubscribes
    // back onto the event loop after a grace period.
    loop_tx: mpsc::UnboundedSender<Command>,
    thread: Option<JoinHandle<()>>,
    _not_send: PhantomData<*const ()>,
}

impl Ctx {
    fn new(tx: mpsc::UnboundedSender<Command>) -> Self {
        let mapper: MapperHandle = Arc::new(Mutex::new(ShmMapper::new()));
        let shm_client: ShmClientSlot = Arc::new(Mutex::new(None));
        Self {
            actors: SlotMap::with_key(),
            pollers: SlotMap::with_key(),
            inproc: InprocTransport::new(tx.clone()),
            unix: UnixTransport::new(tx.clone(), mapper.clone()),
            quic: QuicTransport::new(tx.clone(), mapper.clone(), shm_client.clone()),
            mapper,
            shm_server: None,
            shm_client,
            pending_side_channel: HashMap::new(),
            gateways: Vec::new(),
            loop_tx: tx,
            thread: None,
            _not_send: PhantomData,
        }
    }

    /// Insert a new actor, tracking it in `gateways` if it is one (so side-channel
    /// delivery scans only gateways, never every actor).
    fn insert_actor(&mut self, ident: Option<Vec<u8>>, gateway: bool) -> Key {
        let key = self.actors.insert(ActorEntry::new(ident, gateway));
        if gateway {
            self.gateways.push(key);
        }
        key
    }

    fn run_command(&mut self, command: Command) {
        match command {
            Command::Init { thread } => {
                self.thread = Some(thread);
            }
            Command::CreateActor {
                ident,
                gateway,
                done,
            } => {
                let key =
                    self.insert_actor(ident.as_ref().map(|part| part.as_bytes().to_vec()), gateway);
                drop(ident);
                // A gateway shares the context's slab (created lazily on the first
                // gateway): seed its client slot from it, so it can shm-ify large
                // sends to its children and hand the state down to them. Best-effort
                // — on failure the gateway just streams inline.
                if gateway {
                    self.init_gateway_shm(key);
                }
                let _ = done.send(key);
            }
            Command::DestroyActor { key } => {
                let subscription = self.actor(key).poller_subscription();
                self.die_actor(key, b"actor destroyed".to_vec());
                if let Some((poller, index)) = subscription {
                    if let Some(poller) = self.pollers.get_mut(poller) {
                        let _ = poller.remove(index);
                    }
                    self.actor_mut(key).unsubscribe();
                }
            }
            Command::CreatePoller { tx, event_fd, done } => {
                let key = self.pollers.insert(PollerEntry::new(tx, event_fd));
                let _ = done.send(key);
            }
            Command::DestroyPoller { poller } => {
                if let Some(poller) = self.pollers.remove(poller) {
                    for subscription in poller.subscriptions {
                        self.actor_mut(subscription.actor).unsubscribe();
                    }
                }
            }
            Command::Subscribe {
                poller,
                index,
                actor,
                done,
            } => {
                if !self.pollers.contains_key(poller) {
                    let _ = done.send(Err(anyhow::anyhow!("poller does not exist")));
                } else if self.pollers[poller].has_index(index) {
                    let _ = done.send(Err(anyhow::anyhow!("poller index is already subscribed")));
                } else {
                    // Move the actor onto the poller and flush any messages that were
                    // buffered while it had no poller attached.
                    let result = (|| -> anyhow::Result<()> {
                        let buffered = self.actor_mut(actor).subscribe(poller, index)?;
                        let poller_entry =
                            self.pollers.get_mut(poller).expect("poller should exist");
                        poller_entry.insert(index, actor);
                        for msg in buffered {
                            poller_entry.deliver(index, msg);
                        }
                        Ok(())
                    })();
                    let _ = done.send(result);
                }
            }
            Command::Unsubscribe { poller, index } => {
                if let Some(poller) = self.pollers.get_mut(poller) {
                    if let Some(actor) = poller.remove(index) {
                        self.actor_mut(actor).unsubscribe();
                    }
                }
            }
            Command::ArmPoller { poller, wake_after } => {
                if let Some(poller) = self.pollers.get_mut(poller) {
                    poller.arm(wake_after);
                }
            }
            Command::Send {
                sender,
                destination_ident,
                parts,
            } => {
                self.route_message(
                    sender,
                    destination_ident.as_bytes().to_vec(),
                    SendPayload::ActorMessage(parts),
                );
            }
            Command::Serve {
                actor,
                url,
                request,
            } => {
                self.serve(actor, url, request);
            }
            Command::Join {
                actor,
                url,
                request,
            } => {
                self.join(actor, url, request);
            }
            Command::ConnectionAction { connection, action } => {
                if !matches!(self.connection(connection), Connection::Failed) {
                    self.run_connection_command(connection, action);
                }
            }
            Command::TransportConnected {
                connection,
                transport,
            } => {
                self.transport_connected(connection, transport);
            }
            Command::SideChannelDeliver(message) => {
                self.deliver_side_channel(message);
            }
            Command::Die { actor, reason } => {
                let reason = reason.as_bytes().to_vec();
                self.die_actor(actor, reason);
            }
            Command::Monitor {
                actor,
                id,
                to_monitor,
                failure_prefix,
                timeout_ms,
            } => {
                let to_monitor = to_monitor.as_bytes().to_vec();
                self.monitor_add(actor, id, to_monitor, failure_prefix, timeout_ms);
            }
            Command::CancelMonitor { actor, id } => {
                self.monitor_remove(actor, id);
            }
            Command::CheckMonitorTimeout {
                at,
                listener,
                target,
            } => {
                self.check_monitor_timeout(at, listener, target);
            }
            Command::UnsubscribeMonitor { actor, to_monitor } => {
                self.unsubscribe_monitor(actor, to_monitor);
            }
            Command::Shutdown { .. } => {
                // Handled directly by the event loop (see CtxHandle::new) so it
                // can await the UNIX writer flush before tearing the loop down.
                unreachable!("Shutdown is intercepted by the event loop");
            }
        }
    }

    fn actor(&self, actor: Key) -> &ActorEntry {
        self.actors.get(actor).expect("actor should exist")
    }

    fn actor_mut(&mut self, actor: Key) -> &mut ActorEntry {
        self.actors.get_mut(actor).expect("actor should exist")
    }

    fn connection(&self, connection: ConnectionRef) -> &Connection {
        let actor = self.actor(connection.owning_actor());
        match connection {
            ConnectionRef::ParentConnection { .. } => actor
                .parent
                .as_ref()
                .expect("parent connection should exist"),
            ConnectionRef::ChildConnection { slot, .. } => actor
                .children
                .get(slot)
                .expect("child connection should exist"),
        }
    }

    fn connection_mut(&mut self, connection: ConnectionRef) -> &mut Connection {
        let actor = self.actor_mut(connection.owning_actor());
        match connection {
            ConnectionRef::ParentConnection { .. } => actor
                .parent
                .as_mut()
                .expect("parent connection should exist"),
            ConnectionRef::ChildConnection { slot, .. } => actor
                .children
                .get_mut(slot)
                .expect("child connection should exist"),
        }
    }

    fn connection_set(&mut self, connection: ConnectionRef, value: Connection) -> Connection {
        std::mem::replace(self.connection_mut(connection), value)
    }

    /// The single destination-driven routing pathway. Both an ordinary actor
    /// message and a monitor firing travel through here, differing only in what
    /// `deliver_payload` does once the destination is reached. A monitor fire is
    /// armed only at an ancestor that already routes to its destination, so it
    /// always finds a concrete route down (or a dead one) and never reaches the
    /// gateway-buffer branch.
    fn route_message(&mut self, sender: Key, destination_ident: Vec<u8>, payload: SendPayload) {
        let entry = self.actor(sender);
        if !entry.alive {
            return;
        }
        if entry.name() == Some(destination_ident.as_slice()) {
            self.deliver_payload(sender, payload);
            return;
        }

        match entry.routes.get(destination_ident.as_slice()) {
            // A known child route: hand the payload down toward the destination.
            Some(Route::Connection { child, .. }) => {
                let child_key = *child;
                let connection = self
                    .actor_mut(sender)
                    .children
                    .get_mut(child_key)
                    .expect("route child connection should exist");
                let _ = connection.send(ConnectionCommand::SendMessage {
                    destination_ident,
                    payload,
                });
                return;
            }
            // The destination is known to be dead: drop the payload rather than
            // forwarding it up (where it would buffer forever at the gateway).
            Some(Route::Dead) => return,
            Some(Route::Unknown { .. }) | None => {}
        }

        // No local route. A gateway is the boundary of its routing domain, so a
        // destination on a *different* gateway is reached by a direct side-channel
        // rather than by climbing toward the root. Destinations in the root domain
        // (no specifier) still climb the parent chain; destinations in our own
        // domain that we simply do not know yet are buffered locally — never sent
        // up, which would only bounce them back to us.
        if self.actor(sender).is_gateway() {
            let own_tag = gateway_tag(self.actor(sender).name().unwrap_or_default()).to_vec();
            let dest_tag = gateway_tag(&destination_ident).to_vec();
            if !dest_tag.is_empty() && dest_tag != own_tag {
                self.send_to_gateway(SideChannelMessage {
                    gateway_for_actor: destination_ident,
                    action: SideChannelAction::Send(payload),
                });
                return;
            }
            if !own_tag.is_empty() && dest_tag == own_tag {
                self.actor_mut(sender)
                    .buffer_unrouted(destination_ident, payload);
                return;
            }
        }

        // No known route here. Forward the payload — intact — toward the gateway;
        // the gateway itself (no parent) buffers it until the destination's route
        // is published, since the destination may not even have been created yet. A
        // `Route::Unknown` entry only exists at a gateway, so it also lands here.
        if let Some(parent_connection) = self.actor_mut(sender).parent.as_mut() {
            let _ = parent_connection.send(ConnectionCommand::SendMessage {
                destination_ident,
                payload,
            });
        } else {
            self.actor_mut(sender)
                .buffer_unrouted(destination_ident, payload);
        }
    }

    /// Hand a routed payload to the actor it is addressed to: an actor message
    /// goes to the poller, a monitor fire fans out to every local monitor on the
    /// dead target.
    fn deliver_payload(&mut self, actor: Key, payload: SendPayload) {
        match payload {
            SendPayload::ActorMessage(parts) => self.deliver_to_actor(actor, parts),
            SendPayload::FireMonitor {
                to_monitor,
                is_timeout,
            } => {
                let reason: &[u8] = if is_timeout {
                    b"actor does not exist"
                } else {
                    b"actor died"
                };
                self.fire_local_monitors(actor, to_monitor, reason);
            }
        }
    }

    /// Select the transport for a url's scheme. Only called once the scheme is
    /// known valid (see [`valid_scheme`]), so a missing transport is a bug.
    fn transport_for(&mut self, url: &str) -> &mut dyn Transport {
        if url.starts_with("inproc://") {
            &mut self.inproc
        } else if url.starts_with("unix://") {
            &mut self.unix
        } else if url.starts_with("quic://") {
            &mut self.quic
        } else {
            unreachable!("scheme already validated by valid_scheme");
        }
    }

    fn serve(&mut self, actor: Key, url: String, request: ConnectRequest) {
        // Shared setup runs first regardless of transport: validate the scheme,
        // then attach the unestablished connection. Only then do we hand the
        // connection to the transport, which brings up its pipe and announces it
        // back via TransportConnected. (The scheme is validated before attach so a
        // bad scheme can still report failure with the as-yet-unconsumed request.)
        if !valid_scheme(&url) {
            self.deliver_connect_failure(actor, request, b"unsupported url scheme".to_vec());
            return;
        }
        if self.gateway_parent_rejected(actor, request.role, &url) {
            self.deliver_connect_failure(
                actor,
                request,
                b"gateway must have no parent or a network parent".to_vec(),
            );
            return;
        }
        let Some(connection) = self.attach_connection(actor, request) else {
            return;
        };
        let shm_client = self.actor(actor).shm_client.clone();
        self.transport_for(&url).serve(url, connection, shm_client);
    }

    fn join(&mut self, actor: Key, url: String, request: ConnectRequest) {
        if !valid_scheme(&url) {
            self.deliver_connect_failure(actor, request, b"unsupported url scheme".to_vec());
            return;
        }
        if self.gateway_parent_rejected(actor, request.role, &url) {
            self.deliver_connect_failure(
                actor,
                request,
                b"gateway must have no parent or a network parent".to_vec(),
            );
            return;
        }
        let Some(connection) = self.attach_connection(actor, request) else {
            return;
        };
        let shm_client = self.actor(actor).shm_client.clone();
        self.transport_for(&url).join(url, connection, shm_client);
    }

    /// Whether attaching a parent over `url` to `actor` must be rejected because
    /// `actor` is a gateway gaining a non-network parent. A gateway is the entry
    /// point for its process group, so it may only have no parent or a network
    /// (quic/tcp) parent; a unix/inproc parent would demote it. Only a `Child`
    /// role gains a parent, so a `Parent` role never trips this.
    fn gateway_parent_rejected(&self, actor: Key, role: Role, url: &str) -> bool {
        role == Role::Child && self.actor(actor).is_gateway() && !is_network_scheme(url)
    }

    fn attach_connection(&mut self, actor: Key, request: ConnectRequest) -> Option<ConnectionRef> {
        let role = request.role;
        let connection = Connection::new_unestablished(request);

        let connection = match role {
            Role::Parent => {
                // An actor may adopt children before it has a parent (it buffers
                // routes and hands them upward once it gains one — see
                // publish_routes_after_established). The gateway flag is fixed at
                // creation and no longer gates this.
                let slot = self.actor_mut(actor).children.insert(connection);
                // A new child is where we (re)generate gateway state from what we
                // already hold: send it down the fresh connection. It buffers until
                // the connection establishes, and the transport decides whether to
                // actually forward it (quic drops it).
                if let Some(client) = self.shm_client_of(actor) {
                    if let Some(connection) = self.actor_mut(actor).children.get_mut(slot) {
                        let _ = connection.send(ConnectionCommand::GatewayState { client });
                    }
                }
                ConnectionRef::ChildConnection {
                    ofactor: actor,
                    slot,
                }
            }
            Role::Child => {
                let actor_entry = self.actor_mut(actor);
                // An actor may have at most one parent. It is free to have already
                // served children or buffered messages; gaining a parent hands all
                // of that upward (see publish_routes_after_established).
                if actor_entry.parent.is_some() {
                    let Connection::Unestablished {
                        mut failure_prefix, ..
                    } = connection
                    else {
                        unreachable!("new connection should be unestablished");
                    };
                    failure_prefix.push(MsgPart::from_bytes(Vec::new()));
                    failure_prefix.push(MsgPart::from_bytes(
                        b"invalid parent-child topology".to_vec(),
                    ));
                    self.deliver_to_actor(actor, failure_prefix);
                    return None;
                }
                actor_entry.parent = Some(connection);
                ConnectionRef::ParentConnection { ofactor: actor }
            }
        };

        Some(connection)
    }

    /// A transport's pipe is up. Announce ourselves to the peer along it via an
    /// `Establish`, then hold the transport on the connection until the peer's
    /// `Establish` lands (`Unestablished` → `Connecting`).
    ///
    /// If the owning actor already died, the connection is `Failed`: we still send
    /// an `Establish`, carrying our (dead) ident but `alive: false`, so the peer
    /// severs *and names us* in its failure. We then drop the transport without
    /// transitioning (its drop-severance is the generic fallback, skipped once the
    /// peer is already failing).
    fn transport_connected(
        &mut self,
        connection: ConnectionRef,
        transport: Box<dyn ConnectionTransport>,
    ) {
        let role = connection.role();
        let ident = self
            .actor(connection.owning_actor())
            .name()
            .map(|name| name.to_vec());
        let alive = matches!(
            self.connection(connection),
            Connection::Unestablished { .. }
        );
        let name_for_other = match self.connection(connection) {
            Connection::Unestablished { name_for_other, .. } => name_for_other.clone(),
            _ => None,
        };
        transport.send(ConnectionCommand::Establish {
            role,
            ident,
            name_for_other: name_for_other.clone(),
            alive,
        });
        if !alive {
            return;
        }

        let conn = self.connection_mut(connection);
        let Connection::Unestablished {
            hello_prefix,
            failure_prefix,
            queued_commands,
            ..
        } = std::mem::replace(conn, Connection::Failed)
        else {
            unreachable!("alive implies Unestablished");
        };
        *conn = Connection::Connecting {
            transport,
            name_for_other,
            hello_prefix,
            failure_prefix,
            queued_commands,
        };
    }

    fn connection_topology_is_valid(&self, connection: ConnectionRef) -> bool {
        // The gateway/parent topology is enforced up front at serve/join time (a
        // gateway only gains a network parent; an actor has at most one parent),
        // so by establishment the only thing that can have changed is the actor
        // dying. Both roles are valid as long as the owning actor is still alive.
        self.actor(connection.owning_actor()).alive
    }

    /// Record routes arriving over `child_connection`: `live` idents become
    /// concrete routes to that child (and are remembered against it, so they can be
    /// marked dead if it later fails), `dead` idents are marked [`Route::Dead`].
    /// Both kinds are carried further up.
    fn populate_routes(
        &mut self,
        parent: Key,
        child_connection: ChildConnectionKey,
        live: Vec<Vec<u8>>,
        dead: Vec<Vec<u8>>,
    ) {
        for ident in &live {
            // Take any existing entry so we can carry its monitors onto the now-known
            // route and flush its buffered messages. Monitors stay here: this actor is
            // the responsible ancestor for the destination (it is reachable below it).
            let previous = self.actor_mut(parent).routes.remove(ident.as_slice());
            let (monitors, buffered) = match previous {
                Some(Route::Unknown { messages, monitors }) => (monitors, messages),
                Some(Route::Connection { monitors, .. }) => (monitors, Vec::new()),
                _ => (Vec::new(), Vec::new()),
            };
            self.actor_mut(parent).routes.insert(
                ident.clone(),
                Route::Connection {
                    child: child_connection,
                    monitors,
                },
            );
            // Remember the ident is carried by this child so we can mark exactly
            // these dead (no table scan) if the connection later fails.
            self.actor_mut(parent)
                .record_routed_via_child(child_connection, ident);

            // Re-route every payload buffered against the now-resolved Unknown route;
            // route_message sends them down the child connection we just recorded.
            for payload in buffered {
                self.route_message(parent, ident.clone(), payload);
            }
            // Self-addressing side-channel messages held context-wide for this ident
            // can now be resolved, so replay each through deliver_side_channel — the
            // one place a previously-unresolvable side-channel ident gains a route.
            if let Some(pending) = self.pending_side_channel.remove(ident.as_slice()) {
                for message in pending {
                    self.deliver_side_channel(message);
                }
            }
        }

        // Mark the dead idents dead (they are not tracked against the child — they
        // are already dead, so a later failure of this connection has nothing to
        // re-kill there). Keep only those that newly transitioned so propagation
        // can't loop, firing the monitors that were waiting on each.
        let mut newly_dead = Vec::new();
        // Each fire is the dead ident paired with a subscriber's `dest`.
        let mut to_fire: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for ident in dead {
            let routes = &mut self.actor_mut(parent).routes;
            match routes.get_mut(&ident) {
                Some(Route::Dead) => continue, // already dead
                Some(route) => {
                    let subs = route.monitors_mut().map(std::mem::take).unwrap_or_default();
                    to_fire.extend(subs.into_iter().map(|sub| (sub.dest, ident.clone())));
                    *route = Route::Dead;
                }
                None => {
                    routes.insert(ident.clone(), Route::Dead);
                }
            }
            newly_dead.push(ident);
        }
        for (dest, to_monitor) in to_fire {
            self.route_message(
                parent,
                dest,
                SendPayload::FireMonitor {
                    to_monitor,
                    is_timeout: false,
                },
            );
        }

        // Forward both the live and the newly-dead idents up to the parent.
        self.publish_routes_to_parent(parent, live, newly_dead);
    }

    fn publish_routes_to_parent(&mut self, actor: Key, live: Vec<Vec<u8>>, dead: Vec<Vec<u8>>) {
        if live.is_empty() && dead.is_empty() {
            return;
        }
        // A gateway is the top of its routing domain: it does not leak its
        // subtree's routes up to its (network) parent. This is what keeps actors
        // on one gateway out of another gateway's routing tables — cross-gateway
        // delivery uses direct side-channels (see route_message) instead. (Message
        // forwarding up the parent chain is unaffected; only route publication is.)
        if self.actor(actor).is_gateway() {
            return;
        }
        let actor = self.actor_mut(actor);
        let Some(parent) = actor.parent.as_mut() else {
            return;
        };
        let _ = parent.send(ConnectionCommand::PublishRoutes { live, dead });
    }

    fn publish_routes_after_established(
        &mut self,
        connection: ConnectionRef,
        local_ident: Vec<u8>,
        peer_ident: Vec<u8>,
    ) {
        match connection {
            ConnectionRef::ChildConnection { ofactor, slot } => {
                self.populate_routes(ofactor, slot, vec![peer_ident], Vec::new());
            }
            ConnectionRef::ParentConnection { ofactor } => {
                // We just gained a parent. Walk the whole routing table once: concrete
                // routes are advertised up (live as live, dead as dead — a dead route
                // must travel up *as dead*, else the parent treats a known-dead actor
                // as alive), while buffered Unknown routes — which we could not place
                // while parentless — are dropped and their held messages and monitor
                // subscriptions re-routed up (route_message / route_monitor_change now
                // forward, since there is a parent and no local route).
                let mut live = vec![local_ident];
                let mut dead = Vec::new();
                let mut kept = HashMap::new();
                for (ident, route) in std::mem::take(&mut self.actor_mut(ofactor).routes) {
                    match route {
                        Route::Connection { .. } => {
                            live.push(ident.clone());
                            kept.insert(ident, route);
                        }
                        Route::Dead => {
                            dead.push(ident.clone());
                            kept.insert(ident, route);
                        }
                        Route::Unknown { messages, monitors } => {
                            for payload in messages {
                                self.route_message(ofactor, ident.clone(), payload);
                            }
                            for sub in monitors {
                                // Forward each subscription up *with its own
                                // timeout*, so the new ancestor re-arms a fresh
                                // timer where the subscription now lives. The old
                                // ancestor's route for this target is now `None`,
                                // so its still-pending timer no-ops when it fires.
                                self.route_monitor_change(
                                    ofactor,
                                    sub.dest,
                                    ident.clone(),
                                    MonitorOp::Subscribe {
                                        timeout_ms: sub.timeout_ms,
                                    },
                                );
                            }
                        }
                    }
                }
                self.actor_mut(ofactor).routes = kept;
                self.publish_routes_to_parent(ofactor, live, dead);

                // Gaining a parent is also where gateway routes climb the next hop.
                // Re-publish every gateway tag reachable below us — and, if we are a
                // gateway, our own tag — up the new link, so the ancestry up to the
                // root can route a gateway-death broadcast back down to us. Unlike
                // actor routes, this does not stop at gateway boundaries.
                let mut tags: Vec<Vec<u8>> = self
                    .actor(ofactor)
                    .gateway_routes
                    .values()
                    .flatten()
                    .cloned()
                    .collect();
                if self.actor(ofactor).is_gateway() {
                    if let Some(own) = self
                        .actor(ofactor)
                        .name()
                        .map(|name| gateway_tag(name).to_vec())
                        .filter(|own| !own.is_empty())
                    {
                        tags.push(own);
                    }
                }
                self.publish_gateway_routes_to_parent(ofactor, tags);
            }
        }
    }

    fn run_connection_command(&mut self, connection: ConnectionRef, action: ConnectionCommand) {
        match action {
            ConnectionCommand::SendMessage {
                destination_ident,
                payload,
            } => self.route_message(connection.owning_actor(), destination_ident, payload),
            ConnectionCommand::Establish {
                role,
                ident,
                name_for_other,
                alive,
            } => {
                // The peer announced itself; finalize our side using the transport we
                // stashed when our pipe connected. On failure establish_connection
                // parks the connection carrying its failure info, so fail_connection
                // tears it down the same way a sever does.
                if let Err(reason) =
                    self.establish_connection(connection, role, ident, name_for_other, alive)
                {
                    self.fail_connection(connection, reason);
                }
            }
            ConnectionCommand::Severed { reason } => {
                // The peer (or our own death cascade) tore this connection down.
                self.fail_connection(connection, reason);
            }
            ConnectionCommand::PublishRoutes { live, dead } => {
                assert!(
                    matches!(self.connection(connection), Connection::Established { .. }),
                    "published routes should arrive on an established connection"
                );
                // Published routes always arrive over a child connection: record them
                // (live and dead) as routes to that child, forwarding both up the tree.
                let ConnectionRef::ChildConnection { ofactor, slot } = connection else {
                    panic!("published routes should arrive on a child connection");
                };
                self.populate_routes(ofactor, slot, live, dead);
            }
            ConnectionCommand::UpdateMonitorSubscription {
                listener,
                target,
                op,
            } => {
                self.route_monitor_change(connection.owning_actor(), listener, target, op);
            }
            ConnectionCommand::GatewayState { client } => {
                // Our parent handed us its gateway's client: adopt it and pass it
                // on to our own machine-local children.
                self.receive_gateway_state(connection.owning_actor(), client);
            }
            ConnectionCommand::PublishGatewayRoutes { live } => {
                // Gateway routes always arrive over a child connection (they climb
                // toward the root). Record each against that child, then forward the
                // ones we had not already recorded further up — never stopping at a
                // gateway boundary, so they accumulate all the way to the root.
                let ConnectionRef::ChildConnection { ofactor, slot } = connection else {
                    panic!("published gateway routes should arrive on a child connection");
                };
                let entry = self.actor_mut(ofactor);
                let mut forward = Vec::new();
                for tag in live {
                    // A gateway that died never returns under the same tag (a
                    // recovered host reuses its address with a fresh pseudo-port), so
                    // a tag already known dead is stale — drop it rather than
                    // resurrect a route to it. (Only a gateway holds a dead-set; a
                    // non-gateway relay never filters here.)
                    let known_dead = match &entry.gateway {
                        GatewayState::Gateway { gateway_state } => {
                            matches!(gateway_state.get(&tag), Some(GatewayMonitors::Dead))
                        }
                        GatewayState::NotAGateway => false,
                    };
                    if known_dead {
                        continue;
                    }
                    if entry
                        .gateway_routes
                        .entry(slot)
                        .or_default()
                        .insert(tag.clone())
                    {
                        forward.push(tag);
                    }
                }
                self.publish_gateway_routes_to_parent(ofactor, forward);
            }
            ConnectionCommand::GatewayDied { dead } => {
                // The direction is the connection's role: a death arriving over a
                // child connection is still climbing toward the root; one arriving
                // over the parent connection is fanning back down.
                match connection {
                    ConnectionRef::ChildConnection { ofactor, .. } => {
                        self.gateway_died(ofactor, dead, false)
                    }
                    ConnectionRef::ParentConnection { ofactor } => {
                        self.gateway_died(ofactor, dead, true)
                    }
                }
            }
        }
    }

    /// Tear `connection` down: mark it [`Connection::Failed`] and derive everything
    /// to report from the connection it replaced — the failure prefix, the peer
    /// ident, and the idents it routed. Deliver the failure (prefix, peer ident,
    /// reason), and — since a child cannot exist without its parent — tear the actor
    /// down if this was its parent connection. An already-failed connection yields
    /// nothing (a duplicate severance), so this is safe to call more than once.
    fn fail_connection(&mut self, connection: ConnectionRef, reason: Vec<u8>) {
        let Some((mut failure_prefix, peer_ident, routed_idents)) = self
            .connection_set(connection, Connection::Failed)
            .into_failure_report()
        else {
            return;
        };
        let actor = connection.owning_actor();
        failure_prefix.push(MsgPart::from_bytes(peer_ident));
        failure_prefix.push(MsgPart::from_bytes(reason.clone()));
        self.deliver_to_actor(actor, failure_prefix);
        match connection.role() {
            // The failed connection was this actor's parent: the actor cannot
            // outlive its parent, so it dies (which severs its own children).
            Role::Child => self.die_actor(actor, reason),
            // The failed connection was a child: everything that was live through it
            // is now dead. Publish exactly that — as if those idents had arrived dead
            // over the connection — which marks them dead, fires monitors, and
            // propagates up. (`slot` goes unused: there are no live idents to record.)
            Role::Parent => {
                let ConnectionRef::ChildConnection { slot, .. } = connection else {
                    unreachable!("a Role::Parent connection is a child connection");
                };
                self.populate_routes(actor, slot, Vec::new(), routed_idents.into_iter().collect());
                // Any gateways reachable only through this now-failed connection are
                // implicitly dead — including ones nested below us under a
                // non-gateway child. Begin the gateway-death propagation for them
                // (climbing toward the root, hence `only_downward: false`). A
                // gateway-route entry is always non-empty, so there is nothing to
                // guard against. (This also covers a parent actor's own death: its
                // death severs its parent link, and *that* actor's parent detects the
                // failure here, holding the aggregated gateway routes for the whole
                // lost subtree.)
                if let Some(tags) = self.actor_mut(actor).gateway_routes.remove(&slot) {
                    self.gateway_died(actor, tags.into_iter().collect(), false);
                }
            }
        }
    }

    /// Finalize a connecting connection from the peer's `Establish`. On any failure
    /// (peer dead, roles disagree, name conflict, ...) the connection is parked as
    /// `Established` carrying only its failure info — prefix and the peer ident to
    /// blame — and `Err(reason)` is returned so the caller's `fail_connection` tears
    /// it down uniformly. Routes are empty in that case (it never really came up).
    fn establish_connection(
        &mut self,
        connection: ConnectionRef,
        peer_role: Role,
        peer_name: Option<Vec<u8>>,
        requested_name: Option<Vec<u8>>,
        peer_alive: bool,
    ) -> Result<(), Vec<u8>> {
        // Take the connecting connection apart; establishing it consumes the
        // hello/failure prefixes, any commands queued before it was ready, and the
        // transport stashed when the pipe connected.
        let local_status = std::mem::replace(self.connection_mut(connection), Connection::Failed);
        let Connection::Connecting {
            transport,
            name_for_other,
            hello_prefix,
            failure_prefix,
            queued_commands,
        } = local_status
        else {
            unreachable!("peer Establish should arrive only on a connecting connection");
        };

        let actor = connection.owning_actor();
        // Peer ident to blame in a failure: the peer's own ident, else whatever we
        // named it. Resolved up front so it is available on the error path.
        let failure_peer_ident = peer_name
            .clone()
            .or_else(|| name_for_other.clone())
            .unwrap_or_default();

        // Validate and resolve the two idents we need, without mutating actor state.
        let resolved = 'resolve: {
            if !peer_alive {
                break 'resolve Err(b"peer actor died".to_vec());
            }
            if connection.role() == peer_role {
                break 'resolve Err(b"connection roles do not agree".to_vec());
            }
            if !self.connection_topology_is_valid(connection) {
                break 'resolve Err(b"invalid parent-child topology".to_vec());
            }
            // Resolve the name we will end up with (erroring on a conflict), without
            // mutating yet — the actual adopt-and-flush happens via assign_name once
            // the connection is established and deferred monitors can route.
            let local_ident = if let Some(requested_name) = requested_name {
                match self.actor(actor).name() {
                    Some(existing) if existing != requested_name.as_slice() => {
                        break 'resolve Err(b"actor name conflict".to_vec());
                    }
                    Some(existing) => existing.to_vec(),
                    None => requested_name,
                }
            } else {
                match self.actor(actor).name() {
                    Some(name) => name.to_vec(),
                    None => break 'resolve Err(b"actor has no ident".to_vec()),
                }
            };
            match peer_name.or(name_for_other) {
                Some(peer_ident) => Ok((local_ident, peer_ident)),
                None => Err(b"peer actor has no ident".to_vec()),
            }
        };

        let (local_ident, peer_ident) = match resolved {
            Ok(idents) => idents,
            Err(reason) => {
                // Park the connection carrying just its failure info so
                // fail_connection can report it from the connection like any sever.
                self.connection_set(
                    connection,
                    Connection::Established {
                        transport,
                        failure_prefix,
                        peer_ident: failure_peer_ident,
                        routed_idents: HashSet::new(),
                    },
                );
                return Err(reason);
            }
        };

        self.connection_set(
            connection,
            Connection::Established {
                transport,
                failure_prefix,
                peer_ident: peer_ident.clone(),
                routed_idents: HashSet::new(),
            },
        );
        let connection_entry = self.connection_mut(connection);
        let mut queued_commands = queued_commands;
        while let Some(command) = queued_commands.pop_front() {
            let _ = connection_entry.send(command);
        }

        // Adopt the name now that the connection is established: this also replays
        // any monitor subscriptions deferred while we were unnamed, which can now
        // route up through this freshly-established link.
        self.assign_name(actor, local_ident.clone());

        self.publish_routes_after_established(connection, local_ident.clone(), peer_ident.clone());

        let mut msg = hello_prefix;
        msg.push(MsgPart::from_bytes(local_ident));
        msg.push(MsgPart::from_bytes(peer_ident));
        self.deliver_to_actor(actor, msg);
        Ok(())
    }

    fn deliver_connect_failure(
        &mut self,
        actor: Key,
        mut request: ConnectRequest,
        reason: Vec<u8>,
    ) {
        let other_ident = request
            .name_for_other
            .as_ref()
            .map(|part| part.as_bytes().to_vec())
            .unwrap_or_default();
        request
            .failure_prefix
            .push(MsgPart::from_bytes(other_ident));
        request.failure_prefix.push(MsgPart::from_bytes(reason));
        self.deliver_to_actor(actor, request.failure_prefix);
    }

    fn die_actor(&mut self, actor: Key, reason: Vec<u8>) {
        let entry = self.actor_mut(actor);
        if !entry.alive {
            return;
        }
        entry.alive = false;

        for (_, connection) in &mut entry.children {
            connection.send(ConnectionCommand::Severed {
                reason: reason.clone(),
            });
            *connection = Connection::Failed;
        }
        if let Some(connection) = entry.parent.as_mut() {
            connection.send(ConnectionCommand::Severed { reason });
            *connection = Connection::Failed;
        }
    }

    // -- Gateway death management -----------------------------------------------

    /// Forward live gateway tags up to the parent. Unlike
    /// [`publish_routes_to_parent`](Self::publish_routes_to_parent), this does *not*
    /// stop at gateway boundaries: every gateway tag must reach the root so a death
    /// broadcast can fan back down to every gateway. At the root (no parent) there
    /// is nothing above to inform.
    fn publish_gateway_routes_to_parent(&mut self, at: Key, live: Vec<Vec<u8>>) {
        if live.is_empty() {
            return;
        }
        if let Some(parent) = self.actor_mut(at).parent.as_mut() {
            let _ = parent.send(ConnectionCommand::PublishGatewayRoutes { live });
        }
    }

    /// Propagate a gateway death through `at`. First forget any routes to the dead
    /// gateways here. Then, unless `only_downward`, forward the announcement up the
    /// parent toward the root (which is what carries it there); `only_downward`
    /// masks off that link, as does simply having no parent (the root). Once the
    /// announcement is heading down — at the root turn-around, or because it arrived
    /// from above — record the deaths (gateways only, deduplicating so repeated
    /// waves die out; non-gateways merely relay) and fan it out down every
    /// gateway-route child, so every gateway below is reached. Recording on the way
    /// down (never up) is what lets the broadcast returning from the root still
    /// reach the detector's other gateway children.
    fn gateway_died(&mut self, at: Key, dead: Vec<Vec<u8>>, only_downward: bool) {
        // Forget routes to the dead gateways: they no longer route anywhere, and a
        // child entry left empty is dropped. Idempotent — an absent tag is a no-op.
        let entry = self.actor_mut(at);
        for tags in entry.gateway_routes.values_mut() {
            for tag in &dead {
                tags.remove(tag);
            }
        }
        entry.gateway_routes.retain(|_, tags| !tags.is_empty());

        // Still climbing toward the root: forward up and stop here.
        if !only_downward {
            if let Some(parent) = self.actor_mut(at).parent.as_mut() {
                let _ = parent.send(ConnectionCommand::GatewayDied { dead });
                return;
            }
        }

        // Heading down: record the deaths, then fan out to every gateway-route child.
        // A gateway records each death in its `gateway_state` (deduplicating against
        // an already-`Dead` entry so repeated waves die out) and, at the moment of
        // transition, fires every cross-gateway monitor it held for that gateway. A
        // non-gateway holds no state and merely relays.
        let mut to_fire: Vec<(Vec<u8>, Vec<u8>)> = Vec::new(); // (listener, monitoring)
        let newly: Vec<Vec<u8>> = match &mut self.actor_mut(at).gateway {
            GatewayState::Gateway { gateway_state } => {
                let mut newly = Vec::new();
                for tag in dead {
                    if matches!(gateway_state.get(&tag), Some(GatewayMonitors::Dead)) {
                        continue; // already dead; broadcast already fanned out
                    }
                    if let Some(GatewayMonitors::Subscribed(subs)) =
                        gateway_state.insert(tag.clone(), GatewayMonitors::Dead)
                    {
                        to_fire.extend(subs.into_iter().map(|m| (m.listener, m.monitoring)));
                    }
                    newly.push(tag);
                }
                newly
            }
            GatewayState::NotAGateway => dead,
        };
        for (listener, monitoring) in to_fire {
            self.route_message(
                at,
                listener,
                SendPayload::FireMonitor {
                    to_monitor: monitoring,
                    is_timeout: false,
                },
            );
        }
        if newly.is_empty() {
            return;
        }
        let slots: Vec<ChildConnectionKey> =
            self.actor(at).gateway_routes.keys().copied().collect();
        for slot in slots {
            if let Some(connection) = self.actor_mut(at).children.get_mut(slot) {
                let _ = connection.send(ConnectionCommand::GatewayDied {
                    dead: newly.clone(),
                });
            }
        }
    }

    // -- Shared-memory gateway state --------------------------------------------

    /// Seed a gateway actor's client slot from the context's shared shm server,
    /// creating that server on the first gateway. Best-effort: if the server can't
    /// start, the gateway just streams inline.
    fn init_gateway_shm(&mut self, actor: Key) {
        if let Some(client) = self.ensure_context_shm() {
            self.set_shm_client(actor, client);
        }
    }

    /// The context's shared shm client, creating the single server on first use.
    /// All gateways in the context share it, so the slab is per serving process
    /// rather than per gateway actor. Returns `None` if the server can't start.
    fn ensure_context_shm(&mut self) -> Option<ShmClient> {
        if self.shm_server.is_none() {
            match ShmServer::new() {
                Ok(server) => {
                    *self
                        .shm_client
                        .lock()
                        .expect("shm client slot mutex poisoned") = Some(server.client());
                    self.shm_server = Some(server);
                }
                Err(err) => tracing::warn!("failed to start context shm server: {err}"),
            }
        }
        *self
            .shm_client
            .lock()
            .expect("shm client slot mutex poisoned")
    }

    /// Record `actor`'s gateway client in its slot (which its transport coroutines
    /// read).
    fn set_shm_client(&self, actor: Key, client: ShmClient) {
        *self
            .actor(actor)
            .shm_client
            .lock()
            .expect("shm client slot mutex poisoned") = Some(client);
    }

    /// This actor's current gateway client, if it has learned one.
    fn shm_client_of(&self, actor: Key) -> Option<ShmClient> {
        *self
            .actor(actor)
            .shm_client
            .lock()
            .expect("shm client slot mutex poisoned")
    }

    /// Adopt a gateway client received from our parent and pass it on to every
    /// child connection — the transport decides whether to actually forward it
    /// (quic drops it), and an as-yet-unestablished child buffers it until it
    /// connects. This catches up children that connected before the client
    /// arrived; children that connect afterwards are handled at attach time.
    /// (Re-recording the client the unix reader already set is harmless and covers
    /// the inproc path, which has no reader.)
    fn receive_gateway_state(&mut self, actor: Key, client: ShmClient) {
        self.set_shm_client(actor, client);
        let child_keys: Vec<ChildConnectionKey> = self.actor(actor).children.keys().collect();
        for slot in child_keys {
            if let Some(connection) = self.actor_mut(actor).children.get_mut(slot) {
                let _ = connection.send(ConnectionCommand::GatewayState { client });
            }
        }
    }

    // -- Side-channel delivery --------------------------------------------------

    /// Handle a message that arrived over a gateway side-channel. The owning gateway
    /// is resolved *once* from `gateway_for_actor` (see [`Self::gateway_for`]),
    /// uniformly for every action. If it cannot be resolved yet, the whole
    /// self-addressing message is held in `pending_side_channel` and replayed when a
    /// route to `gateway_for_actor` is learned (see `populate_routes`) — there is no
    /// per-action resolution or dropping. Once resolved, the action is dispatched:
    ///
    /// - `Send`: hand off to `route_message`, which delivers a live route, drops a
    ///   dead one, and buffers an `Unknown` entry.
    /// - `UpdateRemoteMonitorState`: register/cancel the monitor at the owning
    ///   gateway via the *same* `route_monitor_change` (where the target is now
    ///   same-domain, so it becomes an ordinary local `MonitorSub`); a subscribe is
    ///   then confirmed back to the listener's gateway with `AckRemoteMonitor`.
    /// - `AckRemoteMonitor`: mark the matching held `MonitorToFire` acked so its
    ///   "must exist" timer stops being able to declare the gateway dead.
    fn deliver_side_channel(&mut self, message: SideChannelMessage) {
        let Some(gw) = self.gateway_for(&message.gateway_for_actor) else {
            self.pending_side_channel
                .entry(message.gateway_for_actor.clone())
                .or_default()
                .push(message);
            return;
        };
        let SideChannelMessage {
            gateway_for_actor,
            action,
        } = message;
        match action {
            SideChannelAction::Send(payload) => self.route_message(gw, gateway_for_actor, payload),
            SideChannelAction::UpdateRemoteMonitorState { listener, op } => {
                let is_subscribe = matches!(op, MonitorOp::Subscribe { .. });
                self.route_monitor_change(gw, listener.clone(), gateway_for_actor.clone(), op);
                if is_subscribe {
                    // Confirm the registration back to the listener's gateway.
                    self.send_to_gateway(SideChannelMessage {
                        gateway_for_actor: listener,
                        action: SideChannelAction::AckRemoteMonitor {
                            monitoring: gateway_for_actor,
                        },
                    });
                }
            }
            SideChannelAction::AckRemoteMonitor { monitoring } => {
                let tag = gateway_tag(&monitoring).to_vec();
                if let GatewayState::Gateway { gateway_state } = &mut self.actor_mut(gw).gateway {
                    if let Some(GatewayMonitors::Subscribed(subs)) = gateway_state.get_mut(&tag) {
                        for m in subs.iter_mut() {
                            if m.listener == gateway_for_actor && m.monitoring == monitoring {
                                m.acked = true;
                            }
                        }
                    }
                }
            }
        }
    }

    /// The gateway in this context that owns `ident`: the first gateway that already
    /// routes or names it, else (for a target not yet created) the gateway whose own
    /// `@tag` serves `ident`'s `@tag`. An exact route/name match always wins over the
    /// tag fallback. `None` if no gateway owns it yet (the caller holds or drops the
    /// message accordingly).
    fn gateway_for(&self, ident: &[u8]) -> Option<Key> {
        if let Some(key) = self.gateways.iter().copied().find(|&key| {
            let actor = &self.actors[key];
            actor.name() == Some(ident) || actor.routes.contains_key(ident)
        }) {
            return Some(key);
        }
        let tag = gateway_tag(ident);
        if tag.is_empty() {
            return None;
        }
        self.gateways
            .iter()
            .copied()
            .find(|&key| gateway_tag(self.actors[key].name().unwrap_or_default()) == tag)
    }

    /// Ship a self-addressing side-channel message to the gateway that owns
    /// `msg.gateway_for_actor`, dialing the side-channel at that actor's `@tag`.
    /// Without a tokio runtime (the sync unit-test harness) this is a no-op; those
    /// tests drive the receive side (`deliver_side_channel`) directly.
    fn send_to_gateway(&mut self, msg: SideChannelMessage) {
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        let tag = gateway_tag(&msg.gateway_for_actor);
        if tag.is_empty() {
            tracing::warn!("side-channel message has no gateway specifier; dropping");
            return;
        }
        match std::str::from_utf8(tag) {
            Ok(tag) => {
                let tag = tag.to_owned();
                self.quic.send_to_gateway(tag, msg);
            }
            Err(_) => {
                tracing::warn!("gateway destination has non-utf8 specifier; dropping message");
            }
        }
    }

    // -- Monitors ---------------------------------------------------------------

    /// Assign `name` to `actor` (a no-op if already named) and send any target
    /// subscriptions that were deferred while it was unnamed — now they have a
    /// fire-back address. The connection that carried the name must already be
    /// established so the deferred subscriptions can route up.
    fn assign_name(&mut self, actor: Key, name: Vec<u8>) {
        let entry = self.actor_mut(actor);
        if matches!(entry.name, ActorName::Named(_)) {
            return; // already named; nothing was deferred
        }
        entry.name = ActorName::Named(name.clone());
        // Everything watched while unnamed has its subscription held back; send
        // them all now, each carrying the timeout stored on its record. An unnamed
        // actor only ever holds `Active` entries (an emptied target is removed,
        // never left `PendingUnsubscribe` — that path only exists for named
        // actors), so every one needs exactly one upstream `Subscribe`.
        let deferred: Vec<(Vec<u8>, u64)> = entry
            .monitored
            .iter()
            .filter_map(|(target, tm)| match tm {
                TargetMonitors::Active { timeout_ms, .. } => Some((target.clone(), *timeout_ms)),
                TargetMonitors::PendingUnsubscribe(_) => None,
            })
            .collect();
        for (to_monitor, timeout_ms) in deferred {
            self.route_monitor_change(
                actor,
                name.clone(),
                to_monitor,
                MonitorOp::Subscribe { timeout_ms },
            );
        }
    }

    /// Add a local monitor (`id`, `failure_prefix`) on `to_monitor`. The first
    /// monitor for a target sends one upstream `Subscribe` (deferred until named).
    /// Joining an `Active` target, or re-monitoring a `PendingUnsubscribe` one
    /// (whose subscription is still live — we abort its pending unsubscribe),
    /// reuses the existing subscription, so churn sends no extra traffic.
    fn monitor_add(
        &mut self,
        actor: Key,
        id: u64,
        to_monitor: Vec<u8>,
        failure_prefix: Vec<MsgPart>,
        timeout_ms: u64,
    ) {
        let entry = self.actor_mut(actor);
        match entry.monitored.get_mut(&to_monitor) {
            Some(TargetMonitors::Active { entries, .. }) => {
                // Only the first monitor on a target arms a timeout; later ones
                // join the existing subscription and ignore their `timeout_ms`.
                entries.insert(id, failure_prefix);
                return; // subscription already live (or deferred); nothing to send
            }
            Some(TargetMonitors::PendingUnsubscribe(_)) => {
                // Re-monitor within the grace window: cancel the pending unsubscribe
                // and reactivate. The upstream subscription was never torn down, so
                // we do not re-subscribe (and do not re-arm a timeout).
                if let Some(TargetMonitors::PendingUnsubscribe(timer)) =
                    entry.monitored.remove(&to_monitor)
                {
                    timer.abort();
                }
                entry.monitored.insert(
                    to_monitor,
                    TargetMonitors::Active {
                        entries: HashMap::from([(id, failure_prefix)]),
                        timeout_ms: 0,
                    },
                );
                return;
            }
            None => {}
        }
        // Brand-new target: record it (storing the timeout), then subscribe now if
        // named, else defer to `assign_name` (the local record still allows cancel
        // meanwhile and carries the timeout until naming).
        entry.monitored.insert(
            to_monitor.clone(),
            TargetMonitors::Active {
                entries: HashMap::from([(id, failure_prefix)]),
                timeout_ms,
            },
        );
        let dest = match &entry.name {
            ActorName::Named(name) => name.clone(),
            ActorName::Unknown { .. } => return,
        };
        self.route_monitor_change(actor, dest, to_monitor, MonitorOp::Subscribe { timeout_ms });
    }

    /// Remove local monitor `id` (found by scanning its target). Dropping the last
    /// monitor on a target does *not* unsubscribe immediately: it flips the target
    /// to `PendingUnsubscribe` and schedules a debounced unsubscribe, so churn does
    /// not hammer the hierarchy. An in-flight fire is dropped because no local
    /// monitors remain.
    fn monitor_remove(&mut self, actor: Key, id: u64) {
        // Cloned up front so the spawned timer can own it without conflicting with
        // the `entry` borrow below.
        let loop_tx = self.loop_tx.clone();
        let entry = self.actor_mut(actor);
        let Some(to_monitor) = entry
            .monitored
            .iter()
            .find(|(_, tm)| match tm {
                TargetMonitors::Active { entries, .. } => entries.contains_key(&id),
                TargetMonitors::PendingUnsubscribe(_) => false,
            })
            .map(|(target, _)| target.clone())
        else {
            return;
        };
        let now_empty = match entry.monitored.get_mut(&to_monitor) {
            Some(TargetMonitors::Active { entries, .. }) => {
                entries.remove(&id);
                entries.is_empty()
            }
            _ => return,
        };
        if !now_empty {
            return; // other monitors still watch this target
        }
        if !matches!(entry.name, ActorName::Named(_)) {
            // Never sent upstream (deferred while unnamed); just drop it locally.
            entry.monitored.remove(&to_monitor);
            return;
        }
        match tokio::runtime::Handle::try_current() {
            Ok(_) => {
                // Schedule the debounced unsubscribe and park its abort handle in
                // the entry, which now becomes `PendingUnsubscribe`.
                let target = to_monitor.clone();
                let timer = tokio::task::spawn_local(async move {
                    tokio::time::sleep(MONITOR_DEBOUNCE).await;
                    let _ = loop_tx.send(Command::UnsubscribeMonitor {
                        actor,
                        to_monitor: target,
                    });
                });
                entry.monitored.insert(
                    to_monitor,
                    TargetMonitors::PendingUnsubscribe(timer.abort_handle()),
                );
            }
            Err(_) => {
                // No runtime (unit-test harness): unsubscribe synchronously.
                entry.monitored.remove(&to_monitor);
                self.unsubscribe_now(actor, to_monitor);
            }
        }
    }

    /// Debounce timer fired. Act only if the target is still `PendingUnsubscribe`
    /// — i.e. it was not re-monitored (which would have flipped it back to
    /// `Active` and aborted this task) nor torn down by a fire. The variant itself
    /// is the truth, so there is no generation to check.
    fn unsubscribe_monitor(&mut self, actor: Key, to_monitor: Vec<u8>) {
        let Some(entry) = self.actors.get_mut(actor) else {
            return;
        };
        if !matches!(
            entry.monitored.get(&to_monitor),
            Some(TargetMonitors::PendingUnsubscribe(_))
        ) {
            return; // re-monitored (Active) or already gone
        }
        entry.monitored.remove(&to_monitor);
        self.unsubscribe_now(actor, to_monitor);
    }

    /// Send the upstream `Unsubscribe` for `to_monitor` from `actor` (named).
    fn unsubscribe_now(&mut self, actor: Key, to_monitor: Vec<u8>) {
        let dest = match &self.actor(actor).name {
            ActorName::Named(name) => name.clone(),
            ActorName::Unknown { .. } => return,
        };
        self.route_monitor_change(actor, dest, to_monitor, MonitorOp::Unsubscribe);
    }

    /// Apply a monitor subscribe/cancel, sending it to the actor responsible for
    /// `target`. (Replaces the base's `route_ancestor`.) The responsible actor is
    /// chosen by the target's domain, relative to ours:
    ///
    /// - **Root domain** (blank tag): the common ancestor that routes the target —
    ///   climbing transparently *past* gateways, up to the root. Held locally there.
    /// - **Our own domain**: the common ancestor, or — failing that — our gateway, the
    ///   top of the domain, which holds it until the target appears. Held locally.
    /// - **Another gateway's domain**: our gateway, which mirrors the monitor onto the
    ///   gateway that owns the target over a side-channel.
    ///
    /// Until we reach that actor we forward the change one hop up (see
    /// [`Self::forward_monitor_up`]).
    fn route_monitor_change(&mut self, at: Key, listener: Vec<u8>, target: Vec<u8>, op: MonitorOp) {
        let target_domain = gateway_tag(&target);
        let my_domain = gateway_tag(self.actor(at).name().unwrap_or_default());
        let routes_target = self.actor(at).routes.contains_key(target.as_slice());
        let is_gateway = self.actor(at).is_gateway();

        if target_domain.is_empty() {
            // Root domain: only the common ancestor that routes the target handles it;
            // gateways are transparent, so an unrouted target keeps climbing to the root.
            if routes_target {
                self.establish_local_monitor(at, listener, target, op);
            } else {
                self.forward_monitor_up(at, listener, target, op);
            }
        } else if target_domain == my_domain {
            // Our own domain: the common ancestor, or our gateway (the top of the
            // domain), holds it; anyone below them forwards up.
            if routes_target || is_gateway {
                self.establish_local_monitor(at, listener, target, op);
            } else {
                self.forward_monitor_up(at, listener, target, op);
            }
        } else {
            // Another gateway's domain: our gateway mirrors it onto the owning gateway;
            // anyone below the gateway forwards up to it.
            if is_gateway {
                self.mirror_monitor_to_gateway(at, listener, target, op);
            } else {
                self.forward_monitor_up(at, listener, target, op);
            }
        }
    }

    /// Forward a monitor change one hop up toward the actor responsible for `target`.
    /// At the root (no parent) there is nowhere left to climb, so we are that actor:
    /// hold the monitor locally until the target appears.
    fn forward_monitor_up(&mut self, at: Key, listener: Vec<u8>, target: Vec<u8>, op: MonitorOp) {
        if let Some(parent) = self.actor_mut(at).parent.as_mut() {
            let _ = parent.send(ConnectionCommand::UpdateMonitorSubscription {
                listener,
                target,
                op,
            });
        } else {
            self.establish_local_monitor(at, listener, target, op);
        }
    }

    /// Establish a *local* monitor on `target`'s route at the responsible actor `at`
    /// (the target is in `at`'s own domain). A subscribe fires immediately if the
    /// route is already `Dead`, else holds a `MonitorSub` (creating an `Unknown`
    /// buffer if the target is not known here yet) and arms the non-existence timer
    /// while the route is `Unknown` — on a `Connection` route the target exists, so a
    /// non-existence timeout is meaningless. An unsubscribe drops the matching
    /// `MonitorSub` (idempotent: a sub that already fired or was never registered
    /// leaves nothing to remove).
    fn establish_local_monitor(
        &mut self,
        at: Key,
        listener: Vec<u8>,
        target: Vec<u8>,
        op: MonitorOp,
    ) {
        let timeout_ms = match op {
            MonitorOp::Unsubscribe => {
                if let Some(monitors) = self
                    .actor_mut(at)
                    .routes
                    .get_mut(target.as_slice())
                    .and_then(Route::monitors_mut)
                {
                    monitors.retain(|sub| sub.dest != listener);
                }
                return;
            }
            MonitorOp::Subscribe { timeout_ms } => timeout_ms,
        };

        if matches!(
            self.actor(at).routes.get(target.as_slice()),
            Some(Route::Dead)
        ) {
            self.route_message(
                at,
                listener,
                SendPayload::FireMonitor {
                    to_monitor: target,
                    is_timeout: false,
                },
            );
            return;
        }
        self.actor_mut(at).buffer_monitor(
            target.clone(),
            MonitorSub {
                dest: listener.clone(),
                timeout_ms,
            },
        );
        if matches!(
            self.actor(at).routes.get(target.as_slice()),
            Some(Route::Unknown { .. })
        ) {
            self.arm_must_exist_timer(at, listener, target, timeout_ms);
        }
    }

    /// Mirror a cross-gateway monitor onto the gateway that owns `target`, from the
    /// subscribing gateway `at`. A subscribe records an (unacked) `MonitorToFire`
    /// against the owning gateway's tag — so a later death of that gateway fires the
    /// listener — unless the gateway is already known dead, in which case it fires
    /// straight back and holds nothing. A cancel drops the matching `MonitorToFire`,
    /// removing the tag entry if it empties so a stale timer cannot later declare the
    /// gateway dead. Either way the op is relayed to the owning gateway (which applies
    /// it as an ordinary local monitor on `target`); a subscribe additionally arms the
    /// "must exist" timer that declares the gateway dead if it never acknowledges.
    fn mirror_monitor_to_gateway(
        &mut self,
        at: Key,
        listener: Vec<u8>,
        target: Vec<u8>,
        op: MonitorOp,
    ) {
        let tag = gateway_tag(&target).to_vec();
        match op {
            MonitorOp::Subscribe { timeout_ms } => {
                let already_dead = matches!(
                    &self.actor(at).gateway,
                    GatewayState::Gateway { gateway_state }
                        if matches!(gateway_state.get(&tag), Some(GatewayMonitors::Dead))
                );
                if already_dead {
                    self.route_message(
                        at,
                        listener,
                        SendPayload::FireMonitor {
                            to_monitor: target,
                            is_timeout: false,
                        },
                    );
                    return;
                }
                let GatewayState::Gateway { gateway_state } = &mut self.actor_mut(at).gateway
                else {
                    unreachable!("a cross-gateway target is handled only at a gateway");
                };
                let GatewayMonitors::Subscribed(subs) = gateway_state
                    .entry(tag)
                    .or_insert_with(|| GatewayMonitors::Subscribed(Vec::new()))
                else {
                    unreachable!("a known-dead gateway fired above and returned");
                };
                subs.push(MonitorToFire {
                    acked: false,
                    listener: listener.clone(),
                    monitoring: target.clone(),
                });
                self.relay_to_owning_gateway(&listener, &target, op);
                self.arm_must_exist_timer(at, listener, target, timeout_ms);
            }
            MonitorOp::Unsubscribe => {
                if let GatewayState::Gateway { gateway_state } = &mut self.actor_mut(at).gateway {
                    if let Some(GatewayMonitors::Subscribed(subs)) = gateway_state.get_mut(&tag) {
                        subs.retain(|m| !(m.listener == listener && m.monitoring == target));
                        if subs.is_empty() {
                            gateway_state.remove(&tag);
                        }
                    }
                }
                self.relay_to_owning_gateway(&listener, &target, op);
            }
        }
    }

    /// Forward a monitor op to the gateway that owns `target` over a side-channel, so
    /// it (un)registers an ordinary local monitor on `target` firing back to
    /// `listener`.
    fn relay_to_owning_gateway(&mut self, listener: &[u8], target: &[u8], op: MonitorOp) {
        self.send_to_gateway(SideChannelMessage {
            gateway_for_actor: target.to_vec(),
            action: SideChannelAction::UpdateRemoteMonitorState {
                listener: listener.to_vec(),
                op,
            },
        });
    }

    /// The "must exist" timer, armed by both local and remote subscribes. After
    /// `timeout_ms` it sends a [`Command::CheckMonitorTimeout`] back to `at`. Never
    /// tracked or cancelled — correctness comes from the fire-time check, not from
    /// cancellation. Without a tokio runtime (unit-test harness) it is a no-op;
    /// those tests drive `CheckMonitorTimeout` directly.
    fn arm_must_exist_timer(&self, at: Key, listener: Vec<u8>, target: Vec<u8>, timeout_ms: u64) {
        if timeout_ms == 0 || tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        let loop_tx = self.loop_tx.clone();
        let timeout = Duration::from_millis(timeout_ms);
        tokio::task::spawn_local(async move {
            tokio::time::sleep(timeout).await;
            let _ = loop_tx.send(Command::CheckMonitorTimeout {
                at,
                listener,
                target,
            });
        });
    }

    /// A "must exist" timer armed at `at` expired. Remote and local cases are
    /// mutually exclusive (only a cross-gateway target has a `gateway_state` entry;
    /// only a same-domain one has a local route):
    /// - Remote, still unacknowledged: the owning gateway never answered the
    ///   registration, so treat it as unreachable — declare it dead, which fires the
    ///   monitor as the death propagates back (Trigger 2).
    /// - Local, never appeared: the route is still `Unknown`, so the target never
    ///   existed — fire the non-existence timeout. Any other route state (it exists,
    ///   it died, or the subscription migrated up leaving no route) is a no-op.
    fn check_monitor_timeout(&mut self, at: Key, listener: Vec<u8>, target: Vec<u8>) {
        let tag = gateway_tag(&target).to_vec();
        let unacked_remote = match self.actors.get(at).map(|actor| &actor.gateway) {
            Some(GatewayState::Gateway { gateway_state }) => matches!(
                gateway_state.get(&tag),
                Some(GatewayMonitors::Subscribed(subs))
                    if subs.iter().any(|m| m.listener == listener && m.monitoring == target && !m.acked)
            ),
            _ => false,
        };
        if unacked_remote {
            self.gateway_died(at, vec![tag], false);
            return;
        }
        let still_unknown = matches!(
            self.actors
                .get(at)
                .map(|actor| actor.routes.get(target.as_slice())),
            Some(Some(Route::Unknown { .. }))
        );
        if still_unknown {
            self.route_message(
                at,
                listener,
                SendPayload::FireMonitor {
                    to_monitor: target,
                    is_timeout: true,
                },
            );
        }
    }

    /// A monitor firing (death or non-existence timeout) reached the monitoring
    /// actor. Fan it out to *every* local monitor on that target, reconstructing
    /// each one's failure message from its own prefix and the given `reason`, then
    /// drop the whole entry (the monitors are consumed). Because the entry is
    /// removed, the monitor cannot fire twice: a later real death routes a fire to
    /// an actor that no longer has the entry → no-op. A fire for a target with no
    /// entries — already cancelled-and-debounced-away, or already fired — is
    /// dropped.
    fn fire_local_monitors(&mut self, actor: Key, to_monitor: Vec<u8>, reason: &[u8]) {
        let entries = match self.actor_mut(actor).monitored.remove(&to_monitor) {
            Some(TargetMonitors::Active { entries, .. }) => entries,
            // All monitors were cancelled (sub still live during the grace); the
            // pending unsubscribe task is now moot — drop it and fire nothing.
            Some(TargetMonitors::PendingUnsubscribe(timer)) => {
                timer.abort();
                return;
            }
            None => return,
        };
        for failure_prefix in entries.into_values() {
            let mut msg = failure_prefix;
            msg.push(MsgPart::from_bytes(to_monitor.clone()));
            msg.push(MsgPart::from_bytes(reason.to_vec()));
            self.deliver_to_actor(actor, msg);
        }
    }

    fn deliver_to_actor(&mut self, actor: Key, msg: Vec<MsgPart>) {
        let actor = self.actor_mut(actor);
        if !actor.alive {
            return;
        }
        match &mut actor.delivery {
            Delivery::NoPoller { buffered } => {
                buffered.push_back(msg);
            }
            Delivery::Poller { poller_key, index } => {
                let poller_key = *poller_key;
                let index = *index;
                let _ = actor;
                if let Some(poller) = self.pollers.get_mut(poller_key) {
                    poller.deliver(index, msg);
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct CtxHandle {
    tx: mpsc::UnboundedSender<Command>,
}

impl CtxHandle {
    pub fn new() -> anyhow::Result<Self> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let runtime_tx = tx.clone();
        let thread = thread::Builder::new()
            .name("monarch-mini".to_owned())
            .spawn(move || {
                let rt = runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                    .expect("tokio runtime should build");
                let local = tokio::task::LocalSet::new();
                // Event loop: dispatch commands until Shutdown. Shutdown is handled
                // here (not in run_command) so it can await the UNIX writer flush
                // before the runtime — and with it the writer tasks — is dropped.
                local.block_on(&rt, async move {
                    let mut ctx = Ctx::new(runtime_tx);
                    // DEBUG instrumentation: track command throughput and channel
                    // backlog so we can tell whether the single-threaded event loop
                    // is falling behind (incoming faster than handled). We log at
                    // most once per second and only while commands are flowing; a
                    // backlog that grows means the loop is saturated.
                    let mut handled_total: u64 = 0;
                    let mut handled_at_last_log: u64 = 0;
                    let mut last_log = std::time::Instant::now();
                    let log_every = std::time::Duration::from_secs(1);
                    // Verbose loop instrumentation is opt-in (MM_QUIC_DEBUG): when off
                    // we skip the counters, the UDP-stat reads, and the scheduler-lag
                    // probe entirely so normal runs pay nothing.
                    let debug = connection_debug();
                    let mut prev_udp = if debug { read_udp6_stats() } else { None };
                    // Scheduler-lag probe: sleep a fixed interval and record how much
                    // *longer* than that it took to be polled again — "how late an
                    // arbitrary future gets polled", the same starvation a reader's
                    // heartbeat timeout races against. Reported as the max per window.
                    let sched_lag_us: Option<Rc<Cell<u64>>> = debug.then(|| {
                        let cell = Rc::new(Cell::new(0u64));
                        let probe = Rc::clone(&cell);
                        tokio::task::spawn_local(async move {
                            let interval = std::time::Duration::from_millis(100);
                            loop {
                                let before = std::time::Instant::now();
                                tokio::time::sleep(interval).await;
                                let overrun = before.elapsed().saturating_sub(interval);
                                let us = overrun.as_micros() as u64;
                                if us > probe.get() {
                                    probe.set(us);
                                }
                            }
                        });
                        cell
                    });
                    while let Some(command) = rx.recv().await {
                        if let Command::Shutdown { done } = command {
                            // Wait for the socket transports to flush every pending
                            // write and stop their coroutines before the runtime (and
                            // the coroutines) is torn down.
                            ctx.unix.shutdown().await;
                            ctx.quic.shutdown().await;
                            let thread = ctx
                                .thread
                                .take()
                                .expect("context should have thread handle");
                            let _ = done.send(thread);
                            break;
                        }
                        ctx.run_command(command);
                        handled_total += 1;

                        if debug {
                            let elapsed = last_log.elapsed();
                            if elapsed >= log_every {
                                let handled = handled_total - handled_at_last_log;
                                let backlog = rx.len();
                                let sched_lag_ms = sched_lag_us
                                    .as_ref()
                                    .map_or(0.0, |c| c.replace(0) as f64 / 1000.0);
                                // One atomic write (timestamped, newline-terminated) so
                                // the line isn't spliced with other interleaved output.
                                eprint!(
                                    "{} MM_CTX loop: {:.0} cmds/s, backlog {}, \
                                     sched-lag(max) {:.0}ms, total {}\n",
                                    wall_clock_hms(),
                                    handled as f64 / elapsed.as_secs_f64(),
                                    backlog,
                                    sched_lag_ms,
                                    handled_total,
                                );
                                // UDP ingress health: delta of received datagrams and
                                // of drops (InErrors / RcvbufErrors) since the last log.
                                let now_udp = read_udp6_stats();
                                if let (Some((i0, e0, r0)), Some((i1, e1, r1))) =
                                    (prev_udp, now_udp)
                                {
                                    eprintln!(
                                        "{} MM_UDP in +{}/s, in_err +{}, rcvbuf_err +{} \
                                         (cum in_err {}, rcvbuf_err {})",
                                        wall_clock_hms(),
                                        i1.saturating_sub(i0),
                                        e1.saturating_sub(e0),
                                        r1.saturating_sub(r0),
                                        e1,
                                        r1,
                                    );
                                }
                                prev_udp = now_udp;
                                handled_at_last_log = handled_total;
                                last_log = std::time::Instant::now();
                            }
                        }
                    }
                });
            })?;

        let handle = Self { tx };
        handle.send_command(Command::Init { thread })?;
        Ok(handle)
    }

    pub(crate) fn send_command(&self, command: Command) -> anyhow::Result<()> {
        self.tx
            .send(command)
            .map_err(|_| anyhow::anyhow!("context runtime stopped"))
    }
}

// Tests live in a separate file but are declared here as a submodule of `ctx`
// (rather than a crate-level module) so they can reach `Ctx`'s private internals
// without widening its visibility.
#[cfg(test)]
#[path = "tests.rs"]
mod tests;
