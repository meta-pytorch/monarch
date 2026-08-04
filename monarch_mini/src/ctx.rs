/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::os::fd::OwnedFd;
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
use crate::actor::MonitorSub;
use crate::actor::Route;
use crate::actor::TargetMonitors;
use crate::connection::AncestorPayload;
use crate::connection::ConnectRequest;
use crate::connection::Connection;
use crate::connection::ConnectionCommand;
use crate::connection::ConnectionRef;
use crate::connection::ConnectionTransport;
use crate::connection::SendPayload;
use crate::inproc_transport::InprocTransport;
use crate::msg::MsgPart;
use crate::poller::Delivered;
use crate::poller::PollerEntry;
use crate::quic_transport::QuicTransport;
use crate::transport::Transport;
use crate::unix_transport::UnixTransport;

/// Whether `url`'s scheme is one we have a transport for. Kept separate from
/// [`Ctx::transport_for`] so the scheme can be validated — before the connection
/// is attached, while the request can still report failure — without a `&mut self`
/// borrow.
fn valid_scheme(url: &str) -> bool {
    url.starts_with("inproc://") || url.starts_with("unix://") || url.starts_with("quic://")
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
    },
    CancelMonitor {
        actor: Key,
        id: u64,
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
    // A clone of the loop sender, used to schedule debounced monitor unsubscribes
    // back onto the event loop after a grace period.
    loop_tx: mpsc::UnboundedSender<Command>,
    thread: Option<JoinHandle<()>>,
    _not_send: PhantomData<*const ()>,
}

impl Ctx {
    fn new(tx: mpsc::UnboundedSender<Command>) -> Self {
        Self {
            actors: SlotMap::with_key(),
            pollers: SlotMap::with_key(),
            inproc: InprocTransport::new(tx.clone()),
            unix: UnixTransport::new(tx.clone()),
            quic: QuicTransport::new(tx.clone()),
            loop_tx: tx,
            thread: None,
            _not_send: PhantomData,
        }
    }

    fn run_command(&mut self, command: Command) {
        match command {
            Command::Init { thread } => {
                self.thread = Some(thread);
            }
            Command::CreateActor { ident, done } => {
                let key = self.actors.insert(ActorEntry::new(
                    ident.as_ref().map(|part| part.as_bytes().to_vec()),
                ));
                drop(ident);
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
            Command::Die { actor, reason } => {
                let reason = reason.as_bytes().to_vec();
                self.die_actor(actor, reason);
            }
            Command::Monitor {
                actor,
                id,
                to_monitor,
                failure_prefix,
            } => {
                let to_monitor = to_monitor.as_bytes().to_vec();
                self.monitor_add(actor, id, to_monitor, failure_prefix);
            }
            Command::CancelMonitor { actor, id } => {
                self.monitor_remove(actor, id);
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
            SendPayload::FireMonitor(to_monitor) => self.deliver_monitor_failure(actor, to_monitor),
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
        let Some(connection) = self.attach_connection(actor, request) else {
            return;
        };
        self.transport_for(&url).serve(url, connection);
    }

    fn join(&mut self, actor: Key, url: String, request: ConnectRequest) {
        if !valid_scheme(&url) {
            self.deliver_connect_failure(actor, request, b"unsupported url scheme".to_vec());
            return;
        }
        let Some(connection) = self.attach_connection(actor, request) else {
            return;
        };
        self.transport_for(&url).join(url, connection);
    }

    fn attach_connection(&mut self, actor: Key, request: ConnectRequest) -> Option<ConnectionRef> {
        let role = request.role;
        let connection = Connection::new_unestablished(request);

        let connection = match role {
            Role::Parent => {
                let actor_entry = self.actor(actor);
                if !actor_entry.gateway && actor_entry.parent.is_none() {
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
                let slot = self.actor_mut(actor).children.insert(connection);
                ConnectionRef::ChildConnection {
                    ofactor: actor,
                    slot,
                }
            }
            Role::Child => {
                let actor_entry = self.actor_mut(actor);
                // An actor may have at most one parent. It is free to have already
                // served children or buffered messages as a gateway; gaining a parent
                // hands all of that upward (see publish_routes_after_established).
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
                actor_entry.gateway = false;
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
        let actor = self.actor(connection.owning_actor());
        match connection.role() {
            Role::Parent => actor.alive && (actor.gateway || actor.parent.is_some()),
            Role::Child => actor.alive,
        }
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

            // Re-route any payloads buffered while the route was unknown; route_message
            // sends them down the child connection we just recorded.
            for payload in buffered {
                self.route_message(parent, ident.clone(), payload);
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
            self.route_message(parent, dest, SendPayload::FireMonitor(to_monitor));
        }

        // Forward both the live and the newly-dead idents up to the parent.
        self.publish_routes_to_parent(parent, live, newly_dead);
    }

    fn publish_routes_to_parent(&mut self, actor: Key, live: Vec<Vec<u8>>, dead: Vec<Vec<u8>>) {
        if live.is_empty() && dead.is_empty() {
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
                // subscriptions re-routed up (route_message / route_ancestor now
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
                                self.route_ancestor(
                                    ofactor,
                                    ident.clone(),
                                    AncestorPayload::Subscribe { dest: sub.dest },
                                );
                            }
                        }
                    }
                }
                self.actor_mut(ofactor).routes = kept;
                self.publish_routes_to_parent(ofactor, live, dead);
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
            ConnectionCommand::ToAncestor {
                to_monitor,
                payload,
            } => {
                self.route_ancestor(connection.owning_actor(), to_monitor, payload);
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
        // them all now. An unnamed actor only ever holds `Active` entries (an
        // emptied target is removed, never left `PendingUnsubscribe`), so every
        // one needs exactly one upstream `Subscribe`.
        let deferred: Vec<Vec<u8>> = entry.monitored.keys().cloned().collect();
        for to_monitor in deferred {
            self.route_ancestor(
                actor,
                to_monitor,
                AncestorPayload::Subscribe { dest: name.clone() },
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
    ) {
        let entry = self.actor_mut(actor);
        match entry.monitored.get_mut(&to_monitor) {
            Some(TargetMonitors::Active(entries)) => {
                entries.insert(id, failure_prefix);
                return; // subscription already live (or deferred); nothing to send
            }
            Some(TargetMonitors::PendingUnsubscribe(_)) => {
                // Re-monitor within the grace window: cancel the pending unsubscribe
                // and reactivate. The upstream subscription was never torn down, so
                // we do not re-subscribe.
                if let Some(TargetMonitors::PendingUnsubscribe(timer)) =
                    entry.monitored.remove(&to_monitor)
                {
                    timer.abort();
                }
                entry.monitored.insert(
                    to_monitor,
                    TargetMonitors::Active(HashMap::from([(id, failure_prefix)])),
                );
                return;
            }
            None => {}
        }
        // Brand-new target: record it, then subscribe now if named, else defer to
        // `assign_name` (the local record still allows cancel meanwhile).
        entry.monitored.insert(
            to_monitor.clone(),
            TargetMonitors::Active(HashMap::from([(id, failure_prefix)])),
        );
        let dest = match &entry.name {
            ActorName::Named(name) => name.clone(),
            ActorName::Unknown { .. } => return,
        };
        self.route_ancestor(actor, to_monitor, AncestorPayload::Subscribe { dest });
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
                TargetMonitors::Active(entries) => entries.contains_key(&id),
                TargetMonitors::PendingUnsubscribe(_) => false,
            })
            .map(|(target, _)| target.clone())
        else {
            return;
        };
        let now_empty = match entry.monitored.get_mut(&to_monitor) {
            Some(TargetMonitors::Active(entries)) => {
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
        self.route_ancestor(actor, to_monitor, AncestorPayload::Unsubscribe { dest });
    }

    /// Route a monitor operation up to the *common ancestor* of `to_monitor`: the
    /// first actor from `at` upward that holds `to_monitor` in its routing table
    /// (or the root). That ancestor is where a normal message to the target turns
    /// downward, so it is where a subscription is held and where a fire originates.
    /// Both subscribe and cancel travel this one path, differing only in the
    /// [`AncestorPayload`] handler that runs once the ancestor is reached.
    fn route_ancestor(&mut self, at: Key, to_monitor: Vec<u8>, payload: AncestorPayload) {
        // Forward up while this actor is not yet the common ancestor (it has no
        // route to the target) and a parent exists. Otherwise we are the ancestor —
        // or the root, where the target may not exist yet and the subscription is
        // held until it does — so the payload acts here.
        if !self.actor(at).routes.contains_key(to_monitor.as_slice()) {
            if let Some(parent) = self.actor_mut(at).parent.as_mut() {
                let _ = parent.send(ConnectionCommand::ToAncestor {
                    to_monitor,
                    payload,
                });
                return;
            }
        }
        match payload {
            AncestorPayload::Subscribe { dest } => self.subscribe(at, dest, to_monitor),
            AncestorPayload::Unsubscribe { dest } => self.unsubscribe(at, dest, to_monitor),
        }
    }

    /// Register a subscription at the common ancestor `at`: fire immediately if the
    /// target is already known dead, otherwise hold the subscription on its route
    /// (creating an `Unknown` buffer if the target is not yet known here).
    fn subscribe(&mut self, at: Key, dest: Vec<u8>, to_monitor: Vec<u8>) {
        if matches!(
            self.actor(at).routes.get(to_monitor.as_slice()),
            Some(Route::Dead)
        ) {
            self.route_message(at, dest, SendPayload::FireMonitor(to_monitor));
        } else {
            self.actor_mut(at)
                .buffer_monitor(to_monitor, MonitorSub { dest });
        }
    }

    /// Remove the matching subscription at the common ancestor `at`. Idempotent: a
    /// sub that already fired (and was removed), targets a now-dead route, or was
    /// never registered is a no-op.
    fn unsubscribe(&mut self, at: Key, dest: Vec<u8>, to_monitor: Vec<u8>) {
        if let Some(monitors) = self
            .actor_mut(at)
            .routes
            .get_mut(to_monitor.as_slice())
            .and_then(Route::monitors_mut)
        {
            monitors.retain(|sub| sub.dest != dest);
        }
    }

    /// A subscription fired and reached the monitoring actor. The target died, so
    /// fan the fire out to *every* local monitor on that target, reconstructing
    /// each one's failure message from its own prefix, then drop the whole entry
    /// (the target is gone). A fire for a target with no entries — already
    /// cancelled-and-debounced-away, or already fired — is dropped.
    fn deliver_monitor_failure(&mut self, actor: Key, to_monitor: Vec<u8>) {
        let entries = match self.actor_mut(actor).monitored.remove(&to_monitor) {
            Some(TargetMonitors::Active(entries)) => entries,
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
            msg.push(MsgPart::from_bytes(b"actor died".to_vec()));
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
