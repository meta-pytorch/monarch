/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::marker::PhantomData;
use std::os::fd::OwnedFd;
use std::thread;
use std::thread::JoinHandle;

use slotmap::SlotMap;
use slotmap::new_key_type;
use tokio::runtime;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::Role;
use crate::actor::ActorEntry;
use crate::actor::Delivery;
use crate::actor::Route;
use crate::connection::ConnectRequest;
use crate::connection::Connection;
use crate::connection::ConnectionCommand;
use crate::connection::ConnectionRef;
use crate::connection::ConnectionTransport;
use crate::connection::EstablishFailure;
use crate::inproc_transport::InprocTransport;
use crate::msg::MsgPart;
use crate::poller::Delivered;
use crate::poller::PollerEntry;
use crate::transport::Transport;
use crate::unix_transport::UnixTransport;

/// Whether `url`'s scheme is one we have a transport for. Kept separate from
/// [`Ctx::transport_for`] so the scheme can be validated — before the connection
/// is attached, while the request can still report failure — without a `&mut self`
/// borrow.
fn valid_scheme(url: &str) -> bool {
    url.starts_with("inproc://") || url.starts_with("unix://")
}

new_key_type! {
    pub(crate) struct Key;
    pub(crate) struct PollerKey;
    pub(crate) struct ChildConnectionKey;
}

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
    ConnectionSentCommand {
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
    Shutdown {
        done: oneshot::Sender<JoinHandle<()>>,
    },
}

struct Ctx {
    actors: SlotMap<Key, ActorEntry>,
    pollers: SlotMap<PollerKey, PollerEntry>,
    // The transports. Each owns its own pairing/socket state and coroutines plus
    // a clone of the loop sender; the command loop only forwards serves/joins to
    // them and handles the generic TransportConnected/ConnectionSentCommand they
    // emit.
    inproc: InprocTransport,
    unix: UnixTransport,
    thread: Option<JoinHandle<()>>,
    _not_send: PhantomData<*const ()>,
}

impl Ctx {
    fn new(tx: mpsc::UnboundedSender<Command>) -> Self {
        Self {
            actors: SlotMap::with_key(),
            pollers: SlotMap::with_key(),
            inproc: InprocTransport::new(tx.clone()),
            unix: UnixTransport::new(tx),
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
                self.route_message(sender, destination_ident.as_bytes().to_vec(), parts);
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
            Command::ConnectionSentCommand { connection, action } => {
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

    fn route_message(&mut self, sender: Key, destination_ident: Vec<u8>, parts: Vec<MsgPart>) {
        let entry = self.actor(sender);
        if !entry.alive {
            return;
        }
        if entry.ident.as_deref() == Some(destination_ident.as_slice()) {
            self.deliver_to_actor(sender, parts);
            return;
        }

        // A known child route: hand the message down toward the destination.
        if let Some(Route::Connection(child_key)) = entry.routes.get(destination_ident.as_slice()) {
            let child_key = *child_key;
            let connection = self
                .actor_mut(sender)
                .children
                .get_mut(child_key)
                .expect("route child connection should exist");
            let _ = connection.send(ConnectionCommand::SendMessage {
                destination_ident,
                parts,
            });
            return;
        }

        // No known route here. Forward toward the gateway; the gateway itself (no
        // parent) buffers the message until the destination's route is published —
        // the destination may not even have been created yet. A `Route::Unknown`
        // entry only exists at a gateway, so it also lands in the buffer branch.
        if let Some(parent_connection) = self.actor_mut(sender).parent.as_mut() {
            let _ = parent_connection.send(ConnectionCommand::SendMessage {
                destination_ident,
                parts,
            });
        } else {
            self.actor_mut(sender)
                .buffer_unrouted(destination_ident, parts);
        }
    }

    /// Select the transport for a url's scheme. Only called once the scheme is
    /// known valid (see [`valid_scheme`]), so a missing transport is a bug.
    fn transport_for(&mut self, url: &str) -> &mut dyn Transport {
        if url.starts_with("inproc://") {
            &mut self.inproc
        } else if url.starts_with("unix://") {
            &mut self.unix
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
        let ident = self.actor(connection.owning_actor()).ident.clone();
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

    fn populate_routes(
        &mut self,
        parent: Key,
        child_connection: ChildConnectionKey,
        idents: Vec<Vec<u8>>,
    ) {
        for ident in &idents {
            let previous = self
                .actor_mut(parent)
                .routes
                .insert(ident.clone(), Route::Connection(child_connection));

            // If we had been buffering messages for this destination because its route
            // was unknown, re-route them now that it is known; route_message sends them
            // down the child connection we just recorded.
            if let Some(Route::Unknown(buffered)) = previous {
                for parts in buffered {
                    self.route_message(parent, ident.clone(), parts);
                }
            }
        }
        self.publish_routes_to_parent(parent, idents);
    }

    fn publish_routes_to_parent(&mut self, actor: Key, actor_idents: Vec<Vec<u8>>) {
        let actor = self.actor_mut(actor);
        let Some(parent) = actor.parent.as_mut() else {
            return;
        };
        let _ = parent.send(ConnectionCommand::PublishRoutes { actor_idents });
    }

    fn publish_routes_after_established(
        &mut self,
        connection: ConnectionRef,
        local_ident: Vec<u8>,
        peer_ident: Vec<u8>,
    ) {
        match connection {
            ConnectionRef::ChildConnection { ofactor, slot } => {
                self.populate_routes(ofactor, slot, vec![peer_ident]);
            }
            ConnectionRef::ParentConnection { ofactor } => {
                // We just gained a parent. Stop buffering for destinations we couldn't
                // place while parentless: drop those entries and re-route their held
                // messages, which now flow up to the parent (route_message forwards
                // them, since their local routes are gone).
                let buffered = self.actor_mut(ofactor).drain_buffered_routes();

                // Advertise the destinations we can already reach (concrete child
                // routes) plus our own ident.
                let mut actor_idents = self
                    .actor(ofactor)
                    .routes
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>();
                actor_idents.push(local_ident);
                self.publish_routes_to_parent(ofactor, actor_idents);

                for (destination, messages) in buffered {
                    for parts in messages {
                        self.route_message(ofactor, destination.clone(), parts);
                    }
                }
            }
        }
    }

    fn run_connection_command(&mut self, connection: ConnectionRef, action: ConnectionCommand) {
        match action {
            ConnectionCommand::SendMessage {
                destination_ident,
                parts,
            } => self.route_message(connection.owning_actor(), destination_ident, parts),
            ConnectionCommand::Establish {
                role,
                ident,
                name_for_other,
                alive,
            } => {
                // The peer announced itself; finalize our side using the transport
                // we stashed when our pipe connected.
                if let Err(EstablishFailure {
                    failure_prefix,
                    peer_ident,
                    reason,
                }) = self.establish_connection(connection, role, ident, name_for_other, alive)
                {
                    // Establishment failed (peer announced dead, roles disagree,
                    // name conflict, ...): sever the connection and notify the actor.
                    let _ = self.connection_set(connection, Connection::Failed);
                    self.fail_connection(connection, failure_prefix, peer_ident, reason);
                }
            }
            ConnectionCommand::Severed { reason } => {
                // The peer (or our own death cascade) tore this connection down. Mark
                // it failed and pull out what we need to notify the owning actor.
                let Some((failure_prefix, peer_ident)) = self
                    .connection_set(connection, Connection::Failed)
                    .into_failure_report()
                else {
                    return;
                };
                self.fail_connection(connection, failure_prefix, peer_ident, reason);
            }
            ConnectionCommand::PublishRoutes { actor_idents } => {
                assert!(
                    matches!(self.connection(connection), Connection::Established { .. }),
                    "published routes should arrive on an established connection"
                );
                // Published routes always arrive over a child connection: record them
                // as routes to that child (which also forwards them on toward the root).
                let ConnectionRef::ChildConnection { ofactor, slot } = connection else {
                    panic!("published routes should arrive on a child connection");
                };
                self.populate_routes(ofactor, slot, actor_idents);
            }
        }
    }

    /// Notify an actor that one of its connections failed: deliver the failure
    /// prefix (followed by the peer ident and the reason), and — since a child
    /// cannot exist without its parent — tear the actor down if this was its
    /// parent connection. `deliver_to_actor`/`die_actor` are no-ops on a dead
    /// actor, so this is safe to call regardless of the actor's liveness.
    fn fail_connection(
        &mut self,
        connection: ConnectionRef,
        mut failure_prefix: Vec<MsgPart>,
        peer_ident: Vec<u8>,
        reason: Vec<u8>,
    ) {
        let actor = connection.owning_actor();
        failure_prefix.push(MsgPart::from_bytes(peer_ident));
        failure_prefix.push(MsgPart::from_bytes(reason.clone()));
        self.deliver_to_actor(actor, failure_prefix);
        if connection.role() == Role::Child {
            self.die_actor(actor, reason);
        }
    }

    fn establish_connection(
        &mut self,
        connection: ConnectionRef,
        peer_role: Role,
        peer_name: Option<Vec<u8>>,
        requested_name: Option<Vec<u8>>,
        peer_alive: bool,
    ) -> Result<(), EstablishFailure> {
        // Take the connecting connection apart; establishing it consumes the
        // hello/failure prefixes, any commands queued before it was ready, and the
        // transport stashed when the pipe connected.
        let local_connection = self.connection_mut(connection);
        let local_status = std::mem::replace(local_connection, Connection::Failed);

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

        let failure_peer_ident = peer_name
            .clone()
            .or_else(|| name_for_other.clone())
            .unwrap_or_default();

        let mut failure_prefix = Some(failure_prefix);
        let result = (|| -> Result<(), Vec<u8>> {
            // The peer announced it had already died; sever instead of finalizing.
            // `failure_peer_ident` (above) still carries the peer ident it sent, so
            // the failure names which connection died.
            if !peer_alive {
                return Err(b"peer actor died".to_vec());
            }
            if connection.role() == peer_role {
                return Err(b"connection roles do not agree".to_vec());
            }
            if !self.connection_topology_is_valid(connection) {
                return Err(b"invalid parent-child topology".to_vec());
            }

            let actor = connection.owning_actor();
            let local_ident = {
                let actor_entry = self.actor_mut(actor);
                if let Some(requested_name) = requested_name {
                    match &actor_entry.ident {
                        Some(existing) if existing != &requested_name => {
                            return Err(b"actor name conflict".to_vec());
                        }
                        Some(_) => {}
                        None => actor_entry.ident = Some(requested_name),
                    }
                }
                actor_entry
                    .ident
                    .clone()
                    .ok_or_else(|| b"actor has no ident".to_vec())?
            };
            let peer_ident = peer_name
                .or(name_for_other)
                .ok_or_else(|| b"peer actor has no ident".to_vec())?;

            self.connection_set(
                connection,
                Connection::Established {
                    transport,
                    failure_prefix: failure_prefix
                        .take()
                        .expect("failure prefix should be available"),
                    peer_ident: peer_ident.clone(),
                },
            );
            let connection_entry = self.connection_mut(connection);
            let mut queued_commands = queued_commands;
            while let Some(command) = queued_commands.pop_front() {
                let _ = connection_entry.send(command);
            }

            self.publish_routes_after_established(
                connection,
                local_ident.clone(),
                peer_ident.clone(),
            );

            let mut msg = hello_prefix;
            msg.push(MsgPart::from_bytes(local_ident));
            msg.push(MsgPart::from_bytes(peer_ident));
            self.deliver_to_actor(actor, msg);
            Ok(())
        })();

        result.map_err(|reason| EstablishFailure {
            failure_prefix: failure_prefix
                .take()
                .expect("failure prefix should be available"),
            peer_ident: failure_peer_ident,
            reason,
        })
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
                            // Wait for the UNIX transport to flush every pending
                            // write to the OS and stop its coroutines before the
                            // runtime (and the coroutines) is torn down.
                            ctx.unix.shutdown().await;
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
