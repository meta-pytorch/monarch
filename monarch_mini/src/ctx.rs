/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
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
use crate::connection::InprocConnectionTransport;
use crate::matcher::InprocMatcher;
use crate::matcher::Matcher;
use crate::msg::MsgPart;
use crate::poller::Delivered;
use crate::poller::PollerEntry;

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
    tx: mpsc::UnboundedSender<Command>,
    inproc: HashMap<String, InprocMatcher>,
    thread: Option<JoinHandle<()>>,
    _not_send: PhantomData<*const ()>,
}

impl Ctx {
    fn new(tx: mpsc::UnboundedSender<Command>) -> Self {
        Self {
            actors: SlotMap::with_key(),
            pollers: SlotMap::with_key(),
            tx,
            inproc: HashMap::new(),
            thread: None,
            _not_send: PhantomData,
        }
    }

    fn run_command(&mut self, command: Command) -> bool {
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
            Command::Die { actor, reason } => {
                let reason = reason.as_bytes().to_vec();
                self.die_actor(actor, reason);
            }
            Command::Shutdown { done } => {
                let thread = self
                    .thread
                    .take()
                    .expect("context should have thread handle");
                let _ = done.send(thread);
                return true;
            }
        }
        false
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

    fn serve(&mut self, actor: Key, url: String, request: ConnectRequest) {
        if !url.starts_with("inproc://") {
            self.deliver_connect_failure(actor, request, b"unsupported url scheme".to_vec());
            return;
        }

        let Some(pending) = self.attach_inproc_connection(actor, request) else {
            return;
        };
        let mut matcher = self.inproc.remove(&url).unwrap_or_else(Matcher::new);
        let _ = matcher.push_left(pending, |serve, join| {
            self.establish_inproc(serve, join);
        });
        self.inproc.insert(url, matcher);
    }

    fn join(&mut self, actor: Key, url: String, request: ConnectRequest) {
        if !url.starts_with("inproc://") {
            self.deliver_connect_failure(actor, request, b"unsupported url scheme".to_vec());
            return;
        }

        let Some(pending) = self.attach_inproc_connection(actor, request) else {
            return;
        };
        let mut matcher = self.inproc.remove(&url).unwrap_or_else(Matcher::new);
        let _ = matcher.push_right(pending, |serve, join| {
            self.establish_inproc(serve, join);
        });
        self.inproc.insert(url, matcher);
    }

    fn attach_inproc_connection(
        &mut self,
        actor: Key,
        request: ConnectRequest,
    ) -> Option<ConnectionRef> {
        let role = request.role;
        let connection = Connection::new_inproc(request);

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

    fn establish_inproc(&mut self, serve: ConnectionRef, join: ConnectionRef) {
        self.send_inproc_established(serve, join);
        self.send_inproc_established(join, serve);
    }

    fn send_inproc_established(&mut self, connection: ConnectionRef, peer: ConnectionRef) {
        let tx = self.tx.clone();

        // Peer liveness is not something the receiver can observe locally (a real
        // transport's peer may be remote), so report it in the Establish message and
        // let establish treat a dead peer as a sever reason. The peer's own name and
        // the name it wants to give us are only known while its connection is still
        // pending, so a torn-down peer is reported as nameless.
        let peer_alive = self.actor(peer.owning_actor()).alive;
        let (peer_name, requested_name) = match self.connection(peer) {
            Connection::Unestablished { name_for_other, .. } => (
                self.actor(peer.owning_actor()).ident.clone(),
                name_for_other.clone(),
            ),
            _ => (None, None),
        };

        self.send_connection_command(
            connection,
            ConnectionCommand::Establish {
                peer_alive,
                peer_role: peer.role(),
                peer_name,
                requested_name,
                transport: Box::new(InprocConnectionTransport { tx, peer }),
            },
        );
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

    fn send_connection_command(&mut self, connection: ConnectionRef, action: ConnectionCommand) {
        let _ = self
            .tx
            .send(Command::ConnectionSentCommand { connection, action });
    }

    fn run_connection_command(&mut self, connection: ConnectionRef, action: ConnectionCommand) {
        match action {
            ConnectionCommand::SendMessage {
                destination_ident,
                parts,
            } => self.route_message(connection.owning_actor(), destination_ident, parts),
            ConnectionCommand::Establish {
                peer_alive,
                peer_role,
                peer_name,
                requested_name,
                transport,
            } => {
                if let Err(EstablishFailure {
                    failure_prefix,
                    peer_ident,
                    reason,
                }) = self.establish_connection(
                    connection,
                    peer_alive,
                    peer_role,
                    peer_name,
                    requested_name,
                    transport,
                ) {
                    // Establishment failed (roles disagree, name conflict, dead peer,
                    // ...): sever the connection and notify the actor.
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
        peer_alive: bool,
        peer_role: Role,
        peer_name: Option<Vec<u8>>,
        requested_name: Option<Vec<u8>>,
        transport: Box<dyn ConnectionTransport>,
    ) -> Result<(), EstablishFailure> {
        // Take the unestablished connection apart; establishing it consumes the
        // hello/failure prefixes and any commands queued before it was ready.
        let local_connection = self.connection_mut(connection);
        let local_status = std::mem::replace(local_connection, Connection::Failed);

        let Connection::Unestablished {
            name_for_other,
            hello_prefix,
            failure_prefix,
            queued_commands,
        } = local_status
        else {
            unreachable!("local status should be unestablished");
        };

        let failure_peer_ident = peer_name
            .clone()
            .or_else(|| name_for_other.clone())
            .unwrap_or_default();

        let mut failure_prefix = Some(failure_prefix);
        let result = (|| -> Result<(), Vec<u8>> {
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
                    .build()
                    .expect("tokio runtime should build");
                let local = tokio::task::LocalSet::new();
                // Event loop: dispatch commands until a Shutdown command is handled.
                local.block_on(&rt, async move {
                    let mut ctx = Ctx::new(runtime_tx);
                    while let Some(command) = rx.recv().await {
                        if ctx.run_command(command) {
                            break;
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
