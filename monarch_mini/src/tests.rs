/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fs::File;
use std::os::fd::OwnedFd;

use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::Role;
use crate::actor::ActorEntry;
use crate::actor::ActorName;
use crate::actor::Delivery;
use crate::actor::Route;
use crate::connection::ConnectRequest;
use crate::connection::Connection;
use crate::ctx::Command;
use crate::ctx::Ctx;
use crate::ctx::CtxHandle;
use crate::ctx::Key;
use crate::ctx::PollerKey;
use crate::msg::MsgPart;
use crate::poller::Delivered;

#[test]
fn context_starts_and_stops_runtime_thread() {
    let ctx = CtxHandle::new().expect("context should start");
    let (done_tx, done_rx) = oneshot::channel();
    ctx.send_command(Command::Shutdown { done: done_tx })
        .expect("shutdown should send");
    done_rx
        .blocking_recv()
        .expect("shutdown should return thread")
        .join()
        .expect("thread should join");
    drop(ctx);
}

#[test]
fn inproc_matches_join_before_serve() {
    let (mut ctx, mut rx) = test_ctx();
    let parent = actor(&mut ctx, "parent");
    let child = actor(&mut ctx, "child");

    ctx.join(child, "inproc://queue".to_owned(), request(Role::Child));
    assert!(ctx.actors[parent].children.is_empty());

    ctx.serve(parent, "inproc://queue".to_owned(), request(Role::Parent));
    drain_commands(&mut ctx, &mut rx);

    assert!(ctx.actors[child].parent.is_some());
    assert_eq!(buffered_strings(&ctx, parent), vec!["parent", "child"]);
    assert_eq!(buffered_strings(&ctx, child), vec!["child", "parent"]);
}

#[test]
fn inproc_send_queues_until_connection_establishes() {
    let (mut ctx, mut rx) = test_ctx();
    let parent = actor(&mut ctx, "parent");
    let child = actor(&mut ctx, "child");

    ctx.join(child, "inproc://queue".to_owned(), request(Role::Child));
    ctx.route_message(
        child,
        b"parent".to_vec(),
        vec![MsgPart::from_bytes(b"queued".to_vec())],
    );
    ctx.serve(parent, "inproc://queue".to_owned(), request(Role::Parent));
    drain_commands(&mut ctx, &mut rx);

    assert_eq!(
        buffered_strings(&ctx, parent),
        vec!["parent", "child", "queued"]
    );
}

#[test]
fn inproc_routes_through_ancestors_without_sibling_pollution() {
    let (mut ctx, mut rx) = test_ctx();
    let root = actor(&mut ctx, "root");
    let child = actor(&mut ctx, "child");
    let sibling = actor(&mut ctx, "sibling");
    let grandchild = actor(&mut ctx, "grandchild");

    connect(&mut ctx, root, child, "inproc://child");
    connect(&mut ctx, root, sibling, "inproc://sibling");
    connect(&mut ctx, child, grandchild, "inproc://grandchild");
    drain_commands(&mut ctx, &mut rx);

    assert!(ctx.actors[sibling].routes.is_empty());
    assert!(
        ctx.actors[child]
            .routes
            .contains_key(b"grandchild".as_slice())
    );
    assert!(
        ctx.actors[root]
            .routes
            .contains_key(b"grandchild".as_slice())
    );
}

#[test]
fn inproc_receive_loops_route_messages() {
    let ctx = CtxHandle::new().expect("context should start");
    let root = runtime_actor(&ctx, "root");
    let child = runtime_actor(&ctx, "child");
    let sibling = runtime_actor(&ctx, "sibling");
    let grandchild = runtime_actor(&ctx, "grandchild");
    let (poller, mut rx) = runtime_poller(&ctx);
    runtime_subscribe(&ctx, poller, 0, grandchild);

    runtime_connect(&ctx, root, child, "inproc://child");
    runtime_connect(&ctx, root, sibling, "inproc://sibling");
    runtime_connect(&ctx, child, grandchild, "inproc://grandchild");
    assert_eq!(recv_strings(&mut rx), vec!["grandchild", "child"]);

    ctx.send_command(Command::Send {
        sender: sibling,
        destination_ident: MsgPart::from_bytes(b"grandchild".to_vec()),
        parts: vec![MsgPart::from_bytes(b"from sibling".to_vec())],
    })
    .expect("send should enqueue");

    assert_eq!(recv_strings(&mut rx), vec!["from sibling"]);
    shutdown(ctx);
}

#[test]
fn die_breaks_inproc_connection_and_delivers_failure() {
    let ctx = CtxHandle::new().expect("context should start");
    let parent = runtime_actor(&ctx, "parent");
    let child = runtime_actor(&ctx, "child");
    let (poller, mut rx) = runtime_poller(&ctx);
    runtime_subscribe(&ctx, poller, 0, parent);

    ctx.send_command(Command::Serve {
        actor: parent,
        url: "inproc://child".to_owned(),
        request: request_with_failure(Role::Parent, "parent-failed"),
    })
    .expect("serve should enqueue");
    ctx.send_command(Command::Join {
        actor: child,
        url: "inproc://child".to_owned(),
        request: request_with_failure(Role::Child, "child-failed"),
    })
    .expect("join should enqueue");
    assert_eq!(recv_strings(&mut rx), vec!["parent", "child"]);

    ctx.send_command(Command::Die {
        actor: child,
        reason: MsgPart::from_bytes(b"done".to_vec()),
    })
    .expect("die should enqueue");

    assert_eq!(
        recv_strings(&mut rx),
        vec!["parent-failed", "child", "done"]
    );
    shutdown(ctx);
}

#[test]
fn dead_pending_connect_fails_when_matched() {
    let (mut ctx, mut rx) = test_ctx();
    let server = actor(&mut ctx, "server");
    let joiner = actor(&mut ctx, "joiner");

    ctx.serve(
        server,
        "inproc://late".to_owned(),
        request_with_failure(Role::Parent, "server-failed"),
    );
    ctx.die_actor(server, b"gone".to_vec());
    ctx.join(
        joiner,
        "inproc://late".to_owned(),
        request_with_failure(Role::Child, "joiner-failed"),
    );
    drain_commands(&mut ctx, &mut rx);

    // The dead server still announces itself — its ident, but `alive: false` — so
    // the joiner severs instead of establishing (no hello), and its failure both
    // names the dead peer ("server") and says why ("peer actor died"). No peer
    // state was inspected: the dead side reported its own ident and liveness.
    assert_eq!(
        buffered_strings(&ctx, joiner),
        vec!["joiner-failed", "server", "peer actor died"]
    );
    assert!(matches!(
        ctx.actors[joiner].parent,
        Some(Connection::Failed)
    ));
    assert!(!ctx.actors[joiner].gateway);
}

#[test]
fn message_to_unrouted_actor_buffers_at_gateway_then_flushes() {
    let (mut ctx, mut rx) = test_ctx();
    let root = actor(&mut ctx, "root");
    let child = actor(&mut ctx, "child");
    connect(&mut ctx, root, child, "inproc://child");
    drain_commands(&mut ctx, &mut rx);

    // "grandchild" has no route yet, so the message routes up to the gateway
    // (root) and is buffered there instead of being dropped.
    ctx.route_message(
        child,
        b"grandchild".to_vec(),
        vec![MsgPart::from_bytes(b"early".to_vec())],
    );
    drain_commands(&mut ctx, &mut rx);
    assert!(matches!(
        ctx.actors[root].routes.get(b"grandchild".as_slice()),
        Some(Route::Unknown { .. })
    ));

    // The grandchild now appears under the child; publishing its route up to the
    // gateway flushes the buffered message down to it (after the hello message).
    let grandchild = actor(&mut ctx, "grandchild");
    connect(&mut ctx, child, grandchild, "inproc://grandchild");
    drain_commands(&mut ctx, &mut rx);

    assert_eq!(
        buffered_strings(&ctx, grandchild),
        vec!["grandchild", "child", "early"]
    );
}

#[test]
fn buffered_messages_flush_upward_when_actor_gains_a_parent() {
    let (mut ctx, mut rx) = test_ctx();
    let root = actor(&mut ctx, "root");
    let target = actor(&mut ctx, "target");
    let mover = actor(&mut ctx, "mover");

    connect(&mut ctx, root, target, "inproc://target");
    drain_commands(&mut ctx, &mut rx);

    // `mover` sends to "target" before it has a parent, so it buffers the message
    // locally — it has no idea where "target" lives.
    ctx.run_command(Command::Send {
        sender: mover,
        destination_ident: MsgPart::from_bytes(b"target".to_vec()),
        parts: vec![MsgPart::from_bytes(b"hello-target".to_vec())],
    });
    assert!(matches!(
        ctx.actors[mover].routes.get(b"target".as_slice()),
        Some(Route::Unknown { .. })
    ));

    // `mover` now joins `root`. Gaining a parent drops the buffer and re-routes the
    // held message up to root, which delivers it down to `target`.
    connect(&mut ctx, root, mover, "inproc://mover");
    drain_commands(&mut ctx, &mut rx);

    assert!(!ctx.actors[mover].routes.contains_key(b"target".as_slice()));
    assert_eq!(
        buffered_strings(&ctx, target),
        vec!["target", "root", "hello-target"]
    );
}

#[test]
fn monitor_fires_when_sibling_dies() {
    let (mut ctx, mut rx) = test_ctx();
    let root = actor(&mut ctx, "root");
    let watcher = actor(&mut ctx, "watcher");
    let target = actor(&mut ctx, "target");
    connect(&mut ctx, root, watcher, "inproc://watcher");
    connect(&mut ctx, root, target, "inproc://target");
    drain_commands(&mut ctx, &mut rx);

    // `watcher` monitors its sibling `target`. The subscription has no route to
    // `target` locally, so it climbs to their common ancestor `root`, which holds
    // `target` in its routing table.
    monitor(&mut ctx, watcher, 0, "target", "DOWN");
    drain_commands(&mut ctx, &mut rx);
    // The subscription is held on target's route at the common ancestor.
    assert_eq!(route_monitor_count(&ctx, root, "target"), 1);

    ctx.die_actor(target, b"boom".to_vec());
    drain_commands(&mut ctx, &mut rx);

    // root sees target's connection drop, marks it dead, and fires the monitor
    // back down to watcher, which reconstructs [failure_prefix, target, reason].
    assert_eq!(
        buffered_strings(&ctx, watcher),
        vec!["watcher", "root", "DOWN", "target", "actor died"]
    );
}

#[test]
fn monitor_fires_when_parent_of_target_dies() {
    let (mut ctx, mut rx) = test_ctx();
    let root = actor(&mut ctx, "root");
    let watcher = actor(&mut ctx, "watcher");
    let mid = actor(&mut ctx, "mid");
    let target = actor(&mut ctx, "target");
    connect(&mut ctx, root, watcher, "inproc://watcher");
    connect(&mut ctx, root, mid, "inproc://mid");
    connect(&mut ctx, mid, target, "inproc://target");
    drain_commands(&mut ctx, &mut rx);

    monitor(&mut ctx, watcher, 0, "target", "DOWN");
    drain_commands(&mut ctx, &mut rx);

    // `mid` dies, not `target` directly. `target` is unreachable, but the death is
    // reported by root (mid's connection carried both "mid" and "target"), so the
    // monitor still fires. `target` itself cascades dead but never reports it.
    ctx.die_actor(mid, b"crash".to_vec());
    drain_commands(&mut ctx, &mut rx);

    assert_eq!(
        buffered_strings(&ctx, watcher),
        vec!["watcher", "root", "DOWN", "target", "actor died"]
    );
}

#[test]
fn cancelled_monitor_does_not_fire() {
    let (mut ctx, mut rx) = test_ctx();
    let root = actor(&mut ctx, "root");
    let watcher = actor(&mut ctx, "watcher");
    let target = actor(&mut ctx, "target");
    connect(&mut ctx, root, watcher, "inproc://watcher");
    connect(&mut ctx, root, target, "inproc://target");
    drain_commands(&mut ctx, &mut rx);

    monitor(&mut ctx, watcher, 0, "target", "DOWN");
    drain_commands(&mut ctx, &mut rx);
    ctx.run_command(Command::CancelMonitor {
        actor: watcher,
        id: 0,
    });
    drain_commands(&mut ctx, &mut rx);
    // The subscription was walked back off target's route on cancel.
    assert_eq!(route_monitor_count(&ctx, root, "target"), 0);

    ctx.die_actor(target, b"boom".to_vec());
    drain_commands(&mut ctx, &mut rx);

    // Only the establishment hello — no monitor failure was delivered.
    assert_eq!(buffered_strings(&ctx, watcher), vec!["watcher", "root"]);
}

#[test]
fn monitor_on_already_dead_actor_fires_immediately() {
    let (mut ctx, mut rx) = test_ctx();
    let root = actor(&mut ctx, "root");
    let watcher = actor(&mut ctx, "watcher");
    let target = actor(&mut ctx, "target");
    connect(&mut ctx, root, watcher, "inproc://watcher");
    connect(&mut ctx, root, target, "inproc://target");
    drain_commands(&mut ctx, &mut rx);

    // Target dies first; root now records it as a dead route.
    ctx.die_actor(target, b"boom".to_vec());
    drain_commands(&mut ctx, &mut rx);
    assert!(matches!(
        ctx.actors[root].routes.get(b"target".as_slice()),
        Some(Route::Dead)
    ));

    // Monitoring it now must fire right away rather than waiting forever.
    monitor(&mut ctx, watcher, 0, "target", "DOWN");
    drain_commands(&mut ctx, &mut rx);

    assert_eq!(
        buffered_strings(&ctx, watcher),
        vec!["watcher", "root", "DOWN", "target", "actor died"]
    );
}

#[test]
fn buffered_subscription_forwards_up_when_actor_gains_a_parent() {
    let (mut ctx, mut rx) = test_ctx();
    let root = actor(&mut ctx, "root");
    let mid = actor(&mut ctx, "mid");
    let watcher = actor(&mut ctx, "watcher");
    let target = actor(&mut ctx, "target");

    // `watcher` joins `mid` while `mid` is still parentless (its own gateway).
    connect(&mut ctx, mid, watcher, "inproc://buf-w");
    drain_commands(&mut ctx, &mut rx);

    // `watcher` monitors `target`, unknown anywhere yet. The subscription climbs to
    // `mid` (parentless) and is buffered there on an Unknown route — exactly like a
    // message to an unknown destination.
    monitor(&mut ctx, watcher, 0, "target", "DOWN");
    drain_commands(&mut ctx, &mut rx);
    assert!(matches!(
        ctx.actors[mid].routes.get(b"target".as_slice()),
        Some(Route::Unknown { .. })
    ));
    assert_eq!(route_monitor_count(&ctx, mid, "target"), 1);

    // `mid` now joins `root`. The buffered subscription must forward up to `root`
    // (the bug: previously it was stranded at `mid`).
    connect(&mut ctx, root, mid, "inproc://buf-m");
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(route_monitor_count(&ctx, mid, "target"), 0);
    assert_eq!(route_monitor_count(&ctx, root, "target"), 1);

    // `target` finally appears under `root`; its route flips Unknown -> Connection,
    // carrying the subscription. When it dies, the monitor fires down to `watcher`.
    connect(&mut ctx, root, target, "inproc://buf-t");
    drain_commands(&mut ctx, &mut rx);
    assert!(matches!(
        ctx.actors[root].routes.get(b"target".as_slice()),
        Some(Route::Connection { .. })
    ));

    ctx.die_actor(target, b"boom".to_vec());
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(
        buffered_strings(&ctx, watcher),
        vec!["watcher", "mid", "DOWN", "target", "actor died"]
    );
}

#[test]
fn monitor_on_unnamed_actor_waits_for_its_name() {
    let (mut ctx, mut rx) = test_ctx();
    let root = actor(&mut ctx, "root");
    let target = actor(&mut ctx, "target");
    // `watcher` is created without a name; root will name it when it joins.
    let watcher = ctx.actors.insert(ActorEntry::new(None));
    connect(&mut ctx, root, target, "inproc://un-target");
    drain_commands(&mut ctx, &mut rx);

    // Monitoring while unnamed can't address a fire-back yet, so it is deferred:
    // nothing is registered upstream at root.
    monitor(&mut ctx, watcher, 0, "target", "DOWN");
    drain_commands(&mut ctx, &mut rx);
    assert!(matches!(
        ctx.actors[watcher].name,
        ActorName::Unknown { .. }
    ));
    assert_eq!(route_monitor_count(&ctx, root, "target"), 0);

    // root adopts and names `watcher` on join; that releases the deferred monitor,
    // which now subscribes up to root.
    ctx.serve(
        root,
        "inproc://un-watcher".to_owned(),
        request_named(Role::Parent, "watcher"),
    );
    ctx.join(
        watcher,
        "inproc://un-watcher".to_owned(),
        request(Role::Child),
    );
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(route_monitor_count(&ctx, root, "target"), 1);

    ctx.die_actor(target, b"boom".to_vec());
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(
        buffered_strings(&ctx, watcher),
        vec!["watcher", "root", "DOWN", "target", "actor died"]
    );
}

#[test]
fn dead_route_is_carried_up_as_dead_when_gaining_a_parent() {
    let (mut ctx, mut rx) = test_ctx();
    let root = actor(&mut ctx, "root");
    let mid = actor(&mut ctx, "mid");
    let target = actor(&mut ctx, "target");

    // `mid` adopts `target` while still parentless (a gateway of its own subtree).
    connect(&mut ctx, mid, target, "inproc://target");
    drain_commands(&mut ctx, &mut rx);

    // `target` dies; `mid` records the dead route but has no parent to publish to.
    ctx.die_actor(target, b"boom".to_vec());
    drain_commands(&mut ctx, &mut rx);
    assert!(matches!(
        ctx.actors[mid].routes.get(b"target".as_slice()),
        Some(Route::Dead)
    ));

    // `mid` now joins `root`. Republishing its table on gaining a parent must carry
    // the dead route up *as dead* — not as a live route, and not dropped (which
    // would make `target` look like it never existed).
    connect(&mut ctx, root, mid, "inproc://mid");
    drain_commands(&mut ctx, &mut rx);
    assert!(matches!(
        ctx.actors[root].routes.get(b"target".as_slice()),
        Some(Route::Dead)
    ));

    // And a monitor reaching root therefore fires rather than waiting forever.
    let watcher = actor(&mut ctx, "watcher");
    connect(&mut ctx, root, watcher, "inproc://watcher");
    drain_commands(&mut ctx, &mut rx);
    monitor(&mut ctx, watcher, 0, "target", "DOWN");
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(
        buffered_strings(&ctx, watcher),
        vec!["watcher", "root", "DOWN", "target", "actor died"]
    );
}

#[test]
fn unix_serve_join_establishes_and_routes() {
    let ctx = CtxHandle::new().expect("context should start");
    let parent = runtime_actor(&ctx, "ux-parent");
    let child = runtime_actor(&ctx, "ux-child");
    let (parent_poller, mut parent_rx) = runtime_poller(&ctx);
    let (child_poller, mut child_rx) = runtime_poller(&ctx);
    runtime_subscribe(&ctx, parent_poller, 0, parent);
    runtime_subscribe(&ctx, child_poller, 0, child);

    let url = unix_test_url("route");
    ctx.send_command(Command::Serve {
        actor: parent,
        url: url.clone(),
        request: request(Role::Parent),
    })
    .expect("serve should enqueue");
    ctx.send_command(Command::Join {
        actor: child,
        url,
        request: request(Role::Child),
    })
    .expect("join should enqueue");

    // The handshake completes over the socket and both sides get their hello.
    assert_eq!(recv_strings(&mut parent_rx), vec!["ux-parent", "ux-child"]);
    assert_eq!(recv_strings(&mut child_rx), vec!["ux-child", "ux-parent"]);

    // A message addressed across the socket is framed and delivered intact.
    ctx.send_command(Command::Send {
        sender: parent,
        destination_ident: MsgPart::from_bytes(b"ux-child".to_vec()),
        parts: vec![MsgPart::from_bytes(b"hi-child".to_vec())],
    })
    .expect("send should enqueue");
    assert_eq!(recv_strings(&mut child_rx), vec!["hi-child"]);

    shutdown(ctx);
}

#[test]
fn unix_writer_flushes_pending_sends_before_teardown() {
    // Two contexts = two runtimes connected by a real socket, standing in for two
    // processes. A send issued just before the client tears down must reach the
    // OS before teardown completes, so the (separate) server still receives it.
    let server_ctx = CtxHandle::new().expect("server context should start");
    let client_ctx = CtxHandle::new().expect("client context should start");
    let server = runtime_actor(&server_ctx, "flush-server");
    let client = runtime_actor(&client_ctx, "flush-client");
    let (server_poller, mut server_rx) = runtime_poller(&server_ctx);
    runtime_subscribe(&server_ctx, server_poller, 0, server);

    let url = unix_test_url("flush");
    server_ctx
        .send_command(Command::Serve {
            actor: server,
            url: url.clone(),
            request: request(Role::Parent),
        })
        .expect("serve should enqueue");
    client_ctx
        .send_command(Command::Join {
            actor: client,
            url,
            request: request(Role::Child),
        })
        .expect("join should enqueue");
    assert_eq!(
        recv_strings(&mut server_rx),
        vec!["flush-server", "flush-client"]
    );

    // Enqueue a send, then immediately shut the client down. The shutdown drains
    // the writer before completing, so the message is not lost.
    client_ctx
        .send_command(Command::Send {
            sender: client,
            destination_ident: MsgPart::from_bytes(b"flush-server".to_vec()),
            parts: vec![MsgPart::from_bytes(b"last-words".to_vec())],
        })
        .expect("send should enqueue");
    shutdown(client_ctx);

    assert_eq!(recv_strings(&mut server_rx), vec!["last-words"]);
    shutdown(server_ctx);
}

#[test]
fn unix_monitor_fires_across_processes() {
    // Two processes joined by a socket. `target` lives with the server `srv`;
    // `watcher` is in the client process, joined to `srv` as a child. Monitoring
    // `target` makes a Subscribe climb the socket to `srv` (their common
    // ancestor), and `target`'s death sends a FireMonitor back down the socket —
    // exercising both control frames on the wire.
    let server_ctx = CtxHandle::new().expect("server context should start");
    let client_ctx = CtxHandle::new().expect("client context should start");
    let srv = runtime_actor(&server_ctx, "srv");
    let target = runtime_actor(&server_ctx, "target");
    let watcher = runtime_actor(&client_ctx, "watcher");
    let (srv_poller, mut srv_rx) = runtime_poller(&server_ctx);
    let (target_poller, mut target_rx) = runtime_poller(&server_ctx);
    let (watcher_poller, mut watcher_rx) = runtime_poller(&client_ctx);
    runtime_subscribe(&server_ctx, srv_poller, 0, srv);
    runtime_subscribe(&server_ctx, target_poller, 1, target);
    runtime_subscribe(&client_ctx, watcher_poller, 0, watcher);

    // srv adopts target locally (inproc) and serves the socket for the client.
    server_ctx
        .send_command(Command::Serve {
            actor: srv,
            url: "inproc://target".to_owned(),
            request: request(Role::Parent),
        })
        .expect("serve should enqueue");
    server_ctx
        .send_command(Command::Join {
            actor: target,
            url: "inproc://target".to_owned(),
            request: request(Role::Child),
        })
        .expect("join should enqueue");
    let url = unix_test_url("monitor");
    server_ctx
        .send_command(Command::Serve {
            actor: srv,
            url: url.clone(),
            request: request(Role::Parent),
        })
        .expect("serve should enqueue");
    client_ctx
        .send_command(Command::Join {
            actor: watcher,
            url,
            request: request(Role::Child),
        })
        .expect("join should enqueue");

    // Drain both hellos at srv so we know it holds routes to target and watcher
    // before the subscription arrives.
    let first = recv_strings(&mut srv_rx);
    let second = recv_strings(&mut srv_rx);
    let mut srv_peers = vec![first[1].clone(), second[1].clone()];
    srv_peers.sort();
    assert_eq!(srv_peers, vec!["target", "watcher"]);
    // The hello also carries target's route up to srv (inproc), so target is a
    // live route there before we monitor.
    assert_eq!(recv_strings(&mut target_rx), vec!["target", "srv"]);
    assert_eq!(recv_strings(&mut watcher_rx), vec!["watcher", "srv"]);

    runtime_monitor(&client_ctx, watcher, 0, "target", "DOWN");

    // The Subscribe and this probe both travel watcher→srv on the same socket in
    // order. Seeing the probe arrive at target proves srv already registered the
    // subscription, so the kill below races nothing.
    client_ctx
        .send_command(Command::Send {
            sender: watcher,
            destination_ident: MsgPart::from_bytes(b"target".to_vec()),
            parts: vec![MsgPart::from_bytes(b"probe".to_vec())],
        })
        .expect("send should enqueue");
    assert_eq!(recv_strings(&mut target_rx), vec!["probe"]);

    server_ctx
        .send_command(Command::Die {
            actor: target,
            reason: MsgPart::from_bytes(b"boom".to_vec()),
        })
        .expect("die should enqueue");

    assert_eq!(
        recv_strings(&mut watcher_rx),
        vec!["DOWN", "target", "actor died"]
    );

    shutdown(client_ctx);
    shutdown(server_ctx);
}

fn unix_test_url(name: &str) -> String {
    // The listener unlinks any stale socket before binding, so reusing a stable
    // per-process path across runs is safe.
    format!(
        "unix://{}/mm-rs-test-{}-{}.sock",
        std::env::temp_dir().display(),
        std::process::id(),
        name
    )
}

fn test_ctx() -> (Ctx, mpsc::UnboundedReceiver<Command>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (Ctx::new(tx), rx)
}

fn drain_commands(ctx: &mut Ctx, rx: &mut mpsc::UnboundedReceiver<Command>) {
    while let Ok(command) = rx.try_recv() {
        ctx.run_command(command);
    }
}

fn actor(ctx: &mut Ctx, ident: &str) -> Key {
    ctx.actors
        .insert(ActorEntry::new(Some(ident.as_bytes().to_vec())))
}

fn connect(ctx: &mut Ctx, parent: Key, child: Key, url: &str) {
    ctx.serve(parent, url.to_owned(), request(Role::Parent));
    ctx.join(child, url.to_owned(), request(Role::Child));
}

/// Number of monitor subscriptions held on `ident`'s route at `actor` (0 if the
/// route is absent or dead).
fn route_monitor_count(ctx: &Ctx, actor: Key, ident: &str) -> usize {
    match ctx.actors[actor].routes.get(ident.as_bytes()) {
        Some(Route::Connection { monitors, .. }) | Some(Route::Unknown { monitors, .. }) => {
            monitors.len()
        }
        _ => 0,
    }
}

fn monitor(ctx: &mut Ctx, actor: Key, id: u64, to_monitor: &str, failure: &str) {
    ctx.run_command(Command::Monitor {
        actor,
        id,
        to_monitor: MsgPart::from_bytes(to_monitor.as_bytes().to_vec()),
        failure_prefix: vec![MsgPart::from_bytes(failure.as_bytes().to_vec())],
    });
}

fn runtime_connect(ctx: &CtxHandle, parent: Key, child: Key, url: &str) {
    ctx.send_command(Command::Serve {
        actor: parent,
        url: url.to_owned(),
        request: request(Role::Parent),
    })
    .expect("serve should enqueue");
    ctx.send_command(Command::Join {
        actor: child,
        url: url.to_owned(),
        request: request(Role::Child),
    })
    .expect("join should enqueue");
}

fn runtime_actor(ctx: &CtxHandle, ident: &str) -> Key {
    let (done_tx, done_rx) = oneshot::channel();
    ctx.send_command(Command::CreateActor {
        ident: Some(MsgPart::from_bytes(ident.as_bytes().to_vec())),
        done: done_tx,
    })
    .expect("create actor should enqueue");
    done_rx.blocking_recv().expect("actor should be created")
}

fn runtime_poller(ctx: &CtxHandle) -> (PollerKey, mpsc::UnboundedReceiver<Delivered>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let (done_tx, done_rx) = oneshot::channel();
    let event_fd = OwnedFd::from(File::open("/dev/null").expect("dev null should open"));
    ctx.send_command(Command::CreatePoller {
        tx,
        event_fd,
        done: done_tx,
    })
    .expect("create poller should enqueue");
    (
        done_rx.blocking_recv().expect("poller should be created"),
        rx,
    )
}

fn runtime_monitor(ctx: &CtxHandle, actor: Key, id: u64, to_monitor: &str, failure: &str) {
    ctx.send_command(Command::Monitor {
        actor,
        id,
        to_monitor: MsgPart::from_bytes(to_monitor.as_bytes().to_vec()),
        failure_prefix: vec![MsgPart::from_bytes(failure.as_bytes().to_vec())],
    })
    .expect("monitor should enqueue");
}

fn runtime_subscribe(ctx: &CtxHandle, poller: PollerKey, index: usize, actor: Key) {
    let (done_tx, done_rx) = oneshot::channel();
    ctx.send_command(Command::Subscribe {
        poller,
        index,
        actor,
        done: done_tx,
    })
    .expect("subscribe should enqueue");
    done_rx
        .blocking_recv()
        .expect("subscribe should return")
        .expect("subscribe should succeed");
}

fn recv_strings(rx: &mut mpsc::UnboundedReceiver<Delivered>) -> Vec<String> {
    rx.blocking_recv()
        .expect("message should be delivered")
        .msg
        .iter()
        .map(|part| String::from_utf8(part.as_bytes().to_vec()).unwrap())
        .collect()
}

fn shutdown(ctx: CtxHandle) {
    let (done_tx, done_rx) = oneshot::channel();
    ctx.send_command(Command::Shutdown { done: done_tx })
        .expect("shutdown should enqueue");
    done_rx
        .blocking_recv()
        .expect("shutdown should return thread")
        .join()
        .expect("thread should join");
}

fn request(role: Role) -> ConnectRequest {
    ConnectRequest {
        role,
        name_for_other: None,
        hello_prefix: Vec::new(),
        failure_prefix: Vec::new(),
    }
}

fn request_with_failure(role: Role, failure: &str) -> ConnectRequest {
    ConnectRequest {
        role,
        name_for_other: None,
        hello_prefix: Vec::new(),
        failure_prefix: vec![MsgPart::from_bytes(failure.as_bytes().to_vec())],
    }
}

fn request_named(role: Role, name_for_other: &str) -> ConnectRequest {
    ConnectRequest {
        role,
        name_for_other: Some(MsgPart::from_bytes(name_for_other.as_bytes().to_vec())),
        hello_prefix: Vec::new(),
        failure_prefix: Vec::new(),
    }
}

fn buffered_strings(ctx: &Ctx, actor: Key) -> Vec<String> {
    let Delivery::NoPoller { buffered } = &ctx.actors[actor].delivery else {
        panic!("actor should not be subscribed");
    };
    buffered
        .iter()
        .flat_map(|msg| msg.iter())
        .map(|part| String::from_utf8(part.as_bytes().to_vec()).unwrap())
        .collect()
}
