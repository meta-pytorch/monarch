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
        Some(Route::Unknown(_))
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
        Some(Route::Unknown(_))
    ));

    // `mover` now joins `root`. Gaining a parent drops the buffer and re-routes the
    // held message up to root, which delivers it down to `target`.
    connect(&mut ctx, root, mover, "inproc://mover");
    drain_commands(&mut ctx, &mut rx);

    assert!(ctx.actors[mover].routes.get(b"target".as_slice()).is_none());
    assert_eq!(
        buffered_strings(&ctx, target),
        vec!["target", "root", "hello-target"]
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
