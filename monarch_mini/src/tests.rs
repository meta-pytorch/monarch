/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fs::File;
use std::net::SocketAddr;
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
use crate::connection::SendPayload;
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
        SendPayload::ActorMessage(vec![MsgPart::from_bytes(b"queued".to_vec())]),
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
fn gateway_rejected_joining_local_parent() {
    let (mut ctx, mut rx) = test_ctx();
    let gw = gateway_actor(&mut ctx, "gw");

    // A gateway must be the entry point for its process group, so it may not
    // gain a unix/inproc parent. The join is rejected up front with a failure
    // message and no parent connection is ever attached.
    ctx.join(
        gw,
        "inproc://nope".to_owned(),
        request_with_failure(Role::Child, "gw-failed"),
    );
    drain_commands(&mut ctx, &mut rx);

    assert!(ctx.actors[gw].parent.is_none());
    assert_eq!(
        buffered_strings(&ctx, gw),
        vec![
            "gw-failed",
            "",
            "gateway must have no parent or a network parent"
        ]
    );
}

#[test]
fn gateway_serves_local_children() {
    let (mut ctx, mut rx) = test_ctx();
    let gw = gateway_actor(&mut ctx, "gw");
    let child = actor(&mut ctx, "child");

    // Serving children (Role::Parent) is unaffected by the gateway rule — only
    // *gaining a parent* over a local link is rejected. A gateway adopts local
    // children normally.
    connect(&mut ctx, gw, child, "inproc://gw-child");
    drain_commands(&mut ctx, &mut rx);

    assert!(ctx.actors[child].parent.is_some());
    assert_eq!(buffered_strings(&ctx, gw), vec!["gw", "child"]);
    assert_eq!(buffered_strings(&ctx, child), vec!["child", "gw"]);
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
        SendPayload::ActorMessage(vec![MsgPart::from_bytes(b"early".to_vec())]),
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
fn side_channel_routes_to_owning_gateway_among_several_on_one_endpoint() {
    // Two gateways with the same endpoint tag `X` (e.g. two independent trees in
    // one serving process) share the listener. A side-channel arrival is routed by
    // its destination, so a message for c2@X reaches c2 (under gw2), not gw1.
    let (mut ctx, mut rx) = test_ctx();
    let gw1 = gateway_actor(&mut ctx, "gw1@X");
    let c1 = actor(&mut ctx, "c1@X");
    let gw2 = gateway_actor(&mut ctx, "gw2@X");
    let c2 = actor(&mut ctx, "c2@X");
    connect(&mut ctx, gw1, c1, "inproc://gw1-c1");
    connect(&mut ctx, gw2, c2, "inproc://gw2-c2");
    drain_commands(&mut ctx, &mut rx);

    ctx.deliver_side_channel(
        b"c2@X".to_vec(),
        SendPayload::ActorMessage(vec![MsgPart::from_bytes(b"two".to_vec())]),
    );
    ctx.deliver_side_channel(
        b"c1@X".to_vec(),
        SendPayload::ActorMessage(vec![MsgPart::from_bytes(b"one".to_vec())]),
    );
    drain_commands(&mut ctx, &mut rx);

    assert_eq!(buffered_strings(&ctx, c2), vec!["c2@X", "gw2@X", "two"]);
    assert_eq!(buffered_strings(&ctx, c1), vec!["c1@X", "gw1@X", "one"]);
}

#[test]
fn side_channel_message_pends_until_a_gateway_route_is_known() {
    // A side-channel message for a destination no gateway can route yet is held in
    // the context-wide pending table, then flushed once a gateway learns the route.
    let (mut ctx, mut rx) = test_ctx();
    let gw = gateway_actor(&mut ctx, "gw@X");
    let child = actor(&mut ctx, "c@X");

    ctx.deliver_side_channel(
        b"c@X".to_vec(),
        SendPayload::ActorMessage(vec![MsgPart::from_bytes(b"held".to_vec())]),
    );
    assert!(
        ctx.pending_side_channel.contains_key(b"c@X".as_slice()),
        "unroutable side-channel message should pend"
    );

    // The destination joins the gateway; its route propagates up and the pending
    // message flushes down to it.
    connect(&mut ctx, gw, child, "inproc://gw-c");
    drain_commands(&mut ctx, &mut rx);

    assert!(
        !ctx.pending_side_channel.contains_key(b"c@X".as_slice()),
        "pending entry should be released once the route is known"
    );
    assert_eq!(buffered_strings(&ctx, child), vec!["c@X", "gw@X", "held"]);
}

#[test]
fn side_channel_message_dropped_when_owning_gateway_is_dead() {
    // A dead gateway is kept in the lookup set: its routing table still shows the
    // destination was reachable through it, so a side-channel message for that
    // (now-gone) subtree is dropped rather than held pending forever.
    let (mut ctx, mut rx) = test_ctx();
    let gw = gateway_actor(&mut ctx, "gw@X");
    let child = actor(&mut ctx, "c@X");
    connect(&mut ctx, gw, child, "inproc://gw-c");
    drain_commands(&mut ctx, &mut rx);

    ctx.run_command(Command::Die {
        actor: gw,
        reason: MsgPart::from_bytes(b"gone".to_vec()),
    });

    ctx.deliver_side_channel(
        b"c@X".to_vec(),
        SendPayload::ActorMessage(vec![MsgPart::from_bytes(b"x".to_vec())]),
    );
    assert!(
        !ctx.pending_side_channel.contains_key(b"c@X".as_slice()),
        "a message for a dead gateway's subtree should be dropped, not pended"
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
    let watcher = ctx.actors.insert(ActorEntry::new(None, false));
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

// A monitor created while unnamed, then cancelled *after* the actor is named:
// naming subscribes the deferred monitor upstream, and the later cancel must
// tear that subscription back down (no orphan).
#[test]
fn deferred_monitor_cancelled_after_naming_unsubscribes() {
    let (mut ctx, mut rx) = test_ctx();
    let root = actor(&mut ctx, "root");
    let target = actor(&mut ctx, "target");
    let watcher = ctx.actors.insert(ActorEntry::new(None, false));
    connect(&mut ctx, root, target, "inproc://an-target");
    drain_commands(&mut ctx, &mut rx);

    monitor(&mut ctx, watcher, 0, "target", "DOWN"); // deferred while unnamed
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(route_monitor_count(&ctx, root, "target"), 0);

    // Name it: the deferred monitor subscribes up to root.
    ctx.serve(
        root,
        "inproc://an-watcher".to_owned(),
        request_named(Role::Parent, "watcher"),
    );
    ctx.join(
        watcher,
        "inproc://an-watcher".to_owned(),
        request(Role::Child),
    );
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(route_monitor_count(&ctx, root, "target"), 1);

    // Cancel after naming: with no runtime the debounced unsubscribe runs
    // synchronously, so the upstream subscription is gone.
    ctx.run_command(Command::CancelMonitor {
        actor: watcher,
        id: 0,
    });
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(route_monitor_count(&ctx, root, "target"), 0);

    // The target dying now delivers nothing but the establishment hello.
    ctx.die_actor(target, b"boom".to_vec());
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(buffered_strings(&ctx, watcher), vec!["watcher", "root"]);
}

// A monitor created while unnamed, then cancelled *before* the actor is named:
// the deferred record is dropped, so naming subscribes nothing upstream.
#[test]
fn deferred_monitor_cancelled_before_naming_never_subscribes() {
    let (mut ctx, mut rx) = test_ctx();
    let root = actor(&mut ctx, "root");
    let target = actor(&mut ctx, "target");
    let watcher = ctx.actors.insert(ActorEntry::new(None, false));
    connect(&mut ctx, root, target, "inproc://bn-target");
    drain_commands(&mut ctx, &mut rx);

    monitor(&mut ctx, watcher, 0, "target", "DOWN"); // deferred
    ctx.run_command(Command::CancelMonitor {
        actor: watcher,
        id: 0,
    }); // cancel while still unnamed
    drain_commands(&mut ctx, &mut rx);

    // Name it: nothing was left to subscribe.
    ctx.serve(
        root,
        "inproc://bn-watcher".to_owned(),
        request_named(Role::Parent, "watcher"),
    );
    ctx.join(
        watcher,
        "inproc://bn-watcher".to_owned(),
        request(Role::Child),
    );
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(route_monitor_count(&ctx, root, "target"), 0);

    // And the target dying delivers no failure.
    ctx.die_actor(target, b"boom".to_vec());
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(buffered_strings(&ctx, watcher), vec!["watcher", "root"]);
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

#[test]
fn unix_gateway_sends_large_message_through_shared_memory() {
    // Two contexts (= two processes) joined by a unix socket. The gateway owns a
    // slab; on establishment it hands its child the gateway state, then sends a
    // large message that is moved through the slab (not streamed) and arrives
    // intact. If the state had not propagated, the receiver could not reconstruct
    // the slab part and the connection would sever instead.
    let server_ctx = CtxHandle::new().expect("server context should start");
    let client_ctx = CtxHandle::new().expect("client context should start");
    let gw = runtime_gateway_actor(&server_ctx, "shm-gw");
    let child = runtime_actor(&client_ctx, "shm-child");
    let (child_poller, mut child_rx) = runtime_poller(&client_ctx);
    runtime_subscribe(&client_ctx, child_poller, 0, child);

    let url = unix_test_url("shm-gateway");
    server_ctx
        .send_command(Command::Serve {
            actor: gw,
            url: url.clone(),
            request: request(Role::Parent),
        })
        .expect("serve should enqueue");
    client_ctx
        .send_command(Command::Join {
            actor: child,
            url,
            request: request(Role::Child),
        })
        .expect("join should enqueue");

    assert_eq!(recv_strings(&mut child_rx), vec!["shm-child", "shm-gw"]);

    let len = crate::shm::SHM_THRESHOLD as usize + 1234;
    let payload = vec![0x41u8; len];
    server_ctx
        .send_command(Command::Send {
            sender: gw,
            destination_ident: MsgPart::from_bytes(b"shm-child".to_vec()),
            parts: vec![MsgPart::from_bytes(payload.clone())],
        })
        .expect("send should enqueue");

    let parts = recv_parts(&mut child_rx);
    assert_eq!(parts.len(), 1, "one part delivered");
    assert_eq!(parts[0], payload, "large slab payload arrives intact");

    shutdown(client_ctx);
    shutdown(server_ctx);
}

#[test]
fn unix_inproc_unix_forwards_gateway_state_past_inproc_hop() {
    // gw --unix--> a --inproc--> b --unix--> c, across three contexts. The gateway
    // state must travel down the unix edge, across the inproc edge, and out the
    // next unix edge so that the final hop (b -> c) can move a large message
    // through the slab. The message originates at gw and must arrive intact at c.
    let ctx_a = CtxHandle::new().expect("ctx a");
    let ctx_b = CtxHandle::new().expect("ctx b");
    let ctx_c = CtxHandle::new().expect("ctx c");

    let gw = runtime_gateway_actor(&ctx_a, "hop-gw");
    let a = runtime_actor(&ctx_b, "hop-a");
    let b = runtime_actor(&ctx_b, "hop-b");
    let c = runtime_actor(&ctx_c, "hop-c");
    let (c_poller, mut c_rx) = runtime_poller(&ctx_c);
    runtime_subscribe(&ctx_c, c_poller, 0, c);

    // gw --unix--> a
    let gw_url = unix_test_url("hop-gw");
    ctx_a
        .send_command(Command::Serve {
            actor: gw,
            url: gw_url.clone(),
            request: request(Role::Parent),
        })
        .expect("serve");
    ctx_b
        .send_command(Command::Join {
            actor: a,
            url: gw_url,
            request: request(Role::Child),
        })
        .expect("join");

    // a --inproc--> b (same context)
    runtime_connect(&ctx_b, a, b, "inproc://hop-ab");

    // b --unix--> c
    let bc_url = unix_test_url("hop-bc");
    ctx_b
        .send_command(Command::Serve {
            actor: b,
            url: bc_url.clone(),
            request: request(Role::Parent),
        })
        .expect("serve");
    ctx_c
        .send_command(Command::Join {
            actor: c,
            url: bc_url,
            request: request(Role::Child),
        })
        .expect("join");

    assert_eq!(recv_strings(&mut c_rx), vec!["hop-c", "hop-b"]);

    let len = crate::shm::SHM_THRESHOLD as usize + 99;
    let payload = vec![0x5Au8; len];
    ctx_a
        .send_command(Command::Send {
            sender: gw,
            destination_ident: MsgPart::from_bytes(b"hop-c".to_vec()),
            parts: vec![MsgPart::from_bytes(payload.clone())],
        })
        .expect("send");

    let parts = recv_parts(&mut c_rx);
    assert_eq!(parts.len(), 1);
    assert_eq!(
        parts[0], payload,
        "large payload survives unix->inproc->unix"
    );

    shutdown(ctx_c);
    shutdown(ctx_b);
    shutdown(ctx_a);
}

#[test]
fn quic_serve_join_establishes_and_routes() {
    set_quic_env();
    let ctx = CtxHandle::new().expect("context should start");
    let parent = runtime_actor(&ctx, "q-parent");
    let child = runtime_actor(&ctx, "q-child");
    let (parent_poller, mut parent_rx) = runtime_poller(&ctx);
    let (child_poller, mut child_rx) = runtime_poller(&ctx);
    runtime_subscribe(&ctx, parent_poller, 0, parent);
    runtime_subscribe(&ctx, child_poller, 0, child);

    let url = free_quic_url();
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

    // The QUIC handshake completes and both sides get their hello.
    assert_eq!(recv_strings(&mut parent_rx), vec!["q-parent", "q-child"]);
    assert_eq!(recv_strings(&mut child_rx), vec!["q-child", "q-parent"]);

    // A message both directions is framed over the stream and delivered intact.
    ctx.send_command(Command::Send {
        sender: parent,
        destination_ident: MsgPart::from_bytes(b"q-child".to_vec()),
        parts: vec![
            MsgPart::from_bytes(b"down".to_vec()),
            MsgPart::from_bytes(b"stream".to_vec()),
        ],
    })
    .expect("send should enqueue");
    assert_eq!(recv_strings(&mut child_rx), vec!["down", "stream"]);

    ctx.send_command(Command::Send {
        sender: child,
        destination_ident: MsgPart::from_bytes(b"q-parent".to_vec()),
        parts: vec![MsgPart::from_bytes(b"up".to_vec())],
    })
    .expect("send should enqueue");
    assert_eq!(recv_strings(&mut parent_rx), vec!["up"]);

    shutdown(ctx);
}

#[test]
fn quic_gateway_reads_large_message_into_shared_memory() {
    // A large message arriving over quic at a gateway is read straight into the
    // gateway's shared-memory slab (a gateway seeds its own client at creation,
    // before any frame can arrive), so the delivered part is a slab part — ready to
    // relay across a later unix hop by descriptor without copying into shared memory
    // again — and its bytes arrive intact. The non-gateway sender has no slab, so it
    // just streams the bytes; the receiving gateway is what lands them in the slab.
    set_quic_env();
    let ctx = CtxHandle::new().expect("context should start");
    let gw = runtime_gateway_actor(&ctx, "q-shm-gw");
    let sender = runtime_actor(&ctx, "q-shm-sender");
    let (gw_poller, mut gw_rx) = runtime_poller(&ctx);
    runtime_subscribe(&ctx, gw_poller, 0, gw);

    let url = free_quic_url();
    ctx.send_command(Command::Serve {
        actor: gw,
        url: url.clone(),
        request: request(Role::Parent),
    })
    .expect("serve should enqueue");
    ctx.send_command(Command::Join {
        actor: sender,
        url,
        request: request(Role::Child),
    })
    .expect("join should enqueue");

    // Drain the establishment hellos so the next delivery is the large message.
    assert_eq!(recv_strings(&mut gw_rx), vec!["q-shm-gw", "q-shm-sender"]);

    let len = crate::shm::SHM_THRESHOLD as usize + 4321;
    let payload = vec![0x6Cu8; len];
    ctx.send_command(Command::Send {
        sender,
        destination_ident: MsgPart::from_bytes(b"q-shm-gw".to_vec()),
        parts: vec![MsgPart::from_bytes(payload.clone())],
    })
    .expect("send should enqueue");

    let delivered = gw_rx.blocking_recv().expect("large message delivered");
    assert_eq!(delivered.msg.len(), 1, "one part delivered");
    assert!(
        delivered.msg[0].is_shm(),
        "a large part read off quic at a gateway lands in the slab"
    );
    assert_eq!(
        delivered.msg[0].as_bytes(),
        payload.as_slice(),
        "slab payload read off quic arrives intact"
    );

    shutdown(ctx);
}

#[test]
fn quic_join_before_serve() {
    // The joiner's QUIC handshake fails until the server binds, so the connector
    // retries — join may go first.
    set_quic_env();
    let ctx = CtxHandle::new().expect("context should start");
    let parent = runtime_actor(&ctx, "q-late-parent");
    let child = runtime_actor(&ctx, "q-late-child");
    let (parent_poller, mut parent_rx) = runtime_poller(&ctx);
    let (child_poller, mut child_rx) = runtime_poller(&ctx);
    runtime_subscribe(&ctx, parent_poller, 0, parent);
    runtime_subscribe(&ctx, child_poller, 0, child);

    let url = free_quic_url();
    ctx.send_command(Command::Join {
        actor: child,
        url: url.clone(),
        request: request(Role::Child),
    })
    .expect("join should enqueue");
    std::thread::sleep(std::time::Duration::from_millis(50)); // let the connector spin
    ctx.send_command(Command::Serve {
        actor: parent,
        url,
        request: request(Role::Parent),
    })
    .expect("serve should enqueue");

    assert_eq!(
        recv_strings(&mut parent_rx),
        vec!["q-late-parent", "q-late-child"]
    );
    assert_eq!(
        recv_strings(&mut child_rx),
        vec!["q-late-child", "q-late-parent"]
    );

    shutdown(ctx);
}

#[test]
fn quic_gateway_to_gateway_bypasses_root() {
    // Two QUIC gateways (gwA, gwB), each its own process/context, both joined to a
    // shared root gateway. A message from a1@A to b1@B must reach b1 by a *direct*
    // gateway-to-gateway side-channel (A dials B), bypassing root entirely — proven
    // by root receiving only the join hellos and never the cross-gateway traffic.
    set_quic_env();
    let root_ctx = CtxHandle::new().expect("root ctx");
    let a_ctx = CtxHandle::new().expect("a ctx");
    let b_ctx = CtxHandle::new().expect("b ctx");

    let root_url = free_quic_url();
    let a_url = free_quic_url();
    let b_url = free_quic_url();
    let a_tag = quic_authority(&a_url);
    let b_tag = quic_authority(&b_url);

    let root = runtime_gateway_actor(&root_ctx, "root");
    let gw_a = runtime_gateway_actor(&a_ctx, &format!("gwA@{a_tag}"));
    let a1 = runtime_actor(&a_ctx, &format!("a1@{a_tag}"));
    let gw_b = runtime_gateway_actor(&b_ctx, &format!("gwB@{b_tag}"));
    let b1 = runtime_actor(&b_ctx, &format!("b1@{b_tag}"));

    let (root_poller, mut root_rx) = runtime_poller(&root_ctx);
    let (a1_poller, mut a1_rx) = runtime_poller(&a_ctx);
    let (b1_poller, mut b1_rx) = runtime_poller(&b_ctx);
    runtime_subscribe(&root_ctx, root_poller, 0, root);
    runtime_subscribe(&a_ctx, a1_poller, 0, a1);
    runtime_subscribe(&b_ctx, b1_poller, 0, b1);

    // root is the shared rendezvous: each gateway joins it as a child, so root
    // serves once per gateway.
    runtime_serve(&root_ctx, root, &root_url);
    runtime_serve(&root_ctx, root, &root_url);
    runtime_join(&a_ctx, gw_a, &root_url);
    runtime_join(&b_ctx, gw_b, &root_url);

    // Each gateway serves its own address so a sibling gateway can side-channel to
    // it; local children join their gateway over inproc.
    runtime_serve(&a_ctx, gw_a, &a_url);
    runtime_serve(&b_ctx, gw_b, &b_url);
    runtime_connect(&a_ctx, gw_a, a1, "inproc://bypass-a");
    runtime_connect(&b_ctx, gw_b, b1, "inproc://bypass-b");

    // Drain the local-child hellos (so b1 is routable before we send).
    assert_eq!(
        recv_strings(&mut a1_rx),
        vec![format!("a1@{a_tag}"), format!("gwA@{a_tag}")]
    );
    assert_eq!(
        recv_strings(&mut b1_rx),
        vec![format!("b1@{b_tag}"), format!("gwB@{b_tag}")]
    );

    // root's only deliveries are the two gateway-join hellos (in either order).
    let mut peers: Vec<String> = (0..2)
        .map(|_| {
            let hello = recv_strings(&mut root_rx);
            assert_eq!(hello[0], "root");
            hello[1].clone()
        })
        .collect();
    peers.sort();
    assert_eq!(peers, vec![format!("gwA@{a_tag}"), format!("gwB@{b_tag}")]);

    // a1@A -> b1@B: gwA opens a direct side-channel to gwB, bypassing root.
    a_ctx
        .send_command(Command::Send {
            sender: a1,
            destination_ident: MsgPart::from_bytes(format!("b1@{b_tag}").into_bytes()),
            parts: vec![MsgPart::from_bytes(b"cross-gateway".to_vec())],
        })
        .expect("send should enqueue");
    assert_eq!(recv_strings(&mut b1_rx), vec!["cross-gateway"]);

    // The reply path b1@B -> a1@A side-channels back the other way.
    b_ctx
        .send_command(Command::Send {
            sender: b1,
            destination_ident: MsgPart::from_bytes(format!("a1@{a_tag}").into_bytes()),
            parts: vec![MsgPart::from_bytes(b"reply".to_vec())],
        })
        .expect("send should enqueue");
    assert_eq!(recv_strings(&mut a1_rx), vec!["reply"]);

    // Neither message touched root: after both arrived, root has nothing pending.
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(
        root_rx.try_recv().is_err(),
        "root must not receive cross-gateway traffic"
    );

    shutdown(a_ctx);
    shutdown(b_ctx);
    shutdown(root_ctx);
}

#[test]
fn quic_gateway_reaches_root_actor_and_its_inproc_child() {
    // From a child of gateway A, a message to the root domain (an empty specifier)
    // climbs A's parent link to root rather than side-channelling: it reaches both
    // the root actor itself and an inproc child of root.
    set_quic_env();
    let root_ctx = CtxHandle::new().expect("root ctx");
    let a_ctx = CtxHandle::new().expect("a ctx");

    let root_url = free_quic_url();
    // gwA's domain tag; gwA need not serve it here (no side-channels are used).
    let a_tag = quic_authority(&free_quic_url());

    let root = runtime_gateway_actor(&root_ctx, "root");
    let rootchild = runtime_actor(&root_ctx, "rootchild");
    let gw_a = runtime_gateway_actor(&a_ctx, &format!("gwA@{a_tag}"));
    let a1 = runtime_actor(&a_ctx, &format!("a1@{a_tag}"));

    let (root_poller, mut root_rx) = runtime_poller(&root_ctx);
    let (rc_poller, mut rc_rx) = runtime_poller(&root_ctx);
    let (a1_poller, mut a1_rx) = runtime_poller(&a_ctx);
    runtime_subscribe(&root_ctx, root_poller, 0, root);
    runtime_subscribe(&root_ctx, rc_poller, 1, rootchild);
    runtime_subscribe(&a_ctx, a1_poller, 0, a1);

    runtime_serve(&root_ctx, root, &root_url);
    runtime_join(&a_ctx, gw_a, &root_url);
    runtime_connect(&root_ctx, root, rootchild, "inproc://reach-rc");
    runtime_connect(&a_ctx, gw_a, a1, "inproc://reach-a");

    // Drain establishment hellos.
    assert_eq!(recv_strings(&mut rc_rx), vec!["rootchild", "root"]);
    assert_eq!(
        recv_strings(&mut a1_rx),
        vec![format!("a1@{a_tag}"), format!("gwA@{a_tag}")]
    );
    let h1 = recv_strings(&mut root_rx);
    let h2 = recv_strings(&mut root_rx);
    assert_eq!(h1[0], "root");
    assert_eq!(h2[0], "root");
    let mut peers = vec![h1[1].clone(), h2[1].clone()];
    peers.sort();
    assert_eq!(peers, vec![format!("gwA@{a_tag}"), "rootchild".to_string()]);

    // a1@A -> root: empty specifier climbs gwA's parent link to the root actor.
    a_ctx
        .send_command(Command::Send {
            sender: a1,
            destination_ident: MsgPart::from_bytes(b"root".to_vec()),
            parts: vec![MsgPart::from_bytes(b"hi-root".to_vec())],
        })
        .expect("send should enqueue");
    assert_eq!(recv_strings(&mut root_rx), vec!["hi-root"]);

    // a1@A -> rootchild: up to root, then down its inproc child link.
    a_ctx
        .send_command(Command::Send {
            sender: a1,
            destination_ident: MsgPart::from_bytes(b"rootchild".to_vec()),
            parts: vec![MsgPart::from_bytes(b"hi-rc".to_vec())],
        })
        .expect("send should enqueue");
    assert_eq!(recv_strings(&mut rc_rx), vec!["hi-rc"]);

    shutdown(a_ctx);
    shutdown(root_ctx);
}

#[test]
fn quic_heartbeat_timeout_severs_connection() {
    // A peer that holds the QUIC connection open but never sends anything (no
    // Establish, no heartbeats) can't be detected by EOF — only the heartbeat
    // timeout catches it. A raw silent quinn server stands in for such a peer; the
    // joining actor must receive its failure message once the heartbeat lapses.
    set_quic_env();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    // Bind the silent server first so we know its concrete port.
    let port = spawn_silent_quic_server(addr);
    let url = format!("quic://127.0.0.1:{port}");

    let ctx = CtxHandle::new().expect("context should start");
    let client = runtime_actor(&ctx, "q-silent-client");
    let (poller, mut rx) = runtime_poller(&ctx);
    runtime_subscribe(&ctx, poller, 0, client);

    ctx.send_command(Command::Join {
        actor: client,
        url,
        request: request_with_failure(Role::Child, "client-failed"),
    })
    .expect("join should enqueue");

    // No Establish ever arrives, so the peer ident is empty; the heartbeat timeout
    // is what severs the connection and delivers the failure.
    assert_eq!(
        recv_strings(&mut rx),
        vec!["client-failed", "", "quic heartbeat timeout"]
    );

    shutdown(ctx);
}

/// Bind a raw quinn server on an ephemeral port using the test cert, accept one
/// connection and hold it open (never sending a frame). Returns the bound port.
/// The server thread is detached; it exits on its own after a grace period.
fn spawn_silent_quic_server(addr: SocketAddr) -> u16 {
    use std::io::BufReader;

    let (port_tx, port_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("silent server runtime");
        rt.block_on(async move {
            let _ = quinn::rustls::crypto::ring::default_provider().install_default();
            let dir = cert_dir();
            let cert_data = std::fs::read(dir.join("cert.pem")).expect("read cert");
            let key_data = std::fs::read(dir.join("key.pem")).expect("read key");
            let certs = rustls_pemfile::certs(&mut BufReader::new(&cert_data[..]))
                .collect::<Result<Vec<_>, _>>()
                .expect("parse cert");
            let key = rustls_pemfile::private_key(&mut BufReader::new(&key_data[..]))
                .expect("parse key")
                .expect("a private key");
            let server_config =
                quinn::ServerConfig::with_single_cert(certs, key).expect("server config");
            let endpoint = quinn::Endpoint::server(server_config, addr).expect("bind quic server");
            port_tx
                .send(endpoint.local_addr().expect("local addr").port())
                .expect("report port");

            // Accept one connection and hold it open without ever replying.
            let mut held = Vec::new();
            if let Some(incoming) = endpoint.accept().await {
                if let Ok(connecting) = incoming.accept() {
                    if let Ok(conn) = connecting.await {
                        held.push(conn);
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            drop(held);
        });
    });
    port_rx
        .recv()
        .expect("silent server should report its port")
}

fn cert_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("test_certs")
}

fn set_quic_env() {
    // All quic tests use the same fixture certs, so setting the same values from
    // multiple test threads is benign.
    let dir = cert_dir();
    std::env::set_var("MM_QUIC_CERT", dir.join("cert.pem"));
    std::env::set_var("MM_QUIC_KEY", dir.join("key.pem"));
    std::env::set_var("MM_QUIC_CA", dir.join("ca.pem"));
}

fn free_quic_url() -> String {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind ephemeral udp");
    let port = socket.local_addr().expect("local addr").port();
    drop(socket);
    format!("quic://127.0.0.1:{port}")
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
    // Test actors default to non-gateways: most join a local parent, which a
    // gateway is forbidden from doing. Tests that need a gateway construct it
    // explicitly with `gateway_actor`.
    ctx.insert_actor(Some(ident.as_bytes().to_vec()), false)
}

fn gateway_actor(ctx: &mut Ctx, ident: &str) -> Key {
    ctx.insert_actor(Some(ident.as_bytes().to_vec()), true)
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

/// Serve `url` from `actor` as a parent (the listening side). Used to set up a
/// gateway's own QUIC endpoint or a shared rendezvous across contexts.
fn runtime_serve(ctx: &CtxHandle, actor: Key, url: &str) {
    ctx.send_command(Command::Serve {
        actor,
        url: url.to_owned(),
        request: request(Role::Parent),
    })
    .expect("serve should enqueue");
}

/// Join `url` from `actor` as a child (the connecting side).
fn runtime_join(ctx: &CtxHandle, actor: Key, url: &str) {
    ctx.send_command(Command::Join {
        actor,
        url: url.to_owned(),
        request: request(Role::Child),
    })
    .expect("join should enqueue");
}

/// The authority of a `quic://host:port` url — the gateway specifier tag that goes
/// in the `@suffix` of idents owned by the gateway serving that url.
fn quic_authority(url: &str) -> String {
    url.strip_prefix("quic://").unwrap_or(url).to_owned()
}

fn runtime_actor(ctx: &CtxHandle, ident: &str) -> Key {
    runtime_actor_with_gateway(ctx, ident, false)
}

fn runtime_gateway_actor(ctx: &CtxHandle, ident: &str) -> Key {
    runtime_actor_with_gateway(ctx, ident, true)
}

fn runtime_actor_with_gateway(ctx: &CtxHandle, ident: &str, gateway: bool) -> Key {
    let (done_tx, done_rx) = oneshot::channel();
    ctx.send_command(Command::CreateActor {
        ident: Some(MsgPart::from_bytes(ident.as_bytes().to_vec())),
        gateway,
        done: done_tx,
    })
    .expect("create actor should enqueue");
    done_rx.blocking_recv().expect("actor should be created")
}

/// Block for the next delivered message, returning each part's bytes (mapping any
/// shared-memory parts in the process).
fn recv_parts(rx: &mut mpsc::UnboundedReceiver<Delivered>) -> Vec<Vec<u8>> {
    rx.blocking_recv()
        .expect("message should be delivered")
        .msg
        .iter()
        .map(|part| part.as_bytes().to_vec())
        .collect()
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
