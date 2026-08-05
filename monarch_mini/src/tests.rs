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
use crate::actor::GatewayMonitors;
use crate::actor::GatewayState;
use crate::actor::Route;
use crate::connection::ConnectRequest;
use crate::connection::Connection;
use crate::connection::ConnectionCommand;
use crate::connection::ConnectionRef;
use crate::connection::MonitorOp;
use crate::connection::SendPayload;
use crate::connection::SideChannelAction;
use crate::connection::SideChannelMessage;
use crate::ctx::ChildConnectionKey;
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
    assert!(!ctx.actors[joiner].is_gateway());
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

    side_channel_send(&mut ctx, "c2@X", b"two");
    side_channel_send(&mut ctx, "c1@X", b"one");
    drain_commands(&mut ctx, &mut rx);

    assert_eq!(buffered_strings(&ctx, c2), vec!["c2@X", "gw2@X", "two"]);
    assert_eq!(buffered_strings(&ctx, c1), vec!["c1@X", "gw1@X", "one"]);
}

#[test]
fn side_channel_message_pends_until_a_gateway_route_is_known() {
    // A side-channel message that arrives before any gateway for its destination's
    // tag even exists cannot be resolved, so it is held in the context-wide pending
    // table, then flushed once a gateway learns the route.
    let (mut ctx, mut rx) = test_ctx();

    side_channel_send(&mut ctx, "c@X", b"held");
    assert!(
        ctx.pending_side_channel.contains_key(b"c@X".as_slice()),
        "unroutable side-channel message should pend"
    );

    // The gateway and destination appear; the destination's route propagates up and
    // the pending message flushes down to it.
    let gw = gateway_actor(&mut ctx, "gw@X");
    let child = actor(&mut ctx, "c@X");
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

    side_channel_send(&mut ctx, "c@X", b"x");
    assert!(
        !ctx.pending_side_channel.contains_key(b"c@X".as_slice()),
        "a message for a dead gateway's subtree should be dropped, not pended"
    );
}

/// The single child connection key of `actor` (the harness only ever gives a test
/// actor one child where these helpers are used).
fn only_child_slot(ctx: &Ctx, actor: Key) -> ChildConnectionKey {
    ctx.actors[actor]
        .children
        .keys()
        .next()
        .expect("actor should have a child connection")
}

/// Simulate a `PublishGatewayRoutes` arriving over `actor`'s established child
/// connection `slot` — the same path the production handler runs.
fn publish_gateway_routes(ctx: &mut Ctx, actor: Key, slot: ChildConnectionKey, live: Vec<Vec<u8>>) {
    ctx.run_command(Command::ConnectionAction {
        connection: ConnectionRef::ChildConnection {
            ofactor: actor,
            slot,
        },
        action: ConnectionCommand::PublishGatewayRoutes { live },
    });
}

/// Whether `actor` holds a gateway route to `tag` on any child connection.
fn has_gateway_route(ctx: &Ctx, actor: Key, tag: &str) -> bool {
    ctx.actors[actor]
        .gateway_routes
        .values()
        .any(|tags| tags.contains(tag.as_bytes()))
}

/// `actor`'s cross-gateway monitor map (panics if `actor` is not a gateway).
fn gateway_state(ctx: &Ctx, actor: Key) -> &std::collections::HashMap<Vec<u8>, GatewayMonitors> {
    match &ctx.actors[actor].gateway {
        GatewayState::Gateway { gateway_state } => gateway_state,
        GatewayState::NotAGateway => panic!("actor should be a gateway"),
    }
}

/// Whether `actor` (a gateway) has recorded `tag` as dead.
fn gateway_is_dead(ctx: &Ctx, actor: Key, tag: &str) -> bool {
    matches!(
        gateway_state(ctx, actor).get(tag.as_bytes()),
        Some(GatewayMonitors::Dead)
    )
}

/// How many gateway tags `actor` (a gateway) has recorded as dead.
fn dead_gateway_count(ctx: &Ctx, actor: Key) -> usize {
    gateway_state(ctx, actor)
        .values()
        .filter(|state| matches!(state, GatewayMonitors::Dead))
        .count()
}

/// Mark `tag` dead in `actor`'s (a gateway's) gateway state, as a death broadcast
/// would.
fn mark_gateway_dead(ctx: &mut Ctx, actor: Key, tag: &str) {
    match &mut ctx.actors[actor].gateway {
        GatewayState::Gateway { gateway_state } => {
            gateway_state.insert(tag.as_bytes().to_vec(), GatewayMonitors::Dead);
        }
        GatewayState::NotAGateway => panic!("actor should be a gateway"),
    }
}

#[test]
fn gateway_routes_climb_to_root_through_a_nongateway() {
    // Gateway routes, unlike actor routes, are not bounded by gateway domains: a
    // gateway reachable below a non-gateway `mid` must be recorded at `mid` *and*
    // carried up to the root, so the whole ancestry can route a death broadcast
    // back down to it.
    let (mut ctx, mut rx) = test_ctx();
    let root = gateway_actor(&mut ctx, "root");
    let mid = actor(&mut ctx, "mid");
    let leaf = actor(&mut ctx, "leaf");
    connect(&mut ctx, root, mid, "inproc://gr-root-mid");
    connect(&mut ctx, mid, leaf, "inproc://gr-mid-leaf");
    drain_commands(&mut ctx, &mut rx);

    // A gateway with tag "A" becomes reachable through mid's connection to leaf.
    let mid_to_leaf = only_child_slot(&ctx, mid);
    publish_gateway_routes(&mut ctx, mid, mid_to_leaf, vec![b"A".to_vec()]);
    drain_commands(&mut ctx, &mut rx);

    assert!(has_gateway_route(&ctx, mid, "A"), "mid records the route");
    assert!(
        has_gateway_route(&ctx, root, "A"),
        "the route climbs past the non-gateway mid up to the root"
    );
}

#[test]
fn populate_gateway_routes_ignores_already_dead_tags() {
    // A gateway that died never returns under the same tag, so a stale live
    // publication for a known-dead tag is dropped rather than resurrecting a route.
    let (mut ctx, mut rx) = test_ctx();
    let root = gateway_actor(&mut ctx, "root");
    let child = actor(&mut ctx, "child");
    connect(&mut ctx, root, child, "inproc://dead-tag");
    drain_commands(&mut ctx, &mut rx);

    mark_gateway_dead(&mut ctx, root, "A");
    let root_to_child = only_child_slot(&ctx, root);
    publish_gateway_routes(&mut ctx, root, root_to_child, vec![b"A".to_vec()]);

    assert!(
        !has_gateway_route(&ctx, root, "A"),
        "a route to a known-dead gateway is not recorded"
    );
}

#[test]
fn connection_failure_announces_nested_gateway_death_to_root() {
    // When the connection carrying a (nested) gateway fails, its non-gateway parent
    // begins the death propagation: it climbs to the root, which records the death
    // in its dead-gateway set. This is the connection-loss trigger.
    let (mut ctx, mut rx) = test_ctx();
    let root = gateway_actor(&mut ctx, "root");
    let mid = actor(&mut ctx, "mid");
    let gwconn = actor(&mut ctx, "gwconn");
    connect(&mut ctx, root, mid, "inproc://nd-root-mid");
    connect(&mut ctx, mid, gwconn, "inproc://nd-mid-gw");
    drain_commands(&mut ctx, &mut rx);

    // Tag "G" is reachable through mid's connection to gwconn; it climbs to root.
    let mid_to_gw = only_child_slot(&ctx, mid);
    publish_gateway_routes(&mut ctx, mid, mid_to_gw, vec![b"G".to_vec()]);
    drain_commands(&mut ctx, &mut rx);
    assert!(has_gateway_route(&ctx, root, "G"));

    // The gateway dies: its connection to mid drops. mid detects it and announces.
    ctx.run_command(Command::Die {
        actor: gwconn,
        reason: MsgPart::from_bytes(b"gone".to_vec()),
    });
    drain_commands(&mut ctx, &mut rx);

    assert!(
        gateway_is_dead(&ctx, root, "G"),
        "the root learns the gateway died"
    );
    assert!(
        !has_gateway_route(&ctx, mid, "G"),
        "mid forgets the route to the dead gateway"
    );
    assert!(
        !has_gateway_route(&ctx, root, "G"),
        "the root forgets the route to the dead gateway"
    );
}

#[test]
fn gateway_death_fans_down_through_nongateway_relays() {
    // The root turns an upward death around and fans it out down its gateway-route
    // children, traversing non-gateway relays so a gateway nested below them is
    // reached. (A real gateway cannot be an inproc child — it would be rejected — so
    // the broadcast reaching `relay` is observed by `relay` forgetting the route to
    // the now-dead gateway it carried.)
    let (mut ctx, mut rx) = test_ctx();
    let root = gateway_actor(&mut ctx, "root");
    let relay = actor(&mut ctx, "relay");
    let leaf = actor(&mut ctx, "leaf");
    connect(&mut ctx, root, relay, "inproc://bc-relay");
    connect(&mut ctx, relay, leaf, "inproc://bc-leaf");
    drain_commands(&mut ctx, &mut rx);

    // Root reaches a still-live gateway "S" through `relay` (keeping that branch
    // alive so the broadcast is forwarded down it), while the doomed gateway "D" is
    // reachable through `relay`'s connection to `leaf`.
    let root_to_relay = only_child_slot(&ctx, root);
    ctx.actors[root]
        .gateway_routes
        .entry(root_to_relay)
        .or_default()
        .insert(b"S".to_vec());
    let relay_to_leaf = only_child_slot(&ctx, relay);
    ctx.actors[relay]
        .gateway_routes
        .entry(relay_to_leaf)
        .or_default()
        .insert(b"D".to_vec());

    // A death announcement for "D" reaches the root and fans back down.
    ctx.gateway_died(root, vec![b"D".to_vec()], true);
    drain_commands(&mut ctx, &mut rx);

    assert!(gateway_is_dead(&ctx, root, "D"));
    assert!(
        has_gateway_route(&ctx, root, "S"),
        "the still-live sibling route is untouched"
    );
    assert!(
        !has_gateway_route(&ctx, relay, "D"),
        "the broadcast reached the non-gateway relay, which forgot the dead route"
    );
}

#[test]
fn gateway_death_is_recorded_once_at_a_gateway() {
    // A gateway deduplicates repeated death waves against its dead-gateway set, so a
    // second broadcast of the same tag changes nothing and does not re-fan-out.
    let (mut ctx, _rx) = test_ctx();
    let gw = gateway_actor(&mut ctx, "gw@A");

    ctx.gateway_died(gw, vec![b"B".to_vec()], true);
    ctx.gateway_died(gw, vec![b"B".to_vec()], true);

    assert_eq!(
        dead_gateway_count(&ctx, gw),
        1,
        "a repeated death wave is absorbed without duplication"
    );
}

// -- Cross-gateway (remote) monitors, driven synchronously -------------------

/// Number of cross-gateway monitors `gw` holds against owning-gateway `tag` (0 if
/// the tag has no `Subscribed` entry).
fn remote_monitor_count(ctx: &Ctx, gw: Key, tag: &str) -> usize {
    match gateway_state(ctx, gw).get(tag.as_bytes()) {
        Some(GatewayMonitors::Subscribed(subs)) => subs.len(),
        _ => 0,
    }
}

/// Whether `gw` holds an *acked* cross-gateway monitor for (`listener`,
/// `monitoring`).
fn remote_monitor_acked(ctx: &Ctx, gw: Key, listener: &str, monitoring: &str) -> bool {
    let tag = monitoring.rsplit('@').next().unwrap_or("");
    matches!(
        gateway_state(ctx, gw).get(tag.as_bytes()),
        Some(GatewayMonitors::Subscribed(subs))
            if subs.iter().any(|m| m.listener == listener.as_bytes()
                && m.monitoring == monitoring.as_bytes()
                && m.acked)
    )
}

/// The parts of `actor`'s most recently buffered message (panics if none).
fn last_message(ctx: &Ctx, actor: Key) -> Vec<String> {
    let Delivery::NoPoller { buffered } = &ctx.actors[actor].delivery else {
        panic!("actor should not be subscribed");
    };
    buffered
        .back()
        .expect("a message should have been buffered")
        .iter()
        .map(|part| String::from_utf8(part.as_bytes().to_vec()).unwrap())
        .collect()
}

/// Deliver a side-channel `AckRemoteMonitor` confirming (`listener`, `monitoring`).
fn deliver_ack(ctx: &mut Ctx, listener: &str, monitoring: &str) {
    ctx.deliver_side_channel(SideChannelMessage {
        gateway_for_actor: listener.as_bytes().to_vec(),
        action: SideChannelAction::AckRemoteMonitor {
            monitoring: monitoring.as_bytes().to_vec(),
        },
    });
}

/// Build a gateway `gwA@A` with a local listener child `L@A` joined over inproc,
/// returning `(gateway, listener)` with establishment hellos drained.
fn gateway_with_listener(ctx: &mut Ctx, rx: &mut mpsc::UnboundedReceiver<Command>) -> (Key, Key) {
    let gw = gateway_actor(ctx, "gwA@A");
    let listener = actor(ctx, "L@A");
    connect(ctx, gw, listener, "inproc://rm-listener");
    drain_commands(ctx, rx);
    (gw, listener)
}

#[test]
fn remote_monitor_fires_when_owning_gateway_dies() {
    // L@A monitors T@B (a different gateway). The subscription climbs to gwA and is
    // held there as a cross-gateway monitor against tag B. When B is announced dead,
    // gwA fires the held monitor back to L and records B as dead.
    let (mut ctx, mut rx) = test_ctx();
    let (gw, listener) = gateway_with_listener(&mut ctx, &mut rx);

    monitor(&mut ctx, listener, 1, "T@B", "T-DOWN");
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(
        remote_monitor_count(&ctx, gw, "B"),
        1,
        "the cross-gateway subscription is held at gwA"
    );

    ctx.gateway_died(gw, vec![b"B".to_vec()], true);
    drain_commands(&mut ctx, &mut rx);

    assert!(gateway_is_dead(&ctx, gw, "B"), "B is recorded dead");
    assert_eq!(
        last_message(&ctx, listener),
        vec!["T-DOWN", "T@B", "actor died"],
        "the listener's monitor fired"
    );
}

#[test]
fn unacked_remote_subscribe_timeout_declares_gateway_dead() {
    // The owning gateway never acknowledges the registration (the sync harness sends
    // nothing back), so the must-exist timer at gwA treats B as unreachable: it
    // declares B dead, which fires the monitor.
    let (mut ctx, mut rx) = test_ctx();
    let (gw, listener) = gateway_with_listener(&mut ctx, &mut rx);

    monitor_with_timeout(&mut ctx, listener, 1, "T@B", "T-DOWN", 50);
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(remote_monitor_count(&ctx, gw, "B"), 1);
    assert!(
        !remote_monitor_acked(&ctx, gw, "L@A", "T@B"),
        "no ack arrived in the sync harness"
    );

    check_monitor_timeout(&mut ctx, gw, "L@A", "T@B");
    drain_commands(&mut ctx, &mut rx);

    assert!(
        gateway_is_dead(&ctx, gw, "B"),
        "the unreachable gateway is dead"
    );
    assert_eq!(
        last_message(&ctx, listener),
        vec!["T-DOWN", "T@B", "actor died"]
    );
}

#[test]
fn acked_remote_subscribe_timeout_is_noop() {
    // Once the owning gateway acknowledges, the must-exist timer no longer declares
    // it dead: the registration is confirmed, so a timer fire is a no-op.
    let (mut ctx, mut rx) = test_ctx();
    let (gw, listener) = gateway_with_listener(&mut ctx, &mut rx);

    monitor_with_timeout(&mut ctx, listener, 1, "T@B", "T-DOWN", 50);
    drain_commands(&mut ctx, &mut rx);
    deliver_ack(&mut ctx, "L@A", "T@B");
    assert!(
        remote_monitor_acked(&ctx, gw, "L@A", "T@B"),
        "the registration is acked"
    );

    check_monitor_timeout(&mut ctx, gw, "L@A", "T@B");
    drain_commands(&mut ctx, &mut rx);

    assert!(
        !gateway_is_dead(&ctx, gw, "B"),
        "an acked gateway is not declared dead"
    );
    assert_eq!(
        remote_monitor_count(&ctx, gw, "B"),
        1,
        "the monitor is still held"
    );
    assert_eq!(
        buffered_strings(&ctx, listener),
        vec!["L@A", "gwA@A"],
        "no monitor fired (only the establishment hellos)"
    );
}

#[test]
fn cancelled_remote_monitor_timeout_does_not_declare_dead() {
    // Cancelling the monitor drops the held cross-gateway record and its now-empty
    // tag entry, so a stale must-exist timer finds nothing and does not declare the
    // gateway dead.
    let (mut ctx, mut rx) = test_ctx();
    let (gw, listener) = gateway_with_listener(&mut ctx, &mut rx);

    monitor_with_timeout(&mut ctx, listener, 1, "T@B", "T-DOWN", 50);
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(remote_monitor_count(&ctx, gw, "B"), 1);

    ctx.run_command(Command::CancelMonitor {
        actor: listener,
        id: 1,
    });
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(
        remote_monitor_count(&ctx, gw, "B"),
        0,
        "cancellation drops the held monitor"
    );

    check_monitor_timeout(&mut ctx, gw, "L@A", "T@B");
    drain_commands(&mut ctx, &mut rx);
    assert!(
        !gateway_is_dead(&ctx, gw, "B"),
        "a stale timer must not declare the gateway dead after cancellation"
    );
}

#[test]
fn subscribe_on_already_dead_gateway_fires_immediately() {
    // Subscribing to a target on a gateway already known dead fires the monitor at
    // once and holds nothing.
    let (mut ctx, mut rx) = test_ctx();
    let (gw, listener) = gateway_with_listener(&mut ctx, &mut rx);
    mark_gateway_dead(&mut ctx, gw, "B");

    monitor(&mut ctx, listener, 1, "T@B", "T-DOWN");
    drain_commands(&mut ctx, &mut rx);

    assert_eq!(
        remote_monitor_count(&ctx, gw, "B"),
        0,
        "no monitor is held for an already-dead gateway"
    );
    assert_eq!(
        last_message(&ctx, listener),
        vec!["T-DOWN", "T@B", "actor died"],
        "the monitor fired immediately"
    );
}

#[test]
fn pending_side_channel_holds_any_action_until_resolvable() {
    // A non-`Send` side-channel action (a remote subscribe) that arrives before its
    // owning gateway exists is held whole in pending_side_channel — not dropped —
    // and replayed once the gateway and target route appear, registering an ordinary
    // local monitor at the owning gateway.
    let (mut ctx, mut rx) = test_ctx();

    ctx.deliver_side_channel(SideChannelMessage {
        gateway_for_actor: b"T@B".to_vec(),
        action: SideChannelAction::UpdateRemoteMonitorState {
            listener: b"L@A".to_vec(),
            op: MonitorOp::Subscribe { timeout_ms: 0 },
        },
    });
    assert!(
        ctx.pending_side_channel.contains_key(b"T@B".as_slice()),
        "an unresolvable remote subscribe should pend, not be dropped"
    );

    // gwB and its child T@B appear; the route propagates and the pended subscribe is
    // replayed, becoming a local MonitorSub on gwB's route to T.
    let gw_b = gateway_actor(&mut ctx, "gwB@B");
    let target = actor(&mut ctx, "T@B");
    connect(&mut ctx, gw_b, target, "inproc://pend-t");
    drain_commands(&mut ctx, &mut rx);

    assert!(
        !ctx.pending_side_channel.contains_key(b"T@B".as_slice()),
        "the pending message is released once T@B is resolvable"
    );
    assert_eq!(
        route_monitor_count(&ctx, gw_b, "T@B"),
        1,
        "the replayed subscribe registered a local monitor at the owning gateway"
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

// --- Non-existence timeout (timeout_for_nonexistence) ----------------------

// A timeout monitor on a target that never appears fires "actor does not exist"
// when its timer (here driven directly) checks the still-`Unknown` route at the
// common ancestor.
#[test]
fn timeout_fires_when_target_never_appears() {
    let (mut ctx, mut rx) = test_ctx();
    let root = actor(&mut ctx, "root");
    let watcher = actor(&mut ctx, "watcher");
    connect(&mut ctx, root, watcher, "inproc://to-watcher");
    drain_commands(&mut ctx, &mut rx);

    // `target` is never created. The subscription climbs to `root` and buffers on
    // an `Unknown` route there.
    monitor_with_timeout(&mut ctx, watcher, 0, "target", "DOWN", 50);
    drain_commands(&mut ctx, &mut rx);
    assert!(matches!(
        ctx.actors[root].routes.get(b"target".as_slice()),
        Some(Route::Unknown { .. })
    ));

    // Drive the timer: the route is still `Unknown`, so the timeout fires.
    check_monitor_timeout(&mut ctx, root, "watcher", "target");
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(
        buffered_strings(&ctx, watcher),
        vec!["watcher", "root", "DOWN", "target", "actor does not exist"]
    );
}

// After a timeout fires, the local monitor is consumed: a later real death of the
// (now-existing) target delivers nothing more for this monitor.
#[test]
fn timeout_fires_once_then_target_death_is_silent() {
    let (mut ctx, mut rx) = test_ctx();
    let root = actor(&mut ctx, "root");
    let watcher = actor(&mut ctx, "watcher");
    connect(&mut ctx, root, watcher, "inproc://once-watcher");
    drain_commands(&mut ctx, &mut rx);

    monitor_with_timeout(&mut ctx, watcher, 0, "target", "DOWN", 50);
    drain_commands(&mut ctx, &mut rx);
    check_monitor_timeout(&mut ctx, root, "watcher", "target");
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(
        buffered_strings(&ctx, watcher),
        vec!["watcher", "root", "DOWN", "target", "actor does not exist"]
    );

    // The target now appears and later dies. The subscription still lingers at
    // root, so a death fire is routed down — but `watcher`'s local entry is gone,
    // so nothing new is delivered.
    let target = actor(&mut ctx, "target");
    connect(&mut ctx, root, target, "inproc://once-target");
    drain_commands(&mut ctx, &mut rx);
    ctx.die_actor(target, b"boom".to_vec());
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(
        buffered_strings(&ctx, watcher),
        vec!["watcher", "root", "DOWN", "target", "actor does not exist"]
    );
}

// When the target already exists (a `Connection` route at the ancestor), driving
// the timeout is a no-op; a subsequent real death still fires "actor died".
#[test]
fn timeout_noop_when_target_exists() {
    let (mut ctx, mut rx) = test_ctx();
    let root = actor(&mut ctx, "root");
    let watcher = actor(&mut ctx, "watcher");
    let target = actor(&mut ctx, "target");
    connect(&mut ctx, root, watcher, "inproc://ex-watcher");
    connect(&mut ctx, root, target, "inproc://ex-target");
    drain_commands(&mut ctx, &mut rx);

    monitor_with_timeout(&mut ctx, watcher, 0, "target", "DOWN", 50);
    drain_commands(&mut ctx, &mut rx);
    assert!(matches!(
        ctx.actors[root].routes.get(b"target".as_slice()),
        Some(Route::Connection { .. })
    ));

    // The route is `Connection`, so the timer fire delivers nothing.
    check_monitor_timeout(&mut ctx, root, "watcher", "target");
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(buffered_strings(&ctx, watcher), vec!["watcher", "root"]);

    // A real death still fires the ordinary death reason.
    ctx.die_actor(target, b"boom".to_vec());
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(
        buffered_strings(&ctx, watcher),
        vec!["watcher", "root", "DOWN", "target", "actor died"]
    );
}

// A timeout monitor created while the actor is unnamed retains its timeout on the
// local record; once named, the deferred `Subscribe` carries the timeout and a
// later timer fire reports "actor does not exist".
#[test]
fn timeout_deferred_until_named() {
    let (mut ctx, mut rx) = test_ctx();
    let root = actor(&mut ctx, "root");
    let watcher = ctx.actors.insert(ActorEntry::new(None, false));
    drain_commands(&mut ctx, &mut rx);

    monitor_with_timeout(&mut ctx, watcher, 0, "target", "DOWN", 50);
    drain_commands(&mut ctx, &mut rx);
    // Deferred: nothing upstream yet.
    assert_eq!(route_monitor_count(&ctx, root, "target"), 0);

    // Naming releases the deferred subscription (carrying the timeout) up to root.
    ctx.serve(
        root,
        "inproc://def-watcher".to_owned(),
        request_named(Role::Parent, "watcher"),
    );
    ctx.join(
        watcher,
        "inproc://def-watcher".to_owned(),
        request(Role::Child),
    );
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(route_monitor_count(&ctx, root, "target"), 1);

    check_monitor_timeout(&mut ctx, root, "watcher", "target");
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(
        buffered_strings(&ctx, watcher),
        vec!["watcher", "root", "DOWN", "target", "actor does not exist"]
    );
}

// The regression that killed the previous attempt: a subscription armed at `mid`
// migrates to `root` when `mid` gains a parent. The stale timer at `mid` must
// no-op (its route is gone), and the migrated monitor must still fire on a real
// death.
#[test]
fn timeout_migration_does_not_false_fire() {
    let (mut ctx, mut rx) = test_ctx();
    let root = actor(&mut ctx, "root");
    let mid = actor(&mut ctx, "mid");
    let watcher = actor(&mut ctx, "watcher");
    let target = actor(&mut ctx, "target");

    // `watcher` joins parentless `mid` and monitors `target` with a timeout: the
    // subscription buffers on an `Unknown` route at `mid` (the timer is armed here).
    connect(&mut ctx, mid, watcher, "inproc://mig-w");
    drain_commands(&mut ctx, &mut rx);
    monitor_with_timeout(&mut ctx, watcher, 0, "target", "DOWN", 50);
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(route_monitor_count(&ctx, mid, "target"), 1);

    // `mid` joins `root`: the subscription migrates up; `mid`'s route is dropped.
    connect(&mut ctx, root, mid, "inproc://mig-m");
    drain_commands(&mut ctx, &mut rx);
    assert!(!ctx.actors[mid].routes.contains_key(b"target".as_slice()));
    assert_eq!(route_monitor_count(&ctx, root, "target"), 1);

    // `target` appears under `root`.
    connect(&mut ctx, root, target, "inproc://mig-t");
    drain_commands(&mut ctx, &mut rx);

    // Driving the *old* timer at `mid` finds no route there: it must no-op.
    check_monitor_timeout(&mut ctx, mid, "watcher", "target");
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(buffered_strings(&ctx, watcher), vec!["watcher", "mid"]);

    // A real death still fires "actor died" through the migrated subscription.
    ctx.die_actor(target, b"boom".to_vec());
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(
        buffered_strings(&ctx, watcher),
        vec!["watcher", "mid", "DOWN", "target", "actor died"]
    );
}

// As above, but the target never appears: the timer re-armed at `root` after
// migration still fires "actor does not exist".
#[test]
fn timeout_migration_still_fires_when_target_absent() {
    let (mut ctx, mut rx) = test_ctx();
    let root = actor(&mut ctx, "root");
    let mid = actor(&mut ctx, "mid");
    let watcher = actor(&mut ctx, "watcher");

    connect(&mut ctx, mid, watcher, "inproc://mab-w");
    drain_commands(&mut ctx, &mut rx);
    monitor_with_timeout(&mut ctx, watcher, 0, "target", "DOWN", 50);
    drain_commands(&mut ctx, &mut rx);

    connect(&mut ctx, root, mid, "inproc://mab-m");
    drain_commands(&mut ctx, &mut rx);
    assert!(matches!(
        ctx.actors[root].routes.get(b"target".as_slice()),
        Some(Route::Unknown { .. })
    ));

    // The re-armed timer at `root` checks the still-`Unknown` route and fires.
    check_monitor_timeout(&mut ctx, root, "watcher", "target");
    drain_commands(&mut ctx, &mut rx);
    assert_eq!(
        buffered_strings(&ctx, watcher),
        vec!["watcher", "mid", "DOWN", "target", "actor does not exist"]
    );
}

// Cancelling a timeout monitor removes the local entry, so even if a stale timer
// later fires on a still-`Unknown` route nothing is delivered.
#[test]
fn timeout_cancelled_monitor_does_not_fire() {
    let (mut ctx, mut rx) = test_ctx();
    let root = actor(&mut ctx, "root");
    let watcher = actor(&mut ctx, "watcher");
    connect(&mut ctx, root, watcher, "inproc://can-watcher");
    drain_commands(&mut ctx, &mut rx);

    monitor_with_timeout(&mut ctx, watcher, 0, "target", "DOWN", 50);
    drain_commands(&mut ctx, &mut rx);
    ctx.run_command(Command::CancelMonitor {
        actor: watcher,
        id: 0,
    });
    drain_commands(&mut ctx, &mut rx);

    // Force the route back to `Unknown` (cancel emptied its monitors) and drive the
    // stale timer: the monitoring actor's local entry is gone, so no delivery.
    check_monitor_timeout(&mut ctx, root, "watcher", "target");
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
fn tcp_serve_join_establishes_and_routes() {
    // The tcp analogue of `quic_serve_join_establishes_and_routes`: connect opens the
    // data + heartbeat sockets up front (paired by prefix), establishment runs over
    // the data socket, and messages route both directions. Exercises the whole tcp
    // Net impl and the generic transport driving it.
    set_quic_env(); // tcp reuses the same MM_QUIC_* cert material
    let ctx = CtxHandle::new().expect("context should start");
    let parent = runtime_actor(&ctx, "t-parent");
    let child = runtime_actor(&ctx, "t-child");
    let (parent_poller, mut parent_rx) = runtime_poller(&ctx);
    let (child_poller, mut child_rx) = runtime_poller(&ctx);
    runtime_subscribe(&ctx, parent_poller, 0, parent);
    runtime_subscribe(&ctx, child_poller, 0, child);

    let url = free_tcp_url();
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

    // The TLS handshake completes on both sockets and both sides get their hello.
    assert_eq!(recv_strings(&mut parent_rx), vec!["t-parent", "t-child"]);
    assert_eq!(recv_strings(&mut child_rx), vec!["t-child", "t-parent"]);

    // A message both directions is framed over the data socket and delivered intact.
    ctx.send_command(Command::Send {
        sender: parent,
        destination_ident: MsgPart::from_bytes(b"t-child".to_vec()),
        parts: vec![
            MsgPart::from_bytes(b"down".to_vec()),
            MsgPart::from_bytes(b"socket".to_vec()),
        ],
    })
    .expect("send should enqueue");
    assert_eq!(recv_strings(&mut child_rx), vec!["down", "socket"]);

    ctx.send_command(Command::Send {
        sender: child,
        destination_ident: MsgPart::from_bytes(b"t-parent".to_vec()),
        parts: vec![MsgPart::from_bytes(b"up".to_vec())],
    })
    .expect("send should enqueue");
    assert_eq!(recv_strings(&mut parent_rx), vec!["up"]);

    shutdown(ctx);
}

#[test]
fn tcp_gateway_to_gateway_bypasses_root() {
    // The tcp analogue of `quic_gateway_to_gateway_bypasses_root`. The gateway tags
    // carry the `tcp://` scheme, so ctx's `send_to_gateway` routes the direct
    // gateway-to-gateway side-channels over tcp (not quic). Exercises the eager
    // two-socket side-channel open and the scheme-aware routing.
    set_quic_env();
    let root_ctx = CtxHandle::new().expect("root ctx");
    let a_ctx = CtxHandle::new().expect("a ctx");
    let b_ctx = CtxHandle::new().expect("b ctx");

    let root_url = free_tcp_url();
    let a_url = free_tcp_url();
    let b_url = free_tcp_url();
    // The gateway tag *is* the full `tcp://addr` url, so the ident carries the scheme
    // and cross-gateway routing picks the tcp transport.
    let a_tag = a_url.clone();
    let b_tag = b_url.clone();

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

    runtime_serve(&root_ctx, root, &root_url);
    runtime_serve(&root_ctx, root, &root_url);
    runtime_join(&a_ctx, gw_a, &root_url);
    runtime_join(&b_ctx, gw_b, &root_url);

    runtime_serve(&a_ctx, gw_a, &a_url);
    runtime_serve(&b_ctx, gw_b, &b_url);
    runtime_connect(&a_ctx, gw_a, a1, "inproc://bypass-tcp-a");
    runtime_connect(&b_ctx, gw_b, b1, "inproc://bypass-tcp-b");

    assert_eq!(
        recv_strings(&mut a1_rx),
        vec![format!("a1@{a_tag}"), format!("gwA@{a_tag}")]
    );
    assert_eq!(
        recv_strings(&mut b1_rx),
        vec![format!("b1@{b_tag}"), format!("gwB@{b_tag}")]
    );

    let mut peers: Vec<String> = (0..2)
        .map(|_| {
            let hello = recv_strings(&mut root_rx);
            assert_eq!(hello[0], "root");
            hello[1].clone()
        })
        .collect();
    peers.sort();
    assert_eq!(peers, vec![format!("gwA@{a_tag}"), format!("gwB@{b_tag}")]);

    // a1@A -> b1@B: gwA opens a direct tcp side-channel to gwB, bypassing root.
    a_ctx
        .send_command(Command::Send {
            sender: a1,
            destination_ident: MsgPart::from_bytes(format!("b1@{b_tag}").into_bytes()),
            parts: vec![MsgPart::from_bytes(b"cross-gateway".to_vec())],
        })
        .expect("send should enqueue");
    assert_eq!(recv_strings(&mut b1_rx), vec!["cross-gateway"]);

    b_ctx
        .send_command(Command::Send {
            sender: b1,
            destination_ident: MsgPart::from_bytes(format!("a1@{a_tag}").into_bytes()),
            parts: vec![MsgPart::from_bytes(b"reply".to_vec())],
        })
        .expect("send should enqueue");
    assert_eq!(recv_strings(&mut a1_rx), vec!["reply"]);

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
fn quic_monitor_on_root_domain_target_climbs_to_root() {
    // A child of gateway A monitors a root-domain target (blank specifier). The
    // subscription must climb gwA's parent link to the root — where the target
    // actually lives — rather than being held at gwA (which would never see the
    // target and would fire a false non-existence). Killing the root-domain target
    // then fires the monitor back to the gateway child over a side-channel.
    set_quic_env();
    let root_ctx = CtxHandle::new().expect("root ctx");
    let a_ctx = CtxHandle::new().expect("a ctx");

    let root_url = free_quic_url();
    let a_url = free_quic_url();
    let a_tag = quic_authority(&a_url);

    let root = runtime_gateway_actor(&root_ctx, "root");
    let rootchild = runtime_actor(&root_ctx, "rootchild");
    let gw_a = runtime_gateway_actor(&a_ctx, &format!("gwA@{a_tag}"));
    let a1 = runtime_actor(&a_ctx, &format!("a1@{a_tag}"));

    let (rc_poller, mut rc_rx) = runtime_poller(&root_ctx);
    let (a1_poller, mut a1_rx) = runtime_poller(&a_ctx);
    runtime_subscribe(&root_ctx, rc_poller, 0, rootchild);
    runtime_subscribe(&a_ctx, a1_poller, 0, a1);

    runtime_serve(&root_ctx, root, &root_url);
    runtime_join(&a_ctx, gw_a, &root_url);
    // gwA serves its own endpoint so root can side-channel the fire back to it.
    runtime_serve(&a_ctx, gw_a, &a_url);
    runtime_connect(&root_ctx, root, rootchild, "inproc://climb-rc");
    runtime_connect(&a_ctx, gw_a, a1, "inproc://climb-a");

    // Drain establishment hellos so rootchild is routable before a1 subscribes.
    assert_eq!(recv_strings(&mut rc_rx), vec!["rootchild", "root"]);
    assert_eq!(
        recv_strings(&mut a1_rx),
        vec![format!("a1@{a_tag}"), format!("gwA@{a_tag}")]
    );

    // a1@A monitors rootchild (root domain); the subscribe climbs gwA -> root.
    runtime_monitor(&a_ctx, a1, 1, "rootchild", "RC-DOWN");
    std::thread::sleep(std::time::Duration::from_millis(300));

    root_ctx
        .send_command(Command::Die {
            actor: rootchild,
            reason: MsgPart::from_bytes(b"boom".to_vec()),
        })
        .expect("die should enqueue");

    assert_eq!(
        recv_strings(&mut a1_rx),
        vec!["RC-DOWN", "rootchild", "actor died"],
        "the root-domain target's death fired the gateway child's monitor"
    );

    shutdown(a_ctx);
    shutdown(root_ctx);
}

/// Build a three-context cross-gateway topology (root + gwA + gwB) with a listener
/// `L@A` under gwA and a target `T@B` under gwB, all establishment hellos drained.
/// Returns the contexts, the gwB/target keys, the listener's delivery receiver, and
/// the `b_tag`. Mirrors `quic_gateway_to_gateway_bypasses_root`'s wiring.
fn quic_cross_gateway_monitor_setup() -> (
    CtxHandle,
    CtxHandle,
    CtxHandle,
    Key,
    Key,
    Key,
    mpsc::UnboundedReceiver<Delivered>,
    String,
) {
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
    let listener = runtime_actor(&a_ctx, &format!("L@{a_tag}"));
    let gw_b = runtime_gateway_actor(&b_ctx, &format!("gwB@{b_tag}"));
    let target = runtime_actor(&b_ctx, &format!("T@{b_tag}"));

    let (l_poller, l_rx) = runtime_poller(&a_ctx);
    let (t_poller, mut t_rx) = runtime_poller(&b_ctx);
    runtime_subscribe(&a_ctx, l_poller, 0, listener);
    runtime_subscribe(&b_ctx, t_poller, 0, target);

    // root is the shared rendezvous (serves once per joining gateway).
    runtime_serve(&root_ctx, root, &root_url);
    runtime_serve(&root_ctx, root, &root_url);
    runtime_join(&a_ctx, gw_a, &root_url);
    runtime_join(&b_ctx, gw_b, &root_url);
    // Each gateway serves its own endpoint so a sibling can side-channel to it.
    runtime_serve(&a_ctx, gw_a, &a_url);
    runtime_serve(&b_ctx, gw_b, &b_url);
    runtime_connect(&a_ctx, gw_a, listener, "inproc://rm-listener");
    runtime_connect(&b_ctx, gw_b, target, "inproc://rm-target");

    let mut l_rx = l_rx;
    // Drain the local-child hellos so T is routable before L subscribes.
    assert_eq!(
        recv_strings(&mut l_rx),
        vec![format!("L@{a_tag}"), format!("gwA@{a_tag}")]
    );
    assert_eq!(
        recv_strings(&mut t_rx),
        vec![format!("T@{b_tag}"), format!("gwB@{b_tag}")]
    );

    (root_ctx, a_ctx, b_ctx, gw_b, target, listener, l_rx, b_tag)
}

#[test]
fn quic_remote_monitor_fires_when_target_dies() {
    // L@A monitors T@B across gateways. The subscription crosses to gwB over a
    // side-channel and is held as a local monitor on gwB's route to T. When T dies,
    // gwB fires it straight back to L over a side-channel.
    let (root_ctx, a_ctx, b_ctx, _gw_b, target, listener, mut l_rx, b_tag) =
        quic_cross_gateway_monitor_setup();

    runtime_monitor(&a_ctx, listener, 1, &format!("T@{b_tag}"), "T-DOWN");
    // Let the cross-gateway subscribe register at gwB before T dies.
    std::thread::sleep(std::time::Duration::from_millis(300));

    b_ctx
        .send_command(Command::Die {
            actor: target,
            reason: MsgPart::from_bytes(b"boom".to_vec()),
        })
        .expect("die should enqueue");

    assert_eq!(
        recv_strings(&mut l_rx),
        vec![
            "T-DOWN".to_string(),
            format!("T@{b_tag}"),
            "actor died".to_string()
        ],
        "the target's death fires L's monitor"
    );

    shutdown(a_ctx);
    shutdown(b_ctx);
    shutdown(root_ctx);
}

#[test]
fn quic_remote_monitor_fires_when_owning_gateway_dies() {
    // L@A monitors T@B. Killing all of gwB drops its link to root; root broadcasts
    // the gateway death down to gwA, which fires the cross-gateway monitor it held
    // for B directly — even though T itself never sent a death.
    let (root_ctx, a_ctx, b_ctx, gw_b, _target, listener, mut l_rx, b_tag) =
        quic_cross_gateway_monitor_setup();

    runtime_monitor(&a_ctx, listener, 1, &format!("T@{b_tag}"), "GW-DOWN");
    // The MonitorToFire is recorded at gwA as soon as the monitor is processed.
    std::thread::sleep(std::time::Duration::from_millis(300));

    b_ctx
        .send_command(Command::Die {
            actor: gw_b,
            reason: MsgPart::from_bytes(b"gateway gone".to_vec()),
        })
        .expect("die should enqueue");

    assert_eq!(
        recv_strings(&mut l_rx),
        vec![
            "GW-DOWN".to_string(),
            format!("T@{b_tag}"),
            "actor died".to_string()
        ],
        "the owning gateway's death fires L's monitor"
    );

    shutdown(a_ctx);
    shutdown(b_ctx);
    shutdown(root_ctx);
}

#[test]
fn heartbeat_timeout_severs_connection() {
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
    // Keep multi-context tests under CI socket limits; endpoint-pool behavior has
    // dedicated coverage in `quic_net`.
    std::env::set_var("MM_QUIC_CLIENT_ENDPOINTS", "1");
}

fn free_quic_url() -> String {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind ephemeral udp");
    let port = socket.local_addr().expect("local addr").port();
    drop(socket);
    format!("quic://127.0.0.1:{port}")
}

fn free_tcp_url() -> String {
    let socket = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral tcp");
    let port = socket.local_addr().expect("local addr").port();
    drop(socket);
    format!("tcp://127.0.0.1:{port}")
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
    monitor_with_timeout(ctx, actor, id, to_monitor, failure, 0);
}

fn monitor_with_timeout(
    ctx: &mut Ctx,
    actor: Key,
    id: u64,
    to_monitor: &str,
    failure: &str,
    timeout_ms: u64,
) {
    ctx.run_command(Command::Monitor {
        actor,
        id,
        to_monitor: MsgPart::from_bytes(to_monitor.as_bytes().to_vec()),
        failure_prefix: vec![MsgPart::from_bytes(failure.as_bytes().to_vec())],
        timeout_ms,
    });
}

/// Drive a "must exist" timer fire at `at` (the in-process command a real timer
/// would have sent on expiry).
fn check_monitor_timeout(ctx: &mut Ctx, at: Key, listener: &str, target: &str) {
    ctx.run_command(Command::CheckMonitorTimeout {
        at,
        listener: listener.as_bytes().to_vec(),
        target: target.as_bytes().to_vec(),
    });
}

/// Deliver a side-channel `Send` of a single-part actor message to `dest`.
fn side_channel_send(ctx: &mut Ctx, dest: &str, body: &[u8]) {
    ctx.deliver_side_channel(SideChannelMessage {
        gateway_for_actor: dest.as_bytes().to_vec(),
        action: SideChannelAction::Send(SendPayload::ActorMessage(vec![MsgPart::from_bytes(
            body.to_vec(),
        )])),
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
        timeout_ms: 0,
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
