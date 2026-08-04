/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use static_assertions::assert_impl_all;
use static_assertions::assert_not_impl_any;

use crate::Actor;
use crate::Context;
use crate::MonitorHandle;
use crate::Part;
use crate::Poller;
use crate::Role;

// The thread-safety tagging is part of the API contract, asserted at compile
// time. `Context`, `Actor`, and `MonitorHandle` dispatch every operation to the
// runtime over an internal channel, so they are `Send + Sync` (send from many
// threads). A `Poller` may be moved to the thread that drives it (`Send`) but
// the C ABI requires its calls to be externally serialized, so it must not be
// shared concurrently (`!Sync`).
assert_impl_all!(Context: Send, Sync);
assert_impl_all!(Actor: Send, Sync);
assert_impl_all!(MonitorHandle: Send, Sync);
assert_impl_all!(Poller: Send);
assert_not_impl_any!(Poller: Sync);

/// A part built from a `Vec` exposes exactly those bytes and copies out cleanly.
#[test]
fn part_roundtrips_bytes() {
    let part = Part::from_vec(b"payload".to_vec());
    assert_eq!(part.as_bytes(), b"payload");
    assert_eq!(part.len(), 7);
    assert!(!part.is_empty());
    assert_eq!(part.to_vec(), b"payload".to_vec());

    let empty = Part::empty();
    assert!(empty.is_empty());
    assert_eq!(empty.as_bytes(), b"");
}

/// An actor can send a message to itself and receive it back through a poller.
/// Runs on a single-threaded runtime — the blocking setup calls (actor/poller
/// creation, subscribe) work with no special handling because minimonarch's
/// runtime is isolated in its own native library.
#[tokio::test(flavor = "current_thread")]
async fn send_to_self() {
    let ctx = Context::new().expect("context should be creatable");
    let actor = ctx
        .actor(Some(b"self-actor"), /*gateway=*/ true)
        .expect("actor should be creatable");
    let mut poller = ctx.poller().expect("poller should be creatable");
    poller
        .subscribe(0, &actor)
        .expect("subscribe should succeed");

    actor
        .send(b"self-actor", vec![Part::copy_from(b"ping")])
        .expect("send should succeed");

    let (index, parts) = poller.recv().await.expect("a message should arrive");
    assert_eq!(index, 0, "message should come from the subscribed index");
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].as_bytes(), b"ping");
}

/// A parent/child pair over inproc exchanges hello messages and payloads both
/// directions, on a single-threaded runtime.
#[tokio::test(flavor = "current_thread")]
async fn parent_child_inproc() {
    let ctx = Context::new().expect("context should be creatable");
    let parent = ctx
        .actor(Some(b"p-actor"), true)
        .expect("parent should be creatable");
    let child = ctx
        .actor(Some(b"c-actor"), false)
        .expect("child should be creatable");

    let mut p_poll = ctx.poller().expect("poller");
    p_poll.subscribe(0, &parent).expect("subscribe");
    let mut c_poll = ctx.poller().expect("poller");
    c_poll.subscribe(0, &child).expect("subscribe");

    let url = "inproc://rs-test-parent-child";
    parent
        .serve(url, Role::Parent, None, &[], &[])
        .expect("serve should succeed");
    child
        .join(url, Role::Child, None, &[], &[])
        .expect("join should succeed");

    // Both sides get a hello: [self, other].
    let (_, hello) = p_poll.recv().await.expect("parent hello");
    assert_eq!(hello[0].as_bytes(), b"p-actor");
    assert_eq!(hello[1].as_bytes(), b"c-actor");
    let (_, hello) = c_poll.recv().await.expect("child hello");
    assert_eq!(hello[0].as_bytes(), b"c-actor");
    assert_eq!(hello[1].as_bytes(), b"p-actor");

    // Parent -> child.
    parent
        .send(b"c-actor", vec![Part::copy_from(b"down")])
        .expect("send down");
    let (_, msg) = c_poll.recv().await.expect("child receives");
    assert_eq!(msg[0].as_bytes(), b"down");

    // Child -> parent.
    child
        .send(b"p-actor", vec![Part::copy_from(b"up")])
        .expect("send up");
    let (_, msg) = p_poll.recv().await.expect("parent receives");
    assert_eq!(msg[0].as_bytes(), b"up");
}

/// A hello prefix is prepended to the delivered connection-established message.
#[tokio::test(flavor = "current_thread")]
async fn hello_prefix_is_delivered() {
    let ctx = Context::new().expect("context");
    let parent = ctx.actor(Some(b"pp"), true).expect("parent");
    let child = ctx.actor(Some(b"cc"), false).expect("child");

    let mut p_poll = ctx.poller().expect("poller");
    p_poll.subscribe(0, &parent).expect("subscribe");

    let url = "inproc://rs-test-hello-prefix";
    parent
        .serve(url, Role::Parent, None, &[b"CONNECTED"], &[])
        .expect("serve");
    child.join(url, Role::Child, None, &[], &[]).expect("join");

    // Delivered shape is [hello..., self, other].
    let (_, msg) = p_poll.recv().await.expect("parent hello");
    assert_eq!(msg[0].as_bytes(), b"CONNECTED");
    assert_eq!(msg[1].as_bytes(), b"pp");
    assert_eq!(msg[2].as_bytes(), b"cc");
}

/// An actor may outlive its context; using it afterwards returns an error
/// rather than misbehaving, and dropping it stays safe.
#[test]
fn actor_use_after_context_drop_errors() {
    let ctx = Context::new().expect("context");
    let actor = ctx.actor(Some(b"orphan"), true).expect("actor");

    drop(ctx);

    let err = actor
        .send(b"orphan", vec![Part::copy_from(b"lost")])
        .expect_err("send after context drop should error");
    assert!(
        err.to_string().contains("context"),
        "error should mention the stopped context, got: {err}"
    );
    // Dropping `actor` here (after the context is gone) must not misbehave.
}

/// Sending a large multipart message drives the `mm_poller_next` buffer-growth
/// path (initial capacity is 8 parts).
#[tokio::test(flavor = "current_thread")]
async fn large_multipart_grows_buffer() {
    let ctx = Context::new().expect("context");
    let actor = ctx.actor(Some(b"multi"), true).expect("actor");
    let mut poller = ctx.poller().expect("poller");
    poller.subscribe(0, &actor).expect("subscribe");

    let parts: Vec<Part> = (0..20)
        .map(|i| Part::from_vec(format!("part-{i}").into_bytes()))
        .collect();
    actor.send(b"multi", parts).expect("send");

    let (_, received) = poller.recv().await.expect("message");
    assert_eq!(received.len(), 20, "all 20 parts should be delivered");
    assert_eq!(received[0].as_bytes(), b"part-0");
    assert_eq!(received[19].as_bytes(), b"part-19");
}

/// Establish a parent/child inproc link under `root` and drain both hello
/// messages, so the topology (and its routing) is in place before the test acts.
async fn connect(
    root: &Actor,
    root_poll: &mut Poller,
    url: &str,
    child: &Actor,
    child_poll: &mut Poller,
) {
    root.serve(url, Role::Parent, None, &[], &[])
        .expect("serve");
    child.join(url, Role::Child, None, &[], &[]).expect("join");
    root_poll.recv().await.expect("root hello");
    child_poll.recv().await.expect("child hello");
}

/// A monitor fires when its target dies: the failure climbs to the common
/// ancestor (root) and returns to the watcher as [failure.., target, reason].
#[tokio::test(flavor = "current_thread")]
async fn monitor_fires_when_target_dies() {
    let ctx = Context::new().expect("context");
    let root = ctx.actor(Some(b"root"), false).expect("root");
    let watcher = ctx.actor(Some(b"watcher"), false).expect("watcher");
    let target = ctx.actor(Some(b"target"), false).expect("target");

    let mut root_poll = ctx.poller().expect("poller");
    root_poll.subscribe(0, &root).expect("subscribe");
    let mut watcher_poll = ctx.poller().expect("poller");
    watcher_poll.subscribe(0, &watcher).expect("subscribe");
    let mut target_poll = ctx.poller().expect("poller");
    target_poll.subscribe(0, &target).expect("subscribe");

    connect(
        &root,
        &mut root_poll,
        "inproc://rs-mon-w",
        &watcher,
        &mut watcher_poll,
    )
    .await;
    connect(
        &root,
        &mut root_poll,
        "inproc://rs-mon-t",
        &target,
        &mut target_poll,
    )
    .await;

    let handle = watcher.monitor(b"target", &[b"DOWN"], 0).expect("monitor");
    // Dropping the handle detaches (JoinHandle semantics); the monitor stays
    // active and must still fire.
    drop(handle);

    target.die(b"boom");

    let (index, msg) = watcher_poll
        .recv()
        .await
        .expect("monitor should fire even after its handle is dropped");
    assert_eq!(index, 0);
    assert_eq!(msg.len(), 3, "shape is [failure.., target, reason]");
    assert_eq!(msg[0].as_bytes(), b"DOWN");
    assert_eq!(msg[1].as_bytes(), b"target");
    assert_eq!(msg[2].as_bytes(), b"actor died");
}

/// A cancelled monitor must not deliver, even after the target dies. Proven with
/// a sentinel: it must be the first (and only) message the watcher receives.
#[tokio::test(flavor = "current_thread")]
async fn cancelled_monitor_does_not_fire() {
    let ctx = Context::new().expect("context");
    let root = ctx.actor(Some(b"root"), false).expect("root");
    let watcher = ctx.actor(Some(b"watcher"), false).expect("watcher");
    let target = ctx.actor(Some(b"target"), false).expect("target");

    let mut root_poll = ctx.poller().expect("poller");
    root_poll.subscribe(0, &root).expect("subscribe");
    let mut watcher_poll = ctx.poller().expect("poller");
    watcher_poll.subscribe(0, &watcher).expect("subscribe");
    let mut target_poll = ctx.poller().expect("poller");
    target_poll.subscribe(0, &target).expect("subscribe");

    connect(
        &root,
        &mut root_poll,
        "inproc://rs-monc-w",
        &watcher,
        &mut watcher_poll,
    )
    .await;
    connect(
        &root,
        &mut root_poll,
        "inproc://rs-monc-t",
        &target,
        &mut target_poll,
    )
    .await;

    let handle = watcher.monitor(b"target", &[b"DOWN"], 0).expect("monitor");
    watcher.cancel_monitor(handle);
    target.die(b"boom");

    watcher
        .send(b"watcher", vec![Part::copy_from(b"sentinel")])
        .expect("send sentinel");
    let (_, msg) = watcher_poll.recv().await.expect("sentinel");
    assert_eq!(
        msg[0].as_bytes(),
        b"sentinel",
        "cancelled monitor must not have delivered ahead of the sentinel"
    );
}

/// With a non-existence timeout, monitoring a target that never appears fires
/// once with reason "actor does not exist" after the timeout elapses.
#[tokio::test(flavor = "current_thread")]
async fn monitor_timeout_fires_when_target_never_exists() {
    let ctx = Context::new().expect("context");
    let root = ctx.actor(Some(b"root"), false).expect("root");
    let watcher = ctx.actor(Some(b"watcher"), false).expect("watcher");

    let mut root_poll = ctx.poller().expect("poller");
    root_poll.subscribe(0, &root).expect("subscribe");
    let mut watcher_poll = ctx.poller().expect("poller");
    watcher_poll.subscribe(0, &watcher).expect("subscribe");

    connect(
        &root,
        &mut root_poll,
        "inproc://rs-mont-w",
        &watcher,
        &mut watcher_poll,
    )
    .await;

    // 30ms non-existence timeout; "target" never appears.
    let _handle = watcher.monitor(b"target", &[b"DOWN"], 30).expect("monitor");

    let (_, msg) = watcher_poll.recv().await.expect("timeout should fire");
    assert_eq!(msg[0].as_bytes(), b"DOWN");
    assert_eq!(msg[1].as_bytes(), b"target");
    assert_eq!(msg[2].as_bytes(), b"actor does not exist");
}

/// Monitoring an actor already known dead fires immediately rather than waiting.
#[tokio::test(flavor = "current_thread")]
async fn monitor_on_already_dead_actor_fires_immediately() {
    let ctx = Context::new().expect("context");
    let root = ctx.actor(Some(b"root"), false).expect("root");
    let watcher = ctx.actor(Some(b"watcher"), false).expect("watcher");
    let target = ctx.actor(Some(b"target"), false).expect("target");

    let mut root_poll = ctx.poller().expect("poller");
    root_poll.subscribe(0, &root).expect("subscribe");
    let mut watcher_poll = ctx.poller().expect("poller");
    watcher_poll.subscribe(0, &watcher).expect("subscribe");
    let mut target_poll = ctx.poller().expect("poller");
    target_poll.subscribe(0, &target).expect("subscribe");

    connect(
        &root,
        &mut root_poll,
        "inproc://rs-mond-w",
        &watcher,
        &mut watcher_poll,
    )
    .await;
    connect(
        &root,
        &mut root_poll,
        "inproc://rs-mond-t",
        &target,
        &mut target_poll,
    )
    .await;

    target.die(b"boom");
    // root is target's parent, so it gets the connection-failure notification;
    // draining it confirms root recorded target as dead before we subscribe.
    let (_, down) = root_poll.recv().await.expect("root sees target death");
    assert_eq!(down[0].as_bytes(), b"target");

    let _handle = watcher.monitor(b"target", &[b"DOWN"], 0).expect("monitor");
    let (_, msg) = watcher_poll
        .recv()
        .await
        .expect("monitor fires immediately");
    assert_eq!(msg[0].as_bytes(), b"DOWN");
    assert_eq!(msg[1].as_bytes(), b"target");
    assert_eq!(msg[2].as_bytes(), b"actor died");
}
