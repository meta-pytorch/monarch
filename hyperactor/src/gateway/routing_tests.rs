/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use tokio::sync::mpsc;

use super::*;
use crate::Label;
use crate::mailbox::monitored_return_handle;
use crate::testing::ids::test_actor_id;

#[derive(Clone)]
struct RecordingSender(mpsc::UnboundedSender<MessageEnvelope>);

#[async_trait]
impl crate::mailbox::MailboxSender for RecordingSender {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        _return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        self.0.send(envelope).unwrap();
    }
}

fn key(name: &str) -> Uid {
    Uid::singleton(Label::strip(name))
}

fn envelope(location: Location) -> MessageEnvelope {
    MessageEnvelope::serialize(
        test_actor_id("sender", "client"),
        ProcAddr::new(ProcId::anonymous(), location)
            .actor_addr("worker")
            .port_addr(crate::port::Port::from(0)),
        &42u64,
        Default::default(),
    )
    .unwrap()
}

#[tokio::test]
async fn newest_peer_wins_and_old_guard_cannot_remove_it() {
    let gateway = Gateway::isolated();
    let (old_tx, mut old_rx) = mpsc::unbounded_channel();
    let old = gateway.attach_peer(key("worker"), RecordingSender(old_tx).into_boxed());
    let (new_tx, mut new_rx) = mpsc::unbounded_channel();
    let new = gateway.attach_peer(key("worker"), RecordingSender(new_tx).into_boxed());
    assert!(old.replaced.is_cancelled());
    drop(old);
    let message = envelope(gateway.default_location().append_via(key("worker")));
    let dest = message.dest().clone();
    gateway.post(message, monitored_return_handle());
    let received = new_rx.recv().await.unwrap();
    assert_eq!(received.dest(), &dest);
    assert_eq!(received.route_cursor(), Some(1));
    assert!(received.route_complete());
    assert!(old_rx.recv().await.is_none());
    drop(new);
    assert!(gateway.inner.peers.read().unwrap().is_empty());
}

#[tokio::test]
async fn prepared_cursor_is_not_reinitialized_at_an_alias() {
    let gateway = Gateway::isolated();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let _peer = gateway.attach_peer(key("child"), RecordingSender(tx).into_boxed());
    let location = gateway
        .default_location()
        .append_via(key("rack"))
        .append_via(key("child"));
    let mut message = envelope(location);
    message.set_route_cursor(1);
    gateway.post(message, monitored_return_handle());
    assert_eq!(rx.recv().await.unwrap().route_cursor(), Some(2));
}

#[test]
fn cancelled_handshake_releases_reservation() {
    let gateway = Gateway::isolated();
    let first = gateway.reserve_upstream().unwrap();
    assert!(gateway.reserve_upstream().is_err());
    drop(first);
    assert!(gateway.reserve_upstream().is_ok());
}

#[tokio::test]
async fn terminal_slot_and_intermediate_misses_do_not_forward() {
    let (tx, mut forwarded) = mpsc::unbounded_channel();
    let gateway = Gateway::configured(
        channel::reserve_local_addr().into(),
        RecordingSender(tx).into_boxed(),
    );
    let scratch = Proc::isolated();
    let client = scratch.client("failures");
    let (returns, mut failures) = client.open_port::<Undeliverable<MessageEnvelope>>();
    let terminal = gateway.default_location().append_via(key("missing"));
    let intermediate = terminal.clone().append_via(key("child"));
    for (location, final_slot) in [(terminal, true), (intermediate, false)] {
        gateway.post(envelope(location), returns.clone());
        let Undeliverable::Returned(message) = failures.recv().await.unwrap() else {
            panic!("expected returned route failure");
        };
        let failure = &message.root_delivery_failure().unwrap().kind;
        if final_slot {
            assert_eq!(
                failure.as_invalid_reference().unwrap().reason,
                crate::mailbox::InvalidReferenceReason::RouteNotExist
            );
        } else {
            assert!(matches!(
                failure,
                crate::mailbox::DeliveryFailureKind::Undeliverable(UndeliverableReason::Transport(
                    TransportFailure {
                        reason: TransportFailureReason::NoRoute,
                        ..
                    }
                ))
            ));
        }
        assert!(forwarded.try_recv().is_err());
    }
}

#[tokio::test]
async fn live_attachment_replacement_closes_old_session() {
    let core = Gateway::new();
    let mut listener = core
        .serve_duplex(ChannelAddr::any(ChannelTransport::Unix))
        .unwrap();
    let addr = core.default_location().addr().clone();
    let old = Gateway::isolated();
    let old_handle = old
        .serve_via_with_key(addr.clone(), Some(key("worker")))
        .await
        .unwrap();
    let old_location = old.default_location();
    let new = Gateway::isolated();
    let mut new_handle = new
        .serve_via_with_key(addr, Some(key("worker")))
        .await
        .unwrap();
    assert_eq!(new.default_location(), old_location);
    tokio::time::timeout(Duration::from_secs(5), old_handle.join())
        .await
        .unwrap()
        .unwrap();
    assert!(
        core.inner
            .peers
            .read()
            .unwrap()
            .contains_key(&key("worker"))
    );
    new_handle.stop("test");
    new_handle.join().await.unwrap();
    listener.stop("test");
    listener.join().await.unwrap();
}
