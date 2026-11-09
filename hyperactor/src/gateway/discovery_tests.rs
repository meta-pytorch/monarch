/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::time::Duration;

use timed_test::async_timed_test;

use super::*;
use crate::ActorRef;
use crate::Endpoint as _;
use crate::Label;
use crate::PortAddr;
use crate::mailbox::InvalidReferenceReason;
use crate::mailbox::MailboxSender;
use crate::mailbox::PortSender as _;
use crate::mailbox::monitored_return_handle;
use crate::proc::control::ResolveProc;
use crate::proc::control::ResolveProcResult;
use crate::testing::pingpong::PingPongActor;
use crate::testing::pingpong::PingPongMessage;

fn endpoint(gateway: &Gateway) -> (Proc, GatewayServeHandle, Location) {
    let listener = gateway
        .serve_mux_with_listener(
            ChannelAddr::from_zmq_url("tcp://127.0.0.1:0").unwrap(),
            None,
        )
        .unwrap();
    let location = gateway.default_location();
    let owner = service_proc(gateway);
    listener.register_proc_owner(&owner).unwrap();
    (owner, listener, location)
}

fn service_proc(gateway: &Gateway) -> Proc {
    Proc::builder()
        .proc_id(ProcId::instance(Label::strip("service")))
        .shared_gateway(gateway.clone())
        .build()
        .unwrap()
}

#[async_timed_test(timeout_secs = 30)]
async fn self_resolution_selects_exact_listener_not_default_or_local_selector() {
    let gateway = Gateway::new();
    let (first, mut first_listener, first_location) = endpoint(&gateway);
    let (second, mut second_listener, second_location) = endpoint(&gateway);
    let selector = Proc::builder()
        .proc_id(ProcId::singleton(Label::strip("endpoint_owner")))
        .shared_gateway(gateway.clone())
        .build()
        .unwrap();
    let client = selector.client("runtime");
    let (one, two) = tokio::join!(
        gateway.resolve_endpoint_proc(&client, first_location.clone()),
        gateway.resolve_endpoint_proc(&client, second_location.clone()),
    );
    assert_eq!(
        one.unwrap(),
        ProcAddr::new(first.proc_id().clone(), first_location.clone())
    );
    assert_eq!(
        two.unwrap(),
        ProcAddr::new(second.proc_id().clone(), second_location)
    );
    assert!(first_listener.register_proc_owner(&second).is_err());
    let foreign = service_proc(&Gateway::isolated());
    assert!(first_listener.register_proc_owner(&foreign).is_err());
    drop(first);
    assert!(
        gateway
            .resolve_endpoint_proc(&client, first_location.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("unavailable")
    );
    let replacement = service_proc(&gateway);
    first_listener.register_proc_owner(&replacement).unwrap();
    assert_eq!(
        gateway
            .resolve_endpoint_proc(&client, first_location)
            .await
            .unwrap()
            .id(),
        replacement.proc_id()
    );
    first_listener.stop("test complete");
    assert!(first_listener.register_proc_owner(&replacement).is_err());
    second_listener.stop("test complete");
    first_listener.join().await.unwrap();
    second_listener.join().await.unwrap();
}

#[async_timed_test(timeout_secs = 30)]
async fn attachment_resolves_upstream_and_unowned_terminal() {
    let core = Gateway::new();
    let (owner, mut listener, core_location) = endpoint(&core);
    let caller = Gateway::isolated();
    let mut attachment = caller
        .serve_via(core_location.addr().clone())
        .await
        .unwrap();
    let proc = service_proc(&caller);
    let client = proc.client("query");
    assert_eq!(
        caller
            .resolve_endpoint_proc(&client, core_location)
            .await
            .unwrap()
            .id(),
        owner.proc_id()
    );
    let core_client = owner.client("query_peer");
    assert!(
        core.resolve_endpoint_proc(&core_client, caller.default_location())
            .await
            .unwrap_err()
            .to_string()
            .contains("unavailable")
    );
    attachment.stop("test complete");
    listener.stop("test complete");
    attachment.join().await.unwrap();
    listener.join().await.unwrap();
}

#[async_timed_test(timeout_secs = 60)]
async fn stable_leaf_and_nested_leaf_restart_rediscovery_and_stale_delivery() {
    for nested in [false, true] {
        let core = Gateway::new();
        let (core_owner, mut core_listener, core_location) = endpoint(&core);
        let caller = Gateway::isolated();
        let mut caller_attachment = caller
            .serve_via(core_location.addr().clone())
            .await
            .unwrap();
        let caller_proc = service_proc(&caller);
        caller_attachment.register_proc_owner(&caller_proc).unwrap();
        let client = caller_proc.client("caller");

        let rack = Gateway::isolated();
        let mut rack_listener = rack
            .serve_duplex(ChannelAddr::any(ChannelTransport::Unix))
            .unwrap();
        let rack_address = rack.default_location().addr().clone();
        let mut rack_attachment = rack
            .serve_via_with_key(
                core_location.addr().clone(),
                Some(Uid::singleton(Label::strip("rack"))),
            )
            .await
            .unwrap();
        let rack_owner = service_proc(&rack);
        rack_attachment.register_proc_owner(&rack_owner).unwrap();
        rack_listener.register_proc_owner(&rack_owner).unwrap();
        let upstream = if nested {
            rack_address
        } else {
            core_location.addr().clone()
        };
        let key = Uid::singleton(Label::strip("worker"));
        let leaf = Gateway::isolated();
        // A different listener owner makes an accidental local-prefix answer
        // distinguishable from the attachment endpoint's owner.
        let (private_owner, mut private_listener, _) = endpoint(&leaf);
        let mut first_attachment = leaf
            .serve_via_with_key(upstream.clone(), Some(key.clone()))
            .await
            .unwrap();
        let first = service_proc(&leaf);
        first_attachment.register_proc_owner(&first).unwrap();
        let location = leaf.default_location();
        assert_eq!(location.path_len(), if nested { 2 } else { 1 });
        let first_id = first.proc_id().clone();
        let leaf_client = first.client("self_query");
        assert_eq!(
            leaf.resolve_endpoint_proc(&leaf_client, location.clone())
                .await
                .unwrap()
                .id(),
            &first_id
        );
        assert_ne!(private_owner.proc_id(), &first_id);
        assert_eq!(
            caller
                .resolve_endpoint_proc(&client, location.clone())
                .await
                .unwrap()
                .id(),
            &first_id
        );
        assert_eq!(
            core.resolve_endpoint_proc(&core_owner.client("query"), location.clone())
                .await
                .unwrap()
                .id(),
            &first_id
        );
        let old_actor = first.spawn_with_label("ping", PingPongActor::new(None, None, None));
        let old_ref: ActorRef<PingPongActor> = old_actor.bind();
        let (done, receiver) = client.open_once_port::<bool>();
        old_ref.post(&client, PingPongMessage(0, old_ref.clone(), done.bind()));
        assert!(receiver.recv().await.unwrap());

        // Keep the old process and session alive while replacing the stable
        // slot, so discovery must not resolve the old incarnation.
        let replacement_gateway = Gateway::isolated();
        let mut replacement_attachment = replacement_gateway
            .serve_via_with_key(upstream, Some(key))
            .await
            .unwrap();
        let replacement = service_proc(&replacement_gateway);
        replacement_attachment
            .register_proc_owner(&replacement)
            .unwrap();
        assert_eq!(replacement_gateway.default_location(), location);
        let resolved = caller
            .resolve_endpoint_proc(&client, location.clone())
            .await
            .unwrap();
        assert_eq!(resolved.id(), replacement.proc_id());
        assert_ne!(resolved.id(), &first_id);
        let new_actor = replacement.spawn_with_label("ping", PingPongActor::new(None, None, None));
        let new_ref: ActorRef<PingPongActor> = new_actor.bind();
        for _ in 0..3 {
            let (done, receiver) = client.open_once_port::<bool>();
            new_ref.post(&client, PingPongMessage(0, new_ref.clone(), done.bind()));
            assert!(receiver.recv().await.unwrap());
        }
        let (returns, mut failures) = client.open_port::<Undeliverable<MessageEnvelope>>();
        let sender = caller_proc.spawn(PingPongActor::new(Some(returns.bind()), None, None));
        let sender_ref: ActorRef<PingPongActor> = sender.bind();
        let (done, _receiver) = client.open_once_port::<bool>();
        sender_ref.post(&client, PingPongMessage(1, old_ref.clone(), done.bind()));
        let Undeliverable::Returned(returned) = failures.recv().await.unwrap() else {
            panic!("stale ref must be returned")
        };
        assert_eq!(returned.dest().proc_id(), &first_id);
        assert_eq!(
            returned
                .root_delivery_failure()
                .unwrap()
                .kind
                .as_invalid_reference()
                .unwrap()
                .reason,
            InvalidReferenceReason::ProcNotExist,
        );
        first_attachment.stop("old incarnation stopped");
        first_attachment.join().await.unwrap();
        // Old-session teardown must not remove the replacement's key.
        assert_eq!(
            caller
                .resolve_endpoint_proc(&client, location)
                .await
                .unwrap()
                .id(),
            replacement.proc_id()
        );
        old_actor.stop("test complete").unwrap();
        new_actor.stop("test complete").unwrap();
        sender.stop("test complete").unwrap();
        for handle in [
            &mut replacement_attachment,
            &mut private_listener,
            &mut rack_attachment,
            &mut rack_listener,
            &mut caller_attachment,
            &mut core_listener,
        ] {
            handle.stop("test complete");
        }
        replacement_attachment.join().await.unwrap();
        private_listener.join().await.unwrap();
        rack_attachment.join().await.unwrap();
        rack_listener.join().await.unwrap();
        caller_attachment.join().await.unwrap();
        core_listener.join().await.unwrap();
    }
}

#[async_timed_test(timeout_secs = 30)]
async fn broken_attachment_path_reports_route_cause_without_timing_out() {
    let core = Gateway::new();
    let (_owner, mut listener, core_location) = endpoint(&core);
    let caller = Gateway::isolated();
    let mut attachment = caller
        .serve_via(core_location.addr().clone())
        .await
        .unwrap();
    let proc = service_proc(&caller);
    let client = proc.client("query");
    let missing = core_location.append_via(Uid::singleton(Label::strip("missing")));
    let nested = missing
        .clone()
        .append_via(Uid::singleton(Label::strip("child")));
    for location in [missing, nested] {
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            caller.resolve_endpoint_proc_with_timeout(&client, location, Duration::from_secs(30)),
        )
        .await
        .expect("route failure must beat the query deadline")
        .unwrap_err();
        assert!(error.to_string().contains("no route"), "{error:#}");
        assert!(!error.to_string().contains("timed out"));
    }
    attachment.stop("test complete");
    attachment.join().await.unwrap();
    listener.stop("test complete");
    listener.join().await.unwrap();
}

#[tokio::test]
async fn local_query_transport_failure_preserves_its_cause() {
    let caller = Gateway::isolated();
    let proc = service_proc(&caller);
    let client = proc.client("query");
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        caller.resolve_endpoint_proc(&client, ChannelAddr::Local(999999).into()),
    )
    .await
    .expect("local failure must not wait for the query deadline")
    .unwrap_err();
    assert!(error.to_string().contains("no route"), "{error:#}");
    assert!(!error.to_string().contains("timed out"));
}

#[tokio::test]
async fn query_cancellation_timeout_and_reply_location_snapshot() {
    let (tx, mut messages) = tokio::sync::mpsc::unbounded_channel();
    let caller = Gateway::configured(
        ChannelAddr::Local(500).into(),
        BoxedMailboxSender::new(RecordingSender(tx)),
    );
    let first_serve = caller.add_server(caller.default_location());
    let proc = service_proc(&caller);
    let client = proc.client("query");
    let (returns, mut failures) = client.open_port::<Undeliverable<MessageEnvelope>>();
    let target = Location::from(ChannelAddr::Local(501));
    let mut query = Box::pin(caller.resolve_endpoint_proc(&client, target.clone()));
    let envelope = tokio::select! {
        envelope = messages.recv() => envelope.unwrap(),
        result = &mut query => panic!("unexpected early query result {result:?}"),
    };
    assert_eq!(envelope.route_cursor(), None);
    assert_eq!(envelope.sender(), client.self_addr());
    assert_eq!(envelope.dest().location(), &target);
    let request = envelope.deserialized::<ResolveProc>().unwrap();
    let cancelled = request.reply.port_addr().clone();
    drop(query);
    caller.post(
        MessageEnvelope::new_unknown(
            cancelled,
            wirevalue::Any::serialize(&ResolveProcResult::Unavailable).unwrap(),
        ),
        returns.clone(),
    );
    assert!(
        failures.recv().await.is_ok(),
        "cancelled query must release its one-shot port"
    );

    assert!(
        caller
            .resolve_endpoint_proc_with_timeout(&client, target.clone(), Duration::from_millis(10))
            .await
            .unwrap_err()
            .to_string()
            .contains("timed out")
    );
    let request = messages
        .recv()
        .await
        .unwrap()
        .deserialized::<ResolveProc>()
        .unwrap();
    caller.post(
        MessageEnvelope::new_unknown(
            request.reply.port_addr().clone(),
            wirevalue::Any::serialize(&ResolveProcResult::Unavailable).unwrap(),
        ),
        returns,
    );
    assert!(
        failures.recv().await.is_ok(),
        "timed-out query must release its one-shot port"
    );

    let old_location = proc.default_location();
    let mut query = Box::pin(caller.resolve_endpoint_proc(&client, target.clone()));
    let request = tokio::select! {
        envelope = messages.recv() => envelope.unwrap().deserialized::<ResolveProc>().unwrap(),
        result = &mut query => panic!("unexpected early query result {result:?}"),
    };
    let later_serve = caller.add_server(ChannelAddr::Local(502).into());
    assert_eq!(request.reply.port_addr().location(), &old_location);
    let id = ProcId::instance(Label::strip("resolved"));
    caller
        .serialize_and_send_once(
            request.reply,
            ResolveProcResult::Resolved(id.clone()),
            monitored_return_handle(),
        )
        .unwrap();
    assert_eq!(query.await.unwrap(), ProcAddr::new(id, target));
    assert!(messages.try_recv().is_err());
    caller.remove_active_serve(later_serve);
    caller.remove_active_serve(first_serve);
}

#[derive(Clone)]
struct RecordingSender(tokio::sync::mpsc::UnboundedSender<MessageEnvelope>);

#[async_trait::async_trait]
impl MailboxSender for RecordingSender {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        _return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        self.0.send(envelope).unwrap();
    }
}

#[async_timed_test(timeout_secs = 30)]
async fn alias_resolution_does_not_wait_for_actor_sequence_gap() {
    let gateway = Gateway::new();
    let mut listener = gateway
        .serve_mux_with_listener(ChannelAddr::from_zmq_url("tcp://0.0.0.0:0").unwrap(), None)
        .unwrap();
    let owner = service_proc(&gateway);
    listener.register_proc_owner(&owner).unwrap();
    let bind = gateway.default_location().addr().to_zmq_url();
    let dial = ChannelAddr::from_zmq_url(&bind.replace("0.0.0.0", "127.0.0.1")).unwrap();
    let alias = ChannelAddr::Alias {
        dial_to: Box::new(dial.clone()),
        bind_to: Box::new(gateway.default_location().addr().clone()),
    };
    let caller = Gateway::new();
    let mut caller_listener =
        Gateway::serve(&caller, ChannelAddr::any(ChannelTransport::Unix)).unwrap();
    let caller_proc = service_proc(&caller);
    let client = caller_proc.client("query");
    for addr in [dial.clone(), alias] {
        assert_eq!(
            caller
                .resolve_endpoint_proc(&client, addr.into())
                .await
                .unwrap()
                .id(),
            owner.proc_id()
        );
    }
    let target = owner.client("target");
    let (port, mut pending) = target.open_port::<u64>();
    let bound = port.bind().into_port_addr();
    let mut envelope = MessageEnvelope::serialize(
        client.self_addr().clone(),
        PortAddr::new(bound.id().clone(), dial.clone().into()),
        &2u64,
        Default::default(),
    )
    .unwrap();
    envelope.set_header(
        crate::ordering::SEQ_INFO,
        crate::ordering::SeqInfo::Session {
            session_id: uuid::Uuid::now_v7(),
            seq: 2,
        },
    );
    caller.post(envelope, monitored_return_handle());
    caller.flush().await.unwrap();
    assert!(pending.try_recv().unwrap().is_none());
    assert_eq!(
        caller
            .resolve_endpoint_proc(&client, dial.into())
            .await
            .unwrap()
            .id(),
        owner.proc_id()
    );
    assert!(pending.try_recv().unwrap().is_none());
    listener.stop("test complete");
    caller_listener.stop("test complete");
    listener.join().await.unwrap();
    caller_listener.join().await.unwrap();
}
