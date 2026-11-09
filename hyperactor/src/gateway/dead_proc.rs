/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Dead-proc routes.
//!
//! When a proc behind a peer route ends, the owner of the route (the host
//! that spawned the proc) replaces it with a dead-proc route by calling
//! [`PeerAttachGuard::mark_dead`] with the terminal status of the proc's
//! actors. The gateway then answers for every actor behind the peer, known
//! or not:
//!
//! * [`StatusMessage::GetStatus`] gets the status, and its delivery-progress
//!   request gets [`DeliveryProgressResponse::ActorGone`], as for a
//!   terminated actor on a live proc;
//! * [`StatusMessage::SubscribeTerminal`] gets the status at once;
//! * [`StatusMessage::UnsubscribeTerminal`] is dropped;
//! * any other message is returned undeliverable, with the status as the
//!   reason.
//!
//! A caller thus learns that a proc has ended on its next status query
//! instead of after a silence timeout. A frozen proc keeps its live route,
//! so it is still detected by silence.

use std::sync::Weak;

use async_trait::async_trait;
use hyperactor_config::Flattrs;

use super::GatewayState;
use super::PeerAttachGuard;
use crate::ActorAddr;
use crate::actor::ActorStatus;
use crate::mailbox::BoxedMailboxSender;
use crate::mailbox::DeliveryFailure;
use crate::mailbox::MailboxSender;
use crate::mailbox::MessageEnvelope;
use crate::mailbox::PortHandle;
use crate::mailbox::PortSender as _;
use crate::mailbox::TransportFailure;
use crate::mailbox::TransportFailureReason;
use crate::mailbox::Undeliverable;
use crate::mailbox::UndeliverableReason;
use crate::mailbox::monitored_return_handle;
use crate::ordering::SEQ_INFO;
use crate::ordering::SeqInfo;
use crate::port::ControlPort;
use crate::proc::DeliveryProgressResponse;
use crate::proc::StatusMessage;

impl PeerAttachGuard {
    /// Replace this peer's route with a dead-proc route, which answers for
    /// every actor behind the peer with the terminal `status`: status
    /// queries get `status`, and other messages are returned undeliverable.
    /// Dropping the guard removes the dead-proc route, as it would have
    /// removed the live one. A guard whose route was replaced by a newer
    /// attachment leaves the replacement in place.
    ///
    /// Panics if `status` is not terminal.
    pub fn mark_dead(&self, status: ActorStatus) {
        assert!(
            status.is_terminal(),
            "dead-proc route requires a terminal status, got {status}"
        );
        let Some(state) = self.gateway.upgrade() else {
            return;
        };
        let mut peers = state.peers.write().unwrap();
        if let Some(peer) = peers
            .get_mut(&self.uid)
            .filter(|peer| peer.registration == self.registration)
        {
            peer.sender = BoxedMailboxSender::new(DeadProc {
                gateway: self.gateway.clone(),
                status,
            });
        }
    }
}

/// The route for a peer whose proc has ended. See the module documentation.
struct DeadProc {
    /// The gateway that holds this route, through which replies are sent.
    gateway: Weak<GatewayState>,
    /// The terminal status of every actor behind the peer.
    status: ActorStatus,
}

impl DeadProc {
    /// Answer `message`, sent to `actor`'s status port, as a proc answers
    /// for a terminated actor.
    fn answer(&self, actor: ActorAddr, message: StatusMessage) {
        match message {
            StatusMessage::GetStatus { reply, delivery } => {
                if let Err(err) = self.gateway.serialize_and_send_once(
                    reply,
                    Some(self.status.clone()),
                    monitored_return_handle(),
                ) {
                    tracing::error!("dead-proc status reply failed: {err}");
                }
                if let Some((_, _, reply)) = delivery
                    && let Err(err) = self.gateway.serialize_and_send_once(
                        reply,
                        DeliveryProgressResponse::ActorGone,
                        monitored_return_handle(),
                    )
                {
                    tracing::error!("dead-proc delivery progress reply failed: {err}");
                }
            }
            StatusMessage::SubscribeTerminal { subscriber, .. } => {
                let envelope = MessageEnvelope::serialize(
                    actor,
                    subscriber.into_port_addr(),
                    &Some(self.status.clone()),
                    Flattrs::new(),
                );
                let Ok(mut envelope) = envelope else {
                    tracing::error!("failed to serialize dead-proc terminal status notification");
                    return;
                };
                envelope.set_header(SEQ_INFO, SeqInfo::Direct);
                envelope.set_return_undeliverable(false);
                self.gateway.post(envelope, monitored_return_handle());
            }
            StatusMessage::UnsubscribeTerminal { .. } => {}
        }
    }
}

#[async_trait]
impl MailboxSender for DeadProc {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        if envelope.dest().is_control_port_kind(ControlPort::Status)
            && let Ok(message) = envelope.deserialized::<StatusMessage>()
        {
            return self.answer(envelope.dest().actor_addr(), message);
        }
        // A transport failure, rather than an invalid reference, keeps
        // senders' handling unchanged: before, a message to a dead proc
        // also failed in transport, only later.
        let failure = DeliveryFailure::new(UndeliverableReason::Transport(TransportFailure::new(
            envelope.dest().clone(),
            TransportFailureReason::LinkUnavailable(format!("proc ended: {}", self.status)),
        )));
        envelope.undeliverable(failure, return_handle);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use timed_test::async_timed_test;
    use tokio::sync::mpsc;
    use tokio::time;
    use uuid::Uuid;

    use super::*;
    use crate::Endpoint as _;
    use crate::Gateway;
    use crate::Label;
    use crate::Proc;
    use crate::ProcAddr;
    use crate::ProcId;
    use crate::client::Client;
    use crate::id::Uid;
    use crate::mailbox::DeliveryFailureKind;
    use crate::monitor::EndpointId;
    use crate::port::Port;

    /// Records the envelopes posted to a live peer route.
    struct RecordingSender(mpsc::UnboundedSender<MessageEnvelope>);

    #[async_trait]
    impl MailboxSender for RecordingSender {
        fn post_unchecked(
            &self,
            envelope: MessageEnvelope,
            _return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
        ) {
            self.0
                .send(envelope)
                .expect("recording sender should be open");
        }
    }

    /// Updates another peer while dispatching to the dead-proc responder.
    struct PeerUpdatingSender {
        gateway: Weak<GatewayState>,
        sender: BoxedMailboxSender,
        update: Weak<PeerAttachGuard>,
    }

    #[async_trait]
    impl MailboxSender for PeerUpdatingSender {
        fn post_unchecked(
            &self,
            envelope: MessageEnvelope,
            return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
        ) {
            let gateway = self.gateway.upgrade().unwrap();
            // Fail immediately if dispatch retained the read lock, rather
            // than hanging this test behind the queued writer.
            drop(
                gateway
                    .peers
                    .try_write()
                    .expect("peer dispatch must release its routing lock"),
            );
            let peer = self
                .update
                .upgrade()
                .expect("update peer should stay attached");
            thread::scope(|scope| {
                let update = scope.spawn(|| peer.mark_dead(killed()));
                self.sender.post_unchecked(envelope, return_handle);
                update.join().expect("peer update should complete");
            });
        }
    }

    /// The terminal status recorded for the dead proc in these tests.
    fn killed() -> ActorStatus {
        ActorStatus::generic_failure("the process this actor was running on failed: Killed(sig=9)")
    }

    /// A gateway with a client proc attached in-process.
    fn gateway_with_client() -> (Gateway, Proc, Client) {
        let gateway = Gateway::isolated();
        let proc = Proc::builder()
            .proc_id(ProcId::instance(Label::strip("client")))
            .shared_gateway(gateway.clone())
            .build()
            .unwrap();
        let client = proc.client("client");
        (gateway, proc, client)
    }

    /// Attach a recording peer under a fresh uid.
    fn attach_recording_peer(
        gateway: &Gateway,
    ) -> (
        Uid,
        PeerAttachGuard,
        mpsc::UnboundedReceiver<MessageEnvelope>,
    ) {
        let uid = Uid::anonymous();
        let (tx, rx) = mpsc::unbounded_channel();
        let guard = gateway.attach_peer(uid.clone(), BoxedMailboxSender::new(RecordingSender(tx)));
        (uid, guard, rx)
    }

    /// An actor on the proc behind peer `uid`, addressed the way a host
    /// addresses the procs it spawns.
    fn actor_behind(gateway: &Gateway, uid: &Uid, name: &str) -> ActorAddr {
        ProcAddr::new(
            ProcId::instance(Label::strip("worker")),
            gateway.default_location().append_via(uid.clone()),
        )
        .actor_addr(name)
    }

    /// Send `GetStatus` with a delivery-progress request to `actor`, and
    /// return both replies, or `None` if they do not arrive in time.
    async fn get_status(
        client: &Client,
        actor: &ActorAddr,
    ) -> Option<(Option<ActorStatus>, DeliveryProgressResponse)> {
        let (reply, status_rx) = client.open_once_port::<Option<ActorStatus>>();
        let (delivery_reply, delivery_rx) = client.open_once_port::<DeliveryProgressResponse>();
        actor.status_port().post(
            client,
            StatusMessage::GetStatus {
                reply: reply.bind(),
                delivery: Some((Uuid::new_v4(), EndpointId::Handler, delivery_reply.bind())),
            },
        );
        let replies = async { (status_rx.recv().await, delivery_rx.recv().await) };
        match time::timeout(Duration::from_secs(1), replies).await {
            Ok((Ok(status), Ok(delivery))) => Some((status, delivery)),
            _ => None,
        }
    }

    #[async_timed_test(timeout_secs = 30)]
    async fn test_dead_proc_replies_during_peer_update() {
        let gateway = Gateway::isolated();
        gateway.set_default_location(gateway.default_location().append_via(gateway.uid().clone()));
        let proc = Proc::builder()
            .shared_gateway(gateway.clone())
            .build()
            .unwrap();
        let client = proc.client("client");
        let (uid, guard, _rx) = attach_recording_peer(&gateway);
        let (_update_uid, update, _update_rx) = attach_recording_peer(&gateway);
        let update = Arc::new(update);
        guard.mark_dead(killed());
        if let Some(peer) = gateway.inner.peers.write().unwrap().get_mut(&uid) {
            peer.sender = BoxedMailboxSender::new(PeerUpdatingSender {
                gateway: guard.gateway.clone(),
                sender: peer.sender.clone(),
                update: Arc::downgrade(&update),
            });
        }

        assert_eq!(
            get_status(&client, &actor_behind(&gateway, &uid, "actor")).await,
            Some((Some(killed()), DeliveryProgressResponse::ActorGone)),
            "status and delivery replies should survive a concurrent peer update",
        );
    }

    /// Status queries for any actor behind a dead peer are answered with the
    /// recorded status, while a live peer keeps receiving its traffic.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_dead_proc_answers_status_for_any_actor() {
        let (gateway, _proc, client) = gateway_with_client();
        let (dead_uid, dead_guard, _dead_rx) = attach_recording_peer(&gateway);
        let (live_uid, _live_guard, mut live_rx) = attach_recording_peer(&gateway);
        dead_guard.mark_dead(killed());

        for name in ["actor", "unknown"] {
            let actor = actor_behind(&gateway, &dead_uid, name);
            assert_eq!(
                get_status(&client, &actor).await,
                Some((Some(killed()), DeliveryProgressResponse::ActorGone)),
                "status query for {name} should be answered by the dead-proc route",
            );
        }

        let live = actor_behind(&gateway, &live_uid, "actor");
        assert_eq!(
            get_status(&client, &live).await,
            None,
            "a live peer's status query must not be answered by the gateway",
        );
        let forwarded = live_rx
            .try_recv()
            .expect("live peer should receive its query");
        assert_eq!(forwarded.dest(), &live.status_port().into_port_addr());
    }

    /// A terminal-status subscription to an actor behind a dead peer is
    /// answered at once, and its cancellation is dropped.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_dead_proc_answers_terminal_subscription() {
        let (gateway, _proc, client) = gateway_with_client();
        let (uid, guard, _rx) = attach_recording_peer(&gateway);
        guard.mark_dead(killed());
        let actor = actor_behind(&gateway, &uid, "actor");

        let (subscriber, mut status_rx) = client.open_port::<Option<ActorStatus>>();
        let subscription_id = Uuid::new_v4();
        actor.status_port().post(
            &client,
            StatusMessage::UnsubscribeTerminal { subscription_id },
        );
        actor.status_port().post(
            &client,
            StatusMessage::SubscribeTerminal {
                subscription_id: Uuid::new_v4(),
                subscriber: subscriber.bind(),
            },
        );
        let status = time::timeout(Duration::from_secs(5), status_rx.recv())
            .await
            .expect("terminal status notification should arrive")
            .unwrap();
        assert_eq!(status, Some(killed()));
        assert!(
            time::timeout(Duration::from_millis(100), status_rx.recv())
                .await
                .is_err(),
            "only the subscription should be answered",
        );
    }

    /// Other messages for an actor behind a dead peer are returned
    /// undeliverable, with the recorded status as the reason.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_dead_proc_returns_other_messages() {
        let (gateway, _proc, client) = gateway_with_client();
        let (uid, guard, _rx) = attach_recording_peer(&gateway);
        guard.mark_dead(killed());
        let dest = actor_behind(&gateway, &uid, "actor").port_addr(Port::from(0u64));

        let (return_handle, mut return_rx) = client.open_port::<Undeliverable<MessageEnvelope>>();
        gateway.post(
            MessageEnvelope::serialize(
                client.self_addr().clone(),
                dest.clone(),
                &7u64,
                Flattrs::new(),
            )
            .unwrap(),
            return_handle,
        );
        let returned = time::timeout(Duration::from_secs(5), return_rx.recv())
            .await
            .expect("message should be returned")
            .unwrap();
        let Some(DeliveryFailureKind::Undeliverable(UndeliverableReason::Transport(failure))) =
            returned
                .root_delivery_failure()
                .map(|failure| &failure.kind)
        else {
            panic!(
                "expected a transport failure, got {:?}",
                returned.root_delivery_failure()
            );
        };
        assert_eq!(
            failure.reason,
            TransportFailureReason::LinkUnavailable(format!("proc ended: {}", killed())),
        );
        assert_eq!(returned.into_message().unwrap().dest(), &dest);
    }

    /// Dropping the peer guard releases the dead-proc route.
    #[async_timed_test(timeout_secs = 30)]
    async fn test_dead_proc_route_released_with_guard() {
        let (gateway, _proc, client) = gateway_with_client();
        let (uid, guard, _rx) = attach_recording_peer(&gateway);
        guard.mark_dead(killed());
        let actor = actor_behind(&gateway, &uid, "actor");
        assert!(get_status(&client, &actor).await.is_some());

        drop(guard);
        assert!(!gateway.inner.peers.read().unwrap().contains_key(&uid));
    }
}
