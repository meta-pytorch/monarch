/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module implements mailbox support for local proc management.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::RwLock;

use async_trait::async_trait;
use hyperactor::PortHandle;
use hyperactor::Uid;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelError;
use hyperactor::mailbox::DeliveryFailure;
use hyperactor::mailbox::MailboxClient;
use hyperactor::mailbox::MailboxSender;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::TransportFailure;
use hyperactor::mailbox::TransportFailureReason;
use hyperactor::mailbox::Undeliverable;
use hyperactor::mailbox::UndeliverableReason;
use hyperactor::port::ControlPort;
use hyperactor::port::Port;

/// Dials local procs directly through a configured socket directory.
///
/// A same-host destination is direct-dialable when its deterministic socket
/// exists in `socket_dir`. Other destinations, and every actor status message,
/// are routed through the backend sender to the host gateway, which answers
/// status queries for a proc that has exited.
#[derive(Debug)]
pub(crate) struct LocalProcDialer {
    local_addr: ChannelAddr,
    socket_dir: PathBuf,
    backend_sender: MailboxClient,
    local_senders: RwLock<HashMap<Uid, MailboxClient>>,
}

impl LocalProcDialer {
    /// Create a new local proc dialer. Procs with a destination address of
    /// `local_addr` are dialed through direct sockets when present in
    /// `socket_dir`. Messages to other procs are forwarded through the backend
    /// sender.
    pub(crate) fn new(
        local_addr: ChannelAddr,
        socket_dir: PathBuf,
        backend_sender: MailboxClient,
    ) -> Self {
        Self {
            local_addr,
            socket_dir,
            backend_sender,
            local_senders: RwLock::new(HashMap::new()),
        }
    }

    fn return_dial_failure(
        error: &ChannelError,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        let destination = envelope.dest().clone();
        let failure = DeliveryFailure::new(UndeliverableReason::Transport(TransportFailure::new(
            destination.clone(),
            TransportFailureReason::DialFailed {
                addr: destination.location().addr().clone(),
                error: error.to_string(),
            },
        )));
        envelope.undeliverable(failure, return_handle);
    }
}

#[async_trait]
impl MailboxSender for LocalProcDialer {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        let proc_ref = envelope.dest().actor_addr().proc_addr();
        let addr = proc_ref.addr();
        if addr == &self.local_addr && envelope.dest().port() != Port::control(ControlPort::Status)
        {
            let key = proc_ref.uid().clone();
            {
                let senders = self.local_senders.read().unwrap();
                if let Some(sender) = senders.get(&key) {
                    sender.post_unchecked(envelope, return_handle);
                    return;
                }
            }

            // Host-owned service procs do not bind deterministic child sockets.
            if let Ok((local_addr, path)) = super::local_proc_addr(&self.socket_dir, proc_ref.id())
                && path.exists()
            {
                let mut senders = self.local_senders.write().unwrap();
                if let Some(sender) = senders.get(&key) {
                    sender.post_unchecked(envelope, return_handle);
                    return;
                }
                match MailboxClient::dial(local_addr) {
                    Ok(sender) => {
                        sender.post_unchecked(envelope, return_handle);
                        senders.insert(key, sender);
                    }
                    Err(error) => Self::return_dial_failure(&error, envelope, return_handle),
                }
                return;
            }
        }

        self.backend_sender.post_unchecked(envelope, return_handle);
    }

    async fn flush(&self) -> Result<(), anyhow::Error> {
        // We can't hold the RwLockReadGuard across an await, so flush
        // the backend sender (the primary outbound path) only.
        // Local senders are unix-socket MailboxClients whose flush
        // semantics are equivalent.
        self.backend_sender.flush().await
    }
}

#[cfg(test)]
mod tests {
    use hyperactor::Mailbox;
    use hyperactor::channel::ChannelAddr;
    use hyperactor::channel::ChannelRx;
    use hyperactor::channel::ChannelTransport;
    use hyperactor::channel::Rx;
    use hyperactor::channel::{self};
    use hyperactor::id::Label;
    use hyperactor::testing::ids::test_actor_id;
    use hyperactor_config::Flattrs;

    use super::*;
    use crate::bootstrap::local_proc_addr;
    use crate::host::SERVICE_PROC_NAME;
    use crate::mesh_id::ResourceId;

    fn recording_dialer(
        local_addr: ChannelAddr,
        socket_dir: PathBuf,
    ) -> (LocalProcDialer, ChannelRx<MessageEnvelope>) {
        let (backend_addr, backend_rx) =
            channel::serve::<MessageEnvelope>(ChannelTransport::Unix.any()).unwrap();
        let dialer = LocalProcDialer::new(
            local_addr,
            socket_dir,
            MailboxClient::dial(backend_addr).unwrap(),
        );
        (dialer, backend_rx)
    }

    #[tokio::test]
    async fn test_proc_dialer_routes_between_local_proc_sockets_in_both_directions() {
        let dir = tempfile::tempdir().unwrap();
        let local_addr: ChannelAddr = "tcp:3.4.5.6:123".parse().unwrap();
        let first = hyperactor::ProcAddr::instance(local_addr.clone(), "first");
        let second = hyperactor::ProcAddr::instance(local_addr.clone(), "second");
        let (first_serve, _) = local_proc_addr(dir.path(), first.id()).unwrap();
        let (_first_addr, mut first_rx) = channel::serve::<MessageEnvelope>(first_serve).unwrap();
        let (second_serve, _) = local_proc_addr(dir.path(), second.id()).unwrap();
        let (_second_addr, mut second_rx) =
            channel::serve::<MessageEnvelope>(second_serve).unwrap();
        let (proc_dialer, _backend_rx) =
            recording_dialer(local_addr.clone(), dir.path().to_owned());

        let first_actor_id = first.actor_addr("actor");
        let second_actor_id = second.actor_addr("actor");
        let (return_handle, _return_rx) = Mailbox::new(test_actor_id("world_0", "proc"))
            .open_port::<Undeliverable<MessageEnvelope>>();

        let envelope = MessageEnvelope::new(
            first_actor_id.clone(),
            second_actor_id.port_addr(0.into()),
            wirevalue::Any::serialize(&()).unwrap(),
            Flattrs::new(),
        );
        proc_dialer.post(envelope, return_handle.clone());
        assert_eq!(second_rx.recv().await.unwrap().sender(), &first_actor_id);

        let first_via = hyperactor::ProcAddr::new(
            first.id().clone(),
            hyperactor::Location::from(local_addr.clone()).with_via(first.id().uid().clone()),
        );
        let envelope = MessageEnvelope::new(
            second_actor_id.clone(),
            first_via.actor_addr("actor").port_addr(0.into()),
            wirevalue::Any::serialize(&()).unwrap(),
            Flattrs::new(),
        );
        proc_dialer.post(envelope, return_handle);
        assert_eq!(first_rx.recv().await.unwrap().sender(), &second_actor_id);
    }

    #[tokio::test]
    async fn test_proc_dialer_routes_service_proc_through_backend() {
        let dir = tempfile::tempdir().unwrap();
        let local_addr: ChannelAddr = "tcp:3.4.5.6:123".parse().unwrap();
        let sender = hyperactor::ProcAddr::instance(local_addr.clone(), "sender");
        let (proc_dialer, mut backend_rx) =
            recording_dialer(local_addr.clone(), dir.path().to_owned());
        let (return_handle, _return_rx) = Mailbox::new(test_actor_id("world_0", "proc"))
            .open_port::<Undeliverable<MessageEnvelope>>();

        let service_proc_id = hyperactor::ProcId::instance(Label::strip(SERVICE_PROC_NAME));
        let service_proc = hyperactor::ProcAddr::new(service_proc_id, local_addr.clone().into());

        let envelope = MessageEnvelope::new(
            sender.actor_addr("actor"),
            service_proc.actor_addr("actor").port_addr(0.into()),
            wirevalue::Any::serialize(&()).unwrap(),
            Flattrs::new(),
        );
        proc_dialer.post(envelope, return_handle.clone());

        let forwarded = backend_rx.recv().await.unwrap();
        assert_eq!(forwarded.dest().actor_addr().proc_addr(), service_proc);
    }

    /// Status messages for a direct-dialable sibling go through the host
    /// gateway, which answers for the sibling once it has exited; other
    /// messages are still dialed directly.
    #[tokio::test]
    async fn test_proc_dialer_routes_status_messages_through_backend() {
        let dir = tempfile::tempdir().unwrap();
        let local_addr: ChannelAddr = "tcp:3.4.5.6:123".parse().unwrap();
        let sender = hyperactor::ProcAddr::instance(local_addr.clone(), "sender");
        let sibling = hyperactor::ProcAddr::instance(local_addr.clone(), "sibling");
        let (sibling_serve, _) = local_proc_addr(dir.path(), sibling.id()).unwrap();
        let (_sibling_addr, mut sibling_rx) =
            channel::serve::<MessageEnvelope>(sibling_serve).unwrap();
        let (proc_dialer, mut backend_rx) =
            recording_dialer(local_addr.clone(), dir.path().to_owned());
        let (return_handle, _return_rx) = Mailbox::new(test_actor_id("world_0", "proc"))
            .open_port::<Undeliverable<MessageEnvelope>>();

        let actor = sibling.actor_addr("actor");
        for port in [Port::control(ControlPort::Status), Port::from(0u64)] {
            proc_dialer.post(
                MessageEnvelope::new(
                    sender.actor_addr("actor"),
                    actor.port_addr(port),
                    wirevalue::Any::serialize(&()).unwrap(),
                    Flattrs::new(),
                ),
                return_handle.clone(),
            );
        }

        let timeout = std::time::Duration::from_secs(5);
        let forwarded = tokio::time::timeout(timeout, backend_rx.recv())
            .await
            .expect("status message should reach the backend")
            .unwrap();
        assert_eq!(
            forwarded.dest(),
            &actor.port_addr(Port::control(ControlPort::Status))
        );
        let delivered = tokio::time::timeout(timeout, sibling_rx.recv())
            .await
            .expect("other message should reach the sibling")
            .unwrap();
        assert_eq!(delivered.dest(), &actor.port_addr(Port::from(0u64)));
    }

    #[tokio::test]
    async fn test_proc_dialer_dials_singleton_named_local_proc_socket() {
        let dir = tempfile::tempdir().unwrap();
        let local_addr: ChannelAddr = "tcp:3.4.5.6:123".parse().unwrap();
        let sender = hyperactor::ProcAddr::instance(local_addr.clone(), "sender");
        let singleton = hyperactor::ProcAddr::singleton(local_addr.clone(), "singleton");
        let (singleton_addr, _) = local_proc_addr(dir.path(), singleton.id()).unwrap();
        let (_singleton_addr, mut singleton_rx) =
            channel::serve::<MessageEnvelope>(singleton_addr).unwrap();
        let (proc_dialer, _backend_rx) = recording_dialer(local_addr, dir.path().to_owned());
        let (return_handle, _return_rx) = Mailbox::new(test_actor_id("world_0", "proc"))
            .open_port::<Undeliverable<MessageEnvelope>>();

        let envelope = MessageEnvelope::new(
            sender.actor_addr("actor"),
            singleton.actor_addr("actor").port_addr(0.into()),
            wirevalue::Any::serialize(&()).unwrap(),
            Flattrs::new(),
        );
        proc_dialer.post(envelope, return_handle);

        let delivered = singleton_rx.recv().await.unwrap();
        assert_eq!(delivered.dest().actor_addr().proc_addr(), singleton);
    }

    #[tokio::test]
    async fn test_proc_dialer_uses_backend_for_missing_or_remote_proc() {
        let dir = tempfile::tempdir().unwrap();
        let local_addr: ChannelAddr = "tcp:3.4.5.6:123".parse().unwrap();
        let sender = hyperactor::ProcAddr::instance(local_addr.clone(), "sender");
        let missing = hyperactor::ProcAddr::instance(local_addr.clone(), "missing");
        let (proc_dialer, mut backend_rx) = recording_dialer(local_addr, dir.path().to_owned());
        let (return_handle, _return_rx) = Mailbox::new(test_actor_id("world_0", "proc"))
            .open_port::<Undeliverable<MessageEnvelope>>();

        let envelope = MessageEnvelope::new(
            sender.actor_addr("actor"),
            missing.actor_addr("actor").port_addr(0.into()),
            wirevalue::Any::serialize(&()).unwrap(),
            Flattrs::new(),
        );
        proc_dialer.post(envelope, return_handle.clone());
        let forwarded = backend_rx.recv().await.unwrap();
        assert_eq!(forwarded.dest().actor_addr().proc_addr(), missing);

        let remote = ResourceId::proc_addr_from_name(
            "tcp:4.5.6.7:456".parse::<ChannelAddr>().unwrap(),
            "remote",
        );
        let envelope = MessageEnvelope::new(
            sender.actor_addr("actor"),
            remote.actor_addr("actor").port_addr(0.into()),
            wirevalue::Any::serialize(&()).unwrap(),
            Flattrs::new(),
        );
        proc_dialer.post(envelope, return_handle);
        let forwarded = backend_rx.recv().await.unwrap();
        assert_eq!(forwarded.dest().actor_addr().proc_addr(), remote);
    }
}
