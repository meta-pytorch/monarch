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

/// Dials local procs directly through a configured socket directory.
///
/// A same-host destination is direct-dialable when its deterministic socket
/// exists in `socket_dir`. Other destinations are routed through the backend
/// sender to the host gateway.
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
                addr: destination.actor_addr().proc_addr().addr().clone(),
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
        if addr == &self.local_addr {
            let key = proc_ref.id().pseudo_uid();
            {
                let senders = self.local_senders.read().unwrap();
                if let Some(sender) = senders.get(&key) {
                    sender.post_unchecked(envelope, return_handle);
                    return;
                }
            }

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
    use hyperactor::channel::ChannelTransport;
    use hyperactor::channel::Rx;
    use hyperactor::channel::{self};
    use hyperactor::testing::ids::test_actor_id;
    use hyperactor_config::Flattrs;

    use super::*;
    use crate::bootstrap::local_proc_addr;
    use crate::mesh_id::ResourceId;

    #[tokio::test]
    async fn test_proc_dialer() {
        let dir = tempfile::tempdir().unwrap();
        let local_addr: ChannelAddr = "tcp:3.4.5.6:123".parse().unwrap();
        let make_proc = |name: &str| hyperactor::ProcAddr::instance(local_addr.clone(), name);
        let first = make_proc("first");
        let second = make_proc("second");
        let third = make_proc("third");
        let (first_serve, _) = local_proc_addr(dir.path(), first.id()).unwrap();
        let (_first_addr, mut first_rx) = channel::serve::<MessageEnvelope>(first_serve).unwrap();
        let (second_serve, _) = local_proc_addr(dir.path(), second.id()).unwrap();
        let (_second_addr, _second_rx) = channel::serve::<MessageEnvelope>(second_serve).unwrap();
        let (backend_addr, mut backend_rx) =
            channel::serve::<MessageEnvelope>(ChannelTransport::Unix.any()).unwrap();

        // The dialer derives the socket path from each proc's pseudo_uid, so
        // both ends must share the same ProcId.
        let first_actor_id = first.actor_addr("actor");
        let second_actor_id = second.actor_addr("actor");
        let third_notexist_actor_id = third.actor_addr("actor");
        let proc_dialer = LocalProcDialer::new(
            local_addr.clone(),
            dir.path().to_owned(),
            MailboxClient::dial(backend_addr).unwrap(),
        );

        let (return_handle, _return_rx) = Mailbox::new(test_actor_id("world_0", "proc"))
            .open_port::<Undeliverable<MessageEnvelope>>();

        // Existing address on the host:
        let envelope = MessageEnvelope::new(
            third_notexist_actor_id.clone(),
            first_actor_id.port_addr(0.into()),
            wirevalue::Any::serialize(&()).unwrap(),
            Flattrs::new(),
        );
        proc_dialer.post(envelope.clone(), return_handle.clone());
        assert_eq!(
            first_rx.recv().await.unwrap().sender(),
            &third_notexist_actor_id
        );

        // A via-prefixed address for the same proc still resolves to its
        // deterministic direct socket.
        let first_via = hyperactor::ProcAddr::new(
            first.id().clone(),
            hyperactor::Location::from(local_addr.clone()).with_via(first.id().uid().clone()),
        );
        let envelope = MessageEnvelope::new(
            third_notexist_actor_id.clone(),
            first_via.actor_addr("actor").port_addr(0.into()),
            wirevalue::Any::serialize(&()).unwrap(),
            Flattrs::new(),
        );
        proc_dialer.post(envelope, return_handle.clone());
        assert_eq!(
            first_rx.recv().await.unwrap().sender(),
            &third_notexist_actor_id
        );

        // Missing direct socket on the host uses the backend:
        let envelope = MessageEnvelope::new(
            second_actor_id.clone(),
            third_notexist_actor_id.port_addr(0.into()),
            wirevalue::Any::serialize(&()).unwrap(),
            Flattrs::new(),
        );
        proc_dialer.post(envelope.clone(), return_handle.clone());
        assert_eq!(backend_rx.recv().await.unwrap().sender(), &second_actor_id);

        // Outside the host:
        let envelope = MessageEnvelope::new(
            second_actor_id.clone(),
            test_actor_id("external_0", "actor").port_addr(0.into()),
            wirevalue::Any::serialize(&()).unwrap(),
            Flattrs::new(),
        );
        proc_dialer.post(envelope.clone(), return_handle.clone());
        assert_eq!(backend_rx.recv().await.unwrap().sender(), &second_actor_id);

        // System proc on the host (name must be exactly "system"):
        let system_actor_id =
            ResourceId::proc_addr_from_name(local_addr.clone(), "system").actor_addr("actor");
        let envelope = MessageEnvelope::new(
            second_actor_id.clone(),
            system_actor_id.port_addr(0.into()),
            wirevalue::Any::serialize(&()).unwrap(),
            Flattrs::new(),
        );
        proc_dialer.post(envelope.clone(), return_handle.clone());
        assert_eq!(backend_rx.recv().await.unwrap().sender(), &second_actor_id);
    }

    #[tokio::test]
    async fn test_proc_dialer_without_socket_uses_backend() {
        let dir = tempfile::tempdir().unwrap();
        let local_addr: ChannelAddr = "tcp:3.4.5.6:123".parse().unwrap();
        let sender = hyperactor::ProcAddr::instance(local_addr.clone(), "sender");
        let host_local = hyperactor::ProcAddr::instance(local_addr.clone(), "local");
        let (backend_addr, mut backend_rx) =
            channel::serve::<MessageEnvelope>(ChannelTransport::Unix.any()).unwrap();
        let proc_dialer = LocalProcDialer::new(
            local_addr.clone(),
            dir.path().to_owned(),
            MailboxClient::dial(backend_addr).unwrap(),
        );
        let (return_handle, _return_rx) = Mailbox::new(test_actor_id("world_0", "proc"))
            .open_port::<Undeliverable<MessageEnvelope>>();

        let envelope = MessageEnvelope::new(
            sender.actor_addr("actor"),
            host_local.actor_addr("actor").port_addr(0.into()),
            wirevalue::Any::serialize(&()).unwrap(),
            Flattrs::new(),
        );
        proc_dialer.post(envelope, return_handle);

        let forwarded = tokio::time::timeout(std::time::Duration::from_secs(1), backend_rx.recv())
            .await
            .expect("destination without a socket must be forwarded to the backend")
            .expect("backend channel closed before receiving forwarded message");
        assert_eq!(forwarded.dest().actor_addr().proc_addr(), host_local);
    }
}
