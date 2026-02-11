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

use hyperactor::PortHandle;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelError;
use hyperactor::mailbox::DeliveryError;
use hyperactor::mailbox::MailboxClient;
use hyperactor::mailbox::MailboxSender;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::Undeliverable;

use crate::Name;

/// LocalProcDialer dials local procs directly through a configured socket
/// directory.
#[derive(Debug)]
pub(crate) struct LocalProcDialer {
    local_addr: ChannelAddr,
    socket_dir: PathBuf,
    backend_sender: MailboxClient,
    local_senders: RwLock<HashMap<String, Result<MailboxClient, ChannelError>>>,
}

impl LocalProcDialer {
    /// Create a new local proc dialer. Any direct-addressed procs with a destination
    /// address of `local_addr`, will instead be dialed through the direct sockets
    /// present in `socket_dir`. Messages to other procs are forwarded through the
    /// backend sender.
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
}

impl MailboxSender for LocalProcDialer {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        let proc_id = envelope.dest().actor_id().proc_id();
        let addr = proc_id.addr();
        let name = proc_id.name();
        if addr == &self.local_addr
            // ...and only non-system procs on that address; the rest are directly
            // reachable through the backend address.
            && name.parse::<Name>().as_ref().is_ok_and(Name::is_suffixed)
        {
            let senders = self.local_senders.read().unwrap();
            let senders = if senders.contains_key(name) {
                senders
            } else {
                drop(senders);
                let mut senders = self.local_senders.write().unwrap();
                senders.entry(name.to_string()).or_insert_with(|| {
                    let socket_path = self.socket_dir.join(name);
                    if socket_path.exists() {
                        let addr = format!("unix:{}", self.socket_dir.join(name).display());
                        let addr = addr.parse().unwrap();
                        MailboxClient::dial(addr)
                    } else {
                        Err(ChannelError::InvalidAddress(format!(
                            "unix socket path '{}' does not exist",
                            socket_path.display()
                        )))
                    }
                });
                drop(senders);
                self.local_senders.read().unwrap()
            };

            match senders.get(name).unwrap() {
                Ok(sender) => sender.post_unchecked(envelope, return_handle),
                Err(e) => {
                    let err = DeliveryError::BrokenLink(format!("failed to dial proc: {}", e));
                    envelope.undeliverable(err, return_handle);
                }
            }
        } else {
            self.backend_sender.post_unchecked(envelope, return_handle);
        }
    }
}

#[cfg(test)]
mod tests {

    use std::assert_matches::assert_matches;

    use hyperactor::ActorId;
    use hyperactor::Mailbox;
    use hyperactor::PortId;
    use hyperactor::ProcId;
    use hyperactor::channel::ChannelAddr;
    use hyperactor::channel::ChannelTransport;
    use hyperactor::channel::Rx;
    use hyperactor::channel::{self};
    use hyperactor_config::attrs::Attrs;

    use super::*;
    use crate::Name;

    // Helper to create test ActorId
    fn test_actor_id(proc_name: &str, actor_name: &str) -> ActorId {
        let proc_id = ProcId(
            ChannelAddr::any(ChannelTransport::Local),
            proc_name.to_string(),
        );
        proc_id.actor_id(actor_name, 0)
    }

    #[tokio::test]
    async fn test_proc_dialer() {
        let dir = tempfile::tempdir().unwrap();
        let first = Name::new("first").unwrap();
        let second = Name::new("second").unwrap();
        let third = Name::new("third").unwrap();
        let (_first_addr, mut first_rx) = channel::serve::<MessageEnvelope>(
            format!("unix:{}/{}", dir.path().display(), first)
                .parse()
                .unwrap(),
        )
        .unwrap();
        let (_second_addr, _second_rx) = channel::serve::<MessageEnvelope>(
            format!("unix:{}/{}", dir.path().display(), second)
                .parse()
                .unwrap(),
        )
        .unwrap();
        let (backend_addr, mut backend_rx) =
            channel::serve::<MessageEnvelope>(ChannelTransport::Unix.any()).unwrap();

        let local_addr: ChannelAddr = "tcp:3.4.5.6:123".parse().unwrap();
        let first_actor_id = ActorId(
            ProcId(local_addr.clone(), first.to_string()),
            "actor".to_string(),
            0,
        );
        let second_actor_id = ActorId(
            ProcId(local_addr.clone(), second.to_string()),
            "actor".to_string(),
            0,
        );
        let third_notexist_actor_id = ActorId(
            ProcId(local_addr.clone(), third.to_string()),
            "actor".to_string(),
            0,
        );
        let proc_dialer = LocalProcDialer::new(
            local_addr.clone(),
            dir.path().to_owned(),
            MailboxClient::dial(backend_addr).unwrap(),
        );

        let (return_handle, mut return_rx) =
            Mailbox::new_detached(test_actor_id("world_0", "proc"))
                .open_port::<Undeliverable<MessageEnvelope>>();

        // Existing address on the host:
        let envelope = MessageEnvelope::new(
            third_notexist_actor_id.clone(),
            PortId(first_actor_id.clone(), 0),
            wirevalue::Any::serialize(&()).unwrap(),
            Attrs::new(),
        );
        proc_dialer.post(envelope.clone(), return_handle.clone());
        assert_eq!(
            first_rx.recv().await.unwrap().sender(),
            &third_notexist_actor_id
        );

        // Nonexistant address on the host:
        let envelope = MessageEnvelope::new(
            second_actor_id.clone(),
            PortId(third_notexist_actor_id.clone(), 0),
            wirevalue::Any::serialize(&()).unwrap(),
            Attrs::new(),
        );
        proc_dialer.post(envelope.clone(), return_handle.clone());
        assert_matches!(
            &return_rx.recv().await.unwrap().into_inner().errors()[..],
            &[DeliveryError::BrokenLink(_)]
        );

        // Outside the host:
        let envelope = MessageEnvelope::new(
            second_actor_id.clone(),
            PortId(test_actor_id("external_0", "actor"), 0),
            wirevalue::Any::serialize(&()).unwrap(),
            Attrs::new(),
        );
        proc_dialer.post(envelope.clone(), return_handle.clone());
        assert_eq!(backend_rx.recv().await.unwrap().sender(), &second_actor_id);

        // System proc on the host:
        let system_actor_id = ActorId(
            ProcId(local_addr.clone(), "system".to_string()),
            "actor".to_string(),
            0,
        );
        let envelope = MessageEnvelope::new(
            second_actor_id.clone(),
            PortId(system_actor_id, 0),
            wirevalue::Any::serialize(&()).unwrap(),
            Attrs::new(),
        );
        proc_dialer.post(envelope.clone(), return_handle.clone());
        assert_eq!(backend_rx.recv().await.unwrap().sender(), &second_actor_id);
    }
}
