/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use anyhow::Result;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Mailbox;
use hyperactor::actor::Binds;
use hyperactor::actor::RemoteActor;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::id;
use hyperactor::mailbox::BoxedMailboxSender;
use hyperactor::mailbox::DialMailboxRouter;
use hyperactor::mailbox::MailboxClient;
use hyperactor::mailbox::MailboxServer;
use hyperactor::mailbox::MessageEnvelope;
use hyperactor::mailbox::Undeliverable;
use hyperactor::proc::Proc;

pub mod client;
pub mod object;
pub mod state_actor;

/// Creates a state actor server at given address. Returns the server address and a handle to the
/// state actor.
#[allow(dead_code)]
pub async fn spawn_actor<T: Actor + RemoteActor + Binds<T>>(
    addr: ChannelAddr,
    actor_id: ActorId,
    params: T::Params,
) -> Result<(ChannelAddr, ActorRef<T>)> {
    let proc_id = actor_id.proc_id();
    let proc = Proc::new(
        proc_id.clone(),
        BoxedMailboxSender::new(DialMailboxRouter::new()),
    );
    let (local_addr, rx) = channel::serve(addr.clone()).await?;
    let actor_handle: ActorHandle<T> = proc.spawn(actor_id.name(), params).await?;
    actor_handle.bind::<T>();

    // Undeliverable messages encountered by the mailbox server
    // are to be returned to the system actor.
    let return_handle = actor_handle.port::<Undeliverable<MessageEnvelope>>();
    let _mailbox_handle = proc.clone().serve(rx, return_handle);

    Ok((local_addr, actor_handle.bind()))
}

/// Creates a remote client that can send message to actors in the remote addr.
/// It is important to keep the client proc alive for the remote_client's lifetime.
pub async fn create_remote_client(addr: ChannelAddr) -> Result<(Proc, Mailbox)> {
    let remote_sender = MailboxClient::new(channel::dial(addr).unwrap());
    let client_proc_id = id!(client).random_user_proc();
    let client_proc = Proc::new(
        client_proc_id.clone(),
        BoxedMailboxSender::new(remote_sender),
    );
    let remote_client = client_proc.attach("client").unwrap();
    Ok((client_proc, remote_client))
}

pub mod test_utils {
    use crate::object::GenericStateObject;
    use crate::object::LogSpec;
    use crate::object::LogState;
    use crate::object::StateMetadata;
    use crate::object::StateObject;

    pub fn log_items(seq_low: usize, seq_high: usize) -> Vec<GenericStateObject> {
        let mut log_items = vec![];
        let metadata = StateMetadata {
            name: "test".to_string(),
            kind: "log".to_string(),
        };
        let spec = LogSpec {};
        for seq in seq_low..seq_high {
            let state = LogState::new(seq, format!("state {}", seq));
            let state_object =
                StateObject::<LogSpec, LogState>::new(metadata.clone(), spec.clone(), state);
            let generic_state_object = GenericStateObject::try_from(state_object).unwrap();
            log_items.push(generic_state_object);
        }
        log_items
    }
}
