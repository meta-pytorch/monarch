/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Mailbox;
use hyperactor::Named;
use hyperactor::RefClient;
use hyperactor::channel::ChannelAddr;
use hyperactor::proc::Proc;
use serde::Deserialize;
use serde::Serialize;

use crate::client::ClientActor;
use crate::client::ClientMessageClient;
use crate::create_remote_client;
use crate::object::GenericStateObject;

/// A state actor which serves as a centralized store for state.
#[derive(Debug)]
#[hyperactor::export(
    handlers = [StateMessage],
)]
pub struct StateActor {
    subscribers: HashMap<ActorRef<ClientActor>, (Proc, Mailbox)>,
}

/// Endpoints for the state actor.
#[derive(Handler, HandleClient, RefClient, Debug, Serialize, Deserialize, Named)]
pub enum StateMessage {
    /// Send a batch of logs to the state actor.
    PushLogs { logs: Vec<GenericStateObject> },
    /// Log subscription messages from client.
    SubscribeLogs {
        addr: ChannelAddr,
        client_actor_ref: ActorRef<ClientActor>,
    },
}

#[async_trait]
impl Actor for StateActor {
    type Params = ();

    async fn new(_params: ()) -> Result<Self, anyhow::Error> {
        Ok(Self {
            subscribers: HashMap::new(),
        })
    }
}

#[async_trait]
#[hyperactor::forward(StateMessage)]
impl StateMessageHandler for StateActor {
    async fn push_logs(
        &mut self,
        _cx: &Context<Self>,
        logs: Vec<GenericStateObject>,
    ) -> Result<(), anyhow::Error> {
        for (subscriber, (_, remote_client)) in self.subscribers.iter() {
            subscriber.push_logs(remote_client, logs.clone()).await?;
        }
        Ok(())
    }

    async fn subscribe_logs(
        &mut self,
        cx: &Context<Self>,
        addr: ChannelAddr,
        client_actor_ref: ActorRef<ClientActor>,
    ) -> Result<(), anyhow::Error> {
        tracing::info!(
            "StateActor {} gets a new subscriber: {}",
            cx.self_id(),
            client_actor_ref
        );
        self.subscribers
            .insert(client_actor_ref, create_remote_client(addr).await?);
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::time::Duration;

    use hyperactor::channel;
    use tokio::sync::mpsc::Sender;

    use super::*;
    use crate::client::ClientActorParams;
    use crate::client::LogHandler;
    use crate::create_remote_client;
    use crate::test_utils::log_items;
    use crate::test_utils::spawn_actor;

    /// A log handler that flushes GenericStateObject to a mpsc channel.
    #[derive(Debug)]
    struct MpscLogHandler {
        sender: Sender<Vec<GenericStateObject>>,
    }

    #[async_trait]
    impl LogHandler for MpscLogHandler {
        async fn handle_log(&self, logs: Vec<GenericStateObject>) -> Result<()> {
            self.sender.send(logs).await.unwrap();
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_subscribe_logs() {
        let state_actor_addr = ChannelAddr::any(channel::ChannelTransport::Unix);
        let state_proc_id =
            hyperactor::reference::ProcId(hyperactor::WorldId("state_server".to_string()), 0);
        let (state_actor_addr, state_actor_handle, _state_mailbox) =
            spawn_actor::<StateActor>(state_actor_addr.clone(), state_proc_id, "state", ())
                .await
                .unwrap();
        let state_actor_ref: ActorRef<StateActor> = state_actor_handle.bind();

        let client_actor_addr = ChannelAddr::any(channel::ChannelTransport::Unix);
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<Vec<GenericStateObject>>(20);
        let log_handler = Box::new(MpscLogHandler { sender });
        let params = ClientActorParams { log_handler };
        let client_proc_id =
            hyperactor::reference::ProcId(hyperactor::WorldId("client_server".to_string()), 0);
        let (client_actor_addr, client_actor_handle, _client_mailbox) = spawn_actor::<ClientActor>(
            client_actor_addr.clone(),
            client_proc_id,
            "state_client",
            params,
        )
        .await
        .unwrap();
        let client_actor_ref: ActorRef<ClientActor> = client_actor_handle.bind();

        let (_proc, remote_client) = create_remote_client(state_actor_addr).await.unwrap();

        state_actor_ref
            .subscribe_logs(&remote_client, client_actor_addr, client_actor_ref)
            .await
            .unwrap();
        state_actor_ref
            .push_logs(&remote_client, log_items(0, 10))
            .await
            .unwrap();
        // Collect received messages with timeout
        let fetched_logs = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("timed out waiting for message")
            .expect("channel closed unexpectedly");

        // Verify we received all expected logs
        assert_eq!(fetched_logs.len(), 10);
        assert_eq!(fetched_logs, log_items(0, 10));

        // Now test that no extra message is waiting
        let extra = tokio::time::timeout(Duration::from_millis(100), receiver.recv()).await;
        assert!(extra.is_err(), "expected no more messages");
    }
}
