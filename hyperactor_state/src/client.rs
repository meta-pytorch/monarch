/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use anyhow::Result;
use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Named;
use hyperactor_macros::HandleClient;
use hyperactor_macros::RefClient;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use crate::object::GenericStateObject;

/// A client to interact with the state actor.
#[derive(Debug)]
#[hyperactor::export(
    handlers = [ClientMessage],
)]
pub struct ClientActor {
    sender: Sender<GenericStateObject>,
}

/// Endpoints for the client actor.
#[derive(Handler, HandleClient, RefClient, Debug, Serialize, Deserialize, Named)]
pub enum ClientMessage {
    /// Push a batch of logs to the logs buffer
    PushLogs {
        /// Object to set.
        logs: Vec<GenericStateObject>,
    },
}

pub struct ClientActorParams {
    pub sender: Sender<GenericStateObject>,
}

#[async_trait]
impl Actor for ClientActor {
    type Params = ClientActorParams;

    async fn new(ClientActorParams { sender }: ClientActorParams) -> Result<Self, anyhow::Error> {
        Ok(Self { sender })
    }
}

#[async_trait]
#[hyperactor::forward(ClientMessage)]
impl ClientMessageHandler for ClientActor {
    async fn push_logs(
        &mut self,
        _this: &Instance<Self>,
        logs: Vec<GenericStateObject>,
    ) -> Result<(), anyhow::Error> {
        for log in logs {
            self.sender.send(log).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use hyperactor::ActorRef;
    use hyperactor::channel;
    use hyperactor::channel::ChannelAddr;

    use super::*;
    use crate::create_remote_client;
    use crate::spawn_actor;
    use crate::test_utils::log_items;

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_client_basics() {
        let client_actor_addr = ChannelAddr::any(channel::ChannelTransport::Unix);
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<GenericStateObject>(10);
        let params = ClientActorParams { sender };
        let client_proc_id =
            hyperactor::reference::ProcId(hyperactor::WorldId("client_server".to_string()), 0);
        let (client_actor_addr, client_actor_handle) = spawn_actor::<ClientActor>(
            client_actor_addr.clone(),
            client_proc_id,
            "state_client",
            params,
        )
        .await
        .unwrap();
        let client_actor_ref: ActorRef<ClientActor> = client_actor_handle.bind();

        let (_client_proc, remote_client, _) =
            create_remote_client(client_actor_addr).await.unwrap();

        let log_items_0_10 = log_items(0, 10);
        client_actor_ref
            .push_logs(&remote_client, log_items_0_10.clone())
            .await
            .unwrap();

        // Collect received messages with timeout
        let mut fetched_logs = vec![];
        for _ in 0..10 {
            // Timeout prevents hanging if a message is missing
            let log = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                .await
                .expect("timed out waiting for message")
                .expect("channel closed unexpectedly");

            fetched_logs.push(log);
        }

        // Verify we received all expected logs
        assert_eq!(fetched_logs.len(), 10);
        assert_eq!(fetched_logs, log_items_0_10);

        // Now test that no extra message is waiting
        let extra = tokio::time::timeout(Duration::from_millis(100), receiver.recv()).await;
        assert!(extra.is_err(), "expected no more messages");
    }
}
