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
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Mailbox;
use hyperactor::Named;
use hyperactor::PortRef;
use hyperactor::ProcId;
use hyperactor::RefClient;
use hyperactor::channel::ChannelAddr;
use hyperactor::mailbox::DialMailboxRouter;
use hyperactor::proc::Proc;
use serde::Deserialize;
use serde::Serialize;

use crate::client::ClientMessage;
use crate::create_remote_client;
use crate::object::GenericStateObject;

/// Result code for state actor operations
#[derive(Debug, Clone, Serialize, Deserialize, Named)]
pub enum ResultCode {
    /// Operation completed successfully
    OK,
    /// Invalid input provided
    InvalidInput,
}

/// Result of a subscribe logs operation
#[derive(Debug, Clone, Serialize, Deserialize, Named)]
pub struct SubscribeLogsResult {
    /// Result code of the operation
    pub code: ResultCode,
    /// Message providing additional information about the result
    pub message: String,
}

/// Result of an unsubscribe logs operation
#[derive(Debug, Clone, Serialize, Deserialize, Named)]
pub struct UnsubscribeLogsResult {
    /// Result code of the operation
    pub code: ResultCode,
    /// Message providing additional information about the result
    pub message: String,
}

/// A state actor which serves as a centralized store for state.
#[derive(Debug)]
#[hyperactor::export(
    handlers = [StateMessage],
)]
pub struct StateActor {
    // Using PortRef instead of ActorRef<ClientActor> allows any actor to become a subscriber
    log_subscribers: HashMap<PortRef<ClientMessage>, (Proc, Mailbox)>,
}

/// Endpoints for the state actor.
#[derive(Handler, HandleClient, RefClient, Debug, Serialize, Deserialize, Named)]
pub enum StateMessage {
    /// Send a batch of logs to the state actor.
    SetLogs {
        logs: Vec<GenericStateObject>,
    },
    /// Log subscription messages from client. This message should be sent by any actor to
    /// inform the state actor to start sending logs to the client.
    SubscribeLogs {
        addr: ChannelAddr,
        subscriber_port: PortRef<ClientMessage>,
        #[reply]
        reply_port: hyperactor::OncePortRef<SubscribeLogsResult>,
    },
    /// Unsubscribe from logs. This message should be sent by any actor to inform
    /// the state actor to stop sending logs to the client.
    UnsubscribeLogs {
        subscriber_port: PortRef<ClientMessage>,
        #[reply]
        reply_port: hyperactor::OncePortRef<UnsubscribeLogsResult>,
    },
    Join {
        proc_id: ProcId,
        addr: ChannelAddr,
    },
}

#[async_trait]
impl Actor for StateActor {
    type Params = ();

    async fn new(_params: ()) -> Result<Self, anyhow::Error> {
        Ok(Self {
            log_subscribers: HashMap::new(),
        })
    }
}

#[async_trait]
#[hyperactor::forward(StateMessage)]
impl StateMessageHandler for StateActor {
    async fn set_logs(
        &mut self,
        _this: &Instance<Self>,
        logs: Vec<GenericStateObject>,
    ) -> Result<(), anyhow::Error> {
        for (port, (_, remote_client)) in self.log_subscribers.iter() {
            // Send logs message directly to the port
            port.send(remote_client, ClientMessage::Logs { logs: logs.clone() })?;
        }
        Ok(())
    }

    async fn subscribe_logs(
        &mut self,
        _this: &Instance<Self>,
        addr: ChannelAddr,
        subscriber_port: PortRef<ClientMessage>,
    ) -> Result<SubscribeLogsResult, anyhow::Error> {
        if self.log_subscribers.contains_key(&subscriber_port) {
            return Ok(SubscribeLogsResult {
                code: ResultCode::InvalidInput,
                message: "Client with this port is already subscribed".to_string(),
            });
        }
        tracing::info!("Subscribing client with port at {} for logs", &addr);
        let (proc, remote_client, _) = create_remote_client(addr).await?;
        self.log_subscribers
            .insert(subscriber_port, (proc, remote_client));
        Ok(SubscribeLogsResult {
            code: ResultCode::OK,
            message: String::new(),
        })
    }

    async fn unsubscribe_logs(
        &mut self,
        _this: &Instance<Self>,
        subscriber_port: PortRef<ClientMessage>,
    ) -> Result<UnsubscribeLogsResult, anyhow::Error> {
        if !self.log_subscribers.contains_key(&subscriber_port) {
            return Ok(UnsubscribeLogsResult {
                code: ResultCode::InvalidInput,
                message: "Client with this port is not subscribed".to_string(),
            });
        }
        tracing::info!("Unsubscribing client from logs");
        self.log_subscribers.remove(&subscriber_port);
        Ok(UnsubscribeLogsResult {
            code: ResultCode::OK,
            message: String::new(),
        })
    }

    async fn join(
        &mut self,
        this: &Instance<Self>,
        proc_id: ProcId,
        addr: ChannelAddr,
    ) -> Result<(), anyhow::Error> {
        if let Some(router) = this.proc().forwarder().downcast_ref::<DialMailboxRouter>() {
            tracing::info!("binding {} to {}", &proc_id, &addr,);
            router.bind(proc_id.into(), addr);
        } else {
            tracing::warn!(
                "proc {} received update_address but does not use a DialMailboxRouter",
                this.proc().proc_id()
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::time::Duration;

    use hyperactor::ActorRef;
    use hyperactor::channel;

    use super::*;
    use crate::client::ClientActor;
    use crate::client::ClientActorParams;
    use crate::create_remote_client;
    use crate::spawn_actor;
    use crate::test_utils::log_items;

    #[tokio::test]
    async fn test_duplicate_subscribe() {
        // Create a state actor
        let state_actor_addr = ChannelAddr::any(channel::ChannelTransport::Unix);
        let state_proc_id =
            hyperactor::reference::ProcId(hyperactor::WorldId("state_server".to_string()), 0);
        let (state_actor_addr, state_actor_ref) =
            spawn_actor::<StateActor>(state_actor_addr.clone(), state_proc_id, "state", ())
                .await
                .unwrap();

        // Create a client actor
        let client_actor_addr = ChannelAddr::any(channel::ChannelTransport::Unix);
        let (sender, _receiver) = tokio::sync::mpsc::channel::<GenericStateObject>(20);
        let params = ClientActorParams { sender };
        let client_proc_id =
            hyperactor::reference::ProcId(hyperactor::WorldId("client_server".to_string()), 0);
        let (client_actor_addr, client_actor_ref) =
            spawn_actor::<ClientActor>(client_actor_addr.clone(), client_proc_id, "client", params)
                .await
                .unwrap();

        // Connect to the state actor
        let (remote_client_proc, remote_client, remote_client_addr) =
            create_remote_client(state_actor_addr).await.unwrap();
        state_actor_ref
            .join(
                &remote_client,
                remote_client_proc.proc_id().to_owned(),
                remote_client_addr,
            )
            .await
            .unwrap();
        // First subscription should succeed
        // Convert the client actor reference to a PortRef<ClientMessage>
        let client_port =
            PortRef::<ClientMessage>::attest_message_port(client_actor_ref.actor_id());
        let subscribe_result = state_actor_ref
            .subscribe_logs(
                &remote_client,
                client_actor_addr.clone(),
                client_port.clone(),
            )
            .await
            .unwrap();

        // Verify the result code is OK
        assert!(matches!(subscribe_result.code, ResultCode::OK));
        assert!(subscribe_result.message.is_empty());

        // Second subscription with the same client_actor_ref should return InvalidInput
        let duplicate_result = state_actor_ref
            .subscribe_logs(
                &remote_client,
                client_actor_addr.clone(),
                client_port.clone(),
            )
            .await
            .unwrap();

        // Verify that the second subscription attempt returned InvalidInput
        assert!(matches!(duplicate_result.code, ResultCode::InvalidInput));
        assert!(duplicate_result.message.contains("already subscribed"));
    }

    #[tokio::test]
    async fn test_unsubscribe_nonexistent() {
        // Create a state actor
        let state_actor_addr = ChannelAddr::any(channel::ChannelTransport::Unix);
        let state_proc_id =
            hyperactor::reference::ProcId(hyperactor::WorldId("state_server".to_string()), 0);
        let (state_actor_addr, state_actor_ref) =
            spawn_actor::<StateActor>(state_actor_addr.clone(), state_proc_id, "state", ())
                .await
                .unwrap();

        // Create a client actor but don't subscribe it
        let client_actor_addr = ChannelAddr::any(channel::ChannelTransport::Unix);
        let (sender, _receiver) = tokio::sync::mpsc::channel::<GenericStateObject>(20);
        let params = ClientActorParams { sender };
        let client_proc_id =
            hyperactor::reference::ProcId(hyperactor::WorldId("client_server".to_string()), 0);
        let (_unused, client_actor_ref) =
            spawn_actor::<ClientActor>(client_actor_addr.clone(), client_proc_id, "client", params)
                .await
                .unwrap();

        // Connect to the state actor
        let (remote_client_proc, remote_client, remote_client_addr) =
            create_remote_client(state_actor_addr).await.unwrap();
        state_actor_ref
            .join(
                &remote_client,
                remote_client_proc.proc_id().to_owned(),
                remote_client_addr,
            )
            .await
            .unwrap();

        // Trying to unsubscribe a client that was never subscribed should return InvalidInput
        // Convert the client actor reference to a PortRef<ClientMessage>
        let client_port =
            PortRef::<ClientMessage>::attest_message_port(client_actor_ref.actor_id());
        let unsubscribe_result = state_actor_ref
            .unsubscribe_logs(&remote_client, client_port.clone())
            .await
            .unwrap();

        // Verify the result code is InvalidInput
        assert!(matches!(unsubscribe_result.code, ResultCode::InvalidInput));
        assert!(unsubscribe_result.message.contains("not subscribed"));
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_subscribe_logs() {
        let state_actor_addr = ChannelAddr::any(channel::ChannelTransport::Unix);
        let state_proc_id =
            hyperactor::reference::ProcId(hyperactor::WorldId("state_server".to_string()), 0);
        let (state_actor_addr, state_actor_handle) =
            spawn_actor::<StateActor>(state_actor_addr.clone(), state_proc_id, "state", ())
                .await
                .unwrap();
        let state_actor_ref: ActorRef<StateActor> = state_actor_handle.bind();

        let client_actor_addr = ChannelAddr::any(channel::ChannelTransport::Unix);
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<GenericStateObject>(20);
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

        let (remote_client_proc, remote_client, remote_client_addr) =
            create_remote_client(state_actor_addr).await.unwrap();
        state_actor_ref
            .join(
                &remote_client,
                remote_client_proc.proc_id().to_owned(),
                remote_client_addr,
            )
            .await
            .unwrap();

        // 1. Subscribe to logs using the client actor's port
        // Convert the client actor reference to a PortRef<ClientMessage>
        let client_port =
            PortRef::<ClientMessage>::attest_message_port(client_actor_ref.actor_id());
        let subscribe_result = state_actor_ref
            .subscribe_logs(
                &remote_client,
                client_actor_addr.clone(),
                client_port.clone(),
            )
            .await
            .unwrap();

        // Verify the result code is OK
        assert!(matches!(subscribe_result.code, ResultCode::OK));

        // 2. Send logs and verify they are received
        state_actor_ref
            .set_logs(&remote_client, log_items(0, 10))
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
        assert_eq!(fetched_logs, log_items(0, 10));

        // Now test that no extra message is waiting
        let extra = tokio::time::timeout(Duration::from_millis(100), receiver.recv()).await;
        assert!(extra.is_err(), "expected no more messages");

        // 3. Unsubscribe from logs
        let unsubscribe_result = state_actor_ref
            .unsubscribe_logs(&remote_client, client_port.clone())
            .await
            .unwrap();

        // Verify the result code is OK
        assert!(matches!(unsubscribe_result.code, ResultCode::OK));

        // 4. Send more logs and verify they are not received
        state_actor_ref
            .set_logs(&remote_client, log_items(10, 20))
            .await
            .unwrap();

        // Verify no messages are received after unsubscribing
        let no_logs = tokio::time::timeout(Duration::from_millis(500), receiver.recv()).await;
        assert!(no_logs.is_err(), "expected no messages after unsubscribing");

        // 5. Subscribe again
        let resubscribe_result = state_actor_ref
            .subscribe_logs(&remote_client, client_actor_addr, client_port)
            .await
            .unwrap();

        // Verify the result code is OK
        assert!(matches!(resubscribe_result.code, ResultCode::OK));

        // 6. Send logs and verify they are received again
        state_actor_ref
            .set_logs(&remote_client, log_items(20, 30))
            .await
            .unwrap();

        // Collect received messages with timeout
        let mut fetched_logs_after_resubscribe = vec![];
        for _ in 0..10 {
            // Timeout prevents hanging if a message is missing
            let log = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                .await
                .expect("timed out waiting for message after resubscribing")
                .expect("channel closed unexpectedly");

            fetched_logs_after_resubscribe.push(log);
        }

        // Verify we received all expected logs after resubscribing
        assert_eq!(fetched_logs_after_resubscribe.len(), 10);
        assert_eq!(fetched_logs_after_resubscribe, log_items(20, 30));
    }
}
