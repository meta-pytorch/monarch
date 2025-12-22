/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::OncePortHandle;
use pyo3::prelude::*;

#[derive(Debug)]
pub struct LocalState {
    pub response_port: OncePortHandle<Result<PyObject, PyObject>>,
    pub state: Vec<PyObject>,
}

#[derive(Debug)]
pub enum LocalStateBrokerMessage {
    Set(usize, LocalState),
    Get(usize, OncePortHandle<LocalState>),
}

#[derive(Debug, Default)]
#[hyperactor::export(spawn = true)]
pub struct LocalStateBrokerActor {
    states: HashMap<usize, LocalState>,
    ports: HashMap<usize, OncePortHandle<LocalState>>,
}

impl Actor for LocalStateBrokerActor {}

#[async_trait]
impl Handler<LocalStateBrokerMessage> for LocalStateBrokerActor {
    async fn handle(
        &mut self,
        _cx: &Context<Self>,
        message: LocalStateBrokerMessage,
    ) -> anyhow::Result<()> {
        match message {
            LocalStateBrokerMessage::Set(id, state) => match self.ports.remove_entry(&id) {
                Some((_, port)) => {
                    port.send(state)?;
                }
                None => {
                    self.states.insert(id, state);
                }
            },
            LocalStateBrokerMessage::Get(id, port) => match self.states.remove_entry(&id) {
                Some((_, state)) => {
                    port.send(state)?;
                }
                None => {
                    self.ports.insert(id, port);
                }
            },
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct BrokerId(String, usize);

impl BrokerId {
    pub fn new(broker_id: (String, usize)) -> Self {
        BrokerId(broker_id.0, broker_id.1)
    }

    /// Resolve the broker with exponential backoff retry.
    /// Broker creation can race with messages that will use the broker,
    /// so we retry with exponential backoff before panicking.
    /// A better solution would be to figure out some way to get the real broker reference threaded to the client,  but
    /// that is more difficult to figure out right now.
    pub async fn resolve<A: Actor>(
        self,
        cx: &Context<'_, A>,
    ) -> ActorHandle<LocalStateBrokerActor> {
        use std::time::Duration;

        let broker_name = format!("{:?}", self);
        let actor_id = ActorId(cx.proc().proc_id().clone(), self.0, self.1);
        let actor_ref: ActorRef<LocalStateBrokerActor> = ActorRef::attest(actor_id);

        let mut delay_ms = 1;
        loop {
            if let Some(handle) = actor_ref.downcast_handle(cx) {
                return handle;
            }

            if delay_ms > 8192 {
                panic!("Failed to resolve broker {} after retries", broker_name);
            }

            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            delay_ms *= 2;
        }
    }
}
