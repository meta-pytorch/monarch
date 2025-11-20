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
use hyperactor::RemoteSpawn;
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
impl RemoteSpawn for LocalStateBrokerActor {
    type Params = ();

    async fn new(_: ()) -> anyhow::Result<Self> {
        Ok(Default::default())
    }
}

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
    pub fn resolve<A: Actor>(self, cx: &Context<A>) -> Option<ActorHandle<LocalStateBrokerActor>> {
        let actor_id = ActorId(cx.proc().proc_id().clone(), self.0, self.1);
        let actor_ref: ActorRef<LocalStateBrokerActor> = ActorRef::attest(actor_id);
        actor_ref.downcast_handle(cx)
    }
}
