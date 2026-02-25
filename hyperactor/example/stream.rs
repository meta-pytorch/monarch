/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This example implements a basic pub-sub pattern in Hyperactor.

use std::time::Duration;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::PortRef;
use hyperactor::proc::Proc;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

#[derive(Debug, Default)]
struct CounterActor {
    subscribers: Vec<PortRef<u64>>,
    n: u64,
}

impl Actor for CounterActor {}

#[derive(Serialize, Deserialize, Debug, Named)]
struct Subscribe(PortRef<u64>);

#[async_trait]
impl Handler<Subscribe> for CounterActor {
    async fn handle(
        &mut self,
        cx: &Context<Self>,
        subscriber: Subscribe,
    ) -> Result<(), anyhow::Error> {
        self.subscribers.push(subscriber.0);
        for port in &self.subscribers {
            port.send(cx, self.n)?;
        }
        self.n += 1;
        Ok(())
    }
}

#[derive(Debug)]
struct CountClient {
    counter: PortRef<Subscribe>,
}

impl CountClient {
    fn new(counter: PortRef<Subscribe>) -> Self {
        Self { counter }
    }
}

#[async_trait]
impl Actor for CountClient {
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        // Subscribe to the counter on initialization. We give it our u64 port to report
        // messages back to.
        self.counter.send(this, Subscribe(this.port().bind()))?;
        Ok(())
    }
}

#[async_trait]
impl Handler<u64> for CountClient {
    async fn handle(&mut self, cx: &Context<Self>, count: u64) -> Result<(), anyhow::Error> {
        eprintln!("{}: count: {}", cx.self_id(), count);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let proc = Proc::local();

    let counter_actor: ActorHandle<CounterActor> =
        proc.spawn("counter", CounterActor::default()).unwrap();

    for i in 0..10 {
        // Spawn new "countees". Every time each subscribes, the counter broadcasts
        // the count to everyone.
        let _countee_actor: ActorHandle<CountClient> = proc
            .spawn(
                &format!("countee_{}", i),
                CountClient::new(counter_actor.port().bind()),
            )
            .unwrap();
        #[allow(clippy::disallowed_methods)]
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
