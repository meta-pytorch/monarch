/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::Bind;
use hyperactor::Context;
use hyperactor::Handler;
use hyperactor::Named;
use hyperactor::PortRef;
use hyperactor::RemoteSpawn;
use hyperactor::Unbind;
use hyperactor::clock::Clock;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Named, Bind, Unbind)]
pub struct BenchMessage {
    pub step: usize,
    pub reply: PortRef<usize>,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
}

#[derive(Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [
        BenchMessage { cast = true },
    ],
)]
pub struct BenchActor {
    processing_time: Duration,
}

impl Actor for BenchActor {}

#[async_trait]
impl RemoteSpawn for BenchActor {
    type Params = Duration;
    async fn new(params: Duration) -> Result<Self, anyhow::Error> {
        Ok(Self {
            processing_time: params,
        })
    }
}

#[async_trait]
impl Handler<BenchMessage> for BenchActor {
    async fn handle(
        &mut self,
        ctx: &Context<Self>,
        msg: BenchMessage,
    ) -> Result<(), anyhow::Error> {
        hyperactor::clock::ClockKind::default()
            .sleep(self.processing_time.clone())
            .await;

        let _ = msg.reply.send(ctx, msg.step);
        Ok(())
    }
}
