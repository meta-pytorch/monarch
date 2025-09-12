/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::any;
use std::option::Option;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Mailbox;
use hyperactor::Named;
use hyperactor::OncePortRef;
use hyperactor::RefClient;
use serde::Deserialize;
use serde::Serialize;

use crate::IbverbsConfig;
use crate::RdmaBuffer;
use crate::RdmaManagerActor;
use crate::RdmaManagerMessageClient;
use crate::ibverbs_primitives::get_all_devices;

#[derive(Debug, Named, Clone, Serialize, Deserialize)]
pub struct BenchmarkConfig {
    pub buffer_size: usize,
    pub num_buffers: usize,
    pub num_iterations: usize,
}

#[derive(Debug, Named, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub latencies: Vec<Duration>,
}

#[derive(Handler, HandleClient, RefClient, Debug, Serialize, Deserialize, Named)]
pub enum RdmaBufferBenchmarkMessage {
    AllocateBuffers {
        buffer_size: usize,
        num_buffers: usize,
        #[reply]
        reply: OncePortRef<Vec<RdmaBuffer>>,
    },
    DoBenchmark {
        config: BenchmarkConfig,
        #[reply]
        reply: OncePortRef<BenchmarkResult>,
    },
}

#[derive(Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [
        RdmaBufferBenchmarkMessage,
    ],
)]
pub struct RdmaBufferBenchmarkActor {
    rdma_manager: ActorRef<RdmaManagerActor>,
    other: Option<ActorRef<RdmaBufferBenchmarkActor>>,
}

#[async_trait]
impl Actor for RdmaBufferBenchmarkActor {
    type Params = (
        ActorRef<RdmaManagerActor>,
        Option<ActorRef<RdmaBufferBenchmarkActor>>,
    );
    async fn new(params: Self::Params) -> Result<Self, anyhow::Error> {
        Ok(Self {
            rdma_manager: params.0,
            other: params.1,
        })
    }
}

pub async fn _allocate_buffers<'a, 'b>(
    actor: &'a RdmaBufferBenchmarkActor,
    cx: &'b Context<'_, RdmaBufferBenchmarkActor>,
    buffer_size: usize,
    num_buffers: usize,
) -> Result<Vec<RdmaBuffer>, anyhow::Error> {
    let mut rdma_buffers = vec![];
    for _ in 0..num_buffers {
        let raw_buffer_addr = Box::leak(Box::new(vec![0u8; buffer_size])).as_ptr() as usize;
        let rdma_buffer = actor
            .rdma_manager
            .request_buffer(cx, raw_buffer_addr, buffer_size)
            .await?;
        rdma_buffers.push(rdma_buffer);
    }
    Ok(rdma_buffers)
}

#[async_trait]
#[hyperactor::forward(RdmaBufferBenchmarkMessage)]
impl RdmaBufferBenchmarkMessageHandler for RdmaBufferBenchmarkActor {
    async fn allocate_buffers(
        &mut self,
        cx: &Context<Self>,
        buffer_size: usize,
        num_buffers: usize,
    ) -> Result<Vec<RdmaBuffer>, anyhow::Error> {
        Ok(_allocate_buffers(self, cx, buffer_size, num_buffers).await?)
    }

    async fn do_benchmark(
        &mut self,
        cx: &Context<Self>,
        config: BenchmarkConfig,
    ) -> Result<BenchmarkResult, anyhow::Error> {
        let buffer_size = config.buffer_size;
        let num_buffers = config.num_buffers;
        let local_buffers = _allocate_buffers(self, cx, buffer_size, num_buffers).await?;
        let mut latencies = vec![];
        let other = self
            .other
            .clone()
            .ok_or(anyhow::anyhow!("No other actor found"))?;
        let remote_buffers = other.allocate_buffers(cx, buffer_size, num_buffers).await?;
        for _ in 0..config.num_iterations {
            for i in 0..num_buffers {
                let start = Instant::now();
                let local_buffer = &local_buffers[i];
                let remote_buffer = &remote_buffers[i];
                local_buffer
                    .read_into(
                        cx.mailbox_for_py(),
                        remote_buffer.clone(),
                        /*timeout */ 5,
                    )
                    .await?;
                let latency = start.elapsed();
                latencies.push(latency);
            }
        }
        Ok(BenchmarkResult { latencies })
    }
}
