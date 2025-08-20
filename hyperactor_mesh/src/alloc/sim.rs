/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Support for allocating procs in the local process with simulated channels.

#![allow(dead_code)] // until it is used outside of testing

use async_trait::async_trait;
use hyperactor::ProcId;
use hyperactor::WorldId;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelTransport;
use hyperactor::mailbox::MailboxServerHandle;
use hyperactor::proc::Proc;
use ndslice::view::Extent;

use super::ProcStopReason;
use crate::alloc::Alloc;
use crate::alloc::AllocSpec;
use crate::alloc::Allocator;
use crate::alloc::AllocatorError;
use crate::alloc::LocalAlloc;
use crate::alloc::ProcState;
use crate::shortuuid::ShortUuid;

/// An allocator that runs procs in the local process with network traffic going through simulated channels.
/// Other than transport, the underlying implementation is an inner LocalAlloc.
pub struct SimAllocator;

#[async_trait]
impl Allocator for SimAllocator {
    type Alloc = SimAlloc;

    async fn allocate(&mut self, spec: AllocSpec) -> Result<Self::Alloc, AllocatorError> {
        Ok(SimAlloc::new(spec))
    }
}

impl SimAllocator {
    #[cfg(test)]
    pub(crate) fn new_and_start_simnet() -> Self {
        hyperactor::simnet::start();
        Self
    }
}

struct SimProc {
    proc: Proc,
    addr: ChannelAddr,
    handle: MailboxServerHandle,
}

/// A simulated allocation. It is a collection of procs that are running in the local process.
pub struct SimAlloc {
    inner: LocalAlloc,
}

impl SimAlloc {
    fn new(spec: AllocSpec) -> Self {
        let inner = LocalAlloc::new_with_transport(
            spec,
            ChannelTransport::Sim(Box::new(ChannelTransport::Unix)),
        );
        let client_proc_id = ProcId::Ranked(WorldId(format!("{}_manager", inner.name())), 0);

        let ext = inner.extent();

        hyperactor::simnet::simnet_handle()
            .expect("simnet event loop not running")
            .register_proc(
                client_proc_id.clone(),
                ext.point(ext.sizes().iter().map(|_| 0).collect())
                    .expect("should be valid point"),
            );

        Self { inner }
    }
    /// A chaos monkey that can be used to stop procs at random.
    pub(crate) fn chaos_monkey(&self) -> impl Fn(usize, ProcStopReason) + 'static {
        self.inner.chaos_monkey()
    }

    /// A function to shut down the alloc for testing purposes.
    pub(crate) fn stopper(&self) -> impl Fn() + 'static {
        self.inner.stopper()
    }

    pub(crate) fn name(&self) -> &ShortUuid {
        self.inner.name()
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}

#[async_trait]
impl Alloc for SimAlloc {
    async fn next(&mut self) -> Option<ProcState> {
        let proc_state = self.inner.next().await;
        if let Some(ProcState::Created { proc_id, point, .. }) = &proc_state {
            hyperactor::simnet::simnet_handle()
                .expect("simnet event loop not running")
                .register_proc(proc_id.clone(), point.clone());
        }
        proc_state
    }

    fn extent(&self) -> &Extent {
        self.inner.extent()
    }

    fn world_id(&self) -> &WorldId {
        self.inner.world_id()
    }

    fn transport(&self) -> ChannelTransport {
        self.inner.transport()
    }

    async fn stop(&mut self) -> Result<(), AllocatorError> {
        self.inner.stop().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_allocator_basic() {
        hyperactor::simnet::start();
        crate::alloc::testing::test_allocator_basic(SimAllocator).await;
    }
}
