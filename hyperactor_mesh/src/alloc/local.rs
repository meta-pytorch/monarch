/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Support for allocating procs in the local process.

#![allow(dead_code)] // until it is used outside of testing

use std::collections::HashMap;
use std::collections::VecDeque;
use std::time::Duration;

use async_trait::async_trait;
use hyperactor::ProcId;
use hyperactor::WorldId;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::mailbox::MailboxServer;
use hyperactor::mailbox::MailboxServerHandle;
use hyperactor::proc::Proc;
use ndslice::view::Extent;
use tokio::sync::mpsc;
use tokio::time::sleep;

use super::ProcStopReason;
use crate::alloc::Alloc;
use crate::alloc::AllocSpec;
use crate::alloc::Allocator;
use crate::alloc::AllocatorError;
use crate::alloc::ProcState;
use crate::mesh_agent::ProcMeshAgent;
use crate::shortuuid::ShortUuid;

enum Action {
    Start(usize),
    Stop(usize, ProcStopReason),
    Stopped,
}

/// An allocator that runs procs in the local process. It is primarily useful for testing,
/// or small meshes that can run entirely locally.
///
/// Currently, the allocator will allocate all procs, but each is treated as infallible,
/// since they share fault domain with the client of the alloc.
pub struct LocalAllocator;

#[async_trait]
impl Allocator for LocalAllocator {
    type Alloc = LocalAlloc;

    async fn allocate(&mut self, spec: AllocSpec) -> Result<Self::Alloc, AllocatorError> {
        let alloc = LocalAlloc::new(spec);
        tracing::info!(
            name = "LocalAllocStatus",
            alloc_name = %alloc.world_id(),
            status = "Allocated",
        );
        Ok(alloc)
    }
}

struct LocalProc {
    proc: Proc,
    create_key: ShortUuid,
    addr: ChannelAddr,
    handle: MailboxServerHandle,
}

/// A local allocation. It is a collection of procs that are running in the local process.
pub struct LocalAlloc {
    spec: AllocSpec,
    name: ShortUuid,
    world_id: WorldId, // to provide storage
    procs: HashMap<usize, LocalProc>,
    queue: VecDeque<ProcState>,
    todo_tx: mpsc::UnboundedSender<Action>,
    todo_rx: mpsc::UnboundedReceiver<Action>,
    stopped: bool,
    failed: bool,
}

impl LocalAlloc {
    pub(crate) fn new(spec: AllocSpec) -> Self {
        let name = ShortUuid::generate();
        let (todo_tx, todo_rx) = mpsc::unbounded_channel();
        for rank in 0..spec.extent.num_ranks() {
            todo_tx.send(Action::Start(rank)).unwrap();
        }
        Self {
            spec,
            name: name.clone(),
            world_id: WorldId(name.to_string()),
            procs: HashMap::new(),
            queue: VecDeque::new(),
            todo_tx,
            todo_rx,
            stopped: false,
            failed: false,
        }
    }

    /// A chaos monkey that can be used to stop procs at random.
    pub(crate) fn chaos_monkey(&self) -> impl Fn(usize, ProcStopReason) + 'static {
        let todo_tx = self.todo_tx.clone();
        move |rank, reason| {
            todo_tx.send(Action::Stop(rank, reason)).unwrap();
        }
    }

    /// A function to shut down the alloc for testing purposes.
    pub(crate) fn stopper(&self) -> impl Fn() + 'static {
        let todo_tx = self.todo_tx.clone();
        let size = self.size();
        move || {
            for rank in 0..size {
                todo_tx
                    .send(Action::Stop(rank, ProcStopReason::Stopped))
                    .unwrap();
            }
            todo_tx.send(Action::Stopped).unwrap();
        }
    }

    pub(crate) fn name(&self) -> &ShortUuid {
        &self.name
    }

    pub(crate) fn size(&self) -> usize {
        self.spec.extent.num_ranks()
    }
}

#[async_trait]
impl Alloc for LocalAlloc {
    async fn next(&mut self) -> Option<ProcState> {
        if self.stopped {
            return None;
        }
        if self.failed && !self.stopped {
            // Failed alloc. Wait for stop().
            futures::future::pending::<()>().await;
            unreachable!("future::pending completed");
        }
        let event = loop {
            if let state @ Some(_) = self.queue.pop_front() {
                break state;
            }

            match self.todo_rx.recv().await? {
                Action::Start(rank) => {
                    let (addr, proc_rx) = loop {
                        match channel::serve(ChannelAddr::any(self.transport())) {
                            Ok(addr_and_proc_rx) => break addr_and_proc_rx,
                            Err(err) => {
                                tracing::error!(
                                    "failed to create channel for rank {}: {}",
                                    rank,
                                    err
                                );
                                #[allow(clippy::disallowed_methods)]
                                sleep(Duration::from_secs(1)).await;
                                continue;
                            }
                        }
                    };

                    let proc_name = match &self.spec.proc_name {
                        Some(name) => name.clone(),
                        None => format!("{}_{}", self.world_id.name(), rank),
                    };
                    let proc_id = ProcId(addr.clone(), proc_name);

                    let bspan = tracing::info_span!("mesh_agent_bootstrap");
                    let (proc, mesh_agent) = match ProcMeshAgent::bootstrap(proc_id.clone()).await {
                        Ok(proc_and_agent) => proc_and_agent,
                        Err(err) => {
                            let message = format!("failed spawn mesh agent for {}: {}", rank, err);
                            tracing::error!(message);
                            // It's unclear if this is actually recoverable in a practical sense,
                            // so we give up.
                            self.failed = true;
                            break Some(ProcState::Failed {
                                world_id: self.world_id.clone(),
                                description: message,
                            });
                        }
                    };
                    drop(bspan);

                    // Undeliverable messages get forwarded to the mesh agent.
                    let handle = proc.clone().serve(proc_rx);

                    let create_key = ShortUuid::generate();

                    self.procs.insert(
                        rank,
                        LocalProc {
                            proc,
                            create_key: create_key.clone(),
                            addr: addr.clone(),
                            handle,
                        },
                    );

                    let point = match self.spec.extent.point_of_rank(rank) {
                        Ok(point) => point,
                        Err(err) => {
                            tracing::error!("failed to get point for rank {}: {}", rank, err);
                            return None;
                        }
                    };
                    let created = ProcState::Created {
                        create_key: create_key.clone(),
                        point,
                        pid: std::process::id(),
                    };
                    self.queue.push_back(ProcState::Running {
                        create_key,
                        proc_id,
                        mesh_agent: mesh_agent.bind(),
                        addr,
                    });
                    break Some(created);
                }
                Action::Stop(rank, reason) => {
                    let Some(mut proc_to_stop) = self.procs.remove(&rank) else {
                        continue;
                    };

                    // Stop serving the mailbox.
                    proc_to_stop.handle.stop("received Action::Stop");

                    if let Err(err) = proc_to_stop
                        .proc
                        .destroy_and_wait::<()>(
                            Duration::from_millis(10),
                            None,
                            &reason.to_string(),
                        )
                        .await
                    {
                        tracing::error!("error while stopping proc {}: {}", rank, err);
                    }
                    break Some(ProcState::Stopped {
                        reason,
                        create_key: proc_to_stop.create_key.clone(),
                    });
                }
                Action::Stopped => break None,
            }
        };
        self.stopped = event.is_none();
        event
    }

    fn spec(&self) -> &AllocSpec {
        &self.spec
    }

    fn extent(&self) -> &Extent {
        &self.spec.extent
    }

    fn world_id(&self) -> &WorldId {
        &self.world_id
    }

    async fn stop(&mut self) -> Result<(), AllocatorError> {
        tracing::info!(
            name = "LocalAllocStatus",
            alloc_name = %self.world_id(),
            status = "Stopping",
        );
        for rank in 0..self.size() {
            self.todo_tx
                .send(Action::Stop(rank, ProcStopReason::Stopped))
                .unwrap();
        }
        self.todo_tx.send(Action::Stopped).unwrap();
        tracing::info!(
            name = "LocalAllocStatus",
            alloc_name = %self.world_id(),
            status = "Stop::Sent",
            "Stop was sent to local procs; check their log to determine if it exited."
        );
        Ok(())
    }

    fn is_local(&self) -> bool {
        true
    }
}

impl Drop for LocalAlloc {
    fn drop(&mut self) {
        tracing::info!(
            name = "LocalAllocStatus",
            alloc_name = %self.world_id(),
            status = "Dropped",
            "dropping LocalAlloc of name: {}, world id: {}",
            self.name,
            self.world_id
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    crate::alloc_test_suite!(LocalAllocator);
}
