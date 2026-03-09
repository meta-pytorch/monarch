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

use async_trait::async_trait;
use hyperactor::ActorRef;
use hyperactor::channel::ChannelAddr;
use hyperactor::host::Host;
use hyperactor::host::LocalProcManager;
use ndslice::view::Extent;
use tokio::sync::mpsc;

use super::ProcStopReason;
use crate::alloc::Alloc;
use crate::alloc::AllocName;
use crate::alloc::AllocSpec;
use crate::alloc::Allocator;
use crate::alloc::AllocatorError;
use crate::alloc::ProcState;
use crate::host_mesh::host_agent;
use crate::host_mesh::host_agent::HostAgent;
use crate::host_mesh::host_agent::HostAgentMode;
use crate::host_mesh::host_agent::ProcManagerSpawnFn;
use crate::proc_agent::ProcAgent;
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
            alloc_name = %alloc.alloc_name(),
            status = "Allocated",
        );
        Ok(alloc)
    }
}

struct LocalProc {
    create_key: ShortUuid,
    host_addr: ChannelAddr,
    host_agent: ActorRef<HostAgent>,
}

/// A local allocation. It is a collection of procs that are running in the local process.
pub struct LocalAlloc {
    spec: AllocSpec,
    name: ShortUuid,
    alloc_name: AllocName,
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
            alloc_name: AllocName(name.to_string()),
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
                    let addr = ChannelAddr::any(self.transport());

                    // Create an in-process Host with a ProcAgent boot function.
                    let spawn: ProcManagerSpawnFn =
                        Box::new(|proc| Box::pin(std::future::ready(ProcAgent::boot(proc, None))));
                    let manager = LocalProcManager::new(spawn);
                    let host = match Host::new(manager, addr).await {
                        Ok(host) => host,
                        Err(err) => {
                            let message =
                                format!("failed to create host for rank {}: {}", rank, err);
                            tracing::error!(message);
                            self.failed = true;
                            break Some(ProcState::Failed {
                                alloc_name: self.alloc_name.clone(),
                                description: message,
                            });
                        }
                    };
                    let host_addr = host.addr().clone();
                    let system_proc = host.system_proc().clone();
                    let host_mesh_agent = match system_proc.spawn(
                        host_agent::HOST_MESH_AGENT_ACTOR_NAME,
                        HostAgent::new(HostAgentMode::Local(host)),
                    ) {
                        Ok(handle) => handle,
                        Err(err) => {
                            let message =
                                format!("failed to spawn host agent for rank {}: {}", rank, err);
                            tracing::error!(message);
                            self.failed = true;
                            break Some(ProcState::Failed {
                                alloc_name: self.alloc_name.clone(),
                                description: message,
                            });
                        }
                    };
                    let host_agent_ref = host_mesh_agent.bind::<HostAgent>();

                    let create_key = ShortUuid::generate();

                    self.procs.insert(
                        rank,
                        LocalProc {
                            create_key: create_key.clone(),
                            host_addr: host_addr.clone(),
                            host_agent: host_agent_ref.clone(),
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
                        host_addr,
                        host_agent: host_agent_ref,
                    });
                    break Some(created);
                }
                Action::Stop(rank, reason) => {
                    let Some(proc_to_stop) = self.procs.remove(&rank) else {
                        continue;
                    };
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

    fn alloc_name(&self) -> &AllocName {
        &self.alloc_name
    }

    async fn stop(&mut self) -> Result<(), AllocatorError> {
        tracing::info!(
            name = "LocalAllocStatus",
            alloc_name = %self.alloc_name(),
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
            alloc_name = %self.alloc_name(),
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
            alloc_name = %self.alloc_name(),
            status = "Dropped",
            "dropping LocalAlloc of name: {}, alloc_name: {}",
            self.name,
            self.alloc_name
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    crate::alloc_test_suite!(LocalAllocator);
}
