/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! # Parameter Server Example using RDMA
//!
//! This example demonstrates a distributed parameter server architecture using RDMA buffers.
//! It shows a canonical pattern for using RdmaBuffer to efficiently share memory between actors.
//!
//! ## Architecture
//!
//! - A central parameter server actor maintains shared weights
//! - Multiple worker actors read weights from and write gradients to the parameter server
//! - RDMA is used for zero-copy data transfer between the parameter server and workers
//!
//! ## Flow
//!
//! 1. Parameter server initializes weights and gradient buffers (one per worker)
//! 2. Workers connect to the parameter server and get handles to the buffers
//! 3. For each training step:
//!    - Workers compute gradients locally. This is trivial: gradients = weights + 1
//!    - Workers push gradients to their assigned gradient buffer on the parameter server using RDMA
//!    - Parameter server updates weights by applying all gradients. This is trivial: new_weights = old_weights + sum(gradients)
//!    - Workers pull updated weights from the parameter server using RDMA
//!
//! ## Key Components
//!
//! - `ParameterServerActor`: Manages shared weights and per-worker gradient buffers
//! - `WorkerActor`: Computes gradients and applies updates from the parameter server
//! - `RdmaBuffer`: Provides the zero-copy memory access between actors
//! - `RdmaManagerActor`: Handles the underlying RDMA connections and operations
//!
//! This pattern can be extended to implement more complex distributed training systems
//! and serves as a reference for Python integration with RDMA capabilities.
//!
//! # To run this
//!
//! $ buck2 run @//mode/opt  //monarch/monarch_rdma/examples:parameter_server_example
//!
//! Make sure your dev machine has a backend network, i.e.
//! $ cat /etc/fbwhoami | grep DEVICE_BACKEND_NETWORK_TOPOLOGY
//!
//! should not be empty - it should show something like this:
//! $ cat /etc/fbwhoami | grep DEVICE_BACKEND_NETWORK_TOPOLOGY
//! > DEVICE_BACKEND_NETWORK_TOPOLOGY=gtn2/gtn2.2C//rtsw107.c083.f00.gtn2
//!
//! To run the tests:
//!
//! $ buck2 test @//mode/opt  //monarch/monarch_rdma/examples:parameter_server
//! or
//! $ buck2 run @//mode/opt  //monarch/monarch_rdma/examples:parameter_server-unittest
use std::collections::HashMap;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorRef;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Named;
use hyperactor::OncePortRef;
use hyperactor::PortRef;
use hyperactor::message::IndexedErasedUnbound;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_mesh::Mesh;
use hyperactor_mesh::ProcMesh;
use hyperactor_mesh::RootActorMesh;
use hyperactor_mesh::actor_mesh::ActorMesh;
use hyperactor_mesh::actor_mesh::Cast;
use hyperactor_mesh::alloc::AllocConstraints;
use hyperactor_mesh::alloc::AllocSpec;
use hyperactor_mesh::alloc::Allocator;
use hyperactor_mesh::alloc::ProcessAllocator;
use monarch_rdma::IbverbsConfig;
use monarch_rdma::RdmaBuffer;
use monarch_rdma::RdmaManagerActor;
use monarch_rdma::RdmaMemoryRegionView;
use ndslice::selection;
use ndslice::shape;
use serde::Deserialize;
use serde::Serialize;
use tokio::process::Command;

// Constants to control the setup.
const BUFFER_SIZE: usize = 8;

// Parameter Server Actor
#[derive(Debug)]
#[hyperactor::export_spawn(PsGetBuffers, PsUpdate, Log)]
pub struct ParameterServerActor {
    weights_data: Box<[u8]>,
    grad_buffer_data: Box<[Box<[u8]>]>,
    weights_handle: Option<RdmaBuffer>,
    grad_buffer_handles: HashMap<usize, RdmaBuffer>,
    owner_ref: ActorRef<RdmaManagerActor>,
}

#[async_trait]
impl Actor for ParameterServerActor {
    type Params = (ActorRef<RdmaManagerActor>, usize);

    async fn new(_params: Self::Params) -> Result<Self, anyhow::Error> {
        let (owner_ref, worker_world_size) = _params;
        println!("creating parameter server actor");
        let weights_data = vec![0u8; BUFFER_SIZE].into_boxed_slice();
        let grad_buffer_data =
            vec![vec![0u8; BUFFER_SIZE].into_boxed_slice(); worker_world_size].into_boxed_slice();

        // Note that Rdma handles must be initialized when the actor actually exists...
        Ok(Self {
            weights_data,
            grad_buffer_data,
            weights_handle: None,
            grad_buffer_handles: HashMap::new(),
            owner_ref,
        })
    }

    async fn handle_supervision_event(
        &mut self,
        _this: &Instance<Self>,
        _event: &ActorSupervisionEvent,
    ) -> Result<bool, anyhow::Error> {
        tracing::error!("parameterServerActor supervision event: {:?}", _event);
        tracing::error!(
            "parameterServerActor error occurred, stop the worker process, exit code: 1"
        );
        std::process::exit(1);
    }
}

// Message to get handles to the parameter server's weights and gradient buffers.
// - OncePortRef<(RdmaBuffer, RdmaBuffer)>: OncePortRef to the parameter server's weights and gradient buffers.
#[derive(Debug, Serialize, Deserialize, Named, Clone)]
struct PsGetBuffers(pub usize, pub OncePortRef<(RdmaBuffer, RdmaBuffer)>);

// Message to update the parameter server's weights with its current gradient buffer.
// - OncePortRef<bool>: OncePortRef used primarily for workload synchronization.
#[derive(Debug, Serialize, Deserialize, Named, Clone)]
struct PsUpdate(pub OncePortRef<bool>);

// Message to log actors' weights and gradients.
#[derive(Debug, Serialize, Deserialize, Named, Clone)]
struct Log;

#[async_trait]
impl Handler<PsGetBuffers> for ParameterServerActor {
    /// Returns RdmaBuffers for weights data and gradients data. Creates handles if necessary.
    async fn handle(
        &mut self,
        this: &Instance<Self>,
        PsGetBuffers(rank, reply): PsGetBuffers,
    ) -> Result<(), anyhow::Error> {
        if self.weights_handle.is_none() {
            let client = this.mailbox_for_py();

            let mr = RdmaMemoryRegionView::from_boxed_slice(&self.weights_data);
            println!(
                "[parameter server actor] creating RdmaBuffer for weights data (mr: {:?})",
                mr
            );

            let weights_handle = RdmaBuffer::new(
                "weights_buffer".to_string(),
                self.owner_ref.clone(),
                client,
                mr,
            )
            .await?;
            self.weights_handle = Some(weights_handle);
        }
        let weights_handle = self
            .weights_handle
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("weights handle is not initialized"))?;

        let entry = self.grad_buffer_handles.entry(rank);
        let grad_buffer_handle = match entry {
            std::collections::hash_map::Entry::Occupied(e) => e.get().clone(),
            std::collections::hash_map::Entry::Vacant(e) => {
                let client = this.mailbox_for_py();
                let mr = RdmaMemoryRegionView::from_boxed_slice(&self.grad_buffer_data[rank]);
                println!(
                    "[parameter server actor] creating rdma buffer for gradients for worker {} (mr: {:?})",
                    rank, mr
                );
                let grad_buffer_handle = RdmaBuffer::new(
                    format!("gradients_buffer_{}", rank),
                    self.owner_ref.clone(),
                    client,
                    mr,
                )
                .await?;
                e.insert(grad_buffer_handle.clone());
                grad_buffer_handle
            }
        };
        reply.send(this, (weights_handle.clone(), grad_buffer_handle.clone()))?;
        Ok(())
    }
}

#[async_trait]
impl Handler<PsUpdate> for ParameterServerActor {
    /// Updates the parameter server's weights, given data in the gradients buffers. Gradients are wiped afterwards.
    async fn handle(
        &mut self,
        this: &Instance<Self>,
        PsUpdate(reply): PsUpdate,
    ) -> Result<(), anyhow::Error> {
        for grad in self.grad_buffer_data.iter_mut() {
            for (weight, grad_value) in self.weights_data.iter_mut().zip(grad.iter()) {
                *weight = weight.wrapping_add(*grad_value);
            }
            grad.fill(0);
        }
        println!("[parameter server actor] updated");
        reply.send(this, true)?;
        Ok(())
    }
}

#[async_trait]
impl Handler<Log> for ParameterServerActor {
    /// Logs the server's weights and gradient buffer
    async fn handle(&mut self, _this_: &Instance<Self>, _msg_: Log) -> Result<(), anyhow::Error> {
        println!(
            "[parameter server actor] weights: {:?}, grad_buffer: {:?}",
            self.weights_data, self.grad_buffer_data,
        );
        Ok(())
    }
}

// Worker Actor
#[derive(Debug)]
#[hyperactor::export_spawn(
    Cast<WorkerInit>, IndexedErasedUnbound<Cast<WorkerInit>>,
    Cast<WorkerStep>, IndexedErasedUnbound<Cast<WorkerStep>>,
    Cast<WorkerUpdate>, IndexedErasedUnbound<Cast<WorkerUpdate>>,
    Cast<Log>, IndexedErasedUnbound<Cast<Log>>,
)]
pub struct WorkerActor {
    ps_weights_handle: Option<RdmaBuffer>,
    ps_grad_handle: Option<RdmaBuffer>,
    weights_data: Box<[u8]>,
    local_gradients: Box<[u8]>,
    rdma_manager: Option<ActorRef<RdmaManagerActor>>,
}

#[async_trait]
impl Actor for WorkerActor {
    type Params = ();

    async fn new(_params: Self::Params) -> Result<Self, anyhow::Error> {
        let weights_data = vec![0u8; BUFFER_SIZE].into_boxed_slice();
        let local_gradients = vec![0u8; BUFFER_SIZE].into_boxed_slice();
        Ok(Self {
            ps_weights_handle: None,
            ps_grad_handle: None,
            weights_data,
            local_gradients,
            rdma_manager: None,
        })
    }

    async fn handle_supervision_event(
        &mut self,
        _this: &Instance<Self>,
        _event: &ActorSupervisionEvent,
    ) -> Result<bool, anyhow::Error> {
        tracing::error!("workerActor supervision event: {:?}", _event);
        tracing::error!("workerActor error occurred, stop the worker process, exit code: 1");
        std::process::exit(1);
    }
}

// Message to initialize the worker.
// This message is sent to workers to establish their connection with the parameter server
// and obtain handles to the shared weights and gradient buffers.
// - ActorRef<RdmaManagerActor>: the actor ref to the parameter server
// - Vec<ActorRef<RdmaManagerActor>>: the list of RdmaManagerActors. Used for the worker to get
//   given its casted rank.
#[derive(Debug, Serialize, Deserialize, Named, Clone)]
pub struct WorkerInit(
    pub ActorRef<ParameterServerActor>,
    pub Vec<ActorRef<RdmaManagerActor>>,
);

// Message to signal the worker to update its gradients and transmit them to the server.
// The PortRef<bool> is used to notify the main process when the operation completes.
// - Workers compute local gradients (weights + 1)
// - Workers write these gradients to their assigned buffer on the parameter server using RDMA
#[derive(Debug, Serialize, Deserialize, Named, Clone)]
pub struct WorkerStep(PortRef<bool>);

// Message to signal the worker to pull updated weights from the parameter server.
// The PortRef<bool> is used to notify the main process when the operation completes.
// - Workers read the updated weights from the parameter server using RDMA
// - This happens after the parameter server has applied all gradients to update the weights
#[derive(Debug, Serialize, Deserialize, Named, Clone)]
pub struct WorkerUpdate(PortRef<bool>);

#[async_trait]
impl Handler<Cast<WorkerInit>> for WorkerActor {
    /// Initialize the worker. This involves:
    /// 1) getting RdmaBuffers from the parameter server
    /// 2) assigning the associated rdma manager
    async fn handle(
        &mut self,
        this: &Instance<Self>,
        Cast {
            rank,
            message: WorkerInit(ps_ref, rdma_managers),
            ..
        }: Cast<WorkerInit>,
    ) -> Result<(), anyhow::Error> {
        println!("[worker_actor_{}] initializing", *rank);

        let client = this.mailbox_for_py();
        let (handle, receiver) = client.open_once_port::<(RdmaBuffer, RdmaBuffer)>();
        ps_ref.send(client, PsGetBuffers(*rank, handle.bind()))?;
        let (ps_weights_handle, ps_grad_handle) = receiver.recv().await?;
        self.ps_weights_handle = Some(ps_weights_handle);
        self.ps_grad_handle = Some(ps_grad_handle);
        if let Some(rdma_manager) = rdma_managers.get(*rank) {
            self.rdma_manager = Some(rdma_manager.clone());
        } else {
            return Err(anyhow::anyhow!(
                "Invalid rank: {}. No RDMA manager found.",
                *rank
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<Cast<WorkerStep>> for WorkerActor {
    /// Takes a worker step. This involves:
    /// 1) calculating the gradient (worker + 1)
    /// 2) transmitting it to the parameter server over rdma
    /// 3) resetting the gradient to 0
    async fn handle(
        &mut self,
        this: &Instance<Self>,
        Cast {
            rank,
            message: WorkerStep(reply),
            ..
        }: Cast<WorkerStep>,
    ) -> Result<(), anyhow::Error> {
        for (grad_value, weight) in self
            .local_gradients
            .iter_mut()
            .zip(self.weights_data.iter())
        {
            *grad_value = grad_value.wrapping_add(*weight + 1);
        }
        println!(
            "[worker_actor_{}] pushing gradients {:?}",
            *rank, self.local_gradients
        );

        let mr = RdmaMemoryRegionView::from_boxed_slice(&self.local_gradients);
        let client = this.mailbox_for_py();

        let owner_ref = self
            .rdma_manager
            .as_ref()
            .expect("worker should have been initialized");
        let ps_grad_handle = self
            .ps_grad_handle
            .as_mut()
            .expect("worker_actor should be initialized");
        ps_grad_handle
            .write_from(mr, client, owner_ref, Some(5))
            .await?;

        self.local_gradients.fill(0);

        reply.send(this, true)?;
        Ok(())
    }
}

#[async_trait]
impl Handler<Cast<WorkerUpdate>> for WorkerActor {
    /// Pulls weights from the parameter server to the worker
    async fn handle(
        &mut self,
        this: &Instance<Self>,
        Cast {
            rank,
            message: WorkerUpdate(reply),
            ..
        }: Cast<WorkerUpdate>,
    ) -> Result<(), anyhow::Error> {
        println!(
            "[worker_actor_{}] pulling new weights from parameter server (before: {:?})",
            *rank, self.weights_data,
        );
        let mr = RdmaMemoryRegionView::from_boxed_slice(&self.weights_data);
        let client = this.mailbox_for_py();

        let owner_ref = self
            .rdma_manager
            .as_ref()
            .expect("worker should have been initialized");
        let ps_weights_handle = self
            .ps_weights_handle
            .as_mut()
            .expect("worker_actor should be initialized");
        ps_weights_handle
            .read_into(mr, client, owner_ref, Some(5))
            .await?;
        reply.send(this, true)?;
        Ok(())
    }
}

#[async_trait]
impl Handler<Cast<Log>> for WorkerActor {
    /// Logs the worker's weights
    async fn handle(
        &mut self,
        _this_: &Instance<Self>,
        Cast {
            rank, message: Log, ..
        }: Cast<Log>,
    ) -> Result<(), anyhow::Error> {
        println!("[worker_actor_{}] weights: {:?}", *rank, self.weights_data);
        Ok(())
    }
}

/// Main function
pub async fn run(num_workers: usize, num_steps: usize) -> Result<(), anyhow::Error> {
    // Enable the display of all tracing:info logs
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let devices = monarch_rdma::get_all_devices();

    // In this example, the parameter server and workers all live on the same host.
    // Parameter server is assigned to its unique ibverbs device, and all workers
    // share the same ibverbs device.
    // In practice, this toy example could have a single ibverbs device shared across
    // all entities, but this serves to demonstrate that we can specify the underlying
    // device used.
    let ps_ibv_config: IbverbsConfig;
    let worker_ibv_config: IbverbsConfig;

    if devices.len() == 12 {
        // On H100 machines with 12 devices, use specific devices
        ps_ibv_config = IbverbsConfig {
            device: devices.clone().into_iter().next().unwrap(),
            ..Default::default()
        };
        // The second device used is the 3rd. Main reason is because 0 and 3 are both backend
        // devices on gtn H100 devices.
        worker_ibv_config = IbverbsConfig {
            device: devices.clone().into_iter().nth(3).unwrap(),
            ..Default::default()
        };
    } else {
        // For other configurations, use default settings (parameter server + workers all use the same ibv device)
        println!(
            "using default IbverbsConfig as {} devices were found (expected 12 for H100)",
            devices.len()
        );
        ps_ibv_config = IbverbsConfig::default();
        worker_ibv_config = IbverbsConfig::default();
    }

    // As normal, create a proc mesh for the parameter server.
    println!("creating parameter server proc mesh...");

    let mut alloc = ProcessAllocator::new(Command::new(
        buck_resources::get("monarch/monarch_rdma/examples/bootstrap").unwrap(),
    ));

    let ps_proc_mesh = ProcMesh::allocate(
        alloc
            .allocate(AllocSpec {
                shape: shape! {replica=1, host=1, gpu=1},
                constraints: Default::default(),
            })
            .await?,
    )
    .await?;

    println!(
        "creating parameter server's RDMA manager with config: {}",
        ps_ibv_config
    );

    // RdmaBuffer requires an RdmaManagerActor to be spawned on the same
    // host for any actors using RdmaBuffer.
    // We spin this up manually here, but in Python-land we assume this will
    // be spun up with the PyProcMesh.
    let ps_rdma_manager: RootActorMesh<'_, RdmaManagerActor> = ps_proc_mesh
        .spawn("ps_rdma_manager", &ps_ibv_config)
        .await
        .unwrap();

    // Create a proc mesh for workers, where each worker is assigned to its own GPU.
    println!("creating worker proc mesh ({} workers)...", num_workers);
    let worker_proc_mesh = ProcMesh::allocate(
        alloc
            .allocate(AllocSpec {
                shape: shape! {replica=1, host=1, gpu=num_workers},
                constraints: Default::default(),
            })
            .await?,
    )
    .await?;

    println!(
        "creating worker's RDMA manager with config: {}",
        worker_ibv_config
    );
    // Similarly, create an RdmaManagerActor corresponding to each worker.
    let worker_rdma_manager_mesh: RootActorMesh<'_, RdmaManagerActor> = worker_proc_mesh
        .spawn("ps_rdma_manager", &worker_ibv_config)
        .await
        .unwrap();

    println!("spawning parameter server");
    let ps_actor_mesh: RootActorMesh<'_, ParameterServerActor> = ps_proc_mesh
        .spawn(
            "parameter_server",
            &(ps_rdma_manager.iter().next().unwrap(), num_workers),
        )
        .await
        .unwrap();

    // The parameter server is a single actor, we can just grab it and call it directly.
    let ps_actor = ps_actor_mesh.iter().next().unwrap();

    println!("spawning worker actors");
    let worker_actor_mesh: RootActorMesh<'_, WorkerActor> =
        worker_proc_mesh.spawn("worker_actors", &()).await.unwrap();

    let worker_rdma_managers: Vec<ActorRef<RdmaManagerActor>> =
        worker_rdma_manager_mesh.iter().collect();

    // We intentionally decouple spawning with initialization, which is fairly common in Ray workloads
    // In this case, we use it for dual purpose - be able to use the cast APIs to assign rank (Monarch specific) and
    // to get access to return values for error messaging (applies to both Monarch and Ray)
    println!("initializing worker actor mesh");
    worker_actor_mesh
        .cast(
            selection::selection_from(worker_actor_mesh.shape(), &[("gpu", 0..num_workers)])
                .unwrap(),
            WorkerInit(ps_actor.clone(), worker_rdma_managers),
        )
        .unwrap();

    println!("starting training loop");
    for step in 0..num_steps {
        println!("===== starting step {} =====", step);
        worker_actor_mesh
            .cast(
                selection::selection_from(worker_actor_mesh.shape(), &[("gpu", 0..num_workers)])
                    .unwrap(),
                Log {},
            )
            .unwrap();

        let (handle, mut recv) = worker_proc_mesh.client().open_port::<bool>();
        worker_actor_mesh
            .cast(
                selection::selection_from(worker_actor_mesh.shape(), &[("gpu", 0..num_workers)])
                    .unwrap(),
                WorkerStep(handle.bind()),
            )
            .unwrap();

        let mut results = Vec::new();
        for _ in 0..num_workers {
            let finished = recv.recv().await.unwrap();
            results.push(finished);
        }
        if !results.iter().any(|&result| result) {
            panic!("worker update step did not complete properly.");
        }

        let (handle, recv) = worker_proc_mesh.client().open_once_port::<bool>();
        ps_actor
            .send(ps_proc_mesh.client(), PsUpdate(handle.bind()))
            .unwrap();

        let finished = recv.recv().await.unwrap();
        if !finished {
            panic!("ps actor step did not complete properly");
        }

        let (handle, mut recv) = worker_proc_mesh.client().open_port::<bool>();
        worker_actor_mesh
            .cast(
                selection::selection_from(worker_actor_mesh.shape(), &[("gpu", 0..num_workers)])
                    .unwrap(),
                WorkerUpdate(handle.bind()),
            )
            .unwrap();

        let mut results = Vec::new();
        for _ in 0..num_workers {
            let finished = recv.recv().await.unwrap();
            results.push(finished);
        }
        if !results.iter().any(|&result| result) {
            panic!("worker update step did not complete properly.");
        }
        ps_actor.send(ps_proc_mesh.client(), Log {}).unwrap();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[timed_test::async_timed_test(timeout_secs = 60)]
    async fn test_parameter_server() -> Result<(), anyhow::Error> {
        run(1, 4).await
    }
}
