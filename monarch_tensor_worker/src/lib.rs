/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![feature(assert_matches)]
#![feature(duration_constructors)]
#![feature(exit_status_error)]
// NOTE: Until https://github.com/PyO3/pyo3/pull/4674, `pyo3::pymethods` trigger
// and unsafe-op-in-unsafe-fn warnings.
#![allow(unsafe_op_in_unsafe_fn)]

//! A `hyperactor`-based implementation of a PyTorch worker actor.
//!
//! The worker is responsible for executing PyTorch operations on a local
//! device. It assumes it has exclusive access to device resources, and manages
//! concurrency internally via device-specific constructs (CUDA stream, threads,
//! etc.).
//!
//! This is a port of `monarch/python/controller/worker.py` but does have gaps due
//! to drift that needs to be reconciled.
//! This mainly includes:
//! - Support for record and replay
//! - debugger support
//! - general drift in exisitng messages

mod borrow;
mod comm;
pub mod device_mesh;
pub mod stream;
pub mod test_util;

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::ensure;
use async_trait::async_trait;
use borrow::Borrow;
use comm::CommMessageClient;
use comm::CommParams;
use comm::NcclCommActor;
use derive_more::TryInto;
use device_mesh::DeviceMesh;
use futures::future::try_join_all;
use hyperactor::Actor;
use hyperactor::ActorRef;
use hyperactor::Bind;
use hyperactor::Handler;
use hyperactor::RemoteSpawn;
use hyperactor::Unbind;
use hyperactor::actor::ActorHandle;
use hyperactor::context;
use hyperactor::reference::ActorId;
use hyperactor_config::Flattrs;
use hyperactor_mesh::comm::multicast::CastInfo;
use itertools::Itertools;
use monarch_hyperactor::shape::PyPoint;
use monarch_messages::controller::ControllerActor;
use monarch_messages::controller::ControllerMessageClient;
use monarch_messages::controller::Seq;
use monarch_messages::wire_value::WireValue;
use monarch_messages::worker::ActorCallParams;
use monarch_messages::worker::ActorMethodParams;
use monarch_messages::worker::ArgsKwargs;
use monarch_messages::worker::CallFunctionParams;
use monarch_messages::worker::Factory;
use monarch_messages::worker::Reduction;
use monarch_messages::worker::Ref;
use monarch_messages::worker::ResolvableFunction;
use monarch_messages::worker::StreamCreationMode;
use monarch_messages::worker::StreamRef;
use monarch_messages::worker::WorkerMessage;
use monarch_messages::worker::WorkerMessageHandler;
use monarch_messages::worker::WorkerParams;
use ndslice::Slice;
use pyo3::Python;
use pyo3::types::PyAnyMethods;
use serde::Deserialize;
use serde::Serialize;
use sorted_vec::SortedVec;
use stream::StreamActor;
use stream::StreamMessageClient;
use stream::StreamParams;
use torch_sys_cuda::nccl::ReduceOp;
use torch_sys_cuda::nccl::UniqueId;
use torch_sys2::CudaDevice;
use torch_sys2::DeviceIndex;
use torch_sys2::Layout;
use torch_sys2::ScalarType;
use torch_sys2::TensorCell;
use torch_sys2::factory_zeros;
use typeuri::Named;

#[derive(Debug)]
struct RemoteProcessGroupState {
    device_mesh_ref: Ref,
    dims: SortedVec<String>,
    comms: HashMap<StreamRef, Arc<ActorHandle<NcclCommActor>>>,
}

impl RemoteProcessGroupState {
    fn new(device_mesh_ref: Ref, dims: SortedVec<String>) -> Self {
        Self {
            device_mesh_ref,
            dims,
            comms: HashMap::new(),
        }
    }
}

#[derive(Debug)]
enum Recording {
    // In the process of receiving DefineRecording messages for this
    // recording.
    PartialRecording {
        // The index of the last DefineRecording message received.
        last_index: usize,
        // The list of commands seen so far for this recording.
        commands: Vec<WorkerMessage>,
    },

    // The recording is ready to be run.
    CompleteRecording {
        // The list of streams on which this recording is defined.
        streams: HashSet<StreamRef>,
    },
}

/// A PyTorch runtime instance, operating on a single accelerator device,
/// controlled via hyperactor messaging.
///
/// Generally this is a thin multiplexer over a set of [`Stream`]s that do the
/// real work.
///
/// See [`WorkerMessage`] for what it can do!
#[derive(Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [
        WorkerMessage {cast = true},
        AssignRankMessage {cast = true},
    ],
)]
pub struct WorkerActor {
    device: Option<CudaDevice>,
    streams: HashMap<StreamRef, Arc<ActorHandle<StreamActor>>>,
    /// Maps streams to the device mesh and a map of dim names to the concrete
    /// communicator actor that represents the dimension for that stream.
    device_meshes: HashMap<
        Ref,
        (
            DeviceMesh,
            // A map for comms for this mesh for a given pair of stream and dims.
            HashMap<(StreamRef, SortedVec<String>), (usize, Arc<ActorHandle<NcclCommActor>>)>,
        ),
    >,
    world_size: usize,
    rank: usize,
    borrows: HashMap<u64, Borrow>,
    comm: Option<ActorHandle<NcclCommActor>>,
    controller_actor: ActorRef<ControllerActor>,
    /// Remember the process groups "created" via `CreateRemoteProcessGroup` for
    /// subsequent `CallFunction` calls, as this is where the actual allocation
    /// will happen.
    remote_process_groups: HashMap<Ref, RemoteProcessGroupState>,
    /// The comm actor for each pair of streams that need to send/recv tensors.
    send_recv_comms: HashMap<(StreamRef, StreamRef), Arc<ActorHandle<NcclCommActor>>>,
    recordings: HashMap<Ref, Recording>,
    defining_recording: Option<Ref>,
    respond_with_python_message: bool,
}

impl WorkerActor {
    fn try_get_stream(&self, stream: StreamRef) -> Result<&Arc<ActorHandle<StreamActor>>> {
        self.streams
            .get(&stream)
            .ok_or(anyhow::anyhow!("invalid stream id: {:#?}", stream))
    }

    async fn maybe_add_stream_to_recording(
        &mut self,
        cx: &impl context::Actor,
        stream: StreamRef,
    ) -> Result<()> {
        // If we're defining a recording, add the stream to the list of streams that
        // this recording uses, and call define_recording on the stream.
        if let Some(defining_recording) = self.defining_recording {
            let recording = self.recordings.get_mut(&defining_recording).unwrap();
            let fut = match recording {
                Recording::PartialRecording { .. } => panic!("unreachable, in theory"),
                Recording::CompleteRecording { streams } => {
                    streams.insert(stream).then(|| -> Result<_, anyhow::Error> {
                        Ok(self
                            .try_get_stream(stream)?
                            .define_recording(cx, defining_recording))
                    })
                }
            }
            .transpose()?;
            match fut {
                Some(fut) => fut.await,
                None => Ok(()),
            }
        } else {
            Ok(())
        }
    }
}

impl Actor for WorkerActor {}

#[async_trait]
impl RemoteSpawn for WorkerActor {
    type Params = WorkerParams;

    async fn new(
        WorkerParams {
            world_size,
            rank,
            device_index,
            controller_actor,
        }: Self::Params,
        _environment: Flattrs,
    ) -> Result<Self> {
        Python::attach(|py| {
            py.import("monarch.safe_torch").unwrap();
        });
        Ok(Self {
            device: device_index.map(|i| CudaDevice::new(DeviceIndex(i))),
            streams: HashMap::new(),
            device_meshes: HashMap::new(),
            world_size,
            rank,
            borrows: HashMap::new(),
            comm: None,
            controller_actor,
            remote_process_groups: HashMap::new(),
            send_recv_comms: HashMap::new(),
            recordings: HashMap::new(),
            defining_recording: None,
            respond_with_python_message: false,
        })
    }

    // TODO: Exit the worker directly on any worker actor errors, with error exit code.
}

#[async_trait]
impl Handler<AssignRankMessage> for WorkerActor {
    async fn handle(
        &mut self,
        cx: &hyperactor::Context<Self>,
        _: AssignRankMessage,
    ) -> anyhow::Result<()> {
        let point = cx.cast_point();
        self.rank = point.rank();
        self.respond_with_python_message = true;
        Python::attach(|py| {
            let mesh_controller = py.import("monarch.mesh_controller").unwrap();
            let p: PyPoint = point.into();
            mesh_controller
                .call_method1("_initialize_env", (p, cx.proc().proc_id().to_string()))
                .unwrap();
        });
        Ok(())
    }
}

/// Worker messages. These define the observable behavior of the worker, so the
/// documentations here
#[derive(Handler, Clone, Serialize, Deserialize, Debug, Named, Bind, Unbind)]
pub enum AssignRankMessage {
    AssignRank(),
}
wirevalue::register_type!(AssignRankMessage);

#[async_trait]
impl Handler<WorkerMessage> for WorkerActor {
    async fn handle(
        &mut self,
        cx: &hyperactor::Context<Self>,
        message: WorkerMessage,
    ) -> anyhow::Result<()> {
        <Self as WorkerMessageHandler>::handle(self, cx, message).await
    }
}

#[async_trait]
impl WorkerMessageHandler for WorkerActor {
    async fn backend_network_init(
        &mut self,
        cx: &hyperactor::Context<Self>,
        unique_id: UniqueId,
    ) -> Result<()> {
        let device = self
            .device
            .expect("tried to init backend network on a non-CUDA worker");
        let comm = NcclCommActor::new(CommParams::New {
            device,
            unique_id,
            world_size: self.world_size.try_into().unwrap(),
            rank: self.rank.try_into().unwrap(),
        })
        .await?
        .spawn(cx)?;

        let tensor = factory_zeros(&[1], ScalarType::Float, Layout::Strided, device.into());
        let cell = TensorCell::new(tensor);

        comm.all_reduce(
            cx,
            cell,
            ReduceOp::Sum,
            torch_sys_cuda::cuda::Stream::get_current_stream(),
        )
        .await?;

        // TODO: this blocks forward progress of the the actor loop while we
        // wait for the streams to catch up. Once we have a way of spawning
        // tasks that the actor system can monitor in a non-blocking way, we
        // should remove this.

        // We need to be careful to initialize the streams in a consistent order
        // across all workers to avoid NCCL deadlocks. Use the refs to provide
        // this order, as a stream's ref will be the same across all workers.
        let sorted_streams = self
            .streams
            .iter()
            .sorted_by_key(|(k, _)| *k)
            .map(|(_, v)| v.as_ref());

        let mut splits = Vec::new();
        for _ in 0..sorted_streams.len() {
            // Do the split in this event loop, to provide a deterministic
            // order.
            splits.push(comm.split_all(cx).await?);
        }
        let _: Vec<()> = try_join_all(
            sorted_streams
                .into_iter()
                .zip(splits.into_iter())
                .map(|(stream, split)| stream.init_comm(cx, split)),
        )
        .await?;

        self.comm = Some(comm);

        Ok(())
    }

    async fn backend_network_point_to_point_init(
        &mut self,
        cx: &hyperactor::Context<Self>,
        from_stream: StreamRef,
        to_stream: StreamRef,
    ) -> Result<()> {
        if !self.streams.contains_key(&from_stream) {
            bail!("invalid from_stream id: {:#?}", from_stream);
        }
        if !self.streams.contains_key(&to_stream) {
            bail!("invalid to_stream id: {:#?}", to_stream);
        }
        let global_comm = self
            .comm
            .as_ref()
            .context("tried to call Reduce before BackendNetworkInit")?;
        let comm = global_comm.split_all(cx).await?;
        self.send_recv_comms
            .insert((from_stream, to_stream), Arc::new(comm));
        Ok(())
    }

    async fn call_function(
        &mut self,
        cx: &hyperactor::Context<Self>,
        params: CallFunctionParams,
    ) -> Result<()> {
        let stream = self.try_get_stream(params.stream)?.clone();
        self.maybe_add_stream_to_recording(cx, params.stream)
            .await?;

        let device_meshes = self
            .device_meshes
            .iter()
            .map(|(k, v)| (k.clone(), v.0.clone()))
            .collect();

        let mut remote_process_groups = HashMap::new();
        for remote_process_group_ref in &params.remote_process_groups {
            if let Some(state) = self.remote_process_groups.get(remote_process_group_ref) {
                let dims_vec = state.dims.iter().cloned().collect();
                let (device_mesh, _) = self
                    .device_meshes
                    .get(&state.device_mesh_ref)
                    .ok_or_else(|| {
                        anyhow::anyhow!("invalid device mesh id: {:#?}", state.device_mesh_ref)
                    })?
                    .clone();
                let comm = state.comms
                    .get(&params.stream)
                    .ok_or_else(|| {
                        anyhow::anyhow!("no comm found for remote process group {remote_process_group_ref:#?} stream {stream:#?}")
                    })?
                    .clone();
                remote_process_groups.insert(
                    remote_process_group_ref.clone(),
                    (device_mesh, dims_vec, comm),
                );
            }
        }

        stream
            .call_function(cx, params, device_meshes, remote_process_groups)
            .await?;

        Ok(())
    }

    async fn command_group(
        &mut self,
        cx: &hyperactor::Context<Self>,
        params: Vec<WorkerMessage>,
    ) -> Result<()> {
        for msg in params {
            WorkerMessageHandler::handle(self, cx, msg).await?;
        }
        Ok(())
    }

    async fn create_stream(
        &mut self,
        cx: &hyperactor::Context<Self>,
        result: StreamRef,
        creation_mode: StreamCreationMode,
    ) -> Result<()> {
        let handle: ActorHandle<StreamActor> = StreamActor::new(StreamParams {
            world_size: self.world_size,
            rank: self.rank,
            creation_mode,
            id: result,
            device: self.device,
            controller_actor: self.controller_actor.clone(),
            respond_with_python_message: self.respond_with_python_message,
        })
        .spawn(cx)?;
        self.streams.insert(result, Arc::new(handle));
        Ok(())
    }

    async fn create_device_mesh(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        result: Ref,
        names: Vec<String>,
        ranks: Slice,
    ) -> Result<()> {
        self.device_meshes.insert(
            result,
            (DeviceMesh::new(names, ranks, self.rank)?, HashMap::new()),
        );
        Ok(())
    }

    async fn create_remote_process_group(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        result: Ref,
        device_mesh: Ref,
        dims: Vec<String>,
    ) -> Result<()> {
        self.device_meshes
            .get(&device_mesh)
            .with_context(|| format!("invalid device mesh id: {:#?}", device_mesh))?;
        match self.remote_process_groups.entry(result) {
            Entry::Vacant(ent) => ent.insert(RemoteProcessGroupState::new(
                device_mesh,
                SortedVec::from_unsorted(dims),
            )),
            Entry::Occupied(ent) => bail!("remote process group {:?} already create", ent.key()),
        };
        Ok(())
    }

    async fn borrow_create(
        &mut self,
        cx: &hyperactor::Context<Self>,
        result: Ref,
        borrow_id: u64,
        tensor_ref: Ref,
        from_stream: StreamRef,
        to_stream: StreamRef,
    ) -> Result<()> {
        self.maybe_add_stream_to_recording(cx, from_stream).await?;
        self.maybe_add_stream_to_recording(cx, to_stream).await?;
        let from_stream = self.try_get_stream(from_stream)?.clone();
        let to_stream = self.try_get_stream(to_stream)?.clone();

        let borrow =
            Borrow::create(cx, borrow_id, tensor_ref, result, from_stream, to_stream).await?;
        self.borrows.insert(borrow_id, borrow);
        Ok(())
    }

    async fn borrow_first_use(
        &mut self,
        cx: &hyperactor::Context<Self>,
        borrow: u64,
    ) -> Result<()> {
        let borrow = self
            .borrows
            .get_mut(&borrow)
            .ok_or_else(|| anyhow!("invalid borrow id: {:#?}", borrow))?;

        borrow.first_use(cx).await?;
        Ok(())
    }

    async fn borrow_last_use(&mut self, cx: &hyperactor::Context<Self>, borrow: u64) -> Result<()> {
        let borrow = self
            .borrows
            .get_mut(&borrow)
            .ok_or_else(|| anyhow::anyhow!("invalid borrow id: {:#?}", borrow))?;

        borrow.last_use(cx).await?;
        Ok(())
    }

    async fn borrow_drop(&mut self, cx: &hyperactor::Context<Self>, borrow_id: u64) -> Result<()> {
        let borrow = self
            .borrows
            .get_mut(&borrow_id)
            .ok_or_else(|| anyhow::anyhow!("invalid borrow id: {:#?}", borrow_id))?;

        borrow.drop(cx).await?;
        self.borrows.remove(&borrow_id);
        Ok(())
    }

    async fn delete_refs(&mut self, cx: &hyperactor::Context<Self>, refs: Vec<Ref>) -> Result<()> {
        // Fan the delete message to all streams.
        // Check for errors.
        // TODO: this blocks forward progress of the the actor loop while we
        // wait for the streams to catch up. Once we have a way of spawning
        // tasks that the actor system can monitor in a non-blocking way, we
        // should remove this.
        let _: Vec<()> = try_join_all(
            self.streams
                .values()
                .map(|s| s.delete_refs(cx, refs.clone())),
        )
        .await?;
        Ok(())
    }

    async fn request_status(
        &mut self,
        cx: &hyperactor::Context<Self>,
        seq: Seq,
        controller: bool,
    ) -> Result<()> {
        // TODO: this blocks forward progress of the the actor loop while we
        // wait for the streams to catch up. Once we have a way of spawning
        // tasks that the actor system can monitor in a non-blocking way, we
        // should remove this.
        let _: Vec<()> = try_join_all(
            self.streams
                .values()
                .map(|stream| stream.request_status(cx)),
        )
        .await?;

        ControllerMessageClient::status(
            &self.controller_actor,
            cx,
            seq.next(),
            cx.self_id().clone(),
            controller,
        )
        .await?;
        Ok(())
    }

    async fn reduce(
        &mut self,
        cx: &hyperactor::Context<Self>,
        result: Ref,
        local_tensor: Ref,
        factory: Factory,
        source_mesh: Ref,
        stream_ref: StreamRef,
        dims: Vec<String>,
        reduction: Reduction,
        scatter: bool,
        in_place: bool,
        out: Option<Ref>,
    ) -> Result<()> {
        self.maybe_add_stream_to_recording(cx, stream_ref).await?;

        // Sort for stable indexing.
        let dims = SortedVec::from_unsorted(dims);
        let stream = self.try_get_stream(stream_ref)?.clone();

        let (_, comm_map) = self
            .device_meshes
            .get_mut(&source_mesh)
            .ok_or_else(|| anyhow::anyhow!("invalid device mesh id: {:#?}", source_mesh))?;

        let (size, comm) = comm_map
            .get(&(stream_ref, dims.clone()))
            .ok_or_else(|| anyhow::anyhow!("no comm found for stream {stream:#?}, dims {dims:#?}"))?
            .clone();

        stream
            .reduce(
                cx,
                comm,
                size.try_into()?,
                result,
                local_tensor,
                factory,
                reduction,
                scatter,
                in_place,
                out,
            )
            .await?;

        Ok(())
    }

    async fn send_tensor(
        &mut self,
        cx: &hyperactor::Context<Self>,
        result: Ref,
        from_ranks: Slice,
        to_ranks: Slice,
        tensor: Ref,
        factory: Factory,
        from_stream: StreamRef,
        to_stream: StreamRef,
    ) -> Result<()> {
        let comm = self
            .send_recv_comms
            .get(&(from_stream, to_stream))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "could not find stream to stream comm for: {:#?}",
                    (from_stream, to_stream)
                )
            })?
            .clone();

        let to_rank = from_ranks
            .index(self.rank)
            .map(|index| to_ranks.get(index).ok())
            .ok()
            .flatten();
        let from_rank = to_ranks
            .index(self.rank)
            .map(|index| from_ranks.get(index).ok())
            .ok()
            .flatten();

        let (stream, stream_ref) = if to_rank.is_none() {
            (self.try_get_stream(to_stream)?.clone(), to_stream)
        } else if from_rank.is_none() || from_stream == to_stream {
            (self.try_get_stream(from_stream)?.clone(), from_stream)
        } else {
            unimplemented!(
                "We haven't implemented to_mesh between streams if a rank participates as both a sender and receiver. \
                It is possible, but would require the recv stream to send the output buffer tensor to the send stream and sync. \
                Then the send stream would do the nccl op, and then sync with sending stream again."
            );
        };

        self.maybe_add_stream_to_recording(cx, stream_ref).await?;

        stream
            .send_tensor(cx, result, from_rank, to_rank, tensor, factory, comm)
            .await?;

        Ok(())
    }

    async fn exit(
        &mut self,
        cx: &hyperactor::Context<Self>,
        error: Option<(Option<ActorId>, String)>,
    ) -> Result<()> {
        for (_, stream) in self.streams.drain() {
            stream.drain_and_stop("tensor worker exit cleanup")?;
            Arc::into_inner(stream)
                .expect("there should be no owners of this stream handle except the worker stream table")
                .await;
        }

        let self_error_exit_code = std::env::var("MONARCH_TENSOR_WORKER_SELF_ERROR_EXIT_CODE")
            .ok()
            .and_then(|val| val.parse::<i32>().ok())
            .unwrap_or(1);
        let peer_error_exit_code = std::env::var("MONARCH_TENSOR_WORKER_PEER_ERROR_EXIT_CODE")
            .ok()
            .and_then(|val| val.parse::<i32>().ok())
            .unwrap_or(1);

        // Exit the worker process if there is an error.
        let exit_code = match error {
            Some((Some(actor_id), reason)) => {
                tracing::error!(
                    "stopping the worker, actor {} failed with error: {}",
                    actor_id,
                    reason
                );
                if *cx.self_id() == actor_id {
                    self_error_exit_code
                } else {
                    peer_error_exit_code
                }
            }
            Some((None, reason)) => {
                tracing::error!("stopping the worker, reason: {}", reason);
                1
            }
            None => 0,
        };

        if exit_code != 0 {
            tracing::info!("stopping the worker process, exit code: {}", exit_code);
            std::process::exit(exit_code);
        }
        cx.stop("tensor worker exit")?;
        Ok(())
    }

    async fn send_value(
        &mut self,
        cx: &hyperactor::Context<Self>,
        seq: Seq,
        destination: Option<Ref>,
        mutates: Vec<Ref>,
        function: Option<ResolvableFunction>,
        args_kwargs: ArgsKwargs,
        stream: StreamRef,
    ) -> Result<()> {
        // Resolve the stream.
        let stream = self.try_get_stream(stream)?;

        let device_meshes = if function.is_none() {
            HashMap::new()
        } else {
            self.device_meshes
                .iter()
                .map(|(k, v)| (k.clone(), v.0.clone()))
                .collect()
        };

        if destination.is_some() {
            panic!("send_value with pipe destination is no longer implemented")
        }

        // Resolve the value on the stream, then send the value back to the controller.
        stream
            .send_value(
                cx,
                seq,
                cx.self_id().clone(),
                mutates,
                function,
                args_kwargs,
                device_meshes,
            )
            .await
    }

    async fn send_result_of_actor_call(
        &mut self,
        cx: &hyperactor::Context<Self>,
        params: ActorCallParams,
    ) -> Result<()> {
        let stream = self.try_get_stream(params.stream)?;
        stream.send_result_of_actor_call(cx, params).await?;
        Ok(())
    }
    async fn call_actor_method(
        &mut self,
        cx: &hyperactor::Context<Self>,
        params: ActorMethodParams,
    ) -> Result<()> {
        let stream = self.try_get_stream(params.call.stream)?;
        stream.call_actor_method(cx, params).await?;
        Ok(())
    }
    async fn split_comm(
        &mut self,
        cx: &hyperactor::Context<Self>,
        dims: Vec<String>,
        device_mesh: Ref,
        stream_ref: StreamRef,
    ) -> Result<()> {
        let global_comm = self
            .comm
            .as_ref()
            .context("tried to call SplitComm before BackendNetworkInit")?;
        match self.device_meshes.get_mut(&device_mesh) {
            Some((device_mesh, comm_map)) => {
                // This rank is in the group to be split off. Split a new
                // communicator for it off from the global communicator.
                let stream = self
                    .streams
                    .get(&stream_ref)
                    .ok_or_else(|| anyhow::anyhow!("invalid stream id: {:#?}", stream_ref))?;

                let dims = SortedVec::from_unsorted(dims);

                anyhow::ensure!(
                    !comm_map.contains_key(&(stream_ref, dims.clone())),
                    "comm already exists for stream {stream:#?}, dims {dims:#?}"
                );
                let ranks_for_group = device_mesh.get_ranks_for_dim_slice(&dims)?;
                let size = ranks_for_group.len();
                let split_comm = global_comm
                    .split_from(
                        cx,
                        ranks_for_group
                            .into_iter()
                            .map(|v| v.clone().try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                    )
                    .await?
                    .context("split comm should include self rank")?;
                comm_map.insert((stream_ref, dims), (size, Arc::new(split_comm)));
            }
            None => {
                // This rank is not in the group to be split off. We still need to
                // participate in the commSplit call, however.
                global_comm.split_from(cx, vec![]).await?;
            }
        }
        Ok(())
    }

    async fn split_comm_for_process_group(
        &mut self,
        cx: &hyperactor::Context<Self>,
        remote_process_group_ref: Ref,
        stream_ref: StreamRef,
    ) -> Result<()> {
        ensure!(
            self.streams.contains_key(&stream_ref),
            "invalid stream id: {:#?}",
            stream_ref
        );
        let global_comm = self
            .comm
            .as_ref()
            .context("tried to call SplitComm before BackendNetworkInit")?;
        let state = self
            .remote_process_groups
            .get_mut(&remote_process_group_ref)
            .with_context(|| format!("invalid remote process group id: {:#?}", stream_ref))?;
        match self.device_meshes.get_mut(&state.device_mesh_ref) {
            Some((device_mesh, _)) => {
                // This rank is in the group to be split off. Split a new
                // communicator for it off from the global communicator.
                let entry = match state.comms.entry(stream_ref) {
                    Entry::Vacant(entry) => entry,
                    Entry::Occupied(_) => bail!(
                        "comm already exists for remote process group {:#?} on stream {:#?}",
                        remote_process_group_ref,
                        stream_ref,
                    ),
                };
                let ranks_for_group = device_mesh.get_ranks_for_dim_slice(&state.dims)?;
                let split_comm = global_comm
                    .split_from(
                        cx,
                        ranks_for_group
                            .into_iter()
                            .map(|v| v.clone().try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                    )
                    .await?
                    .context("split comm should include self rank")?;
                entry.insert(Arc::new(split_comm));
            }
            None => {
                // This rank is not in the group to be split off. We still need to
                // participate in the commSplit call, however.
                global_comm.split_from(cx, vec![]).await?;
            }
        }
        Ok(())
    }

    async fn pipe_recv(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _seq: Seq,
        _results: Vec<Option<Ref>>,
        _pipe: Ref,
        _stream: StreamRef,
    ) -> Result<()> {
        panic!("pipe_recv is no longer implemented")
    }

    async fn set_ref_unit_tests_only(
        &mut self,
        cx: &hyperactor::Context<Self>,
        reference: Ref,
        value: WireValue,
        stream: StreamRef,
    ) -> Result<()> {
        let stream = self.try_get_stream(stream)?;

        stream.set_ref_unit_tests_only(cx, reference, value).await
    }

    async fn get_ref_unit_tests_only(
        &mut self,
        cx: &hyperactor::Context<Self>,
        ref_id: Ref,
        stream: StreamRef,
    ) -> Result<Option<Result<WireValue, String>>> {
        let stream = self.try_get_stream(stream)?;
        Ok(stream.get_ref_unit_tests_only(cx, ref_id.clone()).await?)
    }

    async fn define_recording(
        &mut self,
        cx: &hyperactor::Context<Self>,
        result: Ref,
        _nresults: usize,
        _nformals: usize,
        commands: Vec<WorkerMessage>,
        ntotal_messages: usize,
        index: usize,
    ) -> Result<()> {
        if self.defining_recording.is_some() && self.defining_recording.unwrap() != result {
            bail!("already defining a different recording");
        }
        self.defining_recording = Some(result);

        match self.recordings.entry(result) {
            Entry::Vacant(entry) => {
                ensure!(
                    index == 0,
                    "got DefineRecording message with (index = {:?}) > 0 for previously unseen recording",
                    index
                );
                entry.insert(Recording::PartialRecording {
                    last_index: 0,
                    commands,
                });
            }
            Entry::Occupied(mut entry) => match entry.get_mut() {
                Recording::CompleteRecording { .. } => {
                    bail!("got DefineRecording message for already complete recording")
                }
                Recording::PartialRecording {
                    last_index,
                    commands: existing_commands,
                } => {
                    ensure!(
                        index == *last_index + 1,
                        "Got DefineRecording message with index = {:?}, but \
                            last seen index for recording is {:?}",
                        index,
                        last_index
                    );
                    *last_index = index;
                    existing_commands.extend(commands);
                }
            },
        };

        if index < ntotal_messages - 1 {
            return Ok(());
        }
        let commands = match self.recordings.remove(&result).unwrap() {
            Recording::CompleteRecording { .. } => panic!("unreachable, in theory"),
            Recording::PartialRecording { commands, .. } => {
                self.recordings.insert(
                    result,
                    Recording::CompleteRecording {
                        streams: HashSet::new(),
                    },
                );
                commands
            }
        };

        for command in commands {
            WorkerMessageHandler::handle(self, cx, command).await?;
        }

        match self.recordings.get(&result).unwrap() {
            Recording::PartialRecording { .. } => panic!("unreachable, in theory"),
            Recording::CompleteRecording { streams, .. } => {
                for stream in streams {
                    self.try_get_stream(*stream)?
                        .finalize_recording(cx, result)
                        .await?;
                }
            }
        }

        self.defining_recording = None;
        Ok(())
    }

    async fn recording_formal(
        &mut self,
        cx: &hyperactor::Context<Self>,
        result: Ref,
        argument_index: usize,
        stream: StreamRef,
    ) -> Result<()> {
        ensure!(self.defining_recording.is_some());
        self.maybe_add_stream_to_recording(cx, stream).await?;
        self.try_get_stream(stream)?
            .recording_formal(cx, result, argument_index)
            .await
    }

    async fn recording_result(
        &mut self,
        cx: &hyperactor::Context<Self>,
        result: Ref,
        output_index: usize,
        stream: StreamRef,
    ) -> Result<()> {
        ensure!(self.defining_recording.is_some());
        self.maybe_add_stream_to_recording(cx, stream).await?;
        self.try_get_stream(stream)?
            .recording_result(cx, result, output_index)
            .await
    }

    async fn call_recording(
        &mut self,
        cx: &hyperactor::Context<Self>,
        seq: Seq,
        recording: Ref,
        results: Vec<Ref>,
        actuals: Vec<Ref>,
    ) -> Result<()> {
        ensure!(self.defining_recording.is_none());
        let recording_ref = recording;
        let recording = self.recordings.get(&recording).ok_or(anyhow::anyhow!(
            "could not find recording: {:#?}",
            recording
        ))?;
        match recording {
            Recording::PartialRecording { .. } => {
                bail!("cannot call recording because it is incomplete")
            }
            Recording::CompleteRecording { streams } => try_join_all(
                streams
                    .iter()
                    .map(|stream| self.try_get_stream(*stream))
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .map(|stream| {
                        stream.call_recording(
                            cx,
                            seq,
                            recording_ref,
                            results.clone(),
                            actuals.clone(),
                        )
                    }),
            )
            .await
            .map(|_| ()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use anyhow::Result;
    use hyperactor::RemoteSpawn;
    use hyperactor::channel::ChannelAddr;
    use hyperactor::proc::Proc;
    use monarch_messages::controller::ControllerMessage;
    use monarch_messages::controller::WorkerError;
    use monarch_messages::worker::WorkerMessageClient;
    use monarch_types::PickledPyObject;
    use pyo3::Python;
    use pyo3::prelude::*;
    use pyo3::types::PyList;
    use pyo3::types::PyString;
    use rand::Rng;
    use rand::distr::Alphanumeric;
    use timed_test::async_timed_test;

    use super::*;
    use crate::test_util::test_setup;

    #[async_timed_test(timeout_secs = 60)]
    async fn basic_worker() -> Result<()> {
        test_setup()?;

        let proc = Proc::local();
        let (client, controller_ref, mut controller_rx) = proc.attach_actor("controller").unwrap();

        let worker_handle = proc
            .spawn(
                "worker",
                WorkerActor::new(
                    WorkerParams {
                        world_size: 1,
                        rank: 0,
                        device_index: None,
                        controller_actor: controller_ref,
                    },
                    Flattrs::default(),
                )
                .await
                .unwrap(),
            )
            .unwrap();
        worker_handle
            .command_group(
                &client,
                vec![
                    WorkerMessage::CreateStream {
                        id: 1.into(),
                        stream_creation: StreamCreationMode::UseDefaultStream,
                    },
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 0.into(),
                        results: vec![Some(0.into())],
                        mutates: vec![],
                        function: "torch.ops.aten.ones.default".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::IntList(vec![2, 3])],
                            HashMap::new(),
                        )
                        .unwrap(),
                        stream: 1.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 2.into(),
                        results: vec![Some(Ref { id: 2 })],
                        mutates: vec![0.into()],
                        function: "torch.ops.aten.sub_.Scalar".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::Ref(0.into()), WireValue::Int(1)],
                            HashMap::new(),
                        )
                        .unwrap(),
                        stream: 1.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 3.into(),
                        results: vec![Some(Ref { id: 3 })],
                        mutates: vec![],
                        function: "torch.ops.aten.zeros.default".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::IntList(vec![2, 3])],
                            HashMap::new(),
                        )
                        .unwrap(),
                        stream: 1.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 4.into(),
                        results: vec![Some(Ref { id: 4 })],
                        mutates: vec![],
                        function: "torch.ops.aten.allclose.default".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::Ref(0.into()), WireValue::Ref(Ref { id: 3 })],
                            HashMap::new(),
                        )
                        .unwrap(),
                        stream: 1.into(),
                        remote_process_groups: vec![],
                    }),
                ],
            )
            .await
            .unwrap();

        let result: bool = worker_handle
            .get_ref_unit_tests_only(&client, Ref { id: 4 }, 1.into())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .try_into()
            .unwrap();
        worker_handle.drain_and_stop("test").unwrap();
        worker_handle.await;
        let error_responses = controller_rx.drain();
        assert!(
            error_responses.is_empty(),
            "Expected no error responses, got: {:#?}",
            error_responses
        );
        assert!(result);

        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn error_sends_response() -> Result<()> {
        test_setup()?;

        let proc = Proc::local();
        let (client, controller_ref, mut controller_rx) = proc.attach_actor("controller").unwrap();

        let worker_handle = proc
            .spawn(
                "worker",
                WorkerActor::new(
                    WorkerParams {
                        world_size: 1,
                        rank: 0,
                        device_index: None,
                        controller_actor: controller_ref,
                    },
                    Flattrs::default(),
                )
                .await
                .unwrap(),
            )
            .unwrap();
        worker_handle
            .command_group(
                &client,
                vec![
                    WorkerMessage::CreateStream {
                        id: 1.into(),
                        stream_creation: StreamCreationMode::UseDefaultStream,
                    },
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 0.into(),
                        results: vec![Some(0.into())],
                        mutates: vec![],
                        function: "torch.ops.aten.rand.default".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(vec![], HashMap::new()).unwrap(),
                        stream: 1.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::Exit { error: None },
                ],
            )
            .await
            .unwrap();

        worker_handle.drain_and_stop("test").unwrap();
        worker_handle.await;
        let response_message = controller_rx.recv().await.unwrap();
        match response_message {
            ControllerMessage::RemoteFunctionFailed {
                seq,
                error: WorkerError { backtrace: msg, .. },
            } => {
                assert_eq!(seq, 0.into());
                assert!(msg.contains("aten::rand() is missing value for argument 'size'"))
            }
            _ => panic!("unexpected response {:#?}", response_message),
        }

        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn mutated_refs_are_updated_with_error() -> Result<()> {
        test_setup()?;

        let proc = Proc::local();
        let (client, controller_ref, mut controller_rx) = proc.attach_actor("controller").unwrap();

        let worker_handle = proc
            .spawn(
                "worker",
                WorkerActor::new(
                    WorkerParams {
                        world_size: 1,
                        rank: 0,
                        device_index: None,
                        controller_actor: controller_ref,
                    },
                    Flattrs::default(),
                )
                .await
                .unwrap(),
            )
            .unwrap();
        worker_handle
            .command_group(
                &client,
                vec![
                    WorkerMessage::CreateStream {
                        id: 1.into(),
                        stream_creation: StreamCreationMode::UseDefaultStream,
                    },
                    WorkerMessage::SetRefUnitTestsOnly {
                        reference: 0.into(),
                        value: WireValue::Int(1),
                        stream: 1.into(),
                    },
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 0.into(),
                        results: vec![Some(Ref { id: 2 })],
                        mutates: vec![0.into()],
                        function: "i.dont.exist".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(vec![], HashMap::new()).unwrap(),
                        stream: 1.into(),
                        remote_process_groups: vec![],
                    }),
                ],
            )
            .await
            .unwrap();

        let result = worker_handle
            .get_ref_unit_tests_only(&client, 0.into(), 1.into())
            .await?;

        // Stop/drain worker before asserts to avoid hangs.
        worker_handle.drain_and_stop("test").unwrap();
        worker_handle.await;

        let mutated_ref = result
            .context("no such ref")?
            .err()
            .context("expected error")?;
        assert!(mutated_ref.contains("failed to resolve function"));

        let responses = controller_rx.drain();
        assert_eq!(
            responses.len(),
            1,
            "Expected one response, got: {:#?}",
            responses
        );
        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn accessing_errored_dependency() -> Result<()> {
        test_setup()?;

        let proc = Proc::local();
        let (client, controller_ref, mut controller_rx) = proc.attach_actor("controller").unwrap();

        let worker_handle = proc
            .spawn(
                "worker",
                WorkerActor::new(
                    WorkerParams {
                        world_size: 1,
                        rank: 0,
                        device_index: None,
                        controller_actor: controller_ref,
                    },
                    Flattrs::default(),
                )
                .await
                .unwrap(),
            )
            .unwrap();
        worker_handle
            .command_group(
                &client,
                vec![
                    WorkerMessage::CreateStream {
                        id: 1.into(),
                        stream_creation: StreamCreationMode::UseDefaultStream,
                    },
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 0.into(),
                        results: vec![Some(0.into())],
                        mutates: vec![],
                        function: "i.dont.exist".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(vec![], HashMap::new()).unwrap(),
                        stream: 1.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 1.into(),
                        results: vec![Some(1.into())],
                        mutates: vec![],
                        function: "torch.ops.aten.sub_.Scalar".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::Ref(0.into())],
                            HashMap::new(),
                        )
                        .unwrap(),
                        stream: 1.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::Exit { error: None },
                ],
            )
            .await
            .unwrap();

        worker_handle.drain_and_stop("test").unwrap();
        worker_handle.await;

        let responses = controller_rx.drain();
        assert_eq!(
            responses.len(),
            1,
            "Expected one response, got: {:#?}",
            responses
        );

        match &responses[0] {
            ControllerMessage::RemoteFunctionFailed { seq, .. } => {
                assert_eq!(seq, &0.into())
            }
            _ => panic!("unexpected response {:#?}", responses[0]),
        };
        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn py_remote_function_calls() -> Result<()> {
        test_setup()?;

        let proc = Proc::local();
        let (client, controller_ref, mut controller_rx) = proc.attach_actor("controller").unwrap();

        let worker_handle = proc
            .spawn(
                "worker",
                WorkerActor::new(
                    WorkerParams {
                        world_size: 1,
                        rank: 0,
                        device_index: None,
                        controller_actor: controller_ref,
                    },
                    Flattrs::default(),
                )
                .await
                .unwrap(),
            )
            .unwrap();
        let (split_arg, sort_list, dim, layout, none, scalar, device, memory_format) =
            Python::attach(|py| {
                let split_arg: PickledPyObject = PyString::new(py, "/fbs/fbc/foo/bar")
                    .into_any()
                    .try_into()?;
                let sort_list: PickledPyObject =
                    PyList::new(py, [65, 34, 79, 1, 5])?.into_any().try_into()?;
                let dim: PickledPyObject = PyString::new(py, "x").into_any().try_into()?;
                let layout: PickledPyObject = py.import("torch")?.getattr("strided")?.try_into()?;
                let none: PickledPyObject = py.None().into_any().into_bound(py).try_into()?;
                let scalar: PickledPyObject = py.import("torch")?.getattr("float32")?.try_into()?;
                let device: PickledPyObject = py
                    .import("torch")?
                    .getattr("device")?
                    .call1(("cuda:1",))?
                    .try_into()?;
                let memory_format: PickledPyObject = py
                    .import("torch")?
                    .getattr("contiguous_format")?
                    .try_into()?;
                PyResult::Ok((
                    split_arg,
                    sort_list,
                    dim,
                    layout,
                    none,
                    scalar,
                    device,
                    memory_format,
                ))
            })?;

        worker_handle
            .command_group(
                &client,
                vec![
                    WorkerMessage::CreateStream {
                        id: 1.into(),
                        stream_creation: StreamCreationMode::UseDefaultStream,
                    },
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 0.into(),
                        results: vec![Some(0.into()), Some(Ref { id: 2 })],
                        mutates: vec![],
                        function: "os.path.split".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![split_arg.into()],
                            HashMap::new(),
                        )
                        .unwrap(),
                        stream: 1.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 2.into(),
                        results: vec![Some(4.into()), None, None, None, None],
                        mutates: vec![],
                        function: "builtins.sorted".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![sort_list.into()],
                            HashMap::new(),
                        )
                        .unwrap(),
                        stream: 1.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::CreateDeviceMesh {
                        result: 5.into(),
                        names: vec!["x".into()],
                        ranks: Slice::new(0, vec![2], vec![1]).unwrap(),
                    },
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 2.into(),
                        results: vec![Some(6.into())],
                        mutates: vec![],
                        function: "monarch.monarch_tensor_worker.test_utils.mesh_rank".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::Ref(Ref { id: 5 }), dim.into()],
                            HashMap::new(),
                        )
                        .unwrap(),
                        stream: 1.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 4.into(),
                        results: vec![Some(7.into())],
                        mutates: vec![],
                        function: "monarch.monarch_tensor_worker.test_utils.test_scalar_type"
                            .into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![scalar.into()],
                            HashMap::new(),
                        )
                        .unwrap(),
                        stream: 1.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 5.into(),
                        results: vec![Some(8.into())],
                        mutates: vec![],
                        function: "monarch.monarch_tensor_worker.test_utils.test_layout".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![layout.into()],
                            HashMap::new(),
                        )
                        .unwrap(),
                        stream: 1.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 6.into(),
                        results: vec![Some(9.into())],
                        mutates: vec![],
                        function: "monarch.monarch_tensor_worker.test_utils.test_none".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![none.into()],
                            HashMap::new(),
                        )
                        .unwrap(),
                        stream: 1.into(),
                        remote_process_groups: vec![],
                    }),
                    // Verify that a function that returns `None` matches up with an
                    // empty result list.
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 7.into(),
                        results: vec![None],
                        mutates: vec![],
                        function: "monarch.monarch_tensor_worker.test_utils.none".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(vec![], HashMap::new()).unwrap(),
                        stream: 1.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 8.into(),
                        results: vec![Some(10.into())],
                        mutates: vec![],
                        function: "monarch.monarch_tensor_worker.test_utils.test_device".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![device.into()],
                            HashMap::new(),
                        )
                        .unwrap(),
                        stream: 1.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 9.into(),
                        results: vec![Some(11.into())],
                        mutates: vec![],
                        function: "monarch.monarch_tensor_worker.test_utils.test_memory_format"
                            .into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![memory_format.into()],
                            HashMap::new(),
                        )
                        .unwrap(),
                        stream: 1.into(),
                        remote_process_groups: vec![],
                    }),
                    // Test that list of tests can be passes correctly
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 10.into(),
                        results: vec![Some(12.into())],
                        mutates: vec![],
                        function: "torch.ops.aten.ones.default".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::IntList(vec![2, 3])],
                            HashMap::new(),
                        )
                        .unwrap(),
                        stream: 1.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 11.into(),
                        results: vec![Some(13.into())],
                        mutates: vec![],
                        function: "torch.ops.aten.stack.default".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::RefList(vec![12.into(), 12.into()])],
                            HashMap::new(),
                        )
                        .unwrap(),
                        stream: 1.into(),
                        remote_process_groups: vec![],
                    }),
                ],
            )
            .await
            .unwrap();

        let result1: String = worker_handle
            .get_ref_unit_tests_only(&client, 0.into(), 1.into())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .try_into()
            .unwrap();
        let result2: String = worker_handle
            .get_ref_unit_tests_only(&client, 2.into(), 1.into())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .try_into()
            .unwrap();
        let result3: i64 = worker_handle
            .get_ref_unit_tests_only(&client, 4.into(), 1.into())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .try_into()
            .unwrap();
        let result4: i64 = worker_handle
            .get_ref_unit_tests_only(&client, 6.into(), 1.into())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .try_into()
            .unwrap();
        worker_handle
            .get_ref_unit_tests_only(&client, 7.into(), 1.into())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        worker_handle
            .get_ref_unit_tests_only(&client, 8.into(), 1.into())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_matches!(
            worker_handle
                .get_ref_unit_tests_only(&client, 9.into(), 1.into())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            WireValue::None(()),
        );
        worker_handle
            .get_ref_unit_tests_only(&client, 10.into(), 1.into())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        worker_handle
            .get_ref_unit_tests_only(&client, 11.into(), 1.into())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        worker_handle.drain_and_stop("test").unwrap();
        worker_handle.await;
        let error_responses = controller_rx.drain();
        assert!(
            error_responses.is_empty(),
            "Expected no error responses, got: {:#?}",
            error_responses
        );

        assert_eq!(result1, "/fbs/fbc/foo");
        assert_eq!(result2, "bar");
        assert_eq!(result3, 1);
        assert_eq!(result4, 0);

        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn delete_refs() -> Result<()> {
        test_setup()?;

        let proc = Proc::local();
        let (client, controller_ref, _) = proc.attach_actor("controller").unwrap();

        let worker_handle = proc
            .spawn(
                "worker",
                WorkerActor::new(
                    WorkerParams {
                        world_size: 1,
                        rank: 0,
                        device_index: None,
                        controller_actor: controller_ref,
                    },
                    Flattrs::default(),
                )
                .await
                .unwrap(),
            )
            .unwrap();
        worker_handle
            .command_group(
                &client,
                vec![
                    WorkerMessage::CreateStream {
                        id: 0.into(),
                        stream_creation: StreamCreationMode::CreateNewStream,
                    },
                    WorkerMessage::CreateStream {
                        id: 1.into(),
                        stream_creation: StreamCreationMode::CreateNewStream,
                    },
                    WorkerMessage::SetRefUnitTestsOnly {
                        reference: Ref { id: 2 },
                        value: WireValue::Bool(false),
                        stream: 0.into(),
                    },
                    WorkerMessage::SetRefUnitTestsOnly {
                        reference: Ref { id: 3 },
                        value: WireValue::Bool(true),
                        stream: 0.into(),
                    },
                    WorkerMessage::SetRefUnitTestsOnly {
                        reference: Ref { id: 4 },
                        value: WireValue::Int(0),
                        stream: 1.into(),
                    },
                    WorkerMessage::DeleteRefs(vec![Ref { id: 2 }, Ref { id: 4 }]),
                ],
            )
            .await
            .unwrap();

        let result: bool = worker_handle
            .get_ref_unit_tests_only(&client, Ref { id: 3 }, 0.into())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .try_into()
            .unwrap();
        let fail_result = worker_handle
            .get_ref_unit_tests_only(&client, Ref { id: 4 }, 1.into())
            .await
            .unwrap();

        worker_handle.drain_and_stop("test").unwrap();
        worker_handle.await;

        assert!(result, "should be able to get a non-deleted ref");
        assert!(fail_result.is_none(), "should fail to get a deleted ref");

        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn request_status() -> Result<()> {
        test_setup()?;

        let proc = Proc::local();
        let (client, controller_ref, mut controller_rx) = proc.attach_actor("controller").unwrap();

        let worker_handle = proc
            .spawn(
                "worker",
                WorkerActor::new(
                    WorkerParams {
                        world_size: 1,
                        rank: 0,
                        device_index: None,
                        controller_actor: controller_ref,
                    },
                    Flattrs::default(),
                )
                .await
                .unwrap(),
            )
            .unwrap();
        worker_handle
            .command_group(
                &client,
                vec![
                    WorkerMessage::CreateStream {
                        id: 0.into(),
                        stream_creation: StreamCreationMode::CreateNewStream,
                    },
                    WorkerMessage::CreateStream {
                        id: 1.into(),
                        stream_creation: StreamCreationMode::CreateNewStream,
                    },
                ],
            )
            .await
            .unwrap();

        for i in 0..100 {
            // call alternating functions on this stream.
            worker_handle
                .call_function(
                    &client,
                    CallFunctionParams {
                        seq: i.into(),
                        results: vec![Some(Ref { id: i + 2 })],
                        mutates: vec![],
                        function: "torch.ops.aten.ones.default".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::IntList(vec![2, 3])],
                            HashMap::new(),
                        )
                        .unwrap(),
                        stream: (i % 2).into(),
                        remote_process_groups: vec![],
                    },
                )
                .await
                .unwrap();
        }

        worker_handle
            .request_status(&client, 100.into(), false)
            .await
            .unwrap();

        worker_handle.drain_and_stop("test").unwrap();
        worker_handle.await;

        let mut responses = controller_rx.drain();
        assert_eq!(
            responses.len(),
            1,
            "Expected one response, got: {:#?}",
            responses
        );

        let response = responses.pop().unwrap();
        match response {
            ControllerMessage::Status { seq, .. } => {
                assert_eq!(seq, 101.into())
            }
            _ => panic!("unexpected response {:#?}", response),
        };

        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn backend_network_init() {
        test_setup().unwrap();
        let proc = Proc::local();
        let (client, controller_ref, _) = proc.attach_actor("controller").unwrap();

        let worker_handle1 = proc
            .spawn(
                "worker0",
                WorkerActor::new(
                    WorkerParams {
                        world_size: 2,
                        rank: 0,
                        device_index: Some(0),
                        controller_actor: controller_ref.clone(),
                    },
                    Flattrs::default(),
                )
                .await
                .unwrap(),
            )
            .unwrap();
        let worker_handle2 = proc
            .spawn(
                "worker1",
                WorkerActor::new(
                    WorkerParams {
                        world_size: 2,
                        rank: 1,
                        device_index: Some(1),
                        controller_actor: controller_ref,
                    },
                    Flattrs::default(),
                )
                .await
                .unwrap(),
            )
            .unwrap();

        let unique_id = UniqueId::new().unwrap();
        worker_handle1
            .backend_network_init(&client, unique_id.clone())
            .await
            .unwrap();
        worker_handle2
            .backend_network_init(&client, unique_id)
            .await
            .unwrap();

        worker_handle1.drain_and_stop("test").unwrap();
        worker_handle1.await;
        worker_handle2.drain_and_stop("test").unwrap();
        worker_handle2.await;
    }

    #[allow(dead_code)]
    fn get_random_channel_addr() -> ChannelAddr {
        let random_string = rand::rng()
            .sample_iter(&Alphanumeric)
            .take(24)
            .map(char::from)
            .collect::<String>();
        format!("unix!@{random_string}").parse().unwrap()
    }
}
