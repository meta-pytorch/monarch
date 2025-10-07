/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::ensure;
use async_trait::async_trait;
use cxx::CxxVector;
use derivative::Derivative;
use hyperactor::Actor;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::Named;
use hyperactor::actor::ActorHandle;
use hyperactor::forward;
use hyperactor::mailbox::OncePortHandle;
use hyperactor::mailbox::OncePortReceiver;
use parking_lot::Mutex;
use tokio::sync::RwLock;
use tokio::task::spawn_blocking;
use torch_sys::CloneUnsafe;
use torch_sys::CudaDevice;
use torch_sys::ScalarType;
use torch_sys::Tensor;
use torch_sys::TensorCell;
use torch_sys::backend::AllToAllOptions;
use torch_sys::backend::AllreduceOptions;
use torch_sys::backend::Backend;
use torch_sys::backend::BarrierOptions;
use torch_sys::backend::BroadcastOptions;
use torch_sys::backend::GatherOptions;
use torch_sys::backend::ReduceOptions;
use torch_sys::backend::ReduceScatterOptions;
use torch_sys::backend::ScatterOptions;
use torch_sys::backend::Work;
use torch_sys_cuda::cuda::Event;
use torch_sys_cuda::cuda::Stream;
use torch_sys_cuda::nccl::Communicator;
use torch_sys_cuda::nccl::NcclConfig;
use torch_sys_cuda::nccl::NcclError;
use torch_sys_cuda::nccl::NcclStatus;
use torch_sys_cuda::nccl::ReduceOp;
use torch_sys_cuda::nccl::UniqueId;
use torch_sys_cuda::nccl::group_end;
use torch_sys_cuda::nccl::group_start;

/// Messages for NcclCommActor. See the underlying [`Communicator`] APIs for what
/// these do.
#[allow(dead_code)]
#[derive(Handler, HandleClient, Debug, Named)]
#[named(register = false)]
pub enum CommMessage {
    AllReduce(TensorCell, ReduceOp, Stream, #[reply] OncePortHandle<Event>),

    AllToAllSingle(
        TensorCell,
        TensorCell,
        Stream,
        #[reply] OncePortHandle<Event>,
    ),

    Broadcast(TensorCell, i32, Stream, #[reply] OncePortHandle<Event>),

    Reduce(
        TensorCell,
        ReduceOp,
        i32,
        Stream,
        #[reply] OncePortHandle<Event>,
    ),

    Barrier(Stream, #[reply] OncePortHandle<Event>),

    AllGather(
        Vec<TensorCell>,
        TensorCell,
        Stream,
        #[reply] OncePortHandle<Event>,
    ),

    AllGatherIntoTensor(
        TensorCell,
        TensorCell,
        Stream,
        #[reply] OncePortHandle<Event>,
    ),

    ReduceScatterTensor(
        TensorCell,
        TensorCell,
        ReduceOp,
        Stream,
        #[reply] OncePortHandle<Event>,
    ),

    Send(TensorCell, i32, Stream, #[reply] OncePortHandle<Event>),

    Recv(TensorCell, i32, Stream, #[reply] OncePortHandle<Event>),

    Group(Vec<CommMessage>, Stream, #[reply] OncePortHandle<Event>),

    SplitAll(
        Option<NcclConfig>,
        #[reply] OncePortHandle<ActorHandle<NcclCommActor>>,
    ),

    SplitFrom(
        Vec<i32>,
        Option<NcclConfig>,
        #[reply] OncePortHandle<Option<ActorHandle<NcclCommActor>>>,
    ),
}

/// Represents a single NCCL communicator.
///
/// This actor's role is to encapsulate the synchronous raw NCCL API with an
/// async interface. This doesn't really need to be an actor per se, since it
/// doesn't represent an independent thread of execution. But I think it's nice
/// anyway:
// 1. NCCL is a hotspot in our current infra, so making a solid boundary will
//    help with debuggability (for example, we will be able to track all
//    messages to the NcclCommActor and use it to debug deadlocks much more easily
//    than we do today).
// 2. NCCL has some...weirdness when it comes to thread safety. I've been assured
//    by several people that it's okay to call a NCCL communicator from multiple
//    threads as long as you guarantee the same order for all ranks, but it will
//    be good to have the option to pin all NCCL stuff to a single thread if
//    necessary.
#[derive(Debug)]
pub struct NcclCommActor {
    // Sadly an `Arc<Mutex>` because we use `spawn_blocking` to call NCCL APIs,
    // which moves the communicator to another thread. If this becomes a
    // practical issue, we can move to managing the synchronous thread directly
    // and spawn the communicator there, eliminating the need for shared
    // ownership and synchronization.
    comm: Arc<Mutex<Communicator>>,
}

impl NcclCommActor {
    async fn collective<F>(&self, op_name: String, stream: Stream, op: F) -> Result<Event>
    where
        F: FnOnce(Arc<Mutex<Communicator>>) -> Result<NcclStatus, NcclError> + Send + 'static,
    {
        let comm = self.comm.clone();
        spawn_blocking(move || {
            let status = op(comm)?;
            match status {
                NcclStatus::Success => Ok(stream.record_event(None)),
                _ => bail!("nccl {op_name} failed: {status:?}"),
            }
        })
        .await?
    }
}

/// Initialization parameters for `NcclCommActor`.
#[derive(Debug, Clone)]
pub enum CommParams {
    /// Create the [`Communicator`] as a new world.
    New {
        /// Device that this `Communicator` will use for compute. All streams
        /// passed to `NcclCommActor` are expected to be on this device.
        device: CudaDevice,
        /// NCCL UniqueID to coordinate group construction.
        unique_id: UniqueId,
        /// Global world size.
        world_size: i32,
        /// This communicator's rank in the world.
        rank: i32,
    },

    /// Initialize using the provided communicator. This form is used when
    /// splitting off from an existing communicator. Do NOT use this to create
    /// two NcclCommActors that manage the same communicator, probably bad things
    /// will happen.
    FromComm(Arc<Mutex<Communicator>>),
}

#[async_trait]
impl Actor for NcclCommActor {
    type Params = CommParams;

    async fn new(params: Self::Params) -> Result<Self> {
        match params {
            CommParams::New {
                device,
                unique_id,
                world_size,
                rank,
            } => {
                let comm =
                    spawn_blocking(move || Communicator::new(device, world_size, unique_id, rank))
                        .await
                        .unwrap()?;
                Ok(Self {
                    comm: Arc::new(Mutex::new(comm)),
                })
            }
            CommParams::FromComm(comm) => Ok(Self { comm }),
        }
    }
}

#[async_trait]
#[forward(CommMessage)]
impl CommMessageHandler for NcclCommActor {
    async fn all_reduce(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        tensor: TensorCell,
        op: ReduceOp,
        stream: Stream,
    ) -> Result<Event> {
        self.collective("all_reduce".into(), stream.clone(), move |comm| {
            comm.lock().all_reduce(&tensor, op, &stream)
        })
        .await
    }

    async fn all_to_all_single(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        output: TensorCell,
        input: TensorCell,
        stream: Stream,
    ) -> Result<Event> {
        self.collective("all_to_all_single".into(), stream.clone(), move |comm| {
            comm.lock().all_to_all_single(&output, &input, &stream)
        })
        .await
    }

    async fn split_all(
        &mut self,
        cx: &hyperactor::Context<Self>,
        nccl_config: Option<NcclConfig>,
    ) -> Result<ActorHandle<NcclCommActor>> {
        let comm = self.comm.clone();

        let split_comm = spawn_blocking(move || comm.lock().split_all(nccl_config))
            .await
            .unwrap()?;

        NcclCommActor::spawn(cx, CommParams::FromComm(Arc::new(Mutex::new(split_comm)))).await
    }

    async fn split_from(
        &mut self,
        cx: &hyperactor::Context<Self>,
        ranks: Vec<i32>,
        nccl_config: Option<NcclConfig>,
    ) -> Result<Option<ActorHandle<NcclCommActor>>> {
        let comm = self.comm.clone();

        let split_comm = spawn_blocking(move || comm.lock().split_from(ranks, nccl_config))
            .await
            .unwrap()?;

        match split_comm {
            Some(split_comm) => Ok(Some(
                NcclCommActor::spawn(cx, CommParams::FromComm(Arc::new(Mutex::new(split_comm))))
                    .await?,
            )),
            None => Ok(None),
        }
    }

    async fn broadcast(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        tensor: TensorCell,
        root: i32,
        stream: Stream,
    ) -> Result<Event> {
        self.collective("broadcast".into(), stream.clone(), move |comm| {
            comm.lock().broadcast(&tensor, root, &stream)
        })
        .await
    }

    async fn barrier(&mut self, _cx: &hyperactor::Context<Self>, stream: Stream) -> Result<Event> {
        self.collective("barrier".into(), stream.clone(), move |comm| {
            comm.lock().barrier(&stream)
        })
        .await
    }

    async fn reduce(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        tensor: TensorCell,
        op: ReduceOp,
        root: i32,
        stream: Stream,
    ) -> Result<Event> {
        self.collective("reduce".into(), stream.clone(), move |comm| {
            comm.lock().reduce(&tensor, op, root, &stream)
        })
        .await
    }

    async fn all_gather(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        output: Vec<TensorCell>,
        input: TensorCell,
        stream: Stream,
    ) -> Result<Event> {
        self.collective("all_gather".into(), stream.clone(), move |comm| {
            comm.lock().all_gather(&output, &input, &stream)
        })
        .await
    }

    async fn all_gather_into_tensor(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        output: TensorCell,
        input: TensorCell,
        stream: Stream,
    ) -> Result<Event> {
        self.collective(
            "all_gather_into_tensor".into(),
            stream.clone(),
            move |comm| comm.lock().all_gather_into_tensor(&output, &input, &stream),
        )
        .await
    }

    async fn reduce_scatter_tensor(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        output: TensorCell,
        input: TensorCell,
        op: ReduceOp,
        stream: Stream,
    ) -> Result<Event> {
        self.collective(
            "reduce_scatter_tensor".into(),
            stream.clone(),
            move |comm| {
                comm.lock()
                    .reduce_scatter_tensor(&output, &input, op, &stream)
            },
        )
        .await
    }

    async fn send(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        tensor: TensorCell,
        dst: i32,
        stream: Stream,
    ) -> Result<Event> {
        self.collective("send".into(), stream.clone(), move |comm| {
            comm.lock().send(&tensor, dst, &stream)
        })
        .await
        .context("from CommMessageHandler::send")
    }

    async fn recv(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        tensor: TensorCell,
        src: i32,
        stream: Stream,
    ) -> Result<Event> {
        self.collective("recv".into(), stream.clone(), move |comm| {
            comm.lock().recv(&tensor, src, &stream)
        })
        .await
        .context("from CommMessageHandler::recv")
    }

    async fn group(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        messages: Vec<CommMessage>,
        stream: Stream,
    ) -> Result<Event> {
        let comm = self.comm.clone();
        // All group operations MUST be performed on a single thread.
        Ok(spawn_blocking(move || {
            let mut comm = comm.lock();
            // Start nccl group call
            let ticket = group_start()?;
            for message in messages {
                match message {
                    CommMessage::Send(tensor, dst, m_stream, _) => {
                        ensure!(
                            stream == m_stream,
                            "All messages in group must be on same stream"
                        );
                        comm.send(&tensor, dst, &m_stream)
                    }
                    CommMessage::Recv(tensor, src, m_stream, _) => {
                        ensure!(
                            stream == m_stream,
                            "All messages in group must be on same stream"
                        );
                        comm.recv(&tensor, src, &m_stream)
                    }
                    _ => bail!("unsupported message type in group: {message:?}"),
                }?;
            }
            group_end(ticket)?;
            // Make an end event on this stream.
            Ok(stream.record_event(None))
        })
        .await
        .unwrap()?)
    }
}

pub struct CommWork {
    // Keep a reference to the "input" streams used in the collective ops, to
    // prevent the allocator from free'ing them w/ using `.record_stream()`
    // (as per https://github.com/pytorch/pytorch/pull/76861).
    #[allow(dead_code)]
    inputs: Vec<TensorCell>,
    event: RwLock<Event>,
}

impl CommWork {
    async fn from(inputs: Vec<TensorCell>, rx: OncePortReceiver<Event>) -> Result<Self> {
        let event = rx.recv().await.map_err(|mailbox_err| {
            anyhow::anyhow!("Error receiving CUDA event: {mailbox_err:?}")
        })?;
        Ok(Self {
            inputs,
            event: RwLock::new(event),
        })
    }
}

#[async_trait]
impl Work for CommWork {
    type Error = anyhow::Error;
    async fn wait(&self) -> Result<()> {
        // As nccl/cuda operations are async, to implement `.wait()`
        // we need to synchronize on an event created after the op. We
        // shouldn't need to wrap this in `spawn_blocking` because this
        // it shouldn't cause a host<->device sync; instead, it will cause
        // a stream<->stream sync, with any future work submitted to the current
        // active stream waiting for the stream on which the event was recorded.
        self.event.write().await.wait(None);
        Ok(())
    }

    async fn is_completed(&self) -> Result<bool> {
        Ok(self.event.read().await.query())
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct CommBackend {
    #[derivative(Debug = "ignore")]
    instance: Instance<()>, // The actor that represents this object.
    comm: Arc<ActorHandle<NcclCommActor>>,
    rank: usize,
    // Size of group. This is less than or equal to world_size.
    group_size: usize,
    // Global world size.
    #[allow(dead_code)]
    world_size: usize,
}

impl CommBackend {
    pub fn new(
        instance: Instance<()>,
        comm: Arc<ActorHandle<NcclCommActor>>,
        rank: usize,
        group_size: usize,
        world_size: usize,
    ) -> Self {
        assert!(
            group_size <= world_size,
            "Group must be smaller or equal to the world size"
        );
        Self {
            instance,
            comm,
            rank,
            group_size,
            world_size,
        }
    }

    fn check_root_rank(&self, rank: i32) -> Result<usize> {
        if rank < 0 || rank >= self.group_size as i32 {
            Err(anyhow!("invalid root rank: {}", rank))
        } else {
            Ok(rank as usize)
        }
    }
}

fn convert_reduce_op(op: torch_sys::backend::ReduceOp) -> Result<ReduceOp> {
    Ok(match op {
        torch_sys::backend::ReduceOp::Sum => ReduceOp::Sum,
        torch_sys::backend::ReduceOp::Avg => ReduceOp::Avg,
        torch_sys::backend::ReduceOp::Max => ReduceOp::Max,
        torch_sys::backend::ReduceOp::Min => ReduceOp::Min,
        _ => bail!("unsupported op: {:?}", op),
    })
}

fn as_singleton<'a>(tensors: &'a [Tensor]) -> Result<&'a Tensor> {
    match tensors {
        [tensor] => Ok(tensor),
        _ => bail!("expected single tensor"),
    }
}

fn assert_type_match(tensor: &Tensor, dtype: ScalarType) -> Result<()> {
    if tensor.scalar_type() != dtype {
        Err(anyhow!(
            "Tensor dtype {:?} doesn't match expected {:?}",
            tensor.scalar_type(),
            dtype
        ))
    } else {
        Ok(())
    }
}

fn assert_size_match(tensor: &Tensor, shape: &[i32]) -> Result<()> {
    if tensor.sizes() != shape {
        Err(anyhow!(
            "Tensor shape {:?} doesn't match expected {:?}",
            tensor.sizes(),
            shape
        ))
    } else {
        Ok(())
    }
}

fn assert_type_and_sizes_match(tensors: &[Tensor], dtype: ScalarType, sizes: &[i32]) -> Result<()> {
    for t in tensors {
        assert_type_match(t, dtype)?;
        assert_size_match(t, sizes)?;
    }
    Ok(())
}

// TODO: move this to comm.rs?
#[async_trait]
impl Backend for CommBackend {
    type Error = anyhow::Error;

    async fn allreduce(
        &self,
        tensors: &CxxVector<Tensor>,
        opts: AllreduceOptions,
    ) -> Result<Box<dyn Work<Error = anyhow::Error>>> {
        // SAFETY: We need to wrap in a `TensorCell` for the `NcclCommActor` API.
        // It should be safe, as the original `CallFunction` that led us here
        // has performed the necessary borrows.
        let cell = TensorCell::new(unsafe { as_singleton(tensors.as_slice())?.clone_unsafe() });

        // Call into `NcclCommActor`.
        let (tx, rx) = self.instance.open_once_port();
        self.comm.send(
            &self.instance,
            CommMessage::AllReduce(
                cell.clone(),
                convert_reduce_op(opts.reduce_op)?,
                Stream::get_current_stream(),
                tx,
            ),
        )?;
        Ok(Box::new(CommWork::from(vec![cell], rx).await?))
    }

    async fn allgather(
        &self,
        output: &CxxVector<Tensor>,
        input: &Tensor,
    ) -> Result<Box<dyn Work<Error = anyhow::Error>>> {
        let output_cell = output
            .iter()
            // SAFETY: We need to wrap in a `TensorCell` for the `NcclCommActor` API.
            // It should be safe, as the original `CallFunction` that led us here
            // has performed the necessary borrows.
            .map(|t| TensorCell::new(unsafe { t.clone_unsafe() }))
            .collect::<Vec<TensorCell>>();
        // SAFETY: As above.
        let input_cell = TensorCell::new(unsafe { input.clone_unsafe() });
        if output_cell.len() != self.group_size {
            return Err(anyhow!(
                "Expected output list of tensors to be one per rank, got {}, expected {}",
                output_cell.len(),
                self.group_size
            ));
        }

        // Call into `NcclCommActor`.
        let (tx, rx) = self.instance.open_once_port();
        // This is not implemented in this function because the broadcasts we need
        // to create will change their behavior based on rank.
        self.comm.send(
            &self.instance,
            CommMessage::AllGather(
                output_cell.clone(),
                input_cell.clone(),
                Stream::get_current_stream(),
                tx,
            ),
        )?;
        let mut input_cells = vec![];
        input_cells.extend(output_cell);
        input_cells.push(input_cell);
        Ok(Box::new(CommWork::from(input_cells, rx).await?))
    }

    async fn _allgather_base(
        &self,
        output: &Tensor,
        input: &Tensor,
    ) -> Result<Box<dyn Work<Error = anyhow::Error>>> {
        // SAFETY: We need to wrap in a `TensorCell` for the `NcclCommActor` API.
        // It should be safe, as the original `CallFunction` that led us here
        // has performed the necessary borrows.
        let output_cell = TensorCell::new(unsafe { output.clone_unsafe() });
        // SAFETY: As above.
        let input_cell = TensorCell::new(unsafe { input.clone_unsafe() });

        // Call into `NcclCommActor`.
        let (tx, rx) = self.instance.open_once_port();
        self.comm.send(
            &self.instance,
            CommMessage::AllGatherIntoTensor(
                output_cell.clone(),
                input_cell.clone(),
                Stream::get_current_stream(),
                tx,
            ),
        )?;
        Ok(Box::new(
            CommWork::from(vec![output_cell, input_cell], rx).await?,
        ))
    }

    async fn barrier(&self, _opts: BarrierOptions) -> Result<Box<dyn Work<Error = anyhow::Error>>> {
        // Call into `NcclCommActor`.
        let (tx, rx) = self.instance.open_once_port();
        self.comm
            // There's no native barrier op in nccl, so impl via all-reduce.
            .send(
                &self.instance,
                CommMessage::Barrier(Stream::get_current_stream(), tx),
            )?;
        Ok(Box::new(CommWork::from(vec![], rx).await?))
    }

    async fn reduce(
        &self,
        input: &Tensor,
        opts: ReduceOptions,
    ) -> Result<Box<dyn Work<Error = anyhow::Error>>> {
        // SAFETY: We need to wrap in a `TensorCell` for the `NcclCommActor` API.
        // It should be safe, as the original `CallFunction` that led us here
        // has performed the necessary borrows.
        let input_cell = TensorCell::new(unsafe { input.clone_unsafe() });

        // Call into `NcclCommActor`.
        let (tx, rx) = self.instance.open_once_port();
        self.comm.send(
            &self.instance,
            CommMessage::Reduce(
                input_cell.clone(),
                convert_reduce_op(opts.reduce_op)?,
                opts.root_rank,
                Stream::get_current_stream(),
                tx,
            ),
        )?;
        Ok(Box::new(CommWork::from(vec![input_cell], rx).await?))
    }

    async fn _reduce_scatter_base(
        &self,
        output: &Tensor,
        input: &Tensor,
        opts: ReduceScatterOptions,
    ) -> Result<Box<dyn Work<Error = anyhow::Error>>> {
        // SAFETY: We need to wrap in a `TensorCell` for the `NcclCommActor` API.
        // It should be safe, as the original `CallFunction` that led us here
        // has performed the necessary borrows.
        let output_cell = TensorCell::new(unsafe { output.clone_unsafe() });
        // SAFETY: As above.
        let input_cell = TensorCell::new(unsafe { input.clone_unsafe() });

        if input_cell.borrow().numel() != output_cell.borrow().numel() * self.group_size as i64 {
            return Err(anyhow!(
                "input tensor must be the same size as output size times group size, input={}, output={}, group_size={}",
                input_cell.borrow().numel(),
                output_cell.borrow().numel(),
                self.group_size,
            ));
        }

        // Call into `NcclCommActor`.
        let (tx, rx) = self.instance.open_once_port();
        self.comm.send(
            &self.instance,
            CommMessage::ReduceScatterTensor(
                output_cell.clone(),
                input_cell.clone(),
                convert_reduce_op(opts.reduce_op)?,
                Stream::get_current_stream(),
                tx,
            ),
        )?;
        Ok(Box::new(
            CommWork::from(vec![output_cell, input_cell], rx).await?,
        ))
    }

    async fn send(
        &self,
        tensors: &CxxVector<Tensor>,
        dst_rank: i32,
        _tag: i32,
    ) -> Result<Box<dyn Work<Error = anyhow::Error>>> {
        ensure!(
            _tag == 0,
            "tag is not yet supported for send in CommBackend"
        );
        // SAFETY: We need to wrap in a `TensorCell` for the `NcclCommActor` API.
        // It should be safe, as the original `CallFunction` that led us here
        // has performed the necessary borrows.
        let cell = TensorCell::new(unsafe { as_singleton(tensors.as_slice())?.clone_unsafe() });

        // Call into `NcclCommActor`.
        let (tx, rx) = self.instance.open_once_port();
        self.comm.send(
            &self.instance,
            CommMessage::Send(cell.clone(), dst_rank, Stream::get_current_stream(), tx),
        )?;
        Ok(Box::new(CommWork::from(vec![cell], rx).await?))
    }

    async fn recv(
        &self,
        tensors: &CxxVector<Tensor>,
        src_rank: i32,
        _tag: i32,
    ) -> Result<Box<dyn Work<Error = anyhow::Error>>> {
        ensure!(
            _tag == 0,
            "tag is not yet supported for recv in CommBackend"
        );
        // SAFETY: We need to wrap in a `TensorCell` for the `NcclCommActor` API.
        // It should be safe, as the original `CallFunction` that led us here
        // has performed the necessary borrows.
        let cell = TensorCell::new(unsafe { as_singleton(tensors.as_slice())?.clone_unsafe() });

        // Call into `NcclCommActor`.
        let (tx, rx) = self.instance.open_once_port();
        self.comm.send(
            &self.instance,
            CommMessage::Recv(cell.clone(), src_rank, Stream::get_current_stream(), tx),
        )?;
        Ok(Box::new(CommWork::from(vec![cell], rx).await?))
    }

    async fn gather(
        &self,
        outputs: &CxxVector<Tensor>,
        input: &Tensor,
        opts: GatherOptions,
    ) -> Result<Box<dyn Work<Error = anyhow::Error>>> {
        let output_cells = outputs
            .iter()
            // SAFETY: We need to wrap in a `TensorCell` for the `NcclCommActor` API.
            // It should be safe, as the original `CallFunction` that led us here
            // has performed the necessary borrows.
            .map(|t| unsafe { TensorCell::new(t.clone_unsafe()) })
            .collect::<Vec<_>>();
        // SAFETY: Same as above.
        let input_cell = TensorCell::new(unsafe { input.clone_unsafe() });
        // Check arguments for correctness.
        let root = self.check_root_rank(opts.root_rank)?;
        assert_type_and_sizes_match(outputs.as_slice(), input.scalar_type(), &input.sizes())?;

        // Call into `NcclCommActor`.
        let (tx, rx) = self.instance.open_once_port();
        let mut messages = vec![];
        // All ranks other than the root Recv, and the root rank calls Send.
        if self.rank == root {
            if output_cells.len() != self.group_size {
                return Err(anyhow!(
                    "Incorrect output list size {}. Output list should be {}, same as size of the process group",
                    output_cells.len(),
                    self.group_size
                ));
            }
            for (r, output) in output_cells.clone().into_iter().enumerate() {
                if r != root {
                    let (tx_recv, _rx_recv) = self.instance.open_once_port();
                    messages.push(CommMessage::Recv(
                        output,
                        r as i32,
                        Stream::get_current_stream(),
                        tx_recv,
                    ));
                } else {
                    // on its own rank, simply copy from the input
                    output.borrow_mut().copy_(&input_cell.borrow());
                }
            }
        } else {
            if !output_cells.is_empty() {
                return Err(anyhow!(
                    "Output list should be empty for non-root ranks, got {} outputs",
                    output_cells.len()
                ));
            }
            let (tx_send, _rx_send) = self.instance.open_once_port();
            messages.push(CommMessage::Send(
                input_cell.clone(),
                root as i32,
                Stream::get_current_stream(),
                tx_send,
            ));
        }
        self.comm.send(
            &self.instance,
            CommMessage::Group(messages, Stream::get_current_stream(), tx),
        )?;
        let mut inputs = vec![];
        inputs.extend(output_cells);
        inputs.push(input_cell);
        Ok(Box::new(CommWork::from(inputs, rx).await?))
    }

    async fn scatter(
        &self,
        output: &Tensor,
        inputs: &CxxVector<Tensor>,
        opts: ScatterOptions,
    ) -> Result<Box<dyn Work<Error = anyhow::Error>>> {
        // SAFETY: We need to wrap in a `TensorCell` for the `NcclCommActor` API.
        // It should be safe, as the original `CallFunction` that led us here
        // has performed the necessary borrows.
        let output_cell = TensorCell::new(unsafe { output.clone_unsafe() });
        let input_cells = inputs
            .iter()
            // SAFETY: Same as above.
            .map(|t| unsafe { TensorCell::new(t.clone_unsafe()) })
            .collect::<Vec<_>>();

        let root = self.check_root_rank(opts.root_rank)?;
        assert_type_and_sizes_match(inputs.as_slice(), output.scalar_type(), &output.sizes())?;

        // Call into `NcclCommActor`.
        let (tx, rx) = self.instance.open_once_port();
        let mut messages = vec![];
        // Implementation is the inverse set of messages from gather, where all ranks
        // other than the root Send, and the root rank calls Recv.
        if self.rank == root {
            if input_cells.len() != self.group_size {
                return Err(anyhow!(
                    "Incorrect input list size {}. Input list should be {}, same as size of the process group",
                    input_cells.len(),
                    self.group_size
                ));
            }
            for (r, input) in input_cells.clone().into_iter().enumerate() {
                if r != root {
                    let (tx_send, _rx_send) = self.instance.open_once_port();
                    messages.push(CommMessage::Send(
                        input,
                        r as i32,
                        Stream::get_current_stream(),
                        tx_send,
                    ));
                } else {
                    // on its own rank, simply copy from the input
                    input.borrow_mut().copy_(&output_cell.borrow());
                }
            }
        } else {
            if !input_cells.is_empty() {
                return Err(anyhow!(
                    "Input list should be empty for non-root ranks, got {} inputs",
                    input_cells.len()
                ));
            }
            let (tx_recv, _rx_recv) = self.instance.open_once_port();
            messages.push(CommMessage::Recv(
                output_cell.clone(),
                root as i32,
                Stream::get_current_stream(),
                tx_recv,
            ));
        }
        self.comm.send(
            &self.instance,
            CommMessage::Group(messages, Stream::get_current_stream(), tx),
        )?;
        let mut inputs = vec![];
        inputs.push(output_cell);
        inputs.extend(input_cells);
        Ok(Box::new(CommWork::from(inputs, rx).await?))
    }

    async fn broadcast(
        &self,
        tensors: &CxxVector<Tensor>,
        opts: BroadcastOptions,
    ) -> Result<Box<dyn Work<Error = anyhow::Error>>> {
        // SAFETY: We need to wrap in a `TensorCell` for the `NcclCommActor` API.
        // It should be safe, as the original `CallFunction` that led us here
        // has performed the necessary borrows.
        let cell = TensorCell::new(unsafe { as_singleton(tensors.as_slice())?.clone_unsafe() });

        // Call into `NcclCommActor`.
        let (tx, rx) = self.instance.open_once_port();
        self.comm.send(
            &self.instance,
            CommMessage::Broadcast(
                cell.clone(),
                opts.root_rank,
                Stream::get_current_stream(),
                tx,
            ),
        )?;
        Ok(Box::new(CommWork::from(vec![cell], rx).await?))
    }

    async fn alltoall_base(
        &self,
        output_buffer: &Tensor,
        input_buffer: &Tensor,
        _opts: AllToAllOptions,
    ) -> Result<Box<dyn Work<Error = anyhow::Error>>> {
        // SAFETY: We need to wrap in a `TensorCell` for the `NcclCommActor` API.
        // It should be safe, as the original `CallFunction` that led us here
        // has performed the necessary borrows.
        let output_cell = TensorCell::new(unsafe { output_buffer.clone_unsafe() });
        // SAFETY: As above.
        let input_cell = TensorCell::new(unsafe { input_buffer.clone_unsafe() });

        // Call into `NcclCommActor`.
        let (tx, rx) = self.instance.open_once_port();
        self.comm.send(
            &self.instance,
            CommMessage::AllToAllSingle(
                output_cell.clone(),
                input_cell.clone(),
                Stream::get_current_stream(),
                tx,
            ),
        )?;
        Ok(Box::new(
            CommWork::from(vec![output_cell, input_cell], rx).await?,
        ))
    }

    async fn alltoall(
        &self,
        output_tensors: &CxxVector<Tensor>,
        input_tensors: &CxxVector<Tensor>,
        _opts: AllToAllOptions,
    ) -> Result<Box<dyn Work<Error = anyhow::Error>>> {
        let output_cells = output_tensors
            .as_slice()
            .iter()
            // SAFETY: We need to wrap in a `TensorCell` for the `NcclCommActor` API.
            // It should be safe, as the original `CallFunction` that led us here
            // has performed the necessary borrows.
            .map(|t| TensorCell::new(unsafe { t.clone_unsafe() }))
            .collect::<Vec<TensorCell>>();
        let input_cells = input_tensors
            .as_slice()
            .iter()
            // SAFETY: As above.
            .map(|t| TensorCell::new(unsafe { t.clone_unsafe() }))
            .collect::<Vec<TensorCell>>();

        if input_cells.len() != self.group_size {
            return Err(anyhow!(
                "Expected input list of tensors to be one per rank, got {}, expected {}",
                input_cells.len(),
                self.group_size
            ));
        }

        if output_cells.len() != self.group_size {
            return Err(anyhow!(
                "Expected output list of tensors to be one per rank, got {}, expected {}",
                output_cells.len(),
                self.group_size
            ));
        }

        // Call into `NcclCommActor`.
        let mut messages: Vec<CommMessage> = vec![];
        let stream = Stream::get_current_stream();
        for r in 0..output_tensors.len() {
            let output_cell = &output_cells[r];
            let input_cell = &input_cells[r];
            let (tx_send, _rx_send) = self.instance.open_once_port();
            let (tx_recv, _rx_recv) = self.instance.open_once_port();
            messages.push(CommMessage::Send(
                input_cell.clone(),
                r as i32,
                stream.clone(),
                tx_send,
            ));
            messages.push(CommMessage::Recv(
                output_cell.clone(),
                r as i32,
                stream.clone(),
                tx_recv,
            ));
        }
        let (tx, rx) = self.instance.open_once_port();
        self.comm
            .send(&self.instance, CommMessage::Group(messages, stream, tx))?;
        let mut all_cells = vec![];
        all_cells.extend(output_cells);
        all_cells.extend(input_cells);
        Ok(Box::new(CommWork::from(all_cells, rx).await?))
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashMap;

    use anyhow::Result;
    use futures::future::try_join_all;
    use hyperactor::actor::ActorStatus;
    use hyperactor::proc::Proc;
    use monarch_messages::worker::WorkerMessageClient;
    use monarch_messages::worker::WorkerParams;
    use ndslice::Slice;
    use timed_test::async_timed_test;
    use torch_sys::DeviceIndex;
    use torch_sys::Layout;
    use torch_sys::ScalarType;
    use torch_sys::factory_float_tensor;
    use torch_sys::testing::allclose;

    use super::*;
    use crate::CallFunctionParams;
    use crate::Factory;
    use crate::Reduction;
    use crate::StreamCreationMode;
    use crate::WireValue;
    use crate::WorkerActor;
    use crate::WorkerMessage;
    use crate::test_util::test_setup;

    #[async_timed_test(timeout_secs = 60)]
    async fn all_reduce() {
        let proc = Proc::local();
        let (client, _handle) = proc.instance("client").unwrap();

        let unique_id = UniqueId::new().unwrap();
        let device0 = CudaDevice::new(DeviceIndex(0));
        let handle0 = proc.spawn::<NcclCommActor>(
            "comm0",
            CommParams::New {
                device: device0,
                unique_id: unique_id.clone(),
                world_size: 2,
                rank: 0,
            },
        );

        let device1 = CudaDevice::new(DeviceIndex(1));
        let handle1 = proc.spawn::<NcclCommActor>(
            "comm1",
            CommParams::New {
                device: device1,
                unique_id,
                world_size: 2,
                rank: 1,
            },
        );
        let (handle0, handle1) = tokio::join!(handle0, handle1);
        let (handle0, handle1) = (handle0.unwrap(), handle1.unwrap());

        let cell0 = TensorCell::new(factory_float_tensor(&[1.0], device0.into()));

        let fut0 = handle0.all_reduce(
            &client,
            cell0.clone(),
            ReduceOp::Sum,
            Stream::get_current_stream_on_device(device0),
        );

        let cell1 = TensorCell::new(factory_float_tensor(&[2.0], device1.into()));

        let fut1 = handle1.all_reduce(
            &client,
            cell1.clone(),
            ReduceOp::Sum,
            Stream::get_current_stream_on_device(device1),
        );

        let (res0, res1) = tokio::join!(fut0, fut1);
        res0.unwrap();
        res1.unwrap();

        assert!(
            allclose(
                &cell0.borrow(),
                &factory_float_tensor(&[3.0], device0.into())
            )
            .unwrap()
        );
        assert!(
            allclose(
                &cell1.borrow(),
                &factory_float_tensor(&[3.0], device1.into())
            )
            .unwrap()
        );
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn group_send_recv() {
        test_setup().unwrap();
        let proc = Proc::local();
        let (client, _handle) = proc.instance("client").unwrap();

        let unique_id = UniqueId::new().unwrap();
        let device0 = CudaDevice::new(DeviceIndex(0));
        let handle0 = NcclCommActor::spawn_detached(CommParams::New {
            device: device0,
            unique_id: unique_id.clone(),
            world_size: 2,
            rank: 0,
        });

        let device1 = CudaDevice::new(DeviceIndex(1));
        let handle1 = NcclCommActor::spawn_detached(CommParams::New {
            device: device1,
            unique_id,
            world_size: 2,
            rank: 1,
        });
        let (handle0, handle1) = tokio::join!(handle0, handle1);
        let (handle0, handle1) = (handle0.unwrap(), handle1.unwrap());

        let cell0 = TensorCell::new(factory_float_tensor(&[1.0], device0.into()));

        let fut0 = handle0.group(
            &client,
            vec![CommMessage::Send(
                cell0.clone(),
                1,
                Stream::get_current_stream_on_device(device0),
                client.open_once_port().0,
            )],
            Stream::get_current_stream_on_device(device0),
        );

        let cell1 = TensorCell::new(factory_float_tensor(&[2.0], device1.into()));

        let fut1 = handle1.group(
            &client,
            vec![CommMessage::Recv(
                cell1.clone(),
                0,
                Stream::get_current_stream_on_device(device1),
                client.open_once_port().0,
            )],
            Stream::get_current_stream_on_device(device1),
        );

        let (res0, res1) = tokio::join!(fut0, fut1);
        res0.unwrap();
        res1.unwrap();

        assert!(
            allclose(
                &cell0.borrow(),
                &factory_float_tensor(&[1.0], device0.into())
            )
            .unwrap()
        );
        assert!(
            allclose(
                &cell1.borrow(),
                &factory_float_tensor(&[1.0], device1.into())
            )
            .unwrap()
        );
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn reduce() -> Result<()> {
        test_setup()?;
        let proc = Proc::local();
        let (client, _handle) = proc.instance("client")?;

        let unique_id = UniqueId::new()?;
        let device0 = CudaDevice::new(DeviceIndex(0));
        let handle0 = proc.spawn::<NcclCommActor>(
            "comm0",
            CommParams::New {
                device: device0,
                unique_id: unique_id.clone(),
                world_size: 2,
                rank: 0,
            },
        );

        let device1 = CudaDevice::new(DeviceIndex(1));
        let handle1 = proc.spawn::<NcclCommActor>(
            "comm1",
            CommParams::New {
                device: device1,
                unique_id,
                world_size: 2,
                rank: 1,
            },
        );
        let (handle0, handle1) = tokio::join!(handle0, handle1);
        let (handle0, handle1) = (handle0.unwrap(), handle1.unwrap());

        let cell0 = TensorCell::new(factory_float_tensor(&[1.0], device0.into()));
        let dest_rank = 0;

        let fut0 = handle0.reduce(
            &client,
            cell0.clone(),
            ReduceOp::Sum,
            dest_rank,
            Stream::get_current_stream_on_device(device0),
        );

        let cell1 = TensorCell::new(factory_float_tensor(&[2.0], device1.into()));

        let fut1 = handle1.reduce(
            &client,
            cell1.clone(),
            ReduceOp::Sum,
            dest_rank,
            Stream::get_current_stream_on_device(device1),
        );

        let (res0, res1) = tokio::join!(fut0, fut1);
        res0?;
        res1?;

        // Only dest_rank=0 should have the reduced value.
        assert!(allclose(
            &cell0.borrow(),
            &factory_float_tensor(&[3.0], device0.into())
        )?);
        // Non-dest ranks should have the original value.
        assert!(allclose(
            &cell1.borrow(),
            &factory_float_tensor(&[2.0], device1.into())
        )?);
        Ok(())
    }

    #[async_timed_test(timeout_secs = 600)]
    async fn worker_reduce() -> Result<()> {
        test_setup()?;

        let proc = Proc::local();
        let (client, controller_ref, mut controller_rx) = proc.attach_actor("controller").unwrap();

        let world_size = 4;
        let workers = try_join_all((0..world_size).map(async |rank| {
            proc.spawn::<WorkerActor>(
                &format!("worker{}", rank),
                WorkerParams {
                    world_size,
                    rank,
                    device_index: Some(rank.try_into()?),
                    controller_actor: controller_ref.clone(),
                },
            )
            .await
        }))
        .await?;

        let unique_id = UniqueId::new().unwrap();
        let messages = vec![
            WorkerMessage::BackendNetworkInit(unique_id.clone()),
            WorkerMessage::CreateStream {
                id: 0.into(),
                stream_creation: StreamCreationMode::UseDefaultStream,
            },
            WorkerMessage::CreateDeviceMesh {
                result: 1.into(),
                names: vec!["x".into(), "y".into()],
                ranks: Slice::new(0, vec![2, 2], vec![2, 1])?,
            },
            WorkerMessage::SplitComm {
                dims: vec!["x".into()],
                device_mesh: 1.into(),
                stream: 0.into(),
                config: None,
            },
            WorkerMessage::CallFunction(CallFunctionParams {
                seq: 0.into(),
                results: vec![Some(2.into())],
                mutates: vec![],
                function: "torch.ops.aten.ones.default".into(),
                args: vec![WireValue::IntList(vec![2, 3])],
                kwargs: HashMap::from([(
                    "device".into(),
                    WireValue::Device("cuda".try_into().unwrap()),
                )]),
                stream: 0.into(),
                remote_process_groups: vec![],
            }),
            // Test reduce over "x".
            WorkerMessage::Reduce {
                result: 3.into(),
                tensor: 2.into(),
                factory: Factory {
                    size: vec![2, 3],
                    dtype: ScalarType::Float,
                    layout: Layout::Strided,
                    device: "cuda".try_into().unwrap(),
                },
                mesh: 1.into(),
                stream: 0.into(),
                dims: vec!["x".to_string()],
                reduction: Reduction::ReduceOp(ReduceOp::Sum),
                scatter: false,
                in_place: false,
                out: None,
            },
            WorkerMessage::CallFunction(CallFunctionParams {
                seq: 1.into(),
                results: vec![Some(4.into())],
                mutates: vec![],
                function: "torch.ops.aten.full.default".into(),
                args: vec![WireValue::IntList(vec![2, 3]), WireValue::Double(2.0)],
                kwargs: HashMap::from([(
                    "device".into(),
                    WireValue::Device("cuda".try_into().unwrap()),
                )]),
                stream: 0.into(),
                remote_process_groups: vec![],
            }),
            WorkerMessage::CallFunction(CallFunctionParams {
                seq: 1.into(),
                results: vec![Some(5.into())],
                mutates: vec![],
                function: "torch.ops.aten.allclose.default".into(),
                args: vec![WireValue::Ref(3.into()), WireValue::Ref(4.into())],
                kwargs: HashMap::new(),
                stream: 0.into(),
                remote_process_groups: vec![],
            }),
            WorkerMessage::SplitComm {
                dims: vec!["x".into(), "y".into()],
                device_mesh: 1.into(),
                stream: 0.into(),
                config: None,
            },
            // Test reduce over "x" and "y".
            WorkerMessage::Reduce {
                result: 6.into(),
                tensor: 2.into(),
                factory: Factory {
                    size: vec![2, 3],
                    dtype: ScalarType::Float,
                    layout: Layout::Strided,
                    device: "cuda".try_into()?,
                },
                mesh: 1.into(),
                stream: 0.into(),
                dims: vec!["x".into(), "y".into()],
                reduction: Reduction::ReduceOp(ReduceOp::Sum),
                scatter: false,
                in_place: false,
                out: None,
            },
            WorkerMessage::CallFunction(CallFunctionParams {
                seq: 1.into(),
                results: vec![Some(7.into())],
                mutates: vec![],
                function: "torch.ops.aten.full.default".into(),
                args: vec![WireValue::IntList(vec![2, 3]), WireValue::Double(4.0)],
                kwargs: HashMap::from([("device".into(), WireValue::Device("cuda".try_into()?))]),
                stream: 0.into(),
                remote_process_groups: vec![],
            }),
            WorkerMessage::CallFunction(CallFunctionParams {
                seq: 1.into(),
                results: vec![Some(8.into())],
                mutates: vec![],
                function: "torch.ops.aten.allclose.default".into(),
                args: vec![WireValue::Ref(6.into()), WireValue::Ref(7.into())],
                kwargs: HashMap::new(),
                stream: 0.into(),
                remote_process_groups: vec![],
            }),
        ];

        for worker in workers.iter() {
            worker.command_group(&client, messages.clone()).await?;
        }

        let val: bool = workers[0]
            .get_ref_unit_tests_only(&client, 5.into(), 0.into())
            .await?
            .unwrap()
            .unwrap()
            .try_into()?;
        assert!(val, "allreduce sum produced unexpected value: {val}");

        let val: bool = workers[0]
            .get_ref_unit_tests_only(&client, 8.into(), 0.into())
            .await?
            .unwrap()
            .unwrap()
            .try_into()?;
        assert!(val, "allreduce sum produced unexpected value: {val}");

        for worker in workers.into_iter() {
            worker.drain_and_stop().unwrap();
            worker.await;
        }

        let error_responses = controller_rx.drain();
        assert!(
            error_responses.is_empty(),
            "Expected no error responses, got: {:#?}",
            error_responses
        );

        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn send_tensor() -> Result<()> {
        test_setup()?;

        let proc = Proc::local();
        let (client, controller_ref, mut controller_rx) = proc.attach_actor("controller").unwrap();

        let handle1 = proc
            .spawn::<WorkerActor>(
                "worker1",
                WorkerParams {
                    world_size: 2,
                    rank: 0,
                    device_index: Some(0),
                    controller_actor: controller_ref.clone(),
                },
            )
            .await
            .unwrap();
        let handle2 = proc
            .spawn::<WorkerActor>(
                "worker2",
                WorkerParams {
                    world_size: 2,
                    rank: 1,
                    device_index: Some(1),
                    controller_actor: controller_ref,
                },
            )
            .await
            .unwrap();

        let unique_id = UniqueId::new().unwrap();

        handle1
            .command_group(
                &client,
                vec![
                    WorkerMessage::CreateStream {
                        id: 0.into(),
                        stream_creation: StreamCreationMode::UseDefaultStream,
                    },
                    WorkerMessage::BackendNetworkInit(unique_id.clone()),
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 1.into(),
                        results: vec![Some(1.into())],
                        mutates: vec![],
                        function: "torch.ops.aten.full.default".into(),
                        args: vec![WireValue::IntList(vec![2, 3]), WireValue::Double(2.0)],
                        kwargs: HashMap::from([(
                            "device".into(),
                            WireValue::Device("cuda".try_into().unwrap()),
                        )]),
                        stream: 0.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::BackendNetworkPointToPointInit {
                        from_stream: 0.into(),
                        to_stream: 0.into(),
                    },
                    WorkerMessage::SendTensor {
                        result: 2.into(),
                        from_ranks: Slice::new(0, vec![1], vec![1]).unwrap(),
                        to_ranks: Slice::new(1, vec![1], vec![1]).unwrap(),
                        tensor: 1.into(),
                        factory: Factory {
                            size: vec![2, 3],
                            dtype: ScalarType::Float,
                            layout: Layout::Strided,
                            device: "cuda".try_into().unwrap(),
                        },
                        from_stream: 0.into(),
                        to_stream: 0.into(),
                    },
                ],
            )
            .await
            .unwrap();

        handle2
            .command_group(
                &client,
                vec![
                    WorkerMessage::CreateStream {
                        id: 0.into(),
                        stream_creation: StreamCreationMode::UseDefaultStream,
                    },
                    WorkerMessage::BackendNetworkInit(unique_id.clone()),
                    WorkerMessage::BackendNetworkPointToPointInit {
                        from_stream: 0.into(),
                        to_stream: 0.into(),
                    },
                    WorkerMessage::SendTensor {
                        result: 1.into(),
                        from_ranks: Slice::new(0, vec![1], vec![1]).unwrap(),
                        to_ranks: Slice::new(1, vec![1], vec![1]).unwrap(),
                        tensor: 1.into(),
                        factory: Factory {
                            size: vec![2, 3],
                            dtype: ScalarType::Float,
                            layout: Layout::Strided,
                            device: "cuda".try_into().unwrap(),
                        },
                        from_stream: 0.into(),
                        to_stream: 0.into(),
                    },
                ],
            )
            .await
            .unwrap();

        handle2
            .command_group(
                &client,
                vec![
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 1.into(),
                        results: vec![Some(2.into())],
                        mutates: vec![],
                        function: "torch.ops.aten.full.default".into(),
                        args: vec![WireValue::IntList(vec![2, 3]), WireValue::Double(2.0)],
                        kwargs: HashMap::from([(
                            "device".into(),
                            WireValue::Device("cuda".try_into().unwrap()),
                        )]),
                        stream: 0.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 1.into(),
                        results: vec![Some(3.into())],
                        mutates: vec![],
                        function: "torch.ops.aten.allclose.default".into(),
                        args: vec![WireValue::Ref(1.into()), WireValue::Ref(2.into())],
                        kwargs: HashMap::new(),
                        stream: 0.into(),
                        remote_process_groups: vec![],
                    }),
                ],
            )
            .await
            .unwrap();

        let val: bool = handle2
            .get_ref_unit_tests_only(&client, 3.into(), 0.into())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .try_into()
            .unwrap();
        assert!(val, "send_tensor result was unexpected value: {val}");

        handle1.drain_and_stop().unwrap();
        assert_matches!(handle1.await, ActorStatus::Stopped);
        handle2.drain_and_stop().unwrap();
        assert_matches!(handle2.await, ActorStatus::Stopped);

        let error_responses = controller_rx.drain();
        assert!(
            error_responses.is_empty(),
            "Expected no error responses, got: {:#?}",
            error_responses
        );

        Ok(())
    }

    #[async_timed_test(timeout_secs = 60)]
    async fn send_tensor_local() -> Result<()> {
        test_setup()?;

        let proc = Proc::local();
        let (client, controller_ref, mut controller_rx) = proc.attach_actor("controller").unwrap();

        let handle = proc
            .spawn::<WorkerActor>(
                "worker",
                WorkerParams {
                    world_size: 1,
                    rank: 0,
                    device_index: Some(0),
                    controller_actor: controller_ref,
                },
            )
            .await
            .unwrap();

        let unique_id = UniqueId::new().unwrap();
        handle
            .command_group(
                &client,
                vec![
                    WorkerMessage::CreateStream {
                        id: 0.into(),
                        stream_creation: StreamCreationMode::UseDefaultStream,
                    },
                    WorkerMessage::BackendNetworkInit(unique_id.clone()),
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 1.into(),
                        results: vec![Some(1.into())],
                        mutates: vec![],
                        function: "torch.ops.aten.full.default".into(),
                        args: vec![WireValue::IntList(vec![2, 3]), WireValue::Double(2.0)],
                        kwargs: HashMap::from([(
                            "device".into(),
                            WireValue::Device("cuda".try_into().unwrap()),
                        )]),
                        stream: 0.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 2.into(),
                        results: vec![Some(2.into())],
                        mutates: vec![],
                        function: "torch.ops.aten.full.default".into(),
                        args: vec![WireValue::IntList(vec![2, 3]), WireValue::Double(4.0)],
                        kwargs: HashMap::from([(
                            "device".into(),
                            WireValue::Device("cuda".try_into().unwrap()),
                        )]),
                        stream: 0.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::BackendNetworkPointToPointInit {
                        from_stream: 0.into(),
                        to_stream: 0.into(),
                    },
                    // Send a tensor within this rank.
                    WorkerMessage::SendTensor {
                        result: 2.into(),
                        from_ranks: Slice::new(0, vec![1], vec![1]).unwrap(),
                        to_ranks: Slice::new(0, vec![1], vec![1]).unwrap(),
                        tensor: 1.into(),
                        factory: Factory {
                            size: vec![2, 3],
                            dtype: ScalarType::Float,
                            layout: Layout::Strided,
                            device: "cuda".try_into().unwrap(),
                        },
                        from_stream: 0.into(),
                        to_stream: 0.into(),
                    },
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 3.into(),
                        results: vec![Some(3.into())],
                        mutates: vec![],
                        function: "torch.ops.aten.full.default".into(),
                        args: vec![WireValue::IntList(vec![2, 3]), WireValue::Double(2.0)],
                        kwargs: HashMap::from([(
                            "device".into(),
                            WireValue::Device("cuda".try_into().unwrap()),
                        )]),
                        stream: 0.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 4.into(),
                        results: vec![Some(4.into())],
                        mutates: vec![],
                        function: "torch.ops.aten.allclose.default".into(),
                        args: vec![WireValue::Ref(2.into()), WireValue::Ref(3.into())],
                        kwargs: HashMap::new(),
                        stream: 0.into(),
                        remote_process_groups: vec![],
                    }),
                ],
            )
            .await
            .unwrap();

        let val: bool = handle
            .get_ref_unit_tests_only(&client, 4.into(), 0.into())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .try_into()
            .unwrap();
        assert!(val, "send_tensor_local result was unexpected value: {val}");

        handle.drain_and_stop().unwrap();
        assert_matches!(handle.await, ActorStatus::Stopped);

        let error_responses = controller_rx.drain();
        assert!(
            error_responses.is_empty(),
            "Expected no error responses, got: {:#?}",
            error_responses
        );

        Ok(())
    }

    #[async_timed_test(timeout_secs = 240)]
    async fn test_comm_work() -> Result<()> {
        test_setup()?;
        let proc = Proc::local();
        let (client, _handle) = proc.instance("client")?;

        let unique_id = UniqueId::new()?;
        let device0 = CudaDevice::new(DeviceIndex(0));
        let handle0 = NcclCommActor::spawn_detached(CommParams::New {
            device: device0,
            unique_id: unique_id.clone(),
            world_size: 2,
            rank: 0,
        });

        let device1 = CudaDevice::new(DeviceIndex(1));
        let handle1 = NcclCommActor::spawn_detached(CommParams::New {
            device: device1,
            unique_id,
            world_size: 2,
            rank: 1,
        });
        let (handle0, handle1) = tokio::join!(handle0, handle1);
        let (handle0, handle1) = (handle0?, handle1?);

        let cell0 = TensorCell::new(factory_float_tensor(&[1.0], device0.into()));
        let port0 = client.open_once_port();
        handle0.send(
            &client,
            CommMessage::Send(
                cell0.clone(),
                1,
                Stream::get_current_stream_on_device(device0),
                port0.0,
            ),
        )?;

        let cell1 = TensorCell::new(factory_float_tensor(&[1.0], device1.into()));
        let port1 = client.open_once_port();
        handle1.send(
            &client,
            CommMessage::Recv(
                cell1.clone(),
                0,
                Stream::get_current_stream_on_device(device1),
                port1.0,
            ),
        )?;
        let (work0, work1) = tokio::join!(
            CommWork::from(vec![cell0.clone()], port0.1),
            CommWork::from(vec![cell1.clone()], port1.1)
        );
        let (work0, work1) = (work0?, work1?);
        // Wait for the work to enqueue onto the stream.
        work0.wait().await?;
        work1.wait().await?;
        // Wait is non-blocking, which means that there's no guarantee that the
        // work is completed.
        // Make sure the work completes.
        while !work0.is_completed().await? {
            // No need to sleep or yield, because the await on each iteration
            // will give other tasks a chance to make progress.
            tracing::debug!("waiting for work0...");
        }
        while !work1.is_completed().await? {
            // Same as above.
            tracing::debug!("waiting for work1...");
        }

        // Check that the tensors are correct after the work completes.
        assert!(
            allclose(
                &cell0.borrow(),
                &factory_float_tensor(&[1.0], device0.into())
            )
            .unwrap()
        );
        assert!(
            allclose(
                &cell1.borrow(),
                &factory_float_tensor(&[1.0], device1.into())
            )
            .unwrap()
        );
        Ok(())
    }
}
