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
use anyhow::bail;
use anyhow::ensure;
use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Named;
use hyperactor::actor::ActorHandle;
use hyperactor::forward;
use hyperactor::mailbox::OncePortHandle;
use parking_lot::Mutex;
use tokio::task::spawn_blocking;
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
use torch_sys2::CudaDevice;
use torch_sys2::TensorCell;

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

impl Actor for NcclCommActor {}

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

impl NcclCommActor {
    pub async fn new(params: CommParams) -> Result<Self> {
        match params {
            CommParams::New {
                device,
                unique_id,
                world_size,
                rank,
            } => {
                // TODO: this should probalby be done in the actor's 'init'
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

        NcclCommActor::new(CommParams::FromComm(Arc::new(Mutex::new(split_comm))))
            .await?
            .spawn(cx)
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
                NcclCommActor::new(CommParams::FromComm(Arc::new(Mutex::new(split_comm))))
                    .await?
                    .spawn(cx)?,
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

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashMap;

    use anyhow::Result;
    use futures::future::try_join_all;
    use hyperactor::RemoteSpawn;
    use hyperactor::actor::ActorStatus;
    use hyperactor::proc::Proc;
    use monarch_messages::worker::ArgsKwargs;
    use monarch_messages::worker::WorkerMessageClient;
    use monarch_messages::worker::WorkerParams;
    use ndslice::Slice;
    
    use timed_test::async_timed_test;
    use torch_sys2::DeviceIndex;
    use torch_sys2::Layout;
    use torch_sys2::ScalarType;
    use torch_sys2::factory_float_tensor;
    use torch_sys2::testing::allclose;

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
        let actor0 = NcclCommActor::new(CommParams::New {
            device: device0,
            unique_id: unique_id.clone(),
            world_size: 2,
            rank: 0,
        });

        let device1 = CudaDevice::new(DeviceIndex(1));
        let actor1 = NcclCommActor::new(CommParams::New {
            device: device1,
            unique_id,
            world_size: 2,
            rank: 1,
        });

        let (actor0, actor1) = tokio::join!(actor0, actor1);
        let (actor0, actor1) = (actor0.unwrap(), actor1.unwrap());

        let handle0 = actor0.spawn_detached().unwrap();
        let handle1 = actor1.spawn_detached().unwrap();

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
        let actor0 = NcclCommActor::new(CommParams::New {
            device: device0,
            unique_id: unique_id.clone(),
            world_size: 2,
            rank: 0,
        });

        let device1 = CudaDevice::new(DeviceIndex(1));
        let actor1 = NcclCommActor::new(CommParams::New {
            device: device1,
            unique_id,
            world_size: 2,
            rank: 1,
        });

        let (actor0, actor1) = tokio::join!(actor0, actor1);
        let (actor0, actor1) = (actor0.unwrap(), actor1.unwrap());

        let handle0 = actor0.spawn_detached().unwrap();
        let handle1 = actor1.spawn_detached().unwrap();

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
        let actor0 = NcclCommActor::new(CommParams::New {
            device: device0,
            unique_id: unique_id.clone(),
            world_size: 2,
            rank: 0,
        });
        let device1 = CudaDevice::new(DeviceIndex(1));
        let actor1 = NcclCommActor::new(CommParams::New {
            device: device1,
            unique_id,
            world_size: 2,
            rank: 1,
        });
        let (actor0, actor1) = tokio::join!(actor0, actor1);
        let (actor0, actor1) = (actor0.unwrap(), actor1.unwrap());

        let handle0 = proc.spawn("comm0", actor0).unwrap();
        let handle1 = proc.spawn("comm1", actor1).unwrap();

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
        ).map_err(|e| anyhow::anyhow!(e))?);
        // Non-dest ranks should have the original value.
        assert!(allclose(
            &cell1.borrow(),
            &factory_float_tensor(&[2.0], device1.into())
        ).map_err(|e| anyhow::anyhow!(e))?);
        Ok(())
    }

    #[async_timed_test(timeout_secs = 600)]
    async fn worker_reduce() -> Result<()> {
        test_setup()?;

        let proc = Proc::local();
        let (client, controller_ref, mut controller_rx) = proc.attach_actor("controller").unwrap();

        let world_size = 4;
        let workers = try_join_all((0..world_size).map(async |rank| {
            proc.spawn(
                &format!("worker{}", rank),
                WorkerActor::new(WorkerParams {
                    world_size,
                    rank,
                    device_index: Some(rank.try_into()?),
                    controller_actor: controller_ref.clone(),
                })
                .await
                .unwrap(),
            )
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
                args_kwargs: ArgsKwargs::from_wire_values(
                    vec![WireValue::IntList(vec![2, 3])],
                    HashMap::from([(
                        "device".into(),
                        WireValue::Device("cuda".parse().unwrap()),
                    )]),
                )
                .unwrap(),
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
                    device: "cuda".parse().unwrap(),
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
                args_kwargs: ArgsKwargs::from_wire_values(
                    vec![WireValue::IntList(vec![2, 3]), WireValue::Double(2.0)],
                    HashMap::from([(
                        "device".into(),
                        WireValue::Device("cuda".parse().unwrap()),
                    )]),
                )
                .unwrap(),
                stream: 0.into(),
                remote_process_groups: vec![],
            }),
            WorkerMessage::CallFunction(CallFunctionParams {
                seq: 1.into(),
                results: vec![Some(5.into())],
                mutates: vec![],
                function: "torch.ops.aten.allclose.default".into(),
                args_kwargs: ArgsKwargs::from_wire_values(
                    vec![WireValue::Ref(3.into()), WireValue::Ref(4.into())],
                    HashMap::new(),
                )
                .unwrap(),
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
                    device: "cuda".parse()?,
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
                args_kwargs: ArgsKwargs::from_wire_values(
                    vec![WireValue::IntList(vec![2, 3]), WireValue::Double(4.0)],
                    HashMap::from([("device".into(), WireValue::Device("cuda".parse()?))]),
                )
                .unwrap(),
                stream: 0.into(),
                remote_process_groups: vec![],
            }),
            WorkerMessage::CallFunction(CallFunctionParams {
                seq: 1.into(),
                results: vec![Some(8.into())],
                mutates: vec![],
                function: "torch.ops.aten.allclose.default".into(),
                args_kwargs: ArgsKwargs::from_wire_values(
                    vec![WireValue::Ref(6.into()), WireValue::Ref(7.into())],
                    HashMap::new(),
                )
                .unwrap(),
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
            .try_into()
            .unwrap();
        assert!(val, "allreduce sum produced unexpected value: {val}");

        let val: bool = workers[0]
            .get_ref_unit_tests_only(&client, 8.into(), 0.into())
            .await?
            .unwrap()
            .unwrap()
            .try_into()
            .unwrap();
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
            .spawn(
                "worker1",
                WorkerActor::new(WorkerParams {
                    world_size: 2,
                    rank: 0,
                    device_index: Some(0),
                    controller_actor: controller_ref.clone(),
                })
                .await
                .unwrap(),
            )
            .unwrap();
        let handle2 = proc
            .spawn(
                "worker2",
                WorkerActor::new(WorkerParams {
                    world_size: 2,
                    rank: 1,
                    device_index: Some(1),
                    controller_actor: controller_ref,
                })
                .await
                .unwrap(),
            )
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
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::IntList(vec![2, 3]), WireValue::Double(2.0)],
                            HashMap::from([(
                                "device".into(),
                                WireValue::Device("cuda".parse().unwrap()),
                            )]),
                        )
                        .unwrap(),
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
                            device: "cuda".parse().unwrap(),
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
                            device: "cuda".parse().unwrap(),
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
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::IntList(vec![2, 3]), WireValue::Double(2.0)],
                            HashMap::from([(
                                "device".into(),
                                WireValue::Device("cuda".parse().unwrap()),
                            )]),
                        )
                        .unwrap(),
                        stream: 0.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 1.into(),
                        results: vec![Some(3.into())],
                        mutates: vec![],
                        function: "torch.ops.aten.allclose.default".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::Ref(1.into()), WireValue::Ref(2.into())],
                            HashMap::new(),
                        )
                        .unwrap(),
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
            .spawn(
                "worker",
                WorkerActor::new(WorkerParams {
                    world_size: 1,
                    rank: 0,
                    device_index: Some(0),
                    controller_actor: controller_ref,
                })
                .await
                .unwrap(),
            )
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
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::IntList(vec![2, 3]), WireValue::Double(2.0)],
                            HashMap::from([(
                                "device".into(),
                                WireValue::Device("cuda".parse().unwrap()),
                            )]),
                        )
                        .unwrap(),
                        stream: 0.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 2.into(),
                        results: vec![Some(2.into())],
                        mutates: vec![],
                        function: "torch.ops.aten.full.default".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::IntList(vec![2, 3]), WireValue::Double(4.0)],
                            HashMap::from([(
                                "device".into(),
                                WireValue::Device("cuda".parse().unwrap()),
                            )]),
                        )
                        .unwrap(),
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
                            device: "cuda".parse().unwrap(),
                        },
                        from_stream: 0.into(),
                        to_stream: 0.into(),
                    },
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 3.into(),
                        results: vec![Some(3.into())],
                        mutates: vec![],
                        function: "torch.ops.aten.full.default".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::IntList(vec![2, 3]), WireValue::Double(2.0)],
                            HashMap::from([(
                                "device".into(),
                                WireValue::Device("cuda".parse().unwrap()),
                            )]),
                        )
                        .unwrap(),
                        stream: 0.into(),
                        remote_process_groups: vec![],
                    }),
                    WorkerMessage::CallFunction(CallFunctionParams {
                        seq: 4.into(),
                        results: vec![Some(4.into())],
                        mutates: vec![],
                        function: "torch.ops.aten.allclose.default".into(),
                        args_kwargs: ArgsKwargs::from_wire_values(
                            vec![WireValue::Ref(2.into()), WireValue::Ref(3.into())],
                            HashMap::new(),
                        )
                        .unwrap(),
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
}
