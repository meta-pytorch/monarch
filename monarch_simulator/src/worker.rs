/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::ops::Add;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use anyhow::ensure;
use async_trait::async_trait;
use dashmap::DashMap;
use hyperactor::Actor;
use hyperactor::ActorRef;
use hyperactor::Named;
use hyperactor::data::Serialized;
use hyperactor::forward;
use hyperactor::reference::ActorId;
use hyperactor::simnet::TorchOpEvent;
use hyperactor::simnet::simnet_handle;
use monarch_messages::controller::ControllerActor;
use monarch_messages::controller::ControllerMessageClient;
use monarch_messages::controller::Seq;
use monarch_messages::controller::WorkerError;
use monarch_messages::wire_value::WireValue;
use monarch_messages::worker::*;
use monarch_tensor_worker::device_mesh::DeviceMesh;
use monarch_types::PyTree;
use ndslice::Slice;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use torch_sys::Device;
use torch_sys::DeviceType;
use torch_sys::Layout;
use torch_sys::RValue;
use torch_sys::ScalarType;
use torch_sys::Tensor;
use torch_sys::TensorCell;
use torch_sys::factory_empty;
use torch_sys::factory_zeros;
use torch_sys_cuda::nccl::NcclConfig;
use torch_sys_cuda::nccl::ReduceOp;
use torch_sys_cuda::nccl::UniqueId;

use crate::collective_coordinator::CollectiveResult;
use crate::collective_coordinator::activate_mesh;
use crate::collective_coordinator::collect;
use crate::collective_coordinator::is_active;

type Channel<M> = (
    mpsc::UnboundedSender<M>,
    Arc<Mutex<mpsc::UnboundedReceiver<M>>>,
);

/// A fake backend network to support sending tensors between nodes.
#[derive(Debug, Deserialize, Serialize)]
pub struct Fabric {
    #[serde(skip)]
    inputs: DashMap<(StreamRef, usize), Channel<TensorCell>>,
    #[serde(skip)]
    outputs: DashMap<(StreamRef, usize), Channel<TensorCell>>,
}

const PAFT_RECONFIG_FTAR_FCN: &str = "paft.paft_worker.reconfig_ftar";
const PAFT_RUN_ALLREDUCE_FCN: &str = "paft.paft_worker.run_allreduce";

impl Fabric {
    pub fn new() -> Self {
        Self {
            inputs: DashMap::new(),
            outputs: DashMap::new(),
        }
    }

    fn put_input(&self, stream: StreamRef, from: usize, tensor: TensorCell) -> Result<()> {
        let sender = self
            .inputs
            .entry((stream, from))
            .or_insert_with(|| {
                let (s, r) = mpsc::unbounded_channel();
                (s, Arc::new(Mutex::new(r)))
            })
            .0
            .clone();
        sender.send(tensor)?;
        Ok(())
    }

    async fn get_input(&self, stream: StreamRef, from: usize) -> Result<TensorCell> {
        let recv = self
            .inputs
            .entry((stream, from))
            .or_insert_with(|| {
                let (s, r) = mpsc::unbounded_channel();
                (s, Arc::new(Mutex::new(r)))
            })
            .1
            .clone();
        let mut recv = recv.lock().await;
        recv.recv().await.context("channel closed")
    }

    fn put_output(&self, stream: StreamRef, to: usize, tensor: TensorCell) -> Result<()> {
        let sender = self
            .outputs
            .entry((stream, to))
            .or_insert_with(|| {
                let (s, r) = mpsc::unbounded_channel();
                (s, Arc::new(Mutex::new(r)))
            })
            .0
            .clone();
        sender.send(tensor)?;
        Ok(())
    }

    async fn get_output(&self, stream: StreamRef, to: usize) -> Result<TensorCell> {
        let recv = self
            .outputs
            .entry((stream, to))
            .or_insert_with(|| {
                let (s, r) = mpsc::unbounded_channel();
                (s, Arc::new(Mutex::new(r)))
            })
            .1
            .clone();
        let mut recv = recv.lock().await;
        recv.recv().await.context("channel closed")
    }
}

fn reduce_op<T: Clone + Default + Add<Output = T>>(
    _op: ReduceOp,
    _output: &mut Tensor,
    _inputs: &[TensorCell],
) -> Result<()> {
    // TODO(agallagher): Do we need an impl for this?
    Ok(())
}

#[derive(Debug)]
#[hyperactor::export(
    spawn = true,
    handlers = [
        WorkerMessage { cast = true },
    ],
)]
pub struct WorkerActor {
    rank: usize,
    worker_actor_id: ActorId,
    fabric: Arc<Fabric>,
    /// factory to use to create fake tensors.
    factory: Factory,
    /// the dimensions used for fake tensors.
    dims: i64,
    device_meshes: HashMap<Ref, DeviceMesh>,
    env: HashMap<Ref, TensorCell>,
    pipes: HashMap<Ref, Ref>,
    controller_actor_ref: ActorRef<ControllerActor>,
    worker_error: Option<WorkerError>,
}

#[derive(Clone, Debug, Named, Serialize, Deserialize)]
pub struct MockWorkerParams {
    rank: usize,
    worker_actor_id: ActorId,
    fabric: Arc<Fabric>,
    factory: Factory,
    dims: i64,
    controller_actor_ref: ActorRef<ControllerActor>,
}

impl MockWorkerParams {
    pub fn new(
        rank: usize,
        worker_actor_id: ActorId,
        fabric: Arc<Fabric>,
        factory: Factory,
        dims: i64,
        controller_actor_ref: ActorRef<ControllerActor>,
    ) -> Self {
        Self {
            rank,
            worker_actor_id,
            fabric,
            factory,
            dims,
            controller_actor_ref,
        }
    }
}

#[async_trait]
impl Actor for WorkerActor {
    type Params = MockWorkerParams;

    async fn new(
        MockWorkerParams {
            rank,
            worker_actor_id,
            fabric,
            factory,
            dims,
            controller_actor_ref,
        }: Self::Params,
    ) -> Result<Self> {
        Ok(Self {
            rank,
            worker_actor_id,
            fabric,
            factory,
            dims,
            device_meshes: HashMap::new(),
            env: HashMap::new(),
            pipes: HashMap::new(),
            controller_actor_ref,
            worker_error: None,
        })
    }
}

#[async_trait]
#[forward(WorkerMessage)]
impl WorkerMessageHandler for WorkerActor {
    async fn backend_network_init(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _unique_id: UniqueId,
    ) -> Result<()> {
        Ok(())
    }

    async fn backend_network_point_to_point_init(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _from_stream: StreamRef,
        _to_stream: StreamRef,
    ) -> Result<()> {
        Ok(())
    }

    async fn call_function(
        &mut self,
        cx: &hyperactor::Context<Self>,
        params: CallFunctionParams,
    ) -> Result<()> {
        tracing::info!("worker received call_function: {:#?}", &params);
        match &params.function {
            ResolvableFunction::FunctionPath(FunctionPath { path }) => {
                tracing::info!("function path: {:#?}", &path);
                if path == PAFT_RECONFIG_FTAR_FCN {
                    let step = match params.kwargs.get("step").unwrap() {
                        WireValue::PyObject(step) => Some(step.clone()),
                        _ => None,
                    };
                    let step = step.unwrap();
                    let serialized_step = serde_json::to_string(&step).unwrap();
                    activate_mesh(self.worker_actor_id.world_name().parse()?, &serialized_step)
                        .await;
                }
                if path == PAFT_RUN_ALLREDUCE_FCN {
                    if !is_active(self.worker_actor_id.world_name().parse()?).await {
                        // Controller will send supervision failure message to controller.
                        panic!("worker is killed by user");
                    }
                    let rx = collect().await;
                    let collective_result = rx.await;
                    if collective_result.unwrap() == CollectiveResult::PeerUnavailable {
                        // Send worker error to controller.
                        let worker_error = WorkerError {
                            backtrace: "AllReduce failed".to_string(),
                            worker_actor_id: self.worker_actor_id.clone(),
                        };
                        self.worker_error = Some(worker_error);
                        return Ok(());
                    }
                }
            }
            _ => {}
        }
        for result in params.results.into_iter() {
            if let Some(result) = result {
                self.env.insert(result, self.mock_tensor()?);
            }
        }
        match &params.function.as_torch_op() {
            Some((op, _)) => {
                self.call_torch_op(op, params.args, params.kwargs, cx.self_id().clone())
                    .await?;
            }
            _ => {
                let _ = self.call_python_fn(
                    cx,
                    params.function,
                    params.args,
                    params.kwargs,
                    &params.mutates,
                );
            }
        }
        Ok(())
    }

    async fn send_result_of_actor_call(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _params: ActorCallParams,
    ) -> Result<()> {
        bail!("unimplemented: send_result_of_actor_call");
    }

    async fn call_actor_method(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _params: ActorMethodParams,
    ) -> Result<()> {
        bail!("unimplemented: call_actor_method");
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
        _cx: &hyperactor::Context<Self>,
        _result: StreamRef,
        _creation_mode: StreamCreationMode,
    ) -> Result<()> {
        Ok(())
    }

    async fn create_device_mesh(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        result: Ref,
        names: Vec<String>,
        ranks: Slice,
    ) -> Result<()> {
        self.device_meshes
            .insert(result, DeviceMesh::new(names, ranks, self.rank)?);
        Ok(())
    }

    async fn create_remote_process_group(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _result: Ref,
        _device_mesh: Ref,
        _dims: Vec<String>,
    ) -> Result<()> {
        bail!("unimplemented: create_remote_process_group")
    }

    async fn borrow_create(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _result: Ref,
        _borrow_id: u64,
        _tensor_ref: Ref,
        _from_stream: StreamRef,
        _to_stream: StreamRef,
    ) -> Result<()> {
        bail!("unimplemented: borrow_create")
    }

    async fn borrow_first_use(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _borrow: u64,
    ) -> Result<()> {
        bail!("unimplemented: borrow_first_use")
    }

    async fn borrow_last_use(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _borrow: u64,
    ) -> Result<()> {
        bail!("unimplemented: borrow_last_use")
    }

    async fn borrow_drop(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _borrow_id: u64,
    ) -> Result<()> {
        bail!("unimplemented: borrow_drop")
    }

    async fn delete_refs(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _refs: Vec<Ref>,
    ) -> Result<()> {
        Ok(())
    }

    async fn request_status(
        &mut self,
        cx: &hyperactor::Context<Self>,
        seq: Seq,
        controller: bool,
    ) -> Result<()> {
        ControllerMessageClient::status(
            &self.controller_actor_ref,
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
        _cx: &hyperactor::Context<Self>,
        result: Ref,
        local_tensor: Ref,
        factory: Factory,
        source_mesh: Ref,
        stream_ref: StreamRef,
        dims: Vec<String>,
        reduction: Reduction,
        _scatter: bool,
        in_place: bool,
        out: Option<Ref>,
    ) -> Result<()> {
        ensure!(
            factory == self.factory,
            "{:?} != {:?}",
            factory,
            self.factory
        );

        let mesh = self
            .device_meshes
            .get(&source_mesh)
            .context("no such mesh")?;
        let input_cell = self.env.get(&local_tensor).context("no such tensor")?;

        // Push input cell onto fabric.
        self.fabric
            .put_input(stream_ref, self.rank, input_cell.clone())?;

        let ranks_for_group = mesh.get_ranks_for_dim_slice(&dims)?;

        // Currentl impl has first rank doing all the work.
        if self.rank == ranks_for_group[0] {
            let mut inputs = Vec::new();
            for rank in ranks_for_group.iter() {
                inputs.push(self.fabric.get_input(stream_ref, *rank).await?);
            }

            // Create space for the result.
            let sizes = [&[dims.len() as i64][..], &self.factory.size[..]].concat();
            let mut result = factory_empty(
                &sizes,
                self.factory.dtype,
                self.factory.layout,
                self.factory.device,
            );

            match reduction {
                Reduction::ReduceOp(op) => match self.factory.dtype {
                    ScalarType::Float => reduce_op::<f32>(op, &mut result, inputs.as_slice())?,
                    ScalarType::Int => reduce_op::<i32>(op, &mut result, inputs.as_slice())?,
                    _ => bail!("unimplemented reduce op"),
                },
                _ => bail!("unimplemented reduction"),
            }

            let result_cell = TensorCell::new(result);
            for rank in ranks_for_group.iter() {
                self.fabric
                    .put_output(stream_ref, *rank, result_cell.clone())?;
            }
        }

        // Emit results.
        let result_cell = self.fabric.get_output(stream_ref, self.rank).await?;
        let result_tensor = result_cell.try_borrow().map_err(anyhow::Error::msg)?;
        let output_cell = match (out, in_place) {
            (None, false) => TensorCell::new(torch_sys::deep_clone(&result_tensor)),
            (None, true) => {
                let input = input_cell.try_borrow_mut().map_err(anyhow::Error::msg)?;
                // SAFETY: ...
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        result_tensor.data_ptr(),
                        input.mut_data_ptr(),
                        input.nbytes(),
                    )
                };
                input_cell.clone()
            }
            _ => bail!("unimplemented output style"),
        };
        self.env.insert(result, output_cell);

        Ok(())
    }

    async fn create_pipe(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        result: Ref,
        _key: String,
        _function: ResolvableFunction,
        _max_messages: i64,
        device_mesh: Ref,
        _args: Vec<WireValue>,
        _kwargs: HashMap<String, WireValue>,
    ) -> Result<()> {
        self.pipes.insert(result, device_mesh);
        Ok(())
    }

    async fn send_tensor(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _result: Ref,
        _from_ranks: Slice,
        _to_ranks: Slice,
        _tensor: Ref,
        _factory: Factory,
        _from_stream: StreamRef,
        _to_stream: StreamRef,
    ) -> Result<()> {
        bail!("unimplemented: send_tensor")
    }

    async fn exit(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _error: Option<(Option<ActorId>, String)>,
    ) -> Result<()> {
        Ok(())
    }

    async fn send_value(
        &mut self,
        cx: &hyperactor::Context<Self>,
        seq: Seq,
        _destination: Option<Ref>,
        _mutates: Vec<Ref>,
        _function: Option<ResolvableFunction>,
        _args: Vec<WireValue>,
        _kwargs: HashMap<String, WireValue>,
        _stream: StreamRef,
    ) -> Result<()> {
        tracing::info!("worker received send_value");
        if let Some(worker_error) = self.worker_error.take() {
            self.controller_actor_ref
                .remote_function_failed(cx, seq, worker_error)
                .await?;
            return Ok(());
        }

        let tensor = factory_zeros(
            &[1],
            ScalarType::Float,
            Layout::Strided,
            Device::new(DeviceType { repr: 0 }),
        );
        let rvalue = RValue::Tensor(TensorCell::new(tensor));
        let value = PyTree::from(rvalue);
        let result = Ok(Serialized::serialize(&value)?);
        self.controller_actor_ref
            .fetch_result(cx, seq, result)
            .await?;
        Ok(())
    }

    async fn split_comm(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _dims: Vec<String>,
        _device_mesh: Ref,
        _stream_ref: StreamRef,
        _config: Option<NcclConfig>,
    ) -> Result<()> {
        Ok(())
    }

    async fn split_comm_for_process_group(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _remote_process_group_ref: Ref,
        _stream_ref: StreamRef,
        _config: Option<NcclConfig>,
    ) -> Result<()> {
        Ok(())
    }

    async fn pipe_recv(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _seq: Seq,
        results: Vec<Option<Ref>>,
        pipe: Ref,
        _stream: StreamRef,
    ) -> Result<()> {
        let mesh = self
            .device_meshes
            .get(self.pipes.get(&pipe).context("missing pipe")?)
            .context("missing mesh")?;
        ensure!(mesh.sizes().len() as i64 == self.dims);
        for result in results.into_iter() {
            if let Some(result) = result {
                self.env.insert(result, self.mock_tensor()?);
            }
        }
        Ok(())
    }

    async fn set_ref_unit_tests_only(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _reference: Ref,
        _value: WireValue,
        _stream: StreamRef,
    ) -> Result<()> {
        bail!("unimplemented: set_ref_unit_tests_only")
    }

    async fn get_ref_unit_tests_only(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _ref_id: Ref,
        _stream: StreamRef,
    ) -> Result<Option<Result<WireValue, String>>> {
        bail!("unimplemented: get_ref_unit_tests_only")
    }

    async fn define_recording(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _result: Ref,
        _nresults: usize,
        _nformals: usize,
        _commands: Vec<WorkerMessage>,
        _ntotal_messages: usize,
        _index: usize,
    ) -> Result<()> {
        unimplemented!()
    }

    async fn recording_formal(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _result: Ref,
        _argument_index: usize,
        _stream: StreamRef,
    ) -> Result<()> {
        unimplemented!()
    }

    async fn recording_result(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _result: Ref,
        _output_index: usize,
        _stream: StreamRef,
    ) -> Result<()> {
        unimplemented!()
    }

    async fn call_recording(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _seq: Seq,
        _recording: Ref,
        _results: Vec<Ref>,
        _actuals: Vec<Ref>,
    ) -> Result<()> {
        unimplemented!()
    }
}

impl WorkerActor {
    fn mock_tensor(&self) -> Result<TensorCell> {
        let sizes = [&[self.dims][..], &self.factory.size[..]].concat();
        let tensor = factory_empty(
            &sizes,
            self.factory.dtype,
            self.factory.layout,
            self.factory.device,
        );
        Ok(TensorCell::new(tensor))
    }

    async fn call_torch_op(
        &self,
        op: &str,
        args: Vec<WireValue>,
        kwargs: HashMap<String, WireValue>,
        actor_id: ActorId,
    ) -> Result<()> {
        let args_string = args
            .iter()
            .filter(|&wirevalue| wirevalue.is_ref())
            .map(|wirevalue| wirevalue.as_ref().unwrap().to_string())
            .collect::<Vec<String>>()
            .join(", ");

        let kwargs_string = kwargs
            .iter()
            .filter_map(|(k, wirevalue)| {
                wirevalue
                    .is_ref()
                    .then(|| format!("{}={}", k, wirevalue.as_ref().unwrap()))
            })
            .collect::<Vec<String>>()
            .join(", ");

        let (tx, rx) = oneshot::channel();

        simnet_handle()?
            .send_event(TorchOpEvent::new(
                op.to_string(),
                tx,
                args_string,
                kwargs_string,
                actor_id,
            ))
            .unwrap();

        rx.await.unwrap();

        Ok(())
    }

    fn call_python_fn(
        &mut self,
        _cx: &hyperactor::Context<Self>,
        _function: ResolvableFunction,
        _args: Vec<WireValue>,
        _kwargs: HashMap<String, WireValue>,
        _mutates: &[Ref],
    ) -> Result<()> {
        bail!("unimplemented: call_python_fn")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use futures::future::try_join_all;
    use hyperactor::id;
    use hyperactor::proc::Proc;
    use hyperactor::simnet;
    use monarch_types::PyTree;
    use torch_sys::Layout;
    use torch_sys::RValue;
    use torch_sys::TensorCell;
    use torch_sys::test_make_tensor;

    use super::*;

    #[tokio::test]
    async fn test_all_reduce() -> Result<()> {
        let proc = Proc::local();
        let _client = proc.attach("client")?;

        let world_size = 4;
        let fabric = Arc::new(Fabric::new());
        let factory = Factory {
            size: vec![2, 3],
            dtype: ScalarType::Float,
            layout: Layout::Strided,
            device: "cpu".try_into()?,
        };

        let mut workers = vec![];
        for rank in 0..world_size {
            workers.push(
                proc.spawn::<WorkerActor>(
                    &format!("worker{}", rank),
                    MockWorkerParams {
                        rank,
                        worker_actor_id: id!(worker[0].root),
                        fabric: fabric.clone(),
                        factory: factory.clone(),
                        dims: 2,
                        controller_actor_ref: ActorRef::attest(id!(controller[0].root)),
                    },
                )
                .await?,
            );
        }

        for worker in workers.into_iter() {
            worker.drain_and_stop()?;
            worker.await;
        }

        Ok(())
    }

    #[tokio::test]
    async fn worker_reduce() -> Result<()> {
        simnet::start();
        let proc = Proc::local();
        let (client, _handle) = proc.instance("client")?;

        let world_size = 4;
        let fabric = Arc::new(Fabric::new());
        let factory = Factory {
            size: vec![2, 3],
            dtype: ScalarType::Float,
            layout: Layout::Strided,
            device: "cpu".try_into()?,
        };

        let workers = try_join_all((0..world_size).map(async |rank| {
            proc.spawn::<WorkerActor>(
                &format!("worker{}", rank),
                MockWorkerParams {
                    rank,
                    worker_actor_id: id!(worker[0].root),
                    fabric: fabric.clone(),
                    factory: factory.clone(),
                    dims: 2,
                    controller_actor_ref: ActorRef::attest(id!(controller[0].root)),
                },
            )
            .await
        }))
        .await?;

        let unique_id = UniqueId::new()?;
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
            WorkerMessage::CallFunction(CallFunctionParams {
                seq: 0.into(),
                results: vec![Some(2.into())],
                mutates: vec![],
                function: "torch.ops.aten.ones.default".into(),
                args: vec![WireValue::IntList(vec![2, 3])],
                kwargs: HashMap::from([("device".into(), WireValue::Device("cuda".try_into()?))]),
                stream: 0.into(),
                remote_process_groups: vec![],
            }),
            // Test reduce over "x".
            WorkerMessage::Reduce {
                result: 3.into(),
                tensor: 2.into(),
                factory: factory.clone(),
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
                kwargs: HashMap::from([("device".into(), WireValue::Device("cuda".try_into()?))]),
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
            // Test reduce over "x" and "y".
            WorkerMessage::Reduce {
                result: 6.into(),
                tensor: 2.into(),
                factory,
                mesh: 1.into(),
                stream: 0.into(),
                dims: vec!["x".into(), "y".into()],
                reduction: Reduction::ReduceOp(ReduceOp::Sum),
                scatter: false,
                in_place: false,
                out: None,
            },
        ];

        for worker in workers.iter() {
            worker.command_group(&client, messages.clone()).await?;
        }

        for worker in workers.into_iter() {
            worker.drain_and_stop()?;
            worker.await;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_create_tensor_pytree() -> Result<()> {
        // A simple test case to show how to create a tensor pytree.
        let tensor = test_make_tensor();
        let rvalue = RValue::Tensor(TensorCell::new(tensor));
        let _pytree = PyTree::from(&rvalue);
        Ok(())
    }
}
