/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::sync::OnceLock;

use hyperactor::ActorId;
use monarch_hyperactor::ndslice::PySlice;
use monarch_hyperactor::proc::PyActorId;
use monarch_messages::controller::Seq;
use monarch_messages::worker;
use monarch_messages::worker::ArgsKwargs;
use monarch_messages::worker::CallFunctionParams;
use monarch_messages::worker::Cloudpickle;
use monarch_messages::worker::Factory;
use monarch_messages::worker::FunctionPath;
use monarch_messages::worker::Reduction;
use monarch_messages::worker::Ref;
use monarch_messages::worker::ResolvableFunction;
use monarch_messages::worker::StreamCreationMode;
use monarch_messages::worker::StreamRef;
use monarch_messages::worker::WorkerMessage;
use ndslice::Slice;
use pyo3::Bound;
use pyo3::PyAny;
use pyo3::PyResult;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::types::PyTuple;
use torch_sys_cuda::nccl::ReduceOp;
use torch_sys_cuda::nccl::UniqueId;

struct MessageParser<'a> {
    current: Bound<'a, PyAny>,
}

fn create_function(obj: Bound<'_, PyAny>) -> PyResult<ResolvableFunction> {
    let cloudpickle = obj
        .py()
        .import("monarch.common.function")?
        .getattr("ResolvableFromCloudpickle")?;
    if obj.is_instance(&cloudpickle)? {
        Ok(ResolvableFunction::Cloudpickle(Cloudpickle::new(
            obj.getattr("data")?.extract()?,
        )))
    } else {
        Ok(ResolvableFunction::FunctionPath(FunctionPath::new(
            obj.getattr("path")?.extract()?,
        )))
    }
}
fn create_ref(obj: Bound<'_, PyAny>) -> PyResult<Ref> {
    let r = obj.getattr("ref")?;
    let v: u64 = r.extract()?;
    Ok(v.into())
}

impl<'a> MessageParser<'a> {
    fn new(current: Bound<'a, PyAny>) -> Self {
        Self { current }
    }

    fn attr(&self, name: &str) -> PyResult<Bound<'a, PyAny>> {
        self.current.getattr(name)
    }
    #[allow(non_snake_case)]
    fn parseStreamRef(&self, name: &str) -> PyResult<StreamRef> {
        let r = self.attr(name)?.getattr("ref")?;
        let id: u64 = r.extract()?;
        Ok(StreamRef { id })
    }
    #[allow(non_snake_case)]
    fn parseStreamCreationMode(&self, name: &str) -> PyResult<StreamCreationMode> {
        let default: bool = self.parse(name)?;
        Ok(if default {
            worker::StreamCreationMode::UseDefaultStream
        } else {
            worker::StreamCreationMode::CreateNewStream
        })
    }
    #[allow(non_snake_case)]
    fn parseSeq(&self, name: &str) -> PyResult<Seq> {
        let v: u64 = self.attr(name)?.extract()?;
        Ok(v.into())
    }
    #[allow(non_snake_case)]
    fn parseFlatReferences(&self, name: &str) -> PyResult<Vec<Option<Ref>>> {
        let tree_flatten = self
            .current
            .py()
            .import("torch.utils._pytree")?
            .getattr("tree_flatten")?;
        let output_tuple: (Bound<'a, PyAny>, Bound<'a, PyAny>) =
            tree_flatten.call1((self.attr(name)?,))?.extract()?;
        let referenceable = self
            .current
            .py()
            .import("monarch.common.reference")?
            .getattr("Referenceable")?;
        let mut flat: Vec<Option<Ref>> = vec![];
        for x in output_tuple.0.try_iter()? {
            let v: Bound<'a, PyAny> = x?;
            if v.is_instance(&referenceable)? {
                flat.push(Some(create_ref(v)?));
            } else {
                let r: Option<Ref> = v.extract().ok();
                flat.push(r);
            }
        }
        Ok(flat)
    }
    #[allow(non_snake_case)]
    fn parseRef(&self, name: &str) -> PyResult<Ref> {
        create_ref(self.attr(name)?)
    }
    #[allow(non_snake_case)]
    fn parseOptionalRef(&self, name: &str) -> PyResult<Option<Ref>> {
        let obj = self.attr(name)?;
        if obj.is_none() {
            Ok(None)
        } else {
            Ok(Some(create_ref(obj)?))
        }
    }

    fn parse<T: pyo3::conversion::FromPyObject<'a>>(&self, name: &str) -> PyResult<T> {
        self.attr(name)?.extract()
    }

    #[allow(non_snake_case)]
    fn parseRefList(&self, name: &str) -> PyResult<Vec<Ref>> {
        self.attr(name)?
            .try_iter()?
            .map(|x| {
                let v = x?;
                let vr: PyResult<u64> = v.extract();
                if let Ok(v) = vr {
                    Ok(v.into())
                } else {
                    create_ref(v)
                }
            })
            .collect()
    }

    #[allow(non_snake_case)]
    fn parseFunction(&self, name: &str) -> PyResult<ResolvableFunction> {
        create_function(self.attr(name)?)
    }
    #[allow(non_snake_case)]
    fn parseOptionalFunction(&self, name: &str) -> PyResult<Option<ResolvableFunction>> {
        let f = self.attr(name)?;
        if f.is_none() {
            Ok(None)
        } else {
            Ok(Some(create_function(f)?))
        }
    }
    #[allow(non_snake_case)]
    fn parseNDSlice(&self, name: &str) -> PyResult<Slice> {
        let slice: PySlice = self.parse(name)?;
        Ok(slice.into())
    }
    #[allow(non_snake_case)]
    fn parseFactory(&self, name: &str) -> PyResult<Factory> {
        Factory::from_py(self.attr(name)?)
    }
    #[allow(non_snake_case)]
    fn parseWorkerMessageList(&self, name: &str) -> PyResult<Vec<WorkerMessage>> {
        self.attr(name)?.try_iter()?.map(|x| convert(x?)).collect()
    }
    fn parse_error_reason(&self, name: &str) -> PyResult<Option<(Option<ActorId>, String)>> {
        let err = self.attr(name)?;
        if err.is_none() {
            return Ok(None);
        }
        if let Ok(actor_source_id) = err.getattr("source_actor_id") {
            let actor_id: PyActorId = actor_source_id.extract()?;
            return Ok(Some((
                Some(actor_id.into()),
                err.getattr("message")?.extract()?,
            )));
        }
        let msg: String = err.to_string();
        Ok(Some((None, msg)))
    }
}

type FnType = for<'py> fn(MessageParser<'py>) -> PyResult<WorkerMessage>;
static CONVERT_MAP: OnceLock<HashMap<u64, FnType>> = OnceLock::new();

fn create_map(py: Python) -> HashMap<u64, FnType> {
    let messages = py
        .import("monarch.common.messages")
        .expect("import monarch.common.messages");
    let mut m: HashMap<u64, FnType> = HashMap::new();
    let key = |name: &str| {
        messages
            .getattr(name)
            .expect("lookup message type")
            .as_ptr() as u64
    };
    m.insert(key("BackendNetworkInit"), |_p| {
        Ok(WorkerMessage::BackendNetworkInit(
            UniqueId::new().map_err(|err| PyRuntimeError::new_err(err.to_string()))?,
        ))
    });
    m.insert(key("BackendNetworkPointToPointInit"), |p| {
        Ok(WorkerMessage::BackendNetworkPointToPointInit {
            from_stream: p.parseStreamRef("from_stream")?,
            to_stream: p.parseStreamRef("to_stream")?,
        })
    });
    m.insert(key("CallFunction"), |p| {
        let function = p.parseFunction("function")?;
        let args: Bound<'_, PyTuple> = p.parse("args")?;
        let kwargs: Bound<'_, PyDict> = p.parse("kwargs")?;

        let args_kwargs = ArgsKwargs::from_python(args.into_any(), kwargs.into_any())?;
        Ok(WorkerMessage::CallFunction(CallFunctionParams {
            seq: p.parseSeq("ident")?,
            results: p.parseFlatReferences("result")?,
            mutates: p.parseRefList("mutates")?,
            function,
            args_kwargs,
            stream: p.parseStreamRef("stream")?,
            remote_process_groups: p.parseRefList("remote_process_groups")?,
        }))
    });
    m.insert(key("CreateStream"), |p| {
        Ok(WorkerMessage::CreateStream {
            id: p.parseStreamRef("result")?,
            stream_creation: p.parseStreamCreationMode("default")?,
        })
    });
    m.insert(key("CreateDeviceMesh"), |p| {
        Ok(WorkerMessage::CreateDeviceMesh {
            result: p.parseRef("result")?,
            names: p.parse("names")?,
            ranks: p.parseNDSlice("ranks")?,
        })
    });
    m.insert(key("CreateRemoteProcessGroup"), |p| {
        Ok(WorkerMessage::CreateRemoteProcessGroup {
            result: p.parseRef("result")?,
            device_mesh: p.parseRef("device_mesh")?,
            dims: p.parse("dims")?,
        })
    });

    m.insert(key("BorrowCreate"), |p| {
        Ok(WorkerMessage::BorrowCreate {
            result: p.parseRef("result")?,
            borrow: p.parse("borrow")?,
            tensor: p.parseRef("tensor")?,
            from_stream: p.parseStreamRef("from_stream")?,
            to_stream: p.parseStreamRef("to_stream")?,
        })
    });
    m.insert(key("BorrowFirstUse"), |p| {
        Ok(WorkerMessage::BorrowFirstUse {
            borrow: p.parse("borrow")?,
        })
    });
    m.insert(key("BorrowLastUse"), |p| {
        Ok(WorkerMessage::BorrowLastUse {
            borrow: p.parse("borrow")?,
        })
    });
    m.insert(key("BorrowDrop"), |p| {
        Ok(WorkerMessage::BorrowDrop {
            borrow: p.parse("borrow")?,
        })
    });
    m.insert(key("DeleteRefs"), |p| {
        Ok(WorkerMessage::DeleteRefs(p.parseRefList("refs")?))
    });
    m.insert(key("RequestStatus"), |p| {
        Ok(WorkerMessage::RequestStatus {
            seq: p.parseSeq("ident")?,
            controller: p.parse("controller")?,
        })
    });
    m.insert(key("Reduce"), |p| {
        let reduction: String = p.parse("reduction")?;
        let reduction = match reduction.as_str() {
            "sum" => Reduction::ReduceOp(ReduceOp::Sum),
            "prod" => Reduction::ReduceOp(ReduceOp::Prod),
            "stack" => Reduction::Stack,
            "avg" => Reduction::ReduceOp(ReduceOp::Avg),
            "min" => Reduction::ReduceOp(ReduceOp::Min),
            "max" => Reduction::ReduceOp(ReduceOp::Max),
            _ => {
                return Err(PyRuntimeError::new_err(format!(
                    "Unsupported reduction {}",
                    reduction
                )));
            }
        };

        Ok(WorkerMessage::Reduce {
            result: p.parseRef("result")?,
            tensor: p.parseRef("local_tensor")?,
            factory: p.parseFactory("factory")?,
            mesh: p.parseRef("source_mesh")?,
            stream: p.parseStreamRef("stream")?,
            dims: p.parse("dims")?,
            reduction,
            scatter: p.parse("scatter")?,
            in_place: p.parse("inplace")?,
            out: p.parseOptionalRef("out")?,
        })
    });
    m.insert(key("SendTensor"), |p| {
        Ok(WorkerMessage::SendTensor {
            result: p.parseRef("result")?,
            from_ranks: p.parseNDSlice("from_ranks")?,
            to_ranks: p.parseNDSlice("to_ranks")?,
            tensor: p.parseRef("tensor")?,
            factory: p.parseFactory("factory")?,
            from_stream: p.parseStreamRef("from_stream")?,
            to_stream: p.parseStreamRef("to_stream")?,
        })
    });
    m.insert(key("SendValue"), |p|  {
            let function = p.parseOptionalFunction("function")?;
            let args: Bound<'_, PyTuple> = p.parse("args")?;
            let kwargs: Bound<'_, PyDict> = p.parse("kwargs")?;

            if function.is_none() && (args.len() != 1 || !kwargs.is_empty()) {
                return Err(PyValueError::new_err(
                    "SendValue with no function must have exactly one argument and no keyword arguments",
                ));
            }
            let args_kwargs = ArgsKwargs::from_python(args.into_any(), kwargs.into_any())?;
            Ok(WorkerMessage::SendValue {
                seq: p.parseSeq("ident")?,
                destination: p.parseOptionalRef("destination")?,
                mutates: p.parseRefList("mutates")?,
                function,
                args_kwargs,
                stream: p.parseStreamRef("stream")?,
            })
        });

    m.insert(key("SplitComm"), |p| {
        Ok(WorkerMessage::SplitComm {
            dims: p.parse("dims")?,
            device_mesh: p.parseRef("device_mesh")?,
            stream: p.parseStreamRef("stream")?,
        })
    });
    m.insert(key("SplitCommForProcessGroup"), |p| {
        Ok(WorkerMessage::SplitCommForProcessGroup {
            remote_process_group: p.parseRef("remote_process_group")?,
            stream: p.parseStreamRef("stream")?,
        })
    });
    m.insert(key("DefineRecording"), |p| {
        Ok(WorkerMessage::DefineRecording {
            result: p.parseRef("result")?,
            nresults: p.parse("nresults")?,
            nformals: p.parse("nformals")?,
            commands: p.parseWorkerMessageList("commands")?,
            ntotal_messages: p.parse("ntotal_messages")?,
            index: p.parse("message_index")?,
        })
    });
    m.insert(key("RecordingFormal"), |p| {
        Ok(WorkerMessage::RecordingFormal {
            result: p.parseRef("result")?,
            argument_index: p.parse("argument_index")?,
            stream: p.parseStreamRef("stream")?,
        })
    });
    m.insert(key("RecordingResult"), |p| {
        Ok(WorkerMessage::RecordingResult {
            result: p.parseRef("input")?,
            output_index: p.parse("output_index")?,
            stream: p.parseStreamRef("stream")?,
        })
    });
    m.insert(key("CallRecording"), |p| {
        Ok(WorkerMessage::CallRecording {
            seq: p.parseSeq("ident")?,
            recording: p.parseRef("recording")?,
            results: p.parseRefList("results")?,
            actuals: p.parseRefList("actuals")?,
        })
    });
    m.insert(key("PipeRecv"), |p| {
        Ok(WorkerMessage::PipeRecv {
            seq: p.parseSeq("ident")?,
            results: p.parseFlatReferences("result")?,
            pipe: p.parseRef("pipe")?,
            stream: p.parseStreamRef("stream")?,
        })
    });
    m.insert(key("Exit"), |p| {
        Ok(WorkerMessage::Exit {
            error: p.parse_error_reason("error")?,
        })
    });
    m.insert(key("CommandGroup"), |p| {
        Ok(WorkerMessage::CommandGroup(
            p.parseWorkerMessageList("commands")?,
        ))
    });
    m.insert(key("SendResultOfActorCall"), |p| {
        Ok(WorkerMessage::SendResultOfActorCall(
            worker::ActorCallParams {
                seq: p.parseSeq("seq")?,
                broker_id: p.parse("broker_id")?,
                local_state: p.parseRefList("local_state")?,
                mutates: p.parseRefList("mutates")?,
                stream: p.parseStreamRef("stream")?,
            },
        ))
    });
    m.insert(key("CallActorMethod"), |p| {
        Ok(WorkerMessage::CallActorMethod(worker::ActorMethodParams {
            call: worker::ActorCallParams {
                seq: p.parseSeq("seq")?,
                broker_id: p.parse("broker_id")?,
                local_state: p.parseRefList("local_state")?,
                mutates: p.parseRefList("mutates")?,
                stream: p.parseStreamRef("stream")?,
            },
            results: p.parseFlatReferences("result")?,
        }))
    });
    m
}

pub fn convert<'py>(m: Bound<'py, PyAny>) -> PyResult<WorkerMessage> {
    let converter = {
        let typ = m.get_type().as_ptr() as u64;
        CONVERT_MAP.get_or_init(|| create_map(m.py()))[&typ]
    };
    converter(MessageParser::new(m))
}

pub fn register_python_bindings(_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
