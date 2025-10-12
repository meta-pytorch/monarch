/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::ops::DerefMut;
use std::sync;
use std::sync::Arc;
use std::sync::atomic;
use std::sync::atomic::AtomicUsize;

use async_trait::async_trait;
use hyperactor::Actor;
use hyperactor::ActorHandle;
use hyperactor::ActorId;
use hyperactor::ActorRef;
use hyperactor::Context;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::Instance;
use hyperactor::PortRef;
use hyperactor::ProcId;
use hyperactor::context;
use hyperactor::mailbox::MailboxSenderError;
use hyperactor_mesh::Mesh;
use hyperactor_mesh::ProcMesh;
use hyperactor_mesh::actor_mesh::ActorMesh;
use hyperactor_mesh::actor_mesh::RootActorMesh;
use hyperactor_mesh::shared_cell::SharedCell;
use hyperactor_mesh::shared_cell::SharedCellRef;
use monarch_hyperactor::actor::PythonMessage;
use monarch_hyperactor::actor::PythonMessageKind;
use monarch_hyperactor::buffers::FrozenBuffer;
use monarch_hyperactor::context::PyInstance;
use monarch_hyperactor::instance_dispatch;
use monarch_hyperactor::local_state_broker::LocalStateBrokerActor;
use monarch_hyperactor::mailbox::PyPortId;
use monarch_hyperactor::ndslice::PySlice;
use monarch_hyperactor::proc_mesh::PyProcMesh;
use monarch_hyperactor::proc_mesh::TrackedProcMesh;
use monarch_hyperactor::runtime::signal_safe_block_on;
use monarch_hyperactor::v1::proc_mesh::PyProcMesh as PyProcMeshV1;
use monarch_messages::controller::ControllerActor;
use monarch_messages::controller::ControllerMessage;
use monarch_messages::controller::Seq;
use monarch_messages::controller::WorkerError;
use monarch_messages::debugger::DebuggerAction;
use monarch_messages::debugger::DebuggerActor;
use monarch_messages::debugger::DebuggerMessage;
use monarch_messages::worker::Ref;
use monarch_messages::worker::WorkerMessage;
use monarch_messages::worker::WorkerParams;
use monarch_tensor_worker::AssignRankMessage;
use monarch_tensor_worker::WorkerActor;
use ndslice::Slice;
use ndslice::ViewExt;
use ndslice::selection;
use ndslice::selection::ReifySlice;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use tokio::sync::Mutex;

use crate::convert::convert;

pub(crate) fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<_Controller>()?;
    Ok(())
}

/// The rust-side implementation of monarch.mesh_controller.Controller
/// It exports the API that interacts with the controller actor (MeshControllerActor)
#[pyclass(
    subclass,
    module = "monarch._rust_bindings.monarch_extension.mesh_controller"
)]
struct _Controller {
    controller_handle: Arc<Mutex<ActorHandle<MeshControllerActor>>>,
    all_ranks: Slice,
    broker_id: (String, usize),
}

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

fn to_py_error<T>(e: T) -> PyErr
where
    T: Error,
{
    PyErr::new::<PyValueError, _>(e.to_string())
}

#[pymethods]
impl _Controller {
    #[new]
    fn new(py: Python, client: PyInstance, py_proc_mesh: &Bound<'_, PyAny>) -> PyResult<Self> {
        let (proc_mesh, rank_map) = if let Ok(v0) = py_proc_mesh.downcast::<PyProcMesh>() {
            (v0.borrow().inner.clone(), None)
        } else {
            // Here, also extract a rank map. We have to look
            // up which ids correspond with which ranks.
            //
            // This should be fixed up for the true v1 support,
            // possibly by having the workers send back their ranks
            // directly.
            let proc_mesh = py_proc_mesh
                .downcast::<PyProcMeshV1>()?
                .borrow()
                .mesh_ref()?;
            let rank_map = proc_mesh
                .iter()
                .map(|(point, proc)| (proc.proc_id().clone(), point.rank()))
                .collect();
            (
                SharedCell::from(TrackedProcMesh::from(ProcMesh::from(proc_mesh))),
                Some(rank_map),
            )
        };

        let proc_mesh_ref = proc_mesh.borrow().unwrap();
        let shape = proc_mesh_ref.shape();
        let slice = shape.slice();
        let all_ranks = shape.slice().clone();
        if !slice.is_contiguous() || slice.offset() != 0 {
            return Err(PyValueError::new_err(
                "NYI: proc mesh for workers must be contiguous and start at offset 0",
            ));
        }

        let id = NEXT_ID.fetch_add(1, atomic::Ordering::Relaxed);
        let controller_handle: Arc<Mutex<ActorHandle<MeshControllerActor>>> =
            signal_safe_block_on(py, async move {
                let controller_handle = instance_dispatch!(client, |instance| {
                    instance
                        .spawn(MeshControllerActorParams {
                            proc_mesh,
                            id,
                            rank_map,
                        })
                        .await?
                });
                Ok::<_, anyhow::Error>(Arc::new(Mutex::new(controller_handle)))
            })??;

        Ok(Self {
            controller_handle,
            all_ranks,
            // note that 0 is the _pid_ of the broker, which will be 0 for
            // top-level spawned actors.
            // todo: plumb these through as proper actor mesh refs
            broker_id: (format!("tensor_engine_brokers_{}", id), 0),
        })
    }

    #[getter]
    fn broker_id(&self) -> (String, usize) {
        self.broker_id.clone()
    }

    #[pyo3(signature = (seq, defs, uses, response_port, tracebacks))]
    fn node<'py>(
        &mut self,
        seq: u64,
        defs: Bound<'py, PyAny>,
        uses: Bound<'py, PyAny>,
        response_port: Option<(PyPortId, PySlice)>,
        tracebacks: Py<PyAny>,
    ) -> PyResult<()> {
        let response_port: Option<PortInfo> = response_port.map(|(port, ranks)| PortInfo {
            port: PortRef::attest(port.into()),
            ranks: ranks.into(),
        });
        let msg = ClientToControllerMessage::Node {
            seq: seq.into(),
            defs: defs
                .try_iter()?
                .map(|x| Ref::from_py_object(&x?))
                .collect::<PyResult<Vec<Ref>>>()?,
            uses: uses
                .try_iter()?
                .map(|x| Ref::from_py_object(&x?))
                .collect::<PyResult<Vec<Ref>>>()?,
            tracebacks,
            response_port,
        };
        self.controller_handle
            .blocking_lock()
            .send(msg)
            .map_err(to_py_error)
    }

    fn drop_refs(&mut self, refs: Vec<Ref>) -> PyResult<()> {
        self.controller_handle
            .blocking_lock()
            .send(ClientToControllerMessage::DropRefs { refs })
            .map_err(to_py_error)
    }

    fn sync_at_exit(&mut self, port: PyPortId) -> PyResult<()> {
        self.controller_handle
            .blocking_lock()
            .send(ClientToControllerMessage::SyncAtExit {
                port: PortRef::attest(port.into()),
            })
            .map_err(to_py_error)
    }

    fn send<'py>(&mut self, ranks: Bound<'py, PyAny>, message: Bound<'py, PyAny>) -> PyResult<()> {
        let slices = if let Ok(slice) = ranks.extract::<PySlice>() {
            vec![slice.into()]
        } else {
            let slices = ranks.extract::<Vec<PySlice>>()?;
            slices.iter().map(|x| x.into()).collect::<Vec<Slice>>()
        };
        let message: WorkerMessage = convert(message)?;
        self.controller_handle
            .blocking_lock()
            .send(ClientToControllerMessage::Send { slices, message })
            .map_err(to_py_error)
    }
    fn _drain_and_stop(&mut self) -> PyResult<()> {
        self.controller_handle
            .blocking_lock()
            .send(ClientToControllerMessage::Send {
                slices: vec![self.all_ranks.clone()],
                message: WorkerMessage::Exit { error: None },
            })
            .map_err(to_py_error)?;
        self.controller_handle
            .blocking_lock()
            .drain_and_stop()
            .map_err(to_py_error)
    }
}

/// An invocation tracks a discrete node in the graph of operations executed by
/// the worker based on instructions from the client.
/// It is useful for tracking the dependencies of an operation and propagating
/// failures. In the future this will be used with more data dependency tracking
/// to support better failure handling.

#[derive(Debug)]
enum Status {
    Errored {
        exception: Arc<PythonMessage>,
    },
    Complete {},

    /// When incomplete this holds this list of users of this invocation,
    /// so a future error can be propagated to them.,
    Incomplete {
        users: HashMap<Seq, Arc<sync::Mutex<Invocation>>>,
        results: Vec<PythonMessage>,
    },
}

impl Status {
    fn incomplete() -> Status {
        Self::Incomplete {
            users: HashMap::new(),
            results: vec![],
        }
    }
}
#[derive(Debug)]
struct Invocation {
    /// The sequence number of the invocation. This should be unique and increasing across all
    /// invocations.
    seq: Seq,
    status: Status,
    /// Result reported to a future if this invocation was a fetch
    /// Not all Invocations will be fetched so sometimes a Invocation will complete with
    /// both result and error == None
    response_port: Option<PortInfo>,
    tracebacks: Py<PyAny>,
}

impl Invocation {
    fn new(seq: Seq, tracebacks: Py<PyAny>, response_port: Option<PortInfo>) -> Self {
        Self {
            seq,
            status: Status::incomplete(),
            response_port,
            tracebacks,
        }
    }

    fn add_user(
        &mut self,
        sender: &impl context::Actor,
        unreported_exception: &mut Option<Arc<PythonMessage>>,
        user: Arc<sync::Mutex<Invocation>>,
    ) -> Result<(), MailboxSenderError> {
        match &mut self.status {
            Status::Complete {} => {}
            Status::Incomplete { users, .. } => {
                let seq = user.lock().unwrap().seq;
                users.insert(seq, user);
            }
            Status::Errored { exception } => {
                user.lock().unwrap().set_exception(
                    sender,
                    unreported_exception,
                    exception.clone(),
                )?;
            }
        }
        Ok(())
    }

    /// Invocation results can only go from valid to failed, or be
    /// set if the invocation result is empty.
    fn set_result(&mut self, result: PythonMessage) {
        match &mut self.status {
            Status::Incomplete { results, .. } => {
                results.push(result);
            }
            Status::Errored { .. } => {}
            Status::Complete {} => {
                panic!("setting result on a complete seq");
            }
        }
    }

    fn complete(&mut self, sender: &impl context::Actor) -> Result<(), MailboxSenderError> {
        let old_status = std::mem::replace(&mut self.status, Status::Complete {});
        match old_status {
            Status::Incomplete { results, .. } => match &self.response_port {
                Some(PortInfo { port, ranks }) => {
                    assert!(ranks.len() == results.iter().len());
                    for result in results.into_iter() {
                        port.send(sender, result)?;
                    }
                }
                None => {}
            },
            _ => {
                self.status = old_status;
            }
        }
        Ok(())
    }

    /// Changes the status of this invocation to an Errored. If this invocation was
    /// Incomplete, it may have users that will also become errored. This function
    /// will return those users so the error can be propagated. It does not autmoatically
    /// propagate the error to avoid deep recursive invocations.
    fn set_exception(
        &mut self,
        sender: &impl context::Actor,
        unreported_exception: &mut Option<Arc<PythonMessage>>,
        exception: Arc<PythonMessage>,
    ) -> Result<(), MailboxSenderError> {
        let mut process =
            |invocation: &mut Invocation, queue: &mut Vec<Arc<sync::Mutex<Invocation>>>| {
                let err = Status::Errored {
                    exception: exception.clone(),
                };
                let old_status = std::mem::replace(&mut invocation.status, err);
                match old_status {
                    Status::Incomplete { users, .. } => {
                        match &invocation.response_port {
                            Some(PortInfo { port, ranks }) => {
                                *unreported_exception = None;
                                for rank in ranks.iter() {
                                    let msg = exception.as_ref().clone().into_rank(rank);
                                    port.send(sender, msg)?;
                                }
                            }
                            None => {}
                        };
                        queue.extend(users.into_values());
                    }
                    Status::Complete {} => {
                        panic!("Complete invocation getting an exception set")
                    }
                    Status::Errored { .. } => invocation.status = old_status,
                }
                Ok(())
            };
        let mut queue = vec![];
        let mut visited = HashSet::new();
        process(self, &mut queue)?;
        while let Some(invocation) = queue.pop() {
            let mut invocation = invocation.lock().unwrap();
            if !visited.insert(invocation.seq) {
                continue;
            };
            process(invocation.deref_mut(), &mut queue)?;
        }
        Ok(())
    }
}

/// The history of invocations sent by the client to be executed on the workers.
/// This is used to track dependencies between invocations and to propagate exceptions.
/// It purges history for completed invocations to avoid memory bloat.
/// TODO: Revisit this setup around purging refs automatically once we start doing
/// more complex data dependency tracking. We will want to be more aware of things like
/// borrows, drops etc. directly.
#[derive(Debug)]
struct History {
    /// The first incomplete Seq for each rank. This is used to determine which
    /// Seqs are no longer relevant and can be purged from the history.
    first_incomplete_seqs: MinVector<Seq>,
    /// The minimum incomplete Seq across all ranks.
    min_incomplete_seq: Seq,
    /// A map of seq to the invocation that it represents for all seq >= min_incomplete_seq
    inflight_invocations: HashMap<Seq, Arc<sync::Mutex<Invocation>>>,
    /// A map of reference to the seq for the invocation that defines it. This is used to
    /// compute dependencies between invocations.
    invocation_for_ref: HashMap<Ref, Arc<sync::Mutex<Invocation>>>,
    // no new sequence numbers should be below this bound. use for
    // sanity checking.
    seq_lower_bound: Seq,
    unreported_exception: Option<Arc<PythonMessage>>,
    exit_port: Option<PortRef<PythonMessage>>,
}

/// A vector that keeps track of the minimum value.
#[derive(Debug)]
struct MinVector<T> {
    data: Vec<T>,
    value_counts: BTreeMap<T, usize>,
}

impl<T> MinVector<T>
where
    T: Ord + Copy,
{
    fn new(data: Vec<T>) -> Self {
        let mut value_counts = BTreeMap::new();
        for &value in &data {
            *value_counts.entry(value).or_insert(0) += 1;
        }
        MinVector { data, value_counts }
    }

    fn set(&mut self, index: usize, value: T) {
        // Decrease the count of the old value
        let old_value = self.data[index];
        if let Some(count) = self.value_counts.get_mut(&old_value) {
            *count -= 1;
            if *count == 0 {
                self.value_counts.remove(&old_value);
            }
        }
        // Update the value in the vector
        self.data[index] = value;

        // Increase the count of the new value
        *self.value_counts.entry(value).or_insert(0) += 1;
    }

    fn min(&self) -> T {
        *self.value_counts.keys().next().unwrap()
    }
}

impl History {
    pub fn new(world_size: usize) -> Self {
        Self {
            first_incomplete_seqs: MinVector::new(vec![Seq::default(); world_size]),
            min_incomplete_seq: Seq::default(),
            invocation_for_ref: HashMap::new(),
            inflight_invocations: HashMap::new(),
            seq_lower_bound: 0.into(),
            unreported_exception: None,
            exit_port: None,
        }
    }

    #[cfg(test)]
    pub fn first_incomplete_seqs(&self) -> &[Seq] {
        self.first_incomplete_seqs.vec()
    }

    pub fn drop_refs(&mut self, refs: Vec<Ref>) {
        for r in refs {
            self.invocation_for_ref.remove(&r);
        }
    }

    /// Add an invocation to the history.
    pub fn add_invocation(
        &mut self,
        sender: &impl context::Actor,
        seq: Seq,
        uses: Vec<Ref>,
        defs: Vec<Ref>,
        tracebacks: Py<PyAny>,
        response_port: Option<PortInfo>,
    ) -> Result<(), MailboxSenderError> {
        assert!(
            seq >= self.seq_lower_bound,
            "nonmonotonic seq: {:?}; current lower bound: {:?}",
            seq,
            self.seq_lower_bound,
        );
        self.seq_lower_bound = seq;
        let invocation = Arc::new(sync::Mutex::new(Invocation::new(
            seq,
            tracebacks,
            response_port,
        )));
        self.inflight_invocations.insert(seq, invocation.clone());
        for ref use_ in uses {
            let producer = self.invocation_for_ref.get(use_).unwrap();
            producer.lock().unwrap().add_user(
                sender,
                &mut self.unreported_exception,
                invocation.clone(),
            )?;
        }

        for def in defs {
            self.invocation_for_ref.insert(def, invocation.clone());
        }
        Ok(())
    }

    /// Propagate worker error to the invocation with the given Seq. This will also propagate
    /// to all seqs that depend on this seq directly or indirectly.
    pub fn propagate_exception(
        &mut self,
        sender: &impl context::Actor,
        seq: Seq,
        exception: WorkerError,
        rank: usize,
    ) -> Result<(), MailboxSenderError> {
        // TODO: supplement PythonMessage with the stack trace we have in invocation

        let invocation = self.inflight_invocations.get(&seq).unwrap().clone();

        let python_message = Arc::new(Python::with_gil(|py| {
            let traceback = invocation
                .lock()
                .unwrap()
                .tracebacks
                .bind(py)
                .get_item(0)
                .unwrap();
            let remote_exception = py
                .import("monarch.mesh_controller")
                .unwrap()
                .getattr("RemoteException")
                .unwrap();
            let pickle = py
                .import("monarch._src.actor.actor_mesh")
                .unwrap()
                .getattr("_pickle")
                .unwrap();
            let exe = remote_exception
                .call1((exception.backtrace, traceback, rank))
                .unwrap();
            let data: FrozenBuffer = pickle.call1((exe,)).unwrap().extract().unwrap();
            PythonMessage::new_from_buf(
                PythonMessageKind::Exception { rank: Some(rank) },
                data.inner,
            )
        }));

        let mut invocation = invocation.lock().unwrap();

        if let Status::Incomplete { .. } = &invocation.status {
            self.unreported_exception = Some(python_message.clone());
        }

        invocation.set_exception(
            sender,
            &mut self.unreported_exception,
            python_message.clone(),
        )?;

        Ok(())
    }

    /// Mark the given rank as completed up to but excluding the given Seq. This will also purge history for
    /// any Seqs that are no longer relevant (completed on all ranks).
    pub fn rank_completed(
        &mut self,
        sender: &impl context::Actor,
        rank: usize,
        seq: Seq,
    ) -> Result<(), MailboxSenderError> {
        self.first_incomplete_seqs.set(rank, seq);
        let prev = self.min_incomplete_seq;
        self.min_incomplete_seq = self.first_incomplete_seqs.min();

        for i in Seq::iter_between(prev, self.min_incomplete_seq) {
            if let Some(invocation) = self.inflight_invocations.remove(&i) {
                let mut invocation = invocation.lock().unwrap();
                invocation.complete(sender)?;
            }
        }
        if let Some(port) = &self.exit_port {
            if self.min_incomplete_seq >= self.seq_lower_bound {
                let result = match &self.unreported_exception {
                    Some(exception) => exception.as_ref().clone(),
                    None => {
                        // the byte string is just a Python None
                        PythonMessage::new_from_buf(
                            PythonMessageKind::Result { rank: None },
                            b"\x80\x04N.".to_vec(),
                        )
                    }
                };
                port.send(sender, result)?;
                self.exit_port = None;
            }
        }
        Ok(())
    }

    pub fn set_result(&mut self, seq: Seq, result: PythonMessage) {
        let invocation = self.inflight_invocations.get(&seq).unwrap();
        invocation.lock().unwrap().set_result(result);
    }

    fn report_exit(&mut self, port: PortRef<PythonMessage>) {
        self.exit_port = Some(port);
    }
}

#[derive(Debug)]
struct PortInfo {
    port: PortRef<PythonMessage>,
    // the slice of ranks expected to respond
    // to the port. used for error reporting.
    ranks: Slice,
}

#[derive(Debug, Handler, HandleClient)]
enum ClientToControllerMessage {
    Send {
        slices: Vec<Slice>,
        message: WorkerMessage,
    },
    Node {
        seq: Seq,
        defs: Vec<Ref>,
        uses: Vec<Ref>,
        tracebacks: Py<PyAny>,
        response_port: Option<PortInfo>,
    },
    DropRefs {
        refs: Vec<Ref>,
    },
    SyncAtExit {
        port: PortRef<PythonMessage>,
    },
}

struct MeshControllerActor {
    proc_mesh: SharedCell<TrackedProcMesh>,
    workers: Option<SharedCell<RootActorMesh<'static, WorkerActor>>>,
    brokers: Option<SharedCell<RootActorMesh<'static, LocalStateBrokerActor>>>,
    history: History,
    id: usize,
    debugger_active: Option<ActorRef<DebuggerActor>>,
    debugger_paused: VecDeque<ActorRef<DebuggerActor>>,
    rank_map: Option<HashMap<ProcId, usize>>,
}

impl MeshControllerActor {
    fn workers(&self) -> SharedCellRef<RootActorMesh<'static, WorkerActor>> {
        self.workers.as_ref().unwrap().borrow().unwrap()
    }

    fn handle_debug(
        &mut self,
        this: &Context<Self>,
        debugger_actor_id: ActorId,
        action: DebuggerAction,
    ) -> anyhow::Result<()> {
        if matches!(action, DebuggerAction::Paused()) {
            self.debugger_paused
                .push_back(ActorRef::attest(debugger_actor_id));
        } else {
            let debugger_actor = self
                .debugger_active
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("no active debugger"))?;
            if debugger_actor_id != *debugger_actor.actor_id() {
                anyhow::bail!("debugger action for wrong actor");
            }
            match action {
                DebuggerAction::Detach() => {
                    self.debugger_active = None;
                }
                DebuggerAction::Read { requested_size } => {
                    Python::with_gil(|py| {
                        let read = py
                            .import("monarch.controller.debugger")
                            .unwrap()
                            .getattr("read")
                            .unwrap();
                        let bytes: Vec<u8> =
                            read.call1((requested_size,)).unwrap().extract().unwrap();

                        debugger_actor.send(
                            this,
                            DebuggerMessage::Action {
                                action: DebuggerAction::Write { bytes },
                            },
                        )
                    })?;
                }
                DebuggerAction::Write { bytes } => {
                    Python::with_gil(|py| -> Result<(), anyhow::Error> {
                        let write = py
                            .import("monarch.controller.debugger")
                            .unwrap()
                            .getattr("write")
                            .unwrap();
                        write.call1((String::from_utf8(bytes)?,)).unwrap();
                        Ok(())
                    })?;
                }
                _ => {
                    anyhow::bail!("unexpected action: {:?}", action);
                }
            }
        }
        if self.debugger_active.is_none() {
            self.debugger_active = self.debugger_paused.pop_front().and_then(|pdb_actor| {
                pdb_actor
                    .send(
                        this,
                        DebuggerMessage::Action {
                            action: DebuggerAction::Attach(),
                        },
                    )
                    .map(|_| pdb_actor)
                    .ok()
            });
        }
        Ok(())
    }
}

impl Debug for MeshControllerActor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeshControllerActor").finish()
    }
}

struct MeshControllerActorParams {
    proc_mesh: SharedCell<TrackedProcMesh>,
    id: usize,
    rank_map: Option<HashMap<ProcId, usize>>,
}

#[async_trait]
impl Actor for MeshControllerActor {
    type Params = MeshControllerActorParams;
    async fn new(
        MeshControllerActorParams {
            proc_mesh,
            id,
            rank_map,
        }: Self::Params,
    ) -> Result<Self, anyhow::Error> {
        let world_size = proc_mesh.borrow().unwrap().shape().slice().len();
        Ok(MeshControllerActor {
            proc_mesh: proc_mesh.clone(),
            workers: None,
            brokers: None,
            history: History::new(world_size),
            id,
            debugger_active: None,
            debugger_paused: VecDeque::new(),
            rank_map,
        })
    }
    async fn init(&mut self, this: &Instance<Self>) -> Result<(), anyhow::Error> {
        let controller_actor_ref: ActorRef<ControllerActor> = this.bind();
        let proc_mesh = self.proc_mesh.borrow().unwrap();
        let slice = proc_mesh.shape().slice();
        let world_size = slice.len();
        let param = WorkerParams {
            world_size,
            // Rank assignment is consistent with proc indices.
            rank: 0,
            device_index: Some(0),
            controller_actor: controller_actor_ref,
        };

        let workers = proc_mesh
            .spawn(this, &format!("tensor_engine_workers_{}", self.id), &param)
            .await?;
        workers.borrow().unwrap().cast(
            this,
            selection::dsl::true_(),
            AssignRankMessage::AssignRank(),
        )?;

        self.workers = Some(workers);
        let brokers = proc_mesh
            .spawn(this, &format!("tensor_engine_brokers_{}", self.id), &())
            .await?;
        self.brokers = Some(brokers);
        Ok(())
    }
}

impl MeshControllerActor {
    fn rank_of_worker(&self, actor_id: &ActorId) -> usize {
        if actor_id.proc_id().is_ranked() {
            actor_id.rank()
        } else {
            self.rank_map
                .as_ref()
                .expect("direct-addressed workers should have a rank map")
                .get(actor_id.proc_id())
                .expect("rank map should contain worker")
                .clone()
        }
    }
}

#[async_trait]
impl Handler<ControllerMessage> for MeshControllerActor {
    async fn handle(
        &mut self,
        this: &Context<Self>,
        message: ControllerMessage,
    ) -> anyhow::Result<()> {
        match message {
            ControllerMessage::DebuggerMessage {
                debugger_actor_id,
                action,
            } => {
                self.handle_debug(this, debugger_actor_id, action)?;
            }
            ControllerMessage::Status {
                seq,
                worker_actor_id,
                controller: false,
            } => {
                self.history
                    .rank_completed(this, self.rank_of_worker(&worker_actor_id), seq)?;
            }
            ControllerMessage::FetchResult {
                seq,
                value: Ok(value),
            } => {
                let msg: PythonMessage = value.deserialized().unwrap();
                self.history.set_result(seq, msg);
            }
            ControllerMessage::RemoteFunctionFailed { seq, error } => {
                let rank = self.rank_of_worker(&error.worker_actor_id);
                self.history.propagate_exception(this, seq, error, rank)?;
            }
            message => {
                panic!("unexpected message: {:?}", message);
            }
        };
        Ok(())
    }
}

#[async_trait]
impl Handler<ClientToControllerMessage> for MeshControllerActor {
    async fn handle(
        &mut self,
        this: &Context<Self>,
        message: ClientToControllerMessage,
    ) -> anyhow::Result<()> {
        match message {
            ClientToControllerMessage::Send { slices, message } => {
                let sel = self.workers().shape().slice().reify_slices(slices)?;
                self.workers().cast(this, sel, message)?;
            }
            ClientToControllerMessage::Node {
                seq,
                defs,
                uses,
                tracebacks,
                response_port,
            } => {
                self.history
                    .add_invocation(this, seq, uses, defs, tracebacks, response_port)?;
            }
            ClientToControllerMessage::DropRefs { refs } => {
                self.history.drop_refs(refs);
            }
            ClientToControllerMessage::SyncAtExit { port } => {
                self.workers().cast(
                    this,
                    selection::dsl::true_(),
                    WorkerMessage::RequestStatus {
                        seq: self.history.seq_lower_bound,
                        controller: false,
                    },
                )?;
                self.history.report_exit(port);
            }
        }
        Ok(())
    }
}
