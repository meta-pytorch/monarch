use std::sync::Arc;

use hyperactor::ActorRef;
use hyperactor_extension::python_registration;
use monarch_hyperactor::proc::InstanceWrapper;
use monarch_hyperactor::proc::PyProc;
use monarch_hyperactor::proc::PySerialized;
use monarch_hyperactor::runtime::signal_safe_block_on;
use monarch_messages::controller::ControllerActor;
use monarch_messages::controller::ControllerMessageClient;
use monarch_messages::debugger::DebuggerAction;
use monarch_messages::debugger::DebuggerMessage;
use monarch_worker::stream::CONTROLLER_ACTOR_REF;
use monarch_worker::stream::PROC;
use monarch_worker::stream::ROOT_ACTOR_ID;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyNone;
use tokio::sync::Mutex;

#[pyclass(
    frozen,
    get_all,
    name = "DebuggerMessage",
    module = "monarch._monarch.debugger"
)]
pub struct PyDebuggerMessage {
    action: DebuggerAction,
}

#[pymethods]
impl PyDebuggerMessage {
    #[new]
    pub fn new(action: DebuggerAction) -> Self {
        Self { action }
    }

    #[getter]
    pub fn action(&self) -> DebuggerAction {
        self.action.clone()
    }

    pub fn serialize(&self) -> PyResult<PySerialized> {
        let msg: DebuggerMessage = self.action.clone().into();
        PySerialized::new(&msg)
    }
}

#[pyfunction]
pub fn get_bytes_from_write_action(
    py: Python<'_>,
    action: DebuggerAction,
) -> PyResult<Bound<'_, PyBytes>> {
    if let DebuggerAction::Write { bytes } = action {
        Ok(PyBytes::new_bound(py, &bytes))
    } else {
        Err(PyRuntimeError::new_err(format!(
            "Cannot extract bytes from non-write debugger action {:?}",
            action
        )))
    }
}

#[pyclass(module = "monarch._monarch.debugger")]
pub struct PdbActor {
    instance: Arc<Mutex<InstanceWrapper<DebuggerMessage>>>,
    controller_actor_ref: ActorRef<ControllerActor>,
}

#[pymethods]
impl PdbActor {
    #[new]
    fn new() -> PyResult<Self> {
        let proc = PyProc::new_from_proc(PROC.with(|cell| cell.get().unwrap().clone()));
        let root_actor_id = ROOT_ACTOR_ID.with(|cell| cell.get().unwrap().clone());
        Ok(Self {
            instance: Arc::new(Mutex::new(InstanceWrapper::new_with_parent(
                &proc,
                &root_actor_id,
            )?)),
            controller_actor_ref: CONTROLLER_ACTOR_REF.with(|cell| cell.get().unwrap().clone()),
        })
    }

    fn send<'py>(&self, py: Python<'py>, action: DebuggerAction) -> PyResult<()> {
        let controller_actor_ref = self.controller_actor_ref.clone();
        let instance = self.instance.clone();
        signal_safe_block_on(py, async move {
            let instance = instance.lock().await;
            let mailbox = instance.mailbox().clone();
            let actor_id = instance.actor_id().clone();
            controller_actor_ref
                .debugger_message(&mailbox, actor_id, action)
                .await
                .map_err(|err| PyRuntimeError::new_err(err.to_string()))
        })?
    }

    fn receive(&mut self, py: Python<'_>) -> PyResult<PyObject> {
        let instance = self.instance.clone();
        let result =
            signal_safe_block_on(
                py,
                async move { instance.lock().await.next_message(None).await },
            )?;
        match result {
            Ok(Some(DebuggerMessage::Action { action })) => Ok(action.into_py(py)),
            Ok(None) => Ok(PyNone::get_bound(py).into_py(py)),
            Err(err) => Err(PyRuntimeError::new_err(err.to_string())),
        }
    }

    fn drain_and_stop(&mut self, py: Python<'_>) -> PyResult<()> {
        let instance = self.instance.clone();
        signal_safe_block_on(py, async move {
            instance
                .lock()
                .await
                .drain_and_stop()
                .map(|_| ())
                .map_err(|err| PyRuntimeError::new_err(err.to_string()))
        })?
    }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let debugger = python_registration::get_or_add_new_module(module, "debugger")?;

    debugger.add_class::<PdbActor>()?;
    debugger.add_class::<monarch_messages::debugger::DebuggerAction>()?;
    debugger.add_class::<PyDebuggerMessage>()?;
    debugger.add_function(wrap_pyfunction!(get_bytes_from_write_action, &debugger)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use hyperactor::ActorId;
    use hyperactor::Mailbox;
    use hyperactor::Named;
    use hyperactor::data::Serialized;
    use hyperactor::mailbox::PortReceiver;
    use hyperactor::proc::Proc;
    use monarch_messages::controller::ControllerMessage;

    use super::*;

    fn send_to_debugger(mbox: &Mailbox, debugger_actor_id: &ActorId, action: DebuggerAction) {
        let debugger_port_id = debugger_actor_id.port_id(DebuggerMessage::port());
        let msg: DebuggerMessage = action.into();
        debugger_port_id.send(
            mbox,
            &Serialized::serialize::<DebuggerMessage>(&msg).unwrap(),
        );
    }

    fn receive_on_debugger(actor: &mut PdbActor) -> DebuggerAction {
        Python::with_gil(|py| {
            let msg = actor.receive(py).unwrap();
            let action: DebuggerAction = msg.extract(py).unwrap();
            action
        })
    }

    fn receive_on_controller(
        rx: Arc<Mutex<PortReceiver<ControllerMessage>>>,
    ) -> (ActorId, DebuggerAction) {
        let msg = Python::with_gil(|py| {
            signal_safe_block_on(py, async move { rx.lock().await.recv().await.unwrap() }).unwrap()
        });
        match msg {
            ControllerMessage::DebuggerMessage {
                debugger_actor_id,
                action,
            } => (debugger_actor_id, action),
            _ => panic!("Expected ControllerMessage::DebuggerMessage, got {:?}", msg),
        }
    }

    /// This is intentionally not a tokio async test. PdbActor and other
    /// structs it relies on use get_tokio_runtime() to create (once) and
    /// access a singleton tokio runtime. Unfortunately our async_timed_test
    /// macro creates its own tokio runtime, so attempting to call
    /// get_tokio_runtime() in a tokio async test will panic.
    #[test]
    fn test_pdb_actor() {
        pyo3::prepare_freethreaded_python();

        let proc = Proc::local();
        let (_, controller_ref, controller_rx) = proc
            .attach_actor::<ControllerActor, ControllerMessage>("controller")
            .unwrap();

        // Need to use signal_safe_block_on for async operations like receiving from controller_rx.
        // This requires the async closure to obtain a mutable reference to controller_rx. The trait
        // bounds on signal_safe_block_on, however, require all references to have lifetime 'static,
        // which controller_rx does not have. So we need to wrap controller_rx in Arc and Mutex.
        let controller_rx = Arc::new(Mutex::new(controller_rx));
        // Allocate a root worker actor id for the pdb actor to be a child of.
        let worker = proc.attach("worker").unwrap();
        PROC.with(|cell| cell.set(proc.clone()).ok());
        CONTROLLER_ACTOR_REF.with(|cell| cell.set(controller_ref.clone()).ok());
        ROOT_ACTOR_ID.with(|cell| cell.set(worker.actor_id().clone()).ok());

        let mut actor = PdbActor::new().unwrap();
        let debugger_actor_id = actor.instance.blocking_lock().actor_id().clone();

        Python::with_gil(|py| actor.send(py, DebuggerAction::Paused()).unwrap());

        let (received_actor_id, action) = receive_on_controller(controller_rx.clone());
        assert_eq!(received_actor_id, debugger_actor_id);
        assert_eq!(action, DebuggerAction::Paused());

        let client = proc.attach("client").unwrap();

        send_to_debugger(&client, &debugger_actor_id, DebuggerAction::Attach());
        let action = receive_on_debugger(&mut actor);
        assert_eq!(action, DebuggerAction::Attach());

        Python::with_gil(|py| {
            actor
                .send(py, DebuggerAction::Read { requested_size: 4 })
                .unwrap()
        });

        let (received_actor_id, action) = receive_on_controller(controller_rx.clone());
        assert_eq!(received_actor_id, debugger_actor_id);
        assert_eq!(action, DebuggerAction::Read { requested_size: 4 });

        send_to_debugger(
            &client,
            &debugger_actor_id,
            DebuggerAction::Write {
                bytes: vec![1, 2, 3, 4],
            },
        );

        let action = receive_on_debugger(&mut actor);
        assert_eq!(
            action,
            DebuggerAction::Write {
                bytes: vec![1, 2, 3, 4],
            }
        );

        Python::with_gil(|py| {
            actor
                .send(
                    py,
                    DebuggerAction::Write {
                        bytes: vec![5, 6, 7, 8],
                    },
                )
                .unwrap()
        });

        let (received_actor_id, action) = receive_on_controller(controller_rx.clone());
        assert_eq!(received_actor_id, debugger_actor_id);
        assert_eq!(
            action,
            DebuggerAction::Write {
                bytes: vec![5, 6, 7, 8],
            }
        );

        send_to_debugger(&client, &debugger_actor_id, DebuggerAction::Detach());

        let action = receive_on_debugger(&mut actor);
        assert_eq!(action, DebuggerAction::Detach());

        Python::with_gil(|py| actor.drain_and_stop(py).unwrap());
    }
}
