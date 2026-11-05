/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Python-facing supervision boundary. `SupervisionError`'s
//! pyclass shape is unchanged by the mesh-name plumbing in this
//! diff; any rendered improvement users see in the Python
//! exception comes from the upstream `Display` chain once mesh
//! name and Python-class `display_name` are populated by their
//! producer sites.

use async_trait::async_trait;
use hyperactor::Instance;
use hyperactor::actor::ActorErrorKind;
use hyperactor::actor::ActorStatus;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor::supervision::SupervisionObservation;
use hyperactor_mesh::supervision::MeshFailure;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::actor::PythonActor;

fn python_event(mut event: ActorSupervisionEvent) -> ActorSupervisionEvent {
    if let Some(observation) = event.observation.take() {
        let target = match observation {
            SupervisionObservation::LinkLost(target) => {
                let message = match &event.actor_status {
                    ActorStatus::Failed(ActorErrorKind::UnhandledSupervisionEvent(_)) => {
                        "supervision liveness failed".to_string()
                    }
                    _ => format!("supervision liveness failed: {event}"),
                };
                event.actor_status = ActorStatus::generic_failure(message);
                target
            }
            SupervisionObservation::ProcLinkLost(target) => {
                event.actor_status =
                    ActorStatus::generic_failure("proc supervision liveness failed");
                target
            }
            SupervisionObservation::RequestFailed(target) => target,
        };
        event.actor_id = target.actor_id;
        event.display_name = target.display_name;
        event.transparent = false;
    }
    if event.actor_status.is_stopped() {
        if let Some(cause) = event.take_stop_cause() {
            let cause = python_event(*cause);
            if event.transparent && cause.actor_status.is_stopped() {
                return cause;
            }
            event = event.with_stop_cause(Some(Box::new(cause)));
        }
        return event;
    }
    event.actor_status = match event.actor_status {
        ActorStatus::Failed(ActorErrorKind::UnhandledSupervisionEvent(cause)) => {
            if event.transparent && cause.is_error() {
                return python_event(*cause);
            }
            ActorStatus::Failed(ActorErrorKind::UnhandledSupervisionEvent(Box::new(
                python_event(*cause),
            )))
        }
        ActorStatus::Failed(ActorErrorKind::ErrorDuringHandlingSupervision(message, cause)) => {
            ActorStatus::Failed(ActorErrorKind::ErrorDuringHandlingSupervision(
                message,
                Box::new(python_event(*cause)),
            ))
        }
        status => status,
    };
    event
}

/// Trait for types that can provide supervision events.
///
/// This trait abstracts the supervision functionality, allowing endpoint
/// operations to work with any type that can monitor actor health without
/// depending on the full ActorMesh interface.
#[async_trait]
pub trait Supervisable: Send + Sync {
    /// Wait for the next supervision event indicating an actor failure.
    ///
    /// Returns `Some(PyErr)` if a supervision failure is detected,
    /// or `None` if supervision is not available or the mesh is healthy.
    async fn supervision_event(&self, instance: &Instance<PythonActor>) -> Option<PyErr>;
}

#[pyclass(
    name = "SupervisionError",
    module = "monarch._rust_bindings.monarch_hyperactor.supervision",
    extends = PyRuntimeError
)]
#[derive(Clone, Debug)]
pub struct SupervisionError {
    #[pyo3(set)]
    pub endpoint: Option<String>,
    pub message: String,
}

#[pymethods]
impl SupervisionError {
    #[new]
    #[pyo3(signature = (message, endpoint=None))]
    fn new(message: String, endpoint: Option<String>) -> Self {
        SupervisionError { endpoint, message }
    }

    #[staticmethod]
    pub fn new_err(message: String) -> PyErr {
        PyErr::new::<Self, _>(message)
    }

    #[staticmethod]
    pub fn new_err_from_endpoint(message: String, endpoint: String) -> PyErr {
        PyErr::new::<Self, _>((message, Some(endpoint)))
    }

    fn __str__(&self) -> String {
        if let Some(ep) = &self.endpoint {
            format!("Endpoint call {} failed, {}", ep, self.message)
        } else {
            self.message.clone()
        }
    }

    fn __repr__(&self) -> String {
        if let Some(ep) = &self.endpoint {
            format!("SupervisionError(endpoint='{}', '{}')", ep, self.message)
        } else {
            format!("SupervisionError('{}')", self.message)
        }
    }
}

impl SupervisionError {
    // Not From<MeshFailure> because the return type needs to be PyErr.
    #[allow(dead_code)]
    pub(crate) fn new_err_from(failure: MeshFailure) -> PyErr {
        let event = python_event(failure.event);
        let message = event
            .failure_report()
            .unwrap_or_else(|| format!("{}", event));
        Self::new_err(message)
    }
    /// Set the endpoint on a PyErr containing a SupervisionError.
    ///
    /// If the error is a SupervisionError, sets its endpoint field and returns a new
    /// error with the endpoint prefix. If not a SupervisionError, returns the original error.
    pub fn set_endpoint_on_err(py: Python<'_>, err: PyErr, endpoint: String) -> PyErr {
        if let Ok(supervision_err) = err.value(py).extract::<SupervisionError>() {
            Self::new_err_from_endpoint(supervision_err.message, endpoint)
        } else {
            err
        }
    }
}

// TODO: find out how to extend a Python exception and have internal data.
#[derive(Clone, Debug)]
#[pyclass(
    name = "MeshFailure",
    module = "monarch._rust_bindings.monarch_hyperactor.supervision"
)]
pub struct PyMeshFailure {
    pub inner: MeshFailure,
}

impl PyMeshFailure {
    pub fn new(mut failure: MeshFailure) -> Self {
        failure.event = python_event(failure.event);
        Self { inner: failure }
    }
}

impl From<MeshFailure> for PyMeshFailure {
    fn from(failure: MeshFailure) -> Self {
        Self::new(failure)
    }
}

impl std::fmt::Display for PyMeshFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MeshFailure(mesh_name={}, crashed_ranks={:?}, event={})",
            self.inner
                .actor_mesh_name
                .clone()
                .unwrap_or("<none>".into()),
            self.inner.crashed_ranks,
            self.inner.event
        )
    }
}

#[pymethods]
impl PyMeshFailure {
    // TODO: store and return the mesh object.
    #[getter]
    fn mesh(&self) {}

    #[getter]
    fn mesh_name(&self) -> String {
        self.inner
            .actor_mesh_name
            .clone()
            .unwrap_or("<none>".into())
    }

    fn __repr__(&self) -> String {
        format!("{}", self)
    }

    fn report(&self) -> String {
        self.inner
            .event
            .failure_report()
            .unwrap_or_else(|| format!("{}", self.inner.event))
    }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    // Get the Python interpreter instance from the module
    let py = module.py();
    // Add the exception to the module using its type object
    module.add("SupervisionError", py.get_type::<SupervisionError>())?;
    module.add("MeshFailure", py.get_type::<PyMeshFailure>())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use hyperactor::Actor;
    use hyperactor::Proc;
    use hyperactor::ProcAddr;
    use hyperactor::channel::ChannelAddr;
    use hyperactor::supervision::SupervisionTarget;
    use hyperactor::testing::proc_supervison::ProcSupervisionCoordinator;

    use super::*;

    fn event(name: &str, status: ActorStatus) -> ActorSupervisionEvent {
        ActorSupervisionEvent::new(
            ProcAddr::singleton(ChannelAddr::Local(0), "test_proc").actor_addr(name),
            Some(name.to_string()),
            status,
            None,
        )
    }

    fn unhandled(
        name: &str,
        cause: ActorSupervisionEvent,
        transparent: bool,
    ) -> ActorSupervisionEvent {
        let mut result = event(
            name,
            ActorStatus::Failed(ActorErrorKind::UnhandledSupervisionEvent(Box::new(cause))),
        );
        result.transparent = transparent;
        result
    }

    #[tokio::test]
    async fn python_view_preserves_rust_user_actor() {
        #[derive(Debug)]
        struct RustParent;

        #[async_trait]
        impl Actor for RustParent {
            async fn init(&mut self, this: &Instance<Self>) -> anyhow::Result<()> {
                this.spawn(RustChild);
                Ok(())
            }
        }

        #[derive(Debug)]
        struct RustChild;

        #[async_trait]
        impl Actor for RustChild {
            async fn init(&mut self, _this: &Instance<Self>) -> anyhow::Result<()> {
                anyhow::bail!("child failed")
            }
        }

        let proc = Proc::isolated();
        let (mut events, _coordinator) = ProcSupervisionCoordinator::set(&proc).await.unwrap();
        let parent = proc.spawn(RustParent);
        let raw = tokio::time::timeout(std::time::Duration::from_secs(30), events.recv())
            .await
            .unwrap();
        assert_eq!(raw.actor_id, parent.actor_addr().clone());
        assert!(!raw.transparent);
        assert_eq!(python_event(raw.clone()), raw);
        let ActorStatus::Failed(ActorErrorKind::UnhandledSupervisionEvent(child)) =
            &raw.actor_status
        else {
            panic!("expected the Rust parent's failure: {raw}");
        };
        assert!(!child.transparent);
        assert_eq!(
            child.actor_status,
            ActorStatus::generic_failure("initialization error: child failed")
        );
    }

    #[test]
    fn python_view_preserves_user_supervision_chain() {
        let leaf = event("leaf", ActorStatus::generic_failure("leaf failed"));
        let expected = unhandled("user_parent", leaf.clone(), false);
        let raw = unhandled(
            "supervisor",
            unhandled(
                "worker",
                unhandled("user_parent", unhandled("inner_proxy", leaf, true), false),
                true,
            ),
            true,
        );
        let failure = MeshFailure {
            actor_mesh_name: Some("mesh".to_string()),
            event: raw.clone(),
            crashed_ranks: vec![2],
            reporting_controller: None,
        };
        let view = PyMeshFailure::from(failure.clone());
        assert_eq!(view.inner.event, expected);
        assert_eq!(view.inner.actor_mesh_name, failure.actor_mesh_name);
        assert_eq!(view.inner.crashed_ranks, failure.crashed_ranks);
        assert_eq!(failure.event, raw);
        assert!(failure.event.to_string().contains("inner_proxy"));
    }

    #[test]
    fn python_view_preserves_transparent_actors_own_errors_and_nonfailure_causes() {
        let stopped = event("child", ActorStatus::Stopped("done".to_string()));
        let declined = unhandled("internal_parent", stopped.clone(), true);
        assert_eq!(python_event(declined.clone()), declined);
        let raised = event(
            "internal_parent",
            ActorStatus::Failed(ActorErrorKind::ErrorDuringHandlingSupervision(
                "handler failed".to_string(),
                Box::new(stopped),
            )),
        )
        .with_transparent(true);
        assert_eq!(
            python_event(unhandled("proxy", raised.clone(), true)),
            raised
        );
        assert_eq!(python_event(raised.clone()), raised);
        let own_error =
            event("proxy", ActorStatus::generic_failure("proxy failed")).with_transparent(true);
        assert_eq!(python_event(own_error.clone()), own_error);
        let stop_after_error = event("proxy", ActorStatus::Stopped("handled".to_string()))
            .with_transparent(true)
            .with_stop_cause(Some(Box::new(own_error)));
        assert_eq!(python_event(stop_after_error.clone()), stop_after_error);
    }

    #[test]
    fn python_view_derives_remote_stop_from_cause() {
        let child = event("remote_child", ActorStatus::Stopped("done".to_string()));
        let worker = event("worker", ActorStatus::Stopped("child stopped".to_string()))
            .with_transparent(true)
            .with_stop_cause(Some(Box::new(child.clone())));
        let raw = event(
            "supervisor",
            ActorStatus::Stopped("worker stopped".to_string()),
        )
        .with_transparent(true)
        .with_stop_cause(Some(Box::new(worker)));
        assert_eq!(python_event(raw.clone()), child);
        assert_eq!(raw.causal_chain().count(), 3);
        assert!(raw.to_string().contains("worker stopped"));
        let user_stop = event("user", ActorStatus::Stopped("handled".to_string()))
            .with_stop_cause(Some(Box::new(raw)));
        assert_eq!(python_event(user_stop.clone()).actor_id, user_stop.actor_id);
        assert_eq!(python_event(user_stop).cause(), Some(&child));
    }

    #[test]
    fn python_view_derives_lost_link_from_observation() {
        let target = SupervisionTarget {
            actor_id: event("remote_child", ActorStatus::Idle).actor_id,
            display_name: Some("remote_child".to_string()),
        };
        let monitor = event("keepalive", ActorStatus::generic_failure("link failed"));
        let raw = unhandled("supervisor", monitor.clone(), true)
            .with_observation(Some(SupervisionObservation::LinkLost(target.clone())));
        assert_eq!(
            python_event(raw.clone()),
            event(
                "remote_child",
                ActorStatus::generic_failure("supervision liveness failed")
            )
        );
        assert_eq!(raw.caused_by(), &monitor);

        let raw = monitor
            .clone()
            .with_observation(Some(SupervisionObservation::LinkLost(target)));
        assert_eq!(
            python_event(raw.clone()),
            event(
                "remote_child",
                ActorStatus::generic_failure(format!("supervision liveness failed: {monitor}"))
            )
        );
        assert_eq!(raw.actor_id, monitor.actor_id);
    }

    #[test]
    fn python_view_derives_lost_proc_link_from_observation() {
        let monitor = event("keepalive", ActorStatus::generic_failure("link failed"));
        let raw = monitor
            .clone()
            .with_observation(Some(SupervisionObservation::ProcLinkLost(
                SupervisionTarget {
                    actor_id: event("proc", ActorStatus::Idle).actor_id,
                    display_name: Some("proc".to_string()),
                },
            )));
        assert_eq!(
            python_event(raw.clone()),
            event(
                "proc",
                ActorStatus::generic_failure("proc supervision liveness failed")
            )
        );
        assert_eq!(raw.actor_id, monitor.actor_id);
    }

    #[test]
    fn python_view_derives_failed_request_from_observation() {
        let raw = event(
            "supervisor",
            ActorStatus::generic_failure("request rejected"),
        )
        .with_observation(Some(SupervisionObservation::RequestFailed(
            SupervisionTarget {
                actor_id: event("requested", ActorStatus::Idle).actor_id,
                display_name: Some("requested".to_string()),
            },
        )));
        assert_eq!(
            python_event(raw.clone()),
            event(
                "requested",
                ActorStatus::generic_failure("request rejected")
            )
        );
        assert_eq!(raw.display_name.as_deref(), Some("supervisor"));
        assert_eq!(raw.causal_chain().count(), 1);
    }
}
