/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Actor-based proc launcher implementation.
//!
//! This module provides [`ActorProcLauncher`], a [`ProcLauncher`]
//! that delegates proc spawning to a Python actor implementing the
//! `ProcLauncher` ABC from `monarch._src.actor.proc_launcher`.
//!
//! This enables custom spawning strategies (Docker, VMs, etc.) while
//! reusing the existing lifecycle management in
//! `BootstrapProcManager`.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hyperactor::ActorHandle;
use hyperactor::Instance;
use hyperactor::Mailbox;
use hyperactor::ProcId;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor_mesh::proc_launcher::LaunchOptions;
use hyperactor_mesh::proc_launcher::LaunchResult;
use hyperactor_mesh::proc_launcher::ProcExitKind;
use hyperactor_mesh::proc_launcher::ProcExitResult;
use hyperactor_mesh::proc_launcher::ProcLauncher;
use hyperactor_mesh::proc_launcher::ProcLauncherError;
use hyperactor_mesh::proc_launcher::StdioHandling;
use pyo3::prelude::*;
use tokio::sync::Mutex;
use tokio::sync::oneshot;

use crate::actor::MethodSpecifier;
use crate::actor::PythonActor;
use crate::actor::PythonMessage;
use crate::actor::PythonMessageKind;
use crate::mailbox::EitherPortRef;
use crate::mailbox::PythonOncePortRef;

/// Python / PyO3 helpers used by the actor-based proc launcher.
///
/// This module contains small utilities that:
/// - format Python objects for diagnostics, and
/// - perform common imports under the GIL with consistent error
///   mapping.
mod py {
    use hyperactor_mesh::proc_launcher::ProcLauncherError;
    use pyo3::prelude::*;

    /// Format a Python object for inclusion in error messages.
    ///
    /// Prefers `str(obj)` (more user-friendly for exceptions), and
    /// falls back to `repr(obj)` if `str()` fails. If both fail,
    /// returns a fixed placeholder.
    pub(super) fn pyany_to_error_string(obj: &Bound<'_, PyAny>) -> String {
        obj.str()
            .map(|s| s.to_string())
            .or_else(|_| obj.repr().map(|r| r.to_string()))
            .unwrap_or_else(|_| "<error formatting exception>".into())
    }

    /// Import the `cloudpickle` module under the GIL.
    ///
    /// Errors are mapped into `ProcLauncherError` with context so
    /// callers can report import failures as protocol/interop errors.
    pub(super) fn import_cloudpickle(
        py: Python<'_>,
    ) -> Result<Bound<'_, pyo3::types::PyModule>, ProcLauncherError> {
        py.import("cloudpickle")
            .map_err(|e| ProcLauncherError::Other(format!("import cloudpickle: {e}")))
    }
}

/// Decoding of exit results returned by the Python proc spawner.
///
/// The spawner replies on an explicit exit port with a pickled
/// payload. That payload must be a `ProcExitResult` dataclass
/// (from `monarch._src.actor.proc_launcher`) with the standard
/// exit-reporting attributes.
///
/// Decoding is *strict*: all required attributes must be present
/// with correct types.
mod decode {
    use hyperactor_mesh::proc_launcher::ProcExitKind;
    use hyperactor_mesh::proc_launcher::ProcExitResult;
    use hyperactor_mesh::proc_launcher::ProcLauncherError;
    use pyo3::prelude::*;

    /// Field names for the `ProcExitResult` dataclass attributes.
    const K_EXIT_CODE: &str = "exit_code";
    const K_SIGNAL: &str = "signal";
    const K_CORE_DUMPED: &str = "core_dumped";
    const K_FAILED_REASON: &str = "failed_reason";
    const K_STDERR_TAIL: &str = "stderr_tail";

    /// Required attributes for a valid `ProcExitResult` dataclass.
    const REQUIRED_ATTRS: [&str; 5] = [
        K_EXIT_CODE,
        K_SIGNAL,
        K_CORE_DUMPED,
        K_FAILED_REASON,
        K_STDERR_TAIL,
    ];

    /// Intermediate representation of exit information decoded from
    /// the Python spawner.
    ///
    /// This struct separates:
    /// (1) parsing/validation of a Python dataclass payload, from
    /// (2) policy decisions when mapping into [`ProcExitResult`] /
    ///     [`ProcExitKind`].
    #[derive(Debug)]
    pub(super) struct DecodedExit {
        /// Process exit code, if known.
        pub exit_code: Option<i32>,
        /// Terminating signal number, if the process was killed by a signal.
        pub signal: Option<i32>,
        /// Whether a core dump was produced for a signaled termination.
        pub core_dumped: bool,
        /// Failure reason reported by the spawner.
        pub failed_reason: Option<String>,
        /// Trailing stderr lines captured by the spawner.
        pub stderr_tail: Vec<String>,
    }

    /// Validate that the object has all required attributes.
    fn validate_shape(obj: &Bound<'_, PyAny>) -> Result<(), ProcLauncherError> {
        for k in REQUIRED_ATTRS {
            if !obj
                .hasattr(k)
                .map_err(|e| ProcLauncherError::Other(format!("hasattr {k}: {e}")))?
            {
                return Err(ProcLauncherError::Other(format!(
                    "ProcExitResult must be a ProcExitResult dataclass; missing attribute {k}"
                )));
            }
        }
        Ok(())
    }

    /// Extract fields from a validated `ProcExitResult` dataclass.
    fn extract_fields(obj: &Bound<'_, PyAny>) -> Result<DecodedExit, ProcLauncherError> {
        let exit_code = obj
            .getattr(K_EXIT_CODE)
            .map_err(|e| ProcLauncherError::Other(format!("getattr {K_EXIT_CODE}: {e}")))?
            .extract::<Option<i32>>()
            .map_err(|e| ProcLauncherError::Other(format!("extract {K_EXIT_CODE}: {e}")))?;

        let signal = obj
            .getattr(K_SIGNAL)
            .map_err(|e| ProcLauncherError::Other(format!("getattr {K_SIGNAL}: {e}")))?
            .extract::<Option<i32>>()
            .map_err(|e| ProcLauncherError::Other(format!("extract {K_SIGNAL}: {e}")))?;

        let core_dumped = obj
            .getattr(K_CORE_DUMPED)
            .map_err(|e| ProcLauncherError::Other(format!("getattr {K_CORE_DUMPED}: {e}")))?
            .extract::<bool>()
            .map_err(|e| ProcLauncherError::Other(format!("extract {K_CORE_DUMPED}: {e}")))?;

        let failed_reason = obj
            .getattr(K_FAILED_REASON)
            .map_err(|e| ProcLauncherError::Other(format!("getattr {K_FAILED_REASON}: {e}")))?
            .extract::<Option<String>>()
            .map_err(|e| ProcLauncherError::Other(format!("extract {K_FAILED_REASON}: {e}")))?;

        let stderr_tail = obj
            .getattr(K_STDERR_TAIL)
            .map_err(|e| ProcLauncherError::Other(format!("getattr {K_STDERR_TAIL}: {e}")))?
            .extract::<Vec<String>>()
            .map_err(|e| ProcLauncherError::Other(format!("extract {K_STDERR_TAIL}: {e}")))?;

        Ok(DecodedExit {
            exit_code,
            signal,
            core_dumped,
            failed_reason,
            stderr_tail,
        })
    }

    /// Decode exit data from a `ProcExitResult` dataclass.
    ///
    /// Decoding is *strict*:
    /// - All required attributes must exist (missing attributes are protocol errors).
    /// - Attribute values must have the expected types (or be `None`
    ///   for optional fields).
    pub(super) fn decode_exit_obj(
        obj: &Bound<'_, PyAny>,
    ) -> Result<DecodedExit, ProcLauncherError> {
        validate_shape(obj)?;
        extract_fields(obj)
    }

    /// Convert a decoded Python exit payload into a
    /// [`ProcExitResult`].
    ///
    /// Mapping rules:
    /// - If `failed_reason` is set, the proc is treated as
    ///   [`ProcExitKind::Failed`].
    /// - Else if `signal` is set, the proc is treated as
    ///   [`ProcExitKind::Signaled`] (propagating `core_dumped`).
    /// - Else the proc is treated as [`ProcExitKind::Exited`]; if
    ///   `exit_code` is missing, `-1` is used as a sentinel for
    ///   "unknown".
    ///
    /// `stderr_tail` is always populated from the decoded payload
    /// (possibly empty).
    fn decoded_to_exit_result(d: DecodedExit) -> ProcExitResult {
        let kind = if let Some(reason) = d.failed_reason {
            ProcExitKind::Failed { reason }
        } else if let Some(sig) = d.signal {
            ProcExitKind::Signaled {
                signal: sig,
                core_dumped: d.core_dumped,
            }
        } else {
            // -1 is a sentinel for "exit_code missing/unknown"
            ProcExitKind::Exited {
                code: d.exit_code.unwrap_or(-1),
            }
        };

        ProcExitResult {
            kind,
            stderr_tail: Some(d.stderr_tail),
        }
    }

    /// Map a Python exception value into a failed [`ProcExitResult`].
    ///
    /// This is used when the spawner reports an exception (rather than a
    /// normal exit payload). The exception is formatted using
    /// [`super::py::pyany_to_error_string`] and embedded in
    /// [`ProcExitKind::Failed`]. No stderr tail is attached because the
    /// failure originated in the spawner logic rather than the launched
    /// process.
    fn exception_to_exit_result(err_obj: &Bound<'_, PyAny>) -> ProcExitResult {
        let reason = format!(
            "spawner raised: {}",
            super::py::pyany_to_error_string(err_obj)
        );
        ProcExitResult {
            kind: ProcExitKind::Failed { reason },
            stderr_tail: None,
        }
    }

    /// Convert a spawner response [`PythonMessage`] into a
    /// [`ProcExitResult`].
    ///
    /// The spawner replies on the exit port with a pickled payload:
    /// - [`PythonMessageKind::Result`]: a pickled `ProcExitResult`-shaped
    ///   object (dataclass), which is decoded via [`decode_exit_obj`]
    ///   and mapped with [`decoded_to_exit_result`].
    /// - [`PythonMessageKind::Exception`]: a pickled exception object,
    ///   which is mapped to [`ProcExitKind::Failed`] via
    ///   [`exception_to_exit_result`].
    ///
    /// Messages carrying a pending pickle state are rejected (exit
    /// results must be fully materialized), and any unexpected message
    /// kind is treated as a protocol error.
    pub(super) fn convert_py_exit_result(
        msg: crate::actor::PythonMessage,
    ) -> Result<ProcExitResult, ProcLauncherError> {
        use crate::actor::PythonMessageKind;

        Python::attach(|py| {
            let cloudpickle = super::py::import_cloudpickle(py)?;

            match msg.kind {
                PythonMessageKind::Result { .. } => {
                    let obj = cloudpickle
                        .call_method1("loads", (msg.message.to_bytes().as_ref(),))
                        .map_err(|e| ProcLauncherError::Other(format!("cloudpickle.loads: {e}")))?;
                    let decoded = decode_exit_obj(&obj)?;
                    Ok(decoded_to_exit_result(decoded))
                }
                PythonMessageKind::Exception { .. } => {
                    let err_obj = cloudpickle
                        .call_method1("loads", (msg.message.to_bytes().as_ref(),))
                        .map_err(|e| {
                            ProcLauncherError::Other(format!("cloudpickle.loads exception: {e}"))
                        })?;
                    Ok(exception_to_exit_result(&err_obj))
                }
                _ => Err(ProcLauncherError::Other(
                    "unexpected message kind in exit result".into(),
                )),
            }
        })
    }

    #[cfg(test)]
    mod tests {
        use std::ffi::CStr;

        use super::*;

        // --
        // Pure Rust tests for decoded_to_exit_result

        // If `failed_reason` is present, it takes priority over
        // signal/exit_code and the stderr tail is preserved.
        #[test]
        fn test_decoded_to_exit_result_failed_reason() {
            let decoded = DecodedExit {
                exit_code: Some(1),
                signal: Some(9),
                core_dumped: true,
                failed_reason: Some("spawn failed".into()),
                stderr_tail: vec!["error line".into()],
            };
            let result = decoded_to_exit_result(decoded);
            // failed_reason takes priority over everything else
            assert!(matches!(
                result.kind,
                ProcExitKind::Failed { reason } if reason == "spawn failed"
            ));
            assert_eq!(result.stderr_tail, Some(vec!["error line".into()]));
        }

        // If `failed_reason` is absent but a signal is present, we
        // produce a `Signaled` exit result (including the
        // core_dumped bit).
        #[test]
        fn test_decoded_to_exit_result_signal() {
            let decoded = DecodedExit {
                exit_code: Some(128 + 9),
                signal: Some(9),
                core_dumped: true,
                failed_reason: None,
                stderr_tail: vec![],
            };
            let result = decoded_to_exit_result(decoded);
            assert!(matches!(
                result.kind,
                ProcExitKind::Signaled {
                    signal: 9,
                    core_dumped: true
                }
            ));
        }

        // If neither `failed_reason` nor `signal` is present, we
        // produce an `Exited` result using the provided exit code
        // and preserve stderr tail.
        #[test]
        fn test_decoded_to_exit_result_exit_code() {
            let decoded = DecodedExit {
                exit_code: Some(42),
                signal: None,
                core_dumped: false,
                failed_reason: None,
                stderr_tail: vec!["line1".into(), "line2".into()],
            };
            let result = decoded_to_exit_result(decoded);
            assert!(matches!(result.kind, ProcExitKind::Exited { code: 42 }));
            assert_eq!(
                result.stderr_tail,
                Some(vec!["line1".into(), "line2".into()])
            );
        }

        // If no `failed_reason`, no `signal`, and `exit_code` is
        // missing, we use the sentinel `-1` to mean "unknown exit
        // code".
        #[test]
        fn test_decoded_to_exit_result_missing_exit_code_sentinel() {
            // When no failed_reason, no signal, and no exit_code, we
            // use -1 sentinel
            let decoded = DecodedExit {
                exit_code: None,
                signal: None,
                core_dumped: false,
                failed_reason: None,
                stderr_tail: vec![],
            };
            let result = decoded_to_exit_result(decoded);
            assert!(matches!(result.kind, ProcExitKind::Exited { code: -1 }));
        }

        // --
        // GIL-based tests for validate_shape and decode_exit_obj

        // Helper: Run a small Python snippet and return its locals
        // dict.
        //
        // The snippet should assign any values it wants to assert on
        // into `locals`, e.g. `obj = FakeExit()`, so Rust can pull
        // them out by name.
        fn run_py_code<'py>(py: Python<'py>, code: &CStr) -> Bound<'py, pyo3::types::PyDict> {
            let locals = pyo3::types::PyDict::new(py);
            py.run(code, None, Some(&locals)).unwrap();
            locals
        }

        // A Python object with all required attributes should pass
        // shape validation.
        #[test]
        fn test_validate_shape_valid_dataclass() {
            Python::initialize();
            Python::attach(|py| {
                // Create a simple class with all required attributes
                let locals = run_py_code(
                    py,
                    c"
class FakeExit:
    exit_code = 0
    signal = None
    core_dumped = False
    failed_reason = None
    stderr_tail = []
obj = FakeExit()
",
                );
                let obj = locals.get_item("obj").unwrap().unwrap();
                assert!(validate_shape(&obj).is_ok());
            });
        }

        // Missing required attributes should be rejected, and the
        // error should mention which attribute is missing to aid
        // debugging.
        #[test]
        fn test_validate_shape_missing_attribute() {
            Python::initialize();
            Python::attach(|py| {
                // Missing stderr_tail
                let locals = run_py_code(
                    py,
                    c"
class IncompleteExit:
    exit_code = 0
    signal = None
    core_dumped = False
    failed_reason = None
obj = IncompleteExit()
",
                );
                let obj = locals.get_item("obj").unwrap().unwrap();
                let err = validate_shape(&obj).unwrap_err();
                assert!(
                    err.to_string().contains("stderr_tail"),
                    "error should mention missing attribute: {err}"
                );
            });
        }

        // A well-formed Python exit object should decode into a
        // `DecodedExit` with the expected field values.
        #[test]
        fn test_decode_exit_obj_valid() {
            Python::initialize();
            Python::attach(|py| {
                let locals = run_py_code(
                    py,
                    c"
class FakeExit:
    exit_code = 42
    signal = None
    core_dumped = False
    failed_reason = None
    stderr_tail = ['line1', 'line2']
obj = FakeExit()
",
                );
                let obj = locals.get_item("obj").unwrap().unwrap();
                let decoded = decode_exit_obj(&obj).unwrap();
                assert_eq!(decoded.exit_code, Some(42));
                assert_eq!(decoded.signal, None);
                assert!(!decoded.core_dumped);
                assert_eq!(decoded.failed_reason, None);
                assert_eq!(decoded.stderr_tail, vec!["line1", "line2"]);
            });
        }

        // Type mismatches in the Python payload should fail
        // decoding, and the error should mention the field that
        // could not be extracted.
        #[test]
        fn test_decode_exit_obj_wrong_type() {
            Python::initialize();
            Python::attach(|py| {
                // exit_code is a string instead of int
                let locals = run_py_code(
                    py,
                    c"
class BadExit:
    exit_code = 'not an int'
    signal = None
    core_dumped = False
    failed_reason = None
    stderr_tail = []
obj = BadExit()
",
                );
                let obj = locals.get_item("obj").unwrap().unwrap();
                let err = decode_exit_obj(&obj).unwrap_err();
                assert!(
                    err.to_string().contains("exit_code"),
                    "error should mention field: {err}"
                );
            });
        }
    }
}

use decode::convert_py_exit_result;
use py::import_cloudpickle;

/// A [`ProcLauncher`] implemented by delegating proc lifecycle
/// operations to a Python actor.
///
/// The `spawner` actor must implement the `ProcLauncher` ABC from
/// `monarch._src.actor.proc_launcher`, and is responsible for
/// actually spawning and controlling OS processes (Docker, VMs,
/// etc.). Rust retains the *lifecycle wiring* expected by
/// [`BootstrapProcManager`]: it initiates launch/terminate/kill
/// requests and exposes an [`oneshot::Receiver`] (`exit_rx`) that
/// resolves when the spawner reports exit.
///
/// ## Semantics
///
/// - **PID is optional**: the Python spawner may not expose a real
///   PID, so [`LaunchResult::pid`] is `None`.
/// - **Exit reporting is required**: the spawner must send exactly
///   one exit result on the provided exit port. If the port is closed
///   or the payload cannot be decoded, the receiver resolves to a
///   [`ProcExitKind::Failed`] result.
/// - **Termination is best-effort**: `terminate` and `kill` are
///   forwarded to the spawner; success only means the request was
///   delivered.
///
/// ## Context requirement
///
/// [`ProcLauncher`] methods don't take a context parameter, but
/// sending actor messages does. This launcher stores an
/// [`Instance<()>`] ("client-only" actor) to use as the send context.
/// The instance is created via [`Proc::instance()`] and must remain
/// valid for the lifetime of the launcher.
#[derive(Debug)]
pub struct ActorProcLauncher {
    /// Handle to the Python spawner actor that implements the
    /// ProcLauncher ABC.
    spawner: ActorHandle<PythonActor>,

    /// Mailbox used to allocate the one-shot exit port per launched
    /// proc.
    mailbox: Mailbox,

    /// Client-only actor instance used as the send context for all
    /// messages to `spawner`.
    ///
    /// Created via `Proc::instance()`. The `()` type indicates this
    /// is not a real actor—just a sending context. Must outlive the
    /// launcher.
    instance: Instance<()>,

    /// Debug-only tracking of procs launched via this instance.
    ///
    /// Not used for correctness; used for diagnostics and sanity
    /// checks.
    active_procs: Arc<Mutex<HashSet<ProcId>>>,
}

impl ActorProcLauncher {
    /// Create a new actor-based proc launcher.
    ///
    /// # Arguments
    ///
    /// * `spawner` - Handle to the Python actor implementing the
    ///   `ProcLauncher` ABC.
    /// * `mailbox` - Mailbox used to create one-shot exit ports.
    /// * `instance` - Send context for `ActorHandle::send` (typically
    ///   from `Proc::instance()`). Any valid instance granting send
    ///   capability is sufficient; it need not be
    ///   `Instance<PythonActor>`. Must remain valid for the
    ///   launcher's lifetime.
    pub fn new(
        spawner: ActorHandle<PythonActor>,
        mailbox: Mailbox,
        instance: Instance<()>,
    ) -> Self {
        Self {
            spawner,
            mailbox,
            instance,
            active_procs: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

#[async_trait]
impl ProcLauncher for ActorProcLauncher {
    /// Spawn a proc by delegating to the Python spawner actor.
    ///
    /// This method:
    /// 1) opens a one-shot mailbox port used for the spawner's exit
    ///    notification,
    /// 2) serializes `(proc_id, LaunchOptions)` with `cloudpickle`,
    /// 3) sends a `CallMethod { launch, ExplicitPort(..) }` message
    ///    to the spawner,
    /// 4) returns immediately with a [`LaunchResult`] whose `exit_rx`
    ///    completes once the spawner reports process termination (or the
    ///    port closes).
    ///
    /// ## Notes
    /// - `pid` is always `None`: the Rust side does not assume an OS
    ///   PID exists.
    /// - Exit is observed asynchronously via `exit_rx`;
    ///   termination/kill are best-effort requests to the spawner actor
    ///   rather than direct OS signals.
    /// - If decoding the exit payload fails, the returned `exit_rx`
    ///   resolves to `ProcExitKind::Failed` with a decode error reason.
    async fn launch(
        &self,
        proc_id: &ProcId,
        opts: LaunchOptions,
    ) -> Result<LaunchResult, ProcLauncherError> {
        let (exit_port, exit_port_rx) = self.mailbox.open_once_port::<PythonMessage>();

        let pickled_args = Python::attach(|py| -> Result<Vec<u8>, ProcLauncherError> {
            let cloudpickle = import_cloudpickle(py)?;

            let mod_ = py
                .import("monarch._src.actor.proc_launcher")
                .map_err(|e| ProcLauncherError::Other(format!("import proc_launcher: {e}")))?;
            let launch_opts_cls = mod_
                .getattr("LaunchOptions")
                .map_err(|e| ProcLauncherError::Other(format!("getattr LaunchOptions: {e}")))?;

            let program = opts.command.program.to_str().ok_or_else(|| {
                ProcLauncherError::Other("program path is not valid UTF-8".into())
            })?;

            let env = pyo3::types::PyDict::new(py);
            for (k, v) in &opts.command.env {
                env.set_item(k, v)
                    .map_err(|e| ProcLauncherError::Other(format!("set env item: {e}")))?;
            }

            let py_opts = launch_opts_cls
                .call1((
                    &opts.bootstrap_payload,
                    &opts.process_name,
                    program,
                    opts.command.arg0.as_deref(),
                    &opts.command.args,
                    env,
                    opts.want_stdio,
                    opts.tail_lines,
                    opts.log_channel.as_ref().map(|a| a.to_string()),
                ))
                .map_err(|e| ProcLauncherError::Other(format!("construct LaunchOptions: {e}")))?;

            let args = (proc_id.to_string(), py_opts);
            let kwargs = pyo3::types::PyDict::new(py);
            let pickled = cloudpickle
                .call_method1("dumps", ((args, kwargs),))
                .map_err(|e| ProcLauncherError::Other(format!("cloudpickle: {e}")))?;

            pickled
                .extract::<Vec<u8>>()
                .map_err(|e| ProcLauncherError::Other(format!("extract bytes: {e}")))
        })?;

        let bound_port = exit_port.bind();
        let message = PythonMessage {
            kind: PythonMessageKind::CallMethod {
                name: MethodSpecifier::ExplicitPort {
                    name: "launch".into(),
                },
                response_port: Some(EitherPortRef::Once(PythonOncePortRef::from(bound_port))),
            },
            message: pickled_args.into(),
        };

        self.spawner
            .send(&self.instance, message)
            .map_err(|e| ProcLauncherError::Other(format!("send to spawner failed: {e}")))?;

        self.active_procs.lock().await.insert(proc_id.clone());

        let (exit_tx, exit_rx) = oneshot::channel();
        let active_procs = Arc::clone(&self.active_procs);
        let proc_id_clone = proc_id.clone();

        tokio::spawn(async move {
            let result = match exit_port_rx.recv().await {
                Ok(py_message) => {
                    convert_py_exit_result(py_message).unwrap_or_else(|e| ProcExitResult {
                        kind: ProcExitKind::Failed {
                            reason: format!("failed to decode exit result: {e}"),
                        },
                        stderr_tail: None,
                    })
                }
                Err(_) => ProcExitResult {
                    kind: ProcExitKind::Failed {
                        reason: "exit port closed (spawner crashed or forgot to send)".into(),
                    },
                    stderr_tail: None,
                },
            };
            active_procs.lock().await.remove(&proc_id_clone);
            let _ = exit_tx.send(result);
        });

        Ok(LaunchResult {
            pid: None,
            started_at: RealClock.system_time_now(),
            stdio: StdioHandling::ManagedByLauncher,
            exit_rx,
        })
    }

    /// Request graceful termination of a proc, with a best-effort
    /// timeout.
    ///
    /// This delegates to the Python spawner actor's
    /// `terminate(proc_id, timeout_secs)` method. The request is sent
    /// fire-and-forget: we do not wait for an acknowledgment, and
    /// there is no guarantee the proc will actually exit within
    /// `timeout`.
    ///
    /// ## Errors
    ///
    /// Returns `ProcLauncherError::Terminate` if we fail to:
    /// - import/serialize the request via `cloudpickle`, or
    /// - send the message to the spawner actor.
    async fn terminate(
        &self,
        proc_id: &ProcId,
        timeout: Duration,
    ) -> Result<(), ProcLauncherError> {
        let pickled = Python::attach(|py| -> Result<Vec<u8>, ProcLauncherError> {
            let cloudpickle =
                import_cloudpickle(py).map_err(|e| ProcLauncherError::Terminate(format!("{e}")))?;
            let args = (proc_id.to_string(), timeout.as_secs_f64());
            let kwargs = pyo3::types::PyDict::new(py);
            cloudpickle
                .call_method1("dumps", ((args, kwargs),))
                .map_err(|e| ProcLauncherError::Terminate(format!("cloudpickle: {e}")))?
                .extract()
                .map_err(|e| ProcLauncherError::Terminate(format!("extract: {e}")))
        })?;

        let message = PythonMessage {
            kind: PythonMessageKind::CallMethod {
                name: MethodSpecifier::ReturnsResponse {
                    name: "terminate".into(),
                },
                response_port: None,
            },
            message: pickled.into(),
        };

        self.spawner
            .send(&self.instance, message)
            .map_err(|e| ProcLauncherError::Terminate(format!("send failed: {e}")))
    }

    /// Forcefully kill a proc.
    ///
    /// This delegates to the Python spawner actor's `kill(proc_id)`
    /// method. Like `terminate`, this is best-effort and
    /// fire-and-forget: success here means the request was serialized
    /// and delivered to the spawner actor, not that the process is
    /// already dead.
    ///
    /// ## Errors
    ///
    /// Returns `ProcLauncherError::Kill` if we fail to:
    /// - import/serialize the request via `cloudpickle`, or
    /// - send the message to the spawner actor.
    async fn kill(&self, proc_id: &ProcId) -> Result<(), ProcLauncherError> {
        let pickled = Python::attach(|py| -> Result<Vec<u8>, ProcLauncherError> {
            let cloudpickle =
                import_cloudpickle(py).map_err(|e| ProcLauncherError::Kill(format!("{e}")))?;
            let args = (proc_id.to_string(),);
            let kwargs = pyo3::types::PyDict::new(py);
            cloudpickle
                .call_method1("dumps", ((args, kwargs),))
                .map_err(|e| ProcLauncherError::Kill(format!("cloudpickle: {e}")))?
                .extract()
                .map_err(|e| ProcLauncherError::Kill(format!("extract: {e}")))
        })?;

        let message = PythonMessage {
            kind: PythonMessageKind::CallMethod {
                name: MethodSpecifier::ReturnsResponse {
                    name: "kill".into(),
                },
                response_port: None,
            },
            message: pickled.into(),
        };

        self.spawner
            .send(&self.instance, message)
            .map_err(|e| ProcLauncherError::Kill(format!("send failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Verifies that `pyany_to_error_string` formats Python objects
    // using `str()` (falling back to `repr()`).
    #[test]
    fn test_pyany_to_error_string() {
        Python::initialize();
        Python::attach(|py| {
            // A Python string should round-trip through `str()`
            // unchanged.
            let s = pyo3::types::PyString::new(py, "hello");
            assert_eq!(py::pyany_to_error_string(s.as_any()), "hello");

            // Non-strings should format via `str()` when possible.
            let i = 42i32.into_pyobject(py).unwrap();
            assert_eq!(py::pyany_to_error_string(i.as_any()), "42");
        });
    }
}
