/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(unsafe_op_in_unsafe_fn)]

use hyperactor_telemetry::sqlite::SqliteTracing;
use opentelemetry::global;
use opentelemetry::metrics;
use pyo3::prelude::*;
use pyo3::types::PyTraceback;

/// Get the current span ID from the active span
#[pyfunction]
pub fn get_current_span_id() -> PyResult<u64> {
    Ok(tracing::Span::current().id().map_or(0, |id| id.into_u64()))
}

/// Log a message with the given metaata
#[pyfunction]
pub fn forward_to_tracing(py: Python, record: Py<PyAny>) -> PyResult<()> {
    let message = record.call_method0(py, "getMessage")?;
    let message: &str = message.extract(py)?;
    let lineno: i64 = record.getattr(py, "lineno")?.extract(py)?;
    let file = record.getattr(py, "filename")?;
    let file: &str = file.extract(py)?;
    let level: i32 = record.getattr(py, "levelno")?.extract(py)?;

    // Extract actor_id from the Python record object if available
    let actor_id = record
        .getattr(py, "actor_id")
        .ok()
        .and_then(|attr| attr.extract::<String>(py).ok());

    // Enter the actor's recording span (if present) so RecorderLayer
    // captures this event in the per-actor flight recorder. The span
    // is entered synchronously for the duration of the tracing emit
    // only — no cross-contamination in shared-asyncio mode.
    //
    // Gracefully falls back to plain tracing when _context is absent,
    // None, or contains an unexpected type.
    let _recording_guard = extract_recording_span(py);

    // Map level number to level name
    match level {
        40 | 50 => {
            let exc = record.getattr(py, "exc_info").ok();
            let traceback = exc
                .and_then(|exc| {
                    if exc.is_none(py) {
                        return None;
                    }
                    exc.extract::<(Py<PyAny>, Py<PyAny>, Bound<'_, PyTraceback>)>(py)
                        .ok()
                })
                .map(|(_, _, tb)| tb.format().unwrap_or_default());
            match traceback {
                Some(traceback) => {
                    tracing::error!(
                        target:"log_events",
                        file = file,
                        lineno = lineno,
                        stacktrace = traceback,
                        actor_id = actor_id.as_deref(),
                        message
                    );
                }
                None => {
                    tracing::error!(
                        file = file,
                        lineno = lineno,
                        actor_id = actor_id.as_deref(),
                        message
                    );
                }
            }
        }
        30 => {
            tracing::warn!(target:"log_events", file = file, lineno = lineno, actor_id = actor_id.as_deref(), message)
        }
        20 => {
            tracing::info!(target:"log_events", file = file, lineno = lineno, actor_id = actor_id.as_deref(), message)
        }
        10 => {
            tracing::debug!(target:"log_events", file = file, lineno = lineno, actor_id = actor_id.as_deref(), message)
        }
        _ => {
            tracing::info!(target:"log_events", file = file, lineno = lineno, actor_id = actor_id.as_deref(), message)
        }
    }
    Ok(())
}

/// Extract the recording span from the current Python actor context.
///
/// Looks up the `_context` ContextVar in
/// `monarch._src.actor.actor_mesh`, downcasts to `PyContext`, and
/// clones the recording span. Returns an entered span guard that
/// routes tracing events to the actor's flight recorder.
///
/// Returns `None` on any failure — missing module, absent context,
/// unexpected type, or no recording span. This function is called
/// from arbitrary Python logging contexts (import-time, client-side,
/// non-actor threads) and must never fail.
fn extract_recording_span(py: Python) -> Option<tracing::span::EnteredSpan> {
    let actor_mesh = py.import("monarch._src.actor.actor_mesh").ok()?;
    let ctx_var = actor_mesh.getattr("_context").ok()?;
    let ctx_obj = ctx_var.call_method1("get", (py.None(),)).ok()?;
    if ctx_obj.is_none() {
        return None;
    }
    let py_ctx = ctx_obj
        .extract::<PyRef<'_, crate::context::PyContext>>()
        .ok()?;
    let span = py_ctx.recording_span()?.clone();
    Some(span.entered())
}

/// Get the current execution ID
#[pyfunction]
pub fn get_execution_id() -> PyResult<String> {
    Ok(hyperactor_telemetry::env::execution_id())
}

#[pyfunction]
pub fn instant_event(message: &str) -> PyResult<()> {
    tracing::info!(message);
    Ok(())
}

// opentelemetry requires that the names of counters etc are static for the lifetime of the program.
// Since we are binding these classes from python to rust, we have to leak these strings in order to
// ensure they live forever. This is fine, as these classes aren't dynamically created.
fn as_static_str(to_leak: &str) -> &'static str {
    String::from(to_leak).leak()
}

#[pyclass(
    subclass,
    module = "monarch._rust_bindings.monarch_hyperactor.telemetry"
)]
struct PyCounter {
    inner: metrics::Counter<u64>,
}

#[pymethods]
impl PyCounter {
    #[new]
    fn new(name: &str) -> Self {
        Self {
            inner: global::meter("monarch")
                .u64_counter(as_static_str(name))
                .build(),
        }
    }

    fn add(&mut self, value: u64, attributes: Option<std::collections::HashMap<String, String>>) {
        let kv_attributes: Vec<opentelemetry::KeyValue> = match attributes {
            Some(attrs) => attrs
                .into_iter()
                .map(|(k, v)| opentelemetry::KeyValue::new(k, v))
                .collect(),
            None => vec![],
        };
        self.inner.add(value, &kv_attributes);
    }
}

#[pyclass(
    subclass,
    module = "monarch._rust_bindings.monarch_hyperactor.telemetry"
)]
struct PyHistogram {
    inner: metrics::Histogram<f64>,
}

#[pymethods]
impl PyHistogram {
    #[new]
    fn new(name: &str) -> Self {
        Self {
            inner: global::meter("monarch")
                .f64_histogram(as_static_str(name))
                .build(),
        }
    }

    fn record(
        &mut self,
        value: f64,
        attributes: Option<std::collections::HashMap<String, String>>,
    ) {
        let kv_attributes: Vec<opentelemetry::KeyValue> = match attributes {
            Some(attrs) => attrs
                .into_iter()
                .map(|(k, v)| opentelemetry::KeyValue::new(k, v))
                .collect(),
            None => vec![],
        };
        self.inner.record(value, &kv_attributes);
    }
}

#[pyclass(
    subclass,
    module = "monarch._rust_bindings.monarch_hyperactor.telemetry"
)]
struct PyUpDownCounter {
    inner: metrics::UpDownCounter<i64>,
}

#[pymethods]
impl PyUpDownCounter {
    #[new]
    fn new(name: &str) -> Self {
        Self {
            inner: global::meter("monarch")
                .i64_up_down_counter(as_static_str(name))
                .build(),
        }
    }

    fn add(&mut self, value: i64, attributes: Option<std::collections::HashMap<String, String>>) {
        let kv_attributes: Vec<opentelemetry::KeyValue> = match attributes {
            Some(attrs) => attrs
                .into_iter()
                .map(|(k, v)| opentelemetry::KeyValue::new(k, v))
                .collect(),
            None => vec![],
        };
        self.inner.add(value, &kv_attributes);
    }
}

#[pyclass(
    unsendable,
    subclass,
    module = "monarch._rust_bindings.monarch_hyperactor.telemetry"
)]
struct PySpan {
    span: tracing::span::EnteredSpan,
}

#[pymethods]
impl PySpan {
    #[new]
    fn new(name: &str, actor_id: Option<&str>) -> Self {
        let span = if let Some(actor_id) = actor_id {
            tracing::span!(
                tracing::Level::INFO,
                "python.span",
                name = name,
                actor_id = actor_id
            )
        } else {
            tracing::span!(tracing::Level::INFO, "python.span", name = name)
        };
        let entered_span = span.entered();

        Self { span: entered_span }
    }

    fn exit(&mut self) {
        self.span = tracing::span::Span::none().entered();
    }
}

#[pyclass(
    subclass,
    module = "monarch._rust_bindings.monarch_hyperactor.telemetry"
)]
struct PySqliteTracing {
    guard: Option<SqliteTracing>,
}

#[pymethods]
impl PySqliteTracing {
    #[new]
    #[pyo3(signature = (in_memory = false))]
    fn new(in_memory: bool) -> PyResult<Self> {
        let guard = if in_memory {
            SqliteTracing::new_in_memory()
        } else {
            SqliteTracing::new()
        };

        match guard {
            Ok(guard) => Ok(Self { guard: Some(guard) }),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create SQLite tracing guard: {}",
                e
            ))),
        }
    }

    fn db_path(&self) -> PyResult<Option<String>> {
        match &self.guard {
            Some(guard) => Ok(guard.db_path().map(|p| p.to_string_lossy().to_string())),
            None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Guard has been closed",
            )),
        }
    }

    fn __enter__(slf: PyRefMut<'_, Self>) -> PyResult<PyRefMut<'_, Self>> {
        Ok(slf)
    }

    fn __exit__(
        &mut self,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        self.guard = None;
        Ok(false) // Don't suppress exceptions
    }

    fn close(&mut self) {
        self.guard = None;
    }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register the forward_to_tracing function
    let f = wrap_pyfunction!(forward_to_tracing, module)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.telemetry",
    )?;
    module.add_function(f)?;

    // Register the span-related functions
    let get_current_span_id_fn = wrap_pyfunction!(get_current_span_id, module)?;
    get_current_span_id_fn.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.telemetry",
    )?;
    module.add_function(get_current_span_id_fn)?;

    let get_execution_id_fn = wrap_pyfunction!(get_execution_id, module)?;
    get_execution_id_fn.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.telemetry",
    )?;
    module.add_function(get_execution_id_fn)?;

    let instant_event_fn = wrap_pyfunction!(instant_event, module)?;
    instant_event_fn.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.telemetry",
    )?;
    module.add_function(instant_event_fn)?;

    module.add_class::<PySpan>()?;
    module.add_class::<PyCounter>()?;
    module.add_class::<PyHistogram>()?;
    module.add_class::<PyUpDownCounter>()?;
    module.add_class::<PySqliteTracing>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use pyo3::ffi::c_str;
    use pyo3::prelude::*;

    use super::*;

    fn init_python() {
        pyo3::Python::initialize();
    }

    /// Helper: create a Python logging.LogRecord with the given message.
    fn make_log_record(py: Python, message: &str) -> Py<PyAny> {
        let locals = pyo3::types::PyDict::new(py);
        locals.set_item("msg", message).unwrap();
        py.run(
            c_str!(
                "import logging\n\
                 record = logging.LogRecord('test', logging.INFO, 'test.py', 1, msg, (), None)"
            ),
            None,
            Some(&locals),
        )
        .unwrap();
        locals.get_item("record").unwrap().unwrap().into()
    }

    /// forward_to_tracing returns Ok when _context is absent.
    /// Must never crash in non-actor logging contexts.
    #[test]
    fn forward_to_tracing_without_context_does_not_crash() {
        init_python();
        Python::attach(|py| {
            let record = make_log_record(py, "no context marker");
            let result = forward_to_tracing(py, record);
            assert!(result.is_ok());
        });
    }

    /// When a recording span is entered on the current thread,
    /// events emitted by forward_to_tracing are captured in the
    /// recording's ring buffer.
    #[test]
    fn forward_to_tracing_captures_when_recording_span_entered() {
        init_python();
        hyperactor_telemetry::initialize_logging_for_test();

        let recording = hyperactor_telemetry::recorder().record(64);
        let span = recording.span();

        Python::attach(|py| {
            let record = make_log_record(py, "recorder marker");
            let _guard = span.enter();
            let result = forward_to_tracing(py, record);
            assert!(result.is_ok());
        });

        let events = recording.tail();
        assert!(
            !events.is_empty(),
            "expected at least one event in recording"
        );
        let last = events.last().unwrap();
        let fields = format!("{:?}", last);
        assert!(
            fields.contains("recorder marker"),
            "expected 'recorder marker' in event fields, got: {fields}"
        );
    }

    /// extract_recording_span returns None when _context is absent.
    #[test]
    fn extract_recording_span_returns_none_without_context() {
        init_python();
        Python::attach(|py| {
            let result = extract_recording_span(py);
            assert!(result.is_none());
        });
    }
}
