/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(unsafe_op_in_unsafe_fn)]

use hyperactor::clock::ClockKind;
use hyperactor::clock::RealClock;
use hyperactor::clock::SimClock;
use hyperactor_telemetry::sqlite::SqliteTracing;
use hyperactor_telemetry::swap_telemetry_clock;
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
pub fn forward_to_tracing(py: Python, record: PyObject) -> PyResult<()> {
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

    // Map level number to level name
    match level {
        40 | 50 => {
            let exc = record.getattr(py, "exc_info").ok();
            let traceback = exc
                .and_then(|exc| {
                    if exc.is_none(py) {
                        return None;
                    }
                    exc.extract::<(PyObject, PyObject, Bound<'_, PyTraceback>)>(py)
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

#[pyfunction]
pub fn use_real_clock() -> PyResult<()> {
    swap_telemetry_clock(ClockKind::Real(RealClock));
    Ok(())
}

#[pyfunction]
pub fn use_sim_clock() -> PyResult<()> {
    swap_telemetry_clock(ClockKind::Sim(SimClock));
    Ok(())
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
        _exc_type: Option<PyObject>,
        _exc_value: Option<PyObject>,
        _traceback: Option<PyObject>,
    ) -> PyResult<bool> {
        self.guard = None;
        Ok(false) // Don't suppress exceptions
    }

    fn close(&mut self) {
        self.guard = None;
    }
}

use pyo3::Bound;
use pyo3::types::PyModule;

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

    let use_real_clock_fn = wrap_pyfunction!(use_real_clock, module)?;
    use_real_clock_fn.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.telemetry",
    )?;
    module.add_function(use_real_clock_fn)?;

    let use_sim_clock_fn = wrap_pyfunction!(use_sim_clock, module)?;
    use_sim_clock_fn.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.telemetry",
    )?;
    module.add_function(use_sim_clock_fn)?;

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
