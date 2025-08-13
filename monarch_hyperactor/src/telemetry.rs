/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(unsafe_op_in_unsafe_fn)]

use std::cell::Cell;

use hyperactor::clock::ClockKind;
use hyperactor::clock::RealClock;
use hyperactor::clock::SimClock;
use hyperactor_telemetry::opentelemetry;
use hyperactor_telemetry::swap_telemetry_clock;
use opentelemetry::global;
use opentelemetry::metrics;
use pyo3::prelude::*;
use pyo3::types::PyTraceback;
use tracing::span::EnteredSpan;
// Thread local to store the current span
thread_local! {
    static ACTIVE_ACTOR_SPAN: Cell<Option<EnteredSpan>> = const { Cell::new(None) };
}

/// Enter the span stored in the thread local
#[pyfunction]
pub fn enter_span(module_name: String, method_name: String, actor_id: String) -> PyResult<()> {
    let mut maybe_span = ACTIVE_ACTOR_SPAN.take();
    if maybe_span.is_none() {
        maybe_span = Some(
            tracing::info_span!(
                "py_actor_method",
                name = method_name,
                target = module_name,
                actor_id = actor_id
            )
            .entered(),
        );
    }
    ACTIVE_ACTOR_SPAN.set(maybe_span);
    Ok(())
}

/// Exit the span stored in the thread local
#[pyfunction]
pub fn exit_span() -> PyResult<()> {
    ACTIVE_ACTOR_SPAN.replace(None);
    Ok(())
}

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
                        file = file,
                        lineno = lineno,
                        stacktrace = traceback,
                        message
                    );
                }
                None => {
                    tracing::error!(file = file, lineno = lineno, message);
                }
            }
        }
        30 => tracing::warn!(file = file, lineno = lineno, message),
        20 => tracing::info!(file = file, lineno = lineno, message),
        10 => tracing::debug!(file = file, lineno = lineno, message),
        _ => tracing::info!(file = file, lineno = lineno, message),
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

    fn add(&mut self, value: u64) {
        self.inner.add(value, &[]);
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

    fn record(&mut self, value: f64) {
        self.inner.record(value, &[]);
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

    fn add(&mut self, value: i64) {
        self.inner.add(value, &[]);
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
    fn new(name: &str) -> Self {
        let span = tracing::span!(tracing::Level::DEBUG, "python.span", name = name);
        let entered_span = span.entered();

        Self { span: entered_span }
    }

    fn exit(&mut self) {
        self.span = tracing::span::Span::none().entered();
    }
}

/// Add to a counter with the given name and attributes
#[pyfunction]
pub fn add_to_counter(
    name: String,
    value: u64,
    attributes: Option<Vec<(String, String)>>,
) -> PyResult<()> {
    let kv_pairs: Vec<opentelemetry::KeyValue> = attributes
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| opentelemetry::KeyValue::new(k, v))
        .collect();

    println!("Added {} to counter {}", value, name);
    let counter = hyperactor_telemetry::meter("python")
        .u64_counter(name)
        .build();

    counter.add(value, &kv_pairs);
    Ok(())
}

/// Add to an up/down counter with the given name and attributes
#[pyfunction]
pub fn add_to_up_down_counter(
    name: String,
    value: i64,
    attributes: Option<Vec<(String, String)>>,
) -> PyResult<()> {
    let kv_pairs: Vec<opentelemetry::KeyValue> = attributes
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| opentelemetry::KeyValue::new(k, v))
        .collect();

    let counter = hyperactor_telemetry::meter("python")
        .i64_up_down_counter(name)
        .build();

    counter.add(value, &kv_pairs);
    Ok(())
}

/// Record a value to a gauge with the given name and attributes
#[pyfunction]
pub fn add_to_gauge(
    name: String,
    value: f64,
    attributes: Option<Vec<(String, String)>>,
) -> PyResult<()> {
    let kv_pairs: Vec<opentelemetry::KeyValue> = attributes
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| opentelemetry::KeyValue::new(k, v))
        .collect();

    let gauge = hyperactor_telemetry::meter("python")
        .f64_gauge(name)
        .build();

    gauge.record(value, &kv_pairs);
    Ok(())
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
    let enter_span_fn = wrap_pyfunction!(enter_span, module)?;
    enter_span_fn.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.telemetry",
    )?;
    module.add_function(enter_span_fn)?;

    let exit_span_fn = wrap_pyfunction!(exit_span, module)?;
    exit_span_fn.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.telemetry",
    )?;
    module.add_function(exit_span_fn)?;

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

    let add_to_counter_fn = wrap_pyfunction!(add_to_counter, module)?;
    add_to_counter_fn.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.telemetry",
    )?;
    module.add_function(add_to_counter_fn)?;

    let add_to_up_down_counter_fn = wrap_pyfunction!(add_to_up_down_counter, module)?;
    add_to_up_down_counter_fn.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.telemetry",
    )?;
    module.add_function(add_to_up_down_counter_fn)?;

    let add_to_gauge_fn = wrap_pyfunction!(add_to_gauge, module)?;
    add_to_gauge_fn.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_hyperactor.telemetry",
    )?;
    module.add_function(add_to_gauge_fn)?;

    module.add_class::<PySpan>()?;
    module.add_class::<PyCounter>()?;
    module.add_class::<PyHistogram>()?;
    module.add_class::<PyUpDownCounter>()?;
    Ok(())
}
