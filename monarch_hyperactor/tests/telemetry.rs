/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Integration test for the Python actor-context bridge used by
//! `forward_to_tracing`.
//!
//! Verifies that when `_context` holds a real `PyContext`, the
//! emitted tracing event is captured in that actor's flight recorder.
//!
//! Requires the real `monarch` Python package via `py_deps` on
//! `test_monarch_hyperactor`.

use anyhow::Result;
use monarch_hyperactor::context::PyContext;
use monarch_hyperactor::runtime::monarch_with_gil_blocking;
use pyo3::ffi::c_str;
use pyo3::prelude::*;

/// Verifies that `forward_to_tracing` uses the current `PyContext`'s
/// recording span, causing the emitted event to be captured in the
/// actor's flight recorder.
///
/// Exercises the `_context -> PyContext -> recording_span` bridge.
#[tokio::test]
#[cfg_attr(not(fbcode_build), ignore)]
async fn forward_to_tracing_captures_via_extract_recording_span() -> Result<()> {
    pyo3::Python::initialize();
    hyperactor_telemetry::initialize_logging_for_test();

    monarch_with_gil_blocking(|py| py.run(c_str!("import monarch._rust_bindings"), None, None))?;

    let recording = hyperactor_telemetry::recorder().record(64);
    let span = recording.span();

    monarch_with_gil_blocking(|py| -> PyResult<()> {
        // Build a PyContext carrying our recording span.
        let py_ctx = PyContext::for_test(py, Some(span))?;
        let py_ctx_obj = Py::new(py, py_ctx)?;

        // Set _context ContextVar to our test context.
        let actor_mesh = py.import("monarch._src.actor.actor_mesh")?;
        let ctx_var = actor_mesh.getattr("_context")?;
        let token = ctx_var.call_method1("set", (py_ctx_obj,))?;

        // Call forward_to_tracing through Python — exercises the
        // full extract_recording_span path.
        let locals = pyo3::types::PyDict::new(py);
        py.run(
            c_str!(
                "import logging
from monarch._rust_bindings.monarch_hyperactor.telemetry import forward_to_tracing
record = logging.LogRecord('test', logging.INFO, 'test.py', 1, 'extract marker', (), None)
forward_to_tracing(record)"
            ),
            None,
            Some(&locals),
        )?;

        // Restore _context.
        ctx_var.call_method1("reset", (token,))?;
        Ok(())
    })?;

    let events = recording.tail();
    assert!(
        !events.is_empty(),
        "expected at least one event in recording via extract_recording_span path"
    );
    let last = events.last().unwrap();
    let fields = format!("{:?}", last);
    assert!(
        fields.contains("extract marker"),
        "expected 'extract marker' in event fields, got: {fields}"
    );
    Ok(())
}
