/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::io::IsTerminal;

use anyhow::Result;
use pyo3::Python;
use pyo3::ffi::c_str;
use tracing_subscriber::fmt::format::FmtSpan;

pub fn test_setup() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_thread_ids(true)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(std::io::stderr().is_terminal())
        .with_writer(std::io::stderr)
        .try_init();

    // Redirect NCCL_DEBUG log output to a file so it doesn't clash on stdout.
    // TestX requires stdout to have JSON output on individual lines, and
    // the NCCL output is not JSON. Because it runs in a different thread, it'll
    // race on writing to stdout.
    // Do this regardless of whether NCCL_DEBUG is set or not, because it can
    // be set after this point in the test. If it doesn't get set, NCCL_DEBUG_FILE
    // will be ignored.
    // %h becomes hostname, %p becomes pid.
    let nccl_debug_file = std::env::temp_dir().join("nccl_debug.%h.%p");
    tracing::debug!("Set NCCL_DEBUG_FILE to {:?}", nccl_debug_file);
    // Safety: Can be unsound if there are multiple threads
    // reading and writing the environment.
    unsafe {
        std::env::set_var("NCCL_DEBUG_FILE", nccl_debug_file);
    }
    // NOTE(agallagher): Calling `Python::initialize()` appears to
    // clear `PYTHONPATH` in the env, which we need for test subprocesses
    // to work.  So, manually preserve it.
    let py_path = std::env::var("PYTHONPATH");
    Python::initialize();
    if let Ok(py_path) = py_path {
        // SAFETY: Re-setting env var cleared by `Python::initialize()`.
        unsafe { std::env::set_var("PYTHONPATH", py_path) }
    }

    // We need to load torch to initialize some internal structures used by
    // the FFI funcs we use to convert ivalues to/from py objects.
    Python::attach(|py| py.run(c_str!("import torch"), None, None))?;

    Ok(())
}
