/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Rust-native receiver for remotemount block transfers.
//!
//! Serves parallel hyperactor channels and writes received blocks directly
//! into a caller-provided buffer.

use std::sync::Mutex;

use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelRx;
use hyperactor::channel::Rx;
use pyo3::exceptions::PyRuntimeError;
use pyo3::ffi;
use pyo3::prelude::*;

use crate::tls_sender::BatchTransfer;

/// Rust receiver that writes incoming blocks directly into a caller-provided buffer.
#[pyclass(module = "monarch._rust_bindings.monarch_extension.tls_receiver")]
struct TlsReceiver {
    /// Channel addresses for senders to dial (one per stream).
    data_addrs: Vec<ChannelAddr>,
    /// Receivers consumed by wait(). Mutex<Option<...>> for one-shot semantics.
    receivers: Mutex<Option<Vec<ChannelRx<BatchTransfer>>>>,
}

#[pymethods]
impl TlsReceiver {
    #[new]
    #[pyo3(signature = (num_streams=1))]
    fn new(py: Python<'_>, num_streams: usize) -> PyResult<Self> {
        let transport = hyperactor_mesh::transport::default_transport();

        // channel::serve needs a Tokio runtime context for spawning server tasks.
        monarch_hyperactor::runtime::signal_safe_block_on(py, async move {
            let mut data_addrs = Vec::with_capacity(num_streams);
            let mut receivers = Vec::with_capacity(num_streams);

            for _ in 0..num_streams {
                let (bound_addr, rx) =
                    channel::serve::<BatchTransfer>(ChannelAddr::any(transport.clone()))
                        .map_err(|e| PyRuntimeError::new_err(format!("serve data channel: {e}")))?;
                data_addrs.push(bound_addr);
                receivers.push(rx);
            }

            Ok(TlsReceiver {
                data_addrs,
                receivers: Mutex::new(Some(receivers)),
            })
        })?
    }

    /// Return the data channel addresses as strings for senders to dial.
    #[getter]
    fn data_addrs(&self) -> Vec<String> {
        self.data_addrs.iter().map(|a| a.to_string()).collect()
    }

    /// Receive blocks into `buffer`, blocking until all streams finish.
    fn wait(&self, py: Python<'_>, buffer: &Bound<'_, PyAny>) -> PyResult<()> {
        // Extract raw pointer and length from the Python buffer protocol.
        // SAFETY: Py_buffer is a POD struct; zeroing is valid initialization.
        let mut buf_view: ffi::Py_buffer = unsafe { std::mem::zeroed() };
        // SAFETY: buffer.as_ptr() is a valid Python object; we check rc != 0.
        let rc = unsafe {
            ffi::PyObject_GetBuffer(
                buffer.as_ptr(),
                &mut buf_view,
                ffi::PyBUF_WRITABLE | ffi::PyBUF_SIMPLE,
            )
        };
        if rc != 0 {
            return Err(PyRuntimeError::new_err(
                "buffer is not writable or does not support the buffer protocol",
            ));
        }
        let buf_ptr = buf_view.buf as usize;
        let buf_len = buf_view.len as usize;
        // SAFETY: buf_view was successfully initialized by PyObject_GetBuffer.
        unsafe { ffi::PyBuffer_Release(&mut buf_view) };

        let receivers = self
            .receivers
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("wait() already called"))?;

        monarch_hyperactor::runtime::signal_safe_block_on(py, async move {
            let mut handles = Vec::with_capacity(receivers.len());

            for mut rx in receivers {
                handles.push(tokio::spawn(async move {
                    // Receive batches until empty sentinel or channel close.
                    while let Ok(batch) = rx.recv().await {
                        if batch.blocks.is_empty() {
                            break;
                        }
                        let data = batch.data.to_bytes();
                        let mut pos = 0usize;
                        for &(offset, size) in &batch.blocks {
                            let offset = offset as usize;
                            let size = size as usize;
                            if pos + size > data.len() {
                                return Err(anyhow::anyhow!(
                                    "batch data too short: need {pos}+{size} but \
                                     have {} bytes",
                                    data.len()
                                ));
                            }
                            let end = offset.checked_add(size).ok_or_else(|| {
                                anyhow::anyhow!(
                                    "block offset {offset} + size {size} overflows usize"
                                )
                            })?;
                            if end > buf_len {
                                return Err(anyhow::anyhow!(
                                    "block at offset {offset} size {size} \
                                     exceeds buffer length {buf_len}"
                                ));
                            }
                            // SAFETY: pos + size <= data.len() and
                            // offset + size <= buf_len, both checked above.
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    data.as_ptr().add(pos),
                                    (buf_ptr + offset) as *mut u8,
                                    size,
                                );
                            }
                            pos += size;
                        }
                    }
                    Ok(())
                }));
            }

            for handle in handles {
                handle
                    .await
                    .map_err(|e| anyhow::anyhow!("recv task panicked: {e}"))??;
            }

            Ok::<(), anyhow::Error>(())
        })?
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<TlsReceiver>()?;
    Ok(())
}
