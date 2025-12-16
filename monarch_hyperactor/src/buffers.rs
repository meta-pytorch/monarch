/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#![allow(unsafe_op_in_unsafe_fn)]

use std::ffi::c_int;
use std::ffi::c_void;

use bytes::Buf;
use hyperactor::Named;
use pyo3::buffer::PyBuffer;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyBytesMethods;
use serde::Deserialize;
use serde::Serialize;
use serde_multipart::Part;

/// Wrapper that keeps Py<PyBytes> alive while allowing zero-copy access to its memory
struct KeepPyBytesAlive {
    _py_bytes: Py<PyBytes>,
    ptr: *const u8,
    len: usize,
}

impl KeepPyBytesAlive {
    fn new(py_bytes: Py<PyBytes>) -> Self {
        let (ptr, len) = Python::with_gil(|py| {
            let bytes_ref = py_bytes.as_bytes(py);
            (bytes_ref.as_ptr(), bytes_ref.len())
        });
        Self {
            _py_bytes: py_bytes,
            ptr,
            len,
        }
    }
}

impl AsRef<[u8]> for KeepPyBytesAlive {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: ptr is valid as long as py_bytes is alive (kept alive by Py<PyBytes>)
        // Python won't free the memory until the Py<PyBytes> refcount reaches 0
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

// SAFETY: Py<PyBytes> is Send/Sync for immutable bytes
unsafe impl Send for KeepPyBytesAlive {}
// SAFETY: Py<PyBytes> is Send/Sync for immutable bytes
unsafe impl Sync for KeepPyBytesAlive {}

/// A mutable buffer for reading and writing bytes data.
///
/// The `Buffer` struct provides an interface for accumulating byte data from Python `bytes` objects
/// that can be converted into a `Part` for zero-copy multipart message serialization.
/// It accumulates references to Python bytes objects without copying.
///
/// # Examples
///
/// ```python
/// from monarch._rust_bindings.monarch_hyperactor.buffers import Buffer
///
/// # Create a new buffer
/// buffer = Buffer()
///
/// # Write some data
/// data = b"Hello, World!"
/// bytes_written = buffer.write(data)
///
/// # Use in multipart serialization
/// # The buffer accumulates multiple writes as separate fragments
/// ```
#[pyclass(subclass, module = "monarch._rust_bindings.monarch_hyperactor.buffers")]
#[derive(Clone, Default)]
pub struct Buffer {
    inner: Vec<Py<PyBytes>>,
}

#[pymethods]
impl Buffer {
    /// Creates a new empty buffer with specified initial capacity.
    ///
    ///
    /// # Returns
    /// A new empty `Buffer` instance with the specified capacity.
    #[new]
    fn new() -> Self {
        Self { inner: Vec::new() }
    }

    /// Writes bytes data to the buffer.
    ///
    /// This keeps a reference to the Python bytes object without copying.
    ///
    /// # Arguments
    /// * `buff` - The bytes object to write to the buffer
    ///
    /// # Returns
    /// The number of bytes written (always equal to the length of input bytes)
    fn write<'py>(&mut self, buff: &Bound<'py, PyBytes>) -> usize {
        let bytes_written = buff.as_bytes().len();
        self.inner.push(buff.clone().unbind());
        bytes_written
    }

    /// Returns the total number of bytes in the buffer.
    ///
    /// This iterates over all accumulated PyBytes fragments and sums their lengths.
    ///
    /// # Returns
    /// The total number of bytes stored in the buffer
    fn __len__(&self) -> usize {
        Python::with_gil(|py| self.inner.iter().map(|b| b.as_bytes(py).len()).sum())
    }

    /// Freezes the buffer, converting it into an immutable `FrozenBuffer` for reading.
    ///
    /// This consumes all accumulated PyBytes and converts them into a contiguous bytes buffer.
    /// After freezing, the original buffer is cleared.
    ///
    /// This operation should avoided in hot paths as it creates a copy in order to concatenate
    /// bytes that are fragmented in memory into a single series of contiguous bytes
    ///
    /// # Returns
    /// A new `FrozenBuffer` containing all the bytes that were written to this buffer
    fn freeze(&mut self) -> FrozenBuffer {
        let part = self.take_part();
        FrozenBuffer {
            inner: part.into_bytes(),
        }
    }
}

impl Buffer {
    /// Converts accumulated `PyBytes` objects to [`Part`] for zero-copy multipart messages.
    /// After calling, the buffer is empty.
    pub fn take_part(&mut self) -> Part {
        let inner = std::mem::take(&mut self.inner);

        Part::from_fragments(
            inner
                .into_iter()
                .map(|py_bytes| {
                    let wrapper = KeepPyBytesAlive::new(py_bytes);
                    bytes::Bytes::from_owner(wrapper)
                })
                .collect::<Vec<_>>(),
        )
    }
}

/// An immutable buffer for reading bytes data.
///
/// The `FrozenBuffer` struct provides a read-only interface to byte data. Once created,
/// the buffer's content cannot be modified, but it supports various reading operations
/// including line-by-line reading and copying data to external buffers. It implements
/// Python's buffer protocol for zero-copy access from Python code.
///
/// # Examples
///
/// ```python
/// from monarch._rust_bindings.monarch_hyperactor.buffers import Buffer
///
/// # Create and populate a buffer
/// buffer = Buffer()
/// buffer.write(b"Hello\nWorld\n")
///
/// # Freeze it for reading
/// frozen = buffer.freeze()
///
/// # Read all content
/// content = frozen.read()
/// print(content)  # b"Hello\nWorld\n"
///
/// # Read line by line (create a new frozen buffer)
/// buffer.write(b"Line 1\nLine 2\n")
/// frozen = buffer.freeze()
/// line1 = frozen.readline()
/// line2 = frozen.readline()
/// ```
#[pyclass(subclass, module = "monarch._rust_bindings.monarch_hyperactor.buffers")]
#[derive(Clone, Serialize, Deserialize, Named, PartialEq, Default)]
pub struct FrozenBuffer {
    pub inner: bytes::Bytes,
}

#[pymethods]
impl FrozenBuffer {
    /// Reads bytes from the buffer.
    ///
    /// Advances the internal read position by the number of bytes read.
    /// This is a consuming operation - once bytes are read, they cannot be read again.
    ///
    /// # Arguments
    /// * `size` - Number of bytes to read. If -1 or not provided, reads all remaining bytes
    ///
    /// # Returns
    /// A PyBytes object containing the bytes read from the buffer
    #[pyo3(signature=(size=-1))]
    fn read<'py>(mut slf: PyRefMut<'py, Self>, size: i64) -> Bound<'py, PyBytes> {
        let size = if size <= 0 {
            slf.inner.remaining() as i64
        } else {
            size.min(slf.inner.remaining() as i64)
        } as usize;
        let out = PyBytes::new(slf.py(), &slf.inner[..size]);
        slf.inner.advance(size);
        out
    }

    /// Returns the number of bytes remaining in the buffer.
    ///
    /// # Returns
    /// The number of bytes that can still be read from the buffer
    fn __len__(&self) -> usize {
        self.inner.remaining()
    }

    /// Returns a string representation of the buffer content.
    ///
    /// This method provides a debug representation of the remaining bytes in the buffer.
    ///
    /// # Returns
    /// A string showing the bytes remaining in the buffer
    fn __str__(&self) -> String {
        format!("{:?}", &self.inner[..])
    }

    /// Implements Python's buffer protocol for zero-copy access.
    ///
    /// This method allows Python code to access the buffer's underlying data without copying,
    /// enabling efficient integration with memoryview, numpy arrays, and other buffer-aware
    /// Python objects. The buffer is read-only and cannot be modified through this interface.
    ///
    /// # Safety
    /// This method uses unsafe FFI calls to implement Python's buffer protocol.
    /// The implementation ensures that:
    /// - The buffer is marked as read-only
    /// - A reference to the PyObject is held to prevent garbage collection
    /// - Proper buffer metadata is set for Python interoperability
    ///
    /// Adapted from https://docs.rs/crate/pyo3/latest/source/tests/test_buffer.rs
    unsafe fn __getbuffer__(
        slf: PyRefMut<'_, Self>,
        view: *mut pyo3::ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            panic!("view is null");
        }
        if (flags & pyo3::ffi::PyBUF_WRITABLE) == pyo3::ffi::PyBUF_WRITABLE {
            panic!("object not writable");
        }
        let bytes = &slf.inner;
        // SAFETY: The view pointer is valid and we're setting up the buffer metadata correctly.
        // The PyObject reference is held by setting (*view).obj to prevent garbage collection.
        unsafe {
            (*view).buf = bytes.as_ptr() as *mut c_void;
            (*view).len = bytes.len() as isize;
            (*view).readonly = 1;
            (*view).itemsize = 1;
            (*view).ndim = 1;
            (*view).shape = &mut (*view).len;
            (*view).strides = &mut (*view).itemsize;
            (*view).suboffsets = std::ptr::null_mut();
            (*view).internal = std::ptr::null_mut();
            // This holds on to the reference to prevent garbage collection
            (*view).obj = slf.into_ptr();
        }
        Ok(())
    }

    /// Reads a line from the buffer up to a newline character.
    ///
    /// Searches for the first newline character ('\n') within the specified size limit
    /// and returns all bytes up to and including that character. If no newline is found
    /// within the limit, returns up to `size` bytes. Advances the read position by the
    /// number of bytes read.
    ///
    /// # Arguments
    /// * `size` - Maximum number of bytes to read. If -1 or not provided, searches through all remaining bytes
    ///
    /// # Returns
    /// A PyBytes object containing the line data (including the newline character if found)
    #[pyo3(signature=(size=-1))]
    fn readline<'py>(&mut self, py: Python<'py>, size: i64) -> Bound<'py, PyBytes> {
        let max_size = if size < 0 {
            self.inner.remaining() as i64
        } else {
            size.min(self.inner.remaining() as i64)
        } as usize;
        let size = self.inner[..max_size]
            .iter()
            .position(|x| *x == b'\n')
            .unwrap_or(max_size);

        let tmp = PyBytes::new(py, &self.inner[..max_size]);
        self.inner.advance(size);
        tmp
    }

    /// Reads bytes from the buffer into an existing buffer-like object.
    ///
    /// This method implements efficient copying of data from the FrozenBuffer into
    /// any Python object that supports the buffer protocol (like bytearray, memoryview, etc.).
    /// The number of bytes copied is limited by either the remaining bytes in this buffer
    /// or the capacity of the destination buffer, whichever is smaller.
    ///
    /// # Arguments
    /// * `b` - Any Python object that supports the buffer protocol for writing
    ///
    /// # Returns
    /// The number of bytes actually copied into the destination buffer
    ///
    /// # Errors
    /// Returns a PyBufferError if the destination object doesn't support the buffer protocol
    /// or if there's an error during the copy operation
    fn readinto<'py>(&mut self, py: Python<'py>, b: &Bound<'py, PyAny>) -> PyResult<i64> {
        let buff: PyBuffer<u8> = PyBuffer::get(b)?;
        let to_write = self.inner.remaining().min(buff.item_count());
        buff.copy_from_slice(py, &self.inner[..to_write])?;
        self.inner.advance(to_write);
        Ok(to_write as i64)
    }
}

pub fn register_python_bindings(hyperactor_mod: &Bound<'_, PyModule>) -> PyResult<()> {
    hyperactor_mod.add_class::<Buffer>()?;
    hyperactor_mod.add_class::<FrozenBuffer>()?;
    Ok(())
}
