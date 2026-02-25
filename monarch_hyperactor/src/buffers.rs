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
use bytes::Bytes;
use bytes::BytesMut;
use hyperactor_config::CONFIG;
use hyperactor_config::ConfigAttr;
use hyperactor_config::attrs::declare_attrs;
use pyo3::buffer::PyBuffer;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyBytesMethods;
use serde::Deserialize;
use serde::Serialize;
use serde_multipart::Part;
use typeuri::Named;

declare_attrs! {
    /// Threshold below which writes are copied into a contiguous buffer.
    /// Writes >= this size are stored as zero-copy references.
    @meta(CONFIG = ConfigAttr::new(
        Some("MONARCH_HYPERACTOR_SMALL_WRITE_THRESHOLD".to_string()),
        Some("small_write_threshold".to_string()),
    ))
    pub attr SMALL_WRITE_THRESHOLD: usize = 256;
}

/// Wrapper that keeps Py<PyBytes> alive while allowing zero-copy access to its memory
struct KeepPyBytesAlive {
    _py_bytes: Py<PyBytes>,
    ptr: *const u8,
    len: usize,
}

impl KeepPyBytesAlive {
    fn new(py_bytes: Py<PyBytes>) -> Self {
        let (ptr, len) = Python::attach(|py| {
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

/// A fragment of data in the buffer, either a copy or a reference.
#[derive(Clone)]
enum Fragment {
    /// Small writes that were copied into a contiguous buffer
    Copy(Bytes),
    /// Large writes stored as references to Python bytes
    Reference(Py<PyBytes>),
}

/// A mutable buffer for reading and writing bytes data.
///
/// The `Buffer` struct provides a hybrid interface for accumulating byte data:
/// - Small writes (< 256 bytes) are copied into a contiguous buffer to minimize fragment overhead
/// - Large writes (>= 256 bytes) are stored as zero-copy references to Python bytes objects
///
/// This approach balances the overhead of per-fragment processing against the cost of copying data.
///
/// # Examples
///
/// ```python
/// from monarch._rust_bindings.monarch_hyperactor.buffers import Buffer
///
/// # Create a new buffer
/// buffer = Buffer()
///
/// # Write some data - small writes are batched, large writes are zero-copy
/// buffer.write(b"small")  # copied into pending buffer
/// buffer.write(b"x" * 1000)  # stored as zero-copy reference
/// ```
#[pyclass(subclass, module = "monarch._rust_bindings.monarch_hyperactor.buffers")]
#[derive(Clone)]
pub struct Buffer {
    /// Finalized fragments in write order
    fragments: Vec<Fragment>,
    /// Accumulator for pending small writes
    pending: BytesMut,
    /// Threshold below which writes are copied into a contiguous buffer.
    /// Writes >= this size are stored as zero-copy references.
    threshold: usize,
}

#[pymethods]
impl Buffer {
    /// Creates a new empty buffer.
    ///
    /// # Returns
    /// A new empty `Buffer` instance.
    #[new]
    fn new() -> Self {
        Self {
            fragments: Vec::new(),
            pending: BytesMut::new(),
            threshold: hyperactor_config::global::get(SMALL_WRITE_THRESHOLD),
        }
    }

    /// Writes bytes data to the buffer.
    ///
    /// Small writes (< 256 bytes) are copied into a contiguous buffer.
    /// Large writes (>= 256 bytes) are stored as zero-copy references.
    ///
    /// # Arguments
    /// * `buff` - The bytes object to write to the buffer
    ///
    /// # Returns
    /// The number of bytes written (always equal to the length of input bytes)
    fn write<'py>(&mut self, buff: &Bound<'py, PyBytes>) -> usize {
        let bytes_written = buff.as_bytes().len();

        if bytes_written < self.threshold {
            self.pending.extend_from_slice(buff.as_bytes());
        } else {
            self.flush_pending();
            self.fragments
                .push(Fragment::Reference(buff.clone().unbind()));
        }
        bytes_written
    }

    /// Returns the total number of bytes in the buffer.
    ///
    /// This sums the lengths of all fragments (both copied and zero-copy) plus pending bytes.
    ///
    /// # Returns
    /// The total number of bytes stored in the buffer
    fn __len__(&self) -> usize {
        let fragments_len: usize = Python::attach(|py| {
            self.fragments
                .iter()
                .map(|frag| match frag {
                    Fragment::Copy(bytes) => bytes.len(),
                    Fragment::Reference(py_bytes) => py_bytes.as_bytes(py).len(),
                })
                .sum()
        });
        fragments_len + self.pending.len()
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
    #[pyo3(name = "freeze")]
    fn py_freeze(&mut self) -> FrozenBuffer {
        Buffer::freeze(self)
    }
}

impl Default for Buffer {
    fn default() -> Self {
        Self {
            fragments: Vec::new(),
            pending: BytesMut::new(),
            threshold: hyperactor_config::global::get(SMALL_WRITE_THRESHOLD),
        }
    }
}

impl Buffer {
    fn flush_pending(&mut self) {
        if !self.pending.is_empty() {
            let bytes = std::mem::take(&mut self.pending).freeze();
            self.fragments.push(Fragment::Copy(bytes));
        }
    }

    /// Converts accumulated data to [`Part`] for zero-copy multipart messages.
    ///
    /// Flushes any pending small writes and converts all fragments to bytes::Bytes.
    pub fn take_part(&mut self) -> Part {
        self.flush_pending();

        let fragments = std::mem::take(&mut self.fragments);

        Part::from_fragments(
            fragments
                .into_iter()
                .map(|frag| match frag {
                    Fragment::Copy(bytes) => bytes,
                    Fragment::Reference(py_bytes) => {
                        let wrapper = KeepPyBytesAlive::new(py_bytes);
                        bytes::Bytes::from_owner(wrapper)
                    }
                })
                .collect::<Vec<_>>(),
        )
    }

    /// Freezes the buffer, converting it into an immutable `FrozenBuffer` for reading.
    ///
    /// This is the Rust-accessible version of the Python freeze method.
    pub fn freeze(&mut self) -> FrozenBuffer {
        let part = self.take_part();
        FrozenBuffer {
            inner: part.into_bytes(),
        }
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
wirevalue::register_type!(FrozenBuffer);

impl From<Vec<u8>> for FrozenBuffer {
    fn from(v: Vec<u8>) -> Self {
        Self {
            inner: bytes::Bytes::from(v),
        }
    }
}

impl From<&'static [u8]> for FrozenBuffer {
    fn from(v: &'static [u8]) -> Self {
        Self {
            inner: bytes::Bytes::from_static(v),
        }
    }
}

impl From<FrozenBuffer> for Part {
    fn from(buf: FrozenBuffer) -> Self {
        Part::from(buf.inner)
    }
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
