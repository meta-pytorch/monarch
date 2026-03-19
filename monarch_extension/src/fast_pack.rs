/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Parallel file packing into a contiguous mmap buffer with block hashing.
//!
//! Given a list of (path, offset, size) entries and a total size,
//! allocates an mmap region, reads all files into it using a thread
//! pool, and computes xxh64 block hashes over the result — all in a
//! single pass through the data (the hash pass reads from hot pages).

use std::fs::File;
use std::os::unix::io::AsRawFd;
use std::thread;

use pyo3::exceptions::PyOSError;
use pyo3::ffi;
use pyo3::prelude::*;
use xxhash_rust::xxh64;

const NUM_THREADS: usize = 16;
pub(crate) const HASH_BLOCK_SIZE: usize = 64 * 1024 * 1024; // 64 MB
/// Maximum bytes a single work unit reads. Large files are split into
/// chunks of this size so multiple threads can read them in parallel.
const READ_CHUNK_SIZE: usize = 64 * 1024 * 1024; // 64 MB

pub(crate) struct FileInfo {
    pub(crate) path: String,
    pub(crate) offset: usize,
    pub(crate) size: usize,
}

/// A unit of read work: read `size` bytes from `file_idx` at `file_offset`
/// into the buffer at `buf_offset`.
struct ReadWork {
    file_idx: usize,
    file_offset: usize,
    buf_offset: usize,
    size: usize,
}

/// Pack files into the buffer using a thread pool with byte-balanced work.
///
/// Large files are split into 64 MB chunks so all threads stay busy.
/// Each chunk is read via `pread` for parallel access to the same file.
/// Work units are distributed round-robin by descending size so that
/// large chunks spread evenly across threads instead of piling up on
/// thread 0.
pub(crate) fn pack_files_into(buf_ptr: usize, files: &[FileInfo]) {
    // Build work units: split large files into READ_CHUNK_SIZE pieces.
    let mut work_units: Vec<ReadWork> = Vec::new();
    for (file_idx, f) in files.iter().enumerate() {
        if f.size == 0 {
            continue;
        }
        let mut file_off = 0;
        while file_off < f.size {
            let chunk = std::cmp::min(READ_CHUNK_SIZE, f.size - file_off);
            work_units.push(ReadWork {
                file_idx,
                file_offset: file_off,
                buf_offset: f.offset + file_off,
                size: chunk,
            });
            file_off += chunk;
        }
    }

    // Sort descending by size, then distribute round-robin across threads.
    // This prevents all large chunks from landing on a single thread
    // (e.g. 64 x 64 MB data.bin chunks all on thread 0 while threads
    // 1-15 get only tiny .py files).
    work_units.sort_by(|a, b| b.size.cmp(&a.size));

    let num_threads = work_units.len().clamp(1, NUM_THREADS);
    let mut per_thread: Vec<Vec<&ReadWork>> = vec![Vec::new(); num_threads];
    for (i, w) in work_units.iter().enumerate() {
        per_thread[i % num_threads].push(w);
    }

    thread::scope(|s| {
        for thread_work in per_thread {
            s.spawn(move || {
                // Cache open file descriptors to avoid re-opening the same
                // file for each chunk.
                let mut cached_fd: Option<(usize, File)> = None;

                for w in thread_work {
                    let file = match &cached_fd {
                        Some((idx, f)) if *idx == w.file_idx => f,
                        _ => {
                            let Ok(f) = File::open(&files[w.file_idx].path) else {
                                eprintln!("fast_pack: failed to open {}", files[w.file_idx].path);
                                continue;
                            };
                            cached_fd = Some((w.file_idx, f));
                            &cached_fd.as_ref().expect("cached_fd was just set").1
                        }
                    };

                    // SAFETY: each work unit writes to a non-overlapping region
                    // [buf_offset..buf_offset+size] of the buffer.
                    let dst = unsafe {
                        std::slice::from_raw_parts_mut((buf_ptr + w.buf_offset) as *mut u8, w.size)
                    };

                    let mut total_read = 0;
                    while total_read < w.size {
                        // SAFETY: file is a valid fd, dst is a valid mutable
                        // slice within the mmap, and pread is thread-safe.
                        let n = unsafe {
                            libc::pread(
                                file.as_raw_fd(),
                                dst[total_read..].as_mut_ptr() as *mut libc::c_void,
                                w.size - total_read,
                                (w.file_offset + total_read) as libc::off_t,
                            )
                        };
                        if n <= 0 {
                            break;
                        }
                        total_read += n as usize;
                    }
                }
            });
        }
    });
}

/// Compute xxh64 block hashes over the buffer in parallel.
///
/// Returns hex digest strings, one per `HASH_BLOCK_SIZE` block.
pub(crate) fn compute_block_hashes(
    buf_ptr: usize,
    total_size: usize,
    block_size: usize,
) -> Vec<String> {
    let num_blocks = total_size.div_ceil(block_size);
    let mut hashes = vec![String::new(); num_blocks];

    let blocks_per_thread = num_blocks.div_ceil(NUM_THREADS);

    thread::scope(|s| {
        for (thread_idx, hash_chunk) in hashes.chunks_mut(blocks_per_thread).enumerate() {
            let start_block = thread_idx * blocks_per_thread;
            s.spawn(move || {
                for (i, hash_slot) in hash_chunk.iter_mut().enumerate() {
                    let block_idx = start_block + i;
                    let offset = block_idx * block_size;
                    let size = std::cmp::min(block_size, total_size - offset);

                    // SAFETY: buffer was allocated with total_size bytes;
                    // offset + size <= total_size.
                    let data = unsafe {
                        std::slice::from_raw_parts((buf_ptr + offset) as *const u8, size)
                    };

                    *hash_slot = format!("{:016x}", xxh64::xxh64(data, 0));
                }
            });
        }
    });

    hashes
}

/// Allocate an anonymous mmap region.
pub(crate) fn mmap_anonymous(total_size: usize) -> Result<*mut libc::c_void, std::io::Error> {
    // SAFETY: mmap with MAP_PRIVATE | MAP_ANONYMOUS allocates zeroed pages
    // backed by swap, not a file descriptor.
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            total_size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(ptr)
    }
}

/// Pack files into a contiguous mmap buffer in parallel, with block hashes.
///
/// Accepts a list of `(path, offset, size)` tuples, a `total_size`, and an
/// optional `hash_block_size` (defaults to 100 MB).
/// Returns `(memoryview, [hash_hex_strings])`.
#[pyfunction]
#[pyo3(signature = (file_list, total_size, hash_block_size=None))]
fn pack_files_with_offsets(
    py: Python<'_>,
    file_list: Vec<(String, usize, usize)>,
    total_size: usize,
    hash_block_size: Option<usize>,
) -> PyResult<Py<PyAny>> {
    let block_size = hash_block_size.unwrap_or(HASH_BLOCK_SIZE);

    if file_list.is_empty() || total_size == 0 {
        let mv = pyo3::types::PyBytes::new(py, &[]).into_any().unbind();
        let hashes = pyo3::types::PyList::empty(py).into_any().unbind();
        let tuple = pyo3::types::PyTuple::new(py, [mv, hashes])?;
        return Ok(tuple.into_any().unbind());
    }

    let files: Vec<FileInfo> = file_list
        .into_iter()
        .map(|(path, offset, size)| FileInfo { path, offset, size })
        .collect();

    let buffer =
        mmap_anonymous(total_size).map_err(|e| PyOSError::new_err(format!("mmap failed: {e}")))?;

    let buf_ptr = buffer as usize;
    pack_files_into(buf_ptr, &files);
    let hashes = compute_block_hashes(buf_ptr, total_size, block_size);

    // SAFETY: buffer is a valid pointer to total_size bytes. We create a
    // writable memoryview so RDMA can use it. Python takes ownership of
    // the view; the mmap region must outlive it.
    unsafe {
        let buf_obj =
            ffi::PyMemoryView_FromMemory(buffer as *mut _, total_size as isize, ffi::PyBUF_WRITE);
        if buf_obj.is_null() {
            libc::munmap(buffer, total_size);
            return Err(PyErr::fetch(py));
        }
        let mv = Py::<PyAny>::from_owned_ptr(py, buf_obj);
        let py_hashes = pyo3::types::PyList::new(py, &hashes)?.into_any().unbind();
        let tuple = pyo3::types::PyTuple::new(py, [mv, py_hashes])?;
        Ok(tuple.into_any().unbind())
    }
}

/// Pack files into a named file-backed mmap with block hashes.
/// Load a single file into anonymous mmap and compute block hashes in parallel.
///
/// Reads the file using the same parallel pread strategy as `pack_files_into`
/// (16 threads, 64 MB chunks) and computes xxh64 hashes over the result.
/// Returns `(memoryview, [hash_hex_strings])`.
///
/// This is ~3-4x faster than the equivalent Python loop (`f.read(64MB)` +
/// memoryview copy + separate xxhash pass) because it avoids the Python bytes
/// allocation per chunk, reads directly into mmap, and hashes data still hot
/// in CPU cache.
#[pyfunction]
#[pyo3(signature = (path, hash_block_size=None, padded_size=None))]
fn load_file_and_hash(
    py: Python<'_>,
    path: String,
    hash_block_size: Option<usize>,
    padded_size: Option<usize>,
) -> PyResult<Py<PyAny>> {
    let block_size = hash_block_size.unwrap_or(HASH_BLOCK_SIZE);

    let file_size = std::fs::metadata(&path)
        .map_err(|e| PyOSError::new_err(format!("stat {path}: {e}")))?
        .len() as usize;

    if file_size == 0 {
        let mv = pyo3::types::PyBytes::new(py, &[]).into_any().unbind();
        let hashes = pyo3::types::PyList::empty(py).into_any().unbind();
        let tuple = pyo3::types::PyTuple::new(py, [mv, hashes])?;
        return Ok(tuple.into_any().unbind());
    }

    // Allocate padded_size if provided (for RDMA buffer reuse), but
    // only read and hash file_size bytes.
    let alloc_size = padded_size.map_or(file_size, |ps| ps.max(file_size));

    let buffer =
        mmap_anonymous(alloc_size).map_err(|e| PyOSError::new_err(format!("mmap failed: {e}")))?;

    let buf_ptr = buffer as usize;

    // Treat the cache file as a single FileInfo entry — pack_files_into
    // splits it into 64 MB chunks and reads them in parallel via pread.
    let files = [FileInfo {
        path,
        offset: 0,
        size: file_size,
    }];
    pack_files_into(buf_ptr, &files);
    let hashes = compute_block_hashes(buf_ptr, file_size, block_size);

    // SAFETY: buffer is a valid mmap pointer of alloc_size bytes.
    // PyMemoryView_FromMemory creates a Python view; null check handles failure.
    unsafe {
        let buf_obj =
            ffi::PyMemoryView_FromMemory(buffer as *mut _, alloc_size as isize, ffi::PyBUF_WRITE);
        if buf_obj.is_null() {
            libc::munmap(buffer, alloc_size);
            return Err(PyErr::fetch(py));
        }
        let mv = Py::<PyAny>::from_owned_ptr(py, buf_obj);
        let py_hashes = pyo3::types::PyList::new(py, &hashes)?.into_any().unbind();
        let tuple = pyo3::types::PyTuple::new(py, [mv, py_hashes])?;
        Ok(tuple.into_any().unbind())
    }
}

/// Load a file into a pre-existing buffer and compute block hashes.
///
/// Reads the file using parallel pread (same as `load_file_and_hash`) but
/// writes into the caller's buffer instead of allocating a new mmap.
/// This preserves RDMA memory registrations across open() calls.
/// Returns `[hash_hex_strings]`.
#[pyfunction]
#[pyo3(signature = (path, buffer, hash_block_size=None))]
fn load_file_into_buffer(
    py: Python<'_>,
    path: String,
    buffer: pyo3::buffer::PyBuffer<u8>,
    hash_block_size: Option<usize>,
) -> PyResult<Py<PyAny>> {
    let block_size = hash_block_size.unwrap_or(HASH_BLOCK_SIZE);

    let file_size = std::fs::metadata(&path)
        .map_err(|e| PyOSError::new_err(format!("stat {path}: {e}")))?
        .len() as usize;

    let buf_ptr = buffer.buf_ptr() as usize;
    let buf_len = buffer.len_bytes();

    if file_size > buf_len {
        return Err(PyOSError::new_err(format!(
            "file {path} is {file_size} bytes but buffer is only {buf_len} bytes"
        )));
    }

    if file_size == 0 {
        let hashes = pyo3::types::PyList::empty(py).into_any().unbind();
        return Ok(hashes);
    }

    let files = [FileInfo {
        path,
        offset: 0,
        size: file_size,
    }];

    // Release GIL for the heavy I/O + hash work.
    let hashes = py.detach(move || {
        pack_files_into(buf_ptr, &files);
        compute_block_hashes(buf_ptr, file_size, block_size)
    });

    let py_hashes = pyo3::types::PyList::new(py, &hashes)?.into_any().unbind();
    Ok(py_hashes)
}

/// Register Python bindings for the fast_pack module.
pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let f = wrap_pyfunction!(pack_files_with_offsets, module)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_extension.fast_pack",
    )?;
    module.add_function(f)?;
    let f3 = wrap_pyfunction!(load_file_and_hash, module)?;
    f3.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_extension.fast_pack",
    )?;
    module.add_function(f3)?;
    let f4 = wrap_pyfunction!(load_file_into_buffer, module)?;
    f4.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_extension.fast_pack",
    )?;
    module.add_function(f4)?;
    Ok(())
}
