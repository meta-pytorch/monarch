/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Rust-native sender for remotemount block transfers.
//!
//! Packs files into an anonymous mmap, computes block hashes, then sends
//! dirty blocks over parallel hyperactor channels directly from the buffer.

use std::str::FromStr;
use std::sync::Arc;

use anyhow::Result;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::Tx;
use pyo3::exceptions::PyOSError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use serde::Deserialize;
use serde::Serialize;
use serde_multipart::Part;
use typeuri::Named;

use crate::fast_pack::FileInfo;
use crate::fast_pack::HASH_BLOCK_SIZE;
use crate::fast_pack::compute_block_hashes;
use crate::fast_pack::mmap_anonymous;
use crate::fast_pack::pack_files_into;

/// Wire message carrying a batch of blocks.
#[derive(Debug, Clone, Serialize, Deserialize, Named)]
pub(crate) struct BatchTransfer {
    /// (offset_in_buffer, size) for each block in this batch.
    pub blocks: Vec<(u64, u32)>,
    /// All block data concatenated. Zero-copy via Part::from_fragments.
    pub data: Part,
}

wirevalue::register_type!(BatchTransfer);

/// RAII wrapper for an anonymous mmap buffer with precomputed block hashes.
#[pyclass(module = "monarch._rust_bindings.monarch_extension.tls_sender")]
struct PackedBuffer {
    ptr: *mut libc::c_void,
    size: usize,
    #[pyo3(get)]
    hashes: Vec<String>,
}

// SAFETY: the mmap region is process-global memory accessible from any thread.
unsafe impl Send for PackedBuffer {}
// SAFETY: PackedBuffer is read-only after construction (ptr/size are immutable).
unsafe impl Sync for PackedBuffer {}

impl Drop for PackedBuffer {
    fn drop(&mut self) {
        if self.size > 0 {
            // SAFETY: self.ptr was returned by mmap_anonymous(self.size) and
            // has not been munmapped yet. Drop runs at most once.
            unsafe {
                libc::munmap(self.ptr, self.size);
            }
        }
    }
}

/// Owned handle to an mmap region, used to create zero-copy Bytes slices.
struct MmapOwner {
    ptr: *const u8,
    len: usize,
}

// SAFETY: the mmap region is process-global memory, safe to share across threads.
unsafe impl Send for MmapOwner {}
// SAFETY: MmapOwner is read-only after construction; concurrent reads are safe.
unsafe impl Sync for MmapOwner {}

impl MmapOwner {
    /// Create a zero-copy `Bytes` slice into this mmap region.
    fn slice(self: &Arc<Self>, offset: usize, size: usize) -> bytes::Bytes {
        debug_assert!(offset + size <= self.len);
        // SAFETY: offset + size <= self.len, and self.ptr is a valid mmap region
        // that lives as long as this Arc.
        let slice = unsafe { std::slice::from_raw_parts(self.ptr.add(offset), size) };
        bytes::Bytes::from_owner(OwnedSlice {
            _owner: Arc::clone(self),
            slice,
        })
    }
}

/// A borrowed slice backed by an Arc<MmapOwner>.
struct OwnedSlice {
    _owner: Arc<MmapOwner>,
    slice: &'static [u8],
}

// SAFETY: the slice points into a process-global mmap region.
unsafe impl Send for OwnedSlice {}
// SAFETY: OwnedSlice is read-only; concurrent reads of the mmap are safe.
unsafe impl Sync for OwnedSlice {}

impl AsRef<[u8]> for OwnedSlice {
    fn as_ref(&self) -> &[u8] {
        self.slice
    }
}

fn parse_addrs(addrs: &[String]) -> PyResult<Vec<ChannelAddr>> {
    addrs
        .iter()
        .map(|s| ChannelAddr::from_str(s).map_err(|e| PyRuntimeError::new_err(e.to_string())))
        .collect()
}

/// Send dirty blocks over parallel hyperactor channels.
///
/// Matches D97817298's pattern: individual 64MB messages per block,
/// send().await per message, parallelism from many channels.
fn send_blocks_impl(
    py: Python<'_>,
    buf_ptr: usize,
    total_size: usize,
    dirty_blocks: Vec<usize>,
    data_addrs: Vec<ChannelAddr>,
    block_size: usize,
) -> PyResult<()> {
    let mmap = Arc::new(MmapOwner {
        ptr: buf_ptr as *const u8,
        len: total_size,
    });

    let runtime = monarch_hyperactor::runtime::get_tokio_runtime();

    // Dial all channels (needs runtime context).
    let senders: Vec<Arc<channel::ChannelTx<BatchTransfer>>> = runtime
        .block_on(async {
            data_addrs
                .into_iter()
                .map(|addr| channel::dial::<BatchTransfer>(addr).map(Arc::new))
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    // Build work items and distribute round-robin across channels.
    let all_work: Vec<(u64, usize)> = dirty_blocks
        .iter()
        .map(|&bi| {
            let offset = bi.checked_mul(block_size).ok_or_else(|| {
                PyRuntimeError::new_err(format!(
                    "block {bi} * block_size {block_size} overflows usize"
                ))
            })?;
            if offset >= total_size {
                return Err(PyRuntimeError::new_err(format!(
                    "block {bi} offset {offset} >= total_size {total_size}"
                )));
            }
            let size = std::cmp::min(block_size, total_size - offset);
            Ok((offset as u64, size))
        })
        .collect::<PyResult<Vec<_>>>()?;

    let num_channels = senders.len();
    let mut per_channel: Vec<Vec<(u64, usize)>> = vec![Vec::new(); num_channels];
    for (i, w) in all_work.iter().enumerate() {
        per_channel[i % num_channels].push(*w);
    }

    // Send blocks using batched transfers. Data batches use post()
    // (fire-and-forget) to avoid the 500ms ack wait. Only the final
    // sentinel uses send().await to ensure all data is transmitted.
    monarch_hyperactor::runtime::signal_safe_block_on(py, async move {
        let mut handles = Vec::with_capacity(num_channels);

        for (tx, blocks) in senders.iter().zip(per_channel.into_iter()) {
            let tx = tx.clone();
            let mmap = mmap.clone();

            handles.push(tokio::spawn(async move {
                if blocks.is_empty() {
                    return Ok::<(), anyhow::Error>(());
                }

                // Batch blocks into ~256MB messages to reduce per-message
                // overhead while staying within codec frame limits.
                const MAX_BATCH_BYTES: usize = 256 * 1024 * 1024;
                let mut batch_start = 0;
                while batch_start < blocks.len() {
                    let mut batch_end = batch_start;
                    let mut batch_bytes = 0usize;
                    while batch_end < blocks.len() {
                        let (_, size) = blocks[batch_end];
                        if batch_bytes + size > MAX_BATCH_BYTES && batch_end > batch_start {
                            break;
                        }
                        batch_bytes += size;
                        batch_end += 1;
                    }

                    let batch = &blocks[batch_start..batch_end];
                    let fragments: Vec<bytes::Bytes> = batch
                        .iter()
                        .map(|&(offset, size)| mmap.slice(offset as usize, size))
                        .collect();
                    let block_meta: Vec<(u64, u32)> = batch
                        .iter()
                        .map(|&(offset, size)| (offset, size as u32))
                        .collect();

                    tx.post(BatchTransfer {
                        blocks: block_meta,
                        data: Part::from_fragments(fragments),
                    });

                    batch_start = batch_end;
                }

                // Send sentinel and wait for ack to ensure all prior
                // posted data has been transmitted.
                tx.send(BatchTransfer {
                    blocks: Vec::new(),
                    data: Part::default(),
                })
                .await
                .map_err(|e| anyhow::anyhow!("channel send sentinel failed: {e}"))?;

                Ok(())
            }));
        }

        for handle in handles {
            handle.await??;
        }
        Ok::<(), anyhow::Error>(())
    })?
    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

#[pymethods]
impl PackedBuffer {
    /// Send dirty blocks over parallel hyperactor channels.
    #[pyo3(signature = (dirty_blocks, data_addrs, hash_block_size=None))]
    fn send_blocks(
        &self,
        py: Python<'_>,
        dirty_blocks: Vec<usize>,
        data_addrs: Vec<String>,
        hash_block_size: Option<usize>,
    ) -> PyResult<()> {
        let block_size = hash_block_size.unwrap_or(HASH_BLOCK_SIZE);
        let addrs = parse_addrs(&data_addrs)?;
        send_blocks_impl(
            py,
            self.ptr as usize,
            self.size,
            dirty_blocks,
            addrs,
            block_size,
        )
    }
}

/// Send dirty blocks from an existing buffer over parallel hyperactor channels.
#[pyfunction]
#[pyo3(signature = (buffer, total_size, dirty_blocks, data_addrs, hash_block_size=None))]
fn send_blocks_from_buffer(
    py: Python<'_>,
    buffer: pyo3::buffer::PyBuffer<u8>,
    total_size: usize,
    dirty_blocks: Vec<usize>,
    data_addrs: Vec<String>,
    hash_block_size: Option<usize>,
) -> PyResult<()> {
    let block_size = hash_block_size.unwrap_or(HASH_BLOCK_SIZE);
    let buf_ptr = buffer.buf_ptr() as usize;
    let addrs = parse_addrs(&data_addrs)?;
    send_blocks_impl(py, buf_ptr, total_size, dirty_blocks, addrs, block_size)
}

/// Pack files into anonymous mmap and compute block hashes.
#[pyfunction]
#[pyo3(signature = (file_list, total_size, hash_block_size=None))]
fn pack_and_hash(
    py: Python<'_>,
    file_list: Vec<(String, usize, usize)>,
    total_size: usize,
    hash_block_size: Option<usize>,
) -> PyResult<PackedBuffer> {
    let block_size = hash_block_size.unwrap_or(HASH_BLOCK_SIZE);

    if file_list.is_empty() || total_size == 0 {
        return Ok(PackedBuffer {
            ptr: std::ptr::null_mut(),
            size: 0,
            hashes: Vec::new(),
        });
    }

    let files: Vec<FileInfo> = file_list
        .into_iter()
        .map(|(path, offset, size)| FileInfo { path, offset, size })
        .collect();

    let buffer =
        mmap_anonymous(total_size).map_err(|e| PyOSError::new_err(format!("mmap failed: {e}")))?;

    let buf_ptr = buffer as usize;

    let hashes = py.detach(move || {
        let nthreads = std::thread::available_parallelism()
            .map(|n| n.get().min(16))
            .unwrap_or(1);
        pack_files_into(buf_ptr, &files, nthreads);
        compute_block_hashes(buf_ptr, total_size, block_size, nthreads)
    });

    Ok(PackedBuffer {
        ptr: buffer,
        size: total_size,
        hashes,
    })
}

pub fn register_python_bindings(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PackedBuffer>()?;
    let f = wrap_pyfunction!(pack_and_hash, module)?;
    f.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_extension.tls_sender",
    )?;
    module.add_function(f)?;
    let f2 = wrap_pyfunction!(send_blocks_from_buffer, module)?;
    f2.setattr(
        "__module__",
        "monarch._rust_bindings.monarch_extension.tls_sender",
    )?;
    module.add_function(f2)?;
    Ok(())
}
