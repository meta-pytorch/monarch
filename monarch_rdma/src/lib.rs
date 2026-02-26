/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// RDMA requires frequent unsafe code blocks
#![allow(clippy::undocumented_unsafe_blocks)]

use std::fmt::Debug;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

pub mod backend;
pub mod device_selection;
pub mod efa;
mod rdma_components;
mod rdma_manager_actor;

#[macro_use]
mod macros;

pub use backend::ibverbs::primitives::*;
pub use rdma_components::RdmaRemoteBuffer;
pub use rdma_components::SegmentScannerFn;
// Re-export segment scanner types for extension crate
pub use rdma_components::register_segment_scanner;
pub use rdma_components::*;
pub use rdma_manager_actor::*;
// Re-export rdmaxcel_sys for extension crate to access types
pub use rdmaxcel_sys;
pub use test_utils::is_cuda_available;

/// Handle to a contiguous region of local memory.
///
/// Implementations must guarantee the underlying allocation is valid for the
/// lifetime of the implementor.
pub trait RdmaLocalMemory: Send + Sync + Debug {
    /// Starting virtual address of the memory region.
    fn addr(&self) -> usize;
    /// Size of the memory region in bytes.
    fn size(&self) -> usize;
}

/// Raw pointer-based local memory handle.
///
/// Wraps a virtual address and size. The caller is responsible for
/// ensuring the underlying allocation outlives this handle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawLocalMemory {
    pub addr: usize,
    pub size: usize,
}

impl RawLocalMemory {
    pub fn new(addr: usize, size: usize) -> Self {
        Self { addr, size }
    }
}

impl RdmaLocalMemory for RawLocalMemory {
    fn addr(&self) -> usize {
        self.addr
    }
    fn size(&self) -> usize {
        self.size
    }
}

/// Type of RDMA operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RdmaOpType {
    ReadIntoLocal,
    WriteFromLocal,
}

/// A single RDMA operation to be submitted to a backend.
#[derive(Debug)]
pub struct RdmaOp {
    pub op_type: RdmaOpType,
    pub local: Arc<dyn RdmaLocalMemory>,
    pub remote: RdmaRemoteBuffer,
}

/// Transport level, ordered slowest to fastest.
///
/// The `Ord` implementation reflects this ordering, enabling transport
/// selection via comparison (e.g., "at least NIC speed").
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RdmaTransportLevel {
    /// TCP/IP sockets (fallback transport).
    Tcp,
    /// RDMA NIC (RoCE, InfiniBand, EFA).
    Nic,
    /// Direct memory access (NVLink, shared memory).
    Memory,
}

/// Print comprehensive RDMA device information for debugging.
/// Controlled by MONARCH_DEBUG_RDMA environment variable.
pub fn print_device_info_if_debug_enabled(context: *mut rdmaxcel_sys::ibv_context) {
    if std::env::var("MONARCH_DEBUG_RDMA").is_ok() {
        unsafe {
            rdmaxcel_sys::rdmaxcel_print_device_info(context);
        }
    }
}

/// Print comprehensive RDMA device information for debugging (always prints).
pub fn print_device_info(context: *mut rdmaxcel_sys::ibv_context) {
    unsafe {
        rdmaxcel_sys::rdmaxcel_print_device_info(context);
    }
}

mod test_utils;
