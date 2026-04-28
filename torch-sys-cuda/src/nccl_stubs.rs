/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Stub types for NCCL when the `cuda` feature is disabled.
//!
//! These types match the public API of the real `nccl` module so that
//! downstream crates (e.g., `monarch_tensor_worker`) compile without CUDA.
//! Constructors and operations panic at runtime; the types exist solely for
//! type-level compatibility, and callers are expected to gate usage on a
//! tensor-engine capability check before reaching this code.

pub use monarch_types::ReduceOp;
pub use monarch_types::UniqueId;
use torch_sys2::CudaDevice;
use torch_sys2::TensorCell;

use crate::cuda::Stream;
pub use crate::nccl_common::DataType;
pub use crate::nccl_common::NcclError;
pub use crate::nccl_common::NcclGroupTicket;
pub use crate::nccl_common::NcclStatus;
pub use crate::nccl_common::RawNcclError;

const UNAVAILABLE: &str = "NCCL requires the `cuda` feature";

pub fn group_start() -> Result<NcclGroupTicket, NcclError> {
    panic!("{}", UNAVAILABLE)
}

pub fn group_end(_ticket: NcclGroupTicket) -> Result<(), NcclError> {
    panic!("{}", UNAVAILABLE)
}

/// Extension trait providing NCCL-specific operations on `UniqueId`.
pub trait UniqueIdExt {
    fn new_nccl() -> Result<UniqueId, RawNcclError>;
    fn to_nccl(&self) -> [std::os::raw::c_char; 128];
}

impl UniqueIdExt for UniqueId {
    fn new_nccl() -> Result<UniqueId, RawNcclError> {
        panic!("{}", UNAVAILABLE)
    }

    fn to_nccl(&self) -> [std::os::raw::c_char; 128] {
        *self.internal()
    }
}

#[derive(Debug)]
pub struct Communicator {
    _private: (),
}

impl Communicator {
    pub fn new(
        _device: CudaDevice,
        _world_size: i32,
        _unique_id: UniqueId,
        _rank: i32,
    ) -> Result<Self, NcclError> {
        panic!("{}", UNAVAILABLE)
    }

    pub fn split_all(&mut self) -> Result<Self, NcclError> {
        panic!("{}", UNAVAILABLE)
    }

    pub fn split_from(&mut self, _ranks: Vec<i32>) -> Result<Option<Self>, NcclError> {
        panic!("{}", UNAVAILABLE)
    }

    pub fn all_reduce(
        &mut self,
        _tensor: &TensorCell,
        _reduce_op: ReduceOp,
        _stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        panic!("{}", UNAVAILABLE)
    }

    pub fn broadcast(
        &mut self,
        _tensor: &TensorCell,
        _root: i32,
        _stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        panic!("{}", UNAVAILABLE)
    }

    pub fn reduce(
        &mut self,
        _tensor: &TensorCell,
        _reduce_op: ReduceOp,
        _root: i32,
        _stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        panic!("{}", UNAVAILABLE)
    }

    pub fn all_gather(
        &mut self,
        _output_cells: &[TensorCell],
        _input_cell: &TensorCell,
        _stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        panic!("{}", UNAVAILABLE)
    }

    pub fn all_gather_into_tensor(
        &mut self,
        _output_cell: &TensorCell,
        _input_cell: &TensorCell,
        _stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        panic!("{}", UNAVAILABLE)
    }

    pub fn reduce_scatter_tensor(
        &mut self,
        _output_cell: &TensorCell,
        _input_cell: &TensorCell,
        _reduce_op: ReduceOp,
        _stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        panic!("{}", UNAVAILABLE)
    }

    pub fn send(
        &mut self,
        _tensor_cell: &TensorCell,
        _dst: i32,
        _stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        panic!("{}", UNAVAILABLE)
    }

    pub fn recv(
        &mut self,
        _tensor_cell: &TensorCell,
        _src: i32,
        _stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        panic!("{}", UNAVAILABLE)
    }

    pub fn all_to_all_single(
        &mut self,
        _output_cell: &TensorCell,
        _input_cell: &TensorCell,
        _stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        panic!("{}", UNAVAILABLE)
    }

    pub fn barrier(&mut self, _stream: &Stream) -> Result<NcclStatus, NcclError> {
        panic!("{}", UNAVAILABLE)
    }
}

// SAFETY: the stub Communicator carries no data; all methods panic.
unsafe impl Send for Communicator {}
// SAFETY: the stub Communicator carries no data; all methods panic.
unsafe impl Sync for Communicator {}
