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
//! All constructors and operations panic at runtime; the types exist solely
//! for type-level compatibility.

use std::marker::PhantomData;

pub use monarch_types::ReduceOp;
pub use monarch_types::UniqueId;
use thiserror::Error;
use torch_sys2::CudaDevice;
use torch_sys2::ScalarType;
use torch_sys2::TensorCell;

use crate::cuda::CudaError;
use crate::cuda::Event;
use crate::cuda::Stream;

#[derive(Debug, Error)]
pub enum RawNcclError {
    #[error("NCCL not available: built without `cuda` feature")]
    Unavailable,
}

#[derive(Debug, Error)]
pub enum NcclError {
    #[error("a NCCL-level error: {0:?}")]
    NcclError(#[from] RawNcclError),

    #[error("a CUDA-level error: {0:?}")]
    CudaError(#[from] CudaError),

    #[error("invalid NCCL data type: {0:#?}")]
    InvalidDataType(ScalarType),

    #[error("tensor used in collective must be contiguous")]
    NoncontiguousTensor,

    #[error("tensor must be on CUDA device, got: {0:?}")]
    InvalidDevice(torch_sys2::DeviceType),

    #[error("got sparse tensor, only dense tensors allowed")]
    InvalidSparseTensor,

    #[error("float8 dtypes are not currently supported for NCCL reductions")]
    Float8Reduction,

    #[error("output tensor must have the same type as input tensor")]
    TypeMismatch,

    #[error("output tensor size must be equal to world size times input tensor size")]
    OutputSizeMismatch,

    #[error("input tensor must be the same size as output size times world size")]
    InputSizeMismatch,

    #[error("ranks passed should be within the global world_size, got: {0:#?}")]
    InvalidSplit(Vec<i32>),

    #[error("undefined tensor used for NCCL operation")]
    UndefinedTensor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NcclStatus {
    Success,
    InProgress,
}

pub struct NcclGroupTicket {
    _unsend: PhantomData<*const ()>,
}

pub fn group_start() -> Result<NcclGroupTicket, NcclError> {
    panic!("NCCL group_start requires the `cuda` feature")
}

pub fn group_end(_ticket: NcclGroupTicket) -> Result<(), NcclError> {
    panic!("NCCL group_end requires the `cuda` feature")
}

/// Extension trait providing NCCL-specific operations on `UniqueId`.
pub trait UniqueIdExt {
    fn new_nccl() -> Result<UniqueId, RawNcclError>;
    fn to_nccl(&self) -> [std::os::raw::c_char; 128];
}

impl UniqueIdExt for UniqueId {
    fn new_nccl() -> Result<UniqueId, RawNcclError> {
        Err(RawNcclError::Unavailable)
    }

    fn to_nccl(&self) -> [std::os::raw::c_char; 128] {
        *self.internal()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Int8 = 0,
    Uint8 = 1,
    Int32 = 2,
    Uint32 = 3,
    Int64 = 4,
    Uint64 = 5,
    Float16 = 6,
    Float32 = 7,
    Float64 = 8,
    Bfloat16 = 9,
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
        panic!("NCCL Communicator requires the `cuda` feature")
    }

    pub fn split_all(&mut self) -> Result<Self, NcclError> {
        panic!("NCCL Communicator requires the `cuda` feature")
    }

    pub fn split_from(&mut self, _ranks: Vec<i32>) -> Result<Option<Self>, NcclError> {
        panic!("NCCL Communicator requires the `cuda` feature")
    }

    pub fn all_reduce(
        &mut self,
        _tensor: &TensorCell,
        _reduce_op: ReduceOp,
        _stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        panic!("NCCL Communicator requires the `cuda` feature")
    }

    pub fn broadcast(
        &mut self,
        _tensor: &TensorCell,
        _root: i32,
        _stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        panic!("NCCL Communicator requires the `cuda` feature")
    }

    pub fn reduce(
        &mut self,
        _tensor: &TensorCell,
        _reduce_op: ReduceOp,
        _root: i32,
        _stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        panic!("NCCL Communicator requires the `cuda` feature")
    }

    pub fn all_gather(
        &mut self,
        _output_cells: &[TensorCell],
        _input_cell: &TensorCell,
        _stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        panic!("NCCL Communicator requires the `cuda` feature")
    }

    pub fn all_gather_into_tensor(
        &mut self,
        _output_cell: &TensorCell,
        _input_cell: &TensorCell,
        _stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        panic!("NCCL Communicator requires the `cuda` feature")
    }

    pub fn reduce_scatter_tensor(
        &mut self,
        _output_cell: &TensorCell,
        _input_cell: &TensorCell,
        _reduce_op: ReduceOp,
        _stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        panic!("NCCL Communicator requires the `cuda` feature")
    }

    pub fn send(
        &mut self,
        _tensor_cell: &TensorCell,
        _dst: i32,
        _stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        panic!("NCCL Communicator requires the `cuda` feature")
    }

    pub fn recv(
        &mut self,
        _tensor_cell: &TensorCell,
        _src: i32,
        _stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        panic!("NCCL Communicator requires the `cuda` feature")
    }

    pub fn all_to_all_single(
        &mut self,
        _output_cell: &TensorCell,
        _input_cell: &TensorCell,
        _stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        panic!("NCCL Communicator requires the `cuda` feature")
    }

    pub fn barrier(&mut self, _stream: &Stream) -> Result<NcclStatus, NcclError> {
        panic!("NCCL Communicator requires the `cuda` feature")
    }
}

unsafe impl Send for Communicator {}
unsafe impl Sync for Communicator {}
