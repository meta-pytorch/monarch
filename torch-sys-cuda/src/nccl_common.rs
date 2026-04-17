/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Types shared between the real NCCL bindings (`nccl`) and the CPU-only stubs
//! (`nccl_stubs`).
//!
//! These types have no dependency on the `nccl-sys` FFI and are always
//! compiled. Keeping them here removes duplication and guarantees both code
//! paths see an identical public API.

use std::marker::PhantomData;

use thiserror::Error;
use torch_sys2::DeviceType;
use torch_sys2::ScalarType;

use crate::cuda::CudaError;

/// Corresponds to ncclResult_t error cases.
#[derive(Debug, Error)]
pub enum RawNcclError {
    #[error("a call to a CUDA function failed")]
    UnhandledCudaError,
    #[error("a call to the system failed")]
    SystemError,
    #[error("an internal check failed; either bug in nccl or memory corruption")]
    InternalError,
    #[error("an argument has an invalid value")]
    InvalidArgument,
    #[error("a call to NCCL is incorrect, usually a programming error")]
    InvalidUsage,
    #[error(
        "a call failed possibly due to a network error or a remote process exiting prematurely"
    )]
    RemoteError,
}

/// Types of errors that the safe `Communicator` API can return.
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

    // TODO would be nice to get real device printouts
    #[error("tensor must be on CUDA device, got: {0:?}")]
    InvalidDevice(DeviceType),

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
    /// Function succeeded.
    Success,
    /// A NCCL operation on the communicator is being enqueued and is being
    /// progressed in the background.
    InProgress,
}

/// A ticket that we use to link group start/end calls. Does not implement
/// `Send`, to enforce that group start and end calls are on the same thread.
// This isn't an RAII guard because ncclGroupEnd can raise errors.
//
// TODO: technically anyone can manufacture a ticket to pass to group_end. We
// can prevent this by checking thread id or something, but seems unnecessary;
// you'd really have to be trying to mess things up.
pub struct NcclGroupTicket {
    // marker to disable Send on this type.
    pub(crate) unsend_marker: PhantomData<*const ()>,
}

/// Rust version of `ncclDataType_t`.
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

impl TryFrom<ScalarType> for DataType {
    type Error = NcclError;

    fn try_from(value: ScalarType) -> Result<Self, Self::Error> {
        match value {
            ScalarType::Char => Ok(DataType::Int8),
            ScalarType::Byte => Ok(DataType::Uint8),
            ScalarType::Half => Ok(DataType::Float16),
            ScalarType::Float => Ok(DataType::Float32),
            ScalarType::Double => Ok(DataType::Float64),
            ScalarType::Int => Ok(DataType::Int32),
            ScalarType::Long => Ok(DataType::Int64),
            ScalarType::Bool => Ok(DataType::Uint8),
            ScalarType::BFloat16 => Ok(DataType::Bfloat16),
            ScalarType::Float8_e5m2 => Ok(DataType::Uint8),
            ScalarType::Float8_e4m3fn => Ok(DataType::Uint8),
            ScalarType::Float8_e4m3fnuz => Ok(DataType::Uint8),
            ScalarType::Float8_e5m2fnuz => Ok(DataType::Uint8),
            _ => Err(NcclError::InvalidDataType(value)),
        }
    }
}
