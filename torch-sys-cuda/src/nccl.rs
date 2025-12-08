/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fmt;
use std::fmt::Write;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::mem::MaybeUninit;

use fxhash::FxHasher32;
use nccl_sys::*;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;
use torch_sys2::CudaDevice;
use torch_sys2::DeviceType;
use torch_sys2::ScalarType;
use torch_sys2::Tensor;
use torch_sys2::TensorCell;
use torch_sys2::factory_float_tensor;
use torch_sys2::is_float8_type;

use crate::cuda::CudaError;
use crate::cuda::Stream;
use crate::cuda::set_device;

/// Corresponds to ncclResult_t error cases
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

/// Types of errors that the safe [`Communicator`] API can return.
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

fn nccl_check(result: ncclResult_t) -> Result<NcclStatus, RawNcclError> {
    match result.0 {
        0 => Ok(NcclStatus::Success),
        1 => Err(RawNcclError::UnhandledCudaError),
        2 => Err(RawNcclError::SystemError),
        3 => Err(RawNcclError::InternalError),
        4 => Err(RawNcclError::InvalidArgument),
        5 => Err(RawNcclError::InvalidUsage),
        6 => Err(RawNcclError::RemoteError),
        7 => Ok(NcclStatus::InProgress),
        _ => panic!("Unknown ncclResult_t: {:?}", result.0),
    }
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
    unsend_marker: PhantomData<*const ()>,
}

/// Start a new NCCL group. All NCCL calls within this group will be combined,
/// provided that they were issued on the same thread.
pub fn group_start() -> Result<NcclGroupTicket, NcclError> {
    // SAFETY: intended use of C function.
    nccl_check(unsafe { ncclGroupStart() })?;
    Ok(NcclGroupTicket {
        unsend_marker: PhantomData,
    })
}

/// End the NCCL group.
pub fn group_end(_ticket: NcclGroupTicket) -> Result<(), NcclError> {
    // SAFETY: intended use of C function.
    nccl_check(unsafe { ncclGroupEnd() })?;
    Ok(())
}

/// Binding for `ncclUniqueId`.
#[derive(Clone, Serialize, Deserialize)]
pub struct UniqueId {
    inner: ncclUniqueId,
}

impl fmt::Debug for UniqueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UniqueId")
            .field(
                "inner",
                &format_args!(
                    "{}",
                    self.inner
                        .internal
                        .iter()
                        .fold(String::new(), |mut output, b| {
                            let _ = write!(output, "{:02x}", b);
                            output
                        })
                ),
            )
            .finish()
    }
}

impl UniqueId {
    /// Create a new `UniqueId`.
    pub fn new() -> Result<Self, RawNcclError> {
        let mut inner = MaybeUninit::uninit();
        // Safety: intended usage of this function
        let inner = unsafe {
            nccl_check(ncclGetUniqueId(inner.as_mut_ptr()))?;
            inner.assume_init()
        };
        Ok(Self { inner })
    }
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

impl From<DataType> for ncclDataType_t {
    fn from(data_type: DataType) -> Self {
        Self(data_type as std::os::raw::c_uint)
    }
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

/// Rust version of `ncclRedOp_t`.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum ReduceOp {
    Sum = 0,
    Prod = 1,
    Max = 2,
    Min = 3,
    Avg = 4,
}

impl From<ReduceOp> for ncclRedOp_t {
    fn from(reduce_op: ReduceOp) -> Self {
        Self(reduce_op as std::os::raw::c_uint)
    }
}

fn check_tensor(tensor: &Tensor, is_p2p: bool) -> Result<(), NcclError> {
    if !tensor.defined() {
        return Err(NcclError::UndefinedTensor);
    }
    if !tensor.is_cuda() {
        return Err(NcclError::InvalidDevice(tensor.device().device_type()));
    }
    if tensor.is_sparse() {
        return Err(NcclError::InvalidSparseTensor);
    }

    if !is_p2p && !tensor.is_contiguous() {
        return Err(NcclError::NoncontiguousTensor);
    }

    Ok(())
}
/// Wraps a NCCL communicator, and provides a Tensor-based interface it.
///
/// This implements a subset of the `c10d::ProcessGroup`API.
#[derive(Debug)]
pub struct Communicator {
    inner: ncclComm_t,
    // World size of this communicator.
    world_size: i32,
    // Rank within the world, value is within 0..world_size.
    rank: i32,
    // Size of the global world. This can be different from `world_size` if this
    // communicator was split off from a larger one.
    global_world_size: i32,
    global_rank: i32,
    device: CudaDevice,
}

/// SAFETY: `ncclComm_t` is okay to access from multiple threads, but each
/// communicator *must* issue nccl calls in the same order. It is up to the user
/// to ensure this.
unsafe impl Send for Communicator {}
/// SAFETY: `ncclComm_t` is okay to access from multiple threads, but each
/// communicator *must* issue nccl calls in the same order. It is up to the user
/// to ensure this.
unsafe impl Sync for Communicator {}

// Ported from: https://github.com/pytorch/pytorch/blob/0d6d29af380d6a639bf23127f05e439fafa640bf/torch/distributed/distributed_c10d.py#L4669
fn calculate_color(ranks: &[i32]) -> i32 {
    // Assumes `ranks` is sorted.
    let mut hasher = FxHasher32::default();
    ranks.iter().for_each(|r| hasher.write_i32(*r));
    let hash = hasher.finish();

    // Convert to positive value to fit as color arg to `ncclCommSplit`.
    (hash % (i32::MAX as u64)) as i32
}

impl Communicator {
    /// Create a new communicator. This function must be called by a different
    /// thread/process per rank.
    pub fn new(
        device: CudaDevice,
        world_size: i32,
        unique_id: UniqueId,
        rank: i32,
    ) -> Result<Self, NcclError> {
        set_device(device)?;
        let mut inner = MaybeUninit::uninit();
        // SAFETY: intended use of C function
        let inner = unsafe {
            nccl_check(ncclCommInitRank(
                inner.as_mut_ptr(),
                world_size,
                unique_id.inner,
                rank,
            ))?;
            inner.assume_init()
        };
        Ok(Self {
            inner,
            world_size,
            rank,
            global_rank: rank,
            global_world_size: world_size,
            device,
        })
    }

    /// Split off a new communicator from this one, preserving the same world
    /// size.
    pub fn split_all(&mut self) -> Result<Self, NcclError> {
        let ranks = (0..self.global_world_size).collect();
        Ok(self.split_from(ranks)?.unwrap())
    }

    /// Split off a new communicator from this one. Only `ranks` will be present
    /// on this new communicator.
    ///
    /// If `ranks` is empty, `ncclCommSplit` will be called with
    /// NCCL_SPLIT_NOCOLOR. This can be useful if ranks excluded from the split
    /// don't even know what ranks will be included.
    pub fn split_from(&mut self, mut ranks: Vec<i32>) -> Result<Option<Self>, NcclError> {
        ranks.sort();
        for rank in &ranks {
            if *rank < 0 || *rank >= self.global_world_size {
                return Err(NcclError::InvalidSplit(ranks));
            }
        }

        let color = match ranks.binary_search(&self.rank) {
            Ok(_) => calculate_color(ranks.as_slice()),
            Err(_) => NCCL_SPLIT_NOCOLOR,
        };

        let mut new = MaybeUninit::uninit();

        // SAFETY: intended use of C function
        let new = unsafe {
            nccl_check(ncclCommSplit(
                self.inner,
                color,
                self.rank,
                new.as_mut_ptr(),
                std::ptr::null_mut(),
            ))?;
            new.assume_init()
        };

        let group_rank = ranks.iter().position(|v| *v == self.rank);
        match color {
            NCCL_SPLIT_NOCOLOR => Ok(None),
            _ => Ok(Some(Self {
                inner: new,
                world_size: ranks.len() as i32,
                rank: group_rank.unwrap() as i32,
                global_rank: self.global_rank,
                global_world_size: self.global_world_size,
                device: self.device,
            })),
        }
    }

    /// Reduce the tensor data across all ranks, with each rank receiving the
    /// final result in-place.
    ///
    /// See `torch.distributed.all_reduce` for more detailed documentation.
    pub fn all_reduce(
        &mut self,
        tensor: &TensorCell,
        reduce_op: ReduceOp,
        stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        let tensor = tensor.borrow_mut();
        let data_type: DataType = tensor.scalar_type().try_into()?;

        check_tensor(&tensor, false)?;
        if is_float8_type(tensor.scalar_type()) {
            return Err(NcclError::Float8Reduction);
        }
        // SAFETY: intended use of C function
        unsafe {
            Ok(nccl_check(ncclAllReduce(
                tensor.data_ptr(),
                tensor.mut_data_ptr(),
                tensor.numel() as usize,
                data_type.into(),
                reduce_op.into(),
                self.inner,
                stream.stream(),
            ))?)
        }
    }

    /// Broadcast the tensor data on the `root` rank to all the others.
    ///
    /// See `torch.distributed.broadcast` for more detailed documentation.
    pub fn broadcast(
        &mut self,
        tensor: &TensorCell,
        root: i32,
        stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        let tensor = tensor.borrow_mut();
        check_tensor(&tensor, false)?;
        let data_type: DataType = tensor.scalar_type().try_into()?;
        // SAFETY: intended use of C function
        unsafe {
            Ok(nccl_check(ncclBroadcast(
                tensor.data_ptr(),
                tensor.mut_data_ptr(),
                tensor.numel() as usize,
                data_type.into(),
                root,
                self.inner,
                stream.stream(),
            ))?)
        }
    }

    /// Reduce the tensor data across all ranks, writing the result out to
    /// tensor on the `root` rank.
    ///
    /// See `torch.distributed.reduce` for more detailed documentation.
    pub fn reduce(
        &mut self,
        tensor: &TensorCell,
        reduce_op: ReduceOp,
        root: i32,
        stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        let tensor = tensor.borrow_mut();
        check_tensor(&tensor, false)?;
        if is_float8_type(tensor.scalar_type()) {
            return Err(NcclError::Float8Reduction);
        }
        let data_type: DataType = tensor.scalar_type().try_into()?;
        // SAFETY: intended use of C function
        unsafe {
            Ok(nccl_check(ncclReduce(
                tensor.data_ptr(),
                tensor.mut_data_ptr(),
                tensor.numel() as usize,
                data_type.into(),
                reduce_op.into(),
                root,
                self.inner,
                stream.stream(),
            ))?)
        }
    }

    /// Gather tensors from all ranks into a list of output tensors.
    ///
    /// See `torch.distributed.all_gather` for more detailed documentation.
    pub fn all_gather(
        &mut self,
        output_cells: &[TensorCell],
        input_cell: &TensorCell,
        stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        let output = output_cells
            .iter()
            .map(|t| t.borrow_mut())
            .collect::<Vec<_>>();
        let input = input_cell.borrow();
        check_tensor(&input, false)?;
        let output_type = output[0].scalar_type();
        let output_numel: i64 = output.iter().map(|t| t.numel()).sum();
        for t in &output {
            if t.scalar_type() != output_type {
                return Err(NcclError::TypeMismatch);
            }
        }
        if input.scalar_type() != output_type {
            return Err(NcclError::TypeMismatch);
        }
        if input.numel() * self.world_size as i64 != output_numel {
            return Err(NcclError::OutputSizeMismatch);
        }
        let data_type: DataType = input.scalar_type().try_into()?;
        // TODO: optimization if the output list are all the same shape, where
        // a single allGather can be done.
        // SAFETY: intended use of C function
        unsafe {
            nccl_check(ncclGroupStart())?;
            for (i, output) in output.iter().enumerate() {
                // auto& input = (i == rank_) ? inputTensor : output;
                let rank = i as i32;
                let output_ptr = output.mut_data_ptr();
                // If the current rank is the sender, we need to broadcast the input tensor.
                // Everything else just broadcasts the output tensor.
                if rank == self.rank {
                    nccl_check(ncclBroadcast(
                        input.data_ptr(),
                        output_ptr,
                        input.numel() as usize,
                        data_type.into(),
                        rank,
                        self.inner,
                        stream.stream(),
                    ))?;
                } else {
                    nccl_check(ncclBroadcast(
                        output_ptr,
                        output_ptr,
                        output.numel() as usize,
                        data_type.into(),
                        rank,
                        self.inner,
                        stream.stream(),
                    ))?;
                }
            }
            nccl_check(ncclGroupEnd())?;
        }
        Ok(NcclStatus::Success)
    }

    /// Gather tensors from all ranks into a single output tensor.
    ///
    /// See `torch.distributed.all_gather_into_tensor` for more detailed
    /// documentation.
    pub fn all_gather_into_tensor(
        &mut self,
        output_cell: &TensorCell,
        input_cell: &TensorCell,
        stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        let output = output_cell.borrow_mut();
        let _input_borrow = if input_cell.aliases(output_cell) {
            None
        } else {
            Some(input_cell.borrow())
        };
        // SAFETY: we either borrowed above or borrowed an alias
        let input = unsafe { input_cell.get_unchecked() };
        check_tensor(&output, false)?;
        check_tensor(input, false)?;
        if input.scalar_type() != output.scalar_type() {
            return Err(NcclError::TypeMismatch);
        }
        if input.numel() * self.world_size as i64 != output.numel() {
            return Err(NcclError::OutputSizeMismatch);
        }

        let data_type: DataType = input.scalar_type().try_into()?;
        // SAFETY: intended use of C function
        unsafe {
            Ok(nccl_check(ncclAllGather(
                input.data_ptr(),
                output.mut_data_ptr(),
                input.numel() as usize,
                data_type.into(),
                self.inner,
                stream.stream(),
            ))?)
        }
    }

    /// Reduce, then scatters the result to all tensors in the group.
    ///
    /// See `torch.distributed.reduce_scatter_tensor` for more detailed
    /// documentation.
    pub fn reduce_scatter_tensor(
        &mut self,
        output_cell: &TensorCell,
        input_cell: &TensorCell,
        reduce_op: ReduceOp,
        stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        let output = output_cell.borrow_mut();
        let _input_borrow = if input_cell.aliases(output_cell) {
            None
        } else {
            Some(input_cell.borrow())
        };

        // SAFETY: we either borrowed above or borrowed an alias
        let input = unsafe { input_cell.get_unchecked() }; // SAFETY: intended use of C function

        check_tensor(&output, false)?;
        check_tensor(input, false)?;
        if input.scalar_type() != output.scalar_type() {
            return Err(NcclError::TypeMismatch);
        }
        if input.numel() != output.numel() * self.world_size as i64 {
            return Err(NcclError::InputSizeMismatch);
        }
        if is_float8_type(input.scalar_type()) {
            return Err(NcclError::Float8Reduction);
        }

        let data_type: DataType = input.scalar_type().try_into()?;
        // SAFETY: intended use of C function
        unsafe {
            Ok(nccl_check(ncclReduceScatter(
                input.data_ptr(),
                output.mut_data_ptr(),
                output.numel() as usize,
                data_type.into(),
                reduce_op.into(),
                self.inner,
                stream.stream(),
            ))?)
        }
    }

    /// Send a tensor to the rank `dst`.
    pub fn send(
        &mut self,
        tensor_cell: &TensorCell,
        dst: i32,
        stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        let tensor = tensor_cell.borrow();
        let data_type: DataType = tensor.scalar_type().try_into()?;

        check_tensor(&tensor, true)?;

        // SAFETY: intended use of C function
        unsafe {
            Ok(nccl_check(ncclSend(
                tensor.data_ptr(),
                tensor.numel() as usize,
                data_type.into(),
                dst,
                self.inner,
                stream.stream(),
            ))?)
        }
    }

    /// Receive a tensor from the rank `src`.
    pub fn recv(
        &mut self,
        tensor_cell: &TensorCell,
        src: i32,
        stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        let tensor = tensor_cell.borrow_mut();
        let data_type: DataType = tensor.scalar_type().try_into()?;

        check_tensor(&tensor, true)?;

        // SAFETY: intended use of C function
        unsafe {
            Ok(nccl_check(ncclRecv(
                tensor.mut_data_ptr(),
                tensor.numel() as usize,
                data_type.into(),
                src,
                self.inner,
                stream.stream(),
            ))?)
        }
    }

    /// Split the input tensor then scatter the split list to all processes in
    /// the group. The received splits are then concatenated into the output tensor.
    ///
    /// See `torch.distributed.all_to_all_single` for more detailed
    /// documentation.
    pub fn all_to_all_single(
        &mut self,
        output_cell: &TensorCell,
        input_cell: &TensorCell,
        stream: &Stream,
    ) -> Result<NcclStatus, NcclError> {
        let output = output_cell.borrow_mut();
        let _input_borrow = if input_cell.aliases(output_cell) {
            None
        } else {
            Some(input_cell.borrow_mut())
        };
        // SAFETY: we either borrowed above or borrowed an alias
        let input = unsafe { input_cell.get_unchecked() };

        check_tensor(&output, false)?;
        check_tensor(input, false)?;
        if input.scalar_type() != output.scalar_type() {
            return Err(NcclError::TypeMismatch);
        }

        let data_type: DataType = input.scalar_type().try_into()?;
        let count = input.numel() as usize / self.world_size as usize;
        let rank_stride = input.nbytes() as isize / self.world_size as isize;
        // SAFETY: intended use of C functions
        unsafe {
            let send_buff = input.data_ptr();
            let recv_buff = output.mut_data_ptr();

            nccl_check(ncclGroupStart())?;
            for r in 0..self.world_size {
                nccl_check(ncclSend(
                    send_buff.offset(r as isize * rank_stride),
                    count,
                    data_type.into(),
                    r,
                    self.inner,
                    stream.stream(),
                ))?;
                nccl_check(ncclRecv(
                    recv_buff.offset(r as isize * rank_stride),
                    count,
                    data_type.into(),
                    r,
                    self.inner,
                    stream.stream(),
                ))?;
            }

            nccl_check(ncclGroupEnd())?;
        };
        Ok(NcclStatus::Success)
    }

    /// Synchronize all ranks.
    ///
    /// See `torch.distributed.barrier` for more detailed documentation.
    pub fn barrier(&mut self, stream: &Stream) -> Result<NcclStatus, NcclError> {
        let tensor = factory_float_tensor(&[1.0], self.device.into());
        let data_type: DataType = tensor.scalar_type().try_into()?;

        // NOTE(agallagher): NCCL doesn't have a native barrier impl, so use
        // `ncclAllReduce` to implement one.
        // SAFETY: intended use of C function
        unsafe {
            Ok(nccl_check(ncclAllReduce(
                tensor.data_ptr(),
                tensor.mut_data_ptr(),
                tensor.numel() as usize,
                data_type.into(),
                ReduceOp::Sum.into(),
                self.inner,
                stream.stream(),
            ))?)
        }
    }
}

#[cfg(test)]
mod tests {
    use torch_sys2::CudaDevice;
    use torch_sys2::DeviceIndex;
    use torch_sys2::factory_float_tensor;
    use torch_sys2::testing::allclose;
    use torch_sys2::testing::cuda_full;
    use torch_sys2::testing::stack;

    use super::*;
    use crate::cuda::set_device;

    /// Initialize Python and import torch in a separate thread.
    /// This is a workaround for a pybind11 bug in PyTorch.
    fn test_setup() {
        pyo3::prepare_freethreaded_python();
        let handle = std::thread::spawn(|| {
            pyo3::Python::with_gil(|py| {
                py.import("torch").expect("failed to import torch");
            });
        });
        handle.join().expect("failed to join torch import thread");
    }

    #[test]
    fn all_reduce() {
        test_setup();
        let unique_id = UniqueId::new().unwrap();
        let mut handles = Vec::new();
        for i in 0..2 {
            let unique_id = unique_id.clone();
            handles.push(std::thread::spawn(move || {
                let device = CudaDevice::new(DeviceIndex(i));
                set_device(device).unwrap();
                let stream = Stream::new();
                let tensor = cuda_full(&[2, 2], 1.0);
                let expected = cuda_full(&[2, 2], 2.0);

                let cell = TensorCell::new(tensor);
                let mut comm = Communicator::new(device, 2, unique_id, i.into()).unwrap();
                comm.all_reduce(&cell, ReduceOp::Sum, &stream).unwrap();
                stream.synchronize();
                assert!(allclose(&cell.borrow(), &expected).unwrap());
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn broadcast() {
        test_setup();
        let unique_id = UniqueId::new().unwrap();
        let mut handles = Vec::new();
        for i in 0..2 {
            let unique_id = unique_id.clone();
            handles.push(std::thread::spawn(move || {
                let device = CudaDevice::new(DeviceIndex(i));
                set_device(device).unwrap();
                let stream = Stream::new();
                let tensor = cuda_full(&[2, 2], i as f32);

                let cell = TensorCell::new(tensor);
                let mut comm = Communicator::new(device, 2, unique_id, i.into()).unwrap();
                comm.broadcast(&cell, 1, &stream).unwrap();
                stream.synchronize();
                assert!(allclose(&cell.borrow(), &cuda_full(&[2, 2], 1.0)).unwrap());
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn reduce() {
        test_setup();
        let unique_id = UniqueId::new().unwrap();
        let mut handles = Vec::new();
        for i in 0..2 {
            let unique_id = unique_id.clone();
            handles.push(std::thread::spawn(move || {
                let device = CudaDevice::new(DeviceIndex(i));
                set_device(device).unwrap();
                let stream = Stream::new();
                let tensor = cuda_full(&[2, 2], 2.0);

                let cell = TensorCell::new(tensor);
                let mut comm = Communicator::new(device, 2, unique_id, i.into()).unwrap();
                comm.reduce(&cell, ReduceOp::Sum, 0, &stream).unwrap();
                stream.synchronize();
                match i {
                    0 => assert!(allclose(&cell.borrow(), &cuda_full(&[2, 2], 4.0)).unwrap()),
                    1 => assert!(allclose(&cell.borrow(), &cuda_full(&[2, 2], 2.0)).unwrap()),
                    _ => unreachable!(),
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn all_gather_into_tensor() {
        test_setup();
        let unique_id = UniqueId::new().unwrap();
        let mut handles = Vec::new();
        for i in 0..2 {
            let unique_id = unique_id.clone();
            handles.push(std::thread::spawn(move || {
                let device = CudaDevice::new(DeviceIndex(i));
                set_device(device).unwrap();
                let stream = Stream::new();
                let input_tensor = cuda_full(&[2, 2], i as f32);
                let output_tensor = cuda_full(&[2, 2, 2], 0.0);

                let expected = {
                    let mut tensor_list = Vec::new();
                    for i in 0..2 {
                        tensor_list.push(cuda_full(&[2, 2], i as f32));
                    }
                    stack(&tensor_list)
                };
                let input_cell = TensorCell::new(input_tensor);
                let output_cell = TensorCell::new(output_tensor);
                let mut comm = Communicator::new(device, 2, unique_id, i.into()).unwrap();
                comm.all_gather_into_tensor(&output_cell, &input_cell, &stream)
                    .unwrap();
                stream.synchronize();
                assert!(allclose(&output_cell.borrow(), &expected).unwrap());
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn send_recv() {
        test_setup();
        let unique_id = UniqueId::new().unwrap();
        let mut handles = Vec::new();
        let unique_id_ = unique_id.clone();
        handles.push(std::thread::spawn(move || {
            let device = CudaDevice::new(DeviceIndex(0));
            set_device(device).unwrap();
            let stream = Stream::new();
            let tensor = cuda_full(&[2, 2], 0.0);

            let cell = TensorCell::new(tensor);
            let mut comm = Communicator::new(device, 2, unique_id_, 0).unwrap();
            comm.send(&cell, 1, &stream).unwrap();
            stream.synchronize();
        }));
        let unique_id_ = unique_id.clone();
        handles.push(std::thread::spawn(move || {
            let device = CudaDevice::new(DeviceIndex(1));
            set_device(device).unwrap();
            let stream = Stream::new();
            let tensor = cuda_full(&[2, 2], 1.1);
            let expected = cuda_full(&[2, 2], 0.0);

            let cell = TensorCell::new(tensor);
            let mut comm = Communicator::new(device, 2, unique_id_, 1).unwrap();
            comm.recv(&cell, 0, &stream).unwrap();
            stream.synchronize();
            assert!(allclose(&cell.borrow(), &expected).unwrap());
        }));
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn all_to_all_single() {
        test_setup();
        let unique_id = UniqueId::new().unwrap();
        let mut handles = Vec::new();
        for i in 0..2 {
            let unique_id = unique_id.clone();
            handles.push(std::thread::spawn(move || {
                let device = CudaDevice::new(DeviceIndex(i));
                set_device(device).unwrap();
                let stream = Stream::new();
                let input = match i {
                    0 => factory_float_tensor(&[0.0, 1.0], device.into()),
                    1 => factory_float_tensor(&[2.0, 3.0], device.into()),
                    _ => unreachable!(),
                };
                let output = cuda_full(&[2], 0.0);

                let input = TensorCell::new(input);
                let output = TensorCell::new(output);

                let mut comm = Communicator::new(device, 2, unique_id, i.into()).unwrap();
                comm.all_to_all_single(&output, &input, &stream).unwrap();
                stream.synchronize();

                let expected = match i {
                    0 => factory_float_tensor(&[0.0, 2.0], device.into()),
                    1 => factory_float_tensor(&[1.0, 3.0], device.into()),
                    _ => unreachable!(),
                };
                assert!(allclose(&output.borrow(), &expected).unwrap());
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn reduce_scatter_tensor() {
        test_setup();
        let unique_id = UniqueId::new().unwrap();
        let mut handles = Vec::new();
        for i in 0..2 {
            let unique_id = unique_id.clone();
            handles.push(std::thread::spawn(move || {
                let device = CudaDevice::new(DeviceIndex(i));
                set_device(device).unwrap();
                let stream = Stream::new();
                let input = factory_float_tensor(&[0.0, 1.0, 2.0, 3.0], device.into());
                let output = cuda_full(&[2], 1.0);

                let input = TensorCell::new(input);
                let output = TensorCell::new(output);

                let mut comm = Communicator::new(device, 2, unique_id, i.into()).unwrap();
                comm.reduce_scatter_tensor(&output, &input, ReduceOp::Sum, &stream)
                    .unwrap();
                stream.synchronize();

                let expected = match i {
                    0 => factory_float_tensor(&[0.0, 2.0], device.into()),
                    1 => factory_float_tensor(&[4.0, 6.0], device.into()),
                    _ => unreachable!(),
                };
                assert!(allclose(&output.borrow(), &expected).unwrap());
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn split_from() {
        test_setup();
        let unique_id = UniqueId::new().unwrap();
        let mut handles = Vec::new();
        for i in 0..2 {
            let unique_id = unique_id.clone();
            handles.push(std::thread::spawn(move || {
                let device = CudaDevice::new(DeviceIndex(i));
                set_device(device).unwrap();
                let stream = Stream::new();
                let tensor = cuda_full(&[2, 2], 1.0);
                let cell = TensorCell::new(tensor);
                let mut comm = Communicator::new(device, 2, unique_id, i.into()).unwrap();

                // Split a new comm with only rank 0
                let split_comm = comm.split_from(vec![0], None).unwrap();

                match i {
                    0 => assert!(split_comm.is_some()),
                    1 => assert!(split_comm.is_none()),
                    _ => unreachable!(),
                };

                match i {
                    0 => {
                        split_comm
                            .unwrap()
                            .all_reduce(&cell, ReduceOp::Sum, &stream)
                            .unwrap();
                        stream.synchronize();
                        let expected = cuda_full(&[2, 2], 1.0);
                        assert!(allclose(&cell.borrow(), &expected).unwrap());
                    }
                    1 => (),
                    _ => unreachable!(),
                };
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }
}
