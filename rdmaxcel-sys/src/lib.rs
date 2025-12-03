/*
 * Portions Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// sections of code adapted from https://github.com/jonhoo/rust-ibverbs
// Copyright (c) 2016 Jon Gjengset under MIT License (MIT)

mod inner {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(unused_attributes)]
    #[cfg(not(cargo))]
    use crate::ibv_wc_flags;
    #[cfg(not(cargo))]
    use crate::ibv_wc_opcode;
    #[cfg(not(cargo))]
    use crate::ibv_wc_status;
    #[cfg(cargo)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

    #[repr(C, packed(1))]
    #[derive(Debug, Default, Clone, Copy)]
    pub struct mlx5_wqe_ctrl_seg {
        pub opmod_idx_opcode: u32,
        pub qpn_ds: u32,
        pub signature: u8,
        pub dci_stream_channel_id: u16,
        pub fm_ce_se: u8,
        pub imm: u32,
    }

    #[repr(C)]
    #[derive(Debug, Copy, Clone)]
    pub struct ibv_wc {
        wr_id: u64,
        status: ibv_wc_status::Type,
        opcode: ibv_wc_opcode::Type,
        vendor_err: u32,
        byte_len: u32,

        /// Immediate data OR the local RKey that was invalidated depending on `wc_flags`.
        /// See `man ibv_poll_cq` for details.
        pub imm_data: u32,
        /// Local QP number of completed WR.
        ///
        /// Relevant for Receive Work Completions that are associated with an SRQ.
        pub qp_num: u32,
        /// Source QP number (remote QP number) of completed WR.
        ///
        /// Relevant for Receive Work Completions of a UD QP.
        pub src_qp: u32,
        /// Flags of the Work Completion. It is either 0 or the bitwise OR of one or more of the
        /// following flags:
        ///
        ///  - `IBV_WC_GRH`: Indicator that GRH is present for a Receive Work Completions of a UD QP.
        ///    If this bit is set, the first 40 bytes of the buffered that were referred to in the
        ///    Receive request will contain the GRH of the incoming message. If this bit is cleared,
        ///    the content of those first 40 bytes is undefined
        ///  - `IBV_WC_WITH_IMM`: Indicator that imm_data is valid. Relevant for Receive Work
        ///    Completions
        pub wc_flags: ibv_wc_flags,
        /// P_Key index (valid only for GSI QPs).
        pub pkey_index: u16,
        /// Source LID (the base LID that this message was sent from).
        ///
        /// Relevant for Receive Work Completions of a UD QP.
        pub slid: u16,
        /// Service Level (the SL LID that this message was sent with).
        ///
        /// Relevant for Receive Work Completions of a UD QP.
        pub sl: u8,
        /// Destination LID path bits.
        ///
        /// Relevant for Receive Work Completions of a UD QP (not applicable for multicast messages).
        pub dlid_path_bits: u8,
    }

    #[allow(clippy::len_without_is_empty)]
    impl ibv_wc {
        /// Returns the 64 bit value that was associated with the corresponding Work Request.
        pub fn wr_id(&self) -> u64 {
            self.wr_id
        }

        /// Returns the number of bytes transferred.
        pub fn len(&self) -> usize {
            self.byte_len as usize
        }

        /// Check if this work requested completed successfully.
        pub fn is_valid(&self) -> bool {
            self.status == ibv_wc_status::IBV_WC_SUCCESS
        }

        /// Returns the work completion status and vendor error syndrome if failed.
        pub fn error(&self) -> Option<(ibv_wc_status::Type, u32)> {
            match self.status {
                ibv_wc_status::IBV_WC_SUCCESS => None,
                status => Some((status, self.vendor_err)),
            }
        }

        /// Returns the operation that the corresponding Work Request performed.
        pub fn opcode(&self) -> ibv_wc_opcode::Type {
            self.opcode
        }

        /// Returns immediate data if present.
        pub fn imm_data(&self) -> Option<u32> {
            if self.is_valid() && ((self.wc_flags & ibv_wc_flags::IBV_WC_WITH_IMM).0 != 0) {
                Some(self.imm_data)
            } else {
                None
            }
        }
    }

    impl Default for ibv_wc {
        fn default() -> Self {
            ibv_wc {
                wr_id: 0,
                status: ibv_wc_status::IBV_WC_GENERAL_ERR,
                opcode: ibv_wc_opcode::IBV_WC_LOCAL_INV,
                vendor_err: 0,
                byte_len: 0,
                imm_data: 0,
                qp_num: 0,
                src_qp: 0,
                wc_flags: ibv_wc_flags(0),
                pkey_index: 0,
                slid: 0,
                sl: 0,
                dlid_path_bits: 0,
            }
        }
    }
}

pub use inner::*;

// =============================================================================
// ROCm/HIP Compatibility Aliases
// =============================================================================
// These allow monarch_rdma to use CUDA names transparently on ROCm builds.

// --- Basic Type Aliases ---
#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::hipDeviceptr_t as CUdeviceptr;

#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::hipDevice_t as CUdevice;

#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::hipError_t as CUresult;

#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::hipCtx_t as CUcontext;

// --- Memory Allocation Types ---
#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::hipMemGenericAllocationHandle_t as CUmemGenericAllocationHandle;

#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::hipMemAllocationProp as CUmemAllocationProp;

#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::hipMemAccessDesc as CUmemAccessDesc;

// --- Status/Success Constants ---
#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::HIP_SUCCESS as CUDA_SUCCESS;

// --- Pointer Attribute Constants ---
#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::HIP_POINTER_ATTRIBUTE_MEMORY_TYPE as CU_POINTER_ATTRIBUTE_MEMORY_TYPE;

// --- Memory Allocation Type Constants ---
#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::hipMemAllocationTypePinned as CU_MEM_ALLOCATION_TYPE_PINNED;

// --- Memory Location Type Constants ---
#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::hipMemLocationTypeDevice as CU_MEM_LOCATION_TYPE_DEVICE;

// --- Memory Handle Type Constants ---
#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::hipMemHandleTypePosixFileDescriptor as CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;

// --- Memory Allocation Granularity Constants ---
#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::hipMemAllocationGranularityMinimum as CU_MEM_ALLOC_GRANULARITY_MINIMUM;

// --- Memory Access Flags Constants ---
#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::hipMemAccessFlagsProtReadWrite as CU_MEM_ACCESS_FLAGS_PROT_READWRITE;

// --- Dmabuf Handle Type Constants ---
#[cfg(rocm_6_x)]
pub const CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD: i32 = 0;

#[cfg(rocm_7_plus)]
pub use inner::hipMemRangeHandleTypeDmaBufFd as CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD;

// --- Driver Init/Device Functions ---
#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::rdmaxcel_hipInit as rdmaxcel_cuInit;

#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::rdmaxcel_hipDeviceGet as rdmaxcel_cuDeviceGet;

#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::rdmaxcel_hipDeviceGetCount as rdmaxcel_cuDeviceGetCount;

#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::rdmaxcel_hipPointerGetAttribute as rdmaxcel_cuPointerGetAttribute;

// --- Context Functions ---
#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::rdmaxcel_hipCtxCreate as rdmaxcel_cuCtxCreate_v2;

#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::rdmaxcel_hipCtxSetCurrent as rdmaxcel_cuCtxSetCurrent;

// --- Error Handling Functions ---
#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::rdmaxcel_hipGetErrorString as rdmaxcel_cuGetErrorString;

// --- Memory Management Functions ---
#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::rdmaxcel_hipMemGetAllocationGranularity as rdmaxcel_cuMemGetAllocationGranularity;

#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::rdmaxcel_hipMemCreate as rdmaxcel_cuMemCreate;

#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::rdmaxcel_hipMemAddressReserve as rdmaxcel_cuMemAddressReserve;

#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::rdmaxcel_hipMemMap as rdmaxcel_cuMemMap;

#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::rdmaxcel_hipMemSetAccess as rdmaxcel_cuMemSetAccess;

#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::rdmaxcel_hipMemUnmap as rdmaxcel_cuMemUnmap;

#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::rdmaxcel_hipMemAddressFree as rdmaxcel_cuMemAddressFree;

#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::rdmaxcel_hipMemRelease as rdmaxcel_cuMemRelease;

// --- Memory Copy/Set Functions ---
#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::rdmaxcel_hipMemcpyHtoD as rdmaxcel_cuMemcpyHtoD_v2;

#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::rdmaxcel_hipMemcpyDtoH as rdmaxcel_cuMemcpyDtoH_v2;

#[cfg(any(rocm_6_x, rocm_7_plus))]
pub use inner::rdmaxcel_hipMemsetD8 as rdmaxcel_cuMemsetD8_v2;

// --- Dmabuf Function ---
// ROCm 7+: direct alias to HIP function
#[cfg(rocm_7_plus)]
pub use inner::rdmaxcel_hipMemGetHandleForAddressRange as rdmaxcel_cuMemGetHandleForAddressRange;

// ROCm 6.x: uses the CUDA-compatible wrapper we added in build.rs that internally calls HSA
#[cfg(rocm_6_x)]
pub use inner::rdmaxcel_cuMemGetHandleForAddressRange;

// RDMA error string function and utility functions
unsafe extern "C" {
    pub fn rdmaxcel_error_string(error_code: std::os::raw::c_int) -> *const std::os::raw::c_char;
    pub fn get_cuda_pci_address_from_ptr(
        cuda_ptr: u64,
        pci_addr_out: *mut std::os::raw::c_char,
        pci_addr_size: usize,
    ) -> std::os::raw::c_int;

    /// Debug: Print comprehensive device attributes
    pub fn rdmaxcel_print_device_info(context: *mut ibv_context);
}
