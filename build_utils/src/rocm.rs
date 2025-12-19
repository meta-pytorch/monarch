/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! ROCm-specific build utilities for patching hipified CUDA code.
//!
//! This module provides functions to patch hipified source files for
//! compatibility with different ROCm versions:
//! - [`patch_hipified_files_rocm7`] for ROCm 7.0+ (native hipMemGetHandleForAddressRange)
//! - [`patch_hipified_files_rocm6`] for ROCm 6.x (uses HSA hsa_amd_portable_export_dmabuf)
//!
//! IMPORTANT: Both paths keep wrapper function names as `rdmaxcel_cu*` for API stability.
//! Only the internal implementations differ (HIP vs HSA).

use std::fs;
use std::path::Path;

// =============================================================================
// Replacement tables
// =============================================================================

/// CUDA CU_* constants → HIP equivalents
/// hipify_torch does not convert these constants in rdmaxcel_hip.cpp
const CUDA_CONSTANT_REPLACEMENTS: &[(&str, &str)] = &[
    ("CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD", "hipMemRangeHandleTypeDmaBufFd"),
    ("CU_DEVICE_ATTRIBUTE_PCI_BUS_ID", "hipDeviceAttributePciBusId"),
    ("CU_DEVICE_ATTRIBUTE_PCI_DEVICE_ID", "hipDeviceAttributePciDeviceId"),
    ("CU_DEVICE_ATTRIBUTE_PCI_DOMAIN_ID", "hipDeviceAttributePciDomainID"),
    ("CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL", "HIP_POINTER_ATTRIBUTE_DEVICE_ORDINAL"),
    ("CUDA_SUCCESS", "hipSuccess"),
];

/// CUDA type replacements that hipify_torch may miss
const CUDA_TYPE_REPLACEMENTS: &[(&str, &str)] = &[
    ("CUresult", "hipError_t"),
    ("CUdevice device", "hipDevice_t device"),
    ("CUmemRangeHandleType", "hipMemRangeHandleType"),
];

/// Macro entry replacements for driver_api_hip.cpp: _(cuXxx) → _(hipXxx)
/// These are entries in the RDMAXCEL_CUDA_DRIVER_API macro for dlsym lookups
const MACRO_ENTRY_REPLACEMENTS: &[(&str, &str)] = &[
    ("_(cuMemGetHandleForAddressRange)", "_(hipMemGetHandleForAddressRange)"),
    ("_(cuMemGetAllocationGranularity)", "_(hipMemGetAllocationGranularity)"),
    ("_(cuMemCreate)", "_(hipMemCreate)"),
    ("_(cuMemAddressReserve)", "_(hipMemAddressReserve)"),
    ("_(cuMemMap)", "_(hipMemMap)"),
    ("_(cuMemSetAccess)", "_(hipMemSetAccess)"),
    ("_(cuMemUnmap)", "_(hipMemUnmap)"),
    ("_(cuMemAddressFree)", "_(hipMemAddressFree)"),
    ("_(cuMemRelease)", "_(hipMemRelease)"),
    ("_(cuMemcpyHtoD_v2)", "_(hipMemcpyHtoD)"),
    ("_(cuMemcpyDtoH_v2)", "_(hipMemcpyDtoH)"),
    ("_(cuMemsetD8_v2)", "_(hipMemsetD8)"),
    ("_(cuPointerGetAttribute)", "_(hipPointerGetAttribute)"),
    ("_(cuInit)", "_(hipInit)"),
    ("_(cuDeviceGet)", "_(hipDeviceGet)"),
    ("_(cuDeviceGetCount)", "_(hipGetDeviceCount)"),
    ("_(cuDeviceGetAttribute)", "_(hipDeviceGetAttribute)"),
    ("_(cuCtxCreate_v2)", "_(hipCtxCreate)"),
    ("_(cuCtxSetCurrent)", "_(hipCtxSetCurrent)"),
    ("_(cuCtxSynchronize)", "_(hipCtxSynchronize)"),
    ("_(cuGetErrorString)", "_(hipDrvGetErrorString)"),
];

/// Struct member access replacements for driver_api_hip.cpp wrapper implementations
/// These fix the ->cuXxx_( calls inside the wrapper functions
const MEMBER_ACCESS_REPLACEMENTS: &[(&str, &str)] = &[
    ("->cuMemGetHandleForAddressRange_(", "->hipMemGetHandleForAddressRange_("),
    ("->cuMemGetAllocationGranularity_(", "->hipMemGetAllocationGranularity_("),
    ("->cuMemCreate_(", "->hipMemCreate_("),
    ("->cuMemAddressReserve_(", "->hipMemAddressReserve_("),
    ("->cuMemMap_(", "->hipMemMap_("),
    ("->cuMemSetAccess_(", "->hipMemSetAccess_("),
    ("->cuMemUnmap_(", "->hipMemUnmap_("),
    ("->cuMemAddressFree_(", "->hipMemAddressFree_("),
    ("->cuMemRelease_(", "->hipMemRelease_("),
    ("->cuMemcpyHtoD_v2_(", "->hipMemcpyHtoD_("),
    ("->cuMemcpyDtoH_v2_(", "->hipMemcpyDtoH_("),
    ("->cuMemsetD8_v2_(", "->hipMemsetD8_("),
    ("->cuPointerGetAttribute_(", "->hipPointerGetAttribute_("),
    ("->cuInit_(", "->hipInit_("),
    ("->cuDeviceGet_(", "->hipDeviceGet_("),
    ("->cuDeviceGetCount_(", "->hipGetDeviceCount_("),
    ("->cuDeviceGetAttribute_(", "->hipDeviceGetAttribute_("),
    ("->cuCtxCreate_v2_(", "->hipCtxCreate_("),
    ("->cuCtxSetCurrent_(", "->hipCtxSetCurrent_("),
    ("->cuCtxSynchronize_(", "->hipCtxSynchronize_("),
    ("->cuGetErrorString_(", "->hipDrvGetErrorString_("),
];

// =============================================================================
// Public API
// =============================================================================

/// Apply ROCm 7+ specific patches to hipified files.
///
/// ROCm 7+ has native `hipMemGetHandleForAddressRange` support.
/// 
/// Key design: We keep wrapper function names as `rdmaxcel_cu*` for API stability.
/// Only the internal HIP API calls are converted.
pub fn patch_hipified_files_rocm7(hip_src_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:warning=Patching hipified sources for ROCm 7.0+...");

    // rdmaxcel_hip.cpp - fix constants and types, keep wrapper function names as rdmaxcel_cu*
    patch_file(hip_src_dir, "rdmaxcel_hip.cpp", |c| patch_rdmaxcel_cpp_rocm7(&c))?;
    
    // Header needs driver_api include path fix
    patch_file(hip_src_dir, "rdmaxcel_hip.h", patch_rdmaxcel_h)?;
    
    // driver_api_hip.h - fix any remaining CUDA types
    patch_file(hip_src_dir, "driver_api_hip.h", |c| patch_driver_api_h_rocm7(&c))?;
    
    // driver_api_hip.cpp - comprehensive patching: macro entries, member access, types
    patch_file(hip_src_dir, "driver_api_hip.cpp", |c| patch_driver_api_cpp_rocm7(&c))?;

    Ok(())
}

/// Apply ROCm 6.x specific patches to hipified files.
///
/// ROCm 6.x does not have `hipMemGetHandleForAddressRange`, so we use
/// HSA's `hsa_amd_portable_export_dmabuf` instead.
///
/// Key design: We keep wrapper function names as `rdmaxcel_cu*` for API stability.
pub fn patch_hipified_files_rocm6(hip_src_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:warning=Patching hipified sources for ROCm 6.x (HSA dmabuf)...");

    patch_file(hip_src_dir, "rdmaxcel_hip.cpp", |c| patch_rdmaxcel_cpp_rocm6(&c))?;
    patch_file(hip_src_dir, "rdmaxcel_hip.h", patch_rdmaxcel_h)?;
    patch_file(hip_src_dir, "driver_api_hip.h", |c| patch_driver_api_h_rocm6(&c))?;
    patch_file(hip_src_dir, "driver_api_hip.cpp", |c| {
        let patched = patch_driver_api_cpp_rocm6(&c);
        patch_for_dlopen(&patched)
    })?;

    println!("cargo:warning=Applied dlopen patches for HIP/HSA functions");
    Ok(())
}

/// Validate that required hipified files exist after hipification.
pub fn validate_hipified_files(hip_src_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    const REQUIRED: &[&str] = &["rdmaxcel_hip.h", "rdmaxcel_hip.c", "rdmaxcel_hip.cpp", "rdmaxcel.hip"];

    for name in REQUIRED {
        let path = hip_src_dir.join(name);
        if !path.exists() {
            return Err(format!(
                "Required hipified file '{}' not found in {}",
                name,
                hip_src_dir.display()
            ).into());
        }
    }
    Ok(())
}

// =============================================================================
// Internal helpers
// =============================================================================

/// Read, transform, and write a file. No-op if file doesn't exist.
fn patch_file<F>(dir: &Path, name: &str, f: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnOnce(&str) -> String,
{
    let path = dir.join(name);
    if path.exists() {
        let content = fs::read_to_string(&path)?;
        fs::write(&path, f(&content))?;
    }
    Ok(())
}

/// Apply a list of string replacements
fn apply_replacements(content: &str, replacements: &[(&str, &str)]) -> String {
    let mut result = content.to_string();
    for (from, to) in replacements {
        result = result.replace(from, to);
    }
    result
}

// =============================================================================
// ROCm 7+ patches
// =============================================================================

fn patch_rdmaxcel_h(content: &str) -> String {
    content
        .replace("#include \"driver_api.h\"", "#include \"driver_api_hip.h\"")
        .replace("CUdeviceptr", "hipDeviceptr_t")
}

/// Patch rdmaxcel_hip.cpp for ROCm 7+
/// 
/// IMPORTANT: We do NOT rename wrapper function calls (rdmaxcel_cu* stays rdmaxcel_cu*)
/// We only fix:
/// - CU_* constants → HIP equivalents  
/// - Type conversions
/// - c10 namespace references
fn patch_rdmaxcel_cpp_rocm7(content: &str) -> String {
    let mut result = content.to_string();
    
    // Add hip_version.h include
    result = result.replace(
        "#include <hip/hip_runtime.h>",
        "#include <hip/hip_runtime.h>\n#include <hip/hip_version.h>"
    );
    
    // Fix c10 namespace (hipify sometimes misses nested references)
    result = result
        .replace("c10::cuda::CUDACachingAllocator", "c10::hip::HIPCachingAllocator")
        .replace("c10::cuda::CUDAAllocatorConfig", "c10::hip::HIPAllocatorConfig")
        .replace("c10::hip::HIPCachingAllocator::CUDAAllocatorConfig",
                 "c10::hip::HIPCachingAllocator::HIPAllocatorConfig")
        .replace("CUDAAllocatorConfig::", "HIPAllocatorConfig::");
    
    // Fix static_cast to reinterpret_cast for device pointers
    result = result
        .replace("static_cast<CUdeviceptr>", "reinterpret_cast<hipDeviceptr_t>")
        .replace("static_cast<hipDeviceptr_t>", "reinterpret_cast<hipDeviceptr_t>");
    
    // Fix PCI domain attribute case (hipify produces wrong case)
    result = result.replace("hipDeviceAttributePciDomainId", "hipDeviceAttributePciDomainID");
    
    // Apply CU_* constant replacements
    result = apply_replacements(&result, CUDA_CONSTANT_REPLACEMENTS);
    
    // Apply type replacements
    result = apply_replacements(&result, CUDA_TYPE_REPLACEMENTS);
    
    // NOTE: We intentionally do NOT rename wrapper function calls here.
    // The wrapper functions keep their rdmaxcel_cu* names for API stability.
    
    result
}

/// Patch driver_api_hip.h for ROCm 7+
/// hipify_torch handles most conversions, but may miss some types
fn patch_driver_api_h_rocm7(content: &str) -> String {
    // Apply type replacements (hipify may miss CUmemRangeHandleType)
    // Do NOT rename wrapper function declarations - they stay as rdmaxcel_cu*
    apply_replacements(content, CUDA_TYPE_REPLACEMENTS)
}

/// Patch driver_api_hip.cpp for ROCm 7+
/// This needs comprehensive patching because hipify_torch doesn't convert:
/// 1. Macro entries like _(cuMemCreate) in RDMAXCEL_CUDA_DRIVER_API
/// 2. Struct member access like ->cuMemCreate_( in wrapper implementations
/// 3. Some types
///
/// NOTE: Wrapper function names (rdmaxcel_cu*) are NOT changed.
fn patch_driver_api_cpp_rocm7(content: &str) -> String {
    let mut result = content.to_string();
    
    // Fix library name
    result = result.replace("libcuda.so.1", "libamdhip64.so");
    
    // Fix runtime header
    result = result.replace("#include <cuda_runtime.h>", "#include <hip/hip_runtime.h>");
    result = result.replace("cudaFree", "hipFree");
    
    // Apply macro entry replacements: _(cuXxx) → _(hipXxx)
    // These are the actual HIP function names for dlsym lookup
    result = apply_replacements(&result, MACRO_ENTRY_REPLACEMENTS);
    
    // Apply struct member access replacements: ->cuXxx_( → ->hipXxx_(
    // These are the function pointers stored in the DriverAPI struct
    result = apply_replacements(&result, MEMBER_ACCESS_REPLACEMENTS);
    
    // Apply type replacements
    result = apply_replacements(&result, CUDA_TYPE_REPLACEMENTS);
    
    // Fix const_cast for HtoD (srcHost needs to be non-const for HIP)
    result = result.replace(
        "dstDevice, srcHost, ByteCount);",
        "dstDevice, const_cast<void*>(srcHost), ByteCount);"
    );
    
    // NOTE: Wrapper function names (rdmaxcel_cu*) are intentionally NOT changed.
    
    result
}

// =============================================================================
// ROCm 6.x patches (HSA dmabuf workaround)
// =============================================================================

/// Patch rdmaxcel_hip.cpp for ROCm 6.x
/// Uses HSA hsa_amd_portable_export_dmabuf instead of hipMemGetHandleForAddressRange
fn patch_rdmaxcel_cpp_rocm6(content: &str) -> String {
    let mut result = content
        .replace("#include <hip/hip_runtime.h>",
                 "#include <hip/hip_runtime.h>\n#include <hip/hip_version.h>\n#include <hsa/hsa.h>\n#include <hsa/hsa_ext_amd.h>")
        .replace("c10::cuda::CUDACachingAllocator", "c10::hip::HIPCachingAllocator")
        .replace("c10::cuda::CUDAAllocatorConfig", "c10::hip::HIPAllocatorConfig")
        .replace("c10::hip::HIPCachingAllocator::CUDAAllocatorConfig",
                 "c10::hip::HIPCachingAllocator::HIPAllocatorConfig")
        .replace("CUDAAllocatorConfig::", "HIPAllocatorConfig::")
        .replace("hipDeviceAttributePciDomainId", "hipDeviceAttributePciDomainID")
        .replace("static_cast<CUdeviceptr>", "reinterpret_cast<hipDeviceptr_t>")
        .replace("static_cast<hipDeviceptr_t>", "reinterpret_cast<hipDeviceptr_t>");

    // Apply constant and type replacements
    result = apply_replacements(&result, CUDA_CONSTANT_REPLACEMENTS);
    result = apply_replacements(&result, CUDA_TYPE_REPLACEMENTS);
    
    // For ROCm 6.x, replace the dmabuf constant with HSA placeholder
    result = result.replace("hipMemRangeHandleTypeDmaBufFd", "0 /* HSA dmabuf */");

    result
}

/// Patch driver_api_hip.h for ROCm 6.x
/// Add HSA includes and fix types. Do NOT rename wrapper functions.
fn patch_driver_api_h_rocm6(content: &str) -> String {
    let mut result = content.to_string();
    
    // Add HSA includes
    if !result.contains("#include <hsa/hsa.h>") {
        result = result.replace(
            "#include <hip/hip_runtime.h>",
            "#include <hip/hip_runtime.h>\n#include <hsa/hsa.h>\n#include <hsa/hsa_ext_amd.h>"
        );
    }
    
    // Apply type replacements only - do NOT rename wrapper function declarations
    result = apply_replacements(&result, CUDA_TYPE_REPLACEMENTS);
    
    result
}

/// Patch driver_api_hip.cpp for ROCm 6.x
/// Converts internal HIP calls and replaces hipMemGetHandleForAddressRange with HSA
fn patch_driver_api_cpp_rocm6(content: &str) -> String {
    let mut result = content.to_string();
    
    // Add HSA includes
    result = result.replace(
        "#include \"driver_api_hip.h\"",
        "#include \"driver_api_hip.h\"\n#include <hsa/hsa.h>\n#include <hsa/hsa_ext_amd.h>"
    );

    // Fix library name and runtime
    result = result
        .replace("libcuda.so.1", "libamdhip64.so")
        .replace("cudaFree", "hipFree")
        .replace("#include <cuda_runtime.h>", "#include <hip/hip_runtime.h>");
    
    // Apply macro entry replacements for dlsym lookups
    result = apply_replacements(&result, MACRO_ENTRY_REPLACEMENTS);
    
    // Apply struct member access replacements
    result = apply_replacements(&result, MEMBER_ACCESS_REPLACEMENTS);
    
    // Apply type replacements
    result = apply_replacements(&result, CUDA_TYPE_REPLACEMENTS);
    
    // Fix const_cast for HtoD
    result = result.replace(
        "dstDevice, srcHost, ByteCount);",
        "dstDevice, const_cast<void*>(srcHost), ByteCount);"
    );
    
    // For ROCm 6.x, hipMemGetHandleForAddressRange doesn't exist
    // Remove it from the macro list and we'll add HSA wrapper separately
    result = result.replace(
        "_(hipMemGetHandleForAddressRange)  \\",
        "/* hipMemGetHandleForAddressRange not available in ROCm 6.x */  \\"
    );
    result = result.replace(
        "_(hipMemGetHandleForAddressRange) \\",
        "/* hipMemGetHandleForAddressRange not available in ROCm 6.x */ \\"
    );
    
    // Replace the wrapper implementation to use HSA
    // The wrapper function name stays as rdmaxcel_cuMemGetHandleForAddressRange
    let old_wrapper = r#"hipError_t rdmaxcel_cuMemGetHandleForAddressRange(
    int* handle,
    hipDeviceptr_t dptr,
    size_t size,
    hipMemRangeHandleType handleType,
    unsigned long long flags) {
  return rdmaxcel::DriverAPI::get()->hipMemGetHandleForAddressRange_(
      handle, dptr, size, handleType, flags);
}"#;
    
    let hsa_wrapper = r#"// ROCm 6.x: Use HSA hsa_amd_portable_export_dmabuf instead of hipMemGetHandleForAddressRange
hipError_t rdmaxcel_cuMemGetHandleForAddressRange(
    int* handle,
    hipDeviceptr_t dptr,
    size_t size,
    hipMemRangeHandleType handleType,
    unsigned long long flags) {
  (void)handleType;
  (void)flags;
  hsa_status_t status = hsa_amd_portable_export_dmabuf(
      reinterpret_cast<void*>(dptr), size, handle, nullptr);
  return (status == HSA_STATUS_SUCCESS) ? hipSuccess : hipErrorUnknown;
}"#;
    
    result = result.replace(old_wrapper, hsa_wrapper);
    
    // Also handle if the type wasn't converted yet
    let old_wrapper2 = r#"hipError_t rdmaxcel_cuMemGetHandleForAddressRange(
    int* handle,
    hipDeviceptr_t dptr,
    size_t size,
    CUmemRangeHandleType handleType,
    unsigned long long flags) {
  return rdmaxcel::DriverAPI::get()->hipMemGetHandleForAddressRange_(
      handle, dptr, size, handleType, flags);
}"#;
    result = result.replace(old_wrapper2, hsa_wrapper);

    result
}

/// Apply dlopen patches to avoid link-time dependencies on HIP/HSA libraries.
fn patch_for_dlopen(content: &str) -> String {
    let mut result = content.to_string();

    // Add hipFree to dlopen macro list if not already there
    if !result.contains("_(hipFree)") {
        result = result.replace(
            "_(hipDrvGetErrorString)",
            "_(hipDrvGetErrorString)              \\\n  _(hipFree)"
        );
    }

    // Reorder DriverAPI::get() to create singleton first, then call hipFree via dlopen
    result = result.replace(
        r#"DriverAPI* DriverAPI::get() {
  // Ensure we have a valid CUDA context for this thread
  hipFree(0);
  static DriverAPI singleton = create_driver_api();
  return &singleton;
}"#,
        r#"DriverAPI* DriverAPI::get() {
  static DriverAPI singleton = create_driver_api();
  // Ensure valid HIP context via dlopen'd hipFree (not direct call)
  singleton.hipFree_(0);
  return &singleton;
}"#
    );

    result
}
