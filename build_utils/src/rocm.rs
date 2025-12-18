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

use std::fs;
use std::path::Path;

// =============================================================================
// Replacement tables - reduces duplication and makes patches easier to maintain
// =============================================================================

/// CUDA → HIP wrapper function name mappings
const WRAPPER_REPLACEMENTS: &[(&str, &str)] = &[
    ("rdmaxcel_cuMemGetAllocationGranularity", "rdmaxcel_hipMemGetAllocationGranularity"),
    ("rdmaxcel_cuMemCreate", "rdmaxcel_hipMemCreate"),
    ("rdmaxcel_cuMemAddressReserve", "rdmaxcel_hipMemAddressReserve"),
    ("rdmaxcel_cuMemMap", "rdmaxcel_hipMemMap"),
    ("rdmaxcel_cuMemSetAccess", "rdmaxcel_hipMemSetAccess"),
    ("rdmaxcel_cuMemUnmap", "rdmaxcel_hipMemUnmap"),
    ("rdmaxcel_cuMemAddressFree", "rdmaxcel_hipMemAddressFree"),
    ("rdmaxcel_cuMemRelease", "rdmaxcel_hipMemRelease"),
    ("rdmaxcel_cuMemcpyHtoD_v2", "rdmaxcel_hipMemcpyHtoD"),
    ("rdmaxcel_cuMemcpyDtoH_v2", "rdmaxcel_hipMemcpyDtoH"),
    ("rdmaxcel_cuMemsetD8_v2", "rdmaxcel_hipMemsetD8"),
    ("rdmaxcel_cuPointerGetAttribute", "rdmaxcel_hipPointerGetAttribute"),
    ("rdmaxcel_cuInit", "rdmaxcel_hipInit"),
    ("rdmaxcel_cuDeviceGetCount", "rdmaxcel_hipDeviceGetCount"),
    ("rdmaxcel_cuDeviceGetAttribute", "rdmaxcel_hipDeviceGetAttribute"),
    ("rdmaxcel_cuDeviceGet", "rdmaxcel_hipDeviceGet"),
    ("rdmaxcel_cuCtxCreate_v2", "rdmaxcel_hipCtxCreate"),
    ("rdmaxcel_cuCtxSetCurrent", "rdmaxcel_hipCtxSetCurrent"),
    ("rdmaxcel_cuGetErrorString", "rdmaxcel_hipGetErrorString"),
];

/// Driver API function pointer replacements (->func_() calls)
const DRIVER_API_PTR_REPLACEMENTS: &[(&str, &str)] = &[
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

/// Driver API macro replacements (_(func) entries)
const DRIVER_API_MACRO_REPLACEMENTS: &[(&str, &str)] = &[
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

// =============================================================================
// Public API
// =============================================================================

/// Apply ROCm 7+ specific patches to hipified files.
///
/// ROCm 7+ has native `hipMemGetHandleForAddressRange` support, so we can
/// use a straightforward CUDA→HIP mapping.
pub fn patch_hipified_files_rocm7(hip_src_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:warning=Patching hipified sources for ROCm 7.0+...");

    patch_file(hip_src_dir, "rdmaxcel_hip.cpp", patch_rdmaxcel_cpp_common)?;
    patch_file(hip_src_dir, "rdmaxcel_hip.h", patch_rdmaxcel_h)?;
    patch_file(hip_src_dir, "driver_api_hip.h", |c| patch_driver_api_h_rocm7(&c))?;
    patch_file(hip_src_dir, "driver_api_hip.cpp", |c| patch_driver_api_cpp_rocm7(&c))?;

    Ok(())
}

/// Apply ROCm 6.x specific patches to hipified files.
///
/// ROCm 6.x does not have `hipMemGetHandleForAddressRange`, so we use
/// HSA's `hsa_amd_portable_export_dmabuf` instead. This requires:
/// - Adding HSA headers
/// - Replacing the handle function with HSA equivalent
/// - Using dlopen for HSA functions to avoid link-time dependency
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
// Patch implementations - shared
// =============================================================================

fn patch_rdmaxcel_h(content: &str) -> String {
    content
        .replace("#include \"driver_api.h\"", "#include \"driver_api_hip.h\"")
        .replace("CUdeviceptr", "hipDeviceptr_t")
}

fn patch_rdmaxcel_cpp_common(content: &str) -> String {
    content
        .replace("#include <hip/hip_runtime.h>", 
                 "#include <hip/hip_runtime.h>\n#include <hip/hip_version.h>")
        .replace("c10::cuda::CUDACachingAllocator", "c10::hip::HIPCachingAllocator")
        .replace("c10::cuda::CUDAAllocatorConfig", "c10::hip::HIPAllocatorConfig")
        .replace("c10::hip::HIPCachingAllocator::CUDAAllocatorConfig", 
                 "c10::hip::HIPCachingAllocator::HIPAllocatorConfig")
        .replace("CUDAAllocatorConfig::", "HIPAllocatorConfig::")
        .replace("hipDeviceAttributePciDomainId", "hipDeviceAttributePciDomainID")
        .replace("static_cast<CUdeviceptr>", "reinterpret_cast<hipDeviceptr_t>")
        .replace("static_cast<hipDeviceptr_t>", "reinterpret_cast<hipDeviceptr_t>")
        .replace("CUDA_SUCCESS", "hipSuccess")
        .replace("CUresult", "hipError_t")
}

// =============================================================================
// ROCm 7+ specific patches
// =============================================================================

fn patch_driver_api_h_rocm7(content: &str) -> String {
    let mut result = apply_replacements(content, WRAPPER_REPLACEMENTS);
    result = result
        .replace("rdmaxcel_cuMemGetHandleForAddressRange", "rdmaxcel_hipMemGetHandleForAddressRange")
        .replace("CUmemRangeHandleType", "hipMemRangeHandleType");
    result
}

fn patch_driver_api_cpp_rocm7(content: &str) -> String {
    let mut result = apply_replacements(content, WRAPPER_REPLACEMENTS);
    result = apply_replacements(&result, DRIVER_API_PTR_REPLACEMENTS);
    result = apply_replacements(&result, DRIVER_API_MACRO_REPLACEMENTS);
    result
        .replace("libcuda.so.1", "libamdhip64.so")
        .replace("rdmaxcel_cuMemGetHandleForAddressRange", "rdmaxcel_hipMemGetHandleForAddressRange")
        .replace("_(cuMemGetHandleForAddressRange)", "_(hipMemGetHandleForAddressRange)")
        .replace("->cuMemGetHandleForAddressRange_(", "->hipMemGetHandleForAddressRange_(")
        .replace("CUmemRangeHandleType", "hipMemRangeHandleType")
}

// =============================================================================
// ROCm 6.x specific patches (HSA dmabuf)
// =============================================================================

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
        .replace("static_cast<hipDeviceptr_t>", "reinterpret_cast<hipDeviceptr_t>")
        .replace("CUDA_SUCCESS", "hipSuccess")
        .replace("CUdevice device", "hipDevice_t device")
        .replace("cuDeviceGet(&device", "hipDeviceGet(&device")
        .replace("cuDeviceGetAttribute", "hipDeviceGetAttribute")
        .replace("cuPointerGetAttribute", "hipPointerGetAttribute")
        .replace("CU_DEVICE_ATTRIBUTE_PCI_BUS_ID", "hipDeviceAttributePciBusId")
        .replace("CU_DEVICE_ATTRIBUTE_PCI_DEVICE_ID", "hipDeviceAttributePciDeviceId")
        .replace("CU_DEVICE_ATTRIBUTE_PCI_DOMAIN_ID", "hipDeviceAttributePciDomainID")
        .replace("CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL", "HIP_POINTER_ATTRIBUTE_DEVICE_ORDINAL")
        .replace("CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD", "0 /* HSA dmabuf */");

    // Convert cuMemGetHandleForAddressRange to HSA with correct argument order
    result = result.replace("cuMemGetHandleForAddressRange(", "hsa_amd_portable_export_dmabuf(");
    
    // Fix argument order for HSA call (ptr, size, fd, flags) vs CUDA (fd, ptr, size, type, flags)
    result = result.replace(
        "hsa_amd_portable_export_dmabuf(\n      &fd,\n      reinterpret_cast<hipDeviceptr_t>(start_addr),\n      total_size,\n      0 /* HSA dmabuf */,\n      0);",
        "hsa_amd_portable_export_dmabuf(\n      reinterpret_cast<void*>(start_addr),\n      total_size,\n      &fd,\n      nullptr);"
    );
    result = result.replace(
        "hsa_amd_portable_export_dmabuf(\n            &fd,\n            reinterpret_cast<hipDeviceptr_t>(chunk_start),\n            chunk_size,\n            0 /* HSA dmabuf */,\n            0);",
        "hsa_amd_portable_export_dmabuf(\n            reinterpret_cast<void*>(chunk_start),\n            chunk_size,\n            &fd,\n            nullptr);"
    );
    
    result
        .replace("CUresult cu_result", "hsa_status_t hsa_result")
        .replace("hipError_t cu_result", "hsa_status_t hsa_result")
        .replace("cu_result != hipSuccess", "hsa_result != HSA_STATUS_SUCCESS")
        .replace("if (cu_result", "if (hsa_result")
        .replace("hipPointerAttribute::device", "HIP_POINTER_ATTRIBUTE_DEVICE_ORDINAL")
}

fn patch_driver_api_h_rocm6(content: &str) -> String {
    let mut result = apply_replacements(content, WRAPPER_REPLACEMENTS);

    // Add HSA includes if not present
    if !result.contains("#include <hsa/hsa.h>") {
        result = result.replace(
            "#include <hip/hip_runtime.h>",
            "#include <hip/hip_runtime.h>\n#include <hsa/hsa.h>\n#include <hsa/hsa_ext_amd.h>"
        );
    }

    // Replace CUDA handle function declaration with HSA version
    let old_decl = "hipError_t rdmaxcel_cuMemGetHandleForAddressRange(\n    int* handle,\n    hipDeviceptr_t dptr,\n    size_t size,\n    CUmemRangeHandleType handleType,\n    unsigned long long flags);";
    let new_decl = "hsa_status_t rdmaxcel_hsa_amd_portable_export_dmabuf(\n    void* ptr,\n    size_t size,\n    int* fd,\n    uint64_t* flags);";
    result = result.replace(old_decl, new_decl);
    result = result.replace("CUmemRangeHandleType", "int /* ROCm 6.x placeholder */");

    // Add CUDA-compatible wrapper declaration for existing callers
    result.push_str(r#"

// CUDA-compatible wrapper for code expecting cuMemGetHandleForAddressRange
hipError_t rdmaxcel_cuMemGetHandleForAddressRange(
    int* handle,
    hipDeviceptr_t dptr,
    size_t size,
    int handleType,
    unsigned long long flags);
"#);
    result
}

fn patch_driver_api_cpp_rocm6(content: &str) -> String {
    let mut result = apply_replacements(content, WRAPPER_REPLACEMENTS);
    result = apply_replacements(&result, DRIVER_API_PTR_REPLACEMENTS);
    result = apply_replacements(&result, DRIVER_API_MACRO_REPLACEMENTS);

    // Add HSA includes
    result = result.replace(
        "#include \"driver_api_hip.h\"",
        "#include \"driver_api_hip.h\"\n#include <hsa/hsa.h>\n#include <hsa/hsa_ext_amd.h>"
    );
    
    result = result
        .replace("libcuda.so.1", "libamdhip64.so")
        .replace("dstDevice, srcHost, ByteCount);", "dstDevice, const_cast<void*>(srcHost), ByteCount);")
        .replace("->cuMemGetHandleForAddressRange_(", "->hipMemGetHandleForAddressRange_(")
        .replace("_(cuMemGetHandleForAddressRange)", "_(hipMemGetHandleForAddressRange)");

    // Replace the wrapper function with HSA version
    let old_wrapper = r#"hipError_t rdmaxcel_cuMemGetHandleForAddressRange(
    int* handle,
    hipDeviceptr_t dptr,
    size_t size,
    CUmemRangeHandleType handleType,
    unsigned long long flags) {
  return rdmaxcel::DriverAPI::get()->hipMemGetHandleForAddressRange_(
      handle, dptr, size, handleType, flags);
}"#;
    let hsa_impl = r#"hsa_status_t rdmaxcel_hsa_amd_portable_export_dmabuf(
    void* ptr,
    size_t size,
    int* fd,
    uint64_t* flags) {
  // Direct HSA call - will be replaced with dlopen version
  return hsa_amd_portable_export_dmabuf(ptr, size, fd, flags);
}"#;
    result = result.replace(old_wrapper, hsa_impl);

    // Handle alternate wrapper pattern
    let old_wrapper2 = r#"hipError_t rdmaxcel_cuMemGetHandleForAddressRange(
    int* handle,
    hipDeviceptr_t dptr,
    size_t size,
    CUmemRangeHandleType handleType,
    unsigned long long flags) {
  return rdmaxcel::DriverAPI::get()->cuMemGetHandleForAddressRange_(
      handle, dptr, size, handleType, flags);
}"#;
    result = result.replace(old_wrapper2, hsa_impl);

    result = result.replace("CUmemRangeHandleType", "int /* ROCm 6.x placeholder */");
    
    // Remove hipMemGetHandleForAddressRange from dlopen macro (we use HSA instead)
    result = result.replace(
        "_(hipMemGetHandleForAddressRange)  \\",
        "/* hipMemGetHandleForAddressRange - using HSA */  \\"
    );
    result = result.replace(
        "_(hipMemGetHandleForAddressRange) \\",
        "/* hipMemGetHandleForAddressRange - using HSA */ \\"
    );

    // Add CUDA-compatible wrapper that translates to HSA
    result.push_str(r#"

// CUDA-compatible wrapper - translates cuMemGetHandleForAddressRange to HSA
hipError_t rdmaxcel_cuMemGetHandleForAddressRange(
    int* handle,
    hipDeviceptr_t dptr,
    size_t size,
    int handleType,
    unsigned long long flags) {
  (void)handleType;  // ROCm 6.x only supports dmabuf
  (void)flags;
  hsa_status_t status = rdmaxcel_hsa_amd_portable_export_dmabuf(
      reinterpret_cast<void*>(dptr), size, handle, nullptr);
  return (status == HSA_STATUS_SUCCESS) ? hipSuccess : hipErrorUnknown;
}
"#);
    result
}

/// Apply dlopen patches to avoid link-time dependencies on HIP/HSA libraries.
///
/// This is important because:
/// 1. hipFree must be called via dlopen'd pointer, not directly
/// 2. HSA functions must be loaded lazily to avoid libhsa-runtime64.so dependency
fn patch_for_dlopen(content: &str) -> String {
    let mut result = content.to_string();

    // 1. Add hipFree to the dlopen macro list
    result = result.replace(
        "_(hipDrvGetErrorString)",
        "_(hipDrvGetErrorString)              \\\n  _(hipFree)"
    );

    // 2. Reorder DriverAPI::get() to create singleton first, then call dlopen'd hipFree
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

    // 3. Replace direct HSA call with lazy dlopen version
    result = result.replace(
        r#"hsa_status_t rdmaxcel_hsa_amd_portable_export_dmabuf(
    void* ptr,
    size_t size,
    int* fd,
    uint64_t* flags) {
  // Direct HSA call - will be replaced with dlopen version
  return hsa_amd_portable_export_dmabuf(ptr, size, fd, flags);
}"#,
        r#"// Lazy-loaded HSA function - avoids link-time dependency on libhsa-runtime64.so
static decltype(&hsa_amd_portable_export_dmabuf) g_hsa_export_dmabuf = nullptr;

hsa_status_t rdmaxcel_hsa_amd_portable_export_dmabuf(
    void* ptr,
    size_t size,
    int* fd,
    uint64_t* flags) {
  if (!g_hsa_export_dmabuf) {
    // Try RTLD_NOLOAD first (library may already be loaded by HIP runtime)
    void* handle = dlopen("libhsa-runtime64.so", RTLD_LAZY | RTLD_NOLOAD);
    if (!handle) handle = dlopen("libhsa-runtime64.so", RTLD_LAZY);
    if (!handle) {
      throw std::runtime_error(
          std::string("[RdmaXcel] Failed to load libhsa-runtime64.so: ") + dlerror());
    }
    g_hsa_export_dmabuf = reinterpret_cast<decltype(&hsa_amd_portable_export_dmabuf)>(
        dlsym(handle, "hsa_amd_portable_export_dmabuf"));
    if (!g_hsa_export_dmabuf) {
      throw std::runtime_error(
          std::string("[RdmaXcel] Symbol not found: hsa_amd_portable_export_dmabuf: ") + dlerror());
    }
  }
  return g_hsa_export_dmabuf(ptr, size, fd, flags);
}"#
    );

    // 4. Update the CUDA-compat wrapper to use our dlopen'd function
    result = result.replace(
        "hsa_status_t status = hsa_amd_portable_export_dmabuf(",
        "hsa_status_t status = rdmaxcel_hsa_amd_portable_export_dmabuf("
    );

    result
}
