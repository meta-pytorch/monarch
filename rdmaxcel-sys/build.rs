/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::env;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

// =============================================================================
// Hipify Patching Functions (specific to rdmaxcel-sys)
// =============================================================================

/// Renames rdmaxcel_cu* wrapper functions to rdmaxcel_hip* in the given content
/// NOTE: rdmaxcel_cuMemGetHandleForAddressRange is intentionally NOT included here
/// because for ROCm 6.x we replace it with HSA function, and for ROCm 7+ we handle it separately
fn rename_rdmaxcel_wrappers(content: &str) -> String {
    content
        // Memory management wrappers
        .replace("rdmaxcel_cuMemGetAllocationGranularity", "rdmaxcel_hipMemGetAllocationGranularity")
        .replace("rdmaxcel_cuMemCreate", "rdmaxcel_hipMemCreate")
        .replace("rdmaxcel_cuMemAddressReserve", "rdmaxcel_hipMemAddressReserve")
        .replace("rdmaxcel_cuMemMap", "rdmaxcel_hipMemMap")
        .replace("rdmaxcel_cuMemSetAccess", "rdmaxcel_hipMemSetAccess")
        .replace("rdmaxcel_cuMemUnmap", "rdmaxcel_hipMemUnmap")
        .replace("rdmaxcel_cuMemAddressFree", "rdmaxcel_hipMemAddressFree")
        .replace("rdmaxcel_cuMemRelease", "rdmaxcel_hipMemRelease")
        .replace("rdmaxcel_cuMemcpyHtoD_v2", "rdmaxcel_hipMemcpyHtoD")
        .replace("rdmaxcel_cuMemcpyDtoH_v2", "rdmaxcel_hipMemcpyDtoH")
        .replace("rdmaxcel_cuMemsetD8_v2", "rdmaxcel_hipMemsetD8")
        // Pointer queries
        .replace("rdmaxcel_cuPointerGetAttribute", "rdmaxcel_hipPointerGetAttribute")
        // Device management
        .replace("rdmaxcel_cuInit", "rdmaxcel_hipInit")
        .replace("rdmaxcel_cuDeviceGetCount", "rdmaxcel_hipDeviceGetCount")
        .replace("rdmaxcel_cuDeviceGetAttribute", "rdmaxcel_hipDeviceGetAttribute")
        .replace("rdmaxcel_cuDeviceGet", "rdmaxcel_hipDeviceGet")
        // Context management
        .replace("rdmaxcel_cuCtxCreate_v2", "rdmaxcel_hipCtxCreate")
        .replace("rdmaxcel_cuCtxSetCurrent", "rdmaxcel_hipCtxSetCurrent")
        // Error handling
        .replace("rdmaxcel_cuGetErrorString", "rdmaxcel_hipGetErrorString")
}

/// Post-processes hipified files for ROCm 7.0+
fn patch_hipified_files_rocm7(hip_src_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:warning=Patching hipify_torch output for ROCm 7.0+...");

    // --- Patch rdmaxcel_hip.cpp ---
    let cpp_file = hip_src_dir.join("rdmaxcel_hip.cpp");
    if cpp_file.exists() {
        let content = fs::read_to_string(&cpp_file)?;

        let patched_content = content
            .replace(
                "#include <hip/hip_runtime.h>",
                "#include <hip/hip_runtime.h>\n#include <hip/hip_version.h>",
            )
            .replace("c10::cuda::CUDACachingAllocator", "c10::hip::HIPCachingAllocator")
            .replace("c10::cuda::CUDAAllocatorConfig", "c10::hip::HIPAllocatorConfig")
            .replace("c10::hip::HIPCachingAllocator::CUDAAllocatorConfig", "c10::hip::HIPCachingAllocator::HIPAllocatorConfig")
            .replace("CUDAAllocatorConfig::", "HIPAllocatorConfig::")
            .replace("hipDeviceAttributePciDomainId", "hipDeviceAttributePciDomainID")
            .replace("static_cast<CUdeviceptr>", "reinterpret_cast<hipDeviceptr_t>")
            .replace("static_cast<hipDeviceptr_t>", "reinterpret_cast<hipDeviceptr_t>")
            .replace("CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD", "hipMemRangeHandleTypeDmaBufFd")
            .replace("cuMemGetHandleForAddressRange", "hipMemGetHandleForAddressRange")
            .replace("CUDA_SUCCESS", "hipSuccess")
            .replace("CUresult", "hipError_t");

        fs::write(&cpp_file, patched_content)?;
    }

    // --- Patch rdmaxcel_hip.h ---
    let header_file = hip_src_dir.join("rdmaxcel_hip.h");
    if header_file.exists() {
        let content = fs::read_to_string(&header_file)?;
        let patched_content = content
            .replace("#include \"driver_api.h\"", "#include \"driver_api_hip.h\"")
            .replace("CUdeviceptr", "hipDeviceptr_t");

        fs::write(&header_file, patched_content)?;
    }

    // --- Patch driver_api_hip.h ---
    let driver_api_h = hip_src_dir.join("driver_api_hip.h");
    if driver_api_h.exists() {
        let content = fs::read_to_string(&driver_api_h)?;
        let mut patched_content = rename_rdmaxcel_wrappers(&content);

        // For ROCm 7+, rename the dmabuf function and fix the type
        patched_content = patched_content
            .replace("rdmaxcel_cuMemGetHandleForAddressRange", "rdmaxcel_hipMemGetHandleForAddressRange")
            .replace("CUmemRangeHandleType", "hipMemRangeHandleType");

        fs::write(&driver_api_h, patched_content)?;
    }

    // --- Patch driver_api_hip.cpp ---
    let driver_api_cpp = hip_src_dir.join("driver_api_hip.cpp");
    if driver_api_cpp.exists() {
        let content = fs::read_to_string(&driver_api_cpp)?;

        let mut patched_content = rename_rdmaxcel_wrappers(&content);

        patched_content = patched_content
            // Fix library name
            .replace("libcuda.so.1", "libamdhip64.so")
            // Rename the dmabuf function
            .replace("rdmaxcel_cuMemGetHandleForAddressRange", "rdmaxcel_hipMemGetHandleForAddressRange")
            // Fix the macro entry hipify missed
            .replace("_(cuMemGetHandleForAddressRange)", "_(hipMemGetHandleForAddressRange)")
            // Fix internal member references
            .replace("->cuMemGetHandleForAddressRange_(", "->hipMemGetHandleForAddressRange_(")
            .replace("->cuMemGetAllocationGranularity_(", "->hipMemGetAllocationGranularity_(")
            .replace("->cuMemCreate_(", "->hipMemCreate_(")
            .replace("->cuMemAddressReserve_(", "->hipMemAddressReserve_(")
            .replace("->cuMemMap_(", "->hipMemMap_(")
            .replace("->cuMemSetAccess_(", "->hipMemSetAccess_(")
            .replace("->cuMemUnmap_(", "->hipMemUnmap_(")
            .replace("->cuMemAddressFree_(", "->hipMemAddressFree_(")
            .replace("->cuMemRelease_(", "->hipMemRelease_(")
            .replace("->cuMemcpyHtoD_v2_(", "->hipMemcpyHtoD_(")
            .replace("->cuMemcpyDtoH_v2_(", "->hipMemcpyDtoH_(")
            .replace("->cuMemsetD8_v2_(", "->hipMemsetD8_(")
            .replace("->cuPointerGetAttribute_(", "->hipPointerGetAttribute_(")
            .replace("->cuInit_(", "->hipInit_(")
            .replace("->cuDeviceGet_(", "->hipDeviceGet_(")
            .replace("->cuDeviceGetCount_(", "->hipGetDeviceCount_(")
            .replace("->cuDeviceGetAttribute_(", "->hipDeviceGetAttribute_(")
            .replace("->cuCtxCreate_v2_(", "->hipCtxCreate_(")
            .replace("->cuCtxSetCurrent_(", "->hipCtxSetCurrent_(")
            .replace("->cuGetErrorString_(", "->hipDrvGetErrorString_(")
            // Fix type
            .replace("CUmemRangeHandleType", "hipMemRangeHandleType");

        fs::write(&driver_api_cpp, patched_content)?;
    }

    println!("cargo:warning=Applied ROCm 7.0+ post-processing fixes to hipified files");
    Ok(())
}

/// Post-processes files for ROCm 6.x (uses HSA dmabuf instead of HIP dmabuf)
fn patch_hipified_files_rocm6(hip_src_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:warning=Patching hipify_torch output for ROCm 6.x (HSA dmabuf)...");

    // --- Patch rdmaxcel_hip.cpp ---
    let cpp_file = hip_src_dir.join("rdmaxcel_hip.cpp");
    if cpp_file.exists() {
        let content = fs::read_to_string(&cpp_file)?;

        let mut patched_content = content
            // Add version and HSA headers at the top
            .replace(
                "#include <hip/hip_runtime.h>",
                "#include <hip/hip_runtime.h>\n#include <hip/hip_version.h>\n#include <hsa/hsa.h>\n#include <hsa/hsa_ext_amd.h>"
            )
            // Fix PyTorch allocator namespace: c10::cuda → c10::hip
            .replace("c10::cuda::CUDACachingAllocator", "c10::hip::HIPCachingAllocator")
            .replace("c10::cuda::CUDAAllocatorConfig", "c10::hip::HIPAllocatorConfig")
            .replace("c10::hip::HIPCachingAllocator::CUDAAllocatorConfig", "c10::hip::HIPCachingAllocator::HIPAllocatorConfig")
            .replace("CUDAAllocatorConfig::", "HIPAllocatorConfig::")
            // Fix HIP API attribute names
            .replace("hipDeviceAttributePciDomainId", "hipDeviceAttributePciDomainID")
            // Fix pointer casts for HIP
            .replace("static_cast<CUdeviceptr>", "reinterpret_cast<hipDeviceptr_t>")
            .replace("static_cast<hipDeviceptr_t>", "reinterpret_cast<hipDeviceptr_t>")
            // Replace CUDA types with HIP types
            .replace("CUDA_SUCCESS", "hipSuccess")
            .replace("CUdevice device", "hipDevice_t device")
            // Fix device functions
            .replace("cuDeviceGet(&device", "hipDeviceGet(&device")
            .replace("cuDeviceGetAttribute", "hipDeviceGetAttribute")
            .replace("cuPointerGetAttribute", "hipPointerGetAttribute")
            // Fix device attribute constants
            .replace("CU_DEVICE_ATTRIBUTE_PCI_BUS_ID", "hipDeviceAttributePciBusId")
            .replace("CU_DEVICE_ATTRIBUTE_PCI_DEVICE_ID", "hipDeviceAttributePciDeviceId")
            .replace("CU_DEVICE_ATTRIBUTE_PCI_DOMAIN_ID", "hipDeviceAttributePciDomainID")
            .replace("CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL", "HIP_POINTER_ATTRIBUTE_DEVICE_ORDINAL")
            // Remove CUDA-specific constants for dmabuf type
            .replace("CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD", "0 /* HSA dmabuf */");

        // Replace cuMemGetHandleForAddressRange calls with hsa_amd_portable_export_dmabuf
        // Note: The parameter order is different:
        // CUDA: cuMemGetHandleForAddressRange(&fd, ptr, size, type, flags)
        // HSA:  hsa_amd_portable_export_dmabuf(ptr, size, &fd, nullptr)
        patched_content = patched_content.replace(
            "cuMemGetHandleForAddressRange(",
            "hsa_amd_portable_export_dmabuf(",
        );

        // Fix the parameter ordering for hsa_amd_portable_export_dmabuf calls
        // Pattern for compact_mrs function
        patched_content = patched_content.replace(
            "hsa_amd_portable_export_dmabuf(\n      &fd,\n      reinterpret_cast<hipDeviceptr_t>(start_addr),\n      total_size,\n      0 /* HSA dmabuf */,\n      0);",
            "hsa_amd_portable_export_dmabuf(\n      reinterpret_cast<void*>(start_addr),\n      total_size,\n      &fd,\n      nullptr);"
        );

        // Pattern for register_segments function
        patched_content = patched_content.replace(
            "hsa_amd_portable_export_dmabuf(\n            &fd,\n            reinterpret_cast<hipDeviceptr_t>(chunk_start),\n            chunk_size,\n            0 /* HSA dmabuf */,\n            0);",
            "hsa_amd_portable_export_dmabuf(\n            reinterpret_cast<void*>(chunk_start),\n            chunk_size,\n            &fd,\n            nullptr);"
        );

        // Replace result types and checks for HSA
        patched_content = patched_content
            .replace("CUresult cu_result", "hsa_status_t hsa_result")
            .replace("hipError_t cu_result", "hsa_status_t hsa_result")
            .replace("cu_result != hipSuccess", "hsa_result != HSA_STATUS_SUCCESS")
            .replace("if (cu_result", "if (hsa_result");

        // Fix hipPointerGetAttribute enum usage
        patched_content = patched_content.replace(
            "hipPointerAttribute::device",
            "HIP_POINTER_ATTRIBUTE_DEVICE_ORDINAL",
        );

        fs::write(&cpp_file, patched_content)?;
    }

    // --- Patch rdmaxcel_hip.h ---
    let header_file = hip_src_dir.join("rdmaxcel_hip.h");
    if header_file.exists() {
        let content = fs::read_to_string(&header_file)?;
        let patched_content = content
            .replace("#include \"driver_api.h\"", "#include \"driver_api_hip.h\"")
            .replace("CUdeviceptr", "hipDeviceptr_t");

        fs::write(&header_file, patched_content)?;
    }

    // --- Patch driver_api_hip.h for ROCm 6.x ---
    // Key change: Replace CUmemRangeHandleType-based function with HSA dmabuf function
    let driver_api_h = hip_src_dir.join("driver_api_hip.h");
    if driver_api_h.exists() {
        let content = fs::read_to_string(&driver_api_h)?;

        // First apply standard renames (but NOT the dmabuf function - we replace it entirely)
        let mut patched_content = rename_rdmaxcel_wrappers(&content);

        // Add HSA header (but don't duplicate if already present)
        if !patched_content.contains("#include <hsa/hsa.h>") {
            patched_content = patched_content.replace(
                "#include <hip/hip_runtime.h>",
                "#include <hip/hip_runtime.h>\n#include <hsa/hsa.h>\n#include <hsa/hsa_ext_amd.h>"
            );
        }

        // Replace the CUmemRangeHandleType-based function declaration with HSA version
        // The hipified file still has CUDA names (hipify doesn't convert custom rdmaxcel_ functions)
        let old_decl = "hipError_t rdmaxcel_cuMemGetHandleForAddressRange(\n    int* handle,\n    hipDeviceptr_t dptr,\n    size_t size,\n    CUmemRangeHandleType handleType,\n    unsigned long long flags);";
        let new_decl = "hsa_status_t rdmaxcel_hsa_amd_portable_export_dmabuf(\n    void* ptr,\n    size_t size,\n    int* fd,\n    uint64_t* flags);";
        patched_content = patched_content.replace(old_decl, new_decl);

        // Also handle any remaining CUmemRangeHandleType references (shouldn't be any after above, but just in case)
        patched_content = patched_content.replace("CUmemRangeHandleType", "int /* placeholder - ROCm 6.x */");

        // Add CUDA-compatible wrapper declaration for monarch_rdma compatibility
        patched_content.push_str("\n\n// CUDA-compatible wrapper for monarch_rdma\nhipError_t rdmaxcel_cuMemGetHandleForAddressRange(\n    int* handle,\n    hipDeviceptr_t dptr,\n    size_t size,\n    int handleType,\n    unsigned long long flags);\n");

        fs::write(&driver_api_h, patched_content)?;
    }

    // --- Patch driver_api_hip.cpp for ROCm 6.x ---
    let driver_api_cpp = hip_src_dir.join("driver_api_hip.cpp");
    if driver_api_cpp.exists() {
        let content = fs::read_to_string(&driver_api_cpp)?;

        // Apply standard wrapper renames first
        let mut patched_content = rename_rdmaxcel_wrappers(&content);

        // Add HSA headers
        patched_content = patched_content.replace(
            "#include \"driver_api_hip.h\"",
            "#include \"driver_api_hip.h\"\n#include <hsa/hsa.h>\n#include <hsa/hsa_ext_amd.h>"
        );

        // Fix library name
        patched_content = patched_content.replace("libcuda.so.1", "libamdhip64.so");

        // Fix const void* to void* conversion for hipMemcpyHtoD (ROCm API difference)
        patched_content = patched_content.replace(
            "dstDevice, srcHost, ByteCount);",
            "dstDevice, const_cast<void*>(srcHost), ByteCount);"
        );

        // Fix internal member references (hipify doesn't convert struct member names)
        patched_content = patched_content
            .replace("->cuMemGetHandleForAddressRange_(", "->hipMemGetHandleForAddressRange_(")
            .replace("->cuMemGetAllocationGranularity_(", "->hipMemGetAllocationGranularity_(")
            .replace("->cuMemCreate_(", "->hipMemCreate_(")
            .replace("->cuMemAddressReserve_(", "->hipMemAddressReserve_(")
            .replace("->cuMemMap_(", "->hipMemMap_(")
            .replace("->cuMemSetAccess_(", "->hipMemSetAccess_(")
            .replace("->cuMemUnmap_(", "->hipMemUnmap_(")
            .replace("->cuMemAddressFree_(", "->hipMemAddressFree_(")
            .replace("->cuMemRelease_(", "->hipMemRelease_(")
            .replace("->cuMemcpyHtoD_v2_(", "->hipMemcpyHtoD_(")
            .replace("->cuMemcpyDtoH_v2_(", "->hipMemcpyDtoH_(")
            .replace("->cuMemsetD8_v2_(", "->hipMemsetD8_(")
            .replace("->cuPointerGetAttribute_(", "->hipPointerGetAttribute_(")
            .replace("->cuInit_(", "->hipInit_(")
            .replace("->cuDeviceGet_(", "->hipDeviceGet_(")
            .replace("->cuDeviceGetCount_(", "->hipGetDeviceCount_(")
            .replace("->cuDeviceGetAttribute_(", "->hipDeviceGetAttribute_(")
            .replace("->cuCtxCreate_v2_(", "->hipCtxCreate_(")
            .replace("->cuCtxSetCurrent_(", "->hipCtxSetCurrent_(")
            .replace("->cuGetErrorString_(", "->hipDrvGetErrorString_(");

        // Fix the macro entries hipify missed
        patched_content = patched_content
            .replace("_(cuMemGetHandleForAddressRange)", "_(hipMemGetHandleForAddressRange)")
            .replace("_(cuMemGetAllocationGranularity)", "_(hipMemGetAllocationGranularity)")
            .replace("_(cuMemCreate)", "_(hipMemCreate)")
            .replace("_(cuMemAddressReserve)", "_(hipMemAddressReserve)")
            .replace("_(cuMemMap)", "_(hipMemMap)")
            .replace("_(cuMemSetAccess)", "_(hipMemSetAccess)")
            .replace("_(cuMemUnmap)", "_(hipMemUnmap)")
            .replace("_(cuMemAddressFree)", "_(hipMemAddressFree)")
            .replace("_(cuMemRelease)", "_(hipMemRelease)")
            .replace("_(cuMemcpyHtoD_v2)", "_(hipMemcpyHtoD)")
            .replace("_(cuMemcpyDtoH_v2)", "_(hipMemcpyDtoH)")
            .replace("_(cuMemsetD8_v2)", "_(hipMemsetD8)")
            .replace("_(cuPointerGetAttribute)", "_(hipPointerGetAttribute)")
            .replace("_(cuInit)", "_(hipInit)")
            .replace("_(cuDeviceGet)", "_(hipDeviceGet)")
            .replace("_(cuDeviceGetCount)", "_(hipGetDeviceCount)")
            .replace("_(cuDeviceGetAttribute)", "_(hipDeviceGetAttribute)")
            .replace("_(cuCtxCreate_v2)", "_(hipCtxCreate)")
            .replace("_(cuCtxSetCurrent)", "_(hipCtxSetCurrent)")
            .replace("_(cuGetErrorString)", "_(hipDrvGetErrorString)");

        // For ROCm 6.x: Replace the rdmaxcel_cuMemGetHandleForAddressRange wrapper function with HSA version
        // The original implementation calls the DriverAPI member function, but for ROCm 6.x we call HSA directly
        let old_wrapper = r#"hipError_t rdmaxcel_cuMemGetHandleForAddressRange(
    int* handle,
    hipDeviceptr_t dptr,
    size_t size,
    CUmemRangeHandleType handleType,
    unsigned long long flags) {
  return rdmaxcel::DriverAPI::get()->hipMemGetHandleForAddressRange_(
      handle, dptr, size, handleType, flags);
}"#;

        let new_wrapper = r#"hsa_status_t rdmaxcel_hsa_amd_portable_export_dmabuf(
    void* ptr,
    size_t size,
    int* fd,
    uint64_t* flags) {
  // Direct HSA call for ROCm 6.x - bypasses DriverAPI dynamic loading
  return hsa_amd_portable_export_dmabuf(ptr, size, fd, flags);
}"#;

        patched_content = patched_content.replace(old_wrapper, new_wrapper);

        // Also try without the member function rename (in case replacement order matters)
        let old_wrapper2 = r#"hipError_t rdmaxcel_cuMemGetHandleForAddressRange(
    int* handle,
    hipDeviceptr_t dptr,
    size_t size,
    CUmemRangeHandleType handleType,
    unsigned long long flags) {
  return rdmaxcel::DriverAPI::get()->cuMemGetHandleForAddressRange_(
      handle, dptr, size, handleType, flags);
}"#;
        patched_content = patched_content.replace(old_wrapper2, new_wrapper);

        // Handle any remaining CUmemRangeHandleType references
        patched_content = patched_content.replace("CUmemRangeHandleType", "int /* placeholder - ROCm 6.x */");

        // Remove hipMemGetHandleForAddressRange from the macro (we use direct HSA call instead)
        // This prevents trying to dlsym a function that doesn't exist in ROCm 6.x
        patched_content = patched_content.replace(
            "_(hipMemGetHandleForAddressRange)  \\",
            "/* hipMemGetHandleForAddressRange removed for ROCm 6.x - using HSA */  \\"
        );
        patched_content = patched_content.replace(
            "_(hipMemGetHandleForAddressRange) \\",
            "/* hipMemGetHandleForAddressRange removed for ROCm 6.x - using HSA */ \\"
        );

        // Add CUDA-compatible wrapper that monarch_rdma can call
        let cuda_compat_wrapper = r#"

// CUDA-compatible wrapper for monarch_rdma - translates to HSA call
hipError_t rdmaxcel_cuMemGetHandleForAddressRange(
    int* handle,
    hipDeviceptr_t dptr,
    size_t size,
    int handleType,
    unsigned long long flags) {
  (void)handleType;  // unused - ROCm 6.x only supports dmabuf
  (void)flags;       // unused
  hsa_status_t status = hsa_amd_portable_export_dmabuf(
      reinterpret_cast<void*>(dptr),
      size,
      handle,
      nullptr);
  return (status == HSA_STATUS_SUCCESS) ? hipSuccess : hipErrorUnknown;
}
"#;
        patched_content.push_str(cuda_compat_wrapper);

        fs::write(&driver_api_cpp, patched_content)?;
    }

    println!("cargo:warning=Applied ROCm 6.x (HSA dmabuf) post-processing fixes to hipified files");
    Ok(())
}

/// Validates that hipified output files exist
fn validate_hipified_files(hip_src_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let required_files = [
        "rdmaxcel_hip.h",
        "rdmaxcel_hip.c",
        "rdmaxcel_hip.cpp",
        "rdmaxcel.hip",
    ];

    for file_name in &required_files {
        let file_path = hip_src_dir.join(file_name);
        if !file_path.exists() {
            return Err(format!(
                "Required hipified file {} was not found in {}",
                file_name,
                hip_src_dir.display()
            )
            .into());
        }
    }

    Ok(())
}

/// Runs `hipify_torch` on the source directory.
fn hipify_sources(
    python_interpreter: &Path,
    src_dir: &Path,
    hip_src_dir: &Path,
    rocm_version: (u32, u32),
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "cargo:warning=Copying sources from {} to {} for in-place hipify...",
        src_dir.display(),
        hip_src_dir.display()
    );
    fs::create_dir_all(hip_src_dir)?;

    // Include driver_api files for hipification
    let files_to_copy = [
        "lib.rs",
        "rdmaxcel.h",
        "rdmaxcel.c",
        "rdmaxcel.cpp",
        "rdmaxcel.cu",
        "test_rdmaxcel.c",
        "driver_api.h",
        "driver_api.cpp",
    ];

    for file_name in files_to_copy {
        let src_file = src_dir.join(file_name);
        let dest_file = hip_src_dir.join(file_name);
        if src_file.exists() {
            fs::copy(&src_file, &dest_file)?;
            println!("cargo:rerun-if-changed={}", src_file.display());
        }
    }

    println!("cargo:warning=Running hipify_torch in-place on copied sources with --v2...");

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let project_root = manifest_dir
        .parent()
        .ok_or("Failed to find project root from manifest dir")?;
    let hipify_script = project_root
        .join("deps")
        .join("hipify_torch")
        .join("hipify_cli.py");

    if !hipify_script.exists() {
        return Err(format!("hipify_cli.py not found at {}", hipify_script.display()).into());
    }
    println!("cargo:rerun-if-changed={}", hipify_script.display());

    let hipify_output = Command::new(python_interpreter)
        .arg(&hipify_script)
        .arg("--project-directory")
        .arg(hip_src_dir)
        .arg("--v2")
        .output()?;

    if !hipify_output.status.success() {
        return Err(format!(
            "hipify_cli.py failed: {}",
            String::from_utf8_lossy(&hipify_output.stderr)
        )
        .into());
    }

    // Apply version-specific patches
    let (major, _minor) = rocm_version;
    if major >= 7 {
        patch_hipified_files_rocm7(hip_src_dir)?;
    } else {
        patch_hipified_files_rocm6(hip_src_dir)?;
    }

    Ok(())
}

/// Gets libtorch include directories from PyTorch
fn get_libtorch_include_dirs(python_interpreter: &Path) -> Vec<PathBuf> {
    let mut include_dirs = Vec::new();

    if let Ok(output) = Command::new(python_interpreter)
        .arg("-c")
        .arg(build_utils::PYTHON_PRINT_PYTORCH_DETAILS)
        .output()
    {
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            if let Some(path) = line.strip_prefix("LIBTORCH_INCLUDE: ") {
                include_dirs.push(PathBuf::from(path));
            }
        }
    }

    include_dirs
}

// =============================================================================
// Main Build Logic
// =============================================================================

#[cfg(target_os = "macos")]
fn main() {}

#[cfg(not(target_os = "macos"))]
fn main() {
    println!("cargo:rustc-link-lib=ibverbs");
    println!("cargo:rustc-link-lib=mlx5");

    let (is_rocm, compute_home, compute_lib_names, rocm_version) =
        if let Ok(rocm_home) = build_utils::validate_rocm_installation() {
            let version = build_utils::get_rocm_version(&rocm_home).unwrap_or((6, 0));
            println!(
                "cargo:warning=Using HIP/ROCm {} from {}",
                format!("{}.{}", version.0, version.1),
                rocm_home
            );

            if version.0 >= 7 {
                println!("cargo:rustc-cfg=rocm_7_plus");
            } else {
                println!("cargo:rustc-cfg=rocm_6_x");
            }

            (true, rocm_home, vec!["amdhip64", "hsa-runtime64"], version)
        } else if let Ok(cuda_home) = build_utils::validate_cuda_installation() {
            println!("cargo:warning=Using CUDA from {}", cuda_home);
            (false, cuda_home, vec!["cuda", "cudart"], (0, 0))
        } else {
            eprintln!("Error: Neither CUDA nor ROCm installation found!");
            build_utils::print_cuda_error_help();
            build_utils::print_rocm_error_help();
            std::process::exit(1);
        };

    // Emit cfg check declarations
    println!("cargo:rustc-check-cfg=cfg(rocm_6_x)");
    println!("cargo:rustc-check-cfg=cfg(rocm_7_plus)");

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| {
        let current_dir = std::env::current_dir().expect("Failed to get current directory");
        let current_path = current_dir.to_string_lossy();
        if let Some(fbsource_pos) = current_path.find("fbsource") {
            let fbsource_path = &current_path[..fbsource_pos + "fbsource".len()];
            format!("{}/fbcode/monarch/rdmaxcel-sys", fbsource_path)
        } else {
            format!("{}/src", current_dir.to_string_lossy())
        }
    }));
    let src_dir = manifest_dir.join("src");

    let python_interpreter = build_utils::find_python_interpreter();

    let compute_include_path = format!("{}/include", compute_home);
    println!("cargo:rustc-env=CUDA_INCLUDE_PATH={}", compute_include_path);

    let python_config = match build_utils::python_env_dirs_with_interpreter("python3") {
        Ok(config) => config,
        Err(_) => {
            eprintln!("Warning: Failed to get Python environment directories");
            build_utils::PythonConfig {
                include_dir: None,
                lib_dir: None,
            }
        }
    };

    let compute_lib_dir = if is_rocm {
        match build_utils::get_rocm_lib_dir() {
            Ok(dir) => dir,
            Err(_) => {
                build_utils::print_rocm_lib_error_help();
                std::process::exit(1);
            }
        }
    } else {
        match build_utils::get_cuda_lib_dir() {
            Ok(dir) => dir,
            Err(_) => {
                build_utils::print_cuda_lib_error_help();
                std::process::exit(1);
            }
        }
    };
    println!("cargo:rustc-link-search=native={}", compute_lib_dir);
    for lib_name in &compute_lib_names {
        println!("cargo:rustc-link-lib={}", lib_name);
    }

    let use_pytorch_apis = build_utils::get_env_var_with_rerun("TORCH_SYS_USE_PYTORCH_APIS")
        .unwrap_or_else(|_| "1".to_owned());

    let libtorch_include_dirs: Vec<PathBuf> = if use_pytorch_apis == "1" {
        get_libtorch_include_dirs(&python_interpreter)
    } else {
        build_utils::get_env_var_with_rerun("LIBTORCH_INCLUDE")
            .unwrap_or_default()
            .split(':')
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .collect()
    };

    if use_pytorch_apis == "1" {
        if let Ok(output) = Command::new(&python_interpreter)
            .arg("-c")
            .arg(build_utils::PYTHON_PRINT_PYTORCH_DETAILS)
            .output()
        {
            for line in String::from_utf8_lossy(&output.stdout).lines() {
                if let Some(path) = line.strip_prefix("LIBTORCH_LIB: ") {
                    println!("cargo:rustc-link-search=native={}", path);
                    break;
                }
            }
        }
        println!("cargo:rustc-link-lib=torch_cpu");
        println!("cargo:rustc-link-lib=torch");
        println!("cargo:rustc-link-lib=c10");
        if is_rocm {
            println!("cargo:rustc-link-lib=c10_hip");
        } else {
            println!("cargo:rustc-link-lib=c10_cuda");
        }
    }

    // Link dl for dynamic loading
    println!("cargo:rustc-link-lib=dl");

    match env::var("OUT_DIR") {
        Ok(out_dir) => {
            let out_path = PathBuf::from(out_dir);
            println!("cargo:out_dir={}", out_path.display());

            let (code_dir, header_path, c_source_path, cpp_source_path, cuda_source_path, driver_api_cpp_path);

            if is_rocm {
                let hip_src_dir = out_path.join("hipified_src");

                hipify_sources(&python_interpreter, &src_dir, &hip_src_dir, rocm_version)
                    .expect("Failed to hipify sources");

                validate_hipified_files(&hip_src_dir).expect("Hipified files validation failed");

                code_dir = hip_src_dir.clone();
                header_path = hip_src_dir.join("rdmaxcel_hip.h");
                c_source_path = hip_src_dir.join("rdmaxcel_hip.c");
                cpp_source_path = hip_src_dir.join("rdmaxcel_hip.cpp");
                cuda_source_path = hip_src_dir.join("rdmaxcel.hip");
                driver_api_cpp_path = hip_src_dir.join("driver_api_hip.cpp");
            } else {
                println!("cargo:rerun-if-changed={}/src/rdmaxcel.h", manifest_dir.display());
                println!("cargo:rerun-if-changed={}/src/rdmaxcel.c", manifest_dir.display());
                println!("cargo:rerun-if-changed={}/src/rdmaxcel.cpp", manifest_dir.display());
                println!("cargo:rerun-if-changed={}/src/rdmaxcel.cu", manifest_dir.display());
                println!("cargo:rerun-if-changed={}/src/driver_api.h", manifest_dir.display());
                println!("cargo:rerun-if-changed={}/src/driver_api.cpp", manifest_dir.display());

                code_dir = src_dir.clone();
                header_path = src_dir.join("rdmaxcel.h");
                c_source_path = src_dir.join("rdmaxcel.c");
                cpp_source_path = src_dir.join("rdmaxcel.cpp");
                cuda_source_path = src_dir.join("rdmaxcel.cu");
                driver_api_cpp_path = src_dir.join("driver_api.cpp");
            }

            if !header_path.exists() {
                panic!("Header file not found at {}", header_path.display());
            }

            let mut builder = bindgen::Builder::default()
                .header(header_path.to_string_lossy())
                .clang_arg("-x")
                .clang_arg("c++")
                .clang_arg("-std=gnu++20")
                .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
                .allowlist_function("ibv_.*")
                .allowlist_function("mlx5dv_.*")
                .allowlist_function("mlx5_wqe_.*")
                .allowlist_function("create_qp")
                .allowlist_function("create_mlx5dv_.*")
                .allowlist_function("register_cuda_memory")
                .allowlist_function("register_hip_memory")
                .allowlist_function("db_ring")
                .allowlist_function("cqe_poll")
                .allowlist_function("send_wqe")
                .allowlist_function("recv_wqe")
                .allowlist_function("launch_db_ring")
                .allowlist_function("launch_cqe_poll")
                .allowlist_function("launch_send_wqe")
                .allowlist_function("launch_recv_wqe")
                .allowlist_function("rdma_get_active_segment_count")
                .allowlist_function("rdma_get_all_segment_info")
                .allowlist_function("pt_cuda_allocator_compatibility")
                .allowlist_function("pt_hip_allocator_compatibility")
                .allowlist_function("register_segments")
                .allowlist_function("deregister_segments")
                .allowlist_function("register_dmabuf_buffer")
                .allowlist_function("get_hip_pci_address_from_ptr")
                // Driver API wrappers (CUDA/HIP/HSA)
                .allowlist_function("rdmaxcel_cu.*")
                .allowlist_function("rdmaxcel_hip.*")
                .allowlist_function("rdmaxcel_hsa.*")
                // QP management functions
                .allowlist_function("rdmaxcel_qp_.*")
                .allowlist_function("rdmaxcel_print_device_info")
                .allowlist_function("rdmaxcel_error_string")
                // Completion cache functions
                .allowlist_function("completion_cache_.*")
                .allowlist_function("poll_cq_with_cache")
                // Types for QP and completion handling
                .allowlist_type("rdmaxcel_qp_t")
                .allowlist_type("rdmaxcel_qp")
                .allowlist_type("rdmaxcel_error_code_t")
                .allowlist_type("completion_cache_t")
                .allowlist_type("completion_cache")
                .allowlist_type("completion_node_t")
                .allowlist_type("completion_node")
                .allowlist_type("poll_context_t")
                .allowlist_type("poll_context")
                .allowlist_type("rdma_qp_type_t")
                // CUDA types (for CUDA builds)
                .allowlist_type("CUdeviceptr")
                .allowlist_type("CUdevice")
                .allowlist_type("CUresult")
                .allowlist_type("CUcontext")
                .allowlist_type("CUmemRangeHandleType")
                .allowlist_var("CUDA_SUCCESS")
                .allowlist_var("CU_.*")
                // HIP types (for ROCm builds)
                .allowlist_type("hipDeviceptr_t")
                .allowlist_type("hipDevice_t")
                .allowlist_type("hipError_t")
                .allowlist_type("hipCtx_t")
                .allowlist_type("hipPointer_attribute")
                .allowlist_var("hipSuccess")
                .allowlist_var("HIP_.*")
                // HSA types (for ROCm 6.x dmabuf)
                .allowlist_type("hsa_status_t")
                .allowlist_var("HSA_STATUS_SUCCESS")
                .allowlist_type("ibv_.*")
                .allowlist_type("mlx5dv_.*")
                .allowlist_type("mlx5_wqe_.*")
                .allowlist_type("cqe_poll_result_t")
                .allowlist_type("wqe_params_t")
                .allowlist_type("cqe_poll_params_t")
                .allowlist_type("rdma_segment_info_t")
                .allowlist_var("MLX5_.*")
                .allowlist_var("IBV_.*")
                .allowlist_var("RDMA_QP_TYPE_.*")
                .blocklist_type("ibv_wc")
                .blocklist_type("mlx5_wqe_ctrl_seg")
                .bitfield_enum("ibv_access_flags")
                .bitfield_enum("ibv_qp_attr_mask")
                .bitfield_enum("ibv_wc_flags")
                .bitfield_enum("ibv_send_flags")
                .bitfield_enum("ibv_port_cap_flags")
                .constified_enum_module("ibv_qp_type")
                .constified_enum_module("ibv_qp_state")
                .constified_enum_module("ibv_port_state")
                .constified_enum_module("ibv_wc_opcode")
                .constified_enum_module("ibv_wr_opcode")
                .constified_enum_module("ibv_wc_status")
                .derive_default(true)
                .prepend_enum_name(false);

            builder = builder.clang_arg(format!("-I{}", compute_include_path));

            // NOTE: Do NOT add libtorch include paths to bindgen

            if is_rocm {
                builder = builder
                    .clang_arg("-D__HIP_PLATFORM_AMD__=1")
                    .clang_arg("-DUSE_ROCM=1");

                if rocm_version.0 >= 7 {
                    builder = builder.clang_arg("-DROCM_7_PLUS=1");
                } else {
                    builder = builder.clang_arg("-DROCM_6_X=1");
                }
            }

            if let Some(include_dir) = &python_config.include_dir {
                builder = builder.clang_arg(format!("-I{}", include_dir));
            }

            let bindings = builder.generate().expect("Unable to generate bindings");
            bindings
                .write_to_file(out_path.join("bindings.rs"))
                .expect("Couldn't write bindings");

            println!("cargo:rustc-cfg=cargo");
            println!("cargo:rustc-check-cfg=cfg(cargo)");

            if c_source_path.exists() {
                let mut build = cc::Build::new();
                build.file(&c_source_path).include(&code_dir).flag("-fPIC");
                build.include(&compute_include_path);
                if is_rocm {
                    build.define("__HIP_PLATFORM_AMD__", "1");
                    build.define("USE_ROCM", "1");
                    if rocm_version.0 >= 7 {
                        build.define("ROCM_7_PLUS", "1");
                    } else {
                        build.define("ROCM_6_X", "1");
                    }
                }
                build.compile("rdmaxcel");
            }

            if cpp_source_path.exists() {
                let mut cpp_build = cc::Build::new();
                cpp_build
                    .file(&cpp_source_path)
                    .include(&code_dir)
                    .flag("-fPIC")
                    .cpp(true)
                    .flag("-std=gnu++20")
                    .flag("-Wno-unused-parameter")
                    .define("PYTORCH_C10_DRIVER_API_SUPPORTED", "1");

                if driver_api_cpp_path.exists() {
                    cpp_build.file(&driver_api_cpp_path);
                }

                cpp_build.include(&compute_include_path);
                if is_rocm {
                    cpp_build.define("__HIP_PLATFORM_AMD__", "1");
                    cpp_build.define("USE_ROCM", "1");
                    if rocm_version.0 >= 7 {
                        cpp_build.define("ROCM_7_PLUS", "1");
                    } else {
                        cpp_build.define("ROCM_6_X", "1");
                    }
                }
                for include_dir in &libtorch_include_dirs {
                    cpp_build.include(include_dir);
                }
                if let Some(include_dir) = &python_config.include_dir {
                    cpp_build.include(include_dir);
                }
                cpp_build.compile("rdmaxcel_cpp");
            }

            if cuda_source_path.exists() {
                let (compiler_path, compiler_name) = if is_rocm {
                    (format!("{}/bin/hipcc", compute_home), "hipcc")
                } else {
                    (format!("{}/bin/nvcc", compute_home), "nvcc")
                };

                let cuda_build_dir = format!("{}/target/cuda_build", manifest_dir.display());
                std::fs::create_dir_all(&cuda_build_dir)
                    .expect("Failed to create CUDA build directory");
                let cuda_obj_path = format!("{}/rdmaxcel_cuda.o", cuda_build_dir);
                let cuda_lib_path = format!("{}/librdmaxcel_cuda.a", cuda_build_dir);

                let compiler_output = if is_rocm {
                    let mut cmd = Command::new(&compiler_path);
                    cmd.args([
                        "-c",
                        cuda_source_path.to_str().unwrap(),
                        "-o",
                        &cuda_obj_path,
                        "-fPIC",
                        "-std=c++20",
                        "-D__HIP_PLATFORM_AMD__=1",
                        "-DUSE_ROCM=1",
                        &format!("-I{}", compute_include_path),
                        &format!("-I{}", code_dir.display()),
                        "-I/usr/include",
                        "-I/usr/include/infiniband",
                    ]);

                    if rocm_version.0 >= 7 {
                        cmd.arg("-DROCM_7_PLUS=1");
                    } else {
                        cmd.arg("-DROCM_6_X=1");
                    }

                    cmd.output()
                } else {
                    Command::new(&compiler_path)
                        .args([
                            "-c",
                            cuda_source_path.to_str().unwrap(),
                            "-o",
                            &cuda_obj_path,
                            "--compiler-options",
                            "-fPIC",
                            "-std=c++20",
                            "--expt-extended-lambda",
                            "-Xcompiler",
                            "-fPIC",
                            &format!("-I{}", compute_include_path),
                            &format!("-I{}", code_dir.display()),
                            "-I/usr/include",
                            "-I/usr/include/infiniband",
                        ])
                        .output()
                };

                match compiler_output {
                    Ok(output) => {
                        if !output.status.success() {
                            eprintln!("{} stderr: {}", compiler_name, String::from_utf8_lossy(&output.stderr));
                            eprintln!("{} stdout: {}", compiler_name, String::from_utf8_lossy(&output.stdout));
                            panic!("Failed to compile CUDA/HIP source with {}", compiler_name);
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to run {}: {}", compiler_name, e);
                        panic!("{} not found or failed to execute", compiler_name);
                    }
                }

                let ar_output = Command::new("ar")
                    .args(["rcs", &cuda_lib_path, &cuda_obj_path])
                    .output();

                match ar_output {
                    Ok(output) => {
                        if !output.status.success() {
                            eprintln!("ar stderr: {}", String::from_utf8_lossy(&output.stderr));
                            panic!("Failed to create CUDA static library with ar");
                        }
                        println!("cargo:rustc-link-lib=static=rdmaxcel_cuda");
                        println!("cargo:rustc-link-search=native={}", cuda_build_dir);
                        if let Err(e) = std::fs::copy(
                            &cuda_lib_path,
                            format!("{}/librdmaxcel_cuda.a", out_path.display()),
                        ) {
                            eprintln!("Warning: Failed to copy CUDA library to OUT_DIR: {}", e);
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to run ar: {}", e);
                        panic!("ar not found or failed to execute");
                    }
                }
            }
        }
        Err(_) => {
            println!("Note: OUT_DIR not set, skipping bindings file generation");
        }
    }
}
