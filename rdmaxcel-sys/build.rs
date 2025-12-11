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

fn rename_rdmaxcel_wrappers(content: &str) -> String {
    content
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
        .replace("rdmaxcel_cuPointerGetAttribute", "rdmaxcel_hipPointerGetAttribute")
        .replace("rdmaxcel_cuInit", "rdmaxcel_hipInit")
        .replace("rdmaxcel_cuDeviceGetCount", "rdmaxcel_hipDeviceGetCount")
        .replace("rdmaxcel_cuDeviceGetAttribute", "rdmaxcel_hipDeviceGetAttribute")
        .replace("rdmaxcel_cuDeviceGet", "rdmaxcel_hipDeviceGet")
        .replace("rdmaxcel_cuCtxCreate_v2", "rdmaxcel_hipCtxCreate")
        .replace("rdmaxcel_cuCtxSetCurrent", "rdmaxcel_hipCtxSetCurrent")
        .replace("rdmaxcel_cuGetErrorString", "rdmaxcel_hipGetErrorString")
}

fn patch_hipified_files_rocm7(hip_src_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:warning=Patching hipify_torch output for ROCm 7.0+...");

    let cpp_file = hip_src_dir.join("rdmaxcel_hip.cpp");
    if cpp_file.exists() {
        let content = fs::read_to_string(&cpp_file)?;
        let patched_content = content
            .replace("#include <hip/hip_runtime.h>", "#include <hip/hip_runtime.h>\n#include <hip/hip_version.h>")
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

    let header_file = hip_src_dir.join("rdmaxcel_hip.h");
    if header_file.exists() {
        let content = fs::read_to_string(&header_file)?;
        let patched_content = content
            .replace("#include \"driver_api.h\"", "#include \"driver_api_hip.h\"")
            .replace("CUdeviceptr", "hipDeviceptr_t");
        fs::write(&header_file, patched_content)?;
    }

    let driver_api_h = hip_src_dir.join("driver_api_hip.h");
    if driver_api_h.exists() {
        let content = fs::read_to_string(&driver_api_h)?;
        let mut patched_content = rename_rdmaxcel_wrappers(&content);
        patched_content = patched_content
            .replace("rdmaxcel_cuMemGetHandleForAddressRange", "rdmaxcel_hipMemGetHandleForAddressRange")
            .replace("CUmemRangeHandleType", "hipMemRangeHandleType");
        fs::write(&driver_api_h, patched_content)?;
    }

    let driver_api_cpp = hip_src_dir.join("driver_api_hip.cpp");
    if driver_api_cpp.exists() {
        let content = fs::read_to_string(&driver_api_cpp)?;
        let mut patched_content = rename_rdmaxcel_wrappers(&content);
        patched_content = patched_content
            .replace("libcuda.so.1", "libamdhip64.so")
            .replace("rdmaxcel_cuMemGetHandleForAddressRange", "rdmaxcel_hipMemGetHandleForAddressRange")
            .replace("_(cuMemGetHandleForAddressRange)", "_(hipMemGetHandleForAddressRange)")
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
            .replace("->cuCtxSynchronize_(", "->hipCtxSynchronize_(")
            .replace("_(cuCtxSynchronize)", "_(hipCtxSynchronize)")
            .replace("->cuGetErrorString_(", "->hipDrvGetErrorString_(")
            .replace("CUmemRangeHandleType", "hipMemRangeHandleType");
        fs::write(&driver_api_cpp, patched_content)?;
    }
    Ok(())
}

fn patch_hipified_files_rocm6(hip_src_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:warning=Patching hipify_torch output for ROCm 6.x (HSA dmabuf)...");

    let cpp_file = hip_src_dir.join("rdmaxcel_hip.cpp");
    if cpp_file.exists() {
        let content = fs::read_to_string(&cpp_file)?;
        let mut patched_content = content
            .replace("#include <hip/hip_runtime.h>", "#include <hip/hip_runtime.h>\n#include <hip/hip_version.h>\n#include <hsa/hsa.h>\n#include <hsa/hsa_ext_amd.h>")
            .replace("c10::cuda::CUDACachingAllocator", "c10::hip::HIPCachingAllocator")
            .replace("c10::cuda::CUDAAllocatorConfig", "c10::hip::HIPAllocatorConfig")
            .replace("c10::hip::HIPCachingAllocator::CUDAAllocatorConfig", "c10::hip::HIPCachingAllocator::HIPAllocatorConfig")
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

        patched_content = patched_content.replace("cuMemGetHandleForAddressRange(", "hsa_amd_portable_export_dmabuf(");
        patched_content = patched_content.replace("hsa_amd_portable_export_dmabuf(\n      &fd,\n      reinterpret_cast<hipDeviceptr_t>(start_addr),\n      total_size,\n      0 /* HSA dmabuf */,\n      0);", "hsa_amd_portable_export_dmabuf(\n      reinterpret_cast<void*>(start_addr),\n      total_size,\n      &fd,\n      nullptr);");
        patched_content = patched_content.replace("hsa_amd_portable_export_dmabuf(\n            &fd,\n            reinterpret_cast<hipDeviceptr_t>(chunk_start),\n            chunk_size,\n            0 /* HSA dmabuf */,\n            0);", "hsa_amd_portable_export_dmabuf(\n            reinterpret_cast<void*>(chunk_start),\n            chunk_size,\n            &fd,\n            nullptr);");
        patched_content = patched_content
            .replace("CUresult cu_result", "hsa_status_t hsa_result")
            .replace("hipError_t cu_result", "hsa_status_t hsa_result")
            .replace("cu_result != hipSuccess", "hsa_result != HSA_STATUS_SUCCESS")
            .replace("if (cu_result", "if (hsa_result");
        patched_content = patched_content.replace("hipPointerAttribute::device", "HIP_POINTER_ATTRIBUTE_DEVICE_ORDINAL");
        fs::write(&cpp_file, patched_content)?;
    }

    let header_file = hip_src_dir.join("rdmaxcel_hip.h");
    if header_file.exists() {
        let content = fs::read_to_string(&header_file)?;
        let patched_content = content
            .replace("#include \"driver_api.h\"", "#include \"driver_api_hip.h\"")
            .replace("CUdeviceptr", "hipDeviceptr_t");
        fs::write(&header_file, patched_content)?;
    }

    let driver_api_h = hip_src_dir.join("driver_api_hip.h");
    if driver_api_h.exists() {
        let content = fs::read_to_string(&driver_api_h)?;
        let mut patched_content = rename_rdmaxcel_wrappers(&content);
        if !patched_content.contains("#include <hsa/hsa.h>") {
            patched_content = patched_content.replace("#include <hip/hip_runtime.h>", "#include <hip/hip_runtime.h>\n#include <hsa/hsa.h>\n#include <hsa/hsa_ext_amd.h>");
        }
        let old_decl = "hipError_t rdmaxcel_cuMemGetHandleForAddressRange(\n    int* handle,\n    hipDeviceptr_t dptr,\n    size_t size,\n    CUmemRangeHandleType handleType,\n    unsigned long long flags);";
        let new_decl = "hsa_status_t rdmaxcel_hsa_amd_portable_export_dmabuf(\n    void* ptr,\n    size_t size,\n    int* fd,\n    uint64_t* flags);";
        patched_content = patched_content.replace(old_decl, new_decl);
        patched_content = patched_content.replace("CUmemRangeHandleType", "int /* placeholder - ROCm 6.x */");
        patched_content.push_str("\n\n// CUDA-compatible wrapper for monarch_rdma\nhipError_t rdmaxcel_cuMemGetHandleForAddressRange(\n    int* handle,\n    hipDeviceptr_t dptr,\n    size_t size,\n    int handleType,\n    unsigned long long flags);\n");
        fs::write(&driver_api_h, patched_content)?;
    }

    let driver_api_cpp = hip_src_dir.join("driver_api_hip.cpp");
    if driver_api_cpp.exists() {
        let content = fs::read_to_string(&driver_api_cpp)?;
        let mut patched_content = rename_rdmaxcel_wrappers(&content);
        patched_content = patched_content.replace("#include \"driver_api_hip.h\"", "#include \"driver_api_hip.h\"\n#include <hsa/hsa.h>\n#include <hsa/hsa_ext_amd.h>");
        patched_content = patched_content.replace("libcuda.so.1", "libamdhip64.so");
        patched_content = patched_content.replace("dstDevice, srcHost, ByteCount);", "dstDevice, const_cast<void*>(srcHost), ByteCount);");
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
            .replace("->cuCtxSynchronize_(", "->hipCtxSynchronize_(")
            .replace("->cuGetErrorString_(", "->hipDrvGetErrorString_(");

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
            .replace("_(cuCtxSynchronize)", "_(hipCtxSynchronize)")
            .replace("_(cuGetErrorString)", "_(hipDrvGetErrorString)");

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
        patched_content = patched_content.replace("CUmemRangeHandleType", "int /* placeholder - ROCm 6.x */");
        patched_content = patched_content.replace("_(hipMemGetHandleForAddressRange)  \\", "/* hipMemGetHandleForAddressRange removed for ROCm 6.x - using HSA */  \\");
        patched_content = patched_content.replace("_(hipMemGetHandleForAddressRange) \\", "/* hipMemGetHandleForAddressRange removed for ROCm 6.x - using HSA */ \\");

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
    Ok(())
}

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
            return Err(format!("Required hipified file {} was not found", file_name).into());
        }
    }
    Ok(())
}

/// Hipify sources for rdmaxcel-sys using build_utils::run_hipify_torch
/// and apply rdmaxcel-specific patches based on ROCm version
fn hipify_sources(
    src_dir: &Path,
    hip_src_dir: &Path,
    rocm_version: (u32, u32),
) -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:warning=Hipifying sources from {} to {}...", src_dir.display(), hip_src_dir.display());

    // Collect source files to hipify
    let files_to_copy = [
        "lib.rs", "rdmaxcel.h", "rdmaxcel.c", "rdmaxcel.cpp", "rdmaxcel.cu",
        "test_rdmaxcel.c", "driver_api.h", "driver_api.cpp",
    ];

    let source_files: Vec<PathBuf> = files_to_copy
        .iter()
        .map(|f| src_dir.join(f))
        .filter(|p| p.exists())
        .collect();

    // Find project root
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let project_root = manifest_dir.parent().ok_or("Failed to find project root")?;

    // Use centralized hipify function from build_utils
    build_utils::run_hipify_torch(project_root, &source_files, hip_src_dir)
        .map_err(|e| format!("hipify_torch failed: {}", e))?;

    // Apply rdmaxcel-specific patches based on ROCm version
    let (major, _minor) = rocm_version;
    if major >= 7 {
        patch_hipified_files_rocm7(hip_src_dir)?;
    } else {
        patch_hipified_files_rocm6(hip_src_dir)?;
    }

    Ok(())
}

fn get_libtorch_include_dirs(python_interpreter: &Path) -> Vec<PathBuf> {
    let mut include_dirs = Vec::new();
    if let Ok(output) = Command::new(python_interpreter).arg("-c").arg(build_utils::PYTHON_PRINT_PYTORCH_DETAILS).output() {
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            if let Some(path) = line.strip_prefix("LIBTORCH_INCLUDE: ") {
                include_dirs.push(PathBuf::from(path));
            }
        }
    }
    include_dirs
}

#[cfg(target_os = "macos")]
fn main() {}

#[cfg(not(target_os = "macos"))]
fn main() {
    // Declare custom cfg options to avoid warnings
    println!("cargo::rustc-check-cfg=cfg(cargo)");
    println!("cargo::rustc-check-cfg=cfg(rocm_6_x)");
    println!("cargo::rustc-check-cfg=cfg(rocm_7_plus)");

    println!("cargo:rustc-link-lib=ibverbs");
    println!("cargo:rustc-link-lib=mlx5");

    let (is_rocm, compute_home, compute_lib_names, rocm_version) =
        if let Ok(rocm_home) = build_utils::validate_rocm_installation() {
            let version = build_utils::get_rocm_version(&rocm_home).unwrap_or((6, 0));
            println!("cargo:warning=Using HIP/ROCm {} from {}", format!("{}.{}", version.0, version.1), rocm_home);
            if version.0 >= 7 { println!("cargo:rustc-cfg=rocm_7_plus"); } else { println!("cargo:rustc-cfg=rocm_6_x"); }
            (true, rocm_home, vec!["amdhip64", "hsa-runtime64"], version)
        } else if let Ok(cuda_home) = build_utils::validate_cuda_installation() {
            println!("cargo:warning=Using CUDA from {}", cuda_home);
            (false, cuda_home, vec!["cuda", "cudart"], (0, 0))
        } else {
            eprintln!("Error: Neither CUDA nor ROCm installation found!");
            std::process::exit(1);
        };

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let src_dir = manifest_dir.join("src");
    let python_interpreter = build_utils::find_python_interpreter();
    let compute_include_path = format!("{}/include", compute_home);
    println!("cargo:rustc-env=CUDA_INCLUDE_PATH={}", compute_include_path);

    let python_config = match build_utils::python_env_dirs_with_interpreter("python3") {
        Ok(config) => config,
        Err(_) => build_utils::PythonConfig { include_dir: None, lib_dir: None },
    };

    let compute_lib_dir = if is_rocm { build_utils::get_rocm_lib_dir().unwrap() } else { build_utils::get_cuda_lib_dir().unwrap() };
    println!("cargo:rustc-link-search=native={}", compute_lib_dir);
    for lib_name in &compute_lib_names {
        println!("cargo:rustc-link-lib={}", lib_name);
    }

    let use_pytorch_apis = build_utils::get_env_var_with_rerun("TORCH_SYS_USE_PYTORCH_APIS").unwrap_or_else(|_| "1".to_owned());
    let libtorch_include_dirs: Vec<PathBuf> = if use_pytorch_apis == "1" {
        get_libtorch_include_dirs(&python_interpreter)
    } else {
        Vec::new()
    };

    if use_pytorch_apis == "1" {
         if let Ok(output) = Command::new(&python_interpreter).arg("-c").arg(build_utils::PYTHON_PRINT_PYTORCH_DETAILS).output() {
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
        if is_rocm { println!("cargo:rustc-link-lib=c10_hip"); } else { println!("cargo:rustc-link-lib=c10_cuda"); }
    }
    println!("cargo:rustc-link-lib=dl");

    match env::var("OUT_DIR") {
        Ok(out_dir) => {
            let out_path = PathBuf::from(out_dir);
            let (code_dir, header_path, c_source_path, cpp_source_path, cuda_source_path, driver_api_cpp_path);

            if is_rocm {
                let hip_src_dir = out_path.join("hipified_src");
                hipify_sources(&src_dir, &hip_src_dir, rocm_version).expect("Failed to hipify sources");
                validate_hipified_files(&hip_src_dir).expect("Hipified files validation failed");
                code_dir = hip_src_dir.clone();
                header_path = hip_src_dir.join("rdmaxcel_hip.h");
                c_source_path = hip_src_dir.join("rdmaxcel_hip.c");
                cpp_source_path = hip_src_dir.join("rdmaxcel_hip.cpp");
                cuda_source_path = hip_src_dir.join("rdmaxcel.hip");
                driver_api_cpp_path = hip_src_dir.join("driver_api_hip.cpp");
            } else {
                code_dir = src_dir.clone();
                header_path = src_dir.join("rdmaxcel.h");
                c_source_path = src_dir.join("rdmaxcel.c");
                cpp_source_path = src_dir.join("rdmaxcel.cpp");
                cuda_source_path = src_dir.join("rdmaxcel.cu");
                driver_api_cpp_path = src_dir.join("driver_api.cpp");
            }

            // Bindgen setup
            let mut builder = bindgen::Builder::default()
                .header(header_path.to_string_lossy())
                .clang_arg("-x").clang_arg("c++").clang_arg("-std=gnu++20")
                .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
                .allowlist_function("ibv_.*").allowlist_function("mlx5dv_.*").allowlist_function("mlx5_wqe_.*")
                .allowlist_function("create_qp").allowlist_function("create_mlx5dv_.*")
                .allowlist_function("register_cuda_memory").allowlist_function("register_hip_memory")
                .allowlist_function("db_ring").allowlist_function("cqe_poll")
                .allowlist_function("send_wqe").allowlist_function("recv_wqe")
                .allowlist_function("launch_db_ring").allowlist_function("launch_cqe_poll")
                .allowlist_function("launch_send_wqe").allowlist_function("launch_recv_wqe")
                .allowlist_function("rdma_get_active_segment_count").allowlist_function("rdma_get_all_segment_info")
                .allowlist_function("pt_cuda_allocator_compatibility").allowlist_function("pt_hip_allocator_compatibility")
                .allowlist_function("register_segments").allowlist_function("deregister_segments")
                .allowlist_function("register_dmabuf_buffer").allowlist_function("get_hip_pci_address_from_ptr")
                .allowlist_function("rdmaxcel_cu.*").allowlist_function("rdmaxcel_hip.*").allowlist_function("rdmaxcel_hsa.*")
                .allowlist_function("rdmaxcel_qp_.*").allowlist_function("rdmaxcel_print_device_info")
                .allowlist_function("rdmaxcel_error_string").allowlist_function("completion_cache_.*")
                .allowlist_function("poll_cq_with_cache").allowlist_function("rdmaxcel_register_segment_scanner")
                .allowlist_type("rdmaxcel_qp_t").allowlist_type("rdmaxcel_qp")
                .allowlist_type("rdmaxcel_error_code_t").allowlist_type("completion_cache_t")
                .allowlist_type("completion_cache").allowlist_type("completion_node_t")
                .allowlist_type("completion_node").allowlist_type("poll_context_t")
                .allowlist_type("poll_context").allowlist_type("rdma_qp_type_t")
                .allowlist_type("rdmaxcel_segment_scanner_fn")
                .allowlist_type("CUdeviceptr").allowlist_type("CUdevice").allowlist_type("CUresult")
                .allowlist_type("CUcontext").allowlist_type("CUmemRangeHandleType")
                .allowlist_var("CUDA_SUCCESS").allowlist_var("CU_.*")
                .allowlist_type("hipDeviceptr_t").allowlist_type("hipDevice_t").allowlist_type("hipError_t")
                .allowlist_type("hipCtx_t").allowlist_type("hipPointer_attribute")
                .allowlist_var("hipSuccess").allowlist_var("HIP_.*")
                .allowlist_type("hsa_status_t").allowlist_var("HSA_STATUS_SUCCESS")
                .allowlist_type("ibv_.*").allowlist_type("mlx5dv_.*").allowlist_type("mlx5_wqe_.*")
                .allowlist_type("cqe_poll_result_t").allowlist_type("wqe_params_t")
                .allowlist_type("cqe_poll_params_t").allowlist_type("rdma_segment_info_t")
                .allowlist_var("MLX5_.*").allowlist_var("IBV_.*").allowlist_var("RDMA_QP_TYPE_.*")
                .blocklist_type("ibv_wc").blocklist_type("mlx5_wqe_ctrl_seg")
                .bitfield_enum("ibv_access_flags").bitfield_enum("ibv_qp_attr_mask")
                .bitfield_enum("ibv_wc_flags").bitfield_enum("ibv_send_flags")
                .bitfield_enum("ibv_port_cap_flags").constified_enum_module("ibv_qp_type")
                .constified_enum_module("ibv_qp_state").constified_enum_module("ibv_port_state")
                .constified_enum_module("ibv_wc_opcode").constified_enum_module("ibv_wr_opcode")
                .constified_enum_module("ibv_wc_status")
                .derive_default(true).prepend_enum_name(false);

            builder = builder.clang_arg(format!("-I{}", compute_include_path));
            if is_rocm {
                builder = builder.clang_arg("-D__HIP_PLATFORM_AMD__=1").clang_arg("-DUSE_ROCM=1");
                if rocm_version.0 >= 7 { builder = builder.clang_arg("-DROCM_7_PLUS=1"); } else { builder = builder.clang_arg("-DROCM_6_X=1"); }
            }
            if let Some(include_dir) = &python_config.include_dir {
                builder = builder.clang_arg(format!("-I{}", include_dir));
            }
            let bindings = builder.generate().expect("Unable to generate bindings");
            bindings.write_to_file(out_path.join("bindings.rs")).expect("Couldn't write bindings");

            println!("cargo:rustc-cfg=cargo");

            // Compile C files (rdmaxcel.c)
            if c_source_path.exists() {
                let mut build = cc::Build::new();
                build.file(&c_source_path).include(&code_dir).flag("-fPIC");
                build.include(&compute_include_path);
                if is_rocm {
                    build.define("__HIP_PLATFORM_AMD__", "1").define("USE_ROCM", "1");
                    if rocm_version.0 >= 7 { build.define("ROCM_7_PLUS", "1"); } else { build.define("ROCM_6_X", "1"); }
                }
                build.compile("rdmaxcel");
            }

            // Compile C++ files (rdmaxcel.cpp)
            if cpp_source_path.exists() {
                let mut cpp_build = cc::Build::new();
                cpp_build.file(&cpp_source_path).include(&code_dir).flag("-fPIC").cpp(true).flag("-std=gnu++20").flag("-Wno-unused-parameter").define("PYTORCH_C10_DRIVER_API_SUPPORTED", "1");
                // Suppress deprecated API warnings for HIP context management APIs (deprecated in ROCm 6.x)
                if is_rocm {
                    cpp_build.flag("-Wno-deprecated-declarations");
                }
                if driver_api_cpp_path.exists() { cpp_build.file(&driver_api_cpp_path); }
                cpp_build.include(&compute_include_path);
                if is_rocm {
                    cpp_build.define("__HIP_PLATFORM_AMD__", "1").define("USE_ROCM", "1");
                    if rocm_version.0 >= 7 { cpp_build.define("ROCM_7_PLUS", "1"); } else { cpp_build.define("ROCM_6_X", "1"); }
                }
                for include_dir in &libtorch_include_dirs { cpp_build.include(include_dir); }
                if let Some(include_dir) = &python_config.include_dir { cpp_build.include(include_dir); }
                cpp_build.compile("rdmaxcel_cpp");
            }

            // Compile CUDA/HIP files
            if cuda_source_path.exists() {
                let compiler_path = if is_rocm { format!("{}/bin/hipcc", compute_home) } else { format!("{}/bin/nvcc", compute_home) };
                let cuda_build_dir = format!("{}/target/cuda_build", manifest_dir.display());
                std::fs::create_dir_all(&cuda_build_dir).expect("Failed to create CUDA build directory");
                let cuda_obj_path = format!("{}/rdmaxcel_cuda.o", cuda_build_dir);
                let cuda_lib_path = format!("{}/librdmaxcel_cuda.a", cuda_build_dir);

                let compiler_output = if is_rocm {
                     let mut cmd = Command::new(&compiler_path);
                     cmd.args(["-c", cuda_source_path.to_str().unwrap(), "-o", &cuda_obj_path, "-fPIC", "-std=c++20", "-D__HIP_PLATFORM_AMD__=1", "-DUSE_ROCM=1", &format!("-I{}", compute_include_path), &format!("-I{}", code_dir.display()), "-I/usr/include", "-I/usr/include/infiniband"]);
                     if rocm_version.0 >= 7 { cmd.arg("-DROCM_7_PLUS=1"); } else { cmd.arg("-DROCM_6_X=1"); }
                     cmd.output()
                } else {
                     Command::new(&compiler_path).args(["-c", cuda_source_path.to_str().unwrap(), "-o", &cuda_obj_path, "-fPIC", "-std=c++20", "-Xcompiler", "-fPIC", &format!("-I{}", compute_include_path), &format!("-I{}", code_dir.display()), "-I/usr/include", "-I/usr/include/infiniband"]).output()
                };

                match compiler_output {
                    Ok(output) => {
                        if !output.status.success() { panic!("Failed to compile CUDA/HIP source: {}", String::from_utf8_lossy(&output.stderr)); }
                    }
                    Err(e) => panic!("Failed to run compiler: {}", e),
                }

                let ar_output = Command::new("ar").args(["rcs", &cuda_lib_path, &cuda_obj_path]).output();
                if let Ok(output) = ar_output {
                    if !output.status.success() { panic!("Failed to create static library"); }
                    println!("cargo:rustc-link-lib=static=rdmaxcel_cuda");
                    println!("cargo:rustc-link-search=native={}", cuda_build_dir);
                    if let Err(e) = std::fs::copy(&cuda_lib_path, out_path.join("librdmaxcel_cuda.a")) {
                         eprintln!("Warning: Failed to copy CUDA library: {}", e);
                    }
                }
            }
        }
        Err(_) => println!("Note: OUT_DIR not set"),
    }
}
