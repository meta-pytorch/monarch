/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Build script for nccl-sys
//!
//! Supports both CUDA (NCCL) and ROCm (RCCL) backends.

use std::path::Path;
use std::path::PathBuf;

#[cfg(target_os = "macos")]
fn main() {}

#[cfg(not(target_os = "macos"))]
fn main() {
    use std::env;

    // Detect platform: ROCm or CUDA
    let (is_rocm, compute_home) = build_utils::detect_gpu_platform();

    // Get directories
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let out_dir = env::var("OUT_DIR").unwrap();
    let out_path = PathBuf::from(&out_dir);
    let src_dir = PathBuf::from(&manifest_dir).join("src");

    // Determine source directory based on platform
    let source_dir = if is_rocm {
        // For ROCm: hipify sources first, then use hipified directory
        let hip_dir = out_path.join("hipified_src");
        build_utils::rocm::hipify_sources(
            &src_dir,
            &["bridge.h", "bridge.cpp"],
            &hip_dir,
            &manifest_dir,
        )
        .expect("hipify failed");
        hip_dir
    } else {
        // For CUDA: use original sources
        src_dir.clone()
    };

    // Setup rerun triggers
    println!("cargo:rerun-if-changed=src/bridge.h");
    println!("cargo:rerun-if-changed=src/bridge.cpp");

    // Find the header and source files (hipified or original)
    let header_file = find_header(&source_dir, is_rocm);
    let cpp_file = find_cpp_source(&source_dir, is_rocm);

    // Compile the bridge.cpp file
    let mut cc_builder = cc::Build::new();
    cc_builder
        .cpp(true)
        .file(&cpp_file)
        .flag("-std=c++14")
        .include(&source_dir)
        .include(format!("{}/include", compute_home));

    if is_rocm {
        cc_builder
            .define("__HIP_PLATFORM_AMD__", "1")
            .define("USE_ROCM", "1");
    }

    cc_builder.compile("nccl_bridge");

    let mut builder = bindgen::Builder::default()
        .header(header_file.to_str().unwrap())
        .clang_arg("-x")
        .clang_arg("c++")
        .clang_arg("-std=c++14")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        // Version and error handling
        .allowlist_function("ncclGetVersion")
        .allowlist_function("ncclGetUniqueId")
        .allowlist_function("ncclGetErrorString")
        .allowlist_function("ncclGetLastError")
        // Communicator creation and management
        .allowlist_function("ncclCommInitRank")
        .allowlist_function("ncclCommInitAll")
        .allowlist_function("ncclCommInitRankConfig")
        .allowlist_function("ncclCommInitRankScalable")
        .allowlist_function("ncclCommSplit")
        .allowlist_function("ncclCommFinalize")
        .allowlist_function("ncclCommDestroy")
        .allowlist_function("ncclCommAbort")
        .allowlist_function("ncclCommGetAsyncError")
        .allowlist_function("ncclCommCount")
        .allowlist_function("ncclCommCuDevice")
        .allowlist_function("ncclCommUserRank")
        .allowlist_function("ncclCommRegister")
        .allowlist_function("ncclCommDeregister")
        .allowlist_function("ncclMemAlloc")
        .allowlist_function("ncclMemFree")
        // Collective communication
        .allowlist_function("ncclAllReduce")
        .allowlist_function("ncclBroadcast")
        .allowlist_function("ncclReduce")
        .allowlist_function("ncclAllGather")
        .allowlist_function("ncclReduceScatter")
        // Group calls
        .allowlist_function("ncclGroupStart")
        .allowlist_function("ncclGroupEnd")
        .allowlist_function("ncclGroupSimulateEnd")
        // Point to point communication
        .allowlist_function("ncclSend")
        .allowlist_function("ncclRecv")
        // User-defined reduction operators
        .allowlist_function("ncclRedOpCreatePreMulSum")
        .allowlist_function("ncclRedOpDestroy")
        // Types
        .allowlist_type("ncclComm_t")
        .allowlist_type("ncclResult_t")
        .allowlist_type("ncclDataType_t")
        .allowlist_type("ncclRedOp_t")
        .allowlist_type("ncclScalarResidence_t")
        .allowlist_type("ncclSimInfo_t")
        .allowlist_type("ncclConfig_t")
        // Constants
        .allowlist_var("NCCL_SPLIT_NOCOLOR")
        .allowlist_var("NCCL_MAJOR")
        .allowlist_var("NCCL_MINOR")
        .allowlist_var("NCCL_PATCH")
        .blocklist_type("ncclUniqueId")
        .default_enum_style(bindgen::EnumVariation::NewType {
            is_bitfield: false,
            is_global: false,
        });

    // Runtime API functions and types
    // Note: hipify converts CUDA names to HIP names in the header
    if is_rocm {
        builder = builder
            .allowlist_function("hipSetDevice")
            .allowlist_function("hipStreamSynchronize")
            .allowlist_type("hipError_t")
            .allowlist_type("hipStream_t")
            .clang_arg(format!("-I{}/include", compute_home))
            .clang_arg("-D__HIP_PLATFORM_AMD__");
    } else {
        builder = builder
            .allowlist_function("cudaSetDevice")
            .allowlist_function("cudaStreamSynchronize")
            .allowlist_type("cudaError_t")
            .allowlist_type("cudaStream_t")
            .clang_arg(format!("-I{}/include", compute_home));
    }

    // Include headers and libs from the active environment
    let python_config = match build_utils::python_env_dirs() {
        Ok(config) => config,
        Err(_) => {
            eprintln!("Warning: Failed to get Python environment directories");
            build_utils::PythonConfig {
                include_dir: None,
                lib_dir: None,
            }
        }
    };

    if let Some(include_dir) = &python_config.include_dir {
        builder = builder.clang_arg(format!("-I{}", include_dir));
    }
    if let Some(lib_dir) = &python_config.lib_dir {
        println!("cargo::rustc-link-search=native={}", lib_dir);
        println!("cargo::metadata=LIB_PATH={}", lib_dir);
    }

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(std::env::var("OUT_DIR").unwrap());

    // Generate bindings (NCCL/RCCL + CUDA/HIP runtime combined)
    builder
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");

    // We no longer link against nccl/rccl directly since we dlopen it
    // But we do link against the compute runtime
    if is_rocm {
        // ROCm: Link dynamically to HIP runtime
        println!("cargo::rustc-link-lib=amdhip64");
        println!("cargo::rustc-link-search=native={}/lib", compute_home);
        println!("cargo::rustc-link-lib=dl");
    } else {
        // CUDA: Link statically to CUDA runtime
        let cuda_lib_dir = build_utils::get_cuda_lib_dir();
        println!("cargo::rustc-link-search=native={}", cuda_lib_dir);
        println!("cargo::rustc-link-lib=static=cudart_static");
        // cudart_static requires linking against librt, libpthread, and libdl
        println!("cargo::rustc-link-lib=rt");
        println!("cargo::rustc-link-lib=pthread");
        println!("cargo::rustc-link-lib=dl");
    }

    println!("cargo::rustc-cfg=cargo");
    println!("cargo::rustc-check-cfg=cfg(cargo)");

    // Emit cfg for ROCm so Rust code can conditionally compile
    if is_rocm {
        println!("cargo::rustc-cfg=use_rocm");
    }
    println!("cargo::rustc-check-cfg=cfg(use_rocm)");
}

/// Find the main header file (bridge.h or bridge_hip.h)
fn find_header(dir: &Path, is_rocm: bool) -> PathBuf {
    let names = if is_rocm {
        vec!["bridge_hip.h", "bridge.h"]
    } else {
        vec!["bridge.h"]
    };

    for name in names {
        let path = dir.join(name);
        if path.exists() {
            return path;
        }
    }
    panic!("Could not find bridge header in {:?}", dir);
}

/// Find C++ source file
fn find_cpp_source(dir: &Path, is_rocm: bool) -> PathBuf {
    let names = if is_rocm {
        vec!["bridge_hip.cpp", "bridge.cpp"]
    } else {
        vec!["bridge.cpp"]
    };

    for name in names {
        let path = dir.join(name);
        if path.exists() {
            return path;
        }
    }
    panic!("Could not find bridge.cpp in {:?}", dir);
}
