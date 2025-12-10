/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::path::PathBuf;

#[cfg(target_os = "macos")]
fn main() {}

#[cfg(not(target_os = "macos"))]
fn main() {
    // Compile the bridge.cpp file
    let mut cc_builder = cc::Build::new();
    cc_builder
        .cpp(true)
        .file("src/bridge.cpp")
        .flag("-std=c++14");

    // Include CUDA headers
    if let Some(cuda_home) = build_utils::find_cuda_home() {
        cc_builder.include(format!("{}/include", cuda_home));
    }

    cc_builder.compile("nccl_bridge");

    let mut builder = bindgen::Builder::default()
        .header("src/bridge.h")
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
        // CUDA runtime functions
        .allowlist_function("cudaSetDevice")
        .allowlist_function("cudaStreamSynchronize")
        // Types
        .allowlist_type("ncclComm_t")
        .allowlist_type("ncclResult_t")
        .allowlist_type("ncclDataType_t")
        .allowlist_type("ncclRedOp_t")
        .allowlist_type("ncclScalarResidence_t")
        .allowlist_type("ncclSimInfo_t")
        .allowlist_type("ncclConfig_t")
        .allowlist_type("cudaError_t")
        .allowlist_type("cudaStream_t")
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

    // Include CUDA headers
    if let Some(cuda_home) = build_utils::find_cuda_home() {
        builder = builder.clang_arg(format!("-I{}/include", cuda_home));
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

    // Generate bindings (NCCL + CUDA runtime combined)
    builder
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");

    // We no longer link against nccl directly since we dlopen it
    // But we do link against CUDA runtime statically
    // Add CUDA library search path first
    let cuda_lib_dir = build_utils::get_cuda_lib_dir();
    println!("cargo::rustc-link-search=native={}", cuda_lib_dir);

    println!("cargo::rustc-link-lib=static=cudart_static");
    // cudart_static requires linking against librt, libpthread, and libdl
    println!("cargo::rustc-link-lib=rt");
    println!("cargo::rustc-link-lib=pthread");
    println!("cargo::rustc-link-lib=dl");
    println!("cargo::rustc-cfg=cargo");
    println!("cargo::rustc-check-cfg=cfg(cargo)");
}
