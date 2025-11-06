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
    let mut builder = bindgen::Builder::default()
        // Parse nccl.h as C++ with -std=gnu++20.
        .header("src/nccl.h")
        .clang_arg("-x")
        .clang_arg("c++")
        .clang_arg("-std=gnu++20")
        .clang_arg(format!(
            "-I{}/include",
            build_utils::find_cuda_home().unwrap()
        ))
        .clang_arg(format!(
            "-I{}/include",
            build_utils::find_nccl_home().unwrap()
        ))
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        // Communicator creation and management
        .allowlist_function("ncclGetLastError")
        .allowlist_function("ncclGetErrorString")
        .allowlist_function("ncclGetVersion")
        .allowlist_function("ncclGetUniqueId")
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
        // Random nccl stuff we want
        .allowlist_function("cudaStream.*")
        .allowlist_function("cudaSetDevice")
        .allowlist_type("ncclComm_t")
        .allowlist_type("ncclResult_t")
        .allowlist_type("ncclDataType_t")
        .allowlist_type("ncclRedOp_t")
        .allowlist_type("ncclScalarResidence_t")
        .allowlist_type("ncclConfig_t")
        .allowlist_type("ncclSimInfo_t")
        .allowlist_var("NCCL_SPLIT_NOCOLOR")
        .allowlist_var("NCCL_MAJOR")
        .allowlist_var("NCCL_MINOR")
        .allowlist_var("NCCL_PATCH")
        .blocklist_type("ncclUniqueId")
        .default_enum_style(bindgen::EnumVariation::NewType {
            is_bitfield: false,
            is_global: false,
        });

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
        // Set cargo metadata to inform dependent binaries about how to set their
        // RPATH (see controller/build.rs for an example).
        println!("cargo::metadata=LIB_PATH={}", lib_dir);
    }

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    builder
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");

    println!("cargo::rustc-link-lib=nccl");
    println!("cargo::rustc-link-search=native={}/lib", build_utils::find_nccl_home().unwrap());
    println!("cargo::rustc-cfg=cargo");
    println!("cargo::rustc-check-cfg=cfg(cargo)");
}
