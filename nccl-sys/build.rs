/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::env;
use std::path::PathBuf;

#[cfg(target_os = "macos")]
fn main() {}

#[cfg(not(target_os = "macos"))]
fn main() {
    // Declare custom cfg options to avoid warnings
    println!("cargo::rustc-check-cfg=cfg(cargo)");
    println!("cargo::rustc-check-cfg=cfg(rocm)");
    println!("cargo::rustc-check-cfg=cfg(rocm_6_x)");
    println!("cargo::rustc-check-cfg=cfg(rocm_7_plus)");

    // Auto-detect ROCm vs CUDA using build_utils
    let (is_rocm, compute_home, _rocm_version) =
        if let Ok(rocm_home) = build_utils::validate_rocm_installation() {
            let version = build_utils::get_rocm_version(&rocm_home).unwrap_or((6, 0));
            println!(
                "cargo:warning=nccl-sys: Using RCCL from ROCm {}.{} at {}",
                version.0, version.1, rocm_home
            );
            println!("cargo:rustc-cfg=rocm");
            if version.0 >= 7 {
                println!("cargo:rustc-cfg=rocm_7_plus");
            } else {
                println!("cargo:rustc-cfg=rocm_6_x");
            }
            (true, rocm_home, version)
        } else if let Ok(cuda_home) = build_utils::validate_cuda_installation() {
            println!(
                "cargo:warning=nccl-sys: Using NCCL from CUDA at {}",
                cuda_home
            );
            (false, cuda_home, (0, 0))
        } else {
            eprintln!("Error: Neither CUDA nor ROCm installation found!");
            std::process::exit(1);
        };

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let src_dir = manifest_dir.join("src");
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    let compute_include_path = format!("{}/include", compute_home);

    // Determine source paths based on platform
    let (bridge_cpp_path, bridge_include_dir, header_path) = if is_rocm {
        // Hipify bridge.h and bridge.cpp for ROCm
        let hip_src_dir = out_path.join("hipified_src");
        let project_root = manifest_dir.parent().expect("Failed to find project root");

        // Only hipify files that exist
        let source_files = vec![
            src_dir.join("bridge.h"),
            src_dir.join("bridge.cpp"),
        ];

        build_utils::run_hipify_torch(project_root, &source_files, &hip_src_dir)
            .expect("Failed to hipify nccl-sys sources");

        // Apply ROCm-specific patches to hipified bridge.cpp
        // hipify_torch doesn't catch all cudaStream_t occurrences in the .cpp file
        let bridge_cpp_hipified = hip_src_dir.join("bridge.cpp");
        if bridge_cpp_hipified.exists() {
            let content = std::fs::read_to_string(&bridge_cpp_hipified)
                .expect("Failed to read hipified bridge.cpp");
            let patched = content
                // Fix include path to use hipified header
                .replace("#include \"bridge.h\"", "#include \"bridge_hip.h\"")
                // Replace all cudaStream_t with hipStream_t
                .replace("cudaStream_t", "hipStream_t")
                // Patch dlopen library name for RCCL
                .replace("libnccl.so", "librccl.so")
                .replace("libnccl.so.2", "librccl.so");
            std::fs::write(&bridge_cpp_hipified, patched)
                .expect("Failed to write patched bridge.cpp");
        }

        (
            bridge_cpp_hipified,
            hip_src_dir.clone(),
            hip_src_dir.join("bridge_hip.h"),
        )
    } else {
        (
            src_dir.join("bridge.cpp"),
            src_dir.clone(),
            src_dir.join("bridge.h"),
        )
    };

    // Compile the bridge.cpp file
    let mut cc_builder = cc::Build::new();
    cc_builder
        .cpp(true)
        .file(&bridge_cpp_path)
        .include(&bridge_include_dir)
        .flag("-std=c++14");

    // Include compute headers (CUDA or ROCm)
    cc_builder.include(&compute_include_path);

    if is_rocm {
        cc_builder
            .define("__HIP_PLATFORM_AMD__", "1")
            .define("USE_ROCM", "1");
    }

    cc_builder.compile("nccl_bridge");

    // Set up bindgen using bridge.h (which contains all NCCL declarations)
    let mut builder = bindgen::Builder::default()
        .header(header_path.to_string_lossy())
        .clang_arg("-x")
        .clang_arg("c++")
        .clang_arg("-std=c++14")
        .clang_arg(format!("-I{}", compute_include_path))
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
        // CUDA/HIP runtime functions
        .allowlist_function("cudaSetDevice")
        .allowlist_function("cudaStreamSynchronize")
        .allowlist_function("hipSetDevice")
        .allowlist_function("hipStreamSynchronize")
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
        .allowlist_type("hipError_t")
        .allowlist_type("hipStream_t")
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

    // Add platform-specific defines for bindgen
    if is_rocm {
        builder = builder
            .clang_arg("-D__HIP_PLATFORM_AMD__=1")
            .clang_arg("-DUSE_ROCM=1");
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
    builder
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");

    // Platform-specific linking
    if is_rocm {
        // ROCm: Link against RCCL and HIP runtime
        println!("cargo::rustc-link-lib=rccl");
        println!("cargo::rustc-link-search=native={}/lib", compute_home);

        // Link HIP runtime
        let hip_lib_dir = format!("{}/lib", compute_home);
        println!("cargo::rustc-link-search=native={}", hip_lib_dir);
        println!("cargo::rustc-link-lib=amdhip64");
    } else {
        // CUDA: We no longer link against nccl directly since we dlopen it
        // But we do link against CUDA runtime statically
        let cuda_lib_dir = build_utils::get_cuda_lib_dir();
        println!("cargo::rustc-link-search=native={}", cuda_lib_dir);

        println!("cargo::rustc-link-lib=static=cudart_static");
        // cudart_static requires linking against librt, libpthread, and libdl
        println!("cargo::rustc-link-lib=rt");
        println!("cargo::rustc-link-lib=pthread");
        println!("cargo::rustc-link-lib=dl");
    }

    println!("cargo::rustc-cfg=cargo");
}
