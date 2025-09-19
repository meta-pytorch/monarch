/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::env;
use std::path::Path;
use std::path::PathBuf;

use build_utils::*;

#[cfg(target_os = "macos")]
fn main() {}

#[cfg(not(target_os = "macos"))]
fn main() {
    // Tell cargo to look for shared libraries in the specified directory
    println!("cargo:rustc-link-search=/usr/lib");
    println!("cargo:rustc-link-search=/usr/lib64");

    // Link against the ibverbs library
    println!("cargo:rustc-link-lib=ibverbs");

    // Link against the mlx5 library
    println!("cargo:rustc-link-lib=mlx5");

    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=src/rdmaxcel.h");
    println!("cargo:rerun-if-changed=src/rdmaxcel.c");
    println!("cargo:rerun-if-changed=src/rdmaxcel.cpp");

    // Validate CUDA installation and get CUDA home path
    let cuda_home = match build_utils::validate_cuda_installation() {
        Ok(home) => home,
        Err(_) => {
            build_utils::print_cuda_error_help();
            std::process::exit(1);
        }
    };

    // Get the directory of the current crate
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| {
        // For buck2 run, we know the package is in fbcode/monarch/rdmaxcel-sys
        // Get the fbsource directory from the current directory path
        let current_dir = std::env::current_dir().expect("Failed to get current directory");
        let current_path = current_dir.to_string_lossy();

        // Find the fbsource part of the path
        if let Some(fbsource_pos) = current_path.find("fbsource") {
            let fbsource_path = &current_path[..fbsource_pos + "fbsource".len()];
            format!("{}/fbcode/monarch/rdmaxcel-sys", fbsource_path)
        } else {
            // If we can't find fbsource in the path, just use the current directory
            format!("{}/src", current_dir.to_string_lossy())
        }
    });

    // Create the absolute path to the header file
    let header_path = format!("{}/src/rdmaxcel.h", manifest_dir);

    // Check if the header file exists
    if !Path::new(&header_path).exists() {
        panic!("Header file not found at {}", header_path);
    }

    // Start building the bindgen configuration
    let mut builder = bindgen::Builder::default()
        // The input header we would like to generate bindings for
        .header(&header_path)
        .clang_arg("-x")
        .clang_arg("c++")
        .clang_arg("-std=gnu++20")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        // Allow the specified functions, types, and variables
        .allowlist_function("ibv_.*")
        .allowlist_function("mlx5dv_.*")
        .allowlist_function("mlx5_wqe_.*")
        .allowlist_function("create_qp")
        .allowlist_function("create_mlx5dv_.*")
        .allowlist_function("register_cuda_memory")
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
        .allowlist_function("register_segments")
        .allowlist_function("pt_cuda_allocator_compatibility")
        .allowlist_type("ibv_.*")
        .allowlist_type("mlx5dv_.*")
        .allowlist_type("mlx5_wqe_.*")
        .allowlist_type("cqe_poll_result_t")
        .allowlist_type("wqe_params_t")
        .allowlist_type("cqe_poll_params_t")
        .allowlist_type("rdma_segment_info_t")
        .allowlist_var("MLX5_.*")
        .allowlist_var("IBV_.*")
        // Block specific types that are manually defined in lib.rs
        .blocklist_type("ibv_wc")
        .blocklist_type("mlx5_wqe_ctrl_seg")
        // Apply the same bindgen flags as in the BUCK file
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

    // Add CUDA include path (we already validated it exists)
    let cuda_include_path = format!("{}/include", cuda_home);
    println!("cargo:rustc-env=CUDA_INCLUDE_PATH={}", cuda_include_path);
    builder = builder.clang_arg(format!("-I{}", cuda_include_path));

    // Include headers and libs from the active environment.
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

    if let Some(include_dir) = &python_config.include_dir {
        builder = builder.clang_arg(format!("-I{}", include_dir));
    }
    if let Some(lib_dir) = &python_config.lib_dir {
        println!("cargo:rustc-link-search=native={}", lib_dir);
        // Set cargo metadata to inform dependent binaries about how to set their
        // RPATH (see controller/build.rs for an example).
        println!("cargo:metadata=LIB_PATH={}", lib_dir);
    }

    // Get CUDA library directory and emit link directives
    let cuda_lib_dir = match build_utils::get_cuda_lib_dir() {
        Ok(dir) => dir,
        Err(_) => {
            build_utils::print_cuda_lib_error_help();
            std::process::exit(1);
        }
    };
    println!("cargo:rustc-link-search=native={}", cuda_lib_dir);
    println!("cargo:rustc-link-lib=cuda");
    println!("cargo:rustc-link-lib=cudart");

    // Generate bindings
    let bindings = builder.generate().expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file
    match env::var("OUT_DIR") {
        Ok(out_dir) => {
            let out_path = PathBuf::from(out_dir);
            match bindings.write_to_file(out_path.join("bindings.rs")) {
                Ok(_) => {
                    println!("cargo:rustc-cfg=cargo");
                    println!("cargo:rustc-check-cfg=cfg(cargo)");
                }
                Err(e) => eprintln!("Warning: Couldn't write bindings: {}", e),
            }

            // Compile the C source file
            let c_source_path = format!("{}/src/rdmaxcel.c", manifest_dir);
            if Path::new(&c_source_path).exists() {
                let mut build = cc::Build::new();
                build
                    .file(&c_source_path)
                    .include(format!("{}/src", manifest_dir))
                    .flag("-fPIC");

                // Add CUDA include paths - reuse the paths we already found for bindgen
                build.include(&cuda_include_path);

                build.compile("rdmaxcel");
            } else {
                panic!("C source file not found at {}", c_source_path);
            }

            // Compile the C++ source file for CUDA allocator compatibility
            let cpp_source_path = format!("{}/src/rdmaxcel.cpp", manifest_dir);
            if Path::new(&cpp_source_path).exists() {
                let mut libtorch_include_dirs: Vec<PathBuf> = vec![];

                // Use the same approach as torch-sys: Python discovery first, env vars as fallback
                let use_pytorch_apis =
                    build_utils::get_env_var_with_rerun("TORCH_SYS_USE_PYTORCH_APIS")
                        .unwrap_or_else(|_| "1".to_owned());

                if use_pytorch_apis == "1" {
                    // Use Python to get PyTorch include paths (same as torch-sys)
                    let python_interpreter = PathBuf::from("python");
                    let output = std::process::Command::new(&python_interpreter)
                        .arg("-c")
                        .arg(build_utils::PYTHON_PRINT_PYTORCH_DETAILS)
                        .output()
                        .unwrap_or_else(|_| panic!("error running {python_interpreter:?}"));

                    for line in String::from_utf8_lossy(&output.stdout).lines() {
                        if let Some(path) = line.strip_prefix("LIBTORCH_INCLUDE: ") {
                            libtorch_include_dirs.push(PathBuf::from(path));
                        }
                    }
                } else {
                    // Use environment variables (fallback approach)
                    libtorch_include_dirs.extend(
                        build_utils::get_env_var_with_rerun("LIBTORCH_INCLUDE")
                            .unwrap_or_default()
                            .split(':')
                            .filter(|s| !s.is_empty())
                            .map(PathBuf::from),
                    );
                }

                let mut cpp_build = cc::Build::new();
                cpp_build
                    .file(&cpp_source_path)
                    .include(format!("{}/src", manifest_dir))
                    .flag("-fPIC")
                    .cpp(true)
                    .flag("-std=gnu++20")
                    .define("PYTORCH_C10_DRIVER_API_SUPPORTED", "1");

                // Add CUDA include paths
                cpp_build.include(&cuda_include_path);

                // Add PyTorch/C10 include paths
                for include_dir in &libtorch_include_dirs {
                    cpp_build.include(include_dir);
                }

                // Add Python include path if available
                if let Some(include_dir) = &python_config.include_dir {
                    cpp_build.include(include_dir);
                }

                cpp_build.compile("rdmaxcel_cpp");
            } else {
                panic!("C++ source file not found at {}", cpp_source_path);
            }
        }
        Err(_) => {
            println!("Note: OUT_DIR not set, skipping bindings file generation");
        }
    }
}
