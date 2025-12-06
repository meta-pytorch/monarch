/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[cfg(target_os = "macos")]
fn main() {}

#[cfg(not(target_os = "macos"))]
fn main() {
    // Check if we are building for ROCm (HIP) - check ROCm first
    let is_rocm = build_utils::find_rocm_home().is_some();

    // Validate compute installation and set cfg flags
    if is_rocm {
        match build_utils::validate_rocm_installation() {
            Ok(_) => println!("cargo:warning=Using ROCm/HIP for monarch_rdma"),
            Err(_) => {
                build_utils::print_rocm_error_help();
                std::process::exit(1);
            }
        }

        // Set ROCm version cfg flags
        let rocm_version = build_utils::find_rocm_home()
            .and_then(|home| build_utils::get_rocm_version(&home))
            .unwrap_or((6, 0));

        if rocm_version.0 >= 7 {
            println!("cargo:rustc-cfg=rocm_7_plus");
        } else {
            println!("cargo:rustc-cfg=rocm_6_x");
        }
    } else {
        match build_utils::validate_cuda_installation() {
            Ok(_) => println!("cargo:warning=Using CUDA for monarch_rdma"),
            Err(_) => {
                build_utils::print_cuda_error_help();
                std::process::exit(1);
            }
        }
    }

    // Emit cfg check declarations
    println!("cargo:rustc-check-cfg=cfg(rocm_6_x)");
    println!("cargo:rustc-check-cfg=cfg(rocm_7_plus)");

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

    if let Some(lib_dir) = &python_config.lib_dir {
        println!("cargo:rustc-link-search=native={}", lib_dir);
        // Set cargo metadata to inform dependent binaries about how to set their
        // RPATH (see controller/build.rs for an example).
        println!("cargo:metadata=LIB_PATH={}", lib_dir);
    }

    // Get compute library directory and emit link directives
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

    // Link compute libraries
    if is_rocm {
        println!("cargo:rustc-link-lib=amdhip64");
        println!("cargo:rustc-link-lib=hsa-runtime64");
    } else {
        println!("cargo:rustc-link-lib=cuda");
        println!("cargo:rustc-link-lib=cudart");
    }

    // Link against the ibverbs and mlx5 libraries (used by rdmaxcel-sys)
    println!("cargo:rustc-link-lib=ibverbs");
    println!("cargo:rustc-link-lib=mlx5");

    // Link PyTorch libraries needed for C10 symbols used by rdmaxcel-sys
    let use_pytorch_apis = build_utils::get_env_var_with_rerun("TORCH_SYS_USE_PYTORCH_APIS")
        .unwrap_or_else(|_| "1".to_owned());
    if use_pytorch_apis == "1" {
        // Get PyTorch library directory using build_utils
        let python_interpreter = std::path::PathBuf::from("python");
        if let Ok(output) = std::process::Command::new(&python_interpreter)
            .arg("-c")
            .arg(build_utils::PYTHON_PRINT_PYTORCH_DETAILS)
            .output()
        {
            if output.status.success() {
                for line in String::from_utf8_lossy(&output.stdout).lines() {
                    if let Some(path) = line.strip_prefix("LIBTORCH_LIB: ") {
                        // Add library search path
                        println!("cargo:rustc-link-search=native={}", path);
                        // Set rpath so runtime linker can find the libraries
                        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", path);
                    }
                }
            }
        }

        // Link core PyTorch libraries needed for C10 symbols
        println!("cargo:rustc-link-lib=torch_cpu");
        println!("cargo:rustc-link-lib=torch");
        println!("cargo:rustc-link-lib=c10");
        if is_rocm {
            println!("cargo:rustc-link-lib=c10_hip");
        } else {
            println!("cargo:rustc-link-lib=c10_cuda");
        }
    } else {
        // Fallback to torch-sys links metadata if available
        if let Ok(torch_lib_path) = std::env::var("DEP_TORCH_LIB_PATH") {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", torch_lib_path);
        }
    }

    // Set rpath for NCCL libraries if available
    if let Ok(nccl_lib_path) = std::env::var("DEP_NCCL_LIB_PATH") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", nccl_lib_path);
    }

    // Disable new dtags, as conda envs generally use `RPATH` over `RUNPATH`
    println!("cargo:rustc-link-arg=-Wl,--disable-new-dtags");

    // Link the static libraries from rdmaxcel-sys
    // Try the Cargo dependency mechanism first, then fall back to fixed paths
    if let Ok(rdmaxcel_out_dir) = std::env::var("DEP_RDMAXCEL_SYS_OUT_DIR") {
        println!("cargo:rustc-link-search=native={}", rdmaxcel_out_dir);
        println!("cargo:rustc-link-lib=static=rdmaxcel");
        println!("cargo:rustc-link-lib=static=rdmaxcel_cpp");
        println!("cargo:rustc-link-lib=static=rdmaxcel_cuda");
    } else {
        eprintln!("Warning: DEP_RDMAXCEL_SYS_OUT_DIR not found. Using fallback paths.");

        // Use relative paths to the known locations
        let cuda_build_dir = "../rdmaxcel-sys/target/cuda_build";
        println!("cargo:rustc-link-search=native={}", cuda_build_dir);
        println!("cargo:rustc-link-lib=static=rdmaxcel_cuda");

        // Find the most recent rdmaxcel-sys build directory for C/C++ libraries
        let monarch_target_dir = "../target/debug/build";
        if let Ok(entries) = std::fs::read_dir(monarch_target_dir) {
            let mut rdmaxcel_dirs: Vec<_> = entries
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with("rdmaxcel-sys-")
                })
                .collect();

            // Sort by modification time and use the most recent
            rdmaxcel_dirs
                .sort_by_key(|entry| entry.metadata().ok().and_then(|m| m.modified().ok()));

            if let Some(most_recent) = rdmaxcel_dirs.last() {
                let out_dir = most_recent.path().join("out");
                if out_dir.exists() {
                    println!("cargo:rustc-link-search=native={}", out_dir.display());
                    println!("cargo:rustc-link-lib=static=rdmaxcel");
                    println!("cargo:rustc-link-lib=static=rdmaxcel_cpp");
                }
            } else {
                eprintln!("Warning: No rdmaxcel-sys build directories found");
            }
        }
    }

    // Set build configuration flags
    println!("cargo:rustc-cfg=cargo");
    println!("cargo:rustc-check-cfg=cfg(cargo)");
}
