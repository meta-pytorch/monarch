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
    // Validate CUDA installation and get CUDA home path
    let _cuda_home = match build_utils::validate_cuda_installation() {
        Ok(home) => home,
        Err(_) => {
            build_utils::print_cuda_error_help();
            std::process::exit(1);
        }
    };

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

    // Link against the ibverbs and mlx5 libraries (used by rdmaxcel-sys)
    println!("cargo:rustc-link-lib=ibverbs");
    println!("cargo:rustc-link-lib=mlx5");

    // Note: We no longer link against libtorch/c10 since rdmaxcel-sys
    // now uses a callback mechanism for segment scanning instead of
    // directly accessing PyTorch C10 APIs.

    // Disable new dtags, as conda envs generally use `RPATH` over `RUNPATH`
    println!("cargo::rustc-link-arg=-Wl,--disable-new-dtags");

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
    println!("cargo::rustc-cfg=cargo");
    println!("cargo::rustc-check-cfg=cfg(cargo)");
}
