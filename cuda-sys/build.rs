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
    // Discover CUDA configuration including include and lib directories
    let cuda_config = match build_utils::discover_cuda_config() {
        Ok(config) => config,
        Err(_) => {
            build_utils::print_cuda_error_help();
            std::process::exit(1);
        }
    };

    // Start building the bindgen configuration
    let mut builder = bindgen::Builder::default()
        // The input header we would like to generate bindings for
        .header("src/wrapper.h")
        .clang_arg("-x")
        .clang_arg("c++")
        .clang_arg("-std=gnu++20")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        // Allow the specified functions and types
        .allowlist_function("cu.*")
        .allowlist_function("CU.*")
        .allowlist_type("cu.*")
        .allowlist_type("CU.*")
        // Use newtype enum style
        .default_enum_style(bindgen::EnumVariation::NewType {
            is_bitfield: false,
            is_global: false,
        });

    // Add CUDA include paths from the discovered configuration
    for include_dir in &cuda_config.include_dirs {
        builder = builder.clang_arg(format!("-I{}", include_dir.display()));
    }

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
        println!("cargo::rustc-link-search=native={}", lib_dir);
        // Set cargo metadata to inform dependent binaries about how to set their
        // RPATH (see controller/build.rs for an example).
        println!("cargo::metadata=LIB_PATH={}", lib_dir);
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

    // Generate bindings - fail fast if this doesn't work
    let bindings = builder.generate().expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file
    match env::var("OUT_DIR") {
        Ok(out_dir) => {
            let out_path = PathBuf::from(out_dir);
            bindings
                .write_to_file(out_path.join("bindings.rs"))
                .expect("Couldn't write bindings");

            println!("cargo::rustc-cfg=cargo");
            println!("cargo::rustc-check-cfg=cfg(cargo)");
        }
        Err(_) => {
            println!("Note: OUT_DIR not set, skipping bindings file generation");
        }
    }
}
