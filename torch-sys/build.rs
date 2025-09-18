/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This build script locates the PyTorch libraries and headers based on your
//! current Python environment and makes them available to the cxx bridge for
//! linking. This version provides CPU-only PyTorch functionality.
//!
//! This script is not very general atm. Functionality that we would probably want:
//! * Support for platforms other than linux.

#![feature(exit_status_error)]

use std::path::PathBuf;
use std::process::Stdio;

use build_utils::*;
use cxx_build::CFG;
use pyo3_build_config::InterpreterConfig;

fn main() {
    let mut libtorch_include_dirs: Vec<PathBuf> = vec![];
    let mut libtorch_lib_dir: Option<PathBuf> = None;
    let mut cxx11_abi = None;
    let python_interpreter = PathBuf::from("python");

    let use_pytorch_apis = build_utils::get_env_var_with_rerun("TORCH_SYS_USE_PYTORCH_APIS")
        .unwrap_or_else(|_| "1".to_owned());
    if use_pytorch_apis == "1" {
        // We use the user's python installation of PyTorch to get the proper
        // headers/libraries for libtorch
        let output = std::process::Command::new(&python_interpreter)
            .arg("-c")
            .arg(build_utils::PYTHON_PRINT_PYTORCH_DETAILS)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap_or_else(|_| panic!("error spawning {python_interpreter:?}"))
            .wait_with_output()
            .unwrap_or_else(|_| panic!("error waiting for {python_interpreter:?}"));
        output
            .status
            .exit_ok()
            .unwrap_or_else(|_| panic!("error running {python_interpreter:?}"));

        for line in String::from_utf8_lossy(&output.stdout).lines() {
            match line.strip_prefix("LIBTORCH_CXX11: ") {
                Some("False") => cxx11_abi = Some("0".to_owned()),
                Some("True") => cxx11_abi = Some("1".to_owned()),
                _ => {}
            };
            if let Some(path) = line.strip_prefix("LIBTORCH_INCLUDE: ") {
                libtorch_include_dirs.push(PathBuf::from(path))
            }
            if let Some(path) = line.strip_prefix("LIBTORCH_LIB: ") {
                libtorch_lib_dir = Some(PathBuf::from(path))
            }
        }
    } else {
        cxx11_abi = Some(build_utils::get_env_var_with_rerun("_GLIBCXX_USE_CXX11_ABI").unwrap());
        libtorch_include_dirs.extend(
            build_utils::get_env_var_with_rerun("LIBTORCH_INCLUDE")
                .unwrap()
                .split(':')
                .map(|s| s.into()),
        );
        libtorch_lib_dir = Some(
            build_utils::get_env_var_with_rerun("LIBTORCH_LIB")
                .unwrap()
                .into(),
        );
    }

    let mut python_include: Option<PathBuf> = None;
    let mut python_include_dir: Option<PathBuf> = None;
    // Include Python headers, and headers / libs from the active env.
    let output = std::process::Command::new(&python_interpreter)
        .arg("-c")
        .arg(build_utils::PYTHON_PRINT_INCLUDE_PATH)
        .output()
        .unwrap_or_else(|_| panic!("error running {python_interpreter:?}"));
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        if let Some(path) = line.strip_prefix("PYTHON_INCLUDE: ") {
            python_include = Some(PathBuf::from(path));
        }
        if let Some(path) = line.strip_prefix("PYTHON_INCLUDE_DIR: ") {
            python_include_dir = Some(PathBuf::from(path));
        }
        if let Some(path) = line.strip_prefix("PYTHON_LIB_DIR: ") {
            println!("cargo::rustc-link-search=native={}", path);
        }
    }

    // Use PyO3's Python discovery to find the correct Python library paths
    // This is more robust than hardcoding platform-specific paths
    let mut python_lib_dir: Option<String> = None;
    match InterpreterConfig::from_interpreter(&python_interpreter) {
        Ok(python_config) => {
            // Add Python library directory to search path
            if let Some(lib_dir) = &python_config.lib_dir {
                println!("cargo::rustc-link-search=native={}", lib_dir);
                python_lib_dir = Some(lib_dir.clone());
            }

            // On some platforms, we may need to explicitly link against Python
            // PyO3 handles the complexity of determining when this is needed
            if let Some(lib_name) = python_config.lib_name {
                println!("cargo::rustc-link-lib={}", lib_name);
            }
        }
        Err(e) => {
            println!(
                "cargo::warning=Failed to get Python interpreter config: {}",
                e
            );
            println!("cargo::warning=This may cause linking issues with Python libraries");
        }
    }

    let bindings = bindgen::Builder::default()
        .header("src/torch.hpp")
        .clang_args(
            libtorch_include_dirs
                .iter()
                .map(|path| format!("-I{}", path.display())),
        )
        .clang_arg(format!(
            "-I{}",
            python_include_dir.clone().unwrap().display()
        ))
        .clang_arg("-std=gnu++20")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .allowlist_type("c10::MemoryFormat")
        .allowlist_type("c10::ScalarType")
        .allowlist_type("c10::Layout")
        .default_enum_style(bindgen::EnumVariation::NewType {
            is_bitfield: false,
            is_global: false,
        })
        .enable_cxx_namespaces()
        .generate()
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");

    println!("cargo::rerun-if-changed=src/torch.hpp");

    // Prefix includes with `monarch` to maintain consistency with fbcode
    // folder structre
    CFG.include_prefix = "monarch/torch-sys";
    let mut builder = cxx_build::bridge("src/bridge.rs");

    builder
        .file("src/bridge.cpp")
        .std("c++20")
        .includes(&libtorch_include_dirs)
        .include(python_include.unwrap())
        .include(python_include_dir.unwrap())
        // Suppress warnings, otherwise we get massive spew from libtorch
        .flag_if_supported("-w")
        .flag(&format!(
            "-Wl,-rpath={}",
            libtorch_lib_dir.clone().unwrap().display()
        ))
        .flag(&format!("-D_GLIBCXX_USE_CXX11_ABI={}", cxx11_abi.unwrap()));

    builder.compile("torch-sys");

    // Link against the various torch libs
    println!(
        "cargo::rustc-link-search=native={}",
        libtorch_lib_dir.clone().unwrap().display()
    );

    // Core PyTorch libraries (CPU-only)
    println!("cargo::rustc-link-lib=torch_cpu");
    println!("cargo::rustc-link-lib=torch");
    println!("cargo::rustc-link-lib=torch_python");
    println!("cargo::rustc-link-lib=c10");

    // Set runtime paths
    println!(
        "cargo::rustc-link-arg=-Wl,-rpath,{}",
        libtorch_lib_dir.clone().unwrap().display()
    );

    // Add Python library directory to rpath for runtime linking
    if let Some(python_lib_dir) = &python_lib_dir {
        println!("cargo::rustc-link-arg=-Wl,-rpath,{}", python_lib_dir);
    }

    // Set cargo metadata to inform dependent binaries about how to set their
    // RPATH (see monarch_tensor_worker/build.rs for an example).
    println!(
        "cargo::metadata=LIB_PATH={}",
        libtorch_lib_dir.clone().unwrap().display()
    );

    println!("cargo::rerun-if-changed=src/bridge.rs");
    println!("cargo::rerun-if-changed=src/bridge.cpp");
    println!("cargo::rerun-if-changed=src/bridge.h");
}
