/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Static NCCL and rdma-core build script
//!
//! This build script:
//! 1. Copies rdma-core and NCCL from the feasibility directories
//! 2. Builds rdma-core with static libraries (libibverbs.a, libmlx5.a)
//! 3. Builds NCCL with static linking to rdma-core (libnccl_static.a)
//! 4. Emits link directives for downstream crates

use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

// Repository configuration
// Source: https://github.com/NVIDIA/nccl - tag v2.28.9-1
const NCCL_SRC: &str = "feasibility/nccl";
// Source: https://github.com/linux-rdma/rdma-core - tag v60.0
const RDMA_CORE_SRC: &str = "feasibility/rdma-core";

#[cfg(target_os = "macos")]
fn main() {}

#[cfg(not(target_os = "macos"))]
fn main() {
    let manifest_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"));
    let monarch_root = manifest_dir
        .parent()
        .expect("Failed to get monarch root directory");

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR not set"));
    let vendor_dir = out_dir.join("vendor");
    std::fs::create_dir_all(&vendor_dir).expect("Failed to create vendor directory");

    let rdma_core_src = monarch_root.join(RDMA_CORE_SRC);
    let nccl_src = monarch_root.join(NCCL_SRC);

    let rdma_core_dir = vendor_dir.join("rdma-core");
    let nccl_dir = vendor_dir.join("nccl");

    // Copy source directories
    copy_dir(&rdma_core_src, &rdma_core_dir);
    copy_dir(&nccl_src, &nccl_dir);

    // Build rdma-core first (NCCL depends on it)
    let rdma_build_dir = build_rdma_core(&rdma_core_dir);

    // Build NCCL with static rdma-core
    let nccl_build_dir = build_nccl(&nccl_dir, &rdma_build_dir);

    // Emit link directives
    emit_link_directives(&nccl_build_dir, &rdma_build_dir);
}

fn copy_dir(src_dir: &Path, target_dir: &Path) {
    if target_dir.exists() {
        println!(
            "cargo:warning=Directory already exists at {}",
            target_dir.display()
        );
        return;
    }

    println!(
        "cargo:warning=Copying {} to {}",
        src_dir.display(),
        target_dir.display()
    );

    let status = Command::new("cp")
        .args(["-r", src_dir.to_str().unwrap(), target_dir.to_str().unwrap()])
        .status()
        .expect("Failed to execute cp");

    if !status.success() {
        panic!(
            "Failed to copy from {} to {}",
            src_dir.display(),
            target_dir.display()
        );
    }
}

fn build_rdma_core(rdma_core_dir: &Path) -> PathBuf {
    let build_dir = rdma_core_dir.join("build");

    // Check if already built
    if build_dir.join("lib/statics/libibverbs.a").exists() {
        println!("cargo:warning=rdma-core already built");
        return build_dir;
    }

    std::fs::create_dir_all(&build_dir).expect("Failed to create rdma-core build directory");

    println!("cargo:warning=Building rdma-core...");

    // Detect cmake command
    let cmake = if Command::new("cmake3").arg("--version").status().is_ok() {
        "cmake3"
    } else {
        "cmake"
    };

    // Detect ninja
    let use_ninja = Command::new("ninja-build")
        .arg("--version")
        .status()
        .is_ok()
        || Command::new("ninja").arg("--version").status().is_ok();

    let ninja_cmd = if Command::new("ninja-build")
        .arg("--version")
        .status()
        .is_ok()
    {
        "ninja-build"
    } else {
        "ninja"
    };

    // CMake configuration
    // IMPORTANT: -DCMAKE_POSITION_INDEPENDENT_CODE=ON is required for static libs
    // that will be linked into a shared object (.so)
    let mut cmake_args = vec![
        "-DIN_PLACE=1",
        "-DENABLE_STATIC=1",
        "-DENABLE_RESOLVE_NEIGH=0",
        "-DNO_PYVERBS=1",
        "-DNO_MAN_PAGES=1",
        "-DCMAKE_POSITION_INDEPENDENT_CODE=ON",
        "-DCMAKE_C_FLAGS=-fPIC",
        "-DCMAKE_CXX_FLAGS=-fPIC",
    ];

    if use_ninja {
        cmake_args.push("-GNinja");
    }

    cmake_args.push("..");

    let status = Command::new(cmake)
        .current_dir(&build_dir)
        .args(&cmake_args)
        .status()
        .expect("Failed to run cmake for rdma-core");

    if !status.success() {
        panic!("Failed to configure rdma-core with cmake");
    }

    // Build only the targets we need: libibverbs.a, libmlx5.a, and librdma_util.a
    // We don't need librdmacm which has build issues with long paths
    let targets = [
        "lib/statics/libibverbs.a",
        "lib/statics/libmlx5.a",
        "util/librdma_util.a",
    ];

    for target in &targets {
        let status = if use_ninja {
            Command::new(ninja_cmd)
                .current_dir(&build_dir)
                .arg(target)
                .status()
                .expect("Failed to run ninja for rdma-core")
        } else {
            let num_jobs = std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(4);
            Command::new("make")
                .current_dir(&build_dir)
                .args(["-j", &num_jobs.to_string(), target])
                .status()
                .expect("Failed to run make for rdma-core")
        };

        if !status.success() {
            panic!("Failed to build rdma-core target: {}", target);
        }
    }

    println!("cargo:warning=rdma-core build complete");
    build_dir
}

fn build_nccl(nccl_dir: &Path, rdma_build_dir: &Path) -> PathBuf {
    let build_dir = nccl_dir.join("build");

    // Check if already built
    if build_dir.join("lib/libnccl_static.a").exists() {
        println!("cargo:warning=NCCL already built");
        return build_dir;
    }

    println!("cargo:warning=Building NCCL...");

    // Find CUDA
    let cuda_home = build_utils::find_cuda_home().expect("CUDA not found");

    // Set up environment
    // IMPORTANT: -fPIC is required for static libs linked into shared objects
    let rdma_include = rdma_build_dir.join("include");
    let cxxflags = format!("-fPIC -I{}", rdma_include.display());

    let num_jobs = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4);

    // Build NCCL with RDMA_CORE=1 and MLX5DV=1
    // Pass -Xcompiler -fPIC to nvcc for host code
    let status = Command::new("make")
        .current_dir(nccl_dir)
        .env("CXXFLAGS", &cxxflags)
        .env("CFLAGS", "-fPIC")
        .args([
            &format!("-j{}", num_jobs),
            "src.build",
            &format!("CUDA_HOME={}", cuda_home),
            "NVCC_GENCODE=-gencode=arch=compute_90,code=sm_90",
            "RDMA_CORE=1",
            "MLX5DV=1",
        ])
        .status()
        .expect("Failed to run make for NCCL");

    if !status.success() {
        panic!("Failed to build NCCL");
    }

    println!("cargo:warning=NCCL build complete");
    build_dir
}

fn emit_link_directives(nccl_build_dir: &Path, rdma_build_dir: &Path) {
    let nccl_lib_dir = nccl_build_dir.join("lib");
    let rdma_static_dir = rdma_build_dir.join("lib/statics");
    let rdma_util_dir = rdma_build_dir.join("util");

    // Emit search paths
    println!(
        "cargo:rustc-link-search=native={}",
        nccl_lib_dir.display()
    );
    println!(
        "cargo:rustc-link-search=native={}",
        rdma_static_dir.display()
    );
    println!(
        "cargo:rustc-link-search=native={}",
        rdma_util_dir.display()
    );

    // Static libraries - order matters for dependency resolution
    // NCCL depends on rdma-core, so link NCCL first

    // Use whole-archive for NCCL static library
    println!("cargo:rustc-link-arg=-Wl,--whole-archive");
    println!("cargo:rustc-link-lib=static=nccl_static");
    println!("cargo:rustc-link-arg=-Wl,--no-whole-archive");

    // Use whole-archive for rdma-core static libraries
    println!("cargo:rustc-link-arg=-Wl,--whole-archive");
    println!("cargo:rustc-link-lib=static=mlx5");
    println!("cargo:rustc-link-lib=static=ibverbs");
    println!("cargo:rustc-link-arg=-Wl,--no-whole-archive");

    // rdma_util helper library
    println!("cargo:rustc-link-lib=static=rdma_util");

    // System libraries
    println!("cargo:rustc-link-lib=cudart_static");
    println!("cargo:rustc-link-lib=pthread");
    println!("cargo:rustc-link-lib=rt");
    println!("cargo:rustc-link-lib=dl");

    // Export metadata for dependent crates
    // Use cargo:: (double colon) format for proper DEP_<LINKS>_<KEY> env vars
    println!(
        "cargo::metadata=NCCL_INCLUDE={}",
        nccl_build_dir.join("include").display()
    );
    println!(
        "cargo::metadata=RDMA_INCLUDE={}",
        rdma_build_dir.join("include").display()
    );
    println!("cargo::metadata=NCCL_LIB_DIR={}", nccl_lib_dir.display());
    println!(
        "cargo::metadata=RDMA_LIB_DIR={}",
        rdma_static_dir.display()
    );
    println!(
        "cargo::metadata=RDMA_UTIL_DIR={}",
        rdma_util_dir.display()
    );

    // Re-run if build scripts change
    println!("cargo:rerun-if-changed=build.rs");
}
