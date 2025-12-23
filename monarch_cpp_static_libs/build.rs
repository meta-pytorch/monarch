/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Static rdma-core build script
//!
//! This build script:
//! 1. Obtains rdma-core source (from MONARCH_RDMA_CORE_SRC or by cloning)
//! 2. Builds rdma-core with static libraries (libibverbs.a, libmlx5.a)
//! 3. Emits link directives for downstream crates

use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

// Repository configuration
const RDMA_CORE_REPO: &str = "https://github.com/linux-rdma/rdma-core";
const RDMA_CORE_TAG: &str = "224154663a9ad5b1ad5629fb76a0c40c675fb936";

#[cfg(not(target_os = "linux"))]
fn main() {}

#[cfg(target_os = "linux")]
fn main() {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR not set"));
    let vendor_dir = out_dir.join("vendor");
    std::fs::create_dir_all(&vendor_dir).expect("Failed to create vendor directory");

    let rdma_core_dir = vendor_dir.join("rdma-core");

    // Get or clone rdma-core source
    get_or_clone_rdma_core(&rdma_core_dir);

    // Build rdma-core
    let rdma_build_dir = build_rdma_core(&rdma_core_dir);

    // Emit link directives
    emit_link_directives(&rdma_build_dir);
}

/// Get or clone rdma-core source.
///
/// If MONARCH_RDMA_CORE_SRC is set, copies from that directory.
/// Otherwise, clones from GitHub at the specified tag.
fn get_or_clone_rdma_core(target_dir: &Path) {
    // Skip if already exists
    if target_dir.exists() {
        println!(
            "cargo:warning=rdma-core source already exists at {}",
            target_dir.display()
        );
        return;
    }

    // Check for MONARCH_RDMA_CORE_SRC environment variable
    println!("cargo:rerun-if-env-changed=MONARCH_RDMA_CORE_SRC");
    if let Ok(src_path) = std::env::var("MONARCH_RDMA_CORE_SRC") {
        let src_dir = PathBuf::from(src_path);
        println!(
            "cargo:warning=Using rdma-core source from MONARCH_RDMA_CORE_SRC: {}",
            src_dir.display()
        );
        copy_dir(&src_dir, target_dir);
    } else {
        println!(
            "cargo:warning=MONARCH_RDMA_CORE_SRC not set, cloning from {} (commit {})",
            RDMA_CORE_REPO, RDMA_CORE_TAG
        );
        clone_rdma_core(target_dir);
    }
}

/// Clone rdma-core from GitHub at the specified commit.
fn clone_rdma_core(target_dir: &Path) {
    // First, clone the repository without checking out
    let status = Command::new("git")
        .args([
            "clone",
            "--no-checkout",
            RDMA_CORE_REPO,
            target_dir.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to execute git clone");

    if !status.success() {
        panic!("Failed to clone rdma-core from {}", RDMA_CORE_REPO);
    }

    // Then checkout the specific commit
    let status = Command::new("git")
        .args(["checkout", RDMA_CORE_TAG])
        .current_dir(target_dir)
        .status()
        .expect("Failed to execute git checkout");

    if !status.success() {
        panic!("Failed to checkout rdma-core commit {}", RDMA_CORE_TAG);
    }

    println!(
        "cargo:warning=Successfully cloned rdma-core at commit {}",
        RDMA_CORE_TAG
    );
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
        .args([
            "-r",
            src_dir.to_str().unwrap(),
            target_dir.to_str().unwrap(),
        ])
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

fn emit_link_directives(rdma_build_dir: &Path) {
    let rdma_static_dir = rdma_build_dir.join("lib/statics");
    let rdma_util_dir = rdma_build_dir.join("util");

    // Link directly to the specific .a files we built, rather than using search paths.
    // This avoids any path ordering issues where the linker might find system libraries
    // or libraries built with different flags (e.g., ENABLE_RESOLVE_NEIGH=1).
    let libmlx5_path = rdma_static_dir.join("libmlx5.a");
    let libibverbs_path = rdma_static_dir.join("libibverbs.a");
    let librdma_util_path = rdma_util_dir.join("librdma_util.a");

    println!("cargo:rustc-link-arg={}", libmlx5_path.display());
    println!("cargo:rustc-link-arg={}", libibverbs_path.display());
    println!("cargo:rustc-link-arg={}", librdma_util_path.display());

    // Export metadata for dependent crates
    // Use cargo:: (double colon) format for proper DEP_<LINKS>_<KEY> env vars
    println!(
        "cargo::metadata=RDMA_INCLUDE_DIR={}",
        rdma_build_dir.join("include").display()
    );

    // Export library paths as a semicolon-separated list
    let lib_paths = format!(
        "{};{};{}",
        libmlx5_path.display(),
        libibverbs_path.display(),
        librdma_util_path.display()
    );
    println!("cargo::metadata=RDMA_STATIC_LIBRARIES={}", lib_paths);

    // Re-run if build scripts change
    println!("cargo:rerun-if-changed=build.rs");
}
