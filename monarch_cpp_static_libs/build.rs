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

/// Detect a working ninja binary. Returns Some(cmd) if found, None otherwise.
///
/// Checks both the exit code and that the binary is actually ninja (not a shim
/// that delegates to make). cmake -GNinja requires a real ninja binary.
fn detect_ninja() -> Option<&'static str> {
    for cmd in &["ninja-build", "ninja"] {
        if let Ok(output) = Command::new(cmd).arg("--version").output() {
            if output.status.success() {
                let version = String::from_utf8_lossy(&output.stdout);
                println!("cargo:warning=Found {} version: {}", cmd, version.trim());
                return Some(cmd);
            }
        }
    }
    println!("cargo:warning=ninja not found, will use make (cmake Makefile generator)");
    None
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
    println!("cargo:warning=Architecture: {}", std::env::consts::ARCH);

    // Detect cmake command
    let cmake = if Command::new("cmake3").arg("--version").status().is_ok() {
        "cmake3"
    } else {
        "cmake"
    };

    // Detect ninja
    let ninja_cmd = detect_ninja();

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

    if ninja_cmd.is_some() {
        cmake_args.push("-GNinja");
    }

    cmake_args.push("..");

    // Run cmake and capture output for diagnostics
    let cmake_output = Command::new(cmake)
        .current_dir(&build_dir)
        .args(&cmake_args)
        .output()
        .expect("Failed to run cmake for rdma-core");

    if !cmake_output.status.success() {
        let stderr = String::from_utf8_lossy(&cmake_output.stderr);
        let stdout = String::from_utf8_lossy(&cmake_output.stdout);
        panic!(
            "Failed to configure rdma-core with cmake.\nstdout: {}\nstderr: {}",
            stdout, stderr
        );
    }

    // Log cmake generator info for diagnostics
    let cmake_stdout = String::from_utf8_lossy(&cmake_output.stdout);
    for line in cmake_stdout.lines() {
        if line.contains("STATIC")
            || line.contains("generator")
            || line.contains("Ninja")
            || line.contains("provider")
            || line.contains("mlx5")
            || line.contains("efa")
        {
            println!("cargo:warning=cmake: {}", line);
        }
    }

    // Verify build.ninja or Makefile was generated
    if ninja_cmd.is_some() {
        let build_ninja = build_dir.join("build.ninja");
        if !build_ninja.exists() {
            panic!(
                "cmake was invoked with -GNinja but build.ninja was not generated in {}. \
                 This usually means cmake could not find the ninja binary despite it being \
                 on PATH. Ensure ninja-build is properly installed.",
                build_dir.display()
            );
        }
    }

    let expected_outputs = [
        "lib/statics/libibverbs.a",
        "lib/statics/libmlx5.a",
        "lib/statics/libefa.a",
        "util/librdma_util.a",
    ];

    let num_jobs = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4);

    if let Some(ninja) = ninja_cmd {
        // Ninja supports building by output file path.
        for target in &expected_outputs {
            let output = Command::new(ninja)
                .current_dir(&build_dir)
                .arg(target)
                .output()
                .expect("Failed to run ninja for rdma-core");

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                panic!(
                    "Failed to build rdma-core target: {}\nstderr: {}",
                    target, stderr
                );
            }
        }
    } else {
        // CMake's Makefile generator does not support building by output
        // file path; only named cmake targets work with make. Build all
        // targets and verify the expected outputs afterward.
        let status = Command::new("make")
            .current_dir(&build_dir)
            .args(["-j", &num_jobs.to_string()])
            .status()
            .expect("Failed to run make for rdma-core");

        if !status.success() {
            panic!("Failed to build rdma-core");
        }
    }

    for output in &expected_outputs {
        let path = build_dir.join(output);
        if !path.exists() {
            // List what IS in lib/statics/ for diagnostics
            let statics_dir = build_dir.join("lib/statics");
            let contents = if statics_dir.exists() {
                std::fs::read_dir(&statics_dir)
                    .map(|entries| {
                        entries
                            .filter_map(|e| e.ok())
                            .map(|e| e.file_name().to_string_lossy().to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_else(|_| "error reading directory".to_string())
            } else {
                "directory does not exist".to_string()
            };
            panic!(
                "rdma-core build completed but expected output not found: {}\n\
                 lib/statics/ contains: [{}]\n\
                 Ensure libnl3-devel is installed (needed by rdma-core cmake to \
                 enable mlx5/efa providers).",
                output, contents
            );
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
    let libefa_path = rdma_static_dir.join("libefa.a");
    let librdma_util_path = rdma_util_dir.join("librdma_util.a");

    println!("cargo:rustc-link-arg={}", libmlx5_path.display());
    println!("cargo:rustc-link-arg={}", libibverbs_path.display());
    println!("cargo:rustc-link-arg={}", libefa_path.display());
    println!("cargo:rustc-link-arg={}", librdma_util_path.display());

    // Export metadata for dependent crates
    // Use cargo:: (double colon) format for proper DEP_<LINKS>_<KEY> env vars
    println!(
        "cargo::metadata=RDMA_INCLUDE_DIR={}",
        rdma_build_dir.join("include").display()
    );

    // Export library paths as a semicolon-separated list
    let lib_paths = format!(
        "{};{};{};{}",
        libmlx5_path.display(),
        libibverbs_path.display(),
        libefa_path.display(),
        librdma_util_path.display()
    );
    println!("cargo::metadata=RDMA_STATIC_LIBRARIES={}", lib_paths);

    // Re-run if build scripts change
    println!("cargo:rerun-if-changed=build.rs");
}
