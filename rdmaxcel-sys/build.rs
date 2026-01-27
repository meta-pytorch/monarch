/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Build script for rdmaxcel-sys
//!
//! Supports both CUDA and ROCm backends. ROCm support uses hipify_torch
//! to convert CUDA sources at build time.

use std::env;
use std::path::Path;
use std::path::PathBuf;

#[cfg(target_os = "macos")]
fn main() {}

#[cfg(not(target_os = "macos"))]
fn main() {
    // Get rdma-core config from cpp_static_libs (same for both CUDA and ROCm)
    let cpp_static_libs_config = build_utils::CppStaticLibsConfig::from_env();
    let rdma_include = &cpp_static_libs_config.rdma_include_dir;

    // Detect platform: ROCm or CUDA
    let (is_rocm, compute_home) = detect_platform();

    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| {
        let current_dir = std::env::current_dir().expect("Failed to get current directory");
        let current_path = current_dir.to_string_lossy();
        if let Some(fbsource_pos) = current_path.find("fbsource") {
            let fbsource_path = &current_path[..fbsource_pos + "fbsource".len()];
            format!("{}/fbcode/monarch/rdmaxcel-sys", fbsource_path)
        } else {
            format!("{}/src", current_dir.to_string_lossy())
        }
    });

    let out_dir = env::var("OUT_DIR").expect("OUT_DIR not set");
    let out_path = PathBuf::from(&out_dir);
    let src_dir = PathBuf::from(&manifest_dir).join("src");

    // Determine source directory based on platform
    let source_dir = if is_rocm {
        // For ROCm: hipify sources first, then use hipified directory
        let hip_dir = out_path.join("hipified_src");
        hipify_sources(&src_dir, &hip_dir, &manifest_dir);
        hip_dir
    } else {
        // For CUDA: use original sources
        src_dir.clone()
    };

    // Setup rerun triggers
    println!("cargo:rerun-if-changed=src/rdmaxcel.h");
    println!("cargo:rerun-if-changed=src/rdmaxcel.c");
    println!("cargo:rerun-if-changed=src/rdmaxcel.cpp");
    println!("cargo:rerun-if-changed=src/rdmaxcel.cu");
    println!("cargo:rerun-if-changed=src/driver_api.h");
    println!("cargo:rerun-if-changed=src/driver_api.cpp");

    // Link against dl for dynamic loading (both platforms)
    println!("cargo:rustc-link-lib=dl");

    // Platform-specific GPU runtime linking
    if is_rocm {
        // ROCm: Link dynamically to HIP runtime
        // Note: Driver API functions (hipMemCreate, etc.) are loaded via dlopen in driver_api.cpp
        println!("cargo:rustc-link-lib=amdhip64");
        println!("cargo:rustc-link-search=native={}/lib", compute_home);
    } else {
        // CUDA: Link statically to CUDA runtime
        // Note: Driver API functions (cuMemCreate, etc.) are loaded via dlopen in driver_api.cpp
        let cuda_lib_dir = build_utils::get_cuda_lib_dir();
        println!("cargo:rustc-link-search=native={}", cuda_lib_dir);
        println!("cargo:rustc-link-lib=static=cudart_static");
        // cudart_static requires these
        println!("cargo:rustc-link-lib=rt");
        println!("cargo:rustc-link-lib=pthread");
    }

    // Get Python config
    let python_config = build_utils::python_env_dirs_with_interpreter("python3").unwrap_or(
        build_utils::PythonConfig {
            include_dir: None,
            lib_dir: None,
        },
    );

    // Compute include path
    let compute_include_path = format!("{}/include", compute_home);
    println!("cargo:rustc-env=CUDA_INCLUDE_PATH={}", compute_include_path);

    // Discover source files (dynamically handles hipified vs original names)
    let header_path = find_header(&source_dir, is_rocm);
    let c_sources = find_sources(&source_dir, "c", is_rocm);
    let cpp_sources = find_sources(&source_dir, "cpp", is_rocm);
    let gpu_source = find_gpu_source(&source_dir, is_rocm);

    // Generate bindings
    let mut builder = bindgen::Builder::default()
        .header(header_path.to_string_lossy())
        .clang_arg("-x")
        .clang_arg("c++")
        .clang_arg("-std=c++14")
        .clang_arg(format!("-I{}", compute_include_path))
        .clang_arg(format!("-I{}", rdma_include))
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        // Functions
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
        .allowlist_function("deregister_segments")
        .allowlist_function("rdmaxcel_cu.*")
        .allowlist_function("get_cuda_pci_address_from_ptr")
        .allowlist_function("rdmaxcel_print_device_info")
        .allowlist_function("rdmaxcel_error_string")
        .allowlist_function("rdmaxcel_qp_.*")
        .allowlist_function("rdmaxcel_register_segment_scanner")
        .allowlist_function("poll_cq_with_cache")
        .allowlist_function("completion_cache_.*")
        // Types
        .allowlist_type("ibv_.*")
        .allowlist_type("mlx5dv_.*")
        .allowlist_type("mlx5_wqe_.*")
        .allowlist_type("cqe_poll_.*")
        .allowlist_type("wqe_params_t")
        .allowlist_type("rdma_segment_info_t")
        .allowlist_type("rdmaxcel_scanned_segment_t")
        .allowlist_type("rdmaxcel_qp_t")
        .allowlist_type("rdmaxcel_qp")
        .allowlist_type("completion_cache_t")
        .allowlist_type("completion_cache")
        .allowlist_type("poll_context_t")
        .allowlist_type("poll_context")
        .allowlist_type("rdmaxcel_segment_scanner_fn")
        // Vars
        .allowlist_var("MLX5_.*")
        .allowlist_var("IBV_.*")
        .allowlist_var("CUDA_SUCCESS")
        .allowlist_var("CU_.*")
        // Block manually defined types
        .blocklist_type("ibv_wc")
        .blocklist_type("mlx5_wqe_ctrl_seg")
        // Enum handling
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

    if is_rocm {
        builder = builder
            .clang_arg("-D__HIP_PLATFORM_AMD__=1")
            .clang_arg("-DUSE_ROCM=1");
    }

    if let Some(include_dir) = &python_config.include_dir {
        builder = builder.clang_arg(format!("-I{}", include_dir));
    }
    if let Some(lib_dir) = &python_config.lib_dir {
        println!("cargo:rustc-link-search=native={}", lib_dir);
        println!("cargo:metadata=LIB_PATH={}", lib_dir);
    }

    // Generate bindings
    builder
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings");

    println!("cargo:out_dir={}", out_dir);
    println!("cargo:rustc-cfg=cargo");
    println!("cargo:rustc-check-cfg=cfg(cargo)");

    // Emit cfg for ROCm so Rust code can conditionally compile
    if is_rocm {
        println!("cargo:rustc-cfg=use_rocm");
    }
    println!("cargo:rustc-check-cfg=cfg(use_rocm)");

    // Compile C sources
    if !c_sources.is_empty() {
        let mut c_build = cc::Build::new();
        for c_source in &c_sources {
            c_build.file(c_source);
        }
        c_build
            .include(&source_dir)
            .include(rdma_include)
            .include(&compute_include_path)
            .flag("-fPIC");

        if is_rocm {
            c_build
                .define("__HIP_PLATFORM_AMD__", "1")
                .define("USE_ROCM", "1");
        }

        c_build.compile("rdmaxcel");
    }

    // Compile C++ sources
    if !cpp_sources.is_empty() {
        let mut cpp_build = cc::Build::new();
        for cpp_source in &cpp_sources {
            cpp_build.file(cpp_source);
        }
        cpp_build
            .include(&source_dir)
            .include(rdma_include)
            .include(&compute_include_path)
            .flag("-fPIC")
            .cpp(true)
            .flag("-std=c++14");

        if is_rocm {
            cpp_build
                .define("__HIP_PLATFORM_AMD__", "1")
                .define("USE_ROCM", "1");
        }

        if let Some(include_dir) = &python_config.include_dir {
            cpp_build.include(include_dir);
        }

        cpp_build.compile("rdmaxcel_cpp");
    }

    // Statically link libstdc++
    build_utils::link_libstdcpp_static();

    // Compile GPU source
    compile_gpu_source(
        &gpu_source,
        &compute_home,
        &compute_include_path,
        rdma_include,
        &source_dir,
        &manifest_dir,
        &out_dir,
        is_rocm,
    );
}

/// Detect whether to use ROCm or CUDA
fn detect_platform() -> (bool, String) {
    // Try ROCm first (ROCm systems may have CUDA installed too)
    if let Ok(rocm_home) = build_utils::validate_rocm_installation() {
        let version = build_utils::get_rocm_version(&rocm_home)
            .unwrap_or_else(|_| panic!("Failed to get ROCm version"));

        if version.0 < 7 {
            panic!(
                "ROCm {}.{} detected, but ROCm 7.0+ is required",
                version.0, version.1
            );
        }

        println!(
            "cargo:warning=Using ROCm {}.{} from {}",
            version.0, version.1, rocm_home
        );
        return (true, rocm_home);
    }

    // Fall back to CUDA
    if let Ok(cuda_home) = build_utils::validate_cuda_installation() {
        println!("cargo:warning=Using CUDA from {}", cuda_home);
        return (false, cuda_home);
    }

    panic!("Neither CUDA nor ROCm installation found!");
}

/// Hipify CUDA sources to HIP
fn hipify_sources(src_dir: &Path, hip_dir: &Path, manifest_dir: &str) {
    println!("cargo:warning=Hipifying CUDA sources to {:?}...", hip_dir);

    let source_files: Vec<PathBuf> = [
        "rdmaxcel.h",
        "rdmaxcel.c",
        "rdmaxcel.cpp",
        "rdmaxcel.cu",
        "driver_api.h",
        "driver_api.cpp",
    ]
    .iter()
    .map(|f| src_dir.join(f))
    .filter(|p| p.exists())
    .collect();

    let project_root = PathBuf::from(manifest_dir)
        .parent()
        .expect("Failed to find project root")
        .to_path_buf();

    // Use custom mapping file for RDMA-specific conversions
    let custom_map = project_root.join(".hipify/rdmaxcel_custom_map.json");
    let custom_map_opt = if custom_map.exists() {
        Some(custom_map.as_path())
    } else {
        None
    };

    build_utils::run_hipify_torch(&project_root, &source_files, hip_dir, custom_map_opt)
        .expect("hipify_torch failed");

    println!("cargo:warning=Hipification complete");
}

/// Find the main header file (rdmaxcel.h or rdmaxcel_hip.h)
fn find_header(dir: &Path, is_rocm: bool) -> PathBuf {
    // For ROCm, prefer hipified version with _hip suffix
    // For CUDA, use original files
    let names = if is_rocm {
        vec!["rdmaxcel_hip.h", "rdmaxcel.h"]
    } else {
        vec!["rdmaxcel.h"]
    };

    for name in names {
        let path = dir.join(name);
        if path.exists() {
            return path;
        }
    }
    panic!("Could not find rdmaxcel header in {:?}", dir);
}

/// Find all C sources in directory
fn find_sources(dir: &Path, extension: &str, is_rocm: bool) -> Vec<PathBuf> {
    std::fs::read_dir(dir)
        .expect("Failed to read source directory")
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension()? == extension {
                let file_name = path.file_stem()?.to_str()?;
                // For ROCm: use files with _hip suffix
                // For CUDA: use files without _hip suffix
                let has_hip_suffix = file_name.ends_with("_hip");
                if is_rocm == has_hip_suffix {
                    Some(path)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect()
}

/// Find GPU source file (.cu or .hip)
fn find_gpu_source(dir: &Path, is_rocm: bool) -> PathBuf {
    let extension = if is_rocm { "hip" } else { "cu" };

    std::fs::read_dir(dir)
        .expect("Failed to read source directory")
        .find_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension()? == extension {
                Some(path)
            } else {
                None
            }
        })
        .unwrap_or_else(|| panic!("Could not find .{} file in {:?}", extension, dir))
}

/// Compile GPU source file (CUDA .cu or HIP .hip)
fn compile_gpu_source(
    gpu_source: &Path,
    compute_home: &str,
    compute_include: &str,
    rdma_include: &str,
    source_dir: &Path,
    manifest_dir: &str,
    out_dir: &str,
    is_rocm: bool,
) {
    let compiler = if is_rocm {
        format!("{}/bin/hipcc", compute_home)
    } else {
        format!("{}/bin/nvcc", compute_home)
    };

    let gpu_build_dir = PathBuf::from(manifest_dir).join("target/gpu_build");
    std::fs::create_dir_all(&gpu_build_dir).expect("Failed to create GPU build directory");

    let obj_name = if is_rocm {
        "rdmaxcel_hip.o"
    } else {
        "rdmaxcel_cuda.o"
    };
    let lib_name = if is_rocm {
        "librdmaxcel_hip.a"
    } else {
        "librdmaxcel_cuda.a"
    };

    let obj_path = gpu_build_dir.join(obj_name);
    let lib_path = gpu_build_dir.join(lib_name);

    // Compile with nvcc/hipcc
    let mut cmd = std::process::Command::new(&compiler);
    cmd.args(&[
        "-c",
        &gpu_source.to_string_lossy(),
        "-o",
        &obj_path.to_string_lossy(),
        "-fPIC",
        "-std=c++14",
        &format!("-I{}", compute_include),
        &format!("-I{}", source_dir.display()),
        &format!("-I{}", rdma_include),
    ]);

    if is_rocm {
        cmd.args(&["-D__HIP_PLATFORM_AMD__=1", "-DUSE_ROCM=1"]);
    } else {
        cmd.args(&[
            "--compiler-options",
            "-fPIC",
            "--expt-extended-lambda",
            "-Xcompiler",
            "-fPIC",
        ]);
    }

    let output = cmd.output().expect("Failed to run GPU compiler");
    if !output.status.success() {
        eprintln!(
            "GPU compiler stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        eprintln!(
            "GPU compiler stdout: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        panic!("Failed to compile GPU source");
    }

    println!("cargo:rerun-if-changed={}", gpu_source.display());

    // Create static library
    let ar_output = std::process::Command::new("ar")
        .args(&[
            "rcs",
            &lib_path.to_string_lossy(),
            &obj_path.to_string_lossy(),
        ])
        .output()
        .expect("Failed to run ar");

    if !ar_output.status.success() {
        eprintln!("ar stderr: {}", String::from_utf8_lossy(&ar_output.stderr));
        panic!("Failed to create static library");
    }

    // Link the library
    let link_name = if is_rocm {
        "rdmaxcel_hip"
    } else {
        "rdmaxcel_cuda"
    };
    println!("cargo:rustc-link-lib=static={}", link_name);
    println!("cargo:rustc-link-search=native={}", gpu_build_dir.display());

    // Copy to OUT_DIR for cargo
    let out_lib = PathBuf::from(out_dir).join(lib_name);
    if let Err(e) = std::fs::copy(&lib_path, &out_lib) {
        eprintln!("Warning: Failed to copy GPU library to OUT_DIR: {}", e);
    }
}
