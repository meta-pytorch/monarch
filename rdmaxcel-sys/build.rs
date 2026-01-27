/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Build script for rdmaxcel-sys
//!
//! Supports both CUDA and ROCm backends. ROCm support requires hipification
//! of CUDA sources and version-specific patches.

use std::env;
use std::path::PathBuf;
use std::process::Command;

#[cfg(target_os = "macos")]
fn main() {}

#[cfg(not(target_os = "macos"))]
fn main() {
    // Declare cfg flags
    println!("cargo::rustc-check-cfg=cfg(cargo)");
    println!("cargo::rustc-check-cfg=cfg(rocm_6_x)");
    println!("cargo::rustc-check-cfg=cfg(rocm_7_plus)");

    // Get rdma-core config from cpp_static_libs (includes are used, links emitted by monarch_extension)
    let cpp_static_libs_config = build_utils::CppStaticLibsConfig::from_env();
    let rdma_include = &cpp_static_libs_config.rdma_include_dir;

    let platform = detect_platform();
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let src_dir = manifest_dir.join("src");

    // Setup linking
    println!("cargo:rustc-link-lib=dl");
    println!("cargo:rustc-link-search=native={}", platform.lib_dir());
    platform.emit_link_libs();

    // Setup rerun triggers
    for f in &[
        "rdmaxcel.h",
        "rdmaxcel.c",
        "rdmaxcel.cpp",
        "rdmaxcel.cu",
        "driver_api.h",
        "driver_api.cpp",
    ] {
        println!("cargo:rerun-if-changed=src/{}", f);
    }

    // Build
    if let Ok(out_dir) = env::var("OUT_DIR") {
        let out_path = PathBuf::from(&out_dir);
        let sources = platform.prepare_sources(&src_dir, &out_path);

        let python_config = build_utils::python_env_dirs_with_interpreter("python3").unwrap_or(
            build_utils::PythonConfig {
                include_dir: None,
                lib_dir: None,
            },
        );

        generate_bindings(&sources, &platform, rdma_include, &python_config, &out_path);
        compile_c(&sources, &platform, rdma_include);
        compile_cpp(&sources, &platform, rdma_include, &python_config);
        compile_gpu(&sources, &platform, rdma_include, &manifest_dir, &out_path);
    }

    println!(
        "cargo:rustc-env=CUDA_INCLUDE_PATH={}",
        platform.include_dir()
    );
    println!("cargo:rustc-cfg=cargo");
}

// =============================================================================
// Platform abstraction
// =============================================================================

enum Platform {
    Cuda { home: String },
    Rocm { home: String, version: (u32, u32) },
}

impl Platform {
    fn include_dir(&self) -> String {
        match self {
            Platform::Cuda { home } | Platform::Rocm { home, .. } => format!("{}/include", home),
        }
    }

    fn lib_dir(&self) -> String {
        match self {
            Platform::Cuda { .. } => build_utils::get_cuda_lib_dir(),
            Platform::Rocm { .. } => {
                build_utils::get_rocm_lib_dir().expect("Failed to get ROCm lib dir")
            }
        }
    }

    fn compiler(&self) -> String {
        match self {
            Platform::Cuda { home } => format!("{}/bin/nvcc", home),
            Platform::Rocm { home, .. } => format!("{}/bin/hipcc", home),
        }
    }

    fn is_rocm(&self) -> bool {
        matches!(self, Platform::Rocm { .. })
    }

    fn rocm_version(&self) -> (u32, u32) {
        match self {
            Platform::Rocm { version, .. } => *version,
            Platform::Cuda { .. } => (0, 0),
        }
    }

    fn emit_link_libs(&self) {
        match self {
            Platform::Cuda { .. } => {
                // CUDA: static runtime, dlopen driver API
                println!("cargo:rustc-link-lib=static=cudart_static");
                println!("cargo:rustc-link-lib=rt");
                println!("cargo:rustc-link-lib=pthread");
            }
            Platform::Rocm { .. } => {
                // ROCm linking strategy:
                // - Driver API (hipMemCreate, etc.): loaded via dlopen in driver_api_hip.cpp
                // - HIP runtime (hipLaunchKernel): needed for compiled GPU kernels
                // - RDMA libraries: used directly by rdmaxcel.c/cpp, not via dlopen
                // - HSA runtime: ROCm 6.x uses hsa_amd_portable_export_dmabuf directly
                //   (ROCm 7+ has native hipMemGetHandleForAddressRange and won't need this)
                println!("cargo:rustc-link-lib=amdhip64");
                println!("cargo:rustc-link-lib=ibverbs");
                println!("cargo:rustc-link-lib=mlx5");
                println!("cargo:rustc-link-lib=hsa-runtime64");
            }
        }
    }

    fn prepare_sources(&self, src_dir: &PathBuf, out_path: &PathBuf) -> Sources {
        match self {
            Platform::Cuda { .. } => Sources {
                dir: src_dir.clone(),
                header: src_dir.join("rdmaxcel.h"),
                c_source: src_dir.join("rdmaxcel.c"),
                cpp_source: src_dir.join("rdmaxcel.cpp"),
                gpu_source: src_dir.join("rdmaxcel.cu"),
                driver_api: src_dir.join("driver_api.cpp"),
            },
            Platform::Rocm { version, .. } => {
                let hip_dir = out_path.join("hipified_src");
                hipify_sources(src_dir, &hip_dir, *version);
                Sources {
                    dir: hip_dir.clone(),
                    header: hip_dir.join("rdmaxcel_hip.h"),
                    c_source: hip_dir.join("rdmaxcel_hip.c"),
                    cpp_source: hip_dir.join("rdmaxcel_hip.cpp"),
                    gpu_source: hip_dir.join("rdmaxcel.hip"),
                    driver_api: hip_dir.join("driver_api_hip.cpp"),
                }
            }
        }
    }

    fn add_defines(&self, build: &mut cc::Build) {
        if let Platform::Rocm { version, .. } = self {
            build.define("__HIP_PLATFORM_AMD__", "1");
            build.define("USE_ROCM", "1");
            if version.0 >= 7 {
                build.define("ROCM_7_PLUS", "1");
            } else {
                build.define("ROCM_6_X", "1");
            }
        }
    }

    fn clang_defines(&self) -> Vec<String> {
        match self {
            Platform::Cuda { .. } => vec![],
            Platform::Rocm { version, .. } => {
                let mut defs = vec!["-D__HIP_PLATFORM_AMD__=1".into(), "-DUSE_ROCM=1".into()];
                if version.0 >= 7 {
                    defs.push("-DROCM_7_PLUS=1".into());
                } else {
                    defs.push("-DROCM_6_X=1".into());
                }
                defs
            }
        }
    }

    fn compiler_args(&self) -> Vec<String> {
        match self {
            Platform::Cuda { .. } => vec![
                "-Xcompiler".into(),
                "-fPIC".into(),
                "-std=c++14".into(),
                "--expt-extended-lambda".into(),
            ],
            Platform::Rocm { version, .. } => {
                let mut args = vec![
                    "-fPIC".into(),
                    "-std=c++14".into(),
                    "-D__HIP_PLATFORM_AMD__=1".into(),
                    "-DUSE_ROCM=1".into(),
                ];
                if version.0 >= 7 {
                    args.push("-DROCM_7_PLUS=1".into());
                } else {
                    args.push("-DROCM_6_X=1".into());
                }
                args
            }
        }
    }
}

struct Sources {
    dir: PathBuf,
    header: PathBuf,
    c_source: PathBuf,
    cpp_source: PathBuf,
    gpu_source: PathBuf,
    driver_api: PathBuf,
}

// =============================================================================
// Platform detection
// =============================================================================

fn detect_platform() -> Platform {
    // Try ROCm first (ROCm systems may also have CUDA installed)
    if let Ok(home) = build_utils::validate_rocm_installation() {
        let version = build_utils::get_rocm_version(&home).unwrap_or((6, 0));
        println!(
            "cargo:warning=Using HIP/ROCm {}.{} from {}",
            version.0, version.1, home
        );

        if version.0 >= 7 {
            println!("cargo:rustc-cfg=rocm_7_plus");
        } else {
            println!("cargo:rustc-cfg=rocm_6_x");
        }

        return Platform::Rocm { home, version };
    }

    // Fall back to CUDA
    if let Ok(home) = build_utils::validate_cuda_installation() {
        println!("cargo:warning=Using CUDA from {}", home);
        return Platform::Cuda { home };
    }

    eprintln!("Error: Neither CUDA nor ROCm installation found!");
    build_utils::print_cuda_error_help();
    std::process::exit(1);
}

// =============================================================================
// Hipification (ROCm only)
// =============================================================================

fn hipify_sources(src_dir: &PathBuf, hip_dir: &PathBuf, version: (u32, u32)) {
    println!(
        "cargo:warning=Hipifying sources to {}...",
        hip_dir.display()
    );

    let files: Vec<PathBuf> = [
        "lib.rs",
        "rdmaxcel.h",
        "rdmaxcel.c",
        "rdmaxcel.cpp",
        "rdmaxcel.cu",
        "test_rdmaxcel.c",
        "driver_api.h",
        "driver_api.cpp",
    ]
    .iter()
    .map(|f| src_dir.join(f))
    .filter(|p| p.exists())
    .collect();

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let project_root = manifest_dir.parent().expect("Failed to find project root");

    build_utils::run_hipify_torch(project_root, &files, hip_dir).expect("hipify_torch failed");

    // Apply version-specific patches
    if version.0 >= 7 {
        build_utils::rocm::patch_hipified_files_rocm7(hip_dir).expect("ROCm 7+ patching failed");
    } else {
        build_utils::rocm::patch_hipified_files_rocm6(hip_dir).expect("ROCm 6.x patching failed");
    }

    build_utils::rocm::validate_hipified_files(hip_dir).expect("Hipified file validation failed");
}

// =============================================================================
// Compilation
// =============================================================================

fn generate_bindings(
    sources: &Sources,
    platform: &Platform,
    rdma_include: &str,
    python_config: &build_utils::PythonConfig,
    out_path: &PathBuf,
) {
    let mut builder = bindgen::Builder::default()
        .header(sources.header.to_string_lossy())
        .clang_arg("-x")
        .clang_arg("c++")
        .clang_arg("-std=c++14")
        .clang_arg(format!("-I{}", platform.include_dir()))
        .clang_arg(format!("-I{}", rdma_include))
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        // Functions
        .allowlist_function("ibv_.*")
        .allowlist_function("mlx5dv_.*")
        .allowlist_function("create_qp")
        .allowlist_function("create_mlx5dv_.*")
        .allowlist_function("register_cuda_memory")
        .allowlist_function("register_hip_memory")
        .allowlist_function("db_ring")
        .allowlist_function("cqe_poll")
        .allowlist_function("send_wqe")
        .allowlist_function("recv_wqe")
        .allowlist_function("launch_.*")
        .allowlist_function("rdma_get_.*")
        .allowlist_function("pt_.*_allocator_compatibility")
        .allowlist_function("register_segments")
        .allowlist_function("deregister_segments")
        .allowlist_function("register_dmabuf_buffer")
        .allowlist_function("get_.*_pci_address_from_ptr")
        .allowlist_function("rdmaxcel_.*")
        .allowlist_function("completion_cache_.*")
        .allowlist_function("poll_cq_with_cache")
        // Types
        .allowlist_type("rdmaxcel_.*")
        .allowlist_type("completion_.*")
        .allowlist_type("poll_context.*")
        .allowlist_type("rdma_qp_type_t")
        .allowlist_type("CU.*")
        .allowlist_type("hip.*")
        .allowlist_type("hsa_status_t")
        .allowlist_type("ibv_.*")
        .allowlist_type("mlx5.*")
        .allowlist_type("cqe_poll_.*")
        .allowlist_type("wqe_params_t")
        .allowlist_type("rdma_segment_info_t")
        // Vars
        .allowlist_var("CUDA_SUCCESS")
        .allowlist_var("CU_.*")
        .allowlist_var("hipSuccess")
        .allowlist_var("HIP_.*")
        .allowlist_var("HSA_STATUS_SUCCESS")
        .allowlist_var("MLX5_.*")
        .allowlist_var("IBV_.*")
        .allowlist_var("RDMA_QP_TYPE_.*")
        // Config
        .blocklist_type("ibv_wc")
        .blocklist_type("mlx5_wqe_ctrl_seg")
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

    for def in platform.clang_defines() {
        builder = builder.clang_arg(def);
    }

    if let Some(ref dir) = python_config.include_dir {
        builder = builder.clang_arg(format!("-I{}", dir));
    }

    builder
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings");
}

fn compile_c(sources: &Sources, platform: &Platform, rdma_include: &str) {
    if !sources.c_source.exists() {
        return;
    }

    let mut build = cc::Build::new();
    build
        .file(&sources.c_source)
        .include(&sources.dir)
        .include(platform.include_dir())
        .include(rdma_include)
        .flag("-fPIC");

    platform.add_defines(&mut build);
    build.compile("rdmaxcel");
}

fn compile_cpp(
    sources: &Sources,
    platform: &Platform,
    rdma_include: &str,
    python_config: &build_utils::PythonConfig,
) {
    if !sources.cpp_source.exists() {
        return;
    }

    let mut build = cc::Build::new();
    build
        .file(&sources.cpp_source)
        .include(&sources.dir)
        .include(platform.include_dir())
        .include(rdma_include)
        .flag("-fPIC")
        .cpp(true)
        .flag("-std=c++14");

    if sources.driver_api.exists() {
        build.file(&sources.driver_api);
    }

    platform.add_defines(&mut build);

    if platform.is_rocm() {
        build.flag("-Wno-deprecated-declarations");
    }

    if let Some(ref dir) = python_config.include_dir {
        build.include(dir);
    }

    build.compile("rdmaxcel_cpp");
    build_utils::link_libstdcpp_static();
}

fn compile_gpu(
    sources: &Sources,
    platform: &Platform,
    rdma_include: &str,
    manifest_dir: &PathBuf,
    out_path: &PathBuf,
) {
    if !sources.gpu_source.exists() {
        return;
    }

    let build_dir = format!("{}/target/cuda_build", manifest_dir.display());
    std::fs::create_dir_all(&build_dir).expect("Failed to create build directory");

    let obj_path = format!("{}/rdmaxcel_cuda.o", build_dir);
    let lib_path = format!("{}/librdmaxcel_cuda.a", build_dir);

    // Build base args without -fPIC (platform.compiler_args() handles it correctly)
    let mut args = vec![
        "-c".to_string(),
        sources.gpu_source.to_string_lossy().to_string(),
        "-o".to_string(),
        obj_path.clone(),
        format!("-I{}", platform.include_dir()),
        format!("-I{}", sources.dir.display()),
        format!("-I{}", rdma_include),
        "-I/usr/include".to_string(),
        "-I/usr/include/infiniband".to_string(),
    ];
    // Add platform-specific args (includes properly formatted -fPIC for each platform)
    args.extend(platform.compiler_args());

    let output = Command::new(platform.compiler())
        .args(&args)
        .output()
        .expect("Failed to run GPU compiler");

    if !output.status.success() {
        panic!(
            "GPU compilation failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let ar_output = Command::new("ar")
        .args(["rcs", &lib_path, &obj_path])
        .output();

    if let Ok(out) = ar_output {
        if out.status.success() {
            println!("cargo:rustc-link-lib=static=rdmaxcel_cuda");
            println!("cargo:rustc-link-search=native={}", build_dir);
            let _ = std::fs::copy(&lib_path, out_path.join("librdmaxcel_cuda.a"));
        }
    }
}
