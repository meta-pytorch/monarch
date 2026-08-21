/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

fn main() {
    // RDMA bindings need the rdma-core static link setup. The slim OSS build
    // enables it via the `rdma` feature; internal buck builds get it through
    // `tensor_engine_gpu` (which folds in `rdma`).
    if std::env::var("CARGO_FEATURE_RDMA").is_ok()
        || std::env::var("CARGO_FEATURE_TENSOR_ENGINE_GPU").is_ok()
    {
        // Set up static linking for rdma-core
        // This emits link directives for libmlx5.a, libibverbs.a, librdma_util.a
        let _config = build_utils::setup_cpp_static_libs();
    }

    if std::env::var("CARGO_FEATURE_EMBEDDED_CPP").is_ok() {
        build_embedded_cpp_modules();
    }
}

fn build_embedded_cpp_modules() {
    let python_include = std::env::var("MONARCH_PYTHON_INCLUDE")
        .expect("MONARCH_PYTHON_INCLUDE must be set for the embedded C++ modules");
    let libtorch_include = std::env::var("LIBTORCH_INCLUDE")
        .expect("LIBTORCH_INCLUDE must be set for the embedded C++ modules");
    let libtorch_lib = std::env::var("LIBTORCH_LIB")
        .expect("LIBTORCH_LIB must be set for the embedded C++ modules");

    println!("cargo:rerun-if-env-changed=MONARCH_PYTHON_INCLUDE");
    println!("cargo:rerun-if-env-changed=LIBTORCH_INCLUDE");
    println!("cargo:rerun-if-env-changed=LIBTORCH_LIB");
    println!("cargo:rerun-if-env-changed=MONARCH_BUILD_CUDA");
    println!("cargo:rerun-if-changed=../python/monarch/common/init.cpp");
    println!("cargo:rerun-if-changed=../python/monarch/common/mock_cuda.cpp");
    println!("cargo:rerun-if-changed=../python/monarch/common/mock_cuda.h");
    println!("cargo:rerun-if-changed=../python/monarch/gradient/_gradient_generator.cpp");

    let mut build = cc::Build::new();
    build
        .cpp(true)
        .std("c++20")
        .debug(true)
        .opt_level(3)
        .include("..")
        .include(python_include)
        .file("../python/monarch/common/init.cpp")
        .file("../python/monarch/gradient/_gradient_generator.cpp");

    for include in std::env::split_paths(&libtorch_include) {
        build.include(include);
    }
    if std::env::var("MONARCH_BUILD_CUDA").as_deref() == Ok("1") {
        build
            .define("MONARCH_BUILD_CUDA", "1")
            .file("../python/monarch/common/mock_cuda.cpp");
    }
    build.compile("monarch_embedded_cpp");

    println!("cargo:rustc-link-search=native={libtorch_lib}");
    for library in ["c10", "torch", "torch_cpu", "torch_python"] {
        println!("cargo:rustc-link-lib=dylib={library}");
    }
    if cfg!(target_os = "linux") {
        println!("cargo:rustc-link-lib=dylib=dl");
    }
}
