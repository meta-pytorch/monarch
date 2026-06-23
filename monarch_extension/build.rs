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
}
