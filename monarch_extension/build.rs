/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

fn main() {
    // Static rdma-core linking is needed whenever monarch_rdma_extension
    // is pulled in, which happens under `tensor_engine` (including the
    // CPU-only stub build) and `tensor_engine_gpu`.
    if std::env::var("CARGO_FEATURE_TENSOR_ENGINE").is_ok() {
        // Emits link directives for libmlx5.a, libibverbs.a, librdma_util.a.
        let _config = build_utils::setup_cpp_static_libs();
    }
}
