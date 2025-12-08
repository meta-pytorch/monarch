/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

fn main() {
    // Only set up static linking if tensor_engine feature is enabled
    if std::env::var("CARGO_FEATURE_TENSOR_ENGINE").is_ok() {
        // Set up static linking for NCCL and rdma-core
        // This emits link directives for libnccl_static.a, libmlx5.a, libibverbs.a, librdma_util.a
        let _config = build_utils::setup_nccl_static();
    }
}
