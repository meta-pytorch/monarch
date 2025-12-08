/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

fn main() {
    // Only set torch-related rpaths if tensor_engine feature is enabled
    #[cfg(feature = "tensor_engine")]
    {
        if let Ok(path) = std::env::var("DEP_NCCL_LIB_PATH") {
            println!("cargo::rustc-link-arg=-Wl,-rpath,{path}");
        }
    }
}
