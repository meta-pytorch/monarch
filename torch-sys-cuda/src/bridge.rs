/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[allow(dead_code)]
#[cxx::bridge(namespace = "monarch")]
pub(crate) mod ffi {
    unsafe extern "C++" {
        include!("monarch/torch-sys-cuda/src/bridge.h");

        // nccl helpers
        #[namespace = ""]
        type ncclConfig_t = nccl_sys::ncclConfig_t;
        fn make_nccl_config() -> ncclConfig_t;
    }
}

