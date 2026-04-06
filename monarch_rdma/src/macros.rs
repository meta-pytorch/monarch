/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#[macro_export]
macro_rules! cu_check {
    ($result:expr) => {
        let result = $result;
        if result != rdmaxcel_sys::CUDA_SUCCESS {
            let mut error_string: *const std::os::raw::c_char = std::ptr::null();
            let err_result = rdmaxcel_sys::rdmaxcel_cuGetErrorString(result, &mut error_string);
            if err_result != rdmaxcel_sys::CUDA_SUCCESS || error_string.is_null() {
                panic!(
                    "cuda failure {}:{} {:?} (cuGetErrorString failed with {:?})",
                    file!(),
                    line!(),
                    result,
                    err_result,
                );
            }
            panic!(
                "cuda failure {}:{} {:?} '{}'",
                file!(),
                line!(),
                result,
                std::ffi::CStr::from_ptr(error_string).to_string_lossy()
            );
        }
    };
}
