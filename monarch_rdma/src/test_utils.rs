/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::sync::Once;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

/// Cached result of CUDA availability check
static CUDA_AVAILABLE: AtomicBool = AtomicBool::new(false);
static INIT: Once = Once::new();

/// Safely checks if CUDA is available on the system.
///
/// This function attempts to initialize CUDA and determine if it's available.
/// The result is cached after the first call, so subsequent calls are very fast.
///
/// # Returns
///
/// `true` if CUDA is available and can be initialized, `false` otherwise.
///
/// # Examples
///
/// ```
/// use monarch_rdma::is_cuda_available;
///
/// if is_cuda_available() {
///     println!("CUDA is available, can use GPU features");
/// } else {
///     println!("CUDA is not available, falling back to CPU-only mode");
/// }
/// ```
pub fn is_cuda_available() -> bool {
    INIT.call_once(|| {
        let available = check_cuda_available();
        CUDA_AVAILABLE.store(available, Ordering::SeqCst);
    });
    CUDA_AVAILABLE.load(Ordering::SeqCst)
}

/// Internal function that performs the actual CUDA availability check
fn check_cuda_available() -> bool {
    unsafe {
        // Try to initialize CUDA
        let result = rdmaxcel_sys::rdmaxcel_cuInit(0);

        if result != rdmaxcel_sys::CUDA_SUCCESS {
            return false;
        }

        // Check if there are any CUDA devices
        let mut device_count: i32 = 0;
        let count_result = rdmaxcel_sys::rdmaxcel_cuDeviceGetCount(&mut device_count);

        if count_result != rdmaxcel_sys::CUDA_SUCCESS || device_count <= 0 {
            return false;
        }

        // Try to get the first device to verify it's actually accessible
        let mut device: rdmaxcel_sys::CUdevice = std::mem::zeroed();
        let device_result = rdmaxcel_sys::rdmaxcel_cuDeviceGet(&mut device, 0);

        if device_result != rdmaxcel_sys::CUDA_SUCCESS {
            return false;
        }

        true
    }
}
