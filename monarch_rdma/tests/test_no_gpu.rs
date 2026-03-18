/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Tests that run on non-GPU hosts to verify graceful degradation
//! when the CUDA driver library cannot be loaded.

use monarch_rdma::local_memory::driver_init_error;
use monarch_rdma::local_memory::is_device_ptr;

#[test]
fn is_device_ptr_returns_false_without_crashing() {
    // On non-GPU hosts, libcuda.so.1 is not available.
    // is_device_ptr must return false (not SIGABRT or panic).
    assert!(!is_device_ptr(0));
}

#[test]
fn is_device_ptr_stack_pointer_returns_false() {
    let x: u64 = 42;
    assert!(!is_device_ptr(&x as *const u64 as usize));
}

#[test]
fn driver_init_error_returns_message_when_cuda_unavailable() {
    // Force driver initialization by probing a pointer.
    let _ = is_device_ptr(0);

    // On non-GPU hosts, driver_init_error should return a message
    // explaining why libcuda.so.1 couldn't be loaded.
    // On GPU hosts, it returns None (CUDA loaded fine).
    if let Some(msg) = driver_init_error() {
        assert!(
            !msg.is_empty(),
            "driver init error message should not be empty"
        );
        assert!(
            msg.contains("libcuda") || msg.contains("RdmaXcel"),
            "error message should mention libcuda or RdmaXcel, got: {msg}"
        );
    }
    // If None, CUDA is available — that's fine too, test passes.
}
