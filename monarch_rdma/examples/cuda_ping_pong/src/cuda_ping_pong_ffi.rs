/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! FFI bindings to CUDA RDMA kernels

// RDMA requires frequent unsafe code blocks
#![allow(clippy::undocumented_unsafe_blocks)]

use std::os::raw::c_int;

/// RDMA parameters structure for CUDA RDMA operations
#[repr(C)]
#[derive(Debug)]
pub struct rdma_params_t {
    pub cu_ptr: usize,                       // CUDA pointer
    pub laddr: usize,                        // Local buffer address
    pub lsize: usize,                        // Local buffer size
    pub lkey: u32,                           // Local memory key
    pub raddr: usize,                        // Remote buffer address
    pub rsize: usize,                        // Remote buffer size
    pub rkey: u32,                           // Remote memory key
    pub qp_num: u32,                         // Queue pair number
    pub rq_buf: *mut u8,                     // Receive queue buffer
    pub rq_cnt: u32,                         // Receive queue count
    pub sq_buf: *mut u8,                     // Send queue buffer
    pub sq_cnt: u32,                         // Send queue count
    pub qp_dbrec: *mut u32,                  // Queue pair doorbell record
    pub send_cqe_buf: *mut std::ffi::c_void, // Send completion queue entry buffer
    pub send_cqe_size: u32,                  // Send completion queue entry size
    pub send_cqe_cnt: u32,                   // Send completion queue entry count
    pub send_cqe_dbrec: *mut u32,            // Send completion queue doorbell record
    pub recv_cqe_buf: *mut std::ffi::c_void, // Receive completion queue entry buffer
    pub recv_cqe_size: u32,                  // Receive completion queue entry size
    pub recv_cqe_cnt: u32,                   // Receive completion queue entry count
    pub recv_cqe_dbrec: *mut u32,            // Receive completion queue doorbell record
    pub qp_db: *mut std::ffi::c_void,        // Queue pair doorbell register
}

unsafe extern "C" {

    pub unsafe fn launchPingPong(
        params: *mut rdma_params_t,
        iterations: c_int,
        initial_length: c_int,
        device_id: c_int,
    ) -> c_int;

}

/// Safe wrapper for performing ping-pong test with RDMA parameters
pub fn ping_pong(
    params: &mut rdma_params_t,
    iterations: i32,
    initial_length: i32,
    device_id: i32,
) -> Result<(), i32> {
    let result = unsafe { launchPingPong(params, iterations, initial_length, device_id) };

    if result == 0 { Ok(()) } else { Err(result) }
}
