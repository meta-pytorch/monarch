/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <assert.h>
#include <cuda_runtime.h>
#include <stdint.h>
#include "rdmaxcel.h"
#include "rdmaxcel_core_impl.h"

//------------------------------------------------------------------------------
// Byte Swapping Utilities
//------------------------------------------------------------------------------




//------------------------------------------------------------------------------
// Doorbell Operations
//------------------------------------------------------------------------------


/**
 * @brief CUDA kernel wrapper for db_ring function
 *
 * This kernel launches a single thread to execute the db_ring function on the
 * GPU. It includes memory fences to ensure proper ordering of memory
 * operations.
 *
 * @param dst Pointer to the destination (doorbell register)
 * @param src Pointer to the source data
 */
__global__ void cu_db_ring(void* dst, void* src) {
  uint32_t i = blockIdx.x * blockDim.x + threadIdx.x;
  if (i == 0) {
    db_ring(dst, src);
  }
  __syncthreads();
  __threadfence_system();
}

/**
 * @brief Host function to launch the cu_db_ring kernel
 *
 * This function launches the cu_db_ring kernel with a single thread.
 *
 * @param dst Pointer to the destination (doorbell register)
 * @param src Pointer to the source data
 */
void launch_db_ring(void* dst, void* src) {
  cu_db_ring<<<1, 1>>>(dst, src);
}

//------------------------------------------------------------------------------
// Work Queue Element (WQE) Operations
//------------------------------------------------------------------------------


/**
 * @brief CUDA kernel wrapper for recv_wqe function
 *
 * This kernel launches a single thread to execute the recv_wqe function on the
 * GPU.
 *
 * @param params Structure containing all parameters needed for the receive WQE
 */
__global__ void cu_recv_wqe(wqe_params_t params) {
  if (threadIdx.x == 0 && blockIdx.x == 0) {
    recv_wqe(params);
  }
}

/**
 * @brief Host function to launch the cu_recv_wqe kernel
 *
 * This function launches the cu_recv_wqe kernel with a single thread and
 * synchronizes the device to ensure completion.
 *
 * @param params Structure containing all parameters needed for the receive WQE
 */
void launch_recv_wqe(wqe_params_t params) {
  // Launch kernel
  cu_recv_wqe<<<1, 1>>>(params);

  // Wait for kernel to complete
  cudaDeviceSynchronize();
}


/**
 * @brief CUDA kernel wrapper for send_wqe function
 *
 * This kernel launches a single thread to execute the send_wqe function on the
 * GPU.
 *
 * @param params Structure containing all parameters needed for the send WQE
 */
__global__ void cu_send_wqe(wqe_params_t params) {
  if (threadIdx.x == 0 && blockIdx.x == 0) {
    send_wqe(params);
  }
}

/**
 * @brief Host function to launch the cu_send_wqe kernel
 *
 * This function launches the cu_send_wqe kernel with a single thread and
 * synchronizes the device to ensure completion.
 *
 * @param params Structure containing all parameters needed for the send WQE
 */
void launch_send_wqe(wqe_params_t params) {
  // Launch kernel
  cu_send_wqe<<<1, 1>>>(params);

  // Wait for kernel to complete
  cudaDeviceSynchronize();
}

//------------------------------------------------------------------------------
// Completion Queue Element (CQE) Operations
//------------------------------------------------------------------------------


/**
 * @brief CUDA kernel wrapper for cqe_poll function
 *
 * This kernel launches a single thread to execute the cqe_poll function on the
 * GPU. It includes memory fences to ensure proper ordering of memory
 * operations.
 *
 * @param result Pointer to store the result of the poll operation
 * @param params Structure containing all parameters needed for polling the CQ
 */
__global__ void cu_cqe_poll(int32_t* result, cqe_poll_params_t params) {
  uint32_t i = blockIdx.x * blockDim.x + threadIdx.x;
  if (i == 0) {
    cqe_poll(result, params);
  }
  __syncthreads();
  __threadfence_system();
}

/**
 * @brief Host function to launch the cu_cqe_poll kernel
 *
 * This function allocates memory for the result, launches the cu_cqe_poll
 * kernel, and returns the result of the poll operation.
 *
 * @param mlx5dv_cq_void Pointer to the mlx5dv_cq structure
 * @param consumer_index Current consumer index
 * @return CQE_POLL_TRUE if a valid completion was found, CQE_POLL_FALSE
 * otherwise, or CQE_POLL_ERROR if an error occurred
 */
cqe_poll_result_t launch_cqe_poll(void* mlx5dv_cq_void, int consumer_index) {
  // Cast to proper types on CPU side
  struct mlx5dv_cq* cq = (struct mlx5dv_cq*)mlx5dv_cq_void;

  // Allocate memory for result
  int32_t* byte_cnt = nullptr;
  cudaError_t err = cudaMallocManaged(&byte_cnt, sizeof(int32_t));
  if (err != cudaSuccess) {
    return CQE_POLL_ERROR;
  }
  *byte_cnt = -1; // Initialize to false

  // Create the parameters struct
  cqe_poll_params_t params = {
      .cqe_buf = (uint8_t*)cq->buf,
      .cqe_size = cq->cqe_size,
      .consumer_index = (uint32_t)consumer_index,
      .cqe_cnt = cq->cqe_cnt,
      .dbrec = (uint32_t*)cq->dbrec};

  // Launch the kernel with the parameters struct
  cu_cqe_poll<<<1, 1>>>(byte_cnt, params);

  // Synchronize and get result
  cudaDeviceSynchronize();

  // Check for errors
  err = cudaGetLastError();
  if (err != cudaSuccess) {
    cudaFree(byte_cnt);
    return CQE_POLL_ERROR;
  }

  // Get the result
  cqe_poll_result_t ret_val = *byte_cnt >= 0 ? CQE_POLL_TRUE : CQE_POLL_FALSE;
  cudaFree(byte_cnt);
  return ret_val;
}

/**
 * @brief Function to poll send completion queue
 *
 * This is a wrapper around launch_cqe_poll specifically for send completions.
 *
 * @param mlx5dv_cq_void Pointer to the mlx5dv_cq structure for the send CQ
 * @param consumer_index Current consumer index
 * @return CQE_POLL_TRUE if a valid completion was found, CQE_POLL_FALSE
 * otherwise, or CQE_POLL_ERROR if an error occurred
 */
cqe_poll_result_t launch_send_cqe_poll(
    void* mlx5dv_cq_void,
    int consumer_index) {
  return launch_cqe_poll(mlx5dv_cq_void, consumer_index);
}

/**
 * @brief Function to poll receive completion queue
 *
 * This is a wrapper around launch_cqe_poll specifically for receive
 * completions.
 *
 * @param mlx5dv_cq_void Pointer to the mlx5dv_cq structure for the receive CQ
 * @param consumer_index Current consumer index
 * @return CQE_POLL_TRUE if a valid completion was found, CQE_POLL_FALSE
 * otherwise, or CQE_POLL_ERROR if an error occurred
 */
cqe_poll_result_t launch_recv_cqe_poll(
    void* mlx5dv_cq_void,
    int consumer_index) {
  return launch_cqe_poll(mlx5dv_cq_void, consumer_index);
}
