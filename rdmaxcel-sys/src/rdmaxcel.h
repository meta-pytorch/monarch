/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#ifndef RDMAXCEL_H
#define RDMAXCEL_H

#include <cuda_runtime.h>
#include <infiniband/mlx5dv.h>
#include <infiniband/verbs.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  CQE_POLL_ERROR = -1,
  CQE_POLL_FALSE = 0,
  CQE_POLL_TRUE = 1
} cqe_poll_result_t;

// C-compatible structure for CUDA segment information
typedef struct {
  size_t phys_address; // Physical memory address of the segment
  size_t phys_size; // Physical size of the segment in bytes
  int32_t device; // CUDA device ID
  int is_expandable; // Boolean: 1 if expandable, 0 if not (using int for C
                     // compatibility)
  uint32_t lkey; // Local key for registered MR (0 if not registered)
  uint32_t rkey; // Remote key for registered MR (0 if not registered)
  size_t mr_size; // Size of the registered MR (0 if not registered)
  uintptr_t mr_addr; // Registered MR address (0 if not registered)
} rdma_segment_info_t;

// Structure for WQE parameters
typedef struct {
  uintptr_t laddr;
  uint32_t lkey;
  size_t length;
  uint64_t wr_id;
  bool signaled;
  uint32_t op_type; // MLX5_OPCODE_*
  uintptr_t raddr;
  uint32_t rkey;
  uint32_t qp_num;
  uint8_t* buf;
  uint32_t* dbrec;
  uint32_t wqe_cnt;
} wqe_params_t;

// Structure for CQE poll parameters
typedef struct {
  uint8_t* cqe_buf; // CQE buffer address (mlx5dv_cq->buf)
  uint32_t cqe_size; // Size of each CQE (mlx5dv_cq->cqe_size)
  uint32_t consumer_index; // Current consumer index
  uint32_t cqe_cnt; // Total number of CQEs (mlx5dv_cq->cqe_cnt)
  uint32_t* dbrec; // Doorbell record (mlx5dv_cq->dbrec)
} cqe_poll_params_t;

struct ibv_qp* create_qp(
    struct ibv_context* context,
    struct ibv_pd* pd,
    int cq_entries,
    int max_send_wr,
    int max_recv_wr,
    int max_send_sge,
    int max_recv_sge);

struct mlx5dv_qp* create_mlx5dv_qp(struct ibv_qp* qp);

struct mlx5dv_cq* create_mlx5dv_cq(struct ibv_qp* qp);
struct mlx5dv_cq* create_mlx5dv_send_cq(struct ibv_qp* qp);
struct mlx5dv_cq* create_mlx5dv_recv_cq(struct ibv_qp* qp);

cudaError_t register_cuda_memory(
    struct mlx5dv_qp* dv_qp,
    struct mlx5dv_cq* dv_recv_cq,
    struct mlx5dv_cq* dv_send_cq);

// Function that can be called from both host and device code
__host__ __device__ void db_ring(void* dst, void* src);

__global__ void cu_db_ring(void* dst, void* src);

// Host function to launch the cu_db_ring kernel
void launch_db_ring(void* dst, void* src);

cqe_poll_result_t launch_cqe_poll(void* mlx5dv_cq, int32_t cqe_idx);
cqe_poll_result_t launch_send_cqe_poll(void* mlx5dv_cq, int32_t cqe_idx);
cqe_poll_result_t launch_recv_cqe_poll(void* mlx5dv_cq, int32_t cqe_idx);

__global__ void cu_cqe_poll(int32_t* result, cqe_poll_params_t params);

__host__ __device__ void cqe_poll(int32_t* result, cqe_poll_params_t params);

// Function that can be called from both host and device code for posting WQEs
__host__ __device__ void send_wqe(wqe_params_t params);
__host__ __device__ void recv_wqe(wqe_params_t params);

// CUDA kernel that calls send_wqe on the device
__global__ void cu_send_wqe(wqe_params_t params);
__global__ void cu_recv_wqe(wqe_params_t params);

// Host function to launch the cu_send_wqe kernel
void launch_send_wqe(wqe_params_t params);
void launch_recv_wqe(wqe_params_t params);

// RDMA Error Codes
typedef enum {
  RDMAXCEL_SUCCESS = 0, // Success
  RDMAXCEL_INVALID_PARAMS = -1, // Invalid parameters provided
  RDMAXCEL_MR_REGISTRATION_FAILED = -2, // Memory region registration failed
  RDMAXCEL_DMABUF_HANDLE_FAILED = -3, // Failed to get dmabuf handle
  RDMAXCEL_MR_REG_FAILED = -4, // MR registration failed in register_segments
  RDMAXCEL_MEMORY_BINDING_FAILED = -5, // Memory binding failed
  RDMAXCEL_QP_EX_FAILED = -6, // Failed to get QP extended
  RDMAXCEL_MLX5DV_QP_EX_FAILED = -7, // Failed to get MLX5DV QP extended
  RDMAXCEL_MKEY_CREATE_FAILED = -8, // Failed to create mkey
  RDMAXCEL_WR_COMPLETE_FAILED = -9, // Work request completion failed
  RDMAXCEL_WC_STATUS_FAILED = -10, // Work completion status failed
  RDMAXCEL_MKEY_REG_LIMIT = -11 // Memory key registration limit exceeded
} rdmaxcel_error_code_t;

// Function to get error message string
const char* rdmaxcel_error_string(int error_code);

// Active segment tracking functions (implemented in C++)
int rdma_get_active_segment_count();
int rdma_get_all_segment_info(rdma_segment_info_t* info_array, int max_count);
bool pt_cuda_allocator_compatibility();
int register_segments(struct ibv_pd* pd, struct ibv_qp* qp);

#ifdef __cplusplus
}
#endif

#endif // RDMAXCEL_H
