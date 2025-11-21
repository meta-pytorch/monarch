/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#ifndef RDMAXCEL_H
#define RDMAXCEL_H

#include <cuda.h>
#include <cuda_runtime.h>
#include <infiniband/mlx5dv.h>
#include <infiniband/verbs.h>
#include "driver_api.h"

// Handle atomics for both C and C++
#ifdef __cplusplus
#include <atomic>
#define _Atomic(T) std::atomic<T>
extern "C" {
#else
#include <stdatomic.h>
#endif

typedef enum {
  CQE_POLL_ERROR = -1,
  CQE_POLL_FALSE = 0,
  CQE_POLL_TRUE = 1
} cqe_poll_result_t;

// RDMA queue pair type selection
typedef enum {
  RDMA_QP_TYPE_STANDARD = 1, // Standard ibverbs queue pair
  RDMA_QP_TYPE_MLX5DV = 2 // mlx5dv extended queue pair
} rdma_qp_type_t;

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
    int max_recv_sge,
    rdma_qp_type_t qp_type);

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
  RDMAXCEL_MKEY_REG_LIMIT = -11, // Memory key registration limit exceeded
  RDMAXCEL_CUDA_GET_ATTRIBUTE_FAILED =
      -12, // Failed to get CUDA device attribute
  RDMAXCEL_CUDA_GET_DEVICE_FAILED = -13, // Failed to get CUDA device handle
  RDMAXCEL_BUFFER_TOO_SMALL = -14, // Output buffer too small
  RDMAXCEL_QUERY_DEVICE_FAILED = -15, // Failed to query device attributes
  RDMAXCEL_CQ_POLL_FAILED = -16, // CQ polling failed
  RDMAXCEL_COMPLETION_FAILED = -17 // Completion status not successful
} rdmaxcel_error_code_t;

// Error/Debugging functions
void rdmaxcel_print_device_info(struct ibv_context* context);
const char* rdmaxcel_error_string(int error_code);

// Active segment tracking functions (implemented in C++)
int rdma_get_active_segment_count();
int rdma_get_all_segment_info(rdma_segment_info_t* info_array, int max_count);
bool pt_cuda_allocator_compatibility();
int deregister_segments();

// CUDA utility functions
int get_cuda_pci_address_from_ptr(
    CUdeviceptr cuda_ptr,
    char* pci_addr_out,
    size_t pci_addr_size);

cudaError_t register_host_mem(void** buf, size_t size);

// Forward declarations
typedef struct completion_cache completion_cache_t;

// RDMA Queue Pair wrapper with atomic counters and completion caches
typedef struct rdmaxcel_qp {
  struct ibv_qp* ibv_qp; // Underlying ibverbs QP
  struct ibv_cq* send_cq; // Send completion queue
  struct ibv_cq* recv_cq; // Receive completion queue

  // Atomic counters
  _Atomic(uint64_t) send_wqe_idx;
  _Atomic(uint64_t) send_db_idx;
  _Atomic(uint64_t) send_cq_idx;
  _Atomic(uint64_t) recv_wqe_idx;
  _Atomic(uint64_t) recv_db_idx;
  _Atomic(uint64_t) recv_cq_idx;
  _Atomic(uint64_t) rts_timestamp;

  // Completion caches
  completion_cache_t* send_completion_cache;
  completion_cache_t* recv_completion_cache;
} rdmaxcel_qp_t;

// Create and initialize an rdmaxcel QP (wraps create_qp + initializes
// counters/caches)
rdmaxcel_qp_t* rdmaxcel_qp_create(
    struct ibv_context* context,
    struct ibv_pd* pd,
    int cq_entries,
    int max_send_wr,
    int max_recv_wr,
    int max_send_sge,
    int max_recv_sge,
    rdma_qp_type_t qp_type);

// Destroy rdmaxcel QP and clean up resources
void rdmaxcel_qp_destroy(rdmaxcel_qp_t* qp);

// Get underlying ibv_qp pointer (for compatibility with existing ibverbs calls)
struct ibv_qp* rdmaxcel_qp_get_ibv_qp(rdmaxcel_qp_t* qp);

// Atomic fetch_add operations
uint64_t rdmaxcel_qp_fetch_add_send_wqe_idx(rdmaxcel_qp_t* qp);
uint64_t rdmaxcel_qp_fetch_add_send_db_idx(rdmaxcel_qp_t* qp);
uint64_t rdmaxcel_qp_fetch_add_send_cq_idx(rdmaxcel_qp_t* qp);
uint64_t rdmaxcel_qp_fetch_add_recv_wqe_idx(rdmaxcel_qp_t* qp);
uint64_t rdmaxcel_qp_fetch_add_recv_db_idx(rdmaxcel_qp_t* qp);
uint64_t rdmaxcel_qp_fetch_add_recv_cq_idx(rdmaxcel_qp_t* qp);

// Atomic load operations (minimal API surface)
// Send side: needed for doorbell ring iteration [db_idx, wqe_idx)
uint64_t rdmaxcel_qp_load_send_wqe_idx(rdmaxcel_qp_t* qp);
uint64_t rdmaxcel_qp_load_send_db_idx(rdmaxcel_qp_t* qp);
// Receive side: needed for receive operations
uint64_t rdmaxcel_qp_load_recv_wqe_idx(rdmaxcel_qp_t* qp);
// Completion queue indices: needed for polling without modifying
uint64_t rdmaxcel_qp_load_send_cq_idx(rdmaxcel_qp_t* qp);
uint64_t rdmaxcel_qp_load_recv_cq_idx(rdmaxcel_qp_t* qp);
// Connection state validation
uint64_t rdmaxcel_qp_load_rts_timestamp(rdmaxcel_qp_t* qp);

// Atomic store operations
void rdmaxcel_qp_store_send_db_idx(rdmaxcel_qp_t* qp, uint64_t value);
void rdmaxcel_qp_store_rts_timestamp(rdmaxcel_qp_t* qp, uint64_t value);

// Get completion caches
completion_cache_t* rdmaxcel_qp_get_send_cache(rdmaxcel_qp_t* qp);
completion_cache_t* rdmaxcel_qp_get_recv_cache(rdmaxcel_qp_t* qp);

// Segment registration (uses rdmaxcel_qp_t, so must come after type definition)
int register_segments(struct ibv_pd* pd, rdmaxcel_qp_t* qp);

// Completion Cache Structures and Functions
#define MAX_CACHED_COMPLETIONS 128

// Linked list node for cached completions
typedef struct completion_node {
  struct ibv_wc wc;
  int next; // Index of next node, or -1 for end of list
} completion_node_t;

// Cache for "unmatched" completions using embedded linked list
typedef struct completion_cache {
  completion_node_t nodes[MAX_CACHED_COMPLETIONS];
  int head; // Index of first used node, or -1 if empty
  int tail; // Index of last used node
  int free_head; // Index of first free node, or -1 if full
  size_t count;
  pthread_mutex_t lock;
} completion_cache_t;

// Context for polling with cache
typedef struct poll_context {
  uint64_t expected_wr_id; // What wr_id am I looking for?
  uint32_t expected_qp_num; // What QP am I expecting?
  completion_cache_t* cache; // Shared completion cache
  struct ibv_cq* cq; // The CQ to poll
} poll_context_t;

// Initialize completion cache
void completion_cache_init(completion_cache_t* cache);

// Destroy completion cache
void completion_cache_destroy(completion_cache_t* cache);

// Add completion to cache
int completion_cache_add(completion_cache_t* cache, struct ibv_wc* wc);

// Find and remove completion from cache
int completion_cache_find(
    completion_cache_t* cache,
    uint64_t wr_id,
    uint32_t qp_num,
    struct ibv_wc* out_wc);

// Poll with cache support
// Returns: 1 = found, 0 = not found, -1 = error
int poll_cq_with_cache(poll_context_t* ctx, struct ibv_wc* out_wc);

#ifdef __cplusplus
}
#endif

#endif // RDMAXCEL_H
