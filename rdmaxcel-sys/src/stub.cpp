/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Stub implementations of the rdmaxcel C API used when building without
// a CUDA/ROCm toolchain (`MONARCH_GPU_PLATFORM=none`).
//
// Every wrapper simply fails fast — `rdmaxcel_cu*` drivers return
// `CUDA_ERROR_NOT_INITIALIZED`, `rdmaxcel_*` helpers return an rdmaxcel
// error code, and scanner/registration functions are no-ops. Callers
// in `monarch_rdma` already handle these failure modes (see commit
// "Catch exceptions at the rdmaxcel extern \"C\" FFI boundary"), so
// the TCP backend works end-to-end while the ibverbs/CUDA paths cleanly
// report "not initialized."

#include "driver_api.h"
#include "rdmaxcel.h"

#include <stddef.h>
#include <stdint.h>

namespace {

constexpr int kRdmaxcelNotInitialized = RDMAXCEL_INVALID_PARAMS;

} // namespace

extern "C" {

// ---- driver_api.h: CUDA driver wrappers --------------------------------

CUresult rdmaxcel_cuMemGetHandleForAddressRange(
    int*,
    CUdeviceptr,
    size_t,
    CUmemRangeHandleType,
    unsigned long long) {
  return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult rdmaxcel_cuMemGetAllocationGranularity(
    size_t*,
    const CUmemAllocationProp*,
    CUmemAllocationGranularity_flags) {
  return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult rdmaxcel_cuMemCreate(
    CUmemGenericAllocationHandle*,
    size_t,
    const CUmemAllocationProp*,
    unsigned long long) {
  return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult rdmaxcel_cuMemAddressReserve(
    CUdeviceptr*,
    size_t,
    size_t,
    CUdeviceptr,
    unsigned long long) {
  return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult rdmaxcel_cuMemMap(
    CUdeviceptr,
    size_t,
    size_t,
    CUmemGenericAllocationHandle,
    unsigned long long) {
  return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult rdmaxcel_cuMemSetAccess(
    CUdeviceptr,
    size_t,
    const CUmemAccessDesc*,
    size_t) {
  return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult rdmaxcel_cuMemUnmap(CUdeviceptr, size_t) {
  return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult rdmaxcel_cuMemAddressFree(CUdeviceptr, size_t) {
  return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult rdmaxcel_cuMemRelease(CUmemGenericAllocationHandle) {
  return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult rdmaxcel_cuMemcpyHtoD_v2(CUdeviceptr, const void*, size_t) {
  return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult rdmaxcel_cuMemcpyDtoH_v2(void*, CUdeviceptr, size_t) {
  return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult rdmaxcel_cuMemsetD8_v2(CUdeviceptr, unsigned char, size_t) {
  return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult
rdmaxcel_cuPointerGetAttribute(void*, CUpointer_attribute, CUdeviceptr) {
  return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult rdmaxcel_cuInit(unsigned int) {
  return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult rdmaxcel_cuDeviceGet(CUdevice*, int) {
  return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult rdmaxcel_cuDeviceGetCount(int* count) {
  if (count != nullptr) {
    *count = 0;
  }
  return CUDA_SUCCESS;
}

CUresult rdmaxcel_cuDeviceGetAttribute(int*, CUdevice_attribute, CUdevice) {
  return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult rdmaxcel_cuCtxCreate_v2(CUcontext*, unsigned int, CUdevice) {
  return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult rdmaxcel_cuDevicePrimaryCtxRetain(CUcontext*, CUdevice) {
  return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult rdmaxcel_cuDevicePrimaryCtxRelease(CUdevice) {
  return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult rdmaxcel_cuCtxGetCurrent(CUcontext* pctx) {
  if (pctx != nullptr) {
    *pctx = nullptr;
  }
  return CUDA_SUCCESS;
}

CUresult rdmaxcel_cuCtxSetCurrent(CUcontext) {
  return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult rdmaxcel_cuCtxSynchronize(void) {
  return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult rdmaxcel_cuGetErrorString(CUresult, const char** pStr) {
  if (pStr != nullptr) {
    *pStr = "[rdmaxcel stub] CUDA not available in this build";
  }
  return CUDA_SUCCESS;
}

// ---- rdmaxcel.h: helper functions --------------------------------------

void rdmaxcel_print_device_info(struct ibv_context*) {}

const char* rdmaxcel_error_string(int) {
  return "[rdmaxcel stub] build without GPU support";
}

int rdma_get_active_segment_count(struct ibv_pd*) {
  return 0;
}

int rdma_get_all_registered_segment_info(
    struct ibv_pd*,
    rdma_segment_info_t*,
    int) {
  return 0;
}

int deregister_segments() {
  return RDMAXCEL_SUCCESS;
}

void rdmaxcel_register_segment_scanner(rdmaxcel_segment_scanner_fn) {}

int get_cuda_pci_address_from_ptr(CUdeviceptr, char*, size_t) {
  return kRdmaxcelNotInitialized;
}

cudaError_t register_host_mem(void**, size_t) {
  return cudaErrorNotInitialized;
}

// ---- QP lifecycle (never called at runtime in TCP-only builds) ---------

rdmaxcel_qp_t* rdmaxcel_qp_create(
    struct ibv_context*,
    struct ibv_pd*,
    int,
    int,
    int,
    int,
    int,
    rdma_qp_type_t) {
  return nullptr;
}

void rdmaxcel_qp_destroy(rdmaxcel_qp_t*) {}

struct ibv_qp* rdmaxcel_qp_get_ibv_qp(rdmaxcel_qp_t*) {
  return nullptr;
}

uint64_t rdmaxcel_qp_fetch_add_send_wqe_idx(rdmaxcel_qp_t*) {
  return 0;
}
uint64_t rdmaxcel_qp_fetch_add_send_db_idx(rdmaxcel_qp_t*) {
  return 0;
}
uint64_t rdmaxcel_qp_fetch_add_send_cq_idx(rdmaxcel_qp_t*) {
  return 0;
}
uint64_t rdmaxcel_qp_fetch_add_recv_wqe_idx(rdmaxcel_qp_t*) {
  return 0;
}
uint64_t rdmaxcel_qp_fetch_add_recv_db_idx(rdmaxcel_qp_t*) {
  return 0;
}
uint64_t rdmaxcel_qp_fetch_add_recv_cq_idx(rdmaxcel_qp_t*) {
  return 0;
}

uint64_t rdmaxcel_qp_load_send_wqe_idx(rdmaxcel_qp_t*) {
  return 0;
}
uint64_t rdmaxcel_qp_load_send_db_idx(rdmaxcel_qp_t*) {
  return 0;
}
uint64_t rdmaxcel_qp_load_recv_wqe_idx(rdmaxcel_qp_t*) {
  return 0;
}
uint64_t rdmaxcel_qp_load_send_cq_idx(rdmaxcel_qp_t*) {
  return 0;
}
uint64_t rdmaxcel_qp_load_recv_cq_idx(rdmaxcel_qp_t*) {
  return 0;
}
uint64_t rdmaxcel_qp_load_rts_timestamp(rdmaxcel_qp_t*) {
  return 0;
}

void rdmaxcel_qp_store_send_db_idx(rdmaxcel_qp_t*, uint64_t) {}
void rdmaxcel_qp_store_rts_timestamp(rdmaxcel_qp_t*, uint64_t) {}

completion_cache_t* rdmaxcel_qp_get_send_cache(rdmaxcel_qp_t*) {
  return nullptr;
}
completion_cache_t* rdmaxcel_qp_get_recv_cache(rdmaxcel_qp_t*) {
  return nullptr;
}

int register_segments(struct ibv_pd**, rdmaxcel_qp_t**, int) {
  return kRdmaxcelNotInitialized;
}

void completion_cache_init(completion_cache_t*) {}
void completion_cache_destroy(completion_cache_t*) {}
int completion_cache_add(completion_cache_t*, struct ibv_wc*) {
  return 0;
}
int completion_cache_find(
    completion_cache_t*,
    uint64_t,
    uint32_t,
    struct ibv_wc*) {
  return 0;
}

int poll_cq_with_cache(poll_context_t*, struct ibv_wc*) {
  return -1;
}

// ---- ibverbs/mlx5dv creation (stubbed for TCP-only builds) -------------

struct ibv_qp* create_qp(
    struct ibv_context*,
    struct ibv_pd*,
    int,
    int,
    int,
    int,
    int,
    rdma_qp_type_t) {
  return nullptr;
}

struct mlx5dv_qp* create_mlx5dv_qp(struct ibv_qp*) {
  return nullptr;
}
struct mlx5dv_cq* create_mlx5dv_cq(struct ibv_qp*) {
  return nullptr;
}
struct mlx5dv_cq* create_mlx5dv_send_cq(struct ibv_qp*) {
  return nullptr;
}
struct mlx5dv_cq* create_mlx5dv_recv_cq(struct ibv_qp*) {
  return nullptr;
}

cudaError_t register_cuda_memory(
    struct mlx5dv_qp*,
    struct mlx5dv_cq*,
    struct mlx5dv_cq*) {
  return cudaErrorNotInitialized;
}

// ---- Launch helpers (normally in .cu) ----------------------------------

void launch_db_ring(void*, void*) {}
cqe_poll_result_t launch_cqe_poll(void*, int32_t) {
  return CQE_POLL_ERROR;
}
cqe_poll_result_t launch_send_cqe_poll(void*, int32_t) {
  return CQE_POLL_ERROR;
}
cqe_poll_result_t launch_recv_cqe_poll(void*, int32_t) {
  return CQE_POLL_ERROR;
}
void launch_send_wqe(wqe_params_t) {}
void launch_recv_wqe(wqe_params_t) {}

// ---- EFA ---------------------------------------------------------------

int rdmaxcel_qp_post_op(
    rdmaxcel_qp_t*,
    void*,
    uint32_t,
    size_t,
    void*,
    uint32_t,
    uint64_t,
    int,
    int) {
  return kRdmaxcelNotInitialized;
}

int rdmaxcel_is_efa_dev(struct ibv_context*) {
  return 0;
}

struct ibv_qp* create_efa_qp(
    struct ibv_context*,
    struct ibv_pd*,
    struct ibv_cq*,
    struct ibv_cq*,
    int,
    int,
    int,
    int) {
  return nullptr;
}

int rdmaxcel_efa_post_write(
    rdmaxcel_qp_t*,
    struct ibv_ah*,
    uint32_t,
    uint32_t,
    void*,
    uint32_t,
    size_t,
    void*,
    uint32_t,
    uint64_t) {
  return kRdmaxcelNotInitialized;
}

int rdmaxcel_efa_post_read(
    rdmaxcel_qp_t*,
    struct ibv_ah*,
    uint32_t,
    uint32_t,
    void*,
    uint32_t,
    size_t,
    void*,
    uint32_t,
    uint64_t) {
  return kRdmaxcelNotInitialized;
}

int rdmaxcel_efa_connect(
    rdmaxcel_qp_t*,
    uint8_t,
    uint16_t,
    uint32_t,
    uint32_t,
    uint8_t,
    const uint8_t*,
    uint32_t) {
  return kRdmaxcelNotInitialized;
}

int rdmaxcel_efa_post_op(
    rdmaxcel_qp_t*,
    struct ibv_ah*,
    uint32_t,
    uint32_t,
    void*,
    uint32_t,
    size_t,
    void*,
    uint32_t,
    uint64_t,
    int) {
  return kRdmaxcelNotInitialized;
}

} // extern "C"
