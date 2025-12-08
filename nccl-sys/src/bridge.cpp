/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "bridge.h"
#include <dlfcn.h>
#include <iostream>
#include <stdexcept>

namespace nccl_sys {

struct NcclAPI {
  // Version and error handling
  ncclResult_t (*ncclGetVersion_)(int*);
  ncclResult_t (*ncclGetUniqueId_)(ncclUniqueId*);
  const char* (*ncclGetErrorString_)(ncclResult_t);
  const char* (*ncclGetLastError_)(ncclComm_t);
  // Communicator creation and management
  ncclResult_t (*ncclCommInitRank_)(ncclComm_t*, int, ncclUniqueId, int);
  ncclResult_t (*ncclCommInitAll_)(ncclComm_t*, int, const int*);
  ncclResult_t (*ncclCommInitRankConfig_)(
      ncclComm_t*,
      int,
      ncclUniqueId,
      int,
      ncclConfig_t*);
  ncclResult_t (*ncclCommInitRankScalable_)(
      ncclComm_t*,
      int,
      int,
      int,
      ncclUniqueId*,
      ncclConfig_t*);
  ncclResult_t (
      *ncclCommSplit_)(ncclComm_t, int, int, ncclComm_t*, ncclConfig_t*);
  ncclResult_t (*ncclCommFinalize_)(ncclComm_t);
  ncclResult_t (*ncclCommDestroy_)(ncclComm_t);
  ncclResult_t (*ncclCommAbort_)(ncclComm_t);
  ncclResult_t (*ncclCommGetAsyncError_)(ncclComm_t, ncclResult_t*);
  ncclResult_t (*ncclCommCount_)(const ncclComm_t, int*);
  ncclResult_t (*ncclCommCuDevice_)(const ncclComm_t, int*);
  ncclResult_t (*ncclCommUserRank_)(const ncclComm_t, int*);
  // Memory management
  ncclResult_t (*ncclCommRegister_)(const ncclComm_t, void*, size_t, void**);
  ncclResult_t (*ncclCommDeregister_)(const ncclComm_t, void*);
  ncclResult_t (*ncclMemAlloc_)(void**, size_t);
  ncclResult_t (*ncclMemFree_)(void*);
  // Collective communication
  ncclResult_t (*ncclAllReduce_)(
      const void*,
      void*,
      size_t,
      ncclDataType_t,
      ncclRedOp_t,
      ncclComm_t,
      cudaStream_t);
  ncclResult_t (*ncclBroadcast_)(
      const void*,
      void*,
      size_t,
      ncclDataType_t,
      int,
      ncclComm_t,
      cudaStream_t);
  ncclResult_t (*ncclReduce_)(
      const void*,
      void*,
      size_t,
      ncclDataType_t,
      ncclRedOp_t,
      int,
      ncclComm_t,
      cudaStream_t);
  ncclResult_t (*ncclAllGather_)(
      const void*,
      void*,
      size_t,
      ncclDataType_t,
      ncclComm_t,
      cudaStream_t);
  ncclResult_t (*ncclReduceScatter_)(
      const void*,
      void*,
      size_t,
      ncclDataType_t,
      ncclRedOp_t,
      ncclComm_t,
      cudaStream_t);
  // Point to point communication
  ncclResult_t (*ncclSend_)(
      const void*,
      size_t,
      ncclDataType_t,
      int,
      ncclComm_t,
      cudaStream_t);
  ncclResult_t (
      *ncclRecv_)(void*, size_t, ncclDataType_t, int, ncclComm_t, cudaStream_t);
  // Group calls
  ncclResult_t (*ncclGroupStart_)();
  ncclResult_t (*ncclGroupEnd_)();
  ncclResult_t (*ncclGroupSimulateEnd_)(ncclSimInfo_t*);
  // User-defined reduction operators
  ncclResult_t (*ncclRedOpCreatePreMulSum_)(
      ncclRedOp_t*,
      void*,
      ncclDataType_t,
      ncclScalarResidence_t,
      ncclComm_t);
  ncclResult_t (*ncclRedOpDestroy_)(ncclRedOp_t, ncclComm_t);

  // Indicates whether initialization succeeded
  ncclResult_t init_result_;

  static NcclAPI* get();
};

namespace {

NcclAPI create_nccl_api() {
  NcclAPI r{};
  r.init_result_ = ncclSuccess;

  // Try to open libnccl.so - RTLD_NOLOAD means only succeed if already loaded
  void* handle = dlopen("libnccl.so", RTLD_LAZY | RTLD_NOLOAD);
  if (!handle) {
    handle = dlopen("libnccl.so", RTLD_LAZY);
  }

  // Try alternative names
  if (!handle) {
    handle = dlopen("libnccl.so.2", RTLD_LAZY);
  }

  if (!handle) {
    std::cerr << "[NCCL-SYS] Warning: Can't open libnccl.so: " << dlerror()
              << std::endl;
    r.init_result_ = ncclSystemError;
    return r;
  }

#define LOOKUP_NCCL_ENTRY(name)                                            \
  r.name##_ = reinterpret_cast<decltype(r.name##_)>(dlsym(handle, #name)); \
  if (!r.name##_) {                                                        \
    std::cerr << "[NCCL-SYS] Warning: Can't find " << #name << ": "       \
              << dlerror() << std::endl;                                   \
    r.init_result_ = ncclSystemError;                                      \
    return r;                                                              \
  }

  LOOKUP_NCCL_ENTRY(ncclGetVersion)
  LOOKUP_NCCL_ENTRY(ncclGetUniqueId)
  LOOKUP_NCCL_ENTRY(ncclGetErrorString)
  LOOKUP_NCCL_ENTRY(ncclGetLastError)
  LOOKUP_NCCL_ENTRY(ncclCommInitRank)
  LOOKUP_NCCL_ENTRY(ncclCommInitAll)
  LOOKUP_NCCL_ENTRY(ncclCommInitRankConfig)
  LOOKUP_NCCL_ENTRY(ncclCommInitRankScalable)
  LOOKUP_NCCL_ENTRY(ncclCommSplit)
  LOOKUP_NCCL_ENTRY(ncclCommFinalize)
  LOOKUP_NCCL_ENTRY(ncclCommDestroy)
  LOOKUP_NCCL_ENTRY(ncclCommAbort)
  LOOKUP_NCCL_ENTRY(ncclCommGetAsyncError)
  LOOKUP_NCCL_ENTRY(ncclCommCount)
  LOOKUP_NCCL_ENTRY(ncclCommCuDevice)
  LOOKUP_NCCL_ENTRY(ncclCommUserRank)
  LOOKUP_NCCL_ENTRY(ncclCommRegister)
  LOOKUP_NCCL_ENTRY(ncclCommDeregister)
  LOOKUP_NCCL_ENTRY(ncclMemAlloc)
  LOOKUP_NCCL_ENTRY(ncclMemFree)
  LOOKUP_NCCL_ENTRY(ncclAllReduce)
  LOOKUP_NCCL_ENTRY(ncclBroadcast)
  LOOKUP_NCCL_ENTRY(ncclReduce)
  LOOKUP_NCCL_ENTRY(ncclAllGather)
  LOOKUP_NCCL_ENTRY(ncclReduceScatter)
  LOOKUP_NCCL_ENTRY(ncclSend)
  LOOKUP_NCCL_ENTRY(ncclRecv)
  LOOKUP_NCCL_ENTRY(ncclGroupStart)
  LOOKUP_NCCL_ENTRY(ncclGroupEnd)
  LOOKUP_NCCL_ENTRY(ncclGroupSimulateEnd)
  LOOKUP_NCCL_ENTRY(ncclRedOpCreatePreMulSum)
  LOOKUP_NCCL_ENTRY(ncclRedOpDestroy)
#undef LOOKUP_NCCL_ENTRY

  // Verify version compatibility
  int version = 0;
  ncclResult_t result = r.ncclGetVersion_(&version);
  if (result != ncclSuccess) {
    std::cerr << "[NCCL-SYS] Warning: Failed to get NCCL version" << std::endl;
    r.init_result_ = ncclSystemError;
    return r;
  }

  // Extract major version (version is encoded as MAJOR*10000 + MINOR*100 +
  // PATCH for >= 2.9)
  int major = version / 10000;
  if (major == 0) {
    // For older versions (2.0-2.8), encoding was MAJOR*1000 + MINOR*100 + PATCH
    major = version / 1000;
  }

  if (major != NCCL_MAJOR) {
    std::cerr
        << "[NCCL-SYS] Warning: NCCL version mismatch. Expected major version "
        << NCCL_MAJOR << ", but got " << major << " (full version: " << version
        << ")" << std::endl;
    r.init_result_ = ncclSystemError;
    return r;
  }

  return r;
}

} // namespace

NcclAPI* NcclAPI::get() {
  static NcclAPI singleton = create_nccl_api();
  return &singleton;
}

} // namespace nccl_sys

// Macro to get the NCCL API and return early if initialization failed
#define GET_NCCL_API(api_ptr)                   \
  nccl_sys::NcclAPI* api_ptr = nccl_sys::NcclAPI::get(); \
  if (api_ptr->init_result_ != ncclSuccess) {   \
    return api_ptr->init_result_;               \
  }

// Macro for functions that return const char*
#define GET_NCCL_API_STR(api_ptr)               \
  nccl_sys::NcclAPI* api_ptr = nccl_sys::NcclAPI::get(); \
  if (api_ptr->init_result_ != ncclSuccess) {   \
    return "[NCCL-SYS] NCCL library not initialized"; \
  }

// C API wrapper implementations
extern "C" {

// Version and error handling
ncclResult_t ncclGetVersion(int* version) {
  GET_NCCL_API(api);
  return api->ncclGetVersion_(version);
}

ncclResult_t ncclGetUniqueId(ncclUniqueId* uniqueId) {
  GET_NCCL_API(api);
  return api->ncclGetUniqueId_(uniqueId);
}

const char* ncclGetErrorString(ncclResult_t result) {
  GET_NCCL_API_STR(api);
  return api->ncclGetErrorString_(result);
}

const char* ncclGetLastError(ncclComm_t comm) {
  GET_NCCL_API_STR(api);
  return api->ncclGetLastError_(comm);
}

// Communicator creation and management
ncclResult_t ncclCommInitRank(
    ncclComm_t* comm,
    int nranks,
    ncclUniqueId commId,
    int rank) {
  GET_NCCL_API(api);
  return api->ncclCommInitRank_(comm, nranks, commId, rank);
}

ncclResult_t
ncclCommInitAll(ncclComm_t* comm, int ndev, const int* devlist) {
  GET_NCCL_API(api);
  return api->ncclCommInitAll_(comm, ndev, devlist);
}

ncclResult_t ncclCommInitRankConfig(
    ncclComm_t* comm,
    int nranks,
    ncclUniqueId commId,
    int rank,
    ncclConfig_t* config) {
  GET_NCCL_API(api);
  return api->ncclCommInitRankConfig_(comm, nranks, commId, rank, config);
}

ncclResult_t ncclCommInitRankScalable(
    ncclComm_t* newcomm,
    int nranks,
    int myrank,
    int nId,
    ncclUniqueId* commIds,
    ncclConfig_t* config) {
  GET_NCCL_API(api);
  return api->ncclCommInitRankScalable_(
      newcomm, nranks, myrank, nId, commIds, config);
}

ncclResult_t ncclCommSplit(
    ncclComm_t comm,
    int color,
    int key,
    ncclComm_t* newcomm,
    ncclConfig_t* config) {
  GET_NCCL_API(api);
  return api->ncclCommSplit_(comm, color, key, newcomm, config);
}

ncclResult_t ncclCommFinalize(ncclComm_t comm) {
  GET_NCCL_API(api);
  return api->ncclCommFinalize_(comm);
}

ncclResult_t ncclCommDestroy(ncclComm_t comm) {
  GET_NCCL_API(api);
  return api->ncclCommDestroy_(comm);
}

ncclResult_t ncclCommAbort(ncclComm_t comm) {
  GET_NCCL_API(api);
  return api->ncclCommAbort_(comm);
}

ncclResult_t ncclCommGetAsyncError(
    ncclComm_t comm,
    ncclResult_t* asyncError) {
  GET_NCCL_API(api);
  return api->ncclCommGetAsyncError_(comm, asyncError);
}

ncclResult_t ncclCommCount(const ncclComm_t comm, int* count) {
  GET_NCCL_API(api);
  return api->ncclCommCount_(comm, count);
}

ncclResult_t ncclCommCuDevice(const ncclComm_t comm, int* device) {
  GET_NCCL_API(api);
  return api->ncclCommCuDevice_(comm, device);
}

ncclResult_t ncclCommUserRank(const ncclComm_t comm, int* rank) {
  GET_NCCL_API(api);
  return api->ncclCommUserRank_(comm, rank);
}

// Memory management
ncclResult_t ncclCommRegister(
    const ncclComm_t comm,
    void* buff,
    size_t size,
    void** handle) {
  GET_NCCL_API(api);
  return api->ncclCommRegister_(comm, buff, size, handle);
}

ncclResult_t ncclCommDeregister(const ncclComm_t comm, void* handle) {
  GET_NCCL_API(api);
  return api->ncclCommDeregister_(comm, handle);
}

ncclResult_t ncclMemAlloc(void** ptr, size_t size) {
  GET_NCCL_API(api);
  return api->ncclMemAlloc_(ptr, size);
}

ncclResult_t ncclMemFree(void* ptr) {
  GET_NCCL_API(api);
  return api->ncclMemFree_(ptr);
}

// Collective communication
ncclResult_t ncclAllReduce(
    const void* sendbuff,
    void* recvbuff,
    size_t count,
    ncclDataType_t datatype,
    ncclRedOp_t op,
    ncclComm_t comm,
    cudaStream_t stream) {
  GET_NCCL_API(api);
  return api->ncclAllReduce_(
      sendbuff, recvbuff, count, datatype, op, comm, stream);
}

ncclResult_t ncclBroadcast(
    const void* sendbuff,
    void* recvbuff,
    size_t count,
    ncclDataType_t datatype,
    int root,
    ncclComm_t comm,
    cudaStream_t stream) {
  GET_NCCL_API(api);
  return api->ncclBroadcast_(
      sendbuff, recvbuff, count, datatype, root, comm, stream);
}

ncclResult_t ncclReduce(
    const void* sendbuff,
    void* recvbuff,
    size_t count,
    ncclDataType_t datatype,
    ncclRedOp_t op,
    int root,
    ncclComm_t comm,
    cudaStream_t stream) {
  GET_NCCL_API(api);
  return api->ncclReduce_(
      sendbuff, recvbuff, count, datatype, op, root, comm, stream);
}

ncclResult_t ncclAllGather(
    const void* sendbuff,
    void* recvbuff,
    size_t sendcount,
    ncclDataType_t datatype,
    ncclComm_t comm,
    cudaStream_t stream) {
  GET_NCCL_API(api);
  return api->ncclAllGather_(
      sendbuff, recvbuff, sendcount, datatype, comm, stream);
}

ncclResult_t ncclReduceScatter(
    const void* sendbuff,
    void* recvbuff,
    size_t recvcount,
    ncclDataType_t datatype,
    ncclRedOp_t op,
    ncclComm_t comm,
    cudaStream_t stream) {
  GET_NCCL_API(api);
  return api->ncclReduceScatter_(
      sendbuff, recvbuff, recvcount, datatype, op, comm, stream);
}

// Point to point communication
ncclResult_t ncclSend(
    const void* sendbuff,
    size_t count,
    ncclDataType_t datatype,
    int peer,
    ncclComm_t comm,
    cudaStream_t stream) {
  GET_NCCL_API(api);
  return api->ncclSend_(sendbuff, count, datatype, peer, comm, stream);
}

ncclResult_t ncclRecv(
    void* recvbuff,
    size_t count,
    ncclDataType_t datatype,
    int peer,
    ncclComm_t comm,
    cudaStream_t stream) {
  GET_NCCL_API(api);
  return api->ncclRecv_(recvbuff, count, datatype, peer, comm, stream);
}

// Group calls
ncclResult_t ncclGroupStart() {
  GET_NCCL_API(api);
  return api->ncclGroupStart_();
}

ncclResult_t ncclGroupEnd() {
  GET_NCCL_API(api);
  return api->ncclGroupEnd_();
}

ncclResult_t ncclGroupSimulateEnd(ncclSimInfo_t* simInfo) {
  GET_NCCL_API(api);
  return api->ncclGroupSimulateEnd_(simInfo);
}

// User-defined reduction operators
ncclResult_t ncclRedOpCreatePreMulSum(
    ncclRedOp_t* op,
    void* scalar,
    ncclDataType_t datatype,
    ncclScalarResidence_t residence,
    ncclComm_t comm) {
  GET_NCCL_API(api);
  return api->ncclRedOpCreatePreMulSum_(op, scalar, datatype, residence, comm);
}

ncclResult_t ncclRedOpDestroy(ncclRedOp_t op, ncclComm_t comm) {
  GET_NCCL_API(api);
  return api->ncclRedOpDestroy_(op, comm);
}

} // extern "C"
