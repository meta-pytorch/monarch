/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <limits.h>
#include <stddef.h>
#include <stdint.h>

// Include real CUDA runtime headers for CUDA types
#include <cuda_runtime_api.h>

#ifdef __cplusplus
extern "C" {
#endif

// NCCL version constants - for NCCL 2.28.0
#define NCCL_MAJOR 2
#define NCCL_MINOR 28
#define NCCL_PATCH 0

// Opaque handle to communicator
typedef struct ncclComm* ncclComm_t;
#define NCCL_COMM_NULL NULL

#define NCCL_UNIQUE_ID_BYTES 128
typedef struct {
  char internal[NCCL_UNIQUE_ID_BYTES];
} ncclUniqueId;

// Error type
typedef enum {
  ncclSuccess = 0,
  ncclUnhandledCudaError = 1,
  ncclSystemError = 2,
  ncclInternalError = 3,
  ncclInvalidArgument = 4,
  ncclInvalidUsage = 5,
  ncclRemoteError = 6,
  ncclInProgress = 7,
  ncclNumResults = 8
} ncclResult_t;

#define NCCL_CONFIG_UNDEF_INT INT_MIN
#define NCCL_CONFIG_UNDEF_PTR NULL
#define NCCL_SPLIT_NOCOLOR -1

// Communicator configuration
typedef struct ncclConfig_v22800 {
  size_t size;
  unsigned int magic;
  unsigned int version;
  int blocking;
  int cgaClusterSize;
  int minCTAs;
  int maxCTAs;
  const char* netName;
  int splitShare;
  int trafficClass;
  const char* commName;
  int collnetEnable;
  int CTAPolicy;
  int shrinkShare;
  int nvlsCTAs;
  int nChannelsPerNetPeer;
  int nvlinkCentricSched;
} ncclConfig_t;

// Simulation info
typedef struct ncclSimInfo_v22200 {
  size_t size;
  unsigned int magic;
  unsigned int version;
  float estimatedTime;
} ncclSimInfo_t;

// Reduction operation selector
typedef enum {
  ncclSum = 0,
  ncclProd = 1,
  ncclMax = 2,
  ncclMin = 3,
  ncclAvg = 4,
  ncclNumOps = 5,
  ncclMaxRedOp = 0x7fffffff
} ncclRedOp_t;

// Data types
typedef enum {
  ncclInt8 = 0,
  ncclChar = 0,
  ncclUint8 = 1,
  ncclInt32 = 2,
  ncclInt = 2,
  ncclUint32 = 3,
  ncclInt64 = 4,
  ncclUint64 = 5,
  ncclFloat16 = 6,
  ncclHalf = 6,
  ncclFloat32 = 7,
  ncclFloat = 7,
  ncclFloat64 = 8,
  ncclDouble = 8,
  ncclBfloat16 = 9,
  ncclFloat8e4m3 = 10,
  ncclFloat8e5m2 = 11,
  ncclNumTypes = 12
} ncclDataType_t;

// Scalar residence
typedef enum {
  ncclScalarDevice = 0,
  ncclScalarHostImmediate = 1
} ncclScalarResidence_t;

// C API wrapper functions for NCCL functions
// These are loaded dynamically via dlopen and exported to Rust via bindgen

// Version and error handling
ncclResult_t ncclGetVersion(int* version);
ncclResult_t ncclGetUniqueId(ncclUniqueId* uniqueId);
const char* ncclGetErrorString(ncclResult_t result);
const char* ncclGetLastError(ncclComm_t comm);

// Communicator creation and management
ncclResult_t
ncclCommInitRank(ncclComm_t* comm, int nranks, ncclUniqueId commId, int rank);
ncclResult_t ncclCommInitAll(ncclComm_t* comm, int ndev, const int* devlist);
ncclResult_t ncclCommInitRankConfig(
    ncclComm_t* comm,
    int nranks,
    ncclUniqueId commId,
    int rank,
    ncclConfig_t* config);
ncclResult_t ncclCommInitRankScalable(
    ncclComm_t* newcomm,
    int nranks,
    int myrank,
    int nId,
    ncclUniqueId* commIds,
    ncclConfig_t* config);
ncclResult_t ncclCommSplit(
    ncclComm_t comm,
    int color,
    int key,
    ncclComm_t* newcomm,
    ncclConfig_t* config);
ncclResult_t ncclCommFinalize(ncclComm_t comm);
ncclResult_t ncclCommDestroy(ncclComm_t comm);
ncclResult_t ncclCommAbort(ncclComm_t comm);
ncclResult_t ncclCommGetAsyncError(ncclComm_t comm, ncclResult_t* asyncError);
ncclResult_t ncclCommCount(const ncclComm_t comm, int* count);
ncclResult_t ncclCommCuDevice(const ncclComm_t comm, int* device);
ncclResult_t ncclCommUserRank(const ncclComm_t comm, int* rank);

// Memory management
ncclResult_t
ncclCommRegister(const ncclComm_t comm, void* buff, size_t size, void** handle);
ncclResult_t ncclCommDeregister(const ncclComm_t comm, void* handle);
ncclResult_t ncclMemAlloc(void** ptr, size_t size);
ncclResult_t ncclMemFree(void* ptr);

// Collective communication
ncclResult_t ncclAllReduce(
    const void* sendbuff,
    void* recvbuff,
    size_t count,
    ncclDataType_t datatype,
    ncclRedOp_t op,
    ncclComm_t comm,
    cudaStream_t stream);
ncclResult_t ncclBroadcast(
    const void* sendbuff,
    void* recvbuff,
    size_t count,
    ncclDataType_t datatype,
    int root,
    ncclComm_t comm,
    cudaStream_t stream);
ncclResult_t ncclReduce(
    const void* sendbuff,
    void* recvbuff,
    size_t count,
    ncclDataType_t datatype,
    ncclRedOp_t op,
    int root,
    ncclComm_t comm,
    cudaStream_t stream);
ncclResult_t ncclAllGather(
    const void* sendbuff,
    void* recvbuff,
    size_t sendcount,
    ncclDataType_t datatype,
    ncclComm_t comm,
    cudaStream_t stream);
ncclResult_t ncclReduceScatter(
    const void* sendbuff,
    void* recvbuff,
    size_t recvcount,
    ncclDataType_t datatype,
    ncclRedOp_t op,
    ncclComm_t comm,
    cudaStream_t stream);

// Point to point communication
ncclResult_t ncclSend(
    const void* sendbuff,
    size_t count,
    ncclDataType_t datatype,
    int peer,
    ncclComm_t comm,
    cudaStream_t stream);
ncclResult_t ncclRecv(
    void* recvbuff,
    size_t count,
    ncclDataType_t datatype,
    int peer,
    ncclComm_t comm,
    cudaStream_t stream);

// Group calls
ncclResult_t ncclGroupStart();
ncclResult_t ncclGroupEnd();
ncclResult_t ncclGroupSimulateEnd(ncclSimInfo_t* simInfo);

// User-defined reduction operators
ncclResult_t ncclRedOpCreatePreMulSum(
    ncclRedOp_t* op,
    void* scalar,
    ncclDataType_t datatype,
    ncclScalarResidence_t residence,
    ncclComm_t comm);
ncclResult_t ncclRedOpDestroy(ncclRedOp_t op, ncclComm_t comm);

#ifdef __cplusplus
} // extern "C"
#endif
