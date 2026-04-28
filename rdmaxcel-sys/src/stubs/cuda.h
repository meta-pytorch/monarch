/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Stub replacement for <cuda.h> used in no-GPU builds.
//
// This header is picked up by bindgen and the stub compilation unit in
// place of the real CUDA driver header when MONARCH_GPU_PLATFORM=none.
// It declares only the handful of types and constants that are named
// in `rdmaxcel.h`, `driver_api.h`, and the Rust code in `monarch_rdma`.
//
// The actual driver functions (cuInit, cuMemAlloc, ...) are NOT
// declared here; callers go through `rdmaxcel_cu*` wrappers whose
// stub implementations return CUDA_ERROR_NOT_INITIALIZED.

#ifndef RDMAXCEL_STUB_CUDA_H
#define RDMAXCEL_STUB_CUDA_H

#include <stddef.h>
#include <stdint.h>

// CUDA function-space keywords. `cuda_runtime.h` normally defines these;
// repeat them here so headers that include only `<cuda.h>` still parse.
#ifndef __host__
#define __host__
#endif
#ifndef __device__
#define __device__
#endif
#ifndef __global__
#define __global__
#endif
#ifndef __forceinline__
#define __forceinline__ inline
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef enum cudaError_enum {
  CUDA_SUCCESS = 0,
  CUDA_ERROR_INVALID_VALUE = 1,
  CUDA_ERROR_NOT_INITIALIZED = 3,
  CUDA_ERROR_DEINITIALIZED = 4,
  CUDA_ERROR_NO_DEVICE = 100,
  CUDA_ERROR_INVALID_DEVICE = 101,
  CUDA_ERROR_UNKNOWN = 999,
} CUresult;

typedef int CUdevice;
typedef unsigned long long CUdeviceptr;
typedef struct CUctx_st* CUcontext;
typedef unsigned long long CUmemGenericAllocationHandle;

typedef enum CUpointer_attribute_enum {
  CU_POINTER_ATTRIBUTE_CONTEXT = 1,
  CU_POINTER_ATTRIBUTE_MEMORY_TYPE = 2,
  CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL = 9,
} CUpointer_attribute;

typedef enum CUmemorytype_enum {
  CU_MEMORYTYPE_HOST = 0x01,
  CU_MEMORYTYPE_DEVICE = 0x02,
  CU_MEMORYTYPE_ARRAY = 0x03,
  CU_MEMORYTYPE_UNIFIED = 0x04,
} CUmemorytype;

typedef enum CUdevice_attribute_enum {
  CU_DEVICE_ATTRIBUTE_UNUSED = 0,
} CUdevice_attribute;

typedef enum CUmemAllocationType_enum {
  CU_MEM_ALLOCATION_TYPE_INVALID = 0,
  CU_MEM_ALLOCATION_TYPE_PINNED = 1,
} CUmemAllocationType;

typedef enum CUmemLocationType_enum {
  CU_MEM_LOCATION_TYPE_INVALID = 0,
  CU_MEM_LOCATION_TYPE_DEVICE = 1,
} CUmemLocationType;

typedef enum CUmemAllocationHandleType_enum {
  CU_MEM_HANDLE_TYPE_NONE = 0,
  CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR = 1,
} CUmemAllocationHandleType;

typedef enum CUmemAccess_flags_enum {
  CU_MEM_ACCESS_FLAGS_PROT_NONE = 0,
  CU_MEM_ACCESS_FLAGS_PROT_READ = 1,
  CU_MEM_ACCESS_FLAGS_PROT_READWRITE = 3,
} CUmemAccess_flags;

typedef enum CUmemAllocationGranularity_flags_enum {
  CU_MEM_ALLOC_GRANULARITY_MINIMUM = 0,
  CU_MEM_ALLOC_GRANULARITY_RECOMMENDED = 1,
} CUmemAllocationGranularity_flags;

typedef enum CUmemRangeHandleType_enum {
  CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD = 1,
} CUmemRangeHandleType;

typedef struct CUmemLocation_st {
  CUmemLocationType type;
  int id;
} CUmemLocation;

typedef struct CUmemAllocationProp_st {
  CUmemAllocationType type;
  CUmemAllocationHandleType requestedHandleTypes;
  CUmemLocation location;
  void* win32HandleMetaData;
  struct {
    unsigned char compressionType;
    unsigned char gpuDirectRDMACapable;
    unsigned short usage;
    unsigned char reserved[4];
  } allocFlags;
} CUmemAllocationProp;

typedef struct CUmemAccessDesc_st {
  CUmemLocation location;
  CUmemAccess_flags flags;
} CUmemAccessDesc;

#ifdef __cplusplus
} // extern "C"
#endif

#endif // RDMAXCEL_STUB_CUDA_H
