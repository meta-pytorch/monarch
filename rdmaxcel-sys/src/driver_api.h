/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once
#include <cuda.h>

// ROCm/HIP compatibility: hipify_torch converts most CUDA->HIP automatically,
// but these CUDA Driver API types/constants are not in hipify's mappings.
// We typedef them to their HIP equivalents so the code compiles on ROCm.
#ifdef USE_ROCM
typedef hipMemRangeHandleType CUmemRangeHandleType;
#define CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD hipMemRangeHandleTypeDmaBufFd
#endif

// `noexcept` is C++-only; elide it when this header is parsed as C (bindgen
// parses it as C, and rdmaxcel.h pulls it into C translation units). The
// no-throw guarantee is enforced where the definition is compiled as C++.
#ifndef RDMAXCEL_NOEXCEPT
#ifdef __cplusplus
#define RDMAXCEL_NOEXCEPT noexcept
#else
#define RDMAXCEL_NOEXCEPT
#endif
#endif

#ifdef __cplusplus
extern "C" {
#endif

// C API wrapper functions for CUDA driver functions
// These are loaded dynamically via dlopen and exported to Rust via bindgen

// Memory management
CUresult rdmaxcel_cuMemGetHandleForAddressRange(
    int* handle,
    CUdeviceptr dptr,
    size_t size,
    CUmemRangeHandleType handleType,
    unsigned long long flags) RDMAXCEL_NOEXCEPT;

CUresult rdmaxcel_cuMemGetAllocationGranularity(
    size_t* granularity,
    const CUmemAllocationProp* prop,
    CUmemAllocationGranularity_flags option) RDMAXCEL_NOEXCEPT;

CUresult rdmaxcel_cuMemCreate(
    CUmemGenericAllocationHandle* handle,
    size_t size,
    const CUmemAllocationProp* prop,
    unsigned long long flags) RDMAXCEL_NOEXCEPT;

CUresult rdmaxcel_cuMemAddressReserve(
    CUdeviceptr* ptr,
    size_t size,
    size_t alignment,
    CUdeviceptr addr,
    unsigned long long flags) RDMAXCEL_NOEXCEPT;

CUresult rdmaxcel_cuMemMap(
    CUdeviceptr ptr,
    size_t size,
    size_t offset,
    CUmemGenericAllocationHandle handle,
    unsigned long long flags) RDMAXCEL_NOEXCEPT;

CUresult rdmaxcel_cuMemSetAccess(
    CUdeviceptr ptr,
    size_t size,
    const CUmemAccessDesc* desc,
    size_t count) RDMAXCEL_NOEXCEPT;

CUresult rdmaxcel_cuMemUnmap(CUdeviceptr ptr, size_t size) RDMAXCEL_NOEXCEPT;

CUresult rdmaxcel_cuMemAddressFree(CUdeviceptr ptr, size_t size)
    RDMAXCEL_NOEXCEPT;

CUresult rdmaxcel_cuMemRelease(CUmemGenericAllocationHandle handle)
    RDMAXCEL_NOEXCEPT;

CUresult rdmaxcel_cuMemcpyHtoD_v2(
    CUdeviceptr dstDevice,
    const void* srcHost,
    size_t ByteCount) RDMAXCEL_NOEXCEPT;

CUresult rdmaxcel_cuMemcpyDtoH_v2(
    void* dstHost,
    CUdeviceptr srcDevice,
    size_t ByteCount) RDMAXCEL_NOEXCEPT;

CUresult rdmaxcel_cuMemsetD8_v2(
    CUdeviceptr dstDevice,
    unsigned char uc,
    size_t N) RDMAXCEL_NOEXCEPT;

// Pointer queries
CUresult rdmaxcel_cuPointerGetAttribute(
    void* data,
    CUpointer_attribute attribute,
    CUdeviceptr ptr) RDMAXCEL_NOEXCEPT;

// Driver library loading
//
// Ensure the GPU driver library is loaded into the process. Call this before
// rdmaxcel_cuInit from tests/tools that must initialize the driver from
// scratch; the wrapper functions never load the library themselves -- they
// only adopt one the owning framework loaded. Returns 0 on success and -1
// if the library could not be loaded. A load failure will be cached behind
// a function-local static variable, so if this fails once, it will never
// succeed for the lifetime of the process.
int ensure_cuda_driver_loaded(void) RDMAXCEL_NOEXCEPT;

// Device management
CUresult rdmaxcel_cuInit(unsigned int Flags) RDMAXCEL_NOEXCEPT;

CUresult rdmaxcel_cuDeviceGet(CUdevice* device, int ordinal) RDMAXCEL_NOEXCEPT;

CUresult rdmaxcel_cuDeviceGetCount(int* count) RDMAXCEL_NOEXCEPT;

CUresult rdmaxcel_cuDeviceGetAttribute(
    int* pi,
    CUdevice_attribute attrib,
    CUdevice dev) RDMAXCEL_NOEXCEPT;

// Context management
CUresult rdmaxcel_cuCtxCreate_v2(
    CUcontext* pctx,
    unsigned int flags,
    CUdevice dev) RDMAXCEL_NOEXCEPT;

CUresult rdmaxcel_cuDevicePrimaryCtxRetain(CUcontext* pctx, CUdevice dev)
    RDMAXCEL_NOEXCEPT;

CUresult rdmaxcel_cuDevicePrimaryCtxRelease(CUdevice dev) RDMAXCEL_NOEXCEPT;

CUresult rdmaxcel_cuCtxGetCurrent(CUcontext* pctx) RDMAXCEL_NOEXCEPT;

CUresult rdmaxcel_cuCtxSetCurrent(CUcontext ctx) RDMAXCEL_NOEXCEPT;

CUresult rdmaxcel_cuCtxSynchronize(void) RDMAXCEL_NOEXCEPT;

// Error handling
CUresult rdmaxcel_cuGetErrorString(CUresult error, const char** pStr)
    RDMAXCEL_NOEXCEPT;

#ifdef __cplusplus
} // extern "C"
#endif
